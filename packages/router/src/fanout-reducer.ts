import { NozzleError } from "@nozzle/core"
import { compareFanoutOrderValues, type FanoutFailure, type FanoutOrderColumn } from "./fanout.js"

export type FanoutReductionShard<T> =
  | {
      readonly kind: "success"
      readonly shardId: string
      readonly value: T
    }
  | {
      readonly errorCode: string
      readonly kind: "failure"
      readonly shardId: string
    }

export interface FanoutReductionResult<T> {
  readonly complete: boolean
  readonly consistency: "best_effort_no_global_snapshot"
  readonly failures: readonly FanoutFailure[]
  readonly incomplete: boolean
  readonly value: T
}

const MAX_REDUCTION_SHARDS = 10_000
const UTF8 = new TextEncoder()

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function exactRecord(value: object, keys: readonly string[]): boolean {
  const actual = Object.keys(value)
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key))
}

function isWellFormedText(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false
    }
  }
  return true
}

function hasAsciiControl(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code <= 0x1f || code === 0x7f) return true
  }
  return false
}

function validText(value: unknown, label: string): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.trim() !== value ||
    !isWellFormedText(value) ||
    UTF8.encode(value).byteLength > 255 ||
    hasAsciiControl(value)
  ) {
    configuration(`${label} is malformed.`)
  }
}

function compareUtf8(left: string, right: string): number {
  return compareFanoutOrderValues(left, right, {
    direction: "asc",
    immutable: true,
    kind: "string",
    nulls: "last",
  })
}

function canonicalShardIds(shardIds: readonly string[]): readonly string[] {
  if (!Array.isArray(shardIds) || shardIds.length < 1 || shardIds.length > MAX_REDUCTION_SHARDS) {
    configuration(`Fan-out reduction requires between 1 and ${MAX_REDUCTION_SHARDS} shards.`)
  }
  for (const shardId of shardIds) validText(shardId, "Fan-out reduction shard ID")
  const canonical = [...new Set(shardIds)].sort(compareUtf8)
  if (
    canonical.length !== shardIds.length ||
    canonical.some((shardId, index) => shardId !== shardIds[index])
  ) {
    configuration("Fan-out reduction shard membership must be unique and canonical.")
  }
  return canonical
}

export function reduceFanout<T, A, R = A>(input: {
  readonly finalize?: (accumulator: A) => R
  readonly initial: A
  readonly partialPolicy: "allow" | "fail"
  readonly reduce: (accumulator: A, value: T, shardId: string) => A
  readonly shardIds: readonly string[]
  readonly shards: readonly FanoutReductionShard<T>[]
}): FanoutReductionResult<R> {
  if (typeof input.reduce !== "function") configuration("Fan-out reducer must be a function.")
  if (input.finalize !== undefined && typeof input.finalize !== "function") {
    configuration("Fan-out reducer finalizer must be a function.")
  }
  if (input.partialPolicy !== "allow" && input.partialPolicy !== "fail") {
    configuration("Fan-out reduction partial-result policy is unsupported.")
  }
  const shardIds = canonicalShardIds(input.shardIds)
  if (!Array.isArray(input.shards) || input.shards.length !== shardIds.length) {
    configuration("Fan-out reduction results do not match the sealed shard set.")
  }
  const expected = new Set(shardIds)
  const byShard = new Map<string, FanoutReductionShard<T>>()
  for (const shard of input.shards) {
    if (typeof shard !== "object" || shard === null) {
      configuration("Fan-out reduction shard result is malformed.")
    }
    if (shard.kind !== "failure" && shard.kind !== "success") {
      configuration("Fan-out reduction shard result kind is unsupported.")
    }
    if (
      !exactRecord(
        shard,
        shard.kind === "failure" ? ["errorCode", "kind", "shardId"] : ["kind", "shardId", "value"],
      )
    ) {
      configuration("Fan-out reduction shard result fields are malformed.")
    }
    validText(shard.shardId, "Fan-out reduction result shard ID")
    if (!expected.has(shard.shardId) || byShard.has(shard.shardId)) {
      configuration("Fan-out reduction result membership is contradictory.")
    }
    if (shard.kind === "failure") {
      validText(shard.errorCode, "Fan-out reduction failure code")
    }
    byShard.set(shard.shardId, shard)
  }
  const failures = Object.freeze(
    shardIds.flatMap((shardId) => {
      const shard = byShard.get(shardId) as FanoutReductionShard<T>
      return shard.kind === "failure"
        ? [Object.freeze({ errorCode: shard.errorCode, shardId })]
        : []
    }),
  )
  if (failures.length > 0 && input.partialPolicy === "fail") {
    throw new NozzleError("ShardUnavailableError", "A required fan-out reduction shard failed.", {
      details: { failures },
    })
  }
  let accumulator = input.initial
  for (const shardId of shardIds) {
    const shard = byShard.get(shardId) as FanoutReductionShard<T>
    if (shard.kind === "success") accumulator = input.reduce(accumulator, shard.value, shardId)
  }
  const value =
    input.finalize === undefined ? (accumulator as unknown as R) : input.finalize(accumulator)
  return Object.freeze({
    complete: failures.length === 0,
    consistency: "best_effort_no_global_snapshot" as const,
    failures,
    incomplete: failures.length > 0,
    value,
  })
}

function unsignedBigInt(value: bigint | number | string): bigint {
  if (typeof value === "bigint") {
    if (value < 0n) configuration("Fan-out count partial must be non-negative.")
    return value
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      configuration("Fan-out count number must be a non-negative safe integer.")
    }
    return BigInt(value)
  }
  if (typeof value !== "string" || value.length > 128 || !/^(?:0|[1-9][0-9]*)$/u.test(value)) {
    configuration("Fan-out count decimal string is malformed.")
  }
  return BigInt(value)
}

function signedBigInt(value: bigint | number | string): bigint {
  if (typeof value === "bigint") return value
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      configuration("Fan-out integer sum partial must be a safe integer or decimal string.")
    }
    return BigInt(value)
  }
  if (typeof value !== "string" || value.length > 128 || !/^(?:0|-?[1-9][0-9]*)$/u.test(value)) {
    configuration("Fan-out integer sum decimal string is malformed.")
  }
  return BigInt(value)
}

type BuiltinInput<T> = Readonly<{
  partialPolicy: "allow" | "fail"
  shardIds: readonly string[]
  shards: readonly FanoutReductionShard<T>[]
}>

export function countFanout(
  input: BuiltinInput<bigint | number | string>,
): FanoutReductionResult<bigint> {
  return reduceFanout({
    ...input,
    initial: 0n,
    reduce: (total, value) => total + unsignedBigInt(value),
  })
}

export function sumFanoutBigInts(
  input: BuiltinInput<bigint | number | string>,
): FanoutReductionResult<bigint> {
  return reduceFanout({
    ...input,
    initial: 0n,
    reduce: (total, value) => total + signedBigInt(value),
  })
}

interface CompensatedSum {
  readonly compensation: number
  readonly sum: number
}

export function sumFanoutNumbers(input: BuiltinInput<number>): FanoutReductionResult<number> {
  return reduceFanout<number, CompensatedSum, number>({
    ...input,
    finalize: ({ compensation, sum }) => sum + compensation,
    initial: { compensation: 0, sum: 0 },
    reduce: ({ compensation, sum }, value) => {
      if (!Number.isFinite(value)) configuration("Fan-out numeric sum partial must be finite.")
      const next = sum + value
      if (!Number.isFinite(next)) configuration("Fan-out numeric sum overflowed.")
      const nextCompensation =
        compensation + (Math.abs(sum) >= Math.abs(value) ? sum - next + value : value - next + sum)
      return { compensation: nextCompensation, sum: next }
    },
  })
}

function extremaFanout(
  input: BuiltinInput<null | number | string> & {
    readonly kind: "number" | "string"
    readonly mode: "max" | "min"
  },
): FanoutReductionResult<null | number | string> {
  const column: FanoutOrderColumn = {
    direction: "asc",
    immutable: true,
    kind: input.kind,
    nulls: "last",
  }
  return reduceFanout({
    ...input,
    initial: null as null | number | string,
    reduce: (current, value) => {
      if (value === null) return current
      if (
        (input.kind === "number" && (typeof value !== "number" || !Number.isFinite(value))) ||
        (input.kind === "string" &&
          (typeof value !== "string" ||
            !isWellFormedText(value) ||
            UTF8.encode(value).byteLength > 4_096))
      ) {
        configuration("Fan-out extrema partial does not match its declared type.")
      }
      if (current === null) return value
      const difference = compareFanoutOrderValues(current, value, column)
      return input.mode === "min"
        ? difference <= 0
          ? current
          : value
        : difference >= 0
          ? current
          : value
    },
  })
}

export function minFanout(
  input: BuiltinInput<null | number | string> & { readonly kind: "number" | "string" },
): FanoutReductionResult<null | number | string> {
  return extremaFanout({ ...input, mode: "min" })
}

export function maxFanout(
  input: BuiltinInput<null | number | string> & { readonly kind: "number" | "string" },
): FanoutReductionResult<null | number | string> {
  return extremaFanout({ ...input, mode: "max" })
}
