import { bytesToHex, NozzleError } from "@nozzle/core"
import type { D1ResultLike } from "@nozzle/drizzle"

export const ROUTER_PROTOCOL_VERSION = 1 as const
export const MAX_ROUTER_BATCH_STATEMENTS = 49 as const
export const MAX_ROUTER_RESULT_BYTES = 16 * 1024 * 1024
export const MAX_ROUTER_RESULT_ROWS = 10_000

const MAX_D1_VALUE_BYTES = 2 * 1024 * 1024
const MAX_RESULT_COLUMNS = 256

export interface WireBlob {
  readonly hex: string
  readonly type: "blob"
}

export type WirePrimitive = boolean | null | number | string
export type WireD1Value = WireBlob | WirePrimitive

export interface WireD1Result {
  readonly meta: Readonly<Record<string, WirePrimitive>>
  readonly results: readonly Readonly<Record<string, WireD1Value>>[]
  readonly success: true
}

interface ByteBudget {
  used: number
}

function wireError(message: string): never {
  throw new NozzleError("ShardUnavailableError", message)
}

function plainRecord(value: unknown, label: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return wireError(`${label} must be a plain object.`)
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return wireError(`${label} must be a plain object.`)
  }
  if (Object.getOwnPropertySymbols(value).length !== 0) {
    return wireError(`${label} cannot contain symbol properties.`)
  }
  for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
    if (!descriptor.enumerable || !("value" in descriptor)) {
      return wireError(`${label} must contain only enumerable data properties.`)
    }
  }
  return value as Readonly<Record<string, unknown>>
}

function addBytes(budget: ByteBudget, bytes: number): void {
  budget.used += bytes
  if (budget.used > MAX_ROUTER_RESULT_BYTES) {
    throw new NozzleError("CapacityGuardError", "A router result exceeds the wire byte budget.")
  }
}

function encodeString(value: string, budget: ByteBudget): string {
  const bytes = new TextEncoder().encode(value).byteLength
  if (bytes > MAX_D1_VALUE_BYTES) {
    throw new NozzleError("CapacityGuardError", "A D1 string exceeds the router value limit.")
  }
  addBytes(budget, bytes)
  return value
}

function asBytes(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) return value
  if (value instanceof ArrayBuffer) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const byte = value[index]
      if (!Object.hasOwn(value, index) || !Number.isInteger(byte) || byte < 0 || byte > 255) {
        return undefined
      }
    }
    return Uint8Array.from(value as number[])
  }
  return undefined
}

function encodeValue(value: unknown, budget: ByteBudget): WireD1Value {
  if (value === null || typeof value === "boolean") return value
  if (typeof value === "number" && Number.isFinite(value)) return value
  if (typeof value === "string") return encodeString(value, budget)
  const bytes = asBytes(value)
  if (!bytes) return wireError("D1 returned an unsupported router result value.")
  if (bytes.byteLength > MAX_D1_VALUE_BYTES) {
    throw new NozzleError("CapacityGuardError", "A D1 BLOB exceeds the router value limit.")
  }
  addBytes(budget, bytes.byteLength)
  return Object.freeze({ hex: bytesToHex(bytes), type: "blob" })
}

function encodeMetaValue(value: unknown, budget: ByteBudget): WirePrimitive {
  const encoded = encodeValue(value, budget)
  if (typeof encoded === "object" && encoded !== null) {
    return wireError("D1 returned unsupported binary result metadata.")
  }
  return encoded
}

export function encodeWireD1Result(result: D1ResultLike): WireD1Result {
  if (result.success !== true || !Array.isArray(result.results)) {
    return wireError("D1 returned a malformed result envelope.")
  }
  if (result.results.length > MAX_ROUTER_RESULT_ROWS) {
    throw new NozzleError("CapacityGuardError", "A router result exceeds the row limit.")
  }
  const budget: ByteBudget = { used: 0 }
  const metaRecord = plainRecord(result.meta, "D1 result metadata")
  const meta: Record<string, WirePrimitive> = {}
  for (const [key, value] of Object.entries(metaRecord)) {
    encodeString(key, budget)
    meta[key] = encodeMetaValue(value, budget)
  }
  const results = result.results.map((rawRow) => {
    const row = plainRecord(rawRow, "A D1 result row")
    const entries = Object.entries(row)
    if (entries.length > MAX_RESULT_COLUMNS) {
      throw new NozzleError("CapacityGuardError", "A router result row has too many columns.")
    }
    const encoded: Record<string, WireD1Value> = {}
    for (const [key, value] of entries) {
      encodeString(key, budget)
      encoded[key] = encodeValue(value, budget)
    }
    return Object.freeze(encoded)
  })
  return Object.freeze({
    meta: Object.freeze(meta),
    results: Object.freeze(results),
    success: true,
  })
}

function assertExactKeys(
  record: Readonly<Record<string, unknown>>,
  expected: readonly string[],
): void {
  const keys = Object.keys(record)
  if (keys.length !== expected.length || expected.some((key) => !Object.hasOwn(record, key))) {
    wireError("A router wire object has missing or unsupported fields.")
  }
}

function decodeValue(value: unknown, budget: ByteBudget): WirePrimitive | Uint8Array {
  if (value === null || typeof value === "boolean") return value
  if (typeof value === "number" && Number.isFinite(value)) return value
  if (typeof value === "string") return encodeString(value, budget)
  const record = plainRecord(value, "A router wire value")
  assertExactKeys(record, ["hex", "type"])
  if (
    record.type !== "blob" ||
    typeof record.hex !== "string" ||
    record.hex.length > MAX_D1_VALUE_BYTES * 2 ||
    !/^(?:[0-9a-f]{2})*$/u.test(record.hex)
  ) {
    return wireError("A router wire BLOB is malformed or too large.")
  }
  const bytes = new Uint8Array(record.hex.length / 2)
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(record.hex.slice(index * 2, index * 2 + 2), 16)
  }
  addBytes(budget, bytes.byteLength)
  return bytes
}

export function decodeWireD1Result(value: unknown): D1ResultLike {
  const record = plainRecord(value, "The router D1 result")
  assertExactKeys(record, ["meta", "results", "success"])
  if (record.success !== true || !Array.isArray(record.results)) {
    return wireError("The router D1 result envelope is invalid.")
  }
  if (record.results.length > MAX_ROUTER_RESULT_ROWS) {
    throw new NozzleError("CapacityGuardError", "A router result exceeds the row limit.")
  }
  const budget: ByteBudget = { used: 0 }
  const metaRecord = plainRecord(record.meta, "Router result metadata")
  const meta: Record<string, WirePrimitive> = {}
  for (const [key, raw] of Object.entries(metaRecord)) {
    encodeString(key, budget)
    const decoded = decodeValue(raw, budget)
    if (decoded instanceof Uint8Array) {
      return wireError("Router result metadata cannot contain BLOB values.")
    }
    meta[key] = decoded
  }
  const results = record.results.map((rawRow) => {
    const row = plainRecord(rawRow, "A router result row")
    const entries = Object.entries(row)
    if (entries.length > MAX_RESULT_COLUMNS) {
      throw new NozzleError("CapacityGuardError", "A router result row has too many columns.")
    }
    const decoded: Record<string, unknown> = {}
    for (const [key, raw] of entries) {
      encodeString(key, budget)
      decoded[key] = decodeValue(raw, budget)
    }
    return Object.freeze(decoded)
  })
  return Object.freeze({
    meta: Object.freeze(meta),
    results: Object.freeze(results),
    success: true,
  })
}
