import {
  type DigestFunction,
  NozzleError,
  type SagaActionReference,
  sagaActionKey,
} from "@nozzle/core"
import {
  assertTrustedExecutionPlan,
  compilePlan,
  type D1BindingValue,
  type MutationPlan,
} from "@nozzle/drizzle"

const RECEIPT_DOMAIN = "nozzle.d1-saga-action-receipt.v1"
const MUTATION_DOMAIN = "nozzle.d1-saga-mutation-set.v1"
const INPUT_DOMAIN = "nozzle.saga-action-input.v1"
const OUTPUT_DOMAIN = "nozzle.saga-action-output.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const MAX_JSON_BYTES = 1024 * 1024
const MAX_MUTATIONS = 64
const MAX_IDENTITY_BYTES = 512
const MAX_SHARD_ID_BYTES = 255
const MAX_SCHEMA_ID_BYTES = 128

export const D1_SAGA_RECEIPT_SCHEMA_STATEMENTS = Object.freeze([
  `CREATE TABLE IF NOT EXISTS "nozzle_saga_action_receipts" (
  "format_version" INTEGER NOT NULL CHECK ("format_version" = 1),
  "idempotency_key" TEXT PRIMARY KEY NOT NULL CHECK (length("idempotency_key") BETWEEN 1 AND 512 AND length(trim("idempotency_key")) > 0),
  "attempt_id" TEXT UNIQUE NOT NULL CHECK (length("attempt_id") BETWEEN 1 AND 512 AND length(trim("attempt_id")) > 0),
  "control_acceptance_checksum" TEXT NOT NULL CHECK (length("control_acceptance_checksum") = 64 AND "control_acceptance_checksum" NOT GLOB '*[^0-9a-f]*'),
  "saga_id" TEXT NOT NULL CHECK (length("saga_id") BETWEEN 1 AND 512 AND length(trim("saga_id")) > 0),
  "operation_id" TEXT NOT NULL CHECK (length("operation_id") BETWEEN 1 AND 512 AND length(trim("operation_id")) > 0),
  "step_id" TEXT NOT NULL CHECK (length("step_id") BETWEEN 1 AND 512 AND length(trim("step_id")) > 0),
  "phase" TEXT NOT NULL CHECK ("phase" IN ('forward', 'compensation')),
  "action_key" TEXT NOT NULL CHECK (length("action_key") BETWEEN 1 AND 512 AND length(trim("action_key")) > 0),
  "shard_id" TEXT NOT NULL CHECK (length("shard_id") BETWEEN 1 AND 255 AND length(trim("shard_id")) > 0),
  "bucket_id" INTEGER NOT NULL CHECK ("bucket_id" BETWEEN 0 AND 4294967295),
  "route_epoch" INTEGER NOT NULL CHECK ("route_epoch" >= 0),
  "schema_id" TEXT NOT NULL CHECK (length("schema_id") BETWEEN 1 AND 128),
  "partition_digest" TEXT NOT NULL CHECK (length("partition_digest") = 64 AND "partition_digest" NOT GLOB '*[^0-9a-f]*'),
  "input_checksum" TEXT NOT NULL CHECK (length("input_checksum") = 64 AND "input_checksum" NOT GLOB '*[^0-9a-f]*'),
  "mutation_checksum" TEXT NOT NULL CHECK (length("mutation_checksum") = 64 AND "mutation_checksum" NOT GLOB '*[^0-9a-f]*'),
  "output_checksum" TEXT NOT NULL CHECK (length("output_checksum") = 64 AND "output_checksum" NOT GLOB '*[^0-9a-f]*'),
  "output_json" TEXT NOT NULL CHECK (length("output_json") BETWEEN 1 AND 1048576 AND json_valid("output_json")),
  "receipt_checksum" TEXT UNIQUE NOT NULL CHECK (length("receipt_checksum") = 64 AND "receipt_checksum" NOT GLOB '*[^0-9a-f]*'),
  "applied_at_ms" INTEGER NOT NULL CHECK ("applied_at_ms" >= 0)
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_saga_receipt_guard"
BEFORE INSERT ON "nozzle_saga_action_receipts"
WHEN NOT EXISTS (
  SELECT 1 FROM "nozzle_bucket_ownership"
  WHERE "bucket_id" = NEW."bucket_id"
    AND "route_epoch" = NEW."route_epoch"
    AND "state" = 'writable'
) OR NOT EXISTS (
  SELECT 1 FROM "nozzle_schema_state"
  WHERE "schema_id" = NEW."schema_id" AND "active" = 1
) OR EXISTS (
  SELECT 1 FROM "nozzle_partition_fences"
  WHERE "hash_version" = 1 AND "partition_digest" = NEW."partition_digest"
)
BEGIN SELECT RAISE(ABORT, 'NOZZLE_SAGA_RECEIPT_ROUTE_REJECTED'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_saga_receipt_update"
BEFORE UPDATE ON "nozzle_saga_action_receipts"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_SAGA_RECEIPT_IMMUTABLE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_saga_receipt_delete"
BEFORE DELETE ON "nozzle_saga_action_receipts"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_SAGA_RECEIPT_PERSISTENT'); END;`,
])

export interface SagaD1PreparedStatement {
  bind(...values: readonly D1BindingValue[]): SagaD1PreparedStatement
  first<T = Record<string, unknown>>(): Promise<T | null>
}

export interface SagaD1Session {
  batch(statements: readonly SagaD1PreparedStatement[]): Promise<readonly unknown[]>
  prepare(sql: string): SagaD1PreparedStatement
}

export interface SagaD1Database {
  withSession(constraint: "first-primary"): SagaD1Session
}

export interface D1SagaTarget {
  readonly bucketId: number
  readonly database: SagaD1Database
  readonly partitionDigest: string
  readonly routeEpoch: number
  readonly schemaId: string
  readonly shardId: string
}

export interface D1SagaAtomicApplyInput {
  readonly action: SagaActionReference
  readonly attemptAcceptanceChecksum: string
  readonly attemptId: string
  readonly idempotencyKey: string
  readonly inputJson: string
  readonly mutations: readonly MutationPlan[]
  readonly operationId: string
  readonly outputJson: string
  readonly phase: "compensation" | "forward"
  readonly sagaId: string
  readonly stepId: string
  readonly target: D1SagaTarget
}

export interface D1SagaExpectedEffect {
  readonly actionKey: string
  readonly attemptAcceptanceChecksum: string
  readonly attemptId: string
  readonly bucketId: number
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly mutationChecksum: string
  readonly operationId: string
  readonly outputChecksum: string
  readonly outputJson: string
  readonly partitionDigest: string
  readonly phase: "compensation" | "forward"
  readonly receiptChecksum: string
  readonly routeEpoch: number
  readonly sagaId: string
  readonly schemaId: string
  readonly shardId: string
  readonly stepId: string
}

export interface D1SagaActionReceipt extends D1SagaExpectedEffect {
  readonly appliedAtMs: number
  readonly formatVersion: 1
}

export type D1SagaAtomicObservation =
  | {
      readonly evidenceJson: string
      readonly kind: "applied"
      readonly receipt: D1SagaActionReceipt
    }
  | { readonly evidenceJson: string; readonly kind: "not_applied" }
  | { readonly errorJson: string; readonly evidenceJson: string; readonly kind: "indeterminate" }

interface ReceiptRow {
  readonly action_key: string
  readonly applied_at_ms: number
  readonly attempt_id: string
  readonly bucket_id: number
  readonly control_acceptance_checksum: string
  readonly format_version: number
  readonly idempotency_key: string
  readonly input_checksum: string
  readonly mutation_checksum: string
  readonly operation_id: string
  readonly output_checksum: string
  readonly output_json: string
  readonly partition_digest: string
  readonly phase: string
  readonly receipt_checksum: string
  readonly route_epoch: number
  readonly saga_id: string
  readonly schema_id: string
  readonly shard_id: string
  readonly step_id: string
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (typeof value !== "object" || value === null) return value
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort())
    output[key] = canonicalValue((value as Record<string, unknown>)[key])
  return output
}

function canonicalJson(value: string, label: string): string {
  if (typeof value !== "string" || value.length === 0) configuration(`${label} must be JSON text.`)
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return configuration(`${label} is not valid JSON.`)
  }
  const json = JSON.stringify(canonicalValue(parsed))
  if (new TextEncoder().encode(json).byteLength > MAX_JSON_BYTES) {
    configuration(`${label} exceeds the one MiB durable limit.`)
  }
  return json
}

function nonEmpty(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0)
    configuration(`${label} must be non-empty.`)
}

function boundedText(value: unknown, label: string, maximumBytes = MAX_IDENTITY_BYTES): string {
  nonEmpty(value, label)
  if (new TextEncoder().encode(value).byteLength > maximumBytes) {
    configuration(`${label} exceeds ${maximumBytes} UTF-8 bytes.`)
  }
  return value
}

function checksum(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !CHECKSUM.test(value))
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
}

function frame(domain: string, values: readonly string[]): Uint8Array {
  const parts = [domain, ...values].map((value) => new TextEncoder().encode(value))
  const output = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0))
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function checkedDigest(
  digest: DigestFunction,
  domain: string,
  values: readonly string[],
): Promise<string> {
  const value = await digest(frame(domain, values))
  checksum(value, "D1 saga checksum")
  return value
}

function planJson(plan: MutationPlan): string {
  return JSON.stringify(canonicalValue(plan))
}

function receiptValues(effect: Omit<D1SagaExpectedEffect, "receiptChecksum">): readonly string[] {
  return [
    effect.idempotencyKey,
    effect.attemptId,
    effect.attemptAcceptanceChecksum,
    effect.sagaId,
    effect.operationId,
    effect.stepId,
    effect.phase,
    effect.actionKey,
    effect.shardId,
    String(effect.bucketId),
    String(effect.routeEpoch),
    effect.schemaId,
    effect.partitionDigest,
    effect.inputChecksum,
    effect.mutationChecksum,
    effect.outputChecksum,
    effect.outputJson,
  ]
}

function validateTarget(target: D1SagaTarget): void {
  if (typeof target !== "object" || target === null || Array.isArray(target))
    configuration("D1 saga target must be a structured object.")
  if (
    !Number.isSafeInteger(target.bucketId) ||
    target.bucketId < 0 ||
    target.bucketId > 0xffff_ffff
  )
    configuration("D1 saga bucket ID is invalid.")
  if (!Number.isSafeInteger(target.routeEpoch) || target.routeEpoch < 0)
    configuration("D1 saga route epoch is invalid.")
  boundedText(target.shardId, "D1 saga shard ID", MAX_SHARD_ID_BYTES)
  boundedText(target.schemaId, "D1 saga schema ID", MAX_SCHEMA_ID_BYTES)
  checksum(target.partitionDigest, "D1 saga partition digest")
  if (typeof target.database?.withSession !== "function")
    configuration("A session-capable D1 database binding is required.")
}

async function expectedEffect(
  input: D1SagaAtomicApplyInput,
  digest: DigestFunction,
): Promise<D1SagaExpectedEffect> {
  if (typeof input !== "object" || input === null || Array.isArray(input))
    configuration("D1 saga action input must be a structured object.")
  if (!Array.isArray(input.mutations)) configuration("D1 saga mutations must be an array.")
  if (input.mutations.length < 1 || input.mutations.length > MAX_MUTATIONS)
    configuration(`A D1 saga action requires between 1 and ${MAX_MUTATIONS} mutations.`)
  validateTarget(input.target)
  boundedText(input.attemptId, "D1 saga attempt ID")
  boundedText(input.idempotencyKey, "D1 saga idempotency key")
  boundedText(input.sagaId, "D1 saga ID")
  boundedText(input.operationId, "D1 saga operation ID")
  boundedText(input.stepId, "D1 saga step ID")
  checksum(input.attemptAcceptanceChecksum, "Control attempt acceptance checksum")
  if (input.phase !== "forward" && input.phase !== "compensation")
    configuration("D1 saga action phase is invalid.")
  const inputJson = canonicalJson(input.inputJson, "D1 saga input")
  const outputJson = canonicalJson(input.outputJson, "D1 saga output")
  const actionKey = sagaActionKey(input.action)
  const planJsons = input.mutations.map((plan) => {
    assertTrustedExecutionPlan(plan)
    if (
      plan.bucketId !== input.target.bucketId ||
      plan.routeEpoch !== input.target.routeEpoch ||
      plan.shardId !== input.target.shardId ||
      plan.schemaId !== input.target.schemaId ||
      plan.partitionDigestHex !== input.target.partitionDigest
    )
      configuration("A D1 saga mutation targets a different sealed shard route.")
    return planJson(plan)
  })
  const inputChecksum = await checkedDigest(digest, INPUT_DOMAIN, [inputJson])
  const mutationChecksum = await checkedDigest(digest, MUTATION_DOMAIN, planJsons)
  const outputChecksum = await checkedDigest(digest, OUTPUT_DOMAIN, [outputJson])
  const effect = Object.freeze({
    actionKey,
    attemptAcceptanceChecksum: input.attemptAcceptanceChecksum,
    attemptId: input.attemptId,
    bucketId: input.target.bucketId,
    idempotencyKey: input.idempotencyKey,
    inputChecksum,
    mutationChecksum,
    operationId: input.operationId,
    outputChecksum,
    outputJson,
    partitionDigest: input.target.partitionDigest,
    phase: input.phase,
    routeEpoch: input.target.routeEpoch,
    sagaId: input.sagaId,
    schemaId: input.target.schemaId,
    shardId: input.target.shardId,
    stepId: input.stepId,
  })
  return Object.freeze({
    ...effect,
    receiptChecksum: await checkedDigest(digest, RECEIPT_DOMAIN, receiptValues(effect)),
  })
}

async function validateExpectedEffect(
  expected: D1SagaExpectedEffect,
  digest: DigestFunction,
): Promise<void> {
  if (typeof expected !== "object" || expected === null || Array.isArray(expected))
    configuration("Expected D1 saga effect must be a structured object.")
  boundedText(expected.actionKey, "D1 saga action key")
  boundedText(expected.attemptId, "D1 saga attempt ID")
  boundedText(expected.idempotencyKey, "D1 saga idempotency key")
  boundedText(expected.sagaId, "D1 saga ID")
  boundedText(expected.operationId, "D1 saga operation ID")
  boundedText(expected.stepId, "D1 saga step ID")
  boundedText(expected.shardId, "D1 saga shard ID", MAX_SHARD_ID_BYTES)
  boundedText(expected.schemaId, "D1 saga schema ID", MAX_SCHEMA_ID_BYTES)
  if (
    !Number.isSafeInteger(expected.bucketId) ||
    expected.bucketId < 0 ||
    expected.bucketId > 0xffff_ffff
  )
    configuration("Expected D1 saga bucket ID is invalid.")
  if (!Number.isSafeInteger(expected.routeEpoch) || expected.routeEpoch < 0)
    configuration("Expected D1 saga route epoch is invalid.")
  if (expected.phase !== "forward" && expected.phase !== "compensation")
    configuration("Expected D1 saga phase is invalid.")
  checksum(expected.attemptAcceptanceChecksum, "Expected Control acceptance checksum")
  checksum(expected.partitionDigest, "Expected D1 saga partition digest")
  checksum(expected.inputChecksum, "Expected D1 saga input checksum")
  checksum(expected.mutationChecksum, "Expected D1 saga mutation checksum")
  checksum(expected.outputChecksum, "Expected D1 saga output checksum")
  checksum(expected.receiptChecksum, "Expected D1 saga receipt checksum")
  const outputJson = canonicalJson(expected.outputJson, "Expected D1 saga output")
  if (outputJson !== expected.outputJson) configuration("Expected D1 saga output is not canonical.")
  const outputChecksum = await checkedDigest(digest, OUTPUT_DOMAIN, [outputJson])
  if (outputChecksum !== expected.outputChecksum)
    configuration("Expected D1 saga output checksum does not match its payload.")
  const receiptChecksum = await checkedDigest(digest, RECEIPT_DOMAIN, receiptValues(expected))
  if (receiptChecksum !== expected.receiptChecksum)
    configuration("Expected D1 saga receipt checksum does not match its claims.")
}

function receiptStatement(
  session: SagaD1Session,
  receipt: D1SagaExpectedEffect,
): SagaD1PreparedStatement {
  return session
    .prepare(
      `INSERT INTO "nozzle_saga_action_receipts" ("format_version", "idempotency_key", "attempt_id", "control_acceptance_checksum", "saga_id", "operation_id", "step_id", "phase", "action_key", "shard_id", "bucket_id", "route_epoch", "schema_id", "partition_digest", "input_checksum", "mutation_checksum", "output_checksum", "output_json", "receipt_checksum", "applied_at_ms") VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, CAST(unixepoch('subsec') * 1000 AS INTEGER))`,
    )
    .bind(
      receipt.idempotencyKey,
      receipt.attemptId,
      receipt.attemptAcceptanceChecksum,
      receipt.sagaId,
      receipt.operationId,
      receipt.stepId,
      receipt.phase,
      receipt.actionKey,
      receipt.shardId,
      receipt.bucketId,
      receipt.routeEpoch,
      receipt.schemaId,
      receipt.partitionDigest,
      receipt.inputChecksum,
      receipt.mutationChecksum,
      receipt.outputChecksum,
      receipt.outputJson,
      receipt.receiptChecksum,
    )
}

function indeterminate(
  reason: "malformed_receipt" | "observation_failed",
): D1SagaAtomicObservation {
  return Object.freeze({
    errorJson: JSON.stringify({ kind: "d1_saga_receipt_indeterminate", reason }),
    evidenceJson: JSON.stringify({ kind: "d1_primary_receipt_observation", reason }),
    kind: "indeterminate",
  })
}

function decodeReceipt(
  row: ReceiptRow,
  expected: D1SagaExpectedEffect,
): D1SagaActionReceipt | undefined {
  if (row.format_version !== 1 || !Number.isSafeInteger(row.applied_at_ms) || row.applied_at_ms < 0)
    return undefined
  const receipt: D1SagaActionReceipt = Object.freeze({
    ...expected,
    appliedAtMs: row.applied_at_ms,
    formatVersion: 1,
  })
  const columns: Readonly<Record<keyof D1SagaExpectedEffect, unknown>> = {
    actionKey: row.action_key,
    attemptAcceptanceChecksum: row.control_acceptance_checksum,
    attemptId: row.attempt_id,
    bucketId: row.bucket_id,
    idempotencyKey: row.idempotency_key,
    inputChecksum: row.input_checksum,
    mutationChecksum: row.mutation_checksum,
    operationId: row.operation_id,
    outputChecksum: row.output_checksum,
    outputJson: row.output_json,
    partitionDigest: row.partition_digest,
    phase: row.phase,
    receiptChecksum: row.receipt_checksum,
    routeEpoch: row.route_epoch,
    sagaId: row.saga_id,
    schemaId: row.schema_id,
    shardId: row.shard_id,
    stepId: row.step_id,
  }
  return (Object.keys(expected) as (keyof D1SagaExpectedEffect)[]).every(
    (key) => columns[key] === expected[key],
  )
    ? receipt
    : undefined
}

export class D1SagaAtomicAdapter {
  readonly #digest: DigestFunction

  constructor(digest: DigestFunction) {
    if (typeof digest !== "function") configuration("A D1 saga digest is required.")
    this.#digest = digest
  }

  async observe(
    target: D1SagaTarget,
    expected: D1SagaExpectedEffect,
  ): Promise<D1SagaAtomicObservation> {
    validateTarget(target)
    await validateExpectedEffect(expected, this.#digest)
    if (
      target.bucketId !== expected.bucketId ||
      target.routeEpoch !== expected.routeEpoch ||
      target.schemaId !== expected.schemaId ||
      target.shardId !== expected.shardId ||
      target.partitionDigest !== expected.partitionDigest
    ) {
      configuration("D1 saga observation target contradicts the sealed physical effect target.")
    }
    try {
      const session = target.database.withSession("first-primary")
      const row = await session
        .prepare(`SELECT * FROM "nozzle_saga_action_receipts" WHERE "idempotency_key" = ?1`)
        .bind(expected.idempotencyKey)
        .first<ReceiptRow>()
      if (row === null)
        return Object.freeze({
          evidenceJson: '{"kind":"d1_primary_receipt_absent"}',
          kind: "not_applied",
        })
      const receipt = decodeReceipt(row, expected)
      if (receipt === undefined) return indeterminate("malformed_receipt")
      return Object.freeze({
        evidenceJson: JSON.stringify({
          kind: "d1_primary_receipt_present",
          receiptChecksum: receipt.receiptChecksum,
        }),
        kind: "applied",
        receipt,
      })
    } catch {
      return indeterminate("observation_failed")
    }
  }

  async apply(input: D1SagaAtomicApplyInput): Promise<D1SagaAtomicObservation> {
    const expected = await expectedEffect(input, this.#digest)
    const session = input.target.database.withSession("first-primary")
    const statements: SagaD1PreparedStatement[] = []
    for (const plan of input.mutations) {
      const compiled = compilePlan(plan)
      statements.push(
        session.prepare(compiled.authorization.sql).bind(...compiled.authorization.params),
      )
      statements.push(session.prepare(compiled.data.sql).bind(...compiled.data.params))
    }
    statements.push(receiptStatement(session, expected))
    try {
      await session.batch(statements)
    } catch {
      // The primary receipt observation below is the only outcome authority.
    }
    return this.observe(input.target, expected)
  }
}

export function sealD1SagaExpectedEffect(
  input: D1SagaAtomicApplyInput,
  digest: DigestFunction,
): Promise<D1SagaExpectedEffect> {
  if (typeof digest !== "function") configuration("A D1 saga digest is required.")
  return expectedEffect(input, digest)
}
