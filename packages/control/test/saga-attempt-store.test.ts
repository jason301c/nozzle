import { createHash } from "node:crypto"
import { readFileSync } from "node:fs"
import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type DigestFunction,
  leaseProof,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import {
  createInternalSagaOperationStore,
  type D1OperationStore,
  operationTransitionIdentity,
} from "../src/operation-store.js"
import {
  acceptedSagaAttemptRecord,
  loadSagaAttemptIdentityRow,
  loadSagaAttemptOutcomeRow,
  SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
  SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
} from "../src/saga-attempt-codec.js"
import {
  D1SagaAttemptStore,
  sagaActionInputChecksum,
  sagaObservationIdempotencyKey,
} from "../src/saga-attempt-store.js"
import { D1SagaCoordinatorStore } from "../src/saga-coordinator-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  type SagaEffectContext,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
import { controlSchemaSql, controlSchemaVersionThreeSql } from "../src/schema.js"
import {
  SAGA_ATTEMPT_ROW_SELECT as FROZEN_VERSION_THREE_SAGA_ATTEMPT_ROW_SELECT,
  type SagaAttemptRecord as FrozenVersionThreeSagaAttemptRecord,
  loadSagaAttemptRecordRow as loadFrozenVersionThreeSagaAttemptRow,
} from "./fixtures/saga-attempt-v3-codec.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

const TEST_READER_BARRIER_CHECKSUM = "7".repeat(64)

function activateRowSafeSagaOutcomes(database: DatabaseSync): void {
  database
    .prepare(
      `INSERT INTO "nozzle_saga_outcome_payload_activations"
       ("protocol_version", "reader_barrier_checksum", "activated_at_ms")
       VALUES (1, ?, 1)`,
    )
    .run(TEST_READER_BARRIER_CHECKSUM)
}

async function frozenVersionThreeGet(
  database: DatabaseSync,
  attemptId: string,
): Promise<FrozenVersionThreeSagaAttemptRecord | undefined> {
  const row = database
    .prepare(
      `SELECT ${FROZEN_VERSION_THREE_SAGA_ATTEMPT_ROW_SELECT}
       FROM "nozzle_saga_action_attempts" AS "attempt"
       LEFT JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
       LEFT JOIN "nozzle_saga_action_attempt_outcomes" AS "outcome" USING ("attempt_id")
       WHERE "attempt"."attempt_id" = ?`,
    )
    .get(attemptId)
  if (row === undefined) return undefined
  return loadFrozenVersionThreeSagaAttemptRow(row, digest, attemptId)
}

class StatementAdapter implements ControlStatement {
  readonly #statement: StatementSync
  #values: Record<string, SQLInputValue> = {}

  constructor(statement: StatementSync) {
    this.#statement = statement
    this.#statement.setAllowBareNamedParameters(false)
    this.#statement.setReadBigInts(false)
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#values = {}
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] as ControlBindingValue
      this.#values[`?${index + 1}`] =
        typeof value === "boolean"
          ? value
            ? 1
            : 0
          : value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : value
    }
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: this.#statement.all(this.#values) as T[], success: true }
  }

  async first<T>(): Promise<T | null> {
    return (this.#statement.get(this.#values) as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    const result = this.#statement.run(this.#values)
    return { meta: { changes: Number(result.changes) }, success: true }
  }
}

class DatabaseAdapter implements TransactionalControlDatabase {
  readonly database = new DatabaseSync(":memory:")

  constructor(schemaSql = controlSchemaSql(), activate = schemaSql === controlSchemaSql()) {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(schemaSql)
    if (activate) activateRowSafeSagaOutcomes(this.database)
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    this.database.exec("BEGIN IMMEDIATE;")
    try {
      const results: ControlRunResult[] = []
      for (const statement of statements) results.push(await statement.run())
      this.database.exec("COMMIT;")
      return results
    } catch (error) {
      this.database.exec("ROLLBACK;")
      throw error
    }
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

class FixedStatement implements ControlStatement {
  readonly #allResult: unknown
  readonly #row: unknown
  readonly #runResult: ControlRunResult

  constructor(input: {
    readonly allResult?: unknown
    readonly row?: unknown
    readonly runResult?: ControlRunResult
  }) {
    this.#allResult = input.allResult
    this.#row = input.row ?? null
    this.#runResult = input.runResult ?? { meta: { changes: 0 }, success: true }
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return (this.#allResult ?? { meta: {}, results: [], success: true }) as ControlQueryResult<T>
  }

  async first<T>(): Promise<T | null> {
    return this.#row as T | null
  }

  async run(): Promise<ControlRunResult> {
    return this.#runResult
  }
}

class ThrowStatement implements ControlStatement {
  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    throw new Error("injected batch failure")
  }

  async first<T>(): Promise<T | null> {
    throw new Error("injected batch failure")
  }

  async run(): Promise<ControlRunResult> {
    throw new Error("injected batch failure")
  }
}

class TransformStatement implements ControlStatement {
  readonly #inner: ControlStatement
  readonly #transform: (row: unknown) => unknown

  constructor(inner: ControlStatement, transform: (row: unknown) => unknown) {
    this.#inner = inner
    this.#transform = transform
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#inner.bind(...values)
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    const result = await this.#inner.all<unknown>()
    return { ...result, results: result.results.map(this.#transform) as T[] }
  }

  async first<T>(): Promise<T | null> {
    const row = await this.#inner.first<unknown>()
    return (row === null ? null : this.#transform(row)) as T | null
  }

  async run(): Promise<ControlRunResult> {
    return this.#inner.run()
  }
}

interface AttemptFaults {
  readonly activationRow?: unknown
  readonly attemptRow?: unknown | (() => unknown)
  readonly batchBefore?: () => Promise<void>
  readonly batchCommitThenThrow?: boolean
  readonly batchFailureIndex?: 0 | 1 | 2
  readonly outcomeRow?: unknown | (() => unknown)
  readonly operationProjectionMissing?: boolean
  readonly operationStepChanges?: null | Readonly<Record<string, unknown>>
  readonly runResult?: ControlRunResult
  readonly sagaBindingRow?: unknown
  readonly sagaProjectionMissing?: boolean
  readonly sagaProjectionChanges?: Readonly<Record<string, unknown>>
  readonly schemaVersionRow?: unknown
  readonly payloadQueryResult?: unknown
}

const identityRowKeys = [
  "acceptance_checksum",
  "accepted_at_ms",
  "acquisition_id",
  "action_key",
  "attempt_id",
  "causal_attempt_id",
  "fencing_token",
  "holder_id",
  "idempotency_key",
  "input_checksum",
  "input_json",
  "lease_key",
  "operation_id",
  "operation_step_id",
  "phase",
  "protocol_classified_at_ms",
  "protocol_version",
  "purpose",
  "saga_id",
  "saga_step_id",
] as const

const outcomeRowKeys = [
  "attempt_id",
  "completed_at_ms",
  "error_checksum",
  "error_json",
  "evidence_checksum",
  "evidence_json",
  "outcome_checksum",
  "output_checksum",
  "output_json",
  "state",
] as const

function projectedRow(value: unknown, keys: readonly string[]): unknown {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return value
  const record = value as Record<string, unknown>
  return Object.fromEntries(
    keys.filter((key) => Object.hasOwn(record, key)).map((key) => [key, record[key]]),
  )
}

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #faults: AttemptFaults

  constructor(base: DatabaseAdapter, faults: AttemptFaults) {
    this.#base = base
    this.#faults = faults
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    await this.#faults.batchBefore?.()
    const batch = [...statements]
    if (this.#faults.batchFailureIndex !== undefined) {
      batch[this.#faults.batchFailureIndex] = new ThrowStatement()
    }
    const results = await this.#base.batch(batch)
    if (this.#faults.batchCommitThenThrow) throw new Error("injected lost batch response")
    return results
  }

  prepare(sql: string): ControlStatement {
    if (
      sql.includes('FROM "nozzle_saga_outcome_payload_activations"') &&
      this.#faults.activationRow !== undefined
    ) {
      return new FixedStatement({ row: this.#faults.activationRow })
    }
    if (
      sql.includes('FROM "nozzle_control_schema_versions"') &&
      sql.includes('"schema_version" = 4') &&
      this.#faults.schemaVersionRow !== undefined
    ) {
      return new FixedStatement({ row: this.#faults.schemaVersionRow })
    }
    if (
      sql.includes('FROM "nozzle_saga_action_attempt_outcome_payloads" AS "payload"') &&
      this.#faults.payloadQueryResult !== undefined
    ) {
      return new FixedStatement({ allResult: this.#faults.payloadQueryResult })
    }
    if (
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"') &&
      this.#faults.attemptRow !== undefined
    ) {
      return new FixedStatement({
        row: projectedRow(
          typeof this.#faults.attemptRow === "function"
            ? this.#faults.attemptRow()
            : this.#faults.attemptRow,
          identityRowKeys,
        ),
      })
    }
    if (sql.includes('FROM "nozzle_saga_action_attempt_outcomes" AS "outcome"')) {
      const source =
        this.#faults.outcomeRow === undefined ? this.#faults.attemptRow : this.#faults.outcomeRow
      if (source !== undefined) {
        const value = typeof source === "function" ? source() : source
        const record = value as Record<string, unknown> | null
        return new FixedStatement({
          row:
            record === null || record.state === null || record.state === undefined
              ? null
              : projectedRow(record, outcomeRowKeys),
        })
      }
    }
    if (
      sql.includes('SELECT "operation_id" FROM "nozzle_sagas"') &&
      this.#faults.sagaBindingRow !== undefined
    ) {
      return new FixedStatement({ row: this.#faults.sagaBindingRow })
    }
    if (this.#faults.sagaProjectionMissing && sql.includes('SELECT "saga".*')) {
      return new FixedStatement({ row: null })
    }
    if (
      this.#faults.operationProjectionMissing &&
      sql.includes('FROM "nozzle_operations" WHERE "operation_id"')
    ) {
      return new FixedStatement({ row: null })
    }
    if (
      sql.includes('FROM "nozzle_operation_steps" WHERE "operation_id"') &&
      this.#faults.operationStepChanges !== undefined
    ) {
      const changes = this.#faults.operationStepChanges
      return new TransformStatement(this.#base.prepare(sql), (row) => {
        const record = row as Record<string, unknown>
        if (record.step_id !== "saga:forward:a") return row
        return changes === null
          ? { ...record, step_id: "missing-from-plan" }
          : { ...record, ...changes }
      })
    }
    if (sql.includes('SELECT "saga".*') && this.#faults.sagaProjectionChanges !== undefined) {
      const changes = this.#faults.sagaProjectionChanges
      return new TransformStatement(this.#base.prepare(sql), (row) => ({
        ...(row as Record<string, unknown>),
        ...changes,
      }))
    }
    if (sql.includes('INSERT INTO "nozzle_saga_action_') && this.#faults.runResult !== undefined) {
      return new FixedStatement({ runResult: this.#faults.runResult })
    }
    return this.#base.prepare(sql)
  }
}

class RecordingDatabase implements TransactionalControlDatabase {
  readonly preparedSql: string[] = []
  readonly #base: DatabaseAdapter

  constructor(base: DatabaseAdapter) {
    this.#base = base
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    this.preparedSql.push(sql)
    return this.#base.prepare(sql)
  }
}

function rawIdentity(database: DatabaseSync, attemptId: string): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "attempt".*, "protocol"."protocol_version",
              "protocol"."classified_at_ms" AS "protocol_classified_at_ms"
       FROM "nozzle_saga_action_attempts" AS "attempt"
       LEFT JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
       WHERE "attempt"."attempt_id" = ?`,
    )
    .get(attemptId) as Record<string, unknown>
}

function rawOutcome(
  database: DatabaseSync,
  attemptId: string,
): Record<string, unknown> | undefined {
  return database
    .prepare(`SELECT * FROM "nozzle_saga_action_attempt_outcomes" WHERE "attempt_id" = ?`)
    .get(attemptId) as Record<string, unknown> | undefined
}

function rawPayloads(database: DatabaseSync, attemptId: string): Record<string, unknown>[] {
  return database
    .prepare(
      `SELECT * FROM "nozzle_saga_action_attempt_outcome_payloads"
       WHERE "attempt_id" = ? ORDER BY "payload_kind"`,
    )
    .all(attemptId) as Record<string, unknown>[]
}

function rawAttempt(database: DatabaseSync, attemptId: string): Record<string, unknown> {
  return { ...rawIdentity(database, attemptId), ...rawOutcome(database, attemptId) }
}

async function framedChecksum(domain: string, parts: readonly string[]): Promise<string> {
  const encoded = [domain, ...parts].map((part) => new TextEncoder().encode(part))
  const output = new Uint8Array(encoded.reduce((length, part) => length + 4 + part.byteLength, 0))
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return digest(output)
}

async function acceptanceForRow(row: Record<string, unknown>): Promise<string> {
  const causalAttemptId = row.causal_attempt_id
  return framedChecksum("nozzle.saga-action-acceptance.v2", [
    row.attempt_id as string,
    causalAttemptId === null ? "0" : "1",
    causalAttemptId === null ? "" : (causalAttemptId as string),
    row.saga_id as string,
    row.operation_id as string,
    row.operation_step_id as string,
    row.saga_step_id as string,
    row.phase as string,
    row.purpose as string,
    row.action_key as string,
    row.idempotency_key as string,
    row.input_checksum as string,
    row.input_json as string,
    row.lease_key as string,
    row.holder_id as string,
    row.acquisition_id as string,
    String(row.fencing_token),
  ])
}

async function outcomeForRow(row: Record<string, unknown>): Promise<string> {
  const confirmed = row.state === "confirmed"
  return framedChecksum("nozzle.saga-action-outcome.v1", [
    row.acceptance_checksum as string,
    row.state as string,
    row.evidence_checksum as string,
    row.evidence_json as string,
    (confirmed ? row.output_checksum : row.error_checksum) as string,
    (confirmed ? row.output_json : row.error_json) as string,
  ])
}

describe("D1SagaAttemptStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let operations: D1OperationStore
  let sagaStore: D1SagaStore
  let attempts: D1SagaAttemptStore
  let coordinator: D1SagaCoordinatorStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    operations = createInternalSagaOperationStore(database, digest)
    sagaStore = new D1SagaStore(database, digest)
    attempts = new D1SagaAttemptStore(database, digest)
    coordinator = new D1SagaCoordinatorStore(database, digest)
    return () => database.close()
  })

  it("checksum-locks the complete frozen version-three joined reader codec", () => {
    const source = readFileSync(
      new URL("./fixtures/saga-attempt-v3-codec.ts", import.meta.url),
      "utf8",
    )
    expect(createHash("sha256").update(source).digest("hex")).toBe(
      "f80b94f7443ab634cadc4006039d4dfcdc9de4fc5812bea4dbce88e51521c9ce",
    )
  })

  async function fixture(
    suffix: string,
    actionInputJson = '{"b":2,"a":1}',
    actionEffectProtocol: "opaque" | "saga_receipt" = "saga_receipt",
  ) {
    const operationId = `attempt-operation-${suffix}`
    const sagaId = `attempt-saga-${suffix}`
    const leaseKey = `saga:${sagaId}`
    const actionStepId = sagaActionOperationStepId("a", "forward")
    const compensationStepId = sagaActionOperationStepId("a", "compensation")
    const finalStepId = sagaActionOperationStepId("b", "forward")
    const capabilitySnapshotJson = JSON.stringify({ runtime: "saga-attempt-v1" })
    const operationInputJson = JSON.stringify({ sagaId })
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: `${operationId}:key`,
        inputChecksum: await digest(new TextEncoder().encode(operationInputJson)),
        operationId,
        operationType: "saga",
        steps: [
          SAGA_INIT_OPERATION_STEP_ID,
          SAGA_TERMINATION_OPERATION_STEP_ID,
          actionStepId,
          compensationStepId,
          finalStepId,
        ].map((stepId) => ({
          checkpoint: stepId === finalStepId ? ("irreversible" as const) : ("reversible" as const),
          dependsOn: [],
          effectProtocol:
            stepId === actionStepId
              ? actionEffectProtocol
              : stepId === compensationStepId || stepId === finalStepId
                ? ("saga_receipt" as const)
                : ("opaque" as const),
          idempotencyKey: `${operationId}:${stepId}:key`,
          inputChecksum: `${operationId}:${stepId}:input`,
          leaseKey,
          postconditionChecksum: `${operationId}:${stepId}:postcondition`,
          preconditionChecksum: `${operationId}:${stepId}:precondition`,
          recoveryInstructions: "Recover from the append-only saga action receipt.",
          retryClassification: "reconcile_first" as const,
          stepId,
        })),
      },
      digest,
    )
    await operations.create({
      actorChecksum: "saga-attempt-actor",
      capabilitySnapshotJson,
      environmentId: "production",
      idempotencyScope: `saga-attempt-${suffix}`,
      inputJson: operationInputJson,
      plan,
      requiredShardIds: ["shard-a"],
    })
    const acquired = await leases.acquire({
      acquisitionId: `attempt-acquisition-${suffix}`,
      holderId: `attempt-controller-${suffix}`,
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Saga attempt fixture lease failed.")
    let proof = leaseProof(acquired.record)
    const context = (
      effectId: string,
      stepId: string,
      transitionId: string,
    ): SagaEffectContext => ({ effectId, operationId, proof, stepId, transitionId })
    const beginOperation = async (stepId: string, attemptId: string) => {
      await operations.beginStep({
        actorChecksum: "saga-attempt-actor",
        attemptId,
        idempotencyKey: `${operationId}:${stepId}:key`,
        observedPreconditionChecksum: `${operationId}:${stepId}:precondition`,
        operationId,
        proof,
        stepId,
      })
    }

    const initAttempt = `${sagaId}:init:1`
    await beginOperation(SAGA_INIT_OPERATION_STEP_ID, initAttempt)
    await operations.completeStep({
      actorChecksum: "saga-attempt-actor",
      attemptId: initAttempt,
      observedPostconditionChecksum: `${operationId}:${SAGA_INIT_OPERATION_STEP_ID}:postcondition`,
      operationId,
      proof,
      resultChecksum: `${sagaId}:init:result`,
      stepId: SAGA_INIT_OPERATION_STEP_ID,
    })
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: `attempt-descriptor-${suffix}`,
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 10,
            compensationAction: {
              actionId: "a.compensate",
              artifactChecksum: "cc".repeat(32),
              version: 1,
            },
            compensationObservation: {
              actionId: "a.observe-compensation",
              artifactChecksum: "dd".repeat(32),
              version: 1,
            },
            forwardAction: {
              actionId: "a.forward",
              artifactChecksum: "aa".repeat(32),
              version: 1,
            },
            forwardObservation: {
              actionId: "a.observe-forward",
              artifactChecksum: "bb".repeat(32),
              version: 1,
            },
            inputSchemaChecksum: "11".repeat(32),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "22".repeat(32),
            stepId: "a",
            timeoutMs: 1_000,
          },
          {
            authorizationPolicyChecksum: "ee".repeat(32),
            baseRetryDelayMs: 10,
            compensationAction: null,
            compensationObservation: null,
            forwardAction: {
              actionId: "b.forward",
              artifactChecksum: "ef".repeat(32),
              version: 1,
            },
            forwardObservation: {
              actionId: "b.observe-forward",
              artifactChecksum: "fe".repeat(32),
              version: 1,
            },
            inputSchemaChecksum: "33".repeat(32),
            irreversible: true,
            maxAttempts: 1,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "44".repeat(32),
            stepId: "b",
            timeoutMs: 1_000,
          },
        ],
        version: 1,
      },
      digest,
    )
    let saga = await sagaStore.create({
      deadlineAtMs: 10_000,
      descriptor,
      effect: context(
        `${sagaId}:create`,
        SAGA_INIT_OPERATION_STEP_ID,
        operationTransitionIdentity("succeeded", [
          operationId,
          SAGA_INIT_OPERATION_STEP_ID,
          initAttempt,
        ]),
      ),
      evidenceChecksum: `${sagaId}:create:evidence`,
      idempotencyKey: `${sagaId}:key`,
      inputChecksum: `${sagaId}:input`,
      sagaId,
      serverTimeMs: 1_000,
      stepInputChecksums: {
        a: await sagaActionInputChecksum(actionInputJson, digest),
        b: await sagaActionInputChecksum("{}", digest),
      },
    })

    const actionAttemptId = `${sagaId}:a:forward:1`
    await beginOperation(actionStepId, actionAttemptId)
    saga = (
      await sagaStore.beginAction({
        attemptId: actionAttemptId,
        effect: context(
          `${actionAttemptId}:begin`,
          actionStepId,
          operationTransitionIdentity("accepted", [operationId, actionStepId, actionAttemptId]),
        ),
        evidenceChecksum: `${actionAttemptId}:accepted`,
        idempotencyKey: saga.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga

    const makeUnknown = async () => {
      saga = await coordinator.settleActionFromReceipt({
        actorChecksum: "saga-attempt-actor",
        attemptId: actionAttemptId,
        operationId,
        phase: "forward",
        proof,
        sagaId,
        stepId: "a",
      })
      await leases.release({ proof })
      const reacquired = await leases.acquire({
        acquisitionId: `${sagaId}:observation-acquisition`,
        holderId: `${sagaId}:observation-controller`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!reacquired.acquired) throw new Error("Saga observation lease failed.")
      proof = leaseProof(reacquired.record)
    }

    const recoverAcceptedUnknown = async (acceptanceChecksum: string) => {
      await leases.release({ proof })
      const recovery = await leases.acquire({
        acquisitionId: `${sagaId}:recovery-acquisition`,
        holderId: `${sagaId}:recovery-controller`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!recovery.acquired) throw new Error("Saga recovery lease failed.")
      proof = leaseProof(recovery.record)
      const recoveryId = `${actionAttemptId}:accepted-recovery`
      saga = await coordinator.recoverActionAfterCrash({
        actorChecksum: "saga-attempt-actor",
        attemptId: actionAttemptId,
        operationId,
        phase: "forward",
        proof,
        recoveryId,
        sagaId,
        stepId: "a",
      })
      if (saga.steps.a?.forward.errorChecksum !== acceptanceChecksum) {
        throw new Error("Accepted recovery did not preserve its acceptance evidence.")
      }
      await leases.release({ proof })
      const observer = await leases.acquire({
        acquisitionId: `${sagaId}:recovered-observation-acquisition`,
        holderId: `${sagaId}:recovered-observation-controller`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!observer.acquired) throw new Error("Recovered saga observation lease failed.")
      proof = leaseProof(observer.record)
    }

    return {
      actionAttemptId,
      actionInputJson,
      actionStepId,
      compensationStepId,
      coordinator,
      context,
      makeUnknown,
      operationId,
      proof: () => proof,
      recoverAcceptedUnknown,
      saga: () => saga,
      sagaId,
    }
  }

  async function acceptForwardEffect(run: Awaited<ReturnType<typeof fixture>>) {
    return attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
  }

  it("round-trips maximum logical payloads through three row-safe outcome rows", async () => {
    const run = await fixture("row-safe-maximum")
    await acceptForwardEffect(run)
    const evidenceJson = JSON.stringify("e".repeat(1024 * 1024 - 2))
    const outputJson = JSON.stringify("o".repeat(1024 * 1024 - 2))
    expect(Buffer.byteLength(evidenceJson)).toBe(1024 * 1024)
    expect(Buffer.byteLength(outputJson)).toBe(1024 * 1024)

    const completed = await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson,
      outputJson,
      proof: run.proof(),
      state: "confirmed",
    })

    expect(completed).toMatchObject({ evidenceJson, outputJson, state: "confirmed" })
    expect(rawOutcome(database.database, run.actionAttemptId)).toMatchObject({
      evidence_json: SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
      output_json: SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
    })
    const payloads = rawPayloads(database.database, run.actionAttemptId)
    expect(payloads).toHaveLength(2)
    expect(payloads.map((row) => Buffer.byteLength(row.payload_json as string))).toEqual([
      1024 * 1024,
      1024 * 1024,
    ])
    expect(
      database.database
        .prepare(
          `SELECT
             (SELECT count(*) FROM "nozzle_saga_action_attempt_outcomes"
              WHERE "attempt_id" = ?) AS "headers",
             (SELECT count(*) FROM "nozzle_saga_action_attempt_outcome_payloads"
              WHERE "attempt_id" = ?) AS "payloads"`,
        )
        .get(run.actionAttemptId, run.actionAttemptId),
    ).toEqual({ headers: 1, payloads: 2 })
    expect(() =>
      database.database
        .prepare(
          `UPDATE "nozzle_saga_action_attempt_outcome_payloads" SET "payload_json" = '{}'
           WHERE "attempt_id" = ? AND "payload_kind" = 'evidence'`,
        )
        .run(run.actionAttemptId),
    ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_PAYLOAD_IMMUTABLE")
    expect(() =>
      database.database
        .prepare(
          `DELETE FROM "nozzle_saga_action_attempt_outcome_payloads"
           WHERE "attempt_id" = ? AND "payload_kind" = 'evidence'`,
        )
        .run(run.actionAttemptId),
    ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_PAYLOAD_PERSISTENT")
    expect(() =>
      database.database
        .prepare(
          `INSERT INTO "nozzle_saga_action_attempt_outcome_payloads"
           ("attempt_id", "payload_kind", "payload_checksum", "payload_json")
           VALUES (?, 'error', ?, '{}')`,
        )
        .run(run.actionAttemptId, "0".repeat(64)),
    ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_PAYLOAD_FENCED")
  })

  it.each([
    0, 1, 2,
  ] as const)("rolls back the complete row-safe outcome batch when statement %i fails", async (batchFailureIndex) => {
    const run = await fixture(`row-safe-rollback-${batchFailureIndex}`)
    await acceptForwardEffect(run)
    const faulted = new D1SagaAttemptStore(
      new FaultDatabase(database, { batchFailureIndex }),
      digest,
    )

    await expect(
      faulted.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: '{"source":"rollback"}',
        outputJson: '{"ok":true}',
        proof: run.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/outcome was not committed/u)
    expect(rawOutcome(database.database, run.actionAttemptId)).toBeUndefined()
    expect(rawPayloads(database.database, run.actionAttemptId)).toEqual([])
    await expect(attempts.get(run.actionAttemptId)).resolves.toMatchObject({ state: "accepted" })
  })

  it("recovers exact commit-response loss and exact or contradictory concurrent winners", async () => {
    const lost = await fixture("row-safe-lost-response")
    await acceptForwardEffect(lost)
    const lostStore = new D1SagaAttemptStore(
      new FaultDatabase(database, { batchCommitThenThrow: true }),
      digest,
    )
    await expect(
      lostStore.complete({
        attemptId: lost.actionAttemptId,
        evidenceJson: '{"source":"lost"}',
        outputJson: '{"ok":true}',
        proof: lost.proof(),
        state: "confirmed",
      }),
    ).resolves.toMatchObject({ state: "confirmed" })
    expect(rawPayloads(database.database, lost.actionAttemptId)).toHaveLength(2)

    const exact = await fixture("row-safe-exact-race")
    await acceptForwardEffect(exact)
    const exactInput = {
      attemptId: exact.actionAttemptId,
      evidenceJson: '{"source":"race"}',
      outputJson: '{"ok":true}',
      proof: exact.proof(),
      state: "confirmed" as const,
    }
    const exactRace = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        batchBefore: async () => {
          await attempts.complete(exactInput)
        },
      }),
      digest,
    )
    await expect(exactRace.complete(exactInput)).resolves.toMatchObject({ state: "confirmed" })
    expect(rawPayloads(database.database, exact.actionAttemptId)).toHaveLength(2)

    const contradictory = await fixture("row-safe-contradictory-race")
    await acceptForwardEffect(contradictory)
    const contradictoryRace = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        batchBefore: async () => {
          await attempts.complete({
            attemptId: contradictory.actionAttemptId,
            evidenceJson: '{"winner":true}',
            outputJson: '{"winner":true}',
            proof: contradictory.proof(),
            state: "confirmed",
          })
        },
      }),
      digest,
    )
    await expect(
      contradictoryRace.complete({
        attemptId: contradictory.actionAttemptId,
        evidenceJson: '{"requested":true}',
        outputJson: '{"requested":true}',
        proof: contradictory.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/committed saga outcome contradicts/u)
    await expect(attempts.get(contradictory.actionAttemptId)).resolves.toMatchObject({
      evidenceJson: '{"winner":true}',
      outputJson: '{"winner":true}',
    })
  })

  it("stays backward-readable until an explicit reader-barrier activation", async () => {
    database.close()
    database = new DatabaseAdapter(controlSchemaVersionThreeSql())
    leases = new D1LeaseStore(database)
    operations = createInternalSagaOperationStore(database, digest)
    sagaStore = new D1SagaStore(database, digest)
    attempts = new D1SagaAttemptStore(database, digest)
    coordinator = new D1SagaCoordinatorStore(database, digest)

    const legacy = await fixture("v3-inline-upgrade")
    await acceptForwardEffect(legacy)
    await attempts.complete({
      attemptId: legacy.actionAttemptId,
      evidenceJson: SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
      outputJson: SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
      proof: legacy.proof(),
      state: "confirmed",
    })
    const legacyHeader = rawOutcome(database.database, legacy.actionAttemptId)
    expect(legacyHeader).toMatchObject({
      evidence_json: SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
      output_json: SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
    })

    database.database.exec(controlSchemaSql())
    attempts = new D1SagaAttemptStore(database, digest)
    await expect(attempts.get(legacy.actionAttemptId)).resolves.toMatchObject({
      evidenceJson: SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
      outputJson: SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
      state: "confirmed",
    })
    expect(rawPayloads(database.database, legacy.actionAttemptId)).toEqual([])

    const boundaryEvidenceJson = JSON.stringify("e")
    const boundaryOutputJson = JSON.stringify("o")
    const boundaryInputBytes =
      1024 * 1024 - Buffer.byteLength(boundaryEvidenceJson) - Buffer.byteLength(boundaryOutputJson)
    const boundaryInputJson = JSON.stringify("x".repeat(boundaryInputBytes - 2))
    expect(
      Buffer.byteLength(boundaryInputJson) +
        Buffer.byteLength(boundaryEvidenceJson) +
        Buffer.byteLength(boundaryOutputJson),
    ).toBe(1024 * 1024)
    const boundary = await fixture("v4-inline-compatibility-boundary", boundaryInputJson)
    await acceptForwardEffect(boundary)
    await attempts.complete({
      attemptId: boundary.actionAttemptId,
      evidenceJson: boundaryEvidenceJson,
      outputJson: boundaryOutputJson,
      proof: boundary.proof(),
      state: "confirmed",
    })
    await expect(
      frozenVersionThreeGet(database.database, boundary.actionAttemptId),
    ).resolves.toMatchObject({
      evidenceJson: boundaryEvidenceJson,
      inputJson: boundaryInputJson,
      outputJson: boundaryOutputJson,
      state: "confirmed",
    })
    const boundaryOverflow = await fixture(
      "v4-inline-compatibility-overflow",
      JSON.stringify("x".repeat(boundaryInputBytes - 1)),
    )
    await acceptForwardEffect(boundaryOverflow)
    await expect(
      attempts.complete({
        attemptId: boundaryOverflow.actionAttemptId,
        evidenceJson: boundaryEvidenceJson,
        outputJson: boundaryOutputJson,
        proof: boundaryOverflow.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_INLINE_PAYLOAD_TOO_LARGE")
    expect(rawOutcome(database.database, boundaryOverflow.actionAttemptId)).toBeUndefined()

    const mixed = await fixture("v4-old-worker-inline")
    await acceptForwardEffect(mixed)
    await attempts.complete({
      attemptId: mixed.actionAttemptId,
      evidenceJson: '{"worker":"v3"}',
      outputJson: '{"ok":true}',
      proof: mixed.proof(),
      state: "confirmed",
    })
    expect(rawPayloads(database.database, mixed.actionAttemptId)).toEqual([])
    await expect(
      frozenVersionThreeGet(database.database, mixed.actionAttemptId),
    ).resolves.toMatchObject({
      evidenceJson: '{"worker":"v3"}',
      outputJson: '{"ok":true}',
      state: "confirmed",
    })
    const legacyWriter = new D1SagaAttemptStore(
      new FaultDatabase(database, { schemaVersionRow: null }),
      digest,
    )

    const mixedFailure = await fixture("v4-old-worker-inline-failure")
    await acceptForwardEffect(mixedFailure)
    await legacyWriter.complete({
      attemptId: mixedFailure.actionAttemptId,
      errorJson: '{"code":"failed"}',
      evidenceJson: '{"worker":"v3"}',
      proof: mixedFailure.proof(),
      state: "failed",
    })
    expect(rawPayloads(database.database, mixedFailure.actionAttemptId)).toEqual([])
    await expect(attempts.get(mixedFailure.actionAttemptId)).resolves.toMatchObject({
      errorJson: '{"code":"failed"}',
      evidenceJson: '{"worker":"v3"}',
      state: "failed",
    })

    const oversized = await fixture("v4-old-worker-oversized-inline")
    await acceptForwardEffect(oversized)
    const large = JSON.stringify("x".repeat(600_000))
    await expect(
      legacyWriter.complete({
        attemptId: oversized.actionAttemptId,
        evidenceJson: large,
        outputJson: large,
        proof: oversized.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_INLINE_PAYLOAD_TOO_LARGE")
    expect(rawOutcome(database.database, oversized.actionAttemptId)).toBeUndefined()
    expect(rawPayloads(database.database, oversized.actionAttemptId)).toEqual([])
    const preactivation = await fixture("v4-preactivation-sidecar-batch")
    await acceptForwardEffect(preactivation)
    const falselyActivated = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        activationRow: {
          activated_at_ms: 1,
          protocol_version: 1,
          reader_barrier_checksum: TEST_READER_BARRIER_CHECKSUM,
        },
      }),
      digest,
    )
    await expect(
      falselyActivated.complete({
        attemptId: preactivation.actionAttemptId,
        evidenceJson: '{"worker":"v4"}',
        outputJson: '{"ok":true}',
        proof: preactivation.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/outcome was not committed/u)
    expect(rawOutcome(database.database, preactivation.actionAttemptId)).toBeUndefined()
    expect(rawPayloads(database.database, preactivation.actionAttemptId)).toEqual([])
    activateRowSafeSagaOutcomes(database.database)
    const activated = await fixture("v4-reader-barrier-activated")
    await acceptForwardEffect(activated)
    await attempts.complete({
      attemptId: activated.actionAttemptId,
      evidenceJson: '{"worker":"v4"}',
      outputJson: '{"ok":true}',
      proof: activated.proof(),
      state: "confirmed",
    })
    expect(rawPayloads(database.database, activated.actionAttemptId)).toHaveLength(2)
    await expect(
      frozenVersionThreeGet(database.database, activated.actionAttemptId),
    ).rejects.toThrow(/outcome checksums do not match/u)
  })

  it("never selects attempt input and outcome payload JSON in one row", async () => {
    const run = await fixture("row-safe-query-shape")
    await acceptForwardEffect(run)
    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"source":"shape"}',
      outputJson: '{"ok":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    const recording = new RecordingDatabase(database)
    await new D1SagaAttemptStore(recording, digest).get(run.actionAttemptId)
    const identityQuery = recording.preparedSql.find((sql) =>
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"'),
    ) as string
    const outcomeQuery = recording.preparedSql.find((sql) =>
      sql.includes('FROM "nozzle_saga_action_attempt_outcomes" AS "outcome"'),
    ) as string
    const payloadQuery = recording.preparedSql.find((sql) =>
      sql.includes('FROM "nozzle_saga_action_attempt_outcome_payloads" AS "payload"'),
    ) as string
    expect(identityQuery).toContain('"attempt"."input_json"')
    expect(identityQuery).not.toContain("outcome_payload")
    expect(identityQuery).not.toContain('"outcome".')
    expect(outcomeQuery).not.toContain('"attempt".')
    expect(outcomeQuery).not.toContain("input_json")
    expect(payloadQuery).not.toContain('"attempt".')
    expect(payloadQuery).not.toContain("input_json")
  })

  it("captures schema, activation, and payload-query metadata before trusting them", async () => {
    const run = await fixture("row-safe-query-metadata")
    await acceptForwardEffect(run)
    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"source":"metadata"}',
      outputJson: '{"ok":true}',
      proof: run.proof(),
      state: "confirmed",
    })

    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { schemaVersionRow: new Proxy({ schema_version: 4 }, {}) }),
        digest,
      ).get(run.actionAttemptId),
    ).rejects.toThrow(/could not be captured safely/u)
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { schemaVersionRow: { extra: true, schema_version: 4 } }),
        digest,
      ).get(run.actionAttemptId),
    ).rejects.toThrow(/publication is malformed/u)
    const activation = {
      activated_at_ms: 1,
      protocol_version: 1,
      reader_barrier_checksum: TEST_READER_BARRIER_CHECKSUM,
    }
    for (const candidate of [
      new Proxy(activation, {}),
      { ...activation, extra: true },
      { ...activation, protocol_version: 2 },
      { ...activation, reader_barrier_checksum: "invalid" },
      { ...activation, activated_at_ms: -1 },
    ]) {
      await expect(
        new D1SagaAttemptStore(
          new FaultDatabase(database, { activationRow: candidate }),
          digest,
        ).get(run.actionAttemptId),
      ).rejects.toThrow(/(?:captured safely|payload activation is malformed)/u)
    }
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, {
          payloadQueryResult: { meta: {}, results: {}, success: true },
        }),
        digest,
      ).get(run.actionAttemptId),
    ).rejects.toThrow(/query metadata is malformed/u)
  })

  it("persists canonical accepted input and a confirmed append-only outcome", async () => {
    const run = await fixture("confirmed")
    const accepted = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    expect(accepted).toMatchObject({
      actionKey: `a.forward@1:${"aa".repeat(32)}`,
      causalAttemptId: null,
      inputJson: '{"a":1,"b":2}',
      operationId: run.operationId,
      operationStepId: run.actionStepId,
      protocolVersion: 2,
      state: "accepted",
    })
    expect(await attempts.get(run.actionAttemptId)).toEqual(accepted)
    await expect(
      attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      }),
    ).resolves.toEqual(accepted)

    const input = {
      attemptId: run.actionAttemptId,
      evidenceJson: '{"z":2,"a":1}',
      outputJson: '{"result":true}',
      proof: run.proof(),
      state: "confirmed" as const,
    }
    const completed = await attempts.complete(input)
    expect(completed).toMatchObject({
      evidenceJson: '{"a":1,"z":2}',
      outputJson: '{"result":true}',
      state: "confirmed",
    })
    await expect(attempts.complete(input)).resolves.toEqual(completed)
    await expect(attempts.complete({ ...input, outputJson: '{"result":false}' })).rejects.toThrow(
      /contradicts durable evidence/u,
    )
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: run.proof(),
        requireState: "accepted",
      }),
    ).rejects.toThrow(/requires an accepted effect receipt/u)
    expect(() =>
      database.database
        .prepare(`UPDATE "nozzle_saga_action_attempts" SET "input_checksum" = 'rewritten'`)
        .run(),
    ).toThrow(/SAGA_ATTEMPT_IMMUTABLE/u)
    expect(() =>
      database.database.prepare(`DELETE FROM "nozzle_saga_action_attempts"`).run(),
    ).toThrow(/SAGA_ATTEMPT_PERSISTENT/u)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: { ...run.proof(), acquisitionId: "different-acquisition" },
        requireState: "terminal",
      }),
    ).rejects.toThrow(/receipt lease or a strictly newer fence/u)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).resolves.toEqual(completed)
    database.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_protocols"
         SET "protocol_version" = 1 WHERE "attempt_id" = ?`,
      )
      .run(run.actionAttemptId)
    const legacyForward = await attempts.get(run.actionAttemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).resolves.toEqual(legacyForward)
    const receiptProof = run.proof()
    await leases.release({ proof: receiptProof })
    const acquired = await leases.acquire({
      acquisitionId: `${run.sagaId}:terminal-recovery-acquisition`,
      holderId: `${run.sagaId}:terminal-recovery-controller`,
      leaseKey: receiptProof.leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Terminal receipt recovery lease failed.")
    const recoveryProof = leaseProof(acquired.record)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: recoveryProof,
        requireState: "terminal",
      }),
    ).resolves.toEqual(legacyForward)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: { ...recoveryProof, leaseKey: "different-lease" },
        requireState: "terminal",
      }),
    ).rejects.toThrow(/receipt lease or a strictly newer fence/u)
  })

  it("records every effect failure classification and rejects indeterminate effects", async () => {
    for (const state of ["failed", "not_applied", "unknown"] as const) {
      const run = await fixture(`effect-${state}`)
      await attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
      await expect(
        attempts.complete({
          attemptId: run.actionAttemptId,
          errorJson: JSON.stringify({ state }),
          evidenceJson: JSON.stringify({ source: "handler" }),
          proof: run.proof(),
          state,
        }),
      ).resolves.toMatchObject({ state })
    }

    const incompatible = await fixture("effect-indeterminate")
    await attempts.accept({
      attemptId: incompatible.actionAttemptId,
      inputJson: incompatible.actionInputJson,
      phase: "forward",
      proof: incompatible.proof(),
      purpose: "effect",
      sagaId: incompatible.sagaId,
      sagaStepId: "a",
    })
    await expect(
      attempts.complete({
        attemptId: incompatible.actionAttemptId,
        errorJson: "{}",
        evidenceJson: "{}",
        proof: incompatible.proof(),
        state: "indeterminate",
      }),
    ).rejects.toThrow(/incompatible/u)
  })

  it("binds observation attempts to the registered oracle under a newer fence", async () => {
    const run = await fixture("observation")
    const effect = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const unknown = await attempts.complete({
      attemptId: run.actionAttemptId,
      errorJson: '{"dispatch":"unknown"}',
      evidenceJson: '{"transport":"lost"}',
      proof: run.proof(),
      state: "unknown",
    })
    if (unknown.state === "accepted") throw new Error("Expected terminal unknown receipt.")
    await run.makeUnknown()

    const observationAttemptId = `${run.sagaId}:a:observe:1`
    const observation = await attempts.accept({
      attemptId: observationAttemptId,
      inputJson: JSON.stringify({ originalAttemptId: effect.attemptId }),
      phase: "forward",
      proof: run.proof(),
      purpose: "observation",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    expect(observation).toMatchObject({
      actionKey: `a.observe-forward@1:${"bb".repeat(32)}`,
      causalAttemptId: effect.attemptId,
      idempotencyKey: sagaObservationIdempotencyKey(effect.idempotencyKey),
      purpose: "observation",
      state: "accepted",
    })
    await expect(
      attempts.complete({
        attemptId: observationAttemptId,
        errorJson: '{"reason":"ambiguous"}',
        evidenceJson: '{"lookup":"complete"}',
        proof: run.proof(),
        state: "indeterminate",
      }),
    ).resolves.toMatchObject({ state: "indeterminate" })

    const wrong = await fixture("observation-unknown")
    const wrongEffect = await attempts.accept({
      attemptId: wrong.actionAttemptId,
      inputJson: wrong.actionInputJson,
      phase: "forward",
      proof: wrong.proof(),
      purpose: "effect",
      sagaId: wrong.sagaId,
      sagaStepId: "a",
    })
    const wrongUnknown = await attempts.complete({
      attemptId: wrong.actionAttemptId,
      errorJson: "{}",
      evidenceJson: "{}",
      proof: wrong.proof(),
      state: "unknown",
    })
    if (wrongUnknown.state === "accepted") throw new Error("Expected terminal unknown receipt.")
    await wrong.makeUnknown()
    const wrongObservationId = `${wrong.sagaId}:a:observe:1`
    await attempts.accept({
      attemptId: wrongObservationId,
      inputJson: JSON.stringify({ originalAttemptId: wrongEffect.attemptId }),
      phase: "forward",
      proof: wrong.proof(),
      purpose: "observation",
      sagaId: wrong.sagaId,
      sagaStepId: "a",
    })
    for (const state of ["failed", "unknown"] as const) {
      await expect(
        attempts.complete({
          attemptId: wrongObservationId,
          errorJson: "{}",
          evidenceJson: "{}",
          proof: wrong.proof(),
          state,
        }),
      ).rejects.toThrow(/incompatible/u)
    }
  })

  it("rejects observation dispatch when the operation ledger disagrees about its cause", async () => {
    const run = await fixture("observation-operation-binding")
    await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const unknown = await attempts.complete({
      attemptId: run.actionAttemptId,
      errorJson: '{"dispatch":"unknown"}',
      evidenceJson: '{"transport":"lost"}',
      proof: run.proof(),
      state: "unknown",
    })
    if (unknown.state === "accepted") throw new Error("Expected terminal unknown receipt.")
    await run.makeUnknown()

    const operationRow = database.database
      .prepare(
        `SELECT * FROM "nozzle_operation_steps"
         WHERE "operation_id" = ? AND "step_id" = ?`,
      )
      .get(run.operationId, run.actionStepId) as Record<string, unknown>
    const record = JSON.parse(operationRow.record_json as string) as Record<string, unknown>
    const input = {
      attemptId: `${run.sagaId}:a:observe:operation-binding`,
      inputJson: JSON.stringify({ originalAttemptId: run.actionAttemptId }),
      phase: "forward" as const,
      proof: run.proof(),
      purpose: "observation" as const,
      sagaId: run.sagaId,
      sagaStepId: "a",
    }
    for (const operationStepChanges of [
      null,
      { state: "running" },
      { record_json: null },
      { fencing_token: null },
    ]) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { operationStepChanges }),
        digest,
      )
      await expect(faulted.accept(input)).rejects.toThrow(/persisted operation step/iu)
    }
    for (const operationStepChanges of [
      { record_json: JSON.stringify({ ...record, lastAttemptId: "other-attempt" }) },
      { record_json: JSON.stringify({ ...record, errorChecksum: "other-error" }) },
      {
        fencing_token: (operationRow.fencing_token as number) + 1,
        record_json: JSON.stringify({
          ...record,
          fencingToken: (operationRow.fencing_token as number) + 1,
        }),
      },
    ]) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { operationStepChanges }),
        digest,
      )
      await expect(faulted.accept(input)).rejects.toThrow(/operation ledger|ledgers disagree/u)
    }

    const causeRow = rawAttempt(database.database, run.actionAttemptId)
    const corruptedCause: Record<string, unknown> = {
      ...causeRow,
      operation_id: "another-operation",
    }
    corruptedCause.acceptance_checksum = await acceptanceForRow(corruptedCause)
    const causePayloads = rawPayloads(database.database, run.actionAttemptId)
    corruptedCause.outcome_checksum = await outcomeForRow({
      ...corruptedCause,
      evidence_json: causePayloads.find((row) => row.payload_kind === "evidence")?.payload_json,
      error_json: causePayloads.find((row) => row.payload_kind === "error")?.payload_json,
    })
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: corruptedCause }),
        digest,
      ).accept(input),
    ).rejects.toThrow(/checksum-verified causal receipt/u)

    const acceptedCause = {
      ...causeRow,
      completed_at_ms: null,
      error_checksum: null,
      error_json: null,
      evidence_checksum: null,
      evidence_json: null,
      outcome_checksum: null,
      state: null,
    }
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: acceptedCause }),
        digest,
      ).accept(input),
    ).rejects.toThrow(/ledgers disagree/u)
  })

  it("binds an accepted-without-outcome recovery to the same receipt evidence in both ledgers", async () => {
    const run = await fixture("observation-accepted-recovery")
    const effect = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    await run.recoverAcceptedUnknown(effect.acceptanceChecksum)

    await expect(
      attempts.accept({
        attemptId: `${run.actionAttemptId}:observation`,
        inputJson: JSON.stringify({ originalAttemptId: run.actionAttemptId }),
        phase: "forward",
        proof: run.proof(),
        purpose: "observation",
        sagaId: run.sagaId,
        sagaStepId: "a",
      }),
    ).resolves.toMatchObject({
      causalAttemptId: run.actionAttemptId,
      purpose: "observation",
      state: "accepted",
    })
  })

  it("revalidates legacy accepted observations before allowing a terminal outcome", async () => {
    const unknownRun = async (suffix: string) => {
      const run = await fixture(suffix)
      await attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
      const unknown = await attempts.complete({
        attemptId: run.actionAttemptId,
        errorJson: '{"dispatch":"unknown"}',
        evidenceJson: '{"transport":"lost"}',
        proof: run.proof(),
        state: "unknown",
      })
      if (unknown.state !== "unknown") throw new Error("Expected an unknown effect receipt.")
      await run.makeUnknown()
      return run
    }
    const acceptObservation = async (run: Awaited<ReturnType<typeof unknownRun>>, suffix: string) =>
      attempts.accept({
        attemptId: `${run.actionAttemptId}:observation:${suffix}`,
        inputJson: JSON.stringify({ originalAttemptId: run.actionAttemptId }),
        phase: "forward",
        proof: run.proof(),
        purpose: "observation",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
    const markLegacy = (attemptId: string) => {
      database.database
        .prepare(
          `UPDATE "nozzle_saga_action_attempt_protocols"
           SET "protocol_version" = 1 WHERE "attempt_id" = ?`,
        )
        .run(attemptId)
    }
    database.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')

    const valid = await unknownRun("legacy-observation-valid")
    const validObservation = await acceptObservation(valid, "valid")
    markLegacy(validObservation.attemptId)
    await expect(
      attempts.complete({
        attemptId: validObservation.attemptId,
        evidenceJson: '{"lookup":"complete"}',
        outputJson: '{"applied":true}',
        proof: valid.proof(),
        state: "confirmed",
      }),
    ).resolves.toMatchObject({ protocolVersion: 1, state: "confirmed" })

    const contradictory = await acceptObservation(valid, "contradictory")
    markLegacy(contradictory.attemptId)
    database.database.exec('DROP TRIGGER "nozzle_control_saga_attempt_update";')
    const contradictoryRow: Record<string, unknown> = {
      ...rawAttempt(database.database, contradictory.attemptId),
      action_key: "different-observation-action",
    }
    const contradictoryAcceptance = await acceptanceForRow(contradictoryRow)
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempts"
         SET "action_key" = ?, "acceptance_checksum" = ? WHERE "attempt_id" = ?`,
      )
      .run("different-observation-action", contradictoryAcceptance, contradictory.attemptId)
    await expect(
      attempts.complete({
        attemptId: contradictory.attemptId,
        evidenceJson: '{"lookup":"contradictory"}',
        outputJson: '{"applied":true}',
        proof: valid.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/protocol-one saga receipt/u)

    const stale = await unknownRun("legacy-observation-stale")
    const winner = await acceptObservation(stale, "winner")
    const staleObservation = await acceptObservation(stale, "stale")
    markLegacy(staleObservation.attemptId)
    await attempts.complete({
      attemptId: winner.attemptId,
      evidenceJson: '{"lookup":"complete"}',
      outputJson: '{"applied":true}',
      proof: stale.proof(),
      state: "confirmed",
    })
    await stale.coordinator.settleObservationFromReceipt({
      actorChecksum: "saga-attempt-actor",
      attemptId: winner.attemptId,
      operationId: stale.operationId,
      phase: "forward",
      proof: stale.proof(),
      sagaId: stale.sagaId,
      stepId: "a",
    })
    await expect(
      attempts.complete({
        attemptId: staleObservation.attemptId,
        evidenceJson: '{"lookup":"late"}',
        outputJson: '{"applied":true}',
        proof: stale.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/not eligible/u)
  })

  it("revalidates terminal protocol-one observations before first settlement", async () => {
    const unknownRun = async (suffix: string) => {
      const run = await fixture(suffix)
      await attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
      const unknown = await attempts.complete({
        attemptId: run.actionAttemptId,
        errorJson: '{"dispatch":"unknown"}',
        evidenceJson: '{"transport":"lost"}',
        proof: run.proof(),
        state: "unknown",
      })
      if (unknown.state !== "unknown") throw new Error("Expected an unknown effect receipt.")
      await run.makeUnknown()
      return run
    }
    const terminalObservation = async (
      run: Awaited<ReturnType<typeof unknownRun>>,
      suffix: string,
    ) => {
      const attemptId = `${run.actionAttemptId}:observation:${suffix}`
      await attempts.accept({
        attemptId,
        inputJson: JSON.stringify({ originalAttemptId: run.actionAttemptId }),
        phase: "forward",
        proof: run.proof(),
        purpose: "observation",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
      return attempts.complete({
        attemptId,
        evidenceJson: '{"lookup":"complete"}',
        outputJson: '{"applied":true}',
        proof: run.proof(),
        state: "confirmed",
      })
    }
    const markLegacy = (attemptId: string) => {
      database.database
        .prepare(
          `UPDATE "nozzle_saga_action_attempt_protocols"
           SET "protocol_version" = 1 WHERE "attempt_id" = ?`,
        )
        .run(attemptId)
    }
    database.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')

    const valid = await unknownRun("terminal-legacy-observation-valid")
    const validReceipt = await terminalObservation(valid, "valid")
    markLegacy(validReceipt.attemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: validReceipt.attemptId,
        proof: valid.proof(),
        requireState: "terminal",
      }),
    ).resolves.toMatchObject({ protocolVersion: 1, state: "confirmed" })
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: validReceipt.attemptId,
        proof: { ...valid.proof(), fencingToken: validReceipt.fencingToken - 1 },
        requireState: "terminal",
      }),
    ).rejects.toThrow(/receipt lease or a strictly newer fence/u)

    const weak = await unknownRun("terminal-legacy-observation-weak")
    const weakReceipt = await terminalObservation(weak, "weak")
    markLegacy(weakReceipt.attemptId)
    database.database.exec('DROP TRIGGER "nozzle_control_saga_attempt_update";')
    database.database.exec('DROP TRIGGER "nozzle_control_saga_outcome_update";')
    const weakRow: Record<string, unknown> = {
      ...rawAttempt(database.database, weakReceipt.attemptId),
      action_key: "different-observation-action",
    }
    weakRow.acceptance_checksum = await acceptanceForRow(weakRow)
    const weakPayloads = rawPayloads(database.database, weakReceipt.attemptId)
    weakRow.outcome_checksum = await outcomeForRow({
      ...weakRow,
      evidence_json: weakPayloads.find((row) => row.payload_kind === "evidence")?.payload_json,
      output_json: weakPayloads.find((row) => row.payload_kind === "output")?.payload_json,
    })
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempts"
         SET "action_key" = ?, "acceptance_checksum" = ? WHERE "attempt_id" = ?`,
      )
      .run(
        weakRow.action_key as string,
        weakRow.acceptance_checksum as string,
        weakReceipt.attemptId,
      )
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_outcomes"
         SET "outcome_checksum" = ? WHERE "attempt_id" = ?`,
      )
      .run(weakRow.outcome_checksum as string, weakReceipt.attemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: weakReceipt.attemptId,
        proof: weak.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/protocol-one saga receipt/u)

    const stale = await unknownRun("terminal-legacy-observation-stale")
    const winner = await terminalObservation(stale, "winner")
    const staleReceipt = await terminalObservation(stale, "stale")
    markLegacy(staleReceipt.attemptId)
    await stale.coordinator.settleObservationFromReceipt({
      actorChecksum: "saga-attempt-actor",
      attemptId: winner.attemptId,
      operationId: stale.operationId,
      phase: "forward",
      proof: stale.proof(),
      sagaId: stale.sagaId,
      stepId: "a",
    })
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: staleReceipt.attemptId,
        proof: stale.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/not eligible/u)

    const invalid = await unknownRun("terminal-legacy-observation-invalid")
    const invalidAttemptId = `${invalid.actionAttemptId}:observation:invalid`
    await attempts.accept({
      attemptId: invalidAttemptId,
      inputJson: JSON.stringify({ originalAttemptId: invalid.actionAttemptId }),
      phase: "forward",
      proof: invalid.proof(),
      purpose: "observation",
      sagaId: invalid.sagaId,
      sagaStepId: "a",
    })
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: invalidAttemptId,
        proof: invalid.proof(),
        requireState: "accepted",
      }),
    ).rejects.toThrow(/requires an accepted effect receipt/u)
    markLegacy(invalidAttemptId)
    database.database.exec('DROP TRIGGER "nozzle_control_saga_outcome_insert_v2";')
    const invalidRow = rawAttempt(database.database, invalidAttemptId)
    const evidenceJson = "{}"
    const errorJson = '{"legacy":"failed"}'
    const evidenceChecksum = await framedChecksum("nozzle.saga-action-evidence.v1", [evidenceJson])
    const errorChecksum = await framedChecksum("nozzle.saga-action-error.v1", [errorJson])
    const outcomeChecksum = await framedChecksum("nozzle.saga-action-outcome.v1", [
      invalidRow.acceptance_checksum as string,
      "failed",
      evidenceChecksum,
      evidenceJson,
      errorChecksum,
      errorJson,
    ])
    database.database
      .prepare(
        `INSERT INTO "nozzle_saga_action_attempt_outcomes"
         ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
          "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
         VALUES (?, 'failed', ?, ?, NULL, NULL, ?, ?, ?, ?)`,
      )
      .run(
        invalidAttemptId,
        evidenceChecksum,
        evidenceJson,
        errorChecksum,
        errorJson,
        outcomeChecksum,
        invalidRow.accepted_at_ms as number,
      )
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: invalidAttemptId,
        proof: invalid.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/incompatible outcome/u)
  })

  it("binds compensation acceptance to the exact confirmed forward attempt", async () => {
    const run = await fixture("compensation-cause")
    const forward = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const forwardOutcome = await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"dispatch":"confirmed"}',
      outputJson: '{"created":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    if (forwardOutcome.state !== "confirmed") throw new Error("Expected confirmed forward.")
    let saga = await run.coordinator.settleActionFromReceipt({
      actorChecksum: "saga-attempt-actor",
      attemptId: run.actionAttemptId,
      operationId: run.operationId,
      phase: "forward",
      proof: run.proof(),
      sagaId: run.sagaId,
      stepId: "a",
    })

    const terminationAttemptId = `${run.sagaId}:termination:1`
    await operations.beginStep({
      actorChecksum: "saga-attempt-actor",
      attemptId: terminationAttemptId,
      idempotencyKey: `${run.operationId}:${SAGA_TERMINATION_OPERATION_STEP_ID}:key`,
      observedPreconditionChecksum: `${run.operationId}:${SAGA_TERMINATION_OPERATION_STEP_ID}:precondition`,
      operationId: run.operationId,
      proof: run.proof(),
      stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
    })
    await operations.completeStep({
      actorChecksum: "saga-attempt-actor",
      attemptId: terminationAttemptId,
      observedPostconditionChecksum: `${run.operationId}:${SAGA_TERMINATION_OPERATION_STEP_ID}:postcondition`,
      operationId: run.operationId,
      proof: run.proof(),
      resultChecksum: `${terminationAttemptId}:result`,
      stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
    })
    saga = await sagaStore.requestTermination({
      cause: "cancellation",
      effect: run.context(
        `${terminationAttemptId}:effect`,
        SAGA_TERMINATION_OPERATION_STEP_ID,
        operationTransitionIdentity("succeeded", [
          run.operationId,
          SAGA_TERMINATION_OPERATION_STEP_ID,
          terminationAttemptId,
        ]),
      ),
      evidenceChecksum: `${terminationAttemptId}:result`,
      sagaId: run.sagaId,
      serverTimeMs: 1_003,
    })

    const compensationAttemptId = `${run.sagaId}:a:compensation:1`
    await operations.beginStep({
      actorChecksum: "saga-attempt-actor",
      attemptId: compensationAttemptId,
      idempotencyKey: `${run.operationId}:${run.compensationStepId}:key`,
      observedPreconditionChecksum: `${run.operationId}:${run.compensationStepId}:precondition`,
      operationId: run.operationId,
      proof: run.proof(),
      stepId: run.compensationStepId,
    })
    saga = (
      await sagaStore.beginAction({
        attemptId: compensationAttemptId,
        effect: run.context(
          `${compensationAttemptId}:begin`,
          run.compensationStepId,
          operationTransitionIdentity("accepted", [
            run.operationId,
            run.compensationStepId,
            compensationAttemptId,
          ]),
        ),
        evidenceChecksum: `${compensationAttemptId}:accepted`,
        idempotencyKey: saga.steps.a?.compensation.idempotencyKey as string,
        phase: "compensation",
        sagaId: run.sagaId,
        serverTimeMs: 1_004,
        stepId: "a",
      })
    ).saga
    const compensationInput = {
      attemptId: compensationAttemptId,
      inputJson: JSON.stringify({
        causalAttemptId: forward.attemptId,
        forwardResultChecksum: forwardOutcome.outputChecksum,
      }),
      phase: "compensation",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    } as const
    await expect(
      new D1SagaAttemptStore(new FaultDatabase(database, { attemptRow: null }), digest).accept(
        compensationInput,
      ),
    ).rejects.toThrow(/exact confirmed forward receipt/u)
    const compensation = await attempts.accept(compensationInput)
    expect(compensation).toMatchObject({
      causalAttemptId: forward.attemptId,
      phase: "compensation",
      state: "accepted",
    })
    const terminalCompensation = await attempts.complete({
      attemptId: compensationAttemptId,
      evidenceJson: '{"dispatch":"confirmed"}',
      outputJson: '{"compensated":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    database.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_protocols"
         SET "protocol_version" = 1 WHERE "attempt_id" = ?`,
      )
      .run(compensationAttemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: compensationAttemptId,
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).resolves.toMatchObject({ protocolVersion: 1, state: terminalCompensation.state })

    database.database.exec('DROP TRIGGER "nozzle_control_saga_outcome_update";')
    const changedOutputJson = '{"created":false}'
    const changedOutputChecksum = await framedChecksum("nozzle.saga-action-output.v1", [
      changedOutputJson,
    ])
    const forwardPayloads = rawPayloads(database.database, forward.attemptId)
    const changedForwardRow: Record<string, unknown> = {
      ...rawAttempt(database.database, forward.attemptId),
      output_checksum: changedOutputChecksum,
    }
    changedForwardRow.outcome_checksum = await outcomeForRow({
      ...changedForwardRow,
      evidence_json: forwardPayloads.find((row) => row.payload_kind === "evidence")?.payload_json,
      output_json: changedOutputJson,
    })
    database.database.exec('DROP TRIGGER "nozzle_control_saga_outcome_payload_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_outcome_payloads"
         SET "payload_checksum" = ?, "payload_json" = ?
         WHERE "attempt_id" = ? AND "payload_kind" = 'output'`,
      )
      .run(changedOutputChecksum, changedOutputJson, forward.attemptId)
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_outcomes"
         SET "output_checksum" = ?, "outcome_checksum" = ?
         WHERE "attempt_id" = ?`,
      )
      .run(changedOutputChecksum, changedForwardRow.outcome_checksum as string, forward.attemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: compensationAttemptId,
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/exact confirmed forward receipt/u)
  })

  it("validates accepted effects for crash recovery separately from terminal settlement", async () => {
    const run = await fixture("settlement-terminal")
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: "missing-attempt",
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/never accepted/u)
    await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: run.proof(),
        requireState: "accepted",
      }),
    ).rejects.toThrow(/strictly newer fence/u)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: run.proof(),
        requireState: "terminal",
      }),
    ).rejects.toThrow(/not terminal/u)
    const originalProof = run.proof()
    await leases.release({ proof: originalProof })
    const acquired = await leases.acquire({
      acquisitionId: `${run.sagaId}:recovery-validation-acquisition`,
      holderId: `${run.sagaId}:recovery-validation-controller`,
      leaseKey: originalProof.leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Saga recovery validation lease failed.")
    const recoveryProof = leaseProof(acquired.record)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: recoveryProof,
        requireState: "accepted",
      }),
    ).resolves.toMatchObject({ protocolVersion: 2, state: "accepted" })
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: { ...recoveryProof, leaseKey: "different-lease" },
        requireState: "accepted",
      }),
    ).rejects.toThrow(/same lease/u)
    database.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempt_protocols"
         SET "protocol_version" = 1 WHERE "attempt_id" = ?`,
      )
      .run(run.actionAttemptId)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: recoveryProof,
        requireState: "accepted",
      }),
    ).resolves.toMatchObject({ protocolVersion: 1, state: "accepted" })

    database.database.exec('DROP TRIGGER "nozzle_control_saga_attempt_update";')
    const changedInputJson = '{"different":true}'
    const changedInputChecksum = await framedChecksum("nozzle.saga-action-input.v1", [
      changedInputJson,
    ])
    const changedAttemptRow: Record<string, unknown> = {
      ...rawAttempt(database.database, run.actionAttemptId),
      input_checksum: changedInputChecksum,
      input_json: changedInputJson,
    }
    changedAttemptRow.acceptance_checksum = await acceptanceForRow(changedAttemptRow)
    database.database
      .prepare(
        `UPDATE "nozzle_saga_action_attempts"
         SET "input_checksum" = ?, "input_json" = ?, "acceptance_checksum" = ?
         WHERE "attempt_id" = ?`,
      )
      .run(
        changedInputChecksum,
        changedInputJson,
        changedAttemptRow.acceptance_checksum as string,
        run.actionAttemptId,
      )
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: recoveryProof,
        requireState: "accepted",
      }),
    ).rejects.toThrow(/protocol-one saga receipt/u)
    await expect(
      attempts.validateProjectionReceipt({
        attemptId: run.actionAttemptId,
        proof: recoveryProof,
        requireState: "invalid" as never,
      }),
    ).rejects.toThrow(/state requirement is invalid/u)
  })

  it("validates receipt inputs and fails closed before an ineligible dispatch", async () => {
    expect(() => new D1SagaAttemptStore(null as never, digest)).toThrow(/binding is required/u)
    expect(
      () => new D1SagaAttemptStore({ prepare: database.prepare.bind(database) } as never, digest),
    ).toThrow(/binding is required/u)
    expect(() => new D1SagaAttemptStore(database, null as never)).toThrow(/digest/u)
    await expect(sagaActionInputChecksum("{}", null as never)).rejects.toThrow(/digest/u)
    await expect(sagaActionInputChecksum("{}", async () => "")).rejects.toThrow(/non-empty/u)
    expect(() => sagaObservationIdempotencyKey("")).toThrow(/non-empty/u)
    await expect(attempts.get("")).rejects.toThrow(/non-empty/u)
    await expect(attempts.get("x".repeat(513))).rejects.toThrow(/receipt limit/u)

    const run = await fixture("validation")
    const base = {
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward" as const,
      proof: run.proof(),
      purpose: "effect" as const,
      sagaId: run.sagaId,
      sagaStepId: "a",
    }
    await expect(attempts.accept({ ...base, phase: "bad" as never })).rejects.toThrow(
      /phase is invalid/u,
    )
    await expect(attempts.accept({ ...base, purpose: "bad" as never })).rejects.toThrow(
      /purpose is invalid/u,
    )
    await expect(
      attempts.accept({ ...base, proof: { ...run.proof(), fencingToken: 0 } }),
    ).rejects.toThrow(/positive safe integer/u)
    await expect(attempts.accept({ ...base, inputJson: "" })).rejects.toThrow(/must be JSON/u)
    await expect(attempts.accept({ ...base, inputJson: "{" })).rejects.toThrow(/not valid JSON/u)
    await expect(
      attempts.accept({ ...base, inputJson: JSON.stringify("x".repeat(1024 * 1024)) }),
    ).rejects.toThrow(/one MiB/u)
    await expect(
      attempts.accept({ ...base, attemptId: `${run.actionAttemptId}:other` }),
    ).rejects.toThrow(/not eligible/u)
    await expect(attempts.accept({ ...base, inputJson: '{"different":true}' })).rejects.toThrow(
      /sealed durable checksum/u,
    )
    await expect(attempts.accept({ ...base, sagaStepId: "missing" })).rejects.toThrow(
      /action state is missing/u,
    )

    const missing = { ...base, attemptId: "missing", sagaId: "missing-saga" }
    await expect(attempts.accept(missing)).rejects.toThrow(/saga does not exist/u)
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { sagaProjectionMissing: true }),
        digest,
      ).accept(base),
    ).rejects.toThrow(/projection disappeared/u)
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { operationProjectionMissing: true }),
        digest,
      ).accept(base),
    ).rejects.toThrow(/operation projection is missing/u)
    await expect(
      new D1SagaAttemptStore(
        new FaultDatabase(database, { sagaBindingRow: { operation_id: null } }),
        digest,
      ).accept(base),
    ).rejects.toThrow(/binding is malformed/u)
    for (const [sagaProjectionChanges, message] of [
      [{ record_checksum: "wrong" }, /canonical record/u],
      [{ effect_record_checksum: "wrong" }, /operation-effect receipt/u],
    ] as const) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { sagaProjectionChanges }),
        digest,
      )
      await expect(faulted.accept(base)).rejects.toThrow(message)
    }

    const wrongProtocol = await fixture("validation-wrong-protocol", undefined, "opaque")
    await expect(
      attempts.accept({
        attemptId: wrongProtocol.actionAttemptId,
        inputJson: wrongProtocol.actionInputJson,
        phase: "forward",
        proof: wrongProtocol.proof(),
        purpose: "effect",
        sagaId: wrongProtocol.sagaId,
        sagaStepId: "a",
      }),
    ).rejects.toThrow(/exact generic operation binding/u)

    const operationRow = database.database
      .prepare(
        `SELECT "record_json" FROM "nozzle_operation_steps"
         WHERE "operation_id" = ? AND "step_id" = ?`,
      )
      .get(run.operationId, run.actionStepId) as { record_json: string }
    const operationRecord = JSON.parse(operationRow.record_json) as Record<string, unknown>
    const activeMismatch = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        operationStepChanges: {
          record_json: JSON.stringify({
            ...operationRecord,
            activeAttemptId: "another-active-attempt",
            lastAttemptId: "another-active-attempt",
          }),
        },
      }),
      digest,
    )
    await expect(activeMismatch.accept(base)).rejects.toThrow(/active attempt/u)

    await expect(
      attempts.complete({
        attemptId: "missing",
        errorJson: "{}",
        evidenceJson: "{}",
        proof: run.proof(),
        state: "failed",
      }),
    ).rejects.toThrow(/never durably accepted/u)
    await expect(
      attempts.complete({
        attemptId: run.actionAttemptId,
        errorJson: "{}",
        evidenceJson: "{}",
        proof: run.proof(),
        state: "bad" as never,
      }),
    ).rejects.toThrow(/outcome is invalid/u)
  })

  it("rejects every malformed or contradictory persisted saga receipt shape", async () => {
    const run = await fixture("persisted-fault")
    await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const acceptedRow = rawAttempt(database.database, run.actionAttemptId)
    for (const [change, message] of [
      [{ attempt_id: "other" }, /identity is malformed/u],
      [{ causal_attempt_id: run.actionAttemptId }, /causal identity is malformed/u],
      [{ causal_attempt_id: "unexpected" }, /causal identity is malformed/u],
      [{ phase: "other" }, /identity is malformed/u],
      [{ protocol_version: 3 }, /identity is malformed/u],
      [
        { protocol_classified_at_ms: (acceptedRow.accepted_at_ms as number) + 1 },
        /identity is malformed/u,
      ],
      [{ input_json: null }, /action input is malformed/u],
      [{ input_json: "{" }, /action input is not valid JSON/u],
      [{ input_json: ` ${acceptedRow.input_json as string}` }, /action input is not canonical/u],
      [{ input_json: JSON.stringify("x".repeat(1024 * 1024)) }, /one MiB/u],
      [{ input_checksum: "0".repeat(64) }, /acceptance checksums do not match/u],
    ] as const) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: { ...acceptedRow, ...change } }),
        digest,
      )
      await expect(faulted.get(run.actionAttemptId)).rejects.toThrow(message)
    }

    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"source":"handler"}',
      outputJson: '{"ok":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    const completedRow = rawAttempt(database.database, run.actionAttemptId)
    const malformedPublication = new D1SagaAttemptStore(
      new FaultDatabase(database, { schemaVersionRow: [] }),
      digest,
    )
    await expect(malformedPublication.get(run.actionAttemptId)).rejects.toThrow(
      /schema publication is malformed/u,
    )
    for (const [change, message] of [
      [{ state: "other" }, /outcome identity is malformed/u],
      [{ completed_at_ms: null }, /terminal saga attempt is incomplete/u],
      [{ evidence_json: null }, /payload references are malformed/u],
      [{ evidence_json: "{" }, /payload references are malformed/u],
      [
        { evidence_json: ` ${completedRow.evidence_json as string}` },
        /payload references are malformed/u,
      ],
      [{ output_checksum: null }, /value checksum is missing/u],
      [{ output_json: null }, /payload references are malformed/u],
      [{ output_json: "{" }, /payload references are malformed/u],
      [
        { output_json: ` ${completedRow.output_json as string}` },
        /payload references are malformed/u,
      ],
      [{ outcome_checksum: "0".repeat(64) }, /outcome checksums do not match/u],
    ] as const) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: { ...completedRow, ...change } }),
        digest,
      )
      await expect(faulted.get(run.actionAttemptId)).rejects.toThrow(message)
    }
  })

  it("loads exact saga receipt rows from one owned snapshot", async () => {
    const accepted = await fixture("codec-accepted")
    await attempts.accept({
      attemptId: accepted.actionAttemptId,
      inputJson: accepted.actionInputJson,
      phase: "forward",
      proof: accepted.proof(),
      purpose: "effect",
      sagaId: accepted.sagaId,
      sagaStepId: "a",
    })
    const acceptedRow = rawIdentity(database.database, accepted.actionAttemptId)
    const loadedIdentity = await loadSagaAttemptIdentityRow(acceptedRow, digest)
    const loadedAccepted = acceptedSagaAttemptRecord(loadedIdentity)
    expect(loadedAccepted).toMatchObject({
      attemptId: accepted.actionAttemptId,
      protocolVersion: 2,
      state: "accepted",
    })
    expect(Object.isFrozen(loadedAccepted)).toBe(true)

    const switching = { ...acceptedRow }
    let inputReads = 0
    Object.defineProperty(switching, "input_json", {
      enumerable: true,
      get() {
        inputReads += 1
        return inputReads === 1 ? acceptedRow.input_json : "{}"
      },
    })
    await expect(
      loadSagaAttemptIdentityRow(switching, digest, accepted.actionAttemptId),
    ).resolves.toMatchObject({ attemptId: accepted.actionAttemptId })
    expect(inputReads).toBe(1)

    const mutable = { ...acceptedRow }
    let digestCalls = 0
    const mutatingDigest: DigestFunction = async (input) => {
      digestCalls += 1
      mutable.input_json = "{}"
      return digest(input)
    }
    await expect(
      loadSagaAttemptIdentityRow(mutable, mutatingDigest, accepted.actionAttemptId),
    ).resolves.toMatchObject({ inputJson: acceptedRow.input_json })
    expect(digestCalls).toBeGreaterThan(0)

    const missing = { ...acceptedRow }
    delete missing.phase
    for (const [candidate, message] of [
      [missing, /row fields are malformed/u],
      [{ ...acceptedRow, unchecked: true }, /row fields are malformed/u],
      [[], /row fields are malformed/u],
      [new Proxy(acceptedRow, {}), /could not be captured safely/u],
    ] as const) {
      await expect(loadSagaAttemptIdentityRow(candidate, digest)).rejects.toThrow(message)
    }
    await expect(loadSagaAttemptIdentityRow(acceptedRow, null as never)).rejects.toMatchObject({
      code: "ConfigurationError",
    })
    await expect(loadSagaAttemptIdentityRow(acceptedRow, () => "bad")).rejects.toThrow(
      /lowercase SHA-256/u,
    )

    const confirmed = await fixture("codec-confirmed")
    await attempts.accept({
      attemptId: confirmed.actionAttemptId,
      inputJson: confirmed.actionInputJson,
      phase: "forward",
      proof: confirmed.proof(),
      purpose: "effect",
      sagaId: confirmed.sagaId,
      sagaStepId: "a",
    })
    await attempts.complete({
      attemptId: confirmed.actionAttemptId,
      evidenceJson: '{"source":"codec"}',
      outputJson: '{"ok":true}',
      proof: confirmed.proof(),
      state: "confirmed",
    })
    const confirmedIdentity = await loadSagaAttemptIdentityRow(
      rawIdentity(database.database, confirmed.actionAttemptId),
      digest,
    )
    const confirmedRow = rawOutcome(database.database, confirmed.actionAttemptId) as Record<
      string,
      unknown
    >
    const confirmedPayloads = rawPayloads(database.database, confirmed.actionAttemptId)
    await expect(
      loadSagaAttemptOutcomeRow(confirmedRow, confirmedPayloads, confirmedIdentity, digest),
    ).resolves.toMatchObject({ state: "confirmed" })
    await expect(
      loadSagaAttemptOutcomeRow(confirmedRow, confirmedPayloads, confirmedIdentity, null as never),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(
      loadSagaAttemptOutcomeRow(
        { ...confirmedRow, error_checksum: "unexpected", error_json: "{}" },
        confirmedPayloads,
        confirmedIdentity,
        digest,
      ),
    ).rejects.toThrow(/value columns are contradictory/u)
    const sparsePayloads = new Array<Record<string, unknown>>(2)
    sparsePayloads[0] = confirmedPayloads[0] as Record<string, unknown>
    await expect(
      loadSagaAttemptOutcomeRow(confirmedRow, sparsePayloads, confirmedIdentity, digest),
    ).rejects.toThrow(/payload row fields are malformed/u)
    const evidencePayload = confirmedPayloads.find(
      (row) => row.payload_kind === "evidence",
    ) as Record<string, unknown>
    const outputPayload = confirmedPayloads.find((row) => row.payload_kind === "output") as Record<
      string,
      unknown
    >
    for (const [payloadRows, message] of [
      [[evidencePayload], /payload references are malformed/u],
      [
        [evidencePayload, outputPayload, { ...outputPayload, payload_kind: "error" }],
        /payload references are malformed/u,
      ],
      [
        [evidencePayload, { ...outputPayload, payload_kind: "error" }],
        /payload identity is malformed/u,
      ],
      [
        [evidencePayload, { ...outputPayload, payload_checksum: "0".repeat(64) }],
        /payload set is incomplete or contradictory/u,
      ],
      [
        [evidencePayload, { ...outputPayload, payload_json: ` ${outputPayload.payload_json}` }],
        /payload is not canonical/u,
      ],
      [
        [evidencePayload, { ...outputPayload, unchecked: true }],
        /payload row fields are malformed/u,
      ],
      [
        [evidencePayload, { ...outputPayload, attempt_id: "different" }],
        /payload identity is malformed/u,
      ],
      [new Proxy(confirmedPayloads, {}), /could not be captured safely/u],
    ] as const) {
      await expect(
        loadSagaAttemptOutcomeRow(confirmedRow, payloadRows, confirmedIdentity, digest),
      ).rejects.toThrow(message)
    }

    const failed = await fixture("codec-failed")
    await attempts.accept({
      attemptId: failed.actionAttemptId,
      inputJson: failed.actionInputJson,
      phase: "forward",
      proof: failed.proof(),
      purpose: "effect",
      sagaId: failed.sagaId,
      sagaStepId: "a",
    })
    await attempts.complete({
      attemptId: failed.actionAttemptId,
      errorJson: '{"code":"failed"}',
      evidenceJson: '{"source":"codec"}',
      proof: failed.proof(),
      state: "failed",
    })
    const failedIdentity = await loadSagaAttemptIdentityRow(
      rawIdentity(database.database, failed.actionAttemptId),
      digest,
    )
    const failedRow = rawOutcome(database.database, failed.actionAttemptId) as Record<
      string,
      unknown
    >
    const failedPayloads = rawPayloads(database.database, failed.actionAttemptId)
    const failedEvidence = failedPayloads.find((row) => row.payload_kind === "evidence") as Record<
      string,
      unknown
    >
    const failedError = failedPayloads.find((row) => row.payload_kind === "error") as Record<
      string,
      unknown
    >
    await expect(
      loadSagaAttemptOutcomeRow(
        {
          ...failedRow,
          error_json: failedError.payload_json,
          evidence_json: failedEvidence.payload_json,
        },
        [],
        failedIdentity,
        digest,
      ),
    ).resolves.toMatchObject({ state: "failed" })
    for (const state of ["failed", "not_applied", "unknown"] as const) {
      const candidate: Record<string, unknown> = { ...failedRow, state }
      candidate.outcome_checksum = await outcomeForRow({
        ...failedIdentity,
        ...candidate,
        acceptance_checksum: failedIdentity.acceptanceChecksum,
        evidence_json: failedEvidence.payload_json,
        error_json: failedError.payload_json,
      })
      await expect(
        loadSagaAttemptOutcomeRow(candidate, failedPayloads, failedIdentity, digest),
      ).resolves.toMatchObject({ state })
    }
    const incompatible: Record<string, unknown> = { ...failedRow, state: "indeterminate" }
    incompatible.outcome_checksum = await outcomeForRow({
      ...failedIdentity,
      ...incompatible,
      acceptance_checksum: failedIdentity.acceptanceChecksum,
      evidence_json: failedEvidence.payload_json,
      error_json: failedError.payload_json,
    })
    await expect(
      loadSagaAttemptOutcomeRow(incompatible, failedPayloads, failedIdentity, digest),
    ).rejects.toThrow(/incompatible outcome/u)
    await expect(
      loadSagaAttemptOutcomeRow(
        { ...failedRow, output_checksum: "unexpected", output_json: "{}" },
        failedPayloads,
        failedIdentity,
        digest,
      ),
    ).rejects.toThrow(/value columns are contradictory/u)
  })

  it("fails closed on rejected writes, stale fences, and contradictory committed outcomes", async () => {
    const run = await fixture("write-fault")
    const acceptInput = {
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward" as const,
      proof: run.proof(),
      purpose: "effect" as const,
      sagaId: run.sagaId,
      sagaStepId: "a",
    }
    const malformed = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 3 }, success: true },
      }),
      digest,
    )
    await expect(malformed.accept(acceptInput)).rejects.toThrow(/malformed saga-receipt/u)
    const rejected = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(rejected.accept(acceptInput)).rejects.toThrow(/not accepted/u)

    await attempts.accept(acceptInput)
    const acceptedRow = rawAttempt(database.database, run.actionAttemptId)
    const alteredRow: Record<string, unknown> = {
      ...acceptedRow,
      operation_step_id: "other-step",
    }
    alteredRow.acceptance_checksum = await acceptanceForRow(alteredRow)
    const contradictoryAccept = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        attemptRow: alteredRow,
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(contradictoryAccept.accept(acceptInput)).rejects.toThrow(
      /contradictory immutable input/u,
    )
    const legacyAccept = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        attemptRow: { ...acceptedRow, protocol_version: 1 },
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(legacyAccept.accept(acceptInput)).rejects.toThrow(/contradictory immutable input/u)

    await expect(
      attempts.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: "{}",
        outputJson: "{}",
        proof: { ...run.proof(), acquisitionId: "other-acquisition" },
        state: "confirmed",
      }),
    ).rejects.toThrow(/fenced by a different lease owner/u)

    const outcomeRejected = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(
      outcomeRejected.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: "{}",
        outputJson: "{}",
        proof: run.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/malformed saga-receipt batch metadata/u)

    const acceptedBefore = rawAttempt(database.database, run.actionAttemptId)
    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"actual":true}',
      outputJson: '{"actual":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    const actualCompleted = rawAttempt(database.database, run.actionAttemptId)
    let reads = 0
    const contradictoryOutcome = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        attemptRow: () => (reads++ === 0 ? acceptedBefore : actualCompleted),
        runResult: { meta: { changes: 1 }, success: true },
      }),
      digest,
    )
    await expect(
      contradictoryOutcome.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: '{"requested":true}',
        outputJson: '{"requested":true}',
        proof: run.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/duplicate saga outcome contradicts/u)
  })
})
