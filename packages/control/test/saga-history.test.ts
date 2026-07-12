import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type DigestFunction,
  type OperationPlan,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import type { SagaAttemptIdentityRow } from "../src/saga-attempt-codec.js"
import {
  D1SagaHistoryReader,
  SAGA_HISTORY_PAGE_MAX_BYTES,
  type SagaHistoryAnchor,
  type SagaHistoryAuditRow,
  type SagaHistoryEffectRow,
  type SagaHistoryTransitionRow,
} from "../src/saga-history.js"

type ScriptedResult = unknown | (() => unknown)

const digest: DigestFunction = async (input) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", input.slice().buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

async function sealedHistoryPlan(
  overrides: Partial<Pick<OperationPlan, "inputChecksum" | "operationId" | "operationType">> = {},
) {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "capability-snapshot",
      idempotencyKey: "operation-key",
      inputChecksum: overrides.inputChecksum ?? "operation-input",
      operationId: overrides.operationId ?? "operation-a",
      operationType: overrides.operationType ?? "saga:fixture@1",
      steps: [
        {
          checkpoint: "reversible",
          dependsOn: [],
          idempotencyKey: "saga-init-key",
          inputChecksum: "saga-init-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "saga-init-postcondition",
          preconditionChecksum: "saga-init-precondition",
          recoveryInstructions: "Reconstruct the immutable saga initialization receipt.",
          retryClassification: "idempotent",
          stepId: "saga:init",
        },
      ],
    },
    digest,
  )
}

async function sealedHistoryDescriptor() {
  return sealSagaDescriptor(
    {
      descriptorId: "history-descriptor",
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 1,
          compensationAction: {
            actionId: "history-compensate",
            artifactChecksum: "a".repeat(64),
            version: 1,
          },
          compensationObservation: {
            actionId: "history-observe-compensation",
            artifactChecksum: "b".repeat(64),
            version: 1,
          },
          forwardAction: {
            actionId: "history-write",
            artifactChecksum: "c".repeat(64),
            version: 1,
          },
          forwardObservation: {
            actionId: "history-observe-write",
            artifactChecksum: "d".repeat(64),
            version: 1,
          },
          inputSchemaChecksum: "e".repeat(64),
          irreversible: false,
          maxAttempts: 1,
          maxRetryDelayMs: 1,
          outputSchemaChecksum: "f".repeat(64),
          stepId: "write",
          timeoutMs: 100,
        },
      ],
      version: 1,
    },
    digest,
  )
}

interface ScriptedCall {
  readonly kind: "all" | "first"
  readonly result: ScriptedResult
  readonly sql: string
}

class ScriptedStatement implements ControlStatement {
  readonly #database: ScriptedDatabase
  readonly #sql: string
  #values: readonly ControlBindingValue[] = []

  constructor(database: ScriptedDatabase, sql: string) {
    this.#database = database
    this.#sql = sql
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#values = values
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return this.#database.take("all", this.#sql, this.#values) as ControlQueryResult<T>
  }

  async first<T>(): Promise<T | null> {
    return this.#database.take("first", this.#sql, this.#values) as T | null
  }

  async run(): Promise<ControlRunResult> {
    throw new Error("Unexpected history mutation")
  }
}

class ScriptedDatabase implements TransactionalControlDatabase {
  readonly calls: { readonly sql: string; readonly values: readonly ControlBindingValue[] }[] = []
  readonly #script: ScriptedCall[]

  constructor(script: readonly ScriptedCall[]) {
    this.#script = [...script]
  }

  async batch(): Promise<readonly ControlRunResult[]> {
    throw new Error("Unexpected history batch")
  }

  prepare(sql: string): ControlStatement {
    return new ScriptedStatement(this, sql)
  }

  take(kind: ScriptedCall["kind"], sql: string, values: readonly ControlBindingValue[]): unknown {
    const call = this.#script.shift()
    if (call === undefined) throw new Error(`Unexpected ${kind} history query: ${sql}`)
    expect(kind).toBe(call.kind)
    expect(sql).toContain(call.sql)
    this.calls.push({ sql, values })
    return typeof call.result === "function" ? call.result() : call.result
  }

  expectComplete(): void {
    expect(this.#script).toEqual([])
  }
}

class SqliteStatement implements ControlStatement {
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
    return {
      meta: {},
      results: this.#statement.all(this.#values) as T[],
      success: true,
    }
  }

  async first<T>(): Promise<T | null> {
    return (this.#statement.get(this.#values) as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    const result = this.#statement.run(this.#values)
    return { meta: { changes: Number(result.changes) }, success: true }
  }
}

class SqliteDatabase implements TransactionalControlDatabase {
  readonly database = new DatabaseSync(":memory:")

  async batch(): Promise<readonly ControlRunResult[]> {
    throw new Error("Unexpected history batch")
  }

  prepare(sql: string): ControlStatement {
    return new SqliteStatement(this.database.prepare(sql))
  }
}

const identity = Object.freeze({
  environment_id: "environment-a",
  operation_id: "operation-a",
  operation_input_checksum: "operation-input",
  operation_plan_checksum: "operation-plan",
  operation_status: "running",
  operation_updated_at_ms: 11,
  saga_descriptor_checksum: "saga-descriptor",
  saga_id: "saga-a",
  saga_input_checksum: "saga-input",
  saga_last_effect_id: "effect-2",
  saga_operation_id: "operation-a",
  saga_record_checksum: "saga-record-2",
  saga_state_version: 2,
  saga_status: "failed",
  saga_updated_at_ms: 12,
})

const transitionSummary = Object.freeze({
  history_count: 3,
  joined_history_count: 3,
  last_audit_sequence: 7,
  last_transition_id: "transition-7",
})

const effectSummary = Object.freeze({
  history_count: 3,
  last_effect_id: "effect-2",
  last_state_version: 2,
})

const attemptSummary = Object.freeze({
  history_count: 3,
  last_accepted_at_ms: 3,
  last_attempt_id: "attempt-3",
})

const auditHead = Object.freeze({ event_hash: "audit-9", sequence: 9 })

const anchor: SagaHistoryAnchor = Object.freeze({
  auditHeadEventHash: "audit-9",
  auditHeadSequence: 9,
  environmentId: "environment-a",
  operationId: "operation-a",
  operationInputChecksum: "operation-input",
  operationPlanChecksum: "operation-plan",
  operationStatus: "running",
  operationTransitionCount: 3,
  operationTransitionLastAuditSequence: 7,
  operationTransitionLastId: "transition-7",
  operationUpdatedAtMs: 11,
  sagaAttemptCount: 3,
  sagaAttemptLastAcceptedAtMs: 3,
  sagaAttemptLastId: "attempt-3",
  sagaDescriptorChecksum: "saga-descriptor",
  sagaEffectCount: 3,
  sagaId: "saga-a",
  sagaInputChecksum: "saga-input",
  sagaLastEffectId: "effect-2",
  sagaRecordChecksum: "saga-record-2",
  sagaStateVersion: 2,
  sagaStatus: "failed",
  sagaUpdatedAtMs: 12,
  schemaVersion: 1,
})

function first(sql: string, result: ScriptedResult): ScriptedCall {
  return { kind: "first", result, sql }
}

function all(sql: string, rows: readonly unknown[], overrides: Record<string, unknown> = {}) {
  return {
    kind: "all" as const,
    result: { meta: {}, results: rows, success: true, ...overrides },
    sql,
  }
}

function captureScript(
  input: {
    readonly attempts?: unknown
    readonly finalAttempts?: unknown
    readonly finalEffects?: unknown
    readonly finalIdentity?: unknown
    readonly finalTransitions?: unknown
    readonly finalAudit?: unknown
    readonly effects?: unknown
    readonly head?: unknown
    readonly identity?: unknown
    readonly transitions?: unknown
  } = {},
): readonly ScriptedCall[] {
  return [
    first('FROM "nozzle_operations" AS "operation"', input.identity ?? identity),
    first(
      'FROM "nozzle_operation_transitions" AS "transition"',
      input.transitions ?? transitionSummary,
    ),
    first('FROM "nozzle_operation_effects"', input.effects ?? effectSummary),
    first(
      'FROM "nozzle_saga_action_attempts"',
      input.attempts === undefined ? attemptSummary : input.attempts,
    ),
    first('FROM "nozzle_audit_log"', input.head ?? auditHead),
    first(
      'FROM "nozzle_operations" AS "operation"',
      input.finalIdentity ?? input.identity ?? identity,
    ),
    first(
      'FROM "nozzle_operation_transitions" AS "transition"',
      input.finalTransitions ?? input.transitions ?? transitionSummary,
    ),
    first('FROM "nozzle_operation_effects"', input.finalEffects ?? input.effects ?? effectSummary),
    first(
      'FROM "nozzle_saga_action_attempts"',
      input.finalAttempts ?? (input.attempts === undefined ? attemptSummary : input.attempts),
    ),
    first('AND "sequence" = ?2', input.finalAudit ?? input.head ?? auditHead),
  ]
}

function auditRow(sequence: number): SagaHistoryAuditRow {
  return {
    event_hash: `audit-${sequence}`,
    event_json: JSON.stringify({ sequence }),
    sequence,
  }
}

function transitionRow(
  sequence: number,
  id = `transition-${sequence}`,
  authorization: 0 | 1 | 2 = 0,
): SagaHistoryTransitionRow {
  return {
    acquisition_id: "acquisition-a",
    audit_event_hash: `audit-${sequence}`,
    audit_event_json: JSON.stringify({ sequence }),
    audit_sequence: sequence,
    authorization_checksum: authorization === 0 ? null : `authorization-${sequence}`,
    authorization_classified_at_ms: authorization === 0 ? null : sequence,
    authorization_id: authorization === 2 ? `authorization-id-${sequence}` : null,
    authorization_protocol_version: authorization === 0 ? null : authorization,
    authorization_transition_id: authorization === 0 ? null : id,
    created_at_ms: sequence,
    fencing_token: 1,
    from_operation_status: "running",
    from_record_json: '{"state":"running"}',
    holder_id: "holder-a",
    lease_key: "saga:saga-a",
    operation_id: "operation-a",
    step_id: "saga:forward:a",
    to_operation_status: "running",
    to_record_json: '{"state":"succeeded"}',
    transition_id: id,
  }
}

function effectRow(version: number): SagaHistoryEffectRow {
  return {
    acquisition_id: "acquisition-a",
    created_at_ms: version,
    effect_id: `effect-${version}`,
    effect_kind: version === 0 ? "create" : "action:forward:success",
    evidence_checksum: `evidence-${version}`,
    fencing_token: 1,
    from_state_version: version === 0 ? null : version - 1,
    holder_id: "holder-a",
    lease_key: "saga:saga-a",
    operation_id: "operation-a",
    record_checksum: `record-${version}`,
    record_json: JSON.stringify({ stateVersion: version }),
    resource_id: "saga-a",
    resource_kind: "saga",
    step_id: version === 0 ? "saga:init" : "saga:forward:a",
    to_state_version: version,
    transition_id: `transition-${version}`,
  }
}

function attemptRow(acceptedAtMs: number, attemptId: string): SagaAttemptIdentityRow {
  return {
    acceptance_checksum: `acceptance-${attemptId}`,
    accepted_at_ms: acceptedAtMs,
    acquisition_id: "acquisition-a",
    action_key: "action@1:checksum",
    attempt_id: attemptId,
    causal_attempt_id: null,
    fencing_token: 1,
    holder_id: "holder-a",
    idempotency_key: `idempotency-${attemptId}`,
    input_checksum: `input-${attemptId}`,
    input_json: "{}",
    lease_key: "saga:saga-a",
    operation_id: "operation-a",
    operation_step_id: "saga:forward:a",
    phase: "forward",
    protocol_classified_at_ms: acceptedAtMs,
    protocol_version: 2,
    purpose: "effect",
    saga_id: "saga-a",
    saga_step_id: "a",
  }
}

function sqliteHistoryDatabase(): SqliteDatabase {
  const adapter = new SqliteDatabase()
  const database = adapter.database
  database.exec(`
    CREATE TABLE "nozzle_operations" (
      "operation_id" TEXT PRIMARY KEY, "environment_id" TEXT, "input_checksum" TEXT,
      "plan_checksum" TEXT, "status" TEXT, "updated_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_sagas" (
      "saga_id" TEXT PRIMARY KEY, "operation_id" TEXT, "descriptor_checksum" TEXT,
      "input_checksum" TEXT, "state_version" INTEGER, "status" TEXT,
      "last_effect_id" TEXT, "record_checksum" TEXT, "updated_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_audit_log" (
      "environment_id" TEXT, "sequence" INTEGER, "event_hash" TEXT, "event_json" TEXT
    );
    CREATE TABLE "nozzle_operation_transitions" (
      "transition_id" TEXT PRIMARY KEY, "operation_id" TEXT, "step_id" TEXT,
      "from_record_json" TEXT, "to_record_json" TEXT, "from_operation_status" TEXT,
      "to_operation_status" TEXT, "audit_event_hash" TEXT, "fencing_token" INTEGER,
      "lease_key" TEXT, "holder_id" TEXT, "acquisition_id" TEXT, "created_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_irreversible_authorization_receipts" (
      "transition_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "authorization_id" TEXT,
      "authorization_checksum" TEXT, "classified_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_operation_effects" (
      "effect_id" TEXT PRIMARY KEY, "transition_id" TEXT, "operation_id" TEXT,
      "step_id" TEXT, "resource_kind" TEXT, "resource_id" TEXT, "effect_kind" TEXT,
      "from_state_version" INTEGER, "to_state_version" INTEGER, "evidence_checksum" TEXT,
      "record_checksum" TEXT, "record_json" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "created_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_saga_action_attempts" (
      "attempt_id" TEXT PRIMARY KEY, "causal_attempt_id" TEXT, "saga_id" TEXT,
      "operation_id" TEXT, "operation_step_id" TEXT, "saga_step_id" TEXT, "phase" TEXT,
      "purpose" TEXT, "action_key" TEXT, "idempotency_key" TEXT, "input_checksum" TEXT,
      "input_json" TEXT, "acceptance_checksum" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "accepted_at_ms" INTEGER
    );
    CREATE TABLE "nozzle_saga_action_attempt_protocols" (
      "attempt_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "classified_at_ms" INTEGER
    );
    INSERT INTO "nozzle_operations" VALUES
      ('operation-a', 'environment-a', 'operation-input', 'operation-plan', 'running', 11);
    INSERT INTO "nozzle_sagas" VALUES
      ('saga-a', 'operation-a', 'saga-descriptor', 'saga-input', 2, 'failed',
       'effect-2', 'saga-record-2', 12);
  `)
  const insertAudit = database.prepare(
    `INSERT INTO "nozzle_audit_log" VALUES ('environment-a', ?, ?, ?)`,
  )
  for (let sequence = 1; sequence <= 9; sequence += 1) {
    insertAudit.run(sequence, `audit-${sequence}`, JSON.stringify({ sequence }))
  }
  const insertTransition = database.prepare(
    `INSERT INTO "nozzle_operation_transitions" VALUES
     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
  for (const row of [
    transitionRow(2),
    transitionRow(4, "transition-4", 1),
    transitionRow(7, "transition-7", 2),
  ]) {
    insertTransition.run(
      row.transition_id,
      row.operation_id,
      row.step_id,
      row.from_record_json,
      row.to_record_json,
      row.from_operation_status,
      row.to_operation_status,
      row.audit_event_hash,
      row.fencing_token,
      row.lease_key,
      row.holder_id,
      row.acquisition_id,
      row.created_at_ms,
    )
    if (row.authorization_protocol_version !== null) {
      database
        .prepare(`INSERT INTO "nozzle_irreversible_authorization_receipts" VALUES (?, ?, ?, ?, ?)`)
        .run(
          row.transition_id,
          row.authorization_protocol_version,
          row.authorization_id,
          row.authorization_checksum,
          row.authorization_classified_at_ms,
        )
    }
  }
  const insertEffect = database.prepare(
    `INSERT INTO "nozzle_operation_effects" VALUES
     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
  for (const row of [effectRow(0), effectRow(1), effectRow(2)]) {
    insertEffect.run(
      row.effect_id,
      row.transition_id,
      row.operation_id,
      row.step_id,
      row.resource_kind,
      row.resource_id,
      row.effect_kind,
      row.from_state_version,
      row.to_state_version,
      row.evidence_checksum,
      row.record_checksum,
      row.record_json,
      row.lease_key,
      row.holder_id,
      row.acquisition_id,
      row.fencing_token,
      row.created_at_ms,
    )
  }
  const insertAttempt = database.prepare(
    `INSERT INTO "nozzle_saga_action_attempts" VALUES
     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
  for (const row of [
    attemptRow(1, "attempt-1"),
    attemptRow(2, "attempt-2"),
    attemptRow(3, "attempt-3"),
  ]) {
    insertAttempt.run(
      row.attempt_id,
      row.causal_attempt_id,
      row.saga_id,
      row.operation_id,
      row.operation_step_id,
      row.saga_step_id,
      row.phase,
      row.purpose,
      row.action_key,
      row.idempotency_key,
      row.input_checksum,
      row.input_json,
      row.acceptance_checksum,
      row.lease_key,
      row.holder_id,
      row.acquisition_id,
      row.fencing_token,
      row.accepted_at_ms,
    )
    database
      .prepare(`INSERT INTO "nozzle_saga_action_attempt_protocols" VALUES (?, ?, ?)`)
      .run(row.attempt_id, row.protocol_version, row.protocol_classified_at_ms)
  }
  return adapter
}

describe("D1SagaHistoryReader", () => {
  it("executes every anchor and keyset query against SQLite", async () => {
    const database = sqliteHistoryDatabase()
    try {
      const reader = new D1SagaHistoryReader(database)
      const captured = await reader.captureAnchor("operation-a", "saga-a")
      expect(captured).toEqual(anchor)
      await expect(reader.assertAnchorCurrent(captured)).resolves.toBeUndefined()
      await expect(reader.auditPage(captured)).resolves.toMatchObject({
        complete: false,
        nextCursor: 2,
      })
      await expect(reader.transitionPage(captured)).resolves.toMatchObject({
        complete: false,
        nextCursor: { auditSequence: 4, transitionId: "transition-4" },
      })
      await expect(reader.effectPage(captured)).resolves.toMatchObject({
        complete: false,
        nextCursor: 1,
      })
      await expect(reader.attemptIdentityPage(captured)).resolves.toMatchObject({
        complete: false,
        nextCursor: { acceptedAtMs: 2, attemptId: "attempt-2" },
      })
    } finally {
      database.database.close()
    }
  })

  it("loads the canonical immutable operation plan bound to an anchor", async () => {
    const plan = await sealedHistoryPlan()
    const inputAnchor = Object.freeze({ ...anchor, operationPlanChecksum: plan.planChecksum })
    const database = new ScriptedDatabase([
      {
        kind: "first",
        result: {
          input_checksum: plan.inputChecksum,
          operation_id: plan.operationId,
          plan_checksum: plan.planChecksum,
          plan_json: JSON.stringify(plan),
        },
        sql: `SELECT "operation_id", "input_checksum", "plan_checksum", "plan_json"`,
      },
    ])
    const loaded = await new D1SagaHistoryReader(database).operationPlan(inputAnchor, digest)
    expect(loaded).toEqual(plan)
    expect(Object.isFrozen(loaded)).toBe(true)
    database.expectComplete()
  })

  it("loads the canonical immutable saga descriptor bound to an anchor", async () => {
    const descriptor = await sealedHistoryDescriptor()
    const inputAnchor = Object.freeze({
      ...anchor,
      sagaDescriptorChecksum: descriptor.descriptorChecksum,
    })
    const database = new ScriptedDatabase([
      {
        kind: "first",
        result: {
          descriptor_checksum: descriptor.descriptorChecksum,
          descriptor_id: descriptor.descriptorId,
          descriptor_json: JSON.stringify(descriptor),
          descriptor_version: descriptor.version,
        },
        sql: `SELECT "descriptor_id", "descriptor_version", "descriptor_checksum", "descriptor_json"`,
      },
    ])
    const loaded = await new D1SagaHistoryReader(database).sagaDescriptor(inputAnchor, digest)
    expect(loaded).toEqual(descriptor)
    expect(Object.isFrozen(loaded)).toBe(true)
    database.expectComplete()
  })

  it("rejects missing, malformed, oversized, or noncanonical persisted saga descriptors", async () => {
    const descriptor = await sealedHistoryDescriptor()
    const inputAnchor = Object.freeze({
      ...anchor,
      sagaDescriptorChecksum: descriptor.descriptorChecksum,
    })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).sagaDescriptor(
        inputAnchor,
        undefined as unknown as DigestFunction,
      ),
    ).rejects.toMatchObject({ code: "ConfigurationError" })

    const anchoredRow = {
      descriptor_checksum: descriptor.descriptorChecksum,
      descriptor_id: descriptor.descriptorId,
      descriptor_json: JSON.stringify(descriptor),
      descriptor_version: descriptor.version,
    }
    const malformedRows: readonly [unknown, RegExp][] = [
      [null, /disappeared/u],
      [{ ...anchoredRow, extra: true }, /fields/u],
      [{ ...anchoredRow, descriptor_checksum: "0".repeat(64) }, /contradicts/u],
      [{ ...anchoredRow, descriptor_id: "" }, /contradicts/u],
      [{ ...anchoredRow, descriptor_version: 0 }, /contradicts/u],
      [{ ...anchoredRow, descriptor_json: "{" }, /contradicts/u],
      [{ ...anchoredRow, descriptor_json: JSON.stringify("x".repeat(2_000_000)) }, /contradicts/u],
      [{ ...anchoredRow, descriptor_json: JSON.stringify(descriptor, null, 2) }, /not canonical/u],
      [{ ...anchoredRow, descriptor_id: "other" }, /not canonical/u],
      [{ ...anchoredRow, descriptor_version: 2 }, /not canonical/u],
    ]
    for (const [result, message] of malformedRows) {
      const database = new ScriptedDatabase([
        {
          kind: "first",
          result,
          sql: `SELECT "descriptor_id", "descriptor_version", "descriptor_checksum", "descriptor_json"`,
        },
      ])
      await expect(
        new D1SagaHistoryReader(database).sagaDescriptor(inputAnchor, digest),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(message),
      })
      database.expectComplete()
    }

    const changedBody = {
      ...descriptor,
      steps: descriptor.steps.map((step) => ({ ...step, timeoutMs: step.timeoutMs + 1 })),
    }
    const checksumDatabase = new ScriptedDatabase([
      {
        kind: "first",
        result: { ...anchoredRow, descriptor_json: JSON.stringify(changedBody) },
        sql: `SELECT "descriptor_id", "descriptor_version", "descriptor_checksum", "descriptor_json"`,
      },
    ])
    await expect(
      new D1SagaHistoryReader(checksumDatabase).sagaDescriptor(inputAnchor, digest),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    checksumDatabase.expectComplete()
  })

  it("rejects missing, malformed, oversized, noncanonical, or non-saga operation plans", async () => {
    const plan = await sealedHistoryPlan()
    const inputAnchor = Object.freeze({ ...anchor, operationPlanChecksum: plan.planChecksum })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).operationPlan(
        inputAnchor,
        undefined as unknown as DigestFunction,
      ),
    ).rejects.toMatchObject({ code: "ConfigurationError" })

    const anchoredRow = {
      input_checksum: plan.inputChecksum,
      operation_id: plan.operationId,
      plan_checksum: plan.planChecksum,
      plan_json: JSON.stringify(plan),
    }
    const malformedRows: readonly [unknown, RegExp][] = [
      [null, /disappeared/u],
      [{ ...anchoredRow, extra: true }, /fields/u],
      [{ ...anchoredRow, operation_id: "other" }, /contradicts/u],
      [{ ...anchoredRow, input_checksum: "other" }, /contradicts/u],
      [{ ...anchoredRow, plan_checksum: "other" }, /contradicts/u],
      [{ ...anchoredRow, plan_json: "{" }, /contradicts/u],
      [{ ...anchoredRow, plan_json: JSON.stringify("x".repeat(2_000_000)) }, /contradicts/u],
      [{ ...anchoredRow, plan_json: JSON.stringify(plan, null, 2) }, /not canonical/u],
    ]
    for (const [result, message] of malformedRows) {
      const database = new ScriptedDatabase([
        {
          kind: "first",
          result,
          sql: `SELECT "operation_id", "input_checksum", "plan_checksum", "plan_json"`,
        },
      ])
      await expect(
        new D1SagaHistoryReader(database).operationPlan(inputAnchor, digest),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(message),
      })
      database.expectComplete()
    }

    const generic = await sealedHistoryPlan({ operationType: "generic" })
    const genericDatabase = new ScriptedDatabase([
      {
        kind: "first",
        result: {
          input_checksum: generic.inputChecksum,
          operation_id: generic.operationId,
          plan_checksum: generic.planChecksum,
          plan_json: JSON.stringify(generic),
        },
        sql: `SELECT "operation_id", "input_checksum", "plan_checksum", "plan_json"`,
      },
    ])
    await expect(
      new D1SagaHistoryReader(genericDatabase).operationPlan(
        { ...inputAnchor, operationPlanChecksum: generic.planChecksum },
        digest,
      ),
    ).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: expect.stringMatching(/not canonical/u),
    })
    genericDatabase.expectComplete()
  })

  it("matches SQLite BINARY keyset order across a UTF-16 ordering boundary", async () => {
    const database = sqliteHistoryDatabase()
    const binaryFirst = "id-\u{e000}"
    const binaryLast = "id-\u{10000}"
    expect(binaryFirst > binaryLast).toBe(true)
    try {
      for (const [currentId, nextId] of [
        ["transition-4", binaryFirst] as const,
        ["transition-7", binaryLast] as const,
      ]) {
        database.database
          .prepare(
            `UPDATE "nozzle_irreversible_authorization_receipts"
             SET "transition_id" = ? WHERE "transition_id" = ?`,
          )
          .run(nextId, currentId)
        database.database
          .prepare(
            `UPDATE "nozzle_operation_transitions"
             SET "transition_id" = ?, "audit_event_hash" = 'audit-7'
             WHERE "transition_id" = ?`,
          )
          .run(nextId, currentId)
      }
      for (const [currentId, nextId] of [
        ["attempt-2", binaryFirst] as const,
        ["attempt-3", binaryLast] as const,
      ]) {
        database.database
          .prepare(
            `UPDATE "nozzle_saga_action_attempt_protocols"
             SET "attempt_id" = ?, "classified_at_ms" = 3 WHERE "attempt_id" = ?`,
          )
          .run(nextId, currentId)
        database.database
          .prepare(
            `UPDATE "nozzle_saga_action_attempts"
             SET "attempt_id" = ?, "accepted_at_ms" = 3 WHERE "attempt_id" = ?`,
          )
          .run(nextId, currentId)
      }

      const reader = new D1SagaHistoryReader(database)
      const captured = await reader.captureAnchor("operation-a", "saga-a")
      expect(captured.operationTransitionLastId).toBe(binaryLast)
      expect(captured.sagaAttemptLastId).toBe(binaryLast)

      const transitionStart = await reader.transitionPage(captured)
      expect(transitionStart.nextCursor).toEqual({
        auditSequence: 7,
        transitionId: binaryFirst,
      })
      await expect(
        reader.transitionPage(captured, transitionStart.nextCursor ?? undefined),
      ).resolves.toMatchObject({
        complete: true,
        rows: [{ transition_id: binaryLast }],
      })

      const attemptStart = await reader.attemptIdentityPage(captured)
      expect(attemptStart.nextCursor).toEqual({ acceptedAtMs: 3, attemptId: binaryFirst })
      await expect(
        reader.attemptIdentityPage(captured, attemptStart.nextCursor ?? undefined),
      ).resolves.toMatchObject({
        complete: true,
        rows: [{ attempt_id: binaryLast }],
      })
    } finally {
      database.database.close()
    }
  })

  it("rejects raw transitions hidden by a missing or wrong-environment audit row", async () => {
    for (const mutation of [
      `DELETE FROM "nozzle_audit_log" WHERE "sequence" = 4`,
      `UPDATE "nozzle_audit_log" SET "environment_id" = 'different' WHERE "sequence" = 4`,
    ]) {
      const database = sqliteHistoryDatabase()
      try {
        database.database.exec(mutation)
        await expect(
          new D1SagaHistoryReader(database).captureAnchor("operation-a", "saga-a"),
        ).rejects.toThrow(/transition summary is malformed/u)
      } finally {
        database.database.close()
      }
    }
  })

  it("captures one stable terminal history anchor without reading unbounded rows", async () => {
    const database = new ScriptedDatabase(captureScript())
    const captured = await new D1SagaHistoryReader(database).captureAnchor("operation-a", "saga-a")

    expect(captured).toEqual(anchor)
    expect(Object.isFrozen(captured)).toBe(true)
    expect(database.calls).toHaveLength(10)
    expect(database.calls.every((call) => !call.sql.includes("OFFSET"))).toBe(true)
    database.expectComplete()

    const noAttempts = new ScriptedDatabase(captureScript({ attempts: null }))
    await expect(
      new D1SagaHistoryReader(noAttempts).captureAnchor("operation-a", "saga-a"),
    ).resolves.toMatchObject({
      sagaAttemptCount: 0,
      sagaAttemptLastAcceptedAtMs: null,
      sagaAttemptLastId: null,
    })
  })

  it("distinguishes a concurrent operation tail from a changed terminal saga", async () => {
    const operationChanged = new ScriptedDatabase(
      captureScript({
        finalIdentity: { ...identity, operation_updated_at_ms: 13 },
        finalTransitions: { ...transitionSummary, history_count: 4, joined_history_count: 4 },
      }),
    )
    await expect(
      new D1SagaHistoryReader(operationChanged).captureAnchor("operation-a", "saga-a"),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })

    const sagaChanged = new ScriptedDatabase(
      captureScript({
        finalEffects: { ...effectSummary, last_effect_id: "effect-3", last_state_version: 3 },
        finalIdentity: {
          ...identity,
          saga_last_effect_id: "effect-3",
          saga_record_checksum: "saga-record-3",
          saga_state_version: 3,
        },
      }),
    )
    await expect(
      new D1SagaHistoryReader(sagaChanged).captureAnchor("operation-a", "saga-a"),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

    const auditChanged = new ScriptedDatabase(
      captureScript({ finalAudit: { event_hash: "changed", sequence: 9 } }),
    )
    await expect(
      new D1SagaHistoryReader(auditChanged).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/audit head changed/u)
  })

  it("rechecks terminal and mutable tails against the exact anchor", async () => {
    const current = new ScriptedDatabase([
      first('FROM "nozzle_operations" AS "operation"', identity),
      first('FROM "nozzle_operation_transitions" AS "transition"', transitionSummary),
      first('FROM "nozzle_operation_effects"', effectSummary),
      first('FROM "nozzle_saga_action_attempts"', attemptSummary),
      first('AND "sequence" = ?2', auditHead),
    ])
    await expect(
      new D1SagaHistoryReader(current).assertAnchorCurrent(anchor),
    ).resolves.toBeUndefined()

    const operationAdvanced = new ScriptedDatabase([
      first('FROM "nozzle_operations" AS "operation"', {
        ...identity,
        operation_status: "succeeded",
      }),
      first('FROM "nozzle_operation_transitions" AS "transition"', {
        ...transitionSummary,
        history_count: 4,
        joined_history_count: 4,
      }),
      first('FROM "nozzle_operation_effects"', effectSummary),
      first('FROM "nozzle_saga_action_attempts"', attemptSummary),
      first('AND "sequence" = ?2', auditHead),
    ])
    await expect(
      new D1SagaHistoryReader(operationAdvanced).assertAnchorCurrent(anchor),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })

    const sagaChanged = new ScriptedDatabase([
      first('FROM "nozzle_operations" AS "operation"', {
        ...identity,
        saga_record_checksum: "changed",
      }),
      first('FROM "nozzle_operation_transitions" AS "transition"', transitionSummary),
      first('FROM "nozzle_operation_effects"', effectSummary),
      first('FROM "nozzle_saga_action_attempts"', attemptSummary),
      first('AND "sequence" = ?2', auditHead),
    ])
    await expect(
      new D1SagaHistoryReader(sagaChanged).assertAnchorCurrent(anchor),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
  })

  it("keyset-pages a dense audit chain and rejects gaps", async () => {
    const database = new ScriptedDatabase([
      all('FROM "nozzle_audit_log"', [auditRow(1), auditRow(2), auditRow(3)]),
    ])
    const result = await new D1SagaHistoryReader(database).auditPage(anchor)
    expect(result).toEqual({ complete: false, nextCursor: 2, rows: [auditRow(1), auditRow(2)] })
    expect(Object.isFrozen(result)).toBe(true)
    expect(Object.isFrozen(result.rows)).toBe(true)
    expect(database.calls[0]?.sql).toContain("LIMIT 3")
    expect(database.calls[0]?.sql).not.toContain("OFFSET")

    const final = new ScriptedDatabase([all('FROM "nozzle_audit_log"', [auditRow(9)])])
    await expect(new D1SagaHistoryReader(final).auditPage(anchor, 8)).resolves.toEqual({
      complete: true,
      nextCursor: null,
      rows: [auditRow(9)],
    })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).auditPage(anchor, 9),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })

    const gap = new ScriptedDatabase([all('FROM "nozzle_audit_log"', [auditRow(2)])])
    await expect(new D1SagaHistoryReader(gap).auditPage(anchor)).rejects.toThrow(/incomplete/u)
  })

  it("keyset-pages transitions with absent, legacy, and full authorization receipts", async () => {
    const rows = [
      transitionRow(2),
      transitionRow(4, "transition-4", 1),
      transitionRow(7, "transition-7", 2),
    ]
    const database = new ScriptedDatabase([
      all('FROM "nozzle_operation_transitions" AS "transition"', rows),
    ])
    const result = await new D1SagaHistoryReader(database).transitionPage(anchor)
    expect(result.complete).toBe(false)
    expect(result.rows).toEqual(rows.slice(0, 2))
    expect(result.nextCursor).toEqual({ auditSequence: 4, transitionId: "transition-4" })
    expect(Object.isFrozen(result.nextCursor)).toBe(true)
    expect(database.calls[0]?.values).toEqual([
      "environment-a",
      "operation-a",
      0,
      "",
      7,
      "transition-7",
    ])

    const final = new ScriptedDatabase([
      all('FROM "nozzle_operation_transitions" AS "transition"', [
        transitionRow(7, "transition-7", 2),
      ]),
    ])
    await expect(
      new D1SagaHistoryReader(final).transitionPage(anchor, {
        auditSequence: 4,
        transitionId: "transition-4",
      }),
    ).resolves.toMatchObject({ complete: true, nextCursor: null })

    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).transitionPage(anchor, {
        auditSequence: 7,
        transitionId: "transition-7",
      }),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })
  })

  it("keyset-pages the dense saga effect chain", async () => {
    const rows = [effectRow(0), effectRow(1), effectRow(2)]
    const database = new ScriptedDatabase([all('FROM "nozzle_operation_effects"', rows)])
    const result = await new D1SagaHistoryReader(database).effectPage(anchor)
    expect(result).toEqual({ complete: false, nextCursor: 1, rows: rows.slice(0, 2) })

    const final = new ScriptedDatabase([all('FROM "nozzle_operation_effects"', [effectRow(2)])])
    await expect(new D1SagaHistoryReader(final).effectPage(anchor, 1)).resolves.toMatchObject({
      complete: true,
      nextCursor: null,
    })

    const gap = new ScriptedDatabase([all('FROM "nozzle_operation_effects"', [effectRow(1)])])
    await expect(new D1SagaHistoryReader(gap).effectPage(anchor)).rejects.toThrow(/incomplete/u)
    const unknown = new ScriptedDatabase([
      all('FROM "nozzle_operation_effects"', [{ ...effectRow(0), effect_kind: "unknown" }]),
    ])
    await expect(new D1SagaHistoryReader(unknown).effectPage(anchor)).rejects.toThrow(/malformed/u)
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).effectPage(anchor, 2),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })
  })

  it("keyset-pages split saga-attempt identities without joining outcomes", async () => {
    const rows = [
      attemptRow(1, "attempt-1"),
      attemptRow(2, "attempt-2"),
      attemptRow(3, "attempt-3"),
    ]
    const database = new ScriptedDatabase([
      all('FROM "nozzle_saga_action_attempts" AS "attempt"', rows),
    ])
    const result = await new D1SagaHistoryReader(database).attemptIdentityPage(anchor)
    expect(result.complete).toBe(false)
    expect(result.rows).toEqual(rows.slice(0, 2))
    expect(result.nextCursor).toEqual({ acceptedAtMs: 2, attemptId: "attempt-2" })
    expect(database.calls[0]?.sql).not.toContain("outcome")
    expect(database.calls[0]?.sql).not.toContain("OFFSET")

    const final = new ScriptedDatabase([
      all('FROM "nozzle_saga_action_attempts" AS "attempt"', [attemptRow(3, "attempt-3")]),
    ])
    await expect(
      new D1SagaHistoryReader(final).attemptIdentityPage(anchor, {
        acceptedAtMs: 2,
        attemptId: "attempt-2",
      }),
    ).resolves.toMatchObject({ complete: true, nextCursor: null })

    const emptyAnchor = {
      ...anchor,
      sagaAttemptCount: 0,
      sagaAttemptLastAcceptedAtMs: null,
      sagaAttemptLastId: null,
    }
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(emptyAnchor),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })
  })

  it("rejects malformed cursors, page metadata, rows, and byte overflow", async () => {
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).auditPage(anchor, 10),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).transitionPage(anchor, {
        auditSequence: 8,
        transitionId: "too-late",
      }),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).effectPage(anchor, 3),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(anchor, {
        acceptedAtMs: 4,
        attemptId: "too-late",
      }),
    ).rejects.toMatchObject({ code: "ConfigurationError" })

    const malformedMetadata = new ScriptedDatabase([
      {
        kind: "all",
        result: { meta: {}, results: {}, success: true },
        sql: 'FROM "nozzle_audit_log"',
      },
    ])
    await expect(new D1SagaHistoryReader(malformedMetadata).auditPage(anchor)).rejects.toThrow(
      /metadata or rows/u,
    )

    const unknownField = new ScriptedDatabase([
      all('FROM "nozzle_audit_log"', [{ ...auditRow(1), unchecked: true }]),
    ])
    await expect(new D1SagaHistoryReader(unknownField).auditPage(anchor)).rejects.toThrow(
      /metadata or rows/u,
    )

    const huge = JSON.stringify("x".repeat(2_400_000))
    const hugeRows = [0, 1, 2].map((index) => ({
      ...transitionRow(index + 2, `transition-${index + 2}`),
      audit_event_json: huge,
      from_record_json: huge,
      to_record_json: huge,
    }))
    const fetchOverflow = new ScriptedDatabase([
      all('FROM "nozzle_operation_transitions" AS "transition"', hugeRows),
    ])
    await expect(new D1SagaHistoryReader(fetchOverflow).transitionPage(anchor)).rejects.toThrow(
      /fetch budget/u,
    )
  })

  it("rejects invalid construction, identities, anchors, and absent history heads", async () => {
    expect(() => new D1SagaHistoryReader(null as never)).toThrow(/binding is required/u)
    expect(() => new D1SagaHistoryReader({ prepare() {} } as never)).toThrow(/binding is required/u)
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).captureAnchor("", "saga-a"),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([first('FROM "nozzle_operations"', null)]),
      ).captureAnchor("operation-a", "saga-a"),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })

    const noTransition = new ScriptedDatabase([
      first('FROM "nozzle_operations"', identity),
      first('FROM "nozzle_operation_transitions"', null),
    ])
    await expect(
      new D1SagaHistoryReader(noTransition).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/no operation transition/u)

    const malformedAnchor = { ...anchor, unchecked: true }
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).auditPage(malformedAnchor as never),
    ).rejects.toThrow(/anchor.*fields/u)
  })

  it("fails closed at every anchor and page decoding boundary", async () => {
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).auditPage(new Proxy(anchor, {})),
    ).rejects.toThrow(/could not be captured safely/u)
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([
          {
            kind: "all",
            result: () => new Proxy({ meta: {}, results: [auditRow(1)], success: true }, {}),
            sql: 'FROM "nozzle_audit_log"',
          },
        ]),
      ).auditPage(anchor),
    ).rejects.toThrow(/page could not be captured safely/u)
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([all('FROM "nozzle_audit_log"', [{ ...auditRow(1), sequence: 1n }])]),
      ).auditPage(anchor),
    ).rejects.toThrow(/page encoding is malformed/u)
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).captureAnchor("x".repeat(2049), "saga-a"),
    ).rejects.toThrow(/identity limit/u)

    const nonterminal = new ScriptedDatabase(
      captureScript({ identity: { ...identity, saga_status: "running" } }),
    )
    await expect(
      new D1SagaHistoryReader(nonterminal).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/nonterminal/u)
    const badTransitionSummary = new ScriptedDatabase(
      captureScript({ transitions: { ...transitionSummary, history_count: 0 } }),
    )
    await expect(
      new D1SagaHistoryReader(badTransitionSummary).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/transition summary is malformed/u)
    const hiddenTransition = new ScriptedDatabase(
      captureScript({ transitions: { ...transitionSummary, joined_history_count: 2 } }),
    )
    await expect(
      new D1SagaHistoryReader(hiddenTransition).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/transition summary is malformed/u)
    const badAttemptSummary = new ScriptedDatabase(
      captureScript({ attempts: { ...attemptSummary, last_attempt_id: "" } }),
    )
    await expect(
      new D1SagaHistoryReader(badAttemptSummary).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/attempt summary is malformed/u)
    const badAuditHead = new ScriptedDatabase(
      captureScript({ head: { event_hash: "", sequence: 9 } }),
    )
    await expect(
      new D1SagaHistoryReader(badAuditHead).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/audit head is malformed/u)
    const missingAuditHead = new ScriptedDatabase(captureScript({ head: () => null }))
    await expect(
      new D1SagaHistoryReader(missingAuditHead).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/no audit head/u)
    const missingEffect = new ScriptedDatabase(captureScript({ effects: () => null }))
    await expect(
      new D1SagaHistoryReader(missingEffect).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/no effect chain/u)
    const missingAnchoredAudit = new ScriptedDatabase(captureScript({ finalAudit: () => null }))
    await expect(
      new D1SagaHistoryReader(missingAnchoredAudit).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/audit event disappeared/u)
    const transitionBeyondAudit = new ScriptedDatabase(
      captureScript({ head: { event_hash: "audit-6", sequence: 6 } }),
    )
    await expect(
      new D1SagaHistoryReader(transitionBeyondAudit).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/transition head exceeds/u)
    const changedTerminalHead = new ScriptedDatabase(
      captureScript({ finalIdentity: { ...identity, saga_record_checksum: "changed" } }),
    )
    await expect(
      new D1SagaHistoryReader(changedTerminalHead).captureAnchor("operation-a", "saga-a"),
    ).rejects.toThrow(/terminal saga anchor changed/u)

    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).auditPage({
        ...anchor,
        schemaVersion: 2,
      } as never),
    ).rejects.toThrow(/anchor is malformed/u)
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).transitionPage(anchor, {
        auditSequence: 0,
        transitionId: "",
      }),
    ).rejects.toThrow(/transition cursor is malformed/u)
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(anchor, {
        acceptedAtMs: -1,
        attemptId: "",
      }),
    ).rejects.toThrow(/attempt cursor is malformed/u)
  })

  it("bounds returned pages separately from the bounded LIMIT-plus-one fetch", async () => {
    const huge = JSON.stringify("x".repeat(Math.ceil(SAGA_HISTORY_PAGE_MAX_BYTES / 6)))
    const rows = [2, 4].map((sequence) => ({
      ...transitionRow(sequence),
      audit_event_json: huge,
      from_record_json: huge,
      to_record_json: huge,
    }))
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([all('FROM "nozzle_operation_transitions" AS "transition"', rows)]),
      ).transitionPage({
        ...anchor,
        operationTransitionCount: 2,
        operationTransitionLastAuditSequence: 4,
        operationTransitionLastId: "transition-4",
      }),
    ).rejects.toThrow(/return budget/u)
  })

  it("rejects malformed transition and attempt rows while accepting equal-key tie breakers", async () => {
    const tiedTransitions = new ScriptedDatabase([
      all('FROM "nozzle_operation_transitions" AS "transition"', [
        transitionRow(2, "transition-a"),
        transitionRow(2, "transition-b"),
      ]),
    ])
    await expect(
      new D1SagaHistoryReader(tiedTransitions).transitionPage({
        ...anchor,
        operationTransitionCount: 2,
        operationTransitionLastAuditSequence: 2,
        operationTransitionLastId: "transition-b",
      }),
    ).resolves.toMatchObject({ complete: true })

    for (const row of [
      { ...transitionRow(2), operation_id: "different" },
      { ...transitionRow(2), from_record_json: "" },
      { ...transitionRow(2), from_record_json: "{" },
      {
        ...transitionRow(2, "transition-2", 1),
        authorization_id: "unexpected",
      },
      {
        ...transitionRow(2, "transition-2", 2),
        authorization_id: null,
      },
    ]) {
      const database = new ScriptedDatabase([
        all('FROM "nozzle_operation_transitions" AS "transition"', [row]),
      ])
      await expect(new D1SagaHistoryReader(database).transitionPage(anchor)).rejects.toThrow(
        /malformed or unordered/u,
      )
    }
    const duplicate = new ScriptedDatabase([
      all('FROM "nozzle_operation_transitions" AS "transition"', [
        transitionRow(2),
        transitionRow(2),
      ]),
    ])
    await expect(new D1SagaHistoryReader(duplicate).transitionPage(anchor)).rejects.toThrow(
      /malformed or unordered/u,
    )

    const observation = {
      ...attemptRow(2, "attempt-b"),
      causal_attempt_id: "attempt-a",
      phase: "compensation",
      purpose: "observation",
    }
    const tiedAttempts = new ScriptedDatabase([
      all('FROM "nozzle_saga_action_attempts" AS "attempt"', [
        attemptRow(2, "attempt-a"),
        observation,
      ]),
    ])
    await expect(
      new D1SagaHistoryReader(tiedAttempts).attemptIdentityPage({
        ...anchor,
        sagaAttemptCount: 2,
        sagaAttemptLastAcceptedAtMs: 2,
        sagaAttemptLastId: "attempt-b",
      }),
    ).resolves.toMatchObject({ complete: true })
    const malformedAttempt = new ScriptedDatabase([
      all('FROM "nozzle_saga_action_attempts" AS "attempt"', [
        { ...attemptRow(1, "attempt-1"), operation_id: "different" },
      ]),
    ])
    await expect(
      new D1SagaHistoryReader(malformedAttempt).attemptIdentityPage(anchor),
    ).rejects.toThrow(/malformed or unordered/u)
  })

  it("covers empty-attempt and exact-high-water continuation paths", async () => {
    const noAttemptAnchor = {
      ...anchor,
      sagaAttemptCount: 0,
      sagaAttemptLastAcceptedAtMs: null,
      sagaAttemptLastId: null,
    }
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(noAttemptAnchor, {
        acceptedAtMs: 0,
        attemptId: "unused",
      }),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(anchor, {
        acceptedAtMs: 3,
        attemptId: "attempt-3",
      }),
    ).resolves.toEqual({ complete: true, nextCursor: null, rows: [] })
    await expect(
      new D1SagaHistoryReader(new ScriptedDatabase([])).attemptIdentityPage(anchor, {
        acceptedAtMs: 3,
        attemptId: "z",
      }),
    ).rejects.toThrow(/exceeds its anchor/u)

    const currentNoAttempts = new ScriptedDatabase([
      first('FROM "nozzle_operations" AS "operation"', identity),
      first('FROM "nozzle_operation_transitions" AS "transition"', transitionSummary),
      first('FROM "nozzle_operation_effects"', effectSummary),
      first('FROM "nozzle_saga_action_attempts"', null),
      first('AND "sequence" = ?2', auditHead),
    ])
    await expect(
      new D1SagaHistoryReader(currentNoAttempts).assertAnchorCurrent(noAttemptAnchor),
    ).resolves.toBeUndefined()
  })

  it("rejects a valid short page that ends before any anchored high-water mark", async () => {
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([all('FROM "nozzle_audit_log"', [auditRow(1)])]),
      ).auditPage(anchor),
    ).rejects.toThrow(/ended before/u)
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([
          all('FROM "nozzle_operation_transitions" AS "transition"', [transitionRow(2)]),
        ]),
      ).transitionPage(anchor),
    ).rejects.toThrow(/ended before/u)
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([all('FROM "nozzle_operation_effects"', [effectRow(0)])]),
      ).effectPage(anchor),
    ).rejects.toThrow(/ended before/u)
    await expect(
      new D1SagaHistoryReader(
        new ScriptedDatabase([
          all('FROM "nozzle_saga_action_attempts" AS "attempt"', [attemptRow(1, "attempt-1")]),
        ]),
      ).attemptIdentityPage(anchor),
    ).rejects.toThrow(/ended before/u)
  })
})
