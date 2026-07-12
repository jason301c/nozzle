import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  appendAuditEvent,
  type DigestFunction,
  leaseProof,
  type OperationPlan,
  type OperationPlanInput,
  sealIrreversibleAuthorization,
  sealOperationPlan,
  verifyAuditChain,
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
import { createInternalSagaOperationStore, D1OperationStore } from "../src/operation-store.js"
import { controlSchemaSql } from "../src/schema.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
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
  #batchTail: Promise<unknown> = Promise.resolve()

  constructor() {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(controlSchemaSql())
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const execute = async (): Promise<readonly ControlRunResult[]> => {
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
    const result = this.#batchTail.then(execute, execute)
    this.#batchTail = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

interface ScriptedState {
  readonly batch?: (
    statements: readonly ControlStatement[],
  ) => Promise<readonly ControlRunResult[]> | readonly ControlRunResult[]
  readonly first: (sql: string) => unknown
  readonly stepResult?: ControlQueryResult<unknown>
}

class ScriptedStatement implements ControlStatement {
  readonly #sql: string
  readonly #state: ScriptedState

  constructor(sql: string, state: ScriptedState) {
    this.#sql = sql
    this.#state = state
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return (this.#state.stepResult ?? {
      meta: {},
      results: [],
      success: true,
    }) as ControlQueryResult<T>
  }

  async first<T>(): Promise<T | null> {
    return this.#state.first(this.#sql) as T | null
  }

  async run(): Promise<ControlRunResult> {
    return { meta: { changes: 0 }, success: true }
  }
}

class ScriptedDatabase implements TransactionalControlDatabase {
  readonly #state: ScriptedState

  constructor(state: ScriptedState) {
    this.#state = state
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#state.batch?.(statements) ?? []
  }

  prepare(sql: string): ControlStatement {
    return new ScriptedStatement(sql, this.#state)
  }
}

interface TransitionFaults {
  readonly auditEventRow?: unknown
  readonly authorizationReceiptRow?: unknown
  readonly dropTransitionBatch?: boolean
  readonly hideAuditAfterBatch?: boolean
  readonly hideCurrentAfterBatch?: boolean
  readonly hideOperationAfterBatch?: boolean
  readonly providerAttemptRow?: unknown
  readonly sagaAttemptRow?: unknown
  readonly transitionRow?: unknown
  readonly throwAfterTransitionBatch?: boolean
  readonly throwAtTransitionStatement?: number
  readonly throwTransitionBatch?: boolean
}

class ThrowingRunStatement implements ControlStatement {
  readonly #delegate: ControlStatement

  constructor(delegate: ControlStatement) {
    this.#delegate = delegate
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#delegate.bind(...values)
    return this
  }

  all<T>(): Promise<ControlQueryResult<T>> {
    return this.#delegate.all<T>()
  }

  first<T>(): Promise<T | null> {
    return this.#delegate.first<T>()
  }

  async run(): Promise<ControlRunResult> {
    throw new Error("injected transition statement failure")
  }
}

class FaultInjectingDatabase implements TransactionalControlDatabase {
  readonly #delegate: TransactionalControlDatabase
  readonly #faults: TransitionFaults
  #transitionBatchRan = false

  constructor(delegate: TransactionalControlDatabase, faults: TransitionFaults) {
    this.#delegate = delegate
    this.#faults = faults
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (statements.length !== 4) return this.#delegate.batch(statements)
    this.#transitionBatchRan = true
    if (this.#faults.throwTransitionBatch) throw new Error("injected transition batch failure")
    if (this.#faults.dropTransitionBatch) return batchResults(4, 0)
    const executed =
      this.#faults.throwAtTransitionStatement === undefined
        ? statements
        : statements.map((statement, index) =>
            index === this.#faults.throwAtTransitionStatement
              ? new ThrowingRunStatement(statement)
              : statement,
          )
    const results = await this.#delegate.batch(executed)
    if (this.#faults.throwAfterTransitionBatch) {
      throw new Error("injected post-commit response loss")
    }
    return results
  }

  prepare(sql: string): ControlStatement {
    if (
      sql.includes('SELECT "event_json" FROM "nozzle_audit_log"') &&
      sql.includes('AND "event_hash" = ?2') &&
      Object.hasOwn(this.#faults, "auditEventRow")
    ) {
      return new ScriptedStatement(sql, { first: () => this.#faults.auditEventRow })
    }
    if (
      sql.includes('FROM "nozzle_irreversible_authorization_receipts"') &&
      Object.hasOwn(this.#faults, "authorizationReceiptRow")
    ) {
      return new ScriptedStatement(sql, { first: () => this.#faults.authorizationReceiptRow })
    }
    if (
      sql.includes('FROM "nozzle_provider_attempts" AS "attempt"') &&
      Object.hasOwn(this.#faults, "providerAttemptRow")
    ) {
      return new ScriptedStatement(sql, { first: () => this.#faults.providerAttemptRow })
    }
    if (
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"') &&
      Object.hasOwn(this.#faults, "sagaAttemptRow")
    ) {
      return new ScriptedStatement(sql, { first: () => this.#faults.sagaAttemptRow })
    }
    if (
      sql.includes('FROM "nozzle_operation_transitions" WHERE "transition_id"') &&
      Object.hasOwn(this.#faults, "transitionRow")
    ) {
      return new ScriptedStatement(sql, { first: () => this.#faults.transitionRow })
    }
    if (
      this.#transitionBatchRan &&
      this.#faults.hideOperationAfterBatch &&
      sql.includes('FROM "nozzle_operations" WHERE "operation_id"')
    ) {
      return new ScriptedStatement(sql, { first: () => null })
    }
    if (
      this.#transitionBatchRan &&
      this.#faults.hideCurrentAfterBatch &&
      sql.includes('JOIN "nozzle_operations" USING ("operation_id")')
    ) {
      return new ScriptedStatement(sql, { first: () => null })
    }
    if (
      this.#transitionBatchRan &&
      this.#faults.hideAuditAfterBatch &&
      sql.includes('SELECT "event_json" FROM "nozzle_audit_log"')
    ) {
      return new ScriptedStatement(sql, { first: () => null })
    }
    return this.#delegate.prepare(sql)
  }
}

function planInput(
  input: {
    readonly idempotencyKey?: string
    readonly inputChecksum?: string
    readonly operationId?: string
  } = {},
): OperationPlanInput {
  return {
    capabilitySnapshotChecksum: "capabilities-v1",
    idempotencyKey: input.idempotencyKey ?? "operation-key",
    inputChecksum: input.inputChecksum ?? "operation-input",
    operationId: input.operationId ?? "operation-1",
    operationType: "test-operation",
    steps: [
      {
        checkpoint: "reversible",
        dependsOn: [],
        idempotencyKey: "step-a-key",
        inputChecksum: "step-a-input",
        leaseKey: "fleet-a:operation",
        postconditionChecksum: "step-a-post",
        preconditionChecksum: "step-a-pre",
        recoveryInstructions: "Inspect step A.",
        retryClassification: "idempotent",
        stepId: "a",
      },
      {
        checkpoint: "reversible",
        dependsOn: ["a"],
        idempotencyKey: "step-b-key",
        inputChecksum: "step-b-input",
        leaseKey: "fleet-a:operation",
        postconditionChecksum: "step-b-post",
        preconditionChecksum: "step-b-pre",
        recoveryInstructions: "Inspect step B.",
        retryClassification: "reconcile_first",
        stepId: "b",
      },
    ],
  }
}

async function sealedPlan(input: Parameters<typeof planInput>[0] = {}) {
  return sealTestPlan(planInput(input))
}

async function sealedIrreversiblePlan(operationId: string): Promise<OperationPlan> {
  const input = planInput({
    idempotencyKey: `${operationId}-key`,
    inputChecksum: `${operationId}-input`,
    operationId,
  })
  const firstStep = input.steps[0]
  if (!firstStep) throw new Error("Fixture step is missing.")
  return sealTestPlan({
    ...input,
    steps: [{ ...firstStep, checkpoint: "irreversible" }],
  })
}

function canonicalRecordJson(value: unknown): string {
  const canonical = (candidate: unknown): unknown => {
    if (Array.isArray(candidate)) return candidate.map(canonical)
    if (typeof candidate === "object" && candidate !== null) {
      const output: Record<string, unknown> = {}
      for (const key of Object.keys(candidate).sort()) {
        output[key] = canonical((candidate as Record<string, unknown>)[key])
      }
      return output
    }
    return candidate
  }
  return JSON.stringify(canonical(value))
}

const payloads = new WeakMap<
  OperationPlan,
  { readonly capabilitySnapshotJson: string; readonly inputJson: string }
>()

async function checksumText(value: string): Promise<string> {
  return digest(new TextEncoder().encode(value)) as Promise<string>
}

async function sealTestPlan(
  input: OperationPlanInput,
  custom: { readonly capabilitySnapshotJson?: string; readonly inputJson?: string } = {},
): Promise<OperationPlan> {
  const inputJson = custom.inputJson ?? JSON.stringify({ input: input.inputChecksum })
  const capabilitySnapshotJson =
    custom.capabilitySnapshotJson ??
    JSON.stringify({ capabilitySnapshot: input.capabilitySnapshotChecksum })
  const plan = await sealOperationPlan(
    {
      ...input,
      capabilitySnapshotChecksum: await checksumText(capabilitySnapshotJson),
      inputChecksum: await checksumText(inputJson),
    },
    digest,
  )
  payloads.set(plan, { capabilitySnapshotJson, inputJson })
  return plan
}

function creationInput(
  plan: Awaited<ReturnType<typeof sealedPlan>>,
  overrides: Record<string, unknown> = {},
) {
  const payload = payloads.get(plan)
  if (!payload) throw new Error("Fixture operation payload is missing.")
  return {
    actorChecksum: "actor-checksum",
    capabilitySnapshotJson: payload.capabilitySnapshotJson,
    environmentId: "production",
    idempotencyScope: "fleet-a",
    inputJson: payload.inputJson,
    plan,
    requiredShardIds: ["shard-b", "shard-a"],
    ...overrides,
  }
}

function persistedRows(database: DatabaseSync, operationId: string) {
  const operationRow = database
    .prepare(`SELECT * FROM "nozzle_operations" WHERE "operation_id" = ?`)
    .get(operationId) as Record<string, unknown>
  const stepRows = database
    .prepare(
      `SELECT "operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
              "record_json", "state", "fencing_token", "updated_at_ms"
       FROM "nozzle_operation_steps" WHERE "operation_id" = ? ORDER BY "step_id"`,
    )
    .all(operationId) as Record<string, unknown>[]
  return { operationRow, stepRows }
}

function firstRouter(input: {
  readonly auditRow?: unknown
  readonly idempotency?: () => unknown
  readonly operation?: () => unknown
}) {
  return (sql: string): unknown => {
    if (sql.includes('FROM "nozzle_idempotency_keys"')) return input.idempotency?.() ?? null
    if (sql.includes('FROM "nozzle_operations" WHERE "operation_id"')) {
      return input.operation?.() ?? null
    }
    if (sql.includes('AS "now_ms"')) {
      return Object.hasOwn(input, "auditRow") ? input.auditRow : { event_json: null, now_ms: 100 }
    }
    throw new Error(`Unexpected scripted SQL: ${sql}`)
  }
}

function batchResults(length: number, changes = 1): readonly ControlRunResult[] {
  return Array.from({ length }, () => ({ meta: { changes }, success: true }))
}

function transitionIdentity(kind: string, parts: readonly string[]): string {
  let identity = `nozzle.operation-transition.v1:${kind}`
  for (const part of parts) identity += `:${part.length}:${part}`
  return identity
}

describe("D1OperationStore", () => {
  let database: DatabaseAdapter
  let store: D1OperationStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    store = new D1OperationStore(database, digest)
    return () => database.close()
  })

  it("atomically creates, integrity-loads, and exactly replays an immutable operation", async () => {
    const plan = await sealedPlan()
    const created = await store.create(creationInput(plan))
    expect(created).toMatchObject({
      created: true,
      environmentId: "production",
      idempotencyScope: "fleet-a",
      operation: { plan, steps: { a: { state: "pending" }, b: { state: "pending" } } },
      requiredShardIds: ["shard-a", "shard-b"],
    })
    expect(Object.isFrozen(created)).toBe(true)
    expect(Object.isFrozen(created.requiredShardIds)).toBe(true)
    await expect(store.get(plan.operationId)).resolves.toEqual({
      capabilitySnapshotJson: created.capabilitySnapshotJson,
      environmentId: created.environmentId,
      idempotencyScope: created.idempotencyScope,
      inputJson: created.inputJson,
      operation: created.operation,
      requiredShardIds: created.requiredShardIds,
    })
    await expect(store.get("missing-operation")).resolves.toBeUndefined()

    const replay = await store.create(creationInput(plan))
    expect(replay.created).toBe(false)
    expect(replay.operation).toEqual(created.operation)
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_audit_log"`).get(),
    ).toEqual({ count: 1 })
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_idempotency_keys"`).get(),
    ).toEqual({ count: 1 })
  })

  it("reserves every saga-shaped operation creation for the internal controller boundary", async () => {
    const base = planInput()
    const first = base.steps[0]
    const second = base.steps[1]
    if (!first || !second) throw new Error("Fixture steps are missing.")
    const variants: readonly OperationPlanInput[] = [
      {
        ...base,
        idempotencyKey: "reserved-type-key",
        operationId: "reserved-type-operation",
        operationType: "saga:reserved@1",
      },
      {
        ...base,
        idempotencyKey: "reserved-step-key",
        operationId: "reserved-step-operation",
        steps: [first, { ...second, stepId: "saga:forward:a" }],
      },
      {
        ...base,
        idempotencyKey: "reserved-protocol-key",
        operationId: "reserved-protocol-operation",
        steps: [{ ...first, effectProtocol: "saga_receipt" }, second],
      },
    ]

    for (const variant of variants) {
      const plan = await sealTestPlan(variant)
      await expect(store.create(creationInput(plan))).rejects.toThrow(
        /reserved for the internal controller boundary/u,
      )
    }
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operations"`).get(),
    ).toEqual({ count: 0 })

    const authorizedPlan = await sealTestPlan({
      ...base,
      idempotencyKey: "authorized-saga-key",
      operationId: "authorized-saga-operation",
      operationType: "saga:authorized@1",
    })
    const authorized = createInternalSagaOperationStore(database, digest)
    await expect(authorized.create(creationInput(authorizedPlan))).resolves.toMatchObject({
      created: true,
      operation: { plan: { operationId: "authorized-saga-operation" } },
    })
  })

  it("persists canonical reconstructible input and capability snapshots", async () => {
    const inputJson = '{"items":[3,{"a":1,"z":2}],"mode":"provision"}'
    const capabilitySnapshotJson = '{"features":{"d1":true},"revision":1}'
    const plan = await sealTestPlan(planInput({ operationId: "payload-operation" }), {
      capabilitySnapshotJson,
      inputJson,
    })
    const created = await store.create(
      creationInput(plan, {
        capabilitySnapshotJson: ' { "revision": 1, "features": { "d1": true } } ',
        inputJson: ' { "mode": "provision", "items": [3, { "z": 2, "a": 1 }] } ',
      }),
    )
    expect(created).toMatchObject({ capabilitySnapshotJson, inputJson })
    expect(
      database.database
        .prepare(
          `SELECT "input_json", "capability_snapshot_json" FROM "nozzle_operations"
           WHERE "operation_id" = ?`,
        )
        .get(plan.operationId),
    ).toEqual({ capability_snapshot_json: capabilitySnapshotJson, input_json: inputJson })
  })

  it("rejects invalid, oversized, mismatched, or unhashable operation payloads", async () => {
    const plan = await sealedPlan({ operationId: "invalid-payload-operation" })
    for (const inputJson of [null, "", "{", JSON.stringify("x".repeat(1024 * 1024 + 1))]) {
      await expect(store.create(creationInput(plan, { inputJson }))).rejects.toThrow(
        /JSON text|valid JSON|one MiB/u,
      )
    }
    await expect(store.create(creationInput(plan, { inputJson: "{}" }))).rejects.toThrow(
      /payload checksums do not match/u,
    )
    const invalidDigest = new D1OperationStore(database, async () => "")
    await expect(invalidDigest.create(creationInput(plan))).rejects.toThrow(
      /checksum is malformed/u,
    )
  })

  it("fails closed when persisted operation payloads lose canonical integrity", async () => {
    const plan = await sealedPlan({ operationId: "corrupt-payload-operation" })
    await store.create(creationInput(plan))
    database.database.exec('DROP TRIGGER "nozzle_control_operation_plan_update";')
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "input_json" = ? WHERE "operation_id" = ?`)
      .run('{ "input": "operation-input" }', plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/not canonical/u)

    database.database.exec("PRAGMA ignore_check_constraints = ON;")
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "input_json" = ? WHERE "operation_id" = ?`)
      .run("not-json", plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/not valid JSON/u)
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "input_json" = ? WHERE "operation_id" = ?`)
      .run(JSON.stringify("x".repeat(1024 * 1024 + 1)), plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/one MiB/u)
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "input_json" = '{}' WHERE "operation_id" = ?`)
      .run(plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/contradict the verified/u)

    const emptyJsonDigest: DigestFunction = async (value) => {
      if (value[0] === 0x7b) return ""
      return digest(value)
    }
    const invalidDigest = new D1OperationStore(database, emptyJsonDigest)
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "input_json" = ? WHERE "operation_id" = ?`)
      .run('{"input":"operation-input"}', plan.operationId)
    await expect(invalidDigest.get(plan.operationId)).rejects.toThrow(/checksum is malformed/u)
  })

  it("serializes concurrent creations into one valid per-environment audit chain", async () => {
    const plans = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        sealedPlan({
          idempotencyKey: `operation-key-${index}`,
          inputChecksum: `operation-input-${index}`,
          operationId: `operation-${index}`,
        }),
      ),
    )
    const results = await Promise.all(plans.map((plan) => store.create(creationInput(plan))))
    expect(results.every((result) => result.created)).toBe(true)
    const events = database.database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = 'production' ORDER BY "sequence"`,
      )
      .all()
      .map((row) => JSON.parse((row as { event_json: string }).event_json))
    expect(events).toHaveLength(8)
    await expect(verifyAuditChain(events, digest)).resolves.toBe(true)
  })

  it("scopes idempotency by environment and explicit operation scope", async () => {
    const first = await sealedPlan({ operationId: "operation-first" })
    const second = await sealedPlan({ operationId: "operation-second" })
    const third = await sealedPlan({ operationId: "operation-third" })
    await expect(store.create(creationInput(first))).resolves.toMatchObject({ created: true })
    await expect(
      store.create(creationInput(second, { idempotencyScope: "fleet-b" })),
    ).resolves.toMatchObject({ created: true })
    await expect(
      store.create(creationInput(third, { environmentId: "staging" })),
    ).resolves.toMatchObject({ created: true })
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operations"`).get(),
    ).toEqual({ count: 3 })
  })

  it("rejects operation-ID, idempotency, target, membership, and input conflicts", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    await expect(
      store.create(creationInput(await sealedPlan({ inputChecksum: "different" }))),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
    await expect(
      store.create(
        creationInput(
          await sealedPlan({
            inputChecksum: "different",
            operationId: "different-operation",
          }),
        ),
      ),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
    await expect(
      store.create(creationInput(plan, { environmentId: "staging" })),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
    await expect(
      store.create(creationInput(plan, { requiredShardIds: ["other"] })),
    ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
  })

  it("rejects invalid creation input before writing", async () => {
    const plan = await sealedPlan()
    for (const input of [
      creationInput(plan, { environmentId: "" }),
      creationInput(plan, { idempotencyScope: "" }),
      creationInput(plan, { actorChecksum: "" }),
      creationInput(plan, { requiredShardIds: null }),
      creationInput(plan, { requiredShardIds: [""] }),
      creationInput(plan, { requiredShardIds: ["duplicate", "duplicate"] }),
    ]) {
      await expect(store.create(input as never)).rejects.toMatchObject({
        code: "ConfigurationError",
      })
    }
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operations"`).get(),
    ).toEqual({ count: 0 })
    await expect(store.get("")).rejects.toMatchObject({ code: "ConfigurationError" })
  })

  it("detects mutable-column corruption instead of trusting denormalized state", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    expect(() =>
      database.database
        .prepare(`UPDATE "nozzle_operations" SET "status" = 'running' WHERE "operation_id" = ?`)
        .run(plan.operationId),
    ).toThrow(/TRANSITION_REQUIRED/u)
    database.database.exec(`DROP TRIGGER "nozzle_control_operation_status_update";`)
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "status" = 'running' WHERE "operation_id" = ?`)
      .run(plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/columns contradict/u)

    database.database
      .prepare(`UPDATE "nozzle_operations" SET "status" = 'planned' WHERE "operation_id" = ?`)
      .run(plan.operationId)
    expect(() =>
      database.database
        .prepare(
          `UPDATE "nozzle_operation_steps" SET "state" = 'running'
           WHERE "operation_id" = ? AND "step_id" = 'a'`,
        )
        .run(plan.operationId),
    ).toThrow(/STEP_TRANSITION_REQUIRED/u)
    database.database.exec(`DROP TRIGGER "nozzle_control_step_state_update";`)
    database.database
      .prepare(
        `UPDATE "nozzle_operation_steps" SET "state" = 'running'
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .run(plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/step columns contradict/u)
  })

  it("requires transactional database and digest capabilities", () => {
    expect(() => new D1OperationStore(null as never, digest)).toThrow(/transactional/u)
    expect(
      () =>
        new D1OperationStore(
          { batch: 1, prepare: database.prepare.bind(database) } as never,
          digest,
        ),
    ).toThrow(/transactional/u)
    expect(() => new D1OperationStore(database, null as never)).toThrow(/digest/u)
  })

  it("fails closed on malformed persisted JSON, memberships, rows, and query results", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    const valid = persistedRows(database.database, plan.operationId)
    const validSteps = { meta: {}, results: valid.stepRows, success: true }
    const cases: ScriptedState[] = [
      {
        first: firstRouter({ operation: () => ({ ...valid.operationRow, plan_json: 1 }) }),
        stepResult: validSteps,
      },
      {
        first: firstRouter({ operation: () => ({ ...valid.operationRow, plan_json: "{" }) }),
        stepResult: validSteps,
      },
      {
        first: firstRouter({
          operation: () => ({ ...valid.operationRow, required_shards_json: "{}" }),
        }),
        stepResult: validSteps,
      },
      {
        first: firstRouter({
          operation: () => ({
            ...valid.operationRow,
            required_shards_json: JSON.stringify(["shard-b", "shard-a"]),
          }),
        }),
        stepResult: validSteps,
      },
      {
        first: firstRouter({
          operation: () => ({ ...valid.operationRow, updated_at_ms: -1 }),
        }),
        stepResult: validSteps,
      },
      {
        first: firstRouter({ operation: () => valid.operationRow }),
        stepResult: { meta: {}, results: valid.stepRows, success: false },
      },
      {
        first: firstRouter({ operation: () => valid.operationRow }),
        stepResult: {
          meta: {},
          results: [{ ...valid.stepRows[0], updated_at_ms: -1 }, valid.stepRows[1]],
          success: true,
        },
      },
    ]
    for (const state of cases) {
      const scripted = new D1OperationStore(new ScriptedDatabase(state), digest)
      await expect(scripted.get(plan.operationId)).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
    }
  })

  it("rejects malformed audit snapshots and idempotency rows", async () => {
    const plan = await sealedPlan()
    for (const auditRow of [
      null,
      { event_json: null, now_ms: -1 },
      { event_json: 1, now_ms: 100 },
      { event_json: "{", now_ms: 100 },
    ]) {
      const scripted = new D1OperationStore(
        new ScriptedDatabase({
          first: firstRouter({ auditRow, operation: () => null }),
        }),
        digest,
      )
      await expect(scripted.create(creationInput(plan))).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
    }

    const stagingEvent = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "actor",
        environmentId: "staging",
        eventType: "operation.created",
        fencingToken: null,
        idempotencyKey: "staging-event",
        operationId: "staging-operation",
        payloadChecksum: "payload",
        serverTimeMs: 100,
        stepId: null,
      },
      digest,
    )
    const wrongEnvironment = new D1OperationStore(
      new ScriptedDatabase({
        first: firstRouter({
          auditRow: { event_json: JSON.stringify(stagingEvent), now_ms: 100 },
          operation: () => null,
        }),
      }),
      digest,
    )
    await expect(wrongEnvironment.create(creationInput(plan))).rejects.toThrow(
      /different environment/u,
    )

    const malformedIdempotency = new D1OperationStore(
      new ScriptedDatabase({
        first: firstRouter({ idempotency: () => ({ input_checksum: "", operation_id: "x" }) }),
      }),
      digest,
    )
    await expect(malformedIdempotency.create(creationInput(plan))).rejects.toThrow(
      /idempotency state is malformed/u,
    )
  })

  it("handles idempotency and operation visibility races without creating a second logical operation", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    const valid = persistedRows(database.database, plan.operationId)
    const stepResult = { meta: {}, results: valid.stepRows, success: true }
    const binding = { input_checksum: plan.inputChecksum, operation_id: plan.operationId }

    const missingBoundOperation = new D1OperationStore(
      new ScriptedDatabase({
        first: firstRouter({ idempotency: () => binding, operation: () => null }),
      }),
      digest,
    )
    await expect(missingBoundOperation.create(creationInput(plan))).rejects.toThrow(
      /references a missing operation/u,
    )

    let idempotencyCalls = 0
    const lateBinding = new D1OperationStore(
      new ScriptedDatabase({
        first: firstRouter({
          idempotency: () => (idempotencyCalls++ === 0 ? null : binding),
          operation: () => valid.operationRow,
        }),
        stepResult,
      }),
      digest,
    )
    await expect(lateBinding.create(creationInput(plan))).resolves.toMatchObject({ created: false })

    const missingBinding = new D1OperationStore(
      new ScriptedDatabase({
        first: firstRouter({ idempotency: () => null, operation: () => valid.operationRow }),
        stepResult,
      }),
      digest,
    )
    await expect(missingBinding.create(creationInput(plan))).rejects.toThrow(
      /idempotency binding contradicts/u,
    )
  })

  it("validates every batch result and stops after a bounded invisible-write retry budget", async () => {
    const plan = await sealedPlan()
    for (const results of [
      [],
      [{ meta: { changes: 1 }, success: false }, ...batchResults(4)],
      [{ meta: { changes: "1" }, success: true }, ...batchResults(4)],
      [{ meta: { changes: 0.5 }, success: true }, ...batchResults(4)],
      [{ meta: { changes: -1 }, success: true }, ...batchResults(4)],
      [{ meta: { changes: 2 }, success: true }, ...batchResults(4)],
    ]) {
      const scripted = new D1OperationStore(
        new ScriptedDatabase({
          batch: () => results as readonly ControlRunResult[],
          first: firstRouter({ operation: () => null }),
        }),
        digest,
      )
      await expect(scripted.create(creationInput(plan))).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
    }

    const invisible = new D1OperationStore(
      new ScriptedDatabase({
        batch: () => batchResults(5, 0),
        first: firstRouter({ operation: () => null }),
      }),
      digest,
    )
    await expect(invisible.create(creationInput(plan))).rejects.toThrow(/retry budget/u)
  })

  it("returns a compatible winner after a batch race", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    const valid = persistedRows(database.database, plan.operationId)
    const binding = { input_checksum: plan.inputChecksum, operation_id: plan.operationId }
    let raced = false
    const scripted = new D1OperationStore(
      new ScriptedDatabase({
        batch: () => {
          raced = true
          throw new Error("audit sequence raced")
        },
        first: firstRouter({
          idempotency: () => (raced ? binding : null),
          operation: () => (raced ? valid.operationRow : null),
        }),
        stepResult: { meta: {}, results: valid.stepRows, success: true },
      }),
      digest,
    )
    await expect(scripted.create(creationInput(plan))).resolves.toMatchObject({ created: false })
  })

  it("persists accepted attempts and successful results with exact receipts and audit", async () => {
    const plan = await sealedPlan()
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)

    const acceptedA = await store.beginStep({
      actorChecksum: "actor",
      attemptId: "attempt-a",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    expect(acceptedA).toMatchObject({
      disposition: "execute",
      operation: { steps: { a: { state: "running" } } },
    })
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "attempt-a",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "in_progress" })
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "other-attempt",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "reconcile" })

    const completedA = await store.completeStep({
      actorChecksum: "actor",
      attemptId: "attempt-a",
      counters: { cost: { providerCalls: 1 }, progress: { resources: 1 } },
      observedPostconditionChecksum: "step-a-post",
      operationId: plan.operationId,
      proof,
      resultChecksum: "result-a",
      stepId: "a",
    })
    expect(completedA).toMatchObject({
      steps: { a: { state: "succeeded" }, b: { state: "pending" } },
    })
    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId: "attempt-a",
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof,
        resultChecksum: "result-a",
        stepId: "a",
      }),
    ).resolves.toEqual(completedA)

    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "attempt-b",
      idempotencyKey: "step-b-key",
      observedPreconditionChecksum: "step-b-pre",
      operationId: plan.operationId,
      proof,
      stepId: "b",
    })
    const completed = await store.completeStep({
      actorChecksum: "actor",
      attemptId: "attempt-b",
      observedPostconditionChecksum: "step-b-post",
      operationId: plan.operationId,
      proof,
      resultChecksum: "result-b",
      stepId: "b",
    })
    expect(completed.steps.b?.state).toBe("succeeded")
    expect(database.database.prepare(`SELECT "status" FROM "nozzle_operations"`).get()).toEqual({
      status: "succeeded",
    })
    expect(
      database.database
        .prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_transitions"`)
        .get(),
    ).toEqual({ count: 4 })
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_audit_log"`).get(),
    ).toEqual({ count: 5 })
  })

  it("atomically settles conditional steps as not required with exact evidence", async () => {
    const input = planInput({ operationId: "conditional-operation" })
    const plan = await sealTestPlan({
      ...input,
      steps: input.steps.map((candidate) =>
        candidate.stepId === "b" ? { ...candidate, activation: "conditional" } : candidate,
      ),
    })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "conditional-acquisition",
      holderId: "conditional-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Conditional fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await expect(
      store.markStepNotRequired({
        actorChecksum: "conditional-actor",
        decisionId: "missing-decision",
        evidenceChecksum: "missing-projection",
        operationId: "missing-conditional-operation",
        proof,
        stepId: "b",
      }),
    ).rejects.toThrow(/operation does not exist/u)
    const settled = await store.markStepNotRequired({
      actorChecksum: "conditional-actor",
      decisionId: "branch-decision",
      evidenceChecksum: "terminal-saga-projection",
      operationId: plan.operationId,
      proof,
      stepId: "b",
    })
    expect(settled.steps.b).toEqual({
      costCounters: {},
      progressCounters: {},
      reconciliationEvidenceChecksum: "terminal-saga-projection",
      startedAttempts: 0,
      state: "not_required",
    })
    await expect(
      store.markStepNotRequired({
        actorChecksum: "conditional-actor",
        decisionId: "branch-decision",
        evidenceChecksum: "terminal-saga-projection",
        operationId: plan.operationId,
        proof,
        stepId: "b",
      }),
    ).resolves.toEqual(settled)
    await expect(
      store.markStepNotRequired({
        actorChecksum: "conditional-actor",
        decisionId: "other-decision",
        evidenceChecksum: "contradictory-projection",
        operationId: plan.operationId,
        proof,
        stepId: "b",
      }),
    ).rejects.toThrow(/contradicts durable evidence/u)
    await expect(
      store.markStepNotRequired({
        actorChecksum: "conditional-actor",
        decisionId: "required-decision",
        evidenceChecksum: "invalid-decision",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).rejects.toThrow(/conditional operation step/u)

    const rows = persistedRows(database.database, plan.operationId)
    expect(rows.stepRows[1]).toMatchObject({ fencing_token: null, state: "not_required" })
    expect(
      database.database
        .prepare(
          `SELECT "event_json" FROM "nozzle_audit_log"
           WHERE "operation_id" = ? AND "step_id" = 'b'`,
        )
        .get(plan.operationId),
    ).toMatchObject({ event_json: expect.stringContaining('"eventType":"step.not_required"') })

    const boundedInput = planInput({
      idempotencyKey: "bounded-conditional-operation-key",
      operationId: "bounded-conditional-operation",
    })
    const boundedPlan = await sealTestPlan({
      ...boundedInput,
      steps: boundedInput.steps.map((candidate) =>
        candidate.stepId === "b" ? { ...candidate, activation: "conditional" } : candidate,
      ),
    })
    await store.create(creationInput(boundedPlan))
    const dropped = new D1OperationStore(
      new FaultInjectingDatabase(database, { dropTransitionBatch: true }),
      digest,
    )
    await expect(
      dropped.markStepNotRequired({
        actorChecksum: "conditional-actor",
        decisionId: "bounded-decision",
        evidenceChecksum: "bounded-projection",
        operationId: boundedPlan.operationId,
        proof,
        stepId: "b",
      }),
    ).rejects.toThrow(/bounded transition retry budget/u)
  })

  it("persists unknown outcomes and reconciles them under a newer active fence", async () => {
    const plan = await sealedPlan({ operationId: "unknown-operation" })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const first = await leases.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!first.acquired) throw new Error("Fixture lease acquisition failed.")
    const firstProof = leaseProof(first.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "attempt-unknown",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof: firstProof,
      stepId: "a",
    })
    const unknown = await store.failStep({
      actorChecksum: "actor",
      attemptId: "attempt-unknown",
      errorChecksum: "lost-response",
      operationId: plan.operationId,
      outcome: "unknown",
      proof: firstProof,
      stepId: "a",
    })
    expect(unknown.steps.a?.state).toBe("unknown")
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "blind-retry",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof: firstProof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "reconcile" })

    await leases.release({ proof: firstProof })
    const second = await leases.acquire({
      acquisitionId: "acquisition-b",
      holderId: "controller-b",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!second.acquired) throw new Error("Fixture lease reacquisition failed.")
    const secondProof = leaseProof(second.record)
    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId: "attempt-unknown",
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof: secondProof,
        resultChecksum: "untrusted-result",
        stepId: "a",
      }),
    ).rejects.toThrow(/active step attempt|fenced/u)
    const reconciled = await store.reconcileStep({
      actorChecksum: "actor",
      evidenceChecksum: "observed-applied",
      observedPostconditionChecksum: "step-a-post",
      operationId: plan.operationId,
      outcome: "applied",
      proof: secondProof,
      reconciliationId: "reconciliation-a",
      resultChecksum: "verified-result",
      stepId: "a",
    })
    expect(reconciled.steps.a).toMatchObject({
      fencingToken: firstProof.fencingToken,
      reconciliationEvidenceChecksum: "observed-applied",
      state: "succeeded",
    })
  })

  it("recovers a crashed running attempt under a newer fence before a safe retry", async () => {
    const input = planInput({ operationId: "crash-recovery-operation" })
    const firstStep = input.steps[0]
    if (!firstStep) throw new Error("Fixture step is missing.")
    const plan = await sealTestPlan({
      ...input,
      steps: [{ ...firstStep, retryClassification: "reconcile_first" }],
    })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const first = await leases.acquire({
      acquisitionId: "crashed-acquisition",
      holderId: "crashed-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!first.acquired) throw new Error("Fixture lease acquisition failed.")
    const firstProof = leaseProof(first.record)
    await expect(
      store.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: plan.operationId,
        proof: firstProof,
        recoveryId: "premature-recovery",
        stepId: "a",
      }),
    ).rejects.toThrow(/Only a running step/u)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "crashed-attempt",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof: firstProof,
      stepId: "a",
    })
    await expect(
      store.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: plan.operationId,
        proof: firstProof,
        recoveryId: "recovery-1",
        stepId: "a",
      }),
    ).rejects.toThrow(/strictly newer/u)

    await leases.release({ proof: firstProof })
    const second = await leases.acquire({
      acquisitionId: "recovery-acquisition",
      holderId: "recovery-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!second.acquired) throw new Error("Fixture lease reacquisition failed.")
    const secondProof = leaseProof(second.record)
    const recovered = await store.recoverRunningStep({
      actorChecksum: "recovery-actor",
      operationId: plan.operationId,
      proof: secondProof,
      recoveryId: "recovery-1",
      stepId: "a",
    })
    expect(recovered.steps.a).toMatchObject({
      fencingToken: firstProof.fencingToken,
      lastAttemptId: "crashed-attempt",
      state: "unknown",
    })
    await expect(
      store.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: plan.operationId,
        proof: secondProof,
        recoveryId: "recovery-1",
        stepId: "a",
      }),
    ).resolves.toEqual(recovered)
    await expect(
      store.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: plan.operationId,
        proof: firstProof,
        recoveryId: "recovery-1",
        stepId: "a",
      }),
    ).rejects.toThrow(/contradictory durable state/u)
    expect(
      database.database
        .prepare(
          `SELECT "fencing_token" FROM "nozzle_operation_transitions"
           WHERE "transition_id" LIKE '%crash-recovered%'`,
        )
        .get(),
    ).toEqual({ fencing_token: secondProof.fencingToken })
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "blind-retry",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof: secondProof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "reconcile" })
    const notApplied = await store.reconcileStep({
      actorChecksum: "actor",
      evidenceChecksum: "proven-absent",
      operationId: plan.operationId,
      outcome: "not_applied",
      proof: secondProof,
      reconciliationId: "reconciliation-not-applied",
      stepId: "a",
    })
    expect(notApplied.steps.a?.state).toBe("retryable_failed")
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "safe-retry",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof: secondProof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
  })

  it("bounds crash-recovery persistence races", async () => {
    const plan = await sealedPlan({ operationId: "crash-recovery-race" })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const first = await leases.acquire({
      acquisitionId: "race-crashed-acquisition",
      holderId: "race-crashed-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!first.acquired) throw new Error("Fixture lease acquisition failed.")
    const firstProof = leaseProof(first.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "race-crashed-attempt",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof: firstProof,
      stepId: "a",
    })
    await leases.release({ proof: firstProof })
    const second = await leases.acquire({
      acquisitionId: "race-recovery-acquisition",
      holderId: "race-recovery-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!second.acquired) throw new Error("Fixture lease reacquisition failed.")
    const dropped = new D1OperationStore(
      new FaultInjectingDatabase(database, { dropTransitionBatch: true }),
      digest,
    )
    await expect(
      dropped.recoverRunningStep({
        actorChecksum: "actor",
        operationId: plan.operationId,
        proof: leaseProof(second.record),
        recoveryId: "race-recovery",
        stepId: "a",
      }),
    ).rejects.toThrow(/bounded transition retry budget/u)
  })

  it("proves a provider step was not dispatched when no acceptance receipt exists", async () => {
    const input = planInput({ operationId: "undispatched-provider-operation" })
    const firstStep = input.steps[0]
    if (!firstStep) throw new Error("Fixture step is missing.")
    const plan = await sealTestPlan({
      ...input,
      steps: [{ ...firstStep, effectProtocol: "provider_receipt" }],
    })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const first = await leases.acquire({
      acquisitionId: "undispatched-acquisition",
      holderId: "undispatched-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!first.acquired) throw new Error("Fixture lease acquisition failed.")
    const firstProof = leaseProof(first.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "undispatched-attempt",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof: firstProof,
      stepId: "a",
    })
    await leases.release({ proof: firstProof })
    const second = await leases.acquire({
      acquisitionId: "undispatched-recovery-acquisition",
      holderId: "undispatched-recovery-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!second.acquired) throw new Error("Fixture lease reacquisition failed.")
    const secondProof = leaseProof(second.record)
    for (const [providerAttemptRow, message] of [
      [
        {
          acceptance_checksum: "accepted",
          attempt_id: "undispatched-attempt",
          operation_id: null,
          outcome_checksum: null,
          purpose: "effect",
          state: null,
          step_id: "a",
        },
        /recovery evidence is malformed/u,
      ],
      [
        {
          acceptance_checksum: "accepted",
          attempt_id: "undispatched-attempt",
          operation_id: "different-operation",
          outcome_checksum: null,
          purpose: "effect",
          state: null,
          step_id: "a",
        },
        /belongs to a different step/u,
      ],
    ] as const) {
      const faulted = new D1OperationStore(
        new FaultInjectingDatabase(database, { providerAttemptRow }),
        digest,
      )
      await expect(
        faulted.recoverRunningStep({
          actorChecksum: "recovery-actor",
          operationId: plan.operationId,
          proof: secondProof,
          recoveryId: "faulted-recovery",
          stepId: "a",
        }),
      ).rejects.toThrow(message)
    }
    const recovered = await store.recoverRunningStep({
      actorChecksum: "recovery-actor",
      operationId: plan.operationId,
      proof: secondProof,
      recoveryId: "undispatched-recovery",
      stepId: "a",
    })
    expect(recovered.steps.a).toMatchObject({
      fencingToken: firstProof.fencingToken,
      lastAttemptId: "undispatched-attempt",
      state: "retryable_failed",
    })
    await expect(
      store.beginStep({
        actorChecksum: "recovery-actor",
        attemptId: "first-physical-attempt",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof: secondProof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
    const audit = database.database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "operation_id" = ? ORDER BY "sequence" DESC LIMIT 2`,
      )
      .all(plan.operationId) as { event_json: string }[]
    expect(audit.map((row) => JSON.parse(row.event_json).eventType)).toContain(
      "step.crash.not_dispatched",
    )
  })

  it("recovers saga-receipt steps from exact dispatch presence or absence", async () => {
    const running = async (suffix: string) => {
      const input = planInput({
        idempotencyKey: `saga-recovery-${suffix}-key`,
        operationId: `saga-recovery-${suffix}`,
      })
      const firstStep = input.steps[0]
      if (!firstStep) throw new Error("Fixture step is missing.")
      const leaseKey = `fleet-a:saga-recovery:${suffix}`
      const plan = await sealTestPlan({
        ...input,
        steps: [{ ...firstStep, effectProtocol: "saga_receipt", leaseKey }],
      })
      await createInternalSagaOperationStore(database, digest).create(creationInput(plan))
      const leases = new D1LeaseStore(database)
      const first = await leases.acquire({
        acquisitionId: `saga-recovery-${suffix}-first`,
        holderId: `saga-recovery-${suffix}-controller`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!first.acquired) throw new Error("Fixture lease acquisition failed.")
      const firstProof = leaseProof(first.record)
      const attemptId = `saga-recovery-${suffix}-attempt`
      await store.beginStep({
        actorChecksum: "actor",
        attemptId,
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof: firstProof,
        stepId: "a",
      })
      await leases.release({ proof: firstProof })
      const second = await leases.acquire({
        acquisitionId: `saga-recovery-${suffix}-second`,
        holderId: `saga-recovery-${suffix}-recovery`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!second.acquired) throw new Error("Fixture lease reacquisition failed.")
      return { attemptId, plan, proof: leaseProof(second.record) }
    }

    const blocked = await running("blocked")
    await expect(
      store.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: blocked.plan.operationId,
        proof: blocked.proof,
        recoveryId: "saga-recovery-blocked",
        stepId: "a",
      }),
    ).rejects.toThrow(/must be consumed through D1SagaCoordinatorStore/u)
    await expect(store.get(blocked.plan.operationId)).resolves.toMatchObject({
      operation: {
        steps: {
          a: { activeAttemptId: blocked.attemptId, state: "running" },
        },
      },
    })
  })

  it("reserves every terminal saga-receipt mutation for the typed coordinator", async () => {
    const input = planInput({
      idempotencyKey: "saga-outcome-key",
      operationId: "saga-outcome-operation",
    })
    const firstStep = input.steps[0]
    if (!firstStep) throw new Error("Fixture step is missing.")
    const leaseKey = "fleet-a:saga-outcome"
    const plan = await sealTestPlan({
      ...input,
      steps: [{ ...firstStep, effectProtocol: "saga_receipt", leaseKey }],
    })
    await createInternalSagaOperationStore(database, digest).create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "saga-outcome-acquisition",
      holderId: "saga-outcome-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const attemptId = "saga-outcome-attempt"
    await store.beginStep({
      actorChecksum: "actor",
      attemptId,
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    const beforeTransitions = database.database
      .prepare(
        `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
         WHERE "operation_id" = ?`,
      )
      .get(plan.operationId)
    const coordinatorOnly = /must be consumed through D1SagaCoordinatorStore/u

    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId,
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof,
        resultChecksum: "result",
        stepId: "a",
      }),
    ).rejects.toThrow(coordinatorOnly)
    await expect(
      store.failStep({
        actorChecksum: "actor",
        attemptId,
        errorChecksum: "failed",
        operationId: plan.operationId,
        outcome: "permanent",
        proof,
        stepId: "a",
      }),
    ).rejects.toThrow(coordinatorOnly)
    await expect(
      store.reconcileStep({
        actorChecksum: "actor",
        evidenceChecksum: "observation-evidence",
        observationAttemptId: "saga-observation-attempt",
        operationId: plan.operationId,
        outcome: "applied",
        proof,
        reconciliationId: "saga-reconciliation",
        resultChecksum: "result",
        stepId: "a",
      }),
    ).rejects.toThrow(coordinatorOnly)

    await expect(store.get(plan.operationId)).resolves.toMatchObject({
      operation: { steps: { a: { activeAttemptId: attemptId, state: "running" } } },
    })
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?`,
        )
        .get(plan.operationId),
    ).toEqual(beforeTransitions)
  })

  it("persists retryable and permanent failures with their bounded next action", async () => {
    const plan = await sealedPlan({ operationId: "failure-operation" })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "attempt-1",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    const retryable = await store.failStep({
      actorChecksum: "actor",
      attemptId: "attempt-1",
      errorChecksum: "rate-limited",
      operationId: plan.operationId,
      outcome: "definitely_not_applied",
      proof,
      stepId: "a",
    })
    expect(retryable.steps.a?.state).toBe("retryable_failed")
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "attempt-2",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
    const failed = await store.failStep({
      actorChecksum: "actor",
      attemptId: "attempt-2",
      errorChecksum: "permission-denied",
      operationId: plan.operationId,
      outcome: "permanent",
      proof,
      stepId: "a",
    })
    expect(failed.steps.a?.state).toBe("failed")
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "attempt-3",
        idempotencyKey: "step-a-key",
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "blocked" })
  })

  it("persists the complete canonical authorization in both durable projections and reloads it frozen", async () => {
    const plan = await sealedIrreversiblePlan("irreversible-operation")
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "irreversible-acquisition",
      holderId: "irreversible-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-a",
        decisionChecksum: "approved",
        lease: acquired.record,
        leaseProof: proof,
        sealedAtServerTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    const accepted = await store.beginStep({
      actorChecksum: "actor",
      attemptId: "irreversible-attempt",
      idempotencyKey: "step-a-key",
      irreversibleAuthorization: authorization,
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    expect(accepted).toMatchObject({
      disposition: "execute",
      operation: {
        steps: {
          a: {
            authorizationChecksum: authorization.authorizationChecksum,
            irreversibleAuthorization: authorization,
            state: "running",
          },
        },
      },
    })

    const current = database.database
      .prepare(
        `SELECT "record_json" FROM "nozzle_operation_steps"
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .get(plan.operationId) as { readonly record_json: string }
    const transition = database.database
      .prepare(
        `SELECT "to_record_json" FROM "nozzle_operation_transitions"
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .get(plan.operationId) as { readonly to_record_json: string }
    expect(transition.to_record_json).toBe(current.record_json)
    expect(current.record_json).toBe(canonicalRecordJson(JSON.parse(current.record_json)))
    expect(JSON.parse(current.record_json)).toMatchObject({
      authorizationChecksum: authorization.authorizationChecksum,
      irreversibleAuthorization: authorization,
      state: "running",
    })

    const restarted = new D1OperationStore(database, digest)
    const loaded = await restarted.get(plan.operationId)
    const loadedStep = loaded?.operation.steps.a
    expect(loadedStep?.irreversibleAuthorization).toEqual(authorization)
    expect(loadedStep?.irreversibleAuthorization).not.toBe(authorization)
    expect(Object.isFrozen(loadedStep)).toBe(true)
    expect(Object.isFrozen(loadedStep?.irreversibleAuthorization)).toBe(true)

    database.database.exec('DROP TRIGGER "nozzle_control_step_state_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_operation_steps" SET "record_json" = ?
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .run(JSON.stringify(JSON.parse(current.record_json), null, 2), plan.operationId)
    await expect(restarted.get(plan.operationId)).rejects.toThrow(
      /step columns contradict the verified step record/u,
    )
  })

  it("loads legacy checksum-only authorization for diagnosis but rejects dispatch and mutation", async () => {
    const plan = await sealedIrreversiblePlan("legacy-authorization-operation")
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "legacy-authorization-acquisition",
      holderId: "legacy-authorization-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "legacy-actor",
        authorizationId: "legacy-authorization",
        decisionChecksum: "legacy-approved",
        lease: acquired.record,
        leaseProof: proof,
        sealedAtServerTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    await store.beginStep({
      actorChecksum: "legacy-actor",
      attemptId: "legacy-attempt",
      idempotencyKey: "step-a-key",
      irreversibleAuthorization: authorization,
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    const row = database.database
      .prepare(
        `SELECT "record_json" FROM "nozzle_operation_steps"
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .get(plan.operationId) as { readonly record_json: string }
    const legacy = JSON.parse(row.record_json) as Record<string, unknown>
    delete legacy.irreversibleAuthorization
    expect(legacy.authorizationChecksum).toBe(authorization.authorizationChecksum)
    database.database.exec('DROP TRIGGER "nozzle_control_step_state_update";')
    database.database
      .prepare(
        `UPDATE "nozzle_operation_steps" SET "record_json" = ?
         WHERE "operation_id" = ? AND "step_id" = 'a'`,
      )
      .run(canonicalRecordJson(legacy), plan.operationId)

    const restarted = new D1OperationStore(database, digest)
    const loaded = await restarted.get(plan.operationId)
    expect(loaded?.operation.steps.a).toMatchObject({
      authorizationChecksum: authorization.authorizationChecksum,
      state: "running",
    })
    expect(loaded?.operation.steps.a).not.toHaveProperty("irreversibleAuthorization")
    await expect(
      restarted.beginStep({
        actorChecksum: "legacy-actor",
        attemptId: "legacy-attempt",
        idempotencyKey: "step-a-key",
        irreversibleAuthorization: authorization,
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).rejects.toThrow(/authorization record is incomplete or contradictory/u)
    await expect(
      restarted.completeStep({
        actorChecksum: "legacy-actor",
        attemptId: "legacy-attempt",
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof,
        resultChecksum: "legacy-result",
        stepId: "a",
      }),
    ).rejects.toThrow(/authorization record is incomplete or contradictory/u)
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?`,
        )
        .get(plan.operationId),
    ).toEqual({ count: 1 })
  })

  it("fails closed when a committed irreversible transition loses its authorization receipt", async () => {
    const plan = await sealedIrreversiblePlan("missing-authorization-receipt")
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "missing-receipt-acquisition",
      holderId: "missing-receipt-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "missing-receipt-actor",
        authorizationId: "missing-receipt-authorization",
        decisionChecksum: "missing-receipt-approved",
        lease: acquired.record,
        leaseProof: proof,
        sealedAtServerTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    const faulted = new D1OperationStore(
      new FaultInjectingDatabase(database, { authorizationReceiptRow: null }),
      digest,
    )

    await expect(
      faulted.beginStep({
        actorChecksum: "missing-receipt-actor",
        attemptId: "missing-receipt-attempt",
        idempotencyKey: "step-a-key",
        irreversibleAuthorization: authorization,
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).rejects.toThrow(/lacks its exact authorization receipt/u)
    expect((await store.get(plan.operationId))?.operation.steps.a).toMatchObject({
      irreversibleAuthorization: authorization,
      state: "running",
    })
  })

  it("retains a distinct receipt for an authorized retry after proven non-application", async () => {
    const plan = await sealedIrreversiblePlan("irreversible-authorized-retry")
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "authorized-retry-acquisition",
      holderId: "authorized-retry-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const seal = (suffix: string) =>
      sealIrreversibleAuthorization(
        plan,
        {
          actorChecksum: "authorized-retry-actor",
          authorizationId: `authorized-retry-${suffix}`,
          decisionChecksum: `authorized-retry-${suffix}-approved`,
          lease: acquired.record,
          leaseProof: proof,
          sealedAtServerTimeMs: 1,
          stepId: "a",
        },
        digest,
      )
    const first = await seal("first")
    await store.beginStep({
      actorChecksum: "authorized-retry-actor",
      attemptId: "authorized-retry-attempt-1",
      idempotencyKey: "step-a-key",
      irreversibleAuthorization: first,
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    await store.failStep({
      actorChecksum: "authorized-retry-actor",
      attemptId: "authorized-retry-attempt-1",
      errorChecksum: "authorized-retry-not-applied",
      operationId: plan.operationId,
      outcome: "definitely_not_applied",
      proof,
      stepId: "a",
    })
    const second = await seal("second")
    await expect(
      store.beginStep({
        actorChecksum: "authorized-retry-actor",
        attemptId: "authorized-retry-attempt-2",
        idempotencyKey: "step-a-key",
        irreversibleAuthorization: second,
        observedPreconditionChecksum: "step-a-pre",
        operationId: plan.operationId,
        proof,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
    expect(
      database.database
        .prepare(
          `SELECT "authorization_id" FROM "nozzle_irreversible_authorization_receipts"
           WHERE "transition_id" IN (
             SELECT "transition_id" FROM "nozzle_operation_transitions"
             WHERE "operation_id" = ? AND "step_id" = 'a'
           ) ORDER BY "classified_at_ms", "transition_id"`,
        )
        .all(plan.operationId),
    ).toEqual([
      { authorization_id: first.authorizationId },
      { authorization_id: second.authorizationId },
    ])
  })

  it("rolls back the complete authorization receipt at every transition statement boundary", async () => {
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "authorization-rollback-acquisition",
      holderId: "authorization-rollback-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)

    for (let position = 0; position < 4; position += 1) {
      const operationId = `authorization-rollback-${position}`
      const plan = await sealedIrreversiblePlan(operationId)
      await store.create(creationInput(plan))
      const authorization = await sealIrreversibleAuthorization(
        plan,
        {
          actorChecksum: "rollback-actor",
          authorizationId: `rollback-authorization-${position}`,
          decisionChecksum: `rollback-approved-${position}`,
          lease: acquired.record,
          leaseProof: proof,
          sealedAtServerTimeMs: 1,
          stepId: "a",
        },
        digest,
      )
      const faulted = new D1OperationStore(
        new FaultInjectingDatabase(database, { throwAtTransitionStatement: position }),
        digest,
      )
      await expect(
        faulted.beginStep({
          actorChecksum: "rollback-actor",
          attemptId: `rollback-attempt-${position}`,
          idempotencyKey: "step-a-key",
          irreversibleAuthorization: authorization,
          observedPreconditionChecksum: "step-a-pre",
          operationId,
          proof,
          stepId: "a",
        }),
      ).rejects.toThrow(/bounded transition retry budget/u)

      const loaded = await store.get(operationId)
      expect(loaded).toMatchObject({
        operation: {
          steps: {
            a: {
              startedAttempts: 0,
              state: "pending",
            },
          },
        },
      })
      expect(loaded?.operation.steps.a).not.toHaveProperty("authorizationChecksum")
      expect(loaded?.operation.steps.a).not.toHaveProperty("irreversibleAuthorization")
      expect(
        database.database
          .prepare(
            `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
             WHERE "operation_id" = ?`,
          )
          .get(operationId),
      ).toEqual({ count: 0 })
      expect(
        database.database
          .prepare(
            `SELECT count(*) AS "count" FROM "nozzle_audit_log"
             WHERE "operation_id" = ?`,
          )
          .get(operationId),
      ).toEqual({ count: 1 })
      expect(
        database.database
          .prepare(`SELECT "status" FROM "nozzle_operations" WHERE "operation_id" = ?`)
          .get(operationId),
      ).toEqual({ status: "planned" })
    }
  })

  it("recovers an exact committed authorization after response loss and captures its accessor once", async () => {
    const plan = await sealedIrreversiblePlan("authorization-response-loss")
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "authorization-response-loss-acquisition",
      holderId: "authorization-response-loss-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "response-loss-actor",
        authorizationId: "response-loss-authorization",
        decisionChecksum: "response-loss-approved",
        lease: acquired.record,
        leaseProof: proof,
        sealedAtServerTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    let authorizationReads = 0
    const request = {
      actorChecksum: "response-loss-actor",
      attemptId: "response-loss-attempt",
      idempotencyKey: "step-a-key",
      get irreversibleAuthorization() {
        authorizationReads += 1
        return authorization
      },
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    }
    const faulted = new D1OperationStore(
      new FaultInjectingDatabase(database, { throwAfterTransitionBatch: true }),
      digest,
    )
    await expect(faulted.beginStep(request)).resolves.toMatchObject({ disposition: "execute" })
    expect(authorizationReads).toBe(1)
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?`,
        )
        .get(plan.operationId),
    ).toEqual({ count: 1 })
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_audit_log"
           WHERE "operation_id" = ?`,
        )
        .get(plan.operationId),
    ).toEqual({ count: 2 })
    await expect(new D1OperationStore(database, digest).beginStep(request)).resolves.toMatchObject({
      disposition: "in_progress",
    })
    expect(authorizationReads).toBe(2)
  })

  it("serializes exact authorization races and rejects a conflicting authorization race", async () => {
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "authorization-race-acquisition",
      holderId: "authorization-race-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)

    const exactPlan = await sealedIrreversiblePlan("exact-authorization-race")
    await store.create(creationInput(exactPlan))
    const exactAuthorization = await sealIrreversibleAuthorization(
      exactPlan,
      {
        actorChecksum: "race-actor",
        authorizationId: "exact-race-authorization",
        decisionChecksum: "exact-race-approved",
        lease: acquired.record,
        leaseProof: proof,
        sealedAtServerTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    const exactRequest = {
      actorChecksum: "race-actor",
      attemptId: "exact-race-attempt",
      idempotencyKey: "step-a-key",
      irreversibleAuthorization: exactAuthorization,
      observedPreconditionChecksum: "step-a-pre",
      operationId: exactPlan.operationId,
      proof,
      stepId: "a",
    }
    const exact = await Promise.all([
      new D1OperationStore(database, digest).beginStep(exactRequest),
      new D1OperationStore(database, digest).beginStep(exactRequest),
    ])
    expect(
      exact.every((decision) => ["execute", "in_progress"].includes(decision.disposition)),
    ).toBe(true)
    expect(exact.some((decision) => decision.disposition === "execute")).toBe(true)
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?`,
        )
        .get(exactPlan.operationId),
    ).toEqual({ count: 1 })

    const conflictingPlan = await sealedIrreversiblePlan("conflicting-authorization-race")
    await store.create(creationInput(conflictingPlan))
    const authorizationInputs = [
      { authorizationId: "conflicting-authorization-a", decisionChecksum: "approved-a" },
      { authorizationId: "conflicting-authorization-b", decisionChecksum: "approved-b" },
    ] as const
    const authorizations = await Promise.all(
      authorizationInputs.map((candidate) =>
        sealIrreversibleAuthorization(
          conflictingPlan,
          {
            actorChecksum: "race-actor",
            ...candidate,
            lease: acquired.record,
            leaseProof: proof,
            sealedAtServerTimeMs: 1,
            stepId: "a",
          },
          digest,
        ),
      ),
    )
    const conflicting = await Promise.allSettled(
      authorizations.map((irreversibleAuthorization) =>
        new D1OperationStore(database, digest).beginStep({
          actorChecksum: "race-actor",
          attemptId: "conflicting-race-attempt",
          idempotencyKey: "step-a-key",
          irreversibleAuthorization,
          observedPreconditionChecksum: "step-a-pre",
          operationId: conflictingPlan.operationId,
          proof,
          stepId: "a",
        }),
      ),
    )
    const fulfilled = conflicting.filter((result) => result.status === "fulfilled")
    const rejected = conflicting.filter((result) => result.status === "rejected")
    expect(fulfilled).toHaveLength(1)
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({
      reason: { code: "OperationInterventionRequiredError" },
    })
    const retained = await store.get(conflictingPlan.operationId)
    expect(authorizations.map((candidate) => candidate.authorizationChecksum)).toContain(
      retained?.operation.steps.a?.authorizationChecksum,
    )
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?`,
        )
        .get(conflictingPlan.operationId),
    ).toEqual({ count: 1 })
  })

  it("supports evidence-only not-applied reconciliation and records optional counters", async () => {
    const plan = await sealedPlan({ operationId: "not-applied-operation" })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "not-applied-acquisition",
      holderId: "not-applied-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "not-applied-attempt",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof,
      stepId: "a",
    })
    await store.failStep({
      actorChecksum: "actor",
      attemptId: "not-applied-attempt",
      counters: { cost: { providerCalls: 1 } },
      errorChecksum: "unknown",
      operationId: plan.operationId,
      outcome: "unknown",
      proof,
      stepId: "a",
    })
    const reconciled = await store.reconcileStep({
      actorChecksum: "actor",
      counters: { progress: { observations: 1 } },
      evidenceChecksum: "definitively-absent",
      operationId: plan.operationId,
      outcome: "not_applied",
      proof,
      reconciliationId: "not-applied-reconciliation",
      stepId: "a",
    })
    expect(reconciled.steps.a).toMatchObject({
      costCounters: { providerCalls: 1 },
      progressCounters: { observations: 1 },
      state: "retryable_failed",
    })
  })

  it("rejects missing operations and a result authorized under the wrong lease", async () => {
    const nonexistentProof = {
      acquisitionId: "missing",
      fencingToken: 1,
      holderId: "missing",
      leaseKey: "missing",
    }
    await expect(
      store.beginStep({
        actorChecksum: "actor",
        attemptId: "missing",
        idempotencyKey: "missing",
        observedPreconditionChecksum: "missing",
        operationId: "missing",
        proof: nonexistentProof,
        stepId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)
    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId: "missing",
        observedPostconditionChecksum: "missing",
        operationId: "missing",
        proof: nonexistentProof,
        resultChecksum: "missing",
        stepId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)
    await expect(
      store.recoverRunningStep({
        actorChecksum: "actor",
        operationId: "missing",
        proof: nonexistentProof,
        recoveryId: "missing-recovery",
        stepId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)

    const plan = await sealedPlan({ operationId: "wrong-lease-operation" })
    await store.create(creationInput(plan))
    const leases = new D1LeaseStore(database)
    const correct = await leases.acquire({
      acquisitionId: "correct-acquisition",
      holderId: "correct-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    const wrong = await leases.acquire({
      acquisitionId: "wrong-acquisition",
      holderId: "wrong-controller",
      leaseKey: "other:operation",
      ttlMs: 60_000,
    })
    if (!correct.acquired || !wrong.acquired) throw new Error("Fixture lease acquisition failed.")
    const correctProof = leaseProof(correct.record)
    await store.beginStep({
      actorChecksum: "actor",
      attemptId: "wrong-lease-attempt",
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId: plan.operationId,
      proof: correctProof,
      stepId: "a",
    })
    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId: "wrong-lease-attempt",
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof: leaseProof(wrong.record),
        resultChecksum: "result",
        stepId: "a",
      }),
    ).rejects.toThrow(/wrong lease/u)
    await leases.release({ proof: correctProof })
    const newer = await leases.acquire({
      acquisitionId: "newer-acquisition",
      holderId: "newer-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!newer.acquired) throw new Error("Fixture lease reacquisition failed.")
    await expect(
      store.completeStep({
        actorChecksum: "actor",
        attemptId: "wrong-lease-attempt",
        observedPostconditionChecksum: "step-a-post",
        operationId: plan.operationId,
        proof: leaseProof(newer.record),
        resultChecksum: "result",
        stepId: "a",
      }),
    ).rejects.toThrow(/fenced/u)
  })

  it("fails closed on missing, malformed, contradictory, or partially applied transition receipts", async () => {
    const leases = new D1LeaseStore(database)
    const acquired = await leases.acquire({
      acquisitionId: "fault-acquisition",
      holderId: "fault-controller",
      leaseKey: "fleet-a:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)

    const prepare = async (operationId: string) => {
      const plan = await sealedPlan({
        idempotencyKey: `${operationId}-key`,
        inputChecksum: `${operationId}-input`,
        operationId,
      })
      await store.create(creationInput(plan))
      return plan
    }
    const beginInput = (operationId: string, attemptId: string) => ({
      actorChecksum: "actor",
      attemptId,
      idempotencyKey: "step-a-key",
      observedPreconditionChecksum: "step-a-pre",
      operationId,
      proof,
      stepId: "a",
    })

    const droppedPlan = await prepare("dropped-transition")
    const dropped = new D1OperationStore(
      new FaultInjectingDatabase(database, { throwTransitionBatch: true }),
      digest,
    )
    await expect(dropped.beginStep(beginInput(droppedPlan.operationId, "dropped"))).rejects.toThrow(
      /transition retry budget/u,
    )

    const malformedPlan = await prepare("malformed-transition")
    const malformed = new D1OperationStore(
      new FaultInjectingDatabase(database, {
        dropTransitionBatch: true,
        transitionRow: {},
      }),
      digest,
    )
    await expect(
      malformed.beginStep(beginInput(malformedPlan.operationId, "malformed")),
    ).rejects.toThrow(/receipt is malformed/u)

    const contradictoryPlan = await prepare("contradictory-transition")
    const contradictoryId = transitionIdentity("accepted", [
      contradictoryPlan.operationId,
      "a",
      "contradictory",
    ])
    const contradictory = new D1OperationStore(
      new FaultInjectingDatabase(database, {
        dropTransitionBatch: true,
        transitionRow: {
          acquisition_id: proof.acquisitionId,
          audit_event_hash: "wrong-audit",
          created_at_ms: 1,
          fencing_token: proof.fencingToken,
          from_operation_status: "planned",
          from_record_json: "{}",
          holder_id: proof.holderId,
          lease_key: proof.leaseKey,
          operation_id: "wrong-operation",
          step_id: "a",
          to_operation_status: "running",
          to_record_json: "{}",
          transition_id: contradictoryId,
        },
      }),
      digest,
    )
    await expect(
      contradictory.beginStep(beginInput(contradictoryPlan.operationId, "contradictory")),
    ).rejects.toThrow(/contradictory durable state/u)

    for (const [operationId, faults, message] of [
      ["hidden-operation", { hideOperationAfterBatch: true }, /missing operation/u],
      ["hidden-current", { hideCurrentAfterBatch: true }, /exact operation state/u],
      ["hidden-audit", { hideAuditAfterBatch: true }, /without its audit event/u],
    ] as const) {
      const plan = await prepare(operationId)
      const faulted = new D1OperationStore(new FaultInjectingDatabase(database, faults), digest)
      await expect(faulted.beginStep(beginInput(plan.operationId, operationId))).rejects.toThrow(
        message,
      )
    }

    const contradictoryAuditPlan = await prepare("contradictory-audit")
    const contradictoryAudit = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "other-actor",
        environmentId: "fleet-a",
        eventType: "step.attempt.accepted",
        fencingToken: proof.fencingToken,
        idempotencyKey: "other-transition",
        operationId: contradictoryAuditPlan.operationId,
        payloadChecksum: "step-a-input",
        serverTimeMs: 1,
        stepId: "a",
      },
      digest,
    )
    const contradictoryAuditStore = new D1OperationStore(
      new FaultInjectingDatabase(database, {
        auditEventRow: { event_json: JSON.stringify(contradictoryAudit) },
      }),
      digest,
    )
    await expect(
      contradictoryAuditStore.beginStep(
        beginInput(contradictoryAuditPlan.operationId, "contradictory-audit"),
      ),
    ).rejects.toThrow(/contradictory audit event/u)

    const outcomePlan = await prepare("dropped-outcome")
    await store.beginStep(beginInput(outcomePlan.operationId, "outcome-attempt"))
    const droppedOutcome = new D1OperationStore(
      new FaultInjectingDatabase(database, { dropTransitionBatch: true }),
      digest,
    )
    await expect(
      droppedOutcome.completeStep({
        actorChecksum: "actor",
        attemptId: "outcome-attempt",
        observedPostconditionChecksum: "step-a-post",
        operationId: outcomePlan.operationId,
        proof,
        resultChecksum: "result",
        stepId: "a",
      }),
    ).rejects.toThrow(/transition retry budget/u)
  })
})
