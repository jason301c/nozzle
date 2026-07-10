import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  appendAuditEvent,
  type DigestFunction,
  type OperationPlanInput,
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
import { D1OperationStore } from "../src/operation-store.js"
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
  return sealOperationPlan(planInput(input), digest)
}

function creationInput(
  plan: Awaited<ReturnType<typeof sealedPlan>>,
  overrides: Record<string, unknown> = {},
) {
  return {
    actorChecksum: "actor-checksum",
    environmentId: "production",
    idempotencyScope: "fleet-a",
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
      environmentId: created.environmentId,
      idempotencyScope: created.idempotencyScope,
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
    database.database
      .prepare(`UPDATE "nozzle_operations" SET "status" = 'running' WHERE "operation_id" = ?`)
      .run(plan.operationId)
    await expect(store.get(plan.operationId)).rejects.toThrow(/columns contradict/u)

    database.database
      .prepare(`UPDATE "nozzle_operations" SET "status" = 'planned' WHERE "operation_id" = ?`)
      .run(plan.operationId)
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
})
