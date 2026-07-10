import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  appendAuditEvent,
  type DigestFunction,
  leaseProof,
  type OperationStepPlanInput,
  sagaActionIdempotencyKey,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { afterEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore } from "../src/operation-store.js"
import { sagaActionInputChecksum } from "../src/saga-attempt-store.js"
import { D1SagaCoordinatorStore, type InitializeSagaInput } from "../src/saga-coordinator-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
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

  constructor() {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(controlSchemaSql())
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
  readonly #row: unknown

  constructor(row: unknown) {
    this.#row = row
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: [], success: true }
  }

  async first<T>(): Promise<T | null> {
    return this.#row as T | null
  }

  async run(): Promise<ControlRunResult> {
    return { meta: { changes: 0 }, success: true }
  }
}

type BatchFault =
  | { readonly kind: "commit_then_throw" }
  | { readonly index: number; readonly kind: "rollback_before" }
  | { readonly kind: "return"; readonly results: readonly ControlRunResult[] }

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #fault: BatchFault

  constructor(base: DatabaseAdapter, fault: BatchFault) {
    this.#base = base
    this.#fault = fault
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#fault.kind === "return") return this.#fault.results
    if (this.#fault.kind === "commit_then_throw") {
      await this.#base.batch(statements)
      throw new Error("injected post-commit response loss")
    }
    this.#base.database.exec("BEGIN IMMEDIATE;")
    try {
      const results: ControlRunResult[] = []
      for (let index = 0; index < statements.length; index += 1) {
        if (index === this.#fault.index) throw new Error("injected coupled statement failure")
        results.push(await (statements[index] as ControlStatement).run())
      }
      this.#base.database.exec("COMMIT;")
      return results
    } catch (error) {
      this.#base.database.exec("ROLLBACK;")
      throw error
    }
  }

  prepare(sql: string): ControlStatement {
    return this.#base.prepare(sql)
  }
}

type QueryFault =
  | { readonly kind: "audit_snapshot"; readonly row: unknown }
  | {
      readonly kind:
        | "audit_missing"
        | "effect_missing"
        | "effect_mismatch"
        | "operation_missing"
        | "saga_missing"
        | "transition_mismatch"
    }
  | { readonly kind: "saga_operation_mismatch" }

class QueryFaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #fault: QueryFault
  #batched = false

  constructor(base: DatabaseAdapter, fault: QueryFault) {
    this.#base = base
    this.#fault = fault
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const results = await this.#base.batch(statements)
    this.#batched = true
    return results
  }

  prepare(sql: string): ControlStatement {
    if (
      this.#fault.kind === "audit_snapshot" &&
      sql.includes('AS "now_ms"') &&
      sql.includes("nozzle_audit_log")
    ) {
      return new FixedStatement(this.#fault.row)
    }
    if (
      this.#fault.kind === "saga_operation_mismatch" &&
      sql === 'SELECT "operation_id" FROM "nozzle_sagas" WHERE "saga_id" = ?1'
    ) {
      return new FixedStatement({ operation_id: "another-operation" })
    }
    if (this.#batched) {
      if (this.#fault.kind === "transition_mismatch" && sql.includes("operation_transitions")) {
        const row = this.#base.database
          .prepare(`SELECT * FROM "nozzle_operation_transitions" ORDER BY rowid DESC LIMIT 1`)
          .get() as Record<string, unknown>
        return new FixedStatement({ ...row, step_id: "wrong-step" })
      }
      if (this.#fault.kind === "effect_missing" && sql.includes("nozzle_operation_effects")) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "effect_mismatch" && sql.includes("nozzle_operation_effects")) {
        const row = this.#base.database
          .prepare(`SELECT * FROM "nozzle_operation_effects" ORDER BY rowid DESC LIMIT 1`)
          .get() as Record<string, unknown>
        return new FixedStatement({ ...row, effect_kind: "wrong-kind" })
      }
      if (
        this.#fault.kind === "operation_missing" &&
        sql.includes('FROM "nozzle_operations" WHERE "operation_id"')
      ) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "saga_missing" && sql.includes('FROM "nozzle_sagas" AS "saga"')) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "audit_missing" && sql.includes('SELECT 1 AS "present"')) {
        return new FixedStatement(null)
      }
    }
    return this.#base.prepare(sql)
  }
}

interface Fixture {
  readonly base: DatabaseAdapter
  readonly coordinator: D1SagaCoordinatorStore
  readonly initialize: InitializeSagaInput
  readonly leases: D1LeaseStore
  readonly operationId: string
  readonly operations: D1OperationStore
  readonly proof: ReturnType<typeof leaseProof>
  readonly sagaId: string
  readonly sagas: D1SagaStore
}

const databases: DatabaseAdapter[] = []
afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

async function fixture(
  suffix: string,
  fault?: BatchFault,
  queryFault?: QueryFault,
  omitActionPlan = false,
): Promise<Fixture> {
  const base = new DatabaseAdapter()
  databases.push(base)
  const database =
    queryFault !== undefined
      ? new QueryFaultDatabase(base, queryFault)
      : fault === undefined
        ? base
        : new FaultDatabase(base, fault)
  const operations = new D1OperationStore(base, digest)
  const leases = new D1LeaseStore(base)
  const sagas = new D1SagaStore(base, digest)
  const coordinator = new D1SagaCoordinatorStore(database, digest)
  const operationId = `coordinator-operation-${suffix}`
  const sagaId = `coordinator-saga-${suffix}`
  const leaseKey = `saga:${sagaId}`
  const forwardStepId = sagaActionOperationStepId("a", "forward")
  const compensationStepId = sagaActionOperationStepId("a", "compensation")
  const descriptor = await sealSagaDescriptor(
    {
      descriptorId: `coordinator-descriptor-${suffix}`,
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
      ],
      version: 1,
    },
    digest,
  )
  const actionInputChecksum = await sagaActionInputChecksum('{"value":1}', digest)
  const capabilitySnapshotJson = '{"runtime":"coordinator-v1"}'
  const operationInputJson = JSON.stringify({ sagaId })
  const plan = await sealOperationPlan(
    {
      capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
      idempotencyKey: `${operationId}:key`,
      inputChecksum: await digest(new TextEncoder().encode(operationInputJson)),
      operationId,
      operationType: `saga:${descriptor.descriptorId}@1`,
      steps: (
        [
          {
            checkpoint: "reversible",
            idempotencyKey: `${operationId}:init:key`,
            inputChecksum: `${operationId}:init:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:init:postcondition`,
            preconditionChecksum: `${operationId}:init:precondition`,
            recoveryInstructions: "Create the saga projection through the coupled coordinator.",
            retryClassification: "idempotent",
            stepId: SAGA_INIT_OPERATION_STEP_ID,
          },
          {
            checkpoint: "reversible",
            completionRole: "settlement",
            idempotencyKey: `${operationId}:settle:key`,
            inputChecksum: `${operationId}:settle:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:settle:postcondition`,
            preconditionChecksum: `${operationId}:settle:precondition`,
            recoveryInstructions: "Settle only from the terminal saga projection.",
            retryClassification: "never",
            stepId: SAGA_SETTLE_OPERATION_STEP_ID,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            idempotencyKey: `${operationId}:termination:key`,
            inputChecksum: `${operationId}:termination:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:termination:postcondition`,
            preconditionChecksum: `${operationId}:termination:precondition`,
            recoveryInstructions: "Materialize termination under the saga lease.",
            retryClassification: "idempotent",
            stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            effectProtocol: "saga_receipt",
            idempotencyKey: sagaActionIdempotencyKey(sagaId, "a", "forward"),
            inputChecksum: actionInputChecksum,
            leaseKey,
            postconditionChecksum: `${operationId}:forward:postcondition`,
            preconditionChecksum: `${operationId}:forward:precondition`,
            recoveryInstructions: "Recover the exact forward action receipt.",
            retryClassification: "reconcile_first",
            stepId: forwardStepId,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            effectProtocol: "saga_receipt",
            idempotencyKey: sagaActionIdempotencyKey(sagaId, "a", "compensation"),
            inputChecksum: `${operationId}:compensation:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:compensation:postcondition`,
            preconditionChecksum: `${operationId}:compensation:precondition`,
            recoveryInstructions: "Recover the exact compensation receipt.",
            retryClassification: "reconcile_first",
            stepId: compensationStepId,
          },
        ] as const satisfies readonly OperationStepPlanInput[]
      ).filter((step) => !omitActionPlan || step.stepId !== forwardStepId),
    },
    digest,
  )
  await operations.create({
    actorChecksum: "coordinator-test-actor",
    capabilitySnapshotJson,
    environmentId: "production",
    idempotencyScope: `coordinator-${suffix}`,
    inputJson: operationInputJson,
    plan,
    requiredShardIds: ["shard-a"],
  })
  const acquired = await leases.acquire({
    acquisitionId: `coordinator-acquisition-${suffix}`,
    holderId: `coordinator-controller-${suffix}`,
    leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Coordinator fixture lease acquisition failed.")
  const proof = leaseProof(acquired.record)
  const initAttemptId = `${sagaId}:init:1`
  await operations.beginStep({
    actorChecksum: "coordinator-test-actor",
    attemptId: initAttemptId,
    idempotencyKey: `${operationId}:init:key`,
    observedPreconditionChecksum: `${operationId}:init:precondition`,
    operationId,
    proof,
    stepId: SAGA_INIT_OPERATION_STEP_ID,
  })
  return {
    base,
    coordinator,
    initialize: {
      actorChecksum: "coordinator-test-actor",
      attemptId: initAttemptId,
      deadlineAtMs: 8_000_000_000_000_000,
      descriptor,
      evidenceChecksum: `${sagaId}:init:evidence`,
      idempotencyKey: `${sagaId}:key`,
      inputChecksum: `${sagaId}:input`,
      observedPostconditionChecksum: `${operationId}:init:postcondition`,
      operationId,
      proof,
      resultChecksum: `${sagaId}:init:result`,
      sagaId,
      stepInputChecksums: { a: actionInputChecksum },
    },
    leases,
    operationId,
    operations,
    proof,
    sagaId,
    sagas,
  }
}

function actionInput(run: Fixture, attemptId = `${run.sagaId}:a:forward:1`) {
  return {
    actorChecksum: "coordinator-test-actor",
    attemptId,
    operationId: run.operationId,
    phase: "forward" as const,
    proof: run.proof,
    sagaId: run.sagaId,
    stepId: "a",
  }
}

function count(database: DatabaseSync, table: string): number {
  return (database.prepare(`SELECT count(*) AS "count" FROM "${table}"`).get() as { count: number })
    .count
}

describe("D1SagaCoordinatorStore", () => {
  it("atomically initializes both ledgers and begins an action with synchronized attempts", async () => {
    const run = await fixture("atomic")
    const initialized = await run.coordinator.initializeSaga(run.initialize)
    expect(initialized).toMatchObject({ sagaId: run.sagaId, stateVersion: 0, status: "planned" })
    expect((await run.operations.get(run.operationId))?.operation.steps["saga:init"]).toMatchObject(
      {
        state: "succeeded",
      },
    )
    expect(await run.sagas.get(run.sagaId)).toEqual(initialized)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(1)
    await expect(run.coordinator.initializeSaga(run.initialize)).resolves.toEqual(initialized)
    await expect(
      run.coordinator.initializeSaga({ ...run.initialize, idempotencyKey: "contradictory" }),
    ).rejects.toThrow(/replay contradicts/u)

    const attemptId = `${run.sagaId}:a:forward:1`
    const begun = await run.coordinator.beginAction(actionInput(run, attemptId))
    expect(begun).toMatchObject({ disposition: "execute" })
    expect(begun.saga.steps.a?.forward).toMatchObject({
      activeAttemptId: attemptId,
      attempts: 1,
      state: "running",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({ activeAttemptId: attemptId, startedAttempts: 1, state: "running" })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)

    await expect(run.coordinator.beginAction(actionInput(run, attemptId))).resolves.toMatchObject({
      disposition: "in_progress",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)
  })

  it("recovers an exact commit when the D1 response is lost", async () => {
    const run = await fixture("lost-response", { kind: "commit_then_throw" })
    await expect(run.coordinator.initializeSaga(run.initialize)).resolves.toMatchObject({
      sagaId: run.sagaId,
      stateVersion: 0,
    })
    expect(count(run.base.database, "nozzle_sagas")).toBe(1)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(1)
  })

  it("rolls every receipt and projection back when any coupled statement fails", async () => {
    const run = await fixture("rollback", { index: 3, kind: "rollback_before" })
    await expect(run.coordinator.initializeSaga(run.initialize)).rejects.toThrow(
      /bounded coupled retry budget/u,
    )
    expect(count(run.base.database, "nozzle_sagas")).toBe(0)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(0)
    expect((await run.operations.get(run.operationId))?.operation.steps["saga:init"]).toMatchObject(
      {
        state: "running",
      },
    )
  })

  it("fails closed on contradictory receipts, projections, and audit visibility", async () => {
    for (const [kind, message] of [
      ["transition_mismatch", /transition receipt is contradictory/u],
      ["effect_missing", /operation-effect receipt is missing/u],
      ["effect_mismatch", /operation-effect receipt is missing/u],
      ["operation_missing", /operation projection does not match/u],
      ["saga_missing", /saga projection does not match/u],
      ["audit_missing", /lacks its exact audit event/u],
    ] as const) {
      const run = await fixture(`verify-${kind}`, undefined, { kind })
      await expect(run.coordinator.initializeSaga(run.initialize)).rejects.toThrow(message)
    }
  })

  it("validates the authoritative audit snapshot before building a coupled batch", async () => {
    const malformedClock = await fixture("audit-clock", undefined, {
      kind: "audit_snapshot",
      row: { event_json: null, now_ms: -1 },
    })
    await expect(
      malformedClock.coordinator.initializeSaga(malformedClock.initialize),
    ).rejects.toThrow(/malformed authoritative/u)

    const malformedJson = await fixture("audit-json", undefined, {
      kind: "audit_snapshot",
      row: { event_json: "{", now_ms: 100 },
    })
    await expect(
      malformedJson.coordinator.initializeSaga(malformedJson.initialize),
    ).rejects.toThrow(/invalid JSON/u)

    const otherEnvironment = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "other-actor",
        environmentId: "another-environment",
        eventType: "test.event",
        fencingToken: null,
        idempotencyKey: "other-event",
        operationId: "other-operation",
        payloadChecksum: "other-payload",
        serverTimeMs: 1,
        stepId: null,
      },
      digest,
    )
    const wrongEnvironment = await fixture("audit-environment", undefined, {
      kind: "audit_snapshot",
      row: { event_json: JSON.stringify(otherEnvironment), now_ms: 100 },
    })
    await expect(
      wrongEnvironment.coordinator.initializeSaga(wrongEnvironment.initialize),
    ).rejects.toThrow(/another environment/u)

    const absentHead = await fixture("audit-absent-head", undefined, {
      kind: "audit_snapshot",
      row: { event_json: null, now_ms: 100 },
    })
    await expect(absentHead.coordinator.initializeSaga(absentHead.initialize)).rejects.toThrow(
      /bounded coupled retry budget/u,
    )
  })

  it("rejects missing, fenced, and divergent coupled state before dispatch", async () => {
    const missingOperation = await fixture("missing-operation")
    await expect(
      missingOperation.coordinator.initializeSaga({
        ...missingOperation.initialize,
        operationId: "missing",
      }),
    ).rejects.toThrow(/operation does not exist/u)
    await expect(
      missingOperation.coordinator.beginAction(actionInput(missingOperation)),
    ).rejects.toThrow(/saga does not exist/u)

    const missingAction = await fixture("missing-action")
    await missingAction.coordinator.initializeSaga(missingAction.initialize)
    await expect(
      missingAction.coordinator.beginAction({ ...actionInput(missingAction), stepId: "missing" }),
    ).rejects.toThrow(/action does not exist/u)

    const missingPlan = await fixture("missing-plan", undefined, undefined, true)
    await missingPlan.coordinator.initializeSaga(missingPlan.initialize)
    await expect(missingPlan.coordinator.beginAction(actionInput(missingPlan))).rejects.toThrow(
      /lacks an operation step/u,
    )

    const wrongOperation = await fixture("wrong-saga-operation")
    await wrongOperation.coordinator.initializeSaga(wrongOperation.initialize)
    const wrongOperationCoordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(wrongOperation.base, { kind: "saga_operation_mismatch" }),
      digest,
    )
    await expect(
      wrongOperationCoordinator.beginAction(actionInput(wrongOperation)),
    ).rejects.toThrow(/different operation/u)

    const divergent = await fixture("divergent")
    await divergent.coordinator.initializeSaga(divergent.initialize)
    const attemptId = `${divergent.sagaId}:a:forward:1`
    await divergent.operations.beginStep({
      actorChecksum: "coordinator-test-actor",
      attemptId,
      idempotencyKey: sagaActionIdempotencyKey(divergent.sagaId, "a", "forward"),
      observedPreconditionChecksum: `${divergent.operationId}:forward:precondition`,
      operationId: divergent.operationId,
      proof: divergent.proof,
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    await expect(
      divergent.coordinator.beginAction(actionInput(divergent, attemptId)),
    ).rejects.toThrow(/begin decisions diverged/u)

    const fenced = await fixture("fenced-initialization")
    await fenced.leases.release({ proof: fenced.proof })
    const reacquired = await fenced.leases.acquire({
      acquisitionId: `${fenced.sagaId}:new-acquisition`,
      holderId: `${fenced.sagaId}:new-controller`,
      leaseKey: fenced.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!reacquired.acquired) throw new Error("Expected coordinator lease reacquisition.")
    await expect(
      fenced.coordinator.initializeSaga({
        ...fenced.initialize,
        proof: leaseProof(reacquired.record),
      }),
    ).rejects.toThrow(/fenced by a newer/u)
  })

  it("bounds a repeatedly rolled-back action begin", async () => {
    const run = await fixture("begin-rollback")
    await run.coordinator.initializeSaga(run.initialize)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index: 3, kind: "rollback_before" }),
      digest,
    )
    await expect(faulted.beginAction(actionInput(run))).rejects.toThrow(
      /Beginning a saga action exceeded/u,
    )
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("pending")
  })

  it("rejects invalid dependencies and malformed D1 batch metadata", async () => {
    expect(() => new D1SagaCoordinatorStore(null as never, digest)).toThrow(/transactional/u)
    const database = new DatabaseAdapter()
    databases.push(database)
    expect(() => new D1SagaCoordinatorStore(database, null as never)).toThrow(/digest/u)
    expect(() => new D1SagaCoordinatorStore({ prepare() {} } as never, digest)).toThrow(
      /transactional/u,
    )

    const validation = await fixture("validation")
    await expect(
      validation.coordinator.initializeSaga({ ...validation.initialize, actorChecksum: "" }),
    ).rejects.toThrow(/non-empty/u)
    await expect(
      validation.coordinator.initializeSaga({
        ...validation.initialize,
        attemptId: "x".repeat(513),
      }),
    ).rejects.toThrow(/identity limit/u)

    const incomplete = await fixture("incomplete", { kind: "return", results: [] })
    await expect(incomplete.coordinator.initializeSaga(incomplete.initialize)).rejects.toThrow(
      /incomplete coupled/u,
    )
    const malformed = await fixture("malformed", {
      kind: "return",
      results: Array.from({ length: 6 }, () => ({
        meta: { changes: 2 },
        success: true,
      })),
    })
    await expect(malformed.coordinator.initializeSaga(malformed.initialize)).rejects.toThrow(
      /malformed coupled/u,
    )
    for (const [suffix, result] of [
      ["false", { meta: { changes: 1 }, success: false }],
      ["fraction", { meta: { changes: 0.5 }, success: true }],
      ["negative", { meta: { changes: -1 }, success: true }],
    ] as const) {
      const faulted = await fixture(`metadata-${suffix}`, {
        kind: "return",
        results: Array.from({ length: 6 }, () => result),
      })
      await expect(faulted.coordinator.initializeSaga(faulted.initialize)).rejects.toThrow(
        /malformed coupled/u,
      )
    }
  })
})
