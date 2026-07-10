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
import { D1SagaAttemptStore, sagaActionInputChecksum } from "../src/saga-attempt-store.js"
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
  | {
      readonly kind:
        | "settlement_effect_missing"
        | "settlement_saga_missing"
        | "settlement_transition_missing"
    }

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
      this.#fault.kind === "settlement_saga_missing" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "settlement_transition_missing" &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "settlement_effect_missing" &&
      sql.includes('WHERE "transition_id" = ?1') &&
      sql.includes("\"resource_kind\" = 'saga'")
    ) {
      return new FixedStatement(null)
    }
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
  readonly actionInputJson: string
  readonly attempts: D1SagaAttemptStore
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
  const attempts = new D1SagaAttemptStore(base, digest)
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
          baseRetryDelayMs: 0,
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
  const actionInputJson = '{"value":1}'
  const actionInputChecksum = await sagaActionInputChecksum(actionInputJson, digest)
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
    actionInputJson,
    attempts,
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

async function terminalReceipt(
  run: Fixture,
  state: "confirmed" | "failed" | "not_applied" | "unknown",
  attemptId = `${run.sagaId}:a:forward:1`,
) {
  await run.coordinator.beginAction(actionInput(run, attemptId))
  await run.attempts.accept({
    attemptId,
    inputJson: run.actionInputJson,
    phase: "forward",
    proof: run.proof,
    purpose: "effect",
    sagaId: run.sagaId,
    sagaStepId: "a",
  })
  return state === "confirmed"
    ? run.attempts.complete({
        attemptId,
        evidenceJson: JSON.stringify({ attemptId, source: "provider" }),
        outputJson: JSON.stringify({ attemptId, value: "created" }),
        proof: run.proof,
        state,
      })
    : run.attempts.complete({
        attemptId,
        errorJson: JSON.stringify({ attemptId, code: state }),
        evidenceJson: JSON.stringify({ attemptId, source: "provider" }),
        proof: run.proof,
        state,
      })
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
  it("atomically classifies a confirmed terminal receipt across both ledgers", async () => {
    const run = await fixture("settle-confirmed")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    if (receipt.state !== "confirmed") throw new Error("Expected a confirmed receipt.")

    const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.steps.a?.forward).toMatchObject({
      lastAttemptId: attemptId,
      resultChecksum: receipt.outputChecksum,
      state: "succeeded",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({
      lastAttemptId: attemptId,
      resultChecksum: receipt.outcomeChecksum,
      state: "succeeded",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)

    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).resolves.toEqual(settled)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("keeps retryable not-applied receipts retryable until the sealed budget is exhausted", async () => {
    const run = await fixture("settle-retries")
    await run.coordinator.initializeSaga(run.initialize)

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const attemptId = `${run.sagaId}:a:forward:${attempt}`
      const receipt = await terminalReceipt(run, "not_applied", attemptId)
      if (receipt.state === "accepted" || receipt.state === "confirmed") {
        throw new Error("Expected a failed receipt.")
      }
      const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
      const action = settled.steps.a?.forward
      const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
        "saga:forward:a"
      ]
      if (attempt < 3) {
        expect(action).toMatchObject({
          attempts: attempt,
          errorChecksum: receipt.errorChecksum,
          state: "retryable_failed",
        })
        expect(operationAction).toMatchObject({
          errorChecksum: receipt.outcomeChecksum,
          state: "retryable_failed",
        })
      } else {
        expect(settled).toMatchObject({ status: "failed", terminationCause: "failure" })
        expect(action).toMatchObject({
          attempts: 3,
          errorChecksum: receipt.errorChecksum,
          state: "failed",
        })
        expect(operationAction).toMatchObject({
          resultChecksum: receipt.outcomeChecksum,
          state: "succeeded",
        })
        const effect = run.base.database
          .prepare(
            `SELECT "effect_kind" FROM "nozzle_operation_effects"
             WHERE "operation_id" = ? AND "step_id" = 'saga:forward:a'
             ORDER BY "created_at_ms" DESC, rowid DESC LIMIT 1`,
          )
          .get(run.operationId) as { effect_kind: string }
        expect(effect.effect_kind).toBe("action:forward:failure:definitely_not_applied_terminal")
      }
      await expect(
        run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
      ).resolves.toEqual(settled)
      if (attempt === 2) {
        await expect(
          run.coordinator.settleActionFromReceipt(actionInput(run, `${run.sagaId}:a:forward:1`)),
        ).rejects.toThrow(/not the current action attempt/u)
      }
    }
  })

  it.each([
    ["failed", "failed", "succeeded", "failed"],
    ["unknown", "unknown", "unknown", "running"],
  ] as const)("maps a %s receipt without confusing protocol classification with business success", async (receiptState, sagaState, operationState, sagaStatus) => {
    const run = await fixture(`settle-${receiptState}`)
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, receiptState, attemptId)
    if (receipt.state === "accepted" || receipt.state === "confirmed") {
      throw new Error("Expected a failed receipt.")
    }

    const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.status).toBe(sagaStatus)
    expect(settled.steps.a?.forward).toMatchObject({
      errorChecksum: receipt.errorChecksum,
      state: sagaState,
    })
    const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
      "saga:forward:a"
    ]
    expect(operationAction).toMatchObject(
      operationState === "succeeded"
        ? { resultChecksum: receipt.outcomeChecksum, state: operationState }
        : { errorChecksum: receipt.outcomeChecksum, state: operationState },
    )
    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).resolves.toEqual(settled)
  })

  it("fails closed if the generic operation action diverges before classification", async () => {
    const run = await fixture("settle-active-divergence")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    if (receipt.state !== "confirmed") throw new Error("Expected a confirmed receipt.")
    await run.operations.completeStep({
      actorChecksum: "coordinator-test-actor",
      attemptId,
      observedPostconditionChecksum: `${run.operationId}:forward:postcondition`,
      operationId: run.operationId,
      proof: run.proof,
      resultChecksum: receipt.outcomeChecksum,
      stepId: "saga:forward:a",
    })

    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).rejects.toThrow(/contradicts the active coupled attempt/u)
  })

  it("rejects observation-only outcomes and missing exact replay evidence", async () => {
    const observation = await fixture("settle-observation")
    await observation.coordinator.initializeSaga(observation.initialize)
    const effectAttemptId = `${observation.sagaId}:a:forward:1`
    await terminalReceipt(observation, "unknown", effectAttemptId)
    await observation.coordinator.settleActionFromReceipt(actionInput(observation, effectAttemptId))
    await observation.leases.release({ proof: observation.proof })
    const reacquired = await observation.leases.acquire({
      acquisitionId: `${observation.sagaId}:observation-acquisition`,
      holderId: `${observation.sagaId}:observation-controller`,
      leaseKey: observation.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!reacquired.acquired) throw new Error("Expected the observation lease.")
    const observationProof = leaseProof(reacquired.record)
    const observationAttemptId = `${effectAttemptId}:observation`
    await observation.attempts.accept({
      attemptId: observationAttemptId,
      inputJson: '{"observe":true}',
      phase: "forward",
      proof: observationProof,
      purpose: "observation",
      sagaId: observation.sagaId,
      sagaStepId: "a",
    })
    await observation.attempts.complete({
      attemptId: observationAttemptId,
      errorJson: '{"code":"indeterminate"}',
      evidenceJson: '{"source":"provider"}',
      proof: observationProof,
      state: "indeterminate",
    })
    await expect(
      observation.coordinator.settleActionFromReceipt({
        ...actionInput(observation, observationAttemptId),
        proof: observationProof,
      }),
    ).rejects.toThrow(/cannot be indeterminate/u)

    const replay = await fixture("settle-replay-evidence")
    await replay.coordinator.initializeSaga(replay.initialize)
    const attemptId = `${replay.sagaId}:a:forward:1`
    await terminalReceipt(replay, "confirmed", attemptId)
    await replay.coordinator.settleActionFromReceipt(actionInput(replay, attemptId))
    for (const [kind, message] of [
      ["settlement_saga_missing", /saga does not exist/u],
      ["settlement_transition_missing", /exact operation transition/u],
      ["settlement_effect_missing", /exact coupled effect receipt/u],
    ] as const) {
      const faulted = new D1SagaCoordinatorStore(
        new QueryFaultDatabase(replay.base, { kind }),
        digest,
      )
      await expect(faulted.settleActionFromReceipt(actionInput(replay, attemptId))).rejects.toThrow(
        message,
      )
    }
  })

  it("requires an exact terminal effect receipt and exact coupled attempt identity", async () => {
    const missing = await fixture("settle-missing")
    await missing.coordinator.initializeSaga(missing.initialize)
    await expect(missing.coordinator.settleActionFromReceipt(actionInput(missing))).rejects.toThrow(
      /not durably accepted/u,
    )

    const accepted = await fixture("settle-accepted")
    await accepted.coordinator.initializeSaga(accepted.initialize)
    const attemptId = `${accepted.sagaId}:a:forward:1`
    await accepted.coordinator.beginAction(actionInput(accepted, attemptId))
    await accepted.attempts.accept({
      attemptId,
      inputJson: accepted.actionInputJson,
      phase: "forward",
      proof: accepted.proof,
      purpose: "effect",
      sagaId: accepted.sagaId,
      sagaStepId: "a",
    })
    await expect(
      accepted.coordinator.settleActionFromReceipt(actionInput(accepted, attemptId)),
    ).rejects.toThrow(/no terminal receipt/u)

    const terminal = await terminalReceipt(accepted, "confirmed", attemptId)
    expect(terminal.state).toBe("confirmed")
    await expect(
      accepted.coordinator.settleActionFromReceipt({
        ...actionInput(accepted, attemptId),
        operationId: "another-operation",
      }),
    ).rejects.toThrow(/different action/u)
    await expect(
      accepted.coordinator.settleActionFromReceipt({
        ...actionInput(accepted, attemptId),
        proof: { ...accepted.proof, acquisitionId: "another-acquisition" },
      }),
    ).rejects.toThrow(/different lease fence/u)
  })

  it("recovers an exact terminal settlement after losing the D1 response", async () => {
    const run = await fixture("settle-lost-response")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { kind: "commit_then_throw" }),
      digest,
    )

    const settled = await faulted.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.steps.a?.forward).toMatchObject({
      resultChecksum: receipt.state === "confirmed" ? receipt.outputChecksum : undefined,
      state: "succeeded",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("rolls terminal settlement back as one unit when an interior statement fails", async () => {
    const run = await fixture("settle-rollback")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", attemptId)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index: 3, kind: "rollback_before" }),
      digest,
    )

    await expect(faulted.settleActionFromReceipt(actionInput(run, attemptId))).rejects.toThrow(
      /Settling a saga action exceeded/u,
    )
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("running")
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"]?.state,
    ).toBe("running")
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)
  })

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
