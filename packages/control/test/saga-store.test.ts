import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type DigestFunction,
  leaseProof,
  type SagaActionReference,
  type SagaStepDescriptorInput,
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
import { D1OperationStore, operationTransitionIdentity } from "../src/operation-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  type SagaEffectContext,
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
  readonly #runResult: ControlRunResult

  constructor(input: { readonly row?: unknown; readonly runResult?: ControlRunResult }) {
    this.#row = input.row ?? null
    this.#runResult = input.runResult ?? { meta: { changes: 0 }, success: true }
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
    return this.#runResult
  }
}

interface DatabaseFaults {
  readonly batchMode?: "commit_then_throw" | "effect_only" | "throw"
  readonly batchResults?: readonly ControlRunResult[]
  readonly effectRow?: unknown
  readonly effectRowAfterBatch?: () => unknown
  readonly sagaRow?: unknown
  readonly sagaRowAfterBatch?: () => unknown
  readonly transitionRow?: unknown
}

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #faults: DatabaseFaults
  #batched = false

  constructor(base: DatabaseAdapter, faults: DatabaseFaults) {
    this.#base = base
    this.#faults = faults
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#faults.batchMode === "throw") throw new Error("injected saga batch failure")
    if (this.#faults.batchMode === "effect_only") {
      const first = statements[0]
      if (!first) throw new Error("missing effect statement")
      const effectResult = await this.#base.batch([first])
      this.#batched = true
      return [effectResult[0] as ControlRunResult, { meta: { changes: 1 }, success: true }]
    }
    if (this.#faults.batchMode === "commit_then_throw") {
      await this.#base.batch(statements)
      this.#batched = true
      throw new Error("injected saga post-commit failure")
    }
    if (this.#faults.batchResults !== undefined) {
      this.#batched = true
      return this.#faults.batchResults
    }
    const results = await this.#base.batch(statements)
    this.#batched = true
    return results
  }

  prepare(sql: string): ControlStatement {
    if (sql.includes('FROM "nozzle_sagas" AS "saga"')) {
      if (this.#batched && this.#faults.sagaRowAfterBatch !== undefined) {
        return new FixedStatement({ row: this.#faults.sagaRowAfterBatch() })
      }
      if (this.#faults.sagaRow !== undefined)
        return new FixedStatement({ row: this.#faults.sagaRow })
    }
    if (sql.includes('SELECT * FROM "nozzle_operation_effects"')) {
      if (this.#batched && this.#faults.effectRowAfterBatch !== undefined) {
        return new FixedStatement({ row: this.#faults.effectRowAfterBatch() })
      }
      if (this.#faults.effectRow !== undefined) {
        return new FixedStatement({ row: this.#faults.effectRow })
      }
    }
    if (
      sql.includes('SELECT "transition"."transition_id"') &&
      this.#faults.transitionRow !== undefined
    ) {
      return new FixedStatement({ row: this.#faults.transitionRow })
    }
    return this.#base.prepare(sql)
  }
}

function action(actionId: string, character: string): SagaActionReference {
  return { actionId, artifactChecksum: character.repeat(64), version: 1 }
}

function descriptorStep(stepId: string): SagaStepDescriptorInput {
  return {
    authorizationPolicyChecksum: null,
    baseRetryDelayMs: 10,
    compensationAction: action(`${stepId}.compensate`, "c"),
    compensationObservation: action(`${stepId}.observe-compensation`, "d"),
    forwardAction: action(`${stepId}.forward`, "a"),
    forwardObservation: action(`${stepId}.observe-forward`, "b"),
    inputSchemaChecksum: "11".repeat(32),
    irreversible: false,
    maxAttempts: 3,
    maxRetryDelayMs: 100,
    outputSchemaChecksum: "22".repeat(32),
    stepId,
    timeoutMs: 1_000,
  }
}

function rawSaga(database: DatabaseSync, sagaId: string): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "saga".*,
              "effect"."effect_id" AS "effect_id",
              "effect"."resource_kind" AS "effect_resource_kind",
              "effect"."resource_id" AS "effect_resource_id",
              "effect"."operation_id" AS "effect_operation_id",
              "effect"."to_state_version" AS "effect_to_state_version",
              "effect"."evidence_checksum" AS "effect_evidence_checksum",
              "effect"."record_checksum" AS "effect_record_checksum",
              "effect"."record_json" AS "effect_record_json"
       FROM "nozzle_sagas" AS "saga"
       LEFT JOIN "nozzle_operation_effects" AS "effect"
         ON "effect"."effect_id" = "saga"."last_effect_id"
       WHERE "saga"."saga_id" = ?`,
    )
    .get(sagaId) as Record<string, unknown>
}

function rawEffect(database: DatabaseSync, effectId: string): Record<string, unknown> {
  return database
    .prepare(`SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?`)
    .get(effectId) as Record<string, unknown>
}

async function sagaRecordChecksum(recordJson: string): Promise<string> {
  const domain = new TextEncoder().encode("nozzle.saga-record.v1")
  const value = new TextEncoder().encode(recordJson)
  const framed = new Uint8Array(8 + domain.byteLength + value.byteLength)
  const view = new DataView(framed.buffer)
  view.setUint32(0, domain.byteLength, false)
  framed.set(domain, 4)
  view.setUint32(4 + domain.byteLength, value.byteLength, false)
  framed.set(value, 8 + domain.byteLength)
  return digest(framed)
}

describe("D1SagaStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let operations: D1OperationStore
  let sagas: D1SagaStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    operations = new D1OperationStore(database, digest)
    sagas = new D1SagaStore(database, digest)
    return () => database.close()
  })

  async function fixture(suffix: string, stepIds = ["a", "b"], providerReceiptActions = false) {
    const operationId = `saga-operation-${suffix}`
    const sagaId = `saga-${suffix}`
    const leaseKey = `saga:${sagaId}`
    const operationStepIds = [
      SAGA_INIT_OPERATION_STEP_ID,
      SAGA_TERMINATION_OPERATION_STEP_ID,
      ...stepIds.flatMap((stepId) => [
        sagaActionOperationStepId(stepId, "forward"),
        sagaActionOperationStepId(stepId, "compensation"),
      ]),
    ]
    const inputJson = JSON.stringify({ sagaId })
    const capabilitySnapshotJson = JSON.stringify({ runtime: "saga-v1" })
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: `saga-operation-key-${suffix}`,
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId,
        operationType: "saga",
        steps: operationStepIds.map((stepId) => ({
          checkpoint: "reversible" as const,
          dependsOn: [],
          effectProtocol:
            providerReceiptActions &&
            stepId !== SAGA_INIT_OPERATION_STEP_ID &&
            stepId !== SAGA_TERMINATION_OPERATION_STEP_ID
              ? ("provider_receipt" as const)
              : ("opaque" as const),
          idempotencyKey: `${operationId}:${stepId}:key`,
          inputChecksum: `${operationId}:${stepId}:input`,
          leaseKey,
          postconditionChecksum: `${operationId}:${stepId}:postcondition`,
          preconditionChecksum: `${operationId}:${stepId}:precondition`,
          recoveryInstructions: "Resume from the durable saga projection and action receipts.",
          retryClassification: "reconcile_first" as const,
          stepId,
        })),
      },
      digest,
    )
    await operations.create({
      actorChecksum: "saga-test-actor",
      capabilitySnapshotJson,
      environmentId: "production",
      idempotencyScope: `saga-test-${suffix}`,
      inputJson,
      plan,
      requiredShardIds: ["shard-a", "shard-b"],
    })
    const acquired = await leases.acquire({
      acquisitionId: `saga-acquisition-${suffix}`,
      holderId: `saga-controller-${suffix}`,
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture saga lease acquisition failed.")
    let proof = leaseProof(acquired.record)

    const canonicalStepId = (stepId: string): string => {
      if (stepId === "init") return SAGA_INIT_OPERATION_STEP_ID
      if (stepId === "termination") return SAGA_TERMINATION_OPERATION_STEP_ID
      const separator = stepId.lastIndexOf(":")
      if (separator < 1) return stepId
      const phase = stepId.slice(separator + 1)
      return phase === "forward" || phase === "compensation"
        ? sagaActionOperationStepId(stepId.slice(0, separator), phase)
        : stepId
    }

    const context = (
      effectId: string,
      operationStepIdInput: string,
      transitionId: string,
    ): SagaEffectContext => {
      const operationStepId = canonicalStepId(operationStepIdInput)
      return { effectId, operationId, proof, stepId: operationStepId, transitionId }
    }

    const begin = async (operationStepIdInput: string, attemptId: string, effectId: string) => {
      const operationStepId = canonicalStepId(operationStepIdInput)
      await operations.beginStep({
        actorChecksum: "saga-test-actor",
        attemptId,
        idempotencyKey: `${operationId}:${operationStepId}:key`,
        observedPreconditionChecksum: `${operationId}:${operationStepId}:precondition`,
        operationId,
        proof,
        stepId: operationStepId,
      })
      return context(
        effectId,
        operationStepId,
        operationTransitionIdentity("accepted", [operationId, operationStepId, attemptId]),
      )
    }

    const complete = async (
      operationStepIdInput: string,
      attemptId: string,
      resultChecksum: string,
      effectId: string,
    ) => {
      const operationStepId = canonicalStepId(operationStepIdInput)
      await operations.completeStep({
        actorChecksum: "saga-test-actor",
        attemptId,
        observedPostconditionChecksum: `${operationId}:${operationStepId}:postcondition`,
        operationId,
        proof,
        resultChecksum,
        stepId: operationStepId,
      })
      return context(
        effectId,
        operationStepId,
        operationTransitionIdentity("succeeded", [operationId, operationStepId, attemptId]),
      )
    }

    const fail = async (
      operationStepIdInput: string,
      attemptId: string,
      errorChecksum: string,
      outcome: "definitely_not_applied" | "permanent" | "unknown",
      effectId: string,
    ) => {
      const operationStepId = canonicalStepId(operationStepIdInput)
      await operations.failStep({
        actorChecksum: "saga-test-actor",
        attemptId,
        errorChecksum,
        operationId,
        outcome,
        proof,
        stepId: operationStepId,
      })
      return context(
        effectId,
        operationStepId,
        operationTransitionIdentity("failed", [operationId, operationStepId, attemptId]),
      )
    }

    const initialize = async () => {
      await begin("init", `${sagaId}:init:1`, `${sagaId}:init:begin`)
      const effect = await complete(
        "init",
        `${sagaId}:init:1`,
        `${sagaId}:init:result`,
        `${sagaId}:create`,
      )
      const descriptor = await sealSagaDescriptor(
        {
          descriptorId: `transfer-${suffix}`,
          steps: stepIds.map(descriptorStep),
          version: 1,
        },
        digest,
      )
      const record = await sagas.create({
        deadlineAtMs: 10_000,
        descriptor,
        effect,
        evidenceChecksum: `${sagaId}:create:evidence`,
        idempotencyKey: `${sagaId}:idempotency`,
        inputChecksum: `${sagaId}:input`,
        sagaId,
        serverTimeMs: 1_000,
        stepInputChecksums: Object.fromEntries(
          stepIds.map((stepId) => [stepId, `${sagaId}:${stepId}:input`]),
        ),
      })
      return { descriptor, effect, record }
    }

    const reacquire = async () => {
      await leases.release({ proof })
      const next = await leases.acquire({
        acquisitionId: `saga-reacquisition-${suffix}-${proof.fencingToken}`,
        holderId: `saga-recovery-controller-${suffix}`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!next.acquired) throw new Error("Fixture saga lease reacquisition failed.")
      proof = leaseProof(next.record)
      return proof
    }

    return {
      begin,
      complete,
      context,
      fail,
      initialize,
      operationId,
      proof: () => proof,
      reacquire,
      sagaId,
    }
  }

  it("persists forward failure and strict reverse compensation through exact operation effects", async () => {
    const run = await fixture("compensation")
    let { effect: createEffect, record } = await run.initialize()
    expect(record).toMatchObject({ stateVersion: 0, status: "planned" })
    expect(await sagas.get(run.sagaId)).toEqual(record)

    const aAttempt = `${run.sagaId}:a:forward:1`
    let effect = await run.begin("a:forward", aAttempt, `${run.sagaId}:a:forward:begin`)
    const begun = await sagas.beginAction({
      attemptId: aAttempt,
      effect,
      evidenceChecksum: `${aAttempt}:accepted`,
      idempotencyKey: record.steps.a?.forward.idempotencyKey as string,
      phase: "forward",
      sagaId: run.sagaId,
      serverTimeMs: 1_001,
      stepId: "a",
    })
    expect(begun.disposition).toBe("execute")
    record = begun.saga
    await expect(
      sagas.beginAction({
        attemptId: aAttempt,
        effect,
        evidenceChecksum: `${aAttempt}:accepted`,
        idempotencyKey: record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: run.sagaId,
        serverTimeMs: 1_002,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "in_progress" })

    effect = await run.complete(
      "a:forward",
      aAttempt,
      `${aAttempt}:result`,
      `${run.sagaId}:a:forward:success`,
    )
    const aSuccessInput = {
      attemptId: aAttempt,
      effect,
      evidenceChecksum: `${aAttempt}:outcome`,
      phase: "forward",
      resultChecksum: `${aAttempt}:result`,
      sagaId: run.sagaId,
      serverTimeMs: 1_003,
      stepId: "a",
    } as const
    record = await sagas.recordActionSuccess(aSuccessInput)
    await expect(sagas.recordActionSuccess(aSuccessInput)).resolves.toEqual(record)

    const bAttempt = `${run.sagaId}:b:forward:1`
    effect = await run.begin("b:forward", bAttempt, `${run.sagaId}:b:forward:begin`)
    record = (
      await sagas.beginAction({
        attemptId: bAttempt,
        effect,
        evidenceChecksum: `${bAttempt}:accepted`,
        idempotencyKey: record.steps.b?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: run.sagaId,
        serverTimeMs: 1_004,
        stepId: "b",
      })
    ).saga
    effect = await run.fail(
      "b:forward",
      bAttempt,
      `${bAttempt}:error`,
      "permanent",
      `${run.sagaId}:b:forward:failure`,
    )
    record = await sagas.recordActionFailure({
      attemptId: bAttempt,
      effect,
      errorChecksum: `${bAttempt}:error`,
      evidenceChecksum: `${bAttempt}:outcome`,
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      sagaId: run.sagaId,
      serverTimeMs: 1_005,
      stepId: "b",
    })
    expect(record.status).toBe("compensating")

    const compensationAttempt = `${run.sagaId}:a:compensation:1`
    effect = await run.begin(
      "a:compensation",
      compensationAttempt,
      `${run.sagaId}:a:compensation:begin`,
    )
    record = (
      await sagas.beginAction({
        attemptId: compensationAttempt,
        effect,
        evidenceChecksum: `${compensationAttempt}:accepted`,
        idempotencyKey: record.steps.a?.compensation.idempotencyKey as string,
        phase: "compensation",
        sagaId: run.sagaId,
        serverTimeMs: 1_006,
        stepId: "a",
      })
    ).saga
    effect = await run.complete(
      "a:compensation",
      compensationAttempt,
      `${compensationAttempt}:result`,
      `${run.sagaId}:a:compensation:success`,
    )
    record = await sagas.recordActionSuccess({
      attemptId: compensationAttempt,
      effect,
      evidenceChecksum: `${compensationAttempt}:outcome`,
      phase: "compensation",
      resultChecksum: `${compensationAttempt}:result`,
      sagaId: run.sagaId,
      serverTimeMs: 1_007,
      stepId: "a",
    })
    expect(record).toMatchObject({ stateVersion: 6, status: "failed" })
    expect(
      database.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_effects" WHERE "resource_kind" = 'saga'`,
        )
        .get(),
    ).toEqual({ count: 7 })

    await leases.release({ proof: run.proof() })
    await expect(
      sagas.create({
        deadlineAtMs: 10_000,
        descriptor: record.descriptor,
        effect: createEffect,
        evidenceChecksum: `${run.sagaId}:create:evidence`,
        idempotencyKey: record.idempotencyKey,
        inputChecksum: record.inputChecksum,
        sagaId: run.sagaId,
        serverTimeMs: 1_000,
        stepInputChecksums: { a: `${run.sagaId}:a:input`, b: `${run.sagaId}:b:input` },
      }),
    ).resolves.toEqual(record)
  })

  it("durably cancels an unknown action, then records observation as not applied", async () => {
    const run = await fixture("cancel-unknown", ["a"])
    let { record } = await run.initialize()
    const attemptId = `${run.sagaId}:a:forward:1`
    let effect = await run.begin("a:forward", attemptId, `${attemptId}:begin`)
    record = (
      await sagas.beginAction({
        attemptId,
        effect,
        evidenceChecksum: `${attemptId}:accepted`,
        idempotencyKey: record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga
    effect = await run.fail(
      "a:forward",
      attemptId,
      "unknown-error",
      "unknown",
      `${attemptId}:unknown`,
    )
    record = await sagas.recordActionFailure({
      attemptId,
      effect,
      errorChecksum: "unknown-error",
      evidenceChecksum: "unknown-outcome",
      outcome: "unknown",
      phase: "forward",
      sagaId: run.sagaId,
      serverTimeMs: 1_002,
      stepId: "a",
    })

    await run.begin("termination", `${run.sagaId}:termination:1`, "termination-begin")
    effect = await run.complete(
      "termination",
      `${run.sagaId}:termination:1`,
      "termination-result",
      "termination-cancel",
    )
    record = await sagas.requestTermination({
      cause: "cancellation",
      effect,
      evidenceChecksum: "cancellation-request",
      sagaId: run.sagaId,
      serverTimeMs: 1_003,
    })
    expect(record.status).toBe("compensating")

    await operations.reconcileStep({
      actorChecksum: "saga-test-actor",
      evidenceChecksum: "not-applied-evidence",
      operationId: run.operationId,
      outcome: "not_applied",
      proof: run.proof(),
      reconciliationId: "a-forward-observation",
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    effect = run.context(
      "a-forward-observed-not-applied",
      "a:forward",
      operationTransitionIdentity("reconciled", [
        run.operationId,
        sagaActionOperationStepId("a", "forward"),
        "a-forward-observation",
      ]),
    )
    record = await sagas.recordObservation({
      effect,
      evidenceChecksum: "observation-outcome-receipt",
      observationEvidenceChecksum: "not-applied-evidence",
      outcome: "not_applied",
      phase: "forward",
      sagaId: run.sagaId,
      serverTimeMs: 1_004,
      stepId: "a",
    })
    expect(record.status).toBe("cancelled")

    const unusedEffect = { ...effect, effectId: "terminal-cancel-noop" }
    await expect(
      sagas.requestTermination({
        cause: "timeout",
        effect: unusedEffect,
        evidenceChecksum: "ignored-timeout",
        sagaId: run.sagaId,
        serverTimeMs: 10_000,
      }),
    ).resolves.toEqual(record)
  })

  it("recovers crashes from durable dispatch presence and absence evidence", async () => {
    const unknownRun = await fixture("recover-unknown", ["a"])
    let { record } = await unknownRun.initialize()
    const attemptId = `${unknownRun.sagaId}:a:forward:1`
    let effect = await unknownRun.begin("a:forward", attemptId, `${attemptId}:begin`)
    record = (
      await sagas.beginAction({
        attemptId,
        effect,
        evidenceChecksum: `${attemptId}:accepted`,
        idempotencyKey: record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: unknownRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga
    await unknownRun.reacquire()
    await operations.recoverRunningStep({
      actorChecksum: "saga-test-actor",
      operationId: unknownRun.operationId,
      proof: unknownRun.proof(),
      recoveryId: "forward-crash",
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    effect = unknownRun.context(
      "forward-crash-unknown",
      "a:forward",
      operationTransitionIdentity("crash-recovered", [
        unknownRun.operationId,
        sagaActionOperationStepId("a", "forward"),
        "forward-crash",
      ]),
    )
    record = await sagas.markRunningActionUnknown({
      attemptId,
      effect,
      errorChecksum: "crash-unknown",
      evidenceChecksum: "crash-recovery-receipt",
      phase: "forward",
      sagaId: unknownRun.sagaId,
      stepId: "a",
    })
    expect(record.steps.a?.forward.state).toBe("unknown")

    await operations.reconcileStep({
      actorChecksum: "saga-test-actor",
      evidenceChecksum: "observed-applied",
      observedPostconditionChecksum: `${unknownRun.operationId}:${sagaActionOperationStepId("a", "forward")}:postcondition`,
      operationId: unknownRun.operationId,
      outcome: "applied",
      proof: unknownRun.proof(),
      reconciliationId: "forward-observation",
      resultChecksum: "forward-result",
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    effect = unknownRun.context(
      "forward-observed-applied",
      "a:forward",
      operationTransitionIdentity("reconciled", [
        unknownRun.operationId,
        sagaActionOperationStepId("a", "forward"),
        "forward-observation",
      ]),
    )
    record = await sagas.recordObservation({
      effect,
      evidenceChecksum: "observation-receipt",
      observationEvidenceChecksum: "observed-applied",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "forward-result",
      sagaId: unknownRun.sagaId,
      serverTimeMs: 1_002,
      stepId: "a",
    })
    expect(record.status).toBe("succeeded")

    const absentRun = await fixture("recover-not-dispatched", ["a"], true)
    let absent = (await absentRun.initialize()).record
    const absentAttemptId = `${absentRun.sagaId}:a:forward:1`
    effect = await absentRun.begin("a:forward", absentAttemptId, `${absentAttemptId}:begin`)
    absent = (
      await sagas.beginAction({
        attemptId: absentAttemptId,
        effect,
        evidenceChecksum: `${absentAttemptId}:accepted`,
        idempotencyKey: absent.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: absentRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga
    await absentRun.reacquire()
    await operations.recoverRunningStep({
      actorChecksum: "saga-test-actor",
      operationId: absentRun.operationId,
      proof: absentRun.proof(),
      recoveryId: "dispatch-absence",
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    effect = absentRun.context(
      "forward-not-dispatched",
      "a:forward",
      operationTransitionIdentity("crash-recovered", [
        absentRun.operationId,
        sagaActionOperationStepId("a", "forward"),
        "dispatch-absence",
      ]),
    )
    absent = await sagas.markActionNotDispatched({
      attemptId: absentAttemptId,
      effect,
      errorChecksum: "dispatch-absence-evidence",
      evidenceChecksum: "not-dispatched-receipt",
      phase: "forward",
      sagaId: absentRun.sagaId,
      serverTimeMs: 1_002,
      stepId: "a",
    })
    expect(absent.steps.a?.forward.state).toBe("retryable_failed")

    const retryRun = await fixture("retryable-failure", ["a"])
    let retry = (await retryRun.initialize()).record
    const retryAttempt = `${retryRun.sagaId}:a:forward:1`
    effect = await retryRun.begin("a:forward", retryAttempt, `${retryAttempt}:begin`)
    retry = (
      await sagas.beginAction({
        attemptId: retryAttempt,
        effect,
        evidenceChecksum: `${retryAttempt}:accepted`,
        idempotencyKey: retry.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: retryRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga
    effect = await retryRun.fail(
      "a:forward",
      retryAttempt,
      "retryable-error",
      "definitely_not_applied",
      "retryable-failure",
    )
    retry = await sagas.recordActionFailure({
      attemptId: retryAttempt,
      effect,
      errorChecksum: "retryable-error",
      evidenceChecksum: "retryable-outcome",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      sagaId: retryRun.sagaId,
      serverTimeMs: 1_002,
      stepId: "a",
    })
    expect(retry.steps.a?.forward.state).toBe("retryable_failed")

    const indeterminateRun = await fixture("indeterminate-observation", ["a"])
    let indeterminate = (await indeterminateRun.initialize()).record
    const indeterminateAttempt = `${indeterminateRun.sagaId}:a:forward:1`
    effect = await indeterminateRun.begin(
      "a:forward",
      indeterminateAttempt,
      `${indeterminateAttempt}:begin`,
    )
    indeterminate = (
      await sagas.beginAction({
        attemptId: indeterminateAttempt,
        effect,
        evidenceChecksum: `${indeterminateAttempt}:accepted`,
        idempotencyKey: indeterminate.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: indeterminateRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga
    effect = await indeterminateRun.fail(
      "a:forward",
      indeterminateAttempt,
      "unknown-before-observation",
      "unknown",
      "unknown-before-observation",
    )
    indeterminate = await sagas.recordActionFailure({
      attemptId: indeterminateAttempt,
      effect,
      errorChecksum: "unknown-before-observation",
      evidenceChecksum: "unknown-before-observation-receipt",
      outcome: "unknown",
      phase: "forward",
      sagaId: indeterminateRun.sagaId,
      serverTimeMs: 1_002,
      stepId: "a",
    })
    await operations.reconcileStep({
      actorChecksum: "saga-test-actor",
      evidenceChecksum: "indeterminate-evidence",
      operationId: indeterminateRun.operationId,
      outcome: "indeterminate",
      proof: indeterminateRun.proof(),
      reconciliationId: "indeterminate-observation",
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    effect = indeterminateRun.context(
      "indeterminate-observation",
      "a:forward",
      operationTransitionIdentity("reconciled", [
        indeterminateRun.operationId,
        sagaActionOperationStepId("a", "forward"),
        "indeterminate-observation",
      ]),
    )
    indeterminate = await sagas.recordObservation({
      effect,
      evidenceChecksum: "indeterminate-observation-receipt",
      observationEvidenceChecksum: "indeterminate-evidence",
      outcome: "indeterminate",
      phase: "forward",
      sagaId: indeterminateRun.sagaId,
      serverTimeMs: 1_003,
      stepId: "a",
    })
    expect(indeterminate.status).toBe("intervention_required")
  })

  it("rejects invalid contexts, immutable identity conflicts, and expired transition authority", async () => {
    expect(() => new D1SagaStore(null as never, digest)).toThrow(/transactional/u)
    expect(() => new D1SagaStore(database, null as never)).toThrow(/digest/u)
    expect(() => sagaActionOperationStepId("a", "other" as never)).toThrow(/unsupported/u)
    await expect(sagas.get("")).rejects.toThrow(/non-empty/u)
    await expect(sagas.get("x".repeat(513))).rejects.toThrow(/identity limit/u)

    const run = await fixture("validation", ["a"])
    const initialized = await run.initialize()
    const createInput = {
      deadlineAtMs: initialized.record.deadlineAtMs,
      descriptor: initialized.descriptor,
      effect: initialized.effect,
      evidenceChecksum: `${run.sagaId}:create:evidence`,
      idempotencyKey: initialized.record.idempotencyKey,
      inputChecksum: initialized.record.inputChecksum,
      sagaId: run.sagaId,
      serverTimeMs: 1_000,
      stepInputChecksums: { a: `${run.sagaId}:a:input` },
    } as const

    await expect(
      sagas.create({
        ...createInput,
        effect: { ...initialized.effect, effectId: "alternate-create-effect" },
      }),
    ).resolves.toEqual(initialized.record)
    await expect(
      sagas.create({
        ...createInput,
        effect: {
          ...initialized.effect,
          effectId: "wrong-operation-effect",
          operationId: "wrong-operation",
        },
      }),
    ).rejects.toThrow(/contradictory immutable intent/u)
    await expect(
      sagas.create({
        ...createInput,
        effect: { ...initialized.effect, effectId: "different-input-effect" },
        inputChecksum: "different-input",
      }),
    ).rejects.toThrow(/contradictory immutable intent/u)
    await expect(
      sagas.create({ ...createInput, evidenceChecksum: "different-evidence" }),
    ).rejects.toThrow(/immutable receipt/u)

    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: { ...initialized.effect, effectId: "" },
        evidenceChecksum: "cancellation",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/non-empty/u)
    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: {
          ...initialized.effect,
          effectId: "bad-fence",
          proof: { ...initialized.effect.proof, fencingToken: 0 },
        },
        evidenceChecksum: "cancellation",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/positive safe integer/u)
    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: { ...initialized.effect, effectId: "empty-evidence" },
        evidenceChecksum: "",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/non-empty/u)

    const missingContext: SagaEffectContext = {
      effectId: "missing-effect",
      operationId: "missing-operation",
      proof: {
        acquisitionId: "missing-acquisition",
        fencingToken: 1,
        holderId: "missing-holder",
        leaseKey: "missing-lease",
      },
      stepId: "missing-step",
      transitionId: "missing-transition",
    }
    await expect(
      sagas.beginAction({
        attemptId: "missing-attempt",
        effect: missingContext,
        evidenceChecksum: "missing-evidence",
        idempotencyKey: "missing-key",
        phase: "forward",
        sagaId: "missing-saga",
        serverTimeMs: 1_000,
        stepId: "missing-step",
      }),
    ).rejects.toThrow(/does not exist/u)
    await expect(
      sagas.recordActionSuccess({
        attemptId: "missing-attempt",
        effect: missingContext,
        evidenceChecksum: "missing-evidence",
        phase: "forward",
        resultChecksum: "missing-result",
        sagaId: "missing-saga",
        serverTimeMs: 1_000,
        stepId: "missing-step",
      }),
    ).rejects.toThrow(/does not exist/u)

    const terminationAttempt = `${run.sagaId}:termination:1`
    await run.begin("termination", terminationAttempt, "expired-termination-begin")
    const terminationEffect = await run.complete(
      "termination",
      terminationAttempt,
      "termination-result",
      "expired-termination",
    )
    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: {
          ...terminationEffect,
          effectId: "wrong-canonical-step",
          stepId: SAGA_INIT_OPERATION_STEP_ID,
        },
        evidenceChecksum: "wrong-canonical-step",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/wrong canonical operation step/u)
    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: {
          ...terminationEffect,
          effectId: "wrong-predecessor-operation",
          operationId: "other-operation",
        },
        evidenceChecksum: "wrong-predecessor",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/contradicts its durable predecessor/u)
    await leases.release({ proof: run.proof() })
    await expect(
      sagas.requestTermination({
        cause: "cancellation",
        effect: terminationEffect,
        evidenceChecksum: "expired-cancellation",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
      }),
    ).rejects.toThrow(/exact transition under the active lease/u)
  })

  it("fails closed on malformed saga projections and missing receipt joins", async () => {
    const run = await fixture("projection-fault", ["a"])
    await run.initialize()
    const row = rawSaga(database.database, run.sagaId)
    for (const [change, message] of [
      [{ saga_id: null }, /columns are malformed/u],
      [{ record_json: null }, /record JSON is malformed/u],
      [{ record_json: "{" }, /record JSON is invalid/u],
      [{ record_json: ` ${row.record_json as string}` }, /record JSON is not canonical/u],
      [{ descriptor_json: null }, /descriptor JSON is malformed/u],
      [{ descriptor_json: "{" }, /descriptor JSON is invalid/u],
      [
        { descriptor_json: ` ${row.descriptor_json as string}` },
        /descriptor JSON is not canonical/u,
      ],
      [{ status: "succeeded" }, /columns contradict/u],
      [{ record_checksum: "wrong" }, /columns contradict/u],
      [{ effect_id: null }, /lacks its exact operation-effect receipt/u],
    ] as const) {
      const faulted = new D1SagaStore(
        new FaultDatabase(database, { sagaRow: { ...row, ...change } }),
        digest,
      )
      await expect(faulted.get(run.sagaId)).rejects.toThrow(message)
    }
  })

  it("rejects malformed, contradictory, stale, or unprojected saga effect receipts", async () => {
    const run = await fixture("receipt-fault", ["a"])
    const initialized = await run.initialize()
    const sagaRow = rawSaga(database.database, run.sagaId)
    const effectRow = rawEffect(database.database, initialized.effect.effectId)
    const createInput = {
      deadlineAtMs: initialized.record.deadlineAtMs,
      descriptor: initialized.descriptor,
      effect: initialized.effect,
      evidenceChecksum: `${run.sagaId}:create:evidence`,
      idempotencyKey: initialized.record.idempotencyKey,
      inputChecksum: initialized.record.inputChecksum,
      sagaId: run.sagaId,
      serverTimeMs: 1_000,
      stepInputChecksums: { a: `${run.sagaId}:a:input` },
    } as const

    for (const [change, message] of [
      [{ effect_id: null }, /receipt is malformed/u],
      [{ record_json: null }, /effect record JSON is malformed/u],
      [{ record_json: "{" }, /effect record JSON is invalid/u],
      [{ record_json: ` ${effectRow.record_json as string}` }, /not canonical/u],
      [{ resource_id: "other-saga" }, /contradicts its canonical record/u],
      [{ record_checksum: "wrong" }, /contradicts its canonical record/u],
    ] as const) {
      const faulted = new D1SagaStore(
        new FaultDatabase(database, { effectRow: { ...effectRow, ...change } }),
        digest,
      )
      await expect(faulted.create(createInput)).rejects.toThrow(message)
    }

    const unprojected = new D1SagaStore(
      new FaultDatabase(database, { effectRow, sagaRow: null }),
      digest,
    )
    await expect(unprojected.create(createInput)).rejects.toThrow(/not reflected/u)

    const attemptId = `${run.sagaId}:a:forward:1`
    const beginEffect = await run.begin("a:forward", attemptId, `${attemptId}:begin`)
    const begun = await sagas.beginAction({
      attemptId,
      effect: beginEffect,
      evidenceChecksum: `${attemptId}:accepted`,
      idempotencyKey: initialized.record.steps.a?.forward.idempotencyKey as string,
      phase: "forward",
      sagaId: run.sagaId,
      serverTimeMs: 1_001,
      stepId: "a",
    })
    const beginEffectRow = rawEffect(database.database, beginEffect.effectId)
    const stale = new D1SagaStore(
      new FaultDatabase(database, { effectRow: beginEffectRow, sagaRow }),
      digest,
    )
    await expect(
      stale.beginAction({
        attemptId,
        effect: beginEffect,
        evidenceChecksum: `${attemptId}:accepted`,
        idempotencyKey: initialized.record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      }),
    ).rejects.toThrow(/not reflected/u)

    const altered = JSON.parse(beginEffectRow.record_json as string) as Record<string, unknown>
    altered.inputChecksum = "contradictory-input"
    const alteredJson = JSON.stringify(altered)
    const alteredEffect = {
      ...beginEffectRow,
      record_checksum: await sagaRecordChecksum(alteredJson),
      record_json: alteredJson,
    }
    const contradictory = new D1SagaStore(
      new FaultDatabase(database, {
        effectRow: alteredEffect,
        sagaRow: rawSaga(database.database, run.sagaId),
      }),
      digest,
    )
    await expect(
      contradictory.beginAction({
        attemptId,
        effect: beginEffect,
        evidenceChecksum: `${attemptId}:accepted`,
        idempotencyKey: begun.saga.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: run.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      }),
    ).rejects.toThrow(/contradicts its current projection/u)
  })

  it("fails closed on missing transitions and every partial saga mutation outcome", async () => {
    const transitionRun = await fixture("transition-fault", ["a"])
    const initialized = await transitionRun.initialize()
    const attemptId = `${transitionRun.sagaId}:a:forward:1`
    const effect = await transitionRun.begin("a:forward", attemptId, `${attemptId}:begin`)
    const input = {
      attemptId,
      effect,
      evidenceChecksum: `${attemptId}:accepted`,
      idempotencyKey: initialized.record.steps.a?.forward.idempotencyKey as string,
      phase: "forward" as const,
      sagaId: transitionRun.sagaId,
      serverTimeMs: 1_001,
      stepId: "a",
    }

    const missingTransition = new D1SagaStore(
      new FaultDatabase(database, { transitionRow: null }),
      digest,
    )
    await expect(missingTransition.beginAction(input)).rejects.toThrow(/exact transition/u)
    const contradictoryTransition = new D1SagaStore(
      new FaultDatabase(database, { transitionRow: { transition_id: "other" } }),
      digest,
    )
    await expect(contradictoryTransition.beginAction(input)).rejects.toThrow(
      /contradictory saga transition/u,
    )
    for (const [transitionRow, message] of [
      [
        { to_record_json: "{", transition_id: effect.transitionId },
        /transition record is invalid JSON/u,
      ],
      [
        { to_record_json: "[]", transition_id: effect.transitionId },
        /transition record is malformed/u,
      ],
      [
        { to_record_json: '{"state":"failed"}', transition_id: effect.transitionId },
        /contradicts its canonical operation-step transition/u,
      ],
      [
        {
          to_record_json: '{"activeAttemptId":"wrong","lastAttemptId":"wrong","state":"running"}',
          transition_id: effect.transitionId,
        },
        /contradicts its canonical operation-step transition/u,
      ],
    ] as const) {
      const faulted = new D1SagaStore(new FaultDatabase(database, { transitionRow }), digest)
      await expect(faulted.beginAction(input)).rejects.toThrow(message)
    }

    for (const [suffix, faults, message] of [
      ["incomplete", { batchResults: [] }, /incomplete saga mutation batch/u],
      [
        "metadata",
        {
          batchResults: [
            { meta: { changes: 1 }, success: false },
            { meta: { changes: 1 }, success: true },
          ],
        },
        /malformed saga mutation metadata/u,
      ],
      [
        "missing-receipt",
        {
          batchResults: [
            { meta: { changes: 1 }, success: true },
            { meta: { changes: 1 }, success: true },
          ],
        },
        /operation-effect receipt is missing/u,
      ],
      ["throw", { batchMode: "throw" }, /injected saga batch failure/u],
    ] as const) {
      const run = await fixture(`batch-${suffix}`, ["a"])
      const created = await run.initialize()
      const actionAttemptId = `${run.sagaId}:a:forward:1`
      const actionEffect = await run.begin("a:forward", actionAttemptId, `${actionAttemptId}:begin`)
      const faulted = new D1SagaStore(new FaultDatabase(database, faults), digest)
      await expect(
        faulted.beginAction({
          attemptId: actionAttemptId,
          effect: actionEffect,
          evidenceChecksum: `${actionAttemptId}:accepted`,
          idempotencyKey: created.record.steps.a?.forward.idempotencyKey as string,
          phase: "forward",
          sagaId: run.sagaId,
          serverTimeMs: 1_001,
          stepId: "a",
        }),
      ).rejects.toThrow(message)
    }
  })

  it("returns a committed winner after a lost response and detects split effect batches", async () => {
    const committedRun = await fixture("commit-then-throw", ["a"])
    const committedInitial = await committedRun.initialize()
    const committedAttempt = `${committedRun.sagaId}:a:forward:1`
    const committedEffect = await committedRun.begin(
      "a:forward",
      committedAttempt,
      `${committedAttempt}:begin`,
    )
    const commitThenThrow = new D1SagaStore(
      new FaultDatabase(database, { batchMode: "commit_then_throw" }),
      digest,
    )
    await expect(
      commitThenThrow.beginAction({
        attemptId: committedAttempt,
        effect: committedEffect,
        evidenceChecksum: `${committedAttempt}:accepted`,
        idempotencyKey: committedInitial.record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: committedRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      }),
    ).resolves.toMatchObject({ disposition: "execute", saga: { stateVersion: 1 } })

    const splitRun = await fixture("effect-only-update", ["a"])
    const splitInitial = await splitRun.initialize()
    const splitAttempt = `${splitRun.sagaId}:a:forward:1`
    const splitEffect = await splitRun.begin("a:forward", splitAttempt, `${splitAttempt}:begin`)
    const effectOnly = new D1SagaStore(
      new FaultDatabase(database, { batchMode: "effect_only" }),
      digest,
    )
    await expect(
      effectOnly.beginAction({
        attemptId: splitAttempt,
        effect: splitEffect,
        evidenceChecksum: `${splitAttempt}:accepted`,
        idempotencyKey: splitInitial.record.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId: splitRun.sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      }),
    ).rejects.toThrow(/concurrent saga mutation/u)

    const createRun = await fixture("effect-only-create", ["a"])
    const initAttempt = `${createRun.sagaId}:init:1`
    await createRun.begin("init", initAttempt, "split-create-begin")
    const createEffect = await createRun.complete(
      "init",
      initAttempt,
      "split-create-result",
      "split-create-effect",
    )
    const descriptor = await sealSagaDescriptor(
      { descriptorId: "split-create", steps: [descriptorStep("a")], version: 1 },
      digest,
    )
    const createEffectOnly = new D1SagaStore(
      new FaultDatabase(database, { batchMode: "effect_only" }),
      digest,
    )
    await expect(
      createEffectOnly.create({
        deadlineAtMs: 10_000,
        descriptor,
        effect: createEffect,
        evidenceChecksum: "split-create-evidence",
        idempotencyKey: "split-create-key",
        inputChecksum: "split-create-input",
        sagaId: createRun.sagaId,
        serverTimeMs: 1_000,
        stepInputChecksums: { a: "split-create-step-input" },
      }),
    ).rejects.toThrow(/did not become durably visible/u)
  })
})
