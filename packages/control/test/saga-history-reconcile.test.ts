import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  appendAuditEvent,
  type DigestFunction,
  type LeaseProof,
  leaseProof,
  MAX_SAGA_STEPS,
  type OperationPlan,
  type SagaDescriptor,
  sealIrreversibleAuthorization,
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
import { D1LeaseStore } from "../src/lease-store.js"
import { createInternalSagaOperationStore } from "../src/operation-store.js"
import { D1SagaAttemptStore } from "../src/saga-attempt-store.js"
import { D1SagaCoordinatorStore } from "../src/saga-coordinator-store.js"
import {
  D1SagaHistoryReader,
  type SagaHistoryAnchor,
  type SagaHistoryAttemptCursor,
  type SagaHistoryTransitionCursor,
} from "../src/saga-history.js"
import {
  finalizeSagaHistoryProof,
  reconcileSagaHistory,
  SagaHistoryAttemptFolder,
  SagaHistoryAuditFolder,
  SagaHistoryEffectFolder,
  SagaHistoryTransitionFolder,
} from "../src/saga-history-fold.js"
import { sealSagaInvocationInput } from "../src/saga-input.js"
import { sealSagaOperationPlan } from "../src/saga-plan.js"
import { type SagaHandlerRegistration, sealSagaHandlerRegistry } from "../src/saga-registry.js"
import { SAGA_INIT_OPERATION_STEP_ID, sagaActionOperationStepId } from "../src/saga-store.js"
import { loadSagaTerminalCapability, mintSagaTerminalCapability } from "../src/saga-terminal.js"
import { D1SagaTerminalStore } from "../src/saga-terminal-store.js"
import { controlSchemaSql } from "../src/schema.js"

const digest: DigestFunction = async (input) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", input.slice().buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
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

class FaultingBatchDatabase implements TransactionalControlDatabase {
  readonly #database: DatabaseAdapter
  readonly #failAt: number | undefined
  #failuresRemaining: number
  #lostResponsesRemaining: number

  constructor(
    database: DatabaseAdapter,
    input: {
      readonly failAt?: number
      readonly failures?: number
      readonly lostResponses?: number
    },
  ) {
    this.#database = database
    this.#failAt = input.failAt
    this.#failuresRemaining = input.failures ?? 0
    this.#lostResponsesRemaining = input.lostResponses ?? 0
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#failuresRemaining > 0 && this.#failAt !== undefined) {
      this.#failuresRemaining -= 1
      this.#database.database.exec("BEGIN IMMEDIATE;")
      try {
        const results: ControlRunResult[] = []
        for (const [index, statement] of statements.entries()) {
          if (index === this.#failAt) throw new Error("fictional terminal batch failure")
          results.push(await statement.run())
        }
        this.#database.database.exec("COMMIT;")
        return results
      } catch (error) {
        this.#database.database.exec("ROLLBACK;")
        throw error
      }
    }
    const results = await this.#database.batch(statements)
    if (this.#lostResponsesRemaining > 0) {
      this.#lostResponsesRemaining -= 1
      throw new Error("fictional lost terminal commit response")
    }
    return results
  }

  prepare(sql: string): ControlStatement {
    return this.#database.prepare(sql)
  }
}

class RewrittenBatchResultDatabase implements TransactionalControlDatabase {
  readonly #database: DatabaseAdapter
  readonly #rewrite: (results: readonly ControlRunResult[]) => readonly ControlRunResult[]

  constructor(
    database: DatabaseAdapter,
    rewrite: (results: readonly ControlRunResult[]) => readonly ControlRunResult[],
  ) {
    this.#database = database
    this.#rewrite = rewrite
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#rewrite(await this.#database.batch(statements))
  }

  prepare(sql: string): ControlStatement {
    return this.#database.prepare(sql)
  }
}

class SyntheticBatchResultDatabase implements TransactionalControlDatabase {
  readonly #database: DatabaseAdapter
  readonly #results: readonly ControlRunResult[]

  constructor(database: DatabaseAdapter, results: readonly ControlRunResult[]) {
    this.#database = database
    this.#results = results
  }

  async batch(): Promise<readonly ControlRunResult[]> {
    return this.#results
  }

  prepare(sql: string): ControlStatement {
    return this.#database.prepare(sql)
  }
}

interface QueryHooks {
  readonly all?: (sql: string, result: ControlQueryResult<unknown>) => ControlQueryResult<unknown>
  readonly first?: (sql: string, result: unknown | null) => unknown | null
}

class HookedStatement implements ControlStatement {
  #delegate: ControlStatement
  readonly #hooks: QueryHooks
  readonly #sql: string

  constructor(delegate: ControlStatement, sql: string, hooks: QueryHooks) {
    this.#delegate = delegate
    this.#hooks = hooks
    this.#sql = sql
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    const result = await this.#delegate.all<T>()
    return (this.#hooks.all?.(this.#sql, result as ControlQueryResult<unknown>) ??
      result) as ControlQueryResult<T>
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#delegate = this.#delegate.bind(...values)
    return this
  }

  async first<T>(): Promise<T | null> {
    const result = await this.#delegate.first<T>()
    return (
      this.#hooks.first === undefined ? result : this.#hooks.first(this.#sql, result)
    ) as T | null
  }

  run(): Promise<ControlRunResult> {
    return this.#delegate.run()
  }
}

class HookedDatabase implements TransactionalControlDatabase {
  readonly #database: DatabaseAdapter
  readonly #hooks: QueryHooks

  constructor(database: DatabaseAdapter, hooks: QueryHooks) {
    this.#database = database
    this.#hooks = hooks
  }

  batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#database.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return new HookedStatement(this.#database.prepare(sql), sql, this.#hooks)
  }
}

class RecordingDatabase implements TransactionalControlDatabase {
  readonly batchStatementCounts: number[] = []
  readonly preparedSql: string[] = []
  readonly #database: DatabaseAdapter

  constructor(database: DatabaseAdapter) {
    this.#database = database
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    this.batchStatementCounts.push(statements.length)
    return this.#database.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    this.preparedSql.push(sql)
    return this.#database.prepare(sql)
  }
}

type Scenario =
  | "accepted_not_applied"
  | "compensated"
  | "compensation_observed"
  | "confirmed"
  | "failed"
  | "irreversible_confirmed"
  | "not_applied"
  | "not_dispatched"
  | "retry"
  | "terminated"
  | "terminated_retryable_crash"
  | "terminated_retryable_direct"
  | "terminated_retryable_observation"
  | "unknown_applied"
  | "unknown_indeterminate"

interface ScenarioFixture {
  readonly database: DatabaseAdapter
  readonly descriptor: SagaDescriptor
  readonly operationId: string
  readonly plan: OperationPlan
  readonly proof: LeaseProof
  readonly sagaId: string
}

async function descriptor(maxAttempts: number, irreversible: boolean): Promise<SagaDescriptor> {
  return sealSagaDescriptor(
    {
      descriptorId: "history-reconcile",
      steps: [
        {
          authorizationPolicyChecksum: irreversible ? "9".repeat(64) : null,
          baseRetryDelayMs: 0,
          compensationAction: irreversible
            ? null
            : {
                actionId: "history-reconcile.compensate",
                artifactChecksum: "a".repeat(64),
                version: 1,
              },
          compensationObservation: irreversible
            ? null
            : {
                actionId: "history-reconcile.observe-compensation",
                artifactChecksum: "b".repeat(64),
                version: 1,
              },
          forwardAction: {
            actionId: "history-reconcile.forward",
            artifactChecksum: "c".repeat(64),
            version: 1,
          },
          forwardObservation: {
            actionId: "history-reconcile.observe-forward",
            artifactChecksum: "d".repeat(64),
            version: 1,
          },
          inputSchemaChecksum: "e".repeat(64),
          irreversible,
          maxAttempts,
          maxRetryDelayMs: 0,
          outputSchemaChecksum: "f".repeat(64),
          stepId: "write",
          timeoutMs: 1_000,
        },
      ],
      version: 1,
    },
    digest,
  )
}

async function nextLease(leases: D1LeaseStore, current: LeaseProof, suffix: string) {
  const released = await leases.release({ proof: current })
  if (!released.released) throw new Error("Fixture lease was not released")
  const acquired = await leases.acquire({
    acquisitionId: `history-reconcile-acquisition-${suffix}`,
    holderId: `history-reconcile-holder-${suffix}`,
    leaseKey: current.leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Fixture lease was not reacquired")
  return leaseProof(acquired.record)
}

async function scenarioFixture(
  scenario: Scenario,
  options: { readonly initResultChecksum?: string } = {},
): Promise<ScenarioFixture> {
  const database = new DatabaseAdapter()
  const operationId = `history-reconcile-operation-${scenario}`
  const sagaId = `history-reconcile-saga-${scenario}`
  const leaseKey = `saga:${sagaId}`
  const maxAttempts = scenario === "retry" || scenario.startsWith("terminated_retryable_") ? 2 : 1
  const irreversible = scenario === "irreversible_confirmed"
  const sealedDescriptor = await descriptor(maxAttempts, irreversible)
  const step = sealedDescriptor.steps[0] as (typeof sealedDescriptor.steps)[number]
  const effectHandler = () => ({
    evidenceJson: "{}",
    outputJson: "{}",
    state: "confirmed" as const,
  })
  const observationHandler = () => ({
    evidenceJson: "{}",
    outputJson: "{}",
    state: "applied" as const,
  })
  const registrations: SagaHandlerRegistration[] = [
    { handler: effectHandler, kind: "effect", reference: step.forwardAction },
    { handler: observationHandler, kind: "observation", reference: step.forwardObservation },
  ]
  if (step.compensationAction !== null && step.compensationObservation !== null) {
    registrations.push(
      { handler: effectHandler, kind: "effect", reference: step.compensationAction },
      {
        handler: observationHandler,
        kind: "observation",
        reference: step.compensationObservation,
      },
    )
  }
  const registry = await sealSagaHandlerRegistry(registrations, digest)
  const invocation = await sealSagaInvocationInput(
    {
      descriptor: sealedDescriptor,
      inputJson: JSON.stringify({ scenario }),
      sagaId,
      stepInputJsons: { write: JSON.stringify({ value: scenario }) },
    },
    digest,
  )
  const capabilitySnapshotJson = JSON.stringify({ scenario, version: 1 })
  const plan = await sealSagaOperationPlan(
    {
      capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
      descriptor: sealedDescriptor,
      inputChecksum: invocation.inputChecksum,
      leaseKey,
      operationId,
      operationIdempotencyKey: `${operationId}:key`,
      registry,
      sagaId,
      stepInputChecksums: invocation.stepInputChecksums,
    },
    digest,
  )
  const operations = createInternalSagaOperationStore(database, digest)
  const leases = new D1LeaseStore(database)
  const attempts = new D1SagaAttemptStore(database, digest)
  const coordinator = new D1SagaCoordinatorStore(database, digest)
  await operations.create({
    actorChecksum: "history-reconcile-actor",
    capabilitySnapshotJson,
    environmentId: `history-reconcile-environment-${scenario}`,
    idempotencyScope: `history-reconcile-scope-${scenario}`,
    inputJson: invocation.operationInputJson,
    plan,
    requiredShardIds: [`history-reconcile-shard-${scenario}`],
  })
  const acquired = await leases.acquire({
    acquisitionId: "history-reconcile-acquisition-1",
    holderId: "history-reconcile-holder-1",
    leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Fixture lease was not acquired")
  let proof = leaseProof(acquired.record)
  const irreversibleAuthorization = irreversible
    ? await (async () => {
        const authorized = await leases.authorizeAt(proof)
        return sealIrreversibleAuthorization(
          plan,
          {
            actorChecksum: "history-reconcile-actor",
            authorizationId: `${sagaId}:authorization`,
            decisionChecksum: `${sagaId}:approved`,
            lease: authorized.record,
            leaseProof: proof,
            sealedAtServerTimeMs: authorized.serverTimeMs,
            stepId: sagaActionOperationStepId("write", "forward"),
          },
          digest,
        )
      })()
    : undefined

  const initPlan = plan.steps.find((candidate) => candidate.stepId === SAGA_INIT_OPERATION_STEP_ID)
  if (initPlan === undefined) throw new Error("Fixture initialization plan is missing")
  const initAttemptId = `${sagaId}:init`
  await operations.beginStep({
    actorChecksum: "history-reconcile-actor",
    attemptId: initAttemptId,
    idempotencyKey: initPlan.idempotencyKey,
    observedPreconditionChecksum: initPlan.preconditionChecksum,
    operationId,
    proof,
    stepId: initPlan.stepId,
  })
  await coordinator.initializeSaga({
    actorChecksum: "history-reconcile-actor",
    attemptId: initAttemptId,
    deadlineAtMs: 8_000_000_000_000_000,
    descriptor: sealedDescriptor,
    evidenceChecksum: `${sagaId}:init:evidence`,
    idempotencyKey: `${sagaId}:key`,
    inputChecksum: invocation.inputChecksum,
    observedPostconditionChecksum: initPlan.postconditionChecksum,
    operationId,
    proof,
    resultChecksum: options.initResultChecksum ?? `${sagaId}:init:result`,
    sagaId,
    stepInputChecksums: invocation.stepInputChecksums,
  })

  const begin = async (attemptId: string, phase: "compensation" | "forward" = "forward") => {
    const decision = await coordinator.beginAction({
      actorChecksum: "history-reconcile-actor",
      attemptId,
      ...(phase === "forward" && irreversibleAuthorization !== undefined
        ? { irreversibleAuthorization }
        : {}),
      operationId,
      phase,
      proof,
      sagaId,
      stepId: "write",
    })
    if (decision.disposition !== "execute") throw new Error("Fixture action did not begin")
  }
  const accept = (attemptId: string, phase: "compensation" | "forward" = "forward") =>
    attempts.accept({
      attemptId,
      inputJson:
        phase === "forward"
          ? (invocation.stepInputJsons.write as string)
          : JSON.stringify({ attemptId, compensate: true }),
      phase,
      proof,
      purpose: "effect",
      sagaId,
      sagaStepId: "write",
    })
  const settleEffect = (attemptId: string, phase: "compensation" | "forward" = "forward") =>
    coordinator.settleActionFromReceipt({
      actorChecksum: "history-reconcile-actor",
      attemptId,
      operationId,
      phase,
      proof,
      sagaId,
      stepId: "write",
    })
  const completeEffect = async (
    attemptId: string,
    state: "confirmed" | "failed" | "not_applied" | "unknown",
    phase: "compensation" | "forward" = "forward",
  ) => {
    await accept(attemptId, phase)
    if (state === "confirmed") {
      await attempts.complete({
        attemptId,
        evidenceJson: JSON.stringify({ attemptId, evidence: true }),
        outputJson: JSON.stringify({ attemptId, output: true }),
        proof,
        state,
      })
    } else {
      await attempts.complete({
        attemptId,
        errorJson: JSON.stringify({ attemptId, error: true }),
        evidenceJson: JSON.stringify({ attemptId, evidence: true }),
        proof,
        state,
      })
    }
    await settleEffect(attemptId, phase)
  }
  const observe = async (
    causalAttemptId: string,
    state: "confirmed" | "indeterminate" | "not_applied",
    suffix: string,
    phase: "compensation" | "forward" = "forward",
  ) => {
    proof = await nextLease(leases, proof, suffix)
    const attemptId = `${causalAttemptId}:observation`
    await attempts.accept({
      attemptId,
      inputJson: JSON.stringify({ causalAttemptId, observe: true }),
      phase,
      proof,
      purpose: "observation",
      sagaId,
      sagaStepId: "write",
    })
    if (state === "confirmed") {
      await attempts.complete({
        attemptId,
        evidenceJson: JSON.stringify({ attemptId, evidence: true }),
        outputJson: JSON.stringify({ attemptId, applied: true }),
        proof,
        state,
      })
    } else {
      await attempts.complete({
        attemptId,
        errorJson: JSON.stringify({ attemptId, state }),
        evidenceJson: JSON.stringify({ attemptId, evidence: true }),
        proof,
        state,
      })
    }
    await coordinator.settleObservationFromReceipt({
      actorChecksum: "history-reconcile-actor",
      attemptId,
      operationId,
      phase,
      proof,
      sagaId,
      stepId: "write",
    })
  }

  const terminate = () =>
    coordinator.requestTermination({
      actorChecksum: "history-reconcile-actor",
      cause: "cancellation",
      operationId,
      proof,
      requestChecksum: `${sagaId}:termination:checksum`,
      requestId: `${sagaId}:termination`,
      sagaId,
    })

  if (scenario === "terminated") {
    await terminate()
    return { database, descriptor: sealedDescriptor, operationId, plan, proof, sagaId }
  }

  const firstAttempt = `${sagaId}:write:1`
  await begin(firstAttempt)
  if (scenario === "not_dispatched") {
    proof = await nextLease(leases, proof, "2")
    await coordinator.recoverActionAfterCrash({
      actorChecksum: "history-reconcile-actor",
      attemptId: firstAttempt,
      operationId,
      phase: "forward",
      proof,
      recoveryId: `${firstAttempt}:recovery`,
      sagaId,
      stepId: "write",
    })
  } else if (scenario === "accepted_not_applied") {
    await accept(firstAttempt)
    proof = await nextLease(leases, proof, "2")
    await coordinator.recoverActionAfterCrash({
      actorChecksum: "history-reconcile-actor",
      attemptId: firstAttempt,
      operationId,
      phase: "forward",
      proof,
      recoveryId: `${firstAttempt}:recovery`,
      sagaId,
      stepId: "write",
    })
    await observe(firstAttempt, "not_applied", "3")
  } else if (scenario === "unknown_applied" || scenario === "unknown_indeterminate") {
    await completeEffect(firstAttempt, "unknown")
    await observe(firstAttempt, scenario === "unknown_applied" ? "confirmed" : "indeterminate", "2")
  } else if (scenario === "compensated" || scenario === "compensation_observed") {
    await accept(firstAttempt)
    await attempts.complete({
      attemptId: firstAttempt,
      evidenceJson: JSON.stringify({ attemptId: firstAttempt, evidence: true }),
      outputJson: JSON.stringify({ attemptId: firstAttempt, output: true }),
      proof,
      state: "confirmed",
    })
    await terminate()
    await settleEffect(firstAttempt)
    const compensationAttempt = `${sagaId}:write:compensation:1`
    await begin(compensationAttempt, "compensation")
    if (scenario === "compensation_observed") {
      await completeEffect(compensationAttempt, "unknown", "compensation")
      await observe(compensationAttempt, "confirmed", "2", "compensation")
    } else {
      await completeEffect(compensationAttempt, "confirmed", "compensation")
    }
  } else if (scenario === "retry") {
    await completeEffect(firstAttempt, "not_applied")
    const secondAttempt = `${sagaId}:write:2`
    await begin(secondAttempt)
    await completeEffect(secondAttempt, "confirmed")
  } else if (scenario === "terminated_retryable_direct") {
    await completeEffect(firstAttempt, "not_applied")
    await terminate()
  } else if (scenario === "terminated_retryable_crash") {
    proof = await nextLease(leases, proof, "2")
    await coordinator.recoverActionAfterCrash({
      actorChecksum: "history-reconcile-actor",
      attemptId: firstAttempt,
      operationId,
      phase: "forward",
      proof,
      recoveryId: `${firstAttempt}:recovery`,
      sagaId,
      stepId: "write",
    })
    await terminate()
  } else if (scenario === "terminated_retryable_observation") {
    await accept(firstAttempt)
    proof = await nextLease(leases, proof, "2")
    await coordinator.recoverActionAfterCrash({
      actorChecksum: "history-reconcile-actor",
      attemptId: firstAttempt,
      operationId,
      phase: "forward",
      proof,
      recoveryId: `${firstAttempt}:recovery`,
      sagaId,
      stepId: "write",
    })
    await observe(firstAttempt, "not_applied", "3")
    await terminate()
  } else if (scenario === "irreversible_confirmed") {
    await completeEffect(firstAttempt, "confirmed")
  } else {
    await completeEffect(firstAttempt, scenario)
  }

  return { database, descriptor: sealedDescriptor, operationId, plan, proof, sagaId }
}

async function maximumWidthScenarioFixture(): Promise<ScenarioFixture> {
  const database = new DatabaseAdapter()
  const operationId = "history-reconcile-operation-maximum-width"
  const sagaId = "history-reconcile-saga-maximum-width"
  const leaseKey = `saga:${sagaId}`
  const steps = Array.from({ length: MAX_SAGA_STEPS }, (_, index) => {
    const stepId = `step-${index.toString(10).padStart(3, "0")}`
    return {
      authorizationPolicyChecksum: null,
      baseRetryDelayMs: 0,
      compensationAction: {
        actionId: `history-reconcile.maximum.compensate.${stepId}`,
        artifactChecksum: "a".repeat(64),
        version: 1,
      },
      compensationObservation: {
        actionId: `history-reconcile.maximum.observe-compensation.${stepId}`,
        artifactChecksum: "b".repeat(64),
        version: 1,
      },
      forwardAction: {
        actionId: `history-reconcile.maximum.forward.${stepId}`,
        artifactChecksum: "c".repeat(64),
        version: 1,
      },
      forwardObservation: {
        actionId: `history-reconcile.maximum.observe-forward.${stepId}`,
        artifactChecksum: "d".repeat(64),
        version: 1,
      },
      inputSchemaChecksum: "e".repeat(64),
      irreversible: false,
      maxAttempts: 1,
      maxRetryDelayMs: 0,
      outputSchemaChecksum: "f".repeat(64),
      stepId,
      timeoutMs: 1_000,
    }
  })
  const sealedDescriptor = await sealSagaDescriptor(
    { descriptorId: "history-reconcile-maximum", steps, version: 1 },
    digest,
  )
  const effectHandler = () => ({
    evidenceJson: "{}",
    outputJson: "{}",
    state: "confirmed" as const,
  })
  const observationHandler = () => ({
    evidenceJson: "{}",
    outputJson: "{}",
    state: "applied" as const,
  })
  const registry = await sealSagaHandlerRegistry(
    sealedDescriptor.steps.flatMap((step) => [
      { handler: effectHandler, kind: "effect" as const, reference: step.forwardAction },
      {
        handler: observationHandler,
        kind: "observation" as const,
        reference: step.forwardObservation,
      },
      {
        handler: effectHandler,
        kind: "effect" as const,
        reference: step.compensationAction as NonNullable<typeof step.compensationAction>,
      },
      {
        handler: observationHandler,
        kind: "observation" as const,
        reference: step.compensationObservation as NonNullable<typeof step.compensationObservation>,
      },
    ]),
    digest,
  )
  const stepInputJsons = Object.fromEntries(
    sealedDescriptor.steps.map((step) => [step.stepId, JSON.stringify({ stepId: step.stepId })]),
  )
  const invocation = await sealSagaInvocationInput(
    {
      descriptor: sealedDescriptor,
      inputJson: JSON.stringify({ width: MAX_SAGA_STEPS }),
      sagaId,
      stepInputJsons,
    },
    digest,
  )
  const capabilitySnapshotJson = JSON.stringify({ maximumWidth: true, version: 1 })
  const plan = await sealSagaOperationPlan(
    {
      capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
      descriptor: sealedDescriptor,
      inputChecksum: invocation.inputChecksum,
      leaseKey,
      operationId,
      operationIdempotencyKey: `${operationId}:key`,
      registry,
      sagaId,
      stepInputChecksums: invocation.stepInputChecksums,
    },
    digest,
  )
  const operations = createInternalSagaOperationStore(database, digest)
  const leases = new D1LeaseStore(database)
  const coordinator = new D1SagaCoordinatorStore(database, digest)
  await operations.create({
    actorChecksum: "history-reconcile-maximum-actor",
    capabilitySnapshotJson,
    environmentId: "history-reconcile-environment-maximum-width",
    idempotencyScope: "history-reconcile-scope-maximum-width",
    inputJson: invocation.operationInputJson,
    plan,
    requiredShardIds: ["history-reconcile-shard-maximum-width"],
  })
  const acquired = await leases.acquire({
    acquisitionId: "history-reconcile-maximum-acquisition",
    holderId: "history-reconcile-maximum-holder",
    leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Maximum-width fixture lease was not acquired")
  const proof = leaseProof(acquired.record)
  const initPlan = plan.steps.find((step) => step.stepId === SAGA_INIT_OPERATION_STEP_ID)
  if (initPlan === undefined) throw new Error("Maximum-width initialization step is missing")
  const initAttemptId = `${sagaId}:init`
  await operations.beginStep({
    actorChecksum: "history-reconcile-maximum-actor",
    attemptId: initAttemptId,
    idempotencyKey: initPlan.idempotencyKey,
    observedPreconditionChecksum: initPlan.preconditionChecksum,
    operationId,
    proof,
    stepId: initPlan.stepId,
  })
  await coordinator.initializeSaga({
    actorChecksum: "history-reconcile-maximum-actor",
    attemptId: initAttemptId,
    deadlineAtMs: 8_000_000_000_000_000,
    descriptor: sealedDescriptor,
    evidenceChecksum: `${sagaId}:init:evidence`,
    idempotencyKey: `${sagaId}:key`,
    inputChecksum: invocation.inputChecksum,
    observedPostconditionChecksum: initPlan.postconditionChecksum,
    operationId,
    proof,
    resultChecksum: `${sagaId}:init:result`,
    sagaId,
    stepInputChecksums: invocation.stepInputChecksums,
  })
  await coordinator.requestTermination({
    actorChecksum: "history-reconcile-maximum-actor",
    cause: "cancellation",
    operationId,
    proof,
    requestChecksum: `${sagaId}:termination:checksum`,
    requestId: `${sagaId}:termination`,
    sagaId,
  })
  return { database, descriptor: sealedDescriptor, operationId, plan, proof, sagaId }
}

interface FoldedHistory {
  readonly anchor: SagaHistoryAnchor
  readonly attempt: SagaHistoryAttemptFolder
  readonly descriptor: SagaDescriptor
  readonly effect: SagaHistoryEffectFolder
  readonly plan: OperationPlan
  readonly reader: D1SagaHistoryReader
  readonly transition: SagaHistoryTransitionFolder
}

async function foldHistory(fixture: ScenarioFixture): Promise<FoldedHistory> {
  const reader = new D1SagaHistoryReader(fixture.database)
  const anchor = await reader.captureAnchor(fixture.operationId, fixture.sagaId)
  const plan = await reader.operationPlan(anchor, digest)
  const loadedDescriptor = await reader.sagaDescriptor(anchor, digest)
  const audit = new SagaHistoryAuditFolder(anchor, digest)
  let auditCursor = 0
  for (;;) {
    const page = await reader.auditPage(anchor, auditCursor)
    await audit.append(page)
    if (page.complete) break
    auditCursor = page.nextCursor as number
  }
  const transition = new SagaHistoryTransitionFolder(anchor, audit.proof(), plan, digest)
  let transitionCursor: SagaHistoryTransitionCursor | undefined
  for (;;) {
    const page = await reader.transitionPage(anchor, transitionCursor)
    await transition.append(page)
    if (page.complete) break
    transitionCursor = page.nextCursor as SagaHistoryTransitionCursor
  }
  const effect = new SagaHistoryEffectFolder(anchor, digest)
  let effectCursor: number | undefined
  for (;;) {
    const page = await reader.effectPage(anchor, effectCursor)
    await effect.append(page)
    if (page.complete) break
    effectCursor = page.nextCursor as number
  }
  const attempt = new SagaHistoryAttemptFolder(anchor, reader, digest)
  let attemptCursor: SagaHistoryAttemptCursor | undefined
  for (;;) {
    const page = await reader.attemptIdentityPage(anchor, attemptCursor)
    await attempt.append(page)
    if (page.complete) break
    attemptCursor = page.nextCursor as SagaHistoryAttemptCursor
  }
  return { anchor, attempt, descriptor: loadedDescriptor, effect, plan, reader, transition }
}

async function terminalCapability(fixture: ScenarioFixture) {
  const history = await foldHistory(fixture)
  const proof = await finalizeSagaHistoryProof(
    history.reader,
    history.anchor,
    history.transition,
    history.effect,
    history.attempt,
    history.plan,
    history.descriptor,
  )
  return mintSagaTerminalCapability(proof)
}

function terminalDatabaseSnapshot(fixture: ScenarioFixture) {
  const operation = fixture.database.database
    .prepare(`SELECT "status", "updated_at_ms" FROM "nozzle_operations" WHERE "operation_id" = ?`)
    .get(fixture.operationId) as { readonly status: string; readonly updated_at_ms: number }
  const transitions = fixture.database.database
    .prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_transitions"`)
    .get() as { readonly count: number }
  const audit = fixture.database.database
    .prepare(`SELECT count(*) AS "count" FROM "nozzle_audit_log"`)
    .get() as { readonly count: number }
  return { audit: audit.count, operation, transitions: transitions.count }
}

interface MutableReconciliationRows {
  readonly attempt: Record<string, unknown>[]
  readonly effect: Record<string, unknown>[]
  readonly transition: Record<string, unknown>[]
}

function matchingRow(
  rows: Record<string, unknown>[],
  predicate: (row: Record<string, unknown>) => boolean,
): Record<string, unknown> {
  const row = rows.find(predicate)
  if (row === undefined) throw new Error("Expected reconciliation fixture row is missing")
  return row
}

async function expectTamperedReconciliationRejected(
  fixture: ScenarioFixture,
  tamper: (rows: MutableReconciliationRows, history: FoldedHistory) => void,
): Promise<void> {
  const history = await foldHistory(fixture)
  const rows: MutableReconciliationRows = {
    attempt: structuredClone(history.attempt.reconciliationHistory()) as unknown as Record<
      string,
      unknown
    >[],
    effect: structuredClone(history.effect.reconciliationHistory()) as unknown as Record<
      string,
      unknown
    >[],
    transition: structuredClone(history.transition.reconciliationHistory()) as unknown as Record<
      string,
      unknown
    >[],
  }
  tamper(rows, history)
  Object.defineProperty(history.attempt, "reconciliationHistory", {
    value: () => rows.attempt,
  })
  Object.defineProperty(history.effect, "reconciliationHistory", {
    value: () => rows.effect,
  })
  Object.defineProperty(history.transition, "reconciliationHistory", {
    value: () => rows.transition,
  })
  await expect(
    reconcileSagaHistory(
      history.transition,
      history.effect,
      history.attempt,
      history.plan,
      history.descriptor,
    ),
  ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
}

describe("saga history cross-stream reconciliation", () => {
  it("reconciles direct, retry, recovery, observation, termination, and compensation histories", async () => {
    const scenarios: Scenario[] = [
      "confirmed",
      "irreversible_confirmed",
      "failed",
      "not_applied",
      "retry",
      "unknown_applied",
      "unknown_indeterminate",
      "accepted_not_applied",
      "not_dispatched",
      "terminated",
      "terminated_retryable_direct",
      "terminated_retryable_crash",
      "terminated_retryable_observation",
      "compensated",
      "compensation_observed",
    ]
    const outcomes = []
    for (const scenario of scenarios) {
      const fixture = await scenarioFixture(scenario)
      try {
        const history = await foldHistory(fixture)
        const proof = await finalizeSagaHistoryProof(
          history.reader,
          history.anchor,
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          history.descriptor,
        )
        const capability = mintSagaTerminalCapability(proof)
        const authority = loadSagaTerminalCapability(capability)
        const settled = await new D1SagaTerminalStore(fixture.database, digest).persistTerminalTail(
          {
            actorChecksum: "history-reconcile-terminal-actor",
            capability,
            proof: fixture.proof,
          },
        )
        const settledHistory = await foldHistory(fixture)
        const settledProof = await finalizeSagaHistoryProof(
          settledHistory.reader,
          settledHistory.anchor,
          settledHistory.transition,
          settledHistory.effect,
          settledHistory.attempt,
          settledHistory.plan,
          settledHistory.descriptor,
        )
        const settledAuthority = loadSagaTerminalCapability(
          mintSagaTerminalCapability(settledProof),
        )
        outcomes.push({ authority, capability, proof, settled, settledAuthority })
      } finally {
        fixture.database.close()
      }
    }
    expect(outcomes).toHaveLength(scenarios.length)
    expect(outcomes.every(({ proof }) => proof.schemaVersion === 1)).toBe(true)
    expect(outcomes.every(({ proof }) => Object.isFrozen(proof.anchor))).toBe(true)
    expect(
      outcomes.every(({ settledAuthority }) => settledAuthority.branchDecisions.length === 0),
    ).toBe(true)
    expect(outcomes.map(({ settled }) => settled.steps["saga:settle"]?.state)).toEqual([
      "succeeded",
      "succeeded",
      "failed",
      "failed",
      "succeeded",
      "succeeded",
      "intervention_required",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
    ])
    expect(outcomes.map(({ proof }) => proof.reconciliation.observationAttemptCount)).toEqual([
      0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1,
    ])
    expect(outcomes.map(({ authority }) => authority.settlementOutcome)).toEqual([
      "succeeded",
      "succeeded",
      "failed",
      "failed",
      "succeeded",
      "succeeded",
      "intervention_required",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
      "failed",
    ])
    expect(outcomes[1]?.authority).toMatchObject({
      branchDecisions: [{ kind: "not_required", stepId: "saga:termination" }],
      operation: {
        steps: {
          "saga:forward:write": {
            authorizationChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
            irreversibleAuthorization: { schemaVersion: 1 },
          },
        },
      },
    })
  })

  it("does not expose reconciliation inputs before every source fold is complete", async () => {
    const fixture = await scenarioFixture("confirmed")
    try {
      const reader = new D1SagaHistoryReader(fixture.database)
      const anchor = await reader.captureAnchor(fixture.operationId, fixture.sagaId)
      expect(() =>
        new SagaHistoryEffectFolder(anchor, digest).reconciliationHistory(),
      ).toThrowError(/requires more verified pages/u)
      expect(() =>
        new SagaHistoryAttemptFolder(anchor, reader, digest).reconciliationHistory(),
      ).toThrowError(/requires more verified pages/u)

      const audit = new SagaHistoryAuditFolder(anchor, digest)
      let cursor = 0
      for (;;) {
        const page = await reader.auditPage(anchor, cursor)
        await audit.append(page)
        if (page.complete) break
        cursor = page.nextCursor as number
      }
      const plan = await reader.operationPlan(anchor, digest)
      expect(() =>
        new SagaHistoryTransitionFolder(
          anchor,
          audit.proof(),
          plan,
          digest,
        ).reconciliationHistory(),
      ).toThrowError(/requires more verified pages/u)
    } finally {
      fixture.database.close()
    }
  })

  it("persists the complete maximum-width terminal tail in the same four statements", async () => {
    const fixture = await maximumWidthScenarioFixture()
    try {
      const capability = await terminalCapability(fixture)
      const authority = loadSagaTerminalCapability(capability)
      expect(fixture.plan.steps).toHaveLength(MAX_SAGA_STEPS * 2 + 3)
      expect(authority.branchDecisions).toHaveLength(MAX_SAGA_STEPS * 2)
      const before = terminalDatabaseSnapshot(fixture)
      const database = new RecordingDatabase(fixture.database)
      const settled = await new D1SagaTerminalStore(database, digest).persistTerminalTail({
        actorChecksum: "history-reconcile-maximum-terminal-actor",
        capability,
        proof: fixture.proof,
      })
      const after = terminalDatabaseSnapshot(fixture)
      expect(database.batchStatementCounts).toEqual([4])
      expect(database.preparedSql).toHaveLength(10)
      expect(
        Math.max(...database.preparedSql.map((sql) => new TextEncoder().encode(sql).byteLength)),
      ).toBeLessThanOrEqual(100_000)
      expect(
        Math.max(
          ...database.preparedSql.flatMap((sql) =>
            [...sql.matchAll(/\?([1-9][0-9]*)/gu)].map((match) => Number(match[1])),
          ),
        ),
      ).toBeLessThanOrEqual(100)
      expect(settled.steps["saga:settle"]?.state).toBe("failed")
      expect(after.transitions - before.transitions).toBe(MAX_SAGA_STEPS * 2 + 1)
      expect(after.audit - before.audit).toBe(MAX_SAGA_STEPS * 2 + 1)
      const finalCapability = await terminalCapability(fixture)
      expect(loadSagaTerminalCapability(finalCapability).branchDecisions).toEqual([])
    } finally {
      fixture.database.close()
    }
  })

  it("binds every folded head and performs the database re-read after reconciliation", async () => {
    const mismatched = await scenarioFixture("confirmed")
    try {
      const history = await foldHistory(mismatched)
      await expect(
        finalizeSagaHistoryProof(
          history.reader,
          {
            ...history.anchor,
            operationTransitionCount: history.anchor.operationTransitionCount + 1,
          },
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          history.descriptor,
        ),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: "The reconciled saga history contradicts its final anchor.",
      })
      await expect(
        finalizeSagaHistoryProof(
          {} as D1SagaHistoryReader,
          history.anchor,
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          history.descriptor,
        ),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    } finally {
      mismatched.database.close()
    }

    const advanced = await scenarioFixture("confirmed")
    try {
      const history = await foldHistory(advanced)
      const mutatingDescriptor = { ...history.descriptor }
      let mutated = false
      Object.defineProperty(mutatingDescriptor, "descriptorId", {
        enumerable: true,
        get: () => {
          if (mutated) return history.descriptor.descriptorId
          mutated = true
          advanced.database.database
            .prepare(
              `UPDATE "nozzle_operations"
               SET "updated_at_ms" = "updated_at_ms" + 1
               WHERE "operation_id" = ?`,
            )
            .run(advanced.operationId)
          return history.descriptor.descriptorId
        },
      })
      await expect(
        finalizeSagaHistoryProof(
          history.reader,
          history.anchor,
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          mutatingDescriptor,
        ),
      ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
      expect(mutated).toBe(true)
    } finally {
      advanced.database.close()
    }
  })

  it("mints opaque idempotent terminal authority only from a live final proof", async () => {
    const fixture = await scenarioFixture("confirmed")
    try {
      const history = await foldHistory(fixture)
      const proof = await finalizeSagaHistoryProof(
        history.reader,
        history.anchor,
        history.transition,
        history.effect,
        history.attempt,
        history.plan,
        history.descriptor,
      )
      const capability = mintSagaTerminalCapability(proof)
      expect(mintSagaTerminalCapability(proof)).toBe(capability)
      expect(Object.isFrozen(capability)).toBe(true)
      expect(Object.keys(capability)).toEqual([])

      const authority = loadSagaTerminalCapability(capability)
      expect(authority).toMatchObject({
        finalProof: proof,
        operation: { plan: { operationId: fixture.operationId } },
        saga: { sagaId: fixture.sagaId, status: "succeeded" },
        settlementOutcome: "succeeded",
      })
      expect(authority.branchDecisions).toMatchObject([
        { kind: "not_required", stepId: "saga:termination" },
        { kind: "not_required", stepId: "saga:compensation:write" },
      ])
      expect(Object.isFrozen(authority)).toBe(true)
      expect(Object.isFrozen(authority.branchDecisions)).toBe(true)

      expect(() => mintSagaTerminalCapability(structuredClone(proof))).toThrowError(
        /live complete-history proof/u,
      )
      expect(() => mintSagaTerminalCapability(null as never)).toThrowError(
        /live complete-history proof/u,
      )
      expect(() => loadSagaTerminalCapability(Object.freeze({}))).toThrowError(
        /verified authority/u,
      )
      expect(() => loadSagaTerminalCapability(null)).toThrowError(/verified authority/u)
    } finally {
      fixture.database.close()
    }
  })

  it("rejects every cross-stream set, descriptor, begin, and terminal-receipt contradiction", async () => {
    const fixture = await scenarioFixture("confirmed")
    try {
      const tamperers: Array<(rows: MutableReconciliationRows, history: FoldedHistory) => void> = [
        (rows) => {
          rows.transition.push({ ...(rows.transition[0] as Record<string, unknown>) })
        },
        (rows) => {
          rows.effect.pop()
        },
        (rows) => {
          const nonCoupled = matchingRow(
            rows.transition,
            (row) => !String(row.eventType).startsWith("saga."),
          )
          const creation = matchingRow(rows.effect, (row) => row.effectKind === "create")
          creation.transitionId = nonCoupled.transitionId
        },
        (rows) => {
          rows.attempt.push({ ...(rows.attempt[0] as Record<string, unknown>) })
        },
        (rows) => {
          matchingRow(rows.effect, (row) =>
            String(row.effectKind).endsWith(":begin"),
          ).actionAttemptId = null
        },
        (rows) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.actionKey = "contradictory-action"
        },
        (rows) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.holderId = "contradictory-holder"
        },
        (rows) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.outcomeChecksum = "contradictory-outcome"
        },
        (rows) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.state = "indeterminate"
        },
        (rows) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.sagaStepId = "missing-step"
        },
        (rows, history) => {
          const attempt = rows.attempt[0] as Record<string, unknown>
          attempt.phase = "compensation"
          const proof = structuredClone(history.effect.proof()) as unknown as {
            saga: { descriptor: { steps: Array<{ compensationAction: unknown }> } }
          }
          const descriptorStep = proof.saga.descriptor.steps[0] as {
            compensationAction: unknown
          }
          descriptorStep.compensationAction = null
          Object.defineProperty(history.effect, "proof", { value: () => proof })
        },
        (rows) => {
          matchingRow(rows.effect, (row) => row.effectKind === "create").phase = "forward"
        },
      ]
      for (const tamper of tamperers) {
        await expectTamperedReconciliationRejected(fixture, tamper)
      }
    } finally {
      fixture.database.close()
    }
  })

  it("rejects contradictory recovery and observation receipt mappings", async () => {
    const recovered = await scenarioFixture("accepted_not_applied")
    try {
      await expectTamperedReconciliationRejected(recovered, (rows) => {
        const accepted = matchingRow(rows.attempt, (row) => row.state === "accepted")
        accepted.acceptanceChecksum = "contradictory-acceptance"
      })
    } finally {
      recovered.database.close()
    }

    const observed = await scenarioFixture("unknown_applied")
    try {
      await expectTamperedReconciliationRejected(observed, (rows) => {
        const observation = matchingRow(rows.attempt, (row) => row.purpose === "observation")
        observation.state = "accepted"
      })
      await expectTamperedReconciliationRejected(observed, (rows) => {
        const observation = matchingRow(rows.attempt, (row) => row.purpose === "observation")
        observation.outcomeChecksum = "contradictory-observation"
      })
    } finally {
      observed.database.close()
    }
  })

  it("rejects cross-stream evidence mismatch, missing receipts, mixed proofs, and fake folders", async () => {
    const mismatch = await scenarioFixture("confirmed")
    try {
      mismatch.database.database.exec(`
        DROP TRIGGER "nozzle_control_operation_effect_update";
        UPDATE "nozzle_operation_effects"
        SET "evidence_checksum" = 'cross-stream-mismatch'
        WHERE "effect_kind" = 'action:forward:success';
      `)
      const history = await foldHistory(mismatch)
      await expect(
        reconcileSagaHistory(
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          history.descriptor,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    } finally {
      mismatch.database.close()
    }

    const missing = await scenarioFixture("confirmed")
    try {
      missing.database.database.exec(`
        DROP TRIGGER "nozzle_control_saga_outcome_delete";
        DROP TRIGGER "nozzle_control_saga_protocol_delete";
        DROP TRIGGER "nozzle_control_saga_attempt_delete";
        DELETE FROM "nozzle_saga_action_attempt_outcomes";
        DELETE FROM "nozzle_saga_action_attempt_protocols";
        DELETE FROM "nozzle_saga_action_attempts";
      `)
      const history = await foldHistory(missing)
      await expect(
        reconcileSagaHistory(
          history.transition,
          history.effect,
          history.attempt,
          history.plan,
          history.descriptor,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    } finally {
      missing.database.close()
    }

    const left = await scenarioFixture("confirmed")
    const right = await scenarioFixture("failed")
    try {
      const leftHistory = await foldHistory(left)
      const rightHistory = await foldHistory(right)
      await expect(
        reconcileSagaHistory(
          leftHistory.transition,
          rightHistory.effect,
          rightHistory.attempt,
          rightHistory.plan,
          rightHistory.descriptor,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      await expect(
        reconcileSagaHistory(
          {} as SagaHistoryTransitionFolder,
          rightHistory.effect,
          rightHistory.attempt,
          rightHistory.plan,
          rightHistory.descriptor,
        ),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    } finally {
      left.database.close()
      right.database.close()
    }
  })

  it("rolls back every terminal-tail statement and retries only from an unchanged anchor", async () => {
    for (let failAt = 0; failAt < 4; failAt += 1) {
      const fixture = await scenarioFixture("confirmed")
      try {
        const capability = await terminalCapability(fixture)
        const before = terminalDatabaseSnapshot(fixture)
        const database = new FaultingBatchDatabase(fixture.database, {
          failAt,
          failures: 16,
        })
        await expect(
          new D1SagaTerminalStore(database, digest).persistTerminalTail({
            actorChecksum: "history-reconcile-terminal-actor",
            capability,
            proof: fixture.proof,
          }),
        ).rejects.toMatchObject({
          code: "OperationInterventionRequiredError",
          message: expect.stringMatching(/bounded audit-race retry budget/u),
        })
        expect(terminalDatabaseSnapshot(fixture)).toEqual(before)
      } finally {
        fixture.database.close()
      }
    }

    const transient = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(transient)
      const settled = await new D1SagaTerminalStore(
        new FaultingBatchDatabase(transient.database, { failAt: 2, failures: 1 }),
        digest,
      ).persistTerminalTail({
        actorChecksum: "history-reconcile-terminal-actor",
        capability,
        proof: transient.proof,
      })
      expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
    } finally {
      transient.database.close()
    }
  })

  it("recovers lost responses, exact races, replay, and a newer lease owner", async () => {
    const fixture = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(fixture)
      const before = terminalDatabaseSnapshot(fixture)
      const lostResponseStore = new D1SagaTerminalStore(
        new FaultingBatchDatabase(fixture.database, { lostResponses: 1 }),
        digest,
      )
      const settled = await lostResponseStore.persistTerminalTail({
        actorChecksum: "history-reconcile-terminal-actor",
        capability,
        proof: fixture.proof,
      })
      expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
      const committed = terminalDatabaseSnapshot(fixture)
      expect(committed.transitions - before.transitions).toBe(3)
      expect(committed.audit - before.audit).toBe(3)

      const store = new D1SagaTerminalStore(fixture.database, digest)
      await expect(
        store.persistTerminalTail({
          actorChecksum: "contradictory-terminal-actor",
          capability,
          proof: fixture.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      expect(
        await store.persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: fixture.proof,
        }),
      ).toEqual(settled)
      expect(terminalDatabaseSnapshot(fixture)).toEqual(committed)

      const leases = new D1LeaseStore(fixture.database)
      await leases.release({ proof: fixture.proof })
      const acquired = await leases.acquire({
        acquisitionId: "history-reconcile-terminal-replay-acquisition",
        holderId: "history-reconcile-terminal-replay-holder",
        leaseKey: fixture.proof.leaseKey,
        ttlMs: 60_000,
      })
      if (!acquired.acquired) throw new Error("Terminal replay lease was not acquired")
      const newerProof = leaseProof(acquired.record)
      expect(
        await store.persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: newerProof,
        }),
      ).toEqual(settled)

      const finalCapability = await terminalCapability(fixture)
      expect(
        await store.persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability: finalCapability,
          proof: newerProof,
        }),
      ).toEqual(settled)
      expect(terminalDatabaseSnapshot(fixture)).toEqual(committed)
    } finally {
      fixture.database.close()
    }

    const raced = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(raced)
      const before = terminalDatabaseSnapshot(raced)
      const input = {
        actorChecksum: "history-reconcile-terminal-race-actor",
        capability,
        proof: raced.proof,
      }
      const [left, right] = await Promise.all([
        new D1SagaTerminalStore(raced.database, digest).persistTerminalTail(input),
        new D1SagaTerminalStore(raced.database, digest).persistTerminalTail(input),
      ])
      expect(left).toEqual(right)
      const after = terminalDatabaseSnapshot(raced)
      expect(after.transitions - before.transitions).toBe(3)
      expect(after.audit - before.audit).toBe(3)
    } finally {
      raced.database.close()
    }
  })

  it("rejects stale leases, changed anchors, projection corruption, and fake inputs without writes", async () => {
    const staleLease = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(staleLease)
      const before = terminalDatabaseSnapshot(staleLease)
      await new D1LeaseStore(staleLease.database).release({ proof: staleLease.proof })
      await expect(
        new D1SagaTerminalStore(staleLease.database, digest).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: staleLease.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
      expect(terminalDatabaseSnapshot(staleLease)).toMatchObject({
        audit: before.audit,
        transitions: before.transitions,
      })
    } finally {
      staleLease.database.close()
    }

    const changedAnchor = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(changedAnchor)
      const before = terminalDatabaseSnapshot(changedAnchor)
      changedAnchor.database.database
        .prepare(
          `UPDATE "nozzle_operations" SET "updated_at_ms" = "updated_at_ms" + 1
           WHERE "operation_id" = ?`,
        )
        .run(changedAnchor.operationId)
      await expect(
        new D1SagaTerminalStore(changedAnchor.database, digest).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: changedAnchor.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
      expect(terminalDatabaseSnapshot(changedAnchor)).toMatchObject({
        audit: before.audit,
        transitions: before.transitions,
      })
    } finally {
      changedAnchor.database.close()
    }

    const corrupted = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(corrupted)
      const before = terminalDatabaseSnapshot(corrupted)
      corrupted.database.database.exec(`DROP TRIGGER "nozzle_control_step_state_update";`)
      corrupted.database.database
        .prepare(
          `UPDATE "nozzle_operation_steps"
           SET "record_json" = json_set("record_json", '$.resultChecksum', 'corrupted-result')
           WHERE "operation_id" = ? AND "step_id" = 'saga:forward:write'`,
        )
        .run(corrupted.operationId)
      await expect(
        new D1SagaTerminalStore(corrupted.database, digest).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: corrupted.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      expect(terminalDatabaseSnapshot(corrupted)).toMatchObject({
        audit: before.audit,
        transitions: before.transitions,
      })
    } finally {
      corrupted.database.close()
    }

    const invalid = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(invalid)
      const store = new D1SagaTerminalStore(invalid.database, digest)
      expect(() => new D1SagaTerminalStore(null as never, digest)).toThrowError(
        /transactional Control D1 binding/u,
      )
      expect(() => new D1SagaTerminalStore(invalid.database, null as never)).toThrowError(
        /terminal digest/u,
      )
      for (const actorChecksum of ["", "x".repeat(513)]) {
        await expect(
          store.persistTerminalTail({ actorChecksum, capability, proof: invalid.proof }),
        ).rejects.toMatchObject({ code: "ConfigurationError" })
      }
      await expect(
        store.persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability: Object.freeze({}) as never,
          proof: invalid.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      await expect(
        store.persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: { ...invalid.proof, holderId: (() => "uncapturable") as never },
        }),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    } finally {
      invalid.database.close()
    }
  })

  it("recovers from untrusted batch metadata only through exact immutable receipts", async () => {
    const rewrites: Array<(results: readonly ControlRunResult[]) => readonly ControlRunResult[]> = [
      (results) => results.slice(0, 3),
      (results) => [
        { ...(results[0] as ControlRunResult), success: false },
        ...(results.slice(1) as ControlRunResult[]),
      ],
      (results) => [
        { ...results[0], meta: { changes: "malformed" } } as unknown as ControlRunResult,
        ...(results.slice(1) as ControlRunResult[]),
      ],
      (results) => [
        { ...(results[0] as ControlRunResult), meta: { changes: 1 } },
        ...(results.slice(1) as ControlRunResult[]),
      ],
      (results) => results.map((result) => ({ ...result, meta: { changes: 0 } })),
    ]
    for (const rewrite of rewrites) {
      const fixture = await scenarioFixture("confirmed")
      try {
        const capability = await terminalCapability(fixture)
        const settled = await new D1SagaTerminalStore(
          new RewrittenBatchResultDatabase(fixture.database, rewrite),
          digest,
        ).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: fixture.proof,
        })
        expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
      } finally {
        fixture.database.close()
      }
    }

    for (const results of [
      [],
      Array.from({ length: 4 }, () => ({ meta: { changes: 0 }, success: true })),
    ]) {
      const fixture = await scenarioFixture("confirmed")
      try {
        const capability = await terminalCapability(fixture)
        const before = terminalDatabaseSnapshot(fixture)
        await expect(
          new D1SagaTerminalStore(
            new SyntheticBatchResultDatabase(fixture.database, results),
            digest,
          ).persistTerminalTail({
            actorChecksum: "history-reconcile-terminal-actor",
            capability,
            proof: fixture.proof,
          }),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
        expect(terminalDatabaseSnapshot(fixture)).toEqual(before)
      } finally {
        fixture.database.close()
      }
    }
  })

  it("rejects every malformed terminal receipt, audit, head, and final projection", async () => {
    const fixture = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(fixture)
      const actorChecksum = "history-reconcile-terminal-actor"
      await new D1SagaTerminalStore(fixture.database, digest).persistTerminalTail({
        actorChecksum,
        capability,
        proof: fixture.proof,
      })

      const receiptMutations: Array<
        (result: ControlQueryResult<unknown>) => ControlQueryResult<unknown>
      > = [
        (result) => ({ ...result, success: false }),
        (result) => ({ ...result, results: result.results.slice(0, -1) }),
        (result) => {
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          ;(rows[0] as Record<string, unknown>).transition_id = null
          return { ...result, results: rows }
        },
        (result) => {
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          ;(rows[0] as Record<string, unknown>).ordinal = -1
          return { ...result, results: rows }
        },
        (result) => {
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          ;(rows[1] as Record<string, unknown>).holder_id = "contradictory-holder"
          return { ...result, results: rows }
        },
        (result) => {
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          ;(rows[0] as Record<string, unknown>).to_record_json = "{}"
          return { ...result, results: rows }
        },
        (result) => {
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          ;(rows[0] as Record<string, unknown>).event_json = "{"
          return { ...result, results: rows }
        },
      ]
      for (const mutate of receiptMutations) {
        const database = new HookedDatabase(fixture.database, {
          all: (sql, result) =>
            sql.includes('WITH "ids" AS MATERIALIZED') ? mutate(result) : result,
        })
        await expect(
          new D1SagaTerminalStore(database, digest).persistTerminalTail({
            actorChecksum,
            capability,
            proof: fixture.proof,
          }),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      }

      for (const first of [
        () => null,
        (_sql: string, result: unknown) => ({
          ...(result as Record<string, unknown>),
          effect_count: Number((result as Record<string, unknown>).effect_count) + 1,
        }),
      ]) {
        const database = new HookedDatabase(fixture.database, {
          first: (sql, result) =>
            sql.includes('SELECT "operation"."environment_id"') ? first(sql, result) : result,
        })
        await expect(
          new D1SagaTerminalStore(database, digest).persistTerminalTail({
            actorChecksum,
            capability,
            proof: fixture.proof,
          }),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      }

      const mismatchedProjection = new HookedDatabase(fixture.database, {
        all: (sql, result) => {
          if (!sql.includes('FROM "nozzle_operation_steps" WHERE "operation_id"')) return result
          const rows = structuredClone(result.results) as Array<Record<string, unknown>>
          const forward = rows.find((row) => row.step_id === "saga:forward:write") as Record<
            string,
            unknown
          >
          forward.record_json = JSON.stringify({
            ...(JSON.parse(String(forward.record_json)) as Record<string, unknown>),
            resultChecksum: "contradictory-final-result",
          })
          return { ...result, results: rows }
        },
      })
      await expect(
        new D1SagaTerminalStore(mismatchedProjection, digest).persistTerminalTail({
          actorChecksum,
          capability,
          proof: fixture.proof,
        }),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(/exact final operation projection/u),
      })

      const finalCapability = await terminalCapability(fixture)
      await expect(
        new D1SagaTerminalStore(mismatchedProjection, digest).persistTerminalTail({
          actorChecksum,
          capability: finalCapability,
          proof: fixture.proof,
        }),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(/already-settled saga projection/u),
      })
    } finally {
      fixture.database.close()
    }
  })

  it("rejects malformed audit snapshots and enforces the D1 bound-value budget", async () => {
    const malformedCases: Array<(result: unknown | null) => unknown | null> = [
      () => null,
      (result) => ({ ...(result as Record<string, unknown>), now_ms: -1 }),
      (result) => ({ ...(result as Record<string, unknown>), event_json: "{" }),
    ]
    for (const mutate of malformedCases) {
      const fixture = await scenarioFixture("confirmed")
      try {
        const capability = await terminalCapability(fixture)
        const database = new HookedDatabase(fixture.database, {
          first: (sql, result) => (sql.includes('AS "now_ms"') ? mutate(result) : result),
        })
        await expect(
          new D1SagaTerminalStore(database, digest).persistTerminalTail({
            actorChecksum: "history-reconcile-terminal-actor",
            capability,
            proof: fixture.proof,
          }),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      } finally {
        fixture.database.close()
      }
    }

    const wrongEnvironment = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(wrongEnvironment)
      const foreignAudit = await appendAuditEvent(
        undefined,
        {
          actorChecksum: "fictional-foreign-actor",
          environmentId: "fictional-foreign-environment",
          eventType: "fictional.foreign.event",
          fencingToken: null,
          idempotencyKey: "fictional-foreign-event",
          operationId: "fictional-foreign-operation",
          payloadChecksum: "fictional-foreign-payload",
          serverTimeMs: 1,
          stepId: null,
        },
        digest,
      )
      const database = new HookedDatabase(wrongEnvironment.database, {
        first: (sql, result) =>
          sql.includes('AS "now_ms"')
            ? { ...(result as Record<string, unknown>), event_json: JSON.stringify(foreignAudit) }
            : result,
      })
      await expect(
        new D1SagaTerminalStore(database, digest).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: wrongEnvironment.proof,
        }),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    } finally {
      wrongEnvironment.database.close()
    }

    const absentOnce = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(absentOnce)
      let first = true
      const database = new HookedDatabase(absentOnce.database, {
        first: (sql, result) => {
          if (!sql.includes('AS "now_ms"') || !first) return result
          first = false
          return { ...(result as Record<string, unknown>), event_json: null }
        },
      })
      const settled = await new D1SagaTerminalStore(database, digest).persistTerminalTail({
        actorChecksum: "history-reconcile-terminal-actor",
        capability,
        proof: absentOnce.proof,
      })
      expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
    } finally {
      absentOnce.database.close()
    }

    const oversized = await scenarioFixture("confirmed")
    try {
      const capability = await terminalCapability(oversized)
      const oversizedDigest: DigestFunction = async (input) => {
        if (new TextDecoder().decode(input).includes("step.not_required")) {
          return "x".repeat(2_000_001)
        }
        return digest(input)
      }
      await expect(
        new D1SagaTerminalStore(oversized.database, oversizedDigest).persistTerminalTail({
          actorChecksum: "history-reconcile-terminal-actor",
          capability,
          proof: oversized.proof,
        }),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(/two-million-byte bound-value limit/u),
      })
    } finally {
      oversized.database.close()
    }

    const nearD1RowLimit = await scenarioFixture("confirmed", {
      initResultChecksum: "r".repeat(1_999_500),
    })
    try {
      const initRow = nearD1RowLimit.database.database
        .prepare(
          `SELECT "record_json" FROM "nozzle_operation_steps"
           WHERE "operation_id" = ? AND "step_id" = 'saga:init'`,
        )
        .get(nearD1RowLimit.operationId) as { readonly record_json: string }
      const initBytes = new TextEncoder().encode(initRow.record_json).byteLength
      expect(initBytes).toBeGreaterThan(1_999_500)
      expect(initBytes).toBeLessThanOrEqual(2_000_000)
      const capability = await terminalCapability(nearD1RowLimit)
      const settled = await new D1SagaTerminalStore(
        nearD1RowLimit.database,
        digest,
      ).persistTerminalTail({
        actorChecksum: "history-reconcile-terminal-actor",
        capability,
        proof: nearD1RowLimit.proof,
      })
      expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
    } finally {
      nearD1RowLimit.database.close()
    }
  })
})
