import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type DigestFunction,
  type LeaseProof,
  leaseProof,
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
import { D1OperationStore } from "../src/operation-store.js"
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
  | "unknown_applied"
  | "unknown_indeterminate"

interface ScenarioFixture {
  readonly database: DatabaseAdapter
  readonly descriptor: SagaDescriptor
  readonly operationId: string
  readonly plan: OperationPlan
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

async function scenarioFixture(scenario: Scenario): Promise<ScenarioFixture> {
  const database = new DatabaseAdapter()
  const operationId = `history-reconcile-operation-${scenario}`
  const sagaId = `history-reconcile-saga-${scenario}`
  const leaseKey = `saga:${sagaId}`
  const maxAttempts = scenario === "retry" ? 2 : 1
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
  const operations = new D1OperationStore(database, digest)
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
    resultChecksum: `${sagaId}:init:result`,
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
    return { database, descriptor: sealedDescriptor, operationId, plan, sagaId }
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
  } else if (scenario === "irreversible_confirmed") {
    await completeEffect(firstAttempt, "confirmed")
  } else {
    await completeEffect(firstAttempt, scenario)
  }

  return { database, descriptor: sealedDescriptor, operationId, plan, sagaId }
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
        outcomes.push({ authority: loadSagaTerminalCapability(capability), capability, proof })
      } finally {
        fixture.database.close()
      }
    }
    expect(outcomes).toHaveLength(scenarios.length)
    expect(outcomes.every(({ proof }) => proof.schemaVersion === 1)).toBe(true)
    expect(outcomes.every(({ proof }) => Object.isFrozen(proof.anchor))).toBe(true)
    expect(outcomes.map(({ proof }) => proof.reconciliation.observationAttemptCount)).toEqual([
      0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1,
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
})
