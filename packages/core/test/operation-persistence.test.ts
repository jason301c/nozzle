import { describe, expect, it } from "vitest"
import {
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  leaseProof,
  loadOperationRecord,
  markRunningStepsUnknownAfterCrash,
  type OperationPlan,
  type OperationRecord,
  type OperationStepPlanInput,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  sealIrreversibleAuthorization,
  sealOperationPlan,
} from "../src/operation.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function step(overrides: Partial<OperationStepPlanInput> = {}): OperationStepPlanInput {
  return {
    checkpoint: "reversible",
    dependsOn: [],
    idempotencyKey: "step-key",
    inputChecksum: "step-input",
    leaseKey: "fleet:operation",
    postconditionChecksum: "postcondition",
    preconditionChecksum: "precondition",
    recoveryInstructions: "Inspect the durable evidence and resume this operation.",
    retryClassification: "idempotent",
    stepId: "one",
    ...overrides,
  }
}

async function plan(overrides: Partial<OperationStepPlanInput> = {}): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "capabilities",
      idempotencyKey: "operation-key",
      inputChecksum: "operation-input",
      operationId: "operation-1",
      operationType: "persistence-test",
      steps: [step(overrides)],
    },
    digest,
  )
}

function lease() {
  const decision = decideLeaseAcquisition(undefined, {
    acquisitionId: "acquisition-1",
    holderId: "controller-1",
    leaseKey: "fleet:operation",
    serverTimeMs: 100,
    ttlMs: 1_000,
  })
  if (!decision.acquired) throw new Error("Fixture lease acquisition failed.")
  return decision.record
}

async function started(overrides: Partial<OperationStepPlanInput> = {}): Promise<OperationRecord> {
  const operation = createOperationRecord(await plan(overrides))
  const activeLease = lease()
  const decision = beginOperationStep(operation, {
    attemptId: "attempt-1",
    idempotencyKey: "step-key",
    lease: activeLease,
    leaseProof: leaseProof(activeLease),
    observedPreconditionChecksum: "precondition",
    serverTimeMs: 110,
    stepId: "one",
  })
  if (decision.disposition !== "execute") throw new Error("Fixture step did not start.")
  return decision.operation
}

async function roundTrip(operation: OperationRecord): Promise<OperationRecord> {
  return loadOperationRecord(JSON.parse(JSON.stringify(operation)) as unknown, digest)
}

function replaceStep(operation: OperationRecord, replacement: unknown): unknown {
  return {
    plan: JSON.parse(JSON.stringify(operation.plan)) as unknown,
    steps: { one: replacement },
  }
}

describe("persisted operation record integrity", () => {
  it("round trips every reachable durable step-state shape", async () => {
    const pending = createOperationRecord(await plan())
    const running = await started()
    const succeeded = recordStepSuccess(running, {
      attemptId: "attempt-1",
      counters: { cost: { providerCalls: 1 }, progress: { resources: 1 } },
      observedPostconditionChecksum: "postcondition",
      resultChecksum: "result",
      stepId: "one",
    })
    const retryable = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "retryable-error",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    const failed = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "permanent-error",
      outcome: "permanent",
      stepId: "one",
    })
    const unknown = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "unknown-error",
      outcome: "unknown",
      stepId: "one",
    })
    const reconciledApplied = recordStepReconciliation(unknown, {
      evidenceChecksum: "applied-evidence",
      observedPostconditionChecksum: "postcondition",
      outcome: "applied",
      resultChecksum: "reconciled-result",
      stepId: "one",
    })
    const reconciledIntervention = recordStepReconciliation(unknown, {
      evidenceChecksum: "indeterminate-evidence",
      outcome: "indeterminate",
      stepId: "one",
    })
    const reconciledRetry = recordStepReconciliation(unknown, {
      evidenceChecksum: "absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })
    const crashUnknown = markRunningStepsUnknownAfterCrash(await started())
    const reconciledCrash = recordStepReconciliation(crashUnknown, {
      evidenceChecksum: "crash-absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })
    const neverCrash = markRunningStepsUnknownAfterCrash(
      await started({ retryClassification: "never" }),
    )
    const reconciledNever = recordStepReconciliation(neverCrash, {
      evidenceChecksum: "never-absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })

    for (const operation of [
      pending,
      running,
      succeeded,
      retryable,
      failed,
      unknown,
      reconciledApplied,
      reconciledIntervention,
      reconciledRetry,
      crashUnknown,
      reconciledCrash,
      reconciledNever,
    ]) {
      const loaded = await roundTrip(operation)
      expect(loaded).toEqual(operation)
      expect(Object.isFrozen(loaded)).toBe(true)
      expect(Object.isFrozen(loaded.plan)).toBe(true)
      expect(Object.isFrozen(loaded.steps)).toBe(true)
      expect(Object.isFrozen(loaded.steps.one)).toBe(true)
      expect(Object.isFrozen(loaded.steps.one?.costCounters)).toBe(true)
    }
  })

  it("round trips irreversible authorization only after an authorized attempt starts", async () => {
    const sealedPlan = await plan({ checkpoint: "irreversible" })
    const operation = createOperationRecord(sealedPlan)
    const activeLease = lease()
    const proof = leaseProof(activeLease)
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-1",
        decisionChecksum: "decision",
        lease: activeLease,
        leaseProof: proof,
        sealedAtServerTimeMs: 105,
        stepId: "one",
      },
      digest,
    )
    const decision = beginOperationStep(operation, {
      attemptId: "attempt-1",
      idempotencyKey: "step-key",
      irreversibleAuthorization: authorization,
      lease: activeLease,
      leaseProof: proof,
      observedPreconditionChecksum: "precondition",
      serverTimeMs: 110,
      stepId: "one",
    })
    if (decision.disposition !== "execute") throw new Error("Fixture step did not start.")
    await expect(roundTrip(operation)).resolves.toEqual(operation)
    await expect(roundTrip(decision.operation)).resolves.toEqual(decision.operation)
  })

  it("accepts ordinary and null-prototype serialized records but rejects other containers", async () => {
    const operation = createOperationRecord(await plan())
    const candidate = Object.assign(Object.create(null) as Record<string, unknown>, {
      plan: JSON.parse(JSON.stringify(operation.plan)) as unknown,
      steps: Object.assign(Object.create(null) as Record<string, unknown>, {
        one: Object.assign(Object.create(null) as Record<string, unknown>, operation.steps.one),
      }),
    })
    await expect(loadOperationRecord(candidate, digest)).resolves.toEqual(operation)
    for (const malformed of [null, [], new Date(), "operation"]) {
      await expect(loadOperationRecord(malformed, digest)).rejects.toThrow(/record is malformed/u)
    }
  })

  it("rejects unknown top-level fields, malformed plans, and mismatched step membership", async () => {
    const operation = createOperationRecord(await plan())
    const serialized = JSON.parse(JSON.stringify(operation)) as Record<string, unknown>
    await expect(loadOperationRecord({ ...serialized, unexpected: true }, digest)).rejects.toThrow(
      /unknown fields/u,
    )
    await expect(loadOperationRecord({ ...serialized, plan: [] }, digest)).rejects.toThrow(
      /plan is malformed/u,
    )
    await expect(loadOperationRecord({ ...serialized, steps: [] }, digest)).rejects.toThrow(
      /steps are malformed/u,
    )
    await expect(loadOperationRecord({ ...serialized, steps: {} }, digest)).rejects.toThrow(
      /membership/u,
    )
    await expect(
      loadOperationRecord(
        { ...serialized, steps: { one: operation.steps.one, unexpected: operation.steps.one } },
        digest,
      ),
    ).rejects.toThrow(/membership/u)
  })

  it("rejects malformed step record structure, strings, counters, and scalar metadata", async () => {
    const operation = createOperationRecord(await plan())
    const base = operation.steps.one as NonNullable<(typeof operation.steps)[string]>
    const malformed = [
      null,
      [],
      { ...base, unexpected: true },
      { ...base, state: "future" },
      { ...base, startedAttempts: -1 },
      { ...base, activeAttemptId: "" },
      { ...base, errorChecksum: "\ud800" },
      { ...base, fencingToken: 0 },
      { ...base, costCounters: null },
      { ...base, costCounters: { "": 1 } },
      { ...base, costCounters: { "\ud800": 1 } },
      { ...base, costCounters: { calls: -1 } },
      { ...base, progressCounters: { rows: 0.5 } },
    ]
    for (const stepRecord of malformed) {
      await expect(
        loadOperationRecord(replaceStep(operation, stepRecord), digest),
      ).rejects.toThrow()
    }
  })

  it("rejects contradictory attempt, result, error, and reconciliation state", async () => {
    const pendingOperation = createOperationRecord(await plan())
    const pending = pendingOperation.steps.one as NonNullable<
      (typeof pendingOperation.steps)[string]
    >
    const runningOperation = await started()
    const running = runningOperation.steps.one as NonNullable<
      (typeof runningOperation.steps)[string]
    >
    const unknownOperation = markRunningStepsUnknownAfterCrash(runningOperation)
    const unknown = unknownOperation.steps.one as NonNullable<
      (typeof unknownOperation.steps)[string]
    >
    const succeeded = recordStepSuccess(runningOperation, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "postcondition",
      resultChecksum: "result",
      stepId: "one",
    }).steps.one as NonNullable<(typeof runningOperation.steps)[string]>

    const malformed = [
      { operation: pendingOperation, step: { ...pending, startedAttempts: 1 } },
      { operation: runningOperation, step: { ...running, startedAttempts: 0 } },
      {
        operation: pendingOperation,
        step: { ...pending, fencingToken: 1, lastAttemptId: "attempt" },
      },
      { operation: unknownOperation, step: { ...unknown, fencingToken: undefined } },
      { operation: unknownOperation, step: { ...unknown, lastAttemptId: undefined } },
      { operation: runningOperation, step: { ...running, activeAttemptId: undefined } },
      { operation: unknownOperation, step: { ...unknown, activeAttemptId: "attempt-1" } },
      { operation: runningOperation, step: { ...running, activeAttemptId: "other" } },
      { operation: runningOperation, step: { ...running, resultChecksum: "result" } },
      { operation: runningOperation, step: { ...succeeded, resultChecksum: undefined } },
      {
        operation: unknownOperation,
        step: { ...unknown, reconciliationEvidenceChecksum: "evidence" },
      },
      { operation: runningOperation, step: { ...running, errorChecksum: "error" } },
      {
        operation: runningOperation,
        step: { ...running, state: "retryable_failed", activeAttemptId: undefined },
      },
      {
        operation: runningOperation,
        step: { ...running, state: "intervention_required", activeAttemptId: undefined },
      },
      { operation: pendingOperation, step: { ...pending, costCounters: { calls: 1 } } },
    ]
    for (const item of malformed) {
      await expect(
        loadOperationRecord(replaceStep(item.operation, item.step), digest),
      ).rejects.toThrow()
    }
  })

  it("rejects authorization on reversible steps and missing authorization after irreversible start", async () => {
    const reversible = createOperationRecord(await plan())
    await expect(
      loadOperationRecord(
        replaceStep(reversible, {
          ...reversible.steps.one,
          authorizationChecksum: "unexpected",
        }),
        digest,
      ),
    ).rejects.toThrow(/reversible step/u)

    const irreversible = createOperationRecord(await plan({ checkpoint: "irreversible" }))
    await expect(
      loadOperationRecord(
        replaceStep(irreversible, {
          ...irreversible.steps.one,
          authorizationChecksum: "premature",
        }),
        digest,
      ),
    ).rejects.toThrow(/authorization state/u)
    await expect(
      loadOperationRecord(
        replaceStep(irreversible, {
          ...irreversible.steps.one,
          fencingToken: 1,
          lastAttemptId: "attempt-1",
          startedAttempts: 1,
          state: "unknown",
        }),
        digest,
      ),
    ).rejects.toThrow(/authorization state/u)
  })
})
