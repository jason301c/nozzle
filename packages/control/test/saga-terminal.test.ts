import {
  beginSagaAction,
  createOperationRecord,
  createSagaRecord,
  type DigestFunction,
  type IrreversibleAuthorization,
  loadSagaRecord,
  markRunningStepNotDispatchedAfterCrash,
  markSagaActionNotDispatched,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordSagaObservation,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  requestSagaTermination,
  type SagaActionPhase,
  type SagaActionRecord,
  type SagaActionReference,
  type SagaRecord,
  type SagaStepDescriptorInput,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import fc from "fast-check"
import { describe, expect, it } from "vitest"
import { sealSagaOperationPlan } from "../src/saga-plan.js"
import { type SagaHandlerRegistration, sealSagaHandlerRegistry } from "../src/saga-registry.js"
import {
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
import {
  modelTerminalSagaBranches as decideBranches,
  type SagaTerminalModelEvidence,
} from "../src/saga-terminal.js"

const digest: DigestFunction = async (input) => {
  const owned = new Uint8Array(input.byteLength)
  owned.set(input)
  const output = new Uint8Array(await crypto.subtle.digest("SHA-256", owned.buffer))
  return [...output].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

const effect = () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" as const })
const observation = () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" as const })
const TERMINAL_SAGA_CHECKSUM = "9".repeat(64)

function reference(actionId: string, character: string): SagaActionReference {
  return { actionId, artifactChecksum: character.repeat(64), version: 1 }
}

function reversibleStep(stepId: string): SagaStepDescriptorInput {
  return {
    authorizationPolicyChecksum: null,
    baseRetryDelayMs: 1,
    compensationAction: reference(`${stepId}.compensate`, "c"),
    compensationObservation: reference(`${stepId}.compensation.observe`, "d"),
    forwardAction: reference(`${stepId}.forward`, "a"),
    forwardObservation: reference(`${stepId}.forward.observe`, "b"),
    inputSchemaChecksum: "1".repeat(64),
    irreversible: false,
    maxAttempts: 2,
    maxRetryDelayMs: 2,
    outputSchemaChecksum: "2".repeat(64),
    stepId,
    timeoutMs: 100,
  }
}

function irreversibleStep(stepId: string): SagaStepDescriptorInput {
  return {
    ...reversibleStep(stepId),
    authorizationPolicyChecksum: "3".repeat(64),
    compensationAction: null,
    compensationObservation: null,
    irreversible: true,
  }
}

function registrations(steps: readonly SagaStepDescriptorInput[]): SagaHandlerRegistration[] {
  return steps.flatMap((step) => [
    { handler: effect, kind: "effect" as const, reference: step.forwardAction },
    { handler: observation, kind: "observation" as const, reference: step.forwardObservation },
    ...(step.compensationAction === null
      ? []
      : [
          { handler: effect, kind: "effect" as const, reference: step.compensationAction },
          {
            handler: observation,
            kind: "observation" as const,
            reference: step.compensationObservation as SagaActionReference,
          },
        ]),
  ])
}

interface Fixture {
  readonly operation: OperationRecord
  readonly saga: SagaRecord
}

async function fixture(
  steps: readonly SagaStepDescriptorInput[] = [reversibleStep("a")],
): Promise<Fixture> {
  const descriptor = await sealSagaDescriptor(
    { descriptorId: "terminal", steps, version: 1 },
    digest,
  )
  const registry = await sealSagaHandlerRegistry(registrations(steps), digest)
  const stepInputChecksums = Object.fromEntries(
    steps.map((step, index) => [step.stepId, ((index + 4) % 16).toString(16).repeat(64)]),
  )
  const plan = await sealSagaOperationPlan(
    {
      capabilitySnapshotChecksum: "e".repeat(64),
      descriptor,
      inputChecksum: "f".repeat(64),
      leaseKey: "saga:terminal",
      operationId: "terminal-operation",
      operationIdempotencyKey: "terminal-operation-key",
      registry,
      sagaId: "terminal-saga",
      stepInputChecksums,
    },
    digest,
  )
  const saga = createSagaRecord({
    deadlineAtMs: 10_000,
    descriptor,
    idempotencyKey: "terminal-saga-key",
    inputChecksum: plan.inputChecksum,
    sagaId: "terminal-saga",
    serverTimeMs: 1_000,
    stepInputChecksums,
  })
  return {
    operation: withOperationStep(
      createOperationRecord(plan),
      SAGA_INIT_OPERATION_STEP_ID,
      atomicRecord("succeeded", "init-result"),
    ),
    saga,
  }
}

function withOperationStep(
  operation: OperationRecord,
  stepId: string,
  record: OperationStepRecord,
): OperationRecord {
  return Object.freeze({
    plan: operation.plan,
    steps: Object.freeze({ ...operation.steps, [stepId]: Object.freeze(record) }),
  })
}

function atomicRecord(
  state: "failed" | "intervention_required" | "succeeded",
  evidence: string,
): OperationStepRecord {
  return Object.freeze({
    costCounters: Object.freeze({}),
    fencingToken: 1,
    lastAttemptId: `atomic-${state}`,
    progressCounters: Object.freeze({}),
    ...(state === "succeeded"
      ? { resultChecksum: evidence }
      : state === "failed"
        ? { errorChecksum: evidence }
        : { reconciliationEvidenceChecksum: evidence }),
    startedAttempts: 1,
    state,
  })
}

function notRequiredRecord(evidence = TERMINAL_SAGA_CHECKSUM): OperationStepRecord {
  return Object.freeze({
    costCounters: Object.freeze({}),
    progressCounters: Object.freeze({}),
    reconciliationEvidenceChecksum: evidence,
    startedAttempts: 0,
    state: "not_required",
  })
}

function action(record: SagaRecord, stepId: string, phase: SagaActionPhase): SagaActionRecord {
  return record.steps[stepId]?.[phase] as SagaActionRecord
}

function begin(
  saga: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  attemptId: string,
  serverTimeMs: number,
): SagaRecord {
  const current = action(saga, stepId, phase)
  const decision = beginSagaAction(saga, {
    attemptId,
    idempotencyKey: current.idempotencyKey,
    phase,
    serverTimeMs,
    stepId,
  })
  if (decision.disposition !== "execute") throw new Error("Expected saga action execution.")
  return decision.saga
}

function succeed(
  saga: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  attemptId: string,
  serverTimeMs: number,
): SagaRecord {
  return recordSagaActionSuccess(saga, {
    attemptId,
    phase,
    resultChecksum: `${attemptId}:business-result`,
    serverTimeMs,
    stepId,
  })
}

function modelAuthorization(
  operation: OperationRecord,
  plan: OperationStepPlan,
  authorizationChecksum = "irreversible-authorization",
): IrreversibleAuthorization {
  return Object.freeze({
    actorChecksum: "terminal-model-actor",
    authorizationChecksum,
    authorizationId: `${plan.stepId}:authorization`,
    decisionChecksum: `${plan.stepId}:decision`,
    fencingToken: 1,
    holderId: "terminal-model-holder",
    leaseAcquisitionId: "terminal-model-acquisition",
    leaseKey: plan.leaseKey,
    operationId: operation.plan.operationId,
    planChecksum: operation.plan.planChecksum,
    sealedAtServerTimeMs: 1_000,
    schemaVersion: 1,
    stepId: plan.stepId,
    stepInputChecksum: plan.inputChecksum,
  })
}

function modelAuthorizationFields(
  operation: OperationRecord,
  stepId: string,
  authorizationChecksum = "irreversible-authorization",
): Pick<OperationStepRecord, "authorizationChecksum" | "irreversibleAuthorization"> {
  const plan = operation.plan.steps.find((step) => step.stepId === stepId)
  if (plan === undefined) throw new Error("Missing irreversible operation plan step.")
  return {
    authorizationChecksum,
    irreversibleAuthorization: modelAuthorization(operation, plan, authorizationChecksum),
  }
}

function selectedRecord(
  operation: OperationRecord,
  selected: SagaActionRecord,
  plan: OperationStepPlan,
): OperationStepRecord {
  const observed = selected.observationEvidenceChecksum
  const intervention = selected.state === "intervention_required" && observed !== undefined
  return Object.freeze({
    ...(plan.checkpoint === "irreversible" ? modelAuthorizationFields(operation, plan.stepId) : {}),
    costCounters: Object.freeze({}),
    ...(observed === undefined
      ? {}
      : {
          errorChecksum: "generic-unknown-error",
          reconciliationEvidenceChecksum: observed,
        }),
    fencingToken: 1,
    lastAttemptId: selected.lastAttemptId as string,
    progressCounters: Object.freeze({}),
    ...(intervention ? {} : { resultChecksum: observed ?? "generic-receipt-outcome" }),
    startedAttempts: selected.attempts,
    state: intervention ? "intervention_required" : "succeeded",
  })
}

function withSelectedAction(
  operation: OperationRecord,
  saga: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): OperationRecord {
  const operationStepId = sagaActionOperationStepId(stepId, phase)
  const plan = operation.plan.steps.find((step) => step.stepId === operationStepId)
  if (plan === undefined) throw new Error("Missing saga action operation plan.")
  return withOperationStep(
    operation,
    operationStepId,
    selectedRecord(operation, action(saga, stepId, phase), plan),
  )
}

function withExplicitTermination(operation: OperationRecord): OperationRecord {
  return withOperationStep(
    operation,
    SAGA_TERMINATION_OPERATION_STEP_ID,
    atomicRecord("succeeded", "termination-result"),
  )
}

function sagaTerminalModelEvidence(
  saga: SagaRecord,
  irreversibleAuthorizationChecksums: Readonly<Record<string, string>> = Object.freeze({}),
): SagaTerminalModelEvidence {
  return Object.freeze({
    irreversibleAuthorizationChecksums: Object.freeze({
      ...irreversibleAuthorizationChecksums,
    }),
    sagaChecksum: TERMINAL_SAGA_CHECKSUM,
    stateVersion: saga.stateVersion,
  })
}

function modelAuthorizationChecksums(
  operation: OperationRecord,
  saga: SagaRecord,
): Readonly<Record<string, string>> {
  const checksums: Record<string, string> = {}
  for (const step of saga.descriptor.steps) {
    if (!step.irreversible || saga.steps[step.stepId]?.forward.attempts === 0) continue
    const operationStepId = sagaActionOperationStepId(step.stepId, "forward")
    const checksum = operation.steps[operationStepId]?.authorizationChecksum
    if (checksum !== undefined) checksums[operationStepId] = checksum
  }
  return Object.freeze(checksums)
}

function decideTerminalSagaBranches(
  operation: OperationRecord,
  saga: SagaRecord,
): ReturnType<typeof decideBranches> {
  return decideBranches(
    operation,
    saga,
    sagaTerminalModelEvidence(saga, modelAuthorizationChecksums(operation, saga)),
  )
}

function decisionEvidence(saga: SagaRecord) {
  return { sagaChecksum: TERMINAL_SAGA_CHECKSUM, stateVersion: saga.stateVersion } as const
}

function expectCode(callback: () => unknown, code: string): void {
  expect(callback).toThrowError(expect.objectContaining({ code }))
}

describe("terminal saga branch oracle", () => {
  it("classifies only unchosen termination and compensation after success", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-forward-1", 1_001)
    saga = succeed(saga, "a", "forward", "a-forward-1", 1_002)
    operation = withSelectedAction(operation, saga, "a", "forward")

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])
  })

  it.each([
    "cancellation",
    "timeout",
  ] as const)("classifies every untouched action after %s", async (cause) => {
    let { operation, saga } = await fixture()
    saga = requestSagaTermination(saga, { cause, serverTimeMs: 1_001 })
    operation = withExplicitTermination(operation)

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "forward"),
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])
  })

  it("classifies automatic failure without pretending its selected forward was a failure", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-forward-failed", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "a-forward-failed",
      errorChecksum: "business-failure",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    operation = withSelectedAction(operation, saga, "a", "forward")

    expect(saga.status).toBe("failed")
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])
  })

  it("accepts terminal compensation failure as generic classification success", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-forward", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    operation = withExplicitTermination(operation)
    saga = succeed(saga, "a", "forward", "a-forward", 1_003)
    operation = withSelectedAction(operation, saga, "a", "forward")
    saga = begin(saga, "a", "compensation", "a-compensation", 1_004)
    saga = recordSagaActionFailure(saga, {
      attemptId: "a-compensation",
      errorChecksum: "compensation-failure",
      outcome: "definitely_not_applied_terminal",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    operation = withSelectedAction(operation, saga, "a", "compensation")

    expect(saga.status).toBe("intervention_required")
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])

    const compensationStepId = sagaActionOperationStepId("a", "compensation")
    const compensationRecord = operation.steps[compensationStepId] as OperationStepRecord
    const { resultChecksum: _resultChecksum, ...withoutResult } = compensationRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, compensationStepId, {
            ...withoutResult,
            errorChecksum: "forged-effect-error",
            reconciliationEvidenceChecksum: "forged-indeterminate-observation",
            state: "intervention_required",
          }),
          saga,
        ),
      "OperationInterventionRequiredError",
    )

    const failedCompensation = await loadSagaRecord(
      {
        ...structuredClone(saga),
        steps: {
          ...structuredClone(saga.steps),
          a: {
            ...structuredClone(saga.steps.a),
            compensation: {
              ...structuredClone(saga.steps.a?.compensation),
              state: "failed",
            },
          },
        },
      },
      digest,
    )
    expectCode(
      () => decideTerminalSagaBranches(operation, failedCompensation),
      "OperationInterventionRequiredError",
    )

    operation = withOperationStep(
      operation,
      SAGA_SETTLE_OPERATION_STEP_ID,
      atomicRecord("intervention_required", TERMINAL_SAGA_CHECKSUM),
    )
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])
  })

  it("accepts observed indeterminate intervention only with exact generic evidence", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-unknown", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "a-unknown",
      errorChecksum: "unknown-business-error",
      outcome: "unknown",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "indeterminate-observation",
      outcome: "indeterminate",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "a",
    })
    operation = withSelectedAction(operation, saga, "a", "forward")

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])

    const forwardStepId = sagaActionOperationStepId("a", "forward")
    const indeterminateRecord = operation.steps[forwardStepId] as OperationStepRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, forwardStepId, {
            ...indeterminateRecord,
            resultChecksum: "indeterminate-observation",
            state: "succeeded",
          }),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
  })

  it("accepts an observed applied action with canonical saga and generic evidence", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "observed-applied", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "observed-applied",
      errorChecksum: "unknown-effect",
      outcome: "unknown",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "applied-observation",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "observed-business-result",
      serverTimeMs: 1_003,
      stepId: "a",
    })
    operation = withSelectedAction(operation, saga, "a", "forward")

    expect(saga.status).toBe("succeeded")
    expect(decideTerminalSagaBranches(operation, saga)).toHaveLength(2)
  })

  it("accepts indeterminate compensation only as generic intervention", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-forward", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    operation = withExplicitTermination(operation)
    saga = succeed(saga, "a", "forward", "a-forward", 1_003)
    operation = withSelectedAction(operation, saga, "a", "forward")
    saga = begin(saga, "a", "compensation", "a-compensation-unknown", 1_004)
    saga = recordSagaActionFailure(saga, {
      attemptId: "a-compensation-unknown",
      errorChecksum: "compensation-effect-unknown",
      outcome: "unknown",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "compensation-indeterminate",
      outcome: "indeterminate",
      phase: "compensation",
      serverTimeMs: 1_006,
      stepId: "a",
    })
    operation = withSelectedAction(operation, saga, "a", "compensation")

    expect(saga.status).toBe("intervention_required")
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])
  })

  it("accepts an irreversible cancellation race only with sealed generic authorization", async () => {
    let { operation, saga } = await fixture([irreversibleStep("commit")])
    saga = begin(saga, "commit", "forward", "commit-forward", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    saga = succeed(saga, "commit", "forward", "commit-forward", 1_003)
    operation = withExplicitTermination(operation)
    operation = withSelectedAction(operation, saga, "commit", "forward")

    expect(saga.status).toBe("intervention_required")
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])
  })

  it("returns exact generic receipt evidence for an abandoned retryable forward", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-retryable", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "a-retryable",
      errorChecksum: "business-not-applied",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    operation = withExplicitTermination(operation)
    operation = withOperationStep(operation, sagaActionOperationStepId("a", "forward"), {
      costCounters: Object.freeze({}),
      errorChecksum: "generic-not-applied-receipt",
      fencingToken: 1,
      lastAttemptId: "a-retryable",
      progressCounters: Object.freeze({}),
      startedAttempts: 1,
      state: "retryable_failed",
    })

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        attemptId: "a-retryable",
        evidenceChecksum: "generic-not-applied-receipt",
        evidenceKind: "direct_receipt",
        kind: "terminal_not_applied",
        stepId: sagaActionOperationStepId("a", "forward"),
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])

    operation = withOperationStep(operation, sagaActionOperationStepId("a", "forward"), {
      costCounters: Object.freeze({}),
      errorChecksum: "generic-not-applied-receipt",
      fencingToken: 1,
      lastAttemptId: "a-retryable",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "generic-not-applied-receipt",
      resultChecksum: "generic-not-applied-receipt",
      startedAttempts: 1,
      state: "succeeded",
    })
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])

    const classified = operation.steps[
      sagaActionOperationStepId("a", "forward")
    ] as OperationStepRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, sagaActionOperationStepId("a", "forward"), {
            ...classified,
            reconciliationEvidenceChecksum: "forged-absence-receipt",
            resultChecksum: "forged-absence-receipt",
          }),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
  })

  it("prefers exact reconciliation evidence for observed or crash-absent retryables", async () => {
    let observed = await fixture()
    let saga = begin(observed.saga, "a", "forward", "observed-retryable", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "observed-retryable",
      errorChecksum: "effect-unknown",
      outcome: "unknown",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "observation-not-applied",
      outcome: "not_applied",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "a",
    })
    saga = requestSagaTermination(saga, { cause: "timeout", serverTimeMs: 1_004 })
    observed = { operation: withExplicitTermination(observed.operation), saga }
    observed = {
      ...observed,
      operation: withOperationStep(observed.operation, sagaActionOperationStepId("a", "forward"), {
        costCounters: Object.freeze({}),
        errorChecksum: "generic-effect-unknown",
        fencingToken: 2,
        lastAttemptId: "observed-retryable",
        progressCounters: Object.freeze({}),
        reconciliationEvidenceChecksum: "observation-not-applied",
        startedAttempts: 1,
        state: "retryable_failed",
      }),
    }
    expect(decideTerminalSagaBranches(observed.operation, observed.saga)[0]).toMatchObject({
      evidenceChecksum: "observation-not-applied",
      evidenceKind: "observation",
      kind: "terminal_not_applied",
    })
    observed = {
      ...observed,
      operation: withOperationStep(observed.operation, sagaActionOperationStepId("a", "forward"), {
        costCounters: Object.freeze({}),
        errorChecksum: "generic-effect-unknown",
        fencingToken: 2,
        lastAttemptId: "observed-retryable",
        progressCounters: Object.freeze({}),
        reconciliationEvidenceChecksum: "observation-not-applied",
        resultChecksum: "observation-not-applied",
        startedAttempts: 1,
        state: "succeeded",
      }),
    }
    expect(decideTerminalSagaBranches(observed.operation, observed.saga)[0]).toEqual({
      ...decisionEvidence(observed.saga),
      kind: "not_required",
      stepId: sagaActionOperationStepId("a", "compensation"),
    })

    let absent = await fixture()
    saga = begin(absent.saga, "a", "forward", "absent-retryable", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "absent-retryable",
      errorChecksum: "dispatch-absence",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    absent = { operation: withExplicitTermination(absent.operation), saga }
    absent = {
      ...absent,
      operation: withOperationStep(absent.operation, sagaActionOperationStepId("a", "forward"), {
        costCounters: Object.freeze({}),
        fencingToken: 2,
        lastAttemptId: "absent-retryable",
        progressCounters: Object.freeze({}),
        reconciliationEvidenceChecksum: "dispatch-absence",
        startedAttempts: 1,
        state: "retryable_failed",
      }),
    }
    expect(decideTerminalSagaBranches(absent.operation, absent.saga)[0]).toMatchObject({
      evidenceChecksum: "dispatch-absence",
      evidenceKind: "crash_absence",
    })
    absent = {
      ...absent,
      operation: withOperationStep(absent.operation, sagaActionOperationStepId("a", "forward"), {
        costCounters: Object.freeze({}),
        fencingToken: 2,
        lastAttemptId: "absent-retryable",
        progressCounters: Object.freeze({}),
        reconciliationEvidenceChecksum: "dispatch-absence",
        resultChecksum: "dispatch-absence",
        startedAttempts: 1,
        state: "succeeded",
      }),
    }
    expect(decideTerminalSagaBranches(absent.operation, absent.saga)[0]).toEqual({
      ...decisionEvidence(absent.saga),
      kind: "not_required",
      stepId: sagaActionOperationStepId("a", "compensation"),
    })
  })

  it("preserves irreversible authorization while terminalizing proven non-application", async () => {
    let { operation, saga } = await fixture([irreversibleStep("commit")])
    saga = begin(saga, "commit", "forward", "commit-retryable", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "commit-retryable",
      errorChecksum: "commit-not-applied",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "commit",
    })
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    operation = withExplicitTermination(operation)
    const stepId = sagaActionOperationStepId("commit", "forward")
    operation = withOperationStep(operation, stepId, {
      ...modelAuthorizationFields(operation, stepId, "commit-authorization"),
      costCounters: Object.freeze({}),
      errorChecksum: "commit-receipt",
      fencingToken: 1,
      lastAttemptId: "commit-retryable",
      progressCounters: Object.freeze({}),
      startedAttempts: 1,
      state: "retryable_failed",
    })

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        attemptId: "commit-retryable",
        evidenceChecksum: "commit-receipt",
        evidenceKind: "direct_receipt",
        kind: "terminal_not_applied",
        stepId,
      },
    ])

    operation = withOperationStep(operation, stepId, {
      ...modelAuthorizationFields(operation, stepId, "commit-authorization"),
      costCounters: Object.freeze({}),
      errorChecksum: "commit-receipt",
      fencingToken: 1,
      lastAttemptId: "commit-retryable",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "commit-receipt",
      resultChecksum: "commit-receipt",
      startedAttempts: 1,
      state: "succeeded",
    })
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])
  })

  it("requires the exact modelled irreversible authorization set", async () => {
    let { operation, saga } = await fixture([irreversibleStep("commit")])
    saga = begin(saga, "commit", "forward", "commit-authorized", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    saga = succeed(saga, "commit", "forward", "commit-authorized", 1_003)
    operation = withExplicitTermination(operation)
    operation = withSelectedAction(operation, saga, "commit", "forward")
    const stepId = sagaActionOperationStepId("commit", "forward")
    const authorizationChecksum = operation.steps[stepId]?.authorizationChecksum as string

    expect(
      decideBranches(
        operation,
        saga,
        sagaTerminalModelEvidence(saga, { [stepId]: authorizationChecksum }),
      ),
    ).toEqual([])
    for (const authorizations of [
      {},
      { [stepId]: "wrong-authorization" },
      { [stepId]: authorizationChecksum, unexpected: "extra-authorization" },
    ]) {
      expectCode(
        () => decideBranches(operation, saga, sagaTerminalModelEvidence(saga, authorizations)),
        "OperationInterventionRequiredError",
      )
    }

    const record = operation.steps[stepId] as OperationStepRecord
    const authorization = record.irreversibleAuthorization as IrreversibleAuthorization
    const malformedBodies: readonly unknown[] = [
      null,
      { ...authorization, unexpected: true },
      { ...authorization, schemaVersion: 2 },
      { ...authorization, authorizationChecksum: "wrong-authorization" },
      { ...authorization, stepId: "saga:forward:other" },
      { ...authorization, stepInputChecksum: "wrong-input" },
      { ...authorization, leaseKey: "wrong-lease" },
      { ...authorization, operationId: "wrong-operation" },
      { ...authorization, planChecksum: "wrong-plan" },
      { ...authorization, fencingToken: 2 },
      { ...authorization, sealedAtServerTimeMs: Number.NaN },
      { ...authorization, sealedAtServerTimeMs: -1 },
      { ...authorization, actorChecksum: "" },
    ]
    for (const irreversibleAuthorization of malformedBodies) {
      expectCode(
        () =>
          decideTerminalSagaBranches(
            withOperationStep(operation, stepId, {
              ...record,
              irreversibleAuthorization: irreversibleAuthorization as IrreversibleAuthorization,
            }),
            saga,
          ),
        "OperationInterventionRequiredError",
      )
    }
  })

  it("accepts exact terminal crash-absence and observed compensation classifications", async () => {
    let crashed = await fixture()
    let saga = begin(crashed.saga, "a", "forward", "crash-one", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "crash-one",
      errorChecksum: "first-absence",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = begin(saga, "a", "forward", "crash-two", 1_003)
    saga = markSagaActionNotDispatched(saga, {
      attemptId: "crash-two",
      errorChecksum: "terminal-crash-absence",
      phase: "forward",
      serverTimeMs: 1_004,
      stepId: "a",
    })
    const crashStepId = sagaActionOperationStepId("a", "forward")
    let crashOperation = withOperationStep(crashed.operation, crashStepId, {
      activeAttemptId: "crash-two",
      costCounters: Object.freeze({}),
      fencingToken: 2,
      lastAttemptId: "crash-two",
      progressCounters: Object.freeze({}),
      startedAttempts: 2,
      state: "running",
    })
    crashOperation = markRunningStepNotDispatchedAfterCrash(
      crashOperation,
      crashStepId,
      "terminal-crash-absence",
    )
    crashOperation = recordSagaStepTerminalClassification(crashOperation, {
      outcome: "not_applied",
      receiptOutcomeChecksum: "terminal-crash-absence",
      stepId: crashStepId,
    })
    expect(crashOperation.steps[crashStepId]).toEqual({
      costCounters: Object.freeze({}),
      fencingToken: 2,
      lastAttemptId: "crash-two",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "terminal-crash-absence",
      resultChecksum: "terminal-crash-absence",
      startedAttempts: 2,
      state: "succeeded",
    })
    crashed = { operation: crashOperation, saga }
    expect(saga.status).toBe("failed")
    expect(decideTerminalSagaBranches(crashed.operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])

    let classifiedDirect = await fixture()
    saga = begin(classifiedDirect.saga, "a", "forward", "direct-one", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "direct-one",
      errorChecksum: "direct-effect-error-one",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = begin(saga, "a", "forward", "direct-two", 1_003)
    saga = recordSagaActionFailure(saga, {
      attemptId: "direct-two",
      errorChecksum: "direct-effect-error-two",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_004,
      stepId: "a",
    })
    const directStepId = sagaActionOperationStepId("a", "forward")
    let directOperation = withOperationStep(classifiedDirect.operation, directStepId, {
      activeAttemptId: "direct-two",
      costCounters: Object.freeze({}),
      fencingToken: 2,
      lastAttemptId: "direct-two",
      progressCounters: Object.freeze({}),
      startedAttempts: 2,
      state: "running",
    })
    directOperation = recordStepFailure(directOperation, {
      attemptId: "direct-two",
      errorChecksum: "direct-receipt-outcome",
      outcome: "definitely_not_applied",
      stepId: directStepId,
    })
    directOperation = recordSagaStepTerminalClassification(directOperation, {
      outcome: "not_applied",
      receiptOutcomeChecksum: "direct-receipt-outcome",
      stepId: directStepId,
    })
    classifiedDirect = { operation: directOperation, saga }
    expect(saga.status).toBe("failed")
    expect(decideTerminalSagaBranches(classifiedDirect.operation, saga)).toEqual([
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      },
      {
        ...decisionEvidence(saga),
        kind: "not_required",
        stepId: sagaActionOperationStepId("a", "compensation"),
      },
    ])

    let compensated = await fixture()
    saga = begin(compensated.saga, "a", "forward", "forward", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    saga = succeed(saga, "a", "forward", "forward", 1_003)
    compensated = {
      operation: withSelectedAction(compensated.operation, saga, "a", "forward"),
      saga,
    }
    compensated = {
      ...compensated,
      operation: withExplicitTermination(compensated.operation),
      saga,
    }
    saga = begin(saga, "a", "compensation", "compensation-one", 1_004)
    saga = recordSagaActionFailure(saga, {
      attemptId: "compensation-one",
      errorChecksum: "compensation-unknown-one",
      outcome: "unknown",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "compensation-absence-one",
      outcome: "not_applied",
      phase: "compensation",
      serverTimeMs: 1_006,
      stepId: "a",
    })
    saga = begin(saga, "a", "compensation", "compensation-two", 1_007)
    saga = recordSagaActionFailure(saga, {
      attemptId: "compensation-two",
      errorChecksum: "compensation-unknown-two",
      outcome: "unknown",
      phase: "compensation",
      serverTimeMs: 1_008,
      stepId: "a",
    })
    saga = recordSagaObservation(saga, {
      evidenceChecksum: "compensation-terminal-absence",
      outcome: "not_applied",
      phase: "compensation",
      serverTimeMs: 1_009,
      stepId: "a",
    })
    const compensationStepId = sagaActionOperationStepId("a", "compensation")
    const compensationRecord: OperationStepRecord = Object.freeze({
      costCounters: Object.freeze({}),
      errorChecksum: "generic-compensation-unknown",
      fencingToken: 2,
      lastAttemptId: "compensation-two",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "compensation-terminal-absence",
      resultChecksum: "compensation-terminal-absence",
      startedAttempts: 2,
      state: "succeeded",
    })
    compensated = {
      operation: withOperationStep(compensated.operation, compensationStepId, compensationRecord),
      saga,
    }
    expect(saga.status).toBe("intervention_required")
    expect(decideTerminalSagaBranches(compensated.operation, saga)).toEqual([])
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(compensated.operation, compensationStepId, {
            ...compensationRecord,
            resultChecksum: "different-observation",
          }),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
  })

  it("accepts exact prior branch decisions and an exact terminal settlement", async () => {
    let { operation, saga } = await fixture()
    saga = begin(saga, "a", "forward", "a-forward", 1_001)
    saga = succeed(saga, "a", "forward", "a-forward", 1_002)
    operation = withSelectedAction(operation, saga, "a", "forward")
    operation = withOperationStep(
      operation,
      SAGA_TERMINATION_OPERATION_STEP_ID,
      notRequiredRecord(),
    )
    operation = withOperationStep(
      operation,
      sagaActionOperationStepId("a", "compensation"),
      notRequiredRecord(),
    )
    operation = withOperationStep(
      operation,
      SAGA_SETTLE_OPERATION_STEP_ID,
      atomicRecord("succeeded", TERMINAL_SAGA_CHECKSUM),
    )

    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])
  })

  it("binds prior branch and final settlement records to exact terminal model evidence", async () => {
    let { operation, saga } = await fixture()
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_001 })
    operation = withExplicitTermination(operation)
    const forwardId = sagaActionOperationStepId("a", "forward")
    const compensationId = sagaActionOperationStepId("a", "compensation")
    operation = withOperationStep(operation, forwardId, notRequiredRecord())
    operation = withOperationStep(operation, compensationId, notRequiredRecord())

    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, forwardId, notRequiredRecord("stale-saga-checksum")),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(
            operation,
            SAGA_SETTLE_OPERATION_STEP_ID,
            atomicRecord("failed", "stale-saga-checksum"),
          ),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
    operation = withOperationStep(
      operation,
      SAGA_SETTLE_OPERATION_STEP_ID,
      atomicRecord("failed", TERMINAL_SAGA_CHECKSUM),
    )
    expect(decideTerminalSagaBranches(operation, saga)).toEqual([])

    expectCode(
      () =>
        decideBranches(operation, saga, {
          ...sagaTerminalModelEvidence(saga),
          stateVersion: saga.stateVersion + 1,
        }),
      "OperationInterventionRequiredError",
    )
  })

  it("rejects nonterminal sagas before making branch decisions", async () => {
    const run = await fixture()
    expectCode(
      () => decideTerminalSagaBranches(run.operation, run.saga),
      "OperationResumeRequiredError",
    )
  })

  it("rejects unverified plan copies and exact operation-step membership drift", async () => {
    const terminal = await fixture()
    const saga = requestSagaTermination(terminal.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const operation = withExplicitTermination(terminal.operation)
    expectCode(
      () => decideTerminalSagaBranches({ ...operation, plan: { ...operation.plan } }, saga),
      "OperationInterventionRequiredError",
    )
    expect(() =>
      decideTerminalSagaBranches(
        {
          ...operation,
          steps: {
            ...operation.steps,
            unexpected: operation.steps["saga:init"] as OperationStepRecord,
          },
        },
        saga,
      ),
    ).toThrow(/membership or order/u)

    expect(() =>
      decideTerminalSagaBranches(operation, {
        ...saga,
        steps: { ...saga.steps, unexpected: saga.steps.a },
      } as SagaRecord),
    ).toThrow(/step membership/u)
    expect(() =>
      decideTerminalSagaBranches(operation, {
        ...saga,
        steps: {
          ...saga.steps,
          a: { ...saga.steps.a, outsideChecksum: "unchecked" },
        },
      } as SagaRecord),
    ).toThrow(/step record/u)
    expectCode(
      () =>
        decideTerminalSagaBranches(operation, {
          ...saga,
          steps: new Map(),
        } as unknown as SagaRecord),
      "OperationInterventionRequiredError",
    )
  })

  it("rejects malformed termination tuples and a failure cause without a failed forward", async () => {
    const run = await fixture()
    const fabricatedFailure = {
      ...run.saga,
      stateVersion: 1,
      status: "failed",
      terminationCause: "failure",
      terminationRequestedAtMs: 1_001,
    } as SagaRecord
    const malformed = [
      fabricatedFailure,
      {
        ...fabricatedFailure,
        terminationCause: "unsupported",
      } as unknown as SagaRecord,
      { ...fabricatedFailure, terminationCause: "cancellation", terminationRequestedAtMs: null },
      { ...fabricatedFailure, terminationCause: "timeout", terminationRequestedAtMs: -1 },
      { ...fabricatedFailure, stateVersion: 0 },
    ] as const
    for (const saga of malformed) {
      expectCode(
        () => decideTerminalSagaBranches(run.operation, saga),
        "OperationInterventionRequiredError",
      )
    }
  })

  it("rejects impossible compensation, serial, reverse-order, and uncertainty histories", async () => {
    const successfulAction = (idempotencyKey: string, attemptId: string): SagaActionRecord => ({
      attempts: 1,
      idempotencyKey,
      lastAttemptId: attemptId,
      nextAttemptAtMs: 0,
      resultChecksum: `${attemptId}-result`,
      state: "succeeded",
    })
    const unknownAction = (idempotencyKey: string, attemptId: string): SagaActionRecord => ({
      attempts: 1,
      errorChecksum: `${attemptId}-error`,
      idempotencyKey,
      lastAttemptId: attemptId,
      nextAttemptAtMs: 0,
      state: "unknown",
    })

    const one = await fixture()
    const cancelled = requestSagaTermination(one.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const compensationBeforeForward = {
      ...cancelled,
      steps: {
        ...cancelled.steps,
        a: {
          ...cancelled.steps.a,
          compensation: successfulAction(
            (cancelled.steps.a as SagaRecord["steps"][string]).compensation.idempotencyKey,
            "compensation-before-forward",
          ),
        },
      },
    } as SagaRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withExplicitTermination(one.operation),
          compensationBeforeForward,
        ),
      "OperationInterventionRequiredError",
    )

    let compensated = await fixture([reversibleStep("a"), reversibleStep("b")])
    let saga = begin(compensated.saga, "a", "forward", "a-forward", 1_001)
    saga = succeed(saga, "a", "forward", "a-forward", 1_002)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    saga = begin(saga, "a", "compensation", "a-compensation", 1_004)
    saga = succeed(saga, "a", "compensation", "a-compensation", 1_005)
    const compensationWithoutTermination = {
      ...saga,
      status: "running",
      terminationCause: null,
      terminationRequestedAtMs: null,
    } as SagaRecord
    compensated = {
      operation: withSelectedAction(
        withSelectedAction(compensated.operation, saga, "a", "forward"),
        saga,
        "a",
        "compensation",
      ),
      saga,
    }
    expectCode(
      () => decideTerminalSagaBranches(compensated.operation, compensationWithoutTermination),
      "OperationInterventionRequiredError",
    )

    const two = await fixture([reversibleStep("a"), reversibleStep("b")])
    const terminatedTwo = requestSagaTermination(two.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const outOfOrderForward = {
      ...terminatedTwo,
      steps: {
        ...terminatedTwo.steps,
        b: {
          ...terminatedTwo.steps.b,
          forward: successfulAction(
            (terminatedTwo.steps.b as SagaRecord["steps"][string]).forward.idempotencyKey,
            "b-before-a",
          ),
        },
      },
    } as SagaRecord

    let reverseSaga = begin(two.saga, "a", "forward", "a", 1_001)
    reverseSaga = succeed(reverseSaga, "a", "forward", "a", 1_002)
    reverseSaga = begin(reverseSaga, "b", "forward", "b", 1_003)
    reverseSaga = succeed(reverseSaga, "b", "forward", "b", 1_004)
    reverseSaga = requestSagaTermination(reverseSaga, {
      cause: "cancellation",
      serverTimeMs: 1_005,
    })
    const reverseCompensation = {
      ...reverseSaga,
      steps: {
        ...reverseSaga.steps,
        a: {
          ...reverseSaga.steps.a,
          compensation: successfulAction(
            (reverseSaga.steps.a as SagaRecord["steps"][string]).compensation.idempotencyKey,
            "a-before-b-compensation",
          ),
        },
      },
    } as SagaRecord

    const multipleUnknown = {
      ...terminatedTwo,
      steps: Object.fromEntries(
        ["a", "b"].map((stepId) => {
          const step = terminatedTwo.steps[stepId] as SagaRecord["steps"][string]
          return [
            stepId,
            {
              ...step,
              forward: unknownAction(step.forward.idempotencyKey, `${stepId}-unknown`),
            },
          ]
        }),
      ),
    } as SagaRecord
    const multipleUnknownCompensations = {
      ...reverseSaga,
      steps: Object.fromEntries(
        ["a", "b"].map((stepId) => {
          const step = reverseSaga.steps[stepId] as SagaRecord["steps"][string]
          return [
            stepId,
            {
              ...step,
              compensation: unknownAction(
                step.compensation.idempotencyKey,
                `${stepId}-compensation-unknown`,
              ),
            },
          ]
        }),
      ),
    } as SagaRecord

    for (const candidate of [
      outOfOrderForward,
      reverseCompensation,
      multipleUnknown,
      multipleUnknownCompensations,
    ]) {
      expectCode(
        () => decideTerminalSagaBranches(withExplicitTermination(two.operation), candidate),
        "OperationInterventionRequiredError",
      )
    }
  })

  it("rejects contradictory init, termination, action, omission, and settlement records", async () => {
    let { operation, saga } = await fixture()
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_001 })
    operation = withExplicitTermination(operation)
    const forwardId = sagaActionOperationStepId("a", "forward")
    const compensationId = sagaActionOperationStepId("a", "compensation")
    const variants = [
      withOperationStep(
        operation,
        SAGA_INIT_OPERATION_STEP_ID,
        operation.steps[forwardId] as OperationStepRecord,
      ),
      withOperationStep(
        operation,
        SAGA_TERMINATION_OPERATION_STEP_ID,
        operation.steps[forwardId] as OperationStepRecord,
      ),
      withOperationStep(operation, forwardId, atomicRecord("succeeded", "fabricated")),
      withOperationStep(operation, forwardId, {
        ...notRequiredRecord(),
        fencingToken: 1,
      }),
      withOperationStep(operation, SAGA_SETTLE_OPERATION_STEP_ID, atomicRecord("succeeded", "x")),
      withOperationStep(operation, SAGA_SETTLE_OPERATION_STEP_ID, atomicRecord("failed", "x")),
      withOperationStep(operation, compensationId, {
        costCounters: Object.freeze({ invalid: -1 }),
        progressCounters: Object.freeze({}),
        startedAttempts: 0,
        state: "pending",
      }),
    ]
    for (const variant of variants) {
      expectCode(
        () => decideTerminalSagaBranches(variant, saga),
        "OperationInterventionRequiredError",
      )
    }
  })

  it("keeps structural model inputs distinct from complete-history authority", async () => {
    let run = await fixture()
    let saga = requestSagaTermination(run.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    run = { operation: withExplicitTermination(run.operation), saga }
    const structuralCopy = structuredClone(saga)
    expectCode(
      () => decideTerminalSagaBranches(run.operation, structuralCopy),
      "OperationInterventionRequiredError",
    )
    const loadedCopy = await loadSagaRecord(structuralCopy, digest)
    expect(decideTerminalSagaBranches(run.operation, loadedCopy)).toEqual(
      decideTerminalSagaBranches(run.operation, saga),
    )
    const tamperedDescriptor = {
      ...saga.descriptor,
      steps: saga.descriptor.steps.map((step) => ({ ...step, irreversible: true })),
    }
    expectCode(
      () =>
        decideTerminalSagaBranches(run.operation, {
          ...saga,
          descriptor: tamperedDescriptor,
        } as SagaRecord),
      "OperationInterventionRequiredError",
    )
    let running = await fixture()
    saga = begin(running.saga, "a", "forward", "still-running", 1_001)
    saga = { ...saga, status: "intervention_required" }
    running = { operation: running.operation, saga }

    expectCode(
      () => decideTerminalSagaBranches(running.operation, running.saga),
      "OperationInterventionRequiredError",
    )

    const malformedOperationSteps = { ...run.operation, steps: null } as unknown as OperationRecord
    const malformedAction = {
      ...run.saga,
      steps: {
        ...run.saga.steps,
        a: { ...run.saga.steps.a, forward: undefined },
      },
    } as unknown as SagaRecord
    expect(() => decideTerminalSagaBranches(malformedOperationSteps, run.saga)).toThrow(
      /record envelope/u,
    )
    expectCode(
      () => decideTerminalSagaBranches(run.operation, malformedAction),
      "OperationInterventionRequiredError",
    )
  })

  it("captures hostile model inputs once and normalizes snapshot failures", async () => {
    let run = await fixture()
    const saga = requestSagaTermination(run.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    run = { operation: withExplicitTermination(run.operation), saga }

    let statusReads = 0
    const switchingSaga = { ...saga }
    Object.defineProperty(switchingSaga, "status", {
      enumerable: true,
      get() {
        statusReads += 1
        return statusReads === 1 ? saga.status : "succeeded"
      },
    })
    expect(decideTerminalSagaBranches(run.operation, switchingSaga)).toEqual(
      decideTerminalSagaBranches(run.operation, saga),
    )
    expect(statusReads).toBe(1)

    const evidence = sagaTerminalModelEvidence(saga)
    const hostileNestedPlan = new Proxy(run.operation.plan, {
      getPrototypeOf() {
        throw new Error("private nested plan proxy detail")
      },
    })
    expectCode(
      () => decideBranches({ ...run.operation, plan: hostileNestedPlan }, saga, evidence),
      "OperationInterventionRequiredError",
    )
    for (const [operation, candidateSaga, candidateEvidence] of [
      [
        new Proxy(run.operation, {
          getPrototypeOf() {
            throw new Error("private operation proxy detail")
          },
        }),
        saga,
        evidence,
      ],
      [run.operation, { ...saga, steps: new Proxy(saga.steps, {}) }, evidence],
      [run.operation, saga, new Proxy(evidence, {})],
    ] as const) {
      expect(() => decideBranches(operation, candidateSaga, candidateEvidence)).toThrowError(
        expect.objectContaining({
          code: "OperationInterventionRequiredError",
          message: "Saga terminal model inputs could not be captured safely.",
        }),
      )
    }
    for (const [operation, candidateSaga] of [
      [{ ...run.operation, unexpected: true } as unknown as OperationRecord, saga],
      [run.operation, { ...saga, unexpected: true } as unknown as SagaRecord],
    ] as const) {
      expect(() => decideBranches(operation, candidateSaga, evidence)).toThrowError(
        expect.objectContaining({
          code: "OperationInterventionRequiredError",
          message: "Saga terminal model inputs could not be captured safely.",
        }),
      )
    }
  })

  it("rejects polluted pending and incomplete selected saga actions", async () => {
    let run = await fixture()
    let saga = requestSagaTermination(run.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    run = { operation: withExplicitTermination(run.operation), saga }
    const pending = saga.steps.a?.forward as SagaActionRecord
    const pollutedPending = {
      ...saga,
      steps: {
        ...saga.steps,
        a: {
          ...saga.steps.a,
          forward: { ...pending, attempts: 3, lastAttemptId: "ghost-attempt" },
        },
      },
    } as unknown as SagaRecord
    expectCode(
      () => decideTerminalSagaBranches(run.operation, pollutedPending),
      "OperationInterventionRequiredError",
    )
    const undefinedEvidence = {
      ...saga,
      steps: {
        ...saga.steps,
        a: {
          ...saga.steps.a,
          forward: { ...pending, resultChecksum: undefined },
        },
      },
    } as unknown as SagaRecord
    expectCode(
      () => decideTerminalSagaBranches(run.operation, undefinedEvidence),
      "OperationInterventionRequiredError",
    )
    const notRequiredForward = {
      ...saga,
      steps: {
        ...saga.steps,
        a: {
          ...saga.steps.a,
          forward: { ...pending, state: "not_required" },
        },
      },
    } as SagaRecord
    expectCode(
      () => decideTerminalSagaBranches(run.operation, notRequiredForward),
      "OperationInterventionRequiredError",
    )

    let exhausted = await fixture()
    saga = begin(exhausted.saga, "a", "forward", "exhausted", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "exhausted",
      errorChecksum: "not-applied",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    const exhaustedRetryable = {
      ...saga,
      steps: {
        ...saga.steps,
        a: {
          ...saga.steps.a,
          forward: {
            ...(saga.steps.a as SagaRecord["steps"][string]).forward,
            attempts: 2,
          },
        },
      },
    } as SagaRecord
    exhausted = { operation: withExplicitTermination(exhausted.operation), saga }
    expectCode(
      () => decideTerminalSagaBranches(exhausted.operation, exhaustedRetryable),
      "OperationInterventionRequiredError",
    )

    const irreversible = await fixture([irreversibleStep("commit")])
    const cancelledIrreversible = requestSagaTermination(irreversible.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const pendingCompensation = {
      ...cancelledIrreversible,
      steps: {
        ...cancelledIrreversible.steps,
        commit: {
          ...cancelledIrreversible.steps.commit,
          compensation: {
            ...cancelledIrreversible.steps.commit?.compensation,
            state: "pending",
          },
        },
      },
    } as SagaRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withExplicitTermination(irreversible.operation),
          pendingCompensation,
        ),
      "OperationInterventionRequiredError",
    )

    let failed = await fixture()
    saga = begin(failed.saga, "a", "forward", "failed-attempt", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "failed-attempt",
      errorChecksum: "failed-error",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    failed = { operation: withSelectedAction(failed.operation, saga, "a", "forward"), saga }
    const failedForward = saga.steps.a?.forward
    if (failedForward === undefined) throw new Error("Failed saga action is missing.")
    const { errorChecksum: _errorChecksum, ...withoutError } = failedForward
    const incompleteFailure = {
      ...saga,
      steps: {
        ...saga.steps,
        a: { ...saga.steps.a, forward: withoutError },
      },
    } as SagaRecord
    expectCode(
      () => decideTerminalSagaBranches(failed.operation, incompleteFailure),
      "OperationInterventionRequiredError",
    )
  })

  it("rejects a nonterminal selected action even in a structurally loaded terminal projection", async () => {
    let { operation, saga } = await fixture([reversibleStep("a"), reversibleStep("b")])
    saga = begin(saga, "a", "forward", "a-forward", 1_001)
    saga = succeed(saga, "a", "forward", "a-forward", 1_002)
    saga = begin(saga, "b", "forward", "b-running", 1_003)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_004 })
    const candidate = {
      ...structuredClone(saga),
      status: "intervention_required",
      steps: {
        ...structuredClone(saga.steps),
        a: {
          ...structuredClone(saga.steps.a),
          compensation: {
            attempts: 1,
            errorChecksum: "a-compensation-error",
            idempotencyKey: saga.steps.a?.compensation.idempotencyKey as string,
            lastAttemptId: "a-compensation",
            nextAttemptAtMs: 0,
            observationEvidenceChecksum: "a-compensation-observation",
            state: "intervention_required",
          },
        },
      },
    }
    saga = await loadSagaRecord(candidate, digest)
    operation = withExplicitTermination(operation)
    operation = withSelectedAction(operation, saga, "a", "forward")
    operation = withSelectedAction(operation, saga, "a", "compensation")
    operation = withSelectedAction(operation, saga, "b", "forward")

    expect(() => decideTerminalSagaBranches(operation, saga)).toThrow(/is not terminal/u)
  })

  it("rejects changed selected attempts, observations, retry evidence, and authorization", async () => {
    let success = await fixture()
    let saga = begin(success.saga, "a", "forward", "selected", 1_001)
    saga = succeed(saga, "a", "forward", "selected", 1_002)
    let operation = withSelectedAction(success.operation, saga, "a", "forward")
    success = { operation, saga }
    const selectedId = sagaActionOperationStepId("a", "forward")
    const selected = operation.steps[selectedId] as OperationStepRecord

    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, selectedId, { ...selected, lastAttemptId: "changed" }),
          saga,
        ),
      "OperationInterventionRequiredError",
    )

    const retryable = await fixture()
    saga = begin(retryable.saga, "a", "forward", "retry", 1_001)
    saga = recordSagaActionFailure(saga, {
      attemptId: "retry",
      errorChecksum: "absence",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_003 })
    operation = withExplicitTermination(retryable.operation)
    operation = withOperationStep(operation, selectedId, {
      costCounters: Object.freeze({}),
      fencingToken: 1,
      lastAttemptId: "retry",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "different",
      startedAttempts: 1,
      state: "retryable_failed",
    })
    expectCode(
      () => decideTerminalSagaBranches(operation, saga),
      "OperationInterventionRequiredError",
    )

    operation = withOperationStep(operation, selectedId, {
      costCounters: Object.freeze({}),
      errorChecksum: "forged-extra-error",
      fencingToken: 1,
      lastAttemptId: "retry",
      progressCounters: Object.freeze({}),
      reconciliationEvidenceChecksum: "absence",
      startedAttempts: 1,
      state: "retryable_failed",
    })
    expectCode(
      () => decideTerminalSagaBranches(operation, saga),
      "OperationInterventionRequiredError",
    )

    const irreversible = await fixture([irreversibleStep("commit")])
    saga = begin(irreversible.saga, "commit", "forward", "commit", 1_001)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_002 })
    saga = succeed(saga, "commit", "forward", "commit", 1_003)
    operation = withExplicitTermination(irreversible.operation)
    operation = withSelectedAction(operation, saga, "commit", "forward")
    const commitId = sagaActionOperationStepId("commit", "forward")
    const { authorizationChecksum: _authorizationChecksum, ...unauthorized } = operation.steps[
      commitId
    ] as OperationStepRecord
    expectCode(
      () =>
        decideTerminalSagaBranches(
          withOperationStep(operation, commitId, unauthorized as OperationStepRecord),
          saga,
        ),
      "OperationInterventionRequiredError",
    )
  })

  it("rejects trusted operation plans that are not the canonical saga plan", async () => {
    const run = await fixture()
    const saga = requestSagaTermination(run.saga, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const operation = withExplicitTermination(run.operation)
    const {
      planChecksum: _planChecksum,
      schemaVersion: _schemaVersion,
      ...input
    } = run.operation.plan
    const changedEnvelope = await sealOperationPlan(
      { ...input, operationType: "saga:other@1" },
      digest,
    )
    const changedStep = await sealOperationPlan(
      {
        ...input,
        steps: input.steps.map((step) =>
          step.stepId === SAGA_INIT_OPERATION_STEP_ID
            ? { ...step, recoveryInstructions: "A different trusted recovery contract." }
            : step,
        ),
      },
      digest,
    )

    for (const plan of [changedEnvelope, changedStep]) {
      expectCode(
        () => decideTerminalSagaBranches(Object.freeze({ plan, steps: operation.steps }), saga),
        "OperationInterventionRequiredError",
      )
    }
  })

  it("preserves deterministic branch order across generated plan widths", async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 6 }), fc.boolean(), async (width, cancel) => {
        const steps = Array.from({ length: width }, (_, index) => reversibleStep(`s${index}`))
        let { operation, saga } = await fixture(steps)
        if (cancel) {
          saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_001 })
          operation = withExplicitTermination(operation)
          expect(
            decideTerminalSagaBranches(operation, saga).map((decision) => decision.stepId),
          ).toEqual(
            steps.flatMap((step) => [
              sagaActionOperationStepId(step.stepId, "forward"),
              sagaActionOperationStepId(step.stepId, "compensation"),
            ]),
          )
          return
        }
        for (const [index, step] of steps.entries()) {
          const attemptId = `${step.stepId}-forward`
          saga = begin(saga, step.stepId, "forward", attemptId, 1_001 + index * 2)
          saga = succeed(saga, step.stepId, "forward", attemptId, 1_002 + index * 2)
          operation = withSelectedAction(operation, saga, step.stepId, "forward")
        }
        expect(
          decideTerminalSagaBranches(operation, saga).map((decision) => decision.stepId),
        ).toEqual([
          SAGA_TERMINATION_OPERATION_STEP_ID,
          ...steps.map((step) => sagaActionOperationStepId(step.stepId, "compensation")),
        ])
      }),
      { numRuns: 20 },
    )
  })

  it("bounds deterministic decisions at the sealed 256-step maximum", async () => {
    const steps = Array.from({ length: 256 }, (_, index) => reversibleStep(`max-${index}`))
    let { operation, saga } = await fixture(steps)
    saga = requestSagaTermination(saga, { cause: "cancellation", serverTimeMs: 1_001 })
    operation = withExplicitTermination(operation)

    const decisions = decideTerminalSagaBranches(operation, saga)

    expect(decisions).toHaveLength(512)
    expect(new Set(decisions.map((decision) => decision.stepId)).size).toBe(512)
    expect(decisions[0]).toEqual({
      ...decisionEvidence(saga),
      kind: "not_required",
      stepId: sagaActionOperationStepId("max-0", "forward"),
    })
    expect(decisions.at(-1)).toEqual({
      ...decisionEvidence(saga),
      kind: "not_required",
      stepId: sagaActionOperationStepId("max-255", "compensation"),
    })
  })
})
