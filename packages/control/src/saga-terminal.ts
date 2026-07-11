import {
  mapSagaSettlementOutcome,
  NozzleError,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  type SagaActionPhase,
  type SagaActionRecord,
  type SagaRecord,
  sagaActionIdempotencyKey,
} from "@nozzle/core"
import { assertTrustedSagaOperationPlan } from "./saga-plan.js"
import {
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "./saga-store.js"

const OPERATION_KEYS = ["plan", "steps"] as const
const SAGA_KEYS = [
  "deadlineAtMs",
  "descriptor",
  "idempotencyKey",
  "inputChecksum",
  "sagaId",
  "stateVersion",
  "status",
  "steps",
  "terminationCause",
  "terminationRequestedAtMs",
] as const
const SAGA_ACTION_KEYS = new Set([
  "activeAttemptId",
  "attempts",
  "errorChecksum",
  "idempotencyKey",
  "lastAttemptId",
  "nextAttemptAtMs",
  "observationEvidenceChecksum",
  "resultChecksum",
  "state",
])
const SAGA_STEP_KEYS = ["compensation", "forward", "inputChecksum"] as const

/** Caller-supplied inputs for the pure terminal model; this is not persistence authority. */
export interface SagaTerminalModelEvidence {
  readonly irreversibleAuthorizationChecksums: Readonly<Record<string, string>>
  readonly sagaChecksum: string
  readonly stateVersion: number
}

export type SagaTerminalModelBranchDecision =
  | {
      readonly kind: "not_required"
      readonly sagaChecksum: string
      readonly stateVersion: number
      readonly stepId: string
    }
  | {
      readonly attemptId: string
      readonly evidenceChecksum: string
      readonly evidenceKind: "crash_absence" | "direct_receipt" | "observation"
      readonly kind: "terminal_not_applied"
      readonly sagaChecksum: string
      readonly stateVersion: number
      readonly stepId: string
    }

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  return Object.getPrototypeOf(value) === Object.prototype
}

function exactKeys(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  return (
    plainRecord(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => Object.hasOwn(value, key))
  )
}

function nonEmpty(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0
}

function positiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1
}

function emptyCounters(value: unknown): boolean {
  return plainRecord(value) && Object.keys(value).length === 0
}

function canonicalSagaAction(
  action: SagaActionRecord,
  expectedIdempotencyKey: string,
  maxAttempts: number,
  allowNotRequired: boolean,
): void {
  if (
    !plainRecord(action) ||
    !Object.keys(action).every((key) => SAGA_ACTION_KEYS.has(key)) ||
    !Object.hasOwn(action, "attempts") ||
    !Object.hasOwn(action, "idempotencyKey") ||
    !Object.hasOwn(action, "nextAttemptAtMs") ||
    !Object.hasOwn(action, "state") ||
    !Number.isSafeInteger(action.attempts) ||
    action.attempts < 0 ||
    action.attempts > maxAttempts ||
    (action.state === "retryable_failed" && action.attempts >= maxAttempts) ||
    action.idempotencyKey !== expectedIdempotencyKey ||
    !Number.isSafeInteger(action.nextAttemptAtMs) ||
    action.nextAttemptAtMs < 0 ||
    ![
      "failed",
      "intervention_required",
      "not_required",
      "pending",
      "retryable_failed",
      "running",
      "succeeded",
      "unknown",
    ].includes(action.state)
  ) {
    intervention("Saga action record is not canonical for its sealed plan.")
  }
  for (const key of [
    "activeAttemptId",
    "errorChecksum",
    "lastAttemptId",
    "observationEvidenceChecksum",
    "resultChecksum",
  ] as const) {
    if (
      Object.hasOwn(action, key) !== (action[key] !== undefined) ||
      (action[key] !== undefined && !nonEmpty(action[key]))
    ) {
      intervention("Saga action evidence is not canonical for its sealed plan.")
    }
  }
  const noAttempt = action.state === "pending" || action.state === "not_required"
  const errorState =
    action.state === "failed" ||
    action.state === "intervention_required" ||
    action.state === "retryable_failed" ||
    action.state === "unknown"
  if (
    (!allowNotRequired && action.state === "not_required") ||
    noAttempt !== (action.attempts === 0) ||
    (action.state === "running") !== (action.activeAttemptId !== undefined) ||
    (noAttempt ? action.lastAttemptId !== undefined : action.lastAttemptId === undefined) ||
    (action.state === "running" && action.activeAttemptId !== action.lastAttemptId) ||
    (action.state === "succeeded") !== (action.resultChecksum !== undefined) ||
    errorState !== (action.errorChecksum !== undefined) ||
    (action.state !== "retryable_failed" && action.nextAttemptAtMs !== 0) ||
    (action.observationEvidenceChecksum !== undefined &&
      action.state !== "failed" &&
      action.state !== "intervention_required" &&
      action.state !== "retryable_failed" &&
      action.state !== "succeeded")
  ) {
    intervention("Saga action state contradicts its canonical evidence.")
  }
}

function canonicalPlan(operation: OperationRecord, saga: SagaRecord): void {
  if (!exactKeys(operation, OPERATION_KEYS) || !plainRecord(operation.steps)) {
    intervention("Saga operation record envelope is not canonical.")
  }
  if (!plainRecord(saga.steps)) {
    intervention("Saga step projection is not canonical for its sealed descriptor.")
  }
  const validTerminationCause =
    saga.terminationCause === null ||
    saga.terminationCause === "cancellation" ||
    saga.terminationCause === "failure" ||
    saga.terminationCause === "timeout"
  if (
    !nonEmpty(saga.idempotencyKey) ||
    !nonEmpty(saga.inputChecksum) ||
    !nonEmpty(saga.sagaId) ||
    !Number.isSafeInteger(saga.deadlineAtMs) ||
    saga.deadlineAtMs < 0 ||
    !Number.isSafeInteger(saga.stateVersion) ||
    saga.stateVersion < 0 ||
    !validTerminationCause ||
    (saga.terminationRequestedAtMs !== null &&
      (!Number.isSafeInteger(saga.terminationRequestedAtMs) ||
        saga.terminationRequestedAtMs < 0)) ||
    (saga.terminationCause === null) !== (saga.terminationRequestedAtMs === null)
  ) {
    intervention("Saga terminal projection fields are malformed or contradictory.")
  }
  assertTrustedSagaOperationPlan(operation.plan, saga)
  const canonicalExpectedStepIds = operation.plan.steps.map((step) => step.stepId)
  const actualRecordStepIds = Object.keys(operation.steps).sort()
  if (
    actualRecordStepIds.length !== canonicalExpectedStepIds.length ||
    canonicalExpectedStepIds.some((stepId, index) => actualRecordStepIds[index] !== stepId)
  ) {
    intervention("Saga operation plan membership or order is not canonical.")
  }
  const expectedSagaStepIds = saga.descriptor.steps.map((step) => step.stepId).sort()
  const actualSagaStepIds = Object.keys(saga.steps).sort()
  if (
    actualSagaStepIds.length !== expectedSagaStepIds.length ||
    expectedSagaStepIds.some((stepId, index) => actualSagaStepIds[index] !== stepId)
  ) {
    intervention("Saga step membership is not canonical for its sealed descriptor.")
  }
  let uncertainActions = 0
  for (const descriptorStep of saga.descriptor.steps) {
    const stepId = descriptorStep.stepId
    const sagaStep = saga.steps[stepId] as SagaRecord["steps"][string]
    if (!exactKeys(sagaStep, SAGA_STEP_KEYS) || !nonEmpty(sagaStep.inputChecksum)) {
      intervention("Saga step record is not canonical for its sealed descriptor.")
    }
    canonicalSagaAction(
      sagaStep.forward,
      sagaActionIdempotencyKey(saga.sagaId, stepId, "forward"),
      descriptorStep.maxAttempts,
      false,
    )
    canonicalSagaAction(
      sagaStep.compensation,
      sagaActionIdempotencyKey(saga.sagaId, stepId, "compensation"),
      descriptorStep.maxAttempts,
      descriptorStep.irreversible,
    )
    if (
      (descriptorStep.irreversible
        ? sagaStep.compensation.state !== "not_required"
        : sagaStep.compensation.state === "not_required") ||
      (sagaStep.compensation.attempts > 0 && sagaStep.forward.state !== "succeeded")
    ) {
      intervention("Saga action phases contradict the sealed descriptor or causal order.")
    }
    if (sagaStep.forward.state === "running" || sagaStep.forward.state === "unknown") {
      uncertainActions += 1
    }
    if (sagaStep.compensation.state === "running" || sagaStep.compensation.state === "unknown") {
      uncertainActions += 1
    }
  }
  if (uncertainActions > 1) {
    intervention("Saga projection contains multiple uncertain actions.")
  }
  if (
    saga.stateVersion === 0 &&
    (saga.status !== "planned" ||
      saga.terminationCause !== null ||
      saga.descriptor.steps.some((step) => {
        const sagaStep = saga.steps[step.stepId] as SagaRecord["steps"][string]
        return (
          sagaStep.forward.state !== "pending" ||
          sagaStep.compensation.state !== (step.irreversible ? "not_required" : "pending")
        )
      }))
  ) {
    intervention("Saga version zero contradicts its initial projection.")
  }
  let priorSucceeded = true
  for (const descriptorStep of saga.descriptor.steps) {
    const forward = (saga.steps[descriptorStep.stepId] as SagaRecord["steps"][string]).forward
    if (forward.attempts > 0 && !priorSucceeded) {
      intervention("Saga forward attempts contradict sealed serial order.")
    }
    if (forward.state !== "succeeded") priorSucceeded = false
  }
  let laterCompensationSettled = true
  for (const descriptorStep of [...saga.descriptor.steps].reverse()) {
    const sagaStep = saga.steps[descriptorStep.stepId] as SagaRecord["steps"][string]
    if (sagaStep.compensation.attempts > 0 && !laterCompensationSettled) {
      intervention("Saga compensation attempts contradict sealed reverse order.")
    }
    if (
      sagaStep.forward.state === "succeeded" &&
      (descriptorStep.irreversible || sagaStep.compensation.state !== "succeeded")
    ) {
      laterCompensationSettled = false
    }
  }
  if (
    (saga.terminationCause === null &&
      saga.descriptor.steps.some(
        (step) =>
          (saga.steps[step.stepId] as SagaRecord["steps"][string]).compensation.attempts > 0,
      )) ||
    (saga.terminationCause === "failure" &&
      !saga.descriptor.steps.some(
        (step) =>
          (saga.steps[step.stepId] as SagaRecord["steps"][string]).forward.state === "failed",
      ))
  ) {
    intervention("Saga termination cause contradicts its action history.")
  }
}

function validateTerminalEvidence(
  evidence: SagaTerminalModelEvidence,
  saga: SagaRecord,
): SagaTerminalModelEvidence {
  if (
    !exactKeys(evidence, ["irreversibleAuthorizationChecksums", "sagaChecksum", "stateVersion"]) ||
    !plainRecord(evidence.irreversibleAuthorizationChecksums) ||
    !nonEmpty(evidence.sagaChecksum) ||
    !Number.isSafeInteger(evidence.stateVersion) ||
    evidence.stateVersion < 0 ||
    evidence.stateVersion !== saga.stateVersion
  ) {
    intervention("Saga terminal model evidence is malformed or stale.")
  }
  const expectedAuthorizationStepIds = saga.descriptor.steps
    .filter(
      (step) =>
        step.irreversible &&
        (saga.steps[step.stepId] as SagaRecord["steps"][string]).forward.attempts > 0,
    )
    .map((step) => sagaActionOperationStepId(step.stepId, "forward"))
    .sort()
  const actualAuthorizationStepIds = Object.keys(evidence.irreversibleAuthorizationChecksums).sort()
  if (
    expectedAuthorizationStepIds.length !== actualAuthorizationStepIds.length ||
    expectedAuthorizationStepIds.some(
      (stepId, index) =>
        stepId !== actualAuthorizationStepIds[index] ||
        !nonEmpty(evidence.irreversibleAuthorizationChecksums[stepId]),
    )
  ) {
    intervention("Saga terminal model has incomplete or unexpected irreversible authorization.")
  }
  return evidence
}

function pristinePending(record: OperationStepRecord): boolean {
  return (
    exactKeys(record, ["costCounters", "progressCounters", "startedAttempts", "state"]) &&
    emptyCounters(record.costCounters) &&
    emptyCounters(record.progressCounters) &&
    record.startedAttempts === 0 &&
    record.state === "pending"
  )
}

function exactNotRequired(record: OperationStepRecord, sagaChecksum: string): boolean {
  return (
    exactKeys(record, [
      "costCounters",
      "progressCounters",
      "reconciliationEvidenceChecksum",
      "startedAttempts",
      "state",
    ]) &&
    emptyCounters(record.costCounters) &&
    emptyCounters(record.progressCounters) &&
    record.reconciliationEvidenceChecksum === sagaChecksum &&
    record.startedAttempts === 0 &&
    record.state === "not_required"
  )
}

function exactAtomicTerminal(
  record: OperationStepRecord,
  state: "failed" | "intervention_required" | "succeeded",
  expectedEvidenceChecksum?: string,
): boolean {
  const evidenceKey =
    state === "succeeded"
      ? "resultChecksum"
      : state === "failed"
        ? "errorChecksum"
        : "reconciliationEvidenceChecksum"
  return (
    exactKeys(record, [
      "costCounters",
      "fencingToken",
      "lastAttemptId",
      "progressCounters",
      evidenceKey,
      "startedAttempts",
      "state",
    ]) &&
    emptyCounters(record.costCounters) &&
    emptyCounters(record.progressCounters) &&
    positiveInteger(record.fencingToken) &&
    nonEmpty(record.lastAttemptId) &&
    (expectedEvidenceChecksum === undefined
      ? nonEmpty(record[evidenceKey])
      : record[evidenceKey] === expectedEvidenceChecksum) &&
    record.startedAttempts === 1 &&
    record.state === state
  )
}

function selectedKeys(
  plan: OperationStepPlan,
  state: "intervention_required" | "succeeded",
  generic: OperationStepRecord,
): readonly string[] {
  return [
    "costCounters",
    "fencingToken",
    "lastAttemptId",
    "progressCounters",
    ...(plan.checkpoint === "irreversible" ? ["authorizationChecksum"] : []),
    ...(generic.errorChecksum === undefined ? [] : ["errorChecksum"]),
    ...(generic.reconciliationEvidenceChecksum === undefined
      ? []
      : ["reconciliationEvidenceChecksum"]),
    ...(state === "succeeded" ? ["resultChecksum"] : []),
    "startedAttempts",
    "state",
  ]
}

function exactAuthorization(
  generic: OperationStepRecord,
  plan: OperationStepPlan,
  expectedAuthorizationChecksum: string | undefined,
): boolean {
  return plan.checkpoint === "irreversible"
    ? nonEmpty(expectedAuthorizationChecksum) &&
        generic.authorizationChecksum === expectedAuthorizationChecksum
    : expectedAuthorizationChecksum === undefined && generic.authorizationChecksum === undefined
}

function exactSelectedAction(
  action: SagaActionRecord,
  generic: OperationStepRecord,
  plan: OperationStepPlan,
  phase: SagaActionPhase,
  expectedAuthorizationChecksum: string | undefined,
): void {
  if (
    action.state !== "succeeded" &&
    action.state !== "failed" &&
    action.state !== "intervention_required"
  ) {
    intervention(`Selected saga action ${plan.stepId} is not terminal.`)
  }
  const observationEvidence = action.observationEvidenceChecksum
  if (action.state === "failed" && phase !== "forward") {
    intervention(`Selected saga action ${plan.stepId} has an invalid failed phase.`)
  }
  let expectedState: "intervention_required" | "succeeded" = "succeeded"
  if (action.state === "intervention_required") {
    if (phase === "forward" || generic.state === "intervention_required") {
      expectedState = "intervention_required"
    }
    if (expectedState === "intervention_required" && observationEvidence === undefined) {
      intervention(`Selected saga action ${plan.stepId} has no indeterminate observation.`)
    }
  }
  const directReceipt =
    expectedState === "succeeded" &&
    generic.errorChecksum === undefined &&
    generic.reconciliationEvidenceChecksum === undefined &&
    nonEmpty(generic.resultChecksum)
  const classifiedDirectReceipt =
    expectedState === "succeeded" &&
    observationEvidence === undefined &&
    nonEmpty(action.errorChecksum) &&
    nonEmpty(generic.errorChecksum) &&
    generic.reconciliationEvidenceChecksum === generic.errorChecksum &&
    generic.resultChecksum === generic.errorChecksum
  const crashAbsence =
    expectedState === "succeeded" &&
    observationEvidence === undefined &&
    generic.errorChecksum === undefined &&
    nonEmpty(action.errorChecksum) &&
    generic.reconciliationEvidenceChecksum === action.errorChecksum &&
    generic.resultChecksum === action.errorChecksum
  const observation =
    observationEvidence !== undefined &&
    nonEmpty(generic.errorChecksum) &&
    generic.reconciliationEvidenceChecksum === observationEvidence &&
    (expectedState === "intervention_required"
      ? generic.resultChecksum === undefined
      : generic.resultChecksum === observationEvidence)
  const exactEvidenceShape = directReceipt || classifiedDirectReceipt || crashAbsence || observation
  if (
    !exactKeys(generic, selectedKeys(plan, expectedState, generic)) ||
    !emptyCounters(generic.costCounters) ||
    !emptyCounters(generic.progressCounters) ||
    !positiveInteger(generic.fencingToken) ||
    generic.lastAttemptId !== action.lastAttemptId ||
    generic.startedAttempts !== action.attempts ||
    generic.state !== expectedState ||
    !exactAuthorization(generic, plan, expectedAuthorizationChecksum) ||
    !exactEvidenceShape
  ) {
    intervention(`Selected saga action ${plan.stepId} contradicts its generic operation step.`)
  }
}

function retryableDecision(
  action: SagaActionRecord,
  generic: OperationStepRecord,
  plan: OperationStepPlan,
  evidence: SagaTerminalModelEvidence,
): SagaTerminalModelBranchDecision | undefined {
  const expectedAuthorizationChecksum = evidence.irreversibleAuthorizationChecksums[plan.stepId]
  if (generic.state === "succeeded") {
    const classifiedKeys = [
      "costCounters",
      "fencingToken",
      "lastAttemptId",
      "progressCounters",
      ...(plan.checkpoint === "irreversible" ? ["authorizationChecksum"] : []),
      ...(generic.errorChecksum === undefined ? [] : ["errorChecksum"]),
      "reconciliationEvidenceChecksum",
      "resultChecksum",
      "startedAttempts",
      "state",
    ]
    if (
      !exactKeys(generic, classifiedKeys) ||
      !emptyCounters(generic.costCounters) ||
      !emptyCounters(generic.progressCounters) ||
      !positiveInteger(generic.fencingToken) ||
      generic.lastAttemptId !== action.lastAttemptId ||
      generic.startedAttempts !== action.attempts ||
      !exactAuthorization(generic, plan, expectedAuthorizationChecksum) ||
      !nonEmpty(generic.reconciliationEvidenceChecksum) ||
      generic.resultChecksum !== generic.reconciliationEvidenceChecksum ||
      (action.observationEvidenceChecksum !== undefined &&
        (!nonEmpty(generic.errorChecksum) ||
          generic.reconciliationEvidenceChecksum !== action.observationEvidenceChecksum)) ||
      (action.observationEvidenceChecksum === undefined &&
        generic.reconciliationEvidenceChecksum !== (generic.errorChecksum ?? action.errorChecksum))
    ) {
      intervention(
        `Classified retryable saga action ${plan.stepId} contradicts its generic operation step.`,
      )
    }
    return undefined
  }
  const keys = [
    "costCounters",
    "fencingToken",
    "lastAttemptId",
    "progressCounters",
    ...(plan.checkpoint === "irreversible" ? ["authorizationChecksum"] : []),
    ...(generic.errorChecksum === undefined ? [] : ["errorChecksum"]),
    ...(generic.reconciliationEvidenceChecksum === undefined
      ? []
      : ["reconciliationEvidenceChecksum"]),
    "startedAttempts",
    "state",
  ]
  const evidenceChecksum = generic.reconciliationEvidenceChecksum ?? generic.errorChecksum
  const directFailure =
    action.observationEvidenceChecksum === undefined &&
    generic.errorChecksum !== undefined &&
    generic.reconciliationEvidenceChecksum === undefined
  const crashAbsence =
    action.observationEvidenceChecksum === undefined &&
    generic.errorChecksum === undefined &&
    generic.reconciliationEvidenceChecksum === action.errorChecksum
  const observation =
    action.observationEvidenceChecksum !== undefined &&
    nonEmpty(generic.errorChecksum) &&
    generic.reconciliationEvidenceChecksum === action.observationEvidenceChecksum
  if (
    !exactKeys(generic, keys) ||
    !emptyCounters(generic.costCounters) ||
    !emptyCounters(generic.progressCounters) ||
    !positiveInteger(generic.fencingToken) ||
    generic.lastAttemptId !== action.lastAttemptId ||
    generic.startedAttempts !== action.attempts ||
    generic.state !== "retryable_failed" ||
    !exactAuthorization(generic, plan, expectedAuthorizationChecksum) ||
    !nonEmpty(evidenceChecksum) ||
    (!directFailure && !crashAbsence && !observation)
  ) {
    intervention(`Retryable saga action ${plan.stepId} contradicts its generic operation step.`)
  }
  return Object.freeze({
    attemptId: action.lastAttemptId as string,
    evidenceChecksum,
    evidenceKind: directFailure ? "direct_receipt" : crashAbsence ? "crash_absence" : "observation",
    kind: "terminal_not_applied",
    sagaChecksum: evidence.sagaChecksum,
    stateVersion: evidence.stateVersion,
    stepId: plan.stepId,
  })
}

function classifyUnchosen(
  record: OperationStepRecord,
  stepId: string,
  evidence: SagaTerminalModelEvidence,
  decisions: SagaTerminalModelBranchDecision[],
): void {
  if (pristinePending(record)) {
    decisions.push(
      Object.freeze({
        kind: "not_required",
        sagaChecksum: evidence.sagaChecksum,
        stateVersion: evidence.stateVersion,
        stepId,
      }),
    )
    return
  }
  if (!exactNotRequired(record, evidence.sagaChecksum)) {
    intervention(`Unchosen saga branch ${stepId} is not pristine or exactly classified.`)
  }
}

function classifyAction(
  action: SagaActionRecord,
  generic: OperationStepRecord,
  plan: OperationStepPlan,
  phase: SagaActionPhase,
  evidence: SagaTerminalModelEvidence,
  decisions: SagaTerminalModelBranchDecision[],
): void {
  if (action.state === "pending") {
    classifyUnchosen(generic, plan.stepId, evidence, decisions)
    return
  }
  if (action.state === "retryable_failed" && phase === "forward") {
    const decision = retryableDecision(action, generic, plan, evidence)
    if (decision !== undefined) decisions.push(decision)
    return
  }
  exactSelectedAction(
    action,
    generic,
    plan,
    phase,
    evidence.irreversibleAuthorizationChecksums[plan.stepId],
  )
}

function snapshotTerminalModelInputs(
  operation: OperationRecord,
  saga: SagaRecord,
  inputEvidence: SagaTerminalModelEvidence,
): {
  readonly evidence: SagaTerminalModelEvidence
  readonly operation: OperationRecord
  readonly saga: SagaRecord
} {
  try {
    if (!exactKeys(operation, OPERATION_KEYS) || !exactKeys(saga, SAGA_KEYS)) throw new Error()
    const plan = operation.plan
    const sourceOperationSteps = operation.steps
    const deadlineAtMs = saga.deadlineAtMs
    const descriptor = saga.descriptor
    const idempotencyKey = saga.idempotencyKey
    const inputChecksum = saga.inputChecksum
    const sagaId = saga.sagaId
    const stateVersion = saga.stateVersion
    const status = saga.status
    const sourceSagaSteps = saga.steps
    const terminationCause = saga.terminationCause
    const terminationRequestedAtMs = saga.terminationRequestedAtMs
    const operationSteps = structuredClone(sourceOperationSteps)
    const sagaSteps = structuredClone(sourceSagaSteps)
    const evidence = structuredClone(inputEvidence)
    return Object.freeze({
      evidence: Object.freeze(evidence),
      operation: Object.freeze({ plan, steps: Object.freeze(operationSteps) }),
      saga: Object.freeze({
        deadlineAtMs,
        descriptor,
        idempotencyKey,
        inputChecksum,
        sagaId,
        stateVersion,
        status,
        steps: Object.freeze(sagaSteps),
        terminationCause,
        terminationRequestedAtMs,
      }),
    })
  } catch {
    intervention("Saga terminal model inputs could not be captured safely.")
  }
}

function evaluateTerminalSagaBranches(
  operation: OperationRecord,
  saga: SagaRecord,
  inputEvidence: SagaTerminalModelEvidence,
): readonly SagaTerminalModelBranchDecision[] {
  canonicalPlan(operation, saga)
  const expectedSettlement = mapSagaSettlementOutcome(saga)
  const evidence = validateTerminalEvidence(inputEvidence, saga)
  if (
    !exactAtomicTerminal(
      operation.steps[SAGA_INIT_OPERATION_STEP_ID] as OperationStepRecord,
      "succeeded",
    )
  ) {
    intervention("Saga initialization is not exactly complete in the generic operation.")
  }

  const decisions: SagaTerminalModelBranchDecision[] = []
  const explicitTermination =
    saga.terminationCause === "cancellation" || saga.terminationCause === "timeout"
  const termination = operation.steps[SAGA_TERMINATION_OPERATION_STEP_ID] as OperationStepRecord
  if (explicitTermination) {
    if (!exactAtomicTerminal(termination, "succeeded")) {
      intervention("Explicit saga termination is not exactly complete in the generic operation.")
    }
  } else {
    classifyUnchosen(termination, SAGA_TERMINATION_OPERATION_STEP_ID, evidence, decisions)
  }

  const planById = new Map(operation.plan.steps.map((step) => [step.stepId, step] as const))
  for (const descriptorStep of saga.descriptor.steps) {
    const sagaStep = saga.steps[descriptorStep.stepId] as SagaRecord["steps"][string]
    for (const phase of ["forward", "compensation"] as const) {
      if (phase === "compensation" && descriptorStep.irreversible) continue
      const stepId = sagaActionOperationStepId(descriptorStep.stepId, phase)
      classifyAction(
        sagaStep[phase],
        operation.steps[stepId] as OperationStepRecord,
        planById.get(stepId) as OperationStepPlan,
        phase,
        evidence,
        decisions,
      )
    }
  }

  const settlement = operation.steps[SAGA_SETTLE_OPERATION_STEP_ID] as OperationStepRecord
  if (pristinePending(settlement)) return Object.freeze(decisions)
  if (
    !exactAtomicTerminal(settlement, expectedSettlement, evidence.sagaChecksum) ||
    decisions.length !== 0
  ) {
    intervention("Saga settlement is terminal before every branch is exactly classified.")
  }
  return Object.freeze(decisions)
}

/**
 * Models deterministic branch decisions for canonical terminal saga/operation projections.
 * Caller-supplied evidence exercises the proof contract but cannot grant settlement authority;
 * production persistence must supply an internal capability minted by complete-history verification.
 */
export function modelTerminalSagaBranches(
  operation: OperationRecord,
  saga: SagaRecord,
  inputEvidence: SagaTerminalModelEvidence,
): readonly SagaTerminalModelBranchDecision[] {
  const snapshot = snapshotTerminalModelInputs(operation, saga, inputEvidence)
  return evaluateTerminalSagaBranches(snapshot.operation, snapshot.saga, snapshot.evidence)
}
