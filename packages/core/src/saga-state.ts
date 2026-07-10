import { NozzleError } from "./errors.js"
import type { DigestFunction } from "./operation.js"
import {
  assertTrustedSagaDescriptor,
  loadSagaDescriptor,
  type SagaActionReference,
  type SagaDescriptor,
} from "./saga.js"

export const SAGA_ACTION_STATES = [
  "pending",
  "running",
  "retryable_failed",
  "unknown",
  "succeeded",
  "failed",
  "intervention_required",
  "not_required",
] as const

export type SagaActionState = (typeof SAGA_ACTION_STATES)[number]
export type SagaActionPhase = "compensation" | "forward"
export type SagaTerminationCause = "cancellation" | "failure" | "timeout"
export type SagaStatus =
  | "cancelled"
  | "failed"
  | "intervention_required"
  | "planned"
  | "running"
  | "succeeded"
  | "timed_out"
  | "compensating"
export type SagaCommitment = "complete" | "confirmed_partial" | "none" | "possible"

export interface SagaActionRecord {
  readonly activeAttemptId?: string
  readonly attempts: number
  readonly errorChecksum?: string
  readonly idempotencyKey: string
  readonly lastAttemptId?: string
  readonly nextAttemptAtMs: number
  readonly observationEvidenceChecksum?: string
  readonly resultChecksum?: string
  readonly state: SagaActionState
}

export interface SagaStepRecord {
  readonly compensation: SagaActionRecord
  readonly forward: SagaActionRecord
  readonly inputChecksum: string
}

export interface SagaRecord {
  readonly deadlineAtMs: number
  readonly descriptor: SagaDescriptor
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly sagaId: string
  readonly stateVersion: number
  readonly status: SagaStatus
  readonly steps: Readonly<Record<string, SagaStepRecord>>
  readonly terminationCause: SagaTerminationCause | null
  readonly terminationRequestedAtMs: number | null
}

export type SagaCommand =
  | {
      readonly action: SagaActionReference
      readonly attemptNumber: number
      readonly idempotencyKey: string
      readonly kind: "execute"
      readonly phase: SagaActionPhase
      readonly stepId: string
      readonly timeoutMs: number
    }
  | {
      readonly action: SagaActionReference
      readonly kind: "observe"
      readonly phase: SagaActionPhase
      readonly stepId: string
      readonly timeoutMs: number
    }
  | { readonly kind: "request_termination"; readonly cause: "timeout" }
  | { readonly kind: "terminal"; readonly status: SagaStatus }
  | {
      readonly kind: "wait"
      readonly reason: "attempt_in_progress" | "retry_backoff"
      readonly untilMs?: number
    }

export type SagaBeginDecision =
  | { readonly disposition: "execute"; readonly saga: SagaRecord }
  | { readonly disposition: "in_progress"; readonly saga: SagaRecord }
  | { readonly disposition: "observe"; readonly saga: SagaRecord }
  | {
      readonly disposition: "replay_failure"
      readonly errorChecksum: string
      readonly saga: SagaRecord
    }
  | {
      readonly disposition: "replay_success"
      readonly resultChecksum: string
      readonly saga: SagaRecord
    }

export type SagaActionFailureOutcome =
  | "definitely_not_applied_retryable"
  | "definitely_not_applied_terminal"
  | "unknown"
export type SagaObservationOutcome = "applied" | "indeterminate" | "not_applied"

type SagaChanges = {
  steps?: Readonly<Record<string, SagaStepRecord>>
  terminationCause?: SagaTerminationCause | null
  terminationRequestedAtMs?: number | null
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function nonEmpty(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function serverTime(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    configuration("Saga server time must be a non-negative safe integer.")
  }
}

function phase(value: unknown): asserts value is SagaActionPhase {
  if (value !== "forward" && value !== "compensation") {
    configuration("Saga action phase is unsupported.")
  }
}

function frozenAction(input: SagaActionRecord): SagaActionRecord {
  return Object.freeze({ ...input })
}

function actionIdempotencyKey(sagaId: string, stepId: string, phase: SagaActionPhase): string {
  return `saga:${sagaId.length}:${sagaId}:${stepId.length}:${stepId}:${phase}`
}

function initialAction(idempotencyKey: string, state: SagaActionState): SagaActionRecord {
  return frozenAction({ attempts: 0, idempotencyKey, nextAttemptAtMs: 0, state })
}

function stepWithAction(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  action: SagaActionRecord,
): Readonly<Record<string, SagaStepRecord>> {
  const current = record.steps[stepId] as SagaStepRecord
  return Object.freeze({
    ...record.steps,
    [stepId]: Object.freeze({ ...current, [phase]: action }),
  })
}

function terminalStatus(cause: SagaTerminationCause): SagaStatus {
  if (cause === "cancellation") return "cancelled"
  if (cause === "timeout") return "timed_out"
  return "failed"
}

function settledStatus(record: SagaRecord): SagaStatus {
  const stepRecords = record.descriptor.steps.map(
    (step) => record.steps[step.stepId] as SagaStepRecord,
  )
  if (
    stepRecords.some(
      (step) =>
        step.forward.state === "intervention_required" ||
        step.compensation.state === "intervention_required" ||
        step.compensation.state === "failed",
    )
  ) {
    return "intervention_required"
  }
  if (record.terminationCause === null) {
    if (stepRecords.every((step) => step.forward.state === "succeeded")) return "succeeded"
    return "running"
  }
  for (const step of record.descriptor.steps) {
    const current = record.steps[step.stepId] as SagaStepRecord
    if (current.forward.state === "running" || current.forward.state === "unknown") {
      return "compensating"
    }
    if (current.forward.state === "succeeded") {
      if (step.irreversible) return "intervention_required"
      if (current.compensation.state !== "succeeded") return "compensating"
    }
  }
  return terminalStatus(record.terminationCause)
}

function withRecord(record: SagaRecord, changes: SagaChanges): SagaRecord {
  const candidate: SagaRecord = Object.freeze({
    ...record,
    ...changes,
    stateVersion: record.stateVersion + 1,
    status: record.status,
  })
  return Object.freeze({ ...candidate, status: settledStatus(candidate) })
}

function lastAttemptId(action: SagaActionRecord): string {
  if (action.lastAttemptId === undefined) {
    return intervention("Saga action attempt identity is missing.")
  }
  return action.lastAttemptId
}

function actionErrorChecksum(action: SagaActionRecord): string {
  if (action.errorChecksum === undefined) {
    return intervention("Unknown saga action error evidence is missing.")
  }
  return action.errorChecksum
}

export function createSagaRecord(input: {
  readonly deadlineAtMs: number
  readonly descriptor: SagaDescriptor
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly sagaId: string
  readonly serverTimeMs: number
  readonly stepInputChecksums: Readonly<Record<string, string>>
}): SagaRecord {
  assertTrustedSagaDescriptor(input.descriptor)
  nonEmpty(input.sagaId, "Saga ID")
  nonEmpty(input.idempotencyKey, "Saga idempotency key")
  nonEmpty(input.inputChecksum, "Saga input checksum")
  serverTime(input.serverTimeMs)
  serverTime(input.deadlineAtMs)
  if (input.deadlineAtMs <= input.serverTimeMs)
    configuration("Saga deadline must be in the future.")
  if (
    typeof input.stepInputChecksums !== "object" ||
    input.stepInputChecksums === null ||
    Array.isArray(input.stepInputChecksums)
  ) {
    configuration("Saga step input checksums are malformed.")
  }
  const expected = input.descriptor.steps.map((step) => step.stepId)
  const actual = Object.keys(input.stepInputChecksums).sort()
  const canonical = [...expected].sort()
  if (
    actual.length !== canonical.length ||
    actual.some((stepId, index) => stepId !== canonical[index])
  ) {
    configuration("Saga step input checksums do not match the descriptor.")
  }
  const steps: Record<string, SagaStepRecord> = {}
  for (const step of input.descriptor.steps) {
    const inputChecksum = input.stepInputChecksums[step.stepId]
    nonEmpty(inputChecksum, "Saga step input checksum")
    steps[step.stepId] = Object.freeze({
      compensation: initialAction(
        actionIdempotencyKey(input.sagaId, step.stepId, "compensation"),
        step.irreversible ? "not_required" : "pending",
      ),
      forward: initialAction(actionIdempotencyKey(input.sagaId, step.stepId, "forward"), "pending"),
      inputChecksum,
    })
  }
  return Object.freeze({
    deadlineAtMs: input.deadlineAtMs,
    descriptor: input.descriptor,
    idempotencyKey: input.idempotencyKey,
    inputChecksum: input.inputChecksum,
    sagaId: input.sagaId,
    stateVersion: 0,
    status: "planned",
    steps: Object.freeze(steps),
    terminationCause: null,
    terminationRequestedAtMs: null,
  })
}

export function sagaCommitment(record: SagaRecord): SagaCommitment {
  if (record.status === "succeeded") return "complete"
  const stepRecords = record.descriptor.steps.map(
    (step) => record.steps[step.stepId] as SagaStepRecord,
  )
  if (
    stepRecords.some(
      (step) => step.forward.state === "unknown" || step.compensation.state === "unknown",
    )
  ) {
    return "possible"
  }
  if (
    stepRecords.some(
      (step) => step.forward.state === "running" || step.compensation.state === "running",
    )
  ) {
    return "possible"
  }
  if (
    record.descriptor.steps.some((step) => {
      const current = record.steps[step.stepId] as SagaStepRecord
      return (
        current.forward.state === "succeeded" &&
        (step.irreversible || current.compensation.state !== "succeeded")
      )
    })
  ) {
    return "confirmed_partial"
  }
  return "none"
}

function stepAction(record: SagaRecord, stepId: string, phase: SagaActionPhase): SagaActionRecord {
  const step = record.steps[stepId]
  if (step === undefined) return configuration("Saga step does not exist.")
  return step[phase]
}

function descriptorStep(record: SagaRecord, stepId: string) {
  const step = record.descriptor.steps.find((candidate) => candidate.stepId === stepId)
  if (step === undefined) return configuration("Saga descriptor step does not exist.")
  return step
}

function executeCommand(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): Extract<SagaCommand, { kind: "execute" }> {
  const step = descriptorStep(record, stepId)
  const action = stepAction(record, stepId, phase)
  const reference = (
    phase === "forward" ? step.forwardAction : step.compensationAction
  ) as SagaActionReference
  return Object.freeze({
    action: reference,
    attemptNumber: action.attempts + 1,
    idempotencyKey: action.idempotencyKey,
    kind: "execute",
    phase,
    stepId,
    timeoutMs: step.timeoutMs,
  })
}

function observeCommand(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): Extract<SagaCommand, { kind: "observe" }> {
  const step = descriptorStep(record, stepId)
  const action = (
    phase === "forward" ? step.forwardObservation : step.compensationObservation
  ) as SagaActionReference
  return Object.freeze({ action, kind: "observe", phase, stepId, timeoutMs: step.timeoutMs })
}

function commandForAction(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  nowMs: number,
): SagaCommand {
  const action = stepAction(record, stepId, phase)
  if (action.state === "running") {
    return Object.freeze({ kind: "wait", reason: "attempt_in_progress" })
  }
  if (action.state === "unknown") return observeCommand(record, stepId, phase)
  if (action.state === "pending" || action.state === "retryable_failed") {
    return action.nextAttemptAtMs > nowMs
      ? Object.freeze({ kind: "wait", reason: "retry_backoff", untilMs: action.nextAttemptAtMs })
      : executeCommand(record, stepId, phase)
  }
  return configuration("Saga action is not executable.")
}

export function nextSagaCommand(record: SagaRecord, nowMs: number): SagaCommand {
  serverTime(nowMs)
  if (
    ["cancelled", "failed", "intervention_required", "succeeded", "timed_out"].includes(
      record.status,
    )
  ) {
    return Object.freeze({ kind: "terminal", status: record.status })
  }
  if (record.terminationCause === null && nowMs >= record.deadlineAtMs) {
    return Object.freeze({ cause: "timeout", kind: "request_termination" })
  }
  if (record.terminationCause !== null) {
    for (const step of record.descriptor.steps) {
      const forward = record.steps[step.stepId]?.forward as SagaActionRecord
      if (forward.state === "running" || forward.state === "unknown") {
        return commandForAction(record, step.stepId, "forward", nowMs)
      }
    }
    for (const step of [...record.descriptor.steps].reverse()) {
      const current = record.steps[step.stepId] as SagaStepRecord
      if (
        current.forward.state === "succeeded" &&
        !step.irreversible &&
        current.compensation.state !== "succeeded"
      ) {
        return commandForAction(record, step.stepId, "compensation", nowMs)
      }
    }
    return Object.freeze({ kind: "terminal", status: settledStatus(record) })
  }
  for (const step of record.descriptor.steps) {
    const forward = record.steps[step.stepId]?.forward as SagaActionRecord
    if (forward.state !== "succeeded")
      return commandForAction(record, step.stepId, "forward", nowMs)
  }
  return Object.freeze({ kind: "terminal", status: settledStatus(record) })
}

export function beginSagaAction(
  record: SagaRecord,
  input: {
    readonly attemptId: string
    readonly idempotencyKey: string
    readonly phase: SagaActionPhase
    readonly serverTimeMs: number
    readonly stepId: string
  },
): SagaBeginDecision {
  nonEmpty(input.attemptId, "Saga attempt ID")
  nonEmpty(input.idempotencyKey, "Saga action idempotency key")
  phase(input.phase)
  serverTime(input.serverTimeMs)
  const current = stepAction(record, input.stepId, input.phase)
  if (input.idempotencyKey !== current.idempotencyKey)
    resume("Saga action idempotency key changed.")
  if (current.lastAttemptId === input.attemptId) {
    if (current.state === "running")
      return Object.freeze({ disposition: "in_progress", saga: record })
    if (current.state === "unknown") return Object.freeze({ disposition: "observe", saga: record })
    if (current.state === "succeeded" && current.resultChecksum !== undefined) {
      return Object.freeze({
        disposition: "replay_success",
        resultChecksum: current.resultChecksum,
        saga: record,
      })
    }
    if (
      (current.state === "retryable_failed" ||
        current.state === "failed" ||
        current.state === "intervention_required") &&
      current.errorChecksum !== undefined
    ) {
      return Object.freeze({
        disposition: "replay_failure",
        errorChecksum: current.errorChecksum,
        saga: record,
      })
    }
    return intervention("Saga duplicate attempt state is contradictory.")
  }
  const command = nextSagaCommand(record, input.serverTimeMs)
  if (
    command.kind !== "execute" ||
    command.stepId !== input.stepId ||
    command.phase !== input.phase
  ) {
    return resume("Saga action is not the next eligible serial action.")
  }
  const next = frozenAction({
    attempts: current.attempts + 1,
    idempotencyKey: current.idempotencyKey,
    activeAttemptId: input.attemptId,
    lastAttemptId: input.attemptId,
    nextAttemptAtMs: 0,
    state: "running",
  })
  const saga = withRecord(record, {
    steps: stepWithAction(record, input.stepId, input.phase, next),
  })
  return Object.freeze({ disposition: "execute", saga })
}

function backoffMs(record: SagaRecord, stepId: string, attempts: number): number {
  const step = descriptorStep(record, stepId)
  let delay = step.baseRetryDelayMs
  for (let attempt = 1; attempt < attempts && delay < step.maxRetryDelayMs; attempt += 1) {
    delay = Math.min(step.maxRetryDelayMs, delay * 2)
  }
  return delay
}

function failedAction(
  record: SagaRecord,
  stepId: string,
  current: SagaActionRecord,
  errorChecksum: string,
  serverTimeMs: number,
  retryable: boolean,
): { readonly action: SagaActionRecord; readonly terminal: boolean } {
  const step = descriptorStep(record, stepId)
  const terminal = !retryable || current.attempts >= step.maxAttempts
  let nextAttemptAtMs = 0
  if (!terminal) {
    nextAttemptAtMs = serverTimeMs + backoffMs(record, stepId, current.attempts)
    if (!Number.isSafeInteger(nextAttemptAtMs)) {
      configuration("Saga retry time exceeds the safe integer range.")
    }
  }
  return Object.freeze({
    action: frozenAction({
      attempts: current.attempts,
      errorChecksum,
      idempotencyKey: current.idempotencyKey,
      lastAttemptId: lastAttemptId(current),
      nextAttemptAtMs,
      state: terminal ? "failed" : "retryable_failed",
    }),
    terminal,
  })
}

function runningAction(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  attemptId: string,
): SagaActionRecord {
  const current = stepAction(record, stepId, phase)
  if (current.state !== "running" || current.activeAttemptId !== attemptId) {
    return resume("Saga action outcome does not match the active attempt.")
  }
  return current
}

export function recordSagaActionSuccess(
  record: SagaRecord,
  input: {
    readonly attemptId: string
    readonly phase: SagaActionPhase
    readonly resultChecksum: string
    readonly serverTimeMs: number
    readonly stepId: string
  },
): SagaRecord {
  nonEmpty(input.resultChecksum, "Saga action result checksum")
  phase(input.phase)
  serverTime(input.serverTimeMs)
  const current = runningAction(record, input.stepId, input.phase, input.attemptId)
  const next = frozenAction({
    attempts: current.attempts,
    idempotencyKey: current.idempotencyKey,
    lastAttemptId: lastAttemptId(current),
    nextAttemptAtMs: 0,
    resultChecksum: input.resultChecksum,
    state: "succeeded",
  })
  return withRecord(record, { steps: stepWithAction(record, input.stepId, input.phase, next) })
}

export function recordSagaActionFailure(
  record: SagaRecord,
  input: {
    readonly attemptId: string
    readonly errorChecksum: string
    readonly outcome: SagaActionFailureOutcome
    readonly phase: SagaActionPhase
    readonly serverTimeMs: number
    readonly stepId: string
  },
): SagaRecord {
  nonEmpty(input.errorChecksum, "Saga action error checksum")
  phase(input.phase)
  serverTime(input.serverTimeMs)
  if (
    input.outcome !== "definitely_not_applied_retryable" &&
    input.outcome !== "definitely_not_applied_terminal" &&
    input.outcome !== "unknown"
  ) {
    configuration("Saga action failure outcome is unsupported.")
  }
  const current = runningAction(record, input.stepId, input.phase, input.attemptId)
  let next: SagaActionRecord
  let terminal = false
  if (input.outcome === "unknown") {
    next = frozenAction({
      attempts: current.attempts,
      errorChecksum: input.errorChecksum,
      idempotencyKey: current.idempotencyKey,
      lastAttemptId: lastAttemptId(current),
      nextAttemptAtMs: 0,
      state: "unknown",
    })
  } else {
    const failure = failedAction(
      record,
      input.stepId,
      current,
      input.errorChecksum,
      input.serverTimeMs,
      input.outcome === "definitely_not_applied_retryable",
    )
    next = failure.action
    terminal = failure.terminal
  }
  const changes: SagaChanges = {
    steps: stepWithAction(record, input.stepId, input.phase, next),
  }
  if (terminal && input.phase === "forward" && record.terminationCause === null) {
    changes.terminationCause = "failure"
    changes.terminationRequestedAtMs = input.serverTimeMs
  }
  if (terminal && input.phase === "compensation") {
    next = frozenAction({ ...next, state: "intervention_required" })
    changes.steps = stepWithAction(record, input.stepId, input.phase, next)
  }
  return withRecord(record, changes)
}

export function recordSagaObservation(
  record: SagaRecord,
  input: {
    readonly evidenceChecksum: string
    readonly outcome: SagaObservationOutcome
    readonly phase: SagaActionPhase
    readonly resultChecksum?: string
    readonly serverTimeMs: number
    readonly stepId: string
  },
): SagaRecord {
  nonEmpty(input.evidenceChecksum, "Saga observation evidence checksum")
  phase(input.phase)
  serverTime(input.serverTimeMs)
  if (
    input.outcome !== "applied" &&
    input.outcome !== "indeterminate" &&
    input.outcome !== "not_applied"
  ) {
    configuration("Saga observation outcome is unsupported.")
  }
  const current = stepAction(record, input.stepId, input.phase)
  if (current.state !== "unknown") return resume("Saga observation requires an unknown action.")
  if (input.outcome === "applied") {
    nonEmpty(input.resultChecksum, "Saga observed result checksum")
    const next = frozenAction({
      attempts: current.attempts,
      idempotencyKey: current.idempotencyKey,
      lastAttemptId: lastAttemptId(current),
      nextAttemptAtMs: 0,
      observationEvidenceChecksum: input.evidenceChecksum,
      resultChecksum: input.resultChecksum,
      state: "succeeded",
    })
    return withRecord(record, { steps: stepWithAction(record, input.stepId, input.phase, next) })
  }
  if (input.outcome === "indeterminate") {
    const next = frozenAction({
      attempts: current.attempts,
      errorChecksum: actionErrorChecksum(current),
      idempotencyKey: current.idempotencyKey,
      lastAttemptId: lastAttemptId(current),
      nextAttemptAtMs: 0,
      observationEvidenceChecksum: input.evidenceChecksum,
      state: "intervention_required",
    })
    return withRecord(record, { steps: stepWithAction(record, input.stepId, input.phase, next) })
  }
  const failure = failedAction(
    record,
    input.stepId,
    current,
    actionErrorChecksum(current),
    input.serverTimeMs,
    true,
  )
  let next = frozenAction({
    ...failure.action,
    observationEvidenceChecksum: input.evidenceChecksum,
  })
  const changes: SagaChanges = {}
  if (failure.terminal) {
    if (input.phase === "compensation") {
      next = frozenAction({ ...next, state: "intervention_required" })
    } else if (record.terminationCause === null) {
      changes.terminationCause = "failure"
      changes.terminationRequestedAtMs = input.serverTimeMs
    }
  }
  changes.steps = stepWithAction(record, input.stepId, input.phase, next)
  return withRecord(record, changes)
}

export function requestSagaTermination(
  record: SagaRecord,
  input: {
    readonly cause: "cancellation" | "timeout"
    readonly serverTimeMs: number
  },
): SagaRecord {
  serverTime(input.serverTimeMs)
  if (input.cause !== "cancellation" && input.cause !== "timeout") {
    configuration("Saga termination request cause is unsupported.")
  }
  if (
    ["cancelled", "failed", "intervention_required", "succeeded", "timed_out"].includes(
      record.status,
    )
  ) {
    return record
  }
  if (record.terminationCause !== null) return record
  return withRecord(record, {
    terminationCause: input.cause,
    terminationRequestedAtMs: input.serverTimeMs,
  })
}

export function markRunningSagaActionUnknown(
  record: SagaRecord,
  input: {
    readonly attemptId: string
    readonly errorChecksum: string
    readonly phase: SagaActionPhase
    readonly stepId: string
  },
): SagaRecord {
  nonEmpty(input.errorChecksum, "Saga recovery error checksum")
  phase(input.phase)
  const current = runningAction(record, input.stepId, input.phase, input.attemptId)
  const next = frozenAction({
    attempts: current.attempts,
    errorChecksum: input.errorChecksum,
    idempotencyKey: current.idempotencyKey,
    lastAttemptId: lastAttemptId(current),
    nextAttemptAtMs: 0,
    state: "unknown",
  })
  return withRecord(record, { steps: stepWithAction(record, input.stepId, input.phase, next) })
}

export function markSagaActionNotDispatched(
  record: SagaRecord,
  input: {
    readonly attemptId: string
    readonly errorChecksum: string
    readonly phase: SagaActionPhase
    readonly serverTimeMs: number
    readonly stepId: string
  },
): SagaRecord {
  nonEmpty(input.errorChecksum, "Saga recovery error checksum")
  phase(input.phase)
  serverTime(input.serverTimeMs)
  const current = runningAction(record, input.stepId, input.phase, input.attemptId)
  const failure = failedAction(
    record,
    input.stepId,
    current,
    input.errorChecksum,
    input.serverTimeMs,
    true,
  )
  let next = failure.action
  const changes: SagaChanges = {}
  if (failure.terminal && input.phase === "forward" && record.terminationCause === null) {
    changes.terminationCause = "failure"
    changes.terminationRequestedAtMs = input.serverTimeMs
  }
  if (failure.terminal && input.phase === "compensation") {
    next = frozenAction({ ...next, state: "intervention_required" })
  }
  changes.steps = stepWithAction(record, input.stepId, input.phase, next)
  return withRecord(record, changes)
}

const ACTION_RECORD_KEYS = new Set([
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

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function persisted(condition: unknown, message: string): asserts condition {
  if (!condition) intervention(message)
}

function persistedAction(
  value: unknown,
  expectedIdempotencyKey: string,
  allowNotRequired: boolean,
): SagaActionRecord {
  persisted(plainRecord(value), "Persisted saga action record is malformed.")
  persisted(
    Object.keys(value).every((key) => ACTION_RECORD_KEYS.has(key)),
    "Persisted saga action record contains unknown fields.",
  )
  persisted(
    typeof value.attempts === "number" &&
      Number.isSafeInteger(value.attempts) &&
      value.attempts >= 0,
    "Persisted saga action attempt count is malformed.",
  )
  persisted(
    typeof value.nextAttemptAtMs === "number" &&
      Number.isSafeInteger(value.nextAttemptAtMs) &&
      value.nextAttemptAtMs >= 0,
    "Persisted saga retry time is malformed.",
  )
  persisted(
    typeof value.idempotencyKey === "string" && value.idempotencyKey === expectedIdempotencyKey,
    "Persisted saga action idempotency key is contradictory.",
  )
  persisted(
    typeof value.state === "string" &&
      (SAGA_ACTION_STATES as readonly string[]).includes(value.state),
    "Persisted saga action state is unsupported.",
  )
  const state = value.state as SagaActionState
  persisted(
    allowNotRequired || state !== "not_required",
    "Persisted forward action cannot be not-required.",
  )
  const optionalStrings = [
    "activeAttemptId",
    "errorChecksum",
    "lastAttemptId",
    "observationEvidenceChecksum",
    "resultChecksum",
  ] as const
  for (const key of optionalStrings) {
    persisted(
      value[key] === undefined || (typeof value[key] === "string" && value[key].trim().length > 0),
      `Persisted saga action ${key} is malformed.`,
    )
  }
  const noAttempt = state === "pending" || state === "not_required"
  persisted(
    noAttempt ? value.attempts === 0 : (value.attempts as number) >= 1,
    "Persisted saga action state contradicts its attempt count.",
  )
  persisted(
    (state === "running") === (value.activeAttemptId !== undefined),
    "Persisted saga active attempt contradicts its state.",
  )
  persisted(
    noAttempt ? value.lastAttemptId === undefined : value.lastAttemptId !== undefined,
    "Persisted saga last attempt contradicts its state.",
  )
  persisted(
    (state === "succeeded") === (value.resultChecksum !== undefined),
    "Persisted saga result contradicts its state.",
  )
  const errorState =
    state === "retryable_failed" ||
    state === "unknown" ||
    state === "failed" ||
    state === "intervention_required"
  persisted(
    errorState === (value.errorChecksum !== undefined),
    "Persisted saga error evidence contradicts its state.",
  )
  persisted(
    state === "retryable_failed"
      ? (value.nextAttemptAtMs as number) > 0
      : value.nextAttemptAtMs === 0,
    "Persisted saga retry time contradicts its state.",
  )
  persisted(
    value.observationEvidenceChecksum === undefined ||
      state === "succeeded" ||
      state === "retryable_failed" ||
      state === "failed" ||
      state === "intervention_required",
    "Persisted saga observation evidence contradicts its state.",
  )
  return frozenAction({
    ...(value.activeAttemptId === undefined
      ? {}
      : { activeAttemptId: value.activeAttemptId as string }),
    attempts: value.attempts as number,
    ...(value.errorChecksum === undefined ? {} : { errorChecksum: value.errorChecksum as string }),
    idempotencyKey: value.idempotencyKey,
    ...(value.lastAttemptId === undefined ? {} : { lastAttemptId: value.lastAttemptId as string }),
    nextAttemptAtMs: value.nextAttemptAtMs as number,
    ...(value.observationEvidenceChecksum === undefined
      ? {}
      : { observationEvidenceChecksum: value.observationEvidenceChecksum as string }),
    ...(value.resultChecksum === undefined
      ? {}
      : { resultChecksum: value.resultChecksum as string }),
    state,
  })
}

async function loadSagaRecordUnchecked(
  candidate: unknown,
  digest: DigestFunction,
): Promise<SagaRecord> {
  persisted(plainRecord(candidate), "Persisted saga record is malformed.")
  const expectedKeys = [
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
  ]
  persisted(
    Object.keys(candidate).length === expectedKeys.length &&
      expectedKeys.every((key) => Object.hasOwn(candidate, key)),
    "Persisted saga record fields are malformed.",
  )
  persisted(
    typeof candidate.sagaId === "string" && candidate.sagaId.trim().length > 0,
    "Persisted saga ID is malformed.",
  )
  persisted(
    typeof candidate.idempotencyKey === "string" && candidate.idempotencyKey.trim().length > 0,
    "Persisted saga idempotency key is malformed.",
  )
  persisted(
    typeof candidate.inputChecksum === "string" && candidate.inputChecksum.trim().length > 0,
    "Persisted saga input checksum is malformed.",
  )
  persisted(
    typeof candidate.deadlineAtMs === "number" &&
      Number.isSafeInteger(candidate.deadlineAtMs) &&
      candidate.deadlineAtMs >= 0,
    "Persisted saga deadline is malformed.",
  )
  persisted(
    typeof candidate.stateVersion === "number" &&
      Number.isSafeInteger(candidate.stateVersion) &&
      candidate.stateVersion >= 0,
    "Persisted saga state version is malformed.",
  )
  persisted(
    typeof candidate.status === "string" &&
      [
        "cancelled",
        "compensating",
        "failed",
        "intervention_required",
        "planned",
        "running",
        "succeeded",
        "timed_out",
      ].includes(candidate.status),
    "Persisted saga status is unsupported.",
  )
  const validCause =
    candidate.terminationCause === null ||
    candidate.terminationCause === "cancellation" ||
    candidate.terminationCause === "failure" ||
    candidate.terminationCause === "timeout"
  persisted(validCause, "Persisted saga termination cause is unsupported.")
  persisted(
    candidate.terminationRequestedAtMs === null ||
      (typeof candidate.terminationRequestedAtMs === "number" &&
        Number.isSafeInteger(candidate.terminationRequestedAtMs) &&
        candidate.terminationRequestedAtMs >= 0),
    "Persisted saga termination time is malformed.",
  )
  persisted(
    (candidate.terminationCause === null) === (candidate.terminationRequestedAtMs === null),
    "Persisted saga termination cause and time are contradictory.",
  )
  const descriptor = await loadSagaDescriptor(candidate.descriptor, digest)
  persisted(plainRecord(candidate.steps), "Persisted saga steps are malformed.")
  const expectedStepIds = descriptor.steps.map((step) => step.stepId).sort()
  const actualStepIds = Object.keys(candidate.steps).sort()
  persisted(
    expectedStepIds.length === actualStepIds.length &&
      expectedStepIds.every((stepId, index) => stepId === actualStepIds[index]),
    "Persisted saga step membership contradicts its descriptor.",
  )
  const steps: Record<string, SagaStepRecord> = {}
  const attemptIds = new Set<string>()
  let uncertainActions = 0
  for (const step of descriptor.steps) {
    const value = candidate.steps[step.stepId]
    persisted(
      plainRecord(value) &&
        Object.keys(value).length === 3 &&
        Object.hasOwn(value, "compensation") &&
        Object.hasOwn(value, "forward") &&
        Object.hasOwn(value, "inputChecksum"),
      "Persisted saga step record is malformed.",
    )
    persisted(
      typeof value.inputChecksum === "string" && value.inputChecksum.trim().length > 0,
      "Persisted saga step input checksum is malformed.",
    )
    const forward = persistedAction(
      value.forward,
      actionIdempotencyKey(candidate.sagaId, step.stepId, "forward"),
      false,
    )
    const compensation = persistedAction(
      value.compensation,
      actionIdempotencyKey(candidate.sagaId, step.stepId, "compensation"),
      step.irreversible,
    )
    persisted(
      forward.attempts <= step.maxAttempts && compensation.attempts <= step.maxAttempts,
      "Persisted saga action attempts exceed the sealed retry limit.",
    )
    persisted(
      (forward.state !== "retryable_failed" || forward.attempts < step.maxAttempts) &&
        (compensation.state !== "retryable_failed" || compensation.attempts < step.maxAttempts),
      "Persisted saga retryable action has exhausted its sealed retry limit.",
    )
    for (const action of [forward, compensation]) {
      persisted(
        action.activeAttemptId === undefined || action.activeAttemptId === action.lastAttemptId,
        "Persisted saga active and last attempt identities are contradictory.",
      )
      if (action.lastAttemptId !== undefined) {
        persisted(
          !attemptIds.has(action.lastAttemptId),
          "Persisted saga reuses an attempt identity across actions.",
        )
        attemptIds.add(action.lastAttemptId)
      }
    }
    persisted(
      step.irreversible
        ? compensation.state === "not_required"
        : compensation.state !== "not_required",
      "Persisted saga compensation state contradicts step reversibility.",
    )
    persisted(
      compensation.attempts === 0 || forward.state === "succeeded",
      "Persisted saga compensation started before confirmed forward application.",
    )
    if (forward.state === "running" || forward.state === "unknown") uncertainActions += 1
    if (compensation.state === "running" || compensation.state === "unknown") uncertainActions += 1
    steps[step.stepId] = Object.freeze({
      compensation,
      forward,
      inputChecksum: value.inputChecksum,
    })
  }
  persisted(uncertainActions <= 1, "Persisted saga has multiple in-flight or unknown actions.")
  let priorSucceeded = true
  for (const step of descriptor.steps) {
    const current = steps[step.stepId] as SagaStepRecord
    persisted(
      current.forward.attempts === 0 || priorSucceeded,
      "Persisted saga forward attempts violate serial descriptor order.",
    )
    if (current.forward.state !== "succeeded") priorSucceeded = false
  }
  let laterCompensationSettled = true
  for (const step of [...descriptor.steps].reverse()) {
    const current = steps[step.stepId] as SagaStepRecord
    persisted(
      current.compensation.attempts === 0 || laterCompensationSettled,
      "Persisted saga compensation attempts violate reverse descriptor order.",
    )
    if (
      current.forward.state === "succeeded" &&
      (step.irreversible || current.compensation.state !== "succeeded")
    ) {
      laterCompensationSettled = false
    }
  }
  persisted(
    candidate.terminationCause !== null ||
      descriptor.steps.every(
        (step) => (steps[step.stepId] as SagaStepRecord).compensation.attempts === 0,
      ),
    "Persisted saga compensation exists without a termination cause.",
  )
  persisted(
    candidate.terminationCause !== "failure" ||
      descriptor.steps.some(
        (step) => (steps[step.stepId] as SagaStepRecord).forward.state === "failed",
      ),
    "Persisted saga failure termination has no failed forward action.",
  )
  const record: SagaRecord = Object.freeze({
    deadlineAtMs: candidate.deadlineAtMs,
    descriptor,
    idempotencyKey: candidate.idempotencyKey,
    inputChecksum: candidate.inputChecksum,
    sagaId: candidate.sagaId,
    stateVersion: candidate.stateVersion,
    status: candidate.status as SagaStatus,
    steps: Object.freeze(steps),
    terminationCause: candidate.terminationCause as SagaTerminationCause | null,
    terminationRequestedAtMs: candidate.terminationRequestedAtMs,
  })
  const expectedStatus = record.stateVersion === 0 ? "planned" : settledStatus(record)
  persisted(
    record.status === expectedStatus,
    "Persisted saga status contradicts its action states.",
  )
  persisted(
    record.stateVersion !== 0 ||
      descriptor.steps.every((step) => {
        const current = record.steps[step.stepId] as SagaStepRecord
        return (
          current.forward.state === "pending" &&
          current.compensation.state === (step.irreversible ? "not_required" : "pending")
        )
      }),
    "Initial persisted saga contains action progress.",
  )
  return record
}

export async function loadSagaRecord(
  candidate: unknown,
  digest: DigestFunction,
): Promise<SagaRecord> {
  try {
    return await loadSagaRecordUnchecked(candidate, digest)
  } catch (error) {
    if (error instanceof NozzleError && error.code === "OperationInterventionRequiredError") {
      throw error
    }
    return intervention("Persisted saga record could not be reconstructed safely.")
  }
}
