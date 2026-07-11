import {
  type AuditEvent,
  beginOperationStep,
  type CounterDeltas,
  createOperationRecord,
  type DigestFunction,
  loadAuditEvent,
  loadOperationRecord,
  markOperationStepNotRequired,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepUnknownAfterCrash,
  NozzleError,
  type OperationPlan,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  operationStatus,
  recordAtomicStepOutcome,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
} from "@nozzle/core"
import { operationStepRecordJson, operationTransitionIdentity } from "./operation-store.js"
import {
  loadSagaHistoryAnchor,
  SAGA_HISTORY_PAGE_ROW_LIMIT,
  type SagaHistoryAnchor,
  type SagaHistoryAuditRow,
  type SagaHistoryPage,
  type SagaHistoryTransitionCursor,
  type SagaHistoryTransitionRow,
} from "./saga-history.js"

const AUDIT_TRANSITION_FOLD_DOMAIN = "nozzle.saga-history.audit-transition-fold.v1"
const EMPTY_FOLD_CHECKSUM = "0".repeat(64)
const CHECKSUM = /^[0-9a-f]{64}$/u
const UTF8_ENCODER = new TextEncoder()

const KNOWN_TRANSITION_AUDIT_EVENT_TYPES = new Set([
  "saga.action.classified",
  "saga.action.observed",
  "saga.action.recovered",
  "saga.action.started",
  "saga.initialized",
  "saga.termination.requested",
  "step.attempt.accepted",
  "step.attempt.definitely_not_applied",
  "step.attempt.permanent",
  "step.attempt.succeeded",
  "step.attempt.unknown",
  "step.crash.not_dispatched",
  "step.crash.outcome_unknown",
  "step.not_required",
  "step.reconciled.applied",
  "step.reconciled.indeterminate",
  "step.reconciled.not_applied",
])

const AUDIT_ROW_KEYS = [
  "event_hash",
  "event_json",
  "sequence",
] as const satisfies readonly (keyof SagaHistoryAuditRow)[]

const TRANSITION_ROW_KEYS = [
  "acquisition_id",
  "audit_event_hash",
  "audit_event_json",
  "audit_sequence",
  "authorization_checksum",
  "authorization_classified_at_ms",
  "authorization_id",
  "authorization_protocol_version",
  "authorization_transition_id",
  "created_at_ms",
  "fencing_token",
  "from_operation_status",
  "from_record_json",
  "holder_id",
  "lease_key",
  "operation_id",
  "step_id",
  "to_operation_status",
  "to_record_json",
  "transition_id",
] as const satisfies readonly (keyof SagaHistoryTransitionRow)[]

export interface SagaHistoryAuditProof {
  readonly auditEventCount: number
  readonly auditHeadEventHash: string
  readonly auditHeadSequence: number
  readonly environmentId: string
  readonly operationCreationEventHash: string
  readonly operationId: string
  readonly operationInputChecksum: string
  readonly operationPlanChecksum: string
  readonly operationTransitionCount: number
  readonly operationTransitionFoldChecksum: string
  readonly schemaVersion: 1
}

const AUDIT_PROOF_KEYS = [
  "auditEventCount",
  "auditHeadEventHash",
  "auditHeadSequence",
  "environmentId",
  "operationCreationEventHash",
  "operationId",
  "operationInputChecksum",
  "operationPlanChecksum",
  "operationTransitionCount",
  "operationTransitionFoldChecksum",
  "schemaVersion",
] as const satisfies readonly (keyof SagaHistoryAuditProof)[]

export interface SagaHistoryTransitionProof {
  readonly auditTransitionFoldChecksum: string
  readonly operation: OperationRecord
  readonly operationId: string
  readonly operationPlanChecksum: string
  readonly operationStatus: string
  readonly schemaVersion: 1
  readonly transitionCount: number
  readonly transitionLastAuditSequence: number
  readonly transitionLastId: string
}

interface SagaHistoryAuditFoldState {
  creationEventHash: string | null
  nextSequence: number
  operationTransitionCount: number
  operationTransitionFoldChecksum: string
  previousEventHash: string | null
  previousServerTimeMs: number | null
}

interface SagaHistoryTransitionFoldState {
  auditTransitionFoldChecksum: string
  cursor: SagaHistoryTransitionCursor
  operation: OperationRecord
  transitionCount: number
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  )
}

function exactRecord<Row extends object>(
  value: unknown,
  keys: readonly (keyof Row)[],
): value is Row {
  return (
    plainRecord(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => Object.hasOwn(value, key))
  )
}

function denseArray(value: unknown): value is readonly unknown[] {
  return (
    Array.isArray(value) &&
    Object.keys(value).length === value.length &&
    Object.keys(value).every((key, index) => key === String(index))
  )
}

function capturedAuditPage(value: unknown): SagaHistoryPage<SagaHistoryAuditRow, number> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga audit fold page could not be captured safely.")
  }
  if (
    !exactRecord<SagaHistoryPage<SagaHistoryAuditRow, number>>(snapshot, [
      "complete",
      "nextCursor",
      "rows",
    ])
  ) {
    return intervention("Saga audit fold page fields are malformed.")
  }
  if (typeof snapshot.complete !== "boolean") {
    return intervention("Saga audit fold page completion metadata is malformed.")
  }
  if (!denseArray(snapshot.rows)) {
    return intervention("Saga audit fold page rows are not a dense array.")
  }
  if (
    snapshot.rows.length === 0 ||
    snapshot.rows.length > SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !snapshot.rows.every((row) => exactRecord<SagaHistoryAuditRow>(row, AUDIT_ROW_KEYS))
  ) {
    return intervention("Saga audit fold page row envelope is malformed.")
  }
  const last = snapshot.rows.at(-1) as SagaHistoryAuditRow
  if (snapshot.complete) {
    if (snapshot.nextCursor !== null) {
      return intervention("A complete saga audit fold page retained a cursor.")
    }
  } else if (
    snapshot.rows.length !== SAGA_HISTORY_PAGE_ROW_LIMIT ||
    snapshot.nextCursor !== last.sequence
  ) {
    return intervention("An incomplete saga audit fold page has contradictory pagination.")
  }
  return Object.freeze({
    complete: snapshot.complete,
    nextCursor: snapshot.nextCursor,
    rows: Object.freeze(snapshot.rows.map((row) => Object.freeze(row))),
  })
}

function persistedText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0
}

function safeInteger(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum
}

function capturedAuditProof(value: unknown, anchor: SagaHistoryAnchor): SagaHistoryAuditProof {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga audit proof could not be captured safely.")
  }
  if (!exactRecord<SagaHistoryAuditProof>(snapshot, AUDIT_PROOF_KEYS)) {
    return intervention("Saga audit proof fields are malformed.")
  }
  if (
    snapshot.schemaVersion !== 1 ||
    snapshot.auditEventCount !== anchor.auditHeadSequence ||
    snapshot.auditHeadSequence !== anchor.auditHeadSequence ||
    snapshot.auditHeadEventHash !== anchor.auditHeadEventHash ||
    snapshot.environmentId !== anchor.environmentId ||
    snapshot.operationId !== anchor.operationId ||
    snapshot.operationInputChecksum !== anchor.operationInputChecksum ||
    snapshot.operationPlanChecksum !== anchor.operationPlanChecksum ||
    snapshot.operationTransitionCount !== anchor.operationTransitionCount ||
    !persistedText(snapshot.operationCreationEventHash) ||
    !CHECKSUM.test(snapshot.operationTransitionFoldChecksum)
  ) {
    return intervention("Saga audit proof contradicts its terminal-history anchor.")
  }
  return Object.freeze(snapshot)
}

function capturedTransitionPage(
  value: unknown,
): SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga transition fold page could not be captured safely.")
  }
  if (
    !exactRecord<SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor>>(snapshot, [
      "complete",
      "nextCursor",
      "rows",
    ])
  ) {
    return intervention("Saga transition fold page fields are malformed.")
  }
  if (typeof snapshot.complete !== "boolean" || !denseArray(snapshot.rows)) {
    return intervention("Saga transition fold page metadata is malformed.")
  }
  if (
    snapshot.rows.length === 0 ||
    snapshot.rows.length > SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !snapshot.rows.every((row) => exactRecord<SagaHistoryTransitionRow>(row, TRANSITION_ROW_KEYS))
  ) {
    return intervention("Saga transition fold page row envelope is malformed.")
  }
  const last = snapshot.rows.at(-1) as SagaHistoryTransitionRow
  if (snapshot.complete) {
    if (snapshot.nextCursor !== null) {
      return intervention("A complete saga transition fold page retained a cursor.")
    }
  } else if (
    !exactRecord<SagaHistoryTransitionCursor>(snapshot.nextCursor, [
      "auditSequence",
      "transitionId",
    ]) ||
    snapshot.rows.length !== SAGA_HISTORY_PAGE_ROW_LIMIT ||
    snapshot.nextCursor.auditSequence !== last.audit_sequence ||
    snapshot.nextCursor.transitionId !== last.transition_id
  ) {
    return intervention("An incomplete saga transition fold page has contradictory pagination.")
  }
  return Object.freeze({
    complete: snapshot.complete,
    nextCursor: snapshot.nextCursor === null ? null : Object.freeze({ ...snapshot.nextCursor }),
    rows: Object.freeze(snapshot.rows.map((row) => Object.freeze(row))),
  })
}

function sqliteBinaryTextCompare(left: string, right: string): number {
  const leftBytes = UTF8_ENCODER.encode(left)
  const rightBytes = UTF8_ENCODER.encode(right)
  const commonLength = Math.min(leftBytes.length, rightBytes.length)
  for (let index = 0; index < commonLength; index += 1) {
    const difference = (leftBytes[index] as number) - (rightBytes[index] as number)
    if (difference !== 0) return difference
  }
  return leftBytes.length - rightBytes.length
}

function pairAfter(
  leftNumber: number,
  leftText: string,
  rightNumber: number,
  rightText: string,
): boolean {
  return (
    leftNumber > rightNumber ||
    (leftNumber === rightNumber && sqliteBinaryTextCompare(leftText, rightText) > 0)
  )
}

function parsedJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value)
  } catch {
    return intervention(`${label} is invalid JSON.`)
  }
}

function frame(parts: readonly string[]): Uint8Array {
  const encoded = [AUDIT_TRANSITION_FOLD_DOMAIN, ...parts].map((part) =>
    new TextEncoder().encode(part),
  )
  const length = encoded.reduce((total, part) => total + 4 + part.byteLength, 0)
  const output = new Uint8Array(length)
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function foldChecksum(
  digest: DigestFunction,
  previous: string,
  event: AuditEvent,
): Promise<string> {
  const checksum = await digest(
    frame([previous, event.sequence.toString(10), event.eventHash]).slice(),
  )
  if (typeof checksum !== "string" || !CHECKSUM.test(checksum)) {
    return configuration("Saga audit fold digest must return a lowercase SHA-256 checksum.")
  }
  return checksum
}

function parsedAuditEvent(row: SagaHistoryAuditRow): unknown {
  try {
    return JSON.parse(row.event_json)
  } catch {
    return intervention("Saga audit fold event JSON is invalid.")
  }
}

function sameOptionalRecord(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function verifyAuthorizationReceipt(
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  planStep: OperationStepPlan,
  before: OperationStepRecord,
  after: OperationStepRecord,
): void {
  const noReceipt = row.authorization_transition_id === null
  const dispatch =
    planStep.checkpoint === "irreversible" &&
    (before.state === "pending" || before.state === "retryable_failed") &&
    after.state === "running"
  if (!dispatch) {
    if (
      !noReceipt ||
      row.authorization_protocol_version !== null ||
      row.authorization_id !== null ||
      row.authorization_checksum !== null ||
      row.authorization_classified_at_ms !== null
    ) {
      intervention("A non-dispatch saga transition retained an authorization receipt.")
    }
    if (
      before.authorizationChecksum !== after.authorizationChecksum ||
      !sameOptionalRecord(before.irreversibleAuthorization, after.irreversibleAuthorization)
    ) {
      intervention("A saga transition did not preserve its irreversible authorization.")
    }
    return
  }
  if (
    noReceipt ||
    row.authorization_transition_id !== row.transition_id ||
    (row.authorization_protocol_version !== 1 && row.authorization_protocol_version !== 2) ||
    !persistedText(row.authorization_checksum) ||
    row.authorization_checksum !== after.authorizationChecksum ||
    row.authorization_classified_at_ms !== row.created_at_ms
  ) {
    intervention("An irreversible saga dispatch lacks its exact authorization receipt.")
  }
  if (before.state === "retryable_failed" && before.irreversibleAuthorization === undefined) {
    intervention(
      "A retried irreversible saga dispatch lacks its complete predecessor authorization.",
    )
  }
  if (row.authorization_protocol_version === 1) {
    if (
      before.state !== "pending" ||
      row.authorization_id !== null ||
      after.irreversibleAuthorization !== undefined
    ) {
      intervention("A legacy irreversible saga dispatch claims a full authorization body.")
    }
    return
  }
  const authorization = after.irreversibleAuthorization
  if (
    !persistedText(row.authorization_id) ||
    authorization === undefined ||
    authorization.authorizationId !== row.authorization_id ||
    authorization.authorizationChecksum !== row.authorization_checksum ||
    authorization.operationId !== row.operation_id ||
    authorization.stepId !== row.step_id ||
    authorization.leaseKey !== row.lease_key ||
    authorization.holderId !== row.holder_id ||
    authorization.leaseAcquisitionId !== row.acquisition_id ||
    authorization.fencingToken !== row.fencing_token ||
    authorization.sealedAtServerTimeMs > event.serverTimeMs
  ) {
    intervention("A protocol-two saga dispatch receipt contradicts its authorization body.")
  }
}

function transitionIdentityKind(
  eventType: string,
  after: OperationStepRecord,
): "accepted" | "failed" | "succeeded" | null {
  if (eventType === "step.attempt.accepted" || eventType === "saga.action.started") {
    return "accepted"
  }
  if (
    eventType === "step.attempt.succeeded" ||
    eventType === "saga.initialized" ||
    eventType === "saga.termination.requested"
  ) {
    return "succeeded"
  }
  if (
    eventType === "step.attempt.definitely_not_applied" ||
    eventType === "step.attempt.permanent" ||
    eventType === "step.attempt.unknown"
  ) {
    return "failed"
  }
  if (eventType === "saga.action.classified") {
    return after.state === "succeeded" ? "succeeded" : "failed"
  }
  return null
}

function verifyDeferredTransitionIdentity(row: SagaHistoryTransitionRow, eventType: string): void {
  const kind =
    eventType === "step.not_required"
      ? "not-required"
      : eventType === "saga.action.recovered" || eventType.startsWith("step.crash.")
        ? "crash-recovered"
        : "reconciled"
  const prefix = `${operationTransitionIdentity(kind, [row.operation_id, row.step_id])}:`
  if (!row.transition_id.startsWith(prefix)) {
    intervention("A deferred saga transition has a contradictory stable identity.")
  }
  const framedSuffix = row.transition_id.slice(prefix.length)
  const separator = framedSuffix.indexOf(":")
  const lengthText = framedSuffix.slice(0, separator)
  const suffix = framedSuffix.slice(separator + 1)
  if (
    separator < 1 ||
    !/^[1-9][0-9]*$/u.test(lengthText) ||
    Number(lengthText) !== suffix.length ||
    !persistedText(suffix)
  ) {
    intervention("A deferred saga transition has a malformed stable identity suffix.")
  }
}

function verifyTransitionIdentity(
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  after: OperationStepRecord,
): void {
  const kind = transitionIdentityKind(event.eventType, after)
  if (kind === null) {
    verifyDeferredTransitionIdentity(row, event.eventType)
    return
  }
  const attemptId = after.lastAttemptId
  const validState =
    kind === "accepted"
      ? after.state === "running" && after.activeAttemptId === attemptId
      : kind === "succeeded"
        ? after.state === "succeeded"
        : after.state === "retryable_failed" ||
          after.state === "unknown" ||
          after.state === "failed"
  if (
    !persistedText(attemptId) ||
    !validState ||
    operationTransitionIdentity(kind, [row.operation_id, row.step_id, attemptId]) !==
      row.transition_id
  ) {
    intervention("A saga operation transition has a contradictory stable identity.")
  }
}

function verifyTransitionFence(
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  before: OperationStepRecord,
): void {
  const predecessorFence = before.fencingToken
  if (
    event.eventType === "step.attempt.succeeded" ||
    event.eventType === "step.attempt.definitely_not_applied" ||
    event.eventType === "step.attempt.permanent" ||
    event.eventType === "step.attempt.unknown" ||
    event.eventType === "saga.initialized"
  ) {
    if (predecessorFence !== row.fencing_token) {
      intervention("A saga outcome transition changed its active attempt fence.")
    }
    return
  }
  if (
    event.eventType === "step.crash.not_dispatched" ||
    event.eventType === "step.crash.outcome_unknown" ||
    event.eventType === "saga.action.recovered" ||
    event.eventType === "saga.action.observed"
  ) {
    if (predecessorFence === undefined || row.fencing_token <= predecessorFence) {
      intervention("A saga recovery transition lacks a strictly newer consumer fence.")
    }
    return
  }
  if (
    event.eventType.startsWith("step.reconciled.") ||
    event.eventType === "saga.action.classified"
  ) {
    if (predecessorFence === undefined || row.fencing_token < predecessorFence) {
      intervention("A saga classification transition regressed its consumer fence.")
    }
  }
}

function verifySagaEventPlan(event: AuditEvent, planStep: OperationStepPlan): void {
  const genericReceiptEvent =
    event.eventType.startsWith("step.attempt.") ||
    event.eventType.startsWith("step.crash.") ||
    event.eventType.startsWith("step.reconciled.")
  if (genericReceiptEvent && planStep.effectProtocol === "saga_receipt") {
    intervention("A saga-receipt action was relabeled as a generic step transition.")
  }
  if (
    event.eventType === "step.crash.not_dispatched" &&
    planStep.effectProtocol !== "provider_receipt"
  ) {
    intervention("A dispatch-absence event is bound to a non-provider operation step.")
  }
  if (event.eventType === "saga.initialized") {
    if (planStep.stepId !== "saga:init") {
      intervention("A saga initialization event is bound to a non-initialization step.")
    }
    return
  }
  if (event.eventType === "saga.termination.requested") {
    if (
      planStep.stepId !== "saga:termination" ||
      planStep.activation !== "conditional" ||
      planStep.checkpoint !== "reversible" ||
      planStep.completionRole !== "work" ||
      planStep.dependsOn.length !== 0 ||
      planStep.effectProtocol !== "opaque" ||
      planStep.retryClassification !== "idempotent"
    ) {
      intervention("A saga termination event lacks its canonical operation step.")
    }
    return
  }
  if (event.eventType.startsWith("saga.action.")) {
    const forward = "saga:forward:"
    const compensation = "saga:compensation:"
    if (
      planStep.effectProtocol !== "saga_receipt" ||
      !(
        (planStep.stepId.startsWith(forward) && planStep.stepId.length > forward.length) ||
        (planStep.stepId.startsWith(compensation) && planStep.stepId.length > compensation.length)
      )
    ) {
      intervention("A saga action event is bound to a non-action operation step.")
    }
  }
}

function requiredEvidence(value: string | undefined, label: string): string {
  if (!persistedText(value)) return intervention(`A saga operation transition lacks ${label}.`)
  return value
}

function counterDelta(
  before: Readonly<Record<string, number>>,
  after: Readonly<Record<string, number>>,
): Readonly<Record<string, number>> | undefined {
  for (const key of Object.keys(before)) {
    if (!Object.hasOwn(after, key)) {
      return intervention("A saga operation transition removed a durable counter.")
    }
  }
  const delta: Record<string, number> = {}
  for (const key of Object.keys(after)) {
    const prior = before[key] ?? 0
    const next = after[key] as number
    if (next < prior) {
      return intervention("A saga operation transition decreased a durable counter.")
    }
    if (!Object.hasOwn(before, key) || next !== prior) delta[key] = next - prior
  }
  return Object.keys(delta).length === 0 ? undefined : Object.freeze(delta)
}

function transitionCounterDeltas(
  before: OperationStepRecord,
  after: OperationStepRecord,
): CounterDeltas | undefined {
  const cost = counterDelta(before.costCounters, after.costCounters)
  const progress = counterDelta(before.progressCounters, after.progressCounters)
  return cost === undefined && progress === undefined
    ? undefined
    : Object.freeze({
        ...(cost === undefined ? {} : { cost }),
        ...(progress === undefined ? {} : { progress }),
      })
}

function operationWithStep(
  operation: OperationRecord,
  stepId: string,
  step: OperationStepRecord,
): OperationRecord {
  return Object.freeze({
    plan: operation.plan,
    steps: Object.freeze({ ...operation.steps, [stepId]: Object.freeze(step) }),
  })
}

function legacyCounters(
  before: OperationStepRecord,
  after: OperationStepRecord,
  allowed: boolean,
): Pick<OperationStepRecord, "costCounters" | "progressCounters"> {
  if (!allowed) {
    return { costCounters: before.costCounters, progressCounters: before.progressCounters }
  }
  transitionCounterDeltas(before, after)
  return { costCounters: after.costCounters, progressCounters: after.progressCounters }
}

function failureState(
  eventType: string,
  planStep: OperationStepPlan,
  after: OperationStepRecord,
): "failed" | "retryable_failed" | "unknown" {
  if (eventType === "step.attempt.unknown") return "unknown"
  if (eventType === "step.attempt.permanent") return "failed"
  if (eventType === "step.attempt.definitely_not_applied") {
    return planStep.retryClassification === "never" ? "failed" : "retryable_failed"
  }
  if (
    eventType === "saga.action.classified" &&
    (after.state === "retryable_failed" || after.state === "unknown")
  ) {
    return after.state
  }
  return intervention("A saga operation transition has an invalid failure classification.")
}

function legacyTransition(
  operation: OperationRecord,
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  planStep: OperationStepPlan,
  before: OperationStepRecord,
  after: OperationStepRecord,
): OperationRecord | undefined {
  const protocolOneDispatch =
    row.authorization_protocol_version === 1 &&
    before.state === "pending" &&
    after.state === "running" &&
    (event.eventType === "step.attempt.accepted" || event.eventType === "saga.action.started")
  const legacyPredecessor =
    planStep.checkpoint === "irreversible" &&
    before.startedAttempts > 0 &&
    before.authorizationChecksum !== undefined &&
    before.irreversibleAuthorization === undefined
  if (!protocolOneDispatch && !legacyPredecessor) return undefined

  if (protocolOneDispatch) {
    for (const dependency of planStep.dependsOn) {
      if (operation.steps[dependency]?.state !== "succeeded") {
        return intervention("A legacy saga dispatch has an unsatisfied operation dependency.")
      }
    }
    const attemptId = requiredEvidence(after.lastAttemptId, "its accepted attempt identity")
    const checksum = requiredEvidence(
      after.authorizationChecksum,
      "its legacy authorization checksum",
    )
    return operationWithStep(operation, row.step_id, {
      activeAttemptId: attemptId,
      authorizationChecksum: checksum,
      costCounters: before.costCounters,
      fencingToken: row.fencing_token,
      lastAttemptId: attemptId,
      progressCounters: before.progressCounters,
      startedAttempts: before.startedAttempts + 1,
      state: "running",
    })
  }

  const authorizationChecksum = requiredEvidence(
    before.authorizationChecksum,
    "its retained legacy authorization checksum",
  )
  const genericCounters = event.eventType.startsWith("step.attempt.")
  if (
    event.eventType === "step.attempt.succeeded" ||
    event.eventType === "saga.initialized" ||
    (event.eventType === "saga.action.classified" && after.state === "succeeded")
  ) {
    if (
      before.state !== "running" ||
      before.activeAttemptId === undefined ||
      before.activeAttemptId !== before.lastAttemptId
    ) {
      return intervention("A legacy saga success has no exact running predecessor.")
    }
    return operationWithStep(operation, row.step_id, {
      authorizationChecksum,
      ...legacyCounters(before, after, genericCounters),
      fencingToken: before.fencingToken as number,
      lastAttemptId: before.lastAttemptId,
      resultChecksum: requiredEvidence(after.resultChecksum, "its result checksum"),
      startedAttempts: before.startedAttempts,
      state: "succeeded",
    })
  }
  if (
    event.eventType === "step.attempt.definitely_not_applied" ||
    event.eventType === "step.attempt.permanent" ||
    event.eventType === "step.attempt.unknown" ||
    event.eventType === "saga.action.classified"
  ) {
    if (
      before.state !== "running" ||
      before.activeAttemptId === undefined ||
      before.activeAttemptId !== before.lastAttemptId
    ) {
      return intervention("A legacy saga failure has no exact running predecessor.")
    }
    return operationWithStep(operation, row.step_id, {
      authorizationChecksum,
      ...legacyCounters(before, after, genericCounters),
      errorChecksum: requiredEvidence(after.errorChecksum, "its failure checksum"),
      fencingToken: before.fencingToken as number,
      lastAttemptId: before.lastAttemptId,
      startedAttempts: before.startedAttempts,
      state: failureState(event.eventType, planStep, after),
    })
  }
  if (
    event.eventType === "step.crash.not_dispatched" ||
    event.eventType === "step.crash.outcome_unknown" ||
    event.eventType === "saga.action.recovered"
  ) {
    if (
      before.state !== "running" ||
      before.activeAttemptId === undefined ||
      before.activeAttemptId !== before.lastAttemptId
    ) {
      return intervention("A legacy saga recovery has no exact running predecessor.")
    }
    const common = {
      authorizationChecksum,
      costCounters: before.costCounters,
      fencingToken: before.fencingToken as number,
      lastAttemptId: before.lastAttemptId,
      progressCounters: before.progressCounters,
      startedAttempts: before.startedAttempts,
    }
    if (event.eventType === "step.crash.outcome_unknown") {
      return operationWithStep(operation, row.step_id, {
        ...common,
        ...(after.errorChecksum === undefined ? {} : { errorChecksum: after.errorChecksum }),
        state: "unknown",
      })
    }
    if (event.eventType === "saga.action.recovered") {
      if (after.state === "unknown") {
        return operationWithStep(operation, row.step_id, {
          ...common,
          errorChecksum: event.payloadChecksum,
          state: "unknown",
        })
      }
      if (after.state === "succeeded") {
        return operationWithStep(operation, row.step_id, {
          ...common,
          errorChecksum: event.payloadChecksum,
          reconciliationEvidenceChecksum: event.payloadChecksum,
          resultChecksum: event.payloadChecksum,
          state: "succeeded",
        })
      }
      if (after.state !== "retryable_failed") {
        return intervention("A legacy saga recovery has an impossible operation outcome.")
      }
      return operationWithStep(operation, row.step_id, {
        ...common,
        reconciliationEvidenceChecksum: event.payloadChecksum,
        state: "retryable_failed",
      })
    }
    const state = planStep.retryClassification === "never" ? "failed" : "retryable_failed"
    return operationWithStep(operation, row.step_id, {
      ...common,
      reconciliationEvidenceChecksum: requiredEvidence(
        after.reconciliationEvidenceChecksum,
        "its recovery evidence",
      ),
      state,
    })
  }
  if (
    event.eventType === "saga.action.observed" ||
    event.eventType.startsWith("step.reconciled.")
  ) {
    const allowedCounters = event.eventType.startsWith("step.reconciled.")
    if (
      (allowedCounters && before.state !== "unknown") ||
      (!allowedCounters && before.state !== "unknown" && before.state !== "retryable_failed")
    ) {
      return intervention("A legacy saga reconciliation has no exact uncertain predecessor.")
    }
    if (
      event.eventType === "saga.action.observed" &&
      before.state === "retryable_failed" &&
      event.payloadChecksum !== (before.reconciliationEvidenceChecksum ?? before.errorChecksum)
    ) {
      return intervention("A legacy saga observation contradicts proven non-application evidence.")
    }
    const evidenceChecksum = allowedCounters
      ? requiredEvidence(after.reconciliationEvidenceChecksum, "its reconciliation evidence")
      : event.payloadChecksum
    const state =
      event.eventType === "step.reconciled.applied"
        ? "succeeded"
        : event.eventType === "step.reconciled.indeterminate"
          ? "intervention_required"
          : event.eventType === "step.reconciled.not_applied"
            ? planStep.retryClassification === "never"
              ? "failed"
              : "retryable_failed"
            : before.state === "retryable_failed"
              ? "succeeded"
              : after.state === "succeeded" ||
                  after.state === "retryable_failed" ||
                  after.state === "intervention_required"
                ? after.state
                : intervention("A legacy saga observation has an impossible operation outcome.")
    return operationWithStep(operation, row.step_id, {
      authorizationChecksum,
      ...legacyCounters(before, after, allowedCounters),
      ...(before.errorChecksum === undefined ? {} : { errorChecksum: before.errorChecksum }),
      fencingToken: before.fencingToken as number,
      lastAttemptId: before.lastAttemptId as string,
      reconciliationEvidenceChecksum: evidenceChecksum,
      ...(state === "succeeded"
        ? {
            resultChecksum: allowedCounters
              ? requiredEvidence(after.resultChecksum, "its reconciled result")
              : event.payloadChecksum,
          }
        : {}),
      startedAttempts: before.startedAttempts,
      state,
    })
  }
  return intervention("A legacy saga operation transition has unsupported semantics.")
}

function coreTransition(
  operation: OperationRecord,
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  planStep: OperationStepPlan,
  before: OperationStepRecord,
  after: OperationStepRecord,
): OperationRecord {
  const proof = Object.freeze({
    acquisitionId: row.acquisition_id,
    fencingToken: row.fencing_token,
    holderId: row.holder_id,
    leaseKey: row.lease_key,
  })
  if (event.eventType === "step.attempt.accepted" || event.eventType === "saga.action.started") {
    if (row.created_at_ms === Number.MAX_SAFE_INTEGER) {
      return intervention("A saga dispatch has no representable active lease interval.")
    }
    const decision = beginOperationStep(operation, {
      attemptId: requiredEvidence(after.lastAttemptId, "its accepted attempt identity"),
      idempotencyKey: planStep.idempotencyKey,
      ...(after.irreversibleAuthorization === undefined
        ? {}
        : { irreversibleAuthorization: after.irreversibleAuthorization }),
      lease: Object.freeze({
        acquisitionId: row.acquisition_id,
        expiresAtServerTimeMs: row.created_at_ms + 1,
        fencingToken: row.fencing_token,
        holderId: row.holder_id,
        leaseKey: row.lease_key,
      }),
      leaseProof: proof,
      observedPreconditionChecksum: planStep.preconditionChecksum,
      serverTimeMs: row.created_at_ms,
      stepId: row.step_id,
    })
    if (decision.disposition !== "execute") {
      return intervention("A saga dispatch does not reconstruct as an exact core transition.")
    }
    return decision.operation
  }
  if (event.eventType === "saga.termination.requested") {
    return recordAtomicStepOutcome(operation, {
      attemptId: requiredEvidence(after.lastAttemptId, "its atomic attempt identity"),
      idempotencyKey: planStep.idempotencyKey,
      leaseProof: proof,
      observedPreconditionChecksum: planStep.preconditionChecksum,
      outcome: {
        observedPostconditionChecksum: planStep.postconditionChecksum,
        resultChecksum: requiredEvidence(after.resultChecksum, "its atomic result checksum"),
        state: "succeeded",
      },
      stepId: row.step_id,
    })
  }
  if (
    event.eventType === "step.attempt.succeeded" ||
    event.eventType === "saga.initialized" ||
    (event.eventType === "saga.action.classified" && after.state === "succeeded")
  ) {
    const counters =
      event.eventType === "step.attempt.succeeded"
        ? transitionCounterDeltas(before, after)
        : undefined
    return recordStepSuccess(operation, {
      attemptId: requiredEvidence(after.lastAttemptId, "its successful attempt identity"),
      ...(counters === undefined ? {} : { counters }),
      observedPostconditionChecksum: planStep.postconditionChecksum,
      resultChecksum: requiredEvidence(after.resultChecksum, "its result checksum"),
      stepId: row.step_id,
    })
  }
  if (
    event.eventType === "step.attempt.definitely_not_applied" ||
    event.eventType === "step.attempt.permanent" ||
    event.eventType === "step.attempt.unknown" ||
    event.eventType === "saga.action.classified"
  ) {
    const counters = event.eventType.startsWith("step.attempt.")
      ? transitionCounterDeltas(before, after)
      : undefined
    const state = failureState(event.eventType, planStep, after)
    return recordStepFailure(operation, {
      attemptId: requiredEvidence(after.lastAttemptId, "its failed attempt identity"),
      ...(counters === undefined ? {} : { counters }),
      errorChecksum: requiredEvidence(after.errorChecksum, "its failure checksum"),
      outcome:
        state === "unknown"
          ? "unknown"
          : state === "retryable_failed" || planStep.retryClassification === "never"
            ? "definitely_not_applied"
            : "permanent",
      stepId: row.step_id,
    })
  }
  if (event.eventType === "step.crash.not_dispatched") {
    return markRunningStepNotDispatchedAfterCrash(
      operation,
      row.step_id,
      requiredEvidence(after.reconciliationEvidenceChecksum, "its dispatch-absence evidence"),
    )
  }
  if (event.eventType === "step.crash.outcome_unknown") {
    return markRunningStepUnknownAfterCrash(operation, row.step_id, after.errorChecksum)
  }
  if (event.eventType === "saga.action.recovered") {
    if (after.state === "unknown") {
      return markRunningStepUnknownAfterCrash(operation, row.step_id, event.payloadChecksum)
    }
    if (after.state === "retryable_failed") {
      return markRunningStepNotDispatchedAfterCrash(operation, row.step_id, event.payloadChecksum)
    }
    if (after.state === "succeeded") {
      const unknown = markRunningStepUnknownAfterCrash(
        operation,
        row.step_id,
        event.payloadChecksum,
      )
      return recordSagaStepTerminalClassification(unknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: event.payloadChecksum,
        stepId: row.step_id,
      })
    }
    return intervention("A saga recovery has an impossible operation outcome.")
  }
  if (event.eventType === "step.not_required") {
    return markOperationStepNotRequired(operation, {
      evidenceChecksum: requiredEvidence(
        after.reconciliationEvidenceChecksum,
        "its conditional decision evidence",
      ),
      stepId: row.step_id,
    })
  }
  if (event.eventType === "saga.action.observed" && before.state === "retryable_failed") {
    return recordSagaStepTerminalClassification(operation, {
      outcome: "not_applied",
      receiptOutcomeChecksum: event.payloadChecksum,
      stepId: row.step_id,
    })
  }
  if (event.eventType === "saga.action.observed" && after.state === "failed") {
    return intervention("A saga observation has an impossible operation outcome.")
  }
  if (
    event.eventType === "saga.action.observed" &&
    after.state === "succeeded" &&
    (after.reconciliationEvidenceChecksum !== event.payloadChecksum ||
      after.resultChecksum !== event.payloadChecksum)
  ) {
    return intervention("A saga observation contradicts its exact receipt outcome evidence.")
  }
  const counters = event.eventType.startsWith("step.reconciled.")
    ? transitionCounterDeltas(before, after)
    : undefined
  const outcome =
    event.eventType === "step.reconciled.applied"
      ? "applied"
      : event.eventType === "step.reconciled.indeterminate"
        ? "indeterminate"
        : event.eventType === "step.reconciled.not_applied"
          ? "not_applied"
          : after.state === "succeeded"
            ? "applied"
            : after.state === "intervention_required"
              ? "indeterminate"
              : "not_applied"
  return recordStepReconciliation(operation, {
    ...(counters === undefined ? {} : { counters }),
    evidenceChecksum: requiredEvidence(
      after.reconciliationEvidenceChecksum,
      "its reconciliation evidence",
    ),
    ...(outcome === "applied"
      ? {
          observedPostconditionChecksum: planStep.postconditionChecksum,
          resultChecksum: requiredEvidence(after.resultChecksum, "its reconciled result"),
        }
      : {}),
    outcome,
    stepId: row.step_id,
  })
}

async function verifyExactTransition(
  operation: OperationRecord,
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
  planStep: OperationStepPlan,
  before: OperationStepRecord,
  after: OperationStepRecord,
  digest: DigestFunction,
): Promise<void> {
  if (
    event.eventType === "step.crash.outcome_unknown" &&
    (planStep.effectProtocol === "provider_receipt") !== (after.errorChecksum !== undefined)
  ) {
    return intervention("A crash transition contradicts its provider-acceptance evidence mode.")
  }
  if (event.eventType === "step.crash.not_dispatched") {
    const attemptId = requiredEvidence(before.activeAttemptId, "its recovered attempt identity")
    const expectedEvidence = await digest(
      UTF8_ENCODER.encode(
        operationTransitionIdentity("provider-not-dispatched-evidence", [
          row.operation_id,
          row.step_id,
          attemptId,
          row.fencing_token.toString(10),
        ]),
      ).slice(),
    )
    if (
      typeof expectedEvidence !== "string" ||
      !CHECKSUM.test(expectedEvidence) ||
      after.reconciliationEvidenceChecksum !== expectedEvidence
    ) {
      return intervention("A saga crash transition has contradictory dispatch-absence evidence.")
    }
  }
  let expected: OperationRecord
  try {
    expected =
      legacyTransition(operation, row, event, planStep, before, after) ??
      coreTransition(operation, row, event, planStep, before, after)
  } catch {
    return intervention("A saga operation transition violates its exact core state transition.")
  }
  if (
    operationStepRecordJson(expected.steps[row.step_id] as OperationStepRecord) !==
    row.to_record_json
  ) {
    return intervention("A saga operation transition violates its exact core state transition.")
  }
}

function verifyDirectAuditPayload(
  event: AuditEvent,
  planStep: OperationStepPlan,
  after: OperationStepRecord,
): void {
  let expected: string | undefined
  if (event.eventType === "step.attempt.accepted") expected = planStep.inputChecksum
  else if (event.eventType === "step.attempt.succeeded") expected = after.resultChecksum
  else if (event.eventType === "saga.termination.requested") expected = after.resultChecksum
  else if (event.eventType === "saga.action.classified") {
    expected = after.state === "succeeded" ? after.resultChecksum : after.errorChecksum
  } else if (event.eventType === "saga.action.observed") {
    expected = after.reconciliationEvidenceChecksum
  } else if (event.eventType === "saga.action.recovered") {
    expected =
      after.state === "unknown"
        ? after.errorChecksum
        : after.state === "succeeded"
          ? after.resultChecksum
          : after.reconciliationEvidenceChecksum
  } else if (event.eventType === "step.crash.not_dispatched") {
    expected = after.lastAttemptId
  } else if (event.eventType === "step.crash.outcome_unknown") {
    expected = after.errorChecksum ?? after.lastAttemptId
  } else if (
    event.eventType === "step.attempt.definitely_not_applied" ||
    event.eventType === "step.attempt.permanent" ||
    event.eventType === "step.attempt.unknown"
  ) {
    expected = after.errorChecksum
  } else if (
    event.eventType === "step.not_required" ||
    event.eventType === "step.reconciled.applied" ||
    event.eventType === "step.reconciled.indeterminate" ||
    event.eventType === "step.reconciled.not_applied"
  ) {
    expected = after.reconciliationEvidenceChecksum
  }
  if (expected !== undefined && event.payloadChecksum !== expected) {
    intervention("A saga operation transition audit payload contradicts its step record.")
  }
}

/**
 * Incrementally verifies the pinned environment audit chain and reduces this operation's
 * transition-linked events to a constant-size ordered checksum. It is intentionally internal
 * and does not reconstruct either projection or grant terminal mutation authority.
 */
export class SagaHistoryAuditFolder {
  readonly #anchor: SagaHistoryAnchor
  readonly #digest: DigestFunction
  #appending = false
  #complete = false
  #creationEventHash: string | null = null
  #nextSequence = 1
  #operationTransitionCount = 0
  #operationTransitionFoldChecksum = EMPTY_FOLD_CHECKSUM
  #previousEventHash: string | null = null
  #previousServerTimeMs: number | null = null

  constructor(inputAnchor: SagaHistoryAnchor, digest: DigestFunction) {
    if (typeof digest !== "function") configuration("A saga audit fold digest is required.")
    this.#anchor = loadSagaHistoryAnchor(inputAnchor)
    this.#digest = digest
  }

  async #foldOperationEvent(event: AuditEvent, state: SagaHistoryAuditFoldState): Promise<void> {
    if (event.eventType === "operation.created") {
      if (state.creationEventHash !== null || state.operationTransitionCount !== 0) {
        return intervention("The saga operation creation audit event is duplicated or reordered.")
      }
      if (
        event.stepId !== null ||
        event.fencingToken !== null ||
        event.payloadChecksum !== this.#anchor.operationInputChecksum
      ) {
        return intervention("The saga operation creation audit event contradicts its anchor.")
      }
      state.creationEventHash = event.eventHash
      return
    }
    if (state.creationEventHash === null) {
      return intervention("A saga operation transition audit event precedes operation creation.")
    }
    if (event.stepId === null || event.fencingToken === null) {
      return intervention("A saga operation transition audit event lacks its fenced step.")
    }
    if (state.operationTransitionCount >= this.#anchor.operationTransitionCount) {
      return intervention("Saga operation audit history exceeds its anchored transition count.")
    }
    state.operationTransitionFoldChecksum = await foldChecksum(
      this.#digest,
      state.operationTransitionFoldChecksum,
      event,
    )
    state.operationTransitionCount += 1
  }

  async #foldRow(row: SagaHistoryAuditRow, state: SagaHistoryAuditFoldState): Promise<void> {
    const event = await loadAuditEvent(parsedAuditEvent(row), this.#digest)
    if (JSON.stringify(event) !== row.event_json) {
      return intervention("Saga audit fold event JSON is not canonical.")
    }
    if (row.sequence !== state.nextSequence || event.sequence !== row.sequence) {
      return intervention("Saga audit fold sequence is incomplete or contradictory.")
    }
    if (row.event_hash !== event.eventHash) {
      return intervention("Saga audit fold row hash contradicts its event body.")
    }
    if (
      event.environmentId !== this.#anchor.environmentId ||
      event.sequence > this.#anchor.auditHeadSequence
    ) {
      return intervention("Saga audit fold event lies outside its anchored environment history.")
    }
    if (event.previousHash !== state.previousEventHash) {
      return intervention("Saga audit fold event does not extend the exact previous hash.")
    }
    if (state.previousServerTimeMs !== null && event.serverTimeMs < state.previousServerTimeMs) {
      return intervention("Saga audit fold server time decreases.")
    }
    if (event.operationId === this.#anchor.operationId) {
      await this.#foldOperationEvent(event, state)
    }
    state.nextSequence += 1
    state.previousEventHash = event.eventHash
    state.previousServerTimeMs = event.serverTimeMs
  }

  async append(inputPage: SagaHistoryPage<SagaHistoryAuditRow, number>): Promise<void> {
    if (this.#complete) configuration("Saga audit history is already completely folded.")
    if (this.#appending) configuration("A saga audit history page is already being folded.")
    this.#appending = true
    try {
      const page = capturedAuditPage(inputPage)
      const state: SagaHistoryAuditFoldState = {
        creationEventHash: this.#creationEventHash,
        nextSequence: this.#nextSequence,
        operationTransitionCount: this.#operationTransitionCount,
        operationTransitionFoldChecksum: this.#operationTransitionFoldChecksum,
        previousEventHash: this.#previousEventHash,
        previousServerTimeMs: this.#previousServerTimeMs,
      }
      for (const row of page.rows) await this.#foldRow(row, state)
      if (!page.complete) {
        if (state.nextSequence > this.#anchor.auditHeadSequence) {
          return intervention("Saga audit fold page failed to close at its anchored head.")
        }
      } else {
        if (
          state.nextSequence !== this.#anchor.auditHeadSequence + 1 ||
          state.previousEventHash !== this.#anchor.auditHeadEventHash ||
          state.creationEventHash === null ||
          state.operationTransitionCount !== this.#anchor.operationTransitionCount
        ) {
          return intervention("Saga audit fold does not reconcile with its complete anchor.")
        }
        this.#complete = true
      }
      this.#creationEventHash = state.creationEventHash
      this.#nextSequence = state.nextSequence
      this.#operationTransitionCount = state.operationTransitionCount
      this.#operationTransitionFoldChecksum = state.operationTransitionFoldChecksum
      this.#previousEventHash = state.previousEventHash
      this.#previousServerTimeMs = state.previousServerTimeMs
    } finally {
      this.#appending = false
    }
  }

  proof(): SagaHistoryAuditProof {
    if (!this.#complete || this.#creationEventHash === null) {
      return resume("Saga audit history requires more verified pages.")
    }
    return Object.freeze({
      auditEventCount: this.#anchor.auditHeadSequence,
      auditHeadEventHash: this.#anchor.auditHeadEventHash,
      auditHeadSequence: this.#anchor.auditHeadSequence,
      environmentId: this.#anchor.environmentId,
      operationCreationEventHash: this.#creationEventHash,
      operationId: this.#anchor.operationId,
      operationInputChecksum: this.#anchor.operationInputChecksum,
      operationPlanChecksum: this.#anchor.operationPlanChecksum,
      operationTransitionCount: this.#operationTransitionCount,
      operationTransitionFoldChecksum: this.#operationTransitionFoldChecksum,
      schemaVersion: 1,
    })
  }
}

/**
 * Reconstructs the generic operation projection from its immutable transition pages and
 * reconciles their ordered audit events with a completed audit proof. This remains a read-only
 * structural proof; saga effects and attempt receipts must still supply cross-ledger semantics.
 */
export class SagaHistoryTransitionFolder {
  readonly #anchor: SagaHistoryAnchor
  readonly #auditProof: SagaHistoryAuditProof
  readonly #digest: DigestFunction
  #appending = false
  #complete = false
  #state: SagaHistoryTransitionFoldState

  constructor(
    inputAnchor: SagaHistoryAnchor,
    inputAuditProof: SagaHistoryAuditProof,
    plan: OperationPlan,
    digest: DigestFunction,
  ) {
    if (typeof digest !== "function") configuration("A saga transition fold digest is required.")
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    const auditProof = capturedAuditProof(inputAuditProof, anchor)
    const operation = createOperationRecord(plan)
    if (
      plan.operationId !== anchor.operationId ||
      plan.inputChecksum !== anchor.operationInputChecksum ||
      plan.planChecksum !== anchor.operationPlanChecksum ||
      !plan.operationType.startsWith("saga:")
    ) {
      intervention("The saga transition fold plan contradicts its history anchor.")
    }
    this.#anchor = anchor
    this.#auditProof = auditProof
    this.#digest = digest
    this.#state = {
      auditTransitionFoldChecksum: EMPTY_FOLD_CHECKSUM,
      cursor: Object.freeze({ auditSequence: 0, transitionId: "" }),
      operation,
      transitionCount: 0,
    }
  }

  async #foldRow(
    row: SagaHistoryTransitionRow,
    state: SagaHistoryTransitionFoldState,
  ): Promise<void> {
    if (
      row.operation_id !== this.#anchor.operationId ||
      !safeInteger(row.audit_sequence, 1) ||
      !safeInteger(row.created_at_ms, 0) ||
      !safeInteger(row.fencing_token, 1) ||
      ![
        row.transition_id,
        row.step_id,
        row.from_operation_status,
        row.to_operation_status,
        row.audit_event_hash,
        row.lease_key,
        row.holder_id,
        row.acquisition_id,
        row.from_record_json,
        row.to_record_json,
        row.audit_event_json,
      ].every(persistedText) ||
      !pairAfter(
        row.audit_sequence,
        row.transition_id,
        state.cursor.auditSequence,
        state.cursor.transitionId,
      ) ||
      row.audit_sequence > this.#anchor.operationTransitionLastAuditSequence ||
      (row.audit_sequence === this.#anchor.operationTransitionLastAuditSequence &&
        sqliteBinaryTextCompare(row.transition_id, this.#anchor.operationTransitionLastId) > 0) ||
      state.transitionCount >= this.#anchor.operationTransitionCount
    ) {
      return intervention("Saga operation-transition fold history is malformed or unordered.")
    }
    const planStep = state.operation.plan.steps.find(
      (candidate) => candidate.stepId === row.step_id,
    )
    const beforeStep = state.operation.steps[row.step_id]
    if (
      planStep === undefined ||
      beforeStep === undefined ||
      planStep.leaseKey !== row.lease_key ||
      operationStepRecordJson(beforeStep) !== row.from_record_json ||
      row.from_record_json === row.to_record_json
    ) {
      return intervention("Saga operation-transition fold predecessor is contradictory.")
    }
    const afterOperation = await loadOperationRecord(
      {
        plan: state.operation.plan,
        steps: {
          ...state.operation.steps,
          [row.step_id]: parsedJson(
            row.to_record_json,
            "Saga operation-transition successor record",
          ),
        },
      },
      this.#digest,
    )
    const afterStep = afterOperation.steps[row.step_id] as OperationStepRecord
    if (
      operationStepRecordJson(afterStep) !== row.to_record_json ||
      operationStatus(state.operation) !== row.from_operation_status ||
      operationStatus(afterOperation) !== row.to_operation_status
    ) {
      return intervention("Saga operation-transition fold successor is contradictory.")
    }

    const event = await loadAuditEvent(
      parsedJson(row.audit_event_json, "Saga operation-transition audit event"),
      this.#digest,
    )
    if (
      JSON.stringify(event) !== row.audit_event_json ||
      event.environmentId !== this.#anchor.environmentId ||
      event.operationId !== this.#anchor.operationId ||
      event.sequence !== row.audit_sequence ||
      event.eventHash !== row.audit_event_hash ||
      event.stepId !== row.step_id ||
      event.fencingToken !== row.fencing_token ||
      event.idempotencyKey !== row.transition_id ||
      event.serverTimeMs > row.created_at_ms ||
      !KNOWN_TRANSITION_AUDIT_EVENT_TYPES.has(event.eventType)
    ) {
      return intervention("Saga operation-transition audit evidence is contradictory.")
    }
    verifySagaEventPlan(event, planStep)
    verifyAuthorizationReceipt(row, event, planStep, beforeStep, afterStep)
    verifyTransitionIdentity(row, event, afterStep)
    verifyTransitionFence(row, event, beforeStep)
    await verifyExactTransition(
      state.operation,
      row,
      event,
      planStep,
      beforeStep,
      afterStep,
      this.#digest,
    )
    verifyDirectAuditPayload(event, planStep, afterStep)

    state.auditTransitionFoldChecksum = await foldChecksum(
      this.#digest,
      state.auditTransitionFoldChecksum,
      event,
    )
    state.cursor = Object.freeze({
      auditSequence: row.audit_sequence,
      transitionId: row.transition_id,
    })
    state.operation = afterOperation
    state.transitionCount += 1
  }

  async append(
    inputPage: SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor>,
  ): Promise<void> {
    if (this.#complete) configuration("Saga operation-transition history is already folded.")
    if (this.#appending) {
      configuration("A saga operation-transition history page is already being folded.")
    }
    this.#appending = true
    try {
      const page = capturedTransitionPage(inputPage)
      const state: SagaHistoryTransitionFoldState = {
        auditTransitionFoldChecksum: this.#state.auditTransitionFoldChecksum,
        cursor: this.#state.cursor,
        operation: this.#state.operation,
        transitionCount: this.#state.transitionCount,
      }
      for (const row of page.rows) await this.#foldRow(row, state)
      const atHead =
        state.cursor.auditSequence === this.#anchor.operationTransitionLastAuditSequence &&
        state.cursor.transitionId === this.#anchor.operationTransitionLastId
      if (!page.complete) {
        if (atHead || state.transitionCount >= this.#anchor.operationTransitionCount) {
          return intervention("Saga operation-transition page failed to close at its anchor.")
        }
      } else if (
        !atHead ||
        state.transitionCount !== this.#anchor.operationTransitionCount ||
        state.auditTransitionFoldChecksum !== this.#auditProof.operationTransitionFoldChecksum ||
        operationStatus(state.operation) !== this.#anchor.operationStatus
      ) {
        return intervention("Saga operation-transition fold does not reconcile with its proofs.")
      } else {
        this.#complete = true
      }
      this.#state = state
    } finally {
      this.#appending = false
    }
  }

  proof(): SagaHistoryTransitionProof {
    if (!this.#complete) {
      return resume("Saga operation-transition history requires more verified pages.")
    }
    return Object.freeze({
      auditTransitionFoldChecksum: this.#state.auditTransitionFoldChecksum,
      operation: this.#state.operation,
      operationId: this.#anchor.operationId,
      operationPlanChecksum: this.#anchor.operationPlanChecksum,
      operationStatus: this.#anchor.operationStatus,
      schemaVersion: 1,
      transitionCount: this.#state.transitionCount,
      transitionLastAuditSequence: this.#state.cursor.auditSequence,
      transitionLastId: this.#state.cursor.transitionId,
    })
  }
}
