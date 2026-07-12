import {
  type AuditEvent,
  beginOperationStep,
  beginSagaAction,
  type CounterDeltas,
  createOperationRecord,
  createSagaRecord,
  type DigestFunction,
  loadAuditEvent,
  loadOperationRecord,
  loadSagaDescriptor,
  loadSagaRecord,
  markOperationStepNotRequired,
  markRunningSagaActionUnknown,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepUnknownAfterCrash,
  markSagaActionNotDispatched,
  NozzleError,
  type OperationPlan,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  operationStatus,
  recordAtomicStepOutcome,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordSagaObservation,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  requestSagaTermination,
  type SagaActionPhase,
  type SagaActionRecord,
  type SagaDescriptor,
  type SagaRecord,
  sagaActionKey,
} from "@nozzle/core"
import { operationStepRecordJson, operationTransitionIdentity } from "./operation-store.js"
import {
  loadSagaAttemptIdentityRow,
  type SagaAttemptIdentity,
  type SagaAttemptIdentityRow,
  type SagaAttemptRecord,
} from "./saga-attempt-codec.js"
import {
  D1SagaHistoryReader,
  loadSagaHistoryAnchor,
  SAGA_HISTORY_PAGE_ROW_LIMIT,
  type SagaHistoryAnchor,
  type SagaHistoryAttemptCursor,
  type SagaHistoryAuditRow,
  type SagaHistoryEffectRow,
  type SagaHistoryPage,
  type SagaHistoryTransitionCursor,
  type SagaHistoryTransitionRow,
} from "./saga-history.js"
import { verifySagaOperationPlan } from "./saga-plan.js"
import {
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "./saga-store.js"

const AUDIT_TRANSITION_FOLD_DOMAIN = "nozzle.saga-history.audit-transition-fold.v1"
const EFFECT_FOLD_DOMAIN = "nozzle.saga-history.effect-fold.v1"
const ATTEMPT_FOLD_DOMAIN = "nozzle.saga-history.attempt-fold.v1"
const SAGA_COORDINATOR_ID_DOMAIN = "nozzle.saga-coordinator-id.v1"
const SAGA_RECORD_DOMAIN = "nozzle.saga-record.v1"
const SETTLEMENT_ATTEMPT_PREFIX = "nozzle.saga-settlement-attempt.v1:"
const EMPTY_FOLD_CHECKSUM = "0".repeat(64)
const CHECKSUM = /^[0-9a-f]{64}$/u
const UTF8_ENCODER = new TextEncoder()

const TERMINAL_NOT_APPLIED_EVENT_TYPES = new Set([
  "saga.action.terminal_not_applied.crash_absence",
  "saga.action.terminal_not_applied.direct_receipt",
  "saga.action.terminal_not_applied.observation",
])

const KNOWN_TRANSITION_AUDIT_EVENT_TYPES = new Set([
  "saga.action.classified",
  "saga.action.observed",
  "saga.action.recovered",
  "saga.action.started",
  ...TERMINAL_NOT_APPLIED_EVENT_TYPES,
  "saga.initialized",
  "saga.settled",
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

const EFFECT_ROW_KEYS = [
  "acquisition_id",
  "created_at_ms",
  "effect_id",
  "effect_kind",
  "evidence_checksum",
  "fencing_token",
  "from_state_version",
  "holder_id",
  "lease_key",
  "operation_id",
  "record_checksum",
  "record_json",
  "resource_id",
  "resource_kind",
  "step_id",
  "to_state_version",
  "transition_id",
] as const satisfies readonly (keyof SagaHistoryEffectRow)[]

const ATTEMPT_ROW_KEYS = [
  "acceptance_checksum",
  "accepted_at_ms",
  "acquisition_id",
  "action_key",
  "attempt_id",
  "causal_attempt_id",
  "fencing_token",
  "holder_id",
  "idempotency_key",
  "input_checksum",
  "input_json",
  "lease_key",
  "operation_id",
  "operation_step_id",
  "phase",
  "protocol_classified_at_ms",
  "protocol_version",
  "purpose",
  "saga_id",
  "saga_step_id",
] as const satisfies readonly (keyof SagaAttemptIdentityRow)[]

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
  readonly auditHeadEventHash: string
  readonly auditHeadSequence: number
  readonly auditTransitionFoldChecksum: string
  readonly environmentId: string
  readonly operation: OperationRecord
  readonly operationId: string
  readonly operationPlanChecksum: string
  readonly operationStatus: string
  readonly schemaVersion: 1
  readonly transitionCount: number
  readonly transitionLastAuditSequence: number
  readonly transitionLastId: string
}

export interface SagaHistoryEffectProof {
  readonly effectCount: number
  readonly effectFoldChecksum: string
  readonly effectLastId: string
  readonly operationId: string
  readonly saga: SagaRecord
  readonly sagaDescriptorChecksum: string
  readonly sagaId: string
  readonly sagaInputChecksum: string
  readonly sagaRecordChecksum: string
  readonly sagaStateVersion: number
  readonly sagaStatus: SagaRecord["status"]
  readonly schemaVersion: 1
}

export interface SagaHistoryPlanProof {
  readonly descriptorChecksum: string
  readonly descriptorId: string
  readonly descriptorVersion: number
  readonly operationId: string
  readonly operationPlanChecksum: string
  readonly operationStepCount: number
  readonly operationType: string
  readonly sagaId: string
  readonly sagaInputChecksum: string
  readonly sagaStepCount: number
  readonly schemaVersion: 1
}

interface SagaHistoryTransitionReconciliationSummary {
  readonly acquisitionId: string
  readonly auditSequence: number
  readonly createdAtMs: number
  readonly eventType: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseKey: string
  readonly payloadChecksum: string
  readonly stepId: string
  readonly transitionId: string
}

interface SagaHistoryEffectReconciliationSummary {
  readonly acquisitionId: string
  readonly actionAttemptId: string | null
  readonly actionErrorChecksum: string | null
  readonly actionIdempotencyKey: string | null
  readonly actionObservationEvidenceChecksum: string | null
  readonly actionResultChecksum: string | null
  readonly actionState: SagaActionRecord["state"] | null
  readonly createdAtMs: number
  readonly effectId: string
  readonly effectKind: string
  readonly evidenceChecksum: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseKey: string
  readonly phase: SagaActionPhase | null
  readonly sagaStepId: string | null
  readonly stepId: string
  readonly transitionId: string
}

export interface SagaHistoryReconciliationProof {
  readonly actionBeginCount: number
  readonly attemptCount: number
  readonly attemptFoldChecksum: string
  readonly auditTransitionFoldChecksum: string
  readonly coupledTransitionCount: number
  readonly effectAttemptCount: number
  readonly effectCount: number
  readonly effectFoldChecksum: string
  readonly observationAttemptCount: number
  readonly operationId: string
  readonly operationPlanChecksum: string
  readonly sagaId: string
  readonly sagaRecordChecksum: string
  readonly schemaVersion: 1
  readonly transitionCount: number
}

export interface SagaHistoryFinalProof {
  readonly anchor: SagaHistoryAnchor
  readonly reconciliation: SagaHistoryReconciliationProof
  readonly schemaVersion: 1
}

export interface VerifiedSagaHistoryFinalState {
  readonly anchor: SagaHistoryAnchor
  readonly operation: OperationRecord
  readonly saga: SagaRecord
}

const VERIFIED_FINAL_HISTORY = new WeakMap<SagaHistoryFinalProof, VerifiedSagaHistoryFinalState>()

export function loadVerifiedSagaHistoryFinalState(input: unknown): VerifiedSagaHistoryFinalState {
  const state =
    typeof input === "object" && input !== null
      ? VERIFIED_FINAL_HISTORY.get(input as SagaHistoryFinalProof)
      : undefined
  if (state === undefined) {
    return intervention("Saga terminal authority requires a live complete-history proof.")
  }
  return state
}

export interface SagaHistoryAttemptSummary {
  readonly acceptanceChecksum: string
  readonly acceptedAtMs: number
  readonly acquisitionId: string
  readonly actionKey: string
  readonly attemptId: string
  readonly causalAttemptId: string | null
  readonly completedAtMs: number | null
  readonly evidenceChecksum: string | null
  readonly fencingToken: number
  readonly holderId: string
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly leaseKey: string
  readonly operationStepId: string
  readonly outcomeChecksum: string | null
  readonly phase: SagaActionPhase
  readonly protocolVersion: 1 | 2
  readonly purpose: "effect" | "observation"
  readonly sagaStepId: string
  readonly state: SagaAttemptRecord["state"]
  readonly valueChecksum: string | null
}

export interface SagaHistoryAttemptProof {
  readonly attemptCount: number
  readonly attemptFoldChecksum: string
  readonly attemptLastAcceptedAtMs: number | null
  readonly attemptLastId: string | null
  readonly attempts: readonly SagaHistoryAttemptSummary[]
  readonly operationId: string
  readonly sagaId: string
  readonly schemaVersion: 1
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
  reconciliation: SagaHistoryTransitionReconciliationSummary[]
  transitionCount: number
}

interface SagaHistoryEffectFoldState {
  acquisitionId: string | null
  createdAtMs: number | null
  effectCount: number
  effectFoldChecksum: string
  effectLastId: string | null
  fencingToken: number | null
  holderId: string | null
  leaseKey: string | null
  reconciliation: SagaHistoryEffectReconciliationSummary[]
  recordChecksum: string | null
  saga: SagaRecord | undefined
  stateVersion: number
}

interface SagaHistoryAttemptFoldState {
  attemptFoldChecksum: string
  attempts: Map<string, SagaHistoryAttemptSummary>
  bindings: Map<string, { readonly actionKey: string; readonly idempotencyKey: string }>
  cursor: SagaHistoryAttemptCursor
  fences: Map<number, { readonly acquisitionId: string; readonly holderId: string }>
  leaseKey: string | null
  maximumFencingToken: number
  summaries: SagaHistoryAttemptSummary[]
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

function capturedEffectPage(value: unknown): SagaHistoryPage<SagaHistoryEffectRow, number> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga effect fold page could not be captured safely.")
  }
  if (
    !exactRecord<SagaHistoryPage<SagaHistoryEffectRow, number>>(snapshot, [
      "complete",
      "nextCursor",
      "rows",
    ])
  ) {
    return intervention("Saga effect fold page fields are malformed.")
  }
  if (typeof snapshot.complete !== "boolean" || !denseArray(snapshot.rows)) {
    return intervention("Saga effect fold page metadata is malformed.")
  }
  if (
    snapshot.rows.length === 0 ||
    snapshot.rows.length > SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !snapshot.rows.every((row) => exactRecord<SagaHistoryEffectRow>(row, EFFECT_ROW_KEYS))
  ) {
    return intervention("Saga effect fold page row envelope is malformed.")
  }
  const last = snapshot.rows.at(-1) as SagaHistoryEffectRow
  if (snapshot.complete) {
    if (snapshot.nextCursor !== null) {
      return intervention("A complete saga effect fold page retained a cursor.")
    }
  } else if (
    snapshot.rows.length !== SAGA_HISTORY_PAGE_ROW_LIMIT ||
    snapshot.nextCursor !== last.to_state_version
  ) {
    return intervention("An incomplete saga effect fold page has contradictory pagination.")
  }
  return Object.freeze({
    complete: snapshot.complete,
    nextCursor: snapshot.nextCursor,
    rows: Object.freeze(snapshot.rows.map((row) => Object.freeze(row))),
  })
}

function capturedAttemptPage(
  value: unknown,
): SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga-attempt fold page could not be captured safely.")
  }
  if (
    !exactRecord<SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor>>(snapshot, [
      "complete",
      "nextCursor",
      "rows",
    ]) ||
    typeof snapshot.complete !== "boolean" ||
    !denseArray(snapshot.rows) ||
    snapshot.rows.length > SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !snapshot.rows.every((row) => exactRecord<SagaAttemptIdentityRow>(row, ATTEMPT_ROW_KEYS))
  ) {
    return intervention("Saga-attempt fold page fields or rows are malformed.")
  }
  if (snapshot.rows.length === 0) {
    if (!snapshot.complete || snapshot.nextCursor !== null) {
      return intervention("An empty saga-attempt fold page has contradictory pagination.")
    }
    return Object.freeze({ complete: true, nextCursor: null, rows: Object.freeze([]) })
  }
  const last = snapshot.rows.at(-1) as SagaAttemptIdentityRow
  if (snapshot.complete) {
    if (snapshot.nextCursor !== null) {
      return intervention("A complete saga-attempt fold page retained a cursor.")
    }
  } else if (
    snapshot.rows.length !== SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !exactRecord<SagaHistoryAttemptCursor>(snapshot.nextCursor, ["acceptedAtMs", "attemptId"]) ||
    snapshot.nextCursor.acceptedAtMs !== last.accepted_at_ms ||
    snapshot.nextCursor.attemptId !== last.attempt_id
  ) {
    return intervention("An incomplete saga-attempt fold page has contradictory pagination.")
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

function domainFrame(domain: string, parts: readonly string[]): Uint8Array {
  const encoded = [domain, ...parts].map((part) => new TextEncoder().encode(part))
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

async function checkedFoldDigest(
  digest: DigestFunction,
  domain: string,
  parts: readonly string[],
): Promise<string> {
  const checksum = await digest(domainFrame(domain, parts).slice())
  if (typeof checksum !== "string" || !CHECKSUM.test(checksum)) {
    return configuration("Saga history fold digest must return a lowercase SHA-256 checksum.")
  }
  return checksum
}

async function foldChecksum(
  digest: DigestFunction,
  previous: string,
  event: AuditEvent,
): Promise<string> {
  return checkedFoldDigest(digest, AUDIT_TRANSITION_FOLD_DOMAIN, [
    previous,
    event.sequence.toString(10),
    event.eventHash,
  ])
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
  if (TERMINAL_NOT_APPLIED_EVENT_TYPES.has(event.eventType)) {
    const attemptId = after.lastAttemptId
    const evidenceKind = event.eventType.slice("saga.action.terminal_not_applied.".length)
    if (
      after.state !== "succeeded" ||
      !persistedText(attemptId) ||
      operationTransitionIdentity("saga-terminal-not-applied", [
        row.operation_id,
        row.step_id,
        attemptId,
        evidenceKind,
      ]) !== row.transition_id
    ) {
      intervention("A terminal saga classification has a contradictory stable identity.")
    }
    return
  }
  if (event.eventType === "saga.settled") {
    const attemptId = after.lastAttemptId
    if (
      !persistedText(attemptId) ||
      attemptId !== `${SETTLEMENT_ATTEMPT_PREFIX}${event.payloadChecksum}` ||
      !["failed", "intervention_required", "succeeded"].includes(after.state) ||
      operationTransitionIdentity("saga-settled", [row.operation_id, row.step_id, attemptId]) !==
        row.transition_id
    ) {
      intervention("A saga settlement has a contradictory stable identity.")
    }
    return
  }
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
    event.eventType === "saga.action.classified" ||
    TERMINAL_NOT_APPLIED_EVENT_TYPES.has(event.eventType)
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
  if (event.eventType === "saga.settled") {
    if (
      planStep.stepId !== SAGA_SETTLE_OPERATION_STEP_ID ||
      planStep.activation !== "required" ||
      planStep.checkpoint !== "reversible" ||
      planStep.completionRole !== "settlement" ||
      planStep.dependsOn.length !== 0 ||
      planStep.effectProtocol !== "opaque" ||
      planStep.retryClassification !== "never"
    ) {
      intervention("A saga settlement event lacks its canonical operation step.")
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
  if (event.eventType === "saga.settled") {
    const outcome =
      after.state === "succeeded"
        ? {
            observedPostconditionChecksum: planStep.postconditionChecksum,
            resultChecksum: requiredEvidence(after.resultChecksum, "its settlement result"),
            state: "succeeded" as const,
          }
        : after.state === "failed"
          ? {
              errorChecksum: requiredEvidence(after.errorChecksum, "its settlement failure"),
              state: "failed" as const,
            }
          : {
              evidenceChecksum: requiredEvidence(
                after.reconciliationEvidenceChecksum,
                "its settlement intervention evidence",
              ),
              state: "intervention_required" as const,
            }
    return recordAtomicStepOutcome(operation, {
      attemptId: requiredEvidence(after.lastAttemptId, "its atomic attempt identity"),
      idempotencyKey: planStep.idempotencyKey,
      leaseProof: proof,
      observedPreconditionChecksum: planStep.preconditionChecksum,
      outcome,
      stepId: row.step_id,
    })
  }
  if (TERMINAL_NOT_APPLIED_EVENT_TYPES.has(event.eventType)) {
    return recordSagaStepTerminalClassification(operation, {
      outcome: "not_applied",
      receiptOutcomeChecksum: event.payloadChecksum,
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
  else if (event.eventType === "saga.settled") {
    expected = after.resultChecksum ?? after.errorChecksum ?? after.reconciliationEvidenceChecksum
  } else if (TERMINAL_NOT_APPLIED_EVENT_TYPES.has(event.eventType)) {
    expected = after.reconciliationEvidenceChecksum
  } else if (event.eventType === "saga.action.classified") {
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

function transitionReconciliationSummary(
  row: SagaHistoryTransitionRow,
  event: AuditEvent,
): SagaHistoryTransitionReconciliationSummary {
  return Object.freeze({
    acquisitionId: row.acquisition_id,
    auditSequence: row.audit_sequence,
    createdAtMs: row.created_at_ms,
    eventType: event.eventType,
    fencingToken: row.fencing_token,
    holderId: row.holder_id,
    leaseKey: row.lease_key,
    payloadChecksum: event.payloadChecksum,
    stepId: row.step_id,
    transitionId: row.transition_id,
  })
}

type ParsedSagaEffectKind =
  | { readonly kind: "create" }
  | { readonly cause: "cancellation" | "timeout"; readonly kind: "termination" }
  | {
      readonly action:
        | "begin"
        | "failure:definitely_not_applied_retryable"
        | "failure:definitely_not_applied_terminal"
        | "failure:unknown"
        | "observation:applied"
        | "observation:indeterminate"
        | "observation:not_applied"
        | "recovery:not-dispatched"
        | "recovery:unknown"
        | "success"
      readonly kind: "action"
      readonly phase: SagaActionPhase
    }

interface ReplayedSagaEffect {
  readonly decisionTimeMs: number
  readonly saga: SagaRecord
}

function parsedSagaEffectKind(value: string): ParsedSagaEffectKind {
  if (value === "create") return Object.freeze({ kind: "create" })
  if (value === "termination:cancellation") {
    return Object.freeze({ cause: "cancellation", kind: "termination" })
  }
  if (value === "termination:timeout") {
    return Object.freeze({ cause: "timeout", kind: "termination" })
  }
  const match =
    /^action:(forward|compensation):(begin|success|failure:(?:unknown|definitely_not_applied_retryable|definitely_not_applied_terminal)|recovery:(?:unknown|not-dispatched)|observation:(?:applied|not_applied|indeterminate))$/u.exec(
      value,
    )
  if (match === null) return intervention("Saga effect history has an unknown semantic kind.")
  return Object.freeze({
    action: match[2] as Extract<ParsedSagaEffectKind, { kind: "action" }>["action"],
    kind: "action",
    phase: match[1] as SagaActionPhase,
  })
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (typeof value !== "object" || value === null) return value
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) {
    output[key] = canonicalValue((value as Record<string, unknown>)[key])
  }
  return output
}

function sagaRecordJson(record: SagaRecord): string {
  return JSON.stringify(canonicalValue(record))
}

function effectAction(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): SagaActionRecord {
  const action = record.steps[stepId]?.[phase]
  if (action === undefined) return intervention("Saga effect history names an unknown action.")
  return action
}

function requiredActionText(value: string | undefined, label: string): string {
  if (!persistedText(value)) return intervention(`Saga effect history lacks ${label}.`)
  return value
}

function effectActionStepId(row: SagaHistoryEffectRow, phase: SagaActionPhase): string {
  const prefix = `saga:${phase}:`
  if (!row.step_id.startsWith(prefix) || row.step_id.length === prefix.length) {
    return intervention("Saga effect history contradicts its canonical action step.")
  }
  const sagaStepId = row.step_id.slice(prefix.length)
  sagaActionOperationStepId(sagaStepId, phase)
  return sagaStepId
}

function retryDecisionTime(
  before: SagaRecord,
  after: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): number {
  const action = effectAction(after, stepId, phase)
  if (action.state === "retryable_failed") {
    const descriptor = before.descriptor.steps.find(
      (step) => step.stepId === stepId,
    ) as SagaRecord["descriptor"]["steps"][number]
    let delay = descriptor.baseRetryDelayMs
    for (
      let attempt = 1;
      attempt < action.attempts && delay < descriptor.maxRetryDelayMs;
      attempt += 1
    ) {
      delay = Math.min(descriptor.maxRetryDelayMs, delay * 2)
    }
    const serverTimeMs = action.nextAttemptAtMs - delay
    if (!safeInteger(serverTimeMs, 0)) {
      return intervention("Saga effect retry time cannot be reconstructed.")
    }
    return serverTimeMs
  }
  if (before.terminationCause === null && after.terminationCause !== null) {
    return after.terminationRequestedAtMs as number
  }
  return 0
}

function beginDecisionTime(record: SagaRecord, stepId: string, phase: SagaActionPhase): number {
  return effectAction(record, stepId, phase).nextAttemptAtMs
}

function verifyTransitionSuffix(row: SagaHistoryEffectRow, kind: string): string {
  const prefix = `${operationTransitionIdentity(kind, [row.operation_id, row.step_id])}:`
  if (!row.transition_id.startsWith(prefix)) {
    return intervention("Saga effect history has a contradictory transition identity.")
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
    return intervention("Saga effect history has a malformed transition identity suffix.")
  }
  return suffix
}

function verifyEffectTransition(
  row: SagaHistoryEffectRow,
  kind: ParsedSagaEffectKind,
  after: SagaRecord,
): void {
  if (kind.kind === "create" || kind.kind === "termination") {
    verifyTransitionSuffix(row, "succeeded")
    return
  }
  const sagaStepId = effectActionStepId(row, kind.phase)
  const action = effectAction(after, sagaStepId, kind.phase)
  if (kind.action === "recovery:unknown" || kind.action === "recovery:not-dispatched") {
    verifyTransitionSuffix(row, "crash-recovered")
    return
  }
  if (kind.action.startsWith("observation:")) {
    verifyTransitionSuffix(row, "reconciled")
    return
  }
  const attemptId = requiredActionText(action.lastAttemptId, "its action attempt identity")
  const transitionKind =
    kind.action === "begin"
      ? "accepted"
      : kind.action === "success" || kind.action === "failure:definitely_not_applied_terminal"
        ? "succeeded"
        : "failed"
  if (
    row.transition_id !==
    operationTransitionIdentity(transitionKind, [row.operation_id, row.step_id, attemptId])
  ) {
    intervention("Saga effect history has a contradictory exact transition identity.")
  }
}

function initialSagaRecord(after: SagaRecord): SagaRecord {
  const stepInputChecksums = Object.freeze(
    Object.fromEntries(
      after.descriptor.steps.map((step) => [
        step.stepId,
        after.steps[step.stepId]?.inputChecksum as string,
      ]),
    ),
  )
  return createSagaRecord({
    deadlineAtMs: after.deadlineAtMs,
    descriptor: after.descriptor,
    idempotencyKey: after.idempotencyKey,
    inputChecksum: after.inputChecksum,
    sagaId: after.sagaId,
    serverTimeMs: 0,
    stepInputChecksums,
  })
}

function replaySagaEffect(
  before: SagaRecord | undefined,
  after: SagaRecord,
  row: SagaHistoryEffectRow,
  kind: ParsedSagaEffectKind,
): ReplayedSagaEffect {
  if (kind.kind === "create") {
    if (
      before !== undefined ||
      row.step_id !== SAGA_INIT_OPERATION_STEP_ID ||
      row.from_state_version !== null ||
      row.to_state_version !== 0
    ) {
      return intervention("Saga creation effect is not the unique version-zero effect.")
    }
    return Object.freeze({ decisionTimeMs: 0, saga: initialSagaRecord(after) })
  }
  if (before === undefined) {
    return intervention("Saga effect history starts without its creation effect.")
  }
  if (kind.kind === "termination") {
    if (
      row.step_id !== SAGA_TERMINATION_OPERATION_STEP_ID ||
      after.terminationCause !== kind.cause ||
      !safeInteger(after.terminationRequestedAtMs, 0) ||
      (kind.cause === "timeout" && after.terminationRequestedAtMs < before.deadlineAtMs)
    ) {
      return intervention("Saga termination effect contradicts its durable cause or time.")
    }
    return Object.freeze({
      decisionTimeMs: after.terminationRequestedAtMs,
      saga: requestSagaTermination(before, {
        cause: kind.cause,
        serverTimeMs: after.terminationRequestedAtMs,
      }),
    })
  }

  const sagaStepId = effectActionStepId(row, kind.phase)
  const beforeAction = effectAction(before, sagaStepId, kind.phase)
  const afterAction = effectAction(after, sagaStepId, kind.phase)
  if (kind.action === "begin") {
    const decisionTimeMs = beginDecisionTime(before, sagaStepId, kind.phase)
    const decision = beginSagaAction(before, {
      attemptId: requiredActionText(afterAction.lastAttemptId, "its begun attempt identity"),
      idempotencyKey: beforeAction.idempotencyKey,
      phase: kind.phase,
      serverTimeMs: decisionTimeMs,
      stepId: sagaStepId,
    })
    if (decision.disposition !== "execute") {
      return intervention("Saga action begin does not replay as an executable serial action.")
    }
    return Object.freeze({ decisionTimeMs, saga: decision.saga })
  }
  if (kind.action === "success") {
    return Object.freeze({
      decisionTimeMs: 0,
      saga: recordSagaActionSuccess(before, {
        attemptId: requiredActionText(beforeAction.activeAttemptId, "its active attempt identity"),
        phase: kind.phase,
        resultChecksum: requiredActionText(afterAction.resultChecksum, "its action result"),
        serverTimeMs: 0,
        stepId: sagaStepId,
      }),
    })
  }
  if (kind.action.startsWith("failure:")) {
    const outcome =
      kind.action === "failure:unknown"
        ? "unknown"
        : kind.action === "failure:definitely_not_applied_retryable"
          ? "definitely_not_applied_retryable"
          : "definitely_not_applied_terminal"
    const decisionTimeMs = retryDecisionTime(before, after, sagaStepId, kind.phase)
    return Object.freeze({
      decisionTimeMs,
      saga: recordSagaActionFailure(before, {
        attemptId: requiredActionText(beforeAction.activeAttemptId, "its active attempt identity"),
        errorChecksum: requiredActionText(afterAction.errorChecksum, "its action error"),
        outcome,
        phase: kind.phase,
        serverTimeMs: decisionTimeMs,
        stepId: sagaStepId,
      }),
    })
  }
  if (kind.action === "recovery:unknown") {
    return Object.freeze({
      decisionTimeMs: 0,
      saga: markRunningSagaActionUnknown(before, {
        attemptId: requiredActionText(
          beforeAction.activeAttemptId,
          "its recovered attempt identity",
        ),
        errorChecksum: row.evidence_checksum,
        phase: kind.phase,
        stepId: sagaStepId,
      }),
    })
  }
  if (kind.action === "recovery:not-dispatched") {
    const decisionTimeMs = retryDecisionTime(before, after, sagaStepId, kind.phase)
    return Object.freeze({
      decisionTimeMs,
      saga: markSagaActionNotDispatched(before, {
        attemptId: requiredActionText(
          beforeAction.activeAttemptId,
          "its recovered attempt identity",
        ),
        errorChecksum: row.evidence_checksum,
        phase: kind.phase,
        serverTimeMs: decisionTimeMs,
        stepId: sagaStepId,
      }),
    })
  }
  const outcome =
    kind.action === "observation:applied"
      ? "applied"
      : kind.action === "observation:not_applied"
        ? "not_applied"
        : "indeterminate"
  const decisionTimeMs = retryDecisionTime(before, after, sagaStepId, kind.phase)
  return Object.freeze({
    decisionTimeMs,
    saga: recordSagaObservation(before, {
      evidenceChecksum: row.evidence_checksum,
      outcome,
      phase: kind.phase,
      ...(outcome === "applied"
        ? { resultChecksum: requiredActionText(afterAction.resultChecksum, "its observed result") }
        : {}),
      serverTimeMs: decisionTimeMs,
      stepId: sagaStepId,
    }),
  })
}

async function expectedSagaEffectId(
  row: SagaHistoryEffectRow,
  digest: DigestFunction,
): Promise<string> {
  const checksum = await checkedFoldDigest(digest, SAGA_COORDINATOR_ID_DOMAIN, [
    "saga-effect",
    row.transition_id,
    row.resource_id,
    row.effect_kind,
    row.to_state_version.toString(10),
  ])
  return `saga-effect:${checksum}`
}

async function verifyBeginEvidence(
  row: SagaHistoryEffectRow,
  kind: ParsedSagaEffectKind,
  after: SagaRecord,
  digest: DigestFunction,
): Promise<void> {
  if (kind.kind !== "action" || kind.action !== "begin") return
  const sagaStepId = effectActionStepId(row, kind.phase)
  const attemptId = requiredActionText(
    effectAction(after, sagaStepId, kind.phase).lastAttemptId,
    "its begun action attempt",
  )
  const expected = await checkedFoldDigest(digest, SAGA_COORDINATOR_ID_DOMAIN, [
    "begin-evidence",
    row.transition_id,
    row.resource_id,
    sagaStepId,
    kind.phase,
    attemptId,
  ])
  if (row.evidence_checksum !== expected) {
    intervention("Saga action begin effect has contradictory deterministic evidence.")
  }
}

async function verifyRecoveryEvidence(
  row: SagaHistoryEffectRow,
  kind: ParsedSagaEffectKind,
  before: SagaRecord | undefined,
  digest: DigestFunction,
): Promise<void> {
  if (kind.kind !== "action" || kind.action !== "recovery:not-dispatched") return
  if (before === undefined) return intervention("Saga recovery effect has no predecessor.")
  const sagaStepId = effectActionStepId(row, kind.phase)
  const attemptId = requiredActionText(
    effectAction(before, sagaStepId, kind.phase).activeAttemptId,
    "its recovered attempt identity",
  )
  const recoveryId = verifyTransitionSuffix(row, "crash-recovered")
  const expected = await checkedFoldDigest(digest, SAGA_COORDINATOR_ID_DOMAIN, [
    "recovery-not-dispatched",
    row.operation_id,
    row.step_id,
    attemptId,
    recoveryId,
    row.fencing_token.toString(10),
  ])
  if (row.evidence_checksum !== expected) {
    return intervention("Saga not-dispatched recovery has contradictory deterministic evidence.")
  }
}

function effectReconciliationSummary(
  row: SagaHistoryEffectRow,
  kind: ParsedSagaEffectKind,
  after: SagaRecord,
): SagaHistoryEffectReconciliationSummary {
  if (kind.kind !== "action") {
    return Object.freeze({
      acquisitionId: row.acquisition_id,
      actionAttemptId: null,
      actionErrorChecksum: null,
      actionIdempotencyKey: null,
      actionObservationEvidenceChecksum: null,
      actionResultChecksum: null,
      actionState: null,
      createdAtMs: row.created_at_ms,
      effectId: row.effect_id,
      effectKind: row.effect_kind,
      evidenceChecksum: row.evidence_checksum,
      fencingToken: row.fencing_token,
      holderId: row.holder_id,
      leaseKey: row.lease_key,
      phase: null,
      sagaStepId: null,
      stepId: row.step_id,
      transitionId: row.transition_id,
    })
  }
  const sagaStepId = effectActionStepId(row, kind.phase)
  const action = effectAction(after, sagaStepId, kind.phase)
  return Object.freeze({
    acquisitionId: row.acquisition_id,
    actionAttemptId: requiredActionText(
      action.lastAttemptId,
      "its reconciliation attempt identity",
    ),
    actionErrorChecksum: action.errorChecksum ?? null,
    actionIdempotencyKey: action.idempotencyKey,
    actionObservationEvidenceChecksum: action.observationEvidenceChecksum ?? null,
    actionResultChecksum: action.resultChecksum ?? null,
    actionState: action.state,
    createdAtMs: row.created_at_ms,
    effectId: row.effect_id,
    effectKind: row.effect_kind,
    evidenceChecksum: row.evidence_checksum,
    fencingToken: row.fencing_token,
    holderId: row.holder_id,
    leaseKey: row.lease_key,
    phase: kind.phase,
    sagaStepId,
    stepId: row.step_id,
    transitionId: row.transition_id,
  })
}

async function nextEffectFoldChecksum(
  digest: DigestFunction,
  previous: string,
  row: SagaHistoryEffectRow,
): Promise<string> {
  return checkedFoldDigest(digest, EFFECT_FOLD_DOMAIN, [
    previous,
    row.effect_id,
    row.transition_id,
    row.effect_kind,
    row.from_state_version === null ? "null" : row.from_state_version.toString(10),
    row.to_state_version.toString(10),
    row.evidence_checksum,
    row.record_checksum,
    row.lease_key,
    row.holder_id,
    row.acquisition_id,
    row.fencing_token.toString(10),
    row.created_at_ms.toString(10),
  ])
}

function attemptIdentityJson(identity: SagaAttemptIdentity): string {
  return JSON.stringify([
    identity.acceptanceChecksum,
    identity.acceptedAtMs,
    identity.acquisitionId,
    identity.actionKey,
    identity.attemptId,
    identity.causalAttemptId,
    identity.fencingToken,
    identity.holderId,
    identity.idempotencyKey,
    identity.inputChecksum,
    identity.inputJson,
    identity.leaseKey,
    identity.operationId,
    identity.operationStepId,
    identity.phase,
    identity.protocolVersion,
    identity.purpose,
    identity.sagaId,
    identity.sagaStepId,
  ])
}

function attemptSummary(record: SagaAttemptRecord): SagaHistoryAttemptSummary {
  const terminal = record.state !== "accepted"
  return Object.freeze({
    acceptanceChecksum: record.acceptanceChecksum,
    acceptedAtMs: record.acceptedAtMs,
    acquisitionId: record.acquisitionId,
    actionKey: record.actionKey,
    attemptId: record.attemptId,
    causalAttemptId: record.causalAttemptId,
    completedAtMs: terminal ? record.completedAtMs : null,
    evidenceChecksum: terminal ? record.evidenceChecksum : null,
    fencingToken: record.fencingToken,
    holderId: record.holderId,
    idempotencyKey: record.idempotencyKey,
    inputChecksum: record.inputChecksum,
    leaseKey: record.leaseKey,
    operationStepId: record.operationStepId,
    outcomeChecksum: terminal ? record.outcomeChecksum : null,
    phase: record.phase,
    protocolVersion: record.protocolVersion,
    purpose: record.purpose,
    sagaStepId: record.sagaStepId,
    state: record.state,
    valueChecksum: terminal
      ? record.state === "confirmed"
        ? record.outputChecksum
        : record.errorChecksum
      : null,
  })
}

function verifyAttemptCausality(attempts: ReadonlyMap<string, SagaHistoryAttemptSummary>): void {
  for (const attempt of attempts.values()) {
    if (attempt.purpose === "observation") {
      const cause = attempts.get(attempt.causalAttemptId as string)
      if (
        cause === undefined ||
        cause.acceptedAtMs > attempt.acceptedAtMs ||
        cause.sagaStepId !== attempt.sagaStepId ||
        cause.operationStepId !== attempt.operationStepId ||
        cause.phase !== attempt.phase ||
        cause.purpose !== "effect" ||
        (cause.state !== "accepted" && cause.state !== "unknown") ||
        (cause.completedAtMs !== null && cause.completedAtMs > attempt.acceptedAtMs) ||
        attempt.fencingToken <= cause.fencingToken ||
        attempt.idempotencyKey !== `${cause.idempotencyKey}:observation`
      ) {
        intervention("Saga observation attempt lacks its exact causal effect receipt.")
      }
      continue
    }
    if (attempt.phase === "compensation") {
      const cause = attempts.get(attempt.causalAttemptId as string)
      if (
        cause === undefined ||
        cause.acceptedAtMs > attempt.acceptedAtMs ||
        cause.sagaStepId !== attempt.sagaStepId ||
        cause.operationStepId !== `saga:forward:${attempt.sagaStepId}` ||
        cause.phase !== "forward" ||
        cause.purpose !== "effect" ||
        cause.state !== "confirmed" ||
        (cause.completedAtMs as number) > attempt.acceptedAtMs ||
        cause.valueChecksum === null
      ) {
        intervention("Saga compensation attempt lacks its confirmed forward cause.")
      }
    }
  }
}

async function nextAttemptFoldChecksum(
  digest: DigestFunction,
  previous: string,
  attempt: SagaHistoryAttemptSummary,
): Promise<string> {
  return checkedFoldDigest(digest, ATTEMPT_FOLD_DOMAIN, [
    previous,
    attempt.attemptId,
    attempt.causalAttemptId ?? "",
    attempt.acceptanceChecksum,
    attempt.acceptedAtMs.toString(10),
    attempt.completedAtMs === null ? "" : attempt.completedAtMs.toString(10),
    attempt.operationStepId,
    attempt.sagaStepId,
    attempt.phase,
    attempt.purpose,
    attempt.actionKey,
    attempt.idempotencyKey,
    attempt.inputChecksum,
    attempt.leaseKey,
    attempt.holderId,
    attempt.acquisitionId,
    attempt.fencingToken.toString(10),
    attempt.protocolVersion.toString(10),
    attempt.state,
    attempt.evidenceChecksum ?? "",
    attempt.valueChecksum ?? "",
    attempt.outcomeChecksum ?? "",
  ])
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
      reconciliation: [],
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
    state.reconciliation.push(transitionReconciliationSummary(row, event))
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
        reconciliation: [...this.#state.reconciliation],
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

  reconciliationHistory(): readonly SagaHistoryTransitionReconciliationSummary[] {
    if (!this.#complete) {
      return resume("Saga operation-transition history requires more verified pages.")
    }
    return Object.freeze([...this.#state.reconciliation])
  }

  proof(): SagaHistoryTransitionProof {
    if (!this.#complete) {
      return resume("Saga operation-transition history requires more verified pages.")
    }
    return Object.freeze({
      auditHeadEventHash: this.#auditProof.auditHeadEventHash,
      auditHeadSequence: this.#auditProof.auditHeadSequence,
      auditTransitionFoldChecksum: this.#state.auditTransitionFoldChecksum,
      environmentId: this.#auditProof.environmentId,
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

/**
 * Reconstructs the exact saga projection from its dense operation-effect chain. The fold verifies
 * canonical records, checksums, deterministic identities, lease progression, and every pure saga
 * state transition. It remains read-only and does not reconcile the operation or attempt streams.
 */
export class SagaHistoryEffectFolder {
  readonly #anchor: SagaHistoryAnchor
  readonly #digest: DigestFunction
  #appending = false
  #complete = false
  #state: SagaHistoryEffectFoldState = {
    acquisitionId: null,
    createdAtMs: null,
    effectCount: 0,
    effectFoldChecksum: EMPTY_FOLD_CHECKSUM,
    effectLastId: null,
    fencingToken: null,
    holderId: null,
    leaseKey: null,
    reconciliation: [],
    recordChecksum: null,
    saga: undefined,
    stateVersion: -1,
  }

  constructor(inputAnchor: SagaHistoryAnchor, digest: DigestFunction) {
    if (typeof digest !== "function") configuration("A saga effect fold digest is required.")
    this.#anchor = loadSagaHistoryAnchor(inputAnchor)
    this.#digest = digest
  }

  async #foldRow(row: SagaHistoryEffectRow, state: SagaHistoryEffectFoldState): Promise<void> {
    const expectedVersion = state.stateVersion + 1
    if (
      row.operation_id !== this.#anchor.operationId ||
      row.resource_kind !== "saga" ||
      row.resource_id !== this.#anchor.sagaId ||
      row.to_state_version !== expectedVersion ||
      row.to_state_version > this.#anchor.sagaStateVersion ||
      row.from_state_version !== (expectedVersion === 0 ? null : state.stateVersion) ||
      state.effectCount >= this.#anchor.sagaEffectCount ||
      ![
        row.effect_id,
        row.transition_id,
        row.step_id,
        row.effect_kind,
        row.evidence_checksum,
        row.record_checksum,
        row.lease_key,
        row.holder_id,
        row.acquisition_id,
        row.record_json,
      ].every(persistedText) ||
      !safeInteger(row.fencing_token, 1) ||
      !safeInteger(row.created_at_ms, 0) ||
      (state.createdAtMs !== null && row.created_at_ms < state.createdAtMs) ||
      (state.leaseKey !== null && row.lease_key !== state.leaseKey) ||
      (state.fencingToken !== null && row.fencing_token < state.fencingToken) ||
      (state.fencingToken === row.fencing_token &&
        (state.holderId !== row.holder_id || state.acquisitionId !== row.acquisition_id))
    ) {
      return intervention("Saga effect fold history is malformed, unordered, or unfenced.")
    }

    const kind = parsedSagaEffectKind(row.effect_kind)
    if (
      kind.kind === "action" &&
      kind.action.startsWith("recovery:") &&
      state.fencingToken !== null &&
      row.fencing_token <= state.fencingToken
    ) {
      return intervention("Saga recovery effect lacks a strictly newer lease fence.")
    }
    const after = await loadSagaRecord(
      parsedJson(row.record_json, "Saga effect successor record"),
      this.#digest,
    )
    if (
      sagaRecordJson(after) !== row.record_json ||
      after.sagaId !== this.#anchor.sagaId ||
      after.inputChecksum !== this.#anchor.sagaInputChecksum ||
      after.descriptor.descriptorChecksum !== this.#anchor.sagaDescriptorChecksum ||
      after.stateVersion !== row.to_state_version
    ) {
      return intervention("Saga effect successor record is noncanonical or contradictory.")
    }
    const recordChecksum = await checkedFoldDigest(this.#digest, SAGA_RECORD_DOMAIN, [
      row.record_json,
    ])
    if (row.record_checksum !== recordChecksum) {
      return intervention("Saga effect successor record checksum is contradictory.")
    }
    if (row.effect_id !== (await expectedSagaEffectId(row, this.#digest))) {
      return intervention("Saga effect history has a contradictory deterministic identity.")
    }
    await verifyRecoveryEvidence(row, kind, state.saga, this.#digest)
    verifyEffectTransition(row, kind, after)
    await verifyBeginEvidence(row, kind, after, this.#digest)

    let replayed: ReplayedSagaEffect
    try {
      replayed = replaySagaEffect(state.saga, after, row, kind)
    } catch {
      return intervention("Saga effect history violates its exact core state transition.")
    }
    if (
      sagaRecordJson(replayed.saga) !== row.record_json ||
      replayed.decisionTimeMs > row.created_at_ms
    ) {
      return intervention("Saga effect history violates its exact core state transition.")
    }

    state.acquisitionId = row.acquisition_id
    state.createdAtMs = row.created_at_ms
    state.effectCount += 1
    state.effectFoldChecksum = await nextEffectFoldChecksum(
      this.#digest,
      state.effectFoldChecksum,
      row,
    )
    state.effectLastId = row.effect_id
    state.fencingToken = row.fencing_token
    state.holderId = row.holder_id
    state.leaseKey = row.lease_key
    state.reconciliation.push(effectReconciliationSummary(row, kind, after))
    state.recordChecksum = recordChecksum
    state.saga = after
    state.stateVersion = row.to_state_version
  }

  async append(inputPage: SagaHistoryPage<SagaHistoryEffectRow, number>): Promise<void> {
    if (this.#complete) configuration("Saga effect history is already completely folded.")
    if (this.#appending) configuration("A saga effect history page is already being folded.")
    this.#appending = true
    try {
      const page = capturedEffectPage(inputPage)
      const state: SagaHistoryEffectFoldState = {
        ...this.#state,
        reconciliation: [...this.#state.reconciliation],
      }
      for (const row of page.rows) await this.#foldRow(row, state)
      const atHead =
        state.stateVersion === this.#anchor.sagaStateVersion &&
        state.effectLastId === this.#anchor.sagaLastEffectId
      if (!page.complete) {
        if (atHead || state.effectCount >= this.#anchor.sagaEffectCount) {
          return intervention("Saga effect fold page failed to close at its anchor.")
        }
      } else if (
        !atHead ||
        state.effectCount !== this.#anchor.sagaEffectCount ||
        state.recordChecksum !== this.#anchor.sagaRecordChecksum ||
        state.saga === undefined ||
        state.saga.status !== this.#anchor.sagaStatus
      ) {
        return intervention("Saga effect fold does not reconcile with its terminal anchor.")
      } else {
        this.#complete = true
      }
      this.#state = state
    } finally {
      this.#appending = false
    }
  }

  async planProof(
    inputPlan: OperationPlan,
    inputDescriptor: SagaDescriptor,
  ): Promise<SagaHistoryPlanProof> {
    if (!this.#complete) return resume("Saga history requires a complete effect fold first.")
    const plan = inputPlan
    let descriptorSnapshot: unknown
    try {
      descriptorSnapshot = structuredClone(inputDescriptor)
    } catch {
      return intervention("The persisted saga descriptor could not be captured safely.")
    }
    const descriptor = await loadSagaDescriptor(descriptorSnapshot, this.#digest)
    const saga = this.#state.saga as SagaRecord
    if (
      descriptor.descriptorChecksum !== this.#anchor.sagaDescriptorChecksum ||
      JSON.stringify(descriptor) !== JSON.stringify(saga.descriptor) ||
      plan.operationId !== this.#anchor.operationId ||
      plan.inputChecksum !== this.#anchor.operationInputChecksum ||
      plan.planChecksum !== this.#anchor.operationPlanChecksum
    ) {
      return intervention("The saga descriptor or operation plan contradicts its history proof.")
    }
    await verifySagaOperationPlan(plan, saga, this.#digest)
    return Object.freeze({
      descriptorChecksum: descriptor.descriptorChecksum,
      descriptorId: descriptor.descriptorId,
      descriptorVersion: descriptor.version,
      operationId: plan.operationId,
      operationPlanChecksum: plan.planChecksum,
      operationStepCount: plan.steps.length,
      operationType: plan.operationType,
      sagaId: saga.sagaId,
      sagaInputChecksum: saga.inputChecksum,
      sagaStepCount: descriptor.steps.length,
      schemaVersion: 1,
    })
  }

  reconciliationHistory(): readonly SagaHistoryEffectReconciliationSummary[] {
    if (!this.#complete) return resume("Saga effect history requires more verified pages.")
    return Object.freeze([...this.#state.reconciliation])
  }

  proof(): SagaHistoryEffectProof {
    if (
      !this.#complete ||
      this.#state.effectLastId === null ||
      this.#state.recordChecksum === null ||
      this.#state.saga === undefined
    ) {
      return resume("Saga effect history requires more verified pages.")
    }
    return Object.freeze({
      effectCount: this.#state.effectCount,
      effectFoldChecksum: this.#state.effectFoldChecksum,
      effectLastId: this.#state.effectLastId,
      operationId: this.#anchor.operationId,
      saga: this.#state.saga,
      sagaDescriptorChecksum: this.#anchor.sagaDescriptorChecksum,
      sagaId: this.#anchor.sagaId,
      sagaInputChecksum: this.#anchor.sagaInputChecksum,
      sagaRecordChecksum: this.#state.recordChecksum,
      sagaStateVersion: this.#state.stateVersion,
      sagaStatus: this.#state.saga.status,
      schemaVersion: 1,
    })
  }
}

/**
 * Verifies paged saga-attempt identities and point-loaded inline or companion-row outcomes while
 * retaining only bounded semantic summaries. Causal receipts are reconciled after the full page
 * stream because D1's millisecond keyset can order same-time cause and dependent IDs either way.
 */
export class SagaHistoryAttemptFolder {
  readonly #anchor: SagaHistoryAnchor
  readonly #digest: DigestFunction
  readonly #reader: D1SagaHistoryReader
  #appending = false
  #complete = false
  #state: SagaHistoryAttemptFoldState = {
    attemptFoldChecksum: EMPTY_FOLD_CHECKSUM,
    attempts: new Map(),
    bindings: new Map(),
    cursor: Object.freeze({ acceptedAtMs: -1, attemptId: "" }),
    fences: new Map(),
    leaseKey: null,
    maximumFencingToken: 0,
    summaries: [],
  }

  constructor(inputAnchor: SagaHistoryAnchor, reader: D1SagaHistoryReader, digest: DigestFunction) {
    if (!(reader instanceof D1SagaHistoryReader)) {
      configuration("A saga-attempt history reader is required.")
    }
    if (typeof digest !== "function") configuration("A saga-attempt fold digest is required.")
    this.#anchor = loadSagaHistoryAnchor(inputAnchor)
    this.#reader = reader
    this.#digest = digest
  }

  async #foldRow(row: SagaAttemptIdentityRow, state: SagaHistoryAttemptFoldState): Promise<void> {
    const lastAcceptedAtMs = this.#anchor.sagaAttemptLastAcceptedAtMs
    const lastAttemptId = this.#anchor.sagaAttemptLastId
    if (
      this.#anchor.sagaAttemptCount === 0 ||
      !safeInteger(row.accepted_at_ms, 0) ||
      !persistedText(row.attempt_id) ||
      !pairAfter(
        row.accepted_at_ms,
        row.attempt_id,
        state.cursor.acceptedAtMs,
        state.cursor.attemptId,
      ) ||
      row.accepted_at_ms > (lastAcceptedAtMs as number) ||
      (row.accepted_at_ms === lastAcceptedAtMs &&
        sqliteBinaryTextCompare(row.attempt_id, lastAttemptId as string) > 0) ||
      state.summaries.length >= this.#anchor.sagaAttemptCount
    ) {
      return intervention("Saga-attempt fold history is malformed or unordered.")
    }
    const identity = await loadSagaAttemptIdentityRow(row, this.#digest)
    const record = await this.#reader.attemptRecord(this.#anchor, row, this.#digest)
    if (
      attemptIdentityJson(record) !== attemptIdentityJson(identity) ||
      identity.sagaId !== this.#anchor.sagaId ||
      identity.operationId !== this.#anchor.operationId ||
      identity.operationStepId !== `saga:${identity.phase}:${identity.sagaStepId}` ||
      (state.leaseKey !== null && identity.leaseKey !== state.leaseKey) ||
      (identity.acceptedAtMs > state.cursor.acceptedAtMs &&
        identity.fencingToken < state.maximumFencingToken) ||
      state.attempts.has(identity.attemptId)
    ) {
      return intervention("Saga-attempt fold identity contradicts its anchored saga action.")
    }
    const fence = state.fences.get(identity.fencingToken)
    if (
      fence !== undefined &&
      (fence.holderId !== identity.holderId || fence.acquisitionId !== identity.acquisitionId)
    ) {
      return intervention("Saga-attempt fold reused a fence under another lease acquisition.")
    }
    const bindingKey = JSON.stringify([identity.sagaStepId, identity.phase, identity.purpose])
    const binding = state.bindings.get(bindingKey)
    if (
      binding !== undefined &&
      (binding.actionKey !== identity.actionKey ||
        binding.idempotencyKey !== identity.idempotencyKey)
    ) {
      return intervention("Saga-attempt fold action binding changed between attempts.")
    }

    const summary = attemptSummary(record)
    state.attemptFoldChecksum = await nextAttemptFoldChecksum(
      this.#digest,
      state.attemptFoldChecksum,
      summary,
    )
    state.attempts.set(summary.attemptId, summary)
    state.bindings.set(
      bindingKey,
      binding ??
        Object.freeze({ actionKey: summary.actionKey, idempotencyKey: summary.idempotencyKey }),
    )
    state.cursor = Object.freeze({
      acceptedAtMs: summary.acceptedAtMs,
      attemptId: summary.attemptId,
    })
    state.fences.set(
      summary.fencingToken,
      fence ?? Object.freeze({ acquisitionId: summary.acquisitionId, holderId: summary.holderId }),
    )
    state.leaseKey = summary.leaseKey
    state.maximumFencingToken = Math.max(state.maximumFencingToken, summary.fencingToken)
    state.summaries.push(summary)
  }

  async append(
    inputPage: SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor>,
  ): Promise<void> {
    if (this.#complete) configuration("Saga-attempt history is already completely folded.")
    if (this.#appending) configuration("A saga-attempt history page is already being folded.")
    this.#appending = true
    try {
      const page = capturedAttemptPage(inputPage)
      const state: SagaHistoryAttemptFoldState = {
        attemptFoldChecksum: this.#state.attemptFoldChecksum,
        attempts: new Map(this.#state.attempts),
        bindings: new Map(this.#state.bindings),
        cursor: this.#state.cursor,
        fences: new Map(this.#state.fences),
        leaseKey: this.#state.leaseKey,
        maximumFencingToken: this.#state.maximumFencingToken,
        summaries: [...this.#state.summaries],
      }
      for (const row of page.rows) await this.#foldRow(row, state)
      const atHead =
        this.#anchor.sagaAttemptCount === 0
          ? state.cursor.acceptedAtMs === -1 && state.cursor.attemptId === ""
          : state.cursor.acceptedAtMs === this.#anchor.sagaAttemptLastAcceptedAtMs &&
            state.cursor.attemptId === this.#anchor.sagaAttemptLastId
      if (!page.complete) {
        if (atHead || state.summaries.length >= this.#anchor.sagaAttemptCount) {
          return intervention("Saga-attempt fold page failed to close at its anchor.")
        }
      } else if (!atHead || state.summaries.length !== this.#anchor.sagaAttemptCount) {
        return intervention("Saga-attempt fold does not reconcile with its terminal anchor.")
      } else {
        verifyAttemptCausality(state.attempts)
        this.#complete = true
      }
      this.#state = state
    } finally {
      this.#appending = false
    }
  }

  reconciliationHistory(): readonly SagaHistoryAttemptSummary[] {
    if (!this.#complete) return resume("Saga-attempt history requires more verified pages.")
    return Object.freeze([...this.#state.summaries])
  }

  proof(): SagaHistoryAttemptProof {
    if (!this.#complete) return resume("Saga-attempt history requires more verified pages.")
    return Object.freeze({
      attemptCount: this.#state.summaries.length,
      attemptFoldChecksum: this.#state.attemptFoldChecksum,
      attemptLastAcceptedAtMs:
        this.#state.summaries.length === 0 ? null : this.#state.cursor.acceptedAtMs,
      attemptLastId: this.#state.summaries.length === 0 ? null : this.#state.cursor.attemptId,
      attempts: Object.freeze([...this.#state.summaries]),
      operationId: this.#anchor.operationId,
      sagaId: this.#anchor.sagaId,
      schemaVersion: 1,
    })
  }
}

const COUPLED_SAGA_EVENT_TYPES = new Set([
  "saga.action.classified",
  "saga.action.observed",
  "saga.action.recovered",
  "saga.action.started",
  "saga.initialized",
  "saga.termination.requested",
])

function effectEventType(effectKind: ParsedSagaEffectKind): string {
  if (effectKind.kind === "create") return "saga.initialized"
  if (effectKind.kind === "termination") return "saga.termination.requested"
  if (effectKind.action === "begin") return "saga.action.started"
  if (effectKind.action.startsWith("recovery:")) return "saga.action.recovered"
  if (effectKind.action.startsWith("observation:")) return "saga.action.observed"
  return "saga.action.classified"
}

function attemptActionBinding(
  saga: SagaRecord,
  attempt: SagaHistoryAttemptSummary,
): { readonly actionKey: string; readonly idempotencyKey: string } {
  const descriptorStep = saga.descriptor.steps.find(
    (candidate) => candidate.stepId === attempt.sagaStepId,
  )
  const action = saga.steps[attempt.sagaStepId]?.[attempt.phase]
  if (descriptorStep === undefined || action === undefined) {
    return intervention("Saga-attempt history names an action outside the verified descriptor.")
  }
  const reference =
    attempt.purpose === "observation"
      ? attempt.phase === "forward"
        ? descriptorStep.forwardObservation
        : descriptorStep.compensationObservation
      : attempt.phase === "forward"
        ? descriptorStep.forwardAction
        : descriptorStep.compensationAction
  if (reference === null) {
    return intervention("Saga-attempt history names an unavailable descriptor action.")
  }
  return Object.freeze({
    actionKey: sagaActionKey(reference),
    idempotencyKey:
      attempt.purpose === "observation"
        ? `${action.idempotencyKey}:observation`
        : action.idempotencyKey,
  })
}

function sameAttemptConsumer(
  effect: SagaHistoryEffectReconciliationSummary,
  attempt: SagaHistoryAttemptSummary,
  requireNewerFence: boolean,
): boolean {
  return (
    effect.leaseKey === attempt.leaseKey &&
    (requireNewerFence
      ? effect.fencingToken > attempt.fencingToken
      : effect.fencingToken >= attempt.fencingToken) &&
    (effect.fencingToken !== attempt.fencingToken ||
      (effect.holderId === attempt.holderId && effect.acquisitionId === attempt.acquisitionId)) &&
    effect.createdAtMs >= attempt.acceptedAtMs &&
    (attempt.completedAtMs === null || effect.createdAtMs >= attempt.completedAtMs)
  )
}

function directAttemptEffectKind(
  attempt: SagaHistoryAttemptSummary,
  effect: SagaHistoryEffectReconciliationSummary,
): boolean {
  const prefix = `action:${attempt.phase}:`
  if (attempt.state === "confirmed") return effect.effectKind === `${prefix}success`
  if (attempt.state === "unknown") return effect.effectKind === `${prefix}failure:unknown`
  if (attempt.state === "failed") {
    return effect.effectKind === `${prefix}failure:definitely_not_applied_terminal`
  }
  if (attempt.state === "not_applied") {
    return (
      effect.effectKind === `${prefix}failure:definitely_not_applied_retryable` ||
      effect.effectKind === `${prefix}failure:definitely_not_applied_terminal`
    )
  }
  return false
}

async function reconcileSagaHistoryWithAccess(
  transitionFolder: SagaHistoryTransitionFolder,
  effectFolder: SagaHistoryEffectFolder,
  attemptFolder: SagaHistoryAttemptFolder,
  plan: OperationPlan,
  descriptor: SagaDescriptor,
  trustedAccess: boolean,
): Promise<SagaHistoryReconciliationProof> {
  if (
    !(transitionFolder instanceof SagaHistoryTransitionFolder) ||
    !(effectFolder instanceof SagaHistoryEffectFolder) ||
    !(attemptFolder instanceof SagaHistoryAttemptFolder)
  ) {
    return configuration("Completed saga-history folders are required for reconciliation.")
  }
  const planProof = trustedAccess
    ? await SagaHistoryEffectFolder.prototype.planProof.call(effectFolder, plan, descriptor)
    : await effectFolder.planProof(plan, descriptor)
  const transitionProof = trustedAccess
    ? SagaHistoryTransitionFolder.prototype.proof.call(transitionFolder)
    : transitionFolder.proof()
  const effectProof = trustedAccess
    ? SagaHistoryEffectFolder.prototype.proof.call(effectFolder)
    : effectFolder.proof()
  const attemptProof = trustedAccess
    ? SagaHistoryAttemptFolder.prototype.proof.call(attemptFolder)
    : attemptFolder.proof()
  if (
    transitionProof.operationId !== effectProof.operationId ||
    transitionProof.operationId !== attemptProof.operationId ||
    transitionProof.operationPlanChecksum !== planProof.operationPlanChecksum ||
    effectProof.sagaId !== attemptProof.sagaId ||
    effectProof.sagaId !== planProof.sagaId
  ) {
    return intervention("Saga-history component proofs belong to different operations or sagas.")
  }

  const transitions = trustedAccess
    ? SagaHistoryTransitionFolder.prototype.reconciliationHistory.call(transitionFolder)
    : transitionFolder.reconciliationHistory()
  const effects = trustedAccess
    ? SagaHistoryEffectFolder.prototype.reconciliationHistory.call(effectFolder)
    : effectFolder.reconciliationHistory()
  const attempts = trustedAccess
    ? SagaHistoryAttemptFolder.prototype.reconciliationHistory.call(attemptFolder)
    : attemptFolder.reconciliationHistory()
  const transitionsById = new Map(
    transitions.map((transition) => [transition.transitionId, transition] as const),
  )
  const effectsByTransition = new Map(
    effects.map((effect) => [effect.transitionId, effect] as const),
  )
  if (transitionsById.size !== transitions.length || effectsByTransition.size !== effects.length) {
    return intervention("Saga history contains duplicate cross-stream transition identities.")
  }
  if (
    transitions.filter((transition) => COUPLED_SAGA_EVENT_TYPES.has(transition.eventType))
      .length !== effects.length
  ) {
    return intervention("Saga operation and effect histories have different coupled cardinality.")
  }

  for (const transition of transitions) {
    const effect = effectsByTransition.get(transition.transitionId)
    if (COUPLED_SAGA_EVENT_TYPES.has(transition.eventType) !== (effect !== undefined)) {
      return intervention("Saga operation and effect histories do not form an exact coupled set.")
    }
    if (effect === undefined) continue
    const kind = parsedSagaEffectKind(effect.effectKind)
    if (
      effectEventType(kind) !== transition.eventType ||
      effect.stepId !== transition.stepId ||
      effect.evidenceChecksum !== transition.payloadChecksum ||
      effect.leaseKey !== transition.leaseKey ||
      effect.holderId !== transition.holderId ||
      effect.acquisitionId !== transition.acquisitionId ||
      effect.fencingToken !== transition.fencingToken
    ) {
      return intervention("A coupled saga transition and effect carry contradictory evidence.")
    }
  }

  const attemptsById = new Map(attempts.map((attempt) => [attempt.attemptId, attempt] as const))
  if (attemptsById.size !== attempts.length) {
    return intervention("Saga-attempt reconciliation contains duplicate identities.")
  }
  const actionEffects = effects.filter((effect) => effect.phase !== null)
  const beginEffects = actionEffects.filter((effect) => effect.effectKind.endsWith(":begin"))
  const beginByAttempt = new Map<string, SagaHistoryEffectReconciliationSummary>()
  for (const effect of beginEffects) {
    if (effect.actionAttemptId === null || beginByAttempt.has(effect.actionAttemptId)) {
      return intervention("Saga action begin history has an ambiguous attempt identity.")
    }
    beginByAttempt.set(effect.actionAttemptId, effect)
  }
  const consumed = new Set<string>()
  let effectAttemptCount = 0
  let observationAttemptCount = 0

  for (const attempt of attempts) {
    const binding = attemptActionBinding(effectProof.saga, attempt)
    const sagaStep = effectProof.saga.steps[attempt.sagaStepId]
    if (
      attempt.actionKey !== binding.actionKey ||
      attempt.idempotencyKey !== binding.idempotencyKey ||
      (attempt.purpose === "effect" &&
        attempt.phase === "forward" &&
        attempt.inputChecksum !== sagaStep?.inputChecksum)
    ) {
      return intervention("Saga-attempt history contradicts its descriptor-bound action.")
    }

    if (attempt.purpose === "effect") {
      effectAttemptCount += 1
      const begin = beginByAttempt.get(attempt.attemptId)
      if (
        begin === undefined ||
        begin.sagaStepId !== attempt.sagaStepId ||
        begin.phase !== attempt.phase ||
        begin.actionIdempotencyKey !== attempt.idempotencyKey ||
        begin.leaseKey !== attempt.leaseKey ||
        begin.holderId !== attempt.holderId ||
        begin.acquisitionId !== attempt.acquisitionId ||
        begin.fencingToken !== attempt.fencingToken ||
        attempt.acceptedAtMs < begin.createdAtMs
      ) {
        return intervention("Saga effect attempt lacks its exact coupled action begin.")
      }
      const related = actionEffects.filter(
        (effect) =>
          effect.actionAttemptId === attempt.attemptId &&
          effect.effectId !== begin.effectId &&
          !effect.effectKind.includes(":observation:"),
      )
      if (attempt.state === "accepted") {
        const recovery = related.filter((effect) => effect.effectKind.endsWith(":recovery:unknown"))
        if (
          recovery.length !== 1 ||
          related.length !== 1 ||
          recovery[0]?.evidenceChecksum !== attempt.acceptanceChecksum ||
          recovery[0]?.actionErrorChecksum !== attempt.acceptanceChecksum ||
          !sameAttemptConsumer(recovery[0] as SagaHistoryEffectReconciliationSummary, attempt, true)
        ) {
          return intervention("An accepted saga attempt lacks its exact unknown recovery effect.")
        }
        consumed.add((recovery[0] as SagaHistoryEffectReconciliationSummary).effectId)
        continue
      }
      const terminal = related.filter((effect) => directAttemptEffectKind(attempt, effect))
      const exact = terminal[0]
      if (
        terminal.length !== 1 ||
        related.length !== 1 ||
        exact === undefined ||
        exact.evidenceChecksum !== attempt.outcomeChecksum ||
        !sameAttemptConsumer(exact, attempt, false) ||
        (attempt.state === "confirmed"
          ? exact.actionResultChecksum !== attempt.valueChecksum
          : exact.actionErrorChecksum !== attempt.valueChecksum)
      ) {
        return intervention(
          "A terminal saga effect receipt lacks its exact coupled outcome effect.",
        )
      }
      consumed.add(exact.effectId)
      continue
    }

    observationAttemptCount += 1
    if (attempt.state === "accepted" || beginByAttempt.has(attempt.attemptId)) {
      return intervention("A terminal saga contains an unresolved or dispatched observation.")
    }
    const transitionId = operationTransitionIdentity("reconciled", [
      effectProof.operationId,
      attempt.operationStepId,
      attempt.attemptId,
    ])
    const effect = effectsByTransition.get(transitionId)
    const expectedKind = `action:${attempt.phase}:observation:${
      attempt.state === "confirmed" ? "applied" : attempt.state
    }`
    if (
      effect === undefined ||
      effect.effectKind !== expectedKind ||
      effect.actionAttemptId !== attempt.causalAttemptId ||
      effect.evidenceChecksum !== attempt.outcomeChecksum ||
      effect.actionObservationEvidenceChecksum !== attempt.outcomeChecksum ||
      !sameAttemptConsumer(effect, attempt, false) ||
      (attempt.state === "confirmed" && effect.actionResultChecksum !== attempt.valueChecksum)
    ) {
      return intervention("A saga observation receipt lacks its exact causal coupled effect.")
    }
    consumed.add(effect.effectId)
  }

  for (const begin of beginEffects) {
    if (begin.actionAttemptId === null || attemptsById.has(begin.actionAttemptId)) continue
    const recoveries = actionEffects.filter(
      (effect) =>
        effect.actionAttemptId === begin.actionAttemptId &&
        effect.effectKind.endsWith(":recovery:not-dispatched"),
    )
    const recovery = recoveries[0]
    if (
      recoveries.length !== 1 ||
      recovery === undefined ||
      recovery.leaseKey !== begin.leaseKey ||
      recovery.fencingToken <= begin.fencingToken ||
      recovery.createdAtMs < begin.createdAtMs ||
      recovery.actionErrorChecksum !== recovery.evidenceChecksum
    ) {
      return intervention("A receipt-free saga begin lacks its exact not-dispatched recovery.")
    }
    consumed.add(recovery.effectId)
  }

  const nonBeginActionEffects = actionEffects.filter(
    (effect) => !effect.effectKind.endsWith(":begin"),
  )
  if (
    consumed.size !== nonBeginActionEffects.length ||
    nonBeginActionEffects.some((effect) => !consumed.has(effect.effectId))
  ) {
    return intervention("Saga action effects and attempt receipts are not an exhaustive set.")
  }

  return Object.freeze({
    actionBeginCount: beginEffects.length,
    attemptCount: attemptProof.attemptCount,
    attemptFoldChecksum: attemptProof.attemptFoldChecksum,
    auditTransitionFoldChecksum: transitionProof.auditTransitionFoldChecksum,
    coupledTransitionCount: effects.length,
    effectAttemptCount,
    effectCount: effectProof.effectCount,
    effectFoldChecksum: effectProof.effectFoldChecksum,
    observationAttemptCount,
    operationId: effectProof.operationId,
    operationPlanChecksum: planProof.operationPlanChecksum,
    sagaId: effectProof.sagaId,
    sagaRecordChecksum: effectProof.sagaRecordChecksum,
    schemaVersion: 1,
    transitionCount: transitionProof.transitionCount,
  })
}

/**
 * Reconciles the already verified operation, saga-effect, and attempt streams as exact sets. This
 * remains a read-only proof and deliberately carries no terminal settlement authority.
 */
export function reconcileSagaHistory(
  transitionFolder: SagaHistoryTransitionFolder,
  effectFolder: SagaHistoryEffectFolder,
  attemptFolder: SagaHistoryAttemptFolder,
  plan: OperationPlan,
  descriptor: SagaDescriptor,
): Promise<SagaHistoryReconciliationProof> {
  return reconcileSagaHistoryWithAccess(
    transitionFolder,
    effectFolder,
    attemptFolder,
    plan,
    descriptor,
    false,
  )
}

/**
 * Binds a reconciled history to the exact anchor used by every fold, then performs the final
 * database re-read. This remains evidence only and cannot authorize terminal persistence.
 */
export async function finalizeSagaHistoryProof(
  reader: D1SagaHistoryReader,
  inputAnchor: SagaHistoryAnchor,
  transitionFolder: SagaHistoryTransitionFolder,
  effectFolder: SagaHistoryEffectFolder,
  attemptFolder: SagaHistoryAttemptFolder,
  plan: OperationPlan,
  descriptor: SagaDescriptor,
): Promise<SagaHistoryFinalProof> {
  if (!(reader instanceof D1SagaHistoryReader)) {
    return configuration("A production saga-history reader is required for final reconciliation.")
  }
  const anchor = loadSagaHistoryAnchor(inputAnchor)
  const reconciliation = await reconcileSagaHistoryWithAccess(
    transitionFolder,
    effectFolder,
    attemptFolder,
    plan,
    descriptor,
    true,
  )
  const transition = SagaHistoryTransitionFolder.prototype.proof.call(transitionFolder)
  const effect = SagaHistoryEffectFolder.prototype.proof.call(effectFolder)
  const attempt = SagaHistoryAttemptFolder.prototype.proof.call(attemptFolder)
  const actualBinding = JSON.stringify([
    transition.auditHeadEventHash,
    transition.auditHeadSequence,
    transition.environmentId,
    transition.operationId,
    transition.operation.plan.inputChecksum,
    transition.operationPlanChecksum,
    transition.operationStatus,
    transition.transitionCount,
    transition.transitionLastAuditSequence,
    transition.transitionLastId,
    effect.operationId,
    effect.sagaDescriptorChecksum,
    effect.effectCount,
    effect.effectLastId,
    effect.sagaId,
    effect.sagaInputChecksum,
    effect.sagaRecordChecksum,
    effect.sagaStateVersion,
    effect.sagaStatus,
    attempt.operationId,
    attempt.sagaId,
    attempt.attemptCount,
    attempt.attemptLastAcceptedAtMs,
    attempt.attemptLastId,
    reconciliation.operationId,
    reconciliation.operationPlanChecksum,
    reconciliation.sagaId,
    reconciliation.transitionCount,
    reconciliation.effectCount,
    reconciliation.attemptCount,
    reconciliation.sagaRecordChecksum,
  ])
  const expectedBinding = JSON.stringify([
    anchor.auditHeadEventHash,
    anchor.auditHeadSequence,
    anchor.environmentId,
    anchor.operationId,
    anchor.operationInputChecksum,
    anchor.operationPlanChecksum,
    anchor.operationStatus,
    anchor.operationTransitionCount,
    anchor.operationTransitionLastAuditSequence,
    anchor.operationTransitionLastId,
    anchor.operationId,
    anchor.sagaDescriptorChecksum,
    anchor.sagaEffectCount,
    anchor.sagaLastEffectId,
    anchor.sagaId,
    anchor.sagaInputChecksum,
    anchor.sagaRecordChecksum,
    anchor.sagaStateVersion,
    anchor.sagaStatus,
    anchor.operationId,
    anchor.sagaId,
    anchor.sagaAttemptCount,
    anchor.sagaAttemptLastAcceptedAtMs,
    anchor.sagaAttemptLastId,
    anchor.operationId,
    anchor.operationPlanChecksum,
    anchor.sagaId,
    anchor.operationTransitionCount,
    anchor.sagaEffectCount,
    anchor.sagaAttemptCount,
    anchor.sagaRecordChecksum,
  ])
  if (actualBinding !== expectedBinding) {
    return intervention("The reconciled saga history contradicts its final anchor.")
  }
  await D1SagaHistoryReader.prototype.assertAnchorCurrent.call(reader, anchor)
  const proof: SagaHistoryFinalProof = Object.freeze({
    anchor,
    reconciliation,
    schemaVersion: 1,
  })
  VERIFIED_FINAL_HISTORY.set(
    proof,
    Object.freeze({
      anchor,
      operation: Object.freeze({ plan, steps: transition.operation.steps }),
      saga: effect.saga,
    }),
  )
  return proof
}
