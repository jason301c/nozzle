import {
  type DigestFunction,
  loadOperationPlan,
  NozzleError,
  type OperationPlan,
} from "@nozzle/core"
import type { TransactionalControlDatabase } from "./database.js"
import {
  SAGA_ATTEMPT_IDENTITY_ROW_SELECT,
  type SagaAttemptIdentityRow,
} from "./saga-attempt-codec.js"

export const SAGA_HISTORY_PAGE_ROW_LIMIT = 2
// A transition result can join one transition, audit, and authorization row. At D1's
// documented 2,000,000-byte row ceiling, two returned rows stay below 12 MiB and the
// LIMIT-plus-one fetch stays below 20 MiB without retaining an unbounded page.
export const SAGA_HISTORY_PAGE_MAX_BYTES = 12 * 1024 * 1024
const SAGA_HISTORY_FETCH_ROW_LIMIT = SAGA_HISTORY_PAGE_ROW_LIMIT + 1
const SAGA_HISTORY_FETCH_MAX_BYTES = 20 * 1024 * 1024
const MAX_HISTORY_TEXT_BYTES = 2048
const UTF8_ENCODER = new TextEncoder()

const TERMINAL_SAGA_STATUSES = new Set([
  "cancelled",
  "failed",
  "intervention_required",
  "succeeded",
  "timed_out",
])

interface HistoryIdentityRow {
  readonly environment_id: string
  readonly operation_id: string
  readonly operation_input_checksum: string
  readonly operation_plan_checksum: string
  readonly operation_status: string
  readonly operation_updated_at_ms: number
  readonly saga_descriptor_checksum: string
  readonly saga_input_checksum: string
  readonly saga_last_effect_id: string
  readonly saga_id: string
  readonly saga_operation_id: string
  readonly saga_record_checksum: string
  readonly saga_state_version: number
  readonly saga_status: string
  readonly saga_updated_at_ms: number
}

interface AuditHeadRow {
  readonly event_hash: string
  readonly sequence: number
}

interface OperationPlanRow {
  readonly input_checksum: string
  readonly operation_id: string
  readonly plan_checksum: string
  readonly plan_json: string
}

interface TransitionSummaryRow {
  readonly history_count: number
  readonly joined_history_count: number
  readonly last_audit_sequence: number
  readonly last_transition_id: string
}

interface EffectSummaryRow {
  readonly history_count: number
  readonly last_effect_id: string
  readonly last_state_version: number
}

interface AttemptSummaryRow {
  readonly history_count: number
  readonly last_accepted_at_ms: number
  readonly last_attempt_id: string
}

export interface SagaHistoryAnchor {
  readonly auditHeadEventHash: string
  readonly auditHeadSequence: number
  readonly environmentId: string
  readonly operationId: string
  readonly operationInputChecksum: string
  readonly operationPlanChecksum: string
  readonly operationStatus: string
  readonly operationTransitionCount: number
  readonly operationTransitionLastAuditSequence: number
  readonly operationTransitionLastId: string
  readonly operationUpdatedAtMs: number
  readonly sagaAttemptCount: number
  readonly sagaAttemptLastAcceptedAtMs: number | null
  readonly sagaAttemptLastId: string | null
  readonly sagaDescriptorChecksum: string
  readonly sagaEffectCount: number
  readonly sagaId: string
  readonly sagaInputChecksum: string
  readonly sagaLastEffectId: string
  readonly sagaRecordChecksum: string
  readonly sagaStateVersion: number
  readonly sagaStatus: string
  readonly sagaUpdatedAtMs: number
  readonly schemaVersion: 1
}

export interface SagaHistoryAuditRow {
  readonly event_hash: string
  readonly event_json: string
  readonly sequence: number
}

export interface SagaHistoryTransitionRow {
  readonly acquisition_id: string
  readonly audit_event_hash: string
  readonly audit_event_json: string
  readonly audit_sequence: number
  readonly authorization_checksum: string | null
  readonly authorization_classified_at_ms: number | null
  readonly authorization_id: string | null
  readonly authorization_protocol_version: number | null
  readonly authorization_transition_id: string | null
  readonly created_at_ms: number
  readonly fencing_token: number
  readonly from_operation_status: string
  readonly from_record_json: string
  readonly holder_id: string
  readonly lease_key: string
  readonly operation_id: string
  readonly step_id: string
  readonly to_operation_status: string
  readonly to_record_json: string
  readonly transition_id: string
}

export interface SagaHistoryEffectRow {
  readonly acquisition_id: string
  readonly created_at_ms: number
  readonly effect_id: string
  readonly effect_kind: string
  readonly evidence_checksum: string
  readonly fencing_token: number
  readonly from_state_version: number | null
  readonly holder_id: string
  readonly lease_key: string
  readonly operation_id: string
  readonly record_checksum: string
  readonly record_json: string
  readonly resource_id: string
  readonly resource_kind: string
  readonly step_id: string
  readonly to_state_version: number
  readonly transition_id: string
}

export interface SagaHistoryTransitionCursor {
  readonly auditSequence: number
  readonly transitionId: string
}

export interface SagaHistoryAttemptCursor {
  readonly acceptedAtMs: number
  readonly attemptId: string
}

export interface SagaHistoryPage<Row, Cursor> {
  readonly complete: boolean
  readonly nextCursor: Cursor | null
  readonly rows: readonly Row[]
}

const HISTORY_ANCHOR_KEYS = [
  "auditHeadEventHash",
  "auditHeadSequence",
  "environmentId",
  "operationId",
  "operationInputChecksum",
  "operationPlanChecksum",
  "operationStatus",
  "operationTransitionCount",
  "operationTransitionLastAuditSequence",
  "operationTransitionLastId",
  "operationUpdatedAtMs",
  "sagaAttemptCount",
  "sagaAttemptLastAcceptedAtMs",
  "sagaAttemptLastId",
  "sagaDescriptorChecksum",
  "sagaEffectCount",
  "sagaId",
  "sagaInputChecksum",
  "sagaLastEffectId",
  "sagaRecordChecksum",
  "sagaStateVersion",
  "sagaStatus",
  "sagaUpdatedAtMs",
  "schemaVersion",
] as const satisfies readonly (keyof SagaHistoryAnchor)[]

const AUDIT_ROW_KEYS = [
  "event_hash",
  "event_json",
  "sequence",
] as const satisfies readonly (keyof SagaHistoryAuditRow)[]

const OPERATION_PLAN_ROW_KEYS = [
  "input_checksum",
  "operation_id",
  "plan_checksum",
  "plan_json",
] as const satisfies readonly (keyof OperationPlanRow)[]

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

const ATTEMPT_IDENTITY_ROW_KEYS = [
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

function rowBytes<Row extends object>(rows: readonly Row[], label: string): number {
  let total = 0
  const encoder = new TextEncoder()
  for (const row of rows) {
    for (const [key, value] of Object.entries(row)) {
      total += encoder.encode(key).byteLength + 1
      if (typeof value === "string") total += encoder.encode(value).byteLength
      else if (typeof value === "number") total += 8
      else if (value === null) total += 1
      else return intervention(`${label} page encoding is malformed.`)
    }
  }
  return total
}

function captured<Row extends object>(
  value: unknown,
  keys: readonly (keyof Row)[],
  label: string,
): Row {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention(`${label} could not be captured safely.`)
  }
  if (!exactRecord<Row>(snapshot, keys)) return intervention(`${label} fields are malformed.`)
  return Object.freeze(snapshot)
}

function capturedRows<Row extends object>(
  result: unknown,
  keys: readonly (keyof Row)[],
  label: string,
): readonly Row[] {
  let snapshot: unknown
  try {
    snapshot = structuredClone(result)
  } catch {
    return intervention(`${label} page could not be captured safely.`)
  }
  if (
    !plainRecord(snapshot) ||
    snapshot.success !== true ||
    !plainRecord(snapshot.meta) ||
    !Array.isArray(snapshot.results) ||
    Object.keys(snapshot.results).length !== snapshot.results.length ||
    Object.keys(snapshot.results).some((key, index) => key !== String(index)) ||
    snapshot.results.length > SAGA_HISTORY_FETCH_ROW_LIMIT ||
    !snapshot.results.every((row) => exactRecord<Row>(row, keys))
  ) {
    return intervention(`${label} page metadata or rows are malformed.`)
  }
  if (rowBytes(snapshot.results, label) > SAGA_HISTORY_FETCH_MAX_BYTES) {
    return intervention(`${label} page exceeds the bounded fetch budget.`)
  }
  return Object.freeze(snapshot.results.map((row) => Object.freeze(row)))
}

function boundedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_HISTORY_TEXT_BYTES) {
    return configuration(`${label} exceeds the saga-history identity limit.`)
  }
  return value
}

function persistedText(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0
}

function safeInteger(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum
}

function validJson(value: unknown): value is string {
  if (typeof value !== "string" || value.length === 0) return false
  try {
    JSON.parse(value)
    return true
  } catch {
    return false
  }
}

function knownSagaEffectKind(value: string): boolean {
  return (
    value === "create" ||
    value === "termination:cancellation" ||
    value === "termination:timeout" ||
    /^action:(?:forward|compensation):(?:begin|success|failure:(?:unknown|definitely_not_applied_retryable|definitely_not_applied_terminal)|recovery:(?:unknown|not-dispatched)|observation:(?:applied|not_applied|indeterminate))$/u.test(
      value,
    )
  )
}

function sameRecord(left: object, right: object): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function identityRow(value: unknown, operationId: string, sagaId: string): HistoryIdentityRow {
  const row = captured<HistoryIdentityRow>(
    value,
    [
      "environment_id",
      "operation_id",
      "operation_input_checksum",
      "operation_plan_checksum",
      "operation_status",
      "operation_updated_at_ms",
      "saga_descriptor_checksum",
      "saga_input_checksum",
      "saga_last_effect_id",
      "saga_id",
      "saga_operation_id",
      "saga_record_checksum",
      "saga_state_version",
      "saga_status",
      "saga_updated_at_ms",
    ],
    "Persisted saga-history identity",
  )
  if (
    row.operation_id !== operationId ||
    row.saga_id !== sagaId ||
    row.saga_operation_id !== operationId ||
    !persistedText(row.environment_id) ||
    !persistedText(row.operation_input_checksum) ||
    !persistedText(row.operation_plan_checksum) ||
    !persistedText(row.operation_status) ||
    !safeInteger(row.operation_updated_at_ms, 0) ||
    !persistedText(row.saga_descriptor_checksum) ||
    !persistedText(row.saga_input_checksum) ||
    !persistedText(row.saga_last_effect_id) ||
    !persistedText(row.saga_record_checksum) ||
    !safeInteger(row.saga_state_version, 0) ||
    row.saga_state_version >= Number.MAX_SAFE_INTEGER ||
    !TERMINAL_SAGA_STATUSES.has(row.saga_status) ||
    !safeInteger(row.saga_updated_at_ms, 0)
  ) {
    return intervention("Persisted saga-history identity is malformed or nonterminal.")
  }
  return row
}

function auditHeadRow(value: unknown): AuditHeadRow {
  const row = captured<AuditHeadRow>(
    value,
    ["event_hash", "sequence"],
    "Persisted saga-history audit head",
  )
  if (!persistedText(row.event_hash) || !safeInteger(row.sequence, 1)) {
    return intervention("Persisted saga-history audit head is malformed.")
  }
  return row
}

function transitionSummaryRow(value: unknown): TransitionSummaryRow {
  const row = captured<TransitionSummaryRow>(
    value,
    ["history_count", "joined_history_count", "last_audit_sequence", "last_transition_id"],
    "Persisted saga-history transition summary",
  )
  if (
    !safeInteger(row.history_count, 1) ||
    row.joined_history_count !== row.history_count ||
    !safeInteger(row.last_audit_sequence, 1) ||
    !persistedText(row.last_transition_id)
  ) {
    return intervention("Persisted saga-history transition summary is malformed.")
  }
  return row
}

function effectSummaryRow(value: unknown, identity: HistoryIdentityRow): EffectSummaryRow {
  const row = captured<EffectSummaryRow>(
    value,
    ["history_count", "last_effect_id", "last_state_version"],
    "Persisted saga-history effect summary",
  )
  if (
    row.history_count !== identity.saga_state_version + 1 ||
    row.last_state_version !== identity.saga_state_version ||
    row.last_effect_id !== identity.saga_last_effect_id
  ) {
    return intervention("Persisted saga-history effect summary contradicts its terminal head.")
  }
  return row
}

function attemptSummaryRow(value: unknown): AttemptSummaryRow | null {
  if (value === null) return null
  const row = captured<AttemptSummaryRow>(
    value,
    ["history_count", "last_accepted_at_ms", "last_attempt_id"],
    "Persisted saga-history attempt summary",
  )
  if (
    !safeInteger(row.history_count, 1) ||
    !safeInteger(row.last_accepted_at_ms, 0) ||
    !persistedText(row.last_attempt_id)
  ) {
    return intervention("Persisted saga-history attempt summary is malformed.")
  }
  return row
}

export function loadSagaHistoryAnchor(value: unknown): SagaHistoryAnchor {
  const anchor = captured<SagaHistoryAnchor>(value, HISTORY_ANCHOR_KEYS, "Saga-history anchor")
  if (
    anchor.schemaVersion !== 1 ||
    !persistedText(anchor.auditHeadEventHash) ||
    !safeInteger(anchor.auditHeadSequence, 1) ||
    !persistedText(anchor.environmentId) ||
    !persistedText(anchor.operationId) ||
    !persistedText(anchor.operationInputChecksum) ||
    !persistedText(anchor.operationPlanChecksum) ||
    !persistedText(anchor.operationStatus) ||
    !safeInteger(anchor.operationTransitionCount, 1) ||
    !safeInteger(anchor.operationTransitionLastAuditSequence, 1) ||
    anchor.operationTransitionLastAuditSequence > anchor.auditHeadSequence ||
    !persistedText(anchor.operationTransitionLastId) ||
    !safeInteger(anchor.operationUpdatedAtMs, 0) ||
    !safeInteger(anchor.sagaAttemptCount, 0) ||
    (anchor.sagaAttemptCount === 0) !== (anchor.sagaAttemptLastAcceptedAtMs === null) ||
    (anchor.sagaAttemptCount === 0) !== (anchor.sagaAttemptLastId === null) ||
    (anchor.sagaAttemptLastAcceptedAtMs !== null &&
      !safeInteger(anchor.sagaAttemptLastAcceptedAtMs, 0)) ||
    (anchor.sagaAttemptLastId !== null && !persistedText(anchor.sagaAttemptLastId)) ||
    !persistedText(anchor.sagaDescriptorChecksum) ||
    !safeInteger(anchor.sagaEffectCount, 1) ||
    !persistedText(anchor.sagaId) ||
    !persistedText(anchor.sagaInputChecksum) ||
    !persistedText(anchor.sagaLastEffectId) ||
    !persistedText(anchor.sagaRecordChecksum) ||
    !safeInteger(anchor.sagaStateVersion, 0) ||
    anchor.sagaStateVersion >= Number.MAX_SAFE_INTEGER ||
    anchor.sagaEffectCount !== anchor.sagaStateVersion + 1 ||
    !TERMINAL_SAGA_STATUSES.has(anchor.sagaStatus) ||
    !safeInteger(anchor.sagaUpdatedAtMs, 0)
  ) {
    return intervention("Saga-history anchor is malformed.")
  }
  return anchor
}

function emptyPage<Row, Cursor>(): SagaHistoryPage<Row, Cursor> {
  return Object.freeze({ complete: true, nextCursor: null, rows: Object.freeze([]) })
}

function page<Row extends object, Cursor>(
  inputRows: readonly Row[],
  cursor: (row: Row) => Cursor,
): SagaHistoryPage<Row, Cursor> {
  const rows = Object.freeze(inputRows.slice(0, SAGA_HISTORY_PAGE_ROW_LIMIT))
  if (rowBytes(rows, "Saga-history return") > SAGA_HISTORY_PAGE_MAX_BYTES) {
    return intervention("Saga-history page exceeds the bounded return budget.")
  }
  const complete = inputRows.length <= SAGA_HISTORY_PAGE_ROW_LIMIT
  return Object.freeze({
    complete,
    nextCursor: complete ? null : cursor(rows.at(-1) as Row),
    rows,
  }) as SagaHistoryPage<Row, Cursor>
}

function transitionCursor(value: unknown): SagaHistoryTransitionCursor {
  const cursor = captured<SagaHistoryTransitionCursor>(
    value,
    ["auditSequence", "transitionId"],
    "Saga-history transition cursor",
  )
  if (!safeInteger(cursor.auditSequence, 1) || !persistedText(cursor.transitionId)) {
    return configuration("Saga-history transition cursor is malformed.")
  }
  return cursor
}

function attemptCursor(value: unknown): SagaHistoryAttemptCursor {
  const cursor = captured<SagaHistoryAttemptCursor>(
    value,
    ["acceptedAtMs", "attemptId"],
    "Saga-history attempt cursor",
  )
  if (!safeInteger(cursor.acceptedAtMs, 0) || !persistedText(cursor.attemptId)) {
    return configuration("Saga-history attempt cursor is malformed.")
  }
  return cursor
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

export class D1SagaHistoryReader {
  readonly #database: TransactionalControlDatabase

  constructor(database: TransactionalControlDatabase) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional Control D1 binding is required for saga history.")
    }
    this.#database = database
  }

  async #identity(operationId: string, sagaId: string): Promise<HistoryIdentityRow> {
    const row = await this.#database
      .prepare(
        `SELECT "operation"."operation_id", "operation"."environment_id",
                "operation"."input_checksum" AS "operation_input_checksum",
                "operation"."plan_checksum" AS "operation_plan_checksum",
                "operation"."status" AS "operation_status",
                "operation"."updated_at_ms" AS "operation_updated_at_ms",
                "saga"."saga_id", "saga"."operation_id" AS "saga_operation_id",
                "saga"."descriptor_checksum" AS "saga_descriptor_checksum",
                "saga"."input_checksum" AS "saga_input_checksum",
                "saga"."state_version" AS "saga_state_version",
                "saga"."status" AS "saga_status",
                "saga"."last_effect_id" AS "saga_last_effect_id",
                "saga"."record_checksum" AS "saga_record_checksum",
                "saga"."updated_at_ms" AS "saga_updated_at_ms"
         FROM "nozzle_operations" AS "operation"
         JOIN "nozzle_sagas" AS "saga" ON "saga"."operation_id" = "operation"."operation_id"
         WHERE "operation"."operation_id" = ?1 AND "saga"."saga_id" = ?2`,
      )
      .bind(operationId, sagaId)
      .first<HistoryIdentityRow>()
    if (row === null) return resume("The terminal saga history does not exist.")
    return identityRow(row, operationId, sagaId)
  }

  async #auditHead(environmentId: string): Promise<AuditHeadRow> {
    const row = await this.#database
      .prepare(
        `SELECT "sequence", "event_hash" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 ORDER BY "sequence" DESC LIMIT 1`,
      )
      .bind(environmentId)
      .first<AuditHeadRow>()
    if (row === null) return intervention("The terminal saga history has no audit head.")
    return auditHeadRow(row)
  }

  async #auditAt(environmentId: string, sequence: number): Promise<AuditHeadRow> {
    const row = await this.#database
      .prepare(
        `SELECT "sequence", "event_hash" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "sequence" = ?2`,
      )
      .bind(environmentId, sequence)
      .first<AuditHeadRow>()
    if (row === null) return intervention("The anchored saga audit event disappeared.")
    return auditHeadRow(row)
  }

  async #transitionSummary(
    environmentId: string,
    operationId: string,
  ): Promise<TransitionSummaryRow> {
    const row = await this.#database
      .prepare(
        `SELECT (SELECT count(*) FROM "nozzle_operation_transitions" AS "raw_transition"
                 WHERE "raw_transition"."operation_id" = ?2) AS "history_count",
                count(*) OVER () AS "joined_history_count",
                "audit"."sequence" AS "last_audit_sequence",
                "transition"."transition_id" AS "last_transition_id"
         FROM "nozzle_operation_transitions" AS "transition"
         JOIN "nozzle_audit_log" AS "audit"
           ON "audit"."environment_id" = ?1
          AND "audit"."event_hash" = "transition"."audit_event_hash"
         WHERE "transition"."operation_id" = ?2
         ORDER BY "audit"."sequence" DESC,
                  "transition"."transition_id" COLLATE BINARY DESC LIMIT 1`,
      )
      .bind(environmentId, operationId)
      .first<TransitionSummaryRow>()
    if (row === null) return intervention("The terminal saga history has no operation transition.")
    return transitionSummaryRow(row)
  }

  async #effectSummary(sagaId: string, identity: HistoryIdentityRow): Promise<EffectSummaryRow> {
    const row = await this.#database
      .prepare(
        `SELECT count(*) OVER () AS "history_count",
                "to_state_version" AS "last_state_version", "effect_id" AS "last_effect_id"
         FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1
         ORDER BY "to_state_version" DESC, "effect_id" DESC LIMIT 1`,
      )
      .bind(sagaId)
      .first<EffectSummaryRow>()
    if (row === null) return intervention("The terminal saga history has no effect chain.")
    return effectSummaryRow(row, identity)
  }

  async #attemptSummary(sagaId: string): Promise<AttemptSummaryRow | null> {
    const row = await this.#database
      .prepare(
        `SELECT count(*) OVER () AS "history_count",
                "accepted_at_ms" AS "last_accepted_at_ms", "attempt_id" AS "last_attempt_id"
         FROM "nozzle_saga_action_attempts" WHERE "saga_id" = ?1
         ORDER BY "accepted_at_ms" DESC, "attempt_id" COLLATE BINARY DESC LIMIT 1`,
      )
      .bind(sagaId)
      .first<AttemptSummaryRow>()
    return attemptSummaryRow(row)
  }

  async captureAnchor(operationIdInput: string, sagaIdInput: string): Promise<SagaHistoryAnchor> {
    const operationId = boundedText(operationIdInput, "Operation ID")
    const sagaId = boundedText(sagaIdInput, "Saga ID")
    const identity = await this.#identity(operationId, sagaId)
    const transitions = await this.#transitionSummary(identity.environment_id, operationId)
    const effects = await this.#effectSummary(sagaId, identity)
    const attempts = await this.#attemptSummary(sagaId)
    const audit = await this.#auditHead(identity.environment_id)
    if (transitions.last_audit_sequence > audit.sequence) {
      return intervention("The operation transition head exceeds the environment audit head.")
    }

    const finalIdentity = await this.#identity(operationId, sagaId)
    const finalTransitions = await this.#transitionSummary(identity.environment_id, operationId)
    const finalEffects = await this.#effectSummary(sagaId, finalIdentity)
    const finalAttempts = await this.#attemptSummary(sagaId)
    const finalAudit = await this.#auditAt(identity.environment_id, audit.sequence)
    if (
      identity.saga_descriptor_checksum !== finalIdentity.saga_descriptor_checksum ||
      identity.saga_input_checksum !== finalIdentity.saga_input_checksum ||
      identity.saga_last_effect_id !== finalIdentity.saga_last_effect_id ||
      identity.saga_record_checksum !== finalIdentity.saga_record_checksum ||
      identity.saga_state_version !== finalIdentity.saga_state_version ||
      identity.saga_status !== finalIdentity.saga_status ||
      identity.saga_updated_at_ms !== finalIdentity.saga_updated_at_ms ||
      !sameRecord(effects, finalEffects)
    ) {
      return intervention("The terminal saga anchor changed while history capture was in progress.")
    }
    if (
      identity.environment_id !== finalIdentity.environment_id ||
      identity.operation_input_checksum !== finalIdentity.operation_input_checksum ||
      identity.operation_plan_checksum !== finalIdentity.operation_plan_checksum ||
      identity.operation_status !== finalIdentity.operation_status ||
      identity.operation_updated_at_ms !== finalIdentity.operation_updated_at_ms ||
      !sameRecord(transitions, finalTransitions) ||
      !sameRecord(attempts ?? {}, finalAttempts ?? {})
    ) {
      return resume("The operation or attempt tail changed while history capture was in progress.")
    }
    if (!sameRecord(audit, finalAudit)) {
      return intervention("The anchored saga audit head changed during history capture.")
    }

    return Object.freeze({
      auditHeadEventHash: audit.event_hash,
      auditHeadSequence: audit.sequence,
      environmentId: identity.environment_id,
      operationId,
      operationInputChecksum: identity.operation_input_checksum,
      operationPlanChecksum: identity.operation_plan_checksum,
      operationStatus: identity.operation_status,
      operationTransitionCount: transitions.history_count,
      operationTransitionLastAuditSequence: transitions.last_audit_sequence,
      operationTransitionLastId: transitions.last_transition_id,
      operationUpdatedAtMs: identity.operation_updated_at_ms,
      sagaAttemptCount: attempts?.history_count ?? 0,
      sagaAttemptLastAcceptedAtMs: attempts?.last_accepted_at_ms ?? null,
      sagaAttemptLastId: attempts?.last_attempt_id ?? null,
      sagaDescriptorChecksum: identity.saga_descriptor_checksum,
      sagaEffectCount: effects.history_count,
      sagaId,
      sagaInputChecksum: identity.saga_input_checksum,
      sagaLastEffectId: identity.saga_last_effect_id,
      sagaRecordChecksum: identity.saga_record_checksum,
      sagaStateVersion: identity.saga_state_version,
      sagaStatus: identity.saga_status,
      sagaUpdatedAtMs: identity.saga_updated_at_ms,
      schemaVersion: 1,
    })
  }

  async assertAnchorCurrent(input: SagaHistoryAnchor): Promise<void> {
    const anchor = loadSagaHistoryAnchor(input)
    const identity = await this.#identity(anchor.operationId, anchor.sagaId)
    const transitions = await this.#transitionSummary(anchor.environmentId, anchor.operationId)
    const effects = await this.#effectSummary(anchor.sagaId, identity)
    const attempts = await this.#attemptSummary(anchor.sagaId)
    const audit = await this.#auditAt(anchor.environmentId, anchor.auditHeadSequence)
    if (
      identity.saga_descriptor_checksum !== anchor.sagaDescriptorChecksum ||
      identity.saga_input_checksum !== anchor.sagaInputChecksum ||
      identity.saga_last_effect_id !== anchor.sagaLastEffectId ||
      identity.saga_record_checksum !== anchor.sagaRecordChecksum ||
      identity.saga_state_version !== anchor.sagaStateVersion ||
      identity.saga_status !== anchor.sagaStatus ||
      identity.saga_updated_at_ms !== anchor.sagaUpdatedAtMs ||
      effects.history_count !== anchor.sagaEffectCount ||
      effects.last_effect_id !== anchor.sagaLastEffectId ||
      effects.last_state_version !== anchor.sagaStateVersion ||
      audit.event_hash !== anchor.auditHeadEventHash
    ) {
      return intervention("The terminal saga history anchor is no longer current.")
    }
    if (
      identity.environment_id !== anchor.environmentId ||
      identity.operation_input_checksum !== anchor.operationInputChecksum ||
      identity.operation_plan_checksum !== anchor.operationPlanChecksum ||
      identity.operation_status !== anchor.operationStatus ||
      identity.operation_updated_at_ms !== anchor.operationUpdatedAtMs ||
      transitions.history_count !== anchor.operationTransitionCount ||
      transitions.last_audit_sequence !== anchor.operationTransitionLastAuditSequence ||
      transitions.last_transition_id !== anchor.operationTransitionLastId ||
      (attempts?.history_count ?? 0) !== anchor.sagaAttemptCount ||
      (attempts?.last_accepted_at_ms ?? null) !== anchor.sagaAttemptLastAcceptedAtMs ||
      (attempts?.last_attempt_id ?? null) !== anchor.sagaAttemptLastId
    ) {
      return resume("The operation or saga-attempt history advanced beyond its verified anchor.")
    }
  }

  async operationPlan(
    inputAnchor: SagaHistoryAnchor,
    digest: DigestFunction,
  ): Promise<OperationPlan> {
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    if (typeof digest !== "function")
      configuration("A saga-history operation-plan digest is required.")
    const candidate = await this.#database
      .prepare(
        `SELECT "operation_id", "input_checksum", "plan_checksum", "plan_json"
         FROM "nozzle_operations" WHERE "operation_id" = ?1`,
      )
      .bind(anchor.operationId)
      .first<OperationPlanRow>()
    if (candidate === null) {
      return intervention("The anchored saga operation plan disappeared.")
    }
    const row = captured<OperationPlanRow>(
      candidate,
      OPERATION_PLAN_ROW_KEYS,
      "Persisted saga-history operation plan",
    )
    if (
      row.operation_id !== anchor.operationId ||
      row.input_checksum !== anchor.operationInputChecksum ||
      row.plan_checksum !== anchor.operationPlanChecksum ||
      !validJson(row.plan_json) ||
      UTF8_ENCODER.encode(row.plan_json).byteLength > 2_000_000
    ) {
      return intervention("The persisted saga operation plan contradicts its history anchor.")
    }
    const plan = await loadOperationPlan(JSON.parse(row.plan_json) as OperationPlan, digest)
    if (
      plan.operationId !== anchor.operationId ||
      plan.inputChecksum !== anchor.operationInputChecksum ||
      plan.planChecksum !== anchor.operationPlanChecksum ||
      !plan.operationType.startsWith("saga:") ||
      JSON.stringify(plan) !== row.plan_json
    ) {
      return intervention("The persisted saga operation plan is not canonical for its anchor.")
    }
    return plan
  }

  async auditPage(
    inputAnchor: SagaHistoryAnchor,
    afterSequenceInput = 0,
  ): Promise<SagaHistoryPage<SagaHistoryAuditRow, number>> {
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    if (!safeInteger(afterSequenceInput, 0) || afterSequenceInput > anchor.auditHeadSequence) {
      return configuration("Saga-history audit cursor is malformed.")
    }
    if (afterSequenceInput === anchor.auditHeadSequence) return emptyPage()
    const result = await this.#database
      .prepare(
        `SELECT "sequence", "event_hash", "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "sequence" > ?2 AND "sequence" <= ?3
         ORDER BY "sequence" LIMIT ${SAGA_HISTORY_FETCH_ROW_LIMIT}`,
      )
      .bind(anchor.environmentId, afterSequenceInput, anchor.auditHeadSequence)
      .all<SagaHistoryAuditRow>()
    const rows = capturedRows<SagaHistoryAuditRow>(result, AUDIT_ROW_KEYS, "Saga audit history")
    let expected = afterSequenceInput + 1
    for (const row of rows) {
      if (
        row.sequence !== expected ||
        row.sequence > anchor.auditHeadSequence ||
        !persistedText(row.event_hash) ||
        !validJson(row.event_json)
      ) {
        return intervention("Saga audit history is malformed, unordered, or incomplete.")
      }
      expected += 1
    }
    if (
      rows.length <= SAGA_HISTORY_PAGE_ROW_LIMIT &&
      rows.at(-1)?.sequence !== anchor.auditHeadSequence
    ) {
      return intervention("Saga audit history ended before its anchored head.")
    }
    return page(rows, (row) => row.sequence)
  }

  async transitionPage(
    inputAnchor: SagaHistoryAnchor,
    inputCursor?: SagaHistoryTransitionCursor,
  ): Promise<SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor>> {
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    const cursor =
      inputCursor === undefined
        ? Object.freeze({ auditSequence: 0, transitionId: "" })
        : transitionCursor(inputCursor)
    const atEnd =
      cursor.auditSequence === anchor.operationTransitionLastAuditSequence &&
      cursor.transitionId === anchor.operationTransitionLastId
    if (
      cursor.auditSequence > anchor.operationTransitionLastAuditSequence ||
      (cursor.auditSequence === anchor.operationTransitionLastAuditSequence &&
        sqliteBinaryTextCompare(cursor.transitionId, anchor.operationTransitionLastId) > 0)
    ) {
      return configuration("Saga-history transition cursor exceeds its anchor.")
    }
    if (atEnd) return emptyPage()
    const result = await this.#database
      .prepare(
        `SELECT "transition"."transition_id", "transition"."operation_id",
                "transition"."step_id", "transition"."from_record_json",
                "transition"."to_record_json", "transition"."from_operation_status",
                "transition"."to_operation_status", "transition"."audit_event_hash",
                "transition"."fencing_token", "transition"."lease_key",
                "transition"."holder_id", "transition"."acquisition_id",
                "transition"."created_at_ms", "audit"."sequence" AS "audit_sequence",
                "audit"."event_json" AS "audit_event_json",
                "authorization"."transition_id" AS "authorization_transition_id",
                "authorization"."protocol_version" AS "authorization_protocol_version",
                "authorization"."authorization_id" AS "authorization_id",
                "authorization"."authorization_checksum" AS "authorization_checksum",
                "authorization"."classified_at_ms" AS "authorization_classified_at_ms"
         FROM "nozzle_operation_transitions" AS "transition"
         JOIN "nozzle_audit_log" AS "audit"
           ON "audit"."environment_id" = ?1
          AND "audit"."event_hash" = "transition"."audit_event_hash"
         LEFT JOIN "nozzle_irreversible_authorization_receipts" AS "authorization"
           ON "authorization"."transition_id" = "transition"."transition_id"
         WHERE "transition"."operation_id" = ?2
           AND ("audit"."sequence" > ?3
             OR ("audit"."sequence" = ?3
               AND "transition"."transition_id" COLLATE BINARY > ?4))
           AND ("audit"."sequence" < ?5
             OR ("audit"."sequence" = ?5
               AND "transition"."transition_id" COLLATE BINARY <= ?6))
         ORDER BY "audit"."sequence", "transition"."transition_id" COLLATE BINARY
         LIMIT ${SAGA_HISTORY_FETCH_ROW_LIMIT}`,
      )
      .bind(
        anchor.environmentId,
        anchor.operationId,
        cursor.auditSequence,
        cursor.transitionId,
        anchor.operationTransitionLastAuditSequence,
        anchor.operationTransitionLastId,
      )
      .all<SagaHistoryTransitionRow>()
    const rows = capturedRows<SagaHistoryTransitionRow>(
      result,
      TRANSITION_ROW_KEYS,
      "Saga operation-transition history",
    )
    let previous = cursor
    for (const row of rows) {
      const noAuthorization = row.authorization_transition_id === null
      if (
        row.operation_id !== anchor.operationId ||
        !pairAfter(
          row.audit_sequence,
          row.transition_id,
          previous.auditSequence,
          previous.transitionId,
        ) ||
        !safeInteger(row.audit_sequence, 1) ||
        row.audit_sequence > anchor.operationTransitionLastAuditSequence ||
        (row.audit_sequence === anchor.operationTransitionLastAuditSequence &&
          sqliteBinaryTextCompare(row.transition_id, anchor.operationTransitionLastId) > 0) ||
        ![
          row.transition_id,
          row.step_id,
          row.from_operation_status,
          row.to_operation_status,
          row.audit_event_hash,
          row.lease_key,
          row.holder_id,
          row.acquisition_id,
        ].every(persistedText) ||
        !validJson(row.from_record_json) ||
        !validJson(row.to_record_json) ||
        !validJson(row.audit_event_json) ||
        !safeInteger(row.fencing_token, 1) ||
        !safeInteger(row.created_at_ms, 0) ||
        (noAuthorization
          ? row.authorization_protocol_version !== null ||
            row.authorization_id !== null ||
            row.authorization_checksum !== null ||
            row.authorization_classified_at_ms !== null
          : row.authorization_transition_id !== row.transition_id ||
            (row.authorization_protocol_version !== 1 &&
              row.authorization_protocol_version !== 2) ||
            (row.authorization_protocol_version === 1
              ? row.authorization_id !== null
              : !persistedText(row.authorization_id)) ||
            !persistedText(row.authorization_checksum) ||
            !safeInteger(row.authorization_classified_at_ms, 0))
      ) {
        return intervention("Saga operation-transition history is malformed or unordered.")
      }
      previous = Object.freeze({
        auditSequence: row.audit_sequence,
        transitionId: row.transition_id,
      })
    }
    if (
      rows.length <= SAGA_HISTORY_PAGE_ROW_LIMIT &&
      (previous.auditSequence !== anchor.operationTransitionLastAuditSequence ||
        previous.transitionId !== anchor.operationTransitionLastId)
    ) {
      return intervention("Saga operation-transition history ended before its anchored head.")
    }
    return page(rows, (row) =>
      Object.freeze({
        auditSequence: row.audit_sequence,
        transitionId: row.transition_id,
      }),
    )
  }

  async effectPage(
    inputAnchor: SagaHistoryAnchor,
    afterStateVersionInput = -1,
  ): Promise<SagaHistoryPage<SagaHistoryEffectRow, number>> {
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    if (
      !safeInteger(afterStateVersionInput, -1) ||
      afterStateVersionInput > anchor.sagaStateVersion
    ) {
      return configuration("Saga-history effect cursor is malformed.")
    }
    if (afterStateVersionInput === anchor.sagaStateVersion) return emptyPage()
    const result = await this.#database
      .prepare(
        `SELECT "effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
                "resource_id", "effect_kind", "from_state_version", "to_state_version",
                "evidence_checksum", "record_checksum", "record_json", "lease_key",
                "holder_id", "acquisition_id", "fencing_token", "created_at_ms"
         FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1
           AND "to_state_version" > ?2 AND "to_state_version" <= ?3
         ORDER BY "to_state_version", "effect_id" LIMIT ${SAGA_HISTORY_FETCH_ROW_LIMIT}`,
      )
      .bind(anchor.sagaId, afterStateVersionInput, anchor.sagaStateVersion)
      .all<SagaHistoryEffectRow>()
    const rows = capturedRows<SagaHistoryEffectRow>(result, EFFECT_ROW_KEYS, "Saga effect history")
    let expected = afterStateVersionInput + 1
    for (const row of rows) {
      if (
        row.operation_id !== anchor.operationId ||
        row.resource_kind !== "saga" ||
        row.resource_id !== anchor.sagaId ||
        row.to_state_version !== expected ||
        row.to_state_version > anchor.sagaStateVersion ||
        row.from_state_version !== (expected === 0 ? null : expected - 1) ||
        ![
          row.effect_id,
          row.transition_id,
          row.step_id,
          row.evidence_checksum,
          row.record_checksum,
          row.lease_key,
          row.holder_id,
          row.acquisition_id,
        ].every(persistedText) ||
        !knownSagaEffectKind(row.effect_kind) ||
        !validJson(row.record_json) ||
        !safeInteger(row.fencing_token, 1) ||
        !safeInteger(row.created_at_ms, 0)
      ) {
        return intervention("Saga effect history is malformed, forked, or incomplete.")
      }
      expected += 1
    }
    if (
      rows.length <= SAGA_HISTORY_PAGE_ROW_LIMIT &&
      rows.at(-1)?.to_state_version !== anchor.sagaStateVersion
    ) {
      return intervention("Saga effect history ended before its anchored head.")
    }
    return page(rows, (row) => row.to_state_version)
  }

  async attemptIdentityPage(
    inputAnchor: SagaHistoryAnchor,
    inputCursor?: SagaHistoryAttemptCursor,
  ): Promise<SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor>> {
    const anchor = loadSagaHistoryAnchor(inputAnchor)
    if (anchor.sagaAttemptCount === 0) {
      if (inputCursor !== undefined) attemptCursor(inputCursor)
      return emptyPage()
    }
    const cursor =
      inputCursor === undefined
        ? Object.freeze({ acceptedAtMs: -1, attemptId: "" })
        : attemptCursor(inputCursor)
    const lastAcceptedAtMs = anchor.sagaAttemptLastAcceptedAtMs as number
    const lastAttemptId = anchor.sagaAttemptLastId as string
    const atEnd = cursor.acceptedAtMs === lastAcceptedAtMs && cursor.attemptId === lastAttemptId
    if (
      cursor.acceptedAtMs > lastAcceptedAtMs ||
      (cursor.acceptedAtMs === lastAcceptedAtMs &&
        sqliteBinaryTextCompare(cursor.attemptId, lastAttemptId) > 0)
    ) {
      return configuration("Saga-history attempt cursor exceeds its anchor.")
    }
    if (atEnd) return emptyPage()
    const result = await this.#database
      .prepare(
        `SELECT ${SAGA_ATTEMPT_IDENTITY_ROW_SELECT}
         FROM "nozzle_saga_action_attempts" AS "attempt"
         LEFT JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
         WHERE "attempt"."saga_id" = ?1
           AND ("attempt"."accepted_at_ms" > ?2
             OR ("attempt"."accepted_at_ms" = ?2
               AND "attempt"."attempt_id" COLLATE BINARY > ?3))
           AND ("attempt"."accepted_at_ms" < ?4
             OR ("attempt"."accepted_at_ms" = ?4
               AND "attempt"."attempt_id" COLLATE BINARY <= ?5))
         ORDER BY "attempt"."accepted_at_ms", "attempt"."attempt_id" COLLATE BINARY
         LIMIT ${SAGA_HISTORY_FETCH_ROW_LIMIT}`,
      )
      .bind(anchor.sagaId, cursor.acceptedAtMs, cursor.attemptId, lastAcceptedAtMs, lastAttemptId)
      .all<SagaAttemptIdentityRow>()
    const rows = capturedRows<SagaAttemptIdentityRow>(
      result,
      ATTEMPT_IDENTITY_ROW_KEYS,
      "Saga-attempt identity history",
    )
    let previous = cursor
    for (const row of rows) {
      if (
        row.saga_id !== anchor.sagaId ||
        row.operation_id !== anchor.operationId ||
        !safeInteger(row.accepted_at_ms, 0) ||
        !persistedText(row.attempt_id) ||
        (row.causal_attempt_id !== null && !persistedText(row.causal_attempt_id)) ||
        ![
          row.acceptance_checksum,
          row.acquisition_id,
          row.action_key,
          row.holder_id,
          row.idempotency_key,
          row.input_checksum,
          row.lease_key,
          row.operation_step_id,
          row.saga_step_id,
        ].every(persistedText) ||
        !validJson(row.input_json) ||
        !safeInteger(row.fencing_token, 1) ||
        (row.phase !== "forward" && row.phase !== "compensation") ||
        (row.purpose !== "effect" && row.purpose !== "observation") ||
        !pairAfter(row.accepted_at_ms, row.attempt_id, previous.acceptedAtMs, previous.attemptId) ||
        row.accepted_at_ms > lastAcceptedAtMs ||
        (row.accepted_at_ms === lastAcceptedAtMs &&
          sqliteBinaryTextCompare(row.attempt_id, lastAttemptId) > 0) ||
        (row.protocol_version !== 1 && row.protocol_version !== 2) ||
        row.protocol_classified_at_ms !== row.accepted_at_ms
      ) {
        return intervention("Saga-attempt identity history is malformed or unordered.")
      }
      previous = Object.freeze({ acceptedAtMs: row.accepted_at_ms, attemptId: row.attempt_id })
    }
    if (
      rows.length <= SAGA_HISTORY_PAGE_ROW_LIMIT &&
      (previous.acceptedAtMs !== lastAcceptedAtMs || previous.attemptId !== lastAttemptId)
    ) {
      return intervention("Saga-attempt identity history ended before its anchored head.")
    }
    return page(rows, (row) =>
      Object.freeze({ acceptedAtMs: row.accepted_at_ms, attemptId: row.attempt_id }),
    )
  }
}
