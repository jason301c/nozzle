import {
  type AuditEvent,
  appendAuditEvent,
  type DigestFunction,
  type LeaseProof,
  loadAuditEvent,
  markOperationStepNotRequired,
  NozzleError,
  type OperationRecord,
  type OperationStepRecord,
  operationStatus,
  recordAtomicStepOutcome,
  recordSagaStepTerminalClassification,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"
import { D1LeaseStore } from "./lease-store.js"
import {
  D1OperationStore,
  operationStepRecordJson,
  operationTransitionIdentity,
} from "./operation-store.js"
import { D1SagaHistoryReader, type SagaHistoryAnchor } from "./saga-history.js"
import { SAGA_SETTLE_OPERATION_STEP_ID } from "./saga-store.js"
import {
  loadSagaTerminalCapability,
  type SagaTerminalCapability,
  type SagaTerminalCapabilityState,
  type SagaTerminalModelBranchDecision,
} from "./saga-terminal.js"

const MAX_PERSIST_ATTEMPTS = 16
const MAX_BOUND_JSON_BYTES = 2_000_000
const MAX_STEP_PROJECTION_CHUNKS = 10
const MAX_IDENTITY_BYTES = 512
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const SETTLEMENT_ATTEMPT_PREFIX = "nozzle.saga-settlement-attempt.v1:"

interface AuditSnapshotRow {
  readonly event_json: string | null
  readonly now_ms: number
}

interface PersistedTailRow {
  readonly acquisition_id: string | null
  readonly audit_event_hash: string | null
  readonly audit_sequence: number | null
  readonly created_at_ms: number | null
  readonly event_json: string | null
  readonly fencing_token: number | null
  readonly from_operation_status: string | null
  readonly from_record_json: string | null
  readonly holder_id: string | null
  readonly lease_key: string | null
  readonly operation_id: string | null
  readonly ordinal: number
  readonly step_id: string | null
  readonly to_operation_status: string | null
  readonly to_record_json: string | null
  readonly transition_id: string | null
}

interface TailHeadRow {
  readonly attempt_count: number
  readonly attempt_last_accepted_at_ms: number | null
  readonly attempt_last_id: string | null
  readonly effect_count: number
  readonly effect_last_id: string | null
  readonly effect_last_state_version: number | null
  readonly environment_id: string
  readonly operation_input_checksum: string
  readonly operation_plan_checksum: string
  readonly operation_status: string
  readonly saga_descriptor_checksum: string
  readonly saga_input_checksum: string
  readonly saga_last_effect_id: string
  readonly saga_record_checksum: string
  readonly saga_state_version: number
  readonly saga_status: string
  readonly saga_updated_at_ms: number
  readonly step_count: number
  readonly transition_count: number
  readonly transition_joined_count: number
  readonly transition_last_audit_sequence: number | null
  readonly transition_last_id: string | null
}

type TailKind = "not_required" | "settlement" | "terminal_not_applied"

interface TailTransition {
  readonly attemptId?: string
  readonly eventType: string
  readonly fromRecordJson: string
  readonly fromStatus: string
  readonly kind: TailKind
  readonly payloadChecksum: string
  readonly stepId: string
  readonly toRecordJson: string
  readonly toState: string
  readonly toStatus: string
  readonly transitionId: string
}

interface PersistSagaTerminalTailInput {
  readonly actorChecksum: string
  readonly capability: SagaTerminalCapability
  readonly proof: LeaseProof
}

interface PersistInputSnapshot {
  readonly actorChecksum: string
  readonly capability: SagaTerminalCapability
  readonly proof: LeaseProof
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function boundedText(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    configuration(`${label} exceeds the durable identity limit.`)
  }
}

function safeInteger(value: unknown, minimum = 0): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum
}

function parseJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value) as unknown
  } catch {
    return intervention(`${label} is not valid JSON.`)
  }
}

function boundedJson(value: unknown, label: string): string {
  const json = JSON.stringify(value)
  if (new TextEncoder().encode(json).byteLength > MAX_BOUND_JSON_BYTES) {
    return intervention(`${label} exceeds D1's two-million-byte bound-value limit.`)
  }
  return json
}

function boundedJsonChunks(values: readonly unknown[], label: string): readonly string[] {
  const chunks: string[] = []
  let chunk: unknown[] = []
  for (const value of values) {
    const candidate = JSON.stringify([...chunk, value])
    if (new TextEncoder().encode(candidate).byteLength > MAX_BOUND_JSON_BYTES) {
      chunks.push(boundedJson(chunk, label))
      chunk = []
    }
    chunk.push(value)
  }
  chunks.push(boundedJson(chunk, label))
  // The verified 256-step saga model and 64 KiB authorization envelope require fewer than ten.
  while (chunks.length < MAX_STEP_PROJECTION_CHUNKS) chunks.push("[]")
  return Object.freeze(chunks)
}

function snapshotInput(input: PersistSagaTerminalTailInput): PersistInputSnapshot {
  try {
    const actorChecksum = input.actorChecksum
    const capability = input.capability
    const proof = Object.freeze(structuredClone(input.proof))
    return Object.freeze({ actorChecksum, capability, proof })
  } catch {
    return configuration("Saga terminal persistence input could not be captured safely.")
  }
}

function sameOperation(left: OperationRecord, right: OperationRecord): boolean {
  const leftIds = Object.keys(left.steps).sort()
  const rightIds = Object.keys(right.steps).sort()
  return (
    leftIds.length === rightIds.length &&
    leftIds.every(
      (stepId, index) =>
        stepId === rightIds[index] &&
        operationStepRecordJson(left.steps[stepId] as OperationStepRecord) ===
          operationStepRecordJson(right.steps[stepId] as OperationStepRecord),
    )
  )
}

function settlementAttemptId(state: SagaTerminalCapabilityState): string {
  return `${SETTLEMENT_ATTEMPT_PREFIX}${state.finalProof.anchor.sagaRecordChecksum}`
}

function terminalNotAppliedEventType(
  decision: Extract<SagaTerminalModelBranchDecision, { readonly kind: "terminal_not_applied" }>,
): string {
  return `saga.action.terminal_not_applied.${decision.evidenceKind}`
}

function applyTailTransition(
  operation: OperationRecord,
  transition: TailTransition,
  proof: LeaseProof,
  settlementOutcome: SagaTerminalCapabilityState["settlementOutcome"],
): OperationRecord {
  if (transition.kind === "not_required") {
    return markOperationStepNotRequired(operation, {
      evidenceChecksum: transition.payloadChecksum,
      stepId: transition.stepId,
    })
  }
  if (transition.kind === "terminal_not_applied") {
    return recordSagaStepTerminalClassification(operation, {
      outcome: "not_applied",
      receiptOutcomeChecksum: transition.payloadChecksum,
      stepId: transition.stepId,
    })
  }
  const planStep = operation.plan.steps.find(
    (candidate) => candidate.stepId === SAGA_SETTLE_OPERATION_STEP_ID,
  ) as OperationRecord["plan"]["steps"][number]
  const outcome =
    settlementOutcome === "succeeded"
      ? {
          observedPostconditionChecksum: planStep.postconditionChecksum,
          resultChecksum: transition.payloadChecksum,
          state: "succeeded" as const,
        }
      : settlementOutcome === "failed"
        ? { errorChecksum: transition.payloadChecksum, state: "failed" as const }
        : {
            evidenceChecksum: transition.payloadChecksum,
            state: "intervention_required" as const,
          }
  return recordAtomicStepOutcome(operation, {
    attemptId: transition.attemptId as string,
    idempotencyKey: planStep.idempotencyKey,
    leaseProof: proof,
    observedPreconditionChecksum: planStep.preconditionChecksum,
    outcome,
    stepId: SAGA_SETTLE_OPERATION_STEP_ID,
  })
}

function buildTail(
  state: SagaTerminalCapabilityState,
  proof: LeaseProof,
): { readonly operation: OperationRecord; readonly transitions: readonly TailTransition[] } {
  let operation = state.operation
  const transitions: TailTransition[] = []
  for (const decision of state.branchDecisions) {
    const fromStatus = operationStatus(operation)
    const before = operation.steps[decision.stepId] as OperationStepRecord
    const next =
      decision.kind === "not_required"
        ? markOperationStepNotRequired(operation, {
            evidenceChecksum: decision.sagaChecksum,
            stepId: decision.stepId,
          })
        : recordSagaStepTerminalClassification(operation, {
            outcome: "not_applied",
            receiptOutcomeChecksum: decision.evidenceChecksum,
            stepId: decision.stepId,
          })
    const after = next.steps[decision.stepId] as OperationStepRecord
    const eventType =
      decision.kind === "not_required" ? "step.not_required" : terminalNotAppliedEventType(decision)
    const payloadChecksum =
      decision.kind === "not_required" ? decision.sagaChecksum : decision.evidenceChecksum
    const transitionId =
      decision.kind === "not_required"
        ? operationTransitionIdentity("not-required", [
            state.operation.plan.operationId,
            decision.stepId,
            `nozzle.saga-terminal-decision.v1:${decision.sagaChecksum}`,
          ])
        : operationTransitionIdentity("saga-terminal-not-applied", [
            state.operation.plan.operationId,
            decision.stepId,
            decision.attemptId,
            decision.evidenceKind,
          ])
    const toStatus = operationStatus(next)
    transitions.push(
      Object.freeze({
        eventType,
        fromRecordJson: operationStepRecordJson(before),
        fromStatus,
        kind: decision.kind,
        payloadChecksum,
        stepId: decision.stepId,
        toRecordJson: operationStepRecordJson(after),
        toState: after.state,
        toStatus,
        transitionId,
      }),
    )
    operation = next
  }

  const before = operation.steps[SAGA_SETTLE_OPERATION_STEP_ID] as OperationStepRecord
  const attemptId = settlementAttemptId(state)
  const settlementTransitionId = operationTransitionIdentity("saga-settled", [
    operation.plan.operationId,
    SAGA_SETTLE_OPERATION_STEP_ID,
    attemptId,
  ])
  const settlementTemplate: TailTransition = Object.freeze({
    attemptId,
    eventType: "saga.settled",
    fromRecordJson: operationStepRecordJson(before),
    fromStatus: operationStatus(operation),
    kind: "settlement",
    payloadChecksum: state.finalProof.anchor.sagaRecordChecksum,
    stepId: SAGA_SETTLE_OPERATION_STEP_ID,
    toRecordJson: "",
    toState: state.settlementOutcome,
    toStatus: state.settlementOutcome,
    transitionId: settlementTransitionId,
  })
  const next = applyTailTransition(operation, settlementTemplate, proof, state.settlementOutcome)
  const after = next.steps[SAGA_SETTLE_OPERATION_STEP_ID] as OperationStepRecord
  const settlement = Object.freeze({
    ...settlementTemplate,
    toRecordJson: operationStepRecordJson(after),
    toState: after.state,
    toStatus: operationStatus(next),
  })
  transitions.push(settlement)
  return Object.freeze({ operation: next, transitions: Object.freeze(transitions) })
}

function validateMutationResults(
  results: readonly ControlRunResult[],
  transitionCount: number,
): "committed" | "guard_rejected" {
  if (!Array.isArray(results) || results.length !== 4) {
    return intervention("Control D1 returned an incomplete terminal-tail batch result.")
  }
  const changes = results.map((result) => result.meta.changes)
  if (
    results.some((result) => result.success !== true) ||
    changes.some((value) => !safeInteger(value))
  ) {
    return intervention("Control D1 returned malformed terminal-tail mutation metadata.")
  }
  if (changes.every((value) => value === 0)) return "guard_rejected"
  if (
    changes[0] !== transitionCount ||
    changes[1] !== transitionCount ||
    changes[2] !== 1 ||
    changes[3] !== transitionCount
  ) {
    return intervention("The atomic terminal-tail batch reported a partial mutation.")
  }
  return "committed"
}

export class D1SagaTerminalStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction
  readonly #history: D1SagaHistoryReader
  readonly #leases: D1LeaseStore
  readonly #operations: D1OperationStore

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional Control D1 binding is required for saga settlement.")
    }
    if (typeof digest !== "function") configuration("A saga terminal digest is required.")
    this.#database = database
    this.#digest = digest
    this.#history = new D1SagaHistoryReader(database)
    this.#leases = new D1LeaseStore(database)
    this.#operations = new D1OperationStore(database, digest)
  }

  async #auditSnapshot(environmentId: string): Promise<{
    readonly nowMs: number
    readonly previous: AuditEvent | undefined
  }> {
    const row = await this.#database
      .prepare(
        `SELECT ${SERVER_TIME_SQL} AS "now_ms",
          (SELECT "event_json" FROM "nozzle_audit_log"
           WHERE "environment_id" = ?1 ORDER BY "sequence" DESC LIMIT 1) AS "event_json"`,
      )
      .bind(environmentId)
      .first<AuditSnapshotRow>()
    if (!row || !safeInteger(row.now_ms)) {
      return intervention("Control D1 returned malformed authoritative terminal-tail time.")
    }
    if (row.event_json === null) return Object.freeze({ nowMs: row.now_ms, previous: undefined })
    const previous = await loadAuditEvent(
      parseJson(row.event_json, "Persisted audit head"),
      this.#digest,
    )
    if (previous.environmentId !== environmentId) {
      return intervention("The terminal-tail audit head belongs to another environment.")
    }
    return Object.freeze({ nowMs: row.now_ms, previous })
  }

  #statements(
    state: SagaTerminalCapabilityState,
    proof: LeaseProof,
    transitionsJson: string,
    auditsJson: string,
    stepProjectionChunks: readonly string[],
    finalStatus: string,
  ) {
    const anchor = state.finalProof.anchor
    return [
      this.#database
        .prepare(
          `WITH "step_chunks" ("chunk_json") AS MATERIALIZED (
             VALUES (?8), (?9), (?10), (?11), (?12), (?13), (?14), (?15), (?16), (?17)
           ), "expected_steps" AS MATERIALIZED (
             SELECT "item"."value" FROM "step_chunks"
             CROSS JOIN json_each("step_chunks"."chunk_json") AS "item"
           ), "guard" AS MATERIALIZED (
             SELECT 1 WHERE EXISTS (
               SELECT 1 FROM "nozzle_operations"
               WHERE "operation_id" = ?2 AND "environment_id" = ?3
                 AND "input_checksum" = ?4 AND "plan_checksum" = ?5
                 AND "status" = ?6 AND "updated_at_ms" = ?7
             ) AND (SELECT count(*) FROM "nozzle_operation_steps"
                    WHERE "operation_id" = ?2) = (SELECT count(*) FROM "expected_steps")
               AND (SELECT count(*) FROM "expected_steps" AS "expected"
                    JOIN "nozzle_operation_steps" AS "step"
                      ON "step"."operation_id" = ?2
                     AND "step"."step_id" = json_extract("expected"."value", '$.stepId')
                     AND "step"."state" = json_extract("expected"."value", '$.state')
                     AND "step"."fencing_token" IS json_extract("expected"."value", '$.fencingToken')
                     AND "step"."record_json" = json_extract("expected"."value", '$.recordJson')
                     AND "step"."lease_key" = ?35) = (SELECT count(*) FROM "expected_steps")
               AND EXISTS (
                 SELECT 1 FROM "nozzle_sagas"
                 WHERE "saga_id" = ?18 AND "operation_id" = ?2
                   AND "descriptor_checksum" = ?19 AND "input_checksum" = ?20
                   AND "state_version" = ?21 AND "status" = ?22
                   AND "last_effect_id" = ?23 AND "record_checksum" = ?24
                   AND "updated_at_ms" = ?25
               ) AND (SELECT count(*) FROM "nozzle_operation_effects"
                      WHERE "resource_kind" = 'saga' AND "resource_id" = ?18) = ?26
               AND EXISTS (
                 SELECT 1 FROM "nozzle_operation_effects"
                 WHERE "resource_kind" = 'saga' AND "resource_id" = ?18
                   AND "effect_id" = ?23 AND "to_state_version" = ?21
               ) AND (SELECT count(*) FROM "nozzle_saga_action_attempts"
                      WHERE "saga_id" = ?18) = ?27
               AND ((?27 = 0 AND NOT EXISTS (
                      SELECT 1 FROM "nozzle_saga_action_attempts" WHERE "saga_id" = ?18
                    )) OR EXISTS (
                      SELECT 1 FROM "nozzle_saga_action_attempts"
                      WHERE "saga_id" = ?18 AND "accepted_at_ms" = ?28 AND "attempt_id" = ?29
                      ORDER BY "accepted_at_ms" DESC, "attempt_id" COLLATE BINARY DESC LIMIT 1
                    ))
               AND (SELECT count(*) FROM "nozzle_operation_transitions"
                    WHERE "operation_id" = ?2) = ?30
               AND (SELECT count(*) FROM "nozzle_operation_transitions" AS "transition"
                    JOIN "nozzle_audit_log" AS "audit"
                      ON "audit"."environment_id" = ?3
                     AND "audit"."event_hash" = "transition"."audit_event_hash"
                    WHERE "transition"."operation_id" = ?2) = ?30
               AND EXISTS (
                 SELECT 1 FROM "nozzle_operation_transitions" AS "transition"
                 JOIN "nozzle_audit_log" AS "audit"
                   ON "audit"."environment_id" = ?3
                  AND "audit"."event_hash" = "transition"."audit_event_hash"
                 WHERE "transition"."operation_id" = ?2
                   AND "transition"."transition_id" = ?32 AND "audit"."sequence" = ?31
               ) AND EXISTS (
                 SELECT 1 FROM "nozzle_audit_log"
                 WHERE "environment_id" = ?3 AND "sequence" = ?33 AND "event_hash" = ?34
               ) AND EXISTS (
                 SELECT 1 FROM "nozzle_leases"
                 WHERE "lease_key" = ?35 AND "holder_id" = ?36
                   AND "acquisition_id" = ?37 AND "fencing_token" = ?38
                   AND "expires_at_ms" > ${SERVER_TIME_SQL}
               )
           ), "tail" AS MATERIALIZED (
             SELECT CAST("key" AS INTEGER) AS "ordinal", "value" FROM json_each(?1)
           )
           INSERT INTO "nozzle_operation_transitions"
             ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
              "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
              "lease_key", "holder_id", "acquisition_id", "created_at_ms")
           SELECT json_extract("tail"."value", '$.transitionId'), ?2,
                  json_extract("tail"."value", '$.stepId'),
                  json_extract("tail"."value", '$.fromRecordJson'),
                  json_extract("tail"."value", '$.toRecordJson'),
                  json_extract("tail"."value", '$.fromStatus'),
                  json_extract("tail"."value", '$.toStatus'),
                  json_extract("tail"."value", '$.auditEventHash'), ?38, ?35, ?36, ?37,
                  ${SERVER_TIME_SQL}
           FROM "tail" CROSS JOIN "guard" ORDER BY "tail"."ordinal"`,
        )
        .bind(
          transitionsJson,
          anchor.operationId,
          anchor.environmentId,
          anchor.operationInputChecksum,
          anchor.operationPlanChecksum,
          anchor.operationStatus,
          anchor.operationUpdatedAtMs,
          ...stepProjectionChunks,
          anchor.sagaId,
          anchor.sagaDescriptorChecksum,
          anchor.sagaInputChecksum,
          anchor.sagaStateVersion,
          anchor.sagaStatus,
          anchor.sagaLastEffectId,
          anchor.sagaRecordChecksum,
          anchor.sagaUpdatedAtMs,
          anchor.sagaEffectCount,
          anchor.sagaAttemptCount,
          anchor.sagaAttemptLastAcceptedAtMs,
          anchor.sagaAttemptLastId,
          anchor.operationTransitionCount,
          anchor.operationTransitionLastAuditSequence,
          anchor.operationTransitionLastId,
          anchor.auditHeadSequence,
          anchor.auditHeadEventHash,
          proof.leaseKey,
          proof.holderId,
          proof.acquisitionId,
          proof.fencingToken,
        ),
      this.#database
        .prepare(
          `WITH "tail" AS MATERIALIZED (
             SELECT "value" FROM json_each(?1)
           )
           UPDATE "nozzle_operation_steps" AS "step"
           SET "record_json" = (SELECT json_extract("value", '$.toRecordJson') FROM "tail"
                                  WHERE json_extract("value", '$.stepId') = "step"."step_id"),
               "state" = (SELECT json_extract("value", '$.toState') FROM "tail"
                            WHERE json_extract("value", '$.stepId') = "step"."step_id"),
               "fencing_token" = (SELECT json_extract("value", '$.toFencingToken') FROM "tail"
                                    WHERE json_extract("value", '$.stepId') = "step"."step_id"),
               "updated_at_ms" = ${SERVER_TIME_SQL}
           WHERE "step"."operation_id" = ?2 AND EXISTS (
             SELECT 1 FROM "tail"
             JOIN "nozzle_operation_transitions" AS "transition"
               ON "transition"."transition_id" = json_extract("tail"."value", '$.transitionId')
              AND "transition"."operation_id" = ?2
              AND "transition"."step_id" = "step"."step_id"
              AND "transition"."from_record_json" = json_extract("tail"."value", '$.fromRecordJson')
              AND "transition"."to_record_json" = json_extract("tail"."value", '$.toRecordJson')
              AND "transition"."audit_event_hash" = json_extract("tail"."value", '$.auditEventHash')
              AND "transition"."lease_key" = ?3 AND "transition"."holder_id" = ?4
              AND "transition"."acquisition_id" = ?5 AND "transition"."fencing_token" = ?6
             WHERE json_extract("tail"."value", '$.stepId') = "step"."step_id"
               AND "step"."record_json" = json_extract("tail"."value", '$.fromRecordJson')
           )`,
        )
        .bind(
          transitionsJson,
          anchor.operationId,
          proof.leaseKey,
          proof.holderId,
          proof.acquisitionId,
          proof.fencingToken,
        ),
      this.#database
        .prepare(
          `WITH "tail" AS MATERIALIZED (SELECT "value" FROM json_each(?1))
           UPDATE "nozzle_operations"
           SET "status" = ?2, "updated_at_ms" = ${SERVER_TIME_SQL}
           WHERE "operation_id" = ?3 AND "status" = ?4
             AND (SELECT count(*) FROM "tail"
                  JOIN "nozzle_operation_transitions" AS "transition"
                    ON "transition"."transition_id" = json_extract("tail"."value", '$.transitionId')
                   AND "transition"."operation_id" = ?3
                   AND "transition"."from_record_json" = json_extract("tail"."value", '$.fromRecordJson')
                   AND "transition"."to_record_json" = json_extract("tail"."value", '$.toRecordJson')
                   AND "transition"."audit_event_hash" = json_extract("tail"."value", '$.auditEventHash'))
                 = json_array_length(?1)
             AND (SELECT count(*) FROM "tail"
                  JOIN "nozzle_operation_steps" AS "step"
                    ON "step"."operation_id" = ?3
                   AND "step"."step_id" = json_extract("tail"."value", '$.stepId')
                   AND "step"."record_json" = json_extract("tail"."value", '$.toRecordJson'))
                 = json_array_length(?1)`,
        )
        .bind(transitionsJson, finalStatus, anchor.operationId, anchor.operationStatus),
      this.#database
        .prepare(
          `WITH "tail" AS MATERIALIZED (
             SELECT CAST("key" AS INTEGER) AS "ordinal", "value" FROM json_each(?1)
           )
           INSERT INTO "nozzle_audit_log"
             ("environment_id", "sequence", "previous_hash", "event_hash", "server_time_ms",
              "operation_id", "step_id", "event_json")
           SELECT ?2, json_extract("tail"."value", '$.sequence'),
                  json_extract("tail"."value", '$.previousHash'),
                  json_extract("tail"."value", '$.eventHash'),
                  json_extract("tail"."value", '$.serverTimeMs'), ?3,
                  json_extract("tail"."value", '$.stepId'),
                  json_extract("tail"."value", '$.eventJson')
           FROM "tail"
           WHERE EXISTS (
             SELECT 1 FROM "nozzle_operation_transitions" AS "transition"
             WHERE "transition"."transition_id" = json_extract("tail"."value", '$.transitionId')
               AND "transition"."operation_id" = ?3
               AND "transition"."step_id" = json_extract("tail"."value", '$.stepId')
               AND "transition"."audit_event_hash" = json_extract("tail"."value", '$.eventHash')
           ) AND EXISTS (
             SELECT 1 FROM "nozzle_operations" WHERE "operation_id" = ?3 AND "status" = ?4
           )
           ORDER BY "tail"."ordinal"`,
        )
        .bind(auditsJson, anchor.environmentId, anchor.operationId, finalStatus),
    ]
  }

  async #receiptRows(
    anchor: SagaHistoryAnchor,
    transitions: readonly TailTransition[],
  ): Promise<readonly PersistedTailRow[]> {
    const ids = boundedJson(
      transitions.map((transition) => transition.transitionId),
      "Saga terminal receipt identity set",
    )
    const result = await this.#database
      .prepare(
        `WITH "ids" AS MATERIALIZED (
           SELECT CAST("key" AS INTEGER) AS "ordinal", "value" AS "transition_id"
           FROM json_each(?1)
         )
         SELECT "ids"."ordinal", "transition"."transition_id", "transition"."operation_id",
                "transition"."step_id", "transition"."from_record_json",
                "transition"."to_record_json", "transition"."from_operation_status",
                "transition"."to_operation_status", "transition"."audit_event_hash",
                "transition"."fencing_token", "transition"."lease_key",
                "transition"."holder_id", "transition"."acquisition_id",
                "transition"."created_at_ms", "audit"."sequence" AS "audit_sequence",
                "audit"."event_json"
         FROM "ids"
         LEFT JOIN "nozzle_operation_transitions" AS "transition"
           ON "transition"."transition_id" = "ids"."transition_id"
         LEFT JOIN "nozzle_audit_log" AS "audit"
           ON "audit"."environment_id" = ?2
          AND "audit"."event_hash" = "transition"."audit_event_hash"
         ORDER BY "ids"."ordinal"`,
      )
      .bind(ids, anchor.environmentId)
      .all<PersistedTailRow>()
    if (result.success !== true || !Array.isArray(result.results)) {
      return intervention("Control D1 returned malformed terminal-tail receipt results.")
    }
    return result.results
  }

  async #head(anchor: SagaHistoryAnchor): Promise<TailHeadRow> {
    const row = await this.#database
      .prepare(
        `SELECT "operation"."environment_id", "operation"."input_checksum" AS "operation_input_checksum",
                "operation"."plan_checksum" AS "operation_plan_checksum",
                "operation"."status" AS "operation_status",
                "saga"."descriptor_checksum" AS "saga_descriptor_checksum",
                "saga"."input_checksum" AS "saga_input_checksum",
                "saga"."state_version" AS "saga_state_version", "saga"."status" AS "saga_status",
                "saga"."last_effect_id" AS "saga_last_effect_id",
                "saga"."record_checksum" AS "saga_record_checksum",
                "saga"."updated_at_ms" AS "saga_updated_at_ms",
                (SELECT count(*) FROM "nozzle_operation_steps"
                 WHERE "operation_id" = ?1) AS "step_count",
                (SELECT count(*) FROM "nozzle_operation_transitions"
                 WHERE "operation_id" = ?1) AS "transition_count",
                (SELECT count(*) FROM "nozzle_operation_transitions" AS "transition"
                 JOIN "nozzle_audit_log" AS "audit"
                   ON "audit"."environment_id" = "operation"."environment_id"
                  AND "audit"."event_hash" = "transition"."audit_event_hash"
                 WHERE "transition"."operation_id" = ?1) AS "transition_joined_count",
                (SELECT "transition"."transition_id"
                 FROM "nozzle_operation_transitions" AS "transition"
                 JOIN "nozzle_audit_log" AS "audit"
                   ON "audit"."environment_id" = "operation"."environment_id"
                  AND "audit"."event_hash" = "transition"."audit_event_hash"
                 WHERE "transition"."operation_id" = ?1
                 ORDER BY "audit"."sequence" DESC, "transition"."transition_id" COLLATE BINARY DESC
                 LIMIT 1) AS "transition_last_id",
                (SELECT "audit"."sequence"
                 FROM "nozzle_operation_transitions" AS "transition"
                 JOIN "nozzle_audit_log" AS "audit"
                   ON "audit"."environment_id" = "operation"."environment_id"
                  AND "audit"."event_hash" = "transition"."audit_event_hash"
                 WHERE "transition"."operation_id" = ?1
                 ORDER BY "audit"."sequence" DESC, "transition"."transition_id" COLLATE BINARY DESC
                 LIMIT 1) AS "transition_last_audit_sequence",
                (SELECT count(*) FROM "nozzle_operation_effects"
                 WHERE "resource_kind" = 'saga' AND "resource_id" = ?2) AS "effect_count",
                (SELECT "effect_id" FROM "nozzle_operation_effects"
                 WHERE "resource_kind" = 'saga' AND "resource_id" = ?2
                 ORDER BY "to_state_version" DESC, "effect_id" DESC LIMIT 1) AS "effect_last_id",
                (SELECT "to_state_version" FROM "nozzle_operation_effects"
                 WHERE "resource_kind" = 'saga' AND "resource_id" = ?2
                 ORDER BY "to_state_version" DESC, "effect_id" DESC LIMIT 1)
                   AS "effect_last_state_version",
                (SELECT count(*) FROM "nozzle_saga_action_attempts"
                 WHERE "saga_id" = ?2) AS "attempt_count",
                (SELECT "attempt_id" FROM "nozzle_saga_action_attempts" WHERE "saga_id" = ?2
                 ORDER BY "accepted_at_ms" DESC, "attempt_id" COLLATE BINARY DESC LIMIT 1)
                   AS "attempt_last_id",
                (SELECT "accepted_at_ms" FROM "nozzle_saga_action_attempts" WHERE "saga_id" = ?2
                 ORDER BY "accepted_at_ms" DESC, "attempt_id" COLLATE BINARY DESC LIMIT 1)
                   AS "attempt_last_accepted_at_ms"
         FROM "nozzle_operations" AS "operation"
         JOIN "nozzle_sagas" AS "saga" ON "saga"."operation_id" = "operation"."operation_id"
         WHERE "operation"."operation_id" = ?1 AND "saga"."saga_id" = ?2`,
      )
      .bind(anchor.operationId, anchor.sagaId)
      .first<TailHeadRow>()
    if (row === null) return intervention("The persisted terminal saga head disappeared.")
    return row
  }

  async #verifyReceipts(
    state: SagaTerminalCapabilityState,
    transitions: readonly TailTransition[],
    actorChecksum: string,
  ): Promise<OperationRecord | undefined> {
    const rows = await this.#receiptRows(state.finalProof.anchor, transitions)
    if (rows.length !== transitions.length) {
      return intervention("The terminal-tail receipt query returned incomplete identities.")
    }
    const present = rows.filter((row) => row.transition_id !== null).length
    if (present === 0) return undefined
    if (present !== rows.length) {
      return intervention("The atomic saga terminal tail is only partially durable.")
    }

    let operation = state.operation
    let receiptProof: LeaseProof | undefined
    let previousAudit: AuditEvent | undefined
    for (const [index, row] of rows.entries()) {
      const expected = transitions[index] as TailTransition
      if (
        row.ordinal !== index ||
        !safeInteger(row.audit_sequence, 1) ||
        !safeInteger(row.created_at_ms) ||
        !safeInteger(row.fencing_token, 1) ||
        [
          row.transition_id,
          row.operation_id,
          row.step_id,
          row.from_record_json,
          row.to_record_json,
          row.from_operation_status,
          row.to_operation_status,
          row.audit_event_hash,
          row.lease_key,
          row.holder_id,
          row.acquisition_id,
          row.event_json,
        ].some((value) => typeof value !== "string" || value.trim().length === 0)
      ) {
        return intervention("A persisted terminal-tail receipt is malformed.")
      }
      const proof = Object.freeze({
        acquisitionId: row.acquisition_id as string,
        fencingToken: row.fencing_token,
        holderId: row.holder_id as string,
        leaseKey: row.lease_key as string,
      })
      if (receiptProof === undefined) receiptProof = proof
      else if (
        receiptProof.acquisitionId !== proof.acquisitionId ||
        receiptProof.fencingToken !== proof.fencingToken ||
        receiptProof.holderId !== proof.holderId ||
        receiptProof.leaseKey !== proof.leaseKey
      ) {
        return intervention("The atomic saga terminal tail spans different lease proofs.")
      }
      const next = applyTailTransition(operation, expected, proof, state.settlementOutcome)
      const before = operation.steps[expected.stepId] as OperationStepRecord
      const after = next.steps[expected.stepId] as OperationStepRecord
      if (
        row.transition_id !== expected.transitionId ||
        row.operation_id !== state.operation.plan.operationId ||
        row.step_id !== expected.stepId ||
        row.from_record_json !== operationStepRecordJson(before) ||
        row.to_record_json !== operationStepRecordJson(after) ||
        row.from_operation_status !== operationStatus(operation) ||
        row.to_operation_status !== operationStatus(next)
      ) {
        return intervention("A saga terminal transition identity is bound to contradictory state.")
      }
      const event = await loadAuditEvent(
        parseJson(row.event_json as string, "Terminal-tail audit event"),
        this.#digest,
      )
      if (
        JSON.stringify(event) !== row.event_json ||
        event.actorChecksum !== actorChecksum ||
        event.environmentId !== state.finalProof.anchor.environmentId ||
        event.eventHash !== row.audit_event_hash ||
        event.eventType !== expected.eventType ||
        event.fencingToken !== proof.fencingToken ||
        event.idempotencyKey !== expected.transitionId ||
        event.operationId !== state.operation.plan.operationId ||
        event.payloadChecksum !== expected.payloadChecksum ||
        event.sequence !== row.audit_sequence ||
        event.serverTimeMs > row.created_at_ms ||
        event.stepId !== expected.stepId ||
        (previousAudit !== undefined &&
          (event.sequence !== previousAudit.sequence + 1 ||
            event.previousHash !== previousAudit.eventHash))
      ) {
        return intervention("A saga terminal transition has contradictory audit evidence.")
      }
      previousAudit = event
      operation = next
    }

    const last = rows.at(-1) as PersistedTailRow
    const head = await this.#head(state.finalProof.anchor)
    const anchor = state.finalProof.anchor
    if (
      head.environment_id !== anchor.environmentId ||
      head.operation_input_checksum !== anchor.operationInputChecksum ||
      head.operation_plan_checksum !== anchor.operationPlanChecksum ||
      head.operation_status !== state.settlementOutcome ||
      head.step_count !== state.operation.plan.steps.length ||
      head.transition_count !== anchor.operationTransitionCount + transitions.length ||
      head.transition_joined_count !== head.transition_count ||
      head.transition_last_id !== transitions.at(-1)?.transitionId ||
      head.transition_last_audit_sequence !== last.audit_sequence ||
      head.saga_descriptor_checksum !== anchor.sagaDescriptorChecksum ||
      head.saga_input_checksum !== anchor.sagaInputChecksum ||
      head.saga_state_version !== anchor.sagaStateVersion ||
      head.saga_status !== anchor.sagaStatus ||
      head.saga_last_effect_id !== anchor.sagaLastEffectId ||
      head.saga_record_checksum !== anchor.sagaRecordChecksum ||
      head.saga_updated_at_ms !== anchor.sagaUpdatedAtMs ||
      head.effect_count !== anchor.sagaEffectCount ||
      head.effect_last_id !== anchor.sagaLastEffectId ||
      head.effect_last_state_version !== anchor.sagaStateVersion ||
      head.attempt_count !== anchor.sagaAttemptCount ||
      head.attempt_last_accepted_at_ms !== anchor.sagaAttemptLastAcceptedAtMs ||
      head.attempt_last_id !== anchor.sagaAttemptLastId
    ) {
      return intervention("The durable saga terminal tail does not exactly descend from its proof.")
    }
    const loaded = await this.#operations.get(anchor.operationId)
    if (loaded === undefined || !sameOperation(loaded.operation, operation)) {
      return intervention("The saga terminal receipts lack their exact final operation projection.")
    }
    return loaded.operation
  }

  async persistTerminalTail(input: PersistSagaTerminalTailInput): Promise<OperationRecord> {
    const snapshot = snapshotInput(input)
    boundedText(snapshot.actorChecksum, "Saga terminal actor checksum")
    const state = loadSagaTerminalCapability(snapshot.capability)
    const settlement = state.operation.steps[SAGA_SETTLE_OPERATION_STEP_ID]
    if (
      settlement?.state === "failed" ||
      settlement?.state === "intervention_required" ||
      settlement?.state === "succeeded"
    ) {
      await this.#history.assertAnchorCurrent(state.finalProof.anchor)
      const loaded = await this.#operations.get(state.operation.plan.operationId)
      if (loaded === undefined || !sameOperation(loaded.operation, state.operation)) {
        return intervention("The already-settled saga projection changed after verification.")
      }
      return loaded.operation
    }

    const built = buildTail(state, snapshot.proof)
    const existing = await this.#verifyReceipts(state, built.transitions, snapshot.actorChecksum)
    if (existing !== undefined) return existing
    const stepProjectionChunks = boundedJsonChunks(
      state.operation.plan.steps.map((step) => {
        const record = state.operation.steps[step.stepId] as OperationStepRecord
        return {
          fencingToken: record.fencingToken ?? null,
          recordJson: operationStepRecordJson(record),
          state: record.state,
          stepId: step.stepId,
        }
      }),
      "Saga terminal step projection",
    )

    for (let attempt = 0; attempt < MAX_PERSIST_ATTEMPTS; attempt += 1) {
      const snapshotAudit = await this.#auditSnapshot(state.finalProof.anchor.environmentId)
      let previous = snapshotAudit.previous
      const audits: AuditEvent[] = []
      for (const transition of built.transitions) {
        const event = await appendAuditEvent(
          previous,
          {
            actorChecksum: snapshot.actorChecksum,
            environmentId: state.finalProof.anchor.environmentId,
            eventType: transition.eventType,
            fencingToken: snapshot.proof.fencingToken,
            idempotencyKey: transition.transitionId,
            operationId: state.operation.plan.operationId,
            payloadChecksum: transition.payloadChecksum,
            serverTimeMs: snapshotAudit.nowMs,
            stepId: transition.stepId,
          },
          this.#digest,
        )
        audits.push(event)
        previous = event
      }
      const transitionsJson = boundedJson(
        built.transitions.map((transition, index) => ({
          ...transition,
          auditEventHash: (audits[index] as AuditEvent).eventHash,
          toFencingToken:
            (built.operation.steps[transition.stepId] as OperationStepRecord).fencingToken ?? null,
        })),
        "Saga terminal transition batch",
      )
      const auditsJson = boundedJson(
        audits.map((audit, index) => ({
          eventHash: audit.eventHash,
          eventJson: JSON.stringify(audit),
          previousHash: audit.previousHash,
          sequence: audit.sequence,
          serverTimeMs: audit.serverTimeMs,
          stepId: audit.stepId,
          transitionId: (built.transitions[index] as TailTransition).transitionId,
        })),
        "Saga terminal audit batch",
      )
      const statements = this.#statements(
        state,
        snapshot.proof,
        transitionsJson,
        auditsJson,
        stepProjectionChunks,
        state.settlementOutcome,
      )
      let results: readonly ControlRunResult[] | undefined
      try {
        results = await this.#database.batch(statements)
      } catch {
        // An audit-head race or lost response is resolved from immutable receipts below.
      }
      let metadataFailure: unknown
      let disposition: "committed" | "guard_rejected" | undefined
      if (results !== undefined) {
        try {
          disposition = validateMutationResults(results, built.transitions.length)
        } catch (error) {
          metadataFailure = error
        }
      }
      const persisted = await this.#verifyReceipts(state, built.transitions, snapshot.actorChecksum)
      if (persisted !== undefined) return persisted
      if (metadataFailure !== undefined) throw metadataFailure
      if (disposition === "guard_rejected") {
        await this.#history.assertAnchorCurrent(state.finalProof.anchor)
        const current = await this.#operations.get(state.operation.plan.operationId)
        if (current === undefined || !sameOperation(current.operation, state.operation)) {
          return intervention("The saga terminal projection changed behind its verified anchor.")
        }
        await this.#leases.authorizeAt(snapshot.proof)
        return intervention("The saga terminal transaction guard rejected verified current state.")
      }
      await this.#history.assertAnchorCurrent(state.finalProof.anchor)
      await this.#leases.authorizeAt(snapshot.proof)
    }
    return intervention("Saga terminal persistence exceeded the bounded audit-race retry budget.")
  }
}
