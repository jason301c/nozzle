import {
  appendAuditEvent,
  beginOperationStep,
  beginSagaAction,
  createSagaRecord,
  type DigestFunction,
  type LeaseProof,
  loadAuditEvent,
  NozzleError,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  operationStatus,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordStepFailure,
  recordStepSuccess,
  type SagaActionPhase,
  type SagaActionRecord,
  type SagaBeginDecision,
  type SagaDescriptor,
  type SagaRecord,
  sagaCommitment,
} from "@nozzle/core"
import type {
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "./database.js"
import { D1LeaseStore } from "./lease-store.js"
import {
  D1OperationStore,
  type LoadedOperation,
  operationStepRecordJson,
  operationTransitionIdentity,
} from "./operation-store.js"
import { D1SagaAttemptStore, type SagaAttemptRecord } from "./saga-attempt-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "./saga-store.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const RECORD_DOMAIN = "nozzle.saga-record.v1"
const COORDINATOR_ID_DOMAIN = "nozzle.saga-coordinator-id.v1"
const MAX_ATTEMPTS = 16
const MAX_IDENTITY_BYTES = 512

interface AuditSnapshotRow {
  readonly event_json: string | null
  readonly now_ms: number
}

interface TransitionReceiptRow {
  readonly acquisition_id: string
  readonly audit_event_hash: string
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

interface EffectReceiptRow {
  readonly acquisition_id: string
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

interface CoupledTransition {
  readonly actorChecksum: string
  readonly afterOperation: OperationRecord
  readonly afterSaga: SagaRecord
  readonly auditEventType: string
  readonly beforeOperation: LoadedOperation
  readonly beforeSaga: SagaRecord | undefined
  readonly effectId: string
  readonly effectKind: string
  readonly evidenceChecksum: string
  readonly proof: LeaseProof
  readonly stepId: string
  readonly transitionId: string
}

export interface InitializeSagaInput {
  readonly actorChecksum: string
  readonly attemptId: string
  readonly deadlineAtMs: number
  readonly descriptor: SagaDescriptor
  readonly evidenceChecksum: string
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly observedPostconditionChecksum: string
  readonly operationId: string
  readonly proof: LeaseProof
  readonly resultChecksum: string
  readonly sagaId: string
  readonly stepInputChecksums: Readonly<Record<string, string>>
}

export interface BeginCoordinatedSagaActionInput {
  readonly actorChecksum: string
  readonly attemptId: string
  readonly operationId: string
  readonly phase: SagaActionPhase
  readonly proof: LeaseProof
  readonly sagaId: string
  readonly stepId: string
}

export type SettleCoordinatedSagaActionInput = BeginCoordinatedSagaActionInput

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function boundedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    return configuration(`${label} exceeds the durable coordinator identity limit.`)
  }
  return value
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

function encode(value: unknown): string {
  return JSON.stringify(canonicalValue(value))
}

function frame(domain: string, values: readonly string[]): Uint8Array {
  const parts = [domain, ...values].map((value) => new TextEncoder().encode(value))
  const output = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0))
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function checkedDigest(
  digest: DigestFunction,
  domain: string,
  values: readonly string[],
): Promise<string> {
  return boundedText(await digest(frame(domain, values)), "Saga coordinator checksum")
}

function mutationResults(results: readonly ControlRunResult[], expected: number): void {
  if (!Array.isArray(results) || results.length !== expected) {
    intervention("Control D1 returned an incomplete coupled saga batch result.")
  }
  for (const result of results) {
    const changes = result.meta.changes
    if (
      result.success !== true ||
      !Number.isSafeInteger(changes) ||
      (changes as number) < 0 ||
      (changes as number) > 1
    ) {
      intervention("Control D1 returned malformed coupled saga mutation metadata.")
    }
  }
}

function sameOperation(left: OperationRecord, right: OperationRecord): boolean {
  return encode(left) === encode(right)
}

function sameSaga(left: SagaRecord, right: SagaRecord): boolean {
  return encode(left) === encode(right)
}

function exactColumns(
  row: Readonly<Record<string, unknown>>,
  expected: Readonly<Record<string, unknown>>,
): boolean {
  return Object.entries(expected).every(([key, value]) => row[key] === value)
}

function alignedBeginDecision(
  operationDisposition: "blocked" | "execute" | "in_progress" | "reconcile" | "replay",
  sagaDisposition: SagaBeginDecision["disposition"],
): boolean {
  const expected: Readonly<Record<typeof operationDisposition, SagaBeginDecision["disposition"]>> =
    {
      blocked: "replay_failure",
      execute: "execute",
      in_progress: "in_progress",
      reconcile: "observe",
      replay: "replay_success",
    }
  return expected[operationDisposition] === sagaDisposition
}

function terminalAttempt(
  receipt: SagaAttemptRecord,
): asserts receipt is Exclude<SagaAttemptRecord, { readonly state: "accepted" }> {
  if (receipt.state === "accepted") resume("The saga action attempt has no terminal receipt.")
}

function classifiedEffectKind(
  phase: SagaActionPhase,
  receipt: Exclude<SagaAttemptRecord, { readonly state: "accepted" }>,
  action: SagaRecord["steps"][string][SagaActionPhase],
): string {
  if (receipt.state === "confirmed") return `action:${phase}:success`
  if (receipt.state === "unknown") return `action:${phase}:failure:unknown`
  const classification =
    receipt.state === "not_applied" && action.state === "retryable_failed"
      ? "definitely_not_applied_retryable"
      : "definitely_not_applied_terminal"
  return `action:${phase}:failure:${classification}`
}

export class D1SagaCoordinatorStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction
  readonly #leases: D1LeaseStore
  readonly #operations: D1OperationStore
  readonly #attempts: D1SagaAttemptStore
  readonly #sagas: D1SagaStore

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional Control D1 binding is required for saga coordination.")
    }
    if (typeof digest !== "function") configuration("A saga coordinator digest is required.")
    this.#database = database
    this.#digest = digest
    this.#leases = new D1LeaseStore(database)
    this.#operations = new D1OperationStore(database, digest)
    this.#attempts = new D1SagaAttemptStore(database, digest)
    this.#sagas = new D1SagaStore(database, digest)
  }

  async #auditSnapshot(environmentId: string) {
    const row = await this.#database
      .prepare(
        `SELECT ${SERVER_TIME_SQL} AS "now_ms",
          (SELECT "event_json" FROM "nozzle_audit_log"
           WHERE "environment_id" = ?1 ORDER BY "sequence" DESC LIMIT 1) AS "event_json"`,
      )
      .bind(environmentId)
      .first<AuditSnapshotRow>()
    if (!row || !Number.isSafeInteger(row.now_ms) || row.now_ms < 0) {
      return intervention("Control D1 returned malformed authoritative saga coordinator time.")
    }
    if (row.event_json === null) return Object.freeze({ nowMs: row.now_ms, previous: undefined })
    let parsed: unknown
    try {
      parsed = JSON.parse(row.event_json)
    } catch {
      return intervention("The persisted coordinator audit head is invalid JSON.")
    }
    const previous = await loadAuditEvent(parsed, this.#digest)
    if (previous.environmentId !== environmentId) {
      return intervention("The persisted coordinator audit head belongs to another environment.")
    }
    return Object.freeze({ nowMs: row.now_ms, previous })
  }

  async #identity(kind: string, values: readonly string[]): Promise<string> {
    return `${kind}:${await checkedDigest(this.#digest, COORDINATOR_ID_DOMAIN, [kind, ...values])}`
  }

  async #recordChecksum(recordJson: string): Promise<string> {
    return checkedDigest(this.#digest, RECORD_DOMAIN, [recordJson])
  }

  async #operation(operationId: string): Promise<LoadedOperation> {
    const operation = await this.#operations.get(boundedText(operationId, "Operation ID"))
    if (operation === undefined) return resume("The saga operation does not exist.")
    return operation
  }

  async #saga(sagaId: string, operationId: string): Promise<SagaRecord | undefined> {
    const id = boundedText(sagaId, "Saga ID")
    const record = await this.#sagas.get(id)
    if (record === undefined) return undefined
    const row = await this.#database
      .prepare(`SELECT "operation_id" FROM "nozzle_sagas" WHERE "saga_id" = ?1`)
      .bind(id)
      .first<{ readonly operation_id: string }>()
    if (row?.operation_id !== operationId) {
      return intervention("The saga projection belongs to a different operation.")
    }
    return record
  }

  #statements(
    input: CoupledTransition,
    audit: Awaited<ReturnType<typeof appendAuditEvent>>,
    recordJson: string,
    recordChecksum: string,
  ): readonly ControlStatement[] {
    const beforeRecord = input.beforeOperation.operation.steps[input.stepId] as OperationStepRecord
    const afterRecord = input.afterOperation.steps[input.stepId] as OperationStepRecord
    const beforeJson = operationStepRecordJson(beforeRecord)
    const afterJson = operationStepRecordJson(afterRecord)
    const fromStatus = operationStatus(input.beforeOperation.operation)
    const toStatus = operationStatus(input.afterOperation)
    const beforeSagaJson = input.beforeSaga === undefined ? undefined : encode(input.beforeSaga)
    const transition = this.#database
      .prepare(
        `INSERT INTO "nozzle_operation_transitions"
         ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
          "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
          "lease_key", "holder_id", "acquisition_id", "created_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_operation_steps"
           WHERE "operation_id" = ?2 AND "step_id" = ?3 AND "record_json" = ?4
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_operations"
           WHERE "operation_id" = ?2 AND "status" = ?6
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?10 AND "holder_id" = ?11 AND "acquisition_id" = ?12
             AND "fencing_token" = ?9 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )`,
      )
      .bind(
        input.transitionId,
        input.beforeOperation.operation.plan.operationId,
        input.stepId,
        beforeJson,
        afterJson,
        fromStatus,
        toStatus,
        audit.eventHash,
        input.proof.fencingToken,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
      )
    const effect = this.#database
      .prepare(
        `INSERT INTO "nozzle_operation_effects"
         ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
          "resource_id", "effect_kind", "from_state_version", "to_state_version",
          "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
          "acquisition_id", "fencing_token", "created_at_ms")
         VALUES (?1, ?2, ?3, ?4, 'saga', ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                 ?12, ?13, ?14, ?15, ${SERVER_TIME_SQL})`,
      )
      .bind(
        input.effectId,
        input.transitionId,
        input.beforeOperation.operation.plan.operationId,
        input.stepId,
        input.afterSaga.sagaId,
        input.effectKind,
        input.beforeSaga?.stateVersion ?? null,
        input.afterSaga.stateVersion,
        input.evidenceChecksum,
        recordChecksum,
        recordJson,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
        input.proof.fencingToken,
      )
    const saga =
      input.beforeSaga === undefined
        ? this.#database
            .prepare(
              `INSERT INTO "nozzle_sagas"
               ("saga_id", "operation_id", "descriptor_id", "descriptor_version",
                "descriptor_checksum", "descriptor_json", "idempotency_key", "input_checksum",
                "deadline_at_ms", "status", "commitment", "termination_cause",
                "termination_requested_at_ms", "state_version", "last_evidence_checksum",
                "last_effect_id", "record_checksum", "record_json", "created_at_ms",
                "updated_at_ms")
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                       ?15, ?16, ?17, ?18, ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL})`,
            )
            .bind(
              input.afterSaga.sagaId,
              input.beforeOperation.operation.plan.operationId,
              input.afterSaga.descriptor.descriptorId,
              input.afterSaga.descriptor.version,
              input.afterSaga.descriptor.descriptorChecksum,
              encode(input.afterSaga.descriptor),
              input.afterSaga.idempotencyKey,
              input.afterSaga.inputChecksum,
              input.afterSaga.deadlineAtMs,
              input.afterSaga.status,
              sagaCommitment(input.afterSaga),
              input.afterSaga.terminationCause,
              input.afterSaga.terminationRequestedAtMs,
              input.afterSaga.stateVersion,
              input.evidenceChecksum,
              input.effectId,
              recordChecksum,
              recordJson,
            )
        : this.#database
            .prepare(
              `UPDATE "nozzle_sagas"
               SET "status" = ?2, "commitment" = ?3, "termination_cause" = ?4,
                   "termination_requested_at_ms" = ?5, "state_version" = ?6,
                   "last_evidence_checksum" = ?7, "last_effect_id" = ?8,
                   "record_checksum" = ?9, "record_json" = ?10,
                   "updated_at_ms" = ${SERVER_TIME_SQL}
               WHERE "saga_id" = ?1 AND "state_version" = ?11
                 AND "record_json" = ?12`,
            )
            .bind(
              input.afterSaga.sagaId,
              input.afterSaga.status,
              sagaCommitment(input.afterSaga),
              input.afterSaga.terminationCause,
              input.afterSaga.terminationRequestedAtMs,
              input.afterSaga.stateVersion,
              input.evidenceChecksum,
              input.effectId,
              recordChecksum,
              recordJson,
              input.beforeSaga.stateVersion,
              beforeSagaJson as string,
            )
    const step = this.#database
      .prepare(
        `UPDATE "nozzle_operation_steps"
         SET "record_json" = ?1, "state" = ?2, "fencing_token" = ?3,
             "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "operation_id" = ?4 AND "step_id" = ?5 AND "record_json" = ?6
           AND EXISTS (
             SELECT 1 FROM "nozzle_operation_transitions"
             WHERE "transition_id" = ?7 AND "operation_id" = ?4 AND "step_id" = ?5
               AND "from_record_json" = ?6 AND "to_record_json" = ?1
               AND "audit_event_hash" = ?8 AND "fencing_token" = ?9
           ) AND EXISTS (
             SELECT 1 FROM "nozzle_leases"
             WHERE "lease_key" = ?10 AND "holder_id" = ?11 AND "acquisition_id" = ?12
               AND "fencing_token" = ?9 AND "expires_at_ms" > ${SERVER_TIME_SQL}
           )`,
      )
      .bind(
        afterJson,
        afterRecord.state,
        afterRecord.fencingToken as number,
        input.beforeOperation.operation.plan.operationId,
        input.stepId,
        beforeJson,
        input.transitionId,
        audit.eventHash,
        input.proof.fencingToken,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
      )
    const operation = this.#database
      .prepare(
        `UPDATE "nozzle_operations"
         SET "status" = ?1, "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "operation_id" = ?2 AND "status" = ?3
           AND EXISTS (
             SELECT 1 FROM "nozzle_operation_transitions"
             WHERE "transition_id" = ?4 AND "operation_id" = ?2
               AND "from_operation_status" = ?3 AND "to_operation_status" = ?1
               AND "audit_event_hash" = ?5
           ) AND EXISTS (
             SELECT 1 FROM "nozzle_operation_steps"
             WHERE "operation_id" = ?2 AND "step_id" = ?6 AND "record_json" = ?7
           )`,
      )
      .bind(
        toStatus,
        input.beforeOperation.operation.plan.operationId,
        fromStatus,
        input.transitionId,
        audit.eventHash,
        input.stepId,
        afterJson,
      )
    const auditStatement = this.#database
      .prepare(
        `INSERT INTO "nozzle_audit_log"
         ("environment_id", "sequence", "previous_hash", "event_hash", "server_time_ms",
          "operation_id", "step_id", "event_json")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_operation_transitions"
           WHERE "transition_id" = ?9 AND "operation_id" = ?6 AND "step_id" = ?7
             AND "audit_event_hash" = ?4
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_operation_effects"
           WHERE "effect_id" = ?10 AND "transition_id" = ?9 AND "record_checksum" = ?11
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_operation_steps"
           WHERE "operation_id" = ?6 AND "step_id" = ?7 AND "record_json" = ?12
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_operations"
           WHERE "operation_id" = ?6 AND "status" = ?13
         )`,
      )
      .bind(
        input.beforeOperation.environmentId,
        audit.sequence,
        audit.previousHash,
        audit.eventHash,
        audit.serverTimeMs,
        input.beforeOperation.operation.plan.operationId,
        input.stepId,
        JSON.stringify(audit),
        input.transitionId,
        input.effectId,
        recordChecksum,
        afterJson,
        toStatus,
      )
    return Object.freeze([transition, effect, saga, step, operation, auditStatement])
  }

  async #verify(input: CoupledTransition, recordJson: string, recordChecksum: string) {
    const beforeRecord = input.beforeOperation.operation.steps[input.stepId] as OperationStepRecord
    const afterRecord = input.afterOperation.steps[input.stepId] as OperationStepRecord
    const beforeJson = operationStepRecordJson(beforeRecord)
    const afterJson = operationStepRecordJson(afterRecord)
    const fromStatus = operationStatus(input.beforeOperation.operation)
    const toStatus = operationStatus(input.afterOperation)
    const transition = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
      .bind(input.transitionId)
      .first<TransitionReceiptRow>()
    if (transition === null) return false
    if (
      !exactColumns(transition as unknown as Record<string, unknown>, {
        acquisition_id: input.proof.acquisitionId,
        fencing_token: input.proof.fencingToken,
        from_operation_status: fromStatus,
        from_record_json: beforeJson,
        holder_id: input.proof.holderId,
        lease_key: input.proof.leaseKey,
        operation_id: input.beforeOperation.operation.plan.operationId,
        step_id: input.stepId,
        to_operation_status: toStatus,
        to_record_json: afterJson,
        transition_id: input.transitionId,
      })
    ) {
      return intervention("The coupled saga transition receipt is contradictory.")
    }
    const effect = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?1`)
      .bind(input.effectId)
      .first<EffectReceiptRow>()
    if (
      effect === null ||
      !exactColumns(effect as unknown as Record<string, unknown>, {
        acquisition_id: input.proof.acquisitionId,
        effect_id: input.effectId,
        effect_kind: input.effectKind,
        evidence_checksum: input.evidenceChecksum,
        fencing_token: input.proof.fencingToken,
        from_state_version: input.beforeSaga?.stateVersion ?? null,
        holder_id: input.proof.holderId,
        lease_key: input.proof.leaseKey,
        operation_id: input.beforeOperation.operation.plan.operationId,
        record_checksum: recordChecksum,
        record_json: recordJson,
        resource_id: input.afterSaga.sagaId,
        resource_kind: "saga",
        step_id: input.stepId,
        to_state_version: input.afterSaga.stateVersion,
        transition_id: input.transitionId,
      })
    ) {
      return intervention("The coupled saga operation-effect receipt is missing or contradictory.")
    }
    const operation = await this.#operations.get(input.beforeOperation.operation.plan.operationId)
    const saga = await this.#sagas.get(input.afterSaga.sagaId)
    if (operation === undefined || !sameOperation(operation.operation, input.afterOperation)) {
      return intervention("The coupled operation projection does not match its receipt.")
    }
    if (saga === undefined || !sameSaga(saga, input.afterSaga)) {
      return intervention("The coupled saga projection does not match its receipt.")
    }
    const audit = await this.#database
      .prepare(
        `SELECT 1 AS "present" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(input.beforeOperation.environmentId, transition.audit_event_hash)
      .first<{ readonly present: number }>()
    if (audit?.present !== 1) {
      return intervention("The coupled saga transition lacks its exact audit event.")
    }
    return true
  }

  async #commit(input: CoupledTransition): Promise<boolean> {
    const recordJson = encode(input.afterSaga)
    const recordChecksum = await this.#recordChecksum(recordJson)
    const auditSnapshot = await this.#auditSnapshot(input.beforeOperation.environmentId)
    const audit = await appendAuditEvent(
      auditSnapshot.previous,
      {
        actorChecksum: input.actorChecksum,
        environmentId: input.beforeOperation.environmentId,
        eventType: input.auditEventType,
        fencingToken: input.proof.fencingToken,
        idempotencyKey: input.transitionId,
        operationId: input.beforeOperation.operation.plan.operationId,
        payloadChecksum: input.evidenceChecksum,
        serverTimeMs: auditSnapshot.nowMs,
        stepId: input.stepId,
      },
      this.#digest,
    )
    const statements = this.#statements(input, audit, recordJson, recordChecksum)
    let results: readonly ControlRunResult[] | undefined
    try {
      results = await this.#database.batch(statements)
    } catch {
      // A conflict may be an exact concurrent winner; immutable receipts decide below.
    }
    if (results !== undefined) mutationResults(results, statements.length)
    return this.#verify(input, recordJson, recordChecksum)
  }

  async initializeSaga(input: InitializeSagaInput): Promise<SagaRecord> {
    boundedText(input.actorChecksum, "Saga coordinator actor checksum")
    boundedText(input.attemptId, "Saga initialization attempt ID")
    boundedText(input.evidenceChecksum, "Saga initialization evidence checksum")
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const beforeOperation = await this.#operation(input.operationId)
      const authorized = await this.#leases.authorizeAt(input.proof)
      const existing = await this.#saga(input.sagaId, input.operationId)
      const fresh = createSagaRecord({
        deadlineAtMs: input.deadlineAtMs,
        descriptor: input.descriptor,
        idempotencyKey: input.idempotencyKey,
        inputChecksum: input.inputChecksum,
        sagaId: input.sagaId,
        serverTimeMs: authorized.serverTimeMs,
        stepInputChecksums: input.stepInputChecksums,
      })
      const afterOperation = recordStepSuccess(beforeOperation.operation, {
        attemptId: input.attemptId,
        observedPostconditionChecksum: input.observedPostconditionChecksum,
        resultChecksum: input.resultChecksum,
        stepId: SAGA_INIT_OPERATION_STEP_ID,
      })
      if (existing !== undefined) {
        if (
          !sameSaga(existing, fresh) ||
          !sameOperation(beforeOperation.operation, afterOperation)
        ) {
          return intervention("Saga initialization replay contradicts durable state.")
        }
        return existing
      }
      if (
        beforeOperation.operation.steps[SAGA_INIT_OPERATION_STEP_ID]?.fencingToken !==
        input.proof.fencingToken
      ) {
        return resume("Saga initialization was fenced by a newer lease owner.")
      }
      const transitionId = operationTransitionIdentity("succeeded", [
        input.operationId,
        SAGA_INIT_OPERATION_STEP_ID,
        input.attemptId,
      ])
      const effectId = await this.#identity("saga-effect", [
        transitionId,
        input.sagaId,
        "create",
        "0",
      ])
      if (
        await this.#commit({
          actorChecksum: input.actorChecksum,
          afterOperation,
          afterSaga: fresh,
          auditEventType: "saga.initialized",
          beforeOperation,
          beforeSaga: undefined,
          effectId,
          effectKind: "create",
          evidenceChecksum: input.evidenceChecksum,
          proof: input.proof,
          stepId: SAGA_INIT_OPERATION_STEP_ID,
          transitionId,
        })
      ) {
        return fresh
      }
    }
    return intervention("Saga initialization exceeded the bounded coupled retry budget.")
  }

  async beginAction(input: BeginCoordinatedSagaActionInput): Promise<SagaBeginDecision> {
    boundedText(input.actorChecksum, "Saga coordinator actor checksum")
    boundedText(input.attemptId, "Saga action attempt ID")
    const operationStepId = sagaActionOperationStepId(input.stepId, input.phase)
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      const action = beforeSaga.steps[input.stepId]?.[input.phase]
      if (action === undefined) return resume("The saga action does not exist.")
      const planStep = beforeOperation.operation.plan.steps.find(
        (step) => step.stepId === operationStepId,
      )
      if (planStep === undefined) return intervention("The saga action lacks an operation step.")
      const authorized = await this.#leases.authorizeAt(input.proof)
      const operationDecision = beginOperationStep(beforeOperation.operation, {
        attemptId: input.attemptId,
        idempotencyKey: action.idempotencyKey,
        lease: authorized.record,
        leaseProof: input.proof,
        observedPreconditionChecksum: planStep.preconditionChecksum,
        serverTimeMs: authorized.serverTimeMs,
        stepId: operationStepId,
      })
      const sagaDecision = beginSagaAction(beforeSaga, {
        attemptId: input.attemptId,
        idempotencyKey: action.idempotencyKey,
        phase: input.phase,
        serverTimeMs: authorized.serverTimeMs,
        stepId: input.stepId,
      })
      if (!alignedBeginDecision(operationDecision.disposition, sagaDecision.disposition)) {
        return intervention("The operation and saga begin decisions diverged.")
      }
      if (operationDecision.disposition !== "execute" || sagaDecision.disposition !== "execute") {
        return sagaDecision
      }
      const transitionId = operationTransitionIdentity("accepted", [
        input.operationId,
        operationStepId,
        input.attemptId,
      ])
      const effectKind = `action:${input.phase}:begin`
      const evidenceChecksum = await checkedDigest(this.#digest, COORDINATOR_ID_DOMAIN, [
        "begin-evidence",
        transitionId,
        input.sagaId,
        input.stepId,
        input.phase,
        input.attemptId,
      ])
      const effectId = await this.#identity("saga-effect", [
        transitionId,
        input.sagaId,
        effectKind,
        sagaDecision.saga.stateVersion.toString(10),
      ])
      if (
        await this.#commit({
          actorChecksum: input.actorChecksum,
          afterOperation: operationDecision.operation,
          afterSaga: sagaDecision.saga,
          auditEventType: "saga.action.started",
          beforeOperation,
          beforeSaga,
          effectId,
          effectKind,
          evidenceChecksum,
          proof: input.proof,
          stepId: operationStepId,
          transitionId,
        })
      ) {
        return sagaDecision
      }
    }
    return intervention("Beginning a saga action exceeded the bounded coupled retry budget.")
  }

  async settleActionFromReceipt(input: SettleCoordinatedSagaActionInput): Promise<SagaRecord> {
    boundedText(input.actorChecksum, "Saga coordinator actor checksum")
    boundedText(input.attemptId, "Saga action attempt ID")
    const operationStepId = sagaActionOperationStepId(input.stepId, input.phase)
    const receipt = await this.#attempts.get(input.attemptId)
    if (receipt === undefined) return resume("The saga action attempt was not durably accepted.")
    terminalAttempt(receipt)
    if (receipt.state === "indeterminate") {
      return intervention("An effect receipt cannot be indeterminate.")
    }
    if (
      receipt.sagaId !== input.sagaId ||
      receipt.operationId !== input.operationId ||
      receipt.operationStepId !== operationStepId ||
      receipt.sagaStepId !== input.stepId ||
      receipt.phase !== input.phase ||
      receipt.purpose !== "effect"
    ) {
      return intervention("The terminal saga receipt belongs to a different action.")
    }
    if (
      receipt.leaseKey !== input.proof.leaseKey ||
      receipt.holderId !== input.proof.holderId ||
      receipt.acquisitionId !== input.proof.acquisitionId ||
      receipt.fencingToken !== input.proof.fencingToken
    ) {
      return resume("The terminal saga receipt was accepted under a different lease fence.")
    }
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      const action = beforeSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      const operationAction = beforeOperation.operation.steps[
        operationStepId
      ] as OperationStepRecord
      if (action.state !== "running") {
        if (action.lastAttemptId !== input.attemptId) {
          return resume("The terminal saga receipt is not the current action attempt.")
        }
        const transitionKind = operationAction.state === "succeeded" ? "succeeded" : "failed"
        const transitionId = operationTransitionIdentity(transitionKind, [
          input.operationId,
          operationStepId,
          input.attemptId,
        ])
        const effectKind = classifiedEffectKind(input.phase, receipt, action)
        const transition = await this.#database
          .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
          .bind(transitionId)
          .first<TransitionReceiptRow>()
        if (
          transition === null ||
          !exactColumns(transition as unknown as Record<string, unknown>, {
            acquisition_id: receipt.acquisitionId,
            fencing_token: receipt.fencingToken,
            holder_id: receipt.holderId,
            lease_key: receipt.leaseKey,
            operation_id: input.operationId,
            step_id: operationStepId,
            to_operation_status: operationStatus(beforeOperation.operation),
            to_record_json: operationStepRecordJson(operationAction),
            transition_id: transitionId,
          })
        ) {
          return intervention("The classified saga action lacks its exact operation transition.")
        }
        const recordJson = encode(beforeSaga)
        const recordChecksum = await this.#recordChecksum(recordJson)
        const effectId = await this.#identity("saga-effect", [
          transitionId,
          input.sagaId,
          effectKind,
          beforeSaga.stateVersion.toString(10),
        ])
        const effect = await this.#database
          .prepare(
            `SELECT * FROM "nozzle_operation_effects"
             WHERE "transition_id" = ?1 AND "resource_kind" = 'saga' AND "resource_id" = ?2`,
          )
          .bind(transitionId, input.sagaId)
          .first<EffectReceiptRow>()
        if (
          effect === null ||
          !exactColumns(effect as unknown as Record<string, unknown>, {
            acquisition_id: receipt.acquisitionId,
            effect_id: effectId,
            effect_kind: effectKind,
            evidence_checksum: receipt.outcomeChecksum,
            fencing_token: receipt.fencingToken,
            from_state_version: beforeSaga.stateVersion - 1,
            holder_id: receipt.holderId,
            lease_key: receipt.leaseKey,
            operation_id: input.operationId,
            record_checksum: recordChecksum,
            record_json: recordJson,
            resource_id: input.sagaId,
            resource_kind: "saga",
            step_id: operationStepId,
            to_state_version: beforeSaga.stateVersion,
            transition_id: transitionId,
          })
        ) {
          return intervention("The classified saga action lacks its exact coupled effect receipt.")
        }
        return beforeSaga
      }
      if (
        operationAction.state !== "running" ||
        action.activeAttemptId !== input.attemptId ||
        operationAction.activeAttemptId !== input.attemptId
      ) {
        return intervention("The terminal saga receipt contradicts the active coupled attempt.")
      }
      const authorized = await this.#leases.authorizeAt(input.proof)
      const planStep = beforeOperation.operation.plan.steps.find(
        (step) => step.stepId === operationStepId,
      ) as OperationStepPlan
      let afterSaga: SagaRecord
      let afterOperation: OperationRecord
      let transitionKind: "failed" | "succeeded"
      if (receipt.state === "confirmed") {
        afterSaga = recordSagaActionSuccess(beforeSaga, {
          attemptId: input.attemptId,
          phase: input.phase,
          resultChecksum: receipt.outputChecksum,
          serverTimeMs: authorized.serverTimeMs,
          stepId: input.stepId,
        })
        afterOperation = recordStepSuccess(beforeOperation.operation, {
          attemptId: input.attemptId,
          observedPostconditionChecksum: planStep.postconditionChecksum,
          resultChecksum: receipt.outcomeChecksum,
          stepId: operationStepId,
        })
        transitionKind = "succeeded"
      } else {
        const outcome =
          receipt.state === "unknown"
            ? "unknown"
            : receipt.state === "not_applied"
              ? "definitely_not_applied_retryable"
              : "definitely_not_applied_terminal"
        afterSaga = recordSagaActionFailure(beforeSaga, {
          attemptId: input.attemptId,
          errorChecksum: receipt.errorChecksum,
          outcome,
          phase: input.phase,
          serverTimeMs: authorized.serverTimeMs,
          stepId: input.stepId,
        })
        const afterAction = afterSaga.steps[input.stepId]?.[input.phase]
        if (receipt.state !== "unknown" && afterAction?.state !== "retryable_failed") {
          afterOperation = recordStepSuccess(beforeOperation.operation, {
            attemptId: input.attemptId,
            observedPostconditionChecksum: planStep.postconditionChecksum,
            resultChecksum: receipt.outcomeChecksum,
            stepId: operationStepId,
          })
          transitionKind = "succeeded"
        } else {
          afterOperation = recordStepFailure(beforeOperation.operation, {
            attemptId: input.attemptId,
            errorChecksum: receipt.outcomeChecksum,
            outcome: receipt.state === "unknown" ? "unknown" : "definitely_not_applied",
            stepId: operationStepId,
          })
          transitionKind = "failed"
        }
      }
      const transitionId = operationTransitionIdentity(transitionKind, [
        input.operationId,
        operationStepId,
        input.attemptId,
      ])
      const afterAction = afterSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      const effectKind = classifiedEffectKind(input.phase, receipt, afterAction)
      const effectId = await this.#identity("saga-effect", [
        transitionId,
        input.sagaId,
        effectKind,
        afterSaga.stateVersion.toString(10),
      ])
      if (
        await this.#commit({
          actorChecksum: input.actorChecksum,
          afterOperation,
          afterSaga,
          auditEventType: "saga.action.classified",
          beforeOperation,
          beforeSaga,
          effectId,
          effectKind,
          evidenceChecksum: receipt.outcomeChecksum,
          proof: input.proof,
          stepId: operationStepId,
          transitionId,
        })
      ) {
        return afterSaga
      }
    }
    return intervention("Settling a saga action exceeded the bounded coupled retry budget.")
  }
}
