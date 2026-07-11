import {
  appendAuditEvent,
  beginOperationStep,
  beginSagaAction,
  createOperationRecord,
  createSagaRecord,
  type DigestFunction,
  type IrreversibleAuthorization,
  type LeaseProof,
  loadAuditEvent,
  loadOperationRecord,
  loadSagaRecord,
  markRunningSagaActionUnknown,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepUnknownAfterCrash,
  markSagaActionNotDispatched,
  NozzleError,
  nextSagaCommand,
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
  SAGA_TERMINATION_OPERATION_STEP_ID,
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

interface AuditEventRow {
  readonly event_json: string
}

interface HistoricalStepRow {
  readonly step_id: string
  readonly to_record_json: string
}

interface TransitionReceiptRow {
  readonly acquisition_id: string
  readonly audit_event_hash: string
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

interface IrreversibleAuthorizationReceiptRow {
  readonly authorization_checksum: string
  readonly authorization_id: string | null
  readonly classified_at_ms: number
  readonly protocol_version: number
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
  readonly recoveryGuard?: {
    readonly acceptanceChecksum?: string
    readonly attemptId: string
    readonly dispatchFencingToken: number
    readonly mode: SagaRecoveryMode
  }
  readonly stepId: string
  readonly terminationGuard?: {
    readonly cause: "cancellation" | "timeout"
    readonly sagaId: string
  }
  readonly transitionId: string
}

type CoupledCommitResult =
  | { readonly kind: "advanced"; readonly saga: SagaRecord }
  | { readonly kind: "exact"; readonly saga: SagaRecord }

type SagaRecoveryMode = "accepted_unknown" | "not_dispatched"

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

interface CoordinatedSagaActionIdentityInput {
  readonly actorChecksum: string
  readonly attemptId: string
  readonly operationId: string
  readonly phase: SagaActionPhase
  readonly proof: LeaseProof
  readonly sagaId: string
  readonly stepId: string
}

export interface BeginCoordinatedSagaActionInput extends CoordinatedSagaActionIdentityInput {
  readonly irreversibleAuthorization?: IrreversibleAuthorization
}

export type SettleCoordinatedSagaActionInput = CoordinatedSagaActionIdentityInput
export type SettleCoordinatedSagaObservationInput = CoordinatedSagaActionIdentityInput
export interface RecoverCoordinatedSagaActionInput extends CoordinatedSagaActionIdentityInput {
  readonly recoveryId: string
}

export interface RequestCoordinatedSagaTerminationInput {
  readonly actorChecksum: string
  readonly cause: "cancellation" | "timeout"
  readonly operationId: string
  readonly proof: LeaseProof
  readonly requestChecksum: string
  readonly requestId: string
  readonly sagaId: string
}

interface SagaTerminationReceiptIdentity {
  readonly attemptId: string
  readonly evidenceChecksum: string
  readonly transitionId: string
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

function isWellFormedUtf16(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false
    }
  }
  return true
}

function boundedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (!isWellFormedUtf16(value)) {
    return configuration(`${label} cannot contain unpaired UTF-16 surrogates.`)
  }
  const encoded = new TextEncoder().encode(value)
  if (encoded.byteLength > MAX_IDENTITY_BYTES) {
    return configuration(`${label} exceeds the durable coordinator identity limit.`)
  }
  return value
}

function terminationPlanStep(operation: OperationRecord): OperationStepPlan {
  const step = operation.plan.steps.find(
    (candidate) => candidate.stepId === SAGA_TERMINATION_OPERATION_STEP_ID,
  )
  if (
    !operation.plan.operationType.startsWith("saga:") ||
    step === undefined ||
    step.activation !== "conditional" ||
    step.checkpoint !== "reversible" ||
    step.completionRole !== "work" ||
    step.dependsOn.length !== 0 ||
    step.effectProtocol !== "opaque" ||
    step.retryClassification !== "idempotent"
  ) {
    return intervention("Saga termination lacks its canonical sealed operation step.")
  }
  return step
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

function mutationResults(
  results: readonly ControlRunResult[],
  expected: number,
  transitionIncludesAuthorizationReceipt = false,
): void {
  if (!Array.isArray(results) || results.length !== expected) {
    intervention("Control D1 returned an incomplete coupled saga batch result.")
  }
  for (const [index, result] of results.entries()) {
    const changes = result.meta.changes
    const maximumChanges = transitionIncludesAuthorizationReceipt && index === 0 ? 2 : 1
    if (
      result.success !== true ||
      !Number.isSafeInteger(changes) ||
      (changes as number) < 0 ||
      (changes as number) > maximumChanges
    ) {
      intervention("Control D1 returned malformed coupled saga mutation metadata.")
    }
  }
}

function sameOperation(left: OperationRecord, right: OperationRecord): boolean {
  return encode(left) === encode(right)
}

function isIrreversibleAuthorizationDispatch(input: CoupledTransition): boolean {
  const plan = input.beforeOperation.operation.plan.steps.find(
    (step) => step.stepId === input.stepId,
  ) as OperationStepPlan
  const before = input.beforeOperation.operation.steps[input.stepId] as OperationStepRecord
  const after = input.afterOperation.steps[input.stepId] as OperationStepRecord
  return (
    plan.checkpoint === "irreversible" &&
    (before.state === "pending" || before.state === "retryable_failed") &&
    after.state === "running"
  )
}

function sameSaga(left: SagaRecord, right: SagaRecord): boolean {
  return encode(left) === encode(right)
}

function sagaIntentJson(record: SagaRecord): string {
  return encode({
    deadlineAtMs: record.deadlineAtMs,
    descriptor: record.descriptor,
    idempotencyKey: record.idempotencyKey,
    inputChecksum: record.inputChecksum,
    sagaId: record.sagaId,
    stepInputChecksums: Object.fromEntries(
      record.descriptor.steps.map((step) => [
        step.stepId,
        (record.steps[step.stepId] as SagaRecord["steps"][string]).inputChecksum,
      ]),
    ),
  })
}

function committedSaga(
  result: CoupledCommitResult | undefined,
  exact: SagaRecord,
): SagaRecord | undefined {
  if (result === undefined) return undefined
  return result.kind === "exact" ? exact : result.saga
}

function exactColumns(
  row: Readonly<Record<string, unknown>>,
  expected: Readonly<Record<string, unknown>>,
): boolean {
  return Object.entries(expected).every(([key, value]) => row[key] === value)
}

function persistedJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value) as unknown
  } catch {
    return intervention(`${label} is invalid JSON.`)
  }
}

function retryServerTime(
  before: SagaRecord,
  after: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
): number {
  const action = after.steps[stepId]?.[phase] as SagaActionRecord
  if (action.state === "retryable_failed") {
    const descriptor = before.descriptor.steps.find(
      (step) => step.stepId === stepId,
    ) as SagaDescriptor["steps"][number]
    let delay = descriptor.baseRetryDelayMs
    for (
      let attempt = 1;
      attempt < action.attempts && delay < descriptor.maxRetryDelayMs;
      attempt += 1
    ) {
      delay = Math.min(descriptor.maxRetryDelayMs, delay * 2)
    }
    return Math.max(0, action.nextAttemptAtMs - delay)
  }
  if (before.terminationCause === null && after.terminationCause !== null) {
    return after.terminationRequestedAtMs as number
  }
  return 0
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

function receiptConsumerMatches(receipt: SagaAttemptRecord, proof: LeaseProof): boolean {
  return (
    receipt.leaseKey === proof.leaseKey &&
    (proof.fencingToken > receipt.fencingToken ||
      (proof.fencingToken === receipt.fencingToken &&
        receipt.holderId === proof.holderId &&
        receipt.acquisitionId === proof.acquisitionId))
  )
}

function transitionConsumerMatches(transition: TransitionReceiptRow, proof: LeaseProof): boolean {
  return (
    transition.lease_key === proof.leaseKey &&
    (proof.fencingToken > transition.fencing_token ||
      (proof.fencingToken === transition.fencing_token &&
        transition.holder_id === proof.holderId &&
        transition.acquisition_id === proof.acquisitionId))
  )
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

function classifiedObservationEffectKind(
  phase: SagaActionPhase,
  receipt: Exclude<SagaAttemptRecord, { readonly state: "accepted" }>,
): string {
  if (receipt.state === "confirmed") return `action:${phase}:observation:applied`
  if (receipt.state === "not_applied") return `action:${phase}:observation:not_applied`
  return `action:${phase}:observation:indeterminate`
}

function recoveredRecords(input: {
  readonly attemptId: string
  readonly beforeOperation: OperationRecord
  readonly beforeSaga: SagaRecord
  readonly evidenceChecksum: string
  readonly mode: SagaRecoveryMode
  readonly operationStepId: string
  readonly phase: SagaActionPhase
  readonly sagaStepId: string
  readonly serverTimeMs: number
}): { readonly operation: OperationRecord; readonly saga: SagaRecord } {
  const saga =
    input.mode === "accepted_unknown"
      ? markRunningSagaActionUnknown(input.beforeSaga, {
          attemptId: input.attemptId,
          errorChecksum: input.evidenceChecksum,
          phase: input.phase,
          stepId: input.sagaStepId,
        })
      : markSagaActionNotDispatched(input.beforeSaga, {
          attemptId: input.attemptId,
          errorChecksum: input.evidenceChecksum,
          phase: input.phase,
          serverTimeMs: input.serverTimeMs,
          stepId: input.sagaStepId,
        })
  if (input.mode === "accepted_unknown") {
    return Object.freeze({
      operation: markRunningStepUnknownAfterCrash(
        input.beforeOperation,
        input.operationStepId,
        input.evidenceChecksum,
      ),
      saga,
    })
  }
  const action = saga.steps[input.sagaStepId]?.[input.phase] as SagaActionRecord
  if (action.state === "retryable_failed") {
    return Object.freeze({
      operation: markRunningStepNotDispatchedAfterCrash(
        input.beforeOperation,
        input.operationStepId,
        input.evidenceChecksum,
      ),
      saga,
    })
  }
  const unknown = markRunningStepUnknownAfterCrash(
    input.beforeOperation,
    input.operationStepId,
    input.evidenceChecksum,
  )
  return Object.freeze({
    operation: recordSagaStepTerminalClassification(unknown, {
      outcome: "not_applied",
      receiptOutcomeChecksum: input.evidenceChecksum,
      stepId: input.operationStepId,
    }),
    saga,
  })
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

  async #terminationIdentity(
    input: RequestCoordinatedSagaTerminationInput,
  ): Promise<SagaTerminationReceiptIdentity> {
    const requestIdentity = [input.operationId, input.sagaId, input.requestId]
    const attemptId = await this.#identity("saga-termination-attempt", requestIdentity)
    return Object.freeze({
      attemptId,
      evidenceChecksum: await checkedDigest(this.#digest, COORDINATOR_ID_DOMAIN, [
        "saga-termination-evidence",
        ...requestIdentity,
        input.requestChecksum,
        input.cause,
      ]),
      transitionId: operationTransitionIdentity("succeeded", [
        input.operationId,
        SAGA_TERMINATION_OPERATION_STEP_ID,
        attemptId,
      ]),
    })
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

  async #historicalStep(
    operation: LoadedOperation,
    stepId: string,
    recordJson: string,
  ): Promise<OperationStepRecord> {
    const record = persistedJson(recordJson, "A historical operation step record")
    const historical = await loadOperationRecord(
      {
        plan: operation.operation.plan,
        steps: { ...operation.operation.steps, [stepId]: record },
      },
      this.#digest,
    )
    const step = historical.steps[stepId] as OperationStepRecord
    if (operationStepRecordJson(step) !== recordJson) {
      return intervention("A historical operation step record is not canonical.")
    }
    return step
  }

  async #historicalSaga(effect: EffectReceiptRow): Promise<SagaRecord> {
    const record = await loadSagaRecord(
      persistedJson(effect.record_json, "A historical saga record"),
      this.#digest,
    )
    if (
      !exactColumns(
        {
          canonical: encode(record),
          checksum: await this.#recordChecksum(effect.record_json),
        },
        { canonical: effect.record_json, checksum: effect.record_checksum },
      )
    ) {
      return intervention("A historical saga effect record is contradictory.")
    }
    return record
  }

  async #operationBefore(
    operation: LoadedOperation,
    auditSequence: number,
  ): Promise<OperationRecord> {
    let before = createOperationRecord(operation.operation.plan)
    const historicalSteps = await this.#database
      .prepare(
        `SELECT "transition"."step_id", "transition"."to_record_json"
         FROM "nozzle_operation_transitions" AS "transition"
         JOIN "nozzle_audit_log" AS "audit"
           ON "audit"."event_hash" = "transition"."audit_event_hash"
          AND "audit"."environment_id" = ?1
         WHERE "transition"."operation_id" = ?2 AND "audit"."sequence" < ?3
         ORDER BY "audit"."sequence", "transition"."transition_id"`,
      )
      .bind(operation.environmentId, operation.operation.plan.operationId, auditSequence)
      .all<HistoricalStepRow>()
    for (const row of historicalSteps.results) {
      const step = await this.#historicalStep(operation, row.step_id, row.to_record_json)
      before = Object.freeze({
        plan: before.plan,
        steps: Object.freeze({ ...before.steps, [row.step_id]: step }),
      })
    }
    return before
  }

  async #initializedProjection(
    initialization: InitializeSagaInput,
  ): Promise<SagaRecord | undefined> {
    const transitionId = operationTransitionIdentity("succeeded", [
      initialization.operationId,
      SAGA_INIT_OPERATION_STEP_ID,
      initialization.attemptId,
    ])
    const transition = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
      .bind(transitionId)
      .first<TransitionReceiptRow>()
    if (transition === null) return undefined
    const operation = await this.#operation(initialization.operationId)
    const saga = await this.#saga(initialization.sagaId, initialization.operationId)
    if (saga === undefined) {
      return intervention("Saga initialization has no current projection.")
    }
    if (
      !exactColumns(transition as unknown as Record<string, unknown>, {
        operation_id: initialization.operationId,
        step_id: SAGA_INIT_OPERATION_STEP_ID,
        transition_id: transitionId,
      }) ||
      !transitionConsumerMatches(transition, initialization.proof)
    ) {
      return intervention("Saga initialization has a contradictory operation transition.")
    }
    const auditRow = await this.#database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(operation.environmentId, transition.audit_event_hash)
      .first<AuditEventRow>()
    if (auditRow === null) {
      return intervention("Saga initialization lacks its exact audit event.")
    }
    const audit = await loadAuditEvent(
      persistedJson(auditRow.event_json, "The saga initialization audit event"),
      this.#digest,
    )
    const operationBefore = await this.#operationBefore(operation, audit.sequence)
    const expectedSaga = createSagaRecord({
      deadlineAtMs: initialization.deadlineAtMs,
      descriptor: initialization.descriptor,
      idempotencyKey: initialization.idempotencyKey,
      inputChecksum: initialization.inputChecksum,
      sagaId: initialization.sagaId,
      serverTimeMs: 0,
      stepInputChecksums: initialization.stepInputChecksums,
    })
    const expectedOperation = recordStepSuccess(operationBefore, {
      attemptId: initialization.attemptId,
      observedPostconditionChecksum: initialization.observedPostconditionChecksum,
      resultChecksum: initialization.resultChecksum,
      stepId: SAGA_INIT_OPERATION_STEP_ID,
    })
    if (sagaIntentJson(saga) !== sagaIntentJson(expectedSaga)) {
      return intervention("Saga initialization replay contradicts its immutable intent.")
    }
    const effectId = await this.#identity("saga-effect", [
      transitionId,
      initialization.sagaId,
      "create",
      "0",
    ])
    const expectedStep = expectedOperation.steps[SAGA_INIT_OPERATION_STEP_ID] as OperationStepRecord
    if (
      operationStepRecordJson(
        operation.operation.steps[SAGA_INIT_OPERATION_STEP_ID] as OperationStepRecord,
      ) !== operationStepRecordJson(expectedStep)
    ) {
      return intervention("Saga initialization replay contradicts its immutable operation step.")
    }
    const committed = await this.#verify(
      {
        actorChecksum: audit.actorChecksum,
        afterOperation: expectedOperation,
        afterSaga: expectedSaga,
        auditEventType: "saga.initialized",
        beforeOperation: Object.freeze({ ...operation, operation: operationBefore }),
        beforeSaga: undefined,
        effectId,
        effectKind: "create",
        evidenceChecksum: initialization.evidenceChecksum,
        proof: {
          acquisitionId: transition.acquisition_id,
          fencingToken: transition.fencing_token,
          holderId: transition.holder_id,
          leaseKey: transition.lease_key,
        },
        stepId: SAGA_INIT_OPERATION_STEP_ID,
        transitionId,
      },
      encode(expectedSaga),
      await this.#recordChecksum(encode(expectedSaga)),
    )
    if (committed === undefined) {
      return intervention("Saga initialization replay lost its immutable transition.")
    }
    return committed.saga
  }

  async #terminationReceiptProjection(input: {
    readonly cause: "cancellation" | "timeout"
    readonly identity: SagaTerminationReceiptIdentity
    readonly operationId: string
    readonly proof: LeaseProof
    readonly sagaId: string
  }): Promise<SagaRecord | undefined> {
    const transition = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
      .bind(input.identity.transitionId)
      .first<TransitionReceiptRow>()
    if (transition === null) return undefined
    const operation = await this.#operation(input.operationId)
    const saga = await this.#saga(input.sagaId, input.operationId)
    if (saga === undefined) {
      return intervention("Saga termination has no current projection.")
    }
    if (
      !exactColumns(transition as unknown as Record<string, unknown>, {
        operation_id: input.operationId,
        step_id: SAGA_TERMINATION_OPERATION_STEP_ID,
        transition_id: input.identity.transitionId,
      }) ||
      !transitionConsumerMatches(transition, input.proof)
    ) {
      return intervention("Saga termination has a contradictory operation transition.")
    }
    const auditRow = await this.#database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(operation.environmentId, transition.audit_event_hash)
      .first<AuditEventRow>()
    if (auditRow === null) {
      return intervention("Saga termination lacks its exact audit event.")
    }
    const audit = await loadAuditEvent(
      persistedJson(auditRow.event_json, "The saga termination audit event"),
      this.#digest,
    )
    const effects = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "transition_id" = ?1 AND "resource_kind" = 'saga' AND "resource_id" = ?2
         ORDER BY "effect_id"`,
      )
      .bind(input.identity.transitionId, input.sagaId)
      .all<EffectReceiptRow>()
    if (effects.results.length === 0) {
      return intervention("Saga termination lacks its exact coupled effect receipt.")
    }
    if (effects.results.length !== 1) {
      return intervention("Saga termination has ambiguous coupled effect receipts.")
    }
    const effect = effects.results[0] as EffectReceiptRow
    const afterSaga = await this.#historicalSaga(effect)
    if (afterSaga.stateVersion < 1 || afterSaga.terminationRequestedAtMs === null) {
      return intervention("Saga termination effect has no valid predecessor or request time.")
    }
    const priorEffect = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1 AND "to_state_version" = ?2`,
      )
      .bind(input.sagaId, afterSaga.stateVersion - 1)
      .first<EffectReceiptRow>()
    if (priorEffect === null) {
      return intervention("Saga termination lacks its prior immutable saga version.")
    }
    const beforeSaga = await this.#historicalSaga(priorEffect)
    if (
      !exactColumns(priorEffect as unknown as Record<string, unknown>, {
        operation_id: input.operationId,
        resource_id: input.sagaId,
        resource_kind: "saga",
        to_state_version: beforeSaga.stateVersion,
      }) ||
      beforeSaga.sagaId !== input.sagaId ||
      beforeSaga.stateVersion !== afterSaga.stateVersion - 1
    ) {
      return intervention("Saga termination has a contradictory saga version chain.")
    }
    if (input.cause === "timeout" && afterSaga.terminationRequestedAtMs < beforeSaga.deadlineAtMs) {
      return intervention("Saga timeout termination predates its immutable deadline.")
    }
    const operationBefore = await this.#operationBefore(operation, audit.sequence)
    const planStep = terminationPlanStep(operationBefore)
    const historicalProof: LeaseProof = Object.freeze({
      acquisitionId: transition.acquisition_id,
      fencingToken: transition.fencing_token,
      holderId: transition.holder_id,
      leaseKey: transition.lease_key,
    })
    const expectedOperation = recordAtomicStepOutcome(operationBefore, {
      attemptId: input.identity.attemptId,
      idempotencyKey: planStep.idempotencyKey,
      leaseProof: historicalProof,
      observedPreconditionChecksum: planStep.preconditionChecksum,
      outcome: {
        observedPostconditionChecksum: planStep.postconditionChecksum,
        resultChecksum: input.identity.evidenceChecksum,
        state: "succeeded",
      },
      stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
    })
    const expectedSaga = requestSagaTermination(beforeSaga, {
      cause: input.cause,
      serverTimeMs: afterSaga.terminationRequestedAtMs,
    })
    const effectKind = `termination:${input.cause}`
    const effectId = await this.#identity("saga-effect", [
      input.identity.transitionId,
      input.sagaId,
      effectKind,
      expectedSaga.stateVersion.toString(10),
    ])
    if (
      sagaIntentJson(saga) !== sagaIntentJson(expectedSaga) ||
      saga.terminationCause !== expectedSaga.terminationCause ||
      saga.terminationRequestedAtMs !== expectedSaga.terminationRequestedAtMs
    ) {
      return intervention("Saga termination replay contradicts immutable saga intent.")
    }
    const expectedStep = expectedOperation.steps[
      SAGA_TERMINATION_OPERATION_STEP_ID
    ] as OperationStepRecord
    if (
      operationStepRecordJson(
        operation.operation.steps[SAGA_TERMINATION_OPERATION_STEP_ID] as OperationStepRecord,
      ) !== operationStepRecordJson(expectedStep)
    ) {
      return intervention("Saga termination replay contradicts its immutable operation step.")
    }
    const expectedRecordJson = encode(expectedSaga)
    const committed = await this.#verify(
      {
        actorChecksum: audit.actorChecksum,
        afterOperation: expectedOperation,
        afterSaga: expectedSaga,
        auditEventType: "saga.termination.requested",
        beforeOperation: Object.freeze({ ...operation, operation: operationBefore }),
        beforeSaga,
        effectId,
        effectKind,
        evidenceChecksum: input.identity.evidenceChecksum,
        proof: historicalProof,
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
        transitionId: input.identity.transitionId,
      },
      expectedRecordJson,
      await this.#recordChecksum(expectedRecordJson),
    )
    if (committed === undefined) {
      return intervention("Saga termination replay lost its immutable transition.")
    }
    return committed.saga
  }

  async #existingTerminationProjection(input: {
    readonly operationId: string
    readonly proof: LeaseProof
    readonly sagaId: string
  }): Promise<SagaRecord> {
    const saga = await this.#saga(input.sagaId, input.operationId)
    if (saga === undefined) {
      return intervention("The explicit termination winner has no current saga projection.")
    }
    const operation = await this.#operation(input.operationId)
    const cause = saga.terminationCause
    if (cause !== "cancellation" && cause !== "timeout") {
      return intervention("The saga has no explicit termination request to replay.")
    }
    const record = operation.operation.steps[
      SAGA_TERMINATION_OPERATION_STEP_ID
    ] as OperationStepRecord
    if (
      record.state !== "succeeded" ||
      record.lastAttemptId === undefined ||
      record.resultChecksum === undefined
    ) {
      return intervention("The explicit saga termination lacks its canonical operation outcome.")
    }
    const replay = await this.#terminationReceiptProjection({
      cause,
      identity: {
        attemptId: record.lastAttemptId,
        evidenceChecksum: record.resultChecksum,
        transitionId: operationTransitionIdentity("succeeded", [
          operation.operation.plan.operationId,
          SAGA_TERMINATION_OPERATION_STEP_ID,
          record.lastAttemptId,
        ]),
      },
      operationId: operation.operation.plan.operationId,
      proof: input.proof,
      sagaId: saga.sagaId,
    })
    if (replay === undefined) {
      return intervention("The explicit saga termination lacks its immutable receipt.")
    }
    return replay
  }

  async #classifiedProjection(input: {
    readonly auditEventType: "saga.action.classified" | "saga.action.observed"
    readonly operation: LoadedOperation
    readonly operationStepId: string
    readonly phase: SagaActionPhase
    readonly receipt: Exclude<SagaAttemptRecord, { readonly state: "accepted" }>
    readonly saga: SagaRecord
    readonly transitionKinds: readonly ("failed" | "reconciled" | "succeeded")[]
  }): Promise<SagaRecord | undefined> {
    const candidates = input.transitionKinds.map((kind) =>
      operationTransitionIdentity(kind, [
        input.operation.operation.plan.operationId,
        input.operationStepId,
        input.receipt.attemptId,
      ]),
    )
    const transitions: TransitionReceiptRow[] = []
    for (const transitionId of candidates) {
      const transition = await this.#database
        .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
        .bind(transitionId)
        .first<TransitionReceiptRow>()
      if (transition !== null) transitions.push(transition)
    }
    if (transitions.length > 1) {
      return intervention("The classified saga action has contradictory operation transitions.")
    }
    if (transitions.length === 0) {
      for (const transitionId of candidates) {
        const effect = await this.#database
          .prepare(
            `SELECT * FROM "nozzle_operation_effects"
             WHERE "transition_id" = ?1 AND "resource_kind" = 'saga' AND "resource_id" = ?2`,
          )
          .bind(transitionId, input.saga.sagaId)
          .first<EffectReceiptRow>()
        if (effect !== null) {
          return intervention("The classified saga action lacks its exact operation transition.")
        }
      }
      return undefined
    }
    const transition = transitions[0] as TransitionReceiptRow
    if (
      !exactColumns(transition as unknown as Record<string, unknown>, {
        operation_id: input.operation.operation.plan.operationId,
        step_id: input.operationStepId,
        transition_id: transition.transition_id,
      }) ||
      transition.lease_key !== input.receipt.leaseKey ||
      transition.fencing_token < input.receipt.fencingToken ||
      (transition.fencing_token === input.receipt.fencingToken &&
        (transition.holder_id !== input.receipt.holderId ||
          transition.acquisition_id !== input.receipt.acquisitionId))
    ) {
      return intervention("The classified saga action has a contradictory operation transition.")
    }
    const auditRow = await this.#database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(input.operation.environmentId, transition.audit_event_hash)
      .first<AuditEventRow>()
    if (auditRow === null) {
      return intervention("The classified saga transition lacks its exact audit event.")
    }
    const audit = await loadAuditEvent(
      persistedJson(auditRow.event_json, "The classified saga audit event"),
      this.#digest,
    )
    if (
      !exactColumns(audit as unknown as Record<string, unknown>, {
        environmentId: input.operation.environmentId,
        eventHash: transition.audit_event_hash,
        eventType: input.auditEventType,
        fencingToken: transition.fencing_token,
        idempotencyKey: transition.transition_id,
        operationId: input.operation.operation.plan.operationId,
        payloadChecksum: input.receipt.outcomeChecksum,
        stepId: input.operationStepId,
      })
    ) {
      return intervention("The classified saga audit event is contradictory.")
    }
    const beforeStep = await this.#historicalStep(
      input.operation,
      input.operationStepId,
      transition.from_record_json,
    )
    const afterStep = await this.#historicalStep(
      input.operation,
      input.operationStepId,
      transition.to_record_json,
    )
    const effect = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "transition_id" = ?1 AND "resource_kind" = 'saga' AND "resource_id" = ?2`,
      )
      .bind(transition.transition_id, input.saga.sagaId)
      .first<EffectReceiptRow>()
    if (effect === null) {
      return intervention("The classified saga action lacks its exact coupled effect receipt.")
    }
    const afterSaga = await this.#historicalSaga(effect)
    const effectKind =
      input.receipt.purpose === "observation"
        ? classifiedObservationEffectKind(input.phase, input.receipt)
        : classifiedEffectKind(
            input.phase,
            input.receipt,
            afterSaga.steps[input.receipt.sagaStepId]?.[input.phase] as SagaActionRecord,
          )
    const effectId = await this.#identity("saga-effect", [
      transition.transition_id,
      afterSaga.sagaId,
      effectKind,
      afterSaga.stateVersion.toString(10),
    ])
    if (
      !exactColumns(
        {
          ...(effect as unknown as Record<string, unknown>),
          loaded_saga_id: afterSaga.sagaId,
        },
        {
          acquisition_id: transition.acquisition_id,
          effect_id: effectId,
          effect_kind: effectKind,
          evidence_checksum: input.receipt.outcomeChecksum,
          fencing_token: transition.fencing_token,
          from_state_version: afterSaga.stateVersion - 1,
          holder_id: transition.holder_id,
          lease_key: transition.lease_key,
          operation_id: input.operation.operation.plan.operationId,
          record_checksum: effect.record_checksum,
          record_json: effect.record_json,
          resource_id: afterSaga.sagaId,
          resource_kind: "saga",
          step_id: input.operationStepId,
          to_state_version: afterSaga.stateVersion,
          transition_id: transition.transition_id,
          loaded_saga_id: input.saga.sagaId,
        },
      )
    ) {
      return intervention("The classified saga action lacks its exact coupled effect receipt.")
    }
    const priorEffect = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1 AND "to_state_version" = ?2`,
      )
      .bind(afterSaga.sagaId, afterSaga.stateVersion - 1)
      .first<EffectReceiptRow>()
    if (priorEffect === null) {
      return intervention("The classified saga action lacks its prior immutable saga version.")
    }
    const beforeSaga = await this.#historicalSaga(priorEffect)
    if (
      !exactColumns(
        {
          before_saga_id: beforeSaga.sagaId,
          before_version: beforeSaga.stateVersion,
          operation_id: priorEffect.operation_id,
          resource_id: priorEffect.resource_id,
          resource_kind: priorEffect.resource_kind,
          to_state_version: priorEffect.to_state_version,
        },
        {
          before_saga_id: afterSaga.sagaId,
          before_version: afterSaga.stateVersion - 1,
          operation_id: effect.operation_id,
          resource_id: afterSaga.sagaId,
          resource_kind: "saga",
          to_state_version: beforeSaga.stateVersion,
        },
      )
    ) {
      return intervention("The classified saga action has a contradictory saga version chain.")
    }

    const operationBefore = await this.#operationBefore(input.operation, audit.sequence)
    let expectedOperation: OperationRecord
    let expectedSaga: SagaRecord
    let expectedTransitionKind: "failed" | "reconciled" | "succeeded"
    const serverTimeMs = retryServerTime(
      beforeSaga,
      afterSaga,
      input.receipt.sagaStepId,
      input.phase,
    )
    if (input.receipt.purpose === "effect") {
      if (input.receipt.state === "confirmed") {
        expectedSaga = recordSagaActionSuccess(beforeSaga, {
          attemptId: input.receipt.attemptId,
          phase: input.phase,
          resultChecksum: input.receipt.outputChecksum,
          serverTimeMs,
          stepId: input.receipt.sagaStepId,
        })
        const planStep = operationBefore.plan.steps.find(
          (step) => step.stepId === input.operationStepId,
        ) as OperationStepPlan
        expectedOperation = recordStepSuccess(operationBefore, {
          attemptId: input.receipt.attemptId,
          observedPostconditionChecksum: planStep.postconditionChecksum,
          resultChecksum: input.receipt.outcomeChecksum,
          stepId: input.operationStepId,
        })
        expectedTransitionKind = "succeeded"
      } else {
        const outcome =
          input.receipt.state === "unknown"
            ? "unknown"
            : input.receipt.state === "not_applied"
              ? "definitely_not_applied_retryable"
              : "definitely_not_applied_terminal"
        expectedSaga = recordSagaActionFailure(beforeSaga, {
          attemptId: input.receipt.attemptId,
          errorChecksum: input.receipt.errorChecksum,
          outcome,
          phase: input.phase,
          serverTimeMs,
          stepId: input.receipt.sagaStepId,
        })
        const action = expectedSaga.steps[input.receipt.sagaStepId]?.[
          input.phase
        ] as SagaActionRecord
        if (input.receipt.state !== "unknown" && action.state !== "retryable_failed") {
          const planStep = operationBefore.plan.steps.find(
            (step) => step.stepId === input.operationStepId,
          ) as OperationStepPlan
          expectedOperation = recordStepSuccess(operationBefore, {
            attemptId: input.receipt.attemptId,
            observedPostconditionChecksum: planStep.postconditionChecksum,
            resultChecksum: input.receipt.outcomeChecksum,
            stepId: input.operationStepId,
          })
          expectedTransitionKind = "succeeded"
        } else {
          expectedOperation = recordStepFailure(operationBefore, {
            attemptId: input.receipt.attemptId,
            errorChecksum: input.receipt.outcomeChecksum,
            outcome: input.receipt.state === "unknown" ? "unknown" : "definitely_not_applied",
            stepId: input.operationStepId,
          })
          expectedTransitionKind = "failed"
        }
      }
    } else {
      expectedSaga = recordSagaObservation(beforeSaga, {
        evidenceChecksum: input.receipt.outcomeChecksum,
        outcome:
          input.receipt.state === "confirmed"
            ? "applied"
            : (input.receipt.state as "indeterminate" | "not_applied"),
        phase: input.phase,
        ...(input.receipt.state === "confirmed"
          ? { resultChecksum: input.receipt.outputChecksum }
          : {}),
        serverTimeMs,
        stepId: input.receipt.sagaStepId,
      })
      const action = expectedSaga.steps[input.receipt.sagaStepId]?.[input.phase] as SagaActionRecord
      const planStep = operationBefore.plan.steps.find(
        (step) => step.stepId === input.operationStepId,
      ) as OperationStepPlan
      if (input.receipt.state === "confirmed") {
        expectedOperation = recordStepReconciliation(operationBefore, {
          evidenceChecksum: input.receipt.outcomeChecksum,
          observedPostconditionChecksum: planStep.postconditionChecksum,
          outcome: "applied",
          resultChecksum: input.receipt.outcomeChecksum,
          stepId: input.operationStepId,
        })
      } else if (input.receipt.state === "indeterminate") {
        expectedOperation = recordStepReconciliation(operationBefore, {
          evidenceChecksum: input.receipt.outcomeChecksum,
          outcome: "indeterminate",
          stepId: input.operationStepId,
        })
      } else if (action.state === "retryable_failed") {
        expectedOperation = recordStepReconciliation(operationBefore, {
          evidenceChecksum: input.receipt.outcomeChecksum,
          outcome: "not_applied",
          stepId: input.operationStepId,
        })
      } else {
        expectedOperation = recordSagaStepTerminalClassification(operationBefore, {
          outcome: "not_applied",
          receiptOutcomeChecksum: input.receipt.outcomeChecksum,
          stepId: input.operationStepId,
        })
      }
      expectedTransitionKind = "reconciled"
    }
    const expectedStep = expectedOperation.steps[input.operationStepId] as OperationStepRecord
    const expectedTransitionId = operationTransitionIdentity(expectedTransitionKind, [
      input.operation.operation.plan.operationId,
      input.operationStepId,
      input.receipt.attemptId,
    ])
    const expectedFromStatus = operationStatus(operationBefore)
    const expectedToStatus = operationStatus(expectedOperation)
    const beforeAction = beforeSaga.steps[input.receipt.sagaStepId]?.[
      input.phase
    ] as SagaActionRecord
    if (
      !exactColumns(
        {
          after_saga: encode(afterSaga),
          after_step: operationStepRecordJson(afterStep),
          before_step: operationStepRecordJson(beforeStep),
          causal_saga_attempt:
            input.receipt.purpose === "observation" ? beforeAction.lastAttemptId : null,
          causal_step_attempt:
            input.receipt.purpose === "observation" ? beforeStep.lastAttemptId : null,
          from_status: transition.from_operation_status,
          to_status: transition.to_operation_status,
          transition_id: transition.transition_id,
        },
        {
          after_saga: encode(expectedSaga),
          after_step: operationStepRecordJson(expectedStep),
          before_step: operationStepRecordJson(
            operationBefore.steps[input.operationStepId] as OperationStepRecord,
          ),
          causal_saga_attempt:
            input.receipt.purpose === "observation" ? input.receipt.causalAttemptId : null,
          causal_step_attempt:
            input.receipt.purpose === "observation" ? input.receipt.causalAttemptId : null,
          from_status: expectedFromStatus,
          to_status: expectedToStatus,
          transition_id: expectedTransitionId,
        },
      )
    ) {
      return intervention("The classified saga history contradicts its terminal receipt.")
    }
    return input.saga
  }

  async #recoveredProjection(input: {
    readonly actorChecksum: string
    readonly attemptId: string
    readonly evidenceChecksum: string
    readonly mode: SagaRecoveryMode
    readonly operation: LoadedOperation
    readonly operationStepId: string
    readonly phase: SagaActionPhase
    readonly proof: LeaseProof
    readonly recoveryId: string
    readonly saga: SagaRecord
    readonly sagaStepId: string
  }): Promise<SagaRecord | undefined> {
    const transitionId = operationTransitionIdentity("crash-recovered", [
      input.operation.operation.plan.operationId,
      input.operationStepId,
      input.recoveryId,
    ])
    const transition = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`)
      .bind(transitionId)
      .first<TransitionReceiptRow>()
    if (transition === null) return undefined
    if (
      !exactColumns(transition as unknown as Record<string, unknown>, {
        acquisition_id: input.proof.acquisitionId,
        fencing_token: input.proof.fencingToken,
        holder_id: input.proof.holderId,
        lease_key: input.proof.leaseKey,
        operation_id: input.operation.operation.plan.operationId,
        step_id: input.operationStepId,
        transition_id: transitionId,
      })
    ) {
      return intervention("The recovered saga action has a contradictory operation transition.")
    }
    const auditRow = await this.#database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(input.operation.environmentId, transition.audit_event_hash)
      .first<AuditEventRow>()
    if (auditRow === null) {
      return intervention("The recovered saga action lacks its exact audit event.")
    }
    const audit = await loadAuditEvent(
      persistedJson(auditRow.event_json, "The recovered saga audit event"),
      this.#digest,
    )
    if (
      !exactColumns(audit as unknown as Record<string, unknown>, {
        actorChecksum: input.actorChecksum,
        environmentId: input.operation.environmentId,
        eventHash: transition.audit_event_hash,
        eventType: "saga.action.recovered",
        fencingToken: input.proof.fencingToken,
        idempotencyKey: transitionId,
        operationId: input.operation.operation.plan.operationId,
        payloadChecksum: input.evidenceChecksum,
        stepId: input.operationStepId,
      })
    ) {
      return intervention("The recovered saga audit event is contradictory.")
    }
    const beforeStep = await this.#historicalStep(
      input.operation,
      input.operationStepId,
      transition.from_record_json,
    )
    const afterStep = await this.#historicalStep(
      input.operation,
      input.operationStepId,
      transition.to_record_json,
    )
    const effect = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "transition_id" = ?1 AND "resource_kind" = 'saga' AND "resource_id" = ?2`,
      )
      .bind(transitionId, input.saga.sagaId)
      .first<EffectReceiptRow>()
    if (effect === null) {
      return intervention("The recovered saga action lacks its exact coupled effect receipt.")
    }
    const afterSaga = await this.#historicalSaga(effect)
    const priorEffect = await this.#database
      .prepare(
        `SELECT * FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1 AND "to_state_version" = ?2`,
      )
      .bind(afterSaga.sagaId, afterSaga.stateVersion - 1)
      .first<EffectReceiptRow>()
    if (priorEffect === null) {
      return intervention("The recovered saga action lacks its prior immutable saga version.")
    }
    const beforeSaga = await this.#historicalSaga(priorEffect)
    const operationBefore = await this.#operationBefore(input.operation, audit.sequence)
    const expected = recoveredRecords({
      attemptId: input.attemptId,
      beforeOperation: operationBefore,
      beforeSaga,
      evidenceChecksum: input.evidenceChecksum,
      mode: input.mode,
      operationStepId: input.operationStepId,
      phase: input.phase,
      sagaStepId: input.sagaStepId,
      serverTimeMs: retryServerTime(beforeSaga, afterSaga, input.sagaStepId, input.phase),
    })
    const effectKind = `action:${input.phase}:recovery:${
      input.mode === "accepted_unknown" ? "unknown" : "not-dispatched"
    }`
    const effectId = await this.#identity("saga-effect", [
      transitionId,
      afterSaga.sagaId,
      effectKind,
      afterSaga.stateVersion.toString(10),
    ])
    const beforeAction = beforeSaga.steps[input.sagaStepId]?.[input.phase] as SagaActionRecord
    const expectedStep = expected.operation.steps[input.operationStepId] as OperationStepRecord
    if (
      !exactColumns(
        {
          ...(effect as unknown as Record<string, unknown>),
          after_saga: encode(afterSaga),
          after_step: operationStepRecordJson(afterStep),
          before_action_attempt: beforeAction.activeAttemptId,
          before_action_state: beforeAction.state,
          before_step: operationStepRecordJson(beforeStep),
          before_step_attempt: beforeStep.activeAttemptId,
          before_step_fence: beforeStep.fencingToken,
          loaded_saga_id: afterSaga.sagaId,
        },
        {
          acquisition_id: input.proof.acquisitionId,
          after_saga: encode(expected.saga),
          after_step: operationStepRecordJson(expectedStep),
          before_action_attempt: input.attemptId,
          before_action_state: "running",
          before_step: operationStepRecordJson(
            operationBefore.steps[input.operationStepId] as OperationStepRecord,
          ),
          before_step_attempt: input.attemptId,
          before_step_fence: beforeStep.fencingToken,
          effect_id: effectId,
          effect_kind: effectKind,
          evidence_checksum: input.evidenceChecksum,
          fencing_token: input.proof.fencingToken,
          from_state_version: beforeSaga.stateVersion,
          holder_id: input.proof.holderId,
          lease_key: input.proof.leaseKey,
          loaded_saga_id: input.saga.sagaId,
          operation_id: input.operation.operation.plan.operationId,
          record_checksum: effect.record_checksum,
          record_json: effect.record_json,
          resource_id: afterSaga.sagaId,
          resource_kind: "saga",
          step_id: input.operationStepId,
          to_state_version: afterSaga.stateVersion,
          transition_id: transitionId,
        },
      ) ||
      transition.from_operation_status !== operationStatus(operationBefore) ||
      transition.to_operation_status !== operationStatus(expected.operation)
    ) {
      return intervention("The recovered saga history contradicts its dispatch evidence.")
    }
    return input.saga
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
         ) AND (
           ?13 = 'none'
           OR (?13 = 'not_dispatched' AND NOT EXISTS (
             SELECT 1 FROM "nozzle_saga_action_attempts" WHERE "attempt_id" = ?14
           ))
           OR (?13 = 'accepted_unknown' AND EXISTS (
             SELECT 1
             FROM "nozzle_saga_action_attempts" AS "attempt"
             LEFT JOIN "nozzle_saga_action_attempt_outcomes" AS "outcome" USING ("attempt_id")
             WHERE "attempt"."attempt_id" = ?14
               AND "attempt"."operation_id" = ?2
               AND "attempt"."operation_step_id" = ?3
               AND "attempt"."purpose" = 'effect'
               AND "attempt"."acceptance_checksum" = ?15
               AND "attempt"."fencing_token" = ?16
               AND "outcome"."attempt_id" IS NULL
           ))
         ) AND (
           ?17 = 'none' OR EXISTS (
             SELECT 1 FROM "nozzle_sagas"
             WHERE "saga_id" = ?18 AND "operation_id" = ?2 AND "record_json" = ?19
               AND "termination_cause" IS NULL
               AND (?17 = 'cancellation'
                 OR (?17 = 'timeout' AND "deadline_at_ms" <= ${SERVER_TIME_SQL}))
           )
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
        input.recoveryGuard?.mode ?? "none",
        input.recoveryGuard?.attemptId ?? "none",
        input.recoveryGuard?.acceptanceChecksum ?? "none",
        input.recoveryGuard?.dispatchFencingToken ?? input.proof.fencingToken,
        input.terminationGuard?.cause ?? "none",
        input.terminationGuard?.sagaId ?? "none",
        beforeSagaJson ?? "none",
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

  async #verify(
    input: CoupledTransition,
    recordJson: string,
    recordChecksum: string,
  ): Promise<CoupledCommitResult | undefined> {
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
    if (transition === null) return undefined
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
    if (isIrreversibleAuthorizationDispatch(input)) {
      const authorization = afterRecord.irreversibleAuthorization
      const authorizationReceipt = await this.#database
        .prepare(
          `SELECT "transition_id", "authorization_id", "authorization_checksum",
                  "protocol_version", "classified_at_ms"
           FROM "nozzle_irreversible_authorization_receipts" WHERE "transition_id" = ?1`,
        )
        .bind(input.transitionId)
        .first<IrreversibleAuthorizationReceiptRow>()
      if (
        authorization === undefined ||
        authorizationReceipt === null ||
        authorizationReceipt.transition_id !== input.transitionId ||
        authorizationReceipt.authorization_id !== authorization.authorizationId ||
        authorizationReceipt.authorization_checksum !== authorization.authorizationChecksum ||
        authorizationReceipt.protocol_version !== 2 ||
        !Number.isSafeInteger(transition.created_at_ms) ||
        transition.created_at_ms < 0 ||
        authorizationReceipt.classified_at_ms !== transition.created_at_ms ||
        !Number.isSafeInteger(authorizationReceipt.classified_at_ms) ||
        authorizationReceipt.classified_at_ms < 0
      ) {
        return intervention(
          "The irreversible saga transition lacks its exact authorization receipt.",
        )
      }
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
    const auditRow = await this.#database
      .prepare(
        `SELECT "event_json" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(input.beforeOperation.environmentId, transition.audit_event_hash)
      .first<AuditEventRow>()
    if (auditRow === null) {
      return intervention("The coupled saga transition lacks its exact audit event.")
    }
    const audit = await loadAuditEvent(
      persistedJson(auditRow.event_json, "The coupled saga audit event"),
      this.#digest,
    )
    if (
      !exactColumns(audit as unknown as Record<string, unknown>, {
        actorChecksum: input.actorChecksum,
        environmentId: input.beforeOperation.environmentId,
        eventHash: transition.audit_event_hash,
        eventType: input.auditEventType,
        fencingToken: input.proof.fencingToken,
        idempotencyKey: input.transitionId,
        operationId: input.beforeOperation.operation.plan.operationId,
        payloadChecksum: input.evidenceChecksum,
        stepId: input.stepId,
      })
    ) {
      return intervention("The coupled saga audit event is contradictory.")
    }
    const operation = await this.#operations.get(input.beforeOperation.operation.plan.operationId)
    const saga = await this.#sagas.get(input.afterSaga.sagaId)
    if (operation === undefined) {
      return intervention("The coupled operation projection does not match its receipt.")
    }
    if (saga === undefined) {
      return intervention("The coupled saga projection does not match its receipt.")
    }
    const operationIsExact = sameOperation(operation.operation, input.afterOperation)
    const sagaIsExact = sameSaga(saga, input.afterSaga)
    if (operationIsExact && sagaIsExact) {
      return Object.freeze({ kind: "exact", saga })
    }
    if (
      saga.stateVersion < input.afterSaga.stateVersion ||
      (saga.stateVersion === input.afterSaga.stateVersion && !sagaIsExact)
    ) {
      return intervention("The coupled saga projection does not descend from its receipt.")
    }
    return Object.freeze({ kind: "advanced", saga })
  }

  async #commit(input: CoupledTransition): Promise<CoupledCommitResult | undefined> {
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
    if (results !== undefined) {
      mutationResults(results, statements.length, isIrreversibleAuthorizationDispatch(input))
    }
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
      const replay = await this.#initializedProjection(input)
      if (replay !== undefined) return replay
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
        return intervention("Saga initialization exists without its immutable receipt.")
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
      const committed = await this.#commit({
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
      const persisted = committedSaga(committed, fresh)
      if (persisted !== undefined) return persisted
    }
    return intervention("Saga initialization exceeded the bounded coupled retry budget.")
  }

  async requestTermination(input: RequestCoordinatedSagaTerminationInput): Promise<SagaRecord> {
    boundedText(input.actorChecksum, "Saga termination actor checksum")
    boundedText(input.operationId, "Operation ID")
    boundedText(input.requestId, "Saga termination request ID")
    boundedText(input.requestChecksum, "Saga termination request checksum")
    boundedText(input.sagaId, "Saga ID")
    if (input.cause !== "cancellation" && input.cause !== "timeout") {
      return configuration("Saga termination request cause is unsupported.")
    }
    const identity = await this.#terminationIdentity(input)
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const authorized = await this.#leases.authorizeAt(input.proof)
      const replay = await this.#terminationReceiptProjection({
        cause: input.cause,
        identity,
        operationId: input.operationId,
        proof: input.proof,
        sagaId: input.sagaId,
      })
      if (replay !== undefined) return replay
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      if (
        beforeSaga.terminationCause === "cancellation" ||
        beforeSaga.terminationCause === "timeout"
      ) {
        return this.#existingTerminationProjection({
          operationId: input.operationId,
          proof: input.proof,
          sagaId: input.sagaId,
        })
      }
      if (beforeSaga.terminationCause === "failure") return beforeSaga
      const command = nextSagaCommand(beforeSaga, authorized.serverTimeMs)
      if (command.kind === "terminal") return beforeSaga
      if (input.cause === "timeout" && command.kind !== "request_termination") {
        return resume("The authoritative saga deadline has not expired.")
      }
      const planStep = terminationPlanStep(beforeOperation.operation)
      const afterOperation = recordAtomicStepOutcome(beforeOperation.operation, {
        attemptId: identity.attemptId,
        idempotencyKey: planStep.idempotencyKey,
        leaseProof: input.proof,
        observedPreconditionChecksum: planStep.preconditionChecksum,
        outcome: {
          observedPostconditionChecksum: planStep.postconditionChecksum,
          resultChecksum: identity.evidenceChecksum,
          state: "succeeded",
        },
        stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
      })
      const afterSaga = requestSagaTermination(beforeSaga, {
        cause: input.cause,
        serverTimeMs: authorized.serverTimeMs,
      })
      const effectKind = `termination:${input.cause}`
      const effectId = await this.#identity("saga-effect", [
        identity.transitionId,
        input.sagaId,
        effectKind,
        afterSaga.stateVersion.toString(10),
      ])
      let committed: CoupledCommitResult | undefined
      try {
        committed = await this.#commit({
          actorChecksum: input.actorChecksum,
          afterOperation,
          afterSaga,
          auditEventType: "saga.termination.requested",
          beforeOperation,
          beforeSaga,
          effectId,
          effectKind,
          evidenceChecksum: identity.evidenceChecksum,
          proof: input.proof,
          stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
          terminationGuard: { cause: input.cause, sagaId: input.sagaId },
          transitionId: identity.transitionId,
        })
      } catch (error) {
        const winner = await this.#terminationReceiptProjection({
          cause: input.cause,
          identity,
          operationId: input.operationId,
          proof: input.proof,
          sagaId: input.sagaId,
        })
        if (winner !== undefined) return winner
        throw error
      }
      const persisted = committedSaga(committed, afterSaga)
      if (persisted !== undefined) return persisted
    }
    return intervention("Saga termination exceeded the bounded coupled retry budget.")
  }

  async beginAction(input: BeginCoordinatedSagaActionInput): Promise<SagaBeginDecision> {
    const irreversibleAuthorization = input.irreversibleAuthorization
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
        ...(irreversibleAuthorization === undefined ? {} : { irreversibleAuthorization }),
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
      const committed = await this.#commit({
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
      if (committed?.kind === "exact") return sagaDecision
    }
    return intervention("Beginning a saga action exceeded the bounded coupled retry budget.")
  }

  async recoverActionAfterCrash(input: RecoverCoordinatedSagaActionInput): Promise<SagaRecord> {
    boundedText(input.actorChecksum, "Saga coordinator actor checksum")
    boundedText(input.attemptId, "Saga recovery attempt ID")
    boundedText(input.recoveryId, "Saga recovery ID")
    const operationStepId = sagaActionOperationStepId(input.stepId, input.phase)
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const receipt = await this.#attempts.get(input.attemptId)
      if (
        receipt !== undefined &&
        (receipt.sagaId !== input.sagaId ||
          receipt.operationId !== input.operationId ||
          receipt.operationStepId !== operationStepId ||
          receipt.sagaStepId !== input.stepId ||
          receipt.phase !== input.phase ||
          receipt.purpose !== "effect")
      ) {
        return intervention("The saga recovery receipt belongs to a different action.")
      }
      if (receipt !== undefined && receipt.state !== "accepted") {
        return this.settleActionFromReceipt({
          actorChecksum: input.actorChecksum,
          attemptId: input.attemptId,
          operationId: input.operationId,
          phase: input.phase,
          proof: input.proof,
          sagaId: input.sagaId,
          stepId: input.stepId,
        })
      }
      const mode: SagaRecoveryMode = receipt === undefined ? "not_dispatched" : "accepted_unknown"
      const evidenceChecksum =
        receipt?.acceptanceChecksum ??
        (await checkedDigest(this.#digest, COORDINATOR_ID_DOMAIN, [
          "recovery-not-dispatched",
          input.operationId,
          operationStepId,
          input.attemptId,
          input.recoveryId,
          input.proof.fencingToken.toString(10),
        ]))
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      const replay = await this.#recoveredProjection({
        actorChecksum: input.actorChecksum,
        attemptId: input.attemptId,
        evidenceChecksum,
        mode,
        operation: beforeOperation,
        operationStepId,
        phase: input.phase,
        proof: input.proof,
        recoveryId: input.recoveryId,
        saga: beforeSaga,
        sagaStepId: input.stepId,
      })
      if (replay !== undefined) return replay
      if (receipt?.protocolVersion === 1) {
        await this.#attempts.validateProjectionReceipt({
          attemptId: input.attemptId,
          proof: input.proof,
          requireState: "accepted",
        })
      }
      const action = beforeSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      const operationAction = beforeOperation.operation.steps[
        operationStepId
      ] as OperationStepRecord
      if (
        !exactColumns(
          {
            action_attempt: action.activeAttemptId,
            action_state: action.state,
            operation_attempt: operationAction.activeAttemptId,
            operation_fence_is_present: operationAction.fencingToken !== undefined,
            operation_state: operationAction.state,
            recovery_fence_is_newer:
              operationAction.fencingToken !== undefined &&
              input.proof.fencingToken > operationAction.fencingToken,
          },
          {
            action_attempt: input.attemptId,
            action_state: "running",
            operation_attempt: input.attemptId,
            operation_fence_is_present: true,
            operation_state: "running",
            recovery_fence_is_newer: true,
          },
        )
      ) {
        return intervention("Saga crash recovery contradicts the active coupled attempt.")
      }
      const originalFencingToken = operationAction.fencingToken as number
      const planStep = beforeOperation.operation.plan.steps.find(
        (step) => step.stepId === operationStepId,
      ) as OperationStepPlan
      if (
        planStep.leaseKey !== input.proof.leaseKey ||
        (receipt !== undefined &&
          (receipt.leaseKey !== planStep.leaseKey || receipt.fencingToken !== originalFencingToken))
      ) {
        return intervention("Saga crash recovery has a contradictory dispatch fence.")
      }
      const authorized = await this.#leases.authorizeAt(input.proof)
      const recovered = recoveredRecords({
        attemptId: input.attemptId,
        beforeOperation: beforeOperation.operation,
        beforeSaga,
        evidenceChecksum,
        mode,
        operationStepId,
        phase: input.phase,
        sagaStepId: input.stepId,
        serverTimeMs: authorized.serverTimeMs,
      })
      const transitionId = operationTransitionIdentity("crash-recovered", [
        input.operationId,
        operationStepId,
        input.recoveryId,
      ])
      const effectKind = `action:${input.phase}:recovery:${
        mode === "accepted_unknown" ? "unknown" : "not-dispatched"
      }`
      const effectId = await this.#identity("saga-effect", [
        transitionId,
        input.sagaId,
        effectKind,
        recovered.saga.stateVersion.toString(10),
      ])
      const committed = await this.#commit({
        actorChecksum: input.actorChecksum,
        afterOperation: recovered.operation,
        afterSaga: recovered.saga,
        auditEventType: "saga.action.recovered",
        beforeOperation,
        beforeSaga,
        effectId,
        effectKind,
        evidenceChecksum,
        proof: input.proof,
        recoveryGuard: {
          ...(receipt === undefined ? {} : { acceptanceChecksum: receipt.acceptanceChecksum }),
          attemptId: input.attemptId,
          dispatchFencingToken: originalFencingToken,
          mode,
        },
        stepId: operationStepId,
        transitionId,
      })
      const persisted = committedSaga(committed, recovered.saga)
      if (persisted !== undefined) return persisted
    }
    return intervention("Recovering a saga action exceeded the bounded coupled retry budget.")
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
    if (!receiptConsumerMatches(receipt, input.proof)) {
      return resume("The terminal saga receipt cannot be consumed under this lease fence.")
    }
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      const action = beforeSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      const operationAction = beforeOperation.operation.steps[
        operationStepId
      ] as OperationStepRecord
      const classified = await this.#classifiedProjection({
        auditEventType: "saga.action.classified",
        operation: beforeOperation,
        operationStepId,
        phase: input.phase,
        receipt,
        saga: beforeSaga,
        transitionKinds:
          receipt.state === "not_applied"
            ? ["failed", "succeeded"]
            : [receipt.state === "unknown" ? "failed" : "succeeded"],
      })
      if (classified !== undefined) return classified
      if (receipt.protocolVersion === 1) {
        await this.#attempts.validateProjectionReceipt({
          attemptId: input.attemptId,
          proof: input.proof,
          requireState: "terminal",
        })
      }
      const planStep = beforeOperation.operation.plan.steps.find(
        (step) => step.stepId === operationStepId,
      ) as OperationStepPlan
      if (
        !exactColumns(
          {
            action_attempt: action.activeAttemptId,
            action_state: action.state,
            operation_attempt: operationAction.activeAttemptId,
            operation_fence: operationAction.fencingToken,
            operation_state: operationAction.state,
            plan_effect_protocol: planStep.effectProtocol,
            plan_lease_key: planStep.leaseKey,
          },
          {
            action_attempt: input.attemptId,
            action_state: "running",
            operation_attempt: input.attemptId,
            operation_fence: receipt.fencingToken,
            operation_state: "running",
            plan_effect_protocol: "saga_receipt",
            plan_lease_key: receipt.leaseKey,
          },
        )
      ) {
        return intervention("The terminal saga receipt contradicts the active coupled attempt.")
      }
      const authorized = await this.#leases.authorizeAt(input.proof)
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
      const committed = await this.#commit({
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
      const persisted = committedSaga(committed, afterSaga)
      if (persisted !== undefined) return persisted
    }
    return intervention("Settling a saga action exceeded the bounded coupled retry budget.")
  }

  async settleObservationFromReceipt(
    input: SettleCoordinatedSagaObservationInput,
  ): Promise<SagaRecord> {
    boundedText(input.actorChecksum, "Saga coordinator actor checksum")
    boundedText(input.attemptId, "Saga observation attempt ID")
    const operationStepId = sagaActionOperationStepId(input.stepId, input.phase)
    const receipt = await this.#attempts.get(input.attemptId)
    if (receipt === undefined) return resume("The saga observation was not durably accepted.")
    terminalAttempt(receipt)
    if (receipt.state === "failed" || receipt.state === "unknown") {
      return intervention("A saga observation receipt has an incompatible terminal outcome.")
    }
    if (
      receipt.sagaId !== input.sagaId ||
      receipt.operationId !== input.operationId ||
      receipt.operationStepId !== operationStepId ||
      receipt.sagaStepId !== input.stepId ||
      receipt.phase !== input.phase ||
      receipt.purpose !== "observation"
    ) {
      return intervention("The terminal saga observation receipt belongs to a different action.")
    }
    const causalAttemptId = receipt.causalAttemptId as string
    if (!receiptConsumerMatches(receipt, input.proof)) {
      return resume("The saga observation receipt cannot be consumed under this lease fence.")
    }
    const cause = await this.#attempts.get(causalAttemptId)
    if (
      cause === undefined ||
      cause.sagaId !== input.sagaId ||
      cause.operationId !== input.operationId ||
      cause.operationStepId !== operationStepId ||
      cause.sagaStepId !== input.stepId ||
      cause.phase !== input.phase ||
      cause.purpose !== "effect" ||
      (cause.state !== "accepted" && cause.state !== "unknown")
    ) {
      return intervention("The saga observation receipt has no exact unknown causal effect.")
    }
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
      const beforeOperation = await this.#operation(input.operationId)
      const beforeSaga = await this.#saga(input.sagaId, input.operationId)
      if (beforeSaga === undefined) return resume("The saga does not exist.")
      const action = beforeSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      const operationAction = beforeOperation.operation.steps[
        operationStepId
      ] as OperationStepRecord
      const classified = await this.#classifiedProjection({
        auditEventType: "saga.action.observed",
        operation: beforeOperation,
        operationStepId,
        phase: input.phase,
        receipt,
        saga: beforeSaga,
        transitionKinds: ["reconciled"],
      })
      if (classified !== undefined) return classified
      if (receipt.protocolVersion === 1) {
        await this.#attempts.validateProjectionReceipt({
          attemptId: input.attemptId,
          proof: input.proof,
          requireState: "terminal",
        })
      }
      if (
        !exactColumns(
          {
            action_attempt: action.lastAttemptId,
            action_error_matches:
              cause.state !== "unknown" || action.errorChecksum === cause.errorChecksum,
            action_state: action.state,
            fence_is_present: operationAction.fencingToken !== undefined,
            fence_is_newer:
              operationAction.fencingToken !== undefined &&
              receipt.fencingToken > operationAction.fencingToken,
            operation_attempt: operationAction.lastAttemptId,
            operation_error_matches:
              cause.state !== "unknown" || operationAction.errorChecksum === cause.outcomeChecksum,
            operation_state: operationAction.state,
          },
          {
            action_attempt: causalAttemptId,
            action_error_matches: true,
            action_state: "unknown",
            fence_is_present: true,
            fence_is_newer: true,
            operation_attempt: causalAttemptId,
            operation_error_matches: true,
            operation_state: "unknown",
          },
        )
      ) {
        return intervention("The saga observation receipt contradicts the unknown coupled action.")
      }
      const authorized = await this.#leases.authorizeAt(input.proof)
      const planStep = beforeOperation.operation.plan.steps.find(
        (step) => step.stepId === operationStepId,
      ) as OperationStepPlan
      const observationOutcome = receipt.state === "confirmed" ? "applied" : receipt.state
      const afterSaga = recordSagaObservation(beforeSaga, {
        evidenceChecksum: receipt.outcomeChecksum,
        outcome: observationOutcome,
        phase: input.phase,
        ...(receipt.state === "confirmed" ? { resultChecksum: receipt.outputChecksum } : {}),
        serverTimeMs: authorized.serverTimeMs,
        stepId: input.stepId,
      })
      const afterAction = afterSaga.steps[input.stepId]?.[input.phase] as SagaActionRecord
      let afterOperation: OperationRecord
      if (receipt.state === "confirmed") {
        afterOperation = recordStepReconciliation(beforeOperation.operation, {
          evidenceChecksum: receipt.outcomeChecksum,
          observedPostconditionChecksum: planStep.postconditionChecksum,
          outcome: "applied",
          resultChecksum: receipt.outcomeChecksum,
          stepId: operationStepId,
        })
      } else if (receipt.state === "indeterminate") {
        afterOperation = recordStepReconciliation(beforeOperation.operation, {
          evidenceChecksum: receipt.outcomeChecksum,
          outcome: "indeterminate",
          stepId: operationStepId,
        })
      } else if (afterAction.state === "retryable_failed") {
        afterOperation = recordStepReconciliation(beforeOperation.operation, {
          evidenceChecksum: receipt.outcomeChecksum,
          outcome: "not_applied",
          stepId: operationStepId,
        })
      } else {
        afterOperation = recordSagaStepTerminalClassification(beforeOperation.operation, {
          outcome: "not_applied",
          receiptOutcomeChecksum: receipt.outcomeChecksum,
          stepId: operationStepId,
        })
      }
      const transitionId = operationTransitionIdentity("reconciled", [
        input.operationId,
        operationStepId,
        input.attemptId,
      ])
      const effectKind = classifiedObservationEffectKind(input.phase, receipt)
      const effectId = await this.#identity("saga-effect", [
        transitionId,
        input.sagaId,
        effectKind,
        afterSaga.stateVersion.toString(10),
      ])
      const committed = await this.#commit({
        actorChecksum: input.actorChecksum,
        afterOperation,
        afterSaga,
        auditEventType: "saga.action.observed",
        beforeOperation,
        beforeSaga,
        effectId,
        effectKind,
        evidenceChecksum: receipt.outcomeChecksum,
        proof: input.proof,
        stepId: operationStepId,
        transitionId,
      })
      const persisted = committedSaga(committed, afterSaga)
      if (persisted !== undefined) return persisted
    }
    return intervention("Settling a saga observation exceeded the bounded coupled retry budget.")
  }
}
