import {
  type DigestFunction,
  type LeaseProof,
  NozzleError,
  type OperationStepRecord,
  type SagaActionPhase,
  type SagaActionReference,
  type SagaRecord,
  sagaActionKey,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"
import { D1OperationStore } from "./operation-store.js"
import {
  acceptedSagaAttemptRecord,
  boundedSagaReceiptText,
  canonicalSagaReceiptJson,
  loadSagaAttemptIdentityRow,
  loadSagaAttemptOutcomeRow,
  MAX_SAGA_RECEIPT_IDENTITY_BYTES,
  SAGA_ATTEMPT_ERROR_DOMAIN,
  SAGA_ATTEMPT_EVIDENCE_DOMAIN,
  SAGA_ATTEMPT_IDENTITY_ROW_SELECT,
  SAGA_ATTEMPT_INPUT_DOMAIN,
  SAGA_ATTEMPT_OUTCOME_ROW_SELECT,
  SAGA_ATTEMPT_OUTPUT_DOMAIN,
  SAGA_ATTEMPT_PAYLOAD_ROW_SELECT,
  SAGA_OUTCOME_ERROR_REFERENCE_JSON,
  SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
  SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
  type SagaAttemptIdentityRow,
  type SagaAttemptOutcomeRow,
  type SagaAttemptOutcomeState,
  type SagaAttemptPayloadRow,
  type SagaAttemptPurpose,
  type SagaAttemptRecord,
  sagaAttemptAcceptanceChecksum,
  sagaAttemptOutcomeChecksum,
  sagaReceiptPayloadChecksum,
} from "./saga-attempt-codec.js"
import { D1SagaStore, sagaActionOperationStepId } from "./saga-store.js"

export type {
  SagaAttemptIdentity,
  SagaAttemptOutcomeState,
  SagaAttemptPurpose,
  SagaAttemptRecord,
} from "./saga-attempt-codec.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const CHECKSUM = /^[0-9a-f]{64}$/u

export interface AcceptSagaAttemptInput {
  readonly attemptId: string
  readonly inputJson: string
  readonly phase: SagaActionPhase
  readonly proof: LeaseProof
  readonly purpose: SagaAttemptPurpose
  readonly sagaId: string
  readonly sagaStepId: string
}

interface CompleteSagaAttemptBase {
  readonly attemptId: string
  readonly evidenceJson: string
  readonly proof: LeaseProof
}

export type CompleteSagaAttemptInput =
  | (CompleteSagaAttemptBase & {
      readonly outputJson: string
      readonly state: "confirmed"
    })
  | (CompleteSagaAttemptBase & {
      readonly errorJson: string
      readonly state: Exclude<SagaAttemptOutcomeState, "confirmed">
    })

export interface ValidateSagaProjectionReceiptInput {
  readonly attemptId: string
  readonly proof: LeaseProof
  readonly requireState: "accepted" | "terminal"
}

interface SagaProjectionRow {
  readonly operation_id: string
}

interface SagaOutcomePayloadActivationRow {
  readonly activated_at_ms: unknown
  readonly protocol_version: unknown
  readonly reader_barrier_checksum: unknown
}

interface SagaSchemaVersionRow {
  readonly schema_version: unknown
}

function capturedPlainRecord(value: unknown, label: string): Record<string, unknown> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention(`${label} could not be captured safely.`)
  }
  if (
    typeof snapshot !== "object" ||
    snapshot === null ||
    Array.isArray(snapshot) ||
    Object.getPrototypeOf(snapshot) !== Object.prototype
  ) {
    return intervention(`${label} is malformed.`)
  }
  return snapshot as Record<string, unknown>
}

function capturedQueryRows(value: unknown): readonly unknown[] {
  const result = capturedPlainRecord(value, "Control D1 saga-payload query metadata")
  if (
    result.success !== true ||
    typeof result.meta !== "object" ||
    result.meta === null ||
    Array.isArray(result.meta) ||
    !Array.isArray(result.results)
  ) {
    return intervention("Control D1 saga-payload query metadata is malformed.")
  }
  return result.results
}

interface ActionBinding {
  readonly actionKey: string
  readonly causalAttemptId: string | null
  readonly idempotencyKey: string
  readonly operationId: string
  readonly operationStepId: string
  readonly saga: SagaRecord
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

function validProof(proof: LeaseProof): void {
  boundedSagaReceiptText(proof.leaseKey, "Lease key", MAX_SAGA_RECEIPT_IDENTITY_BYTES)
  boundedSagaReceiptText(proof.holderId, "Lease holder ID", MAX_SAGA_RECEIPT_IDENTITY_BYTES)
  boundedSagaReceiptText(
    proof.acquisitionId,
    "Lease acquisition ID",
    MAX_SAGA_RECEIPT_IDENTITY_BYTES,
  )
  if (!Number.isSafeInteger(proof.fencingToken) || proof.fencingToken < 1) {
    configuration("Lease fencing token must be a positive safe integer.")
  }
}

export async function sagaActionInputChecksum(
  inputJsonValue: string,
  digest: DigestFunction,
): Promise<string> {
  if (typeof digest !== "function") configuration("A saga action digest is required.")
  const json = canonicalSagaReceiptJson(inputJsonValue, "Saga action input", false)
  return sagaReceiptPayloadChecksum(digest, SAGA_ATTEMPT_INPUT_DOMAIN, json)
}

export function sagaObservationIdempotencyKey(actionIdempotencyKey: string): string {
  return `${boundedSagaReceiptText(actionIdempotencyKey, "Saga action idempotency key")}:observation`
}

function changed(result: ControlRunResult, maximumChanges = 1): boolean {
  const changes = result.meta.changes
  if (
    result.success !== true ||
    !Number.isSafeInteger(changes) ||
    (changes as number) < 0 ||
    (changes as number) > maximumChanges
  ) {
    return intervention("Control D1 returned malformed saga-receipt mutation metadata.")
  }
  return changes === 1
}

function phase(value: unknown): asserts value is SagaActionPhase {
  if (value !== "forward" && value !== "compensation") {
    configuration("Saga action phase is invalid.")
  }
}

function purpose(value: unknown): asserts value is SagaAttemptPurpose {
  if (value !== "effect" && value !== "observation") {
    configuration("Saga attempt purpose is invalid.")
  }
}

function reference(
  saga: SagaRecord,
  stepId: string,
  actionPhase: SagaActionPhase,
  attemptPurpose: SagaAttemptPurpose,
): SagaActionReference {
  const descriptor = saga.descriptor.steps.find(
    (step) => step.stepId === stepId,
  ) as (typeof saga.descriptor.steps)[number]
  const references = {
    "effect:compensation": descriptor.compensationAction,
    "effect:forward": descriptor.forwardAction,
    "observation:compensation": descriptor.compensationObservation,
    "observation:forward": descriptor.forwardObservation,
  }
  return references[`${attemptPurpose}:${actionPhase}`] as SagaActionReference
}

function actionRecord(saga: SagaRecord, stepId: string, actionPhase: SagaActionPhase) {
  const step = saga.steps[stepId]
  if (step === undefined) return intervention("Persisted saga action state is missing.")
  return step[actionPhase]
}

export class D1SagaAttemptStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction
  readonly #operations: D1OperationStore
  readonly #sagas: D1SagaStore

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A control D1 database binding is required.")
    }
    if (typeof digest !== "function") configuration("A saga-attempt digest is required.")
    this.#database = database
    this.#digest = digest
    this.#operations = new D1OperationStore(database, digest)
    this.#sagas = new D1SagaStore(database, digest)
  }

  async #rowSafeOutcomePayloadsActivated(): Promise<boolean> {
    const publicationRow = await this.#database
      .prepare(
        `SELECT "schema_version" FROM "nozzle_control_schema_versions"
         WHERE "schema_version" = 4`,
      )
      .first<SagaSchemaVersionRow>()
    if (publicationRow === null) return false
    const publication = capturedPlainRecord(publicationRow, "Persisted control schema publication")
    if (Object.keys(publication).length !== 1 || publication.schema_version !== 4) {
      return intervention("Persisted control schema publication is malformed.")
    }
    const row = await this.#database
      .prepare(
        `SELECT "protocol_version", "reader_barrier_checksum", "activated_at_ms"
         FROM "nozzle_saga_outcome_payload_activations"
         WHERE "protocol_version" = 1`,
      )
      .first<SagaOutcomePayloadActivationRow>()
    if (row === null) return false
    const snapshot = capturedPlainRecord(row, "Persisted saga-outcome payload activation")
    if (
      Object.keys(snapshot).length !== 3 ||
      snapshot.protocol_version !== 1 ||
      typeof snapshot.reader_barrier_checksum !== "string" ||
      !CHECKSUM.test(snapshot.reader_barrier_checksum) ||
      !Number.isSafeInteger(snapshot.activated_at_ms) ||
      (snapshot.activated_at_ms as number) < 0
    ) {
      return intervention("Persisted saga-outcome payload activation is malformed.")
    }
    return true
  }

  async #binding(
    sagaId: string,
    sagaStepId: string,
    actionPhase: SagaActionPhase,
    attemptPurpose: SagaAttemptPurpose,
    attemptId: string,
    proof: LeaseProof,
  ): Promise<ActionBinding> {
    const row = await this.#database
      .prepare(`SELECT "operation_id" FROM "nozzle_sagas" WHERE "saga_id" = ?1`)
      .bind(sagaId)
      .first<SagaProjectionRow>()
    if (row === null) return resume("The saga does not exist.")
    if (typeof row.operation_id !== "string") {
      return intervention("Persisted saga action binding is malformed.")
    }
    const saga = await this.#sagas.get(sagaId)
    if (saga === undefined) return intervention("The persisted saga projection disappeared.")
    const operation = await this.#operations.get(row.operation_id)
    if (operation === undefined) {
      return intervention("The saga operation projection is missing.")
    }
    const current = actionRecord(saga, sagaStepId, actionPhase)
    if (
      attemptPurpose === "effect"
        ? current.state !== "running" || current.activeAttemptId !== attemptId
        : current.state !== "unknown"
    ) {
      return resume("The saga action is not eligible for this durable attempt.")
    }
    const actionKey = sagaActionKey(reference(saga, sagaStepId, actionPhase, attemptPurpose))
    const operationStepId = sagaActionOperationStepId(sagaStepId, actionPhase)
    const operationAction = operation.operation.steps[operationStepId] as
      | OperationStepRecord
      | undefined
    const planStep = operation.operation.plan.steps.find((step) => step.stepId === operationStepId)
    if (
      operationAction === undefined ||
      planStep === undefined ||
      planStep.effectProtocol !== "saga_receipt" ||
      planStep.leaseKey !== proof.leaseKey
    ) {
      return intervention("The saga action lacks its exact generic operation binding.")
    }
    let causalAttemptId: string | null = null
    if (attemptPurpose === "observation") {
      causalAttemptId = current.lastAttemptId as string
      if (
        operationAction.state !== "unknown" ||
        operationAction.lastAttemptId !== causalAttemptId ||
        operationAction.fencingToken === undefined ||
        proof.fencingToken <= operationAction.fencingToken
      ) {
        return intervention("The operation ledger is not eligible for saga observation.")
      }
      const cause = await this.get(causalAttemptId)
      if (
        cause === undefined ||
        cause.sagaId !== sagaId ||
        cause.operationId !== row.operation_id ||
        cause.operationStepId !== operationStepId ||
        cause.sagaStepId !== sagaStepId ||
        cause.phase !== actionPhase ||
        cause.purpose !== "effect" ||
        (cause.state !== "accepted" && cause.state !== "unknown")
      ) {
        return intervention("The saga observation lacks its checksum-verified causal receipt.")
      }
      const sagaErrorChecksum = current.errorChecksum
      const operationErrorChecksum = operationAction.errorChecksum
      if (
        cause.state === "unknown"
          ? sagaErrorChecksum !== cause.errorChecksum ||
            operationErrorChecksum !== cause.outcomeChecksum
          : sagaErrorChecksum !== cause.acceptanceChecksum ||
            operationErrorChecksum !== cause.acceptanceChecksum
      ) {
        return intervention("The operation and saga ledgers disagree about the observation cause.")
      }
    } else if (actionPhase === "compensation") {
      const forward = actionRecord(saga, sagaStepId, "forward")
      causalAttemptId = forward.lastAttemptId as string
      const cause = await this.get(causalAttemptId)
      if (
        cause === undefined ||
        cause.state !== "confirmed" ||
        cause.sagaId !== sagaId ||
        cause.operationId !== row.operation_id ||
        cause.operationStepId !== sagaActionOperationStepId(sagaStepId, "forward") ||
        cause.sagaStepId !== sagaStepId ||
        cause.phase !== "forward" ||
        cause.purpose !== "effect" ||
        cause.outputChecksum !== forward.resultChecksum
      ) {
        return intervention("Saga compensation lacks its exact confirmed forward receipt.")
      }
    }
    if (
      attemptPurpose === "effect" &&
      (operationAction.state !== "running" ||
        operationAction.activeAttemptId !== attemptId ||
        operationAction.fencingToken !== proof.fencingToken)
    ) {
      return intervention("The operation and saga ledgers disagree about the active attempt.")
    }
    return Object.freeze({
      actionKey,
      causalAttemptId,
      idempotencyKey:
        attemptPurpose === "effect"
          ? current.idempotencyKey
          : sagaObservationIdempotencyKey(current.idempotencyKey),
      operationId: row.operation_id,
      operationStepId,
      saga,
    })
  }

  async #validateProjectionBinding(receipt: SagaAttemptRecord, proof: LeaseProof): Promise<void> {
    const binding = await this.#binding(
      receipt.sagaId,
      receipt.sagaStepId,
      receipt.phase,
      receipt.purpose,
      receipt.attemptId,
      proof,
    )
    const sagaStep = binding.saga.steps[receipt.sagaStepId] as NonNullable<
      SagaRecord["steps"][string]
    >
    const expectedInputChecksum = {
      "effect:compensation": receipt.inputChecksum,
      "effect:forward": sagaStep.inputChecksum,
      "observation:compensation": receipt.inputChecksum,
      "observation:forward": receipt.inputChecksum,
    }[`${receipt.purpose}:${receipt.phase}`] as string
    const actual = JSON.stringify([
      receipt.sagaId,
      receipt.causalAttemptId,
      receipt.operationId,
      receipt.operationStepId,
      receipt.actionKey,
      receipt.idempotencyKey,
      receipt.inputChecksum,
      receipt.leaseKey,
      receipt.holderId,
      receipt.acquisitionId,
      receipt.fencingToken,
    ])
    const expected = JSON.stringify([
      binding.saga.sagaId,
      binding.causalAttemptId,
      binding.operationId,
      binding.operationStepId,
      binding.actionKey,
      binding.idempotencyKey,
      expectedInputChecksum,
      proof.leaseKey,
      proof.holderId,
      proof.acquisitionId,
      proof.fencingToken,
    ])
    if (actual !== expected) {
      return intervention(
        "A protocol-one saga receipt no longer has its exact safe projection binding.",
      )
    }
  }

  async get(attemptIdInput: string): Promise<SagaAttemptRecord | undefined> {
    const attemptId = boundedSagaReceiptText(
      attemptIdInput,
      "Saga attempt ID",
      MAX_SAGA_RECEIPT_IDENTITY_BYTES,
    )
    const identityRow = await this.#database
      .prepare(
        `SELECT ${SAGA_ATTEMPT_IDENTITY_ROW_SELECT}
         FROM "nozzle_saga_action_attempts" AS "attempt"
         LEFT JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
         WHERE "attempt"."attempt_id" = ?1`,
      )
      .bind(attemptId)
      .first<SagaAttemptIdentityRow>()
    if (identityRow === null) return undefined
    const identity = await loadSagaAttemptIdentityRow(identityRow, this.#digest, attemptId)
    const outcomeRow = await this.#database
      .prepare(
        `SELECT ${SAGA_ATTEMPT_OUTCOME_ROW_SELECT}
         FROM "nozzle_saga_action_attempt_outcomes" AS "outcome"
         WHERE "outcome"."attempt_id" = ?1`,
      )
      .bind(attemptId)
      .first<SagaAttemptOutcomeRow>()
    if (outcomeRow === null) return acceptedSagaAttemptRecord(identity)
    const rowSafeOutcomePayloads = await this.#rowSafeOutcomePayloadsActivated()
    const payloadRows = rowSafeOutcomePayloads
      ? capturedQueryRows(
          await this.#database
            .prepare(
              `SELECT ${SAGA_ATTEMPT_PAYLOAD_ROW_SELECT}
               FROM "nozzle_saga_action_attempt_outcome_payloads" AS "payload"
               WHERE "payload"."attempt_id" = ?1
               ORDER BY "payload"."payload_kind"
               LIMIT 4`,
            )
            .bind(attemptId)
            .all<SagaAttemptPayloadRow>(),
        )
      : []
    return loadSagaAttemptOutcomeRow(outcomeRow, payloadRows, identity, this.#digest)
  }

  async validateProjectionReceipt(
    input: ValidateSagaProjectionReceiptInput,
  ): Promise<SagaAttemptRecord> {
    const attemptId = boundedSagaReceiptText(
      input.attemptId,
      "Saga attempt ID",
      MAX_SAGA_RECEIPT_IDENTITY_BYTES,
    )
    validProof(input.proof)
    if (input.requireState !== "accepted" && input.requireState !== "terminal") {
      return configuration("Saga projection receipt state requirement is invalid.")
    }
    const receipt = await this.get(attemptId)
    if (receipt === undefined) return resume("The saga projection receipt was never accepted.")
    const receiptProof: LeaseProof = Object.freeze({
      acquisitionId: receipt.acquisitionId,
      fencingToken: receipt.fencingToken,
      holderId: receipt.holderId,
      leaseKey: receipt.leaseKey,
    })
    let bindingProof: LeaseProof
    if (input.requireState === "accepted") {
      if (receipt.state !== "accepted" || receipt.purpose !== "effect") {
        return intervention("Saga crash recovery requires an accepted effect receipt.")
      }
      if (
        input.proof.leaseKey !== receipt.leaseKey ||
        input.proof.fencingToken <= receipt.fencingToken
      ) {
        return resume("Saga crash recovery requires a strictly newer fence on the same lease.")
      }
      bindingProof = receiptProof
    } else {
      if (receipt.state === "accepted") {
        return resume("The saga settlement receipt is not terminal.")
      }
      const exactReceiptOwner =
        input.proof.holderId === receipt.holderId &&
        input.proof.acquisitionId === receipt.acquisitionId
      const fenceIsUsable =
        input.proof.fencingToken > receipt.fencingToken ||
        (input.proof.fencingToken === receipt.fencingToken && exactReceiptOwner)
      if (input.proof.leaseKey !== receipt.leaseKey || !fenceIsUsable) {
        return resume("Saga settlement requires the receipt lease or a strictly newer fence.")
      }
      bindingProof = receiptProof
    }
    if (receipt.protocolVersion === 2) return receipt
    await this.#validateProjectionBinding(receipt, bindingProof)
    return receipt
  }

  async accept(input: AcceptSagaAttemptInput): Promise<SagaAttemptRecord> {
    const attemptId = boundedSagaReceiptText(
      input.attemptId,
      "Saga attempt ID",
      MAX_SAGA_RECEIPT_IDENTITY_BYTES,
    )
    const sagaId = boundedSagaReceiptText(input.sagaId, "Saga ID", MAX_SAGA_RECEIPT_IDENTITY_BYTES)
    const sagaStepId = boundedSagaReceiptText(
      input.sagaStepId,
      "Saga step ID",
      MAX_SAGA_RECEIPT_IDENTITY_BYTES,
    )
    phase(input.phase)
    purpose(input.purpose)
    validProof(input.proof)
    const canonicalInput = canonicalSagaReceiptJson(input.inputJson, "Saga action input", false)
    const inputChecksum = await sagaReceiptPayloadChecksum(
      this.#digest,
      SAGA_ATTEMPT_INPUT_DOMAIN,
      canonicalInput,
    )
    const binding = await this.#binding(
      sagaId,
      sagaStepId,
      input.phase,
      input.purpose,
      attemptId,
      input.proof,
    )
    if (
      input.purpose === "effect" &&
      input.phase === "forward" &&
      binding.saga.steps[sagaStepId]?.inputChecksum !== inputChecksum
    ) {
      return intervention("Saga forward input contradicts its sealed durable checksum.")
    }
    const identityWithoutReceipt = Object.freeze({
      acquisitionId: input.proof.acquisitionId,
      actionKey: binding.actionKey,
      attemptId,
      causalAttemptId: binding.causalAttemptId,
      fencingToken: input.proof.fencingToken,
      holderId: input.proof.holderId,
      idempotencyKey: binding.idempotencyKey,
      inputChecksum,
      inputJson: canonicalInput,
      leaseKey: input.proof.leaseKey,
      operationId: binding.operationId,
      operationStepId: binding.operationStepId,
      phase: input.phase,
      purpose: input.purpose,
      sagaId,
      sagaStepId,
    })
    const checksum = await sagaAttemptAcceptanceChecksum(this.#digest, identityWithoutReceipt)
    const result = await this.#database
      .prepare(
        `INSERT INTO "nozzle_saga_action_attempts"
         ("attempt_id", "causal_attempt_id", "saga_id", "operation_id", "operation_step_id", "saga_step_id",
          "phase", "purpose", "action_key", "idempotency_key", "input_checksum", "input_json",
          "acceptance_checksum", "lease_key", "holder_id", "acquisition_id", "fencing_token",
          "accepted_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                ?16, ?17, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?14 AND "holder_id" = ?15 AND "acquisition_id" = ?16
             AND "fencing_token" = ?17 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )
         ON CONFLICT ("attempt_id") DO NOTHING`,
      )
      .bind(
        attemptId,
        binding.causalAttemptId,
        sagaId,
        binding.operationId,
        binding.operationStepId,
        sagaStepId,
        input.phase,
        input.purpose,
        binding.actionKey,
        binding.idempotencyKey,
        inputChecksum,
        canonicalInput,
        checksum,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
        input.proof.fencingToken,
      )
      .run()
    changed(result, 2)
    const accepted = await this.get(attemptId)
    if (accepted === undefined) {
      return resume("The saga attempt was not accepted under the active exact fence.")
    }
    if (
      accepted.protocolVersion !== 2 ||
      accepted.sagaId !== sagaId ||
      accepted.causalAttemptId !== binding.causalAttemptId ||
      accepted.operationId !== binding.operationId ||
      accepted.operationStepId !== binding.operationStepId ||
      accepted.sagaStepId !== sagaStepId ||
      accepted.phase !== input.phase ||
      accepted.purpose !== input.purpose ||
      accepted.actionKey !== binding.actionKey ||
      accepted.idempotencyKey !== binding.idempotencyKey ||
      accepted.inputChecksum !== inputChecksum ||
      accepted.inputJson !== canonicalInput ||
      accepted.acceptanceChecksum !== checksum ||
      accepted.leaseKey !== input.proof.leaseKey ||
      accepted.holderId !== input.proof.holderId ||
      accepted.acquisitionId !== input.proof.acquisitionId ||
      accepted.fencingToken !== input.proof.fencingToken
    ) {
      return intervention("The saga attempt ID is bound to contradictory immutable input.")
    }
    return accepted
  }

  async complete(input: CompleteSagaAttemptInput): Promise<SagaAttemptRecord> {
    const attemptId = boundedSagaReceiptText(
      input.attemptId,
      "Saga attempt ID",
      MAX_SAGA_RECEIPT_IDENTITY_BYTES,
    )
    validProof(input.proof)
    if (
      input.state !== "confirmed" &&
      input.state !== "failed" &&
      input.state !== "indeterminate" &&
      input.state !== "not_applied" &&
      input.state !== "unknown"
    ) {
      return configuration("Saga attempt outcome is invalid.")
    }
    const evidenceJson = canonicalSagaReceiptJson(input.evidenceJson, "Saga action evidence", false)
    const valueJson = canonicalSagaReceiptJson(
      input.state === "confirmed" ? input.outputJson : input.errorJson,
      input.state === "confirmed" ? "Saga action output" : "Saga action error",
      false,
    )
    const existing = await this.get(attemptId)
    if (existing === undefined) return resume("The saga attempt was never durably accepted.")
    if (
      existing.state === "accepted" &&
      existing.protocolVersion === 1 &&
      existing.purpose === "observation"
    ) {
      await this.#validateProjectionBinding(existing, input.proof)
    }
    if (
      (existing.purpose === "effect" && input.state === "indeterminate") ||
      (existing.purpose === "observation" &&
        (input.state === "failed" || input.state === "unknown"))
    ) {
      return configuration("Saga attempt outcome is incompatible with its purpose.")
    }
    const evidenceChecksum = await sagaReceiptPayloadChecksum(
      this.#digest,
      SAGA_ATTEMPT_EVIDENCE_DOMAIN,
      evidenceJson,
    )
    const valueChecksum = await sagaReceiptPayloadChecksum(
      this.#digest,
      input.state === "confirmed" ? SAGA_ATTEMPT_OUTPUT_DOMAIN : SAGA_ATTEMPT_ERROR_DOMAIN,
      valueJson,
    )
    const checksum = await sagaAttemptOutcomeChecksum(
      this.#digest,
      existing.acceptanceChecksum,
      input.state,
      evidenceChecksum,
      evidenceJson,
      valueChecksum,
      valueJson,
    )
    if (existing.state !== "accepted") {
      if (
        existing.state !== input.state ||
        existing.evidenceChecksum !== evidenceChecksum ||
        existing.evidenceJson !== evidenceJson ||
        (existing.state === "confirmed"
          ? existing.outputChecksum !== valueChecksum || existing.outputJson !== valueJson
          : existing.errorChecksum !== valueChecksum || existing.errorJson !== valueJson) ||
        existing.outcomeChecksum !== checksum
      ) {
        return intervention("A duplicate saga outcome contradicts durable evidence.")
      }
      return existing
    }
    if (
      existing.leaseKey !== input.proof.leaseKey ||
      existing.holderId !== input.proof.holderId ||
      existing.acquisitionId !== input.proof.acquisitionId ||
      existing.fencingToken !== input.proof.fencingToken
    ) {
      return resume("The saga attempt outcome was fenced by a different lease owner.")
    }
    const rowSafeOutcomePayloads = await this.#rowSafeOutcomePayloadsActivated()
    const outcomeStatement = this.#database
      .prepare(
        `INSERT INTO "nozzle_saga_action_attempt_outcomes"
         ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
          "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?10 AND "holder_id" = ?11 AND "acquisition_id" = ?12
             AND "fencing_token" = ?13 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )${rowSafeOutcomePayloads ? "" : '\n         ON CONFLICT ("attempt_id") DO NOTHING'}`,
      )
      .bind(
        attemptId,
        input.state,
        evidenceChecksum,
        rowSafeOutcomePayloads ? SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON : evidenceJson,
        input.state === "confirmed" ? valueChecksum : null,
        input.state === "confirmed"
          ? rowSafeOutcomePayloads
            ? SAGA_OUTCOME_OUTPUT_REFERENCE_JSON
            : valueJson
          : null,
        input.state === "confirmed" ? null : valueChecksum,
        input.state === "confirmed"
          ? null
          : rowSafeOutcomePayloads
            ? SAGA_OUTCOME_ERROR_REFERENCE_JSON
            : valueJson,
        checksum,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
        input.proof.fencingToken,
      )
    if (rowSafeOutcomePayloads) {
      const payloadStatement = `INSERT INTO "nozzle_saga_action_attempt_outcome_payloads"
         ("attempt_id", "payload_kind", "payload_checksum", "payload_json")
         VALUES (?1, ?2, ?3, ?4)`
      let results: readonly ControlRunResult[] | undefined
      try {
        results = await this.#database.batch([
          outcomeStatement,
          this.#database
            .prepare(payloadStatement)
            .bind(attemptId, "evidence", evidenceChecksum, evidenceJson),
          this.#database
            .prepare(payloadStatement)
            .bind(
              attemptId,
              input.state === "confirmed" ? "output" : "error",
              valueChecksum,
              valueJson,
            ),
        ])
      } catch {
        // A failed batch rolls back atomically; a lost response may still follow a committed batch.
        // The checksum-verified reload below is the only authority in either case.
      }
      if (
        results !== undefined &&
        (!Array.isArray(results) ||
          results.length !== 3 ||
          results.some((result) => !changed(result)))
      ) {
        return intervention("Control D1 returned malformed saga-receipt batch metadata.")
      }
    } else {
      changed(await outcomeStatement.run())
    }
    const completed = await this.get(attemptId)
    if (completed === undefined || completed.state === "accepted") {
      return resume("The saga attempt outcome was not committed under the active exact fence.")
    }
    if (
      completed.state !== input.state ||
      completed.evidenceChecksum !== evidenceChecksum ||
      completed.evidenceJson !== evidenceJson ||
      (completed.state === "confirmed"
        ? completed.outputChecksum !== valueChecksum || completed.outputJson !== valueJson
        : completed.errorChecksum !== valueChecksum || completed.errorJson !== valueJson) ||
      completed.outcomeChecksum !== checksum
    ) {
      return intervention("The committed saga outcome contradicts the requested evidence.")
    }
    return completed
  }
}
