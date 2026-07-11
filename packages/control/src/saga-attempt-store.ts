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
import { D1SagaStore, sagaActionOperationStepId } from "./saga-store.js"

const MAX_IDENTITY_BYTES = 512
const MAX_TEXT_BYTES = 2048
const MAX_JSON_BYTES = 1024 * 1024
const INPUT_DOMAIN = "nozzle.saga-action-input.v1"
const EVIDENCE_DOMAIN = "nozzle.saga-action-evidence.v1"
const OUTPUT_DOMAIN = "nozzle.saga-action-output.v1"
const ERROR_DOMAIN = "nozzle.saga-action-error.v1"
const ACCEPTANCE_DOMAIN = "nozzle.saga-action-acceptance.v2"
const OUTCOME_DOMAIN = "nozzle.saga-action-outcome.v1"
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

export type SagaAttemptPurpose = "effect" | "observation"
export type SagaAttemptOutcomeState =
  | "confirmed"
  | "failed"
  | "indeterminate"
  | "not_applied"
  | "unknown"

export interface SagaAttemptIdentity {
  readonly acceptanceChecksum: string
  readonly acceptedAtMs: number
  readonly acquisitionId: string
  readonly actionKey: string
  readonly attemptId: string
  readonly causalAttemptId: string | null
  readonly fencingToken: number
  readonly holderId: string
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly inputJson: string
  readonly leaseKey: string
  readonly operationId: string
  readonly operationStepId: string
  readonly phase: SagaActionPhase
  readonly protocolVersion: 1 | 2
  readonly purpose: SagaAttemptPurpose
  readonly sagaId: string
  readonly sagaStepId: string
}

export type SagaAttemptRecord =
  | (SagaAttemptIdentity & { readonly state: "accepted" })
  | (SagaAttemptIdentity & {
      readonly completedAtMs: number
      readonly evidenceChecksum: string
      readonly evidenceJson: string
      readonly outcomeChecksum: string
      readonly outputChecksum: string
      readonly outputJson: string
      readonly state: "confirmed"
    })
  | (SagaAttemptIdentity & {
      readonly completedAtMs: number
      readonly errorChecksum: string
      readonly errorJson: string
      readonly evidenceChecksum: string
      readonly evidenceJson: string
      readonly outcomeChecksum: string
      readonly state: Exclude<SagaAttemptOutcomeState, "confirmed">
    })

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

interface SagaAttemptRow {
  readonly acceptance_checksum: string
  readonly accepted_at_ms: number
  readonly acquisition_id: string
  readonly action_key: string
  readonly attempt_id: string
  readonly causal_attempt_id: string | null
  readonly completed_at_ms: number | null
  readonly error_checksum: string | null
  readonly error_json: string | null
  readonly evidence_checksum: string | null
  readonly evidence_json: string | null
  readonly fencing_token: number
  readonly holder_id: string
  readonly idempotency_key: string
  readonly input_checksum: string
  readonly input_json: string
  readonly lease_key: string
  readonly operation_id: string
  readonly operation_step_id: string
  readonly outcome_checksum: string | null
  readonly output_checksum: string | null
  readonly output_json: string | null
  readonly phase: string
  readonly protocol_classified_at_ms: number | null
  readonly protocol_version: number | null
  readonly purpose: string
  readonly saga_id: string
  readonly saga_step_id: string
  readonly state: string | null
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

function boundedText(value: unknown, label: string, maximumBytes = MAX_TEXT_BYTES): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > maximumBytes) {
    return configuration(`${label} exceeds the durable saga receipt limit.`)
  }
  return value
}

function validProof(proof: LeaseProof): void {
  boundedText(proof.leaseKey, "Lease key", MAX_IDENTITY_BYTES)
  boundedText(proof.holderId, "Lease holder ID", MAX_IDENTITY_BYTES)
  boundedText(proof.acquisitionId, "Lease acquisition ID", MAX_IDENTITY_BYTES)
  if (!Number.isSafeInteger(proof.fencingToken) || proof.fencingToken < 1) {
    configuration("Lease fencing token must be a positive safe integer.")
  }
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

function inputJson(value: unknown, label: string, persisted: boolean): string {
  if (typeof value !== "string" || value.length === 0) {
    return persisted
      ? intervention(`${label} is malformed.`)
      : configuration(`${label} must be JSON text.`)
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return persisted
      ? intervention(`${label} is not valid JSON.`)
      : configuration(`${label} is not valid JSON.`)
  }
  const json = JSON.stringify(canonicalValue(parsed))
  if (new TextEncoder().encode(json).byteLength > MAX_JSON_BYTES) {
    return persisted
      ? intervention(`${label} exceeds the one MiB durable limit.`)
      : configuration(`${label} exceeds the one MiB durable limit.`)
  }
  if (persisted && json !== value) return intervention(`${label} is not canonical.`)
  return json
}

function frame(domain: string, parts: readonly string[]): Uint8Array {
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

async function checkedDigest(
  digest: DigestFunction,
  domain: string,
  parts: readonly string[],
): Promise<string> {
  return boundedText(await digest(frame(domain, parts)), "Saga receipt checksum")
}

async function payloadChecksum(
  digest: DigestFunction,
  domain: string,
  json: string,
): Promise<string> {
  return checkedDigest(digest, domain, [json])
}

export async function sagaActionInputChecksum(
  inputJsonValue: string,
  digest: DigestFunction,
): Promise<string> {
  if (typeof digest !== "function") configuration("A saga action digest is required.")
  const json = inputJson(inputJsonValue, "Saga action input", false)
  return payloadChecksum(digest, INPUT_DOMAIN, json)
}

export function sagaObservationIdempotencyKey(actionIdempotencyKey: string): string {
  return `${boundedText(actionIdempotencyKey, "Saga action idempotency key")}:observation`
}

function acceptanceParts(
  identity: Omit<SagaAttemptIdentity, "acceptanceChecksum" | "acceptedAtMs" | "protocolVersion">,
) {
  return [
    identity.attemptId,
    identity.causalAttemptId === null ? "0" : "1",
    identity.causalAttemptId ?? "",
    identity.sagaId,
    identity.operationId,
    identity.operationStepId,
    identity.sagaStepId,
    identity.phase,
    identity.purpose,
    identity.actionKey,
    identity.idempotencyKey,
    identity.inputChecksum,
    identity.inputJson,
    identity.leaseKey,
    identity.holderId,
    identity.acquisitionId,
    identity.fencingToken.toString(10),
  ]
}

async function acceptanceChecksum(
  digest: DigestFunction,
  identity: Omit<SagaAttemptIdentity, "acceptanceChecksum" | "acceptedAtMs" | "protocolVersion">,
): Promise<string> {
  return checkedDigest(digest, ACCEPTANCE_DOMAIN, acceptanceParts(identity))
}

async function outcomeChecksum(
  digest: DigestFunction,
  acceptance: string,
  state: SagaAttemptOutcomeState,
  evidenceChecksum: string,
  evidenceJson: string,
  valueChecksum: string,
  valueJson: string,
): Promise<string> {
  return checkedDigest(digest, OUTCOME_DOMAIN, [
    acceptance,
    state,
    evidenceChecksum,
    evidenceJson,
    valueChecksum,
    valueJson,
  ])
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
    const attemptId = boundedText(attemptIdInput, "Saga attempt ID", MAX_IDENTITY_BYTES)
    const row = await this.#database
      .prepare(
        `SELECT "attempt".*, "protocol"."protocol_version",
                "protocol"."classified_at_ms" AS "protocol_classified_at_ms",
                "outcome"."state", "outcome"."evidence_checksum",
                "outcome"."evidence_json", "outcome"."output_checksum",
                "outcome"."output_json", "outcome"."error_checksum",
                "outcome"."error_json", "outcome"."outcome_checksum",
                "outcome"."completed_at_ms"
         FROM "nozzle_saga_action_attempts" AS "attempt"
         LEFT JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
         LEFT JOIN "nozzle_saga_action_attempt_outcomes" AS "outcome" USING ("attempt_id")
         WHERE "attempt"."attempt_id" = ?1`,
      )
      .bind(attemptId)
      .first<SagaAttemptRow>()
    if (row === null) return undefined
    if (
      row.attempt_id !== attemptId ||
      (row.causal_attempt_id !== null &&
        (typeof row.causal_attempt_id !== "string" || row.causal_attempt_id.trim() === "")) ||
      typeof row.saga_id !== "string" ||
      typeof row.operation_id !== "string" ||
      typeof row.operation_step_id !== "string" ||
      typeof row.saga_step_id !== "string" ||
      (row.phase !== "forward" && row.phase !== "compensation") ||
      (row.protocol_version !== 1 && row.protocol_version !== 2) ||
      row.protocol_classified_at_ms !== row.accepted_at_ms ||
      (row.purpose !== "effect" && row.purpose !== "observation") ||
      typeof row.action_key !== "string" ||
      typeof row.idempotency_key !== "string" ||
      typeof row.input_checksum !== "string" ||
      typeof row.acceptance_checksum !== "string" ||
      typeof row.lease_key !== "string" ||
      typeof row.holder_id !== "string" ||
      typeof row.acquisition_id !== "string" ||
      !Number.isSafeInteger(row.fencing_token) ||
      row.fencing_token < 1 ||
      !Number.isSafeInteger(row.accepted_at_ms) ||
      row.accepted_at_ms < 0
    ) {
      return intervention("Persisted saga-attempt identity is malformed.")
    }
    const causalRequired = row.purpose === "observation" || row.phase === "compensation"
    if (
      causalRequired !== (row.causal_attempt_id !== null) ||
      row.causal_attempt_id === row.attempt_id
    ) {
      return intervention("Persisted saga-attempt causal identity is malformed.")
    }
    const canonicalInput = inputJson(row.input_json, "Persisted saga action input", true)
    const identityWithoutReceipt = Object.freeze({
      acquisitionId: row.acquisition_id,
      actionKey: row.action_key,
      attemptId: row.attempt_id,
      causalAttemptId: row.causal_attempt_id,
      fencingToken: row.fencing_token,
      holderId: row.holder_id,
      idempotencyKey: row.idempotency_key,
      inputChecksum: row.input_checksum,
      inputJson: canonicalInput,
      leaseKey: row.lease_key,
      operationId: row.operation_id,
      operationStepId: row.operation_step_id,
      phase: row.phase,
      protocolVersion: row.protocol_version,
      purpose: row.purpose,
      sagaId: row.saga_id,
      sagaStepId: row.saga_step_id,
    })
    const actualInputChecksum = await payloadChecksum(this.#digest, INPUT_DOMAIN, canonicalInput)
    const actualAcceptance = await acceptanceChecksum(this.#digest, identityWithoutReceipt)
    if (
      actualInputChecksum !== row.input_checksum ||
      actualAcceptance !== row.acceptance_checksum
    ) {
      return intervention("Persisted saga-attempt acceptance checksums do not match.")
    }
    const identity: SagaAttemptIdentity = Object.freeze({
      ...identityWithoutReceipt,
      acceptanceChecksum: row.acceptance_checksum,
      acceptedAtMs: row.accepted_at_ms,
    })
    if (row.state === null) {
      if (
        row.evidence_checksum !== null ||
        row.evidence_json !== null ||
        row.output_checksum !== null ||
        row.output_json !== null ||
        row.error_checksum !== null ||
        row.error_json !== null ||
        row.outcome_checksum !== null ||
        row.completed_at_ms !== null
      ) {
        return intervention("Persisted accepted saga attempt contains partial outcome data.")
      }
      return Object.freeze({ ...identity, state: "accepted" })
    }
    if (
      row.state !== "confirmed" &&
      row.state !== "failed" &&
      row.state !== "indeterminate" &&
      row.state !== "not_applied" &&
      row.state !== "unknown"
    ) {
      return intervention("Persisted saga-attempt outcome state is unsupported.")
    }
    if (
      !Number.isSafeInteger(row.completed_at_ms) ||
      (row.completed_at_ms as number) < identity.acceptedAtMs ||
      typeof row.evidence_checksum !== "string" ||
      typeof row.outcome_checksum !== "string"
    ) {
      return intervention("Persisted terminal saga attempt is incomplete.")
    }
    const evidenceJson = inputJson(row.evidence_json, "Persisted saga action evidence", true)
    const actualEvidenceChecksum = await payloadChecksum(
      this.#digest,
      EVIDENCE_DOMAIN,
      evidenceJson,
    )
    const confirmed = row.state === "confirmed"
    const valueJson = inputJson(
      confirmed ? row.output_json : row.error_json,
      confirmed ? "Persisted saga action output" : "Persisted saga action error",
      true,
    )
    const valueChecksum = confirmed ? row.output_checksum : row.error_checksum
    if (typeof valueChecksum !== "string") {
      return intervention("Persisted terminal saga attempt value checksum is missing.")
    }
    const actualValueChecksum = await payloadChecksum(
      this.#digest,
      confirmed ? OUTPUT_DOMAIN : ERROR_DOMAIN,
      valueJson,
    )
    const actualOutcome = await outcomeChecksum(
      this.#digest,
      identity.acceptanceChecksum,
      row.state,
      row.evidence_checksum,
      evidenceJson,
      valueChecksum,
      valueJson,
    )
    if (
      actualEvidenceChecksum !== row.evidence_checksum ||
      actualValueChecksum !== valueChecksum ||
      actualOutcome !== row.outcome_checksum
    ) {
      return intervention("Persisted saga-attempt outcome checksums do not match.")
    }
    return Object.freeze(
      confirmed
        ? {
            ...identity,
            completedAtMs: row.completed_at_ms as number,
            evidenceChecksum: row.evidence_checksum,
            evidenceJson,
            outcomeChecksum: row.outcome_checksum,
            outputChecksum: valueChecksum,
            outputJson: valueJson,
            state: "confirmed",
          }
        : {
            ...identity,
            completedAtMs: row.completed_at_ms as number,
            errorChecksum: valueChecksum,
            errorJson: valueJson,
            evidenceChecksum: row.evidence_checksum,
            evidenceJson,
            outcomeChecksum: row.outcome_checksum,
            state: row.state,
          },
    )
  }

  async validateProjectionReceipt(
    input: ValidateSagaProjectionReceiptInput,
  ): Promise<SagaAttemptRecord> {
    const attemptId = boundedText(input.attemptId, "Saga attempt ID", MAX_IDENTITY_BYTES)
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
      const compatibleStates: Readonly<
        Record<SagaAttemptPurpose, readonly SagaAttemptOutcomeState[]>
      > = {
        effect: ["confirmed", "failed", "not_applied", "unknown"],
        observation: ["confirmed", "indeterminate", "not_applied"],
      }
      if (!compatibleStates[receipt.purpose].includes(receipt.state)) {
        return intervention("The persisted saga settlement receipt has an incompatible outcome.")
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
    const attemptId = boundedText(input.attemptId, "Saga attempt ID", MAX_IDENTITY_BYTES)
    const sagaId = boundedText(input.sagaId, "Saga ID", MAX_IDENTITY_BYTES)
    const sagaStepId = boundedText(input.sagaStepId, "Saga step ID", MAX_IDENTITY_BYTES)
    phase(input.phase)
    purpose(input.purpose)
    validProof(input.proof)
    const canonicalInput = inputJson(input.inputJson, "Saga action input", false)
    const inputChecksum = await payloadChecksum(this.#digest, INPUT_DOMAIN, canonicalInput)
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
    const checksum = await acceptanceChecksum(this.#digest, identityWithoutReceipt)
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
    const attemptId = boundedText(input.attemptId, "Saga attempt ID", MAX_IDENTITY_BYTES)
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
    const evidenceJson = inputJson(input.evidenceJson, "Saga action evidence", false)
    const valueJson = inputJson(
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
    const evidenceChecksum = await payloadChecksum(this.#digest, EVIDENCE_DOMAIN, evidenceJson)
    const valueChecksum = await payloadChecksum(
      this.#digest,
      input.state === "confirmed" ? OUTPUT_DOMAIN : ERROR_DOMAIN,
      valueJson,
    )
    const checksum = await outcomeChecksum(
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
    const result = await this.#database
      .prepare(
        `INSERT INTO "nozzle_saga_action_attempt_outcomes"
         ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
          "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?10 AND "holder_id" = ?11 AND "acquisition_id" = ?12
             AND "fencing_token" = ?13 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )
         ON CONFLICT ("attempt_id") DO NOTHING`,
      )
      .bind(
        attemptId,
        input.state,
        evidenceChecksum,
        evidenceJson,
        input.state === "confirmed" ? valueChecksum : null,
        input.state === "confirmed" ? valueJson : null,
        input.state === "confirmed" ? null : valueChecksum,
        input.state === "confirmed" ? null : valueJson,
        checksum,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
        input.proof.fencingToken,
      )
      .run()
    changed(result)
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
