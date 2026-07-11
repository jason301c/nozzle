import { type DigestFunction, NozzleError, type SagaActionPhase } from "@nozzle/core"

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

export const MAX_SAGA_RECEIPT_IDENTITY_BYTES = 512
const MAX_TEXT_BYTES = 2048
const MAX_JSON_BYTES = 1024 * 1024
const CHECKSUM = /^[0-9a-f]{64}$/u

export const SAGA_ATTEMPT_INPUT_DOMAIN = "nozzle.saga-action-input.v1"
export const SAGA_ATTEMPT_EVIDENCE_DOMAIN = "nozzle.saga-action-evidence.v1"
export const SAGA_ATTEMPT_OUTPUT_DOMAIN = "nozzle.saga-action-output.v1"
export const SAGA_ATTEMPT_ERROR_DOMAIN = "nozzle.saga-action-error.v1"
const ACCEPTANCE_DOMAIN = "nozzle.saga-action-acceptance.v2"
const OUTCOME_DOMAIN = "nozzle.saga-action-outcome.v1"

export interface SagaAttemptRow {
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

const SAGA_ATTEMPT_ROW_KEYS = [
  "acceptance_checksum",
  "accepted_at_ms",
  "acquisition_id",
  "action_key",
  "attempt_id",
  "causal_attempt_id",
  "completed_at_ms",
  "error_checksum",
  "error_json",
  "evidence_checksum",
  "evidence_json",
  "fencing_token",
  "holder_id",
  "idempotency_key",
  "input_checksum",
  "input_json",
  "lease_key",
  "operation_id",
  "operation_step_id",
  "outcome_checksum",
  "output_checksum",
  "output_json",
  "phase",
  "protocol_classified_at_ms",
  "protocol_version",
  "purpose",
  "saga_id",
  "saga_step_id",
  "state",
] as const satisfies readonly (keyof SagaAttemptRow)[]

export const SAGA_ATTEMPT_ROW_SELECT = `
  "attempt"."acceptance_checksum", "attempt"."accepted_at_ms",
  "attempt"."acquisition_id", "attempt"."action_key", "attempt"."attempt_id",
  "attempt"."causal_attempt_id", "attempt"."fencing_token", "attempt"."holder_id",
  "attempt"."idempotency_key", "attempt"."input_checksum", "attempt"."input_json",
  "attempt"."lease_key", "attempt"."operation_id", "attempt"."operation_step_id",
  "attempt"."phase", "attempt"."purpose", "attempt"."saga_id",
  "attempt"."saga_step_id", "protocol"."protocol_version",
  "protocol"."classified_at_ms" AS "protocol_classified_at_ms",
  "outcome"."state", "outcome"."evidence_checksum", "outcome"."evidence_json",
  "outcome"."output_checksum", "outcome"."output_json", "outcome"."error_checksum",
  "outcome"."error_json", "outcome"."outcome_checksum", "outcome"."completed_at_ms"`

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  return Object.getPrototypeOf(value) === Object.prototype
}

function exactRow(value: unknown): value is SagaAttemptRow {
  return (
    plainRecord(value) &&
    Object.keys(value).length === SAGA_ATTEMPT_ROW_KEYS.length &&
    SAGA_ATTEMPT_ROW_KEYS.every((key) => Object.hasOwn(value, key))
  )
}

function captureRow(value: unknown): SagaAttemptRow {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("The persisted saga-attempt row could not be captured safely.")
  }
  if (!exactRow(snapshot)) {
    return intervention("Persisted saga-attempt row fields are malformed.")
  }
  return snapshot
}

function persistedText(value: unknown, maximumBytes = MAX_TEXT_BYTES): value is string {
  return (
    typeof value === "string" &&
    value.trim().length > 0 &&
    new TextEncoder().encode(value).byteLength <= maximumBytes
  )
}

export function boundedSagaReceiptText(
  value: unknown,
  label: string,
  maximumBytes = MAX_TEXT_BYTES,
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > maximumBytes) {
    return configuration(`${label} exceeds the durable saga receipt limit.`)
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

export function canonicalSagaReceiptJson(
  value: unknown,
  label: string,
  persisted: boolean,
): string {
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
  const checksum = await digest(frame(domain, parts))
  if (typeof checksum !== "string" || checksum.trim().length === 0) {
    return configuration("Saga receipt checksum must be non-empty.")
  }
  if (!CHECKSUM.test(checksum)) {
    return configuration("Saga receipt digest must return a lowercase SHA-256 checksum.")
  }
  return checksum
}

export async function sagaReceiptPayloadChecksum(
  digest: DigestFunction,
  domain: string,
  json: string,
): Promise<string> {
  return checkedDigest(digest, domain, [json])
}

function acceptanceParts(
  identity: Omit<SagaAttemptIdentity, "acceptanceChecksum" | "acceptedAtMs" | "protocolVersion">,
): readonly string[] {
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

export async function sagaAttemptAcceptanceChecksum(
  digest: DigestFunction,
  identity: Omit<SagaAttemptIdentity, "acceptanceChecksum" | "acceptedAtMs" | "protocolVersion">,
): Promise<string> {
  return checkedDigest(digest, ACCEPTANCE_DOMAIN, acceptanceParts(identity))
}

export async function sagaAttemptOutcomeChecksum(
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

export async function loadSagaAttemptRecordRow(
  candidate: unknown,
  digest: DigestFunction,
  expectedAttemptId?: string,
): Promise<SagaAttemptRecord> {
  if (typeof digest !== "function") configuration("A saga-attempt digest is required.")
  const row = captureRow(candidate)
  if (
    !persistedText(row.attempt_id) ||
    (expectedAttemptId !== undefined && row.attempt_id !== expectedAttemptId) ||
    (row.causal_attempt_id !== null && !persistedText(row.causal_attempt_id)) ||
    !persistedText(row.saga_id) ||
    !persistedText(row.operation_id) ||
    !persistedText(row.operation_step_id) ||
    !persistedText(row.saga_step_id) ||
    (row.phase !== "forward" && row.phase !== "compensation") ||
    (row.protocol_version !== 1 && row.protocol_version !== 2) ||
    row.protocol_classified_at_ms !== row.accepted_at_ms ||
    (row.purpose !== "effect" && row.purpose !== "observation") ||
    !persistedText(row.action_key) ||
    !persistedText(row.idempotency_key) ||
    !persistedText(row.input_checksum) ||
    !persistedText(row.acceptance_checksum) ||
    !persistedText(row.lease_key) ||
    !persistedText(row.holder_id) ||
    !persistedText(row.acquisition_id) ||
    !Number.isSafeInteger(row.fencing_token) ||
    row.fencing_token < 1 ||
    !Number.isSafeInteger(row.accepted_at_ms) ||
    row.accepted_at_ms < 0
  ) {
    return intervention("Persisted saga-attempt identity is malformed.")
  }
  const purpose = row.purpose as SagaAttemptPurpose
  const phase = row.phase as SagaActionPhase
  const protocolVersion = row.protocol_version as 1 | 2
  const causalRequired = purpose === "observation" || phase === "compensation"
  if (
    causalRequired !== (row.causal_attempt_id !== null) ||
    row.causal_attempt_id === row.attempt_id
  ) {
    return intervention("Persisted saga-attempt causal identity is malformed.")
  }
  const canonicalInput = canonicalSagaReceiptJson(
    row.input_json,
    "Persisted saga action input",
    true,
  )
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
    phase,
    purpose,
    sagaId: row.saga_id,
    sagaStepId: row.saga_step_id,
  })
  const actualInputChecksum = await sagaReceiptPayloadChecksum(
    digest,
    SAGA_ATTEMPT_INPUT_DOMAIN,
    canonicalInput,
  )
  const actualAcceptance = await sagaAttemptAcceptanceChecksum(digest, identityWithoutReceipt)
  if (actualInputChecksum !== row.input_checksum || actualAcceptance !== row.acceptance_checksum) {
    return intervention("Persisted saga-attempt acceptance checksums do not match.")
  }
  const identity: SagaAttemptIdentity = Object.freeze({
    ...identityWithoutReceipt,
    acceptanceChecksum: row.acceptance_checksum,
    acceptedAtMs: row.accepted_at_ms,
    protocolVersion,
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
  const compatibleState =
    purpose === "effect"
      ? row.state !== "indeterminate"
      : row.state !== "failed" && row.state !== "unknown"
  if (!compatibleState) {
    return intervention("Persisted saga-attempt has an incompatible outcome for its purpose.")
  }
  if (
    !Number.isSafeInteger(row.completed_at_ms) ||
    (row.completed_at_ms as number) < identity.acceptedAtMs ||
    !persistedText(row.evidence_checksum) ||
    !persistedText(row.outcome_checksum)
  ) {
    return intervention("Persisted terminal saga attempt is incomplete.")
  }
  const confirmed = row.state === "confirmed"
  if (
    (confirmed && (row.error_checksum !== null || row.error_json !== null)) ||
    (!confirmed && (row.output_checksum !== null || row.output_json !== null))
  ) {
    return intervention("Persisted terminal saga attempt value columns are contradictory.")
  }
  const evidenceJson = canonicalSagaReceiptJson(
    row.evidence_json,
    "Persisted saga action evidence",
    true,
  )
  const actualEvidenceChecksum = await sagaReceiptPayloadChecksum(
    digest,
    SAGA_ATTEMPT_EVIDENCE_DOMAIN,
    evidenceJson,
  )
  const valueJson = canonicalSagaReceiptJson(
    confirmed ? row.output_json : row.error_json,
    confirmed ? "Persisted saga action output" : "Persisted saga action error",
    true,
  )
  const valueChecksum = confirmed ? row.output_checksum : row.error_checksum
  if (!persistedText(valueChecksum)) {
    return intervention("Persisted terminal saga attempt value checksum is missing.")
  }
  const actualValueChecksum = await sagaReceiptPayloadChecksum(
    digest,
    confirmed ? SAGA_ATTEMPT_OUTPUT_DOMAIN : SAGA_ATTEMPT_ERROR_DOMAIN,
    valueJson,
  )
  const actualOutcome = await sagaAttemptOutcomeChecksum(
    digest,
    identity.acceptanceChecksum,
    row.state,
    row.evidence_checksum as string,
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
          evidenceChecksum: row.evidence_checksum as string,
          evidenceJson,
          outcomeChecksum: row.outcome_checksum as string,
          outputChecksum: valueChecksum,
          outputJson: valueJson,
          state: "confirmed",
        }
      : {
          ...identity,
          completedAtMs: row.completed_at_ms as number,
          errorChecksum: valueChecksum,
          errorJson: valueJson,
          evidenceChecksum: row.evidence_checksum as string,
          evidenceJson,
          outcomeChecksum: row.outcome_checksum as string,
          state: row.state,
        },
  )
}
