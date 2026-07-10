import { type DigestFunction, type LeaseProof, NozzleError } from "@nozzle/core"
import type { ControlDatabase, ControlRunResult } from "./database.js"

const MAX_IDENTITY_BYTES = 512
const MAX_ENDPOINT_BYTES = 2048
const MAX_JSON_BYTES = 1024 * 1024
const ACCEPTANCE_DOMAIN = "nozzle.provider-attempt-acceptance.v1"
const OUTCOME_DOMAIN = "nozzle.provider-attempt-outcome.v1"
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

export type ProviderAttemptState = "accepted" | "confirmed" | "rejected" | "unknown"

interface ProviderAttemptIdentity {
  readonly acceptanceChecksum: string
  readonly acceptedAtMs: number
  readonly acquisitionId: string
  readonly actorChecksum: string
  readonly attemptId: string
  readonly endpoint: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseKey: string
  readonly mutating: boolean
  readonly operationId: string
  readonly purpose: "effect" | "reconciliation"
  readonly requestChecksum: string
  readonly stepId: string
  readonly targetChecksum: string
}

export type ProviderAttemptRecord =
  | (ProviderAttemptIdentity & { readonly state: "accepted" })
  | (ProviderAttemptIdentity & {
      readonly completedAtMs: number
      readonly evidenceJson: string
      readonly outcomeChecksum: string
      readonly resultJson: string
      readonly state: "confirmed"
    })
  | (ProviderAttemptIdentity & {
      readonly completedAtMs: number
      readonly errorJson: string
      readonly evidenceJson: string
      readonly outcomeChecksum: string
      readonly state: "rejected" | "unknown"
    })

export interface AcceptProviderAttemptInput {
  readonly actorChecksum: string
  readonly attemptId: string
  readonly endpoint: string
  readonly mutating: boolean
  readonly operationId: string
  readonly purpose: "effect" | "reconciliation"
  readonly proof: LeaseProof
  readonly requestChecksum: string
  readonly stepId: string
  readonly targetChecksum: string
}

interface CompleteProviderAttemptBase {
  readonly attemptId: string
  readonly evidenceJson: string
  readonly proof: LeaseProof
}

export type CompleteProviderAttemptInput =
  | (CompleteProviderAttemptBase & {
      readonly resultJson: string
      readonly state: "confirmed"
    })
  | (CompleteProviderAttemptBase & {
      readonly errorJson: string
      readonly state: "rejected" | "unknown"
    })

interface ProviderAttemptRow {
  readonly acceptance_checksum: string
  readonly accepted_at_ms: number
  readonly acquisition_id: string
  readonly actor_checksum: string
  readonly attempt_id: string
  readonly completed_at_ms: number | null
  readonly endpoint: string
  readonly error_json: string | null
  readonly evidence_json: string | null
  readonly fencing_token: number
  readonly holder_id: string
  readonly lease_key: string
  readonly mutating: number
  readonly operation_id: string
  readonly outcome_checksum: string | null
  readonly purpose: string
  readonly request_checksum: string
  readonly result_json: string | null
  readonly state: string | null
  readonly step_id: string
  readonly target_checksum: string
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

function boundedText(value: unknown, label: string, maxBytes = MAX_IDENTITY_BYTES): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > maxBytes) {
    return configuration(`${label} exceeds the durable receipt limit.`)
  }
  return value
}

function validProof(proof: LeaseProof): void {
  boundedText(proof.leaseKey, "Lease key")
  boundedText(proof.holderId, "Lease holder ID")
  boundedText(proof.acquisitionId, "Lease acquisition ID")
  if (!Number.isSafeInteger(proof.fencingToken) || proof.fencingToken < 1) {
    configuration("Lease fencing token must be a positive safe integer.")
  }
}

function jsonText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) configuration(`${label} must be JSON text.`)
  if (new TextEncoder().encode(value).byteLength > MAX_JSON_BYTES) {
    configuration(`${label} exceeds the one MiB durable evidence limit.`)
  }
  try {
    JSON.parse(value)
  } catch {
    return configuration(`${label} is not valid JSON.`)
  }
  return value
}

function persistedJson(value: unknown, label: string): string {
  if (typeof value !== "string") return intervention(`${label} is malformed.`)
  try {
    JSON.parse(value)
  } catch {
    return intervention(`${label} is not valid JSON.`)
  }
  return value
}

function changed(result: ControlRunResult): boolean {
  const changes = result.meta.changes
  if (!Number.isSafeInteger(changes) || (changes as number) < 0 || (changes as number) > 1) {
    return intervention("Control D1 returned malformed provider-receipt mutation metadata.")
  }
  return changes === 1
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
  return boundedText(await digest(frame(domain, parts)), "Provider receipt checksum")
}

function acceptanceParts(input: {
  readonly acquisitionId: string
  readonly actorChecksum: string
  readonly attemptId: string
  readonly endpoint: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseKey: string
  readonly mutating: boolean
  readonly operationId: string
  readonly purpose: "effect" | "reconciliation"
  readonly requestChecksum: string
  readonly stepId: string
  readonly targetChecksum: string
}): readonly string[] {
  return [
    input.targetChecksum,
    input.actorChecksum,
    input.operationId,
    input.purpose,
    input.stepId,
    input.attemptId,
    input.endpoint,
    input.mutating ? "1" : "0",
    input.requestChecksum,
    input.leaseKey,
    input.holderId,
    input.acquisitionId,
    input.fencingToken.toString(10),
  ]
}

async function acceptanceChecksum(
  digest: DigestFunction,
  input: Parameters<typeof acceptanceParts>[0],
): Promise<string> {
  return checkedDigest(digest, ACCEPTANCE_DOMAIN, acceptanceParts(input))
}

async function outcomeChecksum(
  digest: DigestFunction,
  acceptance: string,
  state: Exclude<ProviderAttemptState, "accepted">,
  evidenceJson: string,
  valueJson: string,
): Promise<string> {
  return checkedDigest(digest, OUTCOME_DOMAIN, [acceptance, state, evidenceJson, valueJson])
}

function identityFromRow(row: ProviderAttemptRow): ProviderAttemptIdentity {
  if (
    typeof row.attempt_id !== "string" ||
    row.attempt_id.trim() === "" ||
    typeof row.operation_id !== "string" ||
    row.operation_id.trim() === "" ||
    typeof row.step_id !== "string" ||
    row.step_id.trim() === "" ||
    typeof row.target_checksum !== "string" ||
    row.target_checksum.trim() === "" ||
    typeof row.actor_checksum !== "string" ||
    row.actor_checksum.trim() === "" ||
    typeof row.endpoint !== "string" ||
    row.endpoint.trim() === "" ||
    (row.purpose !== "effect" && row.purpose !== "reconciliation") ||
    (row.mutating !== 0 && row.mutating !== 1) ||
    typeof row.request_checksum !== "string" ||
    row.request_checksum.trim() === "" ||
    typeof row.acceptance_checksum !== "string" ||
    row.acceptance_checksum.trim() === "" ||
    typeof row.lease_key !== "string" ||
    row.lease_key.trim() === "" ||
    typeof row.holder_id !== "string" ||
    row.holder_id.trim() === "" ||
    typeof row.acquisition_id !== "string" ||
    row.acquisition_id.trim() === "" ||
    !Number.isSafeInteger(row.fencing_token) ||
    row.fencing_token < 1 ||
    !Number.isSafeInteger(row.accepted_at_ms) ||
    row.accepted_at_ms < 0
  ) {
    return intervention("Persisted provider-attempt identity is malformed.")
  }
  return Object.freeze({
    acceptanceChecksum: row.acceptance_checksum,
    acceptedAtMs: row.accepted_at_ms,
    acquisitionId: row.acquisition_id,
    actorChecksum: row.actor_checksum,
    attemptId: row.attempt_id,
    endpoint: row.endpoint,
    fencingToken: row.fencing_token,
    holderId: row.holder_id,
    leaseKey: row.lease_key,
    mutating: row.mutating === 1,
    operationId: row.operation_id,
    purpose: row.purpose,
    requestChecksum: row.request_checksum,
    stepId: row.step_id,
    targetChecksum: row.target_checksum,
  })
}

export class D1ProviderAttemptStore {
  readonly #database: ControlDatabase
  readonly #digest: DigestFunction

  constructor(database: ControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function"
    ) {
      configuration("A control D1 database binding is required.")
    }
    if (typeof digest !== "function") configuration("A provider-attempt digest is required.")
    this.#database = database
    this.#digest = digest
  }

  async get(attemptId: string): Promise<ProviderAttemptRecord | undefined> {
    boundedText(attemptId, "Provider attempt ID")
    const row = await this.#database
      .prepare(
        `SELECT "attempt"."attempt_id", "attempt"."operation_id", "attempt"."step_id",
                "attempt"."target_checksum", "attempt"."actor_checksum", "attempt"."purpose",
                "attempt"."endpoint",
                "attempt"."mutating", "attempt"."request_checksum",
                "attempt"."acceptance_checksum", "attempt"."lease_key",
                "attempt"."holder_id", "attempt"."acquisition_id",
                "attempt"."fencing_token", "attempt"."accepted_at_ms",
                "outcome"."state", "outcome"."evidence_json", "outcome"."result_json",
                "outcome"."error_json", "outcome"."outcome_checksum",
                "outcome"."completed_at_ms"
         FROM "nozzle_provider_attempts" AS "attempt"
         LEFT JOIN "nozzle_provider_attempt_outcomes" AS "outcome" USING ("attempt_id")
         WHERE "attempt"."attempt_id" = ?1`,
      )
      .bind(attemptId)
      .first<ProviderAttemptRow>()
    if (row === null) return undefined
    const identity = identityFromRow(row)
    const actualAcceptance = await acceptanceChecksum(this.#digest, identity)
    if (actualAcceptance !== identity.acceptanceChecksum) {
      return intervention("Persisted provider-attempt acceptance checksum does not match.")
    }
    if (row.state === null) {
      if (
        row.evidence_json !== null ||
        row.result_json !== null ||
        row.error_json !== null ||
        row.outcome_checksum !== null ||
        row.completed_at_ms !== null
      ) {
        return intervention("Persisted accepted provider attempt contains partial outcome data.")
      }
      return Object.freeze({ ...identity, state: "accepted" })
    }
    if (row.state !== "confirmed" && row.state !== "rejected" && row.state !== "unknown") {
      return intervention("Persisted provider-attempt state is unsupported.")
    }
    if (
      !Number.isSafeInteger(row.completed_at_ms) ||
      (row.completed_at_ms as number) < identity.acceptedAtMs ||
      typeof row.outcome_checksum !== "string" ||
      row.outcome_checksum.trim() === "" ||
      row.evidence_json === null
    ) {
      return intervention("Persisted terminal provider attempt is incomplete.")
    }
    const evidenceJson = persistedJson(row.evidence_json, "Persisted provider evidence")
    const valueJson =
      row.state === "confirmed"
        ? persistedJson(row.result_json, "Persisted provider result")
        : persistedJson(row.error_json, "Persisted provider error")
    const actualOutcome = await outcomeChecksum(
      this.#digest,
      identity.acceptanceChecksum,
      row.state,
      evidenceJson,
      valueJson,
    )
    if (actualOutcome !== row.outcome_checksum) {
      return intervention("Persisted provider-attempt outcome checksum does not match.")
    }
    return Object.freeze(
      row.state === "confirmed"
        ? {
            ...identity,
            completedAtMs: row.completed_at_ms as number,
            evidenceJson,
            outcomeChecksum: row.outcome_checksum,
            resultJson: valueJson,
            state: row.state,
          }
        : {
            ...identity,
            completedAtMs: row.completed_at_ms as number,
            errorJson: valueJson,
            evidenceJson,
            outcomeChecksum: row.outcome_checksum,
            state: row.state,
          },
    )
  }

  async accept(input: AcceptProviderAttemptInput): Promise<ProviderAttemptRecord> {
    const actorChecksum = boundedText(input.actorChecksum, "Provider actor checksum")
    const attemptId = boundedText(input.attemptId, "Provider attempt ID")
    const endpoint = boundedText(input.endpoint, "Provider endpoint", MAX_ENDPOINT_BYTES)
    const operationId = boundedText(input.operationId, "Operation ID")
    if (input.purpose !== "effect" && input.purpose !== "reconciliation") {
      configuration("Provider attempt purpose is invalid.")
    }
    if (input.purpose === "reconciliation" && input.mutating) {
      configuration("Provider reconciliation attempts must be non-mutating.")
    }
    const requestChecksum = boundedText(input.requestChecksum, "Provider request checksum")
    const stepId = boundedText(input.stepId, "Operation step ID")
    const targetChecksum = boundedText(input.targetChecksum, "Provider target checksum")
    if (typeof input.mutating !== "boolean") configuration("Provider mutating flag is invalid.")
    validProof(input.proof)
    const checksum = await acceptanceChecksum(this.#digest, {
      acquisitionId: input.proof.acquisitionId,
      actorChecksum,
      attemptId,
      endpoint,
      fencingToken: input.proof.fencingToken,
      holderId: input.proof.holderId,
      leaseKey: input.proof.leaseKey,
      mutating: input.mutating,
      operationId,
      purpose: input.purpose,
      requestChecksum,
      stepId,
      targetChecksum,
    })
    const result = await this.#database
      .prepare(
        `INSERT INTO "nozzle_provider_attempts"
         ("attempt_id", "operation_id", "step_id", "target_checksum", "actor_checksum", "purpose",
          "endpoint", "mutating", "request_checksum", "acceptance_checksum", "lease_key",
          "holder_id", "acquisition_id", "fencing_token", "accepted_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_operation_steps"
           WHERE "operation_id" = ?2 AND "step_id" = ?3
             AND ((
               ?6 = 'effect' AND "state" = 'running'
               AND json_extract("record_json", '$.activeAttemptId') = ?1
               AND "fencing_token" = ?14
             ) OR (
               ?6 = 'reconciliation' AND "state" = 'unknown'
               AND "fencing_token" < ?14
             ))
         ) AND EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?11 AND "holder_id" = ?12 AND "acquisition_id" = ?13
             AND "fencing_token" = ?14 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )
         ON CONFLICT ("attempt_id") DO NOTHING`,
      )
      .bind(
        attemptId,
        operationId,
        stepId,
        targetChecksum,
        actorChecksum,
        input.purpose,
        endpoint,
        input.mutating,
        requestChecksum,
        checksum,
        input.proof.leaseKey,
        input.proof.holderId,
        input.proof.acquisitionId,
        input.proof.fencingToken,
      )
      .run()
    changed(result)
    const record = await this.get(attemptId)
    if (!record) return resume("The provider attempt was not accepted under the active lease.")
    if (
      record.operationId !== operationId ||
      record.stepId !== stepId ||
      record.targetChecksum !== targetChecksum ||
      record.actorChecksum !== actorChecksum ||
      record.purpose !== input.purpose ||
      record.endpoint !== endpoint ||
      record.mutating !== input.mutating ||
      record.requestChecksum !== requestChecksum ||
      record.acceptanceChecksum !== checksum ||
      record.leaseKey !== input.proof.leaseKey ||
      record.holderId !== input.proof.holderId ||
      record.acquisitionId !== input.proof.acquisitionId ||
      record.fencingToken !== input.proof.fencingToken
    ) {
      return intervention("The provider attempt ID is bound to contradictory immutable input.")
    }
    return record
  }

  async complete(input: CompleteProviderAttemptInput): Promise<ProviderAttemptRecord> {
    const attemptId = boundedText(input.attemptId, "Provider attempt ID")
    validProof(input.proof)
    if (input.state !== "confirmed" && input.state !== "rejected" && input.state !== "unknown") {
      return configuration("Provider attempt outcome is invalid.")
    }
    const evidenceJson = jsonText(input.evidenceJson, "Provider evidence")
    const valueJson =
      input.state === "confirmed"
        ? jsonText(input.resultJson, "Provider result")
        : jsonText(input.errorJson, "Provider error")
    const existing = await this.get(attemptId)
    if (!existing) return resume("The provider attempt was never durably accepted.")
    const checksum = await outcomeChecksum(
      this.#digest,
      existing.acceptanceChecksum,
      input.state,
      evidenceJson,
      valueJson,
    )
    if (existing.state !== "accepted") {
      if (
        existing.state !== input.state ||
        existing.evidenceJson !== evidenceJson ||
        (existing.state === "confirmed"
          ? existing.resultJson !== valueJson
          : existing.errorJson !== valueJson) ||
        existing.outcomeChecksum !== checksum
      ) {
        return intervention("A duplicate provider outcome contradicts durable evidence.")
      }
      return existing
    }
    if (
      existing.leaseKey !== input.proof.leaseKey ||
      existing.holderId !== input.proof.holderId ||
      existing.acquisitionId !== input.proof.acquisitionId ||
      existing.fencingToken !== input.proof.fencingToken
    ) {
      return resume("The provider attempt outcome was fenced by a different lease owner.")
    }
    const result = await this.#database
      .prepare(
        `INSERT INTO "nozzle_provider_attempt_outcomes"
         ("attempt_id", "state", "evidence_json", "result_json", "error_json",
          "outcome_checksum", "completed_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ${SERVER_TIME_SQL}
         WHERE EXISTS (
           SELECT 1 FROM "nozzle_leases"
           WHERE "lease_key" = ?7 AND "holder_id" = ?8 AND "acquisition_id" = ?9
             AND "fencing_token" = ?10 AND "expires_at_ms" > ${SERVER_TIME_SQL}
         )
         ON CONFLICT ("attempt_id") DO NOTHING`,
      )
      .bind(
        attemptId,
        input.state,
        evidenceJson,
        input.state === "confirmed" ? valueJson : null,
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
    if (!completed || completed.state === "accepted") {
      return resume("The provider attempt outcome was not committed under the active lease.")
    }
    if (
      completed.state !== input.state ||
      completed.evidenceJson !== evidenceJson ||
      (completed.state === "confirmed"
        ? completed.resultJson !== valueJson
        : completed.errorJson !== valueJson) ||
      completed.outcomeChecksum !== checksum
    ) {
      return intervention("The committed provider outcome contradicts the requested evidence.")
    }
    return completed
  }
}
