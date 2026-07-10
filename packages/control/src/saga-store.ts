import {
  beginSagaAction,
  createSagaRecord,
  type DigestFunction,
  type LeaseProof,
  loadSagaRecord,
  markRunningSagaActionUnknown,
  markSagaActionNotDispatched,
  NozzleError,
  type OperationStepState,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordSagaObservation,
  requestSagaTermination,
  type SagaActionFailureOutcome,
  type SagaActionPhase,
  type SagaBeginDecision,
  type SagaDescriptor,
  type SagaObservationOutcome,
  type SagaRecord,
  sagaCommitment,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const RECORD_DOMAIN = "nozzle.saga-record.v1"
const MAX_IDENTITY_BYTES = 512

export const SAGA_INIT_OPERATION_STEP_ID = "saga:init"
export const SAGA_SETTLE_OPERATION_STEP_ID = "saga:settle"
export const SAGA_TERMINATION_OPERATION_STEP_ID = "saga:termination"

export function sagaActionOperationStepId(stepId: string, phase: SagaActionPhase): string {
  boundedText(stepId, "Saga step ID")
  if (phase !== "forward" && phase !== "compensation") {
    return configuration("Saga action phase is unsupported.")
  }
  return `saga:${phase}:${stepId}`
}

export interface SagaEffectContext {
  readonly effectId: string
  readonly operationId: string
  readonly proof: LeaseProof
  readonly stepId: string
  readonly transitionId: string
}

interface SagaRow {
  readonly commitment: string
  readonly created_at_ms: number
  readonly deadline_at_ms: number
  readonly descriptor_checksum: string
  readonly descriptor_id: string
  readonly descriptor_json: string
  readonly descriptor_version: number
  readonly effect_evidence_checksum: string | null
  readonly effect_id: string | null
  readonly effect_operation_id: string | null
  readonly effect_record_checksum: string | null
  readonly effect_record_json: string | null
  readonly effect_resource_id: string | null
  readonly effect_resource_kind: string | null
  readonly effect_to_state_version: number | null
  readonly idempotency_key: string
  readonly input_checksum: string
  readonly last_effect_id: string
  readonly last_evidence_checksum: string
  readonly operation_id: string
  readonly record_checksum: string
  readonly record_json: string
  readonly saga_id: string
  readonly state_version: number
  readonly status: string
  readonly termination_cause: string | null
  readonly termination_requested_at_ms: number | null
  readonly updated_at_ms: number
}

interface OperationEffectRow {
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

interface LoadedSaga {
  readonly lastEvidenceChecksum: string
  readonly operationId: string
  readonly record: SagaRecord
}

interface PersistedEffect {
  readonly context: SagaEffectContext
  readonly effectKind: string
  readonly evidenceChecksum: string
  readonly fromStateVersion: number | null
  readonly record: SagaRecord
  readonly recordChecksum: string
  readonly recordJson: string
  readonly toStateVersion: number
}

interface TransitionExpectation {
  readonly attemptId?: string
  readonly states: readonly OperationStepState[]
  readonly stepId: string
}

interface TransitionEvidenceRow {
  readonly to_record_json: string
  readonly transition_id: string
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

function boundedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    return configuration(`${label} exceeds the durable saga identity limit.`)
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

function validateContext(context: SagaEffectContext): void {
  boundedText(context.effectId, "Saga operation effect ID")
  boundedText(context.operationId, "Operation ID")
  boundedText(context.stepId, "Operation step ID")
  boundedText(context.transitionId, "Operation transition ID")
  validProof(context.proof)
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

function canonicalPersistedJson(
  value: unknown,
  label: string,
): {
  readonly json: string
  readonly value: unknown
} {
  if (typeof value !== "string") return intervention(`${label} is malformed.`)
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return intervention(`${label} is invalid.`)
  }
  const json = encode(parsed)
  if (json !== value) return intervention(`${label} is not canonical.`)
  return { json, value: parsed }
}

function frame(domain: string, value: string): Uint8Array {
  const domainBytes = new TextEncoder().encode(domain)
  const valueBytes = new TextEncoder().encode(value)
  const output = new Uint8Array(8 + domainBytes.byteLength + valueBytes.byteLength)
  const view = new DataView(output.buffer)
  view.setUint32(0, domainBytes.byteLength, false)
  output.set(domainBytes, 4)
  view.setUint32(4 + domainBytes.byteLength, valueBytes.byteLength, false)
  output.set(valueBytes, 8 + domainBytes.byteLength)
  return output
}

async function checkedChecksum(digest: DigestFunction, recordJson: string): Promise<string> {
  return boundedText(await digest(frame(RECORD_DOMAIN, recordJson)), "Saga record checksum")
}

function validInteger(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum
}

function mutationChanges(result: ControlRunResult): number {
  const changes = result.meta.changes
  if (
    result.success !== true ||
    !Number.isSafeInteger(changes) ||
    (changes as number) < 0 ||
    (changes as number) > 1
  ) {
    return intervention("Control D1 returned malformed saga mutation metadata.")
  }
  return changes as number
}

function sameRecord(left: SagaRecord, right: SagaRecord): boolean {
  return encode(left) === encode(right)
}

function sameIdentity(left: SagaRecord, right: SagaRecord): boolean {
  if (
    left.sagaId !== right.sagaId ||
    left.descriptor.descriptorChecksum !== right.descriptor.descriptorChecksum ||
    left.idempotencyKey !== right.idempotencyKey ||
    left.inputChecksum !== right.inputChecksum ||
    left.deadlineAtMs !== right.deadlineAtMs
  ) {
    return false
  }
  return left.descriptor.steps.every(
    (step) => left.steps[step.stepId]?.inputChecksum === right.steps[step.stepId]?.inputChecksum,
  )
}

export class D1SagaStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional control D1 database binding is required.")
    }
    if (typeof digest !== "function") configuration("A saga digest implementation is required.")
    this.#database = database
    this.#digest = digest
  }

  async #decode(row: SagaRow): Promise<LoadedSaga> {
    if (
      typeof row.saga_id !== "string" ||
      typeof row.operation_id !== "string" ||
      typeof row.descriptor_id !== "string" ||
      !validInteger(row.descriptor_version, 1) ||
      typeof row.descriptor_checksum !== "string" ||
      typeof row.idempotency_key !== "string" ||
      typeof row.input_checksum !== "string" ||
      !validInteger(row.deadline_at_ms, 0) ||
      typeof row.status !== "string" ||
      typeof row.commitment !== "string" ||
      (row.termination_cause !== null && typeof row.termination_cause !== "string") ||
      (row.termination_requested_at_ms !== null &&
        !validInteger(row.termination_requested_at_ms, 0)) ||
      !validInteger(row.state_version, 0) ||
      typeof row.last_evidence_checksum !== "string" ||
      typeof row.last_effect_id !== "string" ||
      typeof row.record_checksum !== "string" ||
      !validInteger(row.created_at_ms, 0) ||
      !validInteger(row.updated_at_ms, row.created_at_ms)
    ) {
      return intervention("Persisted saga columns are malformed.")
    }
    const persisted = canonicalPersistedJson(row.record_json, "Persisted saga record JSON")
    const descriptor = canonicalPersistedJson(row.descriptor_json, "Persisted saga descriptor JSON")
    const record = await loadSagaRecord(persisted.value, this.#digest)
    const checksum = await checkedChecksum(this.#digest, persisted.json)
    if (
      record.sagaId !== row.saga_id ||
      record.descriptor.descriptorId !== row.descriptor_id ||
      record.descriptor.version !== row.descriptor_version ||
      record.descriptor.descriptorChecksum !== row.descriptor_checksum ||
      encode(record.descriptor) !== descriptor.json ||
      record.idempotencyKey !== row.idempotency_key ||
      record.inputChecksum !== row.input_checksum ||
      record.deadlineAtMs !== row.deadline_at_ms ||
      record.status !== row.status ||
      sagaCommitment(record) !== row.commitment ||
      record.terminationCause !== row.termination_cause ||
      record.terminationRequestedAtMs !== row.termination_requested_at_ms ||
      record.stateVersion !== row.state_version ||
      checksum !== row.record_checksum
    ) {
      return intervention("Persisted saga columns contradict the canonical record.")
    }
    if (
      row.effect_id !== row.last_effect_id ||
      row.effect_resource_kind !== "saga" ||
      row.effect_resource_id !== row.saga_id ||
      row.effect_operation_id !== row.operation_id ||
      row.effect_to_state_version !== row.state_version ||
      row.effect_evidence_checksum !== row.last_evidence_checksum ||
      row.effect_record_checksum !== row.record_checksum ||
      row.effect_record_json !== row.record_json
    ) {
      return intervention("Persisted saga lacks its exact operation-effect receipt.")
    }
    return Object.freeze({
      lastEvidenceChecksum: row.last_evidence_checksum,
      operationId: row.operation_id,
      record,
    })
  }

  async #row(sagaId: string): Promise<SagaRow | undefined> {
    const row = await this.#database
      .prepare(
        `SELECT "saga".*,
                "effect"."effect_id" AS "effect_id",
                "effect"."resource_kind" AS "effect_resource_kind",
                "effect"."resource_id" AS "effect_resource_id",
                "effect"."operation_id" AS "effect_operation_id",
                "effect"."to_state_version" AS "effect_to_state_version",
                "effect"."evidence_checksum" AS "effect_evidence_checksum",
                "effect"."record_checksum" AS "effect_record_checksum",
                "effect"."record_json" AS "effect_record_json"
         FROM "nozzle_sagas" AS "saga"
         LEFT JOIN "nozzle_operation_effects" AS "effect"
           ON "effect"."effect_id" = "saga"."last_effect_id"
         WHERE "saga"."saga_id" = ?1`,
      )
      .bind(sagaId)
      .first<SagaRow>()
    return row ?? undefined
  }

  async #loaded(sagaId: string): Promise<LoadedSaga | undefined> {
    const row = await this.#row(sagaId)
    return row === undefined ? undefined : this.#decode(row)
  }

  async get(sagaIdInput: string): Promise<SagaRecord | undefined> {
    const sagaId = boundedText(sagaIdInput, "Saga ID")
    return (await this.#loaded(sagaId))?.record
  }

  async #effect(effectId: string): Promise<OperationEffectRow | undefined> {
    const row = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?1`)
      .bind(effectId)
      .first<OperationEffectRow>()
    return row ?? undefined
  }

  async #transition(context: SagaEffectContext, expectation: TransitionExpectation): Promise<void> {
    if (context.stepId !== expectation.stepId) {
      return configuration("The saga effect references the wrong canonical operation step.")
    }
    const row = await this.#database
      .prepare(
        `SELECT "transition"."transition_id", "transition"."to_record_json"
         FROM "nozzle_operation_transitions" AS "transition"
         JOIN "nozzle_leases" AS "lease" ON "lease"."lease_key" = "transition"."lease_key"
         WHERE "transition"."transition_id" = ?1
           AND "transition"."operation_id" = ?2 AND "transition"."step_id" = ?3
           AND "transition"."lease_key" = ?4 AND "transition"."holder_id" = ?5
           AND "transition"."acquisition_id" = ?6 AND "transition"."fencing_token" = ?7
           AND "lease"."holder_id" = ?5 AND "lease"."acquisition_id" = ?6
           AND "lease"."fencing_token" = ?7 AND "lease"."expires_at_ms" > ${SERVER_TIME_SQL}`,
      )
      .bind(
        context.transitionId,
        context.operationId,
        context.stepId,
        context.proof.leaseKey,
        context.proof.holderId,
        context.proof.acquisitionId,
        context.proof.fencingToken,
      )
      .first<TransitionEvidenceRow>()
    if (row === null)
      return resume("The saga effect lacks its exact transition under the active lease.")
    if (row.transition_id !== context.transitionId) {
      return intervention("Control D1 returned contradictory saga transition evidence.")
    }
    let record: unknown
    try {
      record = JSON.parse(row.to_record_json)
    } catch {
      return intervention("The saga operation transition record is invalid JSON.")
    }
    if (typeof record !== "object" || record === null || Array.isArray(record)) {
      return intervention("The saga operation transition record is malformed.")
    }
    const candidate = record as Record<string, unknown>
    if (
      typeof candidate.state !== "string" ||
      !(expectation.states as readonly string[]).includes(candidate.state) ||
      (expectation.attemptId !== undefined &&
        (candidate.lastAttemptId !== expectation.attemptId ||
          (candidate.state === "running" && candidate.activeAttemptId !== expectation.attemptId)))
    ) {
      return intervention("The saga effect contradicts its canonical operation-step transition.")
    }
  }

  async #loadEffect(effectId: string): Promise<PersistedEffect | undefined> {
    const row = await this.#effect(effectId)
    if (row === undefined) return undefined
    if (
      typeof row.effect_id !== "string" ||
      typeof row.transition_id !== "string" ||
      typeof row.operation_id !== "string" ||
      typeof row.step_id !== "string" ||
      row.resource_kind !== "saga" ||
      typeof row.resource_id !== "string" ||
      typeof row.effect_kind !== "string" ||
      (row.from_state_version !== null && !validInteger(row.from_state_version, 0)) ||
      !validInteger(row.to_state_version, 0) ||
      typeof row.evidence_checksum !== "string" ||
      typeof row.record_checksum !== "string" ||
      typeof row.lease_key !== "string" ||
      typeof row.holder_id !== "string" ||
      typeof row.acquisition_id !== "string" ||
      !validInteger(row.fencing_token, 1) ||
      !validInteger(row.created_at_ms, 0)
    ) {
      return intervention("Persisted saga operation-effect receipt is malformed.")
    }
    const persisted = canonicalPersistedJson(row.record_json, "Persisted saga effect record JSON")
    const record = await loadSagaRecord(persisted.value, this.#digest)
    const checksum = await checkedChecksum(this.#digest, persisted.json)
    if (
      record.sagaId !== row.resource_id ||
      record.stateVersion !== row.to_state_version ||
      row.record_checksum !== checksum
    ) {
      return intervention("Persisted saga operation-effect contradicts its canonical record.")
    }
    return Object.freeze({
      context: {
        effectId: row.effect_id,
        operationId: row.operation_id,
        proof: {
          acquisitionId: row.acquisition_id,
          fencingToken: row.fencing_token,
          holderId: row.holder_id,
          leaseKey: row.lease_key,
        },
        stepId: row.step_id,
        transitionId: row.transition_id,
      },
      effectKind: row.effect_kind,
      evidenceChecksum: row.evidence_checksum,
      fromStateVersion: row.from_state_version,
      record,
      recordChecksum: row.record_checksum,
      recordJson: persisted.json,
      toStateVersion: row.to_state_version,
    })
  }

  async #replay(
    saga: LoadedSaga | undefined,
    context: SagaEffectContext,
    effectKind: string,
    evidenceChecksum: string,
    sagaId: string,
  ): Promise<SagaRecord | undefined> {
    const receipt = await this.#loadEffect(context.effectId)
    if (receipt === undefined) return undefined
    if (
      receipt.context.transitionId !== context.transitionId ||
      receipt.context.operationId !== context.operationId ||
      receipt.context.stepId !== context.stepId ||
      receipt.context.proof.leaseKey !== context.proof.leaseKey ||
      receipt.context.proof.holderId !== context.proof.holderId ||
      receipt.context.proof.acquisitionId !== context.proof.acquisitionId ||
      receipt.context.proof.fencingToken !== context.proof.fencingToken ||
      receipt.effectKind !== effectKind ||
      receipt.evidenceChecksum !== evidenceChecksum ||
      receipt.record.sagaId !== sagaId
    ) {
      return intervention("Saga operation effect replay contradicts its immutable receipt.")
    }
    if (saga === undefined || saga.record.stateVersion < receipt.toStateVersion) {
      return intervention("Saga operation effect is not reflected in its projection.")
    }
    if (
      saga.record.stateVersion === receipt.toStateVersion &&
      !sameRecord(saga.record, receipt.record)
    ) {
      return intervention("Saga operation effect contradicts its current projection.")
    }
    return saga.record
  }

  async #persist(
    before: LoadedSaga | undefined,
    afterInput: SagaRecord,
    context: SagaEffectContext,
    effectKind: string,
    evidenceChecksum: string,
    transition: TransitionExpectation,
  ): Promise<SagaRecord> {
    const after = await loadSagaRecord(JSON.parse(encode(afterInput)), this.#digest)
    if (
      before !== undefined &&
      (after.stateVersion !== before.record.stateVersion + 1 ||
        !sameIdentity(before.record, after) ||
        before.operationId !== context.operationId)
    ) {
      return intervention("The proposed saga projection contradicts its durable predecessor.")
    }
    await this.#transition(context, transition)
    const recordJson = encode(after)
    const descriptorJson = encode(after.descriptor)
    const recordChecksum = await checkedChecksum(this.#digest, recordJson)
    const beforeJson = before === undefined ? undefined : encode(before.record)
    const beforeChecksum =
      beforeJson === undefined ? undefined : await checkedChecksum(this.#digest, beforeJson)
    const effect = this.#database
      .prepare(
        `INSERT INTO "nozzle_operation_effects"
         ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
          "resource_id", "effect_kind", "from_state_version", "to_state_version",
          "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
          "acquisition_id", "fencing_token", "created_at_ms")
         VALUES (?1, ?2, ?3, ?4, 'saga', ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                 ?12, ?13, ?14, ?15, ${SERVER_TIME_SQL})
         ON CONFLICT ("effect_id") DO NOTHING`,
      )
      .bind(
        context.effectId,
        context.transitionId,
        context.operationId,
        context.stepId,
        after.sagaId,
        effectKind,
        before?.record.stateVersion ?? null,
        after.stateVersion,
        evidenceChecksum,
        recordChecksum,
        recordJson,
        context.proof.leaseKey,
        context.proof.holderId,
        context.proof.acquisitionId,
        context.proof.fencingToken,
      )
    const saga =
      before === undefined
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
                       ?15, ?16, ?17, ?18, ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL})
               ON CONFLICT ("saga_id") DO NOTHING`,
            )
            .bind(
              after.sagaId,
              context.operationId,
              after.descriptor.descriptorId,
              after.descriptor.version,
              after.descriptor.descriptorChecksum,
              descriptorJson,
              after.idempotencyKey,
              after.inputChecksum,
              after.deadlineAtMs,
              after.status,
              sagaCommitment(after),
              after.terminationCause,
              after.terminationRequestedAtMs,
              after.stateVersion,
              evidenceChecksum,
              context.effectId,
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
                 AND "record_checksum" = ?12 AND "record_json" = ?13`,
            )
            .bind(
              after.sagaId,
              after.status,
              sagaCommitment(after),
              after.terminationCause,
              after.terminationRequestedAtMs,
              after.stateVersion,
              evidenceChecksum,
              context.effectId,
              recordChecksum,
              recordJson,
              before.record.stateVersion,
              beforeChecksum as string,
              beforeJson as string,
            )
    let results: readonly ControlRunResult[]
    try {
      results = await this.#database.batch([effect, saga])
    } catch (error) {
      const winner = await this.#loaded(after.sagaId)
      if (winner !== undefined && sameRecord(winner.record, after)) return winner.record
      throw error
    }
    if (results.length !== 2) {
      return intervention("Control D1 returned an incomplete saga mutation batch.")
    }
    mutationChanges(results[0] as ControlRunResult)
    mutationChanges(results[1] as ControlRunResult)
    const receipt = await this.#loadEffect(context.effectId)
    if (
      receipt === undefined ||
      receipt.context.transitionId !== context.transitionId ||
      receipt.fromStateVersion !== (before?.record.stateVersion ?? null) ||
      receipt.toStateVersion !== after.stateVersion ||
      receipt.effectKind !== effectKind ||
      receipt.evidenceChecksum !== evidenceChecksum ||
      receipt.recordChecksum !== recordChecksum ||
      receipt.recordJson !== recordJson
    ) {
      return intervention("The durable saga operation-effect receipt is missing or contradictory.")
    }
    const persisted = await this.#loaded(after.sagaId)
    if (persisted === undefined) return resume("The saga mutation did not become durably visible.")
    if (!sameRecord(persisted.record, after)) {
      return intervention("A concurrent saga mutation committed a contradictory state.")
    }
    return persisted.record
  }

  async #apply(
    input: {
      readonly effect: SagaEffectContext
      readonly evidenceChecksum: string
      readonly sagaId: string
    },
    effectKind: string,
    mutation: (record: SagaRecord) => SagaRecord,
    transition: TransitionExpectation,
  ): Promise<SagaRecord> {
    validateContext(input.effect)
    const sagaId = boundedText(input.sagaId, "Saga ID")
    const evidenceChecksum = boundedText(input.evidenceChecksum, "Saga effect evidence checksum")
    const before = await this.#loaded(sagaId)
    const replay = await this.#replay(before, input.effect, effectKind, evidenceChecksum, sagaId)
    if (replay !== undefined) return replay
    if (before === undefined) return resume("The saga does not exist.")
    const after = mutation(before.record)
    if (after === before.record) return before.record
    return this.#persist(before, after, input.effect, effectKind, evidenceChecksum, transition)
  }

  async create(input: {
    readonly deadlineAtMs: number
    readonly descriptor: SagaDescriptor
    readonly effect: SagaEffectContext
    readonly evidenceChecksum: string
    readonly idempotencyKey: string
    readonly inputChecksum: string
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepInputChecksums: Readonly<Record<string, string>>
  }): Promise<SagaRecord> {
    validateContext(input.effect)
    const evidenceChecksum = boundedText(input.evidenceChecksum, "Saga effect evidence checksum")
    const fresh = createSagaRecord(input)
    const existing = await this.#loaded(fresh.sagaId)
    const replay = await this.#replay(
      existing,
      input.effect,
      "create",
      evidenceChecksum,
      fresh.sagaId,
    )
    if (replay !== undefined) return replay
    if (existing !== undefined) {
      if (
        existing.operationId === input.effect.operationId &&
        sameIdentity(existing.record, fresh)
      ) {
        return existing.record
      }
      return intervention("The saga ID is bound to contradictory immutable intent.")
    }
    return this.#persist(undefined, fresh, input.effect, "create", evidenceChecksum, {
      states: ["succeeded"],
      stepId: SAGA_INIT_OPERATION_STEP_ID,
    })
  }

  async beginAction(input: {
    readonly attemptId: string
    readonly effect: SagaEffectContext
    readonly evidenceChecksum: string
    readonly idempotencyKey: string
    readonly phase: SagaActionPhase
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepId: string
  }): Promise<SagaBeginDecision> {
    validateContext(input.effect)
    const sagaId = boundedText(input.sagaId, "Saga ID")
    const evidenceChecksum = boundedText(input.evidenceChecksum, "Saga effect evidence checksum")
    const before = await this.#loaded(sagaId)
    const replay = await this.#replay(
      before,
      input.effect,
      `action:${input.phase}:begin`,
      evidenceChecksum,
      sagaId,
    )
    const current = replay ?? before?.record
    if (current === undefined) return resume("The saga does not exist.")
    const decision = beginSagaAction(current, input)
    if (replay !== undefined || decision.disposition !== "execute") return decision
    const saga = await this.#persist(
      before,
      decision.saga,
      input.effect,
      `action:${input.phase}:begin`,
      evidenceChecksum,
      {
        attemptId: input.attemptId,
        states: ["running"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
    return Object.freeze({ disposition: "execute", saga })
  }

  async recordActionSuccess(input: {
    readonly attemptId: string
    readonly effect: SagaEffectContext
    readonly evidenceChecksum: string
    readonly phase: SagaActionPhase
    readonly resultChecksum: string
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepId: string
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `action:${input.phase}:success`,
      (record) => recordSagaActionSuccess(record, input),
      {
        attemptId: input.attemptId,
        states: ["succeeded"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
  }

  async recordActionFailure(input: {
    readonly attemptId: string
    readonly effect: SagaEffectContext
    readonly errorChecksum: string
    readonly evidenceChecksum: string
    readonly outcome: SagaActionFailureOutcome
    readonly phase: SagaActionPhase
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepId: string
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `action:${input.phase}:failure:${input.outcome}`,
      (record) => recordSagaActionFailure(record, input),
      {
        attemptId: input.attemptId,
        states:
          input.outcome === "unknown"
            ? ["unknown"]
            : input.outcome === "definitely_not_applied_retryable"
              ? ["retryable_failed"]
              : ["failed"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
  }

  async recordObservation(input: {
    readonly effect: SagaEffectContext
    readonly evidenceChecksum: string
    readonly observationEvidenceChecksum: string
    readonly outcome: SagaObservationOutcome
    readonly phase: SagaActionPhase
    readonly resultChecksum?: string
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepId: string
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `action:${input.phase}:observation:${input.outcome}`,
      (record) =>
        recordSagaObservation(record, {
          evidenceChecksum: input.observationEvidenceChecksum,
          outcome: input.outcome,
          phase: input.phase,
          ...(input.resultChecksum === undefined ? {} : { resultChecksum: input.resultChecksum }),
          serverTimeMs: input.serverTimeMs,
          stepId: input.stepId,
        }),
      {
        states:
          input.outcome === "applied"
            ? ["succeeded"]
            : input.outcome === "not_applied"
              ? ["retryable_failed"]
              : ["intervention_required"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
  }

  async requestTermination(input: {
    readonly cause: "cancellation" | "timeout"
    readonly effect: SagaEffectContext
    readonly evidenceChecksum: string
    readonly sagaId: string
    readonly serverTimeMs: number
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `termination:${input.cause}`,
      (record) => requestSagaTermination(record, input),
      { states: ["succeeded"], stepId: SAGA_TERMINATION_OPERATION_STEP_ID },
    )
  }

  async markRunningActionUnknown(input: {
    readonly attemptId: string
    readonly effect: SagaEffectContext
    readonly errorChecksum: string
    readonly evidenceChecksum: string
    readonly phase: SagaActionPhase
    readonly sagaId: string
    readonly stepId: string
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `action:${input.phase}:recovery:unknown`,
      (record) => markRunningSagaActionUnknown(record, input),
      {
        attemptId: input.attemptId,
        states: ["unknown"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
  }

  async markActionNotDispatched(input: {
    readonly attemptId: string
    readonly effect: SagaEffectContext
    readonly errorChecksum: string
    readonly evidenceChecksum: string
    readonly phase: SagaActionPhase
    readonly sagaId: string
    readonly serverTimeMs: number
    readonly stepId: string
  }): Promise<SagaRecord> {
    return this.#apply(
      input,
      `action:${input.phase}:recovery:not-dispatched`,
      (record) => markSagaActionNotDispatched(record, input),
      {
        attemptId: input.attemptId,
        states: ["retryable_failed"],
        stepId: sagaActionOperationStepId(input.stepId, input.phase),
      },
    )
  }
}
