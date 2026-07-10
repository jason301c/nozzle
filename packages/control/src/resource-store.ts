import {
  createD1ResourceRecord,
  type D1ResourceBinding,
  type D1ResourceIdentity,
  type D1ResourceLifecycleAction,
  type D1ResourceObservationValue,
  type D1ResourceRecord,
  type DigestFunction,
  type LeaseProof,
  loadD1ResourceRecord,
  NozzleError,
  observeD1Resource,
  registerD1Resource,
  transitionD1Resource,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const RECORD_DOMAIN = "nozzle.d1-resource-record.v1"
const MAX_IDENTITY_BYTES = 512

export interface D1ResourceEffectContext {
  readonly effectId: string
  readonly operationId: string
  readonly proof: LeaseProof
  readonly stepId: string
}

interface D1ResourceRow {
  readonly creation_operation_id: string
  readonly created_at_ms: number
  readonly database_id: string | null
  readonly database_name: string
  readonly desired_jurisdiction: string
  readonly effect_evidence_checksum: string | null
  readonly effect_id: string | null
  readonly effect_record_checksum: string | null
  readonly effect_record_json: string | null
  readonly effect_resource_id: string | null
  readonly effect_resource_kind: string | null
  readonly effect_to_state_version: number | null
  readonly environment_id: string
  readonly fleet_id: string
  readonly generation_id: string
  readonly intent_checksum: string
  readonly last_effect_id: string
  readonly last_evidence_checksum: string
  readonly last_observation_presence: string | null
  readonly lifecycle: string
  readonly record_checksum: string
  readonly record_json: string
  readonly resource_id: string
  readonly shard_id: string
  readonly state_version: number
  readonly target_checksum: string
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

interface PersistedEffect {
  readonly context: D1ResourceEffectContext
  readonly effectKind: string
  readonly fromStateVersion: number | null
  readonly record: D1ResourceRecord
  readonly recordChecksum: string
  readonly recordJson: string
  readonly toStateVersion: number
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

function boundedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    return configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    return configuration(`${label} exceeds the durable resource identity limit.`)
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

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (typeof value !== "object" || value === null) return value
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) {
    output[key] = canonicalValue((value as Record<string, unknown>)[key])
  }
  return output
}

function encode(record: D1ResourceRecord): string {
  return JSON.stringify(canonicalValue(record))
}

function canonicalPersistedJson(value: unknown): {
  readonly json: string
  readonly value: unknown
} {
  if (typeof value !== "string") {
    return intervention("Persisted D1 resource JSON is malformed.")
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return intervention("Persisted D1 resource JSON is invalid.")
  }
  const json = JSON.stringify(canonicalValue(parsed))
  if (json !== value) return intervention("Persisted D1 resource JSON is not canonical.")
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
  return boundedText(await digest(frame(RECORD_DOMAIN, recordJson)), "D1 resource record checksum")
}

function mutationChanges(result: ControlRunResult): number {
  const changes = result.meta.changes
  if (
    result.success !== true ||
    !Number.isSafeInteger(changes) ||
    (changes as number) < 0 ||
    (changes as number) > 1
  ) {
    return intervention("Control D1 returned malformed resource mutation metadata.")
  }
  return changes as number
}

function sameRecord(left: D1ResourceRecord, right: D1ResourceRecord): boolean {
  return encode(left) === encode(right)
}

function sameIdentity(left: D1ResourceRecord, right: D1ResourceRecord): boolean {
  return (
    left.resourceId === right.resourceId &&
    left.generationId === right.generationId &&
    left.fleetId === right.fleetId &&
    left.environmentId === right.environmentId &&
    left.shardId === right.shardId &&
    left.targetChecksum === right.targetChecksum &&
    left.creationOperationId === right.creationOperationId &&
    left.intentChecksum === right.intentChecksum &&
    left.databaseName === right.databaseName &&
    left.desiredJurisdiction === right.desiredJurisdiction
  )
}

function validateContext(context: D1ResourceEffectContext): void {
  boundedText(context.effectId, "Operation effect ID")
  boundedText(context.operationId, "Operation ID")
  boundedText(context.stepId, "Operation step ID")
  validProof(context.proof)
}

function validInteger(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum
}

export class D1ResourceStore {
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
    if (typeof digest !== "function") configuration("A digest implementation is required.")
    this.#database = database
    this.#digest = digest
  }

  async #decode(row: D1ResourceRow): Promise<D1ResourceRecord> {
    if (
      typeof row.resource_id !== "string" ||
      typeof row.generation_id !== "string" ||
      typeof row.fleet_id !== "string" ||
      typeof row.environment_id !== "string" ||
      typeof row.shard_id !== "string" ||
      typeof row.target_checksum !== "string" ||
      typeof row.creation_operation_id !== "string" ||
      typeof row.intent_checksum !== "string" ||
      typeof row.database_name !== "string" ||
      typeof row.desired_jurisdiction !== "string" ||
      (row.database_id !== null && typeof row.database_id !== "string") ||
      typeof row.lifecycle !== "string" ||
      !validInteger(row.state_version, 0) ||
      typeof row.last_evidence_checksum !== "string" ||
      (row.last_observation_presence !== null &&
        row.last_observation_presence !== "present" &&
        row.last_observation_presence !== "absent") ||
      typeof row.last_effect_id !== "string" ||
      typeof row.record_checksum !== "string" ||
      !validInteger(row.created_at_ms, 0) ||
      !validInteger(row.updated_at_ms, row.created_at_ms)
    ) {
      return intervention("Persisted D1 resource columns are malformed.")
    }
    const persisted = canonicalPersistedJson(row.record_json)
    const record = loadD1ResourceRecord(persisted.value)
    const checksum = await checkedChecksum(this.#digest, persisted.json)
    if (
      record.resourceId !== row.resource_id ||
      record.generationId !== row.generation_id ||
      record.fleetId !== row.fleet_id ||
      record.environmentId !== row.environment_id ||
      record.shardId !== row.shard_id ||
      record.targetChecksum !== row.target_checksum ||
      record.creationOperationId !== row.creation_operation_id ||
      record.intentChecksum !== row.intent_checksum ||
      record.databaseName !== row.database_name ||
      record.desiredJurisdiction !== row.desired_jurisdiction ||
      (record.binding?.databaseId ?? null) !== row.database_id ||
      record.lifecycle !== row.lifecycle ||
      record.stateVersion !== row.state_version ||
      record.lastEvidenceChecksum !== row.last_evidence_checksum ||
      (record.lastObservation?.presence ?? null) !== row.last_observation_presence ||
      checksum !== row.record_checksum
    ) {
      return intervention("Persisted D1 resource columns contradict the canonical record.")
    }
    if (
      row.effect_id !== row.last_effect_id ||
      row.effect_resource_kind !== "d1_database" ||
      row.effect_resource_id !== row.resource_id ||
      row.effect_to_state_version !== row.state_version ||
      row.effect_evidence_checksum !== row.last_evidence_checksum ||
      row.effect_record_checksum !== row.record_checksum ||
      row.effect_record_json !== row.record_json
    ) {
      return intervention("Persisted D1 resource lacks its exact operation-effect receipt.")
    }
    return record
  }

  async #row(resourceId: string): Promise<D1ResourceRow | undefined> {
    const row = await this.#database
      .prepare(
        `SELECT "resource".*,
                "effect"."effect_id" AS "effect_id",
                "effect"."resource_kind" AS "effect_resource_kind",
                "effect"."resource_id" AS "effect_resource_id",
                "effect"."to_state_version" AS "effect_to_state_version",
                "effect"."evidence_checksum" AS "effect_evidence_checksum",
                "effect"."record_checksum" AS "effect_record_checksum",
                "effect"."record_json" AS "effect_record_json"
         FROM "nozzle_d1_resources" AS "resource"
         LEFT JOIN "nozzle_operation_effects" AS "effect"
           ON "effect"."effect_id" = "resource"."last_effect_id"
         WHERE "resource"."resource_id" = ?1`,
      )
      .bind(resourceId)
      .first<D1ResourceRow>()
    return row ?? undefined
  }

  async get(resourceIdInput: string): Promise<D1ResourceRecord | undefined> {
    const resourceId = boundedText(resourceIdInput, "D1 resource ID")
    const row = await this.#row(resourceId)
    return row === undefined ? undefined : this.#decode(row)
  }

  async #effect(effectId: string): Promise<OperationEffectRow | undefined> {
    const row = await this.#database
      .prepare(`SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?1`)
      .bind(effectId)
      .first<OperationEffectRow>()
    return row ?? undefined
  }

  async #transition(context: D1ResourceEffectContext): Promise<string> {
    const result = await this.#database
      .prepare(
        `SELECT "transition"."transition_id"
         FROM "nozzle_operation_transitions" AS "transition"
         JOIN "nozzle_leases" AS "lease" ON "lease"."lease_key" = "transition"."lease_key"
         WHERE "transition"."operation_id" = ?1 AND "transition"."step_id" = ?2
           AND "transition"."lease_key" = ?3 AND "transition"."holder_id" = ?4
           AND "transition"."acquisition_id" = ?5 AND "transition"."fencing_token" = ?6
           AND json_extract("transition"."to_record_json", '$.state') = 'succeeded'
           AND "lease"."holder_id" = ?4 AND "lease"."acquisition_id" = ?5
           AND "lease"."fencing_token" = ?6 AND "lease"."expires_at_ms" > ${SERVER_TIME_SQL}
         ORDER BY "transition"."created_at_ms", "transition"."transition_id"`,
      )
      .bind(
        context.operationId,
        context.stepId,
        context.proof.leaseKey,
        context.proof.holderId,
        context.proof.acquisitionId,
        context.proof.fencingToken,
      )
      .all<{ transition_id: string }>()
    if (result.success !== true || !Array.isArray(result.results)) {
      return intervention("Control D1 returned malformed operation-transition evidence.")
    }
    if (result.results.length === 0) {
      return resume("The resource effect has no succeeded transition under the active lease.")
    }
    if (
      result.results.length !== 1 ||
      typeof result.results[0]?.transition_id !== "string" ||
      result.results[0].transition_id.trim() === ""
    ) {
      return intervention("The resource effect has ambiguous operation-transition evidence.")
    }
    return result.results[0].transition_id
  }

  async #loadEffect(effectId: string): Promise<PersistedEffect | undefined> {
    const row = await this.#effect(effectId)
    if (row === undefined) return undefined
    if (
      typeof row.effect_id !== "string" ||
      typeof row.transition_id !== "string" ||
      typeof row.operation_id !== "string" ||
      typeof row.step_id !== "string" ||
      row.resource_kind !== "d1_database" ||
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
      return intervention("Persisted operation-effect receipt is malformed.")
    }
    const persisted = canonicalPersistedJson(row.record_json)
    const record = loadD1ResourceRecord(persisted.value)
    const checksum = await checkedChecksum(this.#digest, persisted.json)
    if (
      record.resourceId !== row.resource_id ||
      record.stateVersion !== row.to_state_version ||
      record.lastEvidenceChecksum !== row.evidence_checksum ||
      row.record_checksum !== checksum
    ) {
      return intervention("Persisted operation-effect receipt contradicts its resource record.")
    }
    return {
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
      },
      effectKind: row.effect_kind,
      fromStateVersion: row.from_state_version,
      record,
      recordChecksum: row.record_checksum,
      recordJson: persisted.json,
      toStateVersion: row.to_state_version,
      transitionId: row.transition_id,
    }
  }

  async #replay(
    resource: D1ResourceRecord | undefined,
    context: D1ResourceEffectContext,
    effectKind: string,
    resourceId: string,
  ): Promise<D1ResourceRecord | undefined> {
    const receipt = await this.#loadEffect(context.effectId)
    if (receipt === undefined) return undefined
    if (
      receipt.context.operationId !== context.operationId ||
      receipt.context.stepId !== context.stepId ||
      receipt.context.proof.leaseKey !== context.proof.leaseKey ||
      receipt.context.proof.holderId !== context.proof.holderId ||
      receipt.context.proof.acquisitionId !== context.proof.acquisitionId ||
      receipt.context.proof.fencingToken !== context.proof.fencingToken ||
      receipt.effectKind !== effectKind ||
      receipt.record.resourceId !== resourceId
    ) {
      return intervention("Operation effect replay contradicts its immutable receipt.")
    }
    if (resource === undefined || resource.stateVersion < receipt.toStateVersion) {
      return intervention("Operation effect receipt is not reflected in the resource projection.")
    }
    if (resource.stateVersion === receipt.toStateVersion && !sameRecord(resource, receipt.record)) {
      return intervention("Operation effect receipt contradicts the current resource projection.")
    }
    return resource
  }

  async #persist(
    before: D1ResourceRecord | undefined,
    after: D1ResourceRecord,
    context: D1ResourceEffectContext,
    effectKind: string,
  ): Promise<D1ResourceRecord> {
    const transitionId = await this.#transition(context)
    const recordJson = encode(after)
    const recordChecksum = await checkedChecksum(this.#digest, recordJson)
    const beforeJson = before === undefined ? undefined : encode(before)
    const beforeChecksum =
      beforeJson === undefined ? undefined : await checkedChecksum(this.#digest, beforeJson)
    const effect = this.#database
      .prepare(
        `INSERT INTO "nozzle_operation_effects"
         ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
          "resource_id", "effect_kind", "from_state_version", "to_state_version",
          "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
          "acquisition_id", "fencing_token", "created_at_ms")
         VALUES (?1, ?2, ?3, ?4, 'd1_database', ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                 ?12, ?13, ?14, ?15, ${SERVER_TIME_SQL})
         ON CONFLICT ("effect_id") DO NOTHING`,
      )
      .bind(
        context.effectId,
        transitionId,
        context.operationId,
        context.stepId,
        after.resourceId,
        effectKind,
        before?.stateVersion ?? null,
        after.stateVersion,
        after.lastEvidenceChecksum,
        recordChecksum,
        recordJson,
        context.proof.leaseKey,
        context.proof.holderId,
        context.proof.acquisitionId,
        context.proof.fencingToken,
      )
    const resource =
      before === undefined
        ? this.#database
            .prepare(
              `INSERT INTO "nozzle_d1_resources"
               ("resource_id", "generation_id", "fleet_id", "environment_id", "shard_id",
                "target_checksum", "creation_operation_id", "intent_checksum", "database_name",
                "desired_jurisdiction", "database_id", "lifecycle", "state_version",
                "last_evidence_checksum", "last_observation_presence", "last_effect_id",
                "record_checksum", "record_json", "created_at_ms", "updated_at_ms")
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                       ?15, ?16, ?17, ?18, ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL})
               ON CONFLICT ("resource_id") DO NOTHING`,
            )
            .bind(
              after.resourceId,
              after.generationId,
              after.fleetId,
              after.environmentId,
              after.shardId,
              after.targetChecksum,
              after.creationOperationId,
              after.intentChecksum,
              after.databaseName,
              after.desiredJurisdiction,
              after.binding?.databaseId ?? null,
              after.lifecycle,
              after.stateVersion,
              after.lastEvidenceChecksum,
              after.lastObservation?.presence ?? null,
              context.effectId,
              recordChecksum,
              recordJson,
            )
        : this.#database
            .prepare(
              `UPDATE "nozzle_d1_resources"
               SET "database_id" = ?2, "lifecycle" = ?3, "state_version" = ?4,
                   "last_evidence_checksum" = ?5, "last_observation_presence" = ?6,
                   "last_effect_id" = ?7, "record_checksum" = ?8, "record_json" = ?9,
                   "updated_at_ms" = ${SERVER_TIME_SQL}
               WHERE "resource_id" = ?1 AND "state_version" = ?10
                 AND "record_checksum" = ?11 AND "record_json" = ?12`,
            )
            .bind(
              after.resourceId,
              after.binding?.databaseId ?? null,
              after.lifecycle,
              after.stateVersion,
              after.lastEvidenceChecksum,
              after.lastObservation?.presence ?? null,
              context.effectId,
              recordChecksum,
              recordJson,
              before.stateVersion,
              beforeChecksum as string,
              beforeJson as string,
            )
    let results: readonly ControlRunResult[]
    try {
      results = await this.#database.batch([effect, resource])
    } catch (error) {
      const winner = await this.get(after.resourceId)
      if (winner !== undefined && sameRecord(winner, after)) return winner
      throw error
    }
    if (results.length !== 2) {
      return intervention("Control D1 returned an incomplete resource mutation batch.")
    }
    mutationChanges(results[0] as ControlRunResult)
    mutationChanges(results[1] as ControlRunResult)
    const receipt = await this.#loadEffect(context.effectId)
    if (
      receipt === undefined ||
      receipt.transitionId !== transitionId ||
      receipt.fromStateVersion !== (before?.stateVersion ?? null) ||
      receipt.toStateVersion !== after.stateVersion ||
      receipt.effectKind !== effectKind ||
      receipt.recordChecksum !== recordChecksum ||
      receipt.recordJson !== recordJson
    ) {
      return intervention(
        "The durable resource operation-effect receipt is missing or contradictory.",
      )
    }
    const persisted = await this.get(after.resourceId)
    if (persisted === undefined) {
      return resume("The resource mutation did not become durably visible.")
    }
    if (!sameRecord(persisted, after)) {
      return intervention("A concurrent resource mutation committed a contradictory state.")
    }
    return persisted
  }

  async create(input: {
    readonly effect: D1ResourceEffectContext
    readonly identity: D1ResourceIdentity
  }): Promise<D1ResourceRecord> {
    validateContext(input.effect)
    const fresh = createD1ResourceRecord(input.identity)
    if (fresh.creationOperationId !== input.effect.operationId) {
      configuration("The resource creation operation does not match its operation effect.")
    }
    const existing = await this.get(fresh.resourceId)
    const replay = await this.#replay(existing, input.effect, "plan", fresh.resourceId)
    if (replay !== undefined) return replay
    if (existing !== undefined) {
      if (sameIdentity(existing, fresh)) return existing
      return intervention("The D1 resource ID is bound to contradictory immutable intent.")
    }
    return this.#persist(undefined, fresh, input.effect, "plan")
  }

  async register(input: {
    readonly binding: D1ResourceBinding
    readonly effect: D1ResourceEffectContext
    readonly expectedStateVersion: number
    readonly resourceId: string
  }): Promise<D1ResourceRecord> {
    validateContext(input.effect)
    const resourceId = boundedText(input.resourceId, "D1 resource ID")
    const before = await this.get(resourceId)
    const replay = await this.#replay(before, input.effect, "register", resourceId)
    if (replay !== undefined) return replay
    if (before === undefined) return resume("The D1 resource does not exist.")
    const after = registerD1Resource(before, {
      ...input.binding,
      expectedStateVersion: input.expectedStateVersion,
    })
    if (after === before) return before
    return this.#persist(before, after, input.effect, "register")
  }

  async observe(input: {
    readonly effect: D1ResourceEffectContext
    readonly expectedStateVersion: number
    readonly observation: D1ResourceObservationValue
    readonly resourceId: string
  }): Promise<D1ResourceRecord> {
    validateContext(input.effect)
    const resourceId = boundedText(input.resourceId, "D1 resource ID")
    const before = await this.get(resourceId)
    const replay = await this.#replay(before, input.effect, "observe", resourceId)
    if (replay !== undefined) return replay
    if (before === undefined) return resume("The D1 resource does not exist.")
    const after = observeD1Resource(before, {
      ...input.observation,
      expectedStateVersion: input.expectedStateVersion,
    })
    if (after === before) return before
    return this.#persist(before, after, input.effect, "observe")
  }

  async transition(input: {
    readonly action: D1ResourceLifecycleAction
    readonly effect: D1ResourceEffectContext
    readonly evidenceChecksum: string
    readonly expectedStateVersion: number
    readonly resourceId: string
  }): Promise<D1ResourceRecord> {
    validateContext(input.effect)
    const resourceId = boundedText(input.resourceId, "D1 resource ID")
    const before = await this.get(resourceId)
    const effectKind = `lifecycle:${input.action.kind}`
    const replay = await this.#replay(before, input.effect, effectKind, resourceId)
    if (replay !== undefined) return replay
    if (before === undefined) return resume("The D1 resource does not exist.")
    const after = transitionD1Resource(before, {
      action: input.action,
      evidenceChecksum: input.evidenceChecksum,
      expectedStateVersion: input.expectedStateVersion,
    })
    return this.#persist(before, after, input.effect, effectKind)
  }
}
