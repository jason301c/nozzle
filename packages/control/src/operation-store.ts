import {
  appendAuditEvent,
  assertResumeCompatible,
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  type IrreversibleAuthorization,
  type LeaseProof,
  loadAuditEvent,
  loadOperationRecord,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepUnknownAfterCrash,
  NozzleError,
  type OperationPlan,
  type OperationRecord,
  type OperationStepPlan,
  type OperationStepRecord,
  operationStatus,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  type StepFailureOutcome,
  type StepInvocationDecision,
  type StepReconciliationOutcome,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"
import { D1LeaseStore } from "./lease-store.js"

const MAX_CREATE_ATTEMPTS = 16
const MAX_TRANSITION_ATTEMPTS = 16
const MAX_OPERATION_PAYLOAD_BYTES = 1024 * 1024
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const PROVIDER_ATTEMPT_OUTCOME_STATES = new Set<unknown>([null, "confirmed", "rejected", "unknown"])

interface OperationRow {
  readonly capability_snapshot_checksum: string
  readonly capability_snapshot_json: string
  readonly created_at_ms: number
  readonly environment_id: string
  readonly idempotency_key: string
  readonly idempotency_scope: string
  readonly input_checksum: string
  readonly input_json: string
  readonly operation_id: string
  readonly operation_type: string
  readonly plan_checksum: string
  readonly plan_json: string
  readonly required_shards_json: string
  readonly status: string
  readonly updated_at_ms: number
}

interface OperationStepRow {
  readonly fencing_token: number | null
  readonly idempotency_key: string
  readonly lease_key: string
  readonly operation_id: string
  readonly plan_json: string
  readonly record_json: string
  readonly state: string
  readonly step_id: string
  readonly updated_at_ms: number
}

interface IdempotencyRow {
  readonly input_checksum: string
  readonly operation_id: string
}

interface AuditSnapshotRow {
  readonly event_json: string | null
  readonly now_ms: number
}

interface TransitionRow {
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

interface ProviderAttemptPresenceRow {
  readonly acceptance_checksum: string
  readonly attempt_id: string
  readonly operation_id: string
  readonly outcome_checksum: string | null
  readonly purpose: string
  readonly state: string | null
  readonly step_id: string
}

export interface LoadedOperation {
  readonly capabilitySnapshotJson: string
  readonly environmentId: string
  readonly idempotencyScope: string
  readonly inputJson: string
  readonly operation: OperationRecord
  readonly requiredShardIds: readonly string[]
}

export interface OperationCreationResult extends LoadedOperation {
  readonly created: boolean
}

export interface CreateOperationInput {
  readonly actorChecksum: string
  readonly capabilitySnapshotJson: string
  readonly environmentId: string
  readonly idempotencyScope: string
  readonly inputJson: string
  readonly plan: OperationPlan
  readonly requiredShardIds: readonly string[]
}

interface TransitionIdentity {
  readonly actorChecksum: string
  readonly operationId: string
  readonly proof: LeaseProof
  readonly stepId: string
}

export interface BeginStoredOperationStepInput extends TransitionIdentity {
  readonly attemptId: string
  readonly idempotencyKey: string
  readonly irreversibleAuthorization?: IrreversibleAuthorization
  readonly observedPreconditionChecksum: string
}

export interface CompleteStoredOperationStepInput extends TransitionIdentity {
  readonly attemptId: string
  readonly counters?: {
    readonly cost?: Readonly<Record<string, number>>
    readonly progress?: Readonly<Record<string, number>>
  }
  readonly observedPostconditionChecksum: string
  readonly resultChecksum: string
}

export interface FailStoredOperationStepInput extends TransitionIdentity {
  readonly attemptId: string
  readonly counters?: {
    readonly cost?: Readonly<Record<string, number>>
    readonly progress?: Readonly<Record<string, number>>
  }
  readonly errorChecksum: string
  readonly outcome: StepFailureOutcome
}

export interface ReconcileStoredOperationStepInput extends TransitionIdentity {
  readonly counters?: {
    readonly cost?: Readonly<Record<string, number>>
    readonly progress?: Readonly<Record<string, number>>
  }
  readonly evidenceChecksum: string
  readonly observedPostconditionChecksum?: string
  readonly outcome: StepReconciliationOutcome
  readonly observationAttemptId?: string
  readonly reconciliationId: string
  readonly resultChecksum?: string
}

export interface RecoverStoredOperationStepInput extends TransitionIdentity {
  readonly recoveryId: string
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

function nonEmpty(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function parseJson(value: unknown, label: string): unknown {
  if (typeof value !== "string") return intervention(`${label} is malformed.`)
  try {
    return JSON.parse(value) as unknown
  } catch {
    return intervention(`${label} is not valid JSON.`)
  }
}

function canonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalJsonValue)
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>
    const output: Record<string, unknown> = {}
    for (const key of Object.keys(record).sort()) output[key] = canonicalJsonValue(record[key])
    return output
  }
  return value
}

function canonicalJson(value: unknown, label: string, persisted: boolean): string {
  const fail = persisted ? intervention : configuration
  if (typeof value !== "string" || value.length === 0) return fail(`${label} must be JSON text.`)
  if (new TextEncoder().encode(value).byteLength > MAX_OPERATION_PAYLOAD_BYTES) {
    return fail(`${label} exceeds the one MiB operation payload limit.`)
  }
  let decoded: unknown
  try {
    decoded = JSON.parse(value) as unknown
  } catch {
    return fail(`${label} is not valid JSON.`)
  }
  return JSON.stringify(canonicalJsonValue(decoded)) as string
}

async function checksumJson(
  digest: DigestFunction,
  json: string,
  label: string,
  persisted: boolean,
): Promise<string> {
  const checksum = await digest(new TextEncoder().encode(json))
  const fail = persisted ? intervention : configuration
  if (typeof checksum !== "string" || checksum.trim().length === 0) {
    return fail(`${label} checksum is malformed.`)
  }
  return checksum
}

function canonicalRequiredShards(input: readonly string[]): readonly string[] {
  if (!Array.isArray(input)) return configuration("Required shard membership must be an array.")
  const shards = [...input]
  for (const shardId of shards) nonEmpty(shardId, "Required shard ID")
  const canonical = [...new Set(shards)].sort()
  if (canonical.length !== shards.length) {
    return configuration("Required shard membership must not contain duplicates.")
  }
  return Object.freeze(canonical)
}

function loadRequiredShards(value: unknown): readonly string[] {
  if (
    !Array.isArray(value) ||
    value.some((item) => typeof item !== "string" || item.trim() === "")
  ) {
    return intervention("Persisted required shard membership is malformed.")
  }
  const canonical = [...new Set(value)].sort()
  if (
    canonical.length !== value.length ||
    canonical.some((shardId, index) => shardId !== value[index])
  ) {
    return intervention("Persisted required shard membership is not canonical.")
  }
  return Object.freeze(canonical)
}

function validateMutationResults(
  results: readonly ControlRunResult[],
  expectedCount: number,
): readonly number[] {
  if (!Array.isArray(results) || results.length !== expectedCount) {
    return intervention("Control D1 returned an incomplete operation batch result.")
  }
  const changes: number[] = []
  for (const result of results) {
    const count = result.meta.changes
    if (
      result.success !== true ||
      typeof count !== "number" ||
      !Number.isSafeInteger(count) ||
      count < 0 ||
      count > 1
    ) {
      return intervention("Control D1 returned malformed operation mutation metadata.")
    }
    changes.push(count)
  }
  return Object.freeze(changes)
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}

export function operationTransitionIdentity(kind: string, parts: readonly string[]): string {
  let identity = `nozzle.operation-transition.v1:${kind}`
  for (const part of parts) identity += `:${part.length}:${part}`
  return identity
}

export class D1OperationStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction
  readonly #leases: D1LeaseStore

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional control D1 database binding is required.")
    }
    if (typeof digest !== "function") configuration("An operation digest function is required.")
    this.#database = database
    this.#digest = digest
    this.#leases = new D1LeaseStore(database)
  }

  async #auditSnapshot(environmentId: string): Promise<{
    readonly nowMs: number
    readonly previous: Awaited<ReturnType<typeof loadAuditEvent>> | undefined
  }> {
    const row = await this.#database
      .prepare(
        `SELECT ${SERVER_TIME_SQL} AS "now_ms",
          (SELECT "event_json" FROM "nozzle_audit_log"
           WHERE "environment_id" = ?1 ORDER BY "sequence" DESC LIMIT 1) AS "event_json"`,
      )
      .bind(environmentId)
      .first<AuditSnapshotRow>()
    if (!row || !Number.isSafeInteger(row.now_ms) || row.now_ms < 0) {
      return intervention("Control D1 returned malformed authoritative operation time.")
    }
    if (row.event_json === null) return Object.freeze({ nowMs: row.now_ms, previous: undefined })
    const previous = await loadAuditEvent(
      parseJson(row.event_json, "Persisted audit event"),
      this.#digest,
    )
    if (previous.environmentId !== environmentId) {
      return intervention("The persisted audit head belongs to a different environment.")
    }
    return Object.freeze({ nowMs: row.now_ms, previous })
  }

  async #idempotency(
    environmentId: string,
    scope: string,
    idempotencyKey: string,
  ): Promise<IdempotencyRow | undefined> {
    const row = await this.#database
      .prepare(
        `SELECT "operation_id", "input_checksum" FROM "nozzle_idempotency_keys"
         WHERE "environment_id" = ?1 AND "scope" = ?2 AND "idempotency_key" = ?3`,
      )
      .bind(environmentId, scope, idempotencyKey)
      .first<IdempotencyRow>()
    if (row === null) return undefined
    if (
      typeof row.operation_id !== "string" ||
      row.operation_id.trim() === "" ||
      typeof row.input_checksum !== "string" ||
      row.input_checksum.trim() === ""
    ) {
      return intervention("Persisted operation idempotency state is malformed.")
    }
    return Object.freeze(row)
  }

  async #providerAttempt(attemptId: string): Promise<ProviderAttemptPresenceRow | undefined> {
    const row = await this.#database
      .prepare(
        `SELECT "attempt"."attempt_id", "attempt"."operation_id", "attempt"."step_id",
                "attempt"."acceptance_checksum", "attempt"."purpose", "outcome"."state",
                "outcome"."outcome_checksum"
         FROM "nozzle_provider_attempts" AS "attempt"
         LEFT JOIN "nozzle_provider_attempt_outcomes" AS "outcome" USING ("attempt_id")
         WHERE "attempt"."attempt_id" = ?1`,
      )
      .bind(attemptId)
      .first<ProviderAttemptPresenceRow>()
    if (row === null) return undefined
    if (
      row.attempt_id !== attemptId ||
      typeof row.operation_id !== "string" ||
      row.operation_id.trim() === "" ||
      typeof row.step_id !== "string" ||
      row.step_id.trim() === "" ||
      typeof row.acceptance_checksum !== "string" ||
      row.acceptance_checksum.trim() === "" ||
      (row.purpose !== "effect" && row.purpose !== "reconciliation") ||
      !PROVIDER_ATTEMPT_OUTCOME_STATES.has(row.state) ||
      (row.state === null
        ? row.outcome_checksum !== null
        : typeof row.outcome_checksum !== "string" || row.outcome_checksum.trim() === "")
    ) {
      return intervention("Persisted provider-attempt recovery evidence is malformed.")
    }
    return Object.freeze(row)
  }

  async get(operationId: string): Promise<LoadedOperation | undefined> {
    nonEmpty(operationId, "Operation ID")
    const row = await this.#database
      .prepare(
        `SELECT "operation_id", "environment_id", "operation_type", "idempotency_scope",
                "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
                "capability_snapshot_checksum", "capability_snapshot_json",
                "required_shards_json", "status",
                "created_at_ms", "updated_at_ms"
         FROM "nozzle_operations" WHERE "operation_id" = ?1`,
      )
      .bind(operationId)
      .first<OperationRow>()
    if (row === null) return undefined
    if (
      row.operation_id !== operationId ||
      typeof row.environment_id !== "string" ||
      row.environment_id.trim() === "" ||
      typeof row.idempotency_scope !== "string" ||
      row.idempotency_scope.trim() === "" ||
      !Number.isSafeInteger(row.created_at_ms) ||
      row.created_at_ms < 0 ||
      !Number.isSafeInteger(row.updated_at_ms) ||
      row.updated_at_ms < row.created_at_ms
    ) {
      return intervention("Persisted operation identity or timestamps are malformed.")
    }
    const stepResult = await this.#database
      .prepare(
        `SELECT "operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
                "record_json", "state", "fencing_token", "updated_at_ms"
         FROM "nozzle_operation_steps" WHERE "operation_id" = ?1 ORDER BY "step_id"`,
      )
      .bind(operationId)
      .all<OperationStepRow>()
    if (stepResult.success !== true || !Array.isArray(stepResult.results)) {
      return intervention("Control D1 returned malformed operation step results.")
    }
    const steps: Record<string, unknown> = {}
    for (const stepRow of stepResult.results) {
      if (
        stepRow.operation_id !== operationId ||
        typeof stepRow.step_id !== "string" ||
        stepRow.step_id.trim() === "" ||
        !Number.isSafeInteger(stepRow.updated_at_ms) ||
        stepRow.updated_at_ms < 0 ||
        Object.hasOwn(steps, stepRow.step_id)
      ) {
        return intervention("Persisted operation step identity is malformed.")
      }
      steps[stepRow.step_id] = parseJson(stepRow.record_json, "Persisted operation step record")
    }
    const operation = await loadOperationRecord(
      { plan: parseJson(row.plan_json, "Persisted operation plan"), steps },
      this.#digest,
    )
    const inputJson = canonicalJson(row.input_json, "Persisted operation input", true)
    const capabilitySnapshotJson = canonicalJson(
      row.capability_snapshot_json,
      "Persisted capability snapshot",
      true,
    )
    if (inputJson !== row.input_json || capabilitySnapshotJson !== row.capability_snapshot_json) {
      return intervention("Persisted operation payload JSON is not canonical.")
    }
    const inputChecksum = await checksumJson(this.#digest, inputJson, "Operation input", true)
    const capabilitySnapshotChecksum = await checksumJson(
      this.#digest,
      capabilitySnapshotJson,
      "Capability snapshot",
      true,
    )
    if (
      row.operation_type !== operation.plan.operationType ||
      row.idempotency_key !== operation.plan.idempotencyKey ||
      row.input_checksum !== operation.plan.inputChecksum ||
      row.plan_checksum !== operation.plan.planChecksum ||
      row.capability_snapshot_checksum !== operation.plan.capabilitySnapshotChecksum ||
      inputChecksum !== operation.plan.inputChecksum ||
      capabilitySnapshotChecksum !== operation.plan.capabilitySnapshotChecksum ||
      row.status !== operationStatus(operation)
    ) {
      return intervention("Persisted operation columns contradict the verified operation record.")
    }
    for (const [index, planStep] of operation.plan.steps.entries()) {
      const stepRow = stepResult.results[index]
      const record = operation.steps[planStep.stepId]
      if (
        !stepRow ||
        !record ||
        stepRow.step_id !== planStep.stepId ||
        stepRow.idempotency_key !== planStep.idempotencyKey ||
        stepRow.lease_key !== planStep.leaseKey ||
        stepRow.plan_json !== JSON.stringify(planStep) ||
        stepRow.state !== record.state ||
        stepRow.fencing_token !== (record.fencingToken ?? null)
      ) {
        return intervention("Persisted operation step columns contradict the verified step record.")
      }
    }
    const requiredShardIds = loadRequiredShards(
      parseJson(row.required_shards_json, "Persisted required shard membership"),
    )
    return Object.freeze({
      capabilitySnapshotJson,
      environmentId: row.environment_id,
      idempotencyScope: row.idempotency_scope,
      inputJson,
      operation,
      requiredShardIds,
    })
  }

  async #assertCreationMatches(
    loaded: LoadedOperation,
    input: CreateOperationInput,
    requiredShardIds: readonly string[],
  ): Promise<void> {
    assertResumeCompatible(loaded.operation.plan, input.plan)
    if (
      loaded.environmentId !== input.environmentId ||
      loaded.idempotencyScope !== input.idempotencyScope ||
      loaded.inputJson !== canonicalJson(input.inputJson, "Operation input", false) ||
      loaded.capabilitySnapshotJson !==
        canonicalJson(input.capabilitySnapshotJson, "Capability snapshot", false) ||
      !sameStrings(loaded.requiredShardIds, requiredShardIds)
    ) {
      return resume("The operation ID is already bound to a different target or shard set.")
    }
    const idempotency = await this.#idempotency(
      input.environmentId,
      input.idempotencyScope,
      input.plan.idempotencyKey,
    )
    if (
      !idempotency ||
      idempotency.operation_id !== input.plan.operationId ||
      idempotency.input_checksum !== input.plan.inputChecksum
    ) {
      return intervention("Persisted operation idempotency binding contradicts the operation plan.")
    }
  }

  async create(input: CreateOperationInput): Promise<OperationCreationResult> {
    nonEmpty(input.environmentId, "Environment ID")
    nonEmpty(input.idempotencyScope, "Idempotency scope")
    nonEmpty(input.actorChecksum, "Operation actor checksum")
    const inputJson = canonicalJson(input.inputJson, "Operation input", false)
    const capabilitySnapshotJson = canonicalJson(
      input.capabilitySnapshotJson,
      "Capability snapshot",
      false,
    )
    const inputChecksum = await checksumJson(this.#digest, inputJson, "Operation input", false)
    const capabilitySnapshotChecksum = await checksumJson(
      this.#digest,
      capabilitySnapshotJson,
      "Capability snapshot",
      false,
    )
    if (
      inputChecksum !== input.plan.inputChecksum ||
      capabilitySnapshotChecksum !== input.plan.capabilitySnapshotChecksum
    ) {
      return configuration("Operation payload checksums do not match the sealed plan.")
    }
    const requiredShardIds = canonicalRequiredShards(input.requiredShardIds)
    const initial = createOperationRecord(input.plan)

    for (let attempt = 0; attempt < MAX_CREATE_ATTEMPTS; attempt += 1) {
      const bound = await this.#idempotency(
        input.environmentId,
        input.idempotencyScope,
        input.plan.idempotencyKey,
      )
      if (bound) {
        if (
          bound.operation_id !== input.plan.operationId ||
          bound.input_checksum !== input.plan.inputChecksum
        ) {
          return resume("The idempotency key is already bound to a different operation input.")
        }
        const replay = await this.get(bound.operation_id)
        if (!replay) {
          return intervention("An operation idempotency binding references a missing operation.")
        }
        await this.#assertCreationMatches(replay, input, requiredShardIds)
        return Object.freeze({ ...replay, created: false })
      }
      const existing = await this.get(input.plan.operationId)
      if (existing) {
        await this.#assertCreationMatches(existing, input, requiredShardIds)
        return Object.freeze({ ...existing, created: false })
      }
      const auditSnapshot = await this.#auditSnapshot(input.environmentId)
      const audit = await appendAuditEvent(
        auditSnapshot.previous,
        {
          actorChecksum: input.actorChecksum,
          environmentId: input.environmentId,
          eventType: "operation.created",
          fencingToken: null,
          idempotencyKey: `${input.idempotencyScope}:${input.plan.idempotencyKey}:created`,
          operationId: input.plan.operationId,
          payloadChecksum: input.plan.inputChecksum,
          serverTimeMs: auditSnapshot.nowMs,
          stepId: null,
        },
        this.#digest,
      )
      const statements = [
        this.#database
          .prepare(
            `INSERT INTO "nozzle_operations"
             ("operation_id", "environment_id", "operation_type", "idempotency_scope",
              "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
              "capability_snapshot_checksum", "capability_snapshot_json",
              "required_shards_json", "status",
              "created_at_ms", "updated_at_ms")
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'planned',
                     ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL})
             ON CONFLICT ("operation_id") DO NOTHING`,
          )
          .bind(
            input.plan.operationId,
            input.environmentId,
            input.plan.operationType,
            input.idempotencyScope,
            input.plan.idempotencyKey,
            input.plan.inputChecksum,
            inputJson,
            input.plan.planChecksum,
            JSON.stringify(input.plan),
            input.plan.capabilitySnapshotChecksum,
            capabilitySnapshotJson,
            JSON.stringify(requiredShardIds),
          ),
        ...input.plan.steps.map((planStep) =>
          this.#database
            .prepare(
              `INSERT INTO "nozzle_operation_steps"
               ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
                "record_json", "state", "fencing_token", "updated_at_ms")
               SELECT ?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ${SERVER_TIME_SQL}
               WHERE EXISTS (
                 SELECT 1 FROM "nozzle_operations"
                 WHERE "operation_id" = ?1 AND "plan_checksum" = ?7
                   AND "environment_id" = ?8 AND "idempotency_scope" = ?9
               )
               ON CONFLICT ("operation_id", "step_id") DO NOTHING`,
            )
            .bind(
              input.plan.operationId,
              planStep.stepId,
              planStep.idempotencyKey,
              planStep.leaseKey,
              JSON.stringify(planStep),
              JSON.stringify(initial.steps[planStep.stepId]),
              input.plan.planChecksum,
              input.environmentId,
              input.idempotencyScope,
            ),
        ),
        this.#database
          .prepare(
            `INSERT INTO "nozzle_idempotency_keys"
             ("environment_id", "scope", "idempotency_key", "operation_id", "input_checksum",
              "created_at_ms")
             SELECT ?1, ?2, ?3, ?4, ?5, ${SERVER_TIME_SQL}
             WHERE EXISTS (
               SELECT 1 FROM "nozzle_operations"
               WHERE "operation_id" = ?4 AND "plan_checksum" = ?6
                 AND "environment_id" = ?1 AND "idempotency_scope" = ?2
             )
             ON CONFLICT ("environment_id", "scope", "idempotency_key") DO NOTHING`,
          )
          .bind(
            input.environmentId,
            input.idempotencyScope,
            input.plan.idempotencyKey,
            input.plan.operationId,
            input.plan.inputChecksum,
            input.plan.planChecksum,
          ),
        this.#database
          .prepare(
            `INSERT INTO "nozzle_audit_log"
             ("environment_id", "sequence", "previous_hash", "event_hash", "server_time_ms",
              "operation_id", "step_id", "event_json")
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7
             WHERE EXISTS (
               SELECT 1 FROM "nozzle_operations"
               WHERE "operation_id" = ?6 AND "plan_checksum" = ?8
                 AND "environment_id" = ?1 AND "idempotency_scope" = ?9
             )
             AND (SELECT count(*) FROM "nozzle_operation_steps" WHERE "operation_id" = ?6) = ?10
             AND NOT EXISTS (
               SELECT 1 FROM "nozzle_audit_log"
               WHERE "environment_id" = ?1 AND "event_hash" = ?4
             )`,
          )
          .bind(
            input.environmentId,
            audit.sequence,
            audit.previousHash,
            audit.eventHash,
            audit.serverTimeMs,
            input.plan.operationId,
            JSON.stringify(audit),
            input.plan.planChecksum,
            input.idempotencyScope,
            input.plan.steps.length,
          ),
      ]
      let results: readonly ControlRunResult[]
      try {
        results = await this.#database.batch(statements)
      } catch {
        const raced = await this.get(input.plan.operationId)
        if (raced) {
          await this.#assertCreationMatches(raced, input, requiredShardIds)
          return Object.freeze({ ...raced, created: false })
        }
        continue
      }
      const changes = validateMutationResults(results, statements.length)
      const loaded = await this.get(input.plan.operationId)
      if (!loaded) continue
      await this.#assertCreationMatches(loaded, input, requiredShardIds)
      return Object.freeze({ ...loaded, created: changes[0] === 1 })
    }
    return intervention("Operation creation exceeded the bounded transactional retry budget.")
  }

  async #transition(transitionId: string): Promise<TransitionRow | undefined> {
    const row = await this.#database
      .prepare(
        `SELECT "transition_id", "operation_id", "step_id", "from_record_json",
                "to_record_json", "from_operation_status", "to_operation_status",
                "audit_event_hash", "fencing_token", "lease_key", "holder_id", "acquisition_id"
         FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1`,
      )
      .bind(transitionId)
      .first<TransitionRow>()
    if (row === null) return undefined
    if (
      row.transition_id !== transitionId ||
      typeof row.operation_id !== "string" ||
      row.operation_id.trim() === "" ||
      typeof row.step_id !== "string" ||
      row.step_id.trim() === "" ||
      typeof row.from_record_json !== "string" ||
      typeof row.to_record_json !== "string" ||
      typeof row.from_operation_status !== "string" ||
      typeof row.to_operation_status !== "string" ||
      typeof row.audit_event_hash !== "string" ||
      row.audit_event_hash.trim() === "" ||
      typeof row.lease_key !== "string" ||
      row.lease_key.trim() === "" ||
      typeof row.holder_id !== "string" ||
      row.holder_id.trim() === "" ||
      typeof row.acquisition_id !== "string" ||
      row.acquisition_id.trim() === "" ||
      !Number.isSafeInteger(row.fencing_token) ||
      row.fencing_token < 1
    ) {
      return intervention("Persisted operation transition receipt is malformed.")
    }
    return Object.freeze(row)
  }

  async #persistTransition(input: {
    readonly actorChecksum: string
    readonly after: LoadedOperation
    readonly auditEventType: string
    readonly auditPayloadChecksum: string
    readonly before: LoadedOperation
    readonly proof: LeaseProof
    readonly stepId: string
    readonly transitionId: string
  }): Promise<LoadedOperation | undefined> {
    nonEmpty(input.actorChecksum, "Transition actor checksum")
    nonEmpty(input.auditEventType, "Transition audit event type")
    nonEmpty(input.auditPayloadChecksum, "Transition audit payload checksum")
    nonEmpty(input.transitionId, "Operation transition ID")
    const after: LoadedOperation = Object.freeze({
      ...input.after,
      operation: await loadOperationRecord(input.after.operation, this.#digest),
    })
    const planStep = input.before.operation.plan.steps.find(
      (step) => step.stepId === input.stepId,
    ) as OperationStepPlan
    const beforeRecord = input.before.operation.steps[input.stepId] as OperationStepRecord
    const afterRecord = after.operation.steps[input.stepId] as OperationStepRecord
    if (planStep.leaseKey !== input.proof.leaseKey) {
      return resume("The operation transition was authorized under the wrong lease key.")
    }
    const beforeJson = JSON.stringify(beforeRecord)
    const afterJson = JSON.stringify(afterRecord)
    const fromStatus = operationStatus(input.before.operation)
    const toStatus = operationStatus(after.operation)
    const auditSnapshot = await this.#auditSnapshot(input.before.environmentId)
    const audit = await appendAuditEvent(
      auditSnapshot.previous,
      {
        actorChecksum: input.actorChecksum,
        environmentId: input.before.environmentId,
        eventType: input.auditEventType,
        fencingToken: input.proof.fencingToken,
        idempotencyKey: input.transitionId,
        operationId: input.before.operation.plan.operationId,
        payloadChecksum: input.auditPayloadChecksum,
        serverTimeMs: auditSnapshot.nowMs,
        stepId: input.stepId,
      },
      this.#digest,
    )
    const statements = [
      this.#database
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
           )
           ON CONFLICT ("transition_id") DO NOTHING`,
        )
        .bind(
          input.transitionId,
          input.before.operation.plan.operationId,
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
        ),
      this.#database
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
          input.before.operation.plan.operationId,
          input.stepId,
          beforeJson,
          input.transitionId,
          audit.eventHash,
          input.proof.fencingToken,
          input.proof.leaseKey,
          input.proof.holderId,
          input.proof.acquisitionId,
        ),
      this.#database
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
          input.before.operation.plan.operationId,
          fromStatus,
          input.transitionId,
          audit.eventHash,
          input.stepId,
          afterJson,
        ),
      this.#database
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
             SELECT 1 FROM "nozzle_operation_steps"
             WHERE "operation_id" = ?6 AND "step_id" = ?7 AND "record_json" = ?10
           ) AND EXISTS (
             SELECT 1 FROM "nozzle_operations"
             WHERE "operation_id" = ?6 AND "status" = ?11
           ) AND NOT EXISTS (
             SELECT 1 FROM "nozzle_audit_log"
             WHERE "environment_id" = ?1 AND "event_hash" = ?4
           )`,
        )
        .bind(
          input.before.environmentId,
          audit.sequence,
          audit.previousHash,
          audit.eventHash,
          audit.serverTimeMs,
          input.before.operation.plan.operationId,
          input.stepId,
          JSON.stringify(audit),
          input.transitionId,
          afterJson,
          toStatus,
        ),
    ]
    let results: readonly ControlRunResult[] | undefined
    try {
      results = await this.#database.batch(statements)
    } catch {
      // The audit head or step may have advanced. Exact receipt validation below decides replay.
    }
    if (results !== undefined) validateMutationResults(results, statements.length)
    const receipt = await this.#transition(input.transitionId)
    if (!receipt) return undefined
    if (
      receipt.operation_id !== input.before.operation.plan.operationId ||
      receipt.step_id !== input.stepId ||
      receipt.from_record_json !== beforeJson ||
      receipt.to_record_json !== afterJson ||
      receipt.from_operation_status !== fromStatus ||
      receipt.to_operation_status !== toStatus ||
      receipt.audit_event_hash !== audit.eventHash ||
      receipt.fencing_token !== input.proof.fencingToken ||
      receipt.lease_key !== input.proof.leaseKey ||
      receipt.holder_id !== input.proof.holderId ||
      receipt.acquisition_id !== input.proof.acquisitionId
    ) {
      return intervention("An operation transition ID is bound to contradictory durable state.")
    }
    const persisted = await this.get(input.before.operation.plan.operationId)
    if (!persisted) return intervention("A transition receipt references a missing operation.")
    const current = await this.#database
      .prepare(
        `SELECT "nozzle_operation_steps"."record_json" AS "record_json",
                "nozzle_operations"."status" AS "status"
         FROM "nozzle_operation_steps"
         JOIN "nozzle_operations" USING ("operation_id")
         WHERE "nozzle_operation_steps"."operation_id" = ?1
           AND "nozzle_operation_steps"."step_id" = ?2`,
      )
      .bind(input.before.operation.plan.operationId, input.stepId)
      .first<{ readonly record_json: string; readonly status: string }>()
    if (
      current?.record_json !== afterJson ||
      current.status !== toStatus ||
      operationStatus(persisted.operation) !== toStatus
    ) {
      return intervention("A transition receipt was committed without its exact operation state.")
    }
    const auditRow = await this.#database
      .prepare(
        `SELECT 1 AS "present" FROM "nozzle_audit_log"
         WHERE "environment_id" = ?1 AND "event_hash" = ?2`,
      )
      .bind(input.before.environmentId, audit.eventHash)
      .first<{ readonly present: number }>()
    if (auditRow?.present !== 1) {
      return intervention("A transition receipt was committed without its audit event.")
    }
    return persisted
  }

  async beginStep(input: BeginStoredOperationStepInput): Promise<StepInvocationDecision> {
    nonEmpty(input.operationId, "Operation ID")
    nonEmpty(input.actorChecksum, "Transition actor checksum")
    for (let attempt = 0; attempt < MAX_TRANSITION_ATTEMPTS; attempt += 1) {
      const before = await this.get(input.operationId)
      if (!before) return resume("The operation does not exist.")
      const authorized = await this.#leases.authorizeAt(input.proof)
      const decision = beginOperationStep(before.operation, {
        attemptId: input.attemptId,
        idempotencyKey: input.idempotencyKey,
        ...(input.irreversibleAuthorization === undefined
          ? {}
          : { irreversibleAuthorization: input.irreversibleAuthorization }),
        lease: authorized.record,
        leaseProof: input.proof,
        observedPreconditionChecksum: input.observedPreconditionChecksum,
        serverTimeMs: authorized.serverTimeMs,
        stepId: input.stepId,
      })
      if (decision.disposition !== "execute") return decision
      const after: LoadedOperation = Object.freeze({ ...before, operation: decision.operation })
      const persisted = await this.#persistTransition({
        actorChecksum: input.actorChecksum,
        after,
        auditEventType: "step.attempt.accepted",
        auditPayloadChecksum: (
          before.operation.plan.steps.find(
            (step) => step.stepId === input.stepId,
          ) as OperationStepPlan
        ).inputChecksum,
        before,
        proof: input.proof,
        stepId: input.stepId,
        transitionId: operationTransitionIdentity("accepted", [
          input.operationId,
          input.stepId,
          input.attemptId,
        ]),
      })
      if (persisted)
        return Object.freeze({ disposition: "execute", operation: persisted.operation })
    }
    return intervention("Beginning an operation step exceeded the bounded transition retry budget.")
  }

  async recoverRunningStep(input: RecoverStoredOperationStepInput): Promise<OperationRecord> {
    nonEmpty(input.operationId, "Operation ID")
    nonEmpty(input.actorChecksum, "Transition actor checksum")
    nonEmpty(input.recoveryId, "Crash recovery ID")
    const transitionId = operationTransitionIdentity("crash-recovered", [
      input.operationId,
      input.stepId,
      input.recoveryId,
    ])
    for (let attempt = 0; attempt < MAX_TRANSITION_ATTEMPTS; attempt += 1) {
      const before = await this.get(input.operationId)
      if (!before) return resume("The operation does not exist.")
      const record = before.operation.steps[input.stepId]
      if (record?.state !== "running") {
        const receipt = await this.#transition(transitionId)
        if (!receipt) return markRunningStepUnknownAfterCrash(before.operation, input.stepId)
        if (
          receipt.operation_id !== input.operationId ||
          receipt.step_id !== input.stepId ||
          receipt.to_record_json !== JSON.stringify(record) ||
          receipt.fencing_token !== input.proof.fencingToken ||
          receipt.lease_key !== input.proof.leaseKey ||
          receipt.holder_id !== input.proof.holderId ||
          receipt.acquisition_id !== input.proof.acquisitionId
        ) {
          return intervention("A crash recovery ID is bound to contradictory durable state.")
        }
        return before.operation
      }
      const unknown = markRunningStepUnknownAfterCrash(before.operation, input.stepId)
      const originalFencingToken = record.fencingToken as number
      const activeAttemptId = record.activeAttemptId as string
      const lastAttemptId = record.lastAttemptId as string
      await this.#leases.authorizeAt(input.proof)
      if (input.proof.fencingToken <= originalFencingToken) {
        return resume("Crash recovery requires a strictly newer lease fencing token.")
      }
      const planStep = before.operation.plan.steps.find(
        (candidate) => candidate.stepId === input.stepId,
      ) as OperationStepPlan
      const providerAttempt =
        planStep.effectProtocol === "provider_receipt"
          ? await this.#providerAttempt(activeAttemptId)
          : undefined
      if (
        providerAttempt !== undefined &&
        (providerAttempt.operation_id !== input.operationId ||
          providerAttempt.step_id !== input.stepId)
      ) {
        return intervention("Provider-attempt recovery evidence belongs to a different step.")
      }
      const notDispatched =
        planStep.effectProtocol === "provider_receipt" && providerAttempt === undefined
      let next = unknown
      if (notDispatched) {
        const absenceEvidenceChecksum = await this.#digest(
          new TextEncoder().encode(
            operationTransitionIdentity("provider-not-dispatched-evidence", [
              input.operationId,
              input.stepId,
              activeAttemptId,
              input.proof.fencingToken.toString(10),
            ]),
          ),
        )
        nonEmpty(absenceEvidenceChecksum, "Provider dispatch-absence evidence checksum")
        next = markRunningStepNotDispatchedAfterCrash(
          before.operation,
          input.stepId,
          absenceEvidenceChecksum,
        )
      }
      const after: LoadedOperation = Object.freeze({ ...before, operation: next })
      const persisted = await this.#persistTransition({
        actorChecksum: input.actorChecksum,
        after,
        auditEventType: notDispatched ? "step.crash.not_dispatched" : "step.crash.outcome_unknown",
        auditPayloadChecksum: providerAttempt?.acceptance_checksum ?? lastAttemptId,
        before,
        proof: input.proof,
        stepId: input.stepId,
        transitionId,
      })
      if (persisted) return persisted.operation
    }
    return intervention("Crash recovery exceeded the bounded transition retry budget.")
  }

  async completeStep(input: CompleteStoredOperationStepInput): Promise<OperationRecord> {
    return this.#recordOutcome(
      input,
      "step.attempt.succeeded",
      input.resultChecksum,
      (operation) =>
        recordStepSuccess(operation, {
          attemptId: input.attemptId,
          ...(input.counters === undefined ? {} : { counters: input.counters }),
          observedPostconditionChecksum: input.observedPostconditionChecksum,
          resultChecksum: input.resultChecksum,
          stepId: input.stepId,
        }),
      operationTransitionIdentity("succeeded", [input.operationId, input.stepId, input.attemptId]),
      true,
      {
        attemptId: input.attemptId,
        checksum: input.resultChecksum,
        purpose: "effect",
        state: "confirmed",
      },
    )
  }

  async failStep(input: FailStoredOperationStepInput): Promise<OperationRecord> {
    return this.#recordOutcome(
      input,
      `step.attempt.${input.outcome}`,
      input.errorChecksum,
      (operation) =>
        recordStepFailure(operation, {
          attemptId: input.attemptId,
          ...(input.counters === undefined ? {} : { counters: input.counters }),
          errorChecksum: input.errorChecksum,
          outcome: input.outcome,
          stepId: input.stepId,
        }),
      operationTransitionIdentity("failed", [input.operationId, input.stepId, input.attemptId]),
      true,
      {
        attemptId: input.attemptId,
        checksum: input.errorChecksum,
        purpose: "effect",
        state: input.outcome === "unknown" ? "unknown" : "rejected",
      },
    )
  }

  async reconcileStep(input: ReconcileStoredOperationStepInput): Promise<OperationRecord> {
    nonEmpty(input.reconciliationId, "Reconciliation ID")
    return this.#recordOutcome(
      input,
      `step.reconciled.${input.outcome}`,
      input.evidenceChecksum,
      (operation) =>
        recordStepReconciliation(operation, {
          ...(input.counters === undefined ? {} : { counters: input.counters }),
          evidenceChecksum: input.evidenceChecksum,
          ...(input.observedPostconditionChecksum === undefined
            ? {}
            : { observedPostconditionChecksum: input.observedPostconditionChecksum }),
          outcome: input.outcome,
          ...(input.resultChecksum === undefined ? {} : { resultChecksum: input.resultChecksum }),
          stepId: input.stepId,
        }),
      operationTransitionIdentity("reconciled", [
        input.operationId,
        input.stepId,
        input.reconciliationId,
      ]),
      false,
      input.observationAttemptId === undefined
        ? undefined
        : {
            attemptId: input.observationAttemptId,
            checksum: input.evidenceChecksum,
            purpose: "reconciliation",
            state: "confirmed",
          },
    )
  }

  async #recordOutcome(
    input: TransitionIdentity,
    auditEventType: string,
    auditPayloadChecksum: string,
    transition: (operation: OperationRecord) => OperationRecord,
    transitionId: string,
    requireAttemptFence: boolean,
    providerOutcome?: {
      readonly attemptId: string
      readonly checksum: string
      readonly purpose: "effect" | "reconciliation"
      readonly state: "confirmed" | "rejected" | "unknown"
    },
  ): Promise<OperationRecord> {
    nonEmpty(input.operationId, "Operation ID")
    nonEmpty(input.actorChecksum, "Transition actor checksum")
    for (let attempt = 0; attempt < MAX_TRANSITION_ATTEMPTS; attempt += 1) {
      const before = await this.get(input.operationId)
      if (!before) return resume("The operation does not exist.")
      const planStep = before.operation.plan.steps.find(
        (candidate) => candidate.stepId === input.stepId,
      ) as OperationStepPlan | undefined
      if (planStep?.effectProtocol === "provider_receipt") {
        if (providerOutcome === undefined) {
          return intervention("A provider-receipt step outcome lacks receipt requirements.")
        }
        const receipt = await this.#providerAttempt(providerOutcome.attemptId)
        if (receipt === undefined || receipt.state === null) {
          return resume("The provider attempt has no terminal outcome receipt.")
        }
        if (
          receipt.operation_id !== input.operationId ||
          receipt.step_id !== input.stepId ||
          receipt.purpose !== providerOutcome.purpose ||
          receipt.state !== providerOutcome.state ||
          receipt.outcome_checksum !== providerOutcome.checksum
        ) {
          return intervention("The provider outcome receipt contradicts the operation transition.")
        }
      }
      const next = transition(before.operation)
      if (next === before.operation) return before.operation
      await this.#leases.authorizeAt(input.proof)
      if (
        requireAttemptFence &&
        before.operation.steps[input.stepId]?.fencingToken !== input.proof.fencingToken
      ) {
        return resume("The operation attempt result was fenced by a newer lease owner.")
      }
      const after: LoadedOperation = Object.freeze({ ...before, operation: next })
      const persisted = await this.#persistTransition({
        actorChecksum: input.actorChecksum,
        after,
        auditEventType,
        auditPayloadChecksum,
        before,
        proof: input.proof,
        stepId: input.stepId,
        transitionId,
      })
      if (persisted) return persisted.operation
    }
    return intervention(
      "Recording an operation outcome exceeded the bounded transition retry budget.",
    )
  }
}
