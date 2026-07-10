import {
  appendAuditEvent,
  assertResumeCompatible,
  createOperationRecord,
  type DigestFunction,
  loadAuditEvent,
  loadOperationRecord,
  NozzleError,
  type OperationPlan,
  type OperationRecord,
  operationStatus,
} from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"

const MAX_CREATE_ATTEMPTS = 16
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

interface OperationRow {
  readonly capability_snapshot_checksum: string
  readonly created_at_ms: number
  readonly environment_id: string
  readonly idempotency_key: string
  readonly idempotency_scope: string
  readonly input_checksum: string
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

export interface LoadedOperation {
  readonly environmentId: string
  readonly idempotencyScope: string
  readonly operation: OperationRecord
  readonly requiredShardIds: readonly string[]
}

export interface OperationCreationResult extends LoadedOperation {
  readonly created: boolean
}

export interface CreateOperationInput {
  readonly actorChecksum: string
  readonly environmentId: string
  readonly idempotencyScope: string
  readonly plan: OperationPlan
  readonly requiredShardIds: readonly string[]
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

export class D1OperationStore {
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
    if (typeof digest !== "function") configuration("An operation digest function is required.")
    this.#database = database
    this.#digest = digest
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

  async get(operationId: string): Promise<LoadedOperation | undefined> {
    nonEmpty(operationId, "Operation ID")
    const row = await this.#database
      .prepare(
        `SELECT "operation_id", "environment_id", "operation_type", "idempotency_scope",
                "idempotency_key", "input_checksum", "plan_checksum", "plan_json",
                "capability_snapshot_checksum", "required_shards_json", "status",
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
    if (
      row.operation_type !== operation.plan.operationType ||
      row.idempotency_key !== operation.plan.idempotencyKey ||
      row.input_checksum !== operation.plan.inputChecksum ||
      row.plan_checksum !== operation.plan.planChecksum ||
      row.capability_snapshot_checksum !== operation.plan.capabilitySnapshotChecksum ||
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
      environmentId: row.environment_id,
      idempotencyScope: row.idempotency_scope,
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
              "idempotency_key", "input_checksum", "plan_checksum", "plan_json",
              "capability_snapshot_checksum", "required_shards_json", "status",
              "created_at_ms", "updated_at_ms")
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'planned',
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
            input.plan.planChecksum,
            JSON.stringify(input.plan),
            input.plan.capabilitySnapshotChecksum,
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
}
