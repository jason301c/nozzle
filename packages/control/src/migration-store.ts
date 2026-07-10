import {
  acceptMigrationShard,
  authorizeMigrationResume,
  createMigrationOperation,
  type LeaseProof,
  type MigrationCompatibility,
  type MigrationOperation,
  type MigrationShardState,
  migrationSucceeded,
  NozzleError,
  recordMigrationApplied,
  recordMigrationFailure,
  recordMigrationVerified,
} from "@nozzle/core"
import type { ControlDatabase, ControlRunResult } from "./database.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`
const APPLY_STATES = new Set([
  "applied",
  "blocked_failed",
  "pending",
  "retryable_failed",
  "running",
  "unknown",
])
const VERIFICATION_STATES = new Set(["failed", "pending", "unknown", "verified"])

interface MigrationOperationRow {
  readonly artifact_checksum: string
  readonly fleet_id: string
  readonly halt_control_sequence: number | null
  readonly halt_failed_shard_id: string | null
  readonly halt_fencing_token: number | null
  readonly operation_id: string
  readonly required_shards_json: string
  readonly resume_decision_checksum: string | null
  readonly resume_fencing_token: number | null
  readonly state: "mixed_blocked" | "running" | "succeeded"
  readonly target_schema_checksum: string
}

interface MigrationShardRow {
  readonly apply_state: string
  readonly canonical_schema_checksum: string | null
  readonly ledger_checksum: string | null
  readonly shard_id: string
  readonly verification_state: string
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function migrationError(message: string): never {
  throw new NozzleError("MigrationFailedError", message)
}

function nonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new NozzleError("ConfigurationError", `${label} must be non-empty.`)
  }
}

function mutationChanged(result: ControlRunResult): boolean {
  const changes = result.meta.changes
  // Workerd can report zero for an UPDATE that fired AFTER triggers, while other
  // SQLite runtimes report the direct row. Treat this field as advisory and verify
  // the exact durable state after every transition.
  if (!Number.isSafeInteger(changes) || (changes as number) < 0) {
    throw new NozzleError(
      "OperationInterventionRequiredError",
      "Control D1 returned invalid migration mutation metadata.",
    )
  }
  return changes === 1
}

function sameShardState(
  left: MigrationShardState | undefined,
  right: MigrationShardState,
): boolean {
  return (
    JSON.stringify([
      left?.apply,
      left?.verification,
      left?.ledgerChecksum,
      left?.canonicalSchemaChecksum,
    ]) ===
    JSON.stringify([
      right.apply,
      right.verification,
      right.ledgerChecksum,
      right.canonicalSchemaChecksum,
    ])
  )
}

function freshOperation(operation: MigrationOperation): MigrationOperation {
  const fresh = createMigrationOperation({
    artifactChecksum: operation.artifactChecksum,
    operationId: operation.operationId,
    requiredShardIds: operation.requiredShardIds,
    targetSchemaChecksum: operation.targetSchemaChecksum,
  })
  if (
    operation.halt !== undefined ||
    operation.resume !== undefined ||
    Object.keys(operation.shards).length !== operation.requiredShardIds.length ||
    operation.requiredShardIds.some(
      (shardId) =>
        operation.shards[shardId]?.apply !== "pending" ||
        operation.shards[shardId]?.verification !== "pending",
    )
  ) {
    throw new NozzleError("ConfigurationError", "Only a fresh migration may be registered.")
  }
  return fresh
}

function activeLeaseSql(offset: number): string {
  return `EXISTS (
    SELECT 1 FROM "nozzle_leases"
    WHERE "lease_key" = ?${offset}
      AND "holder_id" = ?${offset + 1}
      AND "acquisition_id" = ?${offset + 2}
      AND "fencing_token" = ?${offset + 3}
      AND "expires_at_ms" > ${SERVER_TIME_SQL}
  )`
}

function proofBindings(proof: LeaseProof): readonly [string, string, string, number] {
  return [proof.leaseKey, proof.holderId, proof.acquisitionId, proof.fencingToken]
}

function decodeRequiredShards(value: string): readonly string[] {
  let decoded: unknown
  try {
    decoded = JSON.parse(value)
  } catch {
    return migrationError("Persisted required-shard JSON is malformed.")
  }
  if (
    !Array.isArray(decoded) ||
    decoded.length === 0 ||
    decoded.some((entry) => typeof entry !== "string" || entry.length === 0) ||
    new Set(decoded).size !== decoded.length
  ) {
    return migrationError("Persisted required-shard membership is invalid.")
  }
  const sorted = [...decoded].sort()
  if (sorted.some((entry, index) => entry !== decoded[index])) {
    return migrationError("Persisted required-shard membership is not canonical.")
  }
  return Object.freeze(sorted)
}

function decodeOperation(
  operation: MigrationOperationRow,
  rows: readonly MigrationShardRow[],
): MigrationOperation {
  const requiredShardIds = decodeRequiredShards(operation.required_shards_json)
  if (rows.length !== requiredShardIds.length) {
    return migrationError("Persisted migration shard state is incomplete.")
  }
  const shards: Record<string, MigrationShardState> = {}
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index]
    const expectedShardId = requiredShardIds[index]
    if (
      !row ||
      row.shard_id !== expectedShardId ||
      !APPLY_STATES.has(row.apply_state) ||
      !VERIFICATION_STATES.has(row.verification_state)
    ) {
      return migrationError("Persisted migration shard state is malformed.")
    }
    shards[row.shard_id] = Object.freeze({
      apply: row.apply_state as MigrationShardState["apply"],
      ...(row.canonical_schema_checksum === null
        ? {}
        : { canonicalSchemaChecksum: row.canonical_schema_checksum }),
      ...(row.ledger_checksum === null ? {} : { ledgerChecksum: row.ledger_checksum }),
      verification: row.verification_state as MigrationShardState["verification"],
    })
  }
  const haltFields = [
    operation.halt_control_sequence,
    operation.halt_failed_shard_id,
    operation.halt_fencing_token,
  ]
  if (haltFields.some((value) => value === null) && haltFields.some((value) => value !== null)) {
    return migrationError("Persisted migration halt state is malformed.")
  }
  if (
    (operation.resume_decision_checksum === null) !== (operation.resume_fencing_token === null) ||
    (operation.resume_fencing_token !== null &&
      (operation.halt_fencing_token === null ||
        operation.resume_fencing_token <= operation.halt_fencing_token))
  ) {
    return migrationError("Persisted migration resume authorization is malformed.")
  }
  return Object.freeze({
    artifactChecksum: operation.artifact_checksum,
    ...(operation.halt_control_sequence === null
      ? {}
      : {
          halt: Object.freeze({
            controlSequence: operation.halt_control_sequence,
            failedShardId: operation.halt_failed_shard_id as string,
            fencingToken: operation.halt_fencing_token as number,
          }),
        }),
    operationId: operation.operation_id,
    requiredShardIds,
    ...(operation.resume_decision_checksum === null
      ? {}
      : {
          resume: Object.freeze({
            decisionChecksum: operation.resume_decision_checksum,
            fencingToken: operation.resume_fencing_token as number,
          }),
        }),
    shards: Object.freeze(shards),
    targetSchemaChecksum: operation.target_schema_checksum,
  })
}

export class D1MigrationStore {
  readonly #database: ControlDatabase

  constructor(database: ControlDatabase) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function"
    ) {
      throw new NozzleError("ConfigurationError", "A control D1 database binding is required.")
    }
    this.#database = database
  }

  async create(input: {
    readonly fleetId: string
    readonly operation: MigrationOperation
    readonly proof: LeaseProof
  }): Promise<MigrationOperation> {
    nonEmpty(input.fleetId, "Fleet ID")
    const operation = freshOperation(input.operation)
    const requiredJson = JSON.stringify(operation.requiredShardIds)
    await this.#database
      .prepare(
        `INSERT INTO "nozzle_migration_operations"
         ("operation_id", "fleet_id", "artifact_checksum", "target_schema_checksum",
          "required_shards_json", "halt_control_sequence", "halt_fencing_token",
          "halt_failed_shard_id", "resume_decision_checksum", "resume_fencing_token",
          "state", "created_at_ms", "updated_at_ms")
         SELECT ?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL,
                'running', ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL}
         WHERE ${activeLeaseSql(6)}
         ON CONFLICT ("operation_id") DO NOTHING`,
      )
      .bind(
        operation.operationId,
        input.fleetId,
        operation.artifactChecksum,
        operation.targetSchemaChecksum,
        requiredJson,
        ...proofBindings(input.proof),
      )
      .run()

    for (const shardId of operation.requiredShardIds) {
      await this.#database
        .prepare(
          `INSERT INTO "nozzle_migrations"
           ("operation_id", "fleet_id", "shard_id", "artifact_checksum", "apply_state",
            "verification_state", "attempts", "error_checksum", "failure_fencing_token", "updated_at_ms")
           SELECT ?1, ?2, ?3, ?4, 'pending', 'pending', 0, NULL, NULL, ${SERVER_TIME_SQL}
           WHERE ${activeLeaseSql(5)}
             AND EXISTS (
               SELECT 1 FROM "nozzle_migration_operations"
               WHERE "operation_id" = ?1 AND "fleet_id" = ?2 AND "artifact_checksum" = ?4
             )
           ON CONFLICT ("operation_id", "shard_id") DO NOTHING`,
        )
        .bind(
          operation.operationId,
          input.fleetId,
          shardId,
          operation.artifactChecksum,
          ...proofBindings(input.proof),
        )
        .run()
    }
    const persisted = await this.load(operation.operationId)
    if (!persisted) return resume("The migration lease expired before registration completed.")
    if (
      persisted.artifactChecksum !== operation.artifactChecksum ||
      persisted.targetSchemaChecksum !== operation.targetSchemaChecksum ||
      JSON.stringify(persisted.requiredShardIds) !== requiredJson
    ) {
      return resume("The operation ID is bound to an incompatible migration plan.")
    }
    return persisted
  }

  async load(operationId: string): Promise<MigrationOperation | undefined> {
    nonEmpty(operationId, "Operation ID")
    const operation = await this.#database
      .prepare(
        `SELECT "operation_id", "fleet_id", "artifact_checksum", "target_schema_checksum",
                "required_shards_json", "halt_control_sequence", "halt_fencing_token",
                "halt_failed_shard_id", "resume_decision_checksum", "resume_fencing_token", "state"
         FROM "nozzle_migration_operations" WHERE "operation_id" = ?1`,
      )
      .bind(operationId)
      .first<MigrationOperationRow>()
    if (!operation) return undefined
    const result = await this.#database
      .prepare(
        `SELECT "shard_id", "apply_state", "verification_state", "ledger_checksum",
                "canonical_schema_checksum"
         FROM "nozzle_migrations" WHERE "operation_id" = ?1 ORDER BY "shard_id"`,
      )
      .bind(operationId)
      .all<MigrationShardRow>()
    return decodeOperation(operation, result.results)
  }

  async #updateShard(
    operationId: string,
    shardId: string,
    proof: LeaseProof,
    transform: (operation: MigrationOperation) => MigrationOperation,
    options: { readonly errorChecksum?: string; readonly requireUnhalted?: boolean } = {},
  ): Promise<MigrationOperation> {
    const current = await this.load(operationId)
    if (!current) return resume("The migration operation does not exist.")
    const next = transform(current)
    const before = current.shards[shardId] as MigrationShardState
    const after = next.shards[shardId] as MigrationShardState
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_migrations"
         SET "apply_state" = ?1,
             "verification_state" = ?2,
             "ledger_checksum" = ?3,
             "canonical_schema_checksum" = ?4,
             "attempts" = "attempts" + CASE WHEN ?1 = 'running' THEN 1 ELSE 0 END,
             "error_checksum" = ?5,
             "failure_fencing_token" = CASE
               WHEN ?1 IN ('retryable_failed', 'blocked_failed', 'unknown') THEN ?6 ELSE NULL END,
             "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "operation_id" = ?7 AND "shard_id" = ?8
           AND "apply_state" = ?9 AND "verification_state" = ?10
           AND "ledger_checksum" IS ?11 AND "canonical_schema_checksum" IS ?12
           AND ${activeLeaseSql(13)}
           ${options.requireUnhalted ? `AND EXISTS (SELECT 1 FROM "nozzle_migration_operations" WHERE "operation_id" = ?7 AND ("halt_control_sequence" IS NULL OR "resume_decision_checksum" IS NOT NULL))` : ""}`,
      )
      .bind(
        after.apply,
        after.verification,
        after.ledgerChecksum ?? null,
        after.canonicalSchemaChecksum ?? null,
        options.errorChecksum ?? null,
        proof.fencingToken,
        operationId,
        shardId,
        before.apply,
        before.verification,
        before.ledgerChecksum ?? null,
        before.canonicalSchemaChecksum ?? null,
        ...proofBindings(proof),
      )
      .run()
    const changed = mutationChanged(result)
    const persisted = await this.load(operationId)
    if (!persisted) return migrationError("The migration disappeared after a durable transition.")
    if (!sameShardState(persisted.shards[shardId], after)) {
      if (!changed) {
        return resume("The migration transition lost its lease or compare-and-swap precondition.")
      }
      return migrationError("Control D1 did not persist the expected migration shard state.")
    }
    return persisted
  }

  accept(operationId: string, shardId: string, proof: LeaseProof): Promise<MigrationOperation> {
    return this.#updateShard(
      operationId,
      shardId,
      proof,
      (operation) => acceptMigrationShard(operation, shardId),
      { requireUnhalted: true },
    )
  }

  applied(
    operationId: string,
    shardId: string,
    ledgerChecksum: string,
    proof: LeaseProof,
  ): Promise<MigrationOperation> {
    return this.#updateShard(operationId, shardId, proof, (operation) =>
      recordMigrationApplied(operation, shardId, ledgerChecksum),
    )
  }

  verified(
    operationId: string,
    shardId: string,
    canonicalSchemaChecksum: string,
    proof: LeaseProof,
  ): Promise<MigrationOperation> {
    return this.#updateShard(operationId, shardId, proof, (operation) =>
      recordMigrationVerified(operation, shardId, canonicalSchemaChecksum),
    )
  }

  failed(
    operationId: string,
    input: {
      readonly apply: "blocked_failed" | "retryable_failed" | "unknown"
      readonly errorChecksum: string
      readonly proof: LeaseProof
      readonly shardId: string
      readonly verification?: "failed" | "unknown"
    },
  ): Promise<MigrationOperation> {
    nonEmpty(input.errorChecksum, "Migration error checksum")
    return this.#updateShard(
      operationId,
      input.shardId,
      input.proof,
      (operation) =>
        recordMigrationFailure(operation, {
          apply: input.apply,
          controlSequence: 1,
          fencingToken: input.proof.fencingToken,
          shardId: input.shardId,
          ...(input.verification ? { verification: input.verification } : {}),
        }),
      { errorChecksum: input.errorChecksum },
    )
  }

  async resume(
    operationId: string,
    input: { readonly decisionChecksum: string; readonly proof: LeaseProof },
  ): Promise<MigrationOperation> {
    const current = await this.load(operationId)
    if (!current) return resume("The migration operation does not exist.")
    const next = authorizeMigrationResume(current, {
      decisionChecksum: input.decisionChecksum,
      fencingToken: input.proof.fencingToken,
    })
    if (next === current) return current
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_migration_operations"
         SET "resume_decision_checksum" = ?1,
             "resume_fencing_token" = ?2,
             "state" = 'running',
             "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "operation_id" = ?3
           AND "halt_control_sequence" IS NOT NULL
           AND "halt_fencing_token" < ?2
           AND "resume_decision_checksum" IS NULL
           AND "resume_fencing_token" IS NULL
           AND NOT EXISTS (
             SELECT 1 FROM "nozzle_migrations"
             WHERE "operation_id" = ?3 AND "apply_state" = 'running'
           )
           AND ${activeLeaseSql(4)}`,
      )
      .bind(
        input.decisionChecksum,
        input.proof.fencingToken,
        operationId,
        ...proofBindings(input.proof),
      )
      .run()
    const changed = mutationChanged(result)
    const persisted = await this.load(operationId)
    if (!persisted) return migrationError("The migration disappeared after resume authorization.")
    if (
      persisted.resume?.decisionChecksum !== next.resume?.decisionChecksum ||
      persisted.resume?.fencingToken !== next.resume?.fencingToken
    ) {
      if (!changed)
        return resume("Migration resume lost its lease or fenced-decision precondition.")
      return migrationError("Control D1 did not persist the expected migration resume decision.")
    }
    return persisted
  }

  async activate(
    operationId: string,
    compatibility: MigrationCompatibility,
    proof: LeaseProof,
  ): Promise<boolean> {
    const operation = await this.load(operationId)
    if (!operation || !migrationSucceeded(operation, compatibility)) return false
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_migration_operations"
         SET "state" = 'succeeded', "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "operation_id" = ?1
           AND ("halt_control_sequence" IS NULL OR "resume_decision_checksum" IS NOT NULL)
           AND ${activeLeaseSql(2)}
           AND NOT EXISTS (
             SELECT 1 FROM "nozzle_migrations"
             WHERE "operation_id" = ?1
               AND ("apply_state" <> 'applied' OR "verification_state" <> 'verified'
                 OR "ledger_checksum" <> ?6 OR "canonical_schema_checksum" <> ?7)
           )`,
      )
      .bind(
        operationId,
        ...proofBindings(proof),
        operation.artifactChecksum,
        operation.targetSchemaChecksum,
      )
      .run()
    const changed = mutationChanged(result)
    const persisted = await this.#database
      .prepare(`SELECT "state" FROM "nozzle_migration_operations" WHERE "operation_id" = ?1`)
      .bind(operationId)
      .first<{ readonly state: string }>()
    if (persisted?.state === "succeeded") return true
    if (!changed) return false
    return migrationError("Control D1 did not persist migration activation.")
  }
}
