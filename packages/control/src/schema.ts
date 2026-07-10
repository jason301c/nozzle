export const CONTROL_SCHEMA_VERSION = 1 as const

export const CONTROL_TABLE_NAMES = Object.freeze([
  "nozzle_audit_log",
  "nozzle_backups",
  "nozzle_buckets",
  "nozzle_capacity_samples",
  "nozzle_config_versions",
  "nozzle_controllers",
  "nozzle_fleets",
  "nozzle_idempotency_keys",
  "nozzle_leases",
  "nozzle_migrations",
  "nozzle_operation_steps",
  "nozzle_operations",
  "nozzle_placement_constraints",
  "nozzle_route_overrides",
  "nozzle_route_versions",
  "nozzle_schema_artifacts",
  "nozzle_schema_versions",
  "nozzle_shards",
  "nozzle_topology_versions",
] as const)

export const CONTROL_SCHEMA_STATEMENTS = Object.freeze([
  `CREATE TABLE IF NOT EXISTS "nozzle_control_meta" (
  "schema_version" INTEGER PRIMARY KEY NOT NULL CHECK ("schema_version" = 1),
  "installed_at_ms" INTEGER NOT NULL CHECK ("installed_at_ms" >= 0)
);`,
  `INSERT INTO "nozzle_control_meta" ("schema_version", "installed_at_ms")
VALUES (1, CAST(unixepoch('subsec') * 1000 AS INTEGER))
ON CONFLICT ("schema_version") DO NOTHING;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_fleets" (
  "fleet_id" TEXT PRIMARY KEY NOT NULL CHECK (length(trim("fleet_id")) BETWEEN 1 AND 255),
  "account_id_checksum" TEXT NOT NULL CHECK (length(trim("account_id_checksum")) > 0),
  "environment_id" TEXT NOT NULL CHECK (length(trim("environment_id")) BETWEEN 1 AND 255),
  "bucket_bits" INTEGER NOT NULL CHECK ("bucket_bits" IN (16, 20)),
  "hash_version" INTEGER NOT NULL CHECK ("hash_version" = 1),
  "fleet_seed" TEXT NOT NULL CHECK (length("fleet_seed") = 43 AND "fleet_seed" NOT GLOB '*[^A-Za-z0-9_-]*'),
  "state" TEXT NOT NULL CHECK ("state" IN ('active', 'mixed_blocked', 'quarantined', 'retired')),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  UNIQUE ("account_id_checksum", "environment_id", "fleet_id")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_config_versions" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "version" INTEGER NOT NULL CHECK ("version" >= 1),
  "config_checksum" TEXT NOT NULL CHECK (length(trim("config_checksum")) > 0),
  "config_json" TEXT NOT NULL CHECK (json_valid("config_json")),
  "published_at_ms" INTEGER NOT NULL CHECK ("published_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "version")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_topology_versions" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "version" INTEGER NOT NULL CHECK ("version" >= 1),
  "manifest_checksum" TEXT NOT NULL CHECK (length(trim("manifest_checksum")) > 0),
  "manifest" BLOB NOT NULL CHECK (typeof("manifest") = 'blob'),
  "published_at_ms" INTEGER NOT NULL CHECK ("published_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "version")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_shards" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "shard_id" TEXT NOT NULL CHECK (length(trim("shard_id")) BETWEEN 1 AND 255),
  "database_id" TEXT NOT NULL CHECK (length(trim("database_id")) > 0),
  "database_name" TEXT NOT NULL CHECK (length(trim("database_name")) > 0),
  "jurisdiction" TEXT NOT NULL CHECK (length(trim("jurisdiction")) > 0),
  "location_hint" TEXT,
  "desired_state" TEXT NOT NULL,
  "recorded_state" TEXT NOT NULL,
  "observed_state" TEXT NOT NULL,
  "schema_version" INTEGER NOT NULL CHECK ("schema_version" >= 0),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "shard_id"),
  UNIQUE ("fleet_id", "database_id"),
  UNIQUE ("fleet_id", "database_name")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_buckets" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "bucket_id" INTEGER NOT NULL CHECK ("bucket_id" BETWEEN 0 AND 4294967295),
  "shard_id" TEXT NOT NULL,
  "route_epoch" INTEGER NOT NULL CHECK ("route_epoch" >= 1),
  "ownership_state" TEXT NOT NULL,
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "bucket_id"),
  FOREIGN KEY ("fleet_id", "shard_id") REFERENCES "nozzle_shards" ("fleet_id", "shard_id")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_route_versions" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "version" INTEGER NOT NULL CHECK ("version" >= 1),
  "topology_version" INTEGER NOT NULL CHECK ("topology_version" >= 1),
  "route_checksum" TEXT NOT NULL CHECK (length(trim("route_checksum")) > 0),
  "published_at_ms" INTEGER NOT NULL CHECK ("published_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "version")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_route_overrides" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "hash_version" INTEGER NOT NULL CHECK ("hash_version" = 1),
  "partition_digest" TEXT NOT NULL CHECK (length("partition_digest") = 64),
  "reserved_bucket_id" INTEGER NOT NULL CHECK ("reserved_bucket_id" BETWEEN 0 AND 4294967295),
  "route_version" INTEGER NOT NULL CHECK ("route_version" >= 1),
  PRIMARY KEY ("fleet_id", "route_version", "hash_version", "partition_digest")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_placement_constraints" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "constraint_id" TEXT NOT NULL CHECK (length(trim("constraint_id")) > 0),
  "constraint_json" TEXT NOT NULL CHECK (json_valid("constraint_json")),
  "active" INTEGER NOT NULL CHECK ("active" IN (0, 1)),
  PRIMARY KEY ("fleet_id", "constraint_id")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_schema_artifacts" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "artifact_checksum" TEXT NOT NULL CHECK (length(trim("artifact_checksum")) > 0),
  "parent_version" INTEGER NOT NULL CHECK ("parent_version" >= 0),
  "result_version" INTEGER NOT NULL CHECK ("result_version" >= 1),
  "classification" TEXT NOT NULL,
  "artifact_json" TEXT NOT NULL CHECK (json_valid("artifact_json")),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  PRIMARY KEY ("fleet_id", "artifact_checksum")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_schema_versions" (
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "version" INTEGER NOT NULL CHECK ("version" >= 0),
  "schema_checksum" TEXT NOT NULL CHECK (length(trim("schema_checksum")) > 0),
  "state" TEXT NOT NULL CHECK ("state" IN ('applied', 'active', 'mixed_blocked', 'retired')),
  "activated_at_ms" INTEGER,
  PRIMARY KEY ("fleet_id", "version")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_migrations" (
  "operation_id" TEXT NOT NULL,
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "shard_id" TEXT NOT NULL,
  "artifact_checksum" TEXT NOT NULL,
  "apply_state" TEXT NOT NULL CHECK ("apply_state" IN ('pending', 'running', 'applied', 'retryable_failed', 'blocked_failed', 'unknown')),
  "verification_state" TEXT NOT NULL CHECK ("verification_state" IN ('pending', 'verified', 'failed', 'unknown')),
  "attempts" INTEGER NOT NULL CHECK ("attempts" >= 0),
  "ledger_checksum" TEXT,
  "canonical_schema_checksum" TEXT,
  "error_checksum" TEXT,
  "failure_fencing_token" INTEGER,
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  CHECK ("failure_fencing_token" IS NULL OR "failure_fencing_token" >= 1),
  PRIMARY KEY ("operation_id", "shard_id")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_migration_operations" (
  "operation_id" TEXT PRIMARY KEY NOT NULL,
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "artifact_checksum" TEXT NOT NULL,
  "target_schema_checksum" TEXT NOT NULL,
  "required_shards_json" TEXT NOT NULL CHECK (json_valid("required_shards_json")),
  "halt_control_sequence" INTEGER,
  "halt_fencing_token" INTEGER,
  "halt_failed_shard_id" TEXT,
  "resume_decision_checksum" TEXT,
  "resume_fencing_token" INTEGER,
  "state" TEXT NOT NULL CHECK ("state" IN ('running', 'mixed_blocked', 'succeeded')),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  CHECK (("halt_control_sequence" IS NULL) = ("halt_fencing_token" IS NULL)),
  CHECK (("halt_control_sequence" IS NULL) = ("halt_failed_shard_id" IS NULL)),
  CHECK (("resume_decision_checksum" IS NULL) = ("resume_fencing_token" IS NULL)),
  CHECK ("halt_control_sequence" IS NULL OR "halt_control_sequence" >= 1),
  CHECK ("halt_fencing_token" IS NULL OR "halt_fencing_token" >= 1),
  CHECK ("resume_fencing_token" IS NULL OR "resume_fencing_token" > "halt_fencing_token")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_control_sequence" (
  "singleton" INTEGER PRIMARY KEY NOT NULL CHECK ("singleton" = 1),
  "sequence" INTEGER NOT NULL CHECK ("sequence" >= 0)
);`,
  `INSERT INTO "nozzle_control_sequence" ("singleton", "sequence") VALUES (1, 0)
ON CONFLICT ("singleton") DO NOTHING;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_movement_operations" (
  "operation_id" TEXT PRIMARY KEY NOT NULL,
  "fleet_id" TEXT NOT NULL REFERENCES "nozzle_fleets" ("fleet_id"),
  "partition_digest" TEXT NOT NULL,
  "source_shard_id" TEXT NOT NULL,
  "destination_shard_id" TEXT NOT NULL,
  "source_route_epoch" INTEGER NOT NULL CHECK ("source_route_epoch" >= 0),
  "target_route_epoch" INTEGER NOT NULL CHECK ("target_route_epoch" = "source_route_epoch" + 1),
  "required_tables_json" TEXT NOT NULL CHECK (json_valid("required_tables_json")),
  "phase" TEXT NOT NULL CHECK ("phase" IN ('planned', 'capturing', 'copying', 'replaying', 'source_read_only', 'tail_drained', 'destination_writable', 'route_published', 'verified', 'quarantined', 'cleanup_authorized', 'completed', 'rollback_pending', 'rolled_back')),
  "state_json" TEXT NOT NULL CHECK (json_valid("state_json")),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= "created_at_ms")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_operations" (
  "operation_id" TEXT PRIMARY KEY NOT NULL CHECK (length(trim("operation_id")) > 0),
  "environment_id" TEXT NOT NULL CHECK (length(trim("environment_id")) > 0),
  "operation_type" TEXT NOT NULL CHECK (length(trim("operation_type")) > 0),
  "idempotency_scope" TEXT NOT NULL CHECK (length(trim("idempotency_scope")) > 0),
  "idempotency_key" TEXT NOT NULL CHECK (length(trim("idempotency_key")) > 0),
  "input_checksum" TEXT NOT NULL CHECK (length(trim("input_checksum")) > 0),
  "plan_checksum" TEXT NOT NULL CHECK (length(trim("plan_checksum")) > 0),
  "plan_json" TEXT NOT NULL CHECK (json_valid("plan_json")),
  "capability_snapshot_checksum" TEXT NOT NULL,
  "required_shards_json" TEXT NOT NULL CHECK (json_valid("required_shards_json")),
  "status" TEXT NOT NULL CHECK ("status" IN ('planned', 'running', 'paused', 'reconciling', 'failed', 'intervention_required', 'succeeded')),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  UNIQUE ("environment_id", "idempotency_scope", "idempotency_key")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_operation_steps" (
  "operation_id" TEXT NOT NULL REFERENCES "nozzle_operations" ("operation_id"),
  "step_id" TEXT NOT NULL CHECK (length(trim("step_id")) > 0),
  "idempotency_key" TEXT NOT NULL,
  "lease_key" TEXT NOT NULL,
  "plan_json" TEXT NOT NULL CHECK (json_valid("plan_json")),
  "record_json" TEXT NOT NULL CHECK (json_valid("record_json")),
  "state" TEXT NOT NULL CHECK ("state" IN ('pending', 'running', 'retryable_failed', 'unknown', 'succeeded', 'failed', 'intervention_required')),
  "fencing_token" INTEGER,
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  PRIMARY KEY ("operation_id", "step_id"),
  UNIQUE ("operation_id", "idempotency_key")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_leases" (
  "lease_key" TEXT PRIMARY KEY NOT NULL CHECK (length(trim("lease_key")) BETWEEN 1 AND 512),
  "holder_id" TEXT CHECK ("holder_id" IS NULL OR length(trim("holder_id")) BETWEEN 1 AND 512),
  "acquisition_id" TEXT CHECK ("acquisition_id" IS NULL OR length(trim("acquisition_id")) BETWEEN 1 AND 512),
  "fencing_token" INTEGER NOT NULL CHECK ("fencing_token" >= 1),
  "expires_at_ms" INTEGER NOT NULL CHECK ("expires_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0),
  CHECK (("holder_id" IS NULL) = ("acquisition_id" IS NULL))
);`,
  `CREATE UNIQUE INDEX IF NOT EXISTS "nozzle_leases_acquisition" ON "nozzle_leases" ("acquisition_id") WHERE "acquisition_id" IS NOT NULL;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_idempotency_keys" (
  "environment_id" TEXT NOT NULL,
  "scope" TEXT NOT NULL,
  "idempotency_key" TEXT NOT NULL,
  "operation_id" TEXT NOT NULL REFERENCES "nozzle_operations" ("operation_id"),
  "input_checksum" TEXT NOT NULL,
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  PRIMARY KEY ("environment_id", "scope", "idempotency_key")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_capacity_samples" (
  "fleet_id" TEXT NOT NULL,
  "shard_id" TEXT NOT NULL,
  "sampled_at_ms" INTEGER NOT NULL CHECK ("sampled_at_ms" >= 0),
  "sample_json" TEXT NOT NULL CHECK (json_valid("sample_json")),
  PRIMARY KEY ("fleet_id", "shard_id", "sampled_at_ms")
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_backups" (
  "backup_id" TEXT PRIMARY KEY NOT NULL,
  "fleet_id" TEXT NOT NULL,
  "shard_id" TEXT NOT NULL,
  "bookmark" TEXT NOT NULL,
  "jurisdiction" TEXT NOT NULL,
  "state" TEXT NOT NULL,
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "verified_at_ms" INTEGER
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_controllers" (
  "controller_id" TEXT PRIMARY KEY NOT NULL,
  "protocol_version" INTEGER NOT NULL CHECK ("protocol_version" >= 1),
  "schema_min" INTEGER NOT NULL CHECK ("schema_min" >= 0),
  "schema_max" INTEGER NOT NULL CHECK ("schema_max" >= "schema_min"),
  "topology_version" INTEGER NOT NULL CHECK ("topology_version" >= 0),
  "reachability_checksum" TEXT NOT NULL,
  "last_seen_at_ms" INTEGER NOT NULL CHECK ("last_seen_at_ms" >= 0)
);`,
  `CREATE TABLE IF NOT EXISTS "nozzle_audit_log" (
  "environment_id" TEXT NOT NULL,
  "sequence" INTEGER NOT NULL CHECK ("sequence" >= 1),
  "previous_hash" TEXT,
  "event_hash" TEXT NOT NULL,
  "server_time_ms" INTEGER NOT NULL CHECK ("server_time_ms" >= 0),
  "operation_id" TEXT NOT NULL,
  "step_id" TEXT,
  "event_json" TEXT NOT NULL CHECK (json_valid("event_json"))
    CHECK (json_extract("event_json", '$.schemaVersion') = 1)
    CHECK (json_extract("event_json", '$.sequence') = "sequence")
    CHECK (json_extract("event_json", '$.previousHash') IS "previous_hash")
    CHECK (json_extract("event_json", '$.eventHash') = "event_hash")
    CHECK (json_extract("event_json", '$.serverTimeMs') = "server_time_ms")
    CHECK (json_extract("event_json", '$.environmentId') = "environment_id")
    CHECK (json_extract("event_json", '$.operationId') = "operation_id")
    CHECK (json_extract("event_json", '$.stepId') IS "step_id"),
  PRIMARY KEY ("environment_id", "sequence"),
  UNIQUE ("environment_id", "event_hash")
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_config_update" BEFORE UPDATE ON "nozzle_config_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_CONFIG'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_config_delete" BEFORE DELETE ON "nozzle_config_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_CONFIG'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_topology_update" BEFORE UPDATE ON "nozzle_topology_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_TOPOLOGY'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_topology_delete" BEFORE DELETE ON "nozzle_topology_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_TOPOLOGY'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_route_update" BEFORE UPDATE ON "nozzle_route_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_ROUTE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_route_delete" BEFORE DELETE ON "nozzle_route_versions" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_ROUTE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_override_update" BEFORE UPDATE ON "nozzle_route_overrides" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_OVERRIDE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_override_delete" BEFORE DELETE ON "nozzle_route_overrides" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_OVERRIDE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_artifact_update" BEFORE UPDATE ON "nozzle_schema_artifacts" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_ARTIFACT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_immutable_artifact_delete" BEFORE DELETE ON "nozzle_schema_artifacts" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_ARTIFACT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_operation_plan_update"
BEFORE UPDATE ON "nozzle_operations"
WHEN NEW."operation_id" IS NOT OLD."operation_id"
  OR NEW."environment_id" IS NOT OLD."environment_id"
  OR NEW."operation_type" IS NOT OLD."operation_type"
  OR NEW."idempotency_scope" IS NOT OLD."idempotency_scope"
  OR NEW."idempotency_key" IS NOT OLD."idempotency_key"
  OR NEW."input_checksum" IS NOT OLD."input_checksum"
  OR NEW."plan_checksum" IS NOT OLD."plan_checksum"
  OR NEW."plan_json" IS NOT OLD."plan_json"
  OR NEW."capability_snapshot_checksum" IS NOT OLD."capability_snapshot_checksum"
  OR NEW."required_shards_json" IS NOT OLD."required_shards_json"
  OR NEW."created_at_ms" IS NOT OLD."created_at_ms"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_OPERATION_PLAN'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_operation_delete" BEFORE DELETE ON "nozzle_operations" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_OPERATION_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_step_plan_update"
BEFORE UPDATE ON "nozzle_operation_steps"
WHEN NEW."operation_id" IS NOT OLD."operation_id"
  OR NEW."step_id" IS NOT OLD."step_id"
  OR NEW."idempotency_key" IS NOT OLD."idempotency_key"
  OR NEW."lease_key" IS NOT OLD."lease_key"
  OR NEW."plan_json" IS NOT OLD."plan_json"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_STEP_PLAN'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_step_delete" BEFORE DELETE ON "nozzle_operation_steps" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_STEP_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_idempotency_insert"
BEFORE INSERT ON "nozzle_idempotency_keys"
WHEN NOT EXISTS (
  SELECT 1 FROM "nozzle_operations"
  WHERE "operation_id" = NEW."operation_id"
    AND "environment_id" = NEW."environment_id"
    AND "idempotency_scope" = NEW."scope"
    AND "idempotency_key" = NEW."idempotency_key"
    AND "input_checksum" = NEW."input_checksum"
)
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IDEMPOTENCY_MISMATCH'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_idempotency_update"
BEFORE UPDATE ON "nozzle_idempotency_keys"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IDEMPOTENCY_IMMUTABLE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_idempotency_delete"
BEFORE DELETE ON "nozzle_idempotency_keys"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IDEMPOTENCY_IMMUTABLE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_operation_update"
BEFORE UPDATE ON "nozzle_migration_operations"
WHEN NEW."operation_id" IS NOT OLD."operation_id"
  OR NEW."fleet_id" IS NOT OLD."fleet_id"
  OR NEW."artifact_checksum" IS NOT OLD."artifact_checksum"
  OR NEW."target_schema_checksum" IS NOT OLD."target_schema_checksum"
  OR NEW."required_shards_json" IS NOT OLD."required_shards_json"
  OR NEW."created_at_ms" IS NOT OLD."created_at_ms"
  OR (OLD."halt_control_sequence" IS NOT NULL AND (
    NEW."halt_control_sequence" IS NOT OLD."halt_control_sequence"
    OR NEW."halt_fencing_token" IS NOT OLD."halt_fencing_token"
    OR NEW."halt_failed_shard_id" IS NOT OLD."halt_failed_shard_id"
  ))
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_MIGRATION_OPERATION'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_operation_delete" BEFORE DELETE ON "nozzle_migration_operations" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_MIGRATION_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_delete" BEFORE DELETE ON "nozzle_migrations" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_MIGRATION_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_halt"
AFTER UPDATE OF "apply_state" ON "nozzle_migrations"
WHEN NEW."apply_state" IN ('retryable_failed', 'blocked_failed', 'unknown')
  AND OLD."apply_state" NOT IN ('retryable_failed', 'blocked_failed', 'unknown')
  AND EXISTS (SELECT 1 FROM "nozzle_migration_operations" WHERE "operation_id" = NEW."operation_id" AND "halt_control_sequence" IS NULL)
BEGIN
  UPDATE "nozzle_control_sequence" SET "sequence" = "sequence" + 1 WHERE "singleton" = 1;
  UPDATE "nozzle_migration_operations"
  SET "halt_control_sequence" = (SELECT "sequence" FROM "nozzle_control_sequence" WHERE "singleton" = 1),
      "halt_fencing_token" = NEW."failure_fencing_token",
      "halt_failed_shard_id" = NEW."shard_id",
      "resume_decision_checksum" = NULL,
      "resume_fencing_token" = NULL,
      "state" = 'mixed_blocked',
      "updated_at_ms" = NEW."updated_at_ms"
  WHERE "operation_id" = NEW."operation_id" AND "halt_control_sequence" IS NULL;
  UPDATE "nozzle_fleets"
  SET "state" = 'mixed_blocked'
  WHERE "fleet_id" = (SELECT "fleet_id" FROM "nozzle_migration_operations" WHERE "operation_id" = NEW."operation_id");
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_rehalt"
AFTER UPDATE OF "apply_state" ON "nozzle_migrations"
WHEN NEW."apply_state" IN ('retryable_failed', 'blocked_failed', 'unknown')
  AND EXISTS (SELECT 1 FROM "nozzle_migration_operations" WHERE "operation_id" = NEW."operation_id" AND "resume_decision_checksum" IS NOT NULL)
BEGIN
  UPDATE "nozzle_migration_operations"
  SET "resume_decision_checksum" = NULL,
      "resume_fencing_token" = NULL,
      "state" = 'mixed_blocked',
      "updated_at_ms" = NEW."updated_at_ms"
  WHERE "operation_id" = NEW."operation_id";
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_migration_succeeded"
AFTER UPDATE OF "state" ON "nozzle_migration_operations"
WHEN NEW."state" = 'succeeded' AND OLD."state" <> 'succeeded'
BEGIN
  UPDATE "nozzle_fleets" SET "state" = 'active' WHERE "fleet_id" = NEW."fleet_id";
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_movement_plan_update"
BEFORE UPDATE ON "nozzle_movement_operations"
WHEN NEW."operation_id" IS NOT OLD."operation_id"
  OR NEW."fleet_id" IS NOT OLD."fleet_id"
  OR NEW."partition_digest" IS NOT OLD."partition_digest"
  OR NEW."source_shard_id" IS NOT OLD."source_shard_id"
  OR NEW."destination_shard_id" IS NOT OLD."destination_shard_id"
  OR NEW."source_route_epoch" IS NOT OLD."source_route_epoch"
  OR NEW."target_route_epoch" IS NOT OLD."target_route_epoch"
  OR NEW."required_tables_json" IS NOT OLD."required_tables_json"
  OR NEW."created_at_ms" IS NOT OLD."created_at_ms"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_IMMUTABLE_MOVEMENT_PLAN'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_movement_delete"
BEFORE DELETE ON "nozzle_movement_operations"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_MOVEMENT_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_lease_update"
BEFORE UPDATE ON "nozzle_leases"
BEGIN
  SELECT CASE
    WHEN NEW."lease_key" IS NOT OLD."lease_key" THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_KEY_IMMUTABLE')
    WHEN NEW."fencing_token" < OLD."fencing_token" THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_TOKEN_ROLLBACK')
    WHEN NEW."holder_id" IS OLD."holder_id" AND NEW."acquisition_id" IS OLD."acquisition_id"
      AND NEW."fencing_token" IS NOT OLD."fencing_token"
      THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_NOT_FENCED')
    WHEN NEW."holder_id" IS NULL AND NEW."fencing_token" IS NOT OLD."fencing_token"
      THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_NOT_FENCED')
    WHEN (NEW."holder_id" IS NOT OLD."holder_id" OR NEW."acquisition_id" IS NOT OLD."acquisition_id")
      AND NEW."holder_id" IS NOT NULL AND NEW."fencing_token" IS NOT OLD."fencing_token" + 1
      THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_NOT_FENCED')
    WHEN (NEW."holder_id" IS NOT OLD."holder_id" OR NEW."acquisition_id" IS NOT OLD."acquisition_id")
      AND NEW."holder_id" IS NOT NULL AND OLD."holder_id" IS NOT NULL
      AND OLD."expires_at_ms" > CAST(unixepoch('subsec') * 1000 AS INTEGER)
      THEN RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_ACTIVE')
  END;
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_lease_delete" BEFORE DELETE ON "nozzle_leases" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_LEASE_PERSISTENT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_audit_append_guard"
BEFORE INSERT ON "nozzle_audit_log"
BEGIN
  SELECT CASE
    WHEN NEW."sequence" IS NOT COALESCE((
      SELECT "sequence" + 1 FROM "nozzle_audit_log"
      WHERE "environment_id" = NEW."environment_id"
      ORDER BY "sequence" DESC LIMIT 1
    ), 1) THEN RAISE(ABORT, 'NOZZLE_CONTROL_AUDIT_SEQUENCE')
    WHEN NEW."previous_hash" IS NOT (
      SELECT "event_hash" FROM "nozzle_audit_log"
      WHERE "environment_id" = NEW."environment_id"
      ORDER BY "sequence" DESC LIMIT 1
    ) THEN RAISE(ABORT, 'NOZZLE_CONTROL_AUDIT_PREVIOUS_HASH')
    WHEN NEW."server_time_ms" < COALESCE((
      SELECT "server_time_ms" FROM "nozzle_audit_log"
      WHERE "environment_id" = NEW."environment_id"
      ORDER BY "sequence" DESC LIMIT 1
    ), 0) THEN RAISE(ABORT, 'NOZZLE_CONTROL_AUDIT_TIME_ROLLBACK')
  END;
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_audit_update" BEFORE UPDATE ON "nozzle_audit_log" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_AUDIT_APPEND_ONLY'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_control_audit_delete" BEFORE DELETE ON "nozzle_audit_log" BEGIN SELECT RAISE(ABORT, 'NOZZLE_CONTROL_AUDIT_APPEND_ONLY'); END;`,
])

export function controlSchemaSql(): string {
  return `${CONTROL_SCHEMA_STATEMENTS.join("\n\n")}\n`
}
