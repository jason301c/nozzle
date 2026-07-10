import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import {
  CONTROL_SCHEMA_STATEMENTS,
  CONTROL_SCHEMA_VERSION,
  CONTROL_TABLE_NAMES,
  controlSchemaSql,
} from "../src/schema.js"

function withDatabase(run: (database: DatabaseSync) => void): void {
  const database = new DatabaseSync(":memory:")
  database.exec("PRAGMA foreign_keys = ON;")
  try {
    database.exec(controlSchemaSql())
    run(database)
  } finally {
    database.close()
  }
}

function seedFleet(database: DatabaseSync): void {
  database
    .prepare(
      `INSERT INTO "nozzle_fleets"
       ("fleet_id", "account_id_checksum", "environment_id", "bucket_bits", "hash_version",
        "fleet_seed", "state", "created_at_ms")
       VALUES (?, ?, ?, 16, 1, ?, 'active', 1)`,
    )
    .run("fleet-a", "account-checksum", "production", "a".repeat(43))
}

function auditEvent(input: {
  readonly environmentId?: string
  readonly eventHash: string
  readonly previousHash: string | null
  readonly sequence: number
  readonly serverTimeMs: number
}): string {
  return JSON.stringify({
    actorChecksum: "actor",
    environmentId: input.environmentId ?? "production",
    eventHash: input.eventHash,
    eventType: "test-event",
    fencingToken: null,
    idempotencyKey: `audit-${input.eventHash}`,
    operationId: "operation-a",
    payloadChecksum: "payload",
    previousHash: input.previousHash,
    schemaVersion: 1,
    sequence: input.sequence,
    serverTimeMs: input.serverTimeMs,
    stepId: null,
  })
}

describe("control D1 schema", () => {
  it("emits one deterministic install artifact with every required ledger table", () => {
    const sql = controlSchemaSql()
    expect(sql).toBe(`${CONTROL_SCHEMA_STATEMENTS.join("\n\n")}\n`)
    expect(CONTROL_SCHEMA_VERSION).toBe(1)
    expect(Object.isFrozen(CONTROL_SCHEMA_STATEMENTS)).toBe(true)
    expect(Object.isFrozen(CONTROL_TABLE_NAMES)).toBe(true)
    expect(sql).not.toContain("fictional-secret")

    withDatabase((database) => {
      const tables = database
        .prepare(
          `SELECT "name" FROM "sqlite_schema"
           WHERE "type" = 'table' AND "name" LIKE 'nozzle_%'
           ORDER BY "name"`,
        )
        .all()
        .map((row) => (row as { name: string }).name)
      expect(tables).toEqual(
        [
          "nozzle_control_meta",
          "nozzle_control_sequence",
          "nozzle_migration_operations",
          "nozzle_movement_operations",
          ...CONTROL_TABLE_NAMES,
        ].sort(),
      )
      expect(database.prepare('SELECT "schema_version" FROM "nozzle_control_meta"').get()).toEqual({
        schema_version: 1,
      })
    })
  })

  it("makes published configuration, topology, and route versions immutable", () => {
    withDatabase((database) => {
      seedFleet(database)
      database
        .prepare(
          `INSERT INTO "nozzle_config_versions"
           ("fleet_id", "version", "config_checksum", "config_json", "published_at_ms")
           VALUES ('fleet-a', 1, 'config-a', '{}', 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_topology_versions"
           ("fleet_id", "version", "manifest_checksum", "manifest", "published_at_ms")
           VALUES ('fleet-a', 1, 'topology-a', X'00', 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_route_versions"
           ("fleet_id", "version", "topology_version", "route_checksum", "published_at_ms")
           VALUES ('fleet-a', 1, 1, 'route-a', 1)`,
        )
        .run()

      for (const [table, error] of [
        ["nozzle_config_versions", "NOZZLE_CONTROL_IMMUTABLE_CONFIG"],
        ["nozzle_topology_versions", "NOZZLE_CONTROL_IMMUTABLE_TOPOLOGY"],
        ["nozzle_route_versions", "NOZZLE_CONTROL_IMMUTABLE_ROUTE"],
      ] as const) {
        expect(() => database.prepare(`UPDATE "${table}" SET "version" = 2`).run()).toThrow(error)
        expect(() => database.prepare(`DELETE FROM "${table}"`).run()).toThrow(error)
      }
    })
  })

  it("protects immutable operation plans while allowing status progress", () => {
    withDatabase((database) => {
      database
        .prepare(
          `INSERT INTO "nozzle_operations"
           ("operation_id", "environment_id", "operation_type", "idempotency_scope",
            "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
            "capability_snapshot_checksum", "capability_snapshot_json", "required_shards_json",
            "status", "created_at_ms", "updated_at_ms")
           VALUES ('operation-a', 'production', 'migration', 'fleet-a', 'idempotency-a',
             'input-a', '{}', 'plan-a', '{}', 'capability-a', '{}', '[]', 'planned', 1, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_steps"
           ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json", "record_json",
            "state", "fencing_token", "updated_at_ms")
           VALUES ('operation-a', 'step-a', 'step-idempotency-a', 'lease-a', '{}', '{}',
             'pending', NULL, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_leases"
           ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
            "updated_at_ms")
           VALUES ('lease-a', 'holder-a', 'acquisition-a', 1, 9000000000000000, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_transitions"
           ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
            "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
            "lease_key", "holder_id", "acquisition_id", "created_at_ms")
           VALUES ('transition-a', 'operation-a', 'step-a', '{}', '{}', 'planned', 'running',
             'transition-audit', 1, 'lease-a', 'holder-a', 'acquisition-a', 1)`,
        )
        .run()

      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operations" SET "status" = 'running', "updated_at_ms" = 2
             WHERE "operation_id" = 'operation-a'`,
          )
          .run(),
      ).not.toThrow()
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operations" SET "status" = 'paused', "updated_at_ms" = 3
             WHERE "operation_id" = 'operation-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_OPERATION_TRANSITION_REQUIRED")
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operation_steps"
             SET "record_json" = '{"state":"running","startedAttempts":1}',
                 "state" = 'running', "fencing_token" = 1
             WHERE "operation_id" = 'operation-a' AND "step_id" = 'step-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_STEP_TRANSITION_REQUIRED")
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operations" SET "plan_checksum" = 'rewritten'
             WHERE "operation_id" = 'operation-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_IMMUTABLE_OPERATION_PLAN")
      for (const column of ["input_json", "capability_snapshot_json"]) {
        expect(() =>
          database
            .prepare(
              `UPDATE "nozzle_operations" SET "${column}" = '{"rewritten":true}'
               WHERE "operation_id" = 'operation-a'`,
            )
            .run(),
        ).toThrow("NOZZLE_CONTROL_IMMUTABLE_OPERATION_PLAN")
      }
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operation_steps" SET "lease_key" = 'other'
             WHERE "operation_id" = 'operation-a' AND "step_id" = 'step-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_IMMUTABLE_STEP_PLAN")
      expect(() =>
        database
          .prepare(
            `DELETE FROM "nozzle_operation_steps"
             WHERE "operation_id" = 'operation-a' AND "step_id" = 'step-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_STEP_PERSISTENT")
      expect(() =>
        database
          .prepare(`UPDATE "nozzle_operation_transitions" SET "audit_event_hash" = 'rewritten'`)
          .run(),
      ).toThrow("NOZZLE_CONTROL_OPERATION_TRANSITION_IMMUTABLE")
      expect(() => database.prepare(`DELETE FROM "nozzle_operation_transitions"`).run()).toThrow(
        "NOZZLE_CONTROL_OPERATION_TRANSITION_IMMUTABLE",
      )
      expect(() =>
        database
          .prepare(`DELETE FROM "nozzle_operations" WHERE "operation_id" = 'operation-a'`)
          .run(),
      ).toThrow("NOZZLE_CONTROL_OPERATION_PERSISTENT")

      database
        .prepare(
          `INSERT INTO "nozzle_idempotency_keys"
           ("environment_id", "scope", "idempotency_key", "operation_id", "input_checksum",
            "created_at_ms")
           VALUES ('production', 'fleet-a', 'idempotency-a', 'operation-a', 'input-a', 1)`,
        )
        .run()
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_idempotency_keys"
             ("environment_id", "scope", "idempotency_key", "operation_id", "input_checksum",
              "created_at_ms")
             VALUES ('production', 'wrong', 'wrong', 'operation-a', 'wrong', 1)`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_IDEMPOTENCY_MISMATCH")
      expect(() =>
        database.prepare(`UPDATE "nozzle_idempotency_keys" SET "input_checksum" = 'wrong'`).run(),
      ).toThrow("NOZZLE_CONTROL_IDEMPOTENCY_IMMUTABLE")
      expect(() => database.prepare(`DELETE FROM "nozzle_idempotency_keys"`).run()).toThrow(
        "NOZZLE_CONTROL_IDEMPOTENCY_IMMUTABLE",
      )
    })
  })

  it("requires fenced operation effects and append-only receipts for saga state", () => {
    withDatabase((database) => {
      const descriptorChecksum = "d".repeat(64)
      const record = (stateVersion: number, status: "planned" | "running" | "succeeded") =>
        JSON.stringify({
          deadlineAtMs: 10_000,
          descriptor: {
            descriptorChecksum,
            descriptorId: "transfer",
            version: 1,
          },
          idempotencyKey: "saga-key",
          inputChecksum: "saga-input",
          sagaId: "saga-a",
          stateVersion,
          status,
          steps: {
            a: {
              compensation: { state: "pending" },
              forward:
                status === "planned"
                  ? { state: "pending" }
                  : status === "running"
                    ? { activeAttemptId: "attempt-a", state: "running" }
                    : { resultChecksum: "action", state: "succeeded" },
            },
          },
          terminationCause: null,
          terminationRequestedAtMs: null,
        })
      database
        .prepare(
          `INSERT INTO "nozzle_operations"
           ("operation_id", "environment_id", "operation_type", "idempotency_scope",
            "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
            "capability_snapshot_checksum", "capability_snapshot_json", "required_shards_json",
            "status", "created_at_ms", "updated_at_ms")
           VALUES ('operation-saga', 'production', 'saga:transfer.v1', 'saga', 'saga-key',
             'saga-input', '{}', 'plan', '{}', 'capability', '{}', '[]', 'running', 1, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_steps"
           ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
            "record_json", "state", "fencing_token", "updated_at_ms")
           VALUES
             ('operation-saga', 'saga:init', 'init-key', 'saga:lease', '{}',
              '{"state":"pending"}', 'pending', NULL, 1),
             ('operation-saga', 'saga:a:forward', 'action-key', 'saga:lease', '{}',
              '{"activeAttemptId":"attempt-a","state":"running"}', 'running', 1, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_leases"
           ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
            "updated_at_ms")
           VALUES ('saga:lease', 'controller', 'acquisition', 1, 9000000000000000, 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_transitions"
           ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
            "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
            "lease_key", "holder_id", "acquisition_id", "created_at_ms")
           VALUES ('transition-init', 'operation-saga', 'saga:init', '{"state":"pending"}',
             '{"resultChecksum":"init","state":"succeeded"}', 'running', 'running',
             'audit-init', 1, 'saga:lease', 'controller', 'acquisition', 1)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_effects"
           ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
            "resource_id", "effect_kind", "from_state_version", "to_state_version",
            "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
            "acquisition_id", "fencing_token", "created_at_ms")
           VALUES ('effect-saga-0', 'transition-init', 'operation-saga', 'saga:init', 'saga',
             'saga-a', 'create', NULL, 0, 'evidence-0', 'record-0', ?, 'saga:lease',
             'controller', 'acquisition', 1, 1)`,
        )
        .run(record(0, "planned"))
      database
        .prepare(
          `INSERT INTO "nozzle_sagas"
           ("saga_id", "operation_id", "descriptor_id", "descriptor_version",
            "descriptor_checksum", "descriptor_json", "idempotency_key", "input_checksum",
            "deadline_at_ms", "status", "commitment", "termination_cause",
            "termination_requested_at_ms", "state_version", "last_evidence_checksum",
            "last_effect_id", "record_checksum", "record_json", "created_at_ms", "updated_at_ms")
           VALUES ('saga-a', 'operation-saga', 'transfer', 1, ?, '{}', 'saga-key', 'saga-input',
             10000, 'planned', 'none', NULL, NULL, 0, 'evidence-0', 'effect-saga-0', 'record-0',
             ?, 1, 1)`,
        )
        .run(descriptorChecksum, record(0, "planned"))

      expect(() =>
        database.prepare(`UPDATE "nozzle_sagas" SET "status" = 'running'`).run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_EFFECT_REQUIRED")
      database
        .prepare(
          `INSERT INTO "nozzle_operation_transitions"
           ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
            "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
            "lease_key", "holder_id", "acquisition_id", "created_at_ms")
           VALUES ('transition-action-accepted', 'operation-saga', 'saga:a:forward',
             '{"activeAttemptId":"attempt-a","state":"running"}',
             '{"activeAttemptId":"attempt-a","lastAttemptId":"attempt-a","state":"running"}',
             'running', 'running', 'audit-action-accepted', 1, 'saga:lease', 'controller',
             'acquisition', 2)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_effects"
           ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
            "resource_id", "effect_kind", "from_state_version", "to_state_version",
            "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
            "acquisition_id", "fencing_token", "created_at_ms")
           VALUES ('effect-saga-running', 'transition-action-accepted', 'operation-saga',
             'saga:a:forward', 'saga', 'saga-a', 'forward:accepted', 0, 1,
             'accepted-evidence', 'record-running', ?, 'saga:lease', 'controller', 'acquisition',
             1, 2)`,
        )
        .run(record(1, "running"))
      database
        .prepare(
          `UPDATE "nozzle_sagas"
           SET "status" = 'running', "commitment" = 'possible', "state_version" = 1,
               "last_evidence_checksum" = 'accepted-evidence',
               "last_effect_id" = 'effect-saga-running', "record_checksum" = 'record-running',
               "record_json" = ?, "updated_at_ms" = 2
           WHERE "saga_id" = 'saga-a'`,
        )
        .run(record(1, "running"))
      database
        .prepare(
          `INSERT INTO "nozzle_saga_action_attempts"
           ("attempt_id", "saga_id", "operation_id", "operation_step_id", "saga_step_id",
            "phase", "purpose", "action_key", "idempotency_key", "input_checksum", "input_json",
            "lease_key", "holder_id", "acquisition_id", "fencing_token", "accepted_at_ms")
           VALUES ('attempt-a', 'saga-a', 'operation-saga', 'saga:a:forward', 'a', 'forward',
             'effect', 'a.forward@1:artifact', 'action-key', 'action-input', '{}', 'saga:lease',
             'controller', 'acquisition', 1, 2)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_saga_action_attempt_outcomes"
           ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
            "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
           VALUES ('attempt-a', 'confirmed', 'evidence', '{}', 'output', '{}', NULL, NULL,
             'outcome', 2)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_transitions"
           ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
            "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
            "lease_key", "holder_id", "acquisition_id", "created_at_ms")
           VALUES ('transition-action', 'operation-saga', 'saga:a:forward',
             '{"activeAttemptId":"attempt-a","state":"running"}',
             '{"resultChecksum":"action","state":"succeeded"}', 'running', 'running',
             'audit-action', 1, 'saga:lease', 'controller', 'acquisition', 2)`,
        )
        .run()
      database
        .prepare(
          `INSERT INTO "nozzle_operation_effects"
           ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
            "resource_id", "effect_kind", "from_state_version", "to_state_version",
            "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
            "acquisition_id", "fencing_token", "created_at_ms")
           VALUES ('effect-saga-1', 'transition-action', 'operation-saga', 'saga:a:forward', 'saga',
             'saga-a', 'forward:succeeded', 1, 2, 'evidence-1', 'record-1', ?, 'saga:lease',
             'controller', 'acquisition', 1, 2)`,
        )
        .run(record(2, "succeeded"))
      database
        .prepare(
          `UPDATE "nozzle_sagas"
           SET "status" = 'succeeded', "commitment" = 'complete', "state_version" = 2,
               "last_evidence_checksum" = 'evidence-1', "last_effect_id" = 'effect-saga-1',
               "record_checksum" = 'record-1', "record_json" = ?, "updated_at_ms" = 2
           WHERE "saga_id" = 'saga-a'`,
        )
        .run(record(2, "succeeded"))
      expect(
        database
          .prepare(
            `SELECT "status", "state_version" FROM "nozzle_sagas" WHERE "saga_id" = 'saga-a'`,
          )
          .get(),
      ).toEqual({ state_version: 2, status: "succeeded" })
      const identityRecord = JSON.stringify({
        ...(JSON.parse(record(3, "succeeded")) as Record<string, unknown>),
        descriptor: {
          descriptorChecksum,
          descriptorId: "other",
          version: 1,
        },
      })
      database
        .prepare(
          `INSERT INTO "nozzle_operation_effects"
           ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
            "resource_id", "effect_kind", "from_state_version", "to_state_version",
            "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
            "acquisition_id", "fencing_token", "created_at_ms")
           VALUES ('effect-identity', 'transition-init', 'operation-saga', 'saga:init', 'saga',
             'saga-a', 'rewrite', 2, 3, 'identity-evidence', 'identity-record', ?, 'saga:lease',
             'controller', 'acquisition', 1, 1)`,
        )
        .run(identityRecord)
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_sagas"
             SET "descriptor_id" = 'other', "state_version" = 3,
                 "last_evidence_checksum" = 'identity-evidence',
                 "last_effect_id" = 'effect-identity', "record_checksum" = 'identity-record',
                 "record_json" = ?, "updated_at_ms" = 2`,
          )
          .run(identityRecord),
      ).toThrow("NOZZLE_CONTROL_SAGA_IDENTITY_IMMUTABLE")
      expect(() => database.prepare(`DELETE FROM "nozzle_sagas"`).run()).toThrow(
        "NOZZLE_CONTROL_SAGA_PERSISTENT",
      )
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_operation_effects"
             ("effect_id", "transition_id", "operation_id", "step_id", "resource_kind",
              "resource_id", "effect_kind", "from_state_version", "to_state_version",
              "evidence_checksum", "record_checksum", "record_json", "lease_key", "holder_id",
              "acquisition_id", "fencing_token", "created_at_ms")
             VALUES ('duplicate-create', 'transition-init', 'operation-saga', 'saga:init', 'saga',
               'saga-a', 'create', NULL, 0, 'evidence', 'record', ?, 'saga:lease', 'controller',
               'acquisition', 1, 1)`,
          )
          .run(record(0, "planned")),
      ).toThrow("NOZZLE_CONTROL_OPERATION_EFFECT_SOURCE_MISMATCH")

      expect(() =>
        database
          .prepare(`UPDATE "nozzle_saga_action_attempts" SET "action_key" = 'rewritten'`)
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_ATTEMPT_IMMUTABLE")
      expect(() => database.prepare(`DELETE FROM "nozzle_saga_action_attempts"`).run()).toThrow(
        "NOZZLE_CONTROL_SAGA_ATTEMPT_PERSISTENT",
      )
      expect(() =>
        database
          .prepare(`UPDATE "nozzle_saga_action_attempt_outcomes" SET "state" = 'unknown'`)
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_IMMUTABLE")
      expect(() =>
        database.prepare(`DELETE FROM "nozzle_saga_action_attempt_outcomes"`).run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_PERSISTENT")
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_saga_action_attempts"
             ("attempt_id", "saga_id", "operation_id", "operation_step_id", "saga_step_id",
              "phase", "purpose", "action_key", "idempotency_key", "input_checksum", "input_json",
              "lease_key", "holder_id", "acquisition_id", "fencing_token", "accepted_at_ms")
             VALUES ('fenced', 'saga-a', 'operation-saga', 'saga:a:forward', 'a', 'forward',
               'effect', 'a.forward@1:artifact', 'action-key', 'action-input', '{}', 'saga:lease',
               'controller', 'acquisition', 2, 1)`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED")
    })
  })

  it("prevents lease rollback, unfenced takeover, deletion, and audit rewriting", () => {
    withDatabase((database) => {
      database
        .prepare(
          `INSERT INTO "nozzle_leases"
           ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms", "updated_at_ms")
           VALUES ('lease-a', 'holder-a', 'acquisition-a', 3, 9000000000000000, 1)`,
        )
        .run()
      expect(() =>
        database.prepare(`UPDATE "nozzle_leases" SET "fencing_token" = 2`).run(),
      ).toThrow("NOZZLE_CONTROL_LEASE_TOKEN_ROLLBACK")
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_leases" SET "holder_id" = 'holder-b', "acquisition_id" = 'acquisition-b'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_LEASE_NOT_FENCED")
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_leases" SET "holder_id" = 'holder-b', "acquisition_id" = 'acquisition-b',
             "fencing_token" = 4`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_LEASE_ACTIVE")
      expect(() => database.prepare(`DELETE FROM "nozzle_leases"`).run()).toThrow(
        "NOZZLE_CONTROL_LEASE_PERSISTENT",
      )
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_leases" SET "holder_id" = NULL, "acquisition_id" = NULL,
             "expires_at_ms" = 2`,
          )
          .run(),
      ).not.toThrow()
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_leases" SET "holder_id" = 'holder-b', "acquisition_id" = 'acquisition-b',
             "fencing_token" = 4, "expires_at_ms" = 200`,
          )
          .run(),
      ).not.toThrow()

      const insertAudit = database.prepare(
        `INSERT INTO "nozzle_audit_log"
         ("environment_id", "sequence", "previous_hash", "event_hash", "server_time_ms",
          "operation_id", "step_id", "event_json")
         VALUES (?, ?, ?, ?, ?, 'operation-a', NULL, ?)`,
      )
      insertAudit.run(
        "production",
        1,
        null,
        "event-a",
        1,
        auditEvent({
          eventHash: "event-a",
          previousHash: null,
          sequence: 1,
          serverTimeMs: 1,
        }),
      )
      insertAudit.run(
        "production",
        2,
        "event-a",
        "event-b",
        2,
        auditEvent({
          eventHash: "event-b",
          previousHash: "event-a",
          sequence: 2,
          serverTimeMs: 2,
        }),
      )
      insertAudit.run(
        "staging",
        1,
        null,
        "event-c",
        1,
        auditEvent({
          environmentId: "staging",
          eventHash: "event-c",
          previousHash: null,
          sequence: 1,
          serverTimeMs: 1,
        }),
      )
      expect(() =>
        insertAudit.run(
          "production",
          4,
          "event-b",
          "bad-sequence",
          3,
          auditEvent({
            eventHash: "bad-sequence",
            previousHash: "event-b",
            sequence: 4,
            serverTimeMs: 3,
          }),
        ),
      ).toThrow("NOZZLE_CONTROL_AUDIT_SEQUENCE")
      expect(() =>
        insertAudit.run(
          "production",
          3,
          "wrong",
          "bad-previous",
          3,
          auditEvent({
            eventHash: "bad-previous",
            previousHash: "wrong",
            sequence: 3,
            serverTimeMs: 3,
          }),
        ),
      ).toThrow("NOZZLE_CONTROL_AUDIT_PREVIOUS_HASH")
      expect(() =>
        insertAudit.run(
          "production",
          3,
          "event-b",
          "bad-time",
          1,
          auditEvent({
            eventHash: "bad-time",
            previousHash: "event-b",
            sequence: 3,
            serverTimeMs: 1,
          }),
        ),
      ).toThrow("NOZZLE_CONTROL_AUDIT_TIME_ROLLBACK")
      const validThird = JSON.parse(
        auditEvent({
          eventHash: "event-third",
          previousHash: "event-b",
          sequence: 3,
          serverTimeMs: 3,
        }),
      ) as Record<string, unknown>
      for (const event of [
        { ...validThird, schemaVersion: 2 },
        { ...validThird, sequence: 4 },
        { ...validThird, previousHash: "wrong" },
        { ...validThird, eventHash: "wrong" },
        { ...validThird, serverTimeMs: 4 },
        { ...validThird, environmentId: "wrong" },
        { ...validThird, operationId: "wrong" },
        { ...validThird, stepId: "wrong" },
      ]) {
        expect(() =>
          insertAudit.run("production", 3, "event-b", "event-third", 3, JSON.stringify(event)),
        ).toThrow()
      }
      expect(() =>
        insertAudit.run("production", 3, "event-b", "event-third", 3, "not-json"),
      ).toThrow()
      expect(() =>
        database.prepare(`UPDATE "nozzle_audit_log" SET "event_hash" = 'x'`).run(),
      ).toThrow("NOZZLE_CONTROL_AUDIT_APPEND_ONLY")
      expect(() => database.prepare(`DELETE FROM "nozzle_audit_log"`).run()).toThrow(
        "NOZZLE_CONTROL_AUDIT_APPEND_ONLY",
      )
    })
  })
})
