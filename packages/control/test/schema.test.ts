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
           ("operation_id", "operation_type", "idempotency_key", "plan_checksum", "plan_json",
            "capability_snapshot_checksum", "required_shards_json", "status", "created_at_ms", "updated_at_ms")
           VALUES ('operation-a', 'migration', 'idempotency-a', 'plan-a', '{}', 'capability-a',
             '[]', 'planned', 1, 1)`,
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
            `UPDATE "nozzle_operations" SET "plan_checksum" = 'rewritten'
             WHERE "operation_id" = 'operation-a'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_IMMUTABLE_OPERATION_PLAN")
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
          .prepare(`DELETE FROM "nozzle_operations" WHERE "operation_id" = 'operation-a'`)
          .run(),
      ).toThrow("NOZZLE_CONTROL_OPERATION_PERSISTENT")
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

      database
        .prepare(
          `INSERT INTO "nozzle_audit_log"
           ("environment_id", "sequence", "previous_hash", "event_hash", "server_time_ms",
            "operation_id", "step_id", "event_json")
           VALUES ('production', 1, NULL, 'event-a', 1, 'operation-a', NULL, '{}')`,
        )
        .run()
      expect(() =>
        database.prepare(`UPDATE "nozzle_audit_log" SET "event_hash" = 'x'`).run(),
      ).toThrow("NOZZLE_CONTROL_AUDIT_APPEND_ONLY")
      expect(() => database.prepare(`DELETE FROM "nozzle_audit_log"`).run()).toThrow(
        "NOZZLE_CONTROL_AUDIT_APPEND_ONLY",
      )
    })
  })
})
