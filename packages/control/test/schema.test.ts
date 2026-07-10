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
            "idempotency_key", "input_checksum", "plan_checksum", "plan_json",
            "capability_snapshot_checksum", "required_shards_json", "status", "created_at_ms", "updated_at_ms")
           VALUES ('operation-a', 'production', 'migration', 'fleet-a', 'idempotency-a',
             'input-a', 'plan-a', '{}', 'capability-a', '[]', 'planned', 1, 1)`,
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
