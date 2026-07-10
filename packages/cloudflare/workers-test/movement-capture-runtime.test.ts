import { env } from "cloudflare:workers"
import { beforeAll, describe, expect, it } from "vitest"
import { generateMovementCaptureSql } from "../src/movement-capture.js"
import { generateShardGuardSql } from "../src/shard-guards.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const schemaChecksum = "ab".repeat(32)

beforeAll(async () => {
  await env.DB.prepare(`CREATE TABLE "movement_records" (
    "binary_id" BLOB NOT NULL,
    "version" INTEGER NOT NULL,
    "workspace_id" TEXT NOT NULL,
    "payload" TEXT NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL,
    PRIMARY KEY ("binary_id", "version")
  )`).run()
  const table = {
    partitionColumn: "workspace_id",
    partitionType: "string" as const,
    tableName: "movement_records",
  }
  for (const statement of generateShardGuardSql({ schemaId: "application-v1", tables: [table] })
    .statements) {
    await env.DB.prepare(statement).run()
  }
  for (const statement of generateMovementCaptureSql({
    schemaId: "application-v1",
    tables: [{ ...table, primaryColumns: ["binary_id", "version"] }],
  }).statements) {
    await env.DB.prepare(statement).run()
  }
  await env.DB.prepare(
    `INSERT INTO "nozzle_schema_state"
     ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
     VALUES ('application-v1', ?1, 1, 'schema-operation', 1)`,
  )
    .bind(schemaChecksum)
    .run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_bucket_ownership"
     ("bucket_id", "route_epoch", "state", "movement_role", "operation_id",
      "fencing_token", "schema_version", "last_verified_checkpoint",
      "last_verified_at_ms", "updated_at_ms")
     VALUES (42, 7, 'writable', 'source', 'movement-1', 1, 1, 'ready', 1, 1)`,
  ).run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_movement_captures"
     ("operation_id", "bucket_id", "scope_kind", "partition_type", "partition_binary",
      "partition_integer", "partition_string", "partition_uuid", "schema_id",
      "schema_checksum", "state", "start_sequence", "acknowledged_sequence",
      "max_pending_entries", "fencing_token", "created_at_ms", "updated_at_ms")
     VALUES ('movement-1', 42, 'partition', 'string', NULL, NULL, 'workspace-a', NULL,
       'application-v1', ?1, 'active', 0, 0, 2, 1, 1, 1)`,
  )
    .bind(schemaChecksum)
    .run()
})

function insertRecord(id: Uint8Array, version: number, workspace: string) {
  return env.DB.prepare(
    `INSERT INTO "movement_records"
     ("binary_id", "version", "workspace_id", "payload", "__nozzle_bucket")
     VALUES (?1, ?2, ?3, 'initial', 42)`,
  ).bind(id, version, workspace)
}

describe("real workerd movement capture", () => {
  it("journals transactionally, isolates a typed partition, and enforces backpressure", async () => {
    const id = new Uint8Array([0, 127, 255])
    await insertRecord(id, 9, "workspace-a").run()
    await insertRecord(new Uint8Array([1]), 1, "workspace-b").run()
    await env.DB.prepare(
      `UPDATE "movement_records" SET "payload" = 'changed'
       WHERE "workspace_id" = 'workspace-a'`,
    ).run()

    const journal = await env.DB.prepare(
      `SELECT "sequence", "mutation_hint", "key_json"
       FROM "nozzle_movement_outbox" ORDER BY "sequence"`,
    ).all<{ key_json: string; mutation_hint: string; sequence: number }>()
    expect(journal.results).toHaveLength(2)
    expect(JSON.parse(journal.results[0]?.key_json ?? "null")).toEqual([
      { column: "binary_id", type: "blob", value: "007fff" },
      { column: "version", type: "integer", value: "9" },
    ])
    await expect(
      env.DB.prepare(`DELETE FROM "movement_records" WHERE "workspace_id" = 'workspace-a'`).run(),
    ).rejects.toThrow(/NOZZLE_CAPTURE_BACKPRESSURE/u)
    await expect(
      env.DB.prepare(
        `SELECT "payload" FROM "movement_records" WHERE "workspace_id" = 'workspace-a'`,
      ).first(),
    ).resolves.toEqual({ payload: "changed" })

    await env.DB.prepare(
      `UPDATE "nozzle_movement_captures"
       SET "acknowledged_sequence" = 1, "updated_at_ms" = 2
       WHERE "operation_id" = 'movement-1'`,
    ).run()
    await env.DB.prepare(
      `DELETE FROM "movement_records" WHERE "workspace_id" = 'workspace-a'`,
    ).run()
    await expect(
      env.DB.prepare(
        `SELECT "mutation_hint" FROM "nozzle_movement_outbox" ORDER BY "sequence"`,
      ).all(),
    ).resolves.toMatchObject({
      results: [
        { mutation_hint: "upsert" },
        { mutation_hint: "upsert" },
        { mutation_hint: "delete" },
      ],
    })

    await env.DB.prepare(
      `UPDATE "nozzle_bucket_ownership"
       SET "state" = 'read_only', "updated_at_ms" = 3
       WHERE "bucket_id" = 42`,
    ).run()
    await expect(insertRecord(new Uint8Array([2]), 2, "workspace-b").run()).rejects.toThrow(
      /NOZZLE_GUARD_OWNERSHIP/u,
    )
  })
})
