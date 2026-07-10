import { env } from "cloudflare:workers"
import { beforeAll, describe, expect, it } from "vitest"
import { generateMovementCaptureSql } from "../src/movement-capture.js"
import { generateMovementTransferSql, movementTransferViewName } from "../src/movement-transfer.js"
import { generateShardGuardSql } from "../src/shard-guards.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const token = "t".repeat(43)

beforeAll(async () => {
  await env.DB.prepare(`CREATE TABLE "transfer_records" (
    "id" TEXT NOT NULL,
    "workspace_id" TEXT NOT NULL,
    "payload" TEXT NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL,
    PRIMARY KEY ("id", "workspace_id")
  )`).run()
  const guard = {
    partitionColumn: "workspace_id",
    partitionType: "string" as const,
    tableName: "transfer_records",
  }
  for (const statement of generateShardGuardSql({ schemaId: "application-v1", tables: [guard] })
    .statements) {
    await env.DB.prepare(statement).run()
  }
  for (const statement of generateMovementCaptureSql({
    schemaId: "application-v1",
    tables: [{ ...guard, primaryColumns: ["id", "workspace_id"] }],
  }).statements) {
    await env.DB.prepare(statement).run()
  }
  for (const statement of generateMovementTransferSql({
    tables: [
      {
        columns: ["id", "workspace_id", "payload"],
        primaryColumns: ["id", "workspace_id"],
        tableName: "transfer_records",
      },
    ],
  }).statements) {
    await env.DB.prepare(statement).run()
  }
  await env.DB.prepare(
    `INSERT INTO "nozzle_schema_state"
     ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
     VALUES ('application-v1', ?1, 1, 'schema-operation', 1)`,
  )
    .bind("ab".repeat(32))
    .run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_bucket_ownership"
     ("bucket_id", "route_epoch", "state", "movement_role", "operation_id",
      "fencing_token", "schema_version", "last_verified_checkpoint",
      "last_verified_at_ms", "updated_at_ms")
     VALUES (77, 9, 'copying', 'destination', 'movement-transfer', 3, 1, 'copying', 1, 1)`,
  ).run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_operation_write_capabilities"
     ("capability_token", "operation_id", "bucket_id", "table_id", "mode",
      "fencing_token", "expires_at_ms", "remaining_uses", "issued_at_ms")
     VALUES (?1, 'movement-transfer', 77, 'transfer_records', 'upsert', 3,
       9999999999999, 2, 1)`,
  )
    .bind(token)
    .run()
})

function apply(payload: string): D1PreparedStatement {
  return env.DB.prepare(
    `INSERT INTO "${movementTransferViewName("transfer_records")}"
     ("id", "workspace_id", "payload", "__nozzle_bucket",
      "__nozzle_capability_token", "__nozzle_mutation_hint")
     VALUES ('record-1', 'workspace-a', ?1, 77, ?2, 'upsert')`,
  ).bind(payload, token)
}

function receipt(): D1PreparedStatement {
  return env.DB.prepare(
    `INSERT INTO "nozzle_movement_replay_receipts"
     ("operation_id", "source_sequence", "table_id", "key_json", "mutation_hint",
      "result_checksum", "applied_at_ms")
     VALUES ('movement-transfer', 1, 'transfer_records', '[]', 'upsert', 'result-1', 1)
     ON CONFLICT ("operation_id", "source_sequence") DO NOTHING`,
  )
}

describe("real workerd operation-scoped movement transfer", () => {
  it("atomically replays through a use-limited capability while base writes remain fenced", async () => {
    await expect(
      env.DB.prepare(
        `INSERT INTO "transfer_records"
         ("id", "workspace_id", "payload", "__nozzle_bucket")
         VALUES ('direct', 'workspace-a', 'blocked', 77)`,
      ).run(),
    ).rejects.toThrow(/NOZZLE_GUARD_OWNERSHIP/u)

    await env.DB.batch([apply("copied"), receipt()])
    await env.DB.batch([apply("replayed"), receipt()])
    await expect(env.DB.prepare(`SELECT * FROM "transfer_records"`).first()).resolves.toEqual({
      __nozzle_bucket: 77,
      id: "record-1",
      payload: "replayed",
      workspace_id: "workspace-a",
    })
    await expect(
      env.DB.prepare(`SELECT count(*) AS "count" FROM "nozzle_movement_replay_receipts"`).first(),
    ).resolves.toEqual({ count: 1 })
    await expect(
      env.DB.prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_write_context"`).first(),
    ).resolves.toEqual({ count: 0 })
    await expect(apply("exhausted").run()).rejects.toThrow(/NOZZLE_OPERATION_CAPABILITY_INVALID/u)
  })
})
