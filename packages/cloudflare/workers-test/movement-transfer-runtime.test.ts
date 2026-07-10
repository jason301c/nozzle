import { env } from "cloudflare:workers"
import { SchemaRegistry } from "@nozzle/drizzle"
import { primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { beforeAll, describe, expect, it } from "vitest"
import { generateMovementCaptureSql } from "../src/movement-capture.js"
import {
  compileMovementDelete,
  compileMovementPage,
  compileMovementReplayRead,
  compileMovementReplayReceipt,
  compileMovementUpsert,
  decodeMovementPage,
} from "../src/movement-data.js"
import { generateMovementTransferSql } from "../src/movement-transfer.js"
import { generateShardGuardSql } from "../src/shard-guards.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const token = "t".repeat(43)
const deleteToken = "d".repeat(43)
const transferRecords = sqliteTable(
  "transfer_records",
  {
    id: text().notNull(),
    workspaceId: text("workspace_id").notNull(),
    payload: text().notNull(),
  },
  (table) => [primaryKey({ columns: [table.id, table.workspaceId] })],
)
const table = new SchemaRegistry({
  partitionKey: "workspaceId",
  schema: { transferRecords },
}).table(transferRecords)

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

function prepared(statement: { readonly params: readonly unknown[]; readonly sql: string }) {
  return env.DB.prepare(statement.sql).bind(...statement.params)
}

const keyJson = JSON.stringify([
  { column: "id", type: "text", value: "record-1" },
  { column: "workspace_id", type: "text", value: "workspace-a" },
])

function apply(payload: string): D1PreparedStatement {
  return prepared(
    compileMovementUpsert({
      capabilityToken: token,
      destinationBucketId: 77,
      row: {
        __nozzle_bucket: 12,
        id: "record-1",
        payload,
        workspace_id: "workspace-a",
      },
      table,
    }),
  )
}

function receipt(sequence = 1, mutationHint: "delete" | "upsert" = "upsert") {
  return prepared(
    compileMovementReplayReceipt({
      appliedAtMs: 1,
      keyJson,
      mutationHint,
      operationId: "movement-transfer",
      resultChecksum: `result-${sequence}`,
      sourceSequence: sequence,
      tableId: "transfer_records",
    }),
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
    const page = compileMovementPage({
      limit: 10,
      maxBytes: 10_000,
      scope: { bucketId: 77 },
      table,
    })
    const pageResult = await prepared(page).all()
    expect(decodeMovementPage(pageResult.results, 10_000).rows).toMatchObject([
      { payload: "replayed" },
    ])
    const current = compileMovementReplayRead({ keyJson, sourceBucketId: 77, table })
    await expect(prepared(current).all()).resolves.toMatchObject({
      results: [{ payload: "replayed" }],
    })
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

    await env.DB.prepare(
      `INSERT INTO "nozzle_operation_write_capabilities"
       ("capability_token", "operation_id", "bucket_id", "table_id", "mode",
        "fencing_token", "expires_at_ms", "remaining_uses", "issued_at_ms")
       VALUES (?1, 'movement-transfer', 77, 'transfer_records', 'delete', 3,
         9999999999999, 1, 1)`,
    )
      .bind(deleteToken)
      .run()
    await env.DB.batch([
      prepared(
        compileMovementDelete({
          capabilityToken: deleteToken,
          destinationBucketId: 77,
          keyJson,
          table,
        }),
      ),
      receipt(2, "delete"),
    ])
    await expect(
      env.DB.prepare(`SELECT count(*) AS "count" FROM "transfer_records"`).first(),
    ).resolves.toEqual({ count: 0 })
  })
})
