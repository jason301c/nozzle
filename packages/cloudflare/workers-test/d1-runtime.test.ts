import { env } from "cloudflare:workers"
import { createScopedDatabase, eq, SchemaRegistry, type ScopedRoute } from "@nozzle/drizzle"
import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { beforeEach, describe, expect, it } from "vitest"
import { generateDrizzleShardGuardSql } from "../src/drizzle-shard-guards.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const SCHEMA_ID = "application-v1"
const SCHEMA_DIGEST = "ab".repeat(32)
const PARTITION_DIGEST = "11".repeat(32)
const FENCED_DIGEST = "22".repeat(32)

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
  active: integer({ mode: "boolean" }).notNull(),
  createdAt: integer("created_at", { mode: "timestamp_ms" }).notNull(),
  settings: text({ mode: "json" }).notNull(),
  payload: blob({ mode: "bigint" }).notNull(),
})
const registry = new SchemaRegistry({ schema: { projects }, partitionKey: "workspaceId" })
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: PARTITION_DIGEST,
  partitionValue: "workspace-fictional",
  routeEpoch: 7,
  shardId: "shard-a",
}

function scopedDatabase(routeOverride: ScopedRoute = route) {
  return createScopedDatabase({
    partitionKey: "workspaceId",
    registry,
    resolveDatabase: () => env.DB,
    resolveRoute: async () => routeOverride,
    schemaId: SCHEMA_ID,
  })
}

async function setOwnership(
  state: "read_only" | "writable",
  routeEpoch = 7,
  fencingToken = 1,
  operationId = "operation-route-1",
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO "nozzle_bucket_ownership" (
      "bucket_id", "route_epoch", "state", "movement_role", "operation_id", "fencing_token",
      "schema_version", "last_verified_checkpoint", "last_verified_at_ms", "updated_at_ms"
    ) VALUES (?1, ?2, ?3, 'none', ?4, ?5, 1, 'checkpoint-1', 1, 1)
    ON CONFLICT ("bucket_id") DO UPDATE SET
      "route_epoch" = excluded."route_epoch",
      "state" = excluded."state",
      "movement_role" = excluded."movement_role",
      "operation_id" = excluded."operation_id",
      "fencing_token" = excluded."fencing_token",
      "schema_version" = excluded."schema_version",
      "last_verified_checkpoint" = excluded."last_verified_checkpoint",
      "last_verified_at_ms" = excluded."last_verified_at_ms",
      "updated_at_ms" = excluded."updated_at_ms"`,
  )
    .bind(42, routeEpoch, state, operationId, fencingToken)
    .run()
}

async function setSchemaActive(active: boolean): Promise<void> {
  await env.DB.prepare(`UPDATE "nozzle_schema_state" SET "active" = ?1 WHERE "schema_id" = ?2`)
    .bind(active ? 1 : 0, SCHEMA_ID)
    .run()
}

async function insertFence(partitionDigest: string, partitionValue: string): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO "nozzle_partition_fences" (
      "hash_version", "partition_digest", "partition_type",
      "partition_binary", "partition_integer", "partition_string", "partition_uuid",
      "source_bucket_id", "source_route_epoch", "operation_id", "audit_event_id",
      "fenced_at_ms", "reason"
    ) VALUES (1, ?1, 'string', NULL, NULL, ?2, NULL, 42, 7,
      'operation-move-1', 'audit-fence-1', 2, 'former-source')`,
  )
    .bind(partitionDigest, partitionValue)
    .run()
}

function rawInsert(id: string, workspaceId: string): D1PreparedStatement {
  return env.DB.prepare(
    `INSERT INTO "projects" (
      "id", "workspace_id", "name", "active", "created_at", "settings", "payload",
      "__nozzle_bucket"
    ) VALUES (?1, ?2, ?3, 1, 1, '{}', ?4, 42)`,
  ).bind(id, workspaceId, id, new TextEncoder().encode("1"))
}

async function resetShard(): Promise<void> {
  for (const table of [
    "projects",
    "nozzle_partition_fences",
    "nozzle_schema_state",
    "nozzle_bucket_ownership",
  ]) {
    await env.DB.prepare(`DROP TABLE IF EXISTS "${table}"`).run()
  }
  await env.DB.prepare(`CREATE TABLE "projects" (
    "id" TEXT PRIMARY KEY NOT NULL,
    "workspace_id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "active" INTEGER NOT NULL,
    "created_at" INTEGER NOT NULL,
    "settings" TEXT NOT NULL,
    "payload" BLOB NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL
  )`).run()
  const guards = generateDrizzleShardGuardSql({
    registry,
    partitionKeyType: "string",
    schemaId: SCHEMA_ID,
  })
  for (const statement of guards.statements) await env.DB.prepare(statement).run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_schema_state" (
      "schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms"
    ) VALUES (?1, ?2, 1, 'operation-schema-1', 1)`,
  )
    .bind(SCHEMA_ID, SCHEMA_DIGEST)
    .run()
  await setOwnership("writable")
}

beforeEach(resetShard)

describe("workerd D1 shard enforcement", () => {
  it("executes scoped writes and returns Drizzle-decoded selected values", async () => {
    const db = scopedDatabase()
    const createdAt = new Date("2026-01-02T03:04:05.678Z")
    const mutation = await db.insert(projects).values({
      id: "project-1",
      name: "Nozzle",
      active: false,
      createdAt,
      settings: { theme: "dark" },
      payload: 9_007_199_254_740_991n,
    })

    expect(mutation.success).toBe(true)
    expect(mutation.meta.changes).toBe(1)
    expect(mutation.meta.rows_written).toBeGreaterThanOrEqual(1)
    await expect(db.select().from(projects).where(eq(projects.id, "project-1"))).resolves.toEqual([
      {
        id: "project-1",
        workspaceId: "workspace-fictional",
        name: "Nozzle",
        active: false,
        createdAt,
        settings: { theme: "dark" },
        payload: 9_007_199_254_740_991n,
      },
    ])
  })

  it("rejects stale ownership epochs and inactive schema identity before data access", async () => {
    await setOwnership("writable", 8)
    await expect(scopedDatabase().select().from(projects)).rejects.toMatchObject({
      code: "StaleRouteRejectedError",
    })

    await setSchemaActive(false)
    await expect(
      scopedDatabase({ ...route, routeEpoch: 8 })
        .select()
        .from(projects),
    ).rejects.toMatchObject({
      code: "StaleRouteRejectedError",
    })
  })

  it("rejects a current-client write through the full-digest fence guard", async () => {
    await insertFence(PARTITION_DIGEST, "workspace-fictional")
    await expect(
      scopedDatabase()
        .insert(projects)
        .values({
          id: "project-fenced",
          name: "Fenced",
          active: true,
          createdAt: new Date(1),
          settings: {},
          payload: 1n,
        }),
    ).rejects.toMatchObject({ code: "StaleRouteRejectedError" })
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "projects"').first<{ count: number }>(),
    ).resolves.toEqual({ count: 0 })
  })

  it("blocks stale raw clients by typed fence while allowing a neighbor in the shared bucket", async () => {
    await rawInsert("existing-fenced", "workspace-fenced").run()
    await insertFence(FENCED_DIGEST, "workspace-fenced")

    await expect(rawInsert("new-fenced", "workspace-fenced").run()).rejects.toThrow(
      /NOZZLE_GUARD_PARTITION_FENCE/u,
    )
    await expect(
      env.DB.prepare('UPDATE "projects" SET "name" = ?1 WHERE "id" = ?2')
        .bind("changed", "existing-fenced")
        .run(),
    ).rejects.toThrow(/NOZZLE_GUARD_PARTITION_FENCE/u)
    await expect(
      env.DB.prepare('DELETE FROM "projects" WHERE "id" = ?1').bind("existing-fenced").run(),
    ).rejects.toThrow(/NOZZLE_GUARD_PARTITION_FENCE/u)

    await expect(rawInsert("neighbor", "workspace-neighbor").run()).resolves.toMatchObject({
      success: true,
    })
    await expect(
      env.DB.prepare('SELECT "id", "name" FROM "projects" ORDER BY "id"').all(),
    ).resolves.toMatchObject({
      results: [
        { id: "existing-fenced", name: "existing-fenced" },
        { id: "neighbor", name: "neighbor" },
      ],
    })
  })

  it("rolls back the entire real D1 batch when a later trigger rejects", async () => {
    await insertFence(FENCED_DIGEST, "workspace-fenced")
    await expect(
      env.DB.batch([
        rawInsert("would-have-succeeded", "workspace-neighbor"),
        rawInsert("must-fail", "workspace-fenced"),
      ]),
    ).rejects.toThrow(/NOZZLE_GUARD_PARTITION_FENCE/u)
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "projects"').first<{ count: number }>(),
    ).resolves.toEqual({ count: 0 })
  })

  it("rolls back a scoped atomic batch when a later application statement fails", async () => {
    const db = scopedDatabase()
    const values = {
      id: "duplicate-project",
      name: "Duplicate",
      active: true,
      createdAt: new Date(1),
      settings: {},
      payload: 1n,
    } as const

    await expect(
      db.batch([db.insert(projects).values(values), db.insert(projects).values(values)]),
    ).rejects.toThrow()
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "projects"').first<{ count: number }>(),
    ).resolves.toEqual({ count: 0 })
  })

  it("rejects raw writes immediately when the local bucket is not writable", async () => {
    await setOwnership("read_only")
    await expect(rawInsert("blocked", "workspace-neighbor").run()).rejects.toThrow(
      /NOZZLE_GUARD_OWNERSHIP/u,
    )
  })
})
