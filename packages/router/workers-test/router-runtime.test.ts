import { env } from "cloudflare:workers"
import { generateDrizzleShardGuardSql } from "@nozzle/cloudflare"
import {
  createScopedDatabase,
  eq,
  type PlanValue,
  SchemaRegistry,
  type ScopedRoute,
} from "@nozzle/drizzle"
import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { beforeEach, describe, expect, it } from "vitest"
import { createFanoutContinuation, mergeFanoutPage } from "../src/fanout.js"
import { executeFanoutPage } from "../src/fanout-executor.js"
import { countFanout, sumFanoutNumbers } from "../src/fanout-reducer.js"
import { createFanoutToken, decodeFanoutToken } from "../src/fanout-token.js"
import { RouterLeaf } from "../src/leaf.js"
import { createSessionToken, resolveRouteAwareSession } from "../src/session.js"
import { createRouterTransport } from "../src/transport.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const SCHEMA_ID = "application-v1"
const SCHEMA_DIGEST = "ab".repeat(32)
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: "11".repeat(32),
  partitionValue: "workspace-a",
  routeEpoch: 7,
  shardId: "shard-a",
}
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

function routerDatabase() {
  const leaf = new RouterLeaf({
    database: env.DB,
    leafShardId: "shard-a",
    registry,
    resolveRoute: async ({ partitionValue }: { partitionValue: PlanValue }) => {
      if (partitionValue !== route.partitionValue) throw new Error("unknown partition")
      return route
    },
    schemaId: SCHEMA_ID,
  })
  return createScopedDatabase({
    partitionKey: "workspaceId",
    registry,
    resolveRoute: async () => route,
    schemaId: SCHEMA_ID,
    transport: createRouterTransport(leaf),
  })
}

async function reset(): Promise<void> {
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
    `INSERT INTO "nozzle_schema_state"
     ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
     VALUES (?1, ?2, 1, 'operation-schema-1', 1)`,
  )
    .bind(SCHEMA_ID, SCHEMA_DIGEST)
    .run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_bucket_ownership"
     ("bucket_id", "route_epoch", "state", "movement_role", "operation_id", "fencing_token",
      "schema_version", "last_verified_checkpoint", "last_verified_at_ms", "updated_at_ms")
     VALUES (42, 7, 'writable', 'none', 'operation-route-1', 1, 1, 'checkpoint-1', 1, 1)`,
  ).run()
}

beforeEach(reset)

describe("real workerd router-to-D1 transport", () => {
  it("runs bounded fan-out merging and encrypted continuations in workerd", async () => {
    const checksums = {
      manifestChecksum: "11".repeat(32),
      queryChecksum: "22".repeat(32),
      schemaChecksum: "33".repeat(32),
    }
    const state = createFanoutContinuation({
      budget: {
        maxBufferedBytes: 100,
        maxBufferedRows: 10,
        maxBytes: 100,
        maxConcurrency: 2,
        maxCostMicros: 100,
        maxCpuMs: 100,
        maxPages: 2,
        maxRows: 10,
        maxShards: 2,
        maxSubrequests: 10,
        timeoutMs: 100,
      },
      deadlineAtMs: 10_000,
      expiresAtMs: 9_000,
      ...checksums,
      nowMs: 1_000,
      order: [{ direction: "asc", immutable: true, kind: "number", nulls: "last" }],
      partialPolicy: "fail",
      shardIds: ["a", "b"],
    })
    const result = mergeFanoutPage({
      current: { ...checksums, shardIds: state.shardIds },
      nowMs: 2_000,
      pageSize: 2,
      pages: [
        {
          exhausted: true,
          kind: "success",
          rows: [{ byteSize: 1, orderValues: [2], primaryKey: "a", value: "second" }],
          shardId: "a",
          usage: { costMicros: 1, cpuMs: 1, subrequests: 1 },
        },
        {
          exhausted: true,
          kind: "success",
          rows: [{ byteSize: 1, orderValues: [1], primaryKey: "b", value: "first" }],
          shardId: "b",
          usage: { costMicros: 1, cpuMs: 1, subrequests: 1 },
        },
      ],
      state,
    })

    expect(result.rows.map((row) => row.value)).toEqual(["first", "second"])
    expect(result.complete).toBe(true)

    const key = Uint8Array.from({ length: 32 }, (_, index) => index)
    const token = await createFanoutToken({ key, nowMs: 2_000, state })
    await expect(
      decodeFanoutToken({
        current: { ...checksums, shardIds: state.shardIds },
        key,
        nowMs: 2_000,
        token,
      }),
    ).resolves.toEqual(state)
  })

  it("executes a budgeted fan-out with native workerd timers and abort signals", async () => {
    const nowMs = Date.now()
    const checksums = {
      manifestChecksum: "11".repeat(32),
      queryChecksum: "22".repeat(32),
      schemaChecksum: "33".repeat(32),
    }
    const state = createFanoutContinuation({
      budget: {
        maxBufferedBytes: 10,
        maxBufferedRows: 10,
        maxBytes: 10,
        maxConcurrency: 1,
        maxCostMicros: 10,
        maxCpuMs: 10,
        maxPages: 2,
        maxRows: 10,
        maxShards: 1,
        maxSubrequests: 10,
        timeoutMs: 100,
      },
      deadlineAtMs: nowMs + 10_000,
      expiresAtMs: nowMs + 9_000,
      ...checksums,
      nowMs,
      order: [{ direction: "asc", immutable: true, kind: "number", nulls: "last" }],
      partialPolicy: "fail",
      shardIds: ["a"],
    })
    const result = await executeFanoutPage({
      current: { ...checksums, shardIds: state.shardIds },
      estimateUsage: () => ({ costMicros: 1, cpuMs: 1, subrequests: 1 }),
      fetchShard: async ({ signal }) => {
        expect(signal).toBeInstanceOf(AbortSignal)
        return {
          exhausted: true,
          rows: [{ byteSize: 1, orderValues: [1], primaryKey: "a", value: "a" }],
          usage: { costMicros: 1, cpuMs: 1, subrequests: 1 },
        }
      },
      pageSize: 1,
      state,
    })
    expect(result.rows[0]?.value).toBe("a")
    expect(result.complete).toBe(true)
  })

  it("reduces exact counts and compensated sums in workerd", () => {
    expect(
      countFanout({
        partialPolicy: "fail",
        shardIds: ["a", "b"],
        shards: [
          { kind: "success", shardId: "a", value: "9007199254740993" },
          { kind: "success", shardId: "b", value: 2n },
        ],
      }).value,
    ).toBe(9_007_199_254_740_995n)
    expect(
      sumFanoutNumbers({
        partialPolicy: "fail",
        shardIds: ["a", "b", "c"],
        shards: [
          { kind: "success", shardId: "a", value: 1e16 },
          { kind: "success", shardId: "b", value: 1 },
          { kind: "success", shardId: "c", value: -1e16 },
        ],
      }).value,
    ).toBe(1)
  })

  it("preserves Drizzle result types through the explicit wire codec", async () => {
    const db = routerDatabase()
    const createdAt = new Date("2026-01-02T03:04:05.678Z")
    await db.insert(projects).values({
      id: "project-a",
      name: "Nozzle",
      active: false,
      createdAt,
      settings: { theme: "dark" },
      payload: 9_007_199_254_740_991n,
    })

    await expect(db.select().from(projects).where(eq(projects.id, "project-a"))).resolves.toEqual([
      {
        active: false,
        createdAt,
        id: "project-a",
        name: "Nozzle",
        payload: 9_007_199_254_740_991n,
        settings: { theme: "dark" },
        workspaceId: "workspace-a",
      },
    ])
  })

  it("keeps a routed batch atomic when a later statement fails", async () => {
    const db = routerDatabase()
    const values = {
      id: "duplicate",
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

  it("returns a stable pre-mutation error after local ownership is fenced", async () => {
    await env.DB.prepare(
      `UPDATE "nozzle_bucket_ownership" SET "state" = 'read_only' WHERE "bucket_id" = 42`,
    ).run()
    await expect(
      routerDatabase()
        .insert(projects)
        .values({
          id: "blocked",
          name: "Blocked",
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

  it("replaces a moved route session without forwarding the source bookmark", async () => {
    const key = Uint8Array.from({ length: 32 }, (_, index) => index)
    const token = await createSessionToken(key, {
      d1Bookmark: "source-bookmark",
      fleetId: "fleet-a",
      issuedAtMs: 1,
      routeEpoch: 7,
      shardId: "shard-a",
    })
    const calls: unknown[][] = []
    const result = await resolveRouteAwareSession({
      currentRoute: { ...route, routeEpoch: 8, shardId: "shard-b" },
      establishFreshBookmark: async (...args: unknown[]) => {
        calls.push(args)
        return "destination-bookmark"
      },
      fleetId: "fleet-a",
      key,
      nowMs: 2,
      token,
    })

    expect(calls).toEqual([["shard-b"]])
    expect(JSON.stringify(calls)).not.toContain("source-bookmark")
    expect(result).toMatchObject({ d1Bookmark: "destination-bookmark", moved: true })
  })
})
