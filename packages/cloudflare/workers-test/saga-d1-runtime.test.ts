import { env } from "cloudflare:workers"
import type { DigestFunction, SagaActionReference } from "@nozzle/core"
import { buildInsertPlan, SchemaRegistry, type ScopedRoute } from "@nozzle/drizzle"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { beforeEach, describe, expect, it } from "vitest"
import { generateDrizzleShardGuardSql } from "../src/drizzle-shard-guards.js"
import {
  D1_SAGA_RECEIPT_SCHEMA_STATEMENTS,
  D1SagaAtomicAdapter,
  type D1SagaAtomicApplyInput,
  type D1SagaTarget,
  type SagaD1Database,
} from "../src/saga-d1.js"

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
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: PARTITION_DIGEST,
  partitionValue: "workspace-fictional",
  routeEpoch: 7,
  shardId: "shard-a",
}
const action: SagaActionReference = {
  actionId: "project.create",
  artifactChecksum: "22".repeat(32),
  version: 1,
}
const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  value: integer().notNull(),
})
const registry = new SchemaRegistry({ partitionKey: "workspaceId", schema: { projects } })

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const value = await crypto.subtle.digest("SHA-256", copy.buffer)
  return [...new Uint8Array(value)].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function sagaDatabase(): SagaD1Database {
  return env.DB as unknown as SagaD1Database
}

function target(routeOverride: ScopedRoute = route): D1SagaTarget {
  return {
    bucketId: routeOverride.bucketId,
    database: sagaDatabase(),
    partitionDigest: routeOverride.partitionDigestHex,
    routeEpoch: routeOverride.routeEpoch,
    schemaId: SCHEMA_ID,
    shardId: routeOverride.shardId,
  }
}

function input(
  id: string,
  options: { readonly idempotencyKey?: string; readonly route?: ScopedRoute } = {},
): D1SagaAtomicApplyInput {
  const selectedRoute = options.route ?? route
  return {
    action,
    attemptAcceptanceChecksum: "33".repeat(32),
    attemptId: `attempt-${id}`,
    idempotencyKey: options.idempotencyKey ?? `saga:create:${id}`,
    inputJson: JSON.stringify({ id, value: 1 }),
    mutations: [
      buildInsertPlan(registry, {
        route: selectedRoute,
        schemaId: SCHEMA_ID,
        table: projects,
        values: { id, value: 1 },
      }),
    ],
    operationId: "operation-1",
    outputJson: JSON.stringify({ id }),
    phase: "forward",
    sagaId: "saga-1",
    stepId: "create",
    target: target(selectedRoute),
  }
}

async function resetShard(): Promise<void> {
  for (const table of [
    "nozzle_saga_action_receipts",
    "projects",
    "nozzle_partition_fences",
    "nozzle_schema_state",
    "nozzle_operation_write_context",
    "nozzle_operation_write_capabilities",
    "nozzle_bucket_ownership",
  ]) {
    await env.DB.prepare(`DROP TABLE IF EXISTS "${table}"`).run()
  }
  await env.DB.prepare(`CREATE TABLE "projects" (
    "id" TEXT PRIMARY KEY NOT NULL,
    "workspace_id" TEXT NOT NULL,
    "value" INTEGER NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL
  )`).run()
  const guards = generateDrizzleShardGuardSql({
    registry,
    partitionKeyType: "string",
    schemaId: SCHEMA_ID,
  })
  for (const statement of guards.statements) await env.DB.prepare(statement).run()
  for (const statement of D1_SAGA_RECEIPT_SCHEMA_STATEMENTS) {
    await env.DB.prepare(statement).run()
  }
  await env.DB.prepare(
    `INSERT INTO "nozzle_schema_state" (
      "schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms"
    ) VALUES (?1, ?2, 1, 'operation-schema-1', 1)`,
  )
    .bind(SCHEMA_ID, SCHEMA_DIGEST)
    .run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_bucket_ownership" (
      "bucket_id", "route_epoch", "state", "movement_role", "operation_id", "fencing_token",
      "schema_version", "last_verified_checkpoint", "last_verified_at_ms", "updated_at_ms"
    ) VALUES (42, 7, 'writable', 'none', 'operation-route-1', 1, 1, 'checkpoint-1', 1, 1)`,
  ).run()
}

beforeEach(resetShard)

describe("workerd D1 atomic saga receipts", () => {
  it("commits application data and its immutable result receipt in one batch", async () => {
    const result = await new D1SagaAtomicAdapter(digest).apply(input("project-1"))

    expect(result).toMatchObject({ kind: "applied" })
    await expect(
      env.DB.prepare('SELECT "id", "value" FROM "projects"').all(),
    ).resolves.toMatchObject({ results: [{ id: "project-1", value: 1 }] })
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "nozzle_saga_action_receipts"').first(),
    ).resolves.toEqual({ count: 1 })
    await expect(
      env.DB.prepare(
        'UPDATE "nozzle_saga_action_receipts" SET "output_json" = ?1 WHERE "idempotency_key" = ?2',
      )
        .bind("{}", "saga:create:project-1")
        .run(),
    ).rejects.toThrow(/NOZZLE_SAGA_RECEIPT_IMMUTABLE/u)
    await expect(
      env.DB.prepare('DELETE FROM "nozzle_saga_action_receipts" WHERE "idempotency_key" = ?1')
        .bind("saga:create:project-1")
        .run(),
    ).rejects.toThrow(/NOZZLE_SAGA_RECEIPT_PERSISTENT/u)
  })

  it("rolls back tentative data when a conflicting idempotency receipt already exists", async () => {
    const adapter = new D1SagaAtomicAdapter(digest)
    const original = input("original", { idempotencyKey: "stable-key" })
    await expect(adapter.apply(original)).resolves.toMatchObject({ kind: "applied" })
    await expect(adapter.apply(original)).resolves.toMatchObject({ kind: "applied" })

    await expect(
      adapter.apply(input("must-roll-back", { idempotencyKey: "stable-key" })),
    ).resolves.toMatchObject({ kind: "indeterminate" })
    await expect(
      env.DB.prepare('SELECT "id" FROM "projects" ORDER BY "id"').all(),
    ).resolves.toMatchObject({ results: [{ id: "original" }] })
  })

  it("converges concurrent identical deliveries on one data row and receipt", async () => {
    const adapter = new D1SagaAtomicAdapter(digest)
    const delivery = input("concurrent")
    const results = await Promise.all([adapter.apply(delivery), adapter.apply(delivery)])
    expect(results.map((result) => result.kind)).toEqual(["applied", "applied"])
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "projects"').first(),
    ).resolves.toEqual({ count: 1 })
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "nozzle_saga_action_receipts"').first(),
    ).resolves.toEqual({ count: 1 })
  })

  it.each([
    "stale route",
    "inactive schema",
    "partition fence",
  ] as const)("returns not-applied and persists nothing under a %s", async (condition) => {
    let selectedRoute = route
    if (condition === "stale route") selectedRoute = { ...route, routeEpoch: 8 }
    if (condition === "inactive schema") {
      await env.DB.prepare('UPDATE "nozzle_schema_state" SET "active" = 0').run()
    }
    if (condition === "partition fence") {
      await env.DB.prepare(
        `INSERT INTO "nozzle_partition_fences" (
            "hash_version", "partition_digest", "partition_type",
            "partition_binary", "partition_integer", "partition_string", "partition_uuid",
            "source_bucket_id", "source_route_epoch", "operation_id", "audit_event_id",
            "fenced_at_ms", "reason"
          ) VALUES (1, ?1, 'string', NULL, NULL, ?2, NULL, 42, 7,
            'operation-move-1', 'audit-fence-1', 2, 'former-source')`,
      )
        .bind(PARTITION_DIGEST, route.partitionValue)
        .run()
    }

    await expect(
      new D1SagaAtomicAdapter(digest).apply(input("blocked", { route: selectedRoute })),
    ).resolves.toEqual({
      evidenceJson: '{"kind":"d1_primary_receipt_absent"}',
      kind: "not_applied",
    })
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "projects"').first(),
    ).resolves.toEqual({ count: 0 })
    await expect(
      env.DB.prepare('SELECT count(*) AS "count" FROM "nozzle_saga_action_receipts"').first(),
    ).resolves.toEqual({ count: 0 })
  })
})
