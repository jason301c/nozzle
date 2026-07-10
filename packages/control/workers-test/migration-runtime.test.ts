import { env } from "cloudflare:workers"
import { createMigrationOperation, leaseProof } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1MigrationStore } from "../src/migration-store.js"
import { CONTROL_SCHEMA_STATEMENTS } from "../src/schema.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

beforeAll(async () => {
  for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_fleets"
     ("fleet_id", "account_id_checksum", "environment_id", "bucket_bits", "hash_version",
      "fleet_seed", "state", "created_at_ms")
     VALUES ('fleet-migration', 'account-checksum', 'production', 16, 1, ?1, 'active', 1)`,
  )
    .bind("a".repeat(43))
    .run()
})

describe("real workerd partial-fleet migration ledger", () => {
  it("halts on one failure, preserves success, fences stale resume, and converges forward", async () => {
    const leases = new D1LeaseStore(env.DB)
    const migrations = new D1MigrationStore(env.DB)
    const firstLease = await leases.acquire({
      acquisitionId: "migration-acquisition-a",
      holderId: "migration-controller-a",
      leaseKey: "fleet-migration:migration",
      ttlMs: 60_000,
    })
    if (!firstLease.acquired) throw new Error("fixture lease acquisition failed")
    const firstProof = leaseProof(firstLease.record)
    let operation = await migrations.create({
      fleetId: "fleet-migration",
      operation: createMigrationOperation({
        artifactChecksum: "artifact-v2",
        operationId: "migration-v2",
        requiredShardIds: ["shard-c", "shard-a", "shard-b"],
        targetSchemaChecksum: "schema-v2",
      }),
      proof: firstProof,
    })
    operation = await migrations.accept(operation.operationId, "shard-a", firstProof)
    operation = await migrations.applied(
      operation.operationId,
      "shard-a",
      "artifact-v2",
      firstProof,
    )
    operation = await migrations.verified(operation.operationId, "shard-a", "schema-v2", firstProof)
    operation = await migrations.accept(operation.operationId, "shard-b", firstProof)
    operation = await migrations.failed(operation.operationId, {
      apply: "retryable_failed",
      errorChecksum: "transient-b",
      proof: firstProof,
      shardId: "shard-b",
    })
    expect(operation).toMatchObject({
      halt: { controlSequence: 1, failedShardId: "shard-b", fencingToken: 1 },
      shards: { "shard-a": { verification: "verified" } },
    })
    await expect(migrations.accept(operation.operationId, "shard-c", firstProof)).rejects.toThrow(
      "No new shard work",
    )

    await leases.release({ proof: firstProof })
    const secondLease = await leases.acquire({
      acquisitionId: "migration-acquisition-b",
      holderId: "migration-controller-b",
      leaseKey: "fleet-migration:migration",
      ttlMs: 60_000,
    })
    if (!secondLease.acquired) throw new Error("fixture lease reacquisition failed")
    const secondProof = leaseProof(secondLease.record)
    await expect(
      migrations.resume(operation.operationId, {
        decisionChecksum: "stale-decision",
        proof: firstProof,
      }),
    ).rejects.toThrow()
    operation = await migrations.resume(operation.operationId, {
      decisionChecksum: "forward-decision",
      proof: secondProof,
    })
    for (const shardId of ["shard-b", "shard-c"]) {
      operation = await migrations.accept(operation.operationId, shardId, secondProof)
      operation = await migrations.applied(
        operation.operationId,
        shardId,
        "artifact-v2",
        secondProof,
      )
      operation = await migrations.verified(
        operation.operationId,
        shardId,
        "schema-v2",
        secondProof,
      )
    }
    await expect(
      migrations.activate(
        operation.operationId,
        { activeApplicationSupportsTarget: true, activeRouterSupportsTarget: true },
        secondProof,
      ),
    ).resolves.toBe(true)
    await expect(
      env.DB.prepare(
        `SELECT "state" FROM "nozzle_fleets" WHERE "fleet_id" = 'fleet-migration'`,
      ).first(),
    ).resolves.toEqual({ state: "active" })
  })
})
