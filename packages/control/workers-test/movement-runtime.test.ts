import { env } from "cloudflare:workers"
import { createMovementOperation, leaseProof } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1MovementStore } from "../src/movement-store.js"
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
     VALUES ('fleet-movement', 'account', 'production', 16, 1, ?1, 'active', 1)`,
  )
    .bind("a".repeat(43))
    .run()
})

describe("real workerd durable movement control", () => {
  it("persists exact CAS state, blocks once, and recovers only under a newer lease", async () => {
    const leases = new D1LeaseStore(env.DB)
    const movements = new D1MovementStore(env.DB)
    const first = await leases.acquire({
      acquisitionId: "movement-acquisition-a",
      holderId: "movement-controller-a",
      leaseKey: "fleet-movement:movement",
      ttlMs: 60_000,
    })
    if (!first.acquired) throw new Error("fixture")
    const firstProof = leaseProof(first.record)
    let operation = await movements.create({
      fleetId: "fleet-movement",
      operation: createMovementOperation({
        destinationShardId: "shard-b",
        operationId: "movement-workerd",
        partitionDigest: "digest",
        requiredTableIds: ["rows"],
        sourceRouteEpoch: 4,
        sourceShardId: "shard-a",
        targetRouteEpoch: 5,
      }),
      proof: firstProof,
    })
    operation = await movements.apply(
      operation.operationId,
      { input: { schemaChecksum: "schema", startSequence: 0 }, kind: "start_capture" },
      firstProof,
    )
    operation = await movements.apply(
      operation.operationId,
      { input: { errorChecksum: "failure", outcome: "unknown" }, kind: "block" },
      firstProof,
    )
    expect(operation.block).toMatchObject({ controlSequence: 1, fencingToken: 1 })
    await expect(
      movements.apply(operation.operationId, { kind: "start_copy" }, firstProof),
    ).rejects.toThrow("requires a newer fenced recovery")

    await leases.release({ proof: firstProof })
    const second = await leases.acquire({
      acquisitionId: "movement-acquisition-b",
      holderId: "movement-controller-b",
      leaseKey: "fleet-movement:movement",
      ttlMs: 60_000,
    })
    if (!second.acquired) throw new Error("fixture")
    const secondProof = leaseProof(second.record)
    await expect(
      movements.apply(
        operation.operationId,
        { input: { decisionChecksum: "recover", fencingToken: 2 }, kind: "authorize_recovery" },
        firstProof,
      ),
    ).rejects.toThrow("compare-and-swap")
    operation = await movements.apply(
      operation.operationId,
      { input: { decisionChecksum: "recover", fencingToken: 2 }, kind: "authorize_recovery" },
      secondProof,
    )
    await expect(
      movements.apply(operation.operationId, { kind: "start_copy" }, secondProof),
    ).resolves.toMatchObject({ phase: "copying", recovery: { fencingToken: 2 } })
  })
})
