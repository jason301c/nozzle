import { env } from "cloudflare:workers"
import { leaseProof } from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { CONTROL_SCHEMA_STATEMENTS } from "../src/schema.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

async function reset(): Promise<void> {
  await env.DB.prepare('DROP TABLE IF EXISTS "nozzle_leases"').run()
  for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
}

beforeEach(reset)

describe("real workerd control D1 leases", () => {
  it("elects exactly one controller and fences every loser", async () => {
    const store = new D1LeaseStore(env.DB)
    const decisions = await Promise.all(
      Array.from({ length: 12 }, (_, index) =>
        store.acquire({
          acquisitionId: `acquisition-${index}`,
          holderId: `controller-${index}`,
          leaseKey: "fleet-a:migration",
          ttlMs: 30_000,
        }),
      ),
    )
    const winners = decisions.filter((decision) => decision.acquired)
    expect(winners).toHaveLength(1)
    expect(decisions.filter((decision) => !decision.acquired)).toHaveLength(11)
    expect(winners[0]).toMatchObject({ record: { fencingToken: 1 } })
  })

  it("persists monotonic fencing across release and reacquisition", async () => {
    const store = new D1LeaseStore(env.DB)
    const first = await store.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:movement",
      ttlMs: 30_000,
    })
    if (!first.acquired) throw new Error("fixture acquisition failed")
    const oldProof = leaseProof(first.record)
    await expect(store.release({ proof: oldProof })).resolves.toMatchObject({ released: true })
    const second = await store.acquire({
      acquisitionId: "acquisition-b",
      holderId: "controller-b",
      leaseKey: "fleet-a:movement",
      ttlMs: 30_000,
    })
    expect(second).toMatchObject({ acquired: true, record: { fencingToken: 2 } })
    await expect(store.authorize(oldProof)).rejects.toMatchObject({
      code: "OperationResumeRequiredError",
    })
  })

  it("enforces lease persistence and token monotonicity below the adapter", async () => {
    const store = new D1LeaseStore(env.DB)
    await store.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:reconcile",
      ttlMs: 30_000,
    })
    await expect(
      env.DB.prepare(
        `UPDATE "nozzle_leases" SET "fencing_token" = 0
         WHERE "lease_key" = 'fleet-a:reconcile'`,
      ).run(),
    ).rejects.toThrow(/NOZZLE_CONTROL_LEASE_TOKEN_ROLLBACK/u)
    await expect(
      env.DB.prepare(`DELETE FROM "nozzle_leases" WHERE "lease_key" = 'fleet-a:reconcile'`).run(),
    ).rejects.toThrow(/NOZZLE_CONTROL_LEASE_PERSISTENT/u)
  })
})
