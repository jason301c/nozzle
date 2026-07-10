import { env } from "cloudflare:workers"
import { type DigestFunction, leaseProof, sealOperationPlan, verifyAuditChain } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore } from "../src/operation-store.js"
import { CONTROL_SCHEMA_STATEMENTS } from "../src/schema.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

beforeAll(async () => {
  for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
})

describe("real workerd operation ledger", () => {
  it("atomically creates concurrent operations with exact replay and a valid audit chain", async () => {
    const store = new D1OperationStore(env.DB, digest)
    const capabilitySnapshotJson = JSON.stringify({ provider: "cloudflare-d1" })
    const inputJson = (index: number) => JSON.stringify({ operation: `workerd-${index}` })
    const plans = await Promise.all(
      Array.from({ length: 4 }, async (_, index) =>
        sealOperationPlan(
          {
            capabilitySnapshotChecksum: await digest(
              new TextEncoder().encode(capabilitySnapshotJson),
            ),
            idempotencyKey: `workerd-key-${index}`,
            inputChecksum: await digest(new TextEncoder().encode(inputJson(index))),
            operationId: `workerd-operation-${index}`,
            operationType: "workerd-test",
            steps: [
              {
                checkpoint: "reversible",
                dependsOn: [],
                idempotencyKey: `workerd-step-key-${index}`,
                inputChecksum: `workerd-step-input-${index}`,
                leaseKey: "workerd:operation",
                postconditionChecksum: "postcondition",
                preconditionChecksum: "precondition",
                recoveryInstructions: "Inspect and resume.",
                retryClassification: "reconcile_first",
                stepId: "provision",
              },
            ],
          },
          digest,
        ),
      ),
    )
    const created = await Promise.all(
      plans.map((plan, index) =>
        store.create({
          actorChecksum: "workerd-actor",
          capabilitySnapshotJson,
          environmentId: "workerd",
          idempotencyScope: "fleet-workerd",
          inputJson: inputJson(index),
          plan,
          requiredShardIds: ["shard-b", "shard-a"],
        }),
      ),
    )
    expect(created.every((result) => result.created)).toBe(true)
    const firstPlan = plans[0]
    if (!firstPlan) throw new Error("Fixture plan is missing.")
    await expect(
      store.create({
        actorChecksum: "workerd-actor",
        capabilitySnapshotJson,
        environmentId: "workerd",
        idempotencyScope: "fleet-workerd",
        inputJson: inputJson(0),
        plan: firstPlan,
        requiredShardIds: ["shard-a", "shard-b"],
      }),
    ).resolves.toMatchObject({ created: false })

    const events = await env.DB.prepare(
      `SELECT "event_json" FROM "nozzle_audit_log"
       WHERE "environment_id" = 'workerd' ORDER BY "sequence"`,
    ).all<{ event_json: string }>()
    expect(events.results).toHaveLength(4)
    await expect(
      verifyAuditChain(
        events.results.map((row) => JSON.parse(row.event_json)),
        digest,
      ),
    ).resolves.toBe(true)

    const leases = new D1LeaseStore(env.DB)
    const acquired = await leases.acquire({
      acquisitionId: "workerd-operation-acquisition",
      holderId: "workerd-operation-controller",
      leaseKey: "workerd:operation",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await expect(
      store.beginStep({
        actorChecksum: "workerd-actor",
        attemptId: "workerd-attempt",
        idempotencyKey: "workerd-step-key-0",
        observedPreconditionChecksum: "precondition",
        operationId: firstPlan.operationId,
        proof,
        stepId: "provision",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
    await expect(
      store.failStep({
        actorChecksum: "workerd-actor",
        attemptId: "workerd-attempt",
        errorChecksum: "lost-response",
        operationId: firstPlan.operationId,
        outcome: "unknown",
        proof,
        stepId: "provision",
      }),
    ).resolves.toMatchObject({ steps: { provision: { state: "unknown" } } })
    await expect(
      store.reconcileStep({
        actorChecksum: "workerd-actor",
        evidenceChecksum: "observed-applied",
        observedPostconditionChecksum: "postcondition",
        operationId: firstPlan.operationId,
        outcome: "applied",
        proof,
        reconciliationId: "workerd-reconciliation",
        resultChecksum: "provider-result",
        stepId: "provision",
      }),
    ).resolves.toMatchObject({ steps: { provision: { state: "succeeded" } } })

    const crashedPlan = plans[1]
    if (!crashedPlan) throw new Error("Crash fixture plan is missing.")
    await store.beginStep({
      actorChecksum: "workerd-actor",
      attemptId: "workerd-crashed-attempt",
      idempotencyKey: "workerd-step-key-1",
      observedPreconditionChecksum: "precondition",
      operationId: crashedPlan.operationId,
      proof,
      stepId: "provision",
    })
    await leases.release({ proof })
    const recoveryLease = await leases.acquire({
      acquisitionId: "workerd-recovery-acquisition",
      holderId: "workerd-recovery-controller",
      leaseKey: "workerd:operation",
      ttlMs: 60_000,
    })
    if (!recoveryLease.acquired) throw new Error("Recovery lease acquisition failed.")
    const recoveryProof = leaseProof(recoveryLease.record)
    const recovered = await store.recoverRunningStep({
      actorChecksum: "workerd-recovery-actor",
      operationId: crashedPlan.operationId,
      proof: recoveryProof,
      recoveryId: "workerd-crash-recovery",
      stepId: "provision",
    })
    expect(recovered.steps.provision).toMatchObject({
      fencingToken: proof.fencingToken,
      lastAttemptId: "workerd-crashed-attempt",
      state: "unknown",
    })
    const retryable = await store.reconcileStep({
      actorChecksum: "workerd-recovery-actor",
      evidenceChecksum: "workerd-proven-absent",
      operationId: crashedPlan.operationId,
      outcome: "not_applied",
      proof: recoveryProof,
      reconciliationId: "workerd-not-applied",
      stepId: "provision",
    })
    expect(retryable.steps.provision?.state).toBe("retryable_failed")
    await expect(
      store.beginStep({
        actorChecksum: "workerd-recovery-actor",
        attemptId: "workerd-safe-retry",
        idempotencyKey: "workerd-step-key-1",
        observedPreconditionChecksum: "precondition",
        operationId: crashedPlan.operationId,
        proof: recoveryProof,
        stepId: "provision",
      }),
    ).resolves.toMatchObject({ disposition: "execute" })
  })
})
