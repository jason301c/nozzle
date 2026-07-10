import { env } from "cloudflare:workers"
import { type DigestFunction, leaseProof, sealOperationPlan } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore } from "../src/operation-store.js"
import { D1ProviderAttemptStore } from "../src/provider-attempt-store.js"
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

describe("real workerd provider receipts", () => {
  it("persists acceptance before a terminal provider outcome under the exact lease", async () => {
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const receipts = new D1ProviderAttemptStore(env.DB, digest)
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: "workerd-provider-capabilities",
        idempotencyKey: "workerd-provider-operation-key",
        inputChecksum: "workerd-provider-operation-input",
        operationId: "workerd-provider-operation",
        operationType: "provider-test",
        steps: [
          {
            checkpoint: "reversible",
            dependsOn: [],
            idempotencyKey: "workerd-provider-step-key",
            inputChecksum: "workerd-provider-step-input",
            leaseKey: "workerd:provider",
            postconditionChecksum: "workerd-provider-postcondition",
            preconditionChecksum: "workerd-provider-precondition",
            recoveryInstructions: "Inspect the durable provider receipt.",
            retryClassification: "reconcile_first",
            stepId: "provider-call",
          },
        ],
      },
      digest,
    )
    await operations.create({
      actorChecksum: "workerd-provider-actor",
      environmentId: "workerd-provider",
      idempotencyScope: "workerd-provider-scope",
      plan,
      requiredShardIds: [],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-provider-acquisition",
      holderId: "workerd-provider-controller",
      leaseKey: "workerd:provider",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await operations.beginStep({
      actorChecksum: "workerd-provider-actor",
      attemptId: "workerd-provider-attempt",
      idempotencyKey: "workerd-provider-step-key",
      observedPreconditionChecksum: "workerd-provider-precondition",
      operationId: plan.operationId,
      proof,
      stepId: "provider-call",
    })
    const accepted = await receipts.accept({
      actorChecksum: "workerd-provider-actor",
      attemptId: "workerd-provider-attempt",
      endpoint: "POST /accounts/{account_id}/d1/database",
      mutating: true,
      operationId: plan.operationId,
      proof,
      requestChecksum: "workerd-provider-request",
      stepId: "provider-call",
      targetChecksum: "workerd-provider-target",
    })
    expect(accepted).toMatchObject({ state: "accepted" })
    const completed = await receipts.complete({
      attemptId: accepted.attemptId,
      evidenceJson: '{"cfRay":"workerd-ray","status":200}',
      proof,
      resultJson: '{"uuid":"00000000-0000-4000-8000-000000000002"}',
      state: "confirmed",
    })
    expect(completed).toMatchObject({
      acceptanceChecksum: accepted.acceptanceChecksum,
      state: "confirmed",
    })
    const counts = await env.DB.prepare(
      `SELECT
         (SELECT count(*) FROM "nozzle_provider_attempts") AS "attempts",
         (SELECT count(*) FROM "nozzle_provider_attempt_outcomes") AS "outcomes"`,
    ).first<{ attempts: number; outcomes: number }>()
    expect(counts).toEqual({ attempts: 1, outcomes: 1 })
  })
})
