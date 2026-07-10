import { env } from "cloudflare:workers"
import { type DigestFunction, leaseProof, sealOperationPlan } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore } from "../src/operation-store.js"
import { type D1ResourceEffectContext, D1ResourceStore } from "../src/resource-store.js"
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

describe("real workerd D1 resource projection", () => {
  it("atomically links resource materialization to succeeded operation effects", async () => {
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const resources = new D1ResourceStore(env.DB, digest)

    const completedEffect = async (suffix: string): Promise<D1ResourceEffectContext> => {
      const operationId = `workerd-resource-operation-${suffix}`
      const inputJson = JSON.stringify({ resourceEffect: suffix })
      const capabilitySnapshotJson = JSON.stringify({ provider: "cloudflare-d1" })
      const plan = await sealOperationPlan(
        {
          capabilitySnapshotChecksum: await digest(
            new TextEncoder().encode(capabilitySnapshotJson),
          ),
          idempotencyKey: `workerd-resource-key-${suffix}`,
          inputChecksum: await digest(new TextEncoder().encode(inputJson)),
          operationId,
          operationType: "d1-resource-effect",
          steps: [
            {
              checkpoint: "reversible",
              dependsOn: [],
              effectProtocol: "opaque",
              idempotencyKey: `workerd-resource-step-key-${suffix}`,
              inputChecksum: `workerd-resource-step-input-${suffix}`,
              leaseKey: "workerd:resource",
              postconditionChecksum: `workerd-resource-postcondition-${suffix}`,
              preconditionChecksum: `workerd-resource-precondition-${suffix}`,
              recoveryInstructions: "Reconcile the D1 resource projection.",
              retryClassification: "reconcile_first",
              stepId: "materialize",
            },
          ],
        },
        digest,
      )
      await operations.create({
        actorChecksum: "workerd-resource-actor",
        capabilitySnapshotJson,
        environmentId: "workerd-resource",
        idempotencyScope: `workerd-resource-scope-${suffix}`,
        inputJson,
        plan,
        requiredShardIds: ["workerd-resource-shard"],
      })
      const acquired = await leases.acquire({
        acquisitionId: `workerd-resource-acquisition-${suffix}`,
        holderId: `workerd-resource-controller-${suffix}`,
        leaseKey: "workerd:resource",
        ttlMs: 60_000,
      })
      if (!acquired.acquired) throw new Error("Resource lease acquisition failed.")
      const proof = leaseProof(acquired.record)
      await operations.beginStep({
        actorChecksum: "workerd-resource-actor",
        attemptId: `workerd-resource-attempt-${suffix}`,
        idempotencyKey: `workerd-resource-step-key-${suffix}`,
        observedPreconditionChecksum: `workerd-resource-precondition-${suffix}`,
        operationId,
        proof,
        stepId: "materialize",
      })
      await operations.completeStep({
        actorChecksum: "workerd-resource-actor",
        attemptId: `workerd-resource-attempt-${suffix}`,
        observedPostconditionChecksum: `workerd-resource-postcondition-${suffix}`,
        operationId,
        proof,
        resultChecksum: `workerd-resource-result-${suffix}`,
        stepId: "materialize",
      })
      return {
        effectId: `workerd-resource-effect-${suffix}`,
        operationId,
        proof,
        stepId: "materialize",
      }
    }

    const planEffect = await completedEffect("plan")
    let resource = await resources.create({
      effect: planEffect,
      identity: {
        creationOperationId: planEffect.operationId,
        databaseName: "nozzle-workerd-resource",
        desiredJurisdiction: "global",
        environmentId: "workerd-resource",
        fleetId: "workerd-resource-fleet",
        generationId: "workerd-resource-generation",
        intentChecksum: "workerd-resource-intent",
        resourceId: "workerd-resource",
        shardId: "workerd-resource-shard",
        targetChecksum: "workerd-resource-target",
      },
    })
    await leases.release({ proof: planEffect.proof })

    const registerEffect = await completedEffect("register")
    resource = await resources.register({
      binding: {
        attributionEvidenceChecksum: "workerd-resource-attribution",
        databaseId: "22222222-3333-4444-8555-666666666666",
        databaseName: resource.databaseName,
        jurisdiction: resource.desiredJurisdiction,
        providerResultChecksum: "workerd-resource-provider-result",
      },
      effect: registerEffect,
      expectedStateVersion: resource.stateVersion,
      resourceId: resource.resourceId,
    })

    expect(resource).toMatchObject({
      binding: { databaseId: "22222222-3333-4444-8555-666666666666" },
      lifecycle: "registered",
      stateVersion: 1,
    })
    const counts = await env.DB.prepare(
      `SELECT
         (SELECT count(*) FROM "nozzle_d1_resources"
          WHERE "resource_id" = 'workerd-resource') AS "resources",
         (SELECT count(*) FROM "nozzle_operation_effects"
          WHERE "resource_id" = 'workerd-resource') AS "effects"`,
    ).first<{ effects: number; resources: number }>()
    expect(counts).toEqual({ effects: 2, resources: 1 })
  })
})
