import { env } from "cloudflare:workers"
import {
  type DigestFunction,
  leaseProof,
  sealIrreversibleAuthorization,
  sealOperationPlan,
  verifyAuditChain,
} from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { createInternalSagaOperationStore, D1OperationStore } from "../src/operation-store.js"
import { D1ProviderAttemptStore } from "../src/provider-attempt-store.js"
import { CONTROL_SCHEMA_STATEMENTS, CONTROL_SCHEMA_VERSION } from "../src/schema.js"

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

function canonicalJson(value: unknown): string {
  const canonical = (candidate: unknown): unknown => {
    if (Array.isArray(candidate)) return candidate.map(canonical)
    if (typeof candidate === "object" && candidate !== null) {
      const output: Record<string, unknown> = {}
      for (const key of Object.keys(candidate).sort()) {
        output[key] = canonical((candidate as Record<string, unknown>)[key])
      }
      return output
    }
    return candidate
  }
  return JSON.stringify(canonical(value))
}

beforeAll(async () => {
  for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
})

describe("real workerd operation ledger", () => {
  it("retains an irreversible provider authorization across a real D1 restart", async () => {
    await expect(
      env.DB.prepare(
        `SELECT "schema_version" FROM "nozzle_control_schema_versions"
         WHERE "schema_version" = ?1`,
      )
        .bind(CONTROL_SCHEMA_VERSION)
        .first(),
    ).resolves.toEqual({ schema_version: CONTROL_SCHEMA_VERSION })

    const operationId = "workerd-irreversible-provider-operation"
    const stepId = "provider-call"
    const attemptId = "workerd-irreversible-provider-attempt"
    const leaseKey = "workerd:irreversible-provider"
    const capabilitySnapshotJson = '{"runtime":"workerd-irreversible-provider-v1"}'
    const inputJson = '{"operation":"create-d1-database"}'
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: "workerd-irreversible-provider-operation-key",
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId,
        operationType: "workerd-irreversible-provider-test",
        steps: [
          {
            checkpoint: "irreversible",
            dependsOn: [],
            effectProtocol: "provider_receipt",
            idempotencyKey: "workerd-irreversible-provider-step-key",
            inputChecksum: "workerd-irreversible-provider-step-input",
            leaseKey,
            postconditionChecksum: "workerd-irreversible-provider-postcondition",
            preconditionChecksum: "workerd-irreversible-provider-precondition",
            recoveryInstructions: "Inspect the durable authorization and provider receipt.",
            retryClassification: "reconcile_first",
            stepId,
          },
        ],
      },
      digest,
    )
    const operations = new D1OperationStore(env.DB, digest)
    await operations.create({
      actorChecksum: "workerd-irreversible-provider-actor",
      capabilitySnapshotJson,
      environmentId: "workerd-irreversible-provider",
      idempotencyScope: "workerd-irreversible-provider",
      inputJson,
      plan,
      requiredShardIds: [],
    })
    const leases = new D1LeaseStore(env.DB)
    const acquired = await leases.acquire({
      acquisitionId: "workerd-irreversible-provider-acquisition",
      holderId: "workerd-irreversible-provider-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Irreversible provider lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const authorized = await leases.authorizeAt(proof)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "workerd-irreversible-provider-actor",
        authorizationId: "workerd-irreversible-provider-authorization",
        decisionChecksum: "workerd-irreversible-provider-approved",
        lease: authorized.record,
        leaseProof: proof,
        sealedAtServerTimeMs: authorized.serverTimeMs,
        stepId,
      },
      digest,
    )
    await expect(
      operations.beginStep({
        actorChecksum: "workerd-irreversible-provider-actor",
        attemptId,
        idempotencyKey: "workerd-irreversible-provider-step-key",
        irreversibleAuthorization: authorization,
        observedPreconditionChecksum: "workerd-irreversible-provider-precondition",
        operationId,
        proof,
        stepId,
      }),
    ).resolves.toMatchObject({ disposition: "execute" })

    const restarted = new D1OperationStore(env.DB, digest)
    const loaded = await restarted.get(operationId)
    const loadedStep = loaded?.operation.steps[stepId]
    expect(loadedStep?.irreversibleAuthorization).toEqual(authorization)
    expect(loadedStep?.irreversibleAuthorization).not.toBe(authorization)
    expect(Object.isFrozen(loadedStep)).toBe(true)
    expect(Object.isFrozen(loadedStep?.irreversibleAuthorization)).toBe(true)

    const durable = await env.DB.prepare(
      `SELECT "step"."record_json", "transition"."created_at_ms",
              "transition"."to_record_json",
              "transition"."transition_id"
       FROM "nozzle_operation_steps" AS "step"
       JOIN "nozzle_operation_transitions" AS "transition"
         ON "transition"."operation_id" = "step"."operation_id"
        AND "transition"."step_id" = "step"."step_id"
       WHERE "step"."operation_id" = ?1 AND "step"."step_id" = ?2`,
    )
      .bind(operationId, stepId)
      .first<{
        created_at_ms: number
        record_json: string
        to_record_json: string
        transition_id: string
      }>()
    if (!durable) throw new Error("Irreversible transition fixture is missing.")
    expect(durable.to_record_json).toBe(durable.record_json)
    expect(durable.record_json).toBe(canonicalJson(JSON.parse(durable.record_json)))
    expect(JSON.parse(durable.record_json)).toMatchObject({
      authorizationChecksum: authorization.authorizationChecksum,
      irreversibleAuthorization: authorization,
      state: "running",
    })
    await expect(
      env.DB.prepare(
        `SELECT "transition_id", "authorization_id", "authorization_checksum",
                "protocol_version", "classified_at_ms"
         FROM "nozzle_irreversible_authorization_receipts"
         WHERE "transition_id" = ?1`,
      )
        .bind(durable.transition_id)
        .first(),
    ).resolves.toEqual({
      authorization_checksum: authorization.authorizationChecksum,
      authorization_id: authorization.authorizationId,
      classified_at_ms: durable.created_at_ms,
      protocol_version: 2,
      transition_id: durable.transition_id,
    })

    const providerAttempts = new D1ProviderAttemptStore(env.DB, digest)
    const accepted = await providerAttempts.accept({
      actorChecksum: "workerd-irreversible-provider-actor",
      attemptId,
      endpoint: "POST /accounts/{account_id}/d1/database",
      mutating: true,
      operationId,
      purpose: "effect",
      proof,
      requestChecksum: "workerd-irreversible-provider-request",
      stepId,
      targetChecksum: "workerd-irreversible-provider-target",
    })
    expect(accepted).toMatchObject({
      attemptId,
      operationId,
      state: "accepted",
      stepId,
    })
    const outcome = await providerAttempts.complete({
      attemptId,
      evidenceJson: '{"cfRay":"workerd-irreversible-ray","status":200}',
      proof,
      resultJson: '{"uuid":"00000000-0000-4000-8000-000000000003"}',
      state: "confirmed",
    })
    if (outcome.state !== "confirmed") throw new Error("Provider outcome was not confirmed.")
    const completed = await restarted.completeStep({
      actorChecksum: "workerd-irreversible-provider-actor",
      attemptId,
      observedPostconditionChecksum: "workerd-irreversible-provider-postcondition",
      operationId,
      proof,
      resultChecksum: outcome.outcomeChecksum,
      stepId,
    })
    expect(completed.steps[stepId]).toMatchObject({
      authorizationChecksum: authorization.authorizationChecksum,
      irreversibleAuthorization: authorization,
      resultChecksum: outcome.outcomeChecksum,
      state: "succeeded",
    })

    const terminal = await env.DB.prepare(
      `SELECT "step"."record_json", "transition"."from_record_json",
              "transition"."to_record_json"
       FROM "nozzle_operation_steps" AS "step"
       JOIN "nozzle_operation_transitions" AS "transition"
         ON "transition"."operation_id" = "step"."operation_id"
        AND "transition"."step_id" = "step"."step_id"
        AND "transition"."to_record_json" = "step"."record_json"
       WHERE "step"."operation_id" = ?1 AND "step"."step_id" = ?2`,
    )
      .bind(operationId, stepId)
      .first<{
        from_record_json: string
        record_json: string
        to_record_json: string
      }>()
    if (!terminal) throw new Error("Terminal irreversible transition fixture is missing.")
    expect(terminal.to_record_json).toBe(terminal.record_json)
    for (const recordJson of [terminal.from_record_json, terminal.to_record_json]) {
      expect(recordJson).toBe(canonicalJson(JSON.parse(recordJson)))
      expect(JSON.parse(recordJson)).toMatchObject({
        authorizationChecksum: authorization.authorizationChecksum,
        irreversibleAuthorization: authorization,
      })
    }
    await expect(
      env.DB.prepare(
        `SELECT count(*) AS "count" FROM "nozzle_irreversible_authorization_receipts"
         WHERE "transition_id" IN (
           SELECT "transition_id" FROM "nozzle_operation_transitions"
           WHERE "operation_id" = ?1 AND "step_id" = ?2
         )`,
      )
        .bind(operationId, stepId)
        .first(),
    ).resolves.toEqual({ count: 1 })
  })

  it("persists an unused conditional path without fabricating an attempt", async () => {
    const store = createInternalSagaOperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const capabilitySnapshotJson = '{"runtime":"workerd-conditional-v1"}'
    const inputJson = '{"branch":"required"}'
    const operationId = "workerd-conditional-operation"
    const leaseKey = "workerd:conditional"
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: "workerd-conditional-operation-key",
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId,
        operationType: "workerd-conditional-test",
        steps: [
          {
            checkpoint: "reversible",
            idempotencyKey: "workerd-required-key",
            inputChecksum: "workerd-required-input",
            leaseKey,
            postconditionChecksum: "workerd-required-postcondition",
            preconditionChecksum: "workerd-required-precondition",
            recoveryInstructions: "Execute the required branch.",
            retryClassification: "idempotent",
            stepId: "required",
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            effectProtocol: "saga_receipt",
            idempotencyKey: "workerd-conditional-key",
            inputChecksum: "workerd-conditional-input",
            leaseKey,
            postconditionChecksum: "workerd-conditional-postcondition",
            preconditionChecksum: "workerd-conditional-precondition",
            recoveryInstructions: "Use the sealed branch decision.",
            retryClassification: "reconcile_first",
            stepId: "conditional",
          },
        ],
      },
      digest,
    )
    await store.create({
      actorChecksum: "workerd-conditional-actor",
      capabilitySnapshotJson,
      environmentId: "workerd-conditional",
      idempotencyScope: "workerd-conditional",
      inputJson,
      plan,
      requiredShardIds: ["workerd-conditional-shard"],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-conditional-acquisition",
      holderId: "workerd-conditional-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Conditional lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await expect(
      store.markStepNotRequired({
        actorChecksum: "workerd-conditional-actor",
        decisionId: "workerd-conditional-decision",
        evidenceChecksum: "workerd-terminal-projection",
        operationId,
        proof,
        stepId: "conditional",
      }),
    ).resolves.toMatchObject({ steps: { conditional: { state: "not_required" } } })
    await expect(
      env.DB.prepare(
        `SELECT "state", "fencing_token" FROM "nozzle_operation_steps"
         WHERE "operation_id" = ?1 AND "step_id" = 'conditional'`,
      )
        .bind(operationId)
        .first(),
    ).resolves.toEqual({ fencing_token: null, state: "not_required" })
  })

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
                effectProtocol: index === 1 ? "provider_receipt" : "opaque",
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
      state: "retryable_failed",
    })
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
