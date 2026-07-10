import { env } from "cloudflare:workers"
import {
  type DigestFunction,
  leaseProof,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore, operationTransitionIdentity } from "../src/operation-store.js"
import { D1SagaAttemptStore, sagaActionInputChecksum } from "../src/saga-attempt-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  type SagaEffectContext,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
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

describe("real workerd D1 saga projection", () => {
  it("atomically advances a saga only through exact fenced operation transitions", async () => {
    const operationId = "workerd-saga-operation"
    const sagaId = "workerd-saga"
    const leaseKey = `saga:${sagaId}`
    const writeOperationStepId = sagaActionOperationStepId("write", "forward")
    const capabilitySnapshotJson = JSON.stringify({ runtime: "workerd-saga-v1" })
    const inputJson = JSON.stringify({ sagaId })
    const writeInputJson = '{"partition":"tenant-a","value":7}'
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: "workerd-saga-operation-key",
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId,
        operationType: "saga",
        steps: [SAGA_INIT_OPERATION_STEP_ID, writeOperationStepId].map((stepId) => ({
          checkpoint: "reversible" as const,
          dependsOn: [],
          effectProtocol:
            stepId === writeOperationStepId ? ("saga_receipt" as const) : ("opaque" as const),
          idempotencyKey: `workerd-saga:${stepId}:key`,
          inputChecksum: `workerd-saga:${stepId}:input`,
          leaseKey,
          postconditionChecksum: `workerd-saga:${stepId}:postcondition`,
          preconditionChecksum: `workerd-saga:${stepId}:precondition`,
          recoveryInstructions: "Resume from the durable saga projection.",
          retryClassification: "reconcile_first" as const,
          stepId,
        })),
      },
      digest,
    )
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const sagas = new D1SagaStore(env.DB, digest)
    const attempts = new D1SagaAttemptStore(env.DB, digest)
    await operations.create({
      actorChecksum: "workerd-saga-actor",
      capabilitySnapshotJson,
      environmentId: "workerd-saga",
      idempotencyScope: "workerd-saga",
      inputJson,
      plan,
      requiredShardIds: ["workerd-saga-shard"],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-saga-acquisition",
      holderId: "workerd-saga-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Saga lease acquisition failed.")
    const proof = leaseProof(acquired.record)

    const beginOperationStep = async (stepId: string, attemptId: string) => {
      await operations.beginStep({
        actorChecksum: "workerd-saga-actor",
        attemptId,
        idempotencyKey: `workerd-saga:${stepId}:key`,
        observedPreconditionChecksum: `workerd-saga:${stepId}:precondition`,
        operationId,
        proof,
        stepId,
      })
    }
    const context = (
      effectId: string,
      stepId: string,
      transitionId: string,
    ): SagaEffectContext => ({ effectId, operationId, proof, stepId, transitionId })

    const initAttempt = "workerd-saga:init:attempt"
    await beginOperationStep(SAGA_INIT_OPERATION_STEP_ID, initAttempt)
    await operations.completeStep({
      actorChecksum: "workerd-saga-actor",
      attemptId: initAttempt,
      observedPostconditionChecksum: `workerd-saga:${SAGA_INIT_OPERATION_STEP_ID}:postcondition`,
      operationId,
      proof,
      resultChecksum: "workerd-saga:init:result",
      stepId: SAGA_INIT_OPERATION_STEP_ID,
    })
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "workerd-write",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 10,
            compensationAction: {
              actionId: "write.compensate",
              artifactChecksum: "cc".repeat(32),
              version: 1,
            },
            compensationObservation: {
              actionId: "write.observe-compensation",
              artifactChecksum: "dd".repeat(32),
              version: 1,
            },
            forwardAction: {
              actionId: "write.forward",
              artifactChecksum: "aa".repeat(32),
              version: 1,
            },
            forwardObservation: {
              actionId: "write.observe-forward",
              artifactChecksum: "bb".repeat(32),
              version: 1,
            },
            inputSchemaChecksum: "11".repeat(32),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "22".repeat(32),
            stepId: "write",
            timeoutMs: 1_000,
          },
        ],
        version: 1,
      },
      digest,
    )
    let saga = await sagas.create({
      deadlineAtMs: 10_000,
      descriptor,
      effect: context(
        "workerd-saga:create",
        SAGA_INIT_OPERATION_STEP_ID,
        operationTransitionIdentity("succeeded", [
          operationId,
          SAGA_INIT_OPERATION_STEP_ID,
          initAttempt,
        ]),
      ),
      evidenceChecksum: "workerd-saga:create:evidence",
      idempotencyKey: "workerd-saga:idempotency",
      inputChecksum: "workerd-saga:input",
      sagaId,
      serverTimeMs: 1_000,
      stepInputChecksums: { write: await sagaActionInputChecksum(writeInputJson, digest) },
    })

    const writeAttempt = "workerd-saga:write:attempt"
    await beginOperationStep(writeOperationStepId, writeAttempt)
    saga = (
      await sagas.beginAction({
        attemptId: writeAttempt,
        effect: context(
          "workerd-saga:write:begin",
          writeOperationStepId,
          operationTransitionIdentity("accepted", [
            operationId,
            writeOperationStepId,
            writeAttempt,
          ]),
        ),
        evidenceChecksum: "workerd-saga:write:accepted",
        idempotencyKey: saga.steps.write?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId,
        serverTimeMs: 1_001,
        stepId: "write",
      })
    ).saga
    const accepted = await attempts.accept({
      attemptId: writeAttempt,
      inputJson: writeInputJson,
      phase: "forward",
      proof,
      purpose: "effect",
      sagaId,
      sagaStepId: "write",
    })
    const outcome = await attempts.complete({
      attemptId: writeAttempt,
      evidenceJson: '{"source":"workerd"}',
      outputJson: '{"written":true}',
      proof,
      state: "confirmed",
    })
    if (outcome.state !== "confirmed") throw new Error("Expected confirmed saga action receipt.")
    await operations.completeStep({
      actorChecksum: "workerd-saga-actor",
      attemptId: writeAttempt,
      observedPostconditionChecksum: `workerd-saga:${writeOperationStepId}:postcondition`,
      operationId,
      proof,
      resultChecksum: outcome.outcomeChecksum,
      stepId: writeOperationStepId,
    })
    saga = await sagas.recordActionSuccess({
      attemptId: writeAttempt,
      effect: context(
        "workerd-saga:write:success",
        writeOperationStepId,
        operationTransitionIdentity("succeeded", [operationId, writeOperationStepId, writeAttempt]),
      ),
      evidenceChecksum: outcome.outcomeChecksum,
      phase: "forward",
      resultChecksum: outcome.outputChecksum,
      sagaId,
      serverTimeMs: 1_002,
      stepId: "write",
    })

    expect(saga).toMatchObject({ stateVersion: 2, status: "succeeded" })
    expect(await sagas.get(sagaId)).toEqual(saga)
    const counts = await env.DB.prepare(
      `SELECT
         (SELECT count(*) FROM "nozzle_sagas" WHERE "saga_id" = ?1) AS "sagas",
         (SELECT count(*) FROM "nozzle_operation_effects"
          WHERE "resource_kind" = 'saga' AND "resource_id" = ?1) AS "effects",
         (SELECT count(*) FROM "nozzle_saga_action_attempts"
          WHERE "saga_id" = ?1) AS "attempts",
         (SELECT count(*) FROM "nozzle_saga_action_attempt_outcomes") AS "outcomes"`,
    )
      .bind(sagaId)
      .first<{ attempts: number; effects: number; outcomes: number; sagas: number }>()
    expect(accepted.state).toBe("accepted")
    expect(counts).toEqual({ attempts: 1, effects: 3, outcomes: 1, sagas: 1 })
  })
})
