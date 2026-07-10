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
import { invokeSagaEffectHandler } from "../src/saga-handler.js"
import { loadSagaInvocationInput, sealSagaInvocationInput } from "../src/saga-input.js"
import { sealSagaOperationPlan } from "../src/saga-plan.js"
import { sealSagaHandlerRegistry } from "../src/saga-registry.js"
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
  it("seals a deploy-time handler registry and complete conditional saga plan", async () => {
    const forwardAction = {
      actionId: "workerd.registry.forward",
      artifactChecksum: "1".repeat(64),
      version: 1,
    }
    const forwardObservation = {
      actionId: "workerd.registry.observe-forward",
      artifactChecksum: "2".repeat(64),
      version: 1,
    }
    const compensationAction = {
      actionId: "workerd.registry.compensate",
      artifactChecksum: "3".repeat(64),
      version: 1,
    }
    const compensationObservation = {
      actionId: "workerd.registry.observe-compensation",
      artifactChecksum: "4".repeat(64),
      version: 1,
    }
    const effect = () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" as const })
    const observation = () => ({
      evidenceJson: "{}",
      outputJson: "{}",
      state: "applied" as const,
    })
    const registry = await sealSagaHandlerRegistry(
      [
        { handler: effect, kind: "effect", reference: forwardAction },
        { handler: observation, kind: "observation", reference: forwardObservation },
        { handler: effect, kind: "effect", reference: compensationAction },
        { handler: observation, kind: "observation", reference: compensationObservation },
      ],
      digest,
    )
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "workerd-registry",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 10,
            compensationAction,
            compensationObservation,
            forwardAction,
            forwardObservation,
            inputSchemaChecksum: "5".repeat(64),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "6".repeat(64),
            stepId: "write",
            timeoutMs: 1_000,
          },
        ],
        version: 1,
      },
      digest,
    )
    const invocation = await sealSagaInvocationInput(
      {
        descriptor,
        inputJson: '{"request":"workerd"}',
        sagaId: "workerd-registry",
        stepInputJsons: { write: '{"value":9}' },
      },
      digest,
    )
    await expect(
      loadSagaInvocationInput(invocation.operationInputJson, descriptor, digest),
    ).resolves.toEqual(invocation)
    const plan = await sealSagaOperationPlan(
      {
        capabilitySnapshotChecksum: "7".repeat(64),
        descriptor,
        inputChecksum: invocation.inputChecksum,
        leaseKey: "saga:workerd-registry",
        operationId: "workerd-registry-operation",
        operationIdempotencyKey: "workerd-registry-operation-key",
        registry,
        sagaId: "workerd-registry",
        stepInputChecksums: invocation.stepInputChecksums,
      },
      digest,
    )
    expect(registry.manifest.manifestChecksum).toMatch(/^[0-9a-f]{64}$/u)
    expect(plan.steps).toHaveLength(4)
    expect(plan.steps.filter((step) => step.activation === "required")).toHaveLength(1)
    expect(plan.steps.filter((step) => step.effectProtocol === "saga_receipt")).toHaveLength(2)
    await expect(
      invokeSagaEffectHandler(registry.effect(forwardAction), {
        action: forwardAction,
        attemptId: "workerd-registry-attempt",
        idempotencyKey: "workerd-registry-action-key",
        inputJson: "{}",
        operationId: plan.operationId,
        phase: "forward",
        proof: {
          acquisitionId: "workerd-registry-acquisition",
          fencingToken: 1,
          holderId: "workerd-registry-controller",
          leaseKey: "saga:workerd-registry",
        },
        sagaId: "workerd-registry",
        stepId: "write",
        timeoutMs: 1_000,
      }),
    ).resolves.toEqual({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" })
  })

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
