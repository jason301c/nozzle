import { env } from "cloudflare:workers"
import {
  type DigestFunction,
  leaseProof,
  sealIrreversibleAuthorization,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { D1LeaseStore } from "../src/lease-store.js"
import {
  D1OperationStore,
  operationStepRecordJson,
  operationTransitionIdentity,
} from "../src/operation-store.js"
import { D1SagaAttemptStore, sagaActionInputChecksum } from "../src/saga-attempt-store.js"
import { D1SagaCoordinatorStore } from "../src/saga-coordinator-store.js"
import { invokeSagaEffectHandler } from "../src/saga-handler.js"
import {
  D1SagaHistoryReader,
  type SagaHistoryAttemptCursor,
  type SagaHistoryTransitionCursor,
} from "../src/saga-history.js"
import {
  finalizeSagaHistoryProof,
  SagaHistoryAttemptFolder,
  SagaHistoryAuditFolder,
  SagaHistoryEffectFolder,
  SagaHistoryTransitionFolder,
} from "../src/saga-history-fold.js"
import { loadSagaInvocationInput, sealSagaInvocationInput } from "../src/saga-input.js"
import { sealSagaOperationPlan } from "../src/saga-plan.js"
import { sealSagaHandlerRegistry } from "../src/saga-registry.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  type SagaEffectContext,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
import { loadSagaTerminalCapability, mintSagaTerminalCapability } from "../src/saga-terminal.js"
import { D1SagaTerminalStore } from "../src/saga-terminal-store.js"
import {
  CONTROL_SCHEMA_STATEMENTS,
  CONTROL_SCHEMA_VERSION_ONE_STATEMENTS,
  CONTROL_SCHEMA_VERSION_TWO_STATEMENTS,
} from "../src/schema.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
      UPGRADE_DB: D1Database
      V1_UPGRADE_DB: D1Database
    }
  }
}

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

async function reconcileStoredSagaHistory(operationId: string, sagaId: string) {
  const reader = new D1SagaHistoryReader(env.DB)
  const anchor = await reader.captureAnchor(operationId, sagaId)
  const plan = await reader.operationPlan(anchor, digest)
  const descriptor = await reader.sagaDescriptor(anchor, digest)
  const audit = new SagaHistoryAuditFolder(anchor, digest)
  let auditCursor = 0
  for (;;) {
    const page = await reader.auditPage(anchor, auditCursor)
    await audit.append(page)
    if (page.complete) break
    auditCursor = page.nextCursor as number
  }
  const transition = new SagaHistoryTransitionFolder(anchor, audit.proof(), plan, digest)
  let transitionCursor: SagaHistoryTransitionCursor | undefined
  for (;;) {
    const page = await reader.transitionPage(anchor, transitionCursor)
    await transition.append(page)
    if (page.complete) break
    transitionCursor = page.nextCursor as SagaHistoryTransitionCursor
  }
  const effect = new SagaHistoryEffectFolder(anchor, digest)
  let effectCursor: number | undefined
  for (;;) {
    const page = await reader.effectPage(anchor, effectCursor)
    await effect.append(page)
    if (page.complete) break
    effectCursor = page.nextCursor as number
  }
  const attempt = new SagaHistoryAttemptFolder(anchor, reader, digest)
  let attemptCursor: SagaHistoryAttemptCursor | undefined
  for (;;) {
    const page = await reader.attemptIdentityPage(anchor, attemptCursor)
    await attempt.append(page)
    if (page.complete) break
    attemptCursor = page.nextCursor as SagaHistoryAttemptCursor
  }
  return finalizeSagaHistoryProof(reader, anchor, transition, effect, attempt, plan, descriptor)
}

beforeAll(async () => {
  for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
  await env.DB.prepare(
    `INSERT INTO "nozzle_saga_outcome_payload_activations"
     ("protocol_version", "reader_barrier_checksum", "activated_at_ms")
     VALUES (1, ?1, 1)`,
  )
    .bind("7".repeat(64))
    .run()
})

describe("real workerd D1 saga projection", () => {
  it("upgrades and reruns the complete historical version-one schema artifact", async () => {
    for (const statement of CONTROL_SCHEMA_VERSION_ONE_STATEMENTS) {
      await env.V1_UPGRADE_DB.prepare(statement).run()
    }
    const beforeIdentity = await env.V1_UPGRADE_DB.prepare(
      `SELECT "schema_version", "installed_at_ms" FROM "nozzle_control_meta"`,
    ).first()
    const legacyGuards = await env.V1_UPGRADE_DB.prepare(
      `SELECT "name" FROM "sqlite_schema"
       WHERE "type" = 'trigger'
         AND "name" IN ('nozzle_control_saga_attempt_insert',
                        'nozzle_control_saga_outcome_insert')
       ORDER BY "name"`,
    ).all()
    expect(legacyGuards.results).toEqual([
      { name: "nozzle_control_saga_attempt_insert" },
      { name: "nozzle_control_saga_outcome_insert" },
    ])
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.V1_UPGRADE_DB.prepare(statement).run()
    }
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.V1_UPGRADE_DB.prepare(statement).run()
    }

    const identity = await env.V1_UPGRADE_DB.prepare(
      `SELECT "schema_version", "installed_at_ms" FROM "nozzle_control_meta"`,
    ).first()
    const versions = await env.V1_UPGRADE_DB.prepare(
      `SELECT "schema_version" FROM "nozzle_control_schema_versions"
       ORDER BY "schema_version"`,
    ).all()
    const replacementGuards = await env.V1_UPGRADE_DB.prepare(
      `SELECT "name" FROM "sqlite_schema"
       WHERE "type" = 'trigger'
         AND "name" IN ('nozzle_control_saga_attempt_insert',
                        'nozzle_control_saga_attempt_insert_v2',
                        'nozzle_control_saga_outcome_insert',
                        'nozzle_control_saga_outcome_insert_v2')
       ORDER BY "name"`,
    ).all()
    expect(identity).toEqual(beforeIdentity)
    expect(versions.results).toEqual([
      { schema_version: 1 },
      { schema_version: 2 },
      { schema_version: 3 },
      { schema_version: 4 },
    ])
    expect(replacementGuards.results).toEqual([
      { name: "nozzle_control_saga_attempt_insert_v2" },
      { name: "nozzle_control_saga_outcome_insert_v2" },
    ])
  })

  it("upgrades and reruns the complete historical version-two schema artifact", async () => {
    for (const statement of CONTROL_SCHEMA_VERSION_TWO_STATEMENTS) {
      await env.UPGRADE_DB.prepare(statement).run()
    }
    const beforeIdentity = await env.UPGRADE_DB.prepare(
      `SELECT "schema_version", "installed_at_ms" FROM "nozzle_control_meta"`,
    ).first()
    const versionTwoGuards = await env.UPGRADE_DB.prepare(
      `SELECT "name" FROM "sqlite_schema"
       WHERE "type" = 'trigger'
         AND "name" IN ('nozzle_control_saga_attempt_insert_v2',
                        'nozzle_control_saga_outcome_insert_v2')
       ORDER BY "name"`,
    ).all()
    expect(versionTwoGuards.results).toEqual([
      { name: "nozzle_control_saga_attempt_insert_v2" },
      { name: "nozzle_control_saga_outcome_insert_v2" },
    ])
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.UPGRADE_DB.prepare(statement).run()
    }
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.UPGRADE_DB.prepare(statement).run()
    }

    const identity = await env.UPGRADE_DB.prepare(
      `SELECT "schema_version", "installed_at_ms" FROM "nozzle_control_meta"`,
    ).first()
    const versions = await env.UPGRADE_DB.prepare(
      `SELECT "schema_version" FROM "nozzle_control_schema_versions"
       ORDER BY "schema_version"`,
    ).all()
    const replacementGuards = await env.UPGRADE_DB.prepare(
      `SELECT "name" FROM "sqlite_schema"
       WHERE "type" = 'trigger'
         AND "name" IN ('nozzle_control_saga_attempt_insert',
                        'nozzle_control_saga_attempt_insert_v2',
                        'nozzle_control_saga_outcome_insert',
                        'nozzle_control_saga_outcome_insert_v2',
                        'nozzle_control_saga_protocol_insert_v2',
                        'nozzle_control_saga_protocol_binding_insert_v2',
                        'nozzle_control_saga_protocol_action_insert_v2',
                        'nozzle_control_saga_protocol_observation_insert_v2',
                        'nozzle_control_saga_protocol_compensation_insert_v2',
                        'nozzle_control_irreversible_authorization_body_unpublished_v3',
                        'nozzle_control_irreversible_authorization_body_shape_v3',
                        'nozzle_control_irreversible_authorization_body_plan_v3',
                        'nozzle_control_irreversible_authorization_body_fence_v3',
                        'nozzle_control_irreversible_authorization_dispatch_v3',
                        'nozzle_control_irreversible_authorization_retry_v3',
                        'nozzle_control_irreversible_authorization_preserve_v3',
                        'nozzle_control_irreversible_authorization_receipt_insert_v3',
                        'nozzle_control_irreversible_authorization_receipt_classify_v3',
                        'nozzle_control_irreversible_saga_attempt_v3',
                        'nozzle_control_irreversible_provider_attempt_v3')
       ORDER BY "name"`,
    ).all()
    expect(identity).toEqual(beforeIdentity)
    expect(versions.results).toEqual([
      { schema_version: 1 },
      { schema_version: 2 },
      { schema_version: 3 },
      { schema_version: 4 },
    ])
    expect(replacementGuards.results).toEqual([
      { name: "nozzle_control_irreversible_authorization_body_fence_v3" },
      { name: "nozzle_control_irreversible_authorization_body_plan_v3" },
      { name: "nozzle_control_irreversible_authorization_body_shape_v3" },
      { name: "nozzle_control_irreversible_authorization_body_unpublished_v3" },
      { name: "nozzle_control_irreversible_authorization_dispatch_v3" },
      { name: "nozzle_control_irreversible_authorization_preserve_v3" },
      { name: "nozzle_control_irreversible_authorization_receipt_classify_v3" },
      { name: "nozzle_control_irreversible_authorization_receipt_insert_v3" },
      { name: "nozzle_control_irreversible_authorization_retry_v3" },
      { name: "nozzle_control_irreversible_provider_attempt_v3" },
      { name: "nozzle_control_irreversible_saga_attempt_v3" },
      { name: "nozzle_control_saga_attempt_insert_v2" },
      { name: "nozzle_control_saga_outcome_insert_v2" },
      { name: "nozzle_control_saga_protocol_action_insert_v2" },
      { name: "nozzle_control_saga_protocol_binding_insert_v2" },
      { name: "nozzle_control_saga_protocol_compensation_insert_v2" },
      { name: "nozzle_control_saga_protocol_insert_v2" },
      { name: "nozzle_control_saga_protocol_observation_insert_v2" },
    ])
  })

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
    expect(plan.steps).toHaveLength(5)
    expect(plan.steps.filter((step) => step.activation === "required")).toHaveLength(2)
    expect(plan.steps.filter((step) => step.completionRole === "settlement")).toHaveLength(1)
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

  it("verifies an authorized irreversible saga through its real trigger receipt", async () => {
    const operationId = "workerd-irreversible-saga-operation"
    const sagaId = "workerd-irreversible-saga"
    const sagaStepId = "commit"
    const operationStepId = sagaActionOperationStepId(sagaStepId, "forward")
    const attemptId = `${sagaId}:${sagaStepId}:forward:1`
    const leaseKey = `saga:${sagaId}`
    const actorChecksum = "workerd-irreversible-saga-actor"
    const forwardAction = {
      actionId: "workerd.irreversible-saga.commit",
      artifactChecksum: "8".repeat(64),
      version: 1,
    }
    const forwardObservation = {
      actionId: "workerd.irreversible-saga.observe-commit",
      artifactChecksum: "9".repeat(64),
      version: 1,
    }
    const registry = await sealSagaHandlerRegistry(
      [
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" }),
          kind: "effect",
          reference: forwardAction,
        },
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" }),
          kind: "observation",
          reference: forwardObservation,
        },
      ],
      digest,
    )
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "workerd-irreversible-saga",
        steps: [
          {
            authorizationPolicyChecksum: "a".repeat(64),
            baseRetryDelayMs: 10,
            compensationAction: null,
            compensationObservation: null,
            forwardAction,
            forwardObservation,
            inputSchemaChecksum: "b".repeat(64),
            irreversible: true,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "c".repeat(64),
            stepId: sagaStepId,
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
        inputJson: '{"request":"commit"}',
        sagaId,
        stepInputJsons: { [sagaStepId]: '{"resource":"workerd"}' },
      },
      digest,
    )
    const capabilitySnapshotJson = '{"runtime":"workerd-irreversible-saga-v1"}'
    const plan = await sealSagaOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        descriptor,
        inputChecksum: invocation.inputChecksum,
        leaseKey,
        operationId,
        operationIdempotencyKey: "workerd-irreversible-saga-operation-key",
        registry,
        sagaId,
        stepInputChecksums: invocation.stepInputChecksums,
      },
      digest,
    )
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const attempts = new D1SagaAttemptStore(env.DB, digest)
    const coordinator = new D1SagaCoordinatorStore(env.DB, digest)
    await operations.create({
      actorChecksum,
      capabilitySnapshotJson,
      environmentId: "workerd-irreversible-saga",
      idempotencyScope: "workerd-irreversible-saga",
      inputJson: invocation.operationInputJson,
      plan,
      requiredShardIds: ["workerd-irreversible-saga-shard"],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-irreversible-saga-acquisition",
      holderId: "workerd-irreversible-saga-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Irreversible saga lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const initPlan = plan.steps.find((step) => step.stepId === SAGA_INIT_OPERATION_STEP_ID)
    if (initPlan === undefined) throw new Error("Irreversible saga init plan is missing.")
    const initAttemptId = `${sagaId}:init:1`
    await operations.beginStep({
      actorChecksum,
      attemptId: initAttemptId,
      idempotencyKey: initPlan.idempotencyKey,
      observedPreconditionChecksum: initPlan.preconditionChecksum,
      operationId,
      proof,
      stepId: initPlan.stepId,
    })
    await coordinator.initializeSaga({
      actorChecksum,
      attemptId: initAttemptId,
      deadlineAtMs: 8_000_000_000_000_000,
      descriptor,
      evidenceChecksum: "workerd-irreversible-saga-init-evidence",
      idempotencyKey: "workerd-irreversible-saga-key",
      inputChecksum: invocation.inputChecksum,
      observedPostconditionChecksum: initPlan.postconditionChecksum,
      operationId,
      proof,
      resultChecksum: "workerd-irreversible-saga-init-result",
      sagaId,
      stepInputChecksums: invocation.stepInputChecksums,
    })
    const authorized = await leases.authorizeAt(proof)
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum,
        authorizationId: "workerd-irreversible-saga-authorization",
        decisionChecksum: "workerd-irreversible-saga-approved",
        lease: authorized.record,
        leaseProof: proof,
        sealedAtServerTimeMs: authorized.serverTimeMs,
        stepId: operationStepId,
      },
      digest,
    )
    const actionInput = {
      actorChecksum,
      attemptId,
      operationId,
      phase: "forward" as const,
      proof,
      sagaId,
      stepId: sagaStepId,
    }
    const begun = await coordinator.beginAction({
      ...actionInput,
      irreversibleAuthorization: authorization,
    })
    expect(begun).toMatchObject({ disposition: "execute" })

    const transitionId = operationTransitionIdentity("accepted", [
      operationId,
      operationStepId,
      attemptId,
    ])
    const durable = await env.DB.prepare(
      `SELECT "transition"."created_at_ms", "transition"."to_record_json",
              "step"."record_json", "receipt"."transition_id",
              "receipt"."authorization_id", "receipt"."authorization_checksum",
              "receipt"."protocol_version", "receipt"."classified_at_ms"
       FROM "nozzle_operation_transitions" AS "transition"
       JOIN "nozzle_operation_steps" AS "step"
         ON "step"."operation_id" = "transition"."operation_id"
        AND "step"."step_id" = "transition"."step_id"
       JOIN "nozzle_irreversible_authorization_receipts" AS "receipt"
         ON "receipt"."transition_id" = "transition"."transition_id"
       WHERE "transition"."transition_id" = ?1`,
    )
      .bind(transitionId)
      .first<{
        authorization_checksum: string
        authorization_id: string
        classified_at_ms: number
        created_at_ms: number
        protocol_version: number
        record_json: string
        to_record_json: string
        transition_id: string
      }>()
    if (durable === null) throw new Error("Irreversible saga authorization receipt is missing.")
    const current = await operations.get(operationId)
    const currentStep = current?.operation.steps[operationStepId]
    if (currentStep === undefined) throw new Error("Irreversible saga operation step is missing.")
    const currentRecordJson = operationStepRecordJson(currentStep)
    expect(currentStep.irreversibleAuthorization).toEqual(authorization)
    expect(currentStep).toMatchObject({
      authorizationChecksum: authorization.authorizationChecksum,
      state: "running",
    })
    expect(durable).toEqual({
      authorization_checksum: authorization.authorizationChecksum,
      authorization_id: authorization.authorizationId,
      classified_at_ms: durable.created_at_ms,
      created_at_ms: durable.created_at_ms,
      protocol_version: 2,
      record_json: currentRecordJson,
      to_record_json: currentRecordJson,
      transition_id: transitionId,
    })

    const restarted = new D1SagaCoordinatorStore(env.DB, digest)
    await expect(restarted.beginAction(actionInput)).resolves.toEqual({
      disposition: "in_progress",
      saga: begun.saga,
    })
    await expect(
      attempts.accept({
        attemptId,
        inputJson: invocation.stepInputJsons[sagaStepId] as string,
        phase: "forward",
        proof,
        purpose: "effect",
        sagaId,
        sagaStepId,
      }),
    ).resolves.toMatchObject({
      attemptId,
      operationId,
      operationStepId,
      protocolVersion: 2,
      state: "accepted",
    })
    await attempts.complete({
      attemptId,
      evidenceJson: '{"source":"workerd-irreversible"}',
      outputJson: '{"committed":true}',
      proof,
      state: "confirmed",
    })
    await expect(restarted.settleActionFromReceipt(actionInput)).resolves.toMatchObject({
      status: "succeeded",
    })
    const finalProof = await reconcileStoredSagaHistory(operationId, sagaId)
    const capability = mintSagaTerminalCapability(finalProof)
    expect(loadSagaTerminalCapability(capability)).toMatchObject({
      branchDecisions: [{ kind: "not_required", stepId: "saga:termination" }],
      operation: {
        steps: {
          [operationStepId]: {
            authorizationChecksum: authorization.authorizationChecksum,
            irreversibleAuthorization: authorization,
          },
        },
      },
      settlementOutcome: "succeeded",
    })
    const settled = await new D1SagaTerminalStore(env.DB, digest).persistTerminalTail({
      actorChecksum: "workerd-irreversible-terminal-actor",
      capability,
      proof,
    })
    expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
    const settledProof = await reconcileStoredSagaHistory(operationId, sagaId)
    expect(
      loadSagaTerminalCapability(mintSagaTerminalCapability(settledProof)).branchDecisions,
    ).toEqual([])
  })

  it("atomically couples an observed unknown action across both real D1 ledgers", async () => {
    const operationId = "workerd-coordinator-operation"
    const sagaId = "workerd-coordinator-saga"
    const leaseKey = `saga:${sagaId}`
    const forwardAction = {
      actionId: "workerd.coordinator.forward",
      artifactChecksum: "a".repeat(64),
      version: 1,
    }
    const forwardObservation = {
      actionId: "workerd.coordinator.observe-forward",
      artifactChecksum: "b".repeat(64),
      version: 1,
    }
    const compensationAction = {
      actionId: "workerd.coordinator.compensate",
      artifactChecksum: "c".repeat(64),
      version: 1,
    }
    const compensationObservation = {
      actionId: "workerd.coordinator.observe-compensation",
      artifactChecksum: "d".repeat(64),
      version: 1,
    }
    const registry = await sealSagaHandlerRegistry(
      [
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" }),
          kind: "effect",
          reference: forwardAction,
        },
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" }),
          kind: "observation",
          reference: forwardObservation,
        },
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" }),
          kind: "effect",
          reference: compensationAction,
        },
        {
          handler: () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" }),
          kind: "observation",
          reference: compensationObservation,
        },
      ],
      digest,
    )
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "workerd-coordinator",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 10,
            compensationAction,
            compensationObservation,
            forwardAction,
            forwardObservation,
            inputSchemaChecksum: "1".repeat(64),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "2".repeat(64),
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
        inputJson: '{"request":"coordinated"}',
        sagaId,
        stepInputJsons: { write: '{"value":42}' },
      },
      digest,
    )
    const capabilitySnapshotJson = '{"runtime":"workerd-coordinator-v1"}'
    const plan = await sealSagaOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        descriptor,
        inputChecksum: invocation.inputChecksum,
        leaseKey,
        operationId,
        operationIdempotencyKey: "workerd-coordinator-operation-key",
        registry,
        sagaId,
        stepInputChecksums: invocation.stepInputChecksums,
      },
      digest,
    )
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const attempts = new D1SagaAttemptStore(env.DB, digest)
    const coordinator = new D1SagaCoordinatorStore(env.DB, digest)
    await operations.create({
      actorChecksum: "workerd-coordinator-actor",
      capabilitySnapshotJson,
      environmentId: "workerd-coordinator",
      idempotencyScope: "workerd-coordinator",
      inputJson: invocation.operationInputJson,
      plan,
      requiredShardIds: ["workerd-coordinator-shard"],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-coordinator-acquisition",
      holderId: "workerd-coordinator-controller",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Coordinator lease acquisition failed.")
    let proof = leaseProof(acquired.record)
    const initAttemptId = `${sagaId}:init:1`
    const initPlan = plan.steps.find((step) => step.stepId === SAGA_INIT_OPERATION_STEP_ID)
    if (initPlan === undefined) throw new Error("Coordinator init plan is missing.")
    await operations.beginStep({
      actorChecksum: "workerd-coordinator-actor",
      attemptId: initAttemptId,
      idempotencyKey: initPlan.idempotencyKey,
      observedPreconditionChecksum: initPlan.preconditionChecksum,
      operationId,
      proof,
      stepId: initPlan.stepId,
    })
    const initialized = await coordinator.initializeSaga({
      actorChecksum: "workerd-coordinator-actor",
      attemptId: initAttemptId,
      deadlineAtMs: 8_000_000_000_000_000,
      descriptor,
      evidenceChecksum: "workerd-coordinator-init-evidence",
      idempotencyKey: "workerd-coordinator-saga-key",
      inputChecksum: invocation.inputChecksum,
      observedPostconditionChecksum: initPlan.postconditionChecksum,
      operationId,
      proof,
      resultChecksum: "workerd-coordinator-init-result",
      sagaId,
      stepInputChecksums: invocation.stepInputChecksums,
    })
    const actionAttemptId = `${sagaId}:write:forward:1`
    const begun = await coordinator.beginAction({
      actorChecksum: "workerd-coordinator-actor",
      attemptId: actionAttemptId,
      operationId,
      phase: "forward",
      proof,
      sagaId,
      stepId: "write",
    })
    expect(initialized).toMatchObject({ stateVersion: 0, status: "planned" })
    expect(begun).toMatchObject({ disposition: "execute" })
    await attempts.accept({
      attemptId: actionAttemptId,
      inputJson: invocation.stepInputJsons.write as string,
      phase: "forward",
      proof,
      purpose: "effect",
      sagaId,
      sagaStepId: "write",
    })
    expect(
      await env.DB.prepare(
        `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
         WHERE "attempt_id" = ?1`,
      )
        .bind(actionAttemptId)
        .first(),
    ).toEqual({ protocol_version: 2 })
    const unknown = await attempts.complete({
      attemptId: actionAttemptId,
      errorJson: '{"dispatch":"unknown"}',
      evidenceJson: '{"source":"workerd-coordinator"}',
      proof,
      state: "unknown",
    })
    if (unknown.state !== "unknown") throw new Error("Expected an unknown action outcome.")
    const unknownSaga = await coordinator.settleActionFromReceipt({
      actorChecksum: "workerd-coordinator-actor",
      attemptId: actionAttemptId,
      operationId,
      phase: "forward",
      proof,
      sagaId,
      stepId: "write",
    })
    expect(unknownSaga.steps.write?.forward.state).toBe("unknown")
    const terminationInput = {
      actorChecksum: "workerd-coordinator-actor",
      cause: "cancellation" as const,
      operationId,
      proof,
      requestChecksum: "workerd-coordinator-cancellation-checksum",
      requestId: "workerd-coordinator-cancellation-request",
      sagaId,
    }
    const terminated = await coordinator.requestTermination(terminationInput)
    expect(terminated).toMatchObject({
      stateVersion: 3,
      status: "compensating",
      terminationCause: "cancellation",
    })
    await expect(coordinator.requestTermination(terminationInput)).resolves.toEqual(terminated)
    await leases.release({ proof })
    const observer = await leases.acquire({
      acquisitionId: "workerd-coordinator-observer-acquisition",
      holderId: "workerd-coordinator-observer",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!observer.acquired) throw new Error("Coordinator observer lease acquisition failed.")
    proof = leaseProof(observer.record)
    const forbiddenObservationAttemptId = `${actionAttemptId}:forbidden-observation`
    await attempts.accept({
      attemptId: forbiddenObservationAttemptId,
      inputJson: JSON.stringify({ effectAttemptId: actionAttemptId }),
      phase: "forward",
      proof,
      purpose: "observation",
      sagaId,
      sagaStepId: "write",
    })
    await expect(
      env.DB.prepare(
        `INSERT INTO "nozzle_saga_action_attempt_outcomes"
         ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
          "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
         VALUES (?1, 'failed', 'forbidden-evidence', '{}', NULL, NULL, 'forbidden-error', '{}',
                 'forbidden-outcome', 1)`,
      )
        .bind(forbiddenObservationAttemptId)
        .run(),
    ).rejects.toThrow(/NOZZLE_CONTROL_SAGA_OUTCOME_FENCED/u)
    const observationAttemptId = `${actionAttemptId}:observation`
    await attempts.accept({
      attemptId: observationAttemptId,
      inputJson: JSON.stringify({ effectAttemptId: actionAttemptId }),
      phase: "forward",
      proof,
      purpose: "observation",
      sagaId,
      sagaStepId: "write",
    })
    const outcome = await attempts.complete({
      attemptId: observationAttemptId,
      evidenceJson: '{"source":"workerd-observer"}',
      outputJson: '{"written":true}',
      proof,
      state: "confirmed",
    })
    if (outcome.state !== "confirmed") throw new Error("Expected an applied observation outcome.")
    expect(
      await env.DB.prepare(
        `SELECT "evidence_json", "output_json", "error_json"
         FROM "nozzle_saga_action_attempt_outcomes"
         WHERE "attempt_id" = ?1`,
      )
        .bind(observationAttemptId)
        .first(),
    ).toEqual({
      error_json: null,
      evidence_json: '{"kind":"evidence","storage":"nozzle.saga-outcome-payload.v1"}',
      output_json: '{"kind":"output","storage":"nozzle.saga-outcome-payload.v1"}',
    })
    expect(
      (
        await env.DB.prepare(
          `SELECT "payload_kind", "payload_checksum", "payload_json"
           FROM "nozzle_saga_action_attempt_outcome_payloads"
           WHERE "attempt_id" = ?1
           ORDER BY "payload_kind"`,
        )
          .bind(observationAttemptId)
          .all()
      ).results,
    ).toEqual([
      {
        payload_checksum: outcome.evidenceChecksum,
        payload_json: '{"source":"workerd-observer"}',
        payload_kind: "evidence",
      },
      {
        payload_checksum: outcome.outputChecksum,
        payload_json: '{"written":true}',
        payload_kind: "output",
      },
    ])
    const settled = await coordinator.settleObservationFromReceipt({
      actorChecksum: "workerd-coordinator-actor",
      attemptId: observationAttemptId,
      operationId,
      phase: "forward",
      proof,
      sagaId,
      stepId: "write",
    })
    expect(settled.steps.write?.forward).toMatchObject({
      observationEvidenceChecksum: outcome.outcomeChecksum,
      resultChecksum: outcome.outputChecksum,
      state: "succeeded",
    })
    const counts = await env.DB.prepare(
      `SELECT
        (SELECT count(*) FROM "nozzle_operation_effects"
         WHERE "resource_kind" = 'saga' AND "resource_id" = ?1) AS "effects",
        (SELECT "state_version" FROM "nozzle_sagas" WHERE "saga_id" = ?1) AS "saga_version",
        (SELECT json_extract("record_json", '$.startedAttempts')
         FROM "nozzle_operation_steps"
         WHERE "operation_id" = ?2 AND "step_id" = ?3) AS "operation_attempts",
        (SELECT json_extract("record_json", '$.state')
         FROM "nozzle_operation_steps"
         WHERE "operation_id" = ?2 AND "step_id" = ?3) AS "operation_state",
        (SELECT json_extract("record_json", '$.resultChecksum')
         FROM "nozzle_operation_steps"
         WHERE "operation_id" = ?2 AND "step_id" = ?3) AS "operation_result",
        (SELECT count(*) FROM "nozzle_saga_action_attempts"
         WHERE "saga_id" = ?1) AS "attempts",
        (SELECT count(*) FROM "nozzle_saga_action_attempt_outcomes" AS "outcome"
         JOIN "nozzle_saga_action_attempts" AS "attempt" USING ("attempt_id")
         WHERE "attempt"."saga_id" = ?1) AS "outcomes"`,
    )
      .bind(sagaId, operationId, sagaActionOperationStepId("write", "forward"))
      .first<{
        effects: number
        attempts: number
        operation_attempts: number
        operation_result: string
        operation_state: string
        outcomes: number
        saga_version: number
      }>()
    expect(counts).toEqual({
      attempts: 3,
      effects: 5,
      operation_attempts: 1,
      operation_result: outcome.outcomeChecksum,
      operation_state: "succeeded",
      outcomes: 2,
      saga_version: 4,
    })
    const terminationEvidence = await env.DB.prepare(
      `SELECT
        (SELECT count(*) FROM "nozzle_operation_transitions"
         WHERE "operation_id" = ?1 AND "step_id" = ?2) AS "transitions",
        (SELECT count(*) FROM "nozzle_operation_effects"
         WHERE "operation_id" = ?1 AND "step_id" = ?2
           AND "resource_kind" = 'saga' AND "resource_id" = ?3
           AND "effect_kind" = 'termination:cancellation') AS "effects",
        (SELECT count(*) FROM "nozzle_audit_log"
         WHERE "operation_id" = ?1 AND "step_id" = ?2
           AND json_extract("event_json", '$.eventType') =
             'saga.termination.requested') AS "audit_events",
        (SELECT json_extract("record_json", '$.state')
         FROM "nozzle_operation_steps"
         WHERE "operation_id" = ?1 AND "step_id" = ?2) AS "operation_state",
        (SELECT "state_version" FROM "nozzle_sagas" WHERE "saga_id" = ?3) AS "saga_version"`,
    )
      .bind(operationId, SAGA_TERMINATION_OPERATION_STEP_ID, sagaId)
      .first<{
        audit_events: number
        effects: number
        operation_state: string
        saga_version: number
        transitions: number
      }>()
    expect(terminationEvidence).toEqual({
      audit_events: 1,
      effects: 1,
      operation_state: "succeeded",
      saga_version: 4,
      transitions: 1,
    })
    await leases.release({ proof })
    const successor = await leases.acquire({
      acquisitionId: "workerd-coordinator-successor-acquisition",
      holderId: "workerd-coordinator-successor",
      leaseKey,
      ttlMs: 60_000,
    })
    expect(successor.acquired).toBe(true)
    for (const statement of CONTROL_SCHEMA_STATEMENTS) await env.DB.prepare(statement).run()
  })

  it("reconciles every terminal saga stream from real paged D1 history", async () => {
    const operationId = "workerd-history-reconciliation-operation"
    const sagaId = "workerd-history-reconciliation-saga"
    const leaseKey = `saga:${sagaId}`
    const forwardAction = {
      actionId: "workerd.history-reconciliation.forward",
      artifactChecksum: "3".repeat(64),
      version: 1,
    }
    const forwardObservation = {
      actionId: "workerd.history-reconciliation.observe-forward",
      artifactChecksum: "4".repeat(64),
      version: 1,
    }
    const compensationAction = {
      actionId: "workerd.history-reconciliation.compensate",
      artifactChecksum: "5".repeat(64),
      version: 1,
    }
    const compensationObservation = {
      actionId: "workerd.history-reconciliation.observe-compensation",
      artifactChecksum: "6".repeat(64),
      version: 1,
    }
    const effectHandler = () => ({
      evidenceJson: "{}",
      outputJson: "{}",
      state: "confirmed" as const,
    })
    const observationHandler = () => ({
      evidenceJson: "{}",
      outputJson: "{}",
      state: "applied" as const,
    })
    const registry = await sealSagaHandlerRegistry(
      [
        { handler: effectHandler, kind: "effect", reference: forwardAction },
        { handler: observationHandler, kind: "observation", reference: forwardObservation },
        { handler: effectHandler, kind: "effect", reference: compensationAction },
        {
          handler: observationHandler,
          kind: "observation",
          reference: compensationObservation,
        },
      ],
      digest,
    )
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "workerd-history-reconciliation",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 0,
            compensationAction,
            compensationObservation,
            forwardAction,
            forwardObservation,
            inputSchemaChecksum: "7".repeat(64),
            irreversible: false,
            maxAttempts: 1,
            maxRetryDelayMs: 0,
            outputSchemaChecksum: "8".repeat(64),
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
        inputJson: '{"request":"reconcile-history"}',
        sagaId,
        stepInputJsons: { write: '{"value":1}' },
      },
      digest,
    )
    const capabilitySnapshotJson = '{"runtime":"workerd-history-reconciliation-v1"}'
    const plan = await sealSagaOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        descriptor,
        inputChecksum: invocation.inputChecksum,
        leaseKey,
        operationId,
        operationIdempotencyKey: `${operationId}:key`,
        registry,
        sagaId,
        stepInputChecksums: invocation.stepInputChecksums,
      },
      digest,
    )
    const operations = new D1OperationStore(env.DB, digest)
    const leases = new D1LeaseStore(env.DB)
    const attempts = new D1SagaAttemptStore(env.DB, digest)
    const coordinator = new D1SagaCoordinatorStore(env.DB, digest)
    await operations.create({
      actorChecksum: "workerd-history-reconciliation-actor",
      capabilitySnapshotJson,
      environmentId: "workerd-history-reconciliation",
      idempotencyScope: "workerd-history-reconciliation",
      inputJson: invocation.operationInputJson,
      plan,
      requiredShardIds: ["workerd-history-reconciliation-shard"],
    })
    const acquired = await leases.acquire({
      acquisitionId: "workerd-history-reconciliation-acquisition",
      holderId: "workerd-history-reconciliation-holder",
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("History reconciliation lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const initPlan = plan.steps.find((step) => step.stepId === SAGA_INIT_OPERATION_STEP_ID)
    if (initPlan === undefined) throw new Error("History reconciliation init plan is missing.")
    const initAttemptId = `${sagaId}:init`
    await operations.beginStep({
      actorChecksum: "workerd-history-reconciliation-actor",
      attemptId: initAttemptId,
      idempotencyKey: initPlan.idempotencyKey,
      observedPreconditionChecksum: initPlan.preconditionChecksum,
      operationId,
      proof,
      stepId: initPlan.stepId,
    })
    await coordinator.initializeSaga({
      actorChecksum: "workerd-history-reconciliation-actor",
      attemptId: initAttemptId,
      deadlineAtMs: 8_000_000_000_000_000,
      descriptor,
      evidenceChecksum: `${sagaId}:init:evidence`,
      idempotencyKey: `${sagaId}:key`,
      inputChecksum: invocation.inputChecksum,
      observedPostconditionChecksum: initPlan.postconditionChecksum,
      operationId,
      proof,
      resultChecksum: `${sagaId}:init:result`,
      sagaId,
      stepInputChecksums: invocation.stepInputChecksums,
    })
    const attemptId = `${sagaId}:write:1`
    const begun = await coordinator.beginAction({
      actorChecksum: "workerd-history-reconciliation-actor",
      attemptId,
      operationId,
      phase: "forward",
      proof,
      sagaId,
      stepId: "write",
    })
    if (begun.disposition !== "execute") {
      throw new Error("History reconciliation action did not begin.")
    }
    await attempts.accept({
      attemptId,
      inputJson: invocation.stepInputJsons.write as string,
      phase: "forward",
      proof,
      purpose: "effect",
      sagaId,
      sagaStepId: "write",
    })
    await attempts.complete({
      attemptId,
      evidenceJson: '{"source":"workerd-history-reconciliation"}',
      outputJson: '{"written":true}',
      proof,
      state: "confirmed",
    })
    const terminal = await coordinator.settleActionFromReceipt({
      actorChecksum: "workerd-history-reconciliation-actor",
      attemptId,
      operationId,
      phase: "forward",
      proof,
      sagaId,
      stepId: "write",
    })
    expect(terminal.status).toBe("succeeded")

    const finalProof = await reconcileStoredSagaHistory(operationId, sagaId)
    expect(finalProof).toMatchObject({
      anchor: { operationId, sagaId },
      reconciliation: {
        actionBeginCount: 1,
        attemptCount: 1,
        coupledTransitionCount: 3,
        effectAttemptCount: 1,
        effectCount: 3,
        observationAttemptCount: 0,
        operationId,
        sagaId,
        transitionCount: 4,
      },
      schemaVersion: 1,
    })
    const capability = mintSagaTerminalCapability(finalProof)
    expect(loadSagaTerminalCapability(capability)).toMatchObject({
      branchDecisions: [
        { kind: "not_required", stepId: "saga:termination" },
        { kind: "not_required", stepId: "saga:compensation:write" },
      ],
      settlementOutcome: "succeeded",
    })
    const settled = await new D1SagaTerminalStore(env.DB, digest).persistTerminalTail({
      actorChecksum: "workerd-history-terminal-actor",
      capability,
      proof,
    })
    expect(settled.steps["saga:settle"]?.state).toBe("succeeded")
    const settledProof = await reconcileStoredSagaHistory(operationId, sagaId)
    expect(
      loadSagaTerminalCapability(mintSagaTerminalCapability(settledProof)).branchDecisions,
    ).toEqual([])
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
    const coordinator = new D1SagaCoordinatorStore(env.DB, digest)
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
    saga = await coordinator.settleActionFromReceipt({
      actorChecksum: "workerd-saga-actor",
      attemptId: writeAttempt,
      operationId,
      phase: "forward",
      proof,
      sagaId,
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
         (SELECT count(*) FROM "nozzle_saga_action_attempt_outcomes" AS "outcome"
          JOIN "nozzle_saga_action_attempts" AS "attempt" USING ("attempt_id")
          WHERE "attempt"."saga_id" = ?1) AS "outcomes"`,
    )
      .bind(sagaId)
      .first<{ attempts: number; effects: number; outcomes: number; sagas: number }>()
    expect(accepted.state).toBe("accepted")
    expect(counts).toEqual({ attempts: 1, effects: 3, outcomes: 1, sagas: 1 })
  })
})
