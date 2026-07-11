import { env } from "cloudflare:test"
import {
  appendAuditEvent,
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  leaseProof,
  type OperationStepRecord,
  operationStatus,
  recordStepSuccess,
  sealOperationPlan,
} from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { operationStepRecordJson, operationTransitionIdentity } from "../src/operation-store.js"
import { D1SagaHistoryReader } from "../src/saga-history.js"
import { SagaHistoryAuditFolder, SagaHistoryTransitionFolder } from "../src/saga-history-fold.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

async function run(sql: string, ...values: unknown[]): Promise<void> {
  await env.DB.prepare(sql)
    .bind(...values)
    .run()
}

const digest: DigestFunction = async (input) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", input.slice().buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

beforeAll(async () => {
  for (const statement of [
    `CREATE TABLE "nozzle_operations" (
      "operation_id" TEXT PRIMARY KEY, "environment_id" TEXT, "input_checksum" TEXT,
      "plan_checksum" TEXT, "plan_json" TEXT, "status" TEXT, "updated_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_sagas" (
      "saga_id" TEXT PRIMARY KEY, "operation_id" TEXT, "descriptor_checksum" TEXT,
      "input_checksum" TEXT, "state_version" INTEGER, "status" TEXT,
      "last_effect_id" TEXT, "record_checksum" TEXT, "updated_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_audit_log" (
      "environment_id" TEXT, "sequence" INTEGER, "event_hash" TEXT, "event_json" TEXT
    )`,
    `CREATE TABLE "nozzle_operation_transitions" (
      "transition_id" TEXT PRIMARY KEY, "operation_id" TEXT, "step_id" TEXT,
      "from_record_json" TEXT, "to_record_json" TEXT, "from_operation_status" TEXT,
      "to_operation_status" TEXT, "audit_event_hash" TEXT, "fencing_token" INTEGER,
      "lease_key" TEXT, "holder_id" TEXT, "acquisition_id" TEXT, "created_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_irreversible_authorization_receipts" (
      "transition_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "authorization_id" TEXT,
      "authorization_checksum" TEXT, "classified_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_operation_effects" (
      "effect_id" TEXT PRIMARY KEY, "transition_id" TEXT, "operation_id" TEXT,
      "step_id" TEXT, "resource_kind" TEXT, "resource_id" TEXT, "effect_kind" TEXT,
      "from_state_version" INTEGER, "to_state_version" INTEGER, "evidence_checksum" TEXT,
      "record_checksum" TEXT, "record_json" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "created_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_saga_action_attempts" (
      "attempt_id" TEXT PRIMARY KEY, "causal_attempt_id" TEXT, "saga_id" TEXT,
      "operation_id" TEXT, "operation_step_id" TEXT, "saga_step_id" TEXT, "phase" TEXT,
      "purpose" TEXT, "action_key" TEXT, "idempotency_key" TEXT, "input_checksum" TEXT,
      "input_json" TEXT, "acceptance_checksum" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "accepted_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_saga_action_attempt_protocols" (
      "attempt_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "classified_at_ms" INTEGER
    )`,
  ]) {
    await env.DB.prepare(statement).run()
  }
  await run(
    `INSERT INTO "nozzle_operations" VALUES (?, ?, ?, ?, ?, ?, ?)`,
    "history-operation",
    "history-environment",
    "operation-input",
    "operation-plan",
    "{}",
    "running",
    11,
  )
  await run(
    `INSERT INTO "nozzle_sagas" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    "history-saga",
    "history-operation",
    "saga-descriptor",
    "saga-input",
    2,
    "failed",
    "effect-2",
    "saga-record-2",
    12,
  )
  await run(
    `INSERT INTO "nozzle_audit_log" VALUES (?, ?, ?, ?)`,
    "history-environment",
    1,
    "audit-1",
    '{"sequence":1}',
  )
  await run(
    `INSERT INTO "nozzle_operation_transitions" VALUES
     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    "transition-1",
    "history-operation",
    "saga:init",
    '{"state":"running"}',
    '{"state":"succeeded"}',
    "running",
    "running",
    "audit-1",
    1,
    "saga:history-saga",
    "history-holder",
    "history-acquisition",
    1,
  )
  for (let version = 0; version <= 2; version += 1) {
    await run(
      `INSERT INTO "nozzle_operation_effects" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      `effect-${version}`,
      "transition-1",
      "history-operation",
      version === 0 ? "saga:init" : "saga:forward:a",
      "saga",
      "history-saga",
      version === 0 ? "create" : "action:forward:success",
      version === 0 ? null : version - 1,
      version,
      `evidence-${version}`,
      `record-${version}`,
      JSON.stringify({ stateVersion: version }),
      "saga:history-saga",
      "history-holder",
      "history-acquisition",
      1,
      version,
    )
  }
})

describe("real workerd D1 saga history paging", () => {
  it("captures and walks the stable terminal anchor with D1 query results", async () => {
    const reader = new D1SagaHistoryReader(env.DB)
    const anchor = await reader.captureAnchor("history-operation", "history-saga")
    expect(anchor).toMatchObject({
      auditHeadSequence: 1,
      operationTransitionCount: 1,
      sagaAttemptCount: 0,
      sagaEffectCount: 3,
      sagaStateVersion: 2,
    })
    await expect(reader.assertAnchorCurrent(anchor)).resolves.toBeUndefined()
    await expect(reader.auditPage(anchor)).resolves.toMatchObject({ complete: true })
    await expect(reader.transitionPage(anchor)).resolves.toMatchObject({ complete: true })
    const effects = await reader.effectPage(anchor)
    expect(effects).toMatchObject({ complete: false, nextCursor: 1 })
    await expect(reader.effectPage(anchor, effects.nextCursor as number)).resolves.toMatchObject({
      complete: true,
      nextCursor: null,
    })
    await expect(reader.attemptIdentityPage(anchor)).resolves.toEqual({
      complete: true,
      nextCursor: null,
      rows: [],
    })
  })

  it("loads the immutable plan and folds queried operation history in workerd", async () => {
    const operationId = "runtime-fold-operation"
    const sagaId = "runtime-fold-saga"
    const environmentId = "runtime-fold-environment"
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: "runtime-capabilities",
        idempotencyKey: "runtime-operation-key",
        inputChecksum: "runtime-operation-input",
        operationId,
        operationType: "saga:runtime-fold@1",
        steps: [
          {
            checkpoint: "reversible",
            dependsOn: [],
            effectProtocol: "opaque",
            idempotencyKey: "runtime-init-key",
            inputChecksum: "runtime-init-input",
            leaseKey: `saga:${sagaId}`,
            postconditionChecksum: "runtime-init-postcondition",
            preconditionChecksum: "runtime-init-precondition",
            recoveryInstructions: "Reconstruct the runtime initialization receipt.",
            retryClassification: "idempotent",
            stepId: "saga:init",
          },
        ],
      },
      digest,
    )
    const initial = createOperationRecord(plan)
    const leaseDecision = decideLeaseAcquisition(undefined, {
      acquisitionId: "runtime-acquisition",
      holderId: "runtime-holder",
      leaseKey: `saga:${sagaId}`,
      serverTimeMs: 1,
      ttlMs: 100,
    })
    if (!leaseDecision.acquired) throw new Error("Runtime fixture lease was not acquired")
    const lease = leaseDecision.record
    const invocation = beginOperationStep(initial, {
      attemptId: "runtime-init-attempt",
      idempotencyKey: "runtime-init-key",
      lease,
      leaseProof: leaseProof(lease),
      observedPreconditionChecksum: "runtime-init-precondition",
      serverTimeMs: 2,
      stepId: "saga:init",
    })
    if (invocation.disposition !== "execute") {
      throw new Error("Runtime fixture initialization did not start")
    }
    const running = invocation.operation
    const succeeded = recordStepSuccess(running, {
      attemptId: "runtime-init-attempt",
      observedPostconditionChecksum: "runtime-init-postcondition",
      resultChecksum: "runtime-init-result",
      stepId: "saga:init",
    })
    const created = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "runtime-actor",
        environmentId,
        eventType: "operation.created",
        fencingToken: null,
        idempotencyKey: `${operationId}:created`,
        operationId,
        payloadChecksum: plan.inputChecksum,
        serverTimeMs: 1,
        stepId: null,
      },
      digest,
    )
    const acceptedId = operationTransitionIdentity("accepted", [
      operationId,
      "saga:init",
      "runtime-init-attempt",
    ])
    const accepted = await appendAuditEvent(
      created,
      {
        actorChecksum: "runtime-actor",
        environmentId,
        eventType: "step.attempt.accepted",
        fencingToken: 1,
        idempotencyKey: acceptedId,
        operationId,
        payloadChecksum: "runtime-init-input",
        serverTimeMs: 2,
        stepId: "saga:init",
      },
      digest,
    )
    const succeededId = operationTransitionIdentity("succeeded", [
      operationId,
      "saga:init",
      "runtime-init-attempt",
    ])
    const initialized = await appendAuditEvent(
      accepted,
      {
        actorChecksum: "runtime-actor",
        environmentId,
        eventType: "saga.initialized",
        fencingToken: 1,
        idempotencyKey: succeededId,
        operationId,
        payloadChecksum: "runtime-init-evidence",
        serverTimeMs: 3,
        stepId: "saga:init",
      },
      digest,
    )

    await run(
      `INSERT INTO "nozzle_operations" VALUES (?, ?, ?, ?, ?, ?, ?)`,
      operationId,
      environmentId,
      plan.inputChecksum,
      plan.planChecksum,
      JSON.stringify(plan),
      operationStatus(succeeded),
      3,
    )
    await run(
      `INSERT INTO "nozzle_sagas" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      sagaId,
      operationId,
      "runtime-descriptor",
      "runtime-saga-input",
      0,
      "succeeded",
      "runtime-effect-0",
      "runtime-record-0",
      3,
    )
    for (const audit of [created, accepted, initialized]) {
      await run(
        `INSERT INTO "nozzle_audit_log" VALUES (?, ?, ?, ?)`,
        environmentId,
        audit.sequence,
        audit.eventHash,
        JSON.stringify(audit),
      )
    }
    const proof = leaseProof(lease)
    for (const transition of [
      {
        after: running,
        audit: accepted,
        before: initial,
        transitionId: acceptedId,
      },
      {
        after: succeeded,
        audit: initialized,
        before: running,
        transitionId: succeededId,
      },
    ]) {
      await run(
        `INSERT INTO "nozzle_operation_transitions" VALUES
         (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        transition.transitionId,
        operationId,
        "saga:init",
        operationStepRecordJson(transition.before.steps["saga:init"] as OperationStepRecord),
        operationStepRecordJson(transition.after.steps["saga:init"] as OperationStepRecord),
        operationStatus(transition.before),
        operationStatus(transition.after),
        transition.audit.eventHash,
        proof.fencingToken,
        proof.leaseKey,
        proof.holderId,
        proof.acquisitionId,
        transition.audit.serverTimeMs,
      )
    }
    await run(
      `INSERT INTO "nozzle_operation_effects" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      "runtime-effect-0",
      succeededId,
      operationId,
      "saga:init",
      "saga",
      sagaId,
      "create",
      null,
      0,
      "runtime-init-evidence",
      "runtime-record-0",
      JSON.stringify({ stateVersion: 0 }),
      proof.leaseKey,
      proof.holderId,
      proof.acquisitionId,
      proof.fencingToken,
      3,
    )

    const reader = new D1SagaHistoryReader(env.DB)
    const anchor = await reader.captureAnchor(operationId, sagaId)
    const loadedPlan = await reader.operationPlan(anchor, digest)
    expect(loadedPlan).toEqual(plan)
    const auditFolder = new SagaHistoryAuditFolder(anchor, digest)
    const firstAuditPage = await reader.auditPage(anchor)
    expect(firstAuditPage).toMatchObject({ complete: false, nextCursor: 2 })
    await auditFolder.append(firstAuditPage)
    const finalAuditPage = await reader.auditPage(anchor, firstAuditPage.nextCursor as number)
    expect(finalAuditPage).toMatchObject({ complete: true, nextCursor: null })
    await auditFolder.append(finalAuditPage)

    const transitionFolder = new SagaHistoryTransitionFolder(
      anchor,
      auditFolder.proof(),
      loadedPlan,
      digest,
    )
    const transitions = await reader.transitionPage(anchor)
    expect(transitions).toMatchObject({ complete: true, nextCursor: null })
    await transitionFolder.append(transitions)
    expect(transitionFolder.proof()).toMatchObject({
      operation: succeeded,
      operationStatus: "succeeded",
      transitionCount: 2,
    })
    await expect(reader.assertAnchorCurrent(anchor)).resolves.toBeUndefined()
  })

  it("folds a checksum-verified audit chain in the workerd runtime", async () => {
    const created = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "actor-checksum",
        environmentId: "fold-environment",
        eventType: "operation.created",
        fencingToken: null,
        idempotencyKey: "fold-operation:created",
        operationId: "fold-operation",
        payloadChecksum: "fold-input",
        serverTimeMs: 1,
        stepId: null,
      },
      digest,
    )
    const transitioned = await appendAuditEvent(
      created,
      {
        actorChecksum: "actor-checksum",
        environmentId: "fold-environment",
        eventType: "saga.initialized",
        fencingToken: 1,
        idempotencyKey: "fold-transition",
        operationId: "fold-operation",
        payloadChecksum: "fold-evidence",
        serverTimeMs: 2,
        stepId: "saga:init",
      },
      digest,
    )
    const folder = new SagaHistoryAuditFolder(
      {
        auditHeadEventHash: transitioned.eventHash,
        auditHeadSequence: 2,
        environmentId: "fold-environment",
        operationId: "fold-operation",
        operationInputChecksum: "fold-input",
        operationPlanChecksum: "fold-plan",
        operationStatus: "running",
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 2,
        operationTransitionLastId: "fold-transition",
        operationUpdatedAtMs: 2,
        sagaAttemptCount: 0,
        sagaAttemptLastAcceptedAtMs: null,
        sagaAttemptLastId: null,
        sagaDescriptorChecksum: "fold-descriptor",
        sagaEffectCount: 1,
        sagaId: "fold-saga",
        sagaInputChecksum: "fold-saga-input",
        sagaLastEffectId: "fold-effect",
        sagaRecordChecksum: "fold-record",
        sagaStateVersion: 0,
        sagaStatus: "failed",
        sagaUpdatedAtMs: 2,
        schemaVersion: 1,
      },
      digest,
    )
    await folder.append({
      complete: true,
      nextCursor: null,
      rows: [created, transitioned].map((event) => ({
        event_hash: event.eventHash,
        event_json: JSON.stringify(event),
        sequence: event.sequence,
      })),
    })
    expect(folder.proof()).toMatchObject({
      auditEventCount: 2,
      operationCreationEventHash: created.eventHash,
      operationTransitionCount: 1,
      operationTransitionFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
    })
  })
})
