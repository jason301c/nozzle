import { env } from "cloudflare:test"
import {
  appendAuditEvent,
  beginOperationStep,
  beginSagaAction,
  createOperationRecord,
  createSagaRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  leaseProof,
  type OperationStepRecord,
  operationStatus,
  recordSagaActionSuccess,
  recordStepSuccess,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import { operationStepRecordJson, operationTransitionIdentity } from "../src/operation-store.js"
import {
  canonicalSagaReceiptJson,
  SAGA_ATTEMPT_ERROR_DOMAIN,
  SAGA_ATTEMPT_EVIDENCE_DOMAIN,
  SAGA_ATTEMPT_INPUT_DOMAIN,
  SAGA_ATTEMPT_OUTPUT_DOMAIN,
  SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
  SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
  sagaAttemptAcceptanceChecksum,
  sagaAttemptOutcomeChecksum,
  sagaReceiptPayloadChecksum,
} from "../src/saga-attempt-codec.js"
import { D1SagaHistoryReader, type SagaHistoryEffectRow } from "../src/saga-history.js"
import {
  SagaHistoryAttemptFolder,
  SagaHistoryAuditFolder,
  SagaHistoryEffectFolder,
  SagaHistoryTransitionFolder,
} from "../src/saga-history-fold.js"

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

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (typeof value !== "object" || value === null) return value
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, canonicalValue((value as Record<string, unknown>)[key])]),
  )
}

function domainFrame(domain: string, values: readonly string[]): Uint8Array {
  const parts = [domain, ...values].map((value) => new TextEncoder().encode(value))
  const output = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0))
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function domainChecksum(domain: string, values: readonly string[]): Promise<string> {
  return digest(domainFrame(domain, values))
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
    `CREATE TABLE "nozzle_saga_action_attempt_outcomes" (
      "attempt_id" TEXT PRIMARY KEY, "state" TEXT, "evidence_checksum" TEXT,
      "evidence_json" TEXT, "output_checksum" TEXT, "output_json" TEXT,
      "error_checksum" TEXT, "error_json" TEXT, "outcome_checksum" TEXT,
      "completed_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_saga_action_attempt_outcome_payloads" (
      "attempt_id" TEXT, "payload_kind" TEXT, "payload_checksum" TEXT, "payload_json" TEXT,
      PRIMARY KEY ("attempt_id", "payload_kind")
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

  it("semantically folds canonical saga effects paged from real workerd D1", async () => {
    const operationId = "runtime-effect-operation"
    const sagaId = "runtime-effect-saga"
    const environmentId = "runtime-effect-environment"
    const stepId = "write"
    const operationStepId = `saga:forward:${stepId}`
    const leaseKey = `saga:${sagaId}`
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "runtime-effect-descriptor",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 1,
            compensationAction: {
              actionId: "runtime-compensate",
              artifactChecksum: "a".repeat(64),
              version: 1,
            },
            compensationObservation: {
              actionId: "runtime-observe-compensation",
              artifactChecksum: "b".repeat(64),
              version: 1,
            },
            forwardAction: {
              actionId: "runtime-write",
              artifactChecksum: "c".repeat(64),
              version: 1,
            },
            forwardObservation: {
              actionId: "runtime-observe-write",
              artifactChecksum: "d".repeat(64),
              version: 1,
            },
            inputSchemaChecksum: "e".repeat(64),
            irreversible: false,
            maxAttempts: 1,
            maxRetryDelayMs: 1,
            outputSchemaChecksum: "f".repeat(64),
            stepId,
            timeoutMs: 100,
          },
        ],
        version: 1,
      },
      digest,
    )
    const initial = createSagaRecord({
      deadlineAtMs: 100,
      descriptor,
      idempotencyKey: "runtime-effect-key",
      inputChecksum: "1".repeat(64),
      sagaId,
      serverTimeMs: 0,
      stepInputChecksums: { [stepId]: "2".repeat(64) },
    })
    const decision = beginSagaAction(initial, {
      attemptId: "runtime-effect-attempt",
      idempotencyKey: initial.steps[stepId]?.forward.idempotencyKey as string,
      phase: "forward",
      serverTimeMs: 2,
      stepId,
    })
    if (decision.disposition !== "execute") throw new Error("Runtime effect did not begin")
    const running = decision.saga
    const succeeded = recordSagaActionSuccess(running, {
      attemptId: "runtime-effect-attempt",
      phase: "forward",
      resultChecksum: "runtime-effect-output",
      serverTimeMs: 3,
      stepId,
    })
    const createTransition = operationTransitionIdentity("succeeded", [
      operationId,
      "saga:init",
      "runtime-effect-init-attempt",
    ])
    const beginTransition = operationTransitionIdentity("accepted", [
      operationId,
      operationStepId,
      "runtime-effect-attempt",
    ])
    const successTransition = operationTransitionIdentity("succeeded", [
      operationId,
      operationStepId,
      "runtime-effect-attempt",
    ])
    const beginEvidence = await domainChecksum("nozzle.saga-coordinator-id.v1", [
      "begin-evidence",
      beginTransition,
      sagaId,
      stepId,
      "forward",
      "runtime-effect-attempt",
    ])
    const effectInputs = [
      {
        effectKind: "create",
        evidenceChecksum: "runtime-effect-create-evidence",
        record: initial,
        stepId: "saga:init",
        transitionId: createTransition,
      },
      {
        effectKind: "action:forward:begin",
        evidenceChecksum: beginEvidence,
        record: running,
        stepId: operationStepId,
        transitionId: beginTransition,
      },
      {
        effectKind: "action:forward:success",
        evidenceChecksum: "runtime-effect-outcome",
        record: succeeded,
        stepId: operationStepId,
        transitionId: successTransition,
      },
    ] as const
    const effectRows: SagaHistoryEffectRow[] = []
    for (const input of effectInputs) {
      const json = JSON.stringify(canonicalValue(input.record))
      const recordChecksum = await domainChecksum("nozzle.saga-record.v1", [json])
      const effectChecksum = await domainChecksum("nozzle.saga-coordinator-id.v1", [
        "saga-effect",
        input.transitionId,
        sagaId,
        input.effectKind,
        input.record.stateVersion.toString(10),
      ])
      effectRows.push({
        acquisition_id: "runtime-effect-acquisition",
        created_at_ms: input.record.stateVersion + 1,
        effect_id: `saga-effect:${effectChecksum}`,
        effect_kind: input.effectKind,
        evidence_checksum: input.evidenceChecksum,
        fencing_token: 1,
        from_state_version: input.record.stateVersion === 0 ? null : input.record.stateVersion - 1,
        holder_id: "runtime-effect-holder",
        lease_key: leaseKey,
        operation_id: operationId,
        record_checksum: recordChecksum,
        record_json: json,
        resource_id: sagaId,
        resource_kind: "saga",
        step_id: input.stepId,
        to_state_version: input.record.stateVersion,
        transition_id: input.transitionId,
      })
    }

    await run(
      `INSERT INTO "nozzle_operations" VALUES (?, ?, ?, ?, ?, ?, ?)`,
      operationId,
      environmentId,
      "runtime-effect-operation-input",
      "runtime-effect-operation-plan",
      "{}",
      "succeeded",
      3,
    )
    const finalEffect = effectRows[2] as SagaHistoryEffectRow
    await run(
      `INSERT INTO "nozzle_sagas" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      sagaId,
      operationId,
      descriptor.descriptorChecksum,
      succeeded.inputChecksum,
      succeeded.stateVersion,
      succeeded.status,
      finalEffect.effect_id,
      finalEffect.record_checksum,
      3,
    )
    await run(
      `INSERT INTO "nozzle_audit_log" VALUES (?, ?, ?, ?)`,
      environmentId,
      1,
      "runtime-effect-audit",
      '{"sequence":1}',
    )
    await run(
      `INSERT INTO "nozzle_operation_transitions" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      createTransition,
      operationId,
      "saga:init",
      '{"state":"running"}',
      '{"state":"succeeded"}',
      "running",
      "succeeded",
      "runtime-effect-audit",
      1,
      leaseKey,
      "runtime-effect-holder",
      "runtime-effect-acquisition",
      1,
    )
    for (const row of effectRows) {
      await run(
        `INSERT INTO "nozzle_operation_effects" VALUES
         (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        row.effect_id,
        row.transition_id,
        row.operation_id,
        row.step_id,
        row.resource_kind,
        row.resource_id,
        row.effect_kind,
        row.from_state_version,
        row.to_state_version,
        row.evidence_checksum,
        row.record_checksum,
        row.record_json,
        row.lease_key,
        row.holder_id,
        row.acquisition_id,
        row.fencing_token,
        row.created_at_ms,
      )
    }

    const reader = new D1SagaHistoryReader(env.DB)
    const anchor = await reader.captureAnchor(operationId, sagaId)
    const folder = new SagaHistoryEffectFolder(anchor, digest)
    const first = await reader.effectPage(anchor)
    expect(first).toMatchObject({ complete: false, nextCursor: 1 })
    await folder.append(first)
    const last = await reader.effectPage(anchor, first.nextCursor as number)
    expect(last).toMatchObject({ complete: true, nextCursor: null })
    await folder.append(last)
    expect(folder.proof()).toMatchObject({
      effectCount: 3,
      saga: succeeded,
      sagaStateVersion: 2,
      sagaStatus: "succeeded",
    })
    await expect(reader.assertAnchorCurrent(anchor)).resolves.toBeUndefined()
  })

  it("folds checksum-verified saga attempt outcomes and companion payloads from D1", async () => {
    const operationId = "runtime-attempt-operation"
    const sagaId = "runtime-attempt-saga"
    const environmentId = "runtime-attempt-environment"
    const sagaStepId = "write"
    const operationStepId = `saga:forward:${sagaStepId}`
    const leaseKey = `saga:${sagaId}`
    const effectAttemptId = "runtime-attempt-effect"
    const observationAttemptId = "runtime-attempt-observation"
    const effectInputJson = canonicalSagaReceiptJson(
      JSON.stringify({ attempt: effectAttemptId }),
      "Runtime effect input",
      false,
    )
    const observationInputJson = canonicalSagaReceiptJson(
      JSON.stringify({ attempt: observationAttemptId }),
      "Runtime observation input",
      false,
    )
    const effectInputChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_INPUT_DOMAIN,
      effectInputJson,
    )
    const observationInputChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_INPUT_DOMAIN,
      observationInputJson,
    )
    const effectIdentity = Object.freeze({
      acquisitionId: "runtime-attempt-acquisition-1",
      actionKey: `runtime-effect@1:${"a".repeat(64)}`,
      attemptId: effectAttemptId,
      causalAttemptId: null,
      fencingToken: 1,
      holderId: "runtime-attempt-holder-1",
      idempotencyKey: "runtime-attempt-effect-key",
      inputChecksum: effectInputChecksum,
      inputJson: effectInputJson,
      leaseKey,
      operationId,
      operationStepId,
      phase: "forward" as const,
      purpose: "effect" as const,
      sagaId,
      sagaStepId,
    })
    const observationIdentity = Object.freeze({
      acquisitionId: "runtime-attempt-acquisition-2",
      actionKey: `runtime-observation@1:${"b".repeat(64)}`,
      attemptId: observationAttemptId,
      causalAttemptId: effectAttemptId,
      fencingToken: 2,
      holderId: "runtime-attempt-holder-2",
      idempotencyKey: `${effectIdentity.idempotencyKey}:observation`,
      inputChecksum: observationInputChecksum,
      inputJson: observationInputJson,
      leaseKey,
      operationId,
      operationStepId,
      phase: "forward" as const,
      purpose: "observation" as const,
      sagaId,
      sagaStepId,
    })
    const effectAcceptance = await sagaAttemptAcceptanceChecksum(digest, effectIdentity)
    const observationAcceptance = await sagaAttemptAcceptanceChecksum(digest, observationIdentity)
    const effectEvidenceJson = canonicalSagaReceiptJson(
      JSON.stringify({ provider: "runtime", receipt: "unknown" }),
      "Runtime effect evidence",
      false,
    )
    const effectErrorJson = canonicalSagaReceiptJson(
      JSON.stringify({ classification: "unknown" }),
      "Runtime effect error",
      false,
    )
    const observationEvidenceJson = canonicalSagaReceiptJson(
      JSON.stringify({ provider: "runtime", receipt: "observed" }),
      "Runtime observation evidence",
      false,
    )
    const observationOutputJson = canonicalSagaReceiptJson(
      JSON.stringify({ applied: true }),
      "Runtime observation output",
      false,
    )
    const effectEvidenceChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_EVIDENCE_DOMAIN,
      effectEvidenceJson,
    )
    const effectErrorChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_ERROR_DOMAIN,
      effectErrorJson,
    )
    const observationEvidenceChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_EVIDENCE_DOMAIN,
      observationEvidenceJson,
    )
    const observationOutputChecksum = await sagaReceiptPayloadChecksum(
      digest,
      SAGA_ATTEMPT_OUTPUT_DOMAIN,
      observationOutputJson,
    )
    const effectOutcomeChecksum = await sagaAttemptOutcomeChecksum(
      digest,
      effectAcceptance,
      "unknown",
      effectEvidenceChecksum,
      effectEvidenceJson,
      effectErrorChecksum,
      effectErrorJson,
    )
    const observationOutcomeChecksum = await sagaAttemptOutcomeChecksum(
      digest,
      observationAcceptance,
      "confirmed",
      observationEvidenceChecksum,
      observationEvidenceJson,
      observationOutputChecksum,
      observationOutputJson,
    )

    await run(
      `INSERT INTO "nozzle_operations" VALUES (?, ?, ?, ?, ?, ?, ?)`,
      operationId,
      environmentId,
      "runtime-attempt-operation-input",
      "runtime-attempt-operation-plan",
      "{}",
      "failed",
      30,
    )
    await run(
      `INSERT INTO "nozzle_sagas" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      sagaId,
      operationId,
      "runtime-attempt-descriptor",
      "runtime-attempt-saga-input",
      0,
      "failed",
      "runtime-attempt-effect-0",
      "runtime-attempt-record-0",
      30,
    )
    await run(
      `INSERT INTO "nozzle_audit_log" VALUES (?, ?, ?, ?)`,
      environmentId,
      1,
      "runtime-attempt-audit",
      '{"sequence":1}',
    )
    await run(
      `INSERT INTO "nozzle_operation_transitions" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      "runtime-attempt-transition",
      operationId,
      "saga:init",
      '{"state":"running"}',
      '{"state":"failed"}',
      "running",
      "failed",
      "runtime-attempt-audit",
      1,
      leaseKey,
      "runtime-attempt-holder-1",
      "runtime-attempt-acquisition-1",
      1,
    )
    await run(
      `INSERT INTO "nozzle_operation_effects" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      "runtime-attempt-effect-0",
      "runtime-attempt-transition",
      operationId,
      "saga:init",
      "saga",
      sagaId,
      "create",
      null,
      0,
      "runtime-attempt-effect-evidence",
      "runtime-attempt-record-0",
      '{"stateVersion":0}',
      leaseKey,
      "runtime-attempt-holder-1",
      "runtime-attempt-acquisition-1",
      1,
      1,
    )
    for (const [identity, acceptance, acceptedAtMs] of [
      [effectIdentity, effectAcceptance, 10],
      [observationIdentity, observationAcceptance, 20],
    ] as const) {
      await run(
        `INSERT INTO "nozzle_saga_action_attempts" VALUES
         (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        identity.attemptId,
        identity.causalAttemptId,
        identity.sagaId,
        identity.operationId,
        identity.operationStepId,
        identity.sagaStepId,
        identity.phase,
        identity.purpose,
        identity.actionKey,
        identity.idempotencyKey,
        identity.inputChecksum,
        identity.inputJson,
        acceptance,
        identity.leaseKey,
        identity.holderId,
        identity.acquisitionId,
        identity.fencingToken,
        acceptedAtMs,
      )
      await run(
        `INSERT INTO "nozzle_saga_action_attempt_protocols" VALUES (?, ?, ?)`,
        identity.attemptId,
        2,
        acceptedAtMs,
      )
    }
    await run(
      `INSERT INTO "nozzle_saga_action_attempt_outcomes" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      effectAttemptId,
      "unknown",
      effectEvidenceChecksum,
      effectEvidenceJson,
      null,
      null,
      effectErrorChecksum,
      effectErrorJson,
      effectOutcomeChecksum,
      11,
    )
    await run(
      `INSERT INTO "nozzle_saga_action_attempt_outcomes" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      observationAttemptId,
      "confirmed",
      observationEvidenceChecksum,
      SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
      observationOutputChecksum,
      SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
      null,
      null,
      observationOutcomeChecksum,
      21,
    )
    for (const [kind, checksum, json] of [
      ["evidence", observationEvidenceChecksum, observationEvidenceJson],
      ["output", observationOutputChecksum, observationOutputJson],
    ] as const) {
      await run(
        `INSERT INTO "nozzle_saga_action_attempt_outcome_payloads" VALUES (?, ?, ?, ?)`,
        observationAttemptId,
        kind,
        checksum,
        json,
      )
    }

    const reader = new D1SagaHistoryReader(env.DB)
    const anchor = await reader.captureAnchor(operationId, sagaId)
    expect(anchor).toMatchObject({
      sagaAttemptCount: 2,
      sagaAttemptLastAcceptedAtMs: 20,
      sagaAttemptLastId: observationAttemptId,
    })
    const page = await reader.attemptIdentityPage(anchor)
    expect(page).toMatchObject({ complete: true, nextCursor: null })
    const folder = new SagaHistoryAttemptFolder(anchor, reader, digest)
    await folder.append(page)
    expect(folder.proof()).toMatchObject({
      attemptCount: 2,
      attemptFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      attemptLastAcceptedAtMs: 20,
      attemptLastId: observationAttemptId,
      attempts: [
        {
          attemptId: effectAttemptId,
          state: "unknown",
          valueChecksum: effectErrorChecksum,
        },
        {
          attemptId: observationAttemptId,
          causalAttemptId: effectAttemptId,
          state: "confirmed",
          valueChecksum: observationOutputChecksum,
        },
      ],
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
