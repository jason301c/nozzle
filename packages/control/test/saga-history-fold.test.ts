import {
  type AuditEvent,
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  encodeAuditEventChecksumInput,
  leaseProof,
  loadOperationRecord,
  markOperationStepNotRequired,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepUnknownAfterCrash,
  type OperationPlan,
  type OperationRecord,
  operationStatus,
  recordAtomicStepOutcome,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  sealIrreversibleAuthorization,
  sealOperationPlan,
} from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { operationStepRecordJson, operationTransitionIdentity } from "../src/operation-store.js"
import type {
  SagaHistoryAnchor,
  SagaHistoryAuditRow,
  SagaHistoryPage,
  SagaHistoryTransitionCursor,
  SagaHistoryTransitionRow,
} from "../src/saga-history.js"
import {
  SagaHistoryAuditFolder,
  type SagaHistoryAuditProof,
  SagaHistoryTransitionFolder,
  type SagaHistoryTransitionProof,
} from "../src/saga-history-fold.js"

const digest: DigestFunction = async (input) => {
  const copy = input.slice()
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

type AuditOverrides = Partial<
  Omit<AuditEvent, "eventHash" | "previousHash" | "schemaVersion" | "sequence">
> & {
  readonly previousHash?: string | null
  readonly sequence?: number
}

async function event(
  previous: AuditEvent | undefined,
  overrides: AuditOverrides,
): Promise<AuditEvent> {
  const candidate: Omit<AuditEvent, "eventHash"> = {
    actorChecksum: "actor-checksum",
    environmentId: "environment-a",
    eventType: "step.attempt.accepted",
    fencingToken: 1,
    idempotencyKey: `audit-${(previous?.sequence ?? 0) + 1}`,
    operationId: "other-operation",
    payloadChecksum: "payload-checksum",
    previousHash: previous?.eventHash ?? null,
    schemaVersion: 1,
    sequence: (previous?.sequence ?? 0) + 1,
    serverTimeMs: (previous?.serverTimeMs ?? 0) + 1,
    stepId: "other-step",
    ...overrides,
  }
  return Object.freeze({
    ...candidate,
    eventHash: await digest(encodeAuditEventChecksumInput(candidate)),
  })
}

function row(value: AuditEvent, json = JSON.stringify(value)): SagaHistoryAuditRow {
  return Object.freeze({ event_hash: value.eventHash, event_json: json, sequence: value.sequence })
}

function page(
  rows: readonly SagaHistoryAuditRow[],
  complete: boolean,
): SagaHistoryPage<SagaHistoryAuditRow, number> {
  return Object.freeze({
    complete,
    nextCursor: complete ? null : (rows.at(-1)?.sequence as number),
    rows: Object.freeze([...rows]),
  })
}

function anchor(
  events: readonly AuditEvent[],
  overrides: Partial<SagaHistoryAnchor> = {},
): SagaHistoryAnchor {
  const head = events.at(-1) as AuditEvent
  return Object.freeze({
    auditHeadEventHash: head.eventHash,
    auditHeadSequence: head.sequence,
    environmentId: "environment-a",
    operationId: "operation-a",
    operationInputChecksum: "operation-input-checksum",
    operationPlanChecksum: "operation-plan-checksum",
    operationStatus: "running",
    operationTransitionCount: 2,
    operationTransitionLastAuditSequence: head.sequence,
    operationTransitionLastId: "transition-2",
    operationUpdatedAtMs: 20,
    sagaAttemptCount: 0,
    sagaAttemptLastAcceptedAtMs: null,
    sagaAttemptLastId: null,
    sagaDescriptorChecksum: "saga-descriptor-checksum",
    sagaEffectCount: 1,
    sagaId: "saga-a",
    sagaInputChecksum: "saga-input-checksum",
    sagaLastEffectId: "effect-0",
    sagaRecordChecksum: "saga-record-checksum",
    sagaStateVersion: 0,
    sagaStatus: "failed",
    sagaUpdatedAtMs: 21,
    schemaVersion: 1,
    ...overrides,
  })
}

async function history(): Promise<readonly AuditEvent[]> {
  const first = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    operationId: "other-operation",
    stepId: null,
  })
  const second = await event(first, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: "operation-input-checksum",
    stepId: null,
  })
  const third = await event(second, {
    eventType: "saga.initialized",
    idempotencyKey: "transition-1",
    operationId: "operation-a",
    stepId: "saga:init",
  })
  const fourth = await event(third, {})
  const fifth = await event(fourth, {
    eventType: "saga.action.started",
    idempotencyKey: "transition-2",
    operationId: "operation-a",
    stepId: "saga:forward:write",
  })
  return Object.freeze([first, second, third, fourth, fifth])
}

async function fold(events: readonly AuditEvent[], inputAnchor = anchor(events)) {
  const folder = new SagaHistoryAuditFolder(inputAnchor, digest)
  const rows = events.map((value) => row(value))
  for (let index = 0; index < rows.length; index += 2) {
    const next = rows.slice(index, index + 2)
    await folder.append(page(next, index + 2 >= rows.length))
  }
  return folder
}

async function transitionPlan(
  overrides: Partial<Pick<OperationPlan, "inputChecksum" | "operationId" | "operationType">> = {},
): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "operation-key",
      inputChecksum: overrides.inputChecksum ?? "operation-input-checksum",
      operationId: overrides.operationId ?? "operation-a",
      operationType: overrides.operationType ?? "saga:fixture@1",
      steps: [
        {
          checkpoint: "reversible",
          dependsOn: [],
          idempotencyKey: "saga-init-key",
          inputChecksum: "saga-init-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "saga-init-postcondition",
          preconditionChecksum: "saga-init-precondition",
          recoveryInstructions: "Reconstruct the saga initialization receipt.",
          retryClassification: "idempotent",
          stepId: "saga:init",
        },
        {
          activation: "conditional",
          checkpoint: "reversible",
          dependsOn: [],
          effectProtocol: "saga_receipt",
          idempotencyKey: "saga-forward-key",
          inputChecksum: "saga-forward-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "saga-forward-postcondition",
          preconditionChecksum: "saga-forward-precondition",
          recoveryInstructions: "Reconstruct the exact saga action receipt.",
          retryClassification: "reconcile_first",
          stepId: "saga:forward:write",
        },
      ],
    },
    digest,
  )
}

async function providerTransitionPlan(
  retryClassification: "idempotent" | "never" | "reconcile_first" = "idempotent",
): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "provider-operation-key",
      inputChecksum: "operation-input-checksum",
      operationId: "operation-a",
      operationType: "saga:provider-fixture@1",
      steps: [
        {
          checkpoint: "reversible",
          dependsOn: [],
          effectProtocol: "provider_receipt",
          idempotencyKey: "provider-step-key",
          inputChecksum: "provider-step-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "provider-postcondition",
          preconditionChecksum: "provider-precondition",
          recoveryInstructions: "Reconstruct the exact provider receipt.",
          retryClassification,
          stepId: "provider:write",
        },
      ],
    },
    digest,
  )
}

async function terminationTransitionPlan(): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "termination-operation-key",
      inputChecksum: "operation-input-checksum",
      operationId: "operation-a",
      operationType: "saga:termination-fixture@1",
      steps: [
        {
          checkpoint: "reversible",
          dependsOn: [],
          effectProtocol: "opaque",
          idempotencyKey: "saga-init-key",
          inputChecksum: "saga-init-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "saga-init-postcondition",
          preconditionChecksum: "saga-init-precondition",
          recoveryInstructions: "Reconstruct saga initialization.",
          retryClassification: "idempotent",
          stepId: "saga:init",
        },
        {
          activation: "conditional",
          checkpoint: "reversible",
          dependsOn: [],
          effectProtocol: "opaque",
          idempotencyKey: "saga-termination-key",
          inputChecksum: "saga-termination-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "saga-termination-postcondition",
          preconditionChecksum: "saga-termination-precondition",
          recoveryInstructions: "Reconstruct the atomic termination receipt.",
          retryClassification: "idempotent",
          stepId: "saga:termination",
        },
      ],
    },
    digest,
  )
}

async function compensationTransitionPlan(): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "compensation-operation-key",
      inputChecksum: "operation-input-checksum",
      operationId: "operation-a",
      operationType: "saga:compensation-fixture@1",
      steps: [
        {
          checkpoint: "reversible",
          dependsOn: [],
          effectProtocol: "saga_receipt",
          idempotencyKey: "compensation-step-key",
          inputChecksum: "compensation-step-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "compensation-postcondition",
          preconditionChecksum: "compensation-precondition",
          recoveryInstructions: "Reconstruct the exact compensation receipt.",
          retryClassification: "reconcile_first",
          stepId: "saga:compensation:write",
        },
      ],
    },
    digest,
  )
}

function activeLease() {
  const decision = decideLeaseAcquisition(undefined, {
    acquisitionId: "acquisition-a",
    holderId: "holder-a",
    leaseKey: "saga:saga-a",
    serverTimeMs: 0,
    ttlMs: 1_000,
  })
  if (!decision.acquired) throw new Error("Fixture lease was not acquired")
  return decision.record
}

function startStep(
  operation: OperationRecord,
  stepId: string,
  attemptId: string,
  serverTimeMs: number,
): OperationRecord {
  const step = operation.plan.steps.find((candidate) => candidate.stepId === stepId)
  if (step === undefined) throw new Error("Fixture operation step is missing")
  const lease = activeLease()
  const decision = beginOperationStep(operation, {
    attemptId,
    idempotencyKey: step.idempotencyKey,
    lease,
    leaseProof: leaseProof(lease),
    observedPreconditionChecksum: step.preconditionChecksum,
    serverTimeMs,
    stepId,
  })
  if (decision.disposition !== "execute") throw new Error("Fixture step did not start")
  return decision.operation
}

function succeedStep(
  operation: OperationRecord,
  stepId: string,
  attemptId: string,
  resultChecksum: string,
): OperationRecord {
  const step = operation.plan.steps.find((candidate) => candidate.stepId === stepId)
  if (step === undefined) throw new Error("Fixture operation step is missing")
  return recordStepSuccess(operation, {
    attemptId,
    observedPostconditionChecksum: step.postconditionChecksum,
    resultChecksum,
    stepId,
  })
}

function transitionRow(
  before: OperationRecord,
  after: OperationRecord,
  audit: AuditEvent,
  stepId: string,
  transitionId: string,
): SagaHistoryTransitionRow {
  return Object.freeze({
    acquisition_id: "acquisition-a",
    audit_event_hash: audit.eventHash,
    audit_event_json: JSON.stringify(audit),
    audit_sequence: audit.sequence,
    authorization_checksum: null,
    authorization_classified_at_ms: null,
    authorization_id: null,
    authorization_protocol_version: null,
    authorization_transition_id: null,
    created_at_ms: audit.serverTimeMs,
    fencing_token: 1,
    from_operation_status: operationStatus(before),
    from_record_json: operationStepRecordJson(
      before.steps[stepId] as NonNullable<OperationRecord["steps"][string]>,
    ),
    holder_id: "holder-a",
    lease_key: "saga:saga-a",
    operation_id: "operation-a",
    step_id: stepId,
    to_operation_status: operationStatus(after),
    to_record_json: operationStepRecordJson(
      after.steps[stepId] as NonNullable<OperationRecord["steps"][string]>,
    ),
    transition_id: transitionId,
  })
}

function transitionPage(
  rows: readonly SagaHistoryTransitionRow[],
  complete: boolean,
): SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor> {
  const last = rows.at(-1) as SagaHistoryTransitionRow
  return Object.freeze({
    complete,
    nextCursor: complete
      ? null
      : Object.freeze({ auditSequence: last.audit_sequence, transitionId: last.transition_id }),
    rows: Object.freeze([...rows]),
  })
}

async function transitionHistory() {
  const plan = await transitionPlan()
  const initial = createOperationRecord(plan)
  const initStarted = startStep(initial, "saga:init", "init-attempt", 2)
  const initSucceeded = succeedStep(initStarted, "saga:init", "init-attempt", "init-result")
  const actionStarted = startStep(initSucceeded, "saga:forward:write", "action-attempt", 4)
  const actionSucceeded = succeedStep(
    actionStarted,
    "saga:forward:write",
    "action-attempt",
    "action-outcome",
  )
  const created = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: plan.inputChecksum,
    stepId: null,
  })
  const initAcceptedId = operationTransitionIdentity("accepted", [
    plan.operationId,
    "saga:init",
    "init-attempt",
  ])
  const initAccepted = await event(created, {
    eventType: "step.attempt.accepted",
    idempotencyKey: initAcceptedId,
    operationId: plan.operationId,
    payloadChecksum: "saga-init-input",
    stepId: "saga:init",
  })
  const initSucceededId = operationTransitionIdentity("succeeded", [
    plan.operationId,
    "saga:init",
    "init-attempt",
  ])
  const initialized = await event(initAccepted, {
    eventType: "saga.initialized",
    idempotencyKey: initSucceededId,
    operationId: plan.operationId,
    payloadChecksum: "init-evidence",
    stepId: "saga:init",
  })
  const actionAcceptedId = operationTransitionIdentity("accepted", [
    plan.operationId,
    "saga:forward:write",
    "action-attempt",
  ])
  const actionAccepted = await event(initialized, {
    eventType: "saga.action.started",
    idempotencyKey: actionAcceptedId,
    operationId: plan.operationId,
    payloadChecksum: "action-start-evidence",
    stepId: "saga:forward:write",
  })
  const actionSucceededId = operationTransitionIdentity("succeeded", [
    plan.operationId,
    "saga:forward:write",
    "action-attempt",
  ])
  const actionClassified = await event(actionAccepted, {
    eventType: "saga.action.classified",
    idempotencyKey: actionSucceededId,
    operationId: plan.operationId,
    payloadChecksum: "action-outcome",
    stepId: "saga:forward:write",
  })
  const events = Object.freeze([
    created,
    initAccepted,
    initialized,
    actionAccepted,
    actionClassified,
  ])
  const rows = Object.freeze([
    transitionRow(initial, initStarted, initAccepted, "saga:init", initAcceptedId),
    transitionRow(initStarted, initSucceeded, initialized, "saga:init", initSucceededId),
    transitionRow(
      initSucceeded,
      actionStarted,
      actionAccepted,
      "saga:forward:write",
      actionAcceptedId,
    ),
    transitionRow(
      actionStarted,
      actionSucceeded,
      actionClassified,
      "saga:forward:write",
      actionSucceededId,
    ),
  ])
  const inputAnchor = anchor(events, {
    operationInputChecksum: plan.inputChecksum,
    operationPlanChecksum: plan.planChecksum,
    operationStatus: "succeeded",
    operationTransitionCount: rows.length,
    operationTransitionLastAuditSequence: actionClassified.sequence,
    operationTransitionLastId: actionSucceededId,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  await auditFolder.append(
    page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    ),
  )
  await auditFolder.append(
    page(
      events.slice(2, 4).map((value) => row(value)),
      false,
    ),
  )
  await auditFolder.append(
    page(
      events.slice(4).map((value) => row(value)),
      true,
    ),
  )
  return Object.freeze({
    anchor: inputAnchor,
    auditProof: auditFolder.proof(),
    events,
    expected: actionSucceeded,
    plan,
    rows,
  })
}

async function reconciliationTransitionHistory() {
  const plan = await transitionPlan()
  const initial = createOperationRecord(plan)
  const started = startStep(initial, "saga:init", "failed-attempt", 2)
  const failed = recordStepFailure(started, {
    attemptId: "failed-attempt",
    errorChecksum: "failure-outcome",
    outcome: "unknown",
    stepId: "saga:init",
  })
  const reconciled = recordStepReconciliation(failed, {
    evidenceChecksum: "observation-evidence",
    outcome: "indeterminate",
    stepId: "saga:init",
  })
  const created = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: plan.inputChecksum,
    stepId: null,
  })
  const acceptedId = operationTransitionIdentity("accepted", [
    "operation-a",
    "saga:init",
    "failed-attempt",
  ])
  const accepted = await event(created, {
    eventType: "step.attempt.accepted",
    idempotencyKey: acceptedId,
    operationId: "operation-a",
    payloadChecksum: "saga-init-input",
    stepId: "saga:init",
  })
  const failedId = operationTransitionIdentity("failed", [
    "operation-a",
    "saga:init",
    "failed-attempt",
  ])
  const failure = await event(accepted, {
    eventType: "step.attempt.unknown",
    idempotencyKey: failedId,
    operationId: "operation-a",
    payloadChecksum: "failure-outcome",
    stepId: "saga:init",
  })
  const reconciledId = operationTransitionIdentity("reconciled", [
    "operation-a",
    "saga:init",
    "observation-attempt",
  ])
  const observation = await event(failure, {
    eventType: "step.reconciled.indeterminate",
    idempotencyKey: reconciledId,
    operationId: "operation-a",
    payloadChecksum: "observation-evidence",
    stepId: "saga:init",
  })
  const events = Object.freeze([created, accepted, failure, observation])
  const rows = Object.freeze([
    transitionRow(initial, started, accepted, "saga:init", acceptedId),
    transitionRow(started, failed, failure, "saga:init", failedId),
    transitionRow(failed, reconciled, observation, "saga:init", reconciledId),
  ])
  const inputAnchor = anchor(events, {
    operationInputChecksum: plan.inputChecksum,
    operationPlanChecksum: plan.planChecksum,
    operationStatus: operationStatus(reconciled),
    operationTransitionCount: rows.length,
    operationTransitionLastAuditSequence: observation.sequence,
    operationTransitionLastId: reconciledId,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  await auditFolder.append(
    page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    ),
  )
  await auditFolder.append(
    page(
      events.slice(2).map((value) => row(value)),
      true,
    ),
  )
  return Object.freeze({ anchor: inputAnchor, auditProof: auditFolder.proof(), plan, rows })
}

async function irreversibleTransitionHistory(protocolVersion: 1 | 2) {
  const plan = await sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "irreversible-operation-key",
      inputChecksum: "operation-input-checksum",
      operationId: "operation-a",
      operationType: "saga:irreversible-fixture@1",
      steps: [
        {
          checkpoint: "irreversible",
          dependsOn: [],
          effectProtocol: "saga_receipt",
          idempotencyKey: "irreversible-step-key",
          inputChecksum: "irreversible-step-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "irreversible-postcondition",
          preconditionChecksum: "irreversible-precondition",
          recoveryInstructions: "Retain and verify the complete irreversible authorization.",
          retryClassification: "reconcile_first",
          stepId: "saga:forward:irreversible",
        },
      ],
    },
    digest,
  )
  const initial = createOperationRecord(plan)
  const lease = activeLease()
  const fullAuthorization = await sealIrreversibleAuthorization(
    plan,
    {
      actorChecksum: "authorization-actor",
      authorizationId: "authorization-a",
      decisionChecksum: "authorization-decision",
      lease,
      leaseProof: leaseProof(lease),
      sealedAtServerTimeMs: 1,
      stepId: "saga:forward:irreversible",
    },
    digest,
  )
  let started: OperationRecord
  if (protocolVersion === 2) {
    const decision = beginOperationStep(initial, {
      attemptId: "irreversible-attempt",
      idempotencyKey: "irreversible-step-key",
      irreversibleAuthorization: fullAuthorization,
      lease,
      leaseProof: leaseProof(lease),
      observedPreconditionChecksum: "irreversible-precondition",
      serverTimeMs: 2,
      stepId: "saga:forward:irreversible",
    })
    if (decision.disposition !== "execute") throw new Error("Fixture dispatch did not start")
    started = decision.operation
  } else {
    const before = initial.steps["saga:forward:irreversible"]
    if (before === undefined) throw new Error("Fixture irreversible step is missing")
    started = await loadOperationRecord(
      {
        plan,
        steps: {
          "saga:forward:irreversible": {
            activeAttemptId: "irreversible-attempt",
            authorizationChecksum: fullAuthorization.authorizationChecksum,
            costCounters: before.costCounters,
            fencingToken: 1,
            lastAttemptId: "irreversible-attempt",
            progressCounters: before.progressCounters,
            startedAttempts: 1,
            state: "running",
          },
        },
      },
      digest,
    )
  }
  const succeeded =
    protocolVersion === 2
      ? recordStepSuccess(started, {
          attemptId: "irreversible-attempt",
          observedPostconditionChecksum: "irreversible-postcondition",
          resultChecksum: "irreversible-result",
          stepId: "saga:forward:irreversible",
        })
      : await loadOperationRecord(
          {
            plan: started.plan,
            steps: {
              "saga:forward:irreversible": {
                authorizationChecksum: fullAuthorization.authorizationChecksum,
                costCounters: {},
                fencingToken: 1,
                lastAttemptId: "irreversible-attempt",
                progressCounters: {},
                resultChecksum: "irreversible-result",
                startedAttempts: 1,
                state: "succeeded",
              },
            },
          },
          digest,
        )
  const created = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: plan.inputChecksum,
    stepId: null,
  })
  const acceptedId = operationTransitionIdentity("accepted", [
    "operation-a",
    "saga:forward:irreversible",
    "irreversible-attempt",
  ])
  const accepted = await event(created, {
    eventType: "saga.action.started",
    idempotencyKey: acceptedId,
    operationId: "operation-a",
    payloadChecksum: "irreversible-step-input",
    stepId: "saga:forward:irreversible",
  })
  const succeededId = operationTransitionIdentity("succeeded", [
    "operation-a",
    "saga:forward:irreversible",
    "irreversible-attempt",
  ])
  const completed = await event(accepted, {
    eventType: "saga.action.classified",
    idempotencyKey: succeededId,
    operationId: "operation-a",
    payloadChecksum: "irreversible-result",
    stepId: "saga:forward:irreversible",
  })
  const dispatch = {
    ...transitionRow(initial, started, accepted, "saga:forward:irreversible", acceptedId),
    authorization_checksum: fullAuthorization.authorizationChecksum,
    authorization_classified_at_ms: accepted.serverTimeMs,
    authorization_id: protocolVersion === 1 ? null : fullAuthorization.authorizationId,
    authorization_protocol_version: protocolVersion,
    authorization_transition_id: acceptedId,
  } satisfies SagaHistoryTransitionRow
  const completion = transitionRow(
    started,
    succeeded,
    completed,
    "saga:forward:irreversible",
    succeededId,
  )
  const events = Object.freeze([created, accepted, completed])
  const rows = Object.freeze([Object.freeze(dispatch), completion])
  const inputAnchor = anchor(events, {
    operationInputChecksum: plan.inputChecksum,
    operationPlanChecksum: plan.planChecksum,
    operationStatus: "succeeded",
    operationTransitionCount: 2,
    operationTransitionLastAuditSequence: completed.sequence,
    operationTransitionLastId: succeededId,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  await auditFolder.append(
    page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    ),
  )
  await auditFolder.append(
    page(
      events.slice(2).map((value) => row(value)),
      true,
    ),
  )
  return Object.freeze({
    anchor: inputAnchor,
    auditProof: auditFolder.proof(),
    events,
    fullAuthorization,
    plan,
    rows,
  })
}

async function irreversibleRetryTransitionHistory() {
  const base = await irreversibleTransitionHistory(2)
  const initial = createOperationRecord(base.plan)
  const lease = activeLease()
  const first = beginOperationStep(initial, {
    attemptId: "irreversible-attempt-1",
    idempotencyKey: "irreversible-step-key",
    irreversibleAuthorization: base.fullAuthorization,
    lease,
    leaseProof: leaseProof(lease),
    observedPreconditionChecksum: "irreversible-precondition",
    serverTimeMs: 2,
    stepId: "saga:forward:irreversible",
  })
  if (first.disposition !== "execute") throw new Error("Fixture first dispatch did not start")
  const failed = recordStepFailure(first.operation, {
    attemptId: "irreversible-attempt-1",
    errorChecksum: "irreversible-not-applied",
    outcome: "definitely_not_applied",
    stepId: "saga:forward:irreversible",
  })
  const second = beginOperationStep(failed, {
    attemptId: "irreversible-attempt-2",
    idempotencyKey: "irreversible-step-key",
    irreversibleAuthorization: base.fullAuthorization,
    lease,
    leaseProof: leaseProof(lease),
    observedPreconditionChecksum: "irreversible-precondition",
    serverTimeMs: 4,
    stepId: "saga:forward:irreversible",
  })
  if (second.disposition !== "execute") throw new Error("Fixture retry dispatch did not start")

  const created = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: base.plan.inputChecksum,
    stepId: null,
  })
  const firstId = operationTransitionIdentity("accepted", [
    "operation-a",
    "saga:forward:irreversible",
    "irreversible-attempt-1",
  ])
  const accepted = await event(created, {
    eventType: "saga.action.started",
    idempotencyKey: firstId,
    operationId: "operation-a",
    payloadChecksum: "first-dispatch-evidence",
    stepId: "saga:forward:irreversible",
  })
  const failureId = operationTransitionIdentity("failed", [
    "operation-a",
    "saga:forward:irreversible",
    "irreversible-attempt-1",
  ])
  const classified = await event(accepted, {
    eventType: "saga.action.classified",
    idempotencyKey: failureId,
    operationId: "operation-a",
    payloadChecksum: "irreversible-not-applied",
    stepId: "saga:forward:irreversible",
  })
  const secondId = operationTransitionIdentity("accepted", [
    "operation-a",
    "saga:forward:irreversible",
    "irreversible-attempt-2",
  ])
  const retried = await event(classified, {
    eventType: "saga.action.started",
    idempotencyKey: secondId,
    operationId: "operation-a",
    payloadChecksum: "second-dispatch-evidence",
    stepId: "saga:forward:irreversible",
  })
  const receipt = (transition: SagaHistoryTransitionRow) =>
    Object.freeze({
      ...transition,
      authorization_checksum: base.fullAuthorization.authorizationChecksum,
      authorization_classified_at_ms: transition.created_at_ms,
      authorization_id: base.fullAuthorization.authorizationId,
      authorization_protocol_version: 2,
      authorization_transition_id: transition.transition_id,
    }) satisfies SagaHistoryTransitionRow
  const rows = Object.freeze([
    receipt(
      transitionRow(initial, first.operation, accepted, "saga:forward:irreversible", firstId),
    ),
    transitionRow(first.operation, failed, classified, "saga:forward:irreversible", failureId),
    receipt(
      transitionRow(failed, second.operation, retried, "saga:forward:irreversible", secondId),
    ),
  ])
  const events = Object.freeze([created, accepted, classified, retried])
  const last = rows.at(-1) as SagaHistoryTransitionRow
  const inputAnchor = anchor(events, {
    operationInputChecksum: base.plan.inputChecksum,
    operationPlanChecksum: base.plan.planChecksum,
    operationStatus: operationStatus(second.operation),
    operationTransitionCount: rows.length,
    operationTransitionLastAuditSequence: last.audit_sequence,
    operationTransitionLastId: last.transition_id,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  await auditFolder.append(
    page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    ),
  )
  await auditFolder.append(
    page(
      events.slice(2).map((value) => row(value)),
      true,
    ),
  )
  return Object.freeze({
    anchor: inputAnchor,
    auditProof: auditFolder.proof(),
    expected: second.operation,
    plan: base.plan,
    rows,
  })
}

async function stripAuthorizationBody(
  operation: OperationRecord,
  stepId: string,
): Promise<OperationRecord> {
  const step = JSON.parse(
    operationStepRecordJson(
      operation.steps[stepId] as NonNullable<OperationRecord["steps"][string]>,
    ),
  ) as Record<string, unknown>
  delete step.irreversibleAuthorization
  return loadOperationRecord({ plan: operation.plan, steps: { [stepId]: step } }, digest)
}

async function legacyTransitionBase(
  effectProtocol: "opaque" | "provider_receipt" | "saga_receipt",
  retryClassification: "idempotent" | "never" | "reconcile_first" = "reconcile_first",
) {
  const stepId =
    effectProtocol === "saga_receipt"
      ? "saga:forward:legacy"
      : effectProtocol === "provider_receipt"
        ? "provider:legacy"
        : "opaque:legacy"
  const plan = await sealOperationPlan(
    {
      capabilitySnapshotChecksum: "operation-capabilities",
      idempotencyKey: "legacy-operation-key",
      inputChecksum: "operation-input-checksum",
      operationId: "operation-a",
      operationType: "saga:legacy-fixture@1",
      steps: [
        {
          checkpoint: "irreversible",
          dependsOn: [],
          effectProtocol,
          idempotencyKey: "legacy-step-key",
          inputChecksum: "legacy-step-input",
          leaseKey: "saga:saga-a",
          postconditionChecksum: "legacy-postcondition",
          preconditionChecksum: "legacy-precondition",
          recoveryInstructions: "Verify checksum-only protocol-one history without authorizing it.",
          retryClassification,
          stepId,
        },
      ],
    },
    digest,
  )
  const initial = createOperationRecord(plan)
  const lease = activeLease()
  const authorization = await sealIrreversibleAuthorization(
    plan,
    {
      actorChecksum: "authorization-actor",
      authorizationId: "legacy-authorization",
      decisionChecksum: "legacy-decision",
      lease,
      leaseProof: leaseProof(lease),
      sealedAtServerTimeMs: 1,
      stepId,
    },
    digest,
  )
  const decision = beginOperationStep(initial, {
    attemptId: "legacy-attempt",
    idempotencyKey: "legacy-step-key",
    irreversibleAuthorization: authorization,
    lease,
    leaseProof: leaseProof(lease),
    observedPreconditionChecksum: "legacy-precondition",
    serverTimeMs: 2,
    stepId,
  })
  if (decision.disposition !== "execute") throw new Error("Legacy fixture dispatch did not start")
  const legacyStarted = await stripAuthorizationBody(decision.operation, stepId)
  const created = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    operationId: plan.operationId,
    payloadChecksum: plan.inputChecksum,
    stepId: null,
  })
  const transitionId = operationTransitionIdentity("accepted", [
    plan.operationId,
    stepId,
    "legacy-attempt",
  ])
  const accepted = await event(created, {
    eventType: effectProtocol === "saga_receipt" ? "saga.action.started" : "step.attempt.accepted",
    idempotencyKey: transitionId,
    operationId: plan.operationId,
    payloadChecksum:
      effectProtocol === "saga_receipt" ? "legacy-start-evidence" : "legacy-step-input",
    stepId,
  })
  const dispatch = Object.freeze({
    ...transitionRow(initial, legacyStarted, accepted, stepId, transitionId),
    authorization_checksum: authorization.authorizationChecksum,
    authorization_classified_at_ms: accepted.serverTimeMs,
    authorization_id: null,
    authorization_protocol_version: 1,
    authorization_transition_id: transitionId,
  }) satisfies SagaHistoryTransitionRow
  return Object.freeze({
    accepted,
    authorization,
    created,
    dispatch,
    fullStarted: decision.operation,
    legacyStarted,
    plan,
    stepId,
  })
}

type TransitionFixture = Awaited<ReturnType<typeof transitionHistory>>

async function expectTransitionFailure(
  fixture: TransitionFixture,
  index: number,
  candidate: SagaHistoryTransitionRow,
  message: RegExp,
) {
  const folder = new SagaHistoryTransitionFolder(
    fixture.anchor,
    fixture.auditProof,
    fixture.plan,
    digest,
  )
  if (index >= 2) await folder.append(transitionPage(fixture.rows.slice(0, 2), false))
  const pageRows = index >= 2 ? [...fixture.rows.slice(2)] : [...fixture.rows.slice(0, 2)]
  pageRows[index % 2] = candidate
  return expect(folder.append(transitionPage(pageRows, index >= 2))).rejects.toMatchObject({
    code: "OperationInterventionRequiredError",
    message: expect.stringMatching(message),
  })
}

async function withAuditEvent(
  row: SagaHistoryTransitionRow,
  source: AuditEvent,
  overrides: Partial<Omit<AuditEvent, "eventHash">>,
): Promise<SagaHistoryTransitionRow> {
  const { eventHash: _eventHash, ...body } = source
  const candidate = { ...body, ...overrides }
  const sealed = Object.freeze({
    ...candidate,
    eventHash: await digest(encodeAuditEventChecksumInput(candidate)),
  })
  return Object.freeze({
    ...row,
    audit_event_hash: sealed.eventHash,
    audit_event_json: JSON.stringify(sealed),
  })
}

async function foldCustomTransitionHistory(
  plan: OperationPlan,
  events: readonly AuditEvent[],
  rows: readonly SagaHistoryTransitionRow[],
  expected: OperationRecord,
): Promise<SagaHistoryTransitionProof> {
  const last = rows.at(-1) as SagaHistoryTransitionRow
  const inputAnchor = anchor(events, {
    operationInputChecksum: plan.inputChecksum,
    operationPlanChecksum: plan.planChecksum,
    operationStatus: operationStatus(expected),
    operationTransitionCount: rows.length,
    operationTransitionLastAuditSequence: last.audit_sequence,
    operationTransitionLastId: last.transition_id,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  const auditRows = events.map((value) => row(value))
  for (let index = 0; index < auditRows.length; index += 2) {
    await auditFolder.append(page(auditRows.slice(index, index + 2), index + 2 >= auditRows.length))
  }
  const transitionFolder = new SagaHistoryTransitionFolder(
    inputAnchor,
    auditFolder.proof(),
    plan,
    digest,
  )
  for (let index = 0; index < rows.length; index += 2) {
    await transitionFolder.append(
      transitionPage(rows.slice(index, index + 2), index + 2 >= rows.length),
    )
  }
  return transitionFolder.proof()
}

async function expectCustomTransitionHistoryFailure(
  plan: OperationPlan,
  events: readonly AuditEvent[],
  rows: readonly SagaHistoryTransitionRow[],
  expected: OperationRecord,
  message: RegExp,
) {
  const last = rows.at(-1) as SagaHistoryTransitionRow
  const inputAnchor = anchor(events, {
    operationInputChecksum: plan.inputChecksum,
    operationPlanChecksum: plan.planChecksum,
    operationStatus: operationStatus(expected),
    operationTransitionCount: rows.length,
    operationTransitionLastAuditSequence: last.audit_sequence,
    operationTransitionLastId: last.transition_id,
  })
  const auditFolder = new SagaHistoryAuditFolder(inputAnchor, digest)
  const auditRows = events.map((value) => row(value))
  for (let index = 0; index < auditRows.length; index += 2) {
    await auditFolder.append(page(auditRows.slice(index, index + 2), index + 2 >= auditRows.length))
  }
  const transitionFolder = new SagaHistoryTransitionFolder(
    inputAnchor,
    auditFolder.proof(),
    plan,
    digest,
  )
  let offset = 0
  while (rows.length - offset > 2) {
    await transitionFolder.append(transitionPage(rows.slice(offset, offset + 2), false))
    offset += 2
  }
  return expect(
    transitionFolder.append(transitionPage(rows.slice(offset), true)),
  ).rejects.toMatchObject({
    code: "OperationInterventionRequiredError",
    message: expect.stringMatching(message),
  })
}

function expectIntervention(promise: Promise<unknown>, message: RegExp) {
  return expect(promise).rejects.toMatchObject({
    code: "OperationInterventionRequiredError",
    message: expect.stringMatching(message),
  })
}

describe("saga history audit fold", () => {
  it("stream-verifies the pinned chain and emits a constant-size operation audit proof", async () => {
    const events = await history()
    const folder = new SagaHistoryAuditFolder(anchor(events), digest)
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )

    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )
    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )

    const proof: SagaHistoryAuditProof = folder.proof()
    expect(proof).toMatchObject({
      auditEventCount: 5,
      auditHeadEventHash: events[4]?.eventHash,
      auditHeadSequence: 5,
      environmentId: "environment-a",
      operationCreationEventHash: events[1]?.eventHash,
      operationId: "operation-a",
      operationInputChecksum: "operation-input-checksum",
      operationPlanChecksum: "operation-plan-checksum",
      operationTransitionCount: 2,
      operationTransitionFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      schemaVersion: 1,
    })
    expect(Object.isFrozen(proof)).toBe(true)
    await expect(folder.append(page([row(events[4] as AuditEvent)], true))).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Saga audit history is already completely folded.",
    })
  })

  it("owns the anchor and page snapshots before checksum work begins", async () => {
    const events = await history()
    const mutableAnchor = { ...anchor(events) }
    const folder = new SagaHistoryAuditFolder(mutableAnchor, digest)
    mutableAnchor.operationId = "changed-operation"

    const mutableRows = events.map((value) => ({ ...row(value) }))
    const mutablePage = {
      complete: false,
      nextCursor: 2,
      rows: mutableRows.slice(0, 2),
    }
    const pending = folder.append(mutablePage)
    mutablePage.complete = true
    mutablePage.nextCursor = 99
    const firstMutableRow = mutablePage.rows[0]
    if (firstMutableRow === undefined) throw new Error("Missing test audit row")
    firstMutableRow.event_json = "{}"
    await expect(pending).resolves.toBeUndefined()

    await folder.append(page(mutableRows.slice(2, 4), false))
    await folder.append(page(mutableRows.slice(4), true))
    expect(folder.proof().operationId).toBe("operation-a")
  })

  it("applies each page atomically so a rejected page can be retried exactly", async () => {
    const events = await history()
    const folder = new SagaHistoryAuditFolder(anchor(events), digest)
    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    await expectIntervention(
      folder.append(
        page(
          [row(events[2] as AuditEvent), { ...row(events[3] as AuditEvent), event_hash: "wrong" }],
          false,
        ),
      ),
      /row hash/u,
    )
    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )
    expect(folder.proof().operationTransitionCount).toBe(2)
  })

  it("rejects overlapping page folds before a delayed checksum can stale-write state", async () => {
    const events = await history()
    let releaseDigest: () => void = () => undefined
    let markDigestStarted: () => void = () => undefined
    const digestGate = new Promise<void>((resolve) => {
      releaseDigest = resolve
    })
    const digestStarted = new Promise<void>((resolve) => {
      markDigestStarted = resolve
    })
    let digestCalls = 0
    const delayedDigest: DigestFunction = async (input) => {
      digestCalls += 1
      if (digestCalls === 1) {
        markDigestStarted()
        await digestGate
      }
      return digest(input)
    }
    const folder = new SagaHistoryAuditFolder(anchor(events), delayedDigest)
    const firstPage = page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    )
    const delayedAppend = folder.append(firstPage)
    await digestStarted
    await expect(folder.append(firstPage)).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "A saga audit history page is already being folded.",
    })
    releaseDigest()
    await delayedAppend

    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )
    expect(folder.proof()).toMatchObject({
      auditHeadSequence: 5,
      operationTransitionCount: 2,
    })
  })

  it("requires a valid anchor and digest", async () => {
    const events = await history()
    expect(
      () => new SagaHistoryAuditFolder(anchor(events), undefined as unknown as DigestFunction),
    ).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
    expect(
      () =>
        new SagaHistoryAuditFolder(
          { ...anchor(events), schemaVersion: 2 } as unknown as SagaHistoryAnchor,
          digest,
        ),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))

    let digestCalls = 0
    const malformedFoldDigest: DigestFunction = async (input) => {
      digestCalls += 1
      return digestCalls === 4 ? "not-a-checksum" : digest(input)
    }
    const folder = new SagaHistoryAuditFolder(anchor(events), malformedFoldDigest)
    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    await expect(
      folder.append(
        page(
          events.slice(2, 4).map((value) => row(value)),
          false,
        ),
      ),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: expect.stringMatching(/digest/u),
    })
  })

  it("rejects malformed page ownership and pagination envelopes", async () => {
    const events = await history()
    const validRow = row(events[0] as AuditEvent)
    const cases: readonly [unknown, RegExp][] = [
      [
        { complete: true, nextCursor: null, rows: [{ ...validRow, event_json: () => "bad" }] },
        /captured safely/u,
      ],
      [{ complete: true, extra: true, nextCursor: null, rows: [validRow] }, /fields/u],
      [{ complete: "yes", nextCursor: null, rows: [validRow] }, /completion metadata/u],
      [{ complete: true, nextCursor: null, rows: {} }, /dense array/u],
      [{ complete: true, nextCursor: null, rows: [] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [validRow, validRow, validRow] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [{ ...validRow, extra: true }] }, /row envelope/u],
      [{ complete: true, nextCursor: 1, rows: [validRow] }, /retained a cursor/u],
      [{ complete: false, nextCursor: 1, rows: [validRow] }, /pagination/u],
      [
        { complete: false, nextCursor: 99, rows: [validRow, row(events[1] as AuditEvent)] },
        /pagination/u,
      ],
    ]
    for (const [candidate, message] of cases) {
      const folder = new SagaHistoryAuditFolder(anchor(events), digest)
      await expectIntervention(
        folder.append(candidate as SagaHistoryPage<SagaHistoryAuditRow, number>),
        message,
      )
    }
  })

  it("rejects checksum-valid audit rows that break the pinned chain", async () => {
    const events = await history()
    const first = events[0] as AuditEvent
    const second = events[1] as AuditEvent
    const wrongEnvironment = await event(undefined, { environmentId: "environment-b" })
    const wrongPrevious = await event(undefined, { previousHash: "unexpected-previous" })
    const reordered = JSON.stringify({
      eventHash: first.eventHash,
      actorChecksum: first.actorChecksum,
      environmentId: first.environmentId,
      eventType: first.eventType,
      fencingToken: first.fencingToken,
      idempotencyKey: first.idempotencyKey,
      operationId: first.operationId,
      payloadChecksum: first.payloadChecksum,
      previousHash: first.previousHash,
      schemaVersion: first.schemaVersion,
      sequence: first.sequence,
      serverTimeMs: first.serverTimeMs,
      stepId: first.stepId,
    })
    const cases: readonly [SagaHistoryAuditRow, RegExp][] = [
      [{ ...row(first), event_json: "{" }, /event JSON is invalid/u],
      [{ ...row(first), sequence: 2 }, /sequence/u],
      [{ ...row(second), sequence: 1 }, /sequence/u],
      [{ ...row(first), event_hash: "wrong-row-hash" }, /row hash/u],
      [row(wrongEnvironment), /environment history/u],
      [row(wrongPrevious), /previous hash/u],
      [row(first, reordered), /not canonical/u],
    ]
    for (const [candidate, message] of cases) {
      const folder = new SagaHistoryAuditFolder(anchor(events), digest)
      await expectIntervention(folder.append(page([candidate], true)), message)
    }

    const decreasing = await event(first, { serverTimeMs: 0 })
    const decreasingFolder = new SagaHistoryAuditFolder(
      anchor([first, decreasing], {
        auditHeadEventHash: decreasing.eventHash,
        auditHeadSequence: 2,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 2,
      }),
      digest,
    )
    await expectIntervention(
      decreasingFolder.append(page([row(first), row(decreasing)], true)),
      /server time/u,
    )

    const beyond = await event(first, {})
    const beyondFolder = new SagaHistoryAuditFolder(
      anchor([first], {
        auditHeadSequence: 1,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 1,
      }),
      digest,
    )
    await expectIntervention(
      beyondFolder.append(page([row(first), row(beyond)], false)),
      /environment history/u,
    )
  })

  it("requires exactly one anchored creation before every fenced transition event", async () => {
    const other = await event(undefined, {})
    const transition = await event(other, {
      operationId: "operation-a",
      stepId: "saga:init",
    })
    const transitionBeforeCreation = new SagaHistoryAuditFolder(
      anchor([other, transition], {
        auditHeadEventHash: transition.eventHash,
        auditHeadSequence: transition.sequence,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: transition.sequence,
      }),
      digest,
    )
    await expectIntervention(
      transitionBeforeCreation.append(page([row(other), row(transition)], true)),
      /precedes operation creation/u,
    )

    const creation = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: "operation-input-checksum",
      stepId: null,
    })
    const duplicate = await event(creation, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: "operation-input-checksum",
      stepId: null,
    })
    const duplicateFolder = new SagaHistoryAuditFolder(
      anchor([creation, duplicate], {
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 2,
      }),
      digest,
    )
    await expectIntervention(
      duplicateFolder.append(page([row(creation), row(duplicate)], true)),
      /duplicated or reordered/u,
    )

    for (const overrides of [
      { stepId: "unexpected-step" },
      { fencingToken: 1 },
      { payloadChecksum: "wrong-input" },
    ] as const) {
      const contradictory = await event(undefined, {
        eventType: "operation.created",
        fencingToken: null,
        operationId: "operation-a",
        payloadChecksum: "operation-input-checksum",
        stepId: null,
        ...overrides,
      })
      const folder = new SagaHistoryAuditFolder(
        anchor([contradictory], {
          operationTransitionCount: 1,
          operationTransitionLastAuditSequence: 1,
        }),
        digest,
      )
      await expectIntervention(
        folder.append(page([row(contradictory)], true)),
        /contradicts its anchor/u,
      )
    }

    for (const overrides of [{ stepId: null }, { fencingToken: null }] as const) {
      const malformed = await event(creation, {
        operationId: "operation-a",
        stepId: "saga:init",
        ...overrides,
      })
      const folder = new SagaHistoryAuditFolder(
        anchor([creation, malformed], {
          operationTransitionCount: 1,
          operationTransitionLastAuditSequence: 2,
        }),
        digest,
      )
      await expectIntervention(
        folder.append(page([row(creation), row(malformed)], true)),
        /lacks its fenced step/u,
      )
    }

    const one = await event(creation, { operationId: "operation-a" })
    const two = await event(one, { operationId: "operation-a" })
    const overflowFolder = new SagaHistoryAuditFolder(
      anchor([creation, one, two], {
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 3,
      }),
      digest,
    )
    await overflowFolder.append(page([row(creation), row(one)], false))
    await expectIntervention(overflowFolder.append(page([row(two)], true)), /exceeds/u)
  })

  it("refuses incomplete pages and complete folds that disagree with any anchor head", async () => {
    const events = await history()
    const firstTwo = events.slice(0, 2).map((value) => row(value))
    const didNotClose = new SagaHistoryAuditFolder(
      anchor(events, { auditHeadSequence: 2, operationTransitionLastAuditSequence: 2 }),
      digest,
    )
    await expectIntervention(didNotClose.append(page(firstTwo, false)), /failed to close/u)

    const reconciliationCases: readonly SagaHistoryAnchor[] = [
      anchor(events),
      anchor(events, { auditHeadEventHash: "wrong-head" }),
      anchor(events, { operationId: "missing-operation", operationTransitionCount: 2 }),
      anchor(events, { operationTransitionCount: 3 }),
    ]
    const suppliedRows = [events.slice(0, 4), events, events, events]
    for (let index = 0; index < reconciliationCases.length; index += 1) {
      const candidateAnchor = reconciliationCases[index] as SagaHistoryAnchor
      const candidateEvents = suppliedRows[index] as readonly AuditEvent[]
      const folder = new SagaHistoryAuditFolder(candidateAnchor, digest)
      const candidateRows = candidateEvents.map((value) => row(value))
      for (let offset = 0; offset < candidateRows.length - 2; offset += 2) {
        await folder.append(page(candidateRows.slice(offset, offset + 2), false))
      }
      const finalOffset = Math.max(0, candidateRows.length - (candidateRows.length % 2 || 2))
      await expectIntervention(
        folder.append(page(candidateRows.slice(finalOffset), true)),
        /does not reconcile/u,
      )
    }
  })

  it("produces the same ordered transition digest across different page boundaries", async () => {
    const events = await history()
    const streamed = await fold(events)
    const differentlyPaged = new SagaHistoryAuditFolder(anchor(events), digest)
    const rows = events.map((value) => row(value))
    await differentlyPaged.append(page(rows.slice(0, 2), false))
    await differentlyPaged.append(page(rows.slice(2, 4), false))
    await differentlyPaged.append(page(rows.slice(4), true))
    expect(differentlyPaged.proof().operationTransitionFoldChecksum).toBe(
      streamed.proof().operationTransitionFoldChecksum,
    )
  })
})

describe("saga history transition fold", () => {
  it("reconstructs the operation ledger and reconciles its ordered audit proof", async () => {
    const fixture = await transitionHistory()
    const folder = new SagaHistoryTransitionFolder(
      fixture.anchor,
      fixture.auditProof,
      fixture.plan,
      digest,
    )
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )
    await folder.append(transitionPage(fixture.rows.slice(0, 2), false))
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )
    await folder.append(transitionPage(fixture.rows.slice(2), true))

    const proof: SagaHistoryTransitionProof = folder.proof()
    expect(proof).toMatchObject({
      auditTransitionFoldChecksum: fixture.auditProof.operationTransitionFoldChecksum,
      operation: fixture.expected,
      operationId: fixture.plan.operationId,
      operationPlanChecksum: fixture.plan.planChecksum,
      operationStatus: "succeeded",
      schemaVersion: 1,
      transitionCount: 4,
      transitionLastAuditSequence: 5,
      transitionLastId: fixture.rows[3]?.transition_id,
    })
    expect(Object.isFrozen(proof)).toBe(true)
    await expect(folder.append(transitionPage(fixture.rows.slice(2), true))).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Saga operation-transition history is already folded.",
    })
  })

  it("rejects skipped acceptance and forged accepted-attempt deltas", async () => {
    const fixture = await transitionHistory()
    const accepted = fixture.rows[0] as SagaHistoryTransitionRow
    const initialized = fixture.rows[1] as SagaHistoryTransitionRow
    const acceptedEvent = fixture.events[1] as AuditEvent
    const skippedId = operationTransitionIdentity("succeeded", [
      fixture.plan.operationId,
      "saga:init",
      "init-attempt",
    ])
    const skippedAudit = await withAuditEvent(accepted, acceptedEvent, {
      eventType: "saga.initialized",
      idempotencyKey: skippedId,
      payloadChecksum: "init-evidence",
    })
    await expectTransitionFailure(
      fixture,
      0,
      {
        ...skippedAudit,
        to_operation_status: initialized.to_operation_status,
        to_record_json: initialized.to_record_json,
        transition_id: skippedId,
      },
      /active attempt fence|exact core state transition/u,
    )

    const acceptedStep = JSON.parse(accepted.to_record_json) as OperationRecord["steps"][string]
    for (const forged of [
      { ...acceptedStep, startedAttempts: acceptedStep.startedAttempts + 1 },
      { ...acceptedStep, costCounters: { forged: 1 } },
      { ...acceptedStep, progressCounters: { forged: 1 } },
    ]) {
      await expectTransitionFailure(
        fixture,
        0,
        { ...accepted, to_record_json: operationStepRecordJson(forged) },
        /exact core state transition/u,
      )
    }

    await expectTransitionFailure(
      fixture,
      0,
      { ...accepted, created_at_ms: Number.MAX_SAFE_INTEGER },
      /active lease interval|exact core state transition/u,
    )

    const initial = createOperationRecord(fixture.plan)
    const first = startStep(initial, "saga:init", "duplicate-attempt-1", 2)
    const firstStep = first.steps["saga:init"] as NonNullable<OperationRecord["steps"][string]>
    const contradictory = await loadOperationRecord(
      {
        plan: fixture.plan,
        steps: {
          ...first.steps,
          "saga:init": {
            ...firstStep,
            activeAttemptId: "duplicate-attempt-2",
            lastAttemptId: "duplicate-attempt-2",
            startedAttempts: 2,
          },
        },
      },
      digest,
    )
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: fixture.plan.operationId,
      payloadChecksum: fixture.plan.inputChecksum,
      stepId: null,
    })
    const firstId = operationTransitionIdentity("accepted", [
      fixture.plan.operationId,
      "saga:init",
      "duplicate-attempt-1",
    ])
    const firstEvent = await event(created, {
      eventType: "step.attempt.accepted",
      idempotencyKey: firstId,
      operationId: fixture.plan.operationId,
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    const secondId = operationTransitionIdentity("accepted", [
      fixture.plan.operationId,
      "saga:init",
      "duplicate-attempt-2",
    ])
    const secondEvent = await event(firstEvent, {
      eventType: "step.attempt.accepted",
      idempotencyKey: secondId,
      operationId: fixture.plan.operationId,
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    await expectCustomTransitionHistoryFailure(
      fixture.plan,
      [created, firstEvent, secondEvent],
      [
        transitionRow(initial, first, firstEvent, "saga:init", firstId),
        transitionRow(first, contradictory, secondEvent, "saga:init", secondId),
      ],
      contradictory,
      /does not reconstruct as an exact core transition|exact core state transition/u,
    )
  })

  it("rejects malformed length-framed identities for every deferred identity family", async () => {
    const fixture = await transitionHistory()
    const accepted = fixture.rows[0] as SagaHistoryTransitionRow
    const acceptedEvent = fixture.events[1] as AuditEvent
    const prefix = operationTransitionIdentity("not-required", [
      fixture.plan.operationId,
      "saga:init",
    ])
    const malformed = [
      "other-domain:not-required",
      operationTransitionIdentity("reconciled", [fixture.plan.operationId, "saga:init", "x"]),
      operationTransitionIdentity("not-required", ["other-operation", "saga:init", "x"]),
      operationTransitionIdentity("not-required", [fixture.plan.operationId, "other-step", "x"]),
      `${prefix}:0:`,
      `${prefix}:2:x`,
      `${prefix}:01:x`,
    ]
    for (const transitionId of malformed) {
      const audit = await withAuditEvent(accepted, acceptedEvent, {
        eventType: "step.not_required",
        idempotencyKey: transitionId,
      })
      await expectTransitionFailure(
        fixture,
        0,
        { ...audit, transition_id: transitionId },
        /stable identity/u,
      )
    }

    const validId = operationTransitionIdentity("not-required", [
      fixture.plan.operationId,
      "saga:init",
      "decision-a",
    ])
    const validAudit = await withAuditEvent(accepted, acceptedEvent, {
      eventType: "step.not_required",
      idempotencyKey: validId,
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...validAudit, transition_id: validId },
      /exact core state transition/u,
    )
  })

  it("replays provider crash modes and binds newer fences, payloads, and absence evidence", async () => {
    const plan = await providerTransitionPlan()
    const initial = createOperationRecord(plan)
    const started = startStep(initial, "provider:write", "provider-attempt", 2)
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const acceptedId = operationTransitionIdentity("accepted", [
      plan.operationId,
      "provider:write",
      "provider-attempt",
    ])
    const accepted = await event(created, {
      eventType: "step.attempt.accepted",
      idempotencyKey: acceptedId,
      operationId: plan.operationId,
      payloadChecksum: "provider-step-input",
      stepId: "provider:write",
    })
    const acceptedRow = transitionRow(initial, started, accepted, "provider:write", acceptedId)
    const recoveryId = operationTransitionIdentity("crash-recovered", [
      plan.operationId,
      "provider:write",
      "recovery-a",
    ])
    const absenceEvidence = await digest(
      new TextEncoder().encode(
        operationTransitionIdentity("provider-not-dispatched-evidence", [
          plan.operationId,
          "provider:write",
          "provider-attempt",
          "2",
        ]),
      ),
    )
    const notDispatched = markRunningStepNotDispatchedAfterCrash(
      started,
      "provider:write",
      absenceEvidence,
    )
    const absent = await event(accepted, {
      eventType: "step.crash.not_dispatched",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: plan.operationId,
      payloadChecksum: "provider-attempt",
      stepId: "provider:write",
    })
    const absenceRow = {
      ...transitionRow(started, notDispatched, absent, "provider:write", recoveryId),
      fencing_token: 2,
    }
    await foldCustomTransitionHistory(
      plan,
      [created, accepted, absent],
      [acceptedRow, absenceRow],
      notDispatched,
    )

    const stale = await event(accepted, {
      eventType: "step.crash.not_dispatched",
      idempotencyKey: recoveryId,
      operationId: plan.operationId,
      payloadChecksum: "provider-attempt",
      stepId: "provider:write",
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, stale],
      [acceptedRow, transitionRow(started, notDispatched, stale, "provider:write", recoveryId)],
      notDispatched,
      /newer consumer fence/u,
    )

    const wrongPayload = await event(accepted, {
      eventType: "step.crash.not_dispatched",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: plan.operationId,
      payloadChecksum: "wrong-attempt",
      stepId: "provider:write",
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, wrongPayload],
      [
        acceptedRow,
        {
          ...transitionRow(started, notDispatched, wrongPayload, "provider:write", recoveryId),
          fencing_token: 2,
        },
      ],
      notDispatched,
      /audit payload/u,
    )

    const forgedAbsence = markRunningStepNotDispatchedAfterCrash(
      started,
      "provider:write",
      "forged-absence",
    )
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, absent],
      [
        acceptedRow,
        {
          ...transitionRow(started, forgedAbsence, absent, "provider:write", recoveryId),
          fencing_token: 2,
        },
      ],
      forgedAbsence,
      /dispatch-absence evidence/u,
    )

    for (const errorChecksum of ["provider-acceptance"] as const) {
      const unknown = markRunningStepUnknownAfterCrash(started, "provider:write", errorChecksum)
      const unknownEvent = await event(accepted, {
        eventType: "step.crash.outcome_unknown",
        fencingToken: 2,
        idempotencyKey: recoveryId,
        operationId: plan.operationId,
        payloadChecksum: errorChecksum ?? "provider-attempt",
        stepId: "provider:write",
      })
      const unknownRow = {
        ...transitionRow(started, unknown, unknownEvent, "provider:write", recoveryId),
        fencing_token: 2,
      }
      await foldCustomTransitionHistory(
        plan,
        [created, accepted, unknownEvent],
        [acceptedRow, unknownRow],
        unknown,
      )
      const forgedPayload = await event(accepted, {
        eventType: "step.crash.outcome_unknown",
        fencingToken: 2,
        idempotencyKey: recoveryId,
        operationId: plan.operationId,
        payloadChecksum: "wrong-outcome",
        stepId: "provider:write",
      })
      await expectCustomTransitionHistoryFailure(
        plan,
        [created, accepted, forgedPayload],
        [
          acceptedRow,
          {
            ...transitionRow(started, unknown, forgedPayload, "provider:write", recoveryId),
            fencing_token: 2,
          },
        ],
        unknown,
        /audit payload/u,
      )
    }

    const missingProviderAcceptance = markRunningStepUnknownAfterCrash(started, "provider:write")
    const missingProviderEvent = await event(accepted, {
      eventType: "step.crash.outcome_unknown",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: plan.operationId,
      payloadChecksum: "provider-attempt",
      stepId: "provider:write",
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, missingProviderEvent],
      [
        acceptedRow,
        {
          ...transitionRow(
            started,
            missingProviderAcceptance,
            missingProviderEvent,
            "provider:write",
            recoveryId,
          ),
          fencing_token: 2,
        },
      ],
      missingProviderAcceptance,
      /provider-acceptance evidence mode/u,
    )

    const opaquePlan = await transitionPlan()
    const opaqueInitial = createOperationRecord(opaquePlan)
    const opaqueStarted = startStep(opaqueInitial, "saga:init", "opaque-attempt", 2)
    const opaqueCreated = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: opaquePlan.operationId,
      payloadChecksum: opaquePlan.inputChecksum,
      stepId: null,
    })
    const opaqueAcceptedId = operationTransitionIdentity("accepted", [
      opaquePlan.operationId,
      "saga:init",
      "opaque-attempt",
    ])
    const opaqueAccepted = await event(opaqueCreated, {
      eventType: "step.attempt.accepted",
      idempotencyKey: opaqueAcceptedId,
      operationId: opaquePlan.operationId,
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    const opaqueRecoveryId = operationTransitionIdentity("crash-recovered", [
      opaquePlan.operationId,
      "saga:init",
      "opaque-recovery",
    ])
    const opaqueUnknown = markRunningStepUnknownAfterCrash(opaqueStarted, "saga:init")
    const opaqueUnknownEvent = await event(opaqueAccepted, {
      eventType: "step.crash.outcome_unknown",
      fencingToken: 2,
      idempotencyKey: opaqueRecoveryId,
      operationId: opaquePlan.operationId,
      payloadChecksum: "opaque-attempt",
      stepId: "saga:init",
    })
    const opaqueAcceptedRow = transitionRow(
      opaqueInitial,
      opaqueStarted,
      opaqueAccepted,
      "saga:init",
      opaqueAcceptedId,
    )
    const opaqueUnknownRow = {
      ...transitionRow(
        opaqueStarted,
        opaqueUnknown,
        opaqueUnknownEvent,
        "saga:init",
        opaqueRecoveryId,
      ),
      fencing_token: 2,
    }
    await foldCustomTransitionHistory(
      opaquePlan,
      [opaqueCreated, opaqueAccepted, opaqueUnknownEvent],
      [opaqueAcceptedRow, opaqueUnknownRow],
      opaqueUnknown,
    )
    const opaqueForged = markRunningStepUnknownAfterCrash(
      opaqueStarted,
      "saga:init",
      "forged-provider-acceptance",
    )
    const opaqueForgedEvent = await event(opaqueAccepted, {
      eventType: "step.crash.outcome_unknown",
      fencingToken: 2,
      idempotencyKey: opaqueRecoveryId,
      operationId: opaquePlan.operationId,
      payloadChecksum: "forged-provider-acceptance",
      stepId: "saga:init",
    })
    await expectCustomTransitionHistoryFailure(
      opaquePlan,
      [opaqueCreated, opaqueAccepted, opaqueForgedEvent],
      [
        opaqueAcceptedRow,
        {
          ...transitionRow(
            opaqueStarted,
            opaqueForged,
            opaqueForgedEvent,
            "saga:init",
            opaqueRecoveryId,
          ),
          fencing_token: 2,
        },
      ],
      opaqueForged,
      /provider-acceptance evidence mode/u,
    )
  })

  it("replays monotonic outcome counters and rejects removal or decrease", async () => {
    const plan = await providerTransitionPlan()
    const initial = createOperationRecord(plan)
    const first = startStep(initial, "provider:write", "counter-attempt-1", 2)
    const failed = recordStepFailure(first, {
      attemptId: "counter-attempt-1",
      counters: { cost: { calls: 2 }, progress: { bytes: 3 } },
      errorChecksum: "counter-retry",
      outcome: "definitely_not_applied",
      stepId: "provider:write",
    })
    const second = startStep(failed, "provider:write", "counter-attempt-2", 4)
    const succeeded = recordStepSuccess(second, {
      attemptId: "counter-attempt-2",
      counters: { cost: { zero: 0 }, progress: { more: 2 } },
      observedPostconditionChecksum: "provider-postcondition",
      resultChecksum: "counter-result",
      stepId: "provider:write",
    })
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const firstId = operationTransitionIdentity("accepted", [
      plan.operationId,
      "provider:write",
      "counter-attempt-1",
    ])
    const firstEvent = await event(created, {
      eventType: "step.attempt.accepted",
      idempotencyKey: firstId,
      operationId: plan.operationId,
      payloadChecksum: "provider-step-input",
      stepId: "provider:write",
    })
    const failureId = operationTransitionIdentity("failed", [
      plan.operationId,
      "provider:write",
      "counter-attempt-1",
    ])
    const failureEvent = await event(firstEvent, {
      eventType: "step.attempt.definitely_not_applied",
      idempotencyKey: failureId,
      operationId: plan.operationId,
      payloadChecksum: "counter-retry",
      stepId: "provider:write",
    })
    const secondId = operationTransitionIdentity("accepted", [
      plan.operationId,
      "provider:write",
      "counter-attempt-2",
    ])
    const secondEvent = await event(failureEvent, {
      eventType: "step.attempt.accepted",
      idempotencyKey: secondId,
      operationId: plan.operationId,
      payloadChecksum: "provider-step-input",
      stepId: "provider:write",
    })
    const successId = operationTransitionIdentity("succeeded", [
      plan.operationId,
      "provider:write",
      "counter-attempt-2",
    ])
    const successEvent = await event(secondEvent, {
      eventType: "step.attempt.succeeded",
      idempotencyKey: successId,
      operationId: plan.operationId,
      payloadChecksum: "counter-result",
      stepId: "provider:write",
    })
    const rows = [
      transitionRow(initial, first, firstEvent, "provider:write", firstId),
      transitionRow(first, failed, failureEvent, "provider:write", failureId),
      transitionRow(failed, second, secondEvent, "provider:write", secondId),
      transitionRow(second, succeeded, successEvent, "provider:write", successId),
    ]
    const events = [created, firstEvent, failureEvent, secondEvent, successEvent]
    await foldCustomTransitionHistory(plan, events, rows, succeeded)

    const successfulStep = succeeded.steps["provider:write"] as NonNullable<
      OperationRecord["steps"][string]
    >
    for (const counters of [
      { ...successfulStep.costCounters, calls: 1 },
      { zero: successfulStep.costCounters.zero as number },
    ]) {
      const forged = await loadOperationRecord(
        {
          plan,
          steps: {
            "provider:write": { ...successfulStep, costCounters: counters },
          },
        },
        digest,
      )
      await expectCustomTransitionHistoryFailure(
        plan,
        events,
        [
          rows[0] as SagaHistoryTransitionRow,
          rows[1] as SagaHistoryTransitionRow,
          rows[2] as SagaHistoryTransitionRow,
          transitionRow(second, forged, successEvent, "provider:write", successId),
        ],
        forged,
        /durable counter|exact core state transition/u,
      )
    }
  })

  it("binds saga event labels to canonical plan steps and consumer fences", async () => {
    const fixture = await transitionHistory()
    const initAccepted = fixture.rows[0] as SagaHistoryTransitionRow
    const initAcceptedEvent = fixture.events[1] as AuditEvent
    const actionAccepted = fixture.rows[2] as SagaHistoryTransitionRow
    const actionAcceptedEvent = fixture.events[3] as AuditEvent

    await expectTransitionFailure(
      fixture,
      0,
      await withAuditEvent(initAccepted, initAcceptedEvent, {
        eventType: "saga.action.started",
      }),
      /non-action operation step/u,
    )
    await expectTransitionFailure(
      fixture,
      2,
      await withAuditEvent(actionAccepted, actionAcceptedEvent, {
        eventType: "step.attempt.accepted",
        payloadChecksum: "saga-forward-input",
      }),
      /relabeled as a generic/u,
    )
    await expectTransitionFailure(
      fixture,
      2,
      await withAuditEvent(actionAccepted, actionAcceptedEvent, {
        eventType: "saga.initialized",
      }),
      /non-initialization step/u,
    )

    const crashId = operationTransitionIdentity("crash-recovered", [
      fixture.plan.operationId,
      "saga:init",
      "recovery-a",
    ])
    const opaqueAbsence = await withAuditEvent(initAccepted, initAcceptedEvent, {
      eventType: "step.crash.not_dispatched",
      idempotencyKey: crashId,
      payloadChecksum: "init-attempt",
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...opaqueAbsence, transition_id: crashId },
      /non-provider operation step/u,
    )

    const reconciliationId = operationTransitionIdentity("reconciled", [
      fixture.plan.operationId,
      "saga:init",
      "reconciliation-a",
    ])
    const unfencedReconciliation = await withAuditEvent(initAccepted, initAcceptedEvent, {
      eventType: "step.reconciled.indeterminate",
      idempotencyKey: reconciliationId,
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...unfencedReconciliation, transition_id: reconciliationId },
      /regressed its consumer fence/u,
    )

    const initialized = fixture.rows[1] as SagaHistoryTransitionRow
    const initializedEvent = fixture.events[2] as AuditEvent
    const changedFence = await withAuditEvent(
      { ...initialized, fencing_token: 2 },
      initializedEvent,
      { fencingToken: 2 },
    )
    await expectTransitionFailure(fixture, 1, changedFence, /active attempt fence/u)

    const classified = fixture.rows[3] as SagaHistoryTransitionRow
    const classifiedEvent = fixture.events[4] as AuditEvent
    for (const eventType of ["saga.action.recovered", "saga.action.observed"] as const) {
      const kind = eventType === "saga.action.recovered" ? "crash-recovered" : "reconciled"
      const transitionId = operationTransitionIdentity(kind, [
        fixture.plan.operationId,
        "saga:forward:write",
        "consumer-a",
      ])
      const stale = await withAuditEvent(classified, classifiedEvent, {
        eventType,
        idempotencyKey: transitionId,
      })
      await expectTransitionFailure(
        fixture,
        3,
        { ...stale, transition_id: transitionId },
        /newer consumer fence/u,
      )
    }

    const compensationPlan = await compensationTransitionPlan()
    const compensationInitial = createOperationRecord(compensationPlan)
    const compensationStarted = startStep(
      compensationInitial,
      "saga:compensation:write",
      "compensation-attempt",
      2,
    )
    const compensationCreated = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: compensationPlan.operationId,
      payloadChecksum: compensationPlan.inputChecksum,
      stepId: null,
    })
    const compensationId = operationTransitionIdentity("accepted", [
      compensationPlan.operationId,
      "saga:compensation:write",
      "compensation-attempt",
    ])
    const compensationEvent = await event(compensationCreated, {
      eventType: "saga.action.started",
      idempotencyKey: compensationId,
      operationId: compensationPlan.operationId,
      payloadChecksum: "compensation-start-evidence",
      stepId: "saga:compensation:write",
    })
    await foldCustomTransitionHistory(
      compensationPlan,
      [compensationCreated, compensationEvent],
      [
        transitionRow(
          compensationInitial,
          compensationStarted,
          compensationEvent,
          "saga:compensation:write",
          compensationId,
        ),
      ],
      compensationStarted,
    )

    const terminationPlan = await terminationTransitionPlan()
    const terminationInitial = createOperationRecord(terminationPlan)
    const terminationLease = activeLease()
    const termination = recordAtomicStepOutcome(terminationInitial, {
      attemptId: "termination-attempt",
      idempotencyKey: "saga-termination-key",
      leaseProof: leaseProof(terminationLease),
      observedPreconditionChecksum: "saga-termination-precondition",
      outcome: {
        observedPostconditionChecksum: "saga-termination-postcondition",
        resultChecksum: "termination-evidence",
        state: "succeeded",
      },
      stepId: "saga:termination",
    })
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: terminationPlan.operationId,
      payloadChecksum: terminationPlan.inputChecksum,
      stepId: null,
    })
    const terminationId = operationTransitionIdentity("succeeded", [
      terminationPlan.operationId,
      "saga:termination",
      "termination-attempt",
    ])
    const terminated = await event(created, {
      eventType: "saga.termination.requested",
      idempotencyKey: terminationId,
      operationId: terminationPlan.operationId,
      payloadChecksum: "termination-evidence",
      stepId: "saga:termination",
    })
    await foldCustomTransitionHistory(
      terminationPlan,
      [created, terminated],
      [
        transitionRow(
          terminationInitial,
          termination,
          terminated,
          "saga:termination",
          terminationId,
        ),
      ],
      termination,
    )

    const wrongTerminationId = operationTransitionIdentity("succeeded", [
      fixture.plan.operationId,
      "saga:init",
      "init-attempt",
    ])
    const wrongTermination = await withAuditEvent(initAccepted, initAcceptedEvent, {
      eventType: "saga.termination.requested",
      idempotencyKey: wrongTerminationId,
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...wrongTermination, transition_id: wrongTerminationId },
      /canonical operation step/u,
    )
  })

  it("replays the current saga observation matrix and rejects impossible classifications", async () => {
    const plan = await transitionPlan()
    const stepId = "saga:forward:write"
    const initial = createOperationRecord(plan)
    const started = startStep(initial, stepId, "observed-attempt", 2)
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const acceptedId = operationTransitionIdentity("accepted", [
      plan.operationId,
      stepId,
      "observed-attempt",
    ])
    const accepted = await event(created, {
      eventType: "saga.action.started",
      idempotencyKey: acceptedId,
      operationId: plan.operationId,
      payloadChecksum: "observation-start-evidence",
      stepId,
    })
    const acceptedRow = transitionRow(initial, started, accepted, stepId, acceptedId)
    const failedId = operationTransitionIdentity("failed", [
      plan.operationId,
      stepId,
      "observed-attempt",
    ])
    const unknown = recordStepFailure(started, {
      attemptId: "observed-attempt",
      errorChecksum: "unknown-effect",
      outcome: "unknown",
      stepId,
    })
    const classified = await event(accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: plan.operationId,
      payloadChecksum: "unknown-effect",
      stepId,
    })
    const classifiedRow = transitionRow(started, unknown, classified, stepId, failedId)
    for (const outcome of ["applied", "indeterminate", "not_applied"] as const) {
      const evidenceChecksum = `observation-${outcome}`
      const observed = recordStepReconciliation(unknown, {
        evidenceChecksum,
        ...(outcome === "applied"
          ? {
              observedPostconditionChecksum: "saga-forward-postcondition",
              resultChecksum: evidenceChecksum,
            }
          : {}),
        outcome,
        stepId,
      })
      const observationId = operationTransitionIdentity("reconciled", [
        plan.operationId,
        stepId,
        `observation-${outcome}`,
      ])
      const observation = await event(classified, {
        eventType: "saga.action.observed",
        fencingToken: 2,
        idempotencyKey: observationId,
        operationId: plan.operationId,
        payloadChecksum: evidenceChecksum,
        stepId,
      })
      await foldCustomTransitionHistory(
        plan,
        [created, accepted, classified, observation],
        [
          acceptedRow,
          classifiedRow,
          {
            ...transitionRow(unknown, observed, observation, stepId, observationId),
            fencing_token: 2,
          },
        ],
        observed,
      )
    }

    const retryable = recordStepFailure(started, {
      attemptId: "observed-attempt",
      errorChecksum: "proven-not-applied",
      outcome: "definitely_not_applied",
      stepId,
    })
    const retryableEvent = await event(accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: plan.operationId,
      payloadChecksum: "proven-not-applied",
      stepId,
    })
    const terminal = recordSagaStepTerminalClassification(retryable, {
      outcome: "not_applied",
      receiptOutcomeChecksum: "proven-not-applied",
      stepId,
    })
    const terminalId = operationTransitionIdentity("reconciled", [
      plan.operationId,
      stepId,
      "terminal-observation",
    ])
    const terminalEvent = await event(retryableEvent, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: terminalId,
      operationId: plan.operationId,
      payloadChecksum: "proven-not-applied",
      stepId,
    })
    await foldCustomTransitionHistory(
      plan,
      [created, accepted, retryableEvent, terminalEvent],
      [
        acceptedRow,
        transitionRow(started, retryable, retryableEvent, stepId, failedId),
        {
          ...transitionRow(retryable, terminal, terminalEvent, stepId, terminalId),
          fencing_token: 2,
        },
      ],
      terminal,
    )

    const permanentlyFailed = recordStepFailure(started, {
      attemptId: "observed-attempt",
      errorChecksum: "permanent-effect",
      outcome: "permanent",
      stepId,
    })
    const permanentEvent = await event(accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: plan.operationId,
      payloadChecksum: "permanent-effect",
      stepId,
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, permanentEvent],
      [acceptedRow, transitionRow(started, permanentlyFailed, permanentEvent, stepId, failedId)],
      permanentlyFailed,
      /exact core state transition/u,
    )

    const recoveryId = operationTransitionIdentity("crash-recovered", [
      plan.operationId,
      stepId,
      "impossible-recovery",
    ])
    const impossibleRecovery = await event(accepted, {
      eventType: "saga.action.recovered",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: plan.operationId,
      payloadChecksum: "permanent-effect",
      stepId,
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, impossibleRecovery],
      [
        acceptedRow,
        {
          ...transitionRow(started, permanentlyFailed, impossibleRecovery, stepId, recoveryId),
          fencing_token: 2,
        },
      ],
      permanentlyFailed,
      /exact core state transition/u,
    )

    const failedStep = {
      ...(unknown.steps[stepId] as NonNullable<OperationRecord["steps"][string]>),
      state: "failed" as const,
    }
    const failedObservation = await loadOperationRecord(
      { plan, steps: { ...unknown.steps, [stepId]: failedStep } },
      digest,
    )
    const observationId = operationTransitionIdentity("reconciled", [
      plan.operationId,
      stepId,
      "invalid-observation",
    ])
    const failedObservationEvent = await event(classified, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: observationId,
      operationId: plan.operationId,
      payloadChecksum: "failed-observation",
      stepId,
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, classified, failedObservationEvent],
      [
        acceptedRow,
        classifiedRow,
        {
          ...transitionRow(
            unknown,
            failedObservation,
            failedObservationEvent,
            stepId,
            observationId,
          ),
          fencing_token: 2,
        },
      ],
      failedObservation,
      /exact core state transition/u,
    )

    const mismatchedResult = recordStepReconciliation(unknown, {
      evidenceChecksum: "observation-evidence",
      observedPostconditionChecksum: "saga-forward-postcondition",
      outcome: "applied",
      resultChecksum: "different-result",
      stepId,
    })
    const mismatchedEvent = await event(classified, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: observationId,
      operationId: plan.operationId,
      payloadChecksum: "observation-evidence",
      stepId,
    })
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted, classified, mismatchedEvent],
      [
        acceptedRow,
        classifiedRow,
        {
          ...transitionRow(unknown, mismatchedResult, mismatchedEvent, stepId, observationId),
          fencing_token: 2,
        },
      ],
      mismatchedResult,
      /exact core state transition/u,
    )
  })

  it("rejects forged audit proofs and operation plans before folding", async () => {
    const fixture = await transitionHistory()
    expect(
      () =>
        new SagaHistoryTransitionFolder(
          fixture.anchor,
          fixture.auditProof,
          fixture.plan,
          undefined as unknown as DigestFunction,
        ),
    ).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
    expect(
      () =>
        new SagaHistoryTransitionFolder(
          fixture.anchor,
          {
            ...fixture.auditProof,
            operationCreationEventHash: (() => "bad") as unknown as string,
          },
          fixture.plan,
          digest,
        ),
    ).toThrowError(expect.objectContaining({ message: expect.stringMatching(/captured/u) }))
    expect(
      () =>
        new SagaHistoryTransitionFolder(
          fixture.anchor,
          { ...fixture.auditProof, extra: true } as unknown as SagaHistoryAuditProof,
          fixture.plan,
          digest,
        ),
    ).toThrowError(expect.objectContaining({ message: expect.stringMatching(/fields/u) }))

    for (const proof of [
      { ...fixture.auditProof, schemaVersion: 2 },
      { ...fixture.auditProof, auditEventCount: 99 },
      { ...fixture.auditProof, auditHeadSequence: 99 },
      { ...fixture.auditProof, auditHeadEventHash: "other" },
      { ...fixture.auditProof, environmentId: "other" },
      { ...fixture.auditProof, operationId: "other" },
      { ...fixture.auditProof, operationInputChecksum: "other" },
      { ...fixture.auditProof, operationPlanChecksum: "other" },
      { ...fixture.auditProof, operationTransitionCount: 99 },
      { ...fixture.auditProof, operationCreationEventHash: "" },
      { ...fixture.auditProof, operationTransitionFoldChecksum: "bad" },
    ] as const) {
      expect(
        () =>
          new SagaHistoryTransitionFolder(
            fixture.anchor,
            proof as SagaHistoryAuditProof,
            fixture.plan,
            digest,
          ),
      ).toThrowError(expect.objectContaining({ message: expect.stringMatching(/contradicts/u) }))
    }

    for (const plan of [
      await transitionPlan({ operationId: "other-operation" }),
      await transitionPlan({ inputChecksum: "other-input" }),
      await transitionPlan({ operationType: "saga:other@1" }),
    ]) {
      expect(
        () => new SagaHistoryTransitionFolder(fixture.anchor, fixture.auditProof, plan, digest),
      ).toThrowError(expect.objectContaining({ message: expect.stringMatching(/plan/u) }))
    }
    const generic = await transitionPlan({ operationType: "generic" })
    expect(
      () =>
        new SagaHistoryTransitionFolder(
          { ...fixture.anchor, operationPlanChecksum: generic.planChecksum },
          { ...fixture.auditProof, operationPlanChecksum: generic.planChecksum },
          generic,
          digest,
        ),
    ).toThrowError(expect.objectContaining({ message: expect.stringMatching(/plan/u) }))
    expect(
      () =>
        new SagaHistoryTransitionFolder(
          fixture.anchor,
          fixture.auditProof,
          structuredClone(fixture.plan),
          digest,
        ),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
  })

  it("rejects malformed transition page ownership and pagination envelopes", async () => {
    const fixture = await transitionHistory()
    const first = fixture.rows[0] as SagaHistoryTransitionRow
    const second = fixture.rows[1] as SagaHistoryTransitionRow
    const cases: readonly [unknown, RegExp][] = [
      [
        {
          complete: true,
          nextCursor: null,
          rows: [{ ...first, audit_event_json: () => "bad" }],
        },
        /captured/u,
      ],
      [{ complete: true, extra: true, nextCursor: null, rows: [first] }, /fields/u],
      [{ complete: "yes", nextCursor: null, rows: [first] }, /metadata/u],
      [{ complete: true, nextCursor: null, rows: {} }, /metadata/u],
      [{ complete: true, nextCursor: null, rows: [] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [first, second, first] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [{ ...first, extra: true }] }, /row envelope/u],
      [
        {
          complete: true,
          nextCursor: { auditSequence: first.audit_sequence, transitionId: first.transition_id },
          rows: [first],
        },
        /retained a cursor/u,
      ],
      [{ complete: false, nextCursor: null, rows: [first, second] }, /pagination/u],
      [
        {
          complete: false,
          nextCursor: { auditSequence: first.audit_sequence, transitionId: first.transition_id },
          rows: [first],
        },
        /pagination/u,
      ],
      [
        {
          complete: false,
          nextCursor: { auditSequence: 99, transitionId: second.transition_id },
          rows: [first, second],
        },
        /pagination/u,
      ],
      [
        {
          complete: false,
          nextCursor: { auditSequence: second.audit_sequence, transitionId: "wrong" },
          rows: [first, second],
        },
        /pagination/u,
      ],
    ]
    for (const [candidate, message] of cases) {
      const folder = new SagaHistoryTransitionFolder(
        fixture.anchor,
        fixture.auditProof,
        fixture.plan,
        digest,
      )
      await expect(
        folder.append(
          candidate as SagaHistoryPage<SagaHistoryTransitionRow, SagaHistoryTransitionCursor>,
        ),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
        message: expect.stringMatching(message),
      })
    }
  })

  it("rejects malformed ordering, predecessors, and successor projections", async () => {
    const fixture = await transitionHistory()
    const first = fixture.rows[0] as SagaHistoryTransitionRow
    const malformed: readonly [number, SagaHistoryTransitionRow, RegExp][] = [
      [0, { ...first, operation_id: "other" }, /malformed or unordered/u],
      [0, { ...first, audit_sequence: 0 }, /malformed or unordered/u],
      [0, { ...first, created_at_ms: -1 }, /malformed or unordered/u],
      [0, { ...first, fencing_token: 0 }, /malformed or unordered/u],
      [0, { ...first, transition_id: "" }, /malformed or unordered/u],
      [0, { ...first, audit_sequence: 99 }, /malformed or unordered/u],
      [
        0,
        {
          ...first,
          audit_sequence: fixture.anchor.operationTransitionLastAuditSequence,
          transition_id: `${fixture.anchor.operationTransitionLastId}z`,
        },
        /malformed or unordered/u,
      ],
      [
        1,
        {
          ...(fixture.rows[1] as SagaHistoryTransitionRow),
          audit_sequence: first.audit_sequence,
          transition_id: first.transition_id,
        },
        /malformed or unordered/u,
      ],
      [0, { ...first, step_id: "missing-step" }, /predecessor/u],
      [0, { ...first, lease_key: "other-lease" }, /predecessor/u],
      [0, { ...first, from_record_json: "{}" }, /predecessor/u],
      [0, { ...first, to_record_json: first.from_record_json }, /predecessor/u],
      [0, { ...first, to_record_json: "{" }, /invalid JSON/u],
      [
        0,
        { ...first, to_record_json: JSON.stringify(JSON.parse(first.to_record_json), null, 2) },
        /successor/u,
      ],
      [0, { ...first, from_operation_status: "paused" }, /successor/u],
      [0, { ...first, to_operation_status: "paused" }, /successor/u],
    ]
    for (const [index, candidate, message] of malformed) {
      await expectTransitionFailure(fixture, index, candidate, message)
    }

    const equalSequence = {
      ...(fixture.rows[1] as SagaHistoryTransitionRow),
      audit_sequence: first.audit_sequence,
      transition_id: `z-${first.transition_id}`,
    }
    await expectTransitionFailure(fixture, 1, equalSequence, /audit evidence/u)
  })

  it("rejects contradictory audit bodies, identities, states, and direct payloads", async () => {
    const fixture = await transitionHistory()
    const first = fixture.rows[0] as SagaHistoryTransitionRow
    const firstEvent = fixture.events[1] as AuditEvent
    await expectTransitionFailure(fixture, 0, { ...first, audit_event_json: "{" }, /invalid JSON/u)
    const noncanonical = JSON.stringify({
      eventHash: firstEvent.eventHash,
      actorChecksum: firstEvent.actorChecksum,
      environmentId: firstEvent.environmentId,
      eventType: firstEvent.eventType,
      fencingToken: firstEvent.fencingToken,
      idempotencyKey: firstEvent.idempotencyKey,
      operationId: firstEvent.operationId,
      payloadChecksum: firstEvent.payloadChecksum,
      previousHash: firstEvent.previousHash,
      schemaVersion: firstEvent.schemaVersion,
      sequence: firstEvent.sequence,
      serverTimeMs: firstEvent.serverTimeMs,
      stepId: firstEvent.stepId,
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...first, audit_event_json: noncanonical },
      /audit evidence/u,
    )
    await expectTransitionFailure(
      fixture,
      0,
      { ...first, audit_event_hash: "wrong" },
      /audit evidence/u,
    )

    for (const overrides of [
      { environmentId: "other" },
      { operationId: "other" },
      { sequence: 99 },
      { stepId: "other" },
      { fencingToken: 2 },
      { idempotencyKey: "other" },
      { serverTimeMs: first.created_at_ms + 1 },
      { eventType: "unknown.event" },
    ] as const) {
      await expectTransitionFailure(
        fixture,
        0,
        await withAuditEvent(first, firstEvent, overrides),
        /audit evidence/u,
      )
    }

    const wrongIdentityEvent = await withAuditEvent(first, firstEvent, {
      idempotencyKey: "wrong-transition",
    })
    await expectTransitionFailure(
      fixture,
      0,
      { ...wrongIdentityEvent, transition_id: "wrong-transition" },
      /stable identity/u,
    )
    await expectTransitionFailure(
      fixture,
      0,
      await withAuditEvent(first, firstEvent, { eventType: "step.not_required" }),
      /stable identity/u,
    )
    await expectTransitionFailure(
      fixture,
      0,
      await withAuditEvent(first, firstEvent, { eventType: "saga.initialized" }),
      /stable identity/u,
    )
    await expectTransitionFailure(
      fixture,
      0,
      await withAuditEvent(first, firstEvent, { payloadChecksum: "wrong-payload" }),
      /audit payload/u,
    )
  })

  it("verifies legacy and full irreversible dispatch receipts and authorization preservation", async () => {
    for (const protocolVersion of [1, 2] as const) {
      const fixture = await irreversibleTransitionHistory(protocolVersion)
      const folder = new SagaHistoryTransitionFolder(
        fixture.anchor,
        fixture.auditProof,
        fixture.plan,
        digest,
      )
      await folder.append(transitionPage(fixture.rows, true))
      expect(folder.proof()).toMatchObject({ operationStatus: "succeeded", transitionCount: 2 })
    }

    const full = await irreversibleTransitionHistory(2)
    const completion = full.rows[1] as SagaHistoryTransitionRow
    const unexpectedReceipt = {
      ...completion,
      authorization_checksum: full.fullAuthorization.authorizationChecksum,
      authorization_classified_at_ms: completion.created_at_ms,
      authorization_id: full.fullAuthorization.authorizationId,
      authorization_protocol_version: 2,
      authorization_transition_id: completion.transition_id,
    }
    const receiptFolder = new SagaHistoryTransitionFolder(
      full.anchor,
      full.auditProof,
      full.plan,
      digest,
    )
    await expect(
      receiptFolder.append(
        transitionPage([full.rows[0] as SagaHistoryTransitionRow, unexpectedReceipt], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/non-dispatch/u) })

    const parsedCompletion = JSON.parse(completion.to_record_json) as Record<string, unknown>
    delete parsedCompletion.irreversibleAuthorization
    const lostBody = {
      ...completion,
      to_record_json: operationStepRecordJson(
        parsedCompletion as unknown as OperationRecord["steps"][string],
      ),
    }
    const preservationFolder = new SagaHistoryTransitionFolder(
      full.anchor,
      full.auditProof,
      full.plan,
      digest,
    )
    await expect(
      preservationFolder.append(
        transitionPage([full.rows[0] as SagaHistoryTransitionRow, lostBody], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/preserve/u) })
  })

  it("folds failed attempts and deferred reconciliation evidence", async () => {
    const fixture = await reconciliationTransitionHistory()
    const folder = new SagaHistoryTransitionFolder(
      fixture.anchor,
      fixture.auditProof,
      fixture.plan,
      digest,
    )
    await folder.append(transitionPage(fixture.rows.slice(0, 2), false))
    await folder.append(transitionPage(fixture.rows.slice(2), true))
    expect(folder.proof()).toMatchObject({
      operationStatus: "intervention_required",
      transitionCount: 3,
    })
  })

  it("replays applied and not-applied generic reconciliation with counters", async () => {
    const plan = await transitionPlan()
    const initial = createOperationRecord(plan)
    const started = startStep(initial, "saga:init", "generic-reconciliation-attempt", 2)
    const unknown = recordStepFailure(started, {
      attemptId: "generic-reconciliation-attempt",
      errorChecksum: "generic-unknown",
      outcome: "unknown",
      stepId: "saga:init",
    })
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const acceptedId = operationTransitionIdentity("accepted", [
      plan.operationId,
      "saga:init",
      "generic-reconciliation-attempt",
    ])
    const accepted = await event(created, {
      eventType: "step.attempt.accepted",
      idempotencyKey: acceptedId,
      operationId: plan.operationId,
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    const failedId = operationTransitionIdentity("failed", [
      plan.operationId,
      "saga:init",
      "generic-reconciliation-attempt",
    ])
    const failed = await event(accepted, {
      eventType: "step.attempt.unknown",
      idempotencyKey: failedId,
      operationId: plan.operationId,
      payloadChecksum: "generic-unknown",
      stepId: "saga:init",
    })
    for (const outcome of ["applied", "not_applied"] as const) {
      const evidenceChecksum = `generic-${outcome}`
      const reconciled = recordStepReconciliation(unknown, {
        counters: { cost: { reconciliations: 1 } },
        evidenceChecksum,
        ...(outcome === "applied"
          ? {
              observedPostconditionChecksum: "saga-init-postcondition",
              resultChecksum: evidenceChecksum,
            }
          : {}),
        outcome,
        stepId: "saga:init",
      })
      const reconciliationId = operationTransitionIdentity("reconciled", [
        plan.operationId,
        "saga:init",
        `generic-${outcome}`,
      ])
      const reconciliation = await event(failed, {
        eventType: `step.reconciled.${outcome}`,
        idempotencyKey: reconciliationId,
        operationId: plan.operationId,
        payloadChecksum: evidenceChecksum,
        stepId: "saga:init",
      })
      await foldCustomTransitionHistory(
        plan,
        [created, accepted, failed, reconciliation],
        [
          transitionRow(initial, started, accepted, "saga:init", acceptedId),
          transitionRow(started, unknown, failed, "saga:init", failedId),
          transitionRow(unknown, reconciled, reconciliation, "saga:init", reconciliationId),
        ],
        reconciled,
      )
    }
  })

  it("covers classified failure, recovery, not-required, retry, and permanent state shapes", async () => {
    const plan = await transitionPlan()

    for (const classified of [false, true]) {
      const stepId = classified ? "saga:forward:write" : "saga:init"
      const initial = createOperationRecord(plan)
      const started = startStep(initial, stepId, "failure-attempt", 2)
      const failed = recordStepFailure(started, {
        attemptId: "failure-attempt",
        errorChecksum: "failure-evidence",
        outcome: classified ? "unknown" : "permanent",
        stepId,
      })
      const created = await event(undefined, {
        eventType: "operation.created",
        fencingToken: null,
        operationId: "operation-a",
        payloadChecksum: plan.inputChecksum,
        stepId: null,
      })
      const acceptedId = operationTransitionIdentity("accepted", [
        "operation-a",
        stepId,
        "failure-attempt",
      ])
      const accepted = await event(created, {
        eventType: classified ? "saga.action.started" : "step.attempt.accepted",
        idempotencyKey: acceptedId,
        operationId: "operation-a",
        payloadChecksum: classified ? "action-start-evidence" : "saga-init-input",
        stepId,
      })
      const failedId = operationTransitionIdentity("failed", [
        "operation-a",
        stepId,
        "failure-attempt",
      ])
      const failure = await event(accepted, {
        eventType: classified ? "saga.action.classified" : "step.attempt.permanent",
        idempotencyKey: failedId,
        operationId: "operation-a",
        payloadChecksum: "failure-evidence",
        stepId,
      })
      const proof = await foldCustomTransitionHistory(
        plan,
        [created, accepted, failure],
        [
          transitionRow(initial, started, accepted, stepId, acceptedId),
          transitionRow(started, failed, failure, stepId, failedId),
        ],
        failed,
      )
      expect(proof.operationStatus).toBe(classified ? "reconciling" : "failed")
    }

    const recoveryStepId = "saga:forward:write"
    const recoveryInitial = createOperationRecord(plan)
    const recoveryStarted = startStep(recoveryInitial, recoveryStepId, "recovery-attempt", 2)
    const terminalUnknown = markRunningStepUnknownAfterCrash(
      recoveryStarted,
      recoveryStepId,
      "terminal-absence",
    )
    const recoveries = [
      {
        evidenceChecksum: "absence-evidence",
        operation: markRunningStepNotDispatchedAfterCrash(
          recoveryStarted,
          recoveryStepId,
          "absence-evidence",
        ),
      },
      {
        evidenceChecksum: "recovery-error",
        operation: markRunningStepUnknownAfterCrash(
          recoveryStarted,
          recoveryStepId,
          "recovery-error",
        ),
      },
      {
        evidenceChecksum: "terminal-absence",
        operation: recordSagaStepTerminalClassification(terminalUnknown, {
          outcome: "not_applied",
          receiptOutcomeChecksum: "terminal-absence",
          stepId: recoveryStepId,
        }),
      },
    ]
    for (const [index, recovery] of recoveries.entries()) {
      const created = await event(undefined, {
        eventType: "operation.created",
        fencingToken: null,
        operationId: "operation-a",
        payloadChecksum: plan.inputChecksum,
        stepId: null,
      })
      const acceptedId = operationTransitionIdentity("accepted", [
        "operation-a",
        recoveryStepId,
        "recovery-attempt",
      ])
      const accepted = await event(created, {
        eventType: "saga.action.started",
        idempotencyKey: acceptedId,
        operationId: "operation-a",
        payloadChecksum: "recovery-start-evidence",
        stepId: recoveryStepId,
      })
      const recoveryId = operationTransitionIdentity("crash-recovered", [
        "operation-a",
        recoveryStepId,
        `recovery-${index}`,
      ])
      const recoveredEvent = await event(accepted, {
        eventType: "saga.action.recovered",
        idempotencyKey: recoveryId,
        operationId: "operation-a",
        payloadChecksum: recovery.evidenceChecksum,
        fencingToken: 2,
        stepId: recoveryStepId,
      })
      await foldCustomTransitionHistory(
        plan,
        [created, accepted, recoveredEvent],
        [
          transitionRow(recoveryInitial, recoveryStarted, accepted, recoveryStepId, acceptedId),
          {
            ...transitionRow(
              recoveryStarted,
              recovery.operation,
              recoveredEvent,
              recoveryStepId,
              recoveryId,
            ),
            fencing_token: 2,
          },
        ],
        recovery.operation,
      )
    }

    const retryInitial = createOperationRecord(plan)
    const firstAttempt = startStep(retryInitial, "saga:init", "retry-attempt-1", 2)
    const retryable = recordStepFailure(firstAttempt, {
      attemptId: "retry-attempt-1",
      errorChecksum: "retry-error",
      outcome: "definitely_not_applied",
      stepId: "saga:init",
    })
    const secondAttempt = startStep(retryable, "saga:init", "retry-attempt-2", 4)
    const retryCreated = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const firstAcceptedId = operationTransitionIdentity("accepted", [
      "operation-a",
      "saga:init",
      "retry-attempt-1",
    ])
    const firstAccepted = await event(retryCreated, {
      eventType: "step.attempt.accepted",
      idempotencyKey: firstAcceptedId,
      operationId: "operation-a",
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    const retryFailedId = operationTransitionIdentity("failed", [
      "operation-a",
      "saga:init",
      "retry-attempt-1",
    ])
    const retryFailed = await event(firstAccepted, {
      eventType: "step.attempt.definitely_not_applied",
      idempotencyKey: retryFailedId,
      operationId: "operation-a",
      payloadChecksum: "retry-error",
      stepId: "saga:init",
    })
    const secondAcceptedId = operationTransitionIdentity("accepted", [
      "operation-a",
      "saga:init",
      "retry-attempt-2",
    ])
    const secondAccepted = await event(retryFailed, {
      eventType: "step.attempt.accepted",
      idempotencyKey: secondAcceptedId,
      operationId: "operation-a",
      payloadChecksum: "saga-init-input",
      stepId: "saga:init",
    })
    await foldCustomTransitionHistory(
      plan,
      [retryCreated, firstAccepted, retryFailed, secondAccepted],
      [
        transitionRow(retryInitial, firstAttempt, firstAccepted, "saga:init", firstAcceptedId),
        transitionRow(firstAttempt, retryable, retryFailed, "saga:init", retryFailedId),
        transitionRow(retryable, secondAttempt, secondAccepted, "saga:init", secondAcceptedId),
      ],
      secondAttempt,
    )

    const notRequired = markOperationStepNotRequired(createOperationRecord(plan), {
      evidenceChecksum: "not-required-evidence",
      stepId: "saga:forward:write",
    })
    const notRequiredCreated = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const notRequiredId = operationTransitionIdentity("not-required", [
      "operation-a",
      "saga:forward:write",
      "decision-a",
    ])
    const notRequiredEvent = await event(notRequiredCreated, {
      eventType: "step.not_required",
      idempotencyKey: notRequiredId,
      operationId: "operation-a",
      payloadChecksum: "not-required-evidence",
      stepId: "saga:forward:write",
    })
    await foldCustomTransitionHistory(
      plan,
      [notRequiredCreated, notRequiredEvent],
      [
        transitionRow(
          createOperationRecord(plan),
          notRequired,
          notRequiredEvent,
          "saga:forward:write",
          notRequiredId,
        ),
      ],
      notRequired,
    )
  })

  it("reconstructs protocol-one generic outcomes and crash recovery without granting authority", async () => {
    const base = await legacyTransitionBase("provider_receipt")

    const success = await stripAuthorizationBody(
      recordStepSuccess(base.fullStarted, {
        attemptId: "legacy-attempt",
        counters: { cost: { requests: 2 }, progress: { bytes: 3 } },
        observedPostconditionChecksum: "legacy-postcondition",
        resultChecksum: "legacy-result",
        stepId: base.stepId,
      }),
      base.stepId,
    )
    const successId = operationTransitionIdentity("succeeded", [
      base.plan.operationId,
      base.stepId,
      "legacy-attempt",
    ])
    const succeeded = await event(base.accepted, {
      eventType: "step.attempt.succeeded",
      idempotencyKey: successId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-result",
      stepId: base.stepId,
    })
    await foldCustomTransitionHistory(
      base.plan,
      [base.created, base.accepted, succeeded],
      [
        base.dispatch,
        transitionRow(base.legacyStarted, success, succeeded, base.stepId, successId),
      ],
      success,
    )

    for (const outcome of ["definitely_not_applied", "permanent", "unknown"] as const) {
      const failed = await stripAuthorizationBody(
        recordStepFailure(base.fullStarted, {
          attemptId: "legacy-attempt",
          counters: { cost: { failures: 1 } },
          errorChecksum: `legacy-${outcome}`,
          outcome,
          stepId: base.stepId,
        }),
        base.stepId,
      )
      const failureId = operationTransitionIdentity("failed", [
        base.plan.operationId,
        base.stepId,
        "legacy-attempt",
      ])
      const failure = await event(base.accepted, {
        eventType: `step.attempt.${outcome}`,
        idempotencyKey: failureId,
        operationId: base.plan.operationId,
        payloadChecksum: `legacy-${outcome}`,
        stepId: base.stepId,
      })
      await foldCustomTransitionHistory(
        base.plan,
        [base.created, base.accepted, failure],
        [base.dispatch, transitionRow(base.legacyStarted, failed, failure, base.stepId, failureId)],
        failed,
      )
    }

    const never = await legacyTransitionBase("provider_receipt", "never")
    const terminalNotApplied = await stripAuthorizationBody(
      recordStepFailure(never.fullStarted, {
        attemptId: "legacy-attempt",
        errorChecksum: "legacy-never",
        outcome: "definitely_not_applied",
        stepId: never.stepId,
      }),
      never.stepId,
    )
    const terminalFailureId = operationTransitionIdentity("failed", [
      never.plan.operationId,
      never.stepId,
      "legacy-attempt",
    ])
    const terminalFailure = await event(never.accepted, {
      eventType: "step.attempt.definitely_not_applied",
      idempotencyKey: terminalFailureId,
      operationId: never.plan.operationId,
      payloadChecksum: "legacy-never",
      stepId: never.stepId,
    })
    await foldCustomTransitionHistory(
      never.plan,
      [never.created, never.accepted, terminalFailure],
      [
        never.dispatch,
        transitionRow(
          never.legacyStarted,
          terminalNotApplied,
          terminalFailure,
          never.stepId,
          terminalFailureId,
        ),
      ],
      terminalNotApplied,
    )

    const recoveryId = operationTransitionIdentity("crash-recovered", [
      base.plan.operationId,
      base.stepId,
      "legacy-recovery",
    ])
    const absenceEvidence = await digest(
      new TextEncoder().encode(
        operationTransitionIdentity("provider-not-dispatched-evidence", [
          base.plan.operationId,
          base.stepId,
          "legacy-attempt",
          "2",
        ]),
      ),
    )
    const absent = await stripAuthorizationBody(
      markRunningStepNotDispatchedAfterCrash(base.fullStarted, base.stepId, absenceEvidence),
      base.stepId,
    )
    const absentEvent = await event(base.accepted, {
      eventType: "step.crash.not_dispatched",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-attempt",
      stepId: base.stepId,
    })
    await foldCustomTransitionHistory(
      base.plan,
      [base.created, base.accepted, absentEvent],
      [
        base.dispatch,
        {
          ...transitionRow(base.legacyStarted, absent, absentEvent, base.stepId, recoveryId),
          fencing_token: 2,
        },
      ],
      absent,
    )

    const unknown = await stripAuthorizationBody(
      markRunningStepUnknownAfterCrash(base.fullStarted, base.stepId, "legacy-provider-acceptance"),
      base.stepId,
    )
    const unknownEvent = await event(base.accepted, {
      eventType: "step.crash.outcome_unknown",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-provider-acceptance",
      stepId: base.stepId,
    })
    await foldCustomTransitionHistory(
      base.plan,
      [base.created, base.accepted, unknownEvent],
      [
        base.dispatch,
        {
          ...transitionRow(base.legacyStarted, unknown, unknownEvent, base.stepId, recoveryId),
          fencing_token: 2,
        },
      ],
      unknown,
    )

    const failedFull = recordStepFailure(base.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "legacy-unknown",
      outcome: "unknown",
      stepId: base.stepId,
    })
    const failed = await stripAuthorizationBody(failedFull, base.stepId)
    const failureId = operationTransitionIdentity("failed", [
      base.plan.operationId,
      base.stepId,
      "legacy-attempt",
    ])
    const failureEvent = await event(base.accepted, {
      eventType: "step.attempt.unknown",
      idempotencyKey: failureId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-unknown",
      stepId: base.stepId,
    })
    const failureRow = transitionRow(
      base.legacyStarted,
      failed,
      failureEvent,
      base.stepId,
      failureId,
    )
    for (const outcome of ["applied", "indeterminate", "not_applied"] as const) {
      const evidenceChecksum = `legacy-reconciliation-${outcome}`
      const reconciledFull = recordStepReconciliation(failedFull, {
        counters: { progress: { reconciliations: 1 } },
        evidenceChecksum,
        ...(outcome === "applied"
          ? {
              observedPostconditionChecksum: "legacy-postcondition",
              resultChecksum: evidenceChecksum,
            }
          : {}),
        outcome,
        stepId: base.stepId,
      })
      const reconciled = await stripAuthorizationBody(reconciledFull, base.stepId)
      const reconciliationId = operationTransitionIdentity("reconciled", [
        base.plan.operationId,
        base.stepId,
        `legacy-reconciliation-${outcome}`,
      ])
      const reconciliation = await event(failureEvent, {
        eventType: `step.reconciled.${outcome}`,
        fencingToken: 2,
        idempotencyKey: reconciliationId,
        operationId: base.plan.operationId,
        payloadChecksum: evidenceChecksum,
        stepId: base.stepId,
      })
      await foldCustomTransitionHistory(
        base.plan,
        [base.created, base.accepted, failureEvent, reconciliation],
        [
          base.dispatch,
          failureRow,
          {
            ...transitionRow(failed, reconciled, reconciliation, base.stepId, reconciliationId),
            fencing_token: 2,
          },
        ],
        reconciled,
      )
    }

    const retryableFull = recordStepFailure(base.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "legacy-retryable",
      outcome: "definitely_not_applied",
      stepId: base.stepId,
    })
    const retryable = await stripAuthorizationBody(retryableFull, base.stepId)
    const retryableEvent = await event(base.accepted, {
      eventType: "step.attempt.definitely_not_applied",
      idempotencyKey: failureId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-retryable",
      stepId: base.stepId,
    })
    const forgedStep = {
      ...(retryable.steps[base.stepId] as NonNullable<OperationRecord["steps"][string]>),
      reconciliationEvidenceChecksum: "forged-reconciliation",
      resultChecksum: "forged-reconciliation",
      state: "succeeded" as const,
    }
    const forged = await loadOperationRecord(
      { plan: base.plan, steps: { [base.stepId]: forgedStep } },
      digest,
    )
    const forgedId = operationTransitionIdentity("reconciled", [
      base.plan.operationId,
      base.stepId,
      "forged-reconciliation",
    ])
    const forgedEvent = await event(retryableEvent, {
      eventType: "step.reconciled.applied",
      fencingToken: 2,
      idempotencyKey: forgedId,
      operationId: base.plan.operationId,
      payloadChecksum: "forged-reconciliation",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, retryableEvent, forgedEvent],
      [
        base.dispatch,
        transitionRow(base.legacyStarted, retryable, retryableEvent, base.stepId, failureId),
        {
          ...transitionRow(retryable, forged, forgedEvent, base.stepId, forgedId),
          fencing_token: 2,
        },
      ],
      forged,
      /exact uncertain predecessor|exact core state transition/u,
    )
  })

  it("reconstructs protocol-one saga recovery and observation with exact evidence", async () => {
    const base = await legacyTransitionBase("saga_receipt")
    const recoveryId = operationTransitionIdentity("crash-recovered", [
      base.plan.operationId,
      base.stepId,
      "legacy-saga-recovery",
    ])
    const recoveredFull = [
      {
        evidenceChecksum: "legacy-accepted-unknown",
        operation: markRunningStepUnknownAfterCrash(
          base.fullStarted,
          base.stepId,
          "legacy-accepted-unknown",
        ),
      },
      {
        evidenceChecksum: "legacy-not-dispatched",
        operation: markRunningStepNotDispatchedAfterCrash(
          base.fullStarted,
          base.stepId,
          "legacy-not-dispatched",
        ),
      },
    ]
    const terminalUnknown = markRunningStepUnknownAfterCrash(
      base.fullStarted,
      base.stepId,
      "legacy-terminal-absence",
    )
    recoveredFull.push({
      evidenceChecksum: "legacy-terminal-absence",
      operation: recordSagaStepTerminalClassification(terminalUnknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "legacy-terminal-absence",
        stepId: base.stepId,
      }),
    })
    for (const recovery of recoveredFull) {
      const recovered = await stripAuthorizationBody(recovery.operation, base.stepId)
      const recoveredEvent = await event(base.accepted, {
        eventType: "saga.action.recovered",
        fencingToken: 2,
        idempotencyKey: recoveryId,
        operationId: base.plan.operationId,
        payloadChecksum: recovery.evidenceChecksum,
        stepId: base.stepId,
      })
      await foldCustomTransitionHistory(
        base.plan,
        [base.created, base.accepted, recoveredEvent],
        [
          base.dispatch,
          {
            ...transitionRow(
              base.legacyStarted,
              recovered,
              recoveredEvent,
              base.stepId,
              recoveryId,
            ),
            fencing_token: 2,
          },
        ],
        recovered,
      )
    }

    const fullUnknown = recordStepFailure(base.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "legacy-unknown-cause",
      outcome: "unknown",
      stepId: base.stepId,
    })
    const legacyUnknown = await stripAuthorizationBody(fullUnknown, base.stepId)
    const failedId = operationTransitionIdentity("failed", [
      base.plan.operationId,
      base.stepId,
      "legacy-attempt",
    ])
    const failed = await event(base.accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-unknown-cause",
      stepId: base.stepId,
    })
    const failureRow = transitionRow(
      base.legacyStarted,
      legacyUnknown,
      failed,
      base.stepId,
      failedId,
    )
    for (const outcome of ["applied", "indeterminate", "not_applied"] as const) {
      const evidenceChecksum = `legacy-observation-${outcome}`
      const observedFull = recordStepReconciliation(fullUnknown, {
        evidenceChecksum,
        ...(outcome === "applied"
          ? {
              observedPostconditionChecksum: "legacy-postcondition",
              resultChecksum: evidenceChecksum,
            }
          : {}),
        outcome,
        stepId: base.stepId,
      })
      const observed = await stripAuthorizationBody(observedFull, base.stepId)
      const observationId = operationTransitionIdentity("reconciled", [
        base.plan.operationId,
        base.stepId,
        `observation-${outcome}`,
      ])
      const observation = await event(failed, {
        eventType: "saga.action.observed",
        fencingToken: 2,
        idempotencyKey: observationId,
        operationId: base.plan.operationId,
        payloadChecksum: evidenceChecksum,
        stepId: base.stepId,
      })
      await foldCustomTransitionHistory(
        base.plan,
        [base.created, base.accepted, failed, observation],
        [
          base.dispatch,
          failureRow,
          {
            ...transitionRow(legacyUnknown, observed, observation, base.stepId, observationId),
            fencing_token: 2,
          },
        ],
        observed,
      )
    }

    const fullRetryable = recordStepFailure(base.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "legacy-proven-not-applied",
      outcome: "definitely_not_applied",
      stepId: base.stepId,
    })
    const legacyRetryable = await stripAuthorizationBody(fullRetryable, base.stepId)
    const retryableEvent = await event(base.accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-proven-not-applied",
      stepId: base.stepId,
    })
    const terminal = await stripAuthorizationBody(
      recordSagaStepTerminalClassification(fullRetryable, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "legacy-proven-not-applied",
        stepId: base.stepId,
      }),
      base.stepId,
    )
    const terminalObservationId = operationTransitionIdentity("reconciled", [
      base.plan.operationId,
      base.stepId,
      "terminal-observation",
    ])
    const terminalObservation = await event(retryableEvent, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: terminalObservationId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-proven-not-applied",
      stepId: base.stepId,
    })
    const terminalRows = [
      base.dispatch,
      transitionRow(base.legacyStarted, legacyRetryable, retryableEvent, base.stepId, failedId),
      {
        ...transitionRow(
          legacyRetryable,
          terminal,
          terminalObservation,
          base.stepId,
          terminalObservationId,
        ),
        fencing_token: 2,
      },
    ]
    await foldCustomTransitionHistory(
      base.plan,
      [base.created, base.accepted, retryableEvent, terminalObservation],
      terminalRows,
      terminal,
    )

    const contradictoryObservation = await event(retryableEvent, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: terminalObservationId,
      operationId: base.plan.operationId,
      payloadChecksum: "different-observation",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, retryableEvent, contradictoryObservation],
      [
        terminalRows[0] as SagaHistoryTransitionRow,
        terminalRows[1] as SagaHistoryTransitionRow,
        {
          ...(terminalRows[2] as SagaHistoryTransitionRow),
          audit_event_hash: contradictoryObservation.eventHash,
          audit_event_json: JSON.stringify(contradictoryObservation),
        },
      ],
      terminal,
      /proven non-application evidence|audit payload|exact core state transition/u,
    )
  })

  it("rejects every impossible protocol-one predecessor and event transition", async () => {
    const base = await legacyTransitionBase("saga_receipt")
    const fullUnknown = recordStepFailure(base.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "legacy-unknown",
      outcome: "unknown",
      stepId: base.stepId,
    })
    const legacyUnknown = await stripAuthorizationBody(fullUnknown, base.stepId)
    const failedId = operationTransitionIdentity("failed", [
      base.plan.operationId,
      base.stepId,
      "legacy-attempt",
    ])
    const failed = await event(base.accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: failedId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-unknown",
      stepId: base.stepId,
    })
    const failedRow = transitionRow(
      base.legacyStarted,
      legacyUnknown,
      failed,
      base.stepId,
      failedId,
    )
    const reconciledFull = recordStepReconciliation(fullUnknown, {
      evidenceChecksum: "legacy-reconciled",
      observedPostconditionChecksum: "legacy-postcondition",
      outcome: "applied",
      resultChecksum: "legacy-reconciled",
      stepId: base.stepId,
    })
    const reconciled = await stripAuthorizationBody(reconciledFull, base.stepId)
    const wrongSuccessId = operationTransitionIdentity("succeeded", [
      base.plan.operationId,
      base.stepId,
      "legacy-attempt",
    ])
    const wrongSuccess = await event(failed, {
      eventType: "saga.action.classified",
      fencingToken: 2,
      idempotencyKey: wrongSuccessId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-reconciled",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, failed, wrongSuccess],
      [
        base.dispatch,
        failedRow,
        {
          ...transitionRow(legacyUnknown, reconciled, wrongSuccess, base.stepId, wrongSuccessId),
          fencing_token: 2,
        },
      ],
      reconciled,
      /exact core state transition/u,
    )

    const retryableFull = recordStepReconciliation(fullUnknown, {
      evidenceChecksum: "legacy-not-applied",
      outcome: "not_applied",
      stepId: base.stepId,
    })
    const retryable = await stripAuthorizationBody(retryableFull, base.stepId)
    const wrongFailure = await event(failed, {
      eventType: "saga.action.classified",
      fencingToken: 2,
      idempotencyKey: failedId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-unknown",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, failed, wrongFailure],
      [
        base.dispatch,
        failedRow,
        {
          ...transitionRow(legacyUnknown, retryable, wrongFailure, base.stepId, failedId),
          fencing_token: 2,
        },
      ],
      retryable,
      /exact core state transition/u,
    )

    const recoveryId = operationTransitionIdentity("crash-recovered", [
      base.plan.operationId,
      base.stepId,
      "wrong-predecessor",
    ])
    const wrongRecovery = await event(failed, {
      eventType: "saga.action.recovered",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-not-applied",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, failed, wrongRecovery],
      [
        base.dispatch,
        failedRow,
        {
          ...transitionRow(legacyUnknown, retryable, wrongRecovery, base.stepId, recoveryId),
          fencing_token: 2,
        },
      ],
      retryable,
      /exact core state transition/u,
    )

    const permanentlyFailed = await stripAuthorizationBody(
      recordStepFailure(base.fullStarted, {
        attemptId: "legacy-attempt",
        errorChecksum: "legacy-permanent",
        outcome: "permanent",
        stepId: base.stepId,
      }),
      base.stepId,
    )
    const impossibleRecovery = await event(base.accepted, {
      eventType: "saga.action.recovered",
      fencingToken: 2,
      idempotencyKey: recoveryId,
      operationId: base.plan.operationId,
      payloadChecksum: "legacy-permanent",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, impossibleRecovery],
      [
        base.dispatch,
        {
          ...transitionRow(
            base.legacyStarted,
            permanentlyFailed,
            impossibleRecovery,
            base.stepId,
            recoveryId,
          ),
          fencing_token: 2,
        },
      ],
      permanentlyFailed,
      /exact core state transition/u,
    )

    const notRequiredId = operationTransitionIdentity("not-required", [
      base.plan.operationId,
      base.stepId,
      "unsupported-decision",
    ])
    const unsupported = await event(base.accepted, {
      eventType: "step.not_required",
      idempotencyKey: notRequiredId,
      operationId: base.plan.operationId,
      payloadChecksum: "unsupported-decision",
      stepId: base.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      base.plan,
      [base.created, base.accepted, unsupported],
      [
        base.dispatch,
        transitionRow(
          base.legacyStarted,
          permanentlyFailed,
          unsupported,
          base.stepId,
          notRequiredId,
        ),
      ],
      permanentlyFailed,
      /exact core state transition/u,
    )
  })

  it("covers opaque, never-retry, and invalid-state protocol-one branches", async () => {
    const opaque = await legacyTransitionBase("opaque")
    const opaqueUnknownFull = markRunningStepUnknownAfterCrash(opaque.fullStarted, opaque.stepId)
    const opaqueUnknown = await stripAuthorizationBody(opaqueUnknownFull, opaque.stepId)
    const opaqueRecoveryId = operationTransitionIdentity("crash-recovered", [
      opaque.plan.operationId,
      opaque.stepId,
      "opaque-legacy-recovery",
    ])
    const opaqueRecovered = await event(opaque.accepted, {
      eventType: "step.crash.outcome_unknown",
      fencingToken: 2,
      idempotencyKey: opaqueRecoveryId,
      operationId: opaque.plan.operationId,
      payloadChecksum: "legacy-attempt",
      stepId: opaque.stepId,
    })
    const opaqueRecoveryRow = {
      ...transitionRow(
        opaque.legacyStarted,
        opaqueUnknown,
        opaqueRecovered,
        opaque.stepId,
        opaqueRecoveryId,
      ),
      fencing_token: 2,
    }
    await foldCustomTransitionHistory(
      opaque.plan,
      [opaque.created, opaque.accepted, opaqueRecovered],
      [opaque.dispatch, opaqueRecoveryRow],
      opaqueUnknown,
    )

    const opaqueReconciledFull = recordStepReconciliation(opaqueUnknownFull, {
      counters: { progress: { observations: 1 } },
      evidenceChecksum: "opaque-reconciliation",
      observedPostconditionChecksum: "legacy-postcondition",
      outcome: "applied",
      resultChecksum: "opaque-reconciliation",
      stepId: opaque.stepId,
    })
    const opaqueReconciled = await stripAuthorizationBody(opaqueReconciledFull, opaque.stepId)
    const opaqueReconciliationId = operationTransitionIdentity("reconciled", [
      opaque.plan.operationId,
      opaque.stepId,
      "opaque-reconciliation",
    ])
    const opaqueReconciliation = await event(opaqueRecovered, {
      eventType: "step.reconciled.applied",
      fencingToken: 2,
      idempotencyKey: opaqueReconciliationId,
      operationId: opaque.plan.operationId,
      payloadChecksum: "opaque-reconciliation",
      stepId: opaque.stepId,
    })
    await foldCustomTransitionHistory(
      opaque.plan,
      [opaque.created, opaque.accepted, opaqueRecovered, opaqueReconciliation],
      [
        opaque.dispatch,
        opaqueRecoveryRow,
        {
          ...transitionRow(
            opaqueUnknown,
            opaqueReconciled,
            opaqueReconciliation,
            opaque.stepId,
            opaqueReconciliationId,
          ),
          fencing_token: 2,
        },
      ],
      opaqueReconciled,
    )

    const never = await legacyTransitionBase("provider_receipt", "never")
    const absenceEvidence = await digest(
      new TextEncoder().encode(
        operationTransitionIdentity("provider-not-dispatched-evidence", [
          never.plan.operationId,
          never.stepId,
          "legacy-attempt",
          "2",
        ]),
      ),
    )
    const neverAbsent = await stripAuthorizationBody(
      markRunningStepNotDispatchedAfterCrash(never.fullStarted, never.stepId, absenceEvidence),
      never.stepId,
    )
    const neverRecoveryId = operationTransitionIdentity("crash-recovered", [
      never.plan.operationId,
      never.stepId,
      "never-recovery",
    ])
    const neverRecovered = await event(never.accepted, {
      eventType: "step.crash.not_dispatched",
      fencingToken: 2,
      idempotencyKey: neverRecoveryId,
      operationId: never.plan.operationId,
      payloadChecksum: "legacy-attempt",
      stepId: never.stepId,
    })
    await foldCustomTransitionHistory(
      never.plan,
      [never.created, never.accepted, neverRecovered],
      [
        never.dispatch,
        {
          ...transitionRow(
            never.legacyStarted,
            neverAbsent,
            neverRecovered,
            never.stepId,
            neverRecoveryId,
          ),
          fencing_token: 2,
        },
      ],
      neverAbsent,
    )

    const neverUnknownFull = recordStepFailure(never.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "never-unknown",
      outcome: "unknown",
      stepId: never.stepId,
    })
    const neverUnknown = await stripAuthorizationBody(neverUnknownFull, never.stepId)
    const neverFailureId = operationTransitionIdentity("failed", [
      never.plan.operationId,
      never.stepId,
      "legacy-attempt",
    ])
    const neverFailed = await event(never.accepted, {
      eventType: "step.attempt.unknown",
      idempotencyKey: neverFailureId,
      operationId: never.plan.operationId,
      payloadChecksum: "never-unknown",
      stepId: never.stepId,
    })
    const neverReconciled = await stripAuthorizationBody(
      recordStepReconciliation(neverUnknownFull, {
        evidenceChecksum: "never-not-applied",
        outcome: "not_applied",
        stepId: never.stepId,
      }),
      never.stepId,
    )
    const neverReconciliationId = operationTransitionIdentity("reconciled", [
      never.plan.operationId,
      never.stepId,
      "never-reconciliation",
    ])
    const neverReconciliation = await event(neverFailed, {
      eventType: "step.reconciled.not_applied",
      fencingToken: 2,
      idempotencyKey: neverReconciliationId,
      operationId: never.plan.operationId,
      payloadChecksum: "never-not-applied",
      stepId: never.stepId,
    })
    await foldCustomTransitionHistory(
      never.plan,
      [never.created, never.accepted, neverFailed, neverReconciliation],
      [
        never.dispatch,
        transitionRow(never.legacyStarted, neverUnknown, neverFailed, never.stepId, neverFailureId),
        {
          ...transitionRow(
            neverUnknown,
            neverReconciled,
            neverReconciliation,
            never.stepId,
            neverReconciliationId,
          ),
          fencing_token: 2,
        },
      ],
      neverReconciled,
    )

    const saga = await legacyTransitionBase("saga_receipt")
    const sagaUnknownFull = recordStepFailure(saga.fullStarted, {
      attemptId: "legacy-attempt",
      errorChecksum: "invalid-observation-cause",
      outcome: "unknown",
      stepId: saga.stepId,
    })
    const sagaUnknown = await stripAuthorizationBody(sagaUnknownFull, saga.stepId)
    const sagaFailureId = operationTransitionIdentity("failed", [
      saga.plan.operationId,
      saga.stepId,
      "legacy-attempt",
    ])
    const sagaFailed = await event(saga.accepted, {
      eventType: "saga.action.classified",
      idempotencyKey: sagaFailureId,
      operationId: saga.plan.operationId,
      payloadChecksum: "invalid-observation-cause",
      stepId: saga.stepId,
    })
    const invalidObserved = await loadOperationRecord(
      {
        plan: saga.plan,
        steps: {
          [saga.stepId]: {
            ...(sagaUnknown.steps[saga.stepId] as NonNullable<OperationRecord["steps"][string]>),
            state: "failed",
          },
        },
      },
      digest,
    )
    const invalidObservationId = operationTransitionIdentity("reconciled", [
      saga.plan.operationId,
      saga.stepId,
      "invalid-observation",
    ])
    const invalidObservation = await event(sagaFailed, {
      eventType: "saga.action.observed",
      fencingToken: 2,
      idempotencyKey: invalidObservationId,
      operationId: saga.plan.operationId,
      payloadChecksum: "invalid-observation",
      stepId: saga.stepId,
    })
    await expectCustomTransitionHistoryFailure(
      saga.plan,
      [saga.created, saga.accepted, sagaFailed, invalidObservation],
      [
        saga.dispatch,
        transitionRow(saga.legacyStarted, sagaUnknown, sagaFailed, saga.stepId, sagaFailureId),
        {
          ...transitionRow(
            sagaUnknown,
            invalidObserved,
            invalidObservation,
            saga.stepId,
            invalidObservationId,
          ),
          fencing_token: 2,
        },
      ],
      invalidObserved,
      /exact core state transition/u,
    )
  })

  it("rejects a protocol-one dispatch whose declared dependency is pending", async () => {
    const stepId = "provider:dependent"
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: "operation-capabilities",
        idempotencyKey: "dependent-operation-key",
        inputChecksum: "operation-input-checksum",
        operationId: "operation-a",
        operationType: "saga:dependent-fixture@1",
        steps: [
          {
            checkpoint: "reversible",
            dependsOn: [],
            idempotencyKey: "prerequisite-key",
            inputChecksum: "prerequisite-input",
            leaseKey: "saga:saga-a",
            postconditionChecksum: "prerequisite-postcondition",
            preconditionChecksum: "prerequisite-precondition",
            recoveryInstructions: "Complete the prerequisite.",
            retryClassification: "idempotent",
            stepId: "prerequisite",
          },
          {
            checkpoint: "irreversible",
            dependsOn: ["prerequisite"],
            effectProtocol: "provider_receipt",
            idempotencyKey: "dependent-key",
            inputChecksum: "dependent-input",
            leaseKey: "saga:saga-a",
            postconditionChecksum: "dependent-postcondition",
            preconditionChecksum: "dependent-precondition",
            recoveryInstructions: "Never bypass the prerequisite.",
            retryClassification: "reconcile_first",
            stepId,
          },
        ],
      },
      digest,
    )
    const initial = createOperationRecord(plan)
    const lease = activeLease()
    const authorization = await sealIrreversibleAuthorization(
      plan,
      {
        actorChecksum: "authorization-actor",
        authorizationId: "dependent-authorization",
        decisionChecksum: "dependent-decision",
        lease,
        leaseProof: leaseProof(lease),
        sealedAtServerTimeMs: 1,
        stepId,
      },
      digest,
    )
    const before = initial.steps[stepId] as NonNullable<OperationRecord["steps"][string]>
    const forged = await loadOperationRecord(
      {
        plan,
        steps: {
          ...initial.steps,
          [stepId]: {
            activeAttemptId: "dependent-attempt",
            authorizationChecksum: authorization.authorizationChecksum,
            costCounters: before.costCounters,
            fencingToken: 1,
            lastAttemptId: "dependent-attempt",
            progressCounters: before.progressCounters,
            startedAttempts: 1,
            state: "running",
          },
        },
      },
      digest,
    )
    const created = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const transitionId = operationTransitionIdentity("accepted", [
      plan.operationId,
      stepId,
      "dependent-attempt",
    ])
    const accepted = await event(created, {
      eventType: "step.attempt.accepted",
      idempotencyKey: transitionId,
      operationId: plan.operationId,
      payloadChecksum: "dependent-input",
      stepId,
    })
    const dispatch = {
      ...transitionRow(initial, forged, accepted, stepId, transitionId),
      authorization_checksum: authorization.authorizationChecksum,
      authorization_classified_at_ms: accepted.serverTimeMs,
      authorization_id: null,
      authorization_protocol_version: 1,
      authorization_transition_id: transitionId,
    }
    await expectCustomTransitionHistoryFailure(
      plan,
      [created, accepted],
      [dispatch],
      forged,
      /exact core state transition/u,
    )

    const prerequisiteStarted = startStep(initial, "prerequisite", "prerequisite-attempt", 2)
    const prerequisiteSucceeded = succeedStep(
      prerequisiteStarted,
      "prerequisite",
      "prerequisite-attempt",
      "prerequisite-result",
    )
    const dependentBefore = prerequisiteSucceeded.steps[stepId] as NonNullable<
      OperationRecord["steps"][string]
    >
    const dependentStarted = await loadOperationRecord(
      {
        plan,
        steps: {
          ...prerequisiteSucceeded.steps,
          [stepId]: {
            activeAttemptId: "dependent-attempt",
            authorizationChecksum: authorization.authorizationChecksum,
            costCounters: dependentBefore.costCounters,
            fencingToken: 1,
            lastAttemptId: "dependent-attempt",
            progressCounters: dependentBefore.progressCounters,
            startedAttempts: 1,
            state: "running",
          },
        },
      },
      digest,
    )
    const validCreated = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: plan.operationId,
      payloadChecksum: plan.inputChecksum,
      stepId: null,
    })
    const prerequisiteAcceptedId = operationTransitionIdentity("accepted", [
      plan.operationId,
      "prerequisite",
      "prerequisite-attempt",
    ])
    const prerequisiteAccepted = await event(validCreated, {
      eventType: "step.attempt.accepted",
      idempotencyKey: prerequisiteAcceptedId,
      operationId: plan.operationId,
      payloadChecksum: "prerequisite-input",
      stepId: "prerequisite",
    })
    const prerequisiteSucceededId = operationTransitionIdentity("succeeded", [
      plan.operationId,
      "prerequisite",
      "prerequisite-attempt",
    ])
    const prerequisiteCompleted = await event(prerequisiteAccepted, {
      eventType: "step.attempt.succeeded",
      idempotencyKey: prerequisiteSucceededId,
      operationId: plan.operationId,
      payloadChecksum: "prerequisite-result",
      stepId: "prerequisite",
    })
    const dependentAccepted = await event(prerequisiteCompleted, {
      eventType: "step.attempt.accepted",
      idempotencyKey: transitionId,
      operationId: plan.operationId,
      payloadChecksum: "dependent-input",
      stepId,
    })
    const dependentDispatch = {
      ...transitionRow(
        prerequisiteSucceeded,
        dependentStarted,
        dependentAccepted,
        stepId,
        transitionId,
      ),
      authorization_checksum: authorization.authorizationChecksum,
      authorization_classified_at_ms: dependentAccepted.serverTimeMs,
      authorization_id: null,
      authorization_protocol_version: 1,
      authorization_transition_id: transitionId,
    }
    await foldCustomTransitionHistory(
      plan,
      [validCreated, prerequisiteAccepted, prerequisiteCompleted, dependentAccepted],
      [
        transitionRow(
          initial,
          prerequisiteStarted,
          prerequisiteAccepted,
          "prerequisite",
          prerequisiteAcceptedId,
        ),
        transitionRow(
          prerequisiteStarted,
          prerequisiteSucceeded,
          prerequisiteCompleted,
          "prerequisite",
          prerequisiteSucceededId,
        ),
        dependentDispatch,
      ],
      dependentStarted,
    )
  })

  it("rejects future-sealed bodies, protocol-one retries, and checksum-only retry predecessors", async () => {
    const full = await irreversibleTransitionHistory(2)
    const dispatch = full.rows[0] as SagaHistoryTransitionRow
    const lease = activeLease()
    const futureAuthorization = await sealIrreversibleAuthorization(
      full.plan,
      {
        actorChecksum: "authorization-actor",
        authorizationId: "future-authorization",
        decisionChecksum: "future-decision",
        lease,
        leaseProof: leaseProof(lease),
        sealedAtServerTimeMs: 3,
        stepId: "saga:forward:irreversible",
      },
      digest,
    )
    const futureStep = {
      ...(JSON.parse(dispatch.to_record_json) as OperationRecord["steps"][string]),
      authorizationChecksum: futureAuthorization.authorizationChecksum,
      irreversibleAuthorization: futureAuthorization,
    }
    const futureFolder = new SagaHistoryTransitionFolder(
      full.anchor,
      full.auditProof,
      full.plan,
      digest,
    )
    await expect(
      futureFolder.append(
        transitionPage(
          [
            {
              ...dispatch,
              authorization_checksum: futureAuthorization.authorizationChecksum,
              authorization_id: futureAuthorization.authorizationId,
              to_record_json: operationStepRecordJson(futureStep),
            },
            full.rows[1] as SagaHistoryTransitionRow,
          ],
          true,
        ),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/protocol-two/u) })

    const retry = await irreversibleRetryTransitionHistory()
    const retryFolder = new SagaHistoryTransitionFolder(
      retry.anchor,
      retry.auditProof,
      retry.plan,
      digest,
    )
    await retryFolder.append(transitionPage(retry.rows.slice(0, 2), false))
    const retryDispatch = retry.rows[2] as SagaHistoryTransitionRow
    const retryAfter = JSON.parse(retryDispatch.to_record_json) as Record<string, unknown>
    delete retryAfter.irreversibleAuthorization
    await expect(
      retryFolder.append(
        transitionPage(
          [
            {
              ...retryDispatch,
              authorization_id: null,
              authorization_protocol_version: 1,
              to_record_json: operationStepRecordJson(
                retryAfter as unknown as NonNullable<OperationRecord["steps"][string]>,
              ),
            },
          ],
          true,
        ),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/legacy/u) })

    const first = retry.rows[0] as SagaHistoryTransitionRow
    const failed = retry.rows[1] as SagaHistoryTransitionRow
    const legacyRunning = JSON.parse(first.to_record_json) as Record<string, unknown>
    delete legacyRunning.irreversibleAuthorization
    const legacyRunningJson = operationStepRecordJson(
      legacyRunning as unknown as NonNullable<OperationRecord["steps"][string]>,
    )
    const legacyFailed = JSON.parse(failed.to_record_json) as Record<string, unknown>
    delete legacyFailed.irreversibleAuthorization
    const legacyFailedJson = operationStepRecordJson(
      legacyFailed as unknown as NonNullable<OperationRecord["steps"][string]>,
    )
    const quarantined = new SagaHistoryTransitionFolder(
      retry.anchor,
      retry.auditProof,
      retry.plan,
      digest,
    )
    await quarantined.append(
      transitionPage(
        [
          {
            ...first,
            authorization_id: null,
            authorization_protocol_version: 1,
            to_record_json: legacyRunningJson,
          },
          {
            ...failed,
            from_record_json: legacyRunningJson,
            to_record_json: legacyFailedJson,
          },
        ],
        false,
      ),
    )
    await expect(
      quarantined.append(
        transitionPage([{ ...retryDispatch, from_record_json: legacyFailedJson }], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/predecessor authorization/u) })
  })

  it("rejects every malformed irreversible authorization receipt shape", async () => {
    const legacy = await irreversibleTransitionHistory(1)
    const full = await irreversibleTransitionHistory(2)
    const legacyDispatch = legacy.rows[0] as SagaHistoryTransitionRow
    const fullDispatch = full.rows[0] as SagaHistoryTransitionRow

    const malformedDispatches: readonly SagaHistoryTransitionRow[] = [
      {
        ...legacyDispatch,
        authorization_checksum: null,
        authorization_classified_at_ms: null,
        authorization_protocol_version: null,
        authorization_transition_id: null,
      },
      { ...legacyDispatch, authorization_transition_id: "other" },
      { ...legacyDispatch, authorization_protocol_version: 3 },
      { ...legacyDispatch, authorization_checksum: null },
      { ...legacyDispatch, authorization_checksum: "other" },
      { ...legacyDispatch, authorization_classified_at_ms: 99 },
    ]
    for (const candidate of malformedDispatches) {
      const folder = new SagaHistoryTransitionFolder(
        legacy.anchor,
        legacy.auditProof,
        legacy.plan,
        digest,
      )
      await expect(
        folder.append(
          transitionPage([candidate, legacy.rows[1] as SagaHistoryTransitionRow], true),
        ),
      ).rejects.toMatchObject({ message: expect.stringMatching(/lacks its exact/u) })
    }

    const legacyClaimingBody = {
      ...fullDispatch,
      authorization_id: null,
      authorization_protocol_version: 1,
    }
    const legacyBodyFolder = new SagaHistoryTransitionFolder(
      full.anchor,
      full.auditProof,
      full.plan,
      digest,
    )
    await expect(
      legacyBodyFolder.append(
        transitionPage([legacyClaimingBody, full.rows[1] as SagaHistoryTransitionRow], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/legacy/u) })

    const protocolTwoShapes: SagaHistoryTransitionRow[] = [
      { ...fullDispatch, authorization_id: null },
      { ...fullDispatch, authorization_id: "other" },
      { ...fullDispatch, holder_id: "other" },
      { ...fullDispatch, acquisition_id: "other" },
      {
        ...fullDispatch,
        to_record_json: legacyDispatch.to_record_json,
      },
    ]
    const changedFenceEvent = await withAuditEvent(
      { ...fullDispatch, fencing_token: 2 },
      full.events[1] as AuditEvent,
      { fencingToken: 2 },
    )
    protocolTwoShapes.push(changedFenceEvent)
    for (const candidate of protocolTwoShapes) {
      const folder = new SagaHistoryTransitionFolder(
        full.anchor,
        full.auditProof,
        full.plan,
        digest,
      )
      await expect(
        folder.append(transitionPage([candidate, full.rows[1] as SagaHistoryTransitionRow], true)),
      ).rejects.toMatchObject({ message: expect.stringMatching(/protocol-two/u) })
    }
  })

  it("serializes page folding and leaves no partial state after a rejected page", async () => {
    const fixture = await transitionHistory()
    let releaseDigest: () => void = () => undefined
    let markDigestStarted: () => void = () => undefined
    const gate = new Promise<void>((resolve) => {
      releaseDigest = resolve
    })
    const started = new Promise<void>((resolve) => {
      markDigestStarted = resolve
    })
    let calls = 0
    const delayedDigest: DigestFunction = async (input) => {
      calls += 1
      if (calls === 1) {
        markDigestStarted()
        await gate
      }
      return digest(input)
    }
    const folder = new SagaHistoryTransitionFolder(
      fixture.anchor,
      fixture.auditProof,
      fixture.plan,
      delayedDigest,
    )
    const firstPage = transitionPage(fixture.rows.slice(0, 2), false)
    const pending = folder.append(firstPage)
    await started
    await expect(folder.append(firstPage)).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "A saga operation-transition history page is already being folded.",
    })
    releaseDigest()
    await pending

    const malformedFinal = transitionPage(
      [
        fixture.rows[2] as SagaHistoryTransitionRow,
        {
          ...(fixture.rows[3] as SagaHistoryTransitionRow),
          audit_event_hash: "wrong",
        },
      ],
      true,
    )
    await expect(folder.append(malformedFinal)).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
    await folder.append(transitionPage(fixture.rows.slice(2), true))
    expect(folder.proof().transitionCount).toBe(4)
  })

  it("rejects premature page closure and every final proof mismatch", async () => {
    const fixture = await transitionHistory()
    const incomplete = new SagaHistoryTransitionFolder(
      fixture.anchor,
      fixture.auditProof,
      fixture.plan,
      digest,
    )
    await incomplete.append(transitionPage(fixture.rows.slice(0, 2), false))
    await expect(
      incomplete.append(transitionPage(fixture.rows.slice(2), false)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/failed to close/u) })

    const early = new SagaHistoryTransitionFolder(
      fixture.anchor,
      fixture.auditProof,
      fixture.plan,
      digest,
    )
    await expect(
      early.append(transitionPage(fixture.rows.slice(0, 2), true)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })

    const countAnchor = { ...fixture.anchor, operationTransitionCount: 5 }
    const countProof = { ...fixture.auditProof, operationTransitionCount: 5 }
    const wrongCount = new SagaHistoryTransitionFolder(
      countAnchor,
      countProof,
      fixture.plan,
      digest,
    )
    await wrongCount.append(transitionPage(fixture.rows.slice(0, 2), false))
    await expect(
      wrongCount.append(transitionPage(fixture.rows.slice(2), true)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })

    for (const [inputAnchor, auditProof] of [
      [fixture.anchor, { ...fixture.auditProof, operationTransitionFoldChecksum: "a".repeat(64) }],
      [{ ...fixture.anchor, operationStatus: "paused" }, fixture.auditProof],
    ] as const) {
      const folder = new SagaHistoryTransitionFolder(inputAnchor, auditProof, fixture.plan, digest)
      await folder.append(transitionPage(fixture.rows.slice(0, 2), false))
      await expect(
        folder.append(transitionPage(fixture.rows.slice(2), true)),
      ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })
    }
  })
})
