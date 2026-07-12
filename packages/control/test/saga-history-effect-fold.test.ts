import {
  beginSagaAction,
  createSagaRecord,
  type DigestFunction,
  markRunningSagaActionUnknown,
  markSagaActionNotDispatched,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordSagaObservation,
  requestSagaTermination,
  type SagaActionPhase,
  type SagaDescriptor,
  type SagaRecord,
  sealSagaDescriptor,
} from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { operationTransitionIdentity } from "../src/operation-store.js"
import type {
  SagaHistoryAnchor,
  SagaHistoryEffectRow,
  SagaHistoryPage,
} from "../src/saga-history.js"
import { SagaHistoryEffectFolder, type SagaHistoryEffectProof } from "../src/saga-history-fold.js"

const COORDINATOR_DOMAIN = "nozzle.saga-coordinator-id.v1"
const RECORD_DOMAIN = "nozzle.saga-record.v1"
const OPERATION_ID = "effect-fold-operation"
const SAGA_ID = "effect-fold-saga"
const SAGA_STEP_ID = "write"
const LEASE_KEY = `saga:${SAGA_ID}`

const digest: DigestFunction = async (input) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", input.slice().buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

function checksum(character: string): string {
  return character.repeat(64)
}

function frame(domain: string, values: readonly string[]): Uint8Array {
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
  return digest(frame(domain, values))
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

function recordJson(record: SagaRecord): string {
  return JSON.stringify(canonicalValue(record))
}

async function descriptor(maxAttempts = 2): Promise<SagaDescriptor> {
  return sealSagaDescriptor(
    {
      descriptorId: "effect-fold-descriptor",
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 5,
          compensationAction: {
            actionId: "compensate-write",
            artifactChecksum: checksum("b"),
            version: 1,
          },
          compensationObservation: {
            actionId: "observe-compensation",
            artifactChecksum: checksum("c"),
            version: 1,
          },
          forwardAction: {
            actionId: "write",
            artifactChecksum: checksum("d"),
            version: 1,
          },
          forwardObservation: {
            actionId: "observe-write",
            artifactChecksum: checksum("e"),
            version: 1,
          },
          inputSchemaChecksum: checksum("f"),
          irreversible: false,
          maxAttempts,
          maxRetryDelayMs: 20,
          outputSchemaChecksum: checksum("a"),
          stepId: SAGA_STEP_ID,
          timeoutMs: 1_000,
        },
      ],
      version: 1,
    },
    digest,
  )
}

async function initialSaga(maxAttempts = 2): Promise<SagaRecord> {
  return createSagaRecord({
    deadlineAtMs: 10_000,
    descriptor: await descriptor(maxAttempts),
    idempotencyKey: "effect-fold-key",
    inputChecksum: checksum("1"),
    sagaId: SAGA_ID,
    serverTimeMs: 0,
    stepInputChecksums: { [SAGA_STEP_ID]: checksum("2") },
  })
}

function operationStepId(phase: SagaActionPhase): string {
  return `saga:${phase}:${SAGA_STEP_ID}`
}

function exactTransition(kind: string, stepId: string, suffix: string): string {
  return operationTransitionIdentity(kind, [OPERATION_ID, stepId, suffix])
}

interface AddEffectInput {
  readonly createdAtMs?: number
  readonly effectKind: string
  readonly evidenceChecksum?: string
  readonly fencingToken?: number
  readonly stepId: string
  readonly transitionId: string
}

class EffectFixtureBuilder {
  readonly rows: SagaHistoryEffectRow[] = []
  saga: SagaRecord | undefined

  async add(after: SagaRecord, input: AddEffectInput): Promise<SagaHistoryEffectRow> {
    const record = recordJson(after)
    const recordChecksum = await domainChecksum(RECORD_DOMAIN, [record])
    let evidenceChecksum = input.evidenceChecksum ?? `evidence-${after.stateVersion}`
    if (input.effectKind === "action:forward:begin") {
      const attemptId = after.steps[SAGA_STEP_ID]?.forward.lastAttemptId as string
      evidenceChecksum = await domainChecksum(COORDINATOR_DOMAIN, [
        "begin-evidence",
        input.transitionId,
        SAGA_ID,
        SAGA_STEP_ID,
        "forward",
        attemptId,
      ])
    }
    if (input.effectKind === "action:compensation:begin") {
      const attemptId = after.steps[SAGA_STEP_ID]?.compensation.lastAttemptId as string
      evidenceChecksum = await domainChecksum(COORDINATOR_DOMAIN, [
        "begin-evidence",
        input.transitionId,
        SAGA_ID,
        SAGA_STEP_ID,
        "compensation",
        attemptId,
      ])
    }
    const effectChecksum = await domainChecksum(COORDINATOR_DOMAIN, [
      "saga-effect",
      input.transitionId,
      SAGA_ID,
      input.effectKind,
      after.stateVersion.toString(10),
    ])
    const fencingToken = input.fencingToken ?? 1
    const row: SagaHistoryEffectRow = {
      acquisition_id: `acquisition-${fencingToken}`,
      created_at_ms: input.createdAtMs ?? after.stateVersion + 100,
      effect_id: `saga-effect:${effectChecksum}`,
      effect_kind: input.effectKind,
      evidence_checksum: evidenceChecksum,
      fencing_token: fencingToken,
      from_state_version: this.saga?.stateVersion ?? null,
      holder_id: `holder-${fencingToken}`,
      lease_key: LEASE_KEY,
      operation_id: OPERATION_ID,
      record_checksum: recordChecksum,
      record_json: record,
      resource_id: SAGA_ID,
      resource_kind: "saga",
      step_id: input.stepId,
      to_state_version: after.stateVersion,
      transition_id: input.transitionId,
    }
    this.rows.push(row)
    this.saga = after
    return row
  }

  async create(record: SagaRecord): Promise<void> {
    await this.add(record, {
      createdAtMs: 1,
      effectKind: "create",
      evidenceChecksum: "initialization-evidence",
      stepId: "saga:init",
      transitionId: exactTransition("succeeded", "saga:init", "initialization-attempt"),
    })
  }
}

function terminalAnchor(
  builder: EffectFixtureBuilder,
  overrides: Partial<SagaHistoryAnchor> = {},
): SagaHistoryAnchor {
  const saga = builder.saga as SagaRecord
  const last = builder.rows.at(-1) as SagaHistoryEffectRow
  return {
    auditHeadEventHash: "audit-head",
    auditHeadSequence: 10,
    environmentId: "effect-fold-environment",
    operationId: OPERATION_ID,
    operationInputChecksum: "operation-input",
    operationPlanChecksum: "operation-plan",
    operationStatus: "succeeded",
    operationTransitionCount: 1,
    operationTransitionLastAuditSequence: 10,
    operationTransitionLastId: last.transition_id,
    operationUpdatedAtMs: 100,
    sagaAttemptCount: 0,
    sagaAttemptLastAcceptedAtMs: null,
    sagaAttemptLastId: null,
    sagaDescriptorChecksum: saga.descriptor.descriptorChecksum,
    sagaEffectCount: builder.rows.length,
    sagaId: SAGA_ID,
    sagaInputChecksum: saga.inputChecksum,
    sagaLastEffectId: last.effect_id,
    sagaRecordChecksum: last.record_checksum,
    sagaStateVersion: saga.stateVersion,
    sagaStatus: saga.status,
    sagaUpdatedAtMs: 100,
    schemaVersion: 1,
    ...overrides,
  }
}

function effectPage(
  rows: readonly SagaHistoryEffectRow[],
  complete: boolean,
): SagaHistoryPage<SagaHistoryEffectRow, number> {
  return {
    complete,
    nextCursor: complete ? null : (rows.at(-1) as SagaHistoryEffectRow).to_state_version,
    rows,
  }
}

async function fold(builder: EffectFixtureBuilder): Promise<SagaHistoryEffectProof> {
  const folder = new SagaHistoryEffectFolder(terminalAnchor(builder), digest)
  for (let offset = 0; offset < builder.rows.length; offset += 2) {
    const rows = builder.rows.slice(offset, offset + 2)
    await folder.append(effectPage(rows, offset + rows.length === builder.rows.length))
  }
  return folder.proof()
}

function begun(
  saga: SagaRecord,
  phase: SagaActionPhase,
  attemptId: string,
  serverTimeMs = 2,
): SagaRecord {
  const decision = beginSagaAction(saga, {
    attemptId,
    idempotencyKey: saga.steps[SAGA_STEP_ID]?.[phase].idempotencyKey as string,
    phase,
    serverTimeMs,
    stepId: SAGA_STEP_ID,
  })
  if (decision.disposition !== "execute") throw new Error("fixture action did not begin")
  return decision.saga
}

async function successfulFixture(): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const initial = await initialSaga()
  await builder.create(initial)
  const running = begun(initial, "forward", "forward-attempt")
  await builder.add(running, {
    createdAtMs: 2,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), "forward-attempt"),
  })
  const succeeded = recordSagaActionSuccess(running, {
    attemptId: "forward-attempt",
    phase: "forward",
    resultChecksum: "forward-output",
    serverTimeMs: 3,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(succeeded, {
    createdAtMs: 3,
    effectKind: "action:forward:success",
    evidenceChecksum: "forward-outcome",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("succeeded", operationStepId("forward"), "forward-attempt"),
  })
  return builder
}

async function retryFixture(): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const initial = await initialSaga(3)
  await builder.create(initial)
  const first = begun(initial, "forward", "retry-attempt-1")
  await builder.add(first, {
    createdAtMs: 2,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), "retry-attempt-1"),
  })
  const retryable = recordSagaActionFailure(first, {
    attemptId: "retry-attempt-1",
    errorChecksum: "retryable-error",
    outcome: "definitely_not_applied_retryable",
    phase: "forward",
    serverTimeMs: 10,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(retryable, {
    createdAtMs: 10,
    effectKind: "action:forward:failure:definitely_not_applied_retryable",
    evidenceChecksum: "retryable-outcome",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("failed", operationStepId("forward"), "retry-attempt-1"),
  })
  const second = begun(retryable, "forward", "retry-attempt-2", 15)
  await builder.add(second, {
    createdAtMs: 15,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), "retry-attempt-2"),
  })
  const retryableAgain = recordSagaActionFailure(second, {
    attemptId: "retry-attempt-2",
    errorChecksum: "second-retryable-error",
    outcome: "definitely_not_applied_retryable",
    phase: "forward",
    serverTimeMs: 20,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(retryableAgain, {
    createdAtMs: 20,
    effectKind: "action:forward:failure:definitely_not_applied_retryable",
    evidenceChecksum: "second-retryable-outcome",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("failed", operationStepId("forward"), "retry-attempt-2"),
  })
  const third = begun(retryableAgain, "forward", "retry-attempt-3", 30)
  await builder.add(third, {
    createdAtMs: 30,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), "retry-attempt-3"),
  })
  const failed = recordSagaActionFailure(third, {
    attemptId: "retry-attempt-3",
    errorChecksum: "terminal-error",
    outcome: "definitely_not_applied_terminal",
    phase: "forward",
    serverTimeMs: 31,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(failed, {
    createdAtMs: 31,
    effectKind: "action:forward:failure:definitely_not_applied_terminal",
    evidenceChecksum: "terminal-outcome",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("succeeded", operationStepId("forward"), "retry-attempt-3"),
  })
  return builder
}

async function observedFixture(
  outcome: "applied" | "indeterminate" | "not_applied",
): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const initial = await initialSaga(1)
  await builder.create(initial)
  const running = begun(initial, "forward", `unknown-${outcome}`)
  await builder.add(running, {
    createdAtMs: 2,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), `unknown-${outcome}`),
  })
  const unknown = recordSagaActionFailure(running, {
    attemptId: `unknown-${outcome}`,
    errorChecksum: `unknown-error-${outcome}`,
    outcome: "unknown",
    phase: "forward",
    serverTimeMs: 3,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(unknown, {
    createdAtMs: 3,
    effectKind: "action:forward:failure:unknown",
    evidenceChecksum: `unknown-outcome-${outcome}`,
    stepId: operationStepId("forward"),
    transitionId: exactTransition("failed", operationStepId("forward"), `unknown-${outcome}`),
  })
  const observed = recordSagaObservation(unknown, {
    evidenceChecksum: `observation-${outcome}`,
    outcome,
    phase: "forward",
    ...(outcome === "applied" ? { resultChecksum: "observed-output" } : {}),
    serverTimeMs: 4,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(observed, {
    createdAtMs: 4,
    effectKind: `action:forward:observation:${outcome}`,
    evidenceChecksum: `observation-${outcome}`,
    fencingToken: 2,
    stepId: operationStepId("forward"),
    transitionId: exactTransition(
      "reconciled",
      operationStepId("forward"),
      `observation-attempt-${outcome}`,
    ),
  })
  return builder
}

async function recoveryFixture(mode: "not-dispatched" | "unknown"): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const initial = await initialSaga(1)
  await builder.create(initial)
  const running = begun(initial, "forward", `recovery-${mode}`)
  await builder.add(running, {
    createdAtMs: 2,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), `recovery-${mode}`),
  })
  const evidence = `recovery-evidence-${mode}`
  const recovered =
    mode === "unknown"
      ? markRunningSagaActionUnknown(running, {
          attemptId: `recovery-${mode}`,
          errorChecksum: evidence,
          phase: "forward",
          stepId: SAGA_STEP_ID,
        })
      : markSagaActionNotDispatched(running, {
          attemptId: `recovery-${mode}`,
          errorChecksum: evidence,
          phase: "forward",
          serverTimeMs: 3,
          stepId: SAGA_STEP_ID,
        })
  await builder.add(recovered, {
    createdAtMs: 3,
    effectKind: `action:forward:recovery:${mode}`,
    evidenceChecksum: evidence,
    fencingToken: 2,
    stepId: operationStepId("forward"),
    transitionId: exactTransition("crash-recovered", operationStepId("forward"), `gap-${mode}`),
  })
  if (mode === "unknown") {
    const observed = recordSagaObservation(recovered, {
      evidenceChecksum: "recovery-observation",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "recovery-output",
      serverTimeMs: 4,
      stepId: SAGA_STEP_ID,
    })
    await builder.add(observed, {
      createdAtMs: 4,
      effectKind: "action:forward:observation:applied",
      evidenceChecksum: "recovery-observation",
      fencingToken: 2,
      stepId: operationStepId("forward"),
      transitionId: exactTransition("reconciled", operationStepId("forward"), "recovery-observe"),
    })
  }
  return builder
}

async function compensationFixture(): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const oneStepDescriptor = await descriptor()
  const firstStep = oneStepDescriptor.steps[0] as SagaDescriptor["steps"][number]
  const twoStepDescriptor = await sealSagaDescriptor(
    {
      descriptorId: "effect-fold-compensation-descriptor",
      steps: [
        firstStep,
        {
          ...firstStep,
          compensationAction: {
            actionId: "compensate-later",
            artifactChecksum: checksum("3"),
            version: 1,
          },
          compensationObservation: {
            actionId: "observe-later-compensation",
            artifactChecksum: checksum("4"),
            version: 1,
          },
          forwardAction: {
            actionId: "later",
            artifactChecksum: checksum("5"),
            version: 1,
          },
          forwardObservation: {
            actionId: "observe-later",
            artifactChecksum: checksum("6"),
            version: 1,
          },
          stepId: "later",
        },
      ],
      version: 1,
    },
    digest,
  )
  const initial = createSagaRecord({
    deadlineAtMs: 10_000,
    descriptor: twoStepDescriptor,
    idempotencyKey: "effect-fold-compensation-key",
    inputChecksum: checksum("1"),
    sagaId: SAGA_ID,
    serverTimeMs: 0,
    stepInputChecksums: { later: checksum("7"), [SAGA_STEP_ID]: checksum("2") },
  })
  await builder.create(initial)
  const running = begun(initial, "forward", "forward-attempt")
  await builder.add(running, {
    createdAtMs: 2,
    effectKind: "action:forward:begin",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("accepted", operationStepId("forward"), "forward-attempt"),
  })
  const succeeded = recordSagaActionSuccess(running, {
    attemptId: "forward-attempt",
    phase: "forward",
    resultChecksum: "forward-output",
    serverTimeMs: 3,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(succeeded, {
    createdAtMs: 3,
    effectKind: "action:forward:success",
    evidenceChecksum: "forward-outcome",
    stepId: operationStepId("forward"),
    transitionId: exactTransition("succeeded", operationStepId("forward"), "forward-attempt"),
  })
  const terminating = requestSagaTermination(succeeded, {
    cause: "cancellation",
    serverTimeMs: 4,
  })
  await builder.add(terminating, {
    createdAtMs: 4,
    effectKind: "termination:cancellation",
    evidenceChecksum: "cancellation-evidence",
    stepId: "saga:termination",
    transitionId: exactTransition("succeeded", "saga:termination", "cancellation-attempt"),
  })
  const compensating = begun(terminating, "compensation", "compensation-attempt", 5)
  await builder.add(compensating, {
    createdAtMs: 5,
    effectKind: "action:compensation:begin",
    stepId: operationStepId("compensation"),
    transitionId: exactTransition(
      "accepted",
      operationStepId("compensation"),
      "compensation-attempt",
    ),
  })
  const cancelled = recordSagaActionSuccess(compensating, {
    attemptId: "compensation-attempt",
    phase: "compensation",
    resultChecksum: "compensation-output",
    serverTimeMs: 6,
    stepId: SAGA_STEP_ID,
  })
  await builder.add(cancelled, {
    createdAtMs: 6,
    effectKind: "action:compensation:success",
    evidenceChecksum: "compensation-outcome",
    stepId: operationStepId("compensation"),
    transitionId: exactTransition(
      "succeeded",
      operationStepId("compensation"),
      "compensation-attempt",
    ),
  })
  return builder
}

async function timeoutFixture(): Promise<EffectFixtureBuilder> {
  const builder = new EffectFixtureBuilder()
  const initial = await initialSaga()
  await builder.create(initial)
  const timedOut = requestSagaTermination(initial, { cause: "timeout", serverTimeMs: 10_000 })
  await builder.add(timedOut, {
    createdAtMs: 10_001,
    effectKind: "termination:timeout",
    evidenceChecksum: "timeout-evidence",
    stepId: "saga:termination",
    transitionId: exactTransition("succeeded", "saga:termination", "timeout-attempt"),
  })
  return builder
}

async function resealRow(
  row: SagaHistoryEffectRow,
  changes: Partial<SagaHistoryEffectRow>,
): Promise<SagaHistoryEffectRow> {
  const candidate = { ...row, ...changes }
  const recordChecksum = await domainChecksum(RECORD_DOMAIN, [candidate.record_json])
  const effectChecksum = await domainChecksum(COORDINATOR_DOMAIN, [
    "saga-effect",
    candidate.transition_id,
    candidate.resource_id,
    candidate.effect_kind,
    candidate.to_state_version.toString(10),
  ])
  return {
    ...candidate,
    effect_id: `saga-effect:${effectChecksum}`,
    record_checksum: recordChecksum,
  }
}

describe("SagaHistoryEffectFolder", () => {
  it("reconstructs the dense saga projection and emits constant-size terminal proof", async () => {
    const builder = await successfulFixture()
    const folder = new SagaHistoryEffectFolder(terminalAnchor(builder), digest)
    await folder.append(effectPage(builder.rows.slice(0, 2), false))
    expect(() => folder.proof()).toThrowError(/requires more verified pages/u)
    await folder.append(effectPage(builder.rows.slice(2), true))
    expect(folder.proof()).toMatchObject({
      effectCount: 3,
      effectFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      effectLastId: builder.rows[2]?.effect_id,
      operationId: OPERATION_ID,
      saga: builder.saga,
      sagaDescriptorChecksum: builder.saga?.descriptor.descriptorChecksum,
      sagaId: SAGA_ID,
      sagaInputChecksum: builder.saga?.inputChecksum,
      sagaRecordChecksum: builder.rows[2]?.record_checksum,
      sagaStateVersion: 2,
      sagaStatus: "succeeded",
      schemaVersion: 1,
    })
    await expect(folder.append(effectPage(builder.rows.slice(2), true))).rejects.toMatchObject({
      code: "ConfigurationError",
    })
  })

  it("semantically folds every action, recovery, observation, and termination effect kind", async () => {
    const fixtures = [
      await retryFixture(),
      await observedFixture("applied"),
      await observedFixture("not_applied"),
      await observedFixture("indeterminate"),
      await recoveryFixture("not-dispatched"),
      await recoveryFixture("unknown"),
      await compensationFixture(),
      await timeoutFixture(),
    ]
    const statuses = []
    for (const fixture of fixtures) statuses.push((await fold(fixture)).sagaStatus)
    expect(statuses).toEqual([
      "failed",
      "succeeded",
      "failed",
      "intervention_required",
      "failed",
      "succeeded",
      "cancelled",
      "timed_out",
    ])
  })

  it("owns pages, serializes folding, and applies no partial state after rejection", async () => {
    const builder = await successfulFixture()
    expect(
      () => new SagaHistoryEffectFolder(terminalAnchor(builder), undefined as never),
    ).toThrowError(/digest is required/u)

    let startDigest: () => void = () => undefined
    let releaseDigest: () => void = () => undefined
    const started = new Promise<void>((resolve) => {
      startDigest = resolve
    })
    const gate = new Promise<void>((resolve) => {
      releaseDigest = resolve
    })
    let calls = 0
    const delayed: DigestFunction = async (input) => {
      calls += 1
      if (calls === 1) {
        startDigest()
        await gate
      }
      return digest(input)
    }
    const folder = new SagaHistoryEffectFolder(terminalAnchor(builder), delayed)
    const mutable = effectPage(builder.rows.slice(0, 2), false)
    const pending = folder.append(mutable)
    await started
    ;(mutable.rows[0] as { effect_kind: string }).effect_kind = "unknown"
    await expect(folder.append(effectPage(builder.rows.slice(0, 2), false))).rejects.toMatchObject({
      code: "ConfigurationError",
      message: expect.stringMatching(/already being folded/u),
    })
    releaseDigest()
    await pending

    const invalidFinal = {
      ...(builder.rows[2] as SagaHistoryEffectRow),
      record_checksum: checksum("0"),
    }
    await expect(folder.append(effectPage([invalidFinal], true))).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
    await folder.append(effectPage(builder.rows.slice(2), true))
    expect(folder.proof().sagaStatus).toBe("succeeded")
  })

  it("rejects malformed page snapshots and pagination metadata", async () => {
    const builder = await timeoutFixture()
    const anchor = terminalAnchor(builder)
    const valid = effectPage(builder.rows, true)
    const sparseRows = [builder.rows[0], builder.rows[1]] as SagaHistoryEffectRow[]
    delete sparseRows[1]
    const decoratedRows = [...builder.rows]
    Object.assign(decoratedRows, { note: true })
    const malformed: unknown[] = [
      new Proxy(valid, {
        ownKeys: () => {
          throw new Error("capture")
        },
      }),
      { ...valid, extra: true },
      { ...valid, complete: "yes" },
      { ...valid, rows: sparseRows },
      { ...valid, rows: decoratedRows },
      { complete: true, nextCursor: null, rows: [] },
      { complete: true, nextCursor: null, rows: [...builder.rows, builder.rows[0]] },
      {
        complete: true,
        nextCursor: null,
        rows: [{ ...(builder.rows[0] as SagaHistoryEffectRow), extra: true }],
      },
      { ...valid, nextCursor: 1 },
      { complete: false, nextCursor: 0, rows: [builder.rows[0]] },
      { complete: false, nextCursor: 0, rows: builder.rows },
    ]
    for (const page of malformed) {
      await expect(
        new SagaHistoryEffectFolder(anchor, digest).append(
          page as SagaHistoryPage<SagaHistoryEffectRow, number>,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects malformed, unordered, unfenced, or cryptographically contradictory rows", async () => {
    const builder = await timeoutFixture()
    const anchor = terminalAnchor(builder)
    const first = builder.rows[0] as SagaHistoryEffectRow
    const second = builder.rows[1] as SagaHistoryEffectRow
    const invalidFirst: SagaHistoryEffectRow[] = [
      { ...first, operation_id: "other" },
      { ...first, resource_kind: "resource" },
      { ...first, resource_id: "other" },
      { ...first, to_state_version: 1 },
      { ...first, from_state_version: 0 },
      { ...first, effect_id: "" },
      { ...first, fencing_token: 0 },
      { ...first, created_at_ms: -1 },
      { ...first, effect_kind: "unknown" },
      { ...first, record_json: JSON.stringify(JSON.parse(first.record_json), null, 2) },
      { ...first, record_checksum: checksum("0") },
      { ...first, effect_id: "wrong" },
      { ...first, transition_id: "wrong" },
      {
        ...first,
        transition_id: `${operationTransitionIdentity("succeeded", [OPERATION_ID, "saga:init"])}:x`,
      },
    ]
    for (const row of invalidFirst) {
      await expect(
        new SagaHistoryEffectFolder(anchor, digest).append(effectPage([row], true)),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }

    const validPrefix = new SagaHistoryEffectFolder(anchor, digest)
    await validPrefix.append(effectPage([first, second], true))
    expect(validPrefix.proof().sagaStatus).toBe("timed_out")

    const invalidSeconds = [
      { ...second, created_at_ms: 0 },
      { ...second, lease_key: "other" },
      { ...second, fencing_token: 0 },
      { ...second, holder_id: "other" },
      { ...second, acquisition_id: "other" },
    ]
    for (const row of invalidSeconds) {
      const folder = new SagaHistoryEffectFolder(anchor, digest)
      await folder.append(effectPage([first, row], true)).then(
        () => {
          throw new Error("malformed row unexpectedly folded")
        },
        (error: unknown) => {
          expect(error).toMatchObject({ code: "OperationInterventionRequiredError" })
        },
      )
    }

    let badDigestCalls = 0
    const badDigest: DigestFunction = async (input) => {
      badDigestCalls += 1
      return badDigestCalls === 1 ? digest(input) : "not-a-checksum"
    }
    await expect(
      new SagaHistoryEffectFolder(anchor, badDigest).append(effectPage(builder.rows, true)),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
  })

  it("rejects semantic relabeling, evidence forgery, invalid time, and stale recovery fences", async () => {
    const success = await successfulFixture()
    const begin = success.rows[1] as SagaHistoryEffectRow
    const badEvidence = { ...begin, evidence_checksum: "wrong" }
    const beginFolder = new SagaHistoryEffectFolder(terminalAnchor(success), digest)
    await expect(
      beginFolder.append(effectPage([success.rows[0] as SagaHistoryEffectRow, badEvidence], false)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/deterministic evidence/u) })

    const successRow = success.rows[2] as SagaHistoryEffectRow
    const relabeled = await resealRow(successRow, {
      effect_kind: "action:forward:failure:definitely_not_applied_terminal",
    })
    const relabeledFolder = new SagaHistoryEffectFolder(terminalAnchor(success), digest)
    await relabeledFolder.append(effectPage(success.rows.slice(0, 2), false))
    await expect(relabeledFolder.append(effectPage([relabeled], true))).rejects.toMatchObject({
      message: expect.stringMatching(/core state transition/u),
    })

    const timeout = await timeoutFixture()
    const termination = timeout.rows[1] as SagaHistoryEffectRow
    const wrongCause = await resealRow(termination, {
      effect_kind: "termination:cancellation",
    })
    await expect(
      new SagaHistoryEffectFolder(terminalAnchor(timeout), digest).append(
        effectPage([timeout.rows[0] as SagaHistoryEffectRow, wrongCause], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })

    const retry = await retryFixture()
    const tooEarly = { ...(retry.rows[2] as SagaHistoryEffectRow), created_at_ms: 9 }
    const earlyFolder = new SagaHistoryEffectFolder(terminalAnchor(retry), digest)
    await earlyFolder.append(effectPage(retry.rows.slice(0, 2), false))
    await expect(
      earlyFolder.append(effectPage([tooEarly, retry.rows[3] as SagaHistoryEffectRow], false)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })

    const recovery = await recoveryFixture("not-dispatched")
    const stale = {
      ...(recovery.rows[2] as SagaHistoryEffectRow),
      acquisition_id: "acquisition-1",
      fencing_token: 1,
      holder_id: "holder-1",
    }
    const recoveryFolder = new SagaHistoryEffectFolder(terminalAnchor(recovery), digest)
    await recoveryFolder.append(effectPage(recovery.rows.slice(0, 2), false))
    await expect(recoveryFolder.append(effectPage([stale], true))).rejects.toMatchObject({
      message: expect.stringMatching(/strictly newer/u),
    })
  })

  it("rejects unknown action bindings and every transition-identity contradiction", async () => {
    const builder = await successfulFixture()
    const create = builder.rows[0] as SagaHistoryEffectRow
    const begin = builder.rows[1] as SagaHistoryEffectRow
    const malformed = [
      await resealRow(begin, {
        step_id: "not-an-action-step",
        transition_id: exactTransition("accepted", "not-an-action-step", "forward-attempt"),
      }),
      await resealRow(begin, {
        step_id: "saga:forward:",
        transition_id: exactTransition("accepted", "saga:forward:", "forward-attempt"),
      }),
      await resealRow(begin, {
        step_id: "saga:forward:missing",
        transition_id: exactTransition("accepted", "saga:forward:missing", "forward-attempt"),
      }),
      await resealRow(begin, {
        transition_id: exactTransition("failed", operationStepId("forward"), "forward-attempt"),
      }),
      await resealRow(begin, {
        transition_id: `${operationTransitionIdentity("accepted", [
          OPERATION_ID,
          operationStepId("forward"),
        ])}:x`,
      }),
      await resealRow(begin, {
        transition_id: exactTransition("accepted", operationStepId("forward"), "other-attempt"),
      }),
    ]
    for (const row of malformed) {
      await expect(
        new SagaHistoryEffectFolder(terminalAnchor(builder), digest).append(
          effectPage([create, row], false),
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }

    const wrongCreate = await resealRow(create, {
      step_id: "saga:other-init",
      transition_id: exactTransition("succeeded", "saga:other-init", "initialization-attempt"),
    })
    await expect(
      new SagaHistoryEffectFolder(terminalAnchor(builder), digest).append(
        effectPage([wrongCreate], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })

    const missingCreate = await resealRow(create, {
      effect_kind: "termination:cancellation",
      step_id: "saga:termination",
      transition_id: exactTransition("succeeded", "saga:termination", "cancellation-attempt"),
    })
    await expect(
      new SagaHistoryEffectFolder(terminalAnchor(builder), digest).append(
        effectPage([missingCreate], true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })

    const timeout = await timeoutFixture()
    const termination = timeout.rows[1] as SagaHistoryEffectRow
    const malformedTerminationTransitions = [
      await resealRow(termination, {
        transition_id: exactTransition("failed", "saga:termination", "timeout-attempt"),
      }),
      await resealRow(termination, {
        transition_id: `${operationTransitionIdentity("succeeded", [
          OPERATION_ID,
          "saga:termination",
        ])}:x`,
      }),
    ]
    for (const row of malformedTerminationTransitions) {
      await expect(
        new SagaHistoryEffectFolder(terminalAnchor(timeout), digest).append(
          effectPage([timeout.rows[0] as SagaHistoryEffectRow, row], true),
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects impossible retry timing and duplicate action-begin effects", async () => {
    const retry = await retryFixture()
    const retryable = retry.rows[2] as SagaHistoryEffectRow
    const malformedRecord = JSON.parse(retryable.record_json) as Record<string, unknown>
    const malformedSteps = malformedRecord.steps as Record<string, Record<string, unknown>>
    const malformedStep = malformedSteps[SAGA_STEP_ID] as Record<string, unknown>
    const malformedAction = malformedStep.forward as Record<string, unknown>
    malformedAction.nextAttemptAtMs = 1
    const impossibleRetry = await resealRow(retryable, {
      record_json: JSON.stringify(canonicalValue(malformedRecord)),
    })
    const retryFolder = new SagaHistoryEffectFolder(terminalAnchor(retry), digest)
    await retryFolder.append(effectPage(retry.rows.slice(0, 2), false))
    await expect(
      retryFolder.append(
        effectPage([impossibleRetry, retry.rows[3] as SagaHistoryEffectRow], false),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })

    const duplicate = new EffectFixtureBuilder()
    const initial = await initialSaga(1)
    await duplicate.create(initial)
    const running = begun(initial, "forward", "duplicate-attempt")
    const beginTransition = exactTransition(
      "accepted",
      operationStepId("forward"),
      "duplicate-attempt",
    )
    await duplicate.add(running, {
      createdAtMs: 2,
      effectKind: "action:forward:begin",
      stepId: operationStepId("forward"),
      transitionId: beginTransition,
    })
    const duplicateRunning: SagaRecord = Object.freeze({ ...running, stateVersion: 2 })
    await duplicate.add(duplicateRunning, {
      createdAtMs: 3,
      effectKind: "action:forward:begin",
      stepId: operationStepId("forward"),
      transitionId: beginTransition,
    })
    const failed = recordSagaActionFailure(duplicateRunning, {
      attemptId: "duplicate-attempt",
      errorChecksum: "duplicate-terminal-error",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 4,
      stepId: SAGA_STEP_ID,
    })
    await duplicate.add(failed, {
      createdAtMs: 4,
      effectKind: "action:forward:failure:definitely_not_applied_terminal",
      evidenceChecksum: "duplicate-terminal-outcome",
      stepId: operationStepId("forward"),
      transitionId: exactTransition("succeeded", operationStepId("forward"), "duplicate-attempt"),
    })
    const duplicateFolder = new SagaHistoryEffectFolder(terminalAnchor(duplicate), digest)
    await duplicateFolder.append(effectPage(duplicate.rows.slice(0, 2), false))
    await expect(
      duplicateFolder.append(effectPage(duplicate.rows.slice(2), true)),
    ).rejects.toMatchObject({ message: expect.stringMatching(/core state transition/u) })
  })

  it("rejects premature closure and every terminal anchor contradiction", async () => {
    const builder = await successfulFixture()
    await expect(
      new SagaHistoryEffectFolder(terminalAnchor(builder), digest).append(
        effectPage(builder.rows.slice(0, 2), true),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })

    const incomplete = new SagaHistoryEffectFolder(terminalAnchor(builder), digest)
    await incomplete.append(effectPage(builder.rows.slice(0, 2), false))
    await expect(
      incomplete.append({
        complete: false,
        nextCursor: builder.rows[2]?.to_state_version as number,
        rows: [builder.rows[2], builder.rows[2]] as SagaHistoryEffectRow[],
      }),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

    const atHead = await timeoutFixture()
    await expect(
      new SagaHistoryEffectFolder(terminalAnchor(atHead), digest).append(
        effectPage(atHead.rows, false),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/failed to close/u) })

    for (const overrides of [
      { sagaLastEffectId: "other-effect" },
      { sagaRecordChecksum: checksum("9") },
      { sagaStatus: "failed" as const },
    ]) {
      const folder = new SagaHistoryEffectFolder(terminalAnchor(builder, overrides), digest)
      await folder.append(effectPage(builder.rows.slice(0, 2), false))
      await expect(folder.append(effectPage(builder.rows.slice(2), true))).rejects.toMatchObject({
        message: expect.stringMatching(/does not reconcile/u),
      })
    }
  })
})
