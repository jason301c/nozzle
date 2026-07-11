import fc from "fast-check"
import { describe, expect, it } from "vitest"
import type { DigestFunction } from "../src/operation.js"
import {
  type SagaActionReference,
  type SagaStepDescriptorInput,
  sealSagaDescriptor,
} from "../src/saga.js"
import {
  beginSagaAction,
  createSagaRecord,
  loadSagaRecord,
  mapSagaSettlementOutcome,
  markRunningSagaActionUnknown,
  markSagaActionNotDispatched,
  nextSagaCommand,
  recordSagaActionFailure,
  recordSagaActionSuccess,
  recordSagaObservation,
  requestSagaTermination,
  type SagaActionPhase,
  type SagaRecord,
  sagaCommitment,
} from "../src/saga-state.js"

const digest: DigestFunction = async (input) => {
  const owned = new Uint8Array(input.byteLength)
  owned.set(input)
  const output = new Uint8Array(await crypto.subtle.digest("SHA-256", owned.buffer))
  return [...output].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function action(actionId: string, character: string): SagaActionReference {
  return { actionId, artifactChecksum: character.repeat(64), version: 1 }
}

function descriptorStep(
  stepId: string,
  overrides: Partial<SagaStepDescriptorInput> = {},
): SagaStepDescriptorInput {
  return {
    authorizationPolicyChecksum: null,
    baseRetryDelayMs: 10,
    compensationAction: action(`${stepId}.compensate`, "c"),
    compensationObservation: action(`${stepId}.observe-compensation`, "d"),
    forwardAction: action(`${stepId}.forward`, "a"),
    forwardObservation: action(`${stepId}.observe-forward`, "b"),
    inputSchemaChecksum: "11".repeat(32),
    irreversible: false,
    maxAttempts: 3,
    maxRetryDelayMs: 100,
    outputSchemaChecksum: "22".repeat(32),
    stepId,
    timeoutMs: 1_000,
    ...overrides,
  }
}

function irreversibleStep(stepId: string): SagaStepDescriptorInput {
  return descriptorStep(stepId, {
    authorizationPolicyChecksum: "ee".repeat(32),
    compensationAction: null,
    compensationObservation: null,
    irreversible: true,
  })
}

async function saga(
  steps: readonly SagaStepDescriptorInput[] = [descriptorStep("a"), descriptorStep("b")],
): Promise<SagaRecord> {
  const descriptor = await sealSagaDescriptor(
    { descriptorId: "transfer", steps, version: 1 },
    digest,
  )
  return createSagaRecord({
    deadlineAtMs: 10_000,
    descriptor,
    idempotencyKey: "transfer-request-1",
    inputChecksum: "transfer-input",
    sagaId: "saga-1",
    serverTimeMs: 1_000,
    stepInputChecksums: Object.fromEntries(
      steps.map((step) => [step.stepId, `input-${step.stepId}`]),
    ),
  })
}

function execute(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  attemptId: string,
  serverTimeMs: number,
): SagaRecord {
  const command = nextSagaCommand(record, serverTimeMs)
  expect(command).toMatchObject({ kind: "execute", phase, stepId })
  if (command.kind !== "execute") throw new Error("expected execute command")
  const decision = beginSagaAction(record, {
    attemptId,
    idempotencyKey: command.idempotencyKey,
    phase,
    serverTimeMs,
    stepId,
  })
  expect(decision.disposition).toBe("execute")
  return decision.saga
}

function succeed(
  record: SagaRecord,
  stepId: string,
  phase: SagaActionPhase,
  attemptId: string,
  serverTimeMs: number,
): SagaRecord {
  return recordSagaActionSuccess(record, {
    attemptId,
    phase,
    resultChecksum: `result-${stepId}-${phase}`,
    serverTimeMs,
    stepId,
  })
}

function expectCode(callback: () => unknown, code: string): void {
  expect(callback).toThrowError(expect.objectContaining({ code }))
}

async function expectAsyncCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code })
}

type MutableRecord = Record<string, unknown>
type SagaCreationInput = Parameters<typeof createSagaRecord>[0]

function mutableRecord(value: unknown): MutableRecord {
  return value as MutableRecord
}

function mutableStep(candidate: MutableRecord, stepId: string): MutableRecord {
  return mutableRecord(mutableRecord(candidate.steps)[stepId])
}

function mutableAction(
  candidate: MutableRecord,
  stepId: string,
  phase: SagaActionPhase,
): MutableRecord {
  return mutableRecord(mutableStep(candidate, stepId)[phase])
}

async function terminalSagaRecords(): Promise<readonly SagaRecord[]> {
  let succeeded = await saga([descriptorStep("a")])
  succeeded = execute(succeeded, "a", "forward", "a-1", 1_000)
  succeeded = succeed(succeeded, "a", "forward", "a-1", 1_001)

  const cancelled = requestSagaTermination(await saga([descriptorStep("a")]), {
    cause: "cancellation",
    serverTimeMs: 1_001,
  })
  const timedOut = requestSagaTermination(await saga([descriptorStep("a")]), {
    cause: "timeout",
    serverTimeMs: 10_000,
  })

  let failed = await saga([descriptorStep("a", { maxAttempts: 1 })])
  failed = execute(failed, "a", "forward", "failed-a-1", 1_000)
  failed = recordSagaActionFailure(failed, {
    attemptId: "failed-a-1",
    errorChecksum: "terminal-failure",
    outcome: "definitely_not_applied_terminal",
    phase: "forward",
    serverTimeMs: 1_001,
    stepId: "a",
  })

  let interventionRequired = await saga([irreversibleStep("commit")])
  interventionRequired = execute(interventionRequired, "commit", "forward", "commit-1", 1_000)
  interventionRequired = requestSagaTermination(interventionRequired, {
    cause: "cancellation",
    serverTimeMs: 1_001,
  })
  interventionRequired = succeed(interventionRequired, "commit", "forward", "commit-1", 1_002)

  return Object.freeze([succeeded, failed, cancelled, timedOut, interventionRequired])
}

describe("terminal saga settlement outcomes", () => {
  it("maps and round trips every terminal domain outcome without changing its meaning", async () => {
    const records = await terminalSagaRecords()
    const expectedOutcomes = [
      "succeeded",
      "failed",
      "failed",
      "failed",
      "intervention_required",
    ] as const

    for (const [index, record] of records.entries()) {
      const before = structuredClone(record)
      expect(mapSagaSettlementOutcome(record)).toBe(expectedOutcomes[index])
      expect(record).toEqual(before)
      expect(nextSagaCommand(record, 0)).toEqual({
        kind: "terminal",
        status: record.status,
      })

      const serialized = structuredClone(record)
      expect(mapSagaSettlementOutcome(serialized)).toBe(expectedOutcomes[index])
      const loaded = await loadSagaRecord(serialized, digest)
      expect(loaded).toEqual(record)
      expect(Object.isFrozen(loaded)).toBe(true)
      expect(Object.isFrozen(loaded.steps)).toBe(true)
      expect(mapSagaSettlementOutcome(loaded)).toBe(expectedOutcomes[index])
      expect(mapSagaSettlementOutcome(serialized)).toBe(expectedOutcomes[index])
    }
  })

  it("rejects every nonterminal, contradictory, and malformed projection", async () => {
    const planned = await saga([descriptorStep("a"), descriptorStep("b")])
    const running = execute(planned, "a", "forward", "a-1", 1_000)
    let compensating = succeed(running, "a", "forward", "a-1", 1_001)
    compensating = requestSagaTermination(compensating, {
      cause: "cancellation",
      serverTimeMs: 1_002,
    })
    expect(planned.status).toBe("planned")
    expect(running.status).toBe("running")
    expect(compensating.status).toBe("compensating")

    for (const record of [planned, running, compensating]) {
      expectCode(() => mapSagaSettlementOutcome(record), "OperationResumeRequiredError")
    }

    const succeeded = (await terminalSagaRecords())[0] as SagaRecord
    const structuralClone = Object.freeze({ ...succeeded })
    expect(mapSagaSettlementOutcome(structuralClone)).toBe("succeeded")
    expectCode(
      () => mapSagaSettlementOutcome({ ...succeeded, status: "failed" }),
      "OperationInterventionRequiredError",
    )
    expect(() =>
      mapSagaSettlementOutcome({ ...succeeded, steps: null } as unknown as SagaRecord),
    ).toThrowError(
      expect.objectContaining({
        code: "OperationInterventionRequiredError",
        message: "Saga settlement projection is malformed.",
      }),
    )
    for (const record of [
      {
        ...succeeded,
        status: "failed",
        terminationCause: "unsupported",
        terminationRequestedAtMs: 1,
      },
      { ...succeeded, terminationCause: "cancellation", terminationRequestedAtMs: null },
      { ...succeeded, terminationCause: null, terminationRequestedAtMs: 1 },
      { ...succeeded, terminationCause: "timeout", terminationRequestedAtMs: -1 },
      { ...succeeded, terminationCause: "timeout", terminationRequestedAtMs: "invalid" },
    ]) {
      expectCode(
        () => mapSagaSettlementOutcome(record as unknown as SagaRecord),
        "OperationInterventionRequiredError",
      )
    }

    let causeReads = 0
    const switchingCause = { ...succeeded }
    Object.defineProperty(switchingCause, "terminationCause", {
      enumerable: true,
      get() {
        causeReads += 1
        return causeReads === 1 ? null : "unsupported"
      },
    })
    expect(mapSagaSettlementOutcome(switchingCause)).toBe("succeeded")
    expect(causeReads).toBe(1)
    expectCode(
      () => mapSagaSettlementOutcome(new Proxy(succeeded, {})),
      "OperationInterventionRequiredError",
    )
  })

  it("keeps outcome mapping independent from persistence and history authority", async () => {
    const planned = await saga([descriptorStep("a")])
    const serialized = structuredClone(planned)
    const structuralTerminal = requestSagaTermination(serialized, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    expect(structuralTerminal.status).toBe("cancelled")
    expect(mapSagaSettlementOutcome(structuralTerminal)).toBe("failed")

    const loaded = await loadSagaRecord(serialized, digest)
    const loadedTerminal = requestSagaTermination(loaded, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    expect(mapSagaSettlementOutcome(loadedTerminal)).toBe("failed")
    expectCode(() => mapSagaSettlementOutcome(serialized), "OperationResumeRequiredError")
  })

  it("is independent of arbitrary persisted terminal versions", async () => {
    const records = await terminalSagaRecords()
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...records),
        fc.integer({ min: 1, max: Number.MAX_SAFE_INTEGER - 1 }),
        async (record, stateVersion) => {
          const versioned = Object.freeze({ ...record, stateVersion })
          expect(mapSagaSettlementOutcome(versioned)).toBe(mapSagaSettlementOutcome(record))
          expect(versioned.stateVersion).toBe(stateVersion)
          expect(versioned.steps).toBe(record.steps)
          expect(versioned.descriptor).toBe(record.descriptor)
          expect(sagaCommitment(versioned)).toBe(sagaCommitment(record))
          expect(Object.isFrozen(versioned)).toBe(true)

          const loaded = await loadSagaRecord(structuredClone(versioned), digest)
          expect(loaded).toEqual(versioned)
          expect(mapSagaSettlementOutcome(loaded)).toBe(mapSagaSettlementOutcome(record))
          expect(mapSagaSettlementOutcome(versioned)).toBe(mapSagaSettlementOutcome(record))
        },
      ),
      { numRuns: 100 },
    )
  })
})

describe("serial saga state machine", () => {
  it("executes forwards in sealed order and reports commitment honestly", async () => {
    let record = await saga()
    expect(record).toMatchObject({ stateVersion: 0, status: "planned", terminationCause: null })
    expect(sagaCommitment(record)).toBe("none")
    expect(nextSagaCommand(record, 1_000)).toMatchObject({
      action: { actionId: "a.forward" },
      attemptNumber: 1,
      kind: "execute",
      phase: "forward",
      stepId: "a",
      timeoutMs: 1_000,
    })

    record = execute(record, "a", "forward", "a-1", 1_000)
    expect(record.status).toBe("running")
    expect(sagaCommitment(record)).toBe("possible")
    expect(nextSagaCommand(record, 1_001)).toEqual({
      kind: "wait",
      reason: "attempt_in_progress",
    })
    record = succeed(record, "a", "forward", "a-1", 1_002)
    expect(sagaCommitment(record)).toBe("confirmed_partial")
    expect(nextSagaCommand(record, 1_003)).toMatchObject({ stepId: "b" })
    record = execute(record, "b", "forward", "b-1", 1_003)
    record = succeed(record, "b", "forward", "b-1", 1_004)

    expect(record.status).toBe("succeeded")
    expect(sagaCommitment(record)).toBe("complete")
    expect(nextSagaCommand(record, 1_005)).toEqual({ kind: "terminal", status: "succeeded" })
    expect(Object.isFrozen(record)).toBe(true)
    expect(Object.isFrozen(record.steps.a)).toBe(true)
  })

  it("backs off retryable forwards, then fails and compensates in reverse", async () => {
    let record = await saga([
      descriptorStep("a"),
      descriptorStep("b"),
      descriptorStep("c", { maxAttempts: 2 }),
    ])
    for (const stepId of ["a", "b"]) {
      record = execute(record, stepId, "forward", `${stepId}-1`, 1_000)
      record = succeed(record, stepId, "forward", `${stepId}-1`, 1_001)
    }
    record = execute(record, "c", "forward", "c-1", 1_002)
    record = recordSagaActionFailure(record, {
      attemptId: "c-1",
      errorChecksum: "retry-c",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "c",
    })
    expect(nextSagaCommand(record, 1_012)).toEqual({
      kind: "wait",
      reason: "retry_backoff",
      untilMs: 1_013,
    })
    record = execute(record, "c", "forward", "c-2", 1_013)
    record = recordSagaActionFailure(record, {
      attemptId: "c-2",
      errorChecksum: "terminal-c",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_014,
      stepId: "c",
    })
    expect(record).toMatchObject({ status: "compensating", terminationCause: "failure" })
    expect(nextSagaCommand(record, 1_015)).toMatchObject({ phase: "compensation", stepId: "b" })
    record = execute(record, "b", "compensation", "b-c-1", 1_015)
    record = succeed(record, "b", "compensation", "b-c-1", 1_016)
    expect(nextSagaCommand(record, 1_017)).toMatchObject({ phase: "compensation", stepId: "a" })
    record = execute(record, "a", "compensation", "a-c-1", 1_017)
    record = succeed(record, "a", "compensation", "a-c-1", 1_018)
    expect(record.status).toBe("failed")
    expect(sagaCommitment(record)).toBe("none")
  })

  it("reconciles an unknown forward before cancellation compensation", async () => {
    let record = await saga()
    record = execute(record, "a", "forward", "a-1", 1_000)
    record = succeed(record, "a", "forward", "a-1", 1_001)
    record = execute(record, "b", "forward", "b-1", 1_002)
    record = recordSagaActionFailure(record, {
      attemptId: "b-1",
      errorChecksum: "unknown-b",
      outcome: "unknown",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "b",
    })
    record = requestSagaTermination(record, { cause: "cancellation", serverTimeMs: 1_004 })
    expect(record.status).toBe("compensating")
    expect(sagaCommitment(record)).toBe("possible")
    expect(nextSagaCommand(record, 1_005)).toMatchObject({
      action: { actionId: "b.observe-forward" },
      kind: "observe",
      phase: "forward",
      stepId: "b",
    })
    record = recordSagaObservation(record, {
      evidenceChecksum: "not-applied-b",
      outcome: "not_applied",
      phase: "forward",
      serverTimeMs: 1_005,
      stepId: "b",
    })
    expect(nextSagaCommand(record, 1_006)).toMatchObject({ phase: "compensation", stepId: "a" })
    record = execute(record, "a", "compensation", "a-c-1", 1_006)
    record = succeed(record, "a", "compensation", "a-c-1", 1_007)
    expect(record.status).toBe("cancelled")
    expect(sagaCommitment(record)).toBe("none")
  })

  it("compensates an unknown forward observed as applied in reverse order", async () => {
    let record = await saga()
    record = execute(record, "a", "forward", "a-1", 1_000)
    record = succeed(record, "a", "forward", "a-1", 1_001)
    record = execute(record, "b", "forward", "b-1", 1_002)
    record = markRunningSagaActionUnknown(record, {
      attemptId: "b-1",
      errorChecksum: "crash-b",
      phase: "forward",
      stepId: "b",
    })
    record = requestSagaTermination(record, { cause: "timeout", serverTimeMs: 1_003 })
    record = recordSagaObservation(record, {
      evidenceChecksum: "applied-b",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "result-b",
      serverTimeMs: 1_004,
      stepId: "b",
    })
    expect(nextSagaCommand(record, 1_005)).toMatchObject({ stepId: "b" })
    record = execute(record, "b", "compensation", "b-c-1", 1_005)
    record = succeed(record, "b", "compensation", "b-c-1", 1_006)
    record = execute(record, "a", "compensation", "a-c-1", 1_007)
    record = succeed(record, "a", "compensation", "a-c-1", 1_008)
    expect(record.status).toBe("timed_out")
  })

  it("reconciles unknown compensation and surfaces indeterminate or exhausted recovery", async () => {
    let record = await saga([descriptorStep("a", { maxAttempts: 1 }), descriptorStep("b")])
    record = execute(record, "a", "forward", "a-1", 1_000)
    record = succeed(record, "a", "forward", "a-1", 1_001)
    record = execute(record, "b", "forward", "b-1", 1_002)
    record = recordSagaActionFailure(record, {
      attemptId: "b-1",
      errorChecksum: "failure-b",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "b",
    })
    record = execute(record, "a", "compensation", "a-c-1", 1_004)
    record = recordSagaActionFailure(record, {
      attemptId: "a-c-1",
      errorChecksum: "unknown-compensation",
      outcome: "unknown",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    expect(nextSagaCommand(record, 1_006)).toMatchObject({
      action: { actionId: "a.observe-compensation" },
      phase: "compensation",
    })
    const indeterminate = recordSagaObservation(record, {
      evidenceChecksum: "indeterminate",
      outcome: "indeterminate",
      phase: "compensation",
      serverTimeMs: 1_006,
      stepId: "a",
    })
    expect(indeterminate.status).toBe("intervention_required")
    expect(sagaCommitment(indeterminate)).toBe("confirmed_partial")

    const notApplied = recordSagaObservation(record, {
      evidenceChecksum: "not-applied",
      outcome: "not_applied",
      phase: "compensation",
      serverTimeMs: 1_006,
      stepId: "a",
    })
    expect(notApplied.status).toBe("intervention_required")
    const applied = recordSagaObservation(record, {
      evidenceChecksum: "applied",
      outcome: "applied",
      phase: "compensation",
      resultChecksum: "compensated-a",
      serverTimeMs: 1_006,
      stepId: "a",
    })
    expect(applied.status).toBe("failed")
    expect(sagaCommitment(applied)).toBe("none")
  })

  it("handles cancellation, timeout, and first durable termination cause idempotently", async () => {
    let record = await saga()
    expect(nextSagaCommand(record, 10_000)).toEqual({
      cause: "timeout",
      kind: "request_termination",
    })
    record = requestSagaTermination(record, { cause: "cancellation", serverTimeMs: 2_000 })
    expect(record.status).toBe("cancelled")
    const version = record.stateVersion
    expect(requestSagaTermination(record, { cause: "timeout", serverTimeMs: 2_001 })).toBe(record)
    expect(record.stateVersion).toBe(version)
    expect(nextSagaCommand(record, 2_002)).toEqual({ kind: "terminal", status: "cancelled" })

    const timedOut = requestSagaTermination(await saga(), {
      cause: "timeout",
      serverTimeMs: 10_000,
    })
    expect(timedOut.status).toBe("timed_out")
  })

  it("permits authorized last irreversible success but surfaces a cancellation race", async () => {
    let completed = await saga([descriptorStep("a"), irreversibleStep("commit")])
    completed = execute(completed, "a", "forward", "a-1", 1_000)
    completed = succeed(completed, "a", "forward", "a-1", 1_001)
    completed = execute(completed, "commit", "forward", "commit-1", 1_002)
    completed = succeed(completed, "commit", "forward", "commit-1", 1_003)
    expect(completed.status).toBe("succeeded")

    let raced = await saga([irreversibleStep("commit")])
    raced = execute(raced, "commit", "forward", "commit-1", 1_000)
    raced = requestSagaTermination(raced, { cause: "cancellation", serverTimeMs: 1_001 })
    raced = succeed(raced, "commit", "forward", "commit-1", 1_002)
    expect(raced.status).toBe("intervention_required")
    expect(sagaCommitment(raced)).toBe("confirmed_partial")
  })

  it("deduplicates active, unknown, successful, and failed attempt deliveries", async () => {
    const initial = await saga([descriptorStep("a")])
    const command = nextSagaCommand(initial, 1_000)
    if (command.kind !== "execute") throw new Error("expected execute")
    let record = execute(initial, "a", "forward", "a-1", 1_000)
    expect(
      beginSagaAction(record, {
        attemptId: "a-1",
        idempotencyKey: command.idempotencyKey,
        phase: "forward",
        serverTimeMs: 1_001,
        stepId: "a",
      }).disposition,
    ).toBe("in_progress")
    const unknown = markRunningSagaActionUnknown(record, {
      attemptId: "a-1",
      errorChecksum: "unknown",
      phase: "forward",
      stepId: "a",
    })
    expect(
      beginSagaAction(unknown, {
        attemptId: "a-1",
        idempotencyKey: command.idempotencyKey,
        phase: "forward",
        serverTimeMs: 1_001,
        stepId: "a",
      }).disposition,
    ).toBe("observe")
    const succeeded = recordSagaObservation(unknown, {
      evidenceChecksum: "applied",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "result-a",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    expect(
      beginSagaAction(succeeded, {
        attemptId: "a-1",
        idempotencyKey: command.idempotencyKey,
        phase: "forward",
        serverTimeMs: 1_003,
        stepId: "a",
      }),
    ).toMatchObject({ disposition: "replay_success", resultChecksum: "result-a" })

    record = execute(initial, "a", "forward", "a-1", 1_000)
    const failed = recordSagaActionFailure(record, {
      attemptId: "a-1",
      errorChecksum: "retry",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    expect(
      beginSagaAction(failed, {
        attemptId: "a-1",
        idempotencyKey: command.idempotencyKey,
        phase: "forward",
        serverTimeMs: 1_002,
        stepId: "a",
      }),
    ).toMatchObject({ disposition: "replay_failure", errorChecksum: "retry" })
  })

  it("round trips an epoch retry with a sealed zero delay", async () => {
    const step = descriptorStep("a", {
      baseRetryDelayMs: 0,
      maxAttempts: 2,
      maxRetryDelayMs: 0,
    })
    const descriptor = await sealSagaDescriptor(
      { descriptorId: "epoch-retry", steps: [step], version: 1 },
      digest,
    )
    let record = createSagaRecord({
      deadlineAtMs: 100,
      descriptor,
      idempotencyKey: "epoch-retry-request",
      inputChecksum: "epoch-retry-input",
      sagaId: "epoch-retry-saga",
      serverTimeMs: 0,
      stepInputChecksums: { a: "epoch-retry-step-input" },
    })
    record = execute(record, "a", "forward", "epoch-attempt", 0)
    record = recordSagaActionFailure(record, {
      attemptId: "epoch-attempt",
      errorChecksum: "not-applied",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 0,
      stepId: "a",
    })

    expect(record.steps.a?.forward.nextAttemptAtMs).toBe(0)
    expect(nextSagaCommand(record, 0)).toMatchObject({ kind: "execute", stepId: "a" })

    const loaded = await loadSagaRecord(structuredClone(record), digest)
    expect(loaded).toEqual(record)
    expect(loaded.steps.a?.forward.nextAttemptAtMs).toBe(0)
    expect(nextSagaCommand(loaded, 0)).toMatchObject({ kind: "execute", stepId: "a" })
  })

  it("rejects version overflow while preserving every safe boundary increment", async () => {
    const initial = await saga([descriptorStep("a")])
    const command = nextSagaCommand(initial, 1_000)
    if (command.kind !== "execute") throw new Error("expected execute")

    await fc.assert(
      fc.asyncProperty(
        fc.integer({
          min: Number.MAX_SAFE_INTEGER - 100,
          max: Number.MAX_SAFE_INTEGER - 1,
        }),
        async (stateVersion) => {
          const projection = mutableRecord(structuredClone(initial))
          projection.stateVersion = stateVersion
          projection.status = "running"
          const loaded = await loadSagaRecord(projection, digest)
          const begin = () =>
            beginSagaAction(loaded, {
              attemptId: "boundary-attempt",
              idempotencyKey: command.idempotencyKey,
              phase: "forward",
              serverTimeMs: 1_000,
              stepId: "a",
            })

          const decision = begin()
          expect(decision.disposition).toBe("execute")
          expect(decision.saga.stateVersion).toBe(stateVersion + 1)
          expect(Number.isSafeInteger(decision.saga.stateVersion)).toBe(true)
        },
      ),
      { numRuns: 100 },
    )

    const exhaustedProjection = mutableRecord(structuredClone(initial))
    exhaustedProjection.stateVersion = Number.MAX_SAFE_INTEGER
    exhaustedProjection.status = "running"
    const exhausted = await loadSagaRecord(exhaustedProjection, digest)
    expectCode(
      () =>
        beginSagaAction(exhausted, {
          attemptId: "overflow-attempt",
          idempotencyKey: command.idempotencyKey,
          phase: "forward",
          serverTimeMs: 1_000,
          stepId: "a",
        }),
      "OperationInterventionRequiredError",
    )
  })

  it("distinguishes accepted unknown actions from proven non-dispatch", async () => {
    let record = await saga([descriptorStep("a", { maxAttempts: 2 })])
    record = execute(record, "a", "forward", "a-1", 1_000)
    const unknown = markRunningSagaActionUnknown(record, {
      attemptId: "a-1",
      errorChecksum: "accepted-before-crash",
      phase: "forward",
      stepId: "a",
    })
    expect(nextSagaCommand(unknown, 1_001)).toMatchObject({ kind: "observe" })

    const notDispatched = markSagaActionNotDispatched(record, {
      attemptId: "a-1",
      errorChecksum: "receipt-absent",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    expect(nextSagaCommand(notDispatched, 1_010)).toMatchObject({
      kind: "wait",
      untilMs: 1_011,
    })
    expect(nextSagaCommand(notDispatched, 1_011)).toMatchObject({ kind: "execute" })

    let terminal = await saga([descriptorStep("a", { maxAttempts: 1 })])
    terminal = execute(terminal, "a", "forward", "a-1", 1_000)
    terminal = markSagaActionNotDispatched(terminal, {
      attemptId: "a-1",
      errorChecksum: "receipt-absent",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    expect(terminal.status).toBe("failed")
  })

  it("captures changing creation accessors once and preserves the trusted descriptor", async () => {
    const steps = [descriptorStep("a"), descriptorStep("b")]
    const descriptor = await sealSagaDescriptor(
      { descriptorId: "captured-creation", steps, version: 1 },
      digest,
    )
    const stable: SagaCreationInput = {
      deadlineAtMs: 10_000,
      descriptor,
      idempotencyKey: "captured-request",
      inputChecksum: "captured-input",
      sagaId: "captured-saga",
      serverTimeMs: 1_000,
      stepInputChecksums: { a: "captured-a", b: "captured-b" },
    }
    const changed: { readonly [Key in keyof SagaCreationInput]: unknown } = {
      deadlineAtMs: -1,
      descriptor: structuredClone(descriptor),
      idempotencyKey: "",
      inputChecksum: "",
      sagaId: "",
      serverTimeMs: -1,
      stepInputChecksums: [],
    }
    const reads: Record<string, number> = {}
    const changingInput: MutableRecord = {}
    for (const key of Object.keys(stable) as readonly (keyof SagaCreationInput)[]) {
      Object.defineProperty(changingInput, key, {
        enumerable: true,
        get() {
          const count = (reads[key] ?? 0) + 1
          reads[key] = count
          return count === 1 ? stable[key] : changed[key]
        },
      })
    }

    const created = createSagaRecord(changingInput as unknown as SagaCreationInput)
    expect(reads).toEqual({
      deadlineAtMs: 1,
      descriptor: 1,
      idempotencyKey: 1,
      inputChecksum: 1,
      sagaId: 1,
      serverTimeMs: 1,
      stepInputChecksums: 1,
    })
    expect(created).toMatchObject({
      deadlineAtMs: 10_000,
      idempotencyKey: "captured-request",
      inputChecksum: "captured-input",
      sagaId: "captured-saga",
      steps: {
        a: { inputChecksum: "captured-a" },
        b: { inputChecksum: "captured-b" },
      },
    })
    expect(created.descriptor).toBe(descriptor)

    const loaded = await loadSagaRecord(structuredClone(created), digest)
    expect(loaded).toEqual(created)
    expect(loaded.descriptor).not.toBe(created.descriptor)
  })

  it("owns a mutating step-checksum map before validating creation", async () => {
    const steps = [descriptorStep("a"), descriptorStep("b")]
    const descriptor = await sealSagaDescriptor(
      { descriptorId: "owned-creation", steps, version: 1 },
      digest,
    )
    const values: Record<string, string> = { a: "owned-a", b: "owned-b" }
    const reads: Record<string, number> = { a: 0, b: 0 }
    const changingChecksums: MutableRecord = {}
    for (const stepId of ["a", "b"] as const) {
      Object.defineProperty(changingChecksums, stepId, {
        enumerable: true,
        get() {
          reads[stepId] = (reads[stepId] ?? 0) + 1
          const current = values[stepId] as string
          values[stepId] = `mutated-${stepId}`
          return current
        },
      })
    }

    const created = createSagaRecord({
      deadlineAtMs: 10_000,
      descriptor,
      idempotencyKey: "owned-request",
      inputChecksum: "owned-input",
      sagaId: "owned-saga",
      serverTimeMs: 1_000,
      stepInputChecksums: changingChecksums as Readonly<Record<string, string>>,
    })
    expect(reads).toEqual({ a: 1, b: 1 })
    expect(values).toEqual({ a: "mutated-a", b: "mutated-b" })
    expect(created.steps.a?.inputChecksum).toBe("owned-a")
    expect(created.steps.b?.inputChecksum).toBe("owned-b")

    values.a = "changed-after-creation"
    values.b = "changed-after-creation"
    expect(created.steps.a?.inputChecksum).toBe("owned-a")
    expect(created.steps.b?.inputChecksum).toBe("owned-b")
    expect(await loadSagaRecord(structuredClone(created), digest)).toEqual(created)
  })

  it("normalizes creation getter, proxy, and clone failures", async () => {
    const descriptor = await sealSagaDescriptor(
      { descriptorId: "capture-failures", steps: [descriptorStep("a")], version: 1 },
      digest,
    )
    const valid: SagaCreationInput = {
      deadlineAtMs: 10_000,
      descriptor,
      idempotencyKey: "capture-failure-request",
      inputChecksum: "capture-failure-input",
      sagaId: "capture-failure-saga",
      serverTimeMs: 1_000,
      stepInputChecksums: { a: "capture-failure-a" },
    }
    const throwingGetter = { ...valid }
    Object.defineProperty(throwingGetter, "sagaId", {
      enumerable: true,
      get() {
        throw new Error("getter-specific detail")
      },
    })
    const throwingProxy = new Proxy(valid, {
      get(target, property, receiver) {
        if (property === "inputChecksum") throw new Error("proxy-specific detail")
        return Reflect.get(target, property, receiver)
      },
    })
    const proxiedChecksums = {
      ...valid,
      stepInputChecksums: new Proxy({ a: "capture-failure-a" }, {}),
    }
    const uncloneableChecksums = {
      ...valid,
      stepInputChecksums: { a: () => "capture-failure-a" },
    }

    for (const candidate of [
      throwingGetter,
      throwingProxy,
      proxiedChecksums,
      uncloneableChecksums,
    ]) {
      expect(() => createSagaRecord(candidate as SagaCreationInput)).toThrowError(
        expect.objectContaining({
          code: "ConfigurationError",
          message: "Saga creation input could not be captured safely.",
        }),
      )
    }
  })

  it("rejects malformed creation, nonserial commands, and contradictory outcomes", async () => {
    const descriptor = (await saga()).descriptor
    for (const invalid of [
      { sagaId: "", deadlineAtMs: 2_000, stepInputChecksums: { a: "a", b: "b" } },
      { sagaId: "s", deadlineAtMs: 1_000, stepInputChecksums: { a: "a", b: "b" } },
      { sagaId: "s", deadlineAtMs: 2_000, stepInputChecksums: { a: "a" } },
      { sagaId: "s", deadlineAtMs: 2_000, stepInputChecksums: [] as never },
    ]) {
      expectCode(
        () =>
          createSagaRecord({
            deadlineAtMs: invalid.deadlineAtMs,
            descriptor,
            idempotencyKey: "key",
            inputChecksum: "input",
            sagaId: invalid.sagaId,
            serverTimeMs: 1_000,
            stepInputChecksums: invalid.stepInputChecksums,
          }),
        "ConfigurationError",
      )
    }
    expectCode(
      () =>
        createSagaRecord({
          deadlineAtMs: 2_000,
          descriptor: structuredClone(descriptor),
          idempotencyKey: "key",
          inputChecksum: "input",
          sagaId: "s",
          serverTimeMs: 1_000,
          stepInputChecksums: { a: "a", b: "b" },
        }),
      "ConfigurationError",
    )

    let record = await saga()
    const command = nextSagaCommand(record, 1_000)
    if (command.kind !== "execute") throw new Error("expected execute")
    expectCode(
      () =>
        beginSagaAction(record, {
          attemptId: "b-1",
          idempotencyKey: command.idempotencyKey,
          phase: "forward",
          serverTimeMs: 1_000,
          stepId: "b",
        }),
      "OperationResumeRequiredError",
    )
    expectCode(
      () =>
        beginSagaAction(record, {
          attemptId: "a-1",
          idempotencyKey: "wrong",
          phase: "forward",
          serverTimeMs: 1_000,
          stepId: "a",
        }),
      "OperationResumeRequiredError",
    )
    record = execute(record, "a", "forward", "a-1", 1_000)
    const contradictions: readonly [() => unknown, string][] = [
      [() => succeed(record, "a", "forward", "other", 1_001), "OperationResumeRequiredError"],
      [
        () =>
          recordSagaActionFailure(record, {
            attemptId: "a-1",
            errorChecksum: "error",
            outcome: "bad" as never,
            phase: "forward",
            serverTimeMs: 1_001,
            stepId: "a",
          }),
        "ConfigurationError",
      ],
      [() => nextSagaCommand(record, -1), "ConfigurationError"],
      [
        () =>
          requestSagaTermination(record, {
            cause: "other" as never,
            serverTimeMs: 1_001,
          }),
        "ConfigurationError",
      ],
      [
        () =>
          recordSagaActionFailure(record, {
            attemptId: "a-1",
            errorChecksum: "error",
            outcome: "unknown",
            phase: "bad" as never,
            serverTimeMs: 1_001,
            stepId: "a",
          }),
        "ConfigurationError",
      ],
      [
        () =>
          recordSagaObservation(record, {
            evidenceChecksum: "evidence",
            outcome: "applied",
            phase: "forward",
            resultChecksum: "result",
            serverTimeMs: 1_001,
            stepId: "a",
          }),
        "OperationResumeRequiredError",
      ],
    ]
    for (const [callback, code] of contradictions) {
      expectCode(callback, code)
    }
  })

  it("fails closed on malformed projections and covers terminal recovery edges", async () => {
    const initial = await saga()
    expectCode(
      () =>
        beginSagaAction(initial, {
          attemptId: "missing-1",
          idempotencyKey: "key",
          phase: "forward",
          serverTimeMs: 1_000,
          stepId: "missing",
        }),
      "ConfigurationError",
    )

    const runningA = execute(initial, "a", "forward", "a-1", 1_000)
    const ghost = {
      ...runningA,
      steps: { ...runningA.steps, ghost: runningA.steps.a },
    } as unknown as SagaRecord
    expectCode(
      () =>
        recordSagaActionFailure(ghost, {
          attemptId: "a-1",
          errorChecksum: "ghost",
          outcome: "definitely_not_applied_retryable",
          phase: "forward",
          serverTimeMs: 1_001,
          stepId: "ghost",
        }),
      "ConfigurationError",
    )

    const malformedFailed = {
      ...initial,
      status: "running" as const,
      steps: {
        ...initial.steps,
        a: {
          ...initial.steps.a,
          forward: { ...initial.steps.a?.forward, state: "failed" as const },
        },
      },
    } as unknown as SagaRecord
    expectCode(() => nextSagaCommand(malformedFailed, 1_000), "ConfigurationError")
    expect(
      nextSagaCommand({ ...initial, status: "compensating", terminationCause: "failure" }, 1_000),
    ).toEqual({
      kind: "terminal",
      status: "failed",
    })

    let succeeded = execute(initial, "a", "forward", "a-1", 1_000)
    succeeded = succeed(succeeded, "a", "forward", "a-1", 1_001)
    succeeded = execute(succeeded, "b", "forward", "b-1", 1_002)
    succeeded = succeed(succeeded, "b", "forward", "b-1", 1_003)
    expect(nextSagaCommand({ ...succeeded, status: "running" }, 1_004)).toEqual({
      kind: "terminal",
      status: "succeeded",
    })

    const bIdempotencyKey = initial.steps.b?.forward.idempotencyKey as string
    expectCode(
      () =>
        beginSagaAction(initial, {
          attemptId: "b-1",
          idempotencyKey: bIdempotencyKey,
          phase: "forward",
          serverTimeMs: 1_000,
          stepId: "b",
        }),
      "OperationResumeRequiredError",
    )

    let retry = await saga([descriptorStep("a", { maxAttempts: 3 })])
    retry = execute(retry, "a", "forward", "a-1", 1_000)
    retry = recordSagaActionFailure(retry, {
      attemptId: "a-1",
      errorChecksum: "retry-1",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    retry = execute(retry, "a", "forward", "a-2", 1_011)
    retry = recordSagaActionFailure(retry, {
      attemptId: "a-2",
      errorChecksum: "retry-2",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_012,
      stepId: "a",
    })
    expect(nextSagaCommand(retry, 1_031)).toMatchObject({ untilMs: 1_032 })

    let overflow = await saga([descriptorStep("a")])
    overflow = { ...overflow, deadlineAtMs: Number.MAX_SAFE_INTEGER }
    overflow = execute(overflow, "a", "forward", "a-1", Number.MAX_SAFE_INTEGER - 5)
    expectCode(
      () =>
        recordSagaActionFailure(overflow, {
          attemptId: "a-1",
          errorChecksum: "retry",
          outcome: "definitely_not_applied_retryable",
          phase: "forward",
          serverTimeMs: Number.MAX_SAFE_INTEGER - 5,
          stepId: "a",
        }),
      "ConfigurationError",
    )

    let compensating = await saga([descriptorStep("a", { maxAttempts: 1 }), descriptorStep("b")])
    compensating = execute(compensating, "a", "forward", "a-1", 1_000)
    compensating = succeed(compensating, "a", "forward", "a-1", 1_001)
    compensating = execute(compensating, "b", "forward", "b-1", 1_002)
    compensating = recordSagaActionFailure(compensating, {
      attemptId: "b-1",
      errorChecksum: "terminal",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "b",
    })
    compensating = execute(compensating, "a", "compensation", "a-c-1", 1_004)
    const compensationFailed = recordSagaActionFailure(compensating, {
      attemptId: "a-c-1",
      errorChecksum: "compensation-terminal",
      outcome: "definitely_not_applied_terminal",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    expect(compensationFailed.status).toBe("intervention_required")
    expect(
      beginSagaAction(compensationFailed, {
        attemptId: "a-c-1",
        idempotencyKey: compensationFailed.steps.a?.compensation.idempotencyKey as string,
        phase: "compensation",
        serverTimeMs: 1_006,
        stepId: "a",
      }),
    ).toMatchObject({ disposition: "replay_failure" })

    const notDispatchedCompensation = markSagaActionNotDispatched(compensating, {
      attemptId: "a-c-1",
      errorChecksum: "not-dispatched",
      phase: "compensation",
      serverTimeMs: 1_005,
      stepId: "a",
    })
    expect(notDispatchedCompensation.status).toBe("intervention_required")

    const cancelling = requestSagaTermination(runningA, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    expect(requestSagaTermination(cancelling, { cause: "timeout", serverTimeMs: 1_002 })).toBe(
      cancelling,
    )
  })

  it("requires complete unknown-action evidence before recording observations", async () => {
    let record = await saga([descriptorStep("a", { maxAttempts: 1 })])
    record = execute(record, "a", "forward", "a-1", 1_000)
    const unknown = markRunningSagaActionUnknown(record, {
      attemptId: "a-1",
      errorChecksum: "unknown",
      phase: "forward",
      stepId: "a",
    })
    expectCode(
      () =>
        recordSagaObservation(unknown, {
          evidenceChecksum: "evidence",
          outcome: "unsupported" as never,
          phase: "forward",
          serverTimeMs: 1_001,
          stepId: "a",
        }),
      "ConfigurationError",
    )
    const notApplied = recordSagaObservation(unknown, {
      evidenceChecksum: "not-applied",
      outcome: "not_applied",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    expect(notApplied).toMatchObject({ status: "failed", terminationCause: "failure" })

    const missingAttempt = {
      ...unknown,
      steps: {
        a: {
          ...unknown.steps.a,
          forward: { ...unknown.steps.a?.forward, lastAttemptId: undefined },
        },
      },
    } as unknown as SagaRecord
    expectCode(
      () =>
        recordSagaObservation(missingAttempt, {
          evidenceChecksum: "applied",
          outcome: "applied",
          phase: "forward",
          resultChecksum: "result",
          serverTimeMs: 1_001,
          stepId: "a",
        }),
      "OperationInterventionRequiredError",
    )
    const missingError = {
      ...unknown,
      steps: {
        a: {
          ...unknown.steps.a,
          forward: { ...unknown.steps.a?.forward, errorChecksum: undefined },
        },
      },
    } as unknown as SagaRecord
    expectCode(
      () =>
        recordSagaObservation(missingError, {
          evidenceChecksum: "indeterminate",
          outcome: "indeterminate",
          phase: "forward",
          serverTimeMs: 1_001,
          stepId: "a",
        }),
      "OperationInterventionRequiredError",
    )

    const contradictoryDuplicate = {
      ...record,
      steps: {
        a: {
          ...record.steps.a,
          forward: {
            ...record.steps.a?.forward,
            activeAttemptId: undefined,
            lastAttemptId: "a-1",
            state: "pending",
          },
        },
      },
    } as unknown as SagaRecord
    expectCode(
      () =>
        beginSagaAction(contradictoryDuplicate, {
          attemptId: "a-1",
          idempotencyKey: contradictoryDuplicate.steps.a?.forward.idempotencyKey as string,
          phase: "forward",
          serverTimeMs: 1_001,
          stepId: "a",
        }),
      "OperationInterventionRequiredError",
    )

    const cancelledUnknown = requestSagaTermination(unknown, {
      cause: "cancellation",
      serverTimeMs: 1_001,
    })
    const cancelledNotApplied = recordSagaObservation(cancelledUnknown, {
      evidenceChecksum: "not-applied-after-cancel",
      outcome: "not_applied",
      phase: "forward",
      serverTimeMs: 1_002,
      stepId: "a",
    })
    expect(cancelledNotApplied.status).toBe("cancelled")
  })

  it("reconstructs every durable saga action phase without trusting object identity", async () => {
    const initial = await saga()
    const running = execute(initial, "a", "forward", "a-1", 1_000)
    const unknown = markRunningSagaActionUnknown(running, {
      attemptId: "a-1",
      errorChecksum: "dispatch-uncertain",
      phase: "forward",
      stepId: "a",
    })
    const retryable = recordSagaActionFailure(running, {
      attemptId: "a-1",
      errorChecksum: "retryable",
      outcome: "definitely_not_applied_retryable",
      phase: "forward",
      serverTimeMs: 1_001,
      stepId: "a",
    })
    const observed = recordSagaObservation(unknown, {
      evidenceChecksum: "observed-applied",
      outcome: "applied",
      phase: "forward",
      resultChecksum: "result-a",
      serverTimeMs: 1_002,
      stepId: "a",
    })

    let compensating = succeed(running, "a", "forward", "a-1", 1_001)
    compensating = execute(compensating, "b", "forward", "b-1", 1_002)
    compensating = recordSagaActionFailure(compensating, {
      attemptId: "b-1",
      errorChecksum: "terminal-b",
      outcome: "definitely_not_applied_terminal",
      phase: "forward",
      serverTimeMs: 1_003,
      stepId: "b",
    })
    const compensatingRunning = execute(compensating, "a", "compensation", "a-compensate-1", 1_004)
    const compensatingUnknown = markRunningSagaActionUnknown(compensatingRunning, {
      attemptId: "a-compensate-1",
      errorChecksum: "compensation-uncertain",
      phase: "compensation",
      stepId: "a",
    })
    const compensated = succeed(compensatingRunning, "a", "compensation", "a-compensate-1", 1_005)

    const irreversibleInitial = await saga([irreversibleStep("commit")])
    let irreversible = execute(irreversibleInitial, "commit", "forward", "commit-1", 1_000)
    irreversible = succeed(irreversible, "commit", "forward", "commit-1", 1_001)

    for (const record of [
      initial,
      running,
      unknown,
      retryable,
      observed,
      compensating,
      compensatingRunning,
      compensatingUnknown,
      compensated,
      irreversibleInitial,
      irreversible,
    ]) {
      const loaded = await loadSagaRecord(structuredClone(record), digest)
      expect(loaded).toEqual(record)
      expect(loaded).not.toBe(record)
      expect(Object.isFrozen(loaded)).toBe(true)
      expect(Object.isFrozen(loaded.steps)).toBe(true)
      expect(Object.isFrozen(loaded.steps[loaded.descriptor.steps[0]?.stepId as string])).toBe(true)
    }
  })

  it("fails closed on malformed or contradictory persisted saga projections", async () => {
    const initial = await saga()
    const running = execute(initial, "a", "forward", "a-1", 1_000)
    const irreversible = await saga([irreversibleStep("commit")])

    for (const candidate of [null, [], new Date(), Object.create(null)]) {
      await expectAsyncCode(loadSagaRecord(candidate, digest), "OperationInterventionRequiredError")
    }

    const initialMutations: readonly ((candidate: MutableRecord) => void)[] = [
      (candidate) => {
        candidate.extra = true
      },
      (candidate) => {
        delete candidate.status
      },
      (candidate) => {
        candidate.sagaId = ""
      },
      (candidate) => {
        candidate.idempotencyKey = 1
      },
      (candidate) => {
        candidate.inputChecksum = " "
      },
      (candidate) => {
        candidate.deadlineAtMs = -1
      },
      (candidate) => {
        candidate.deadlineAtMs = 1.5
      },
      (candidate) => {
        candidate.stateVersion = -1
      },
      (candidate) => {
        candidate.stateVersion = Number.MAX_SAFE_INTEGER + 1
      },
      (candidate) => {
        candidate.status = "paused"
      },
      (candidate) => {
        candidate.terminationCause = "operator"
      },
      (candidate) => {
        candidate.terminationRequestedAtMs = -1
      },
      (candidate) => {
        candidate.terminationCause = "timeout"
      },
      (candidate) => {
        mutableRecord(candidate.descriptor).descriptorChecksum = "tampered"
      },
      (candidate) => {
        candidate.steps = []
      },
      (candidate) => {
        mutableRecord(candidate.steps).ghost = mutableStep(candidate, "a")
      },
      (candidate) => {
        delete mutableRecord(candidate.steps).a
      },
      (candidate) => {
        mutableRecord(candidate.steps).a = null
      },
      (candidate) => {
        mutableStep(candidate, "a").extra = true
      },
      (candidate) => {
        delete mutableStep(candidate, "a").forward
      },
      (candidate) => {
        mutableStep(candidate, "a").inputChecksum = ""
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").extra = true
      },
      (candidate) => {
        delete mutableAction(candidate, "a", "forward").attempts
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").attempts = 0.5
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").nextAttemptAtMs = -1
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").idempotencyKey = "wrong"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").state = "lost"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").state = "not_required"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").errorChecksum = ""
      },
      (candidate) => {
        const action = mutableAction(candidate, "a", "forward")
        action.state = "failed"
        action.attempts = 0
      },
      (candidate) => {
        const action = mutableAction(candidate, "a", "forward")
        action.state = "running"
        action.attempts = 1
        action.lastAttemptId = "a-1"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").lastAttemptId = "a-1"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").resultChecksum = "unexpected"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").errorChecksum = "unexpected"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").nextAttemptAtMs = 1_001
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").observationEvidenceChecksum = "unexpected"
      },
      (candidate) => {
        const compensation = mutableAction(candidate, "a", "compensation")
        compensation.state = "running"
        compensation.attempts = 1
        compensation.activeAttemptId = "a-c-1"
        compensation.lastAttemptId = "a-c-1"
      },
      (candidate) => {
        const forward = mutableAction(candidate, "b", "forward")
        forward.state = "running"
        forward.attempts = 1
        forward.activeAttemptId = "b-1"
        forward.lastAttemptId = "b-1"
        candidate.stateVersion = 1
        candidate.status = "running"
      },
      (candidate) => {
        candidate.stateVersion = 1
      },
      (candidate) => {
        candidate.terminationCause = "failure"
        candidate.terminationRequestedAtMs = 1_000
        candidate.stateVersion = 1
        candidate.status = "failed"
      },
    ]

    for (const mutate of initialMutations) {
      const candidate = mutableRecord(structuredClone(initial))
      mutate(candidate)
      await expectAsyncCode(loadSagaRecord(candidate, digest), "OperationInterventionRequiredError")
    }

    const runningMutations: readonly ((candidate: MutableRecord) => void)[] = [
      (candidate) => {
        candidate.stateVersion = 0
        candidate.status = "planned"
      },
      (candidate) => {
        candidate.status = "succeeded"
      },
      (candidate) => {
        const action = mutableAction(candidate, "a", "forward")
        action.activeAttemptId = ""
      },
      (candidate) => {
        const action = mutableAction(candidate, "a", "forward")
        action.activeAttemptId = "other-attempt"
      },
      (candidate) => {
        mutableAction(candidate, "a", "forward").attempts = 4
      },
      (candidate) => {
        const action = mutableAction(candidate, "a", "forward")
        action.resultChecksum = "unexpected"
      },
      (candidate) => {
        const action = mutableAction(candidate, "b", "forward")
        action.state = "unknown"
        action.attempts = 1
        action.errorChecksum = "unknown-b"
        action.lastAttemptId = "b-1"
      },
    ]
    for (const mutate of runningMutations) {
      const candidate = mutableRecord(structuredClone(running))
      mutate(candidate)
      await expectAsyncCode(loadSagaRecord(candidate, digest), "OperationInterventionRequiredError")
    }

    const exhaustedRetry = mutableRecord(structuredClone(running))
    const exhaustedAction = mutableAction(exhaustedRetry, "a", "forward")
    delete exhaustedAction.activeAttemptId
    exhaustedAction.state = "retryable_failed"
    exhaustedAction.attempts = 3
    exhaustedAction.errorChecksum = "retryable"
    exhaustedAction.nextAttemptAtMs = 2_000
    await expectAsyncCode(
      loadSagaRecord(exhaustedRetry, digest),
      "OperationInterventionRequiredError",
    )

    let progressed = succeed(running, "a", "forward", "a-1", 1_001)
    progressed = execute(progressed, "b", "forward", "b-1", 1_002)
    let reverseOrder = requestSagaTermination(progressed, {
      cause: "cancellation",
      serverTimeMs: 1_003,
    })
    reverseOrder = succeed(reverseOrder, "b", "forward", "b-1", 1_004)
    const reverseOrderProjection = mutableRecord(structuredClone(reverseOrder))
    const earlyCompensation = mutableAction(reverseOrderProjection, "a", "compensation")
    earlyCompensation.state = "running"
    earlyCompensation.attempts = 1
    earlyCompensation.activeAttemptId = "a-c-1"
    earlyCompensation.lastAttemptId = "a-c-1"
    await expectAsyncCode(
      loadSagaRecord(reverseOrderProjection, digest),
      "OperationInterventionRequiredError",
    )

    const compensationRunning = execute(reverseOrder, "b", "compensation", "b-c-1", 1_005)
    const exhaustedCompensation = mutableRecord(structuredClone(compensationRunning))
    const exhaustedCompensationAction = mutableAction(exhaustedCompensation, "b", "compensation")
    delete exhaustedCompensationAction.activeAttemptId
    exhaustedCompensationAction.state = "retryable_failed"
    exhaustedCompensationAction.attempts = 3
    exhaustedCompensationAction.errorChecksum = "retryable"
    exhaustedCompensationAction.nextAttemptAtMs = 2_000
    await expectAsyncCode(
      loadSagaRecord(exhaustedCompensation, digest),
      "OperationInterventionRequiredError",
    )

    const illegalIrreversibleCompensation = mutableRecord(structuredClone(irreversible))
    mutableAction(illegalIrreversibleCompensation, "commit", "compensation").state = "pending"
    await expectAsyncCode(
      loadSagaRecord(illegalIrreversibleCompensation, digest),
      "OperationInterventionRequiredError",
    )

    const throwingDigest: DigestFunction = async () => {
      throw new Error("digest unavailable")
    }
    await expectAsyncCode(
      loadSagaRecord(structuredClone(initial), throwingDigest),
      "OperationInterventionRequiredError",
    )

    const throwingProjection = mutableRecord(structuredClone(initial))
    Object.defineProperty(throwingProjection, "sagaId", {
      enumerable: true,
      get() {
        throw new Error("storage decoder failed")
      },
    })
    await expectAsyncCode(
      loadSagaRecord(throwingProjection, digest),
      "OperationInterventionRequiredError",
    )
  })

  it("validates an owned stable snapshot before reconstructing a persisted saga", async () => {
    const initial = await saga([descriptorStep("a")])
    const changingGetter = mutableRecord(structuredClone(initial))
    let statusReads = 0
    Object.defineProperty(changingGetter, "status", {
      configurable: true,
      enumerable: true,
      get() {
        statusReads += 1
        return statusReads === 1 ? "planned" : "succeeded"
      },
    })

    const loadedGetter = await loadSagaRecord(changingGetter, digest)
    expect(statusReads).toBe(1)
    expect(loadedGetter.status).toBe("planned")
    expect(Object.getOwnPropertyDescriptor(loadedGetter, "status")).toMatchObject({
      value: "planned",
    })
    expectCode(() => mapSagaSettlementOutcome(loadedGetter), "OperationResumeRequiredError")

    let proxyStatusReads = 0
    const changingProxy = new Proxy(mutableRecord(structuredClone(initial)), {
      get(target, property, receiver) {
        if (property === "status") {
          proxyStatusReads += 1
          return proxyStatusReads === 1 ? "planned" : "succeeded"
        }
        return Reflect.get(target, property, receiver)
      },
    })
    await expectAsyncCode(
      loadSagaRecord(changingProxy, digest),
      "OperationInterventionRequiredError",
    )
    expect(proxyStatusReads).toBe(0)

    const asynchronouslyChanged = mutableRecord(structuredClone(initial))
    let changed = false
    const mutatingDigest: DigestFunction = async (input) => {
      asynchronouslyChanged.status = "succeeded"
      mutableAction(asynchronouslyChanged, "a", "forward").state = "succeeded"
      changed = true
      return digest(input)
    }
    const loadedOwned = await loadSagaRecord(asynchronouslyChanged, mutatingDigest)
    expect(changed).toBe(true)
    expect(asynchronouslyChanged.status).toBe("succeeded")
    expect(loadedOwned.status).toBe("planned")
    expect(loadedOwned.steps.a?.forward.state).toBe("pending")
    expect(Object.isFrozen(loadedOwned)).toBe(true)
  })

  it("model-checks generated forward and compensation outcomes for bounded step counts", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.boolean(), { maxLength: 5, minLength: 1 }),
        async (forwardSuccesses) => {
          const steps = forwardSuccesses.map((_, index) =>
            descriptorStep(`s${index}`, { maxAttempts: 1 }),
          )
          let record = await saga(steps)
          for (let index = 0; index < forwardSuccesses.length; index += 1) {
            const stepId = `s${index}`
            record = execute(record, stepId, "forward", `${stepId}-1`, 1_000 + index * 2)
            if (forwardSuccesses[index]) {
              record = succeed(record, stepId, "forward", `${stepId}-1`, 1_001 + index * 2)
            } else {
              record = recordSagaActionFailure(record, {
                attemptId: `${stepId}-1`,
                errorChecksum: "terminal",
                outcome: "definitely_not_applied_terminal",
                phase: "forward",
                serverTimeMs: 1_001 + index * 2,
                stepId,
              })
              break
            }
          }
          while (record.status === "compensating") {
            const command = nextSagaCommand(record, 5_000)
            if (command.kind !== "execute") break
            record = execute(record, command.stepId, command.phase, `${command.stepId}-c`, 5_000)
            record = succeed(record, command.stepId, command.phase, `${command.stepId}-c`, 5_001)
          }
          expect(["failed", "succeeded"]).toContain(record.status)
          expect(sagaCommitment(record)).toBe(record.status === "succeeded" ? "complete" : "none")
        },
      ),
      { numRuns: 100 },
    )
  })
})
