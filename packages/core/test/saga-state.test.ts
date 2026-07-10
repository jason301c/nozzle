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
