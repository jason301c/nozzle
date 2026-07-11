import { describe, expect, it } from "vitest"
import {
  type AtomicStepOutcome,
  appendAuditEvent,
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  encodeIrreversibleAuthorizationChecksumInput,
  type IrreversibleAuthorization,
  leaseProof,
  loadAuditEvent,
  loadOperationRecord,
  markOperationStepNotRequired,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepsUnknownAfterCrash,
  type OperationPlan,
  type OperationRecord,
  type OperationStepPlanInput,
  recordAtomicStepOutcome,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  sealIrreversibleAuthorization,
  sealOperationPlan,
} from "../src/operation.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function step(overrides: Partial<OperationStepPlanInput> = {}): OperationStepPlanInput {
  return {
    checkpoint: "reversible",
    dependsOn: [],
    idempotencyKey: "step-key",
    inputChecksum: "step-input",
    leaseKey: "fleet:operation",
    postconditionChecksum: "postcondition",
    preconditionChecksum: "precondition",
    recoveryInstructions: "Inspect the durable evidence and resume this operation.",
    retryClassification: "idempotent",
    stepId: "one",
    ...overrides,
  }
}

async function plan(overrides: Partial<OperationStepPlanInput> = {}): Promise<OperationPlan> {
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: "capabilities",
      idempotencyKey: "operation-key",
      inputChecksum: "operation-input",
      operationId: "operation-1",
      operationType: "persistence-test",
      steps: [step(overrides)],
    },
    digest,
  )
}

function lease() {
  const decision = decideLeaseAcquisition(undefined, {
    acquisitionId: "acquisition-1",
    holderId: "controller-1",
    leaseKey: "fleet:operation",
    serverTimeMs: 100,
    ttlMs: 1_000,
  })
  if (!decision.acquired) throw new Error("Fixture lease acquisition failed.")
  return decision.record
}

function atomicInput(outcome: AtomicStepOutcome) {
  const activeLease = lease()
  return {
    attemptId: "atomic-attempt-1",
    idempotencyKey: "step-key",
    leaseProof: leaseProof(activeLease),
    observedPreconditionChecksum: "precondition",
    outcome,
    stepId: "one",
  }
}

async function started(overrides: Partial<OperationStepPlanInput> = {}): Promise<OperationRecord> {
  const operation = createOperationRecord(await plan(overrides))
  const activeLease = lease()
  const decision = beginOperationStep(operation, {
    attemptId: "attempt-1",
    idempotencyKey: "step-key",
    lease: activeLease,
    leaseProof: leaseProof(activeLease),
    observedPreconditionChecksum: "precondition",
    serverTimeMs: 110,
    stepId: "one",
  })
  if (decision.disposition !== "execute") throw new Error("Fixture step did not start.")
  return decision.operation
}

async function authorizedStarted(overrides: Partial<OperationStepPlanInput> = {}): Promise<{
  readonly authorization: IrreversibleAuthorization
  readonly operation: OperationRecord
}> {
  const sealedPlan = await plan({
    checkpoint: "irreversible",
    effectProtocol: "saga_receipt",
    retryClassification: "reconcile_first",
    ...overrides,
  })
  const operation = createOperationRecord(sealedPlan)
  const activeLease = lease()
  const proof = leaseProof(activeLease)
  const authorization = await sealIrreversibleAuthorization(
    sealedPlan,
    {
      actorChecksum: "actor",
      authorizationId: "authorization-1",
      decisionChecksum: "decision",
      lease: activeLease,
      leaseProof: proof,
      sealedAtServerTimeMs: 105,
      stepId: "one",
    },
    digest,
  )
  const decision = beginOperationStep(operation, {
    attemptId: "attempt-1",
    idempotencyKey: "step-key",
    irreversibleAuthorization: authorization,
    lease: activeLease,
    leaseProof: proof,
    observedPreconditionChecksum: "precondition",
    serverTimeMs: 110,
    stepId: "one",
  })
  if (decision.disposition !== "execute") throw new Error("Fixture step did not start.")
  return { authorization, operation: decision.operation }
}

async function roundTrip(operation: OperationRecord): Promise<OperationRecord> {
  return loadOperationRecord(JSON.parse(JSON.stringify(operation)) as unknown, digest)
}

async function resignAuthorization(
  authorization: IrreversibleAuthorization,
): Promise<IrreversibleAuthorization> {
  const { authorizationChecksum: _authorizationChecksum, ...unsigned } = authorization
  return {
    ...unsigned,
    authorizationChecksum: await digest(encodeIrreversibleAuthorizationChecksumInput(unsigned)),
  }
}

function replaceStep(operation: OperationRecord, replacement: unknown): unknown {
  return {
    plan: JSON.parse(JSON.stringify(operation.plan)) as unknown,
    steps: { one: replacement },
  }
}

describe("persisted operation record integrity", () => {
  it("round trips every reachable durable step-state shape", async () => {
    const pending = createOperationRecord(await plan())
    const running = await started()
    const atomicSucceeded = recordAtomicStepOutcome(
      pending,
      atomicInput({
        observedPostconditionChecksum: "postcondition",
        resultChecksum: "atomic-result",
        state: "succeeded",
      }),
    )
    const atomicFailed = recordAtomicStepOutcome(
      createOperationRecord(await plan()),
      atomicInput({ errorChecksum: "atomic-error", state: "failed" }),
    )
    const atomicIntervention = recordAtomicStepOutcome(
      createOperationRecord(await plan()),
      atomicInput({ evidenceChecksum: "atomic-evidence", state: "intervention_required" }),
    )
    const succeeded = recordStepSuccess(running, {
      attemptId: "attempt-1",
      counters: { cost: { providerCalls: 1 }, progress: { resources: 1 } },
      observedPostconditionChecksum: "postcondition",
      resultChecksum: "result",
      stepId: "one",
    })
    const retryable = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "retryable-error",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    const failed = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "permanent-error",
      outcome: "permanent",
      stepId: "one",
    })
    const unknown = recordStepFailure(await started(), {
      attemptId: "attempt-1",
      errorChecksum: "unknown-error",
      outcome: "unknown",
      stepId: "one",
    })
    const reconciledApplied = recordStepReconciliation(unknown, {
      evidenceChecksum: "applied-evidence",
      observedPostconditionChecksum: "postcondition",
      outcome: "applied",
      resultChecksum: "reconciled-result",
      stepId: "one",
    })
    const reconciledIntervention = recordStepReconciliation(unknown, {
      evidenceChecksum: "indeterminate-evidence",
      outcome: "indeterminate",
      stepId: "one",
    })
    const reconciledRetry = recordStepReconciliation(unknown, {
      evidenceChecksum: "absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })
    const crashUnknown = markRunningStepsUnknownAfterCrash(await started())
    const reconciledCrash = recordStepReconciliation(crashUnknown, {
      evidenceChecksum: "crash-absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })
    const neverCrash = markRunningStepsUnknownAfterCrash(
      await started({ retryClassification: "never" }),
    )
    const reconciledNever = recordStepReconciliation(neverCrash, {
      evidenceChecksum: "never-absent-evidence",
      outcome: "not_applied",
      stepId: "one",
    })
    const sagaUnknown = recordStepFailure(
      await started({ effectProtocol: "saga_receipt", retryClassification: "reconcile_first" }),
      {
        attemptId: "attempt-1",
        errorChecksum: "saga-unknown-error",
        outcome: "unknown",
        stepId: "one",
      },
    )
    const reconciledSagaTerminal = recordSagaStepTerminalClassification(sagaUnknown, {
      outcome: "not_applied",
      receiptOutcomeChecksum: "saga-terminal-receipt",
      stepId: "one",
    })
    const sagaRetryable = recordStepFailure(
      await started({ effectProtocol: "saga_receipt", retryClassification: "reconcile_first" }),
      {
        attemptId: "attempt-1",
        counters: { cost: { calls: 1 }, progress: { receipts: 2 } },
        errorChecksum: "saga-proven-absence",
        outcome: "definitely_not_applied",
        stepId: "one",
      },
    )
    const classifiedSagaRetryable = recordSagaStepTerminalClassification(sagaRetryable, {
      counters: { cost: { calls: 3 }, progress: { receipts: 4 } },
      outcome: "not_applied",
      receiptOutcomeChecksum: "saga-proven-absence",
      stepId: "one",
    })
    const conditional = createOperationRecord(
      await sealOperationPlan(
        {
          capabilitySnapshotChecksum: "capabilities",
          idempotencyKey: "conditional-operation-key",
          inputChecksum: "conditional-operation-input",
          operationId: "conditional-operation",
          operationType: "persistence-test",
          steps: [
            step(),
            step({
              activation: "conditional",
              idempotencyKey: "conditional-step-key",
              inputChecksum: "conditional-step-input",
              postconditionChecksum: "conditional-postcondition",
              preconditionChecksum: "conditional-precondition",
              stepId: "conditional",
            }),
          ],
        },
        digest,
      ),
    )
    const notRequired = markOperationStepNotRequired(conditional, {
      evidenceChecksum: "conditional-decision",
      stepId: "conditional",
    })

    for (const operation of [
      pending,
      running,
      atomicSucceeded,
      atomicFailed,
      atomicIntervention,
      succeeded,
      retryable,
      failed,
      unknown,
      reconciledApplied,
      reconciledIntervention,
      reconciledRetry,
      crashUnknown,
      reconciledCrash,
      reconciledNever,
      reconciledSagaTerminal,
      classifiedSagaRetryable,
      notRequired,
    ]) {
      const loaded = await roundTrip(operation)
      expect(loaded).toEqual(operation)
      expect(Object.isFrozen(loaded)).toBe(true)
      expect(Object.isFrozen(loaded.plan)).toBe(true)
      expect(Object.isFrozen(loaded.steps)).toBe(true)
      expect(Object.isFrozen(loaded.steps.one)).toBe(true)
      expect(Object.isFrozen(loaded.steps.one?.costCounters)).toBe(true)
      if (loaded.steps.conditional !== undefined) {
        expect(Object.isFrozen(loaded.steps.conditional)).toBe(true)
      }
    }
  })

  it("round trips atomic terminal records as exact duplicate replays", async () => {
    for (const outcome of [
      {
        observedPostconditionChecksum: "postcondition",
        resultChecksum: "atomic-result",
        state: "succeeded",
      },
      { errorChecksum: "atomic-error", state: "failed" },
      { evidenceChecksum: "atomic-evidence", state: "intervention_required" },
    ] satisfies readonly AtomicStepOutcome[]) {
      const input = atomicInput(outcome)
      const committed = recordAtomicStepOutcome(createOperationRecord(await plan()), input)
      const loaded = await roundTrip(committed)
      expect(recordAtomicStepOutcome(loaded, input)).toBe(loaded)
    }
  })

  it("round trips exact saga terminal classification replays from both eligible states", async () => {
    for (const outcome of ["unknown", "definitely_not_applied"] as const) {
      const source = recordStepFailure(
        await started({ effectProtocol: "saga_receipt", retryClassification: "reconcile_first" }),
        {
          attemptId: "attempt-1",
          counters: { cost: { calls: 1 }, progress: { receipts: 2 } },
          errorChecksum: `saga-${outcome}`,
          outcome,
          stepId: "one",
        },
      )
      const input = {
        counters: { cost: { calls: 3 }, progress: { receipts: 4 } },
        outcome: "not_applied" as const,
        receiptOutcomeChecksum:
          outcome === "definitely_not_applied"
            ? "saga-definitely_not_applied"
            : `saga-terminal-${outcome}`,
        stepId: "one",
      }
      const classified = recordSagaStepTerminalClassification(source, input)
      const loaded = await roundTrip(classified)

      expect(loaded).toEqual(classified)
      expect(recordSagaStepTerminalClassification(loaded, input)).toBe(loaded)
      expect(loaded.steps.one).toMatchObject({
        costCounters: { calls: 4 },
        progressCounters: { receipts: 6 },
        reconciliationEvidenceChecksum: input.receiptOutcomeChecksum,
        resultChecksum: input.receiptOutcomeChecksum,
        state: "succeeded",
      })
      expect(() =>
        recordSagaStepTerminalClassification(loaded, {
          ...input,
          receiptOutcomeChecksum: `${input.receiptOutcomeChecksum}:different`,
        }),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    }
  })

  it("round trips terminal dispatch-absence evidence for a never-retry step", async () => {
    const recovered = markRunningStepNotDispatchedAfterCrash(
      await started({ effectProtocol: "provider_receipt", retryClassification: "never" }),
      "one",
      "provider-dispatch-absence",
    )
    const loaded = await roundTrip(recovered)

    expect(loaded).toEqual(recovered)
    expect(loaded.steps.one).toEqual({
      costCounters: {},
      fencingToken: 1,
      lastAttemptId: "attempt-1",
      progressCounters: {},
      reconciliationEvidenceChecksum: "provider-dispatch-absence",
      startedAttempts: 1,
      state: "failed",
    })
    expect(Object.isFrozen(loaded.steps.one)).toBe(true)

    const activeLease = lease()
    expect(
      beginOperationStep(loaded, {
        attemptId: "attempt-2",
        idempotencyKey: "step-key",
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        observedPreconditionChecksum: "precondition",
        serverTimeMs: 110,
        stepId: "one",
      }),
    ).toEqual({ disposition: "blocked", operation: loaded })
  })

  it("round trips irreversible authorization only after an authorized attempt starts", async () => {
    const sealedPlan = await plan({ checkpoint: "irreversible" })
    const operation = createOperationRecord(sealedPlan)
    const activeLease = lease()
    const proof = leaseProof(activeLease)
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-1",
        decisionChecksum: "decision",
        lease: activeLease,
        leaseProof: proof,
        sealedAtServerTimeMs: 105,
        stepId: "one",
      },
      digest,
    )
    const decision = beginOperationStep(operation, {
      attemptId: "attempt-1",
      idempotencyKey: "step-key",
      irreversibleAuthorization: authorization,
      lease: activeLease,
      leaseProof: proof,
      observedPreconditionChecksum: "precondition",
      serverTimeMs: 110,
      stepId: "one",
    })
    if (decision.disposition !== "execute") throw new Error("Fixture step did not start.")
    await expect(roundTrip(operation)).resolves.toEqual(operation)
    const loaded = await roundTrip(decision.operation)
    expect(loaded).toEqual(decision.operation)
    expect(loaded.steps.one?.irreversibleAuthorization).toEqual(authorization)
    expect(loaded.steps.one?.irreversibleAuthorization).not.toBe(authorization)
    expect(Object.isFrozen(loaded.steps.one?.irreversibleAuthorization)).toBe(true)
    expect(
      recordStepSuccess(loaded, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "postcondition",
        resultChecksum: "trusted-after-load",
        stepId: "one",
      }).steps.one,
    ).toMatchObject({
      irreversibleAuthorization: authorization,
      resultChecksum: "trusted-after-load",
      state: "succeeded",
    })
  })

  it("round trips the full authorization through every attempted durable state", async () => {
    const { authorization, operation: running } = await authorizedStarted()
    const retryable = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "definitely-absent",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    const unknown = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "lost-response",
      outcome: "unknown",
      stepId: "one",
    })
    const activeLease = lease()
    const retried = beginOperationStep(retryable, {
      attemptId: "attempt-2",
      idempotencyKey: "step-key",
      irreversibleAuthorization: authorization,
      lease: activeLease,
      leaseProof: leaseProof(activeLease),
      observedPreconditionChecksum: "precondition",
      serverTimeMs: 110,
      stepId: "one",
    })
    if (retried.disposition !== "execute") throw new Error("Fixture retry did not start.")
    const records = [
      running,
      recordStepSuccess(running, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "postcondition",
        resultChecksum: "succeeded",
        stepId: "one",
      }),
      retryable,
      retried.operation,
      recordStepFailure(running, {
        attemptId: "attempt-1",
        errorChecksum: "permanent",
        outcome: "permanent",
        stepId: "one",
      }),
      unknown,
      markRunningStepsUnknownAfterCrash(running),
      markRunningStepNotDispatchedAfterCrash(running, "one", "dispatch-absent"),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observed-applied",
        observedPostconditionChecksum: "postcondition",
        outcome: "applied",
        resultChecksum: "reconciled",
        stepId: "one",
      }),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observed-absent",
        outcome: "not_applied",
        stepId: "one",
      }),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "indeterminate",
        outcome: "indeterminate",
        stepId: "one",
      }),
      recordSagaStepTerminalClassification(unknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-absence",
        stepId: "one",
      }),
      recordSagaStepTerminalClassification(retryable, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "definitely-absent",
        stepId: "one",
      }),
    ]

    for (const record of records) {
      const loaded = await roundTrip(record)
      expect(loaded).toEqual(record)
      expect(loaded.steps.one?.authorizationChecksum).toBe(authorization.authorizationChecksum)
      expect(loaded.steps.one?.irreversibleAuthorization).toEqual(authorization)
      expect(Object.isFrozen(loaded.steps.one?.irreversibleAuthorization)).toBe(true)
    }
  })

  it("loads legacy checksum-only records only for quarantine and refuses later mutations", async () => {
    const { authorization, operation: running } = await authorizedStarted()
    const runningStep = running.steps.one
    if (!runningStep) throw new Error("Fixture step is missing.")
    const { irreversibleAuthorization: _authorization, ...legacyRunningStep } = runningStep
    const loaded = await loadOperationRecord(replaceStep(running, legacyRunningStep), digest)

    expect(loaded.steps.one).toEqual(legacyRunningStep)
    expect(loaded.steps.one?.authorizationChecksum).toBeDefined()
    expect(loaded.steps.one?.irreversibleAuthorization).toBeUndefined()
    for (const mutate of [
      () =>
        recordStepSuccess(loaded, {
          attemptId: "attempt-1",
          observedPostconditionChecksum: "postcondition",
          resultChecksum: "result",
          stepId: "one",
        }),
      () =>
        recordStepFailure(loaded, {
          attemptId: "attempt-1",
          errorChecksum: "failed",
          outcome: "permanent",
          stepId: "one",
        }),
      () => markRunningStepsUnknownAfterCrash(loaded),
      () => markRunningStepNotDispatchedAfterCrash(loaded, "one", "dispatch-absent"),
      () =>
        recordSagaStepTerminalClassification(loaded, {
          outcome: "not_applied",
          receiptOutcomeChecksum: "terminal-absence",
          stepId: "one",
        }),
    ]) {
      expect(mutate).toThrowError(
        expect.objectContaining({ code: "OperationInterventionRequiredError" }),
      )
    }

    const unknown = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "lost-response",
      outcome: "unknown",
      stepId: "one",
    })
    const unknownStep = unknown.steps.one
    if (!unknownStep) throw new Error("Fixture unknown step is missing.")
    const { irreversibleAuthorization: _unknownAuthorization, ...legacyUnknownStep } = unknownStep
    const loadedUnknown = await loadOperationRecord(replaceStep(unknown, legacyUnknownStep), digest)
    expect(() =>
      recordStepReconciliation(loadedUnknown, {
        evidenceChecksum: "observed-absent",
        outcome: "not_applied",
        stepId: "one",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))

    const retryable = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "definitely-absent",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    const retryableStep = retryable.steps.one
    if (!retryableStep) throw new Error("Fixture retryable step is missing.")
    const { irreversibleAuthorization: _retryAuthorization, ...legacyRetryableStep } = retryableStep
    const loadedRetryable = await loadOperationRecord(
      replaceStep(retryable, legacyRetryableStep),
      digest,
    )
    const activeLease = lease()
    expect(() =>
      beginOperationStep(loadedRetryable, {
        attemptId: "attempt-2",
        idempotencyKey: "step-key",
        irreversibleAuthorization: authorization,
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        observedPreconditionChecksum: "precondition",
        serverTimeMs: 110,
        stepId: "one",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
  })

  it("rejects incomplete, contradictory, tampered, and extended authorization bodies", async () => {
    const { authorization, operation: running } = await authorizedStarted()
    const stepRecord = running.steps.one
    if (!stepRecord) throw new Error("Fixture step is missing.")
    const { authorizationChecksum: _stepChecksum, ...bodyWithoutStepChecksum } = stepRecord
    const { decisionChecksum: _decisionChecksum, ...bodyMissingField } = authorization
    const malformed = [
      bodyWithoutStepChecksum,
      { ...stepRecord, authorizationChecksum: "different-from-body" },
      {
        ...stepRecord,
        irreversibleAuthorization: { ...authorization, decisionChecksum: "tampered" },
      },
      {
        ...stepRecord,
        irreversibleAuthorization: { ...authorization, unexpected: "field" },
      },
      { ...stepRecord, irreversibleAuthorization: bodyMissingField },
      { ...stepRecord, irreversibleAuthorization: null },
      { ...stepRecord, irreversibleAuthorization: [] },
    ]

    for (const replacement of malformed) {
      await expect(loadOperationRecord(replaceStep(running, replacement), digest)).rejects.toThrow()
    }
  })

  it("rejects valid receipts bound to another plan, step, input, lease, or fence", async () => {
    const { authorization, operation: running } = await authorizedStarted()
    const stepRecord = running.steps.one
    if (!stepRecord) throw new Error("Fixture step is missing.")
    const mismatches = [
      { field: "operation", value: { ...authorization, operationId: "operation-2" } },
      { field: "plan", value: { ...authorization, planChecksum: "another-plan" } },
      { field: "step", value: { ...authorization, stepId: "another-step" } },
      { field: "input", value: { ...authorization, stepInputChecksum: "another-input" } },
      { field: "lease", value: { ...authorization, leaseKey: "another-lease" } },
      { field: "fence", value: { ...authorization, fencingToken: 2 } },
    ]

    for (const mismatch of mismatches) {
      const resigned = await resignAuthorization(mismatch.value)
      await expect(
        loadOperationRecord(
          replaceStep(running, {
            ...stepRecord,
            authorizationChecksum: resigned.authorizationChecksum,
            irreversibleAuthorization: resigned,
          }),
          digest,
        ),
        mismatch.field,
      ).rejects.toThrow(/different immutable plan|different operation fence/u)
    }
  })

  it("captures nested authorization accessors once and rejects nested proxies", async () => {
    const { authorization, operation: running } = await authorizedStarted()
    const serialized = structuredClone(running) as unknown as {
      plan: OperationPlan
      steps: Record<string, Record<string, unknown>>
    }
    const serializedStep = serialized.steps.one
    if (!serializedStep) throw new Error("Serialized fixture step is missing.")
    let bodyReads = 0
    Object.defineProperty(serializedStep, "irreversibleAuthorization", {
      enumerable: true,
      get() {
        bodyReads += 1
        return bodyReads === 1 ? { ...authorization } : { ...authorization, stepId: "changed" }
      },
    })

    const loaded = await loadOperationRecord(serialized, digest)
    expect(bodyReads).toBe(1)
    expect(loaded).toEqual(running)
    expect(Object.isFrozen(loaded.steps.one?.irreversibleAuthorization)).toBe(true)

    const proxied = structuredClone(running) as unknown as {
      plan: OperationPlan
      steps: Record<string, Record<string, unknown>>
    }
    const proxiedStep = proxied.steps.one
    if (!proxiedStep) throw new Error("Serialized proxy fixture step is missing.")
    proxiedStep.irreversibleAuthorization = new Proxy({ ...authorization }, {})
    await expect(loadOperationRecord(proxied, digest)).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The persisted operation record could not be captured safely.",
    })
  })

  it("accepts ordinary and null-prototype serialized records but rejects other containers", async () => {
    const operation = createOperationRecord(await plan())
    const candidate = Object.assign(Object.create(null) as Record<string, unknown>, {
      plan: JSON.parse(JSON.stringify(operation.plan)) as unknown,
      steps: Object.assign(Object.create(null) as Record<string, unknown>, {
        one: Object.assign(Object.create(null) as Record<string, unknown>, operation.steps.one),
      }),
    })
    await expect(loadOperationRecord(candidate, digest)).resolves.toEqual(operation)
    for (const malformed of [null, [], new Date(), "operation"]) {
      await expect(loadOperationRecord(malformed, digest)).rejects.toThrow(/record is malformed/u)
    }
  })

  it("rejects unknown top-level fields, malformed plans, and mismatched step membership", async () => {
    const operation = createOperationRecord(await plan())
    const serialized = JSON.parse(JSON.stringify(operation)) as Record<string, unknown>
    await expect(loadOperationRecord({ ...serialized, unexpected: true }, digest)).rejects.toThrow(
      /unknown fields/u,
    )
    await expect(loadOperationRecord({ ...serialized, plan: [] }, digest)).rejects.toThrow(
      /plan is malformed/u,
    )
    await expect(loadOperationRecord({ ...serialized, steps: [] }, digest)).rejects.toThrow(
      /steps are malformed/u,
    )
    await expect(loadOperationRecord({ ...serialized, steps: {} }, digest)).rejects.toThrow(
      /membership/u,
    )
    await expect(
      loadOperationRecord(
        { ...serialized, steps: { one: operation.steps.one, unexpected: operation.steps.one } },
        digest,
      ),
    ).rejects.toThrow(/membership/u)
  })

  it("owns the complete persisted record before awaiting plan verification", async () => {
    const operation = createOperationRecord(await plan())
    const serialized = structuredClone(operation) as {
      plan: OperationPlan
      steps: Record<string, unknown>
    }
    let mutated = false
    const mutatingDigest: DigestFunction = async (input) => {
      mutated = true
      serialized.steps.one = { unexpected: "post-capture mutation" }
      return digest(input)
    }

    await expect(loadOperationRecord(serialized, mutatingDigest)).resolves.toEqual(operation)
    expect(mutated).toBe(true)
    expect(serialized.steps.one).toEqual({ unexpected: "post-capture mutation" })
    await expect(
      loadOperationRecord(new Proxy(structuredClone(operation), {}), digest),
    ).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The persisted operation record could not be captured safely.",
    })
  })

  it("rejects malformed step record structure, strings, counters, and scalar metadata", async () => {
    const operation = createOperationRecord(await plan())
    const base = operation.steps.one as NonNullable<(typeof operation.steps)[string]>
    const malformed = [
      null,
      [],
      { ...base, unexpected: true },
      { ...base, state: "future" },
      { ...base, startedAttempts: -1 },
      { ...base, activeAttemptId: "" },
      { ...base, errorChecksum: "\ud800" },
      { ...base, fencingToken: 0 },
      { ...base, costCounters: null },
      { ...base, costCounters: { "": 1 } },
      { ...base, costCounters: { "\ud800": 1 } },
      { ...base, costCounters: { calls: -1 } },
      { ...base, progressCounters: { rows: 0.5 } },
    ]
    for (const stepRecord of malformed) {
      await expect(
        loadOperationRecord(replaceStep(operation, stepRecord), digest),
      ).rejects.toThrow()
    }
  })

  it("rejects contradictory attempt, result, error, and reconciliation state", async () => {
    const pendingOperation = createOperationRecord(await plan())
    const pending = pendingOperation.steps.one as NonNullable<
      (typeof pendingOperation.steps)[string]
    >
    const runningOperation = await started()
    const running = runningOperation.steps.one as NonNullable<
      (typeof runningOperation.steps)[string]
    >
    const unknownOperation = markRunningStepsUnknownAfterCrash(runningOperation)
    const unknown = unknownOperation.steps.one as NonNullable<
      (typeof unknownOperation.steps)[string]
    >
    const succeeded = recordStepSuccess(runningOperation, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "postcondition",
      resultChecksum: "result",
      stepId: "one",
    }).steps.one as NonNullable<(typeof runningOperation.steps)[string]>
    const malformed = [
      { operation: pendingOperation, step: { ...pending, startedAttempts: 1 } },
      { operation: runningOperation, step: { ...running, startedAttempts: 0 } },
      {
        operation: pendingOperation,
        step: { ...pending, fencingToken: 1, lastAttemptId: "attempt" },
      },
      { operation: unknownOperation, step: { ...unknown, fencingToken: undefined } },
      { operation: unknownOperation, step: { ...unknown, lastAttemptId: undefined } },
      { operation: runningOperation, step: { ...running, activeAttemptId: undefined } },
      { operation: unknownOperation, step: { ...unknown, activeAttemptId: "attempt-1" } },
      { operation: runningOperation, step: { ...running, activeAttemptId: "other" } },
      { operation: runningOperation, step: { ...running, resultChecksum: "result" } },
      { operation: runningOperation, step: { ...succeeded, resultChecksum: undefined } },
      {
        operation: unknownOperation,
        step: { ...unknown, reconciliationEvidenceChecksum: "evidence" },
      },
      { operation: runningOperation, step: { ...running, errorChecksum: "error" } },
      {
        operation: runningOperation,
        step: { ...running, state: "retryable_failed", activeAttemptId: undefined },
      },
      {
        operation: runningOperation,
        step: { ...running, state: "intervention_required", activeAttemptId: undefined },
      },
      { operation: pendingOperation, step: { ...pending, costCounters: { calls: 1 } } },
      {
        operation: pendingOperation,
        step: {
          ...pending,
          reconciliationEvidenceChecksum: "decision",
          state: "not_required",
        },
      },
    ]
    for (const item of malformed) {
      await expect(
        loadOperationRecord(replaceStep(item.operation, item.step), digest),
      ).rejects.toThrow()
    }
  })

  it("rejects retryable persisted state under a sealed never-retry policy", async () => {
    const failed = recordStepFailure(
      await started({ effectProtocol: "saga_receipt", retryClassification: "never" }),
      {
        attemptId: "attempt-1",
        errorChecksum: "confirmed-non-application",
        outcome: "definitely_not_applied",
        stepId: "one",
      },
    )
    expect(failed.steps.one?.state).toBe("failed")

    await expect(
      loadOperationRecord(
        replaceStep(failed, { ...failed.steps.one, state: "retryable_failed" }),
        digest,
      ),
    ).rejects.toThrow(/never-retry/u)
  })

  it("rejects authorization on reversible steps and missing authorization after irreversible start", async () => {
    const reversible = createOperationRecord(await plan())
    await expect(
      loadOperationRecord(
        replaceStep(reversible, {
          ...reversible.steps.one,
          authorizationChecksum: "unexpected",
        }),
        digest,
      ),
    ).rejects.toThrow(/reversible step/u)

    const irreversible = createOperationRecord(await plan({ checkpoint: "irreversible" }))
    await expect(
      loadOperationRecord(
        replaceStep(irreversible, {
          ...irreversible.steps.one,
          authorizationChecksum: "premature",
        }),
        digest,
      ),
    ).rejects.toThrow(/authorization state/u)
    await expect(
      loadOperationRecord(
        replaceStep(irreversible, {
          ...irreversible.steps.one,
          fencingToken: 1,
          lastAttemptId: "attempt-1",
          startedAttempts: 1,
          state: "unknown",
        }),
        digest,
      ),
    ).rejects.toThrow(/authorization state/u)
  })
})

describe("persisted audit event integrity", () => {
  it("loads standalone verified audit heads with null and non-null chain fields", async () => {
    const first = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "actor",
        environmentId: "production",
        eventType: "operation.created",
        fencingToken: null,
        idempotencyKey: "audit-one",
        operationId: "operation-1",
        payloadChecksum: "payload-one",
        serverTimeMs: 100,
        stepId: null,
      },
      digest,
    )
    const second = await appendAuditEvent(
      first,
      {
        actorChecksum: "actor",
        environmentId: "production",
        eventType: "step.started",
        fencingToken: 1,
        idempotencyKey: "audit-two",
        operationId: "operation-1",
        payloadChecksum: "payload-two",
        serverTimeMs: 101,
        stepId: "one",
      },
      digest,
    )
    for (const event of [first, second]) {
      const loaded = await loadAuditEvent(JSON.parse(JSON.stringify(event)) as unknown, digest)
      expect(loaded).toEqual(event)
      expect(Object.isFrozen(loaded)).toBe(true)
    }
  })

  it("rejects malformed, incomplete, unsupported, and checksum-invalid audit heads", async () => {
    const valid = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "actor",
        environmentId: "production",
        eventType: "operation.created",
        fencingToken: null,
        idempotencyKey: "audit-one",
        operationId: "operation-1",
        payloadChecksum: "payload",
        serverTimeMs: 100,
        stepId: null,
      },
      digest,
    )
    for (const malformed of [
      null,
      [],
      new Date(),
      { ...valid, unknown: true },
      { ...valid, schemaVersion: 2 },
      { ...valid, sequence: 0 },
      { ...valid, serverTimeMs: -1 },
      { ...valid, actorChecksum: undefined },
      { ...valid, previousHash: undefined },
      { ...valid, stepId: undefined },
      { ...valid, fencingToken: 0 },
      { ...valid, eventHash: "tampered" },
    ]) {
      await expect(loadAuditEvent(malformed, digest)).rejects.toThrow()
    }
  })
})
