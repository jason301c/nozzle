import fc from "fast-check"
import { describe, expect, it } from "vitest"
import {
  type AuditEvent,
  appendAuditEvent,
  assertLeaseAuthorized,
  assertResumeCompatible,
  beginOperationStep,
  createOperationRecord,
  type DigestFunction,
  decideLeaseAcquisition,
  decideLeaseRelease,
  decideLeaseRenewal,
  encodeOperationPlanChecksumInput,
  type FencedLeaseRecord,
  leaseProof,
  loadIrreversibleAuthorization,
  loadOperationPlan,
  markOperationStepNotRequired,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepsUnknownAfterCrash,
  markRunningStepUnknownAfterCrash,
  type OperationPlan,
  type OperationPlanInput,
  type OperationRecord,
  type OperationStepPlanInput,
  operationStatus,
  recordSagaStepTerminalClassification,
  recordStepFailure,
  recordStepReconciliation,
  recordStepSuccess,
  sealIrreversibleAuthorization,
  sealOperationPlan,
  verifyAuditChain,
  verifyIrreversibleAuthorizationChecksum,
} from "../src/operation.js"

const digest: DigestFunction = async (input) => {
  const bytes = new Uint8Array(input.byteLength)
  bytes.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function step(
  stepId: string,
  overrides: Partial<OperationStepPlanInput> = {},
): OperationStepPlanInput {
  return {
    checkpoint: "reversible",
    dependsOn: [],
    idempotencyKey: `idempotency-${stepId}`,
    inputChecksum: `input-${stepId}`,
    leaseKey: "fleet:example",
    postconditionChecksum: `post-${stepId}`,
    preconditionChecksum: `pre-${stepId}`,
    recoveryInstructions: `Inspect ${stepId} and resume the same operation.`,
    retryClassification: "idempotent",
    stepId,
    ...overrides,
  }
}

function planInput(steps: readonly OperationStepPlanInput[] = [step("one")]): OperationPlanInput {
  return {
    capabilitySnapshotChecksum: "capabilities-v1",
    idempotencyKey: "operation-key",
    inputChecksum: "operation-input",
    operationId: "operation-1",
    operationType: "test-operation",
    steps,
  }
}

async function plan(steps?: readonly OperationStepPlanInput[]): Promise<OperationPlan> {
  return sealOperationPlan(planInput(steps), digest)
}

function acquire(
  current?: FencedLeaseRecord,
  input: {
    acquisitionId?: string
    holderId?: string
    leaseKey?: string
    serverTimeMs?: number
    ttlMs?: number
  } = {},
): FencedLeaseRecord {
  const decision = decideLeaseAcquisition(current, {
    acquisitionId: input.acquisitionId ?? "acquisition-1",
    holderId: input.holderId ?? "controller-1",
    leaseKey: input.leaseKey ?? "fleet:example",
    serverTimeMs: input.serverTimeMs ?? 100,
    ttlMs: input.ttlMs ?? 50,
  })
  if (!decision.acquired) throw new Error("expected lease acquisition")
  return decision.record
}

function begin(
  operation: OperationRecord,
  lease: FencedLeaseRecord,
  input: {
    attemptId?: string
    authorization?: Awaited<ReturnType<typeof sealIrreversibleAuthorization>>
    idempotencyKey?: string
    precondition?: string
    serverTimeMs?: number
    stepId?: string
  } = {},
) {
  const stepId = input.stepId ?? "one"
  return beginOperationStep(operation, {
    attemptId: input.attemptId ?? "attempt-1",
    idempotencyKey: input.idempotencyKey ?? `idempotency-${stepId}`,
    ...(input.authorization ? { irreversibleAuthorization: input.authorization } : {}),
    lease,
    leaseProof: leaseProof(lease),
    observedPreconditionChecksum: input.precondition ?? `pre-${stepId}`,
    serverTimeMs: input.serverTimeMs ?? 110,
    stepId,
  })
}

describe("immutable operation plans", () => {
  it("canonicalizes membership and hashes every immutable identity input", async () => {
    const left = {
      ...planInput([step("b", { dependsOn: ["a"] }), step("a")]),
      operationType: "test-🚀",
    }
    const right = {
      ...planInput([step("a"), step("b", { dependsOn: ["a"] })]),
      operationType: "test-🚀",
    }
    const sealedLeft = await sealOperationPlan(left, digest)
    const sealedRight = await sealOperationPlan(right, digest)

    expect(sealedLeft).toEqual(sealedRight)
    expect([...encodeOperationPlanChecksumInput(left)]).toEqual([
      ...encodeOperationPlanChecksumInput(right),
    ])
    expect(sealedLeft.steps.map((entry) => entry.stepId)).toEqual(["a", "b"])
    expect(Object.isFrozen(sealedLeft)).toBe(true)
    expect(Object.isFrozen(sealedLeft.steps)).toBe(true)
    expect(Object.isFrozen(sealedLeft.steps[0]?.dependsOn)).toBe(true)

    const changed = await sealOperationPlan(
      { ...right, capabilitySnapshotChecksum: "different-capabilities" },
      digest,
    )
    expect(changed.planChecksum).not.toBe(sealedLeft.planChecksum)

    const noDependencies = { ...step("standalone") }
    delete noDependencies.dependsOn
    await expect(sealOperationPlan(planInput([noDependencies]), digest)).resolves.toMatchObject({
      steps: [
        {
          activation: "required",
          completionRole: "work",
          dependsOn: [],
          effectProtocol: "opaque",
        },
      ],
    })
  })

  it("allows only an exact operation ID, idempotency key, and plan checksum to resume", async () => {
    const existing = await plan()
    expect(() => assertResumeCompatible(existing, existing)).not.toThrow()
    await expect(
      sealOperationPlan({ ...planInput(), operationId: "other" }, digest).then((requested) =>
        assertResumeCompatible(existing, requested),
      ),
    ).rejects.toThrow("operation ID")
    await expect(
      sealOperationPlan({ ...planInput(), idempotencyKey: "other" }, digest).then((requested) =>
        assertResumeCompatible(existing, requested),
      ),
    ).rejects.toThrow("different idempotency key")
    await expect(
      sealOperationPlan({ ...planInput(), inputChecksum: "other" }, digest).then((requested) =>
        assertResumeCompatible(existing, requested),
      ),
    ).rejects.toThrow("incompatible immutable plan")
  })

  it.each([
    planInput([]),
    { ...planInput(), operationId: "" },
    { ...planInput(), operationType: "\ud800" },
    { ...planInput(), operationType: "\udc00" },
    { ...planInput(), idempotencyKey: "" },
    { ...planInput(), inputChecksum: "" },
    { ...planInput(), capabilitySnapshotChecksum: "" },
    planInput([step("a"), step("a")]),
    planInput([step("a"), step("b", { idempotencyKey: "idempotency-a" })]),
    planInput([step("a", { dependsOn: ["missing"] })]),
    planInput([step("a", { dependsOn: ["a"] })]),
    planInput([step("a", { dependsOn: ["b"] }), step("b", { dependsOn: ["a"] })]),
    planInput([step("a", { dependsOn: ["b", "b"] }), step("b")]),
    planInput([step("a", { checkpoint: "bad" as "reversible" })]),
    planInput([step("a", { retryClassification: "bad" as "never" })]),
    planInput([step("a", { effectProtocol: "bad" as "opaque" })]),
    planInput([step("a", { activation: "bad" as "required" })]),
    planInput([step("a", { completionRole: "bad" as "work" })]),
    planInput([step("a", { activation: "conditional" })]),
    planInput([
      step("a", { completionRole: "settlement" }),
      step("b", { completionRole: "settlement" }),
    ]),
    planInput([
      step("a"),
      step("settle", { activation: "conditional", completionRole: "settlement" }),
    ]),
    planInput([step("", {})]),
    planInput([step("a", { recoveryInstructions: "" })]),
  ])("rejects malformed or ambiguous plan input", async (input) => {
    await expect(sealOperationPlan(input, digest)).rejects.toThrowError(
      expect.objectContaining({ code: "ConfigurationError" }),
    )
  })

  it("rejects an empty digest result", async () => {
    await expect(sealOperationPlan(planInput(), () => "")).rejects.toThrow(
      "Operation plan checksum",
    )
  })

  it("requires persisted plans to be integrity-verified before execution", async () => {
    const sealed = await plan()
    const persisted = { ...sealed, steps: sealed.steps.map((entry) => ({ ...entry })) }
    expect(() => createOperationRecord(persisted)).toThrow("integrity-verified")

    const loaded = await loadOperationPlan(persisted, digest)
    expect(loaded).toEqual(sealed)
    expect(loaded).not.toBe(persisted)
    expect(() => createOperationRecord(loaded)).not.toThrow()

    await expect(
      loadOperationPlan({ ...persisted, inputChecksum: "tampered" }, digest),
    ).rejects.toThrow("checksum does not match")
    await expect(
      loadOperationPlan({ ...persisted, schemaVersion: 2 } as unknown as OperationPlan, digest),
    ).rejects.toThrow("version is unsupported")
  })
})

describe("D1-server-time fenced lease decisions", () => {
  it("acquires, exactly replays, conflicts, expires, and advances the fence", () => {
    const first = decideLeaseAcquisition(undefined, {
      acquisitionId: "a1",
      holderId: "h1",
      leaseKey: "fleet:example",
      serverTimeMs: 100,
      ttlMs: 20,
    })
    expect(first).toMatchObject({
      acquired: true,
      condition: { kind: "insert_if_absent" },
      record: { fencingToken: 1, expiresAtServerTimeMs: 120 },
      replayed: false,
    })
    if (!first.acquired) throw new Error("expected acquired")

    expect(
      decideLeaseAcquisition(first.record, {
        acquisitionId: "a1",
        holderId: "h1",
        leaseKey: "fleet:example",
        serverTimeMs: 110,
        ttlMs: 200,
      }),
    ).toEqual({ acquired: true, condition: null, record: first.record, replayed: true })

    expect(
      decideLeaseAcquisition(first.record, {
        acquisitionId: "a2",
        holderId: "h2",
        leaseKey: "fleet:example",
        serverTimeMs: 119,
        ttlMs: 20,
      }),
    ).toEqual({
      acquired: false,
      currentFencingToken: 1,
      reason: "held",
      retryAtServerTimeMs: 120,
    })

    const second = decideLeaseAcquisition(first.record, {
      acquisitionId: "a2",
      holderId: "h2",
      leaseKey: "fleet:example",
      serverTimeMs: 120,
      ttlMs: 20,
    })
    expect(second).toMatchObject({
      acquired: true,
      condition: {
        fencingToken: 1,
        kind: "replace_exact",
        serverTimeRequirement: "expired_or_released",
      },
      record: { fencingToken: 2 },
    })
  })

  it("renews only the live exact fence without shortening it", () => {
    const current = acquire()
    const proof = leaseProof(current)
    expect(decideLeaseRenewal(undefined, { proof, serverTimeMs: 110, ttlMs: 10 })).toEqual({
      reason: "fenced",
      renewed: false,
    })
    expect(decideLeaseRenewal(current, { proof, serverTimeMs: 110, ttlMs: 10 })).toMatchObject({
      renewed: true,
      replayed: true,
      condition: null,
    })
    expect(decideLeaseRenewal(current, { proof, serverTimeMs: 120, ttlMs: 100 })).toMatchObject({
      renewed: true,
      replayed: false,
      condition: { serverTimeRequirement: "unexpired" },
      record: { expiresAtServerTimeMs: 220, fencingToken: current.fencingToken },
    })
    expect(
      decideLeaseRenewal(current, {
        proof: { ...proof, fencingToken: proof.fencingToken + 1 },
        serverTimeMs: 110,
        ttlMs: 10,
      }),
    ).toEqual({ reason: "fenced", renewed: false })
    expect(decideLeaseRenewal(current, { proof, serverTimeMs: 150, ttlMs: 10 })).toEqual({
      reason: "expired",
      renewed: false,
    })
  })

  it("releases without resetting the token and forces the next owner to advance it", () => {
    const current = acquire()
    expect(
      decideLeaseRelease(undefined, { proof: leaseProof(current), serverTimeMs: 115 }),
    ).toEqual({ reason: "fenced", released: false })
    const released = decideLeaseRelease(current, {
      proof: leaseProof(current),
      serverTimeMs: 115,
    })
    expect(released).toMatchObject({
      released: true,
      condition: { serverTimeRequirement: "none" },
      record: { fencingToken: 1, holderId: null, acquisitionId: null },
    })
    if (!released.released) throw new Error("expected release")
    expect(() => leaseProof(released.record)).toThrow("released lease")

    const next = decideLeaseAcquisition(released.record, {
      acquisitionId: "a2",
      holderId: "h2",
      leaseKey: "fleet:example",
      serverTimeMs: 116,
      ttlMs: 10,
    })
    expect(next).toMatchObject({ acquired: true, record: { fencingToken: 2 } })
    expect(
      decideLeaseRelease(current, {
        proof: { ...leaseProof(current), fencingToken: 9 },
        serverTimeMs: 115,
      }),
    ).toEqual({ reason: "fenced", released: false })
  })

  it("authorizes protected work only under the exact unexpired token", () => {
    const first = acquire()
    const proof = leaseProof(first)
    expect(() => assertLeaseAuthorized(undefined, proof, 100)).toThrow("fenced")
    expect(() => assertLeaseAuthorized(first, proof, 149)).not.toThrow()
    expect(() => assertLeaseAuthorized(first, proof, 150)).toThrow("expired")
    const second = acquire(first, {
      acquisitionId: "a2",
      holderId: "h2",
      serverTimeMs: 150,
    })
    expect(() => assertLeaseAuthorized(second, proof, 151)).toThrow("fenced")
  })

  it.each([
    () =>
      decideLeaseAcquisition(undefined, {
        acquisitionId: "",
        holderId: "h",
        leaseKey: "k",
        serverTimeMs: 0,
        ttlMs: 1,
      }),
    () =>
      decideLeaseAcquisition(undefined, {
        acquisitionId: "a",
        holderId: "h",
        leaseKey: "k",
        serverTimeMs: -1,
        ttlMs: 1,
      }),
    () =>
      decideLeaseAcquisition(undefined, {
        acquisitionId: "a",
        holderId: "h",
        leaseKey: "k",
        serverTimeMs: Number.MAX_SAFE_INTEGER,
        ttlMs: 1,
      }),
    () =>
      decideLeaseAcquisition(
        {
          acquisitionId: "a",
          expiresAtServerTimeMs: 1,
          fencingToken: 1,
          holderId: null,
          leaseKey: "k",
        },
        { acquisitionId: "b", holderId: "h", leaseKey: "k", serverTimeMs: 2, ttlMs: 1 },
      ),
    () =>
      decideLeaseAcquisition(acquire(), {
        acquisitionId: "b",
        holderId: "h",
        leaseKey: "different",
        serverTimeMs: 200,
        ttlMs: 1,
      }),
  ])("rejects invalid lease data", (run) => {
    expect(run).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
  })

  it("never decreases or reuses a fencing token across arbitrary ownership changes", () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: 1, max: 10_000 }), { minLength: 1 }), (ttls) => {
        let current: FencedLeaseRecord | undefined
        let now = 0
        let previousToken = 0
        for (const [index, ttlMs] of ttls.entries()) {
          const next = acquire(current, {
            acquisitionId: `a-${index}`,
            holderId: `h-${index}`,
            serverTimeMs: now,
            ttlMs,
          })
          expect(next.fencingToken).toBe(previousToken + 1)
          previousToken = next.fencingToken
          current = next
          now = next.expiresAtServerTimeMs
        }
      }),
      { numRuns: 200 },
    )
  })
})

describe("sealed irreversible authorization", () => {
  it("binds authorization to the plan, step, decision, and current lease fence", async () => {
    const sealedPlan = await plan([step("delete", { checkpoint: "irreversible" })])
    const operation = createOperationRecord(sealedPlan)
    const lease = acquire()
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor-hash",
        authorizationId: "authorization-1",
        decisionChecksum: "explicit-recovery-decision",
        lease,
        leaseProof: leaseProof(lease),
        sealedAtServerTimeMs: 105,
        stepId: "delete",
      },
      digest,
    )
    expect(Object.isFrozen(authorization)).toBe(true)
    expect(await verifyIrreversibleAuthorizationChecksum(authorization, digest)).toBe(true)
    expect(
      await verifyIrreversibleAuthorizationChecksum(
        { ...authorization, decisionChecksum: "tampered" },
        digest,
      ),
    ).toBe(false)

    const authorizedStart = begin(operation, lease, {
      authorization,
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    })
    expect(authorizedStart.disposition).toBe("execute")
    expect(
      recordStepSuccess(authorizedStart.operation, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-delete",
        resultChecksum: "deleted",
        stepId: "delete",
      }).steps.delete,
    ).toMatchObject({ authorizationChecksum: authorization.authorizationChecksum })
    expect(
      recordStepFailure(authorizedStart.operation, {
        attemptId: "attempt-1",
        errorChecksum: "provider-rejected",
        outcome: "permanent",
        stepId: "delete",
      }).steps.delete,
    ).toMatchObject({ authorizationChecksum: authorization.authorizationChecksum, state: "failed" })
    expect(markRunningStepsUnknownAfterCrash(authorizedStart.operation).steps.delete).toMatchObject(
      {
        authorizationChecksum: authorization.authorizationChecksum,
        state: "unknown",
      },
    )
    const unknown = recordStepFailure(authorizedStart.operation, {
      attemptId: "attempt-1",
      errorChecksum: "lost-response",
      outcome: "unknown",
      stepId: "delete",
    })
    expect(
      recordStepReconciliation(unknown, {
        evidenceChecksum: "still-unknown",
        outcome: "indeterminate",
        stepId: "delete",
      }).steps.delete,
    ).toMatchObject({ authorizationChecksum: authorization.authorizationChecksum })
    expect(() =>
      begin(operation, lease, {
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        stepId: "delete",
      }),
    ).toThrow("no sealed authorization")

    const newerLease = acquire(lease, {
      acquisitionId: "a2",
      holderId: "h2",
      serverTimeMs: 150,
    })
    expect(() =>
      begin(operation, newerLease, {
        authorization,
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        serverTimeMs: 151,
        stepId: "delete",
      }),
    ).toThrow("different lease fence")
    expect(() =>
      begin(operation, lease, {
        authorization: { ...authorization, planChecksum: "different-plan" },
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        stepId: "delete",
      }),
    ).toThrow("different immutable plan")
    expect(() =>
      begin(operation, lease, {
        authorization: { ...authorization, sealedAtServerTimeMs: 120 },
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        serverTimeMs: 110,
        stepId: "delete",
      }),
    ).toThrow("future server timestamp")
    const persistedAuthorization = { ...authorization }
    expect(() =>
      begin(operation, lease, {
        authorization: persistedAuthorization,
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        stepId: "delete",
      }),
    ).toThrow("integrity-verified")
    const loadedAuthorization = await loadIrreversibleAuthorization(persistedAuthorization, digest)
    expect(
      begin(operation, lease, {
        authorization: loadedAuthorization,
        idempotencyKey: "idempotency-delete",
        precondition: "pre-delete",
        stepId: "delete",
      }).disposition,
    ).toBe("execute")
    await expect(
      loadIrreversibleAuthorization(
        { ...persistedAuthorization, decisionChecksum: "tampered" },
        digest,
      ),
    ).rejects.toThrow("checksum does not match")
    await expect(
      loadIrreversibleAuthorization(
        { ...persistedAuthorization, schemaVersion: 2 } as unknown as typeof authorization,
        digest,
      ),
    ).rejects.toThrow("version is unsupported")
    const reversibleOperation = createOperationRecord(await plan())
    expect(() => begin(reversibleOperation, lease, { authorization })).toThrow("reversible step")
  })

  it("rejects authorization for reversible, absent, and mismatched steps", async () => {
    const sealedPlan = await plan()
    const lease = acquire()
    await expect(
      sealIrreversibleAuthorization(
        sealedPlan,
        {
          actorChecksum: "actor",
          authorizationId: "auth",
          decisionChecksum: "decision",
          lease,
          leaseProof: leaseProof(lease),
          sealedAtServerTimeMs: 101,
          stepId: "one",
        },
        digest,
      ),
    ).rejects.toThrow("only seal an irreversible step")
    await expect(
      sealIrreversibleAuthorization(
        sealedPlan,
        {
          actorChecksum: "actor",
          authorizationId: "auth",
          decisionChecksum: "decision",
          lease,
          leaseProof: leaseProof(lease),
          sealedAtServerTimeMs: 101,
          stepId: "missing",
        },
        digest,
      ),
    ).rejects.toThrow("not part")
    const mismatchedPlan = await plan([
      step("delete", { checkpoint: "irreversible", leaseKey: "different-lease" }),
    ])
    await expect(
      sealIrreversibleAuthorization(
        mismatchedPlan,
        {
          actorChecksum: "actor",
          authorizationId: "auth",
          decisionChecksum: "decision",
          lease,
          leaseProof: leaseProof(lease),
          sealedAtServerTimeMs: 101,
          stepId: "delete",
        },
        digest,
      ),
    ).rejects.toThrow("lease keys do not match")
  })
})

describe("operation crash, resume, and idempotency guards", () => {
  it("defers top-level terminal status to an explicit required settlement step", async () => {
    const activeLease = acquire()
    const operation = createOperationRecord(
      await plan([step("work"), step("settle", { completionRole: "settlement" })]),
    )
    expect(operationStatus(operation)).toBe("planned")
    const running = begin(operation, activeLease, {
      idempotencyKey: "idempotency-work",
      precondition: "pre-work",
      stepId: "work",
    }).operation
    expect(operationStatus(running)).toBe("running")
    const failedWork = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "classified-failure",
      outcome: "permanent",
      stepId: "work",
    })
    expect(failedWork.steps.work?.state).toBe("failed")
    expect(operationStatus(failedWork)).toBe("paused")
    const settling = begin(failedWork, activeLease, {
      idempotencyKey: "idempotency-settle",
      precondition: "pre-settle",
      stepId: "settle",
    }).operation
    const settledSuccess = recordStepSuccess(settling, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-settle",
      resultChecksum: "terminal-saga-result",
      stepId: "settle",
    })
    expect(operationStatus(settledSuccess)).toBe("succeeded")
    const settledFailure = recordStepFailure(settling, {
      attemptId: "attempt-1",
      errorChecksum: "terminal-saga-failure",
      outcome: "permanent",
      stepId: "settle",
    })
    expect(operationStatus(settledFailure)).toBe("failed")
    const unknownSettlement = recordStepFailure(settling, {
      attemptId: "attempt-1",
      errorChecksum: "terminal-saga-unknown",
      outcome: "unknown",
      stepId: "settle",
    })
    expect(operationStatus(unknownSettlement)).toBe("reconciling")
    const intervention = recordStepReconciliation(unknownSettlement, {
      evidenceChecksum: "terminal-saga-indeterminate",
      outcome: "indeterminate",
      stepId: "settle",
    })
    expect(operationStatus(intervention)).toBe("intervention_required")
    const unknownWork = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "work-unknown",
      outcome: "unknown",
      stepId: "work",
    })
    expect(operationStatus(unknownWork)).toBe("reconciling")
  })

  it("settles unused conditional steps with durable evidence instead of fake success", async () => {
    const activeLease = acquire()
    const operation = createOperationRecord(
      await plan([step("required"), step("unused", { activation: "conditional" })]),
    )
    expect(operationStatus(operation)).toBe("planned")
    const notRequired = markOperationStepNotRequired(operation, {
      evidenceChecksum: "sealed-branch-decision",
      stepId: "unused",
    })
    expect(notRequired.steps.unused).toEqual({
      costCounters: {},
      progressCounters: {},
      reconciliationEvidenceChecksum: "sealed-branch-decision",
      startedAttempts: 0,
      state: "not_required",
    })
    expect(
      begin(notRequired, activeLease, {
        idempotencyKey: "idempotency-unused",
        precondition: "pre-unused",
        stepId: "unused",
      }).disposition,
    ).toBe("blocked")
    expect(
      markOperationStepNotRequired(notRequired, {
        evidenceChecksum: "sealed-branch-decision",
        stepId: "unused",
      }),
    ).toBe(notRequired)
    expect(() =>
      markOperationStepNotRequired(notRequired, {
        evidenceChecksum: "contradictory-decision",
        stepId: "unused",
      }),
    ).toThrow(/contradicts durable evidence/u)
    expect(() =>
      markOperationStepNotRequired(operation, {
        evidenceChecksum: "decision",
        stepId: "required",
      }),
    ).toThrow(/conditional operation step/u)
    expect(() =>
      markOperationStepNotRequired(operation, { evidenceChecksum: "", stepId: "unused" }),
    ).toThrow(/evidence checksum/u)

    const requiredRunning = begin(notRequired, activeLease, {
      idempotencyKey: "idempotency-required",
      precondition: "pre-required",
      stepId: "required",
    }).operation
    const complete = recordStepSuccess(requiredRunning, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-required",
      resultChecksum: "required-result",
      stepId: "required",
    })
    expect(operationStatus(complete)).toBe("succeeded")
    expect(() =>
      markOperationStepNotRequired(requiredRunning, {
        evidenceChecksum: "too-late",
        stepId: "required",
      }),
    ).toThrow(/conditional operation step/u)
    expect(() =>
      markOperationStepNotRequired(
        begin(operation, activeLease, {
          idempotencyKey: "idempotency-unused",
          precondition: "pre-unused",
          stepId: "unused",
        }).operation,
        { evidenceChecksum: "too-late", stepId: "unused" },
      ),
    ).toThrow(/unattempted pending/u)
  })

  it("executes dependencies in order and exactly replays a completed logical result", async () => {
    const operation = createOperationRecord(
      await plan([step("first"), step("second", { dependsOn: ["first"] })]),
    )
    const lease = acquire()
    expect(operationStatus(operation)).toBe("planned")
    expect(() => begin(operation, lease, { stepId: "second" })).toThrow(
      "dependency has not succeeded",
    )

    const started = begin(operation, lease, { stepId: "first" })
    expect(started.disposition).toBe("execute")
    expect(operationStatus(started.operation)).toBe("running")
    expect(begin(started.operation, lease, { stepId: "first" }).disposition).toBe("in_progress")
    expect(
      begin(started.operation, lease, { attemptId: "other", stepId: "first" }).disposition,
    ).toBe("reconcile")

    const completedFirst = recordStepSuccess(started.operation, {
      attemptId: "attempt-1",
      counters: { cost: { rowsWritten: 2 }, progress: { rows: 2 } },
      observedPostconditionChecksum: "post-first",
      resultChecksum: "result-first",
      stepId: "first",
    })
    expect(begin(completedFirst, lease, { stepId: "first" })).toMatchObject({
      disposition: "replay",
      resultChecksum: "result-first",
    })
    expect(
      recordStepSuccess(completedFirst, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-first",
        resultChecksum: "result-first",
        stepId: "first",
      }),
    ).toBe(completedFirst)
    expect(completedFirst.steps.first).toMatchObject({
      costCounters: { rowsWritten: 2 },
      progressCounters: { rows: 2 },
      startedAttempts: 1,
      state: "succeeded",
    })

    const startedSecond = begin(completedFirst, lease, { stepId: "second" }).operation
    const completed = recordStepSuccess(startedSecond, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-second",
      resultChecksum: "result-second",
      stepId: "second",
    })
    expect(operationStatus(completed)).toBe("succeeded")
  })

  it("never turns a lost provider response into failure or an automatic replay", async () => {
    const lease = acquire()
    const started = begin(createOperationRecord(await plan()), lease).operation
    const unknown = recordStepFailure(started, {
      attemptId: "attempt-1",
      errorChecksum: "lost-response",
      outcome: "unknown",
      stepId: "one",
    })
    expect(operationStatus(unknown)).toBe("reconciling")
    expect(begin(unknown, lease).disposition).toBe("reconcile")

    const applied = recordStepReconciliation(unknown, {
      evidenceChecksum: "provider-observation",
      observedPostconditionChecksum: "post-one",
      outcome: "applied",
      resultChecksum: "provider-result",
      stepId: "one",
    })
    expect(applied.steps.one).toMatchObject({
      reconciliationEvidenceChecksum: "provider-observation",
      resultChecksum: "provider-result",
      state: "succeeded",
    })
  })

  it("distinguishes retryable saga absence from terminal classification success", async () => {
    const lease = acquire()
    const started = begin(
      createOperationRecord(
        await plan([
          step("one", {
            effectProtocol: "saga_receipt",
            retryClassification: "reconcile_first",
          }),
        ]),
      ),
      lease,
    ).operation
    const unknown = recordStepFailure(started, {
      attemptId: "attempt-1",
      errorChecksum: "unknown-effect",
      outcome: "unknown",
      stepId: "one",
    })

    const applied = recordStepReconciliation(unknown, {
      evidenceChecksum: "applied-observation",
      observedPostconditionChecksum: "post-one",
      outcome: "applied",
      resultChecksum: "business-result",
      stepId: "one",
    })
    expect(applied.steps.one).toMatchObject({
      reconciliationEvidenceChecksum: "applied-observation",
      resultChecksum: "business-result",
      state: "succeeded",
    })

    const retryable = recordStepReconciliation(unknown, {
      evidenceChecksum: "not-applied-observation",
      outcome: "not_applied",
      stepId: "one",
    })
    expect(retryable.steps.one).toMatchObject({
      reconciliationEvidenceChecksum: "not-applied-observation",
      state: "retryable_failed",
    })

    const terminal = recordSagaStepTerminalClassification(unknown, {
      counters: { cost: { observations: 1 }, progress: { classifications: 1 } },
      outcome: "not_applied",
      receiptOutcomeChecksum: "terminal-receipt-outcome",
      stepId: "one",
    })
    expect(terminal.steps.one).toMatchObject({
      costCounters: { observations: 1 },
      errorChecksum: "unknown-effect",
      progressCounters: { classifications: 1 },
      reconciliationEvidenceChecksum: "terminal-receipt-outcome",
      resultChecksum: "terminal-receipt-outcome",
      state: "succeeded",
    })
    expect(operationStatus(terminal)).toBe("succeeded")

    const indeterminate = recordStepReconciliation(unknown, {
      evidenceChecksum: "indeterminate-observation",
      outcome: "indeterminate",
      stepId: "one",
    })
    expect(indeterminate.steps.one).toMatchObject({
      reconciliationEvidenceChecksum: "indeterminate-observation",
      state: "intervention_required",
    })
  })

  it("marks every in-flight attempt unknown on crash before resuming", async () => {
    const lease = acquire()
    const operation = createOperationRecord(await plan([step("a"), step("b"), step("c")]))
    const runningA = begin(operation, lease, { stepId: "a" }).operation
    expect(
      markRunningStepUnknownAfterCrash(runningA, "a", "accepted-receipt").steps.a,
    ).toMatchObject({
      errorChecksum: "accepted-receipt",
      state: "unknown",
    })
    expect(() => markRunningStepUnknownAfterCrash(runningA, "a", "")).toThrow(
      "Crash-recovery error checksum",
    )
    const notDispatched = markRunningStepNotDispatchedAfterCrash(
      runningA,
      "a",
      "provider-receipt-absent",
    )
    expect(notDispatched.steps.a?.state).toBe("retryable_failed")
    expect(
      begin(notDispatched, lease, { attemptId: "provider-attempt-2", stepId: "a" }).disposition,
    ).toBe("execute")
    const runningBoth = begin(runningA, lease, { stepId: "b" }).operation
    const crashed = markRunningStepsUnknownAfterCrash(runningBoth)
    expect(crashed.steps.a?.state).toBe("unknown")
    expect(crashed.steps.b?.state).toBe("unknown")
    expect(operationStatus(crashed)).toBe("reconciling")

    const retry = recordStepReconciliation(crashed, {
      evidenceChecksum: "definitely-absent",
      outcome: "not_applied",
      stepId: "a",
    })
    expect(retry.steps.a?.state).toBe("retryable_failed")
    expect(begin(retry, lease, { attemptId: "attempt-2", stepId: "a" }).disposition).toBe("execute")

    const intervention = recordStepReconciliation(crashed, {
      evidenceChecksum: "still-ambiguous",
      outcome: "indeterminate",
      stepId: "b",
    })
    expect(intervention.steps.b?.state).toBe("intervention_required")
    expect(operationStatus(intervention)).toBe("intervention_required")
    expect(begin(intervention, lease, { stepId: "b" }).disposition).toBe("blocked")
  })

  it("classifies known failures according to the sealed retry policy", async () => {
    const lease = acquire()
    for (const [retryClassification, expected] of [
      ["idempotent", "retryable_failed"],
      ["reconcile_first", "retryable_failed"],
      ["never", "failed"],
    ] as const) {
      const started = begin(
        createOperationRecord(await plan([step("one", { retryClassification })])),
        lease,
      ).operation
      const failed = recordStepFailure(started, {
        attemptId: "attempt-1",
        counters: { cost: { providerCalls: 1 } },
        errorChecksum: "known-rejection",
        outcome: "definitely_not_applied",
        stepId: "one",
      })
      expect(failed.steps.one?.state).toBe(expected)
      expect(operationStatus(failed)).toBe(expected === "failed" ? "failed" : "paused")
      if (retryClassification !== "never") {
        const retried = begin(failed, lease, { attemptId: "attempt-2" }).operation
        const completed = recordStepSuccess(retried, {
          attemptId: "attempt-2",
          counters: { cost: { providerCalls: 1 } },
          observedPostconditionChecksum: "post-one",
          resultChecksum: "result",
          stepId: "one",
        })
        expect(completed.steps.one?.costCounters.providerCalls).toBe(2)
      }
    }
  })

  it("rejects stale result, precondition, postcondition, identity, and counter data", async () => {
    const lease = acquire()
    const operation = createOperationRecord(await plan())
    expect(() => begin(operation, lease, { idempotencyKey: "other" })).toThrow(
      "different idempotency key",
    )
    expect(() => begin(operation, lease, { precondition: "wrong" })).toThrow("precondition")
    expect(() => begin(operation, acquire(undefined, { leaseKey: "other" }))).toThrow(
      "wrong lease key",
    )
    const started = begin(operation, lease).operation
    expect(() =>
      recordStepSuccess(started, {
        attemptId: "wrong-attempt",
        observedPostconditionChecksum: "post-one",
        resultChecksum: "result",
        stepId: "one",
      }),
    ).toThrow("active step attempt")
    expect(() =>
      recordStepSuccess(started, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "wrong",
        resultChecksum: "result",
        stepId: "one",
      }),
    ).toThrow("postcondition")
    expect(() =>
      recordStepSuccess(started, {
        attemptId: "attempt-1",
        counters: { progress: { rows: -1 } },
        observedPostconditionChecksum: "post-one",
        resultChecksum: "result",
        stepId: "one",
      }),
    ).toThrow("Counter values")
    expect(() =>
      recordStepSuccess(started, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-one",
        resultChecksum: "",
        stepId: "one",
      }),
    ).toThrow("result checksum")
    expect(() => begin(operation, lease, { stepId: "missing" })).toThrow("immutable operation plan")

    const completed = recordStepSuccess(started, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-one",
      resultChecksum: "result",
      stepId: "one",
    })
    expect(() =>
      recordStepSuccess(completed, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-one",
        resultChecksum: "contradictory-result",
        stepId: "one",
      }),
    ).toThrow("contradicts")
  })

  it("fails closed on malformed persisted step records and invalid observations", async () => {
    const lease = acquire()
    const operation = createOperationRecord(await plan())
    const missingRecord = { ...operation, steps: {} } as OperationRecord
    expect(() => begin(missingRecord, lease)).toThrow("no persisted operation record")

    const successfulWithoutResult = {
      ...operation,
      steps: {
        one: { ...operation.steps.one, state: "succeeded" },
      },
    } as OperationRecord
    expect(() => begin(successfulWithoutResult, lease)).toThrow("no result checksum")

    const started = begin(operation, lease).operation
    const malformedRunningStep = { ...started.steps.one }
    delete malformedRunningStep.fencingToken
    const malformedRunning = {
      ...started,
      steps: { one: malformedRunningStep },
    } as OperationRecord
    expect(() =>
      recordStepSuccess(malformedRunning, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-one",
        resultChecksum: "result",
        stepId: "one",
      }),
    ).toThrow("incomplete fencing metadata")
    expect(() => markRunningStepsUnknownAfterCrash(malformedRunning)).toThrow(
      "incomplete crash-recovery metadata",
    )
    expect(() => markRunningStepUnknownAfterCrash(operation, "one")).toThrow("Only a running step")

    expect(() =>
      recordStepFailure(started, {
        attemptId: "attempt-1",
        errorChecksum: "error",
        outcome: "invalid" as "unknown",
        stepId: "one",
      }),
    ).toThrow("failure outcome is invalid")
    expect(() =>
      recordStepReconciliation(operation, {
        evidenceChecksum: "evidence",
        outcome: "not_applied",
        stepId: "one",
      }),
    ).toThrow("Only an unknown")

    const unknown = recordStepFailure(started, {
      attemptId: "attempt-1",
      errorChecksum: "unknown",
      outcome: "unknown",
      stepId: "one",
    })
    const malformedUnknownStep = { ...unknown.steps.one }
    delete malformedUnknownStep.lastAttemptId
    const malformedUnknown = {
      ...unknown,
      steps: { one: malformedUnknownStep },
    } as OperationRecord
    expect(() =>
      recordStepReconciliation(malformedUnknown, {
        evidenceChecksum: "evidence",
        outcome: "not_applied",
        stepId: "one",
      }),
    ).toThrow("incomplete reconciliation metadata")
    expect(() =>
      recordStepReconciliation(unknown, {
        evidenceChecksum: "evidence",
        outcome: "invalid" as "not_applied",
        stepId: "one",
      }),
    ).toThrow("reconciliation outcome is invalid")

    expect(() =>
      recordSagaStepTerminalClassification(unknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "one",
      }),
    ).toThrow("requires a saga-receipt step")

    const sagaOperation = createOperationRecord(
      await plan([step("one", { effectProtocol: "saga_receipt" })]),
    )
    expect(() =>
      recordSagaStepTerminalClassification(sagaOperation, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "one",
      }),
    ).toThrow("Only an unknown saga step")
    const sagaUnknown = recordStepFailure(begin(sagaOperation, lease).operation, {
      attemptId: "attempt-1",
      errorChecksum: "unknown",
      outcome: "unknown",
      stepId: "one",
    })
    expect(() =>
      recordSagaStepTerminalClassification(sagaUnknown, {
        outcome: "invalid" as "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "one",
      }),
    ).toThrow("classification outcome is invalid")
    expect(() =>
      recordSagaStepTerminalClassification(sagaUnknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "",
        stepId: "one",
      }),
    ).toThrow("receipt-outcome checksum")
    for (const missing of ["fencingToken", "lastAttemptId"] as const) {
      const malformedStep = { ...sagaUnknown.steps.one }
      delete malformedStep[missing]
      expect(() =>
        recordSagaStepTerminalClassification(
          { ...sagaUnknown, steps: { one: malformedStep } } as OperationRecord,
          {
            outcome: "not_applied",
            receiptOutcomeChecksum: "terminal-receipt",
            stepId: "one",
          },
        ),
      ).toThrow("incomplete classification metadata")
    }
  })

  it("requires full evidence to reconcile applied outcomes and preserves never-retry", async () => {
    const lease = acquire()
    const started = begin(
      createOperationRecord(await plan([step("one", { retryClassification: "never" })])),
      lease,
    ).operation
    const unknown = recordStepFailure(started, {
      attemptId: "attempt-1",
      errorChecksum: "unknown",
      outcome: "unknown",
      stepId: "one",
    })
    expect(() =>
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observation",
        outcome: "applied",
        stepId: "one",
      }),
    ).toThrow("requires result and postcondition")
    expect(() =>
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observation",
        observedPostconditionChecksum: "wrong",
        outcome: "applied",
        resultChecksum: "result",
        stepId: "one",
      }),
    ).toThrow("contradictory")
    const absent = recordStepReconciliation(unknown, {
      evidenceChecksum: "absent",
      outcome: "not_applied",
      stepId: "one",
    })
    expect(absent.steps.one?.state).toBe("failed")
  })
})

describe("append-only hash-chain audit primitives", () => {
  const auditInput = (serverTimeMs: number, eventType = "operation.started") => ({
    actorChecksum: "actor-hash",
    environmentId: "test",
    eventType,
    fencingToken: 1,
    idempotencyKey: "operation-key",
    operationId: "operation-1",
    payloadChecksum: `payload-${eventType}`,
    serverTimeMs,
    stepId: null,
  })

  it("appends deterministic, immutable, sequence-checked events", async () => {
    const first = await appendAuditEvent(undefined, auditInput(100), digest)
    const second = await appendAuditEvent(first, auditInput(100, "step.started"), digest)
    const third = await appendAuditEvent(
      second,
      { ...auditInput(101, "step.finished"), fencingToken: null, stepId: "one" },
      digest,
    )
    expect(first).toMatchObject({ previousHash: null, sequence: 1, schemaVersion: 1 })
    expect(second).toMatchObject({ previousHash: first.eventHash, sequence: 2 })
    expect(Object.isFrozen(second)).toBe(true)
    expect(third).toMatchObject({ fencingToken: null, stepId: "one" })
    expect(await verifyAuditChain([first, second, third], digest)).toBe(true)

    const same = await appendAuditEvent(first, auditInput(100, "step.started"), digest)
    expect(same.eventHash).toBe(second.eventHash)
  })

  it("detects payload, link, sequence, order, and time tampering", async () => {
    const first = await appendAuditEvent(undefined, auditInput(100), digest)
    const second = await appendAuditEvent(first, auditInput(101, "step.started"), digest)
    const cases: readonly (readonly AuditEvent[])[] = [
      [{ ...first, payloadChecksum: "tampered" }, second],
      [first, { ...second, previousHash: "tampered" }],
      [first, { ...second, sequence: 3 }],
      [second, first],
      [first, { ...second, serverTimeMs: 99 }],
    ]
    for (const events of cases) expect(await verifyAuditChain(events, digest)).toBe(false)
    await expect(appendAuditEvent(first, auditInput(99), digest)).rejects.toThrow("cannot decrease")
  })

  it("preserves a valid chain for arbitrary public-safe event sequences", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.string({ minLength: 1 }).filter((value) => value.trim().length > 0),
          {
            maxLength: 30,
          },
        ),
        async (eventTypes) => {
          const events: AuditEvent[] = []
          for (const [index, eventType] of eventTypes.entries()) {
            const previous = events.at(-1)
            events.push(
              await appendAuditEvent(previous, auditInput(100 + index, eventType), digest),
            )
          }
          expect(await verifyAuditChain(events, digest)).toBe(true)
        },
      ),
      { numRuns: 100 },
    )
  })

  it("rejects invalid auditable identity and fencing inputs", async () => {
    await expect(
      appendAuditEvent(undefined, { ...auditInput(0), actorChecksum: "" }, digest),
    ).rejects.toThrow("Audit actor checksum")
    await expect(
      appendAuditEvent(undefined, { ...auditInput(0), fencingToken: 0 }, digest),
    ).rejects.toThrow("Audit fencing token")
  })
})
