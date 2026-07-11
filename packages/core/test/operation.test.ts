import fc from "fast-check"
import { describe, expect, it } from "vitest"
import {
  type AtomicStepOutcome,
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
  encodeAuditEventChecksumInput,
  encodeIrreversibleAuthorizationChecksumInput,
  encodeOperationPlanChecksumInput,
  type FencedLeaseRecord,
  type IrreversibleAuthorization,
  leaseProof,
  loadAuditEvent,
  loadIrreversibleAuthorization,
  loadOperationPlan,
  MAX_IRREVERSIBLE_AUTHORIZATION_BYTES,
  markOperationStepNotRequired,
  markRunningStepNotDispatchedAfterCrash,
  markRunningStepsUnknownAfterCrash,
  markRunningStepUnknownAfterCrash,
  type OperationPlan,
  type OperationPlanInput,
  type OperationRecord,
  type OperationStepPlanInput,
  operationStatus,
  recordAtomicStepOutcome,
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

async function resignAuthorization(
  authorization: IrreversibleAuthorization,
): Promise<IrreversibleAuthorization> {
  const { authorizationChecksum: _authorizationChecksum, ...unsigned } = authorization
  return {
    ...unsigned,
    authorizationChecksum: await digest(encodeIrreversibleAuthorizationChecksumInput(unsigned)),
  }
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

function atomicOutcome(
  operation: OperationRecord,
  activeLease: FencedLeaseRecord,
  outcome: AtomicStepOutcome,
  overrides: {
    attemptId?: string
    idempotencyKey?: string
    observedPreconditionChecksum?: string
    proof?: ReturnType<typeof leaseProof>
    stepId?: string
  } = {},
): OperationRecord {
  const stepId = overrides.stepId ?? "one"
  return recordAtomicStepOutcome(operation, {
    attemptId: overrides.attemptId ?? "atomic-attempt-1",
    idempotencyKey: overrides.idempotencyKey ?? `idempotency-${stepId}`,
    leaseProof: overrides.proof ?? leaseProof(activeLease),
    observedPreconditionChecksum: overrides.observedPreconditionChecksum ?? `pre-${stepId}`,
    outcome,
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
    expect(Object.keys(sealedLeft.steps[0] ?? {})).toEqual([
      "activation",
      "checkpoint",
      "completionRole",
      "dependsOn",
      "effectProtocol",
      "idempotencyKey",
      "inputChecksum",
      "leaseKey",
      "postconditionChecksum",
      "preconditionChecksum",
      "recoveryInstructions",
      "retryClassification",
      "stepId",
    ])
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
    { ...planInput(), steps: "not-an-array" as never },
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
    planInput([step("a", { dependsOn: "not-an-array" as never })]),
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

  it("rejects operation step fields outside the canonical checksum contract", async () => {
    const stepWithExtraField = { ...step("one"), outsideChecksum: "unchecked" }

    await expect(sealOperationPlan(planInput([stepWithExtraField]), digest)).rejects.toThrow(
      /unknown fields/u,
    )
    await expect(
      sealOperationPlan(
        { ...planInput(), outsideChecksum: "unchecked" } as OperationPlanInput,
        digest,
      ),
    ).rejects.toThrow(/input fields/u)
  })

  it("rejects sparse or decorated checksum-bearing plan arrays", async () => {
    const decoratedSteps = [step("one")] as OperationStepPlanInput[] & {
      outsideChecksum?: string
    }
    decoratedSteps.outsideChecksum = "unchecked"
    const sparseSteps = new Array<OperationStepPlanInput>(2)
    sparseSteps[1] = step("one")
    const balancedSparseSteps = new Array<OperationStepPlanInput>(1) as OperationStepPlanInput[] & {
      outsideChecksum?: string
    }
    balancedSparseSteps.outsideChecksum = "unchecked"

    for (const steps of [decoratedSteps, sparseSteps, balancedSparseSteps]) {
      await expect(sealOperationPlan(planInput(steps), digest)).rejects.toMatchObject({
        code: "ConfigurationError",
        message: "Operation plan steps must be an array.",
      })
    }

    const decoratedDependencies = [] as string[] & { outsideChecksum?: string }
    decoratedDependencies.outsideChecksum = "unchecked"
    await expect(
      sealOperationPlan(planInput([step("one", { dependsOn: decoratedDependencies })]), digest),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Step dependencies must be an array.",
    })

    const sealed = await plan()
    const decoratedPersistedSteps = sealed.steps.map((entry) => ({ ...entry })) as Array<
      OperationPlan["steps"][number]
    > & { outsideChecksum?: string }
    decoratedPersistedSteps.outsideChecksum = "unchecked"
    await expect(
      loadOperationPlan({ ...sealed, steps: decoratedPersistedSteps }, digest),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Operation plan steps must be an array.",
    })
  })

  it("captures every plan input once before hashing and normalizes unsafe capture", async () => {
    const switchingStep = { ...step("one") }
    let retryClassificationReads = 0
    Object.defineProperty(switchingStep, "retryClassification", {
      enumerable: true,
      get() {
        retryClassificationReads += 1
        return retryClassificationReads === 1 ? "idempotent" : "invalid"
      },
    })
    const sealed = await sealOperationPlan(planInput([switchingStep]), digest)
    expect(retryClassificationReads).toBe(1)
    expect(sealed.steps[0]?.retryClassification).toBe("idempotent")

    const hostile = new Proxy(planInput(), {})
    await expect(sealOperationPlan(hostile, digest)).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Operation plan input could not be captured safely.",
    })
    expect(() => encodeOperationPlanChecksumInput(hostile)).toThrowError(
      expect.objectContaining({
        code: "ConfigurationError",
        message: "Operation plan input could not be captured safely.",
      }),
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
    await expect(
      loadOperationPlan(
        {
          ...persisted,
          steps: persisted.steps.map((entry) => ({ ...entry, outsideChecksum: "unchecked" })),
        },
        digest,
      ),
    ).rejects.toThrow(/unknown fields/u)

    let checksumReads = 0
    const switchingChecksum = { ...persisted }
    Object.defineProperty(switchingChecksum, "planChecksum", {
      enumerable: true,
      get() {
        checksumReads += 1
        return checksumReads === 1 ? sealed.planChecksum : "attacker-selected-checksum"
      },
    })
    const captured = await loadOperationPlan(switchingChecksum, digest)
    expect(checksumReads).toBe(1)
    expect(captured.planChecksum).toBe(sealed.planChecksum)
    expect(captured).toEqual(sealed)

    await expect(loadOperationPlan(new Proxy(persisted, {}), digest)).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The persisted operation plan could not be captured safely.",
    })
    await expect(
      loadOperationPlan({ ...persisted, outsideChecksum: "unchecked" } as OperationPlan, digest),
    ).rejects.toThrow(/plan fields/u)
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
    expect(Object.isFrozen(loadedAuthorization)).toBe(true)
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

  it("retains the exact trusted authorization through every attempted transition", async () => {
    const sealedPlan = await plan([
      step("delete", {
        checkpoint: "irreversible",
        effectProtocol: "saga_receipt",
        retryClassification: "reconcile_first",
      }),
    ])
    const activeLease = acquire()
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-retained",
        decisionChecksum: "decision",
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        sealedAtServerTimeMs: 105,
        stepId: "delete",
      },
      digest,
    )
    const running = begin(createOperationRecord(sealedPlan), activeLease, {
      authorization,
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    }).operation
    const retryable = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "definitely-absent",
      outcome: "definitely_not_applied",
      stepId: "delete",
    })
    const unknown = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "lost-response",
      outcome: "unknown",
      stepId: "delete",
    })
    const retried = begin(retryable, activeLease, {
      attemptId: "attempt-2",
      authorization,
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    }).operation
    const states = [
      running,
      recordStepSuccess(running, {
        attemptId: "attempt-1",
        observedPostconditionChecksum: "post-delete",
        resultChecksum: "deleted",
        stepId: "delete",
      }),
      retryable,
      retried,
      recordStepFailure(running, {
        attemptId: "attempt-1",
        errorChecksum: "permanent",
        outcome: "permanent",
        stepId: "delete",
      }),
      unknown,
      markRunningStepUnknownAfterCrash(running, "delete", "crash-unknown"),
      markRunningStepNotDispatchedAfterCrash(running, "delete", "dispatch-absent"),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observed-applied",
        observedPostconditionChecksum: "post-delete",
        outcome: "applied",
        resultChecksum: "deleted-after-reconciliation",
        stepId: "delete",
      }),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "observed-absent",
        outcome: "not_applied",
        stepId: "delete",
      }),
      recordStepReconciliation(unknown, {
        evidenceChecksum: "still-indeterminate",
        outcome: "indeterminate",
        stepId: "delete",
      }),
      recordSagaStepTerminalClassification(unknown, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-absence",
        stepId: "delete",
      }),
      recordSagaStepTerminalClassification(retryable, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "definitely-absent",
        stepId: "delete",
      }),
    ]

    for (const state of states) {
      expect(state.steps.delete?.authorizationChecksum).toBe(authorization.authorizationChecksum)
      expect(state.steps.delete?.irreversibleAuthorization).toBe(authorization)
      expect(Object.isFrozen(state.steps.delete?.irreversibleAuthorization)).toBe(true)
    }
  })

  it("fails closed when an attempted step loses or forges its retained authorization", async () => {
    const sealedPlan = await plan([step("delete", { checkpoint: "irreversible" })])
    const activeLease = acquire()
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-retained",
        decisionChecksum: "decision",
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        sealedAtServerTimeMs: 105,
        stepId: "delete",
      },
      digest,
    )
    const running = begin(createOperationRecord(sealedPlan), activeLease, {
      authorization,
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    }).operation
    const record = running.steps.delete
    if (!record) throw new Error("Fixture step is missing.")
    const { irreversibleAuthorization: _body, ...checksumOnly } = record
    const { authorizationChecksum: _checksum, ...bodyOnly } = record
    const replacements = [
      checksumOnly,
      bodyOnly,
      { ...record, authorizationChecksum: "contradictory-checksum" },
      { ...record, irreversibleAuthorization: Object.freeze({ ...authorization }) },
      { ...record, irreversibleAuthorization: new Proxy(authorization, {}) },
    ]

    for (const replacement of replacements) {
      const malformed = {
        ...running,
        steps: { ...running.steps, delete: replacement },
      } as OperationRecord
      expect(() =>
        recordStepSuccess(malformed, {
          attemptId: "attempt-1",
          observedPostconditionChecksum: "post-delete",
          resultChecksum: "deleted",
          stepId: "delete",
        }),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    }
  })

  it("accepts only an exact trusted authorization when one is supplied on replay", async () => {
    const sealedPlan = await plan([step("delete", { checkpoint: "irreversible" })])
    const activeLease = acquire()
    const authorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-replay",
        decisionChecksum: "decision",
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        sealedAtServerTimeMs: 105,
        stepId: "delete",
      },
      digest,
    )
    const otherTrustedAuthorization = await sealIrreversibleAuthorization(
      sealedPlan,
      {
        actorChecksum: "actor",
        authorizationId: "authorization-other",
        decisionChecksum: "other-decision",
        lease: activeLease,
        leaseProof: leaseProof(activeLease),
        sealedAtServerTimeMs: 105,
        stepId: "delete",
      },
      digest,
    )
    const running = begin(createOperationRecord(sealedPlan), activeLease, {
      authorization,
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    }).operation
    const succeeded = recordStepSuccess(running, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-delete",
      resultChecksum: "deleted",
      stepId: "delete",
    })
    const exactLoadedAuthorization = await loadIrreversibleAuthorization(
      { ...authorization },
      digest,
    )
    const replayInput = {
      idempotencyKey: "idempotency-delete",
      precondition: "pre-delete",
      stepId: "delete",
    }

    expect(begin(succeeded, activeLease, replayInput)).toMatchObject({
      disposition: "replay",
      resultChecksum: "deleted",
    })
    expect(begin(succeeded, activeLease, { ...replayInput, authorization })).toMatchObject({
      disposition: "replay",
      resultChecksum: "deleted",
    })
    expect(
      begin(succeeded, activeLease, {
        ...replayInput,
        authorization: exactLoadedAuthorization,
      }),
    ).toMatchObject({ disposition: "replay", resultChecksum: "deleted" })
    expect(begin(running, activeLease, { ...replayInput, authorization }).disposition).toBe(
      "in_progress",
    )
    const unknown = markRunningStepUnknownAfterCrash(running, "delete", "lost-response")
    expect(begin(unknown, activeLease, { ...replayInput, authorization }).disposition).toBe(
      "reconcile",
    )

    for (const contradictory of [
      otherTrustedAuthorization,
      Object.freeze({ ...authorization }),
      new Proxy(authorization, {}),
    ]) {
      expect(() =>
        begin(succeeded, activeLease, {
          ...replayInput,
          authorization: contradictory,
        }),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    }

    const reversibleRunning = begin(createOperationRecord(await plan()), activeLease).operation
    const reversibleSucceeded = recordStepSuccess(reversibleRunning, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-one",
      resultChecksum: "done",
      stepId: "one",
    })
    expect(() => begin(reversibleSucceeded, activeLease, { authorization })).toThrow(
      "reversible step",
    )
  })

  it("accepts an exact 64 KiB receipt and rejects the next byte", async () => {
    const sealedPlan = await plan([step("delete", { checkpoint: "irreversible" })])
    const activeLease = acquire()
    const sealWithActor = (actorChecksum: string) =>
      sealIrreversibleAuthorization(
        sealedPlan,
        {
          actorChecksum,
          authorizationId: "authorization-sized",
          decisionChecksum: "decision",
          lease: activeLease,
          leaseProof: leaseProof(activeLease),
          sealedAtServerTimeMs: 105,
          stepId: "delete",
        },
        digest,
      )
    const smallest = await sealWithActor("a")
    const encoder = new TextEncoder()
    const smallestBytes = encoder.encode(JSON.stringify(smallest)).byteLength
    const exact = await sealWithActor(
      "a".repeat(1 + MAX_IRREVERSIBLE_AUTHORIZATION_BYTES - smallestBytes),
    )
    expect(encoder.encode(JSON.stringify(exact)).byteLength).toBe(
      MAX_IRREVERSIBLE_AUTHORIZATION_BYTES,
    )
    await expect(
      sealWithActor("a".repeat(2 + MAX_IRREVERSIBLE_AUTHORIZATION_BYTES - smallestBytes)),
    ).rejects.toThrow("exceeds the 64 KiB receipt limit")
    let oversizedDigestCalls = 0
    await expect(
      sealIrreversibleAuthorization(
        sealedPlan,
        {
          actorChecksum: "a".repeat(MAX_IRREVERSIBLE_AUTHORIZATION_BYTES),
          authorizationId: "authorization-oversized-before-digest",
          decisionChecksum: "decision",
          lease: activeLease,
          leaseProof: leaseProof(activeLease),
          sealedAtServerTimeMs: 105,
          stepId: "delete",
        },
        async (input) => {
          oversizedDigestCalls += 1
          return digest(input)
        },
      ),
    ).rejects.toThrow("exceeds the 64 KiB receipt limit")
    expect(oversizedDigestCalls).toBe(0)

    const loaded = await loadIrreversibleAuthorization({ ...exact }, digest)
    expect(loaded).toEqual(exact)
    expect(Object.isFrozen(loaded)).toBe(true)
    const oversized = await resignAuthorization({
      ...exact,
      actorChecksum: `${exact.actorChecksum}a`,
    })
    expect(encoder.encode(JSON.stringify(oversized)).byteLength).toBe(
      MAX_IRREVERSIBLE_AUTHORIZATION_BYTES + 1,
    )
    await expect(loadIrreversibleAuthorization(oversized, digest)).rejects.toThrow(
      "exceeds the 64 KiB receipt limit",
    )
  })

  it("captures authorization inputs once and brands only exact persisted fields", async () => {
    const sealedPlan = await plan([step("delete", { checkpoint: "irreversible" })])
    const lease = acquire()
    const base = {
      actorChecksum: "actor",
      authorizationId: "authorization-captured",
      decisionChecksum: "decision",
      lease,
      leaseProof: leaseProof(lease),
      sealedAtServerTimeMs: 105,
      stepId: "delete",
    }
    let actorReads = 0
    const switching = { ...base }
    Object.defineProperty(switching, "actorChecksum", {
      enumerable: true,
      get() {
        actorReads += 1
        return actorReads === 1 ? "actor" : "changed-actor"
      },
    })

    const authorization = await sealIrreversibleAuthorization(sealedPlan, switching, digest)
    expect(actorReads).toBe(1)
    expect(authorization.actorChecksum).toBe("actor")

    await expect(
      sealIrreversibleAuthorization(sealedPlan, new Proxy(base, {}), digest),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Irreversible authorization input could not be captured safely.",
    })
    await expect(
      sealIrreversibleAuthorization(
        sealedPlan,
        { ...base, outsideChecksum: "unchecked" } as typeof base,
        digest,
      ),
    ).rejects.toThrow(/input fields/u)
    const { leaseKey: _leaseKey, ...leaseWithoutKey } = lease
    for (const candidate of [
      { ...base, lease: { ...lease, outsideChecksum: "unchecked" } },
      { ...base, leaseProof: { ...base.leaseProof, outsideChecksum: "unchecked" } },
      {
        ...base,
        lease: { ...leaseWithoutKey, outsideChecksum: "unchecked" } as unknown as FencedLeaseRecord,
      },
      {
        ...base,
        lease: Object.assign(new Map(), lease) as unknown as FencedLeaseRecord,
      },
    ]) {
      await expect(sealIrreversibleAuthorization(sealedPlan, candidate, digest)).rejects.toThrow(
        /lease fields/u,
      )
    }
    await expect(
      sealIrreversibleAuthorization(structuredClone(sealedPlan), base, digest),
    ).rejects.toThrow(/sealed or integrity-verified plan/u)

    let checksumReads = 0
    const switchingChecksum = { ...authorization }
    Object.defineProperty(switchingChecksum, "authorizationChecksum", {
      enumerable: true,
      get() {
        checksumReads += 1
        return checksumReads === 1 ? authorization.authorizationChecksum : "changed-checksum"
      },
    })
    const loaded = await loadIrreversibleAuthorization(switchingChecksum, digest)
    expect(checksumReads).toBe(1)
    expect(loaded).toEqual(authorization)
    expect(Object.keys(loaded)).toEqual(Object.keys(authorization))
    await expect(
      loadIrreversibleAuthorization(
        { ...authorization, outsideChecksum: "unchecked" } as typeof authorization,
        digest,
      ),
    ).rejects.toThrow(/authorization fields/u)
    await expect(
      loadIrreversibleAuthorization(new Proxy({ ...authorization }, {}), digest),
    ).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The persisted irreversible authorization could not be captured safely.",
    })
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

describe("atomic internal operation outcomes", () => {
  const success: AtomicStepOutcome = {
    observedPostconditionChecksum: "post-one",
    resultChecksum: "atomic-result",
    state: "succeeded",
  }

  it("moves a pristine pending reversible step directly to each valid terminal state", async () => {
    const activeLease = acquire()
    const operation = createOperationRecord(
      await plan([step("one"), step("two", { dependsOn: ["one"] })]),
    )
    expect(() =>
      atomicOutcome(
        operation,
        activeLease,
        {
          observedPostconditionChecksum: "post-two",
          resultChecksum: "result-two",
          state: "succeeded",
        },
        { stepId: "two" },
      ),
    ).toThrow("dependency has not succeeded")

    const first = atomicOutcome(operation, activeLease, success)
    const completed = atomicOutcome(
      first,
      activeLease,
      {
        observedPostconditionChecksum: "post-two",
        resultChecksum: "result-two",
        state: "succeeded",
      },
      { stepId: "two" },
    )
    expect(operation.steps.one?.state).toBe("pending")
    expect(first.steps.one).toEqual({
      costCounters: {},
      fencingToken: activeLease.fencingToken,
      lastAttemptId: "atomic-attempt-1",
      progressCounters: {},
      resultChecksum: "atomic-result",
      startedAttempts: 1,
      state: "succeeded",
    })
    expect(first.steps.one?.activeAttemptId).toBeUndefined()
    expect(Object.isFrozen(first)).toBe(true)
    expect(Object.isFrozen(first.steps)).toBe(true)
    expect(Object.isFrozen(first.steps.one)).toBe(true)
    expect(Object.isFrozen(first.steps.one?.costCounters)).toBe(true)
    expect(Object.isFrozen(first.steps.one?.progressCounters)).toBe(true)
    expect(operationStatus(completed)).toBe("succeeded")

    const failed = atomicOutcome(createOperationRecord(await plan()), activeLease, {
      errorChecksum: "atomic-failure",
      state: "failed",
    })
    expect(failed.steps.one).toEqual({
      costCounters: {},
      errorChecksum: "atomic-failure",
      fencingToken: activeLease.fencingToken,
      lastAttemptId: "atomic-attempt-1",
      progressCounters: {},
      startedAttempts: 1,
      state: "failed",
    })
    expect(operationStatus(failed)).toBe("failed")

    const intervention = atomicOutcome(createOperationRecord(await plan()), activeLease, {
      evidenceChecksum: "atomic-intervention",
      state: "intervention_required",
    })
    expect(intervention.steps.one).toEqual({
      costCounters: {},
      fencingToken: activeLease.fencingToken,
      lastAttemptId: "atomic-attempt-1",
      progressCounters: {},
      reconciliationEvidenceChecksum: "atomic-intervention",
      startedAttempts: 1,
      state: "intervention_required",
    })
    expect(operationStatus(intervention)).toBe("intervention_required")
  })

  it("replays only the exact terminal attempt, fence, and logical evidence", async () => {
    const activeLease = acquire()
    for (const outcome of [
      success,
      { errorChecksum: "atomic-failure", state: "failed" },
      { evidenceChecksum: "atomic-intervention", state: "intervention_required" },
    ] satisfies readonly AtomicStepOutcome[]) {
      const committed = atomicOutcome(createOperationRecord(await plan()), activeLease, outcome)
      expect(atomicOutcome(committed, activeLease, outcome)).toBe(committed)
      expect(() =>
        atomicOutcome(committed, activeLease, outcome, { attemptId: "different-attempt" }),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
      expect(() =>
        atomicOutcome(committed, activeLease, outcome, {
          proof: { ...leaseProof(activeLease), fencingToken: activeLease.fencingToken + 1 },
        }),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    }

    const succeeded = atomicOutcome(createOperationRecord(await plan()), activeLease, success)
    for (const replacement of [
      { ...succeeded.steps.one, unexpected: "field" },
      { ...succeeded.steps.one, activeAttemptId: "atomic-attempt-1" },
      { ...succeeded.steps.one, authorizationChecksum: "unexpected-authorization" },
      { ...succeeded.steps.one, costCounters: { calls: 1 } },
      { ...succeeded.steps.one, errorChecksum: "unexpected-error" },
      { ...succeeded.steps.one, fencingToken: activeLease.fencingToken + 1 },
      { ...succeeded.steps.one, lastAttemptId: "different-attempt" },
      { ...succeeded.steps.one, progressCounters: { phases: 1 } },
      { ...succeeded.steps.one, reconciliationEvidenceChecksum: "unexpected-evidence" },
      { ...succeeded.steps.one, resultChecksum: "different-result" },
      { ...succeeded.steps.one, startedAttempts: 2 },
    ]) {
      const contradictory = {
        ...succeeded,
        steps: { ...succeeded.steps, one: replacement },
      } as OperationRecord
      expect(() => atomicOutcome(contradictory, activeLease, success)).toThrowError(
        expect.objectContaining({ code: "OperationInterventionRequiredError" }),
      )
    }
    expect(() =>
      atomicOutcome(succeeded, activeLease, {
        observedPostconditionChecksum: "post-one",
        resultChecksum: "different-result",
        state: "succeeded",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    expect(() =>
      atomicOutcome(succeeded, activeLease, {
        errorChecksum: "different-terminal-state",
        state: "failed",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
  })

  it("requires exact plan bindings, a positive lease proof, and well-formed outcome evidence", async () => {
    const activeLease = acquire()
    const operation = createOperationRecord(await plan())
    expect(() =>
      atomicOutcome(operation, activeLease, success, { idempotencyKey: "different-key" }),
    ).toThrow("different idempotency key")
    expect(() =>
      atomicOutcome(operation, activeLease, success, {
        observedPreconditionChecksum: "different-precondition",
      }),
    ).toThrow("precondition")
    expect(() =>
      atomicOutcome(operation, activeLease, success, {
        proof: { ...leaseProof(activeLease), leaseKey: "different-lease" },
      }),
    ).toThrow("wrong lease key")
    expect(() =>
      atomicOutcome(operation, activeLease, {
        ...success,
        observedPostconditionChecksum: "different-postcondition",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    const irreversible = createOperationRecord(
      await plan([step("one", { checkpoint: "irreversible" })]),
    )
    expect(() => atomicOutcome(irreversible, activeLease, success)).toThrow("requires a reversible")
    for (const effectProtocol of ["provider_receipt", "saga_receipt"] as const) {
      const receiptOwned = createOperationRecord(await plan([step("one", { effectProtocol })]))
      expect(() => atomicOutcome(receiptOwned, activeLease, success)).toThrow(
        "cannot bypass a receipt-owned step",
      )
    }

    for (const proof of [
      { ...leaseProof(activeLease), leaseKey: "" },
      { ...leaseProof(activeLease), holderId: "" },
      { ...leaseProof(activeLease), acquisitionId: "" },
      { ...leaseProof(activeLease), fencingToken: 0 },
    ]) {
      expect(() => atomicOutcome(operation, activeLease, success, { proof })).toThrowError(
        expect.objectContaining({ code: "ConfigurationError" }),
      )
    }
    for (const outcome of [
      null as unknown as AtomicStepOutcome,
      { state: "future" } as unknown as AtomicStepOutcome,
      { ...success, resultChecksum: "" },
      { ...success, observedPostconditionChecksum: "" },
      { errorChecksum: "", state: "failed" },
      { evidenceChecksum: "", state: "intervention_required" },
    ] satisfies readonly AtomicStepOutcome[]) {
      expect(() => atomicOutcome(operation, activeLease, outcome)).toThrowError(
        expect.objectContaining({ code: "ConfigurationError" }),
      )
    }
  })

  it("fails closed for attempted or conditionally omitted steps and malformed pending evidence", async () => {
    const activeLease = acquire()
    const operation = createOperationRecord(await plan())
    const running = begin(operation, activeLease).operation
    const retryable = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "known-absence",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    const unknown = recordStepFailure(running, {
      attemptId: "attempt-1",
      errorChecksum: "unknown-effect",
      outcome: "unknown",
      stepId: "one",
    })
    const conditional = createOperationRecord(
      await plan([step("one"), step("optional", { activation: "conditional" })]),
    )
    const notRequired = markOperationStepNotRequired(conditional, {
      evidenceChecksum: "branch-decision",
      stepId: "optional",
    })
    for (const attempted of [running, retryable, unknown]) {
      expect(() => atomicOutcome(attempted, activeLease, success)).toThrowError(
        expect.objectContaining({ code: "OperationResumeRequiredError" }),
      )
    }
    expect(() =>
      atomicOutcome(
        notRequired,
        activeLease,
        {
          observedPostconditionChecksum: "post-optional",
          resultChecksum: "result-optional",
          state: "succeeded",
        },
        { stepId: "optional" },
      ),
    ).toThrowError(expect.objectContaining({ code: "OperationResumeRequiredError" }))

    const pending = operation.steps.one
    for (const replacement of [
      { ...pending, unexpected: "field" },
      { ...pending, startedAttempts: 1 },
      { ...pending, costCounters: { calls: 1 } },
      { ...pending, progressCounters: { phases: 1 } },
      { ...pending, activeAttemptId: "attempt" },
      { ...pending, authorizationChecksum: "authorization" },
      { ...pending, errorChecksum: "error" },
      { ...pending, fencingToken: 1 },
      { ...pending, lastAttemptId: "attempt" },
      { ...pending, reconciliationEvidenceChecksum: "evidence" },
      { ...pending, resultChecksum: "result" },
    ]) {
      const malformed = { ...operation, steps: { one: replacement } } as OperationRecord
      expect(() => atomicOutcome(malformed, activeLease, success)).toThrowError(
        expect.objectContaining({ code: "OperationInterventionRequiredError" }),
      )
    }
  })

  it("preserves one terminal attempt and rejects every changed replay across arbitrary fences", async () => {
    const sealedPlan = await plan()
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: Number.MAX_SAFE_INTEGER - 1 }),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        (fencingToken, attemptNumber, resultNumber) => {
          const proof = {
            acquisitionId: `acquisition-${fencingToken}`,
            fencingToken,
            holderId: `holder-${fencingToken}`,
            leaseKey: "fleet:example",
          }
          const input = {
            attemptId: `attempt-${attemptNumber}`,
            idempotencyKey: "idempotency-one",
            leaseProof: proof,
            observedPreconditionChecksum: "pre-one",
            outcome: {
              observedPostconditionChecksum: "post-one",
              resultChecksum: `result-${resultNumber}`,
              state: "succeeded" as const,
            },
            stepId: "one",
          }
          const committed = recordAtomicStepOutcome(createOperationRecord(sealedPlan), input)
          expect(recordAtomicStepOutcome(committed, input)).toBe(committed)
          expect(committed.steps.one).toMatchObject({
            fencingToken,
            lastAttemptId: input.attemptId,
            startedAttempts: 1,
            state: "succeeded",
          })
          expect(() =>
            recordAtomicStepOutcome(committed, {
              ...input,
              outcome: {
                ...input.outcome,
                resultChecksum: `${input.outcome.resultChecksum}-other`,
              },
            }),
          ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
        },
      ),
      { numRuns: 200 },
    )
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
    expect(
      recordSagaStepTerminalClassification(terminal, {
        counters: { cost: { observations: 1 }, progress: { classifications: 1 } },
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt-outcome",
        stepId: "one",
      }),
    ).toBe(terminal)
    expect(() =>
      recordSagaStepTerminalClassification(terminal, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "contradictory-terminal-receipt",
        stepId: "one",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))

    const terminalAfterRetryable = recordSagaStepTerminalClassification(retryable, {
      counters: { cost: { observations: 2 }, progress: { classifications: 3 } },
      outcome: "not_applied",
      receiptOutcomeChecksum: "not-applied-observation",
      stepId: "one",
    })
    expect(terminalAfterRetryable.steps.one).toEqual({
      costCounters: { observations: 2 },
      errorChecksum: "unknown-effect",
      fencingToken: lease.fencingToken,
      lastAttemptId: "attempt-1",
      progressCounters: { classifications: 3 },
      reconciliationEvidenceChecksum: "not-applied-observation",
      resultChecksum: "not-applied-observation",
      startedAttempts: 1,
      state: "succeeded",
    })
    expect(retryable.steps.one).toMatchObject({
      reconciliationEvidenceChecksum: "not-applied-observation",
      state: "retryable_failed",
    })

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

  it("preserves arbitrary retryable and unknown saga attempt evidence during terminal classification", async () => {
    const sealedPlan = await plan([
      step("one", { effectProtocol: "saga_receipt", retryClassification: "reconcile_first" }),
    ])
    fc.assert(
      fc.property(
        fc.constantFrom("retryable_failed" as const, "unknown" as const),
        fc.integer({ min: 1, max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        fc.nat({ max: 1_000_000 }),
        (
          sourceState,
          fencingToken,
          initialCost,
          initialProgress,
          classificationCost,
          classificationProgress,
          receiptNumber,
        ) => {
          const activeLease: FencedLeaseRecord = Object.freeze({
            acquisitionId: `acquisition-${fencingToken}`,
            expiresAtServerTimeMs: 1_000,
            fencingToken,
            holderId: `holder-${fencingToken}`,
            leaseKey: "fleet:example",
          })
          const running = begin(createOperationRecord(sealedPlan), activeLease, {
            attemptId: `attempt-${fencingToken}`,
          }).operation
          const source = recordStepFailure(running, {
            attemptId: `attempt-${fencingToken}`,
            counters: {
              cost: { calls: initialCost },
              progress: { receipts: initialProgress },
            },
            errorChecksum: `source-error-${receiptNumber}`,
            outcome: sourceState === "unknown" ? "unknown" : "definitely_not_applied",
            stepId: "one",
          })
          expect(source.steps.one?.state).toBe(sourceState)
          const before = source.steps.one
          const receiptOutcomeChecksum =
            sourceState === "retryable_failed"
              ? `source-error-${receiptNumber}`
              : `receipt-${receiptNumber}`
          const input = {
            counters: {
              cost: { calls: classificationCost },
              progress: { receipts: classificationProgress },
            },
            outcome: "not_applied" as const,
            receiptOutcomeChecksum,
            stepId: "one",
          }

          const terminal = recordSagaStepTerminalClassification(source, input)

          expect(source.steps.one).toBe(before)
          expect(terminal.steps.one).toEqual({
            costCounters: { calls: initialCost + classificationCost },
            errorChecksum: `source-error-${receiptNumber}`,
            fencingToken,
            lastAttemptId: `attempt-${fencingToken}`,
            progressCounters: { receipts: initialProgress + classificationProgress },
            reconciliationEvidenceChecksum: receiptOutcomeChecksum,
            resultChecksum: receiptOutcomeChecksum,
            startedAttempts: 1,
            state: "succeeded",
          })
          expect(Object.isFrozen(terminal)).toBe(true)
          expect(Object.isFrozen(terminal.steps.one)).toBe(true)
          expect(recordSagaStepTerminalClassification(terminal, input)).toBe(terminal)
          expect(() =>
            recordSagaStepTerminalClassification(terminal, {
              ...input,
              receiptOutcomeChecksum: `${receiptOutcomeChecksum}:different`,
            }),
          ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
        },
      ),
      { numRuns: 200 },
    )
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

  it("terminally fails a never-retry step proven not dispatched after a crash", async () => {
    const lease = acquire()
    const running = begin(
      createOperationRecord(
        await plan([
          step("one", { effectProtocol: "provider_receipt", retryClassification: "never" }),
        ]),
      ),
      lease,
      { attemptId: "provider-attempt-1" },
    ).operation

    const recovered = markRunningStepNotDispatchedAfterCrash(
      running,
      "one",
      "provider-dispatch-absence",
    )

    expect(recovered.steps.one).toEqual({
      costCounters: {},
      fencingToken: lease.fencingToken,
      lastAttemptId: "provider-attempt-1",
      progressCounters: {},
      reconciliationEvidenceChecksum: "provider-dispatch-absence",
      startedAttempts: 1,
      state: "failed",
    })
    expect(operationStatus(recovered)).toBe("failed")
    expect(Object.isFrozen(recovered.steps.one)).toBe(true)
  })

  it("blocks every second dispatch after never-retry crash absence is terminal", async () => {
    const lease = acquire()
    const running = begin(
      createOperationRecord(await plan([step("one", { retryClassification: "never" })])),
      lease,
      { attemptId: "first-attempt" },
    ).operation
    const recovered = markRunningStepNotDispatchedAfterCrash(
      running,
      "one",
      "first-attempt-was-not-dispatched",
    )

    for (const attemptId of ["first-attempt", "second-attempt"]) {
      const decision = begin(recovered, lease, { attemptId })
      expect(decision).toEqual({ disposition: "blocked", operation: recovered })
      expect(decision.operation.steps.one?.startedAttempts).toBe(1)
    }
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

    for (const effectProtocol of ["opaque", "provider_receipt"] as const) {
      const protocolOperation = createOperationRecord(await plan([step("one", { effectProtocol })]))
      const protocolUnknown = recordStepFailure(begin(protocolOperation, lease).operation, {
        attemptId: "attempt-1",
        errorChecksum: "unknown",
        outcome: "unknown",
        stepId: "one",
      })
      expect(() =>
        recordSagaStepTerminalClassification(protocolUnknown, {
          outcome: "not_applied",
          receiptOutcomeChecksum: "terminal-receipt",
          stepId: "one",
        }),
      ).toThrow("requires a saga-receipt step")
    }

    const sagaOperation = createOperationRecord(
      await plan([step("one", { effectProtocol: "saga_receipt" })]),
    )
    expect(() =>
      recordSagaStepTerminalClassification(sagaOperation, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "one",
      }),
    ).toThrow("Only an unknown or retryable saga step")
    const sagaRunning = begin(sagaOperation, lease).operation
    expect(() =>
      recordSagaStepTerminalClassification(sagaRunning, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "one",
      }),
    ).toThrow("Only an unknown or retryable saga step")
    const sagaUnknown = recordStepFailure(sagaRunning, {
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
    const sagaRetryable = recordStepFailure(sagaRunning, {
      attemptId: "attempt-1",
      errorChecksum: "proven-absence",
      outcome: "definitely_not_applied",
      stepId: "one",
    })
    expect(() =>
      recordSagaStepTerminalClassification(sagaRetryable, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "different-absence-evidence",
        stepId: "one",
      }),
    ).toThrow(/contradicts durable non-application evidence/u)
    for (const source of [sagaUnknown, sagaRetryable]) {
      for (const missing of ["fencingToken", "lastAttemptId"] as const) {
        const malformedStep = { ...source.steps.one }
        delete malformedStep[missing]
        expect(() =>
          recordSagaStepTerminalClassification(
            { ...source, steps: { one: malformedStep } } as OperationRecord,
            {
              outcome: "not_applied",
              receiptOutcomeChecksum: "terminal-receipt",
              stepId: "one",
            },
          ),
        ).toThrow("incomplete classification metadata")
      }
    }
    for (const malformedStep of [
      { ...sagaRetryable.steps.one, activeAttemptId: "attempt-1" },
      { ...sagaRetryable.steps.one, startedAttempts: 0 },
      { ...sagaRetryable.steps.one, costCounters: { calls: -1 } },
      { ...sagaRetryable.steps.one, unexpected: "field" },
      {
        ...sagaRetryable.steps.one,
        errorChecksum: undefined,
        reconciliationEvidenceChecksum: undefined,
      },
    ]) {
      expect(() =>
        recordSagaStepTerminalClassification(
          { ...sagaRetryable, steps: { one: malformedStep } } as OperationRecord,
          {
            outcome: "not_applied",
            receiptOutcomeChecksum: "terminal-receipt",
            stepId: "one",
          },
        ),
      ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))
    }

    const sagaFailed = recordStepFailure(sagaRunning, {
      attemptId: "attempt-1",
      errorChecksum: "permanent",
      outcome: "permanent",
      stepId: "one",
    })
    const sagaIntervention = recordStepReconciliation(sagaUnknown, {
      evidenceChecksum: "indeterminate",
      outcome: "indeterminate",
      stepId: "one",
    })
    for (const source of [sagaFailed, sagaIntervention]) {
      expect(() =>
        recordSagaStepTerminalClassification(source, {
          outcome: "not_applied",
          receiptOutcomeChecksum: "terminal-receipt",
          stepId: "one",
        }),
      ).toThrow("Only an unknown or retryable saga step")
    }

    const ordinarySuccess = recordStepSuccess(sagaRunning, {
      attemptId: "attempt-1",
      observedPostconditionChecksum: "post-one",
      resultChecksum: "business-result",
      stepId: "one",
    })
    expect(() =>
      recordSagaStepTerminalClassification(ordinarySuccess, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "business-result",
        stepId: "one",
      }),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))

    const conditional = createOperationRecord(
      await plan([
        step("one"),
        step("optional", { activation: "conditional", effectProtocol: "saga_receipt" }),
      ]),
    )
    const notRequired = markOperationStepNotRequired(conditional, {
      evidenceChecksum: "branch-evidence",
      stepId: "optional",
    })
    expect(() =>
      recordSagaStepTerminalClassification(notRequired, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "terminal-receipt",
        stepId: "optional",
      }),
    ).toThrow("Only an unknown or retryable saga step")
  })

  it("requires full evidence to reconcile applied outcomes and preserves never-retry", async () => {
    const lease = acquire()
    const started = begin(
      createOperationRecord(
        await plan([step("one", { effectProtocol: "saga_receipt", retryClassification: "never" })]),
      ),
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

    const failedStep = absent.steps.one
    if (failedStep === undefined) throw new Error("Fixture step is missing.")
    const forgedRetryable: OperationRecord = {
      ...absent,
      steps: { ...absent.steps, one: { ...failedStep, state: "retryable_failed" } },
    }
    expect(() =>
      recordSagaStepTerminalClassification(forgedRetryable, {
        outcome: "not_applied",
        receiptOutcomeChecksum: "absent",
        stepId: "one",
      }),
    ).toThrow(/never-retry/u)
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

    const otherFirst = await appendAuditEvent(
      undefined,
      auditInput(100, "other.operation.started"),
      digest,
    )
    const otherSecond = await appendAuditEvent(
      otherFirst,
      auditInput(101, "other.step.started"),
      digest,
    )
    expect(await verifyAuditChain([first, otherSecond], digest)).toBe(false)

    const backwardsUnsigned = {
      ...auditInput(99, "step.backwards"),
      previousHash: first.eventHash,
      schemaVersion: 1 as const,
      sequence: 2,
    }
    const backwards: AuditEvent = {
      ...backwardsUnsigned,
      eventHash: await digest(encodeAuditEventChecksumInput(backwardsUnsigned)),
    }
    expect(await verifyAuditChain([first, backwards], digest)).toBe(false)
    await expect(appendAuditEvent(first, auditInput(99), digest)).rejects.toThrow("cannot decrease")
  })

  it("captures complete audit inputs and chains before hashing", async () => {
    const switchingInput = auditInput(100)
    let actorReads = 0
    Object.defineProperty(switchingInput, "actorChecksum", {
      enumerable: true,
      get() {
        actorReads += 1
        return actorReads === 1 ? "actor-hash" : "changed-actor"
      },
    })
    const first = await appendAuditEvent(undefined, switchingInput, digest)
    expect(actorReads).toBe(1)
    expect(first.actorChecksum).toBe("actor-hash")

    const switchingPrevious = { ...first }
    let hashReads = 0
    Object.defineProperty(switchingPrevious, "eventHash", {
      enumerable: true,
      get() {
        hashReads += 1
        return hashReads === 1 ? first.eventHash : "changed-previous-hash"
      },
    })
    const second = await appendAuditEvent(switchingPrevious, auditInput(101), digest)
    expect(hashReads).toBe(1)
    expect(second.previousHash).toBe(first.eventHash)

    await expect(
      appendAuditEvent(
        undefined,
        { ...auditInput(100), outsideChecksum: "unchecked" } as ReturnType<typeof auditInput>,
        digest,
      ),
    ).rejects.toThrow(/input fields/u)
    await expect(
      appendAuditEvent(undefined, new Proxy(auditInput(100), {}), digest),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Audit append input could not be captured safely.",
    })
    await expect(
      appendAuditEvent(
        { ...first, outsideChecksum: "unchecked" } as AuditEvent,
        auditInput(101),
        digest,
      ),
    ).rejects.toThrow(/audit event fields/u)
    await expect(
      appendAuditEvent({ ...first, payloadChecksum: "tampered" }, auditInput(101), digest),
    ).rejects.toThrow(/checksum does not match/u)

    let persistedHashReads = 0
    const switchingPersisted = { ...first }
    Object.defineProperty(switchingPersisted, "eventHash", {
      enumerable: true,
      get() {
        persistedHashReads += 1
        return persistedHashReads === 1 ? first.eventHash : "changed-event-hash"
      },
    })
    await expect(loadAuditEvent(switchingPersisted, digest)).resolves.toEqual(first)
    expect(persistedHashReads).toBe(1)
    await expect(loadAuditEvent(new Proxy({ ...first }, {}), digest)).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The persisted audit event could not be captured safely.",
    })

    const chain = [first, second]
    let mutated = false
    const mutatingDigest: DigestFunction = async (input) => {
      if (!mutated) {
        mutated = true
        chain[1] = { ...second, previousHash: "post-capture-mutation" }
      }
      return digest(input)
    }
    expect(await verifyAuditChain(chain, mutatingDigest)).toBe(true)
    expect(chain[1]?.previousHash).toBe("post-capture-mutation")
    expect(await verifyAuditChain(new Proxy([first], {}), digest)).toBe(false)

    expect(
      await verifyAuditChain([{ ...first, outsideChecksum: "unchecked" } as AuditEvent], digest),
    ).toBe(false)
    const decoratedChain = [first] as AuditEvent[] & { outsideChecksum?: string }
    decoratedChain.outsideChecksum = "unchecked"
    const sparseChain = new Array<AuditEvent>(2)
    sparseChain[1] = first
    const balancedSparseChain = new Array<AuditEvent>(1) as AuditEvent[] & {
      outsideChecksum?: string
    }
    balancedSparseChain.outsideChecksum = "unchecked"
    for (const malformedChain of [decoratedChain, sparseChain, balancedSparseChain]) {
      expect(await verifyAuditChain(malformedChain, digest)).toBe(false)
    }
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
