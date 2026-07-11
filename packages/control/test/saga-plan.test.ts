import {
  createSagaRecord,
  type DigestFunction,
  loadOperationPlan,
  NozzleError,
  type OperationPlan,
  type SagaActionReference,
  type SagaRecord,
  sagaActionIdempotencyKey,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { describe, expect, it } from "vitest"
import {
  assertTrustedSagaOperationPlan,
  sealSagaOperationPlan,
  verifySagaOperationPlan,
} from "../src/saga-plan.js"
import { type SagaHandlerRegistration, sealSagaHandlerRegistry } from "../src/saga-registry.js"
import {
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "../src/saga-store.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function reference(actionId: string, byte: string): SagaActionReference {
  return { actionId, artifactChecksum: byte.repeat(64), version: 1 }
}

const actions = {
  chargeCompensation: reference("charge.compensate", "3"),
  chargeCompensationObservation: reference("charge.compensation.observe", "4"),
  chargeForward: reference("charge.forward", "1"),
  chargeForwardObservation: reference("charge.forward.observe", "2"),
  notifyForward: reference("notify.forward", "5"),
  notifyForwardObservation: reference("notify.forward.observe", "6"),
}

const effect = () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" as const })
const observation = () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" as const })

function registrations(): SagaHandlerRegistration[] {
  return [
    { handler: effect, kind: "effect", reference: actions.chargeForward },
    {
      handler: observation,
      kind: "observation",
      reference: actions.chargeForwardObservation,
    },
    { handler: effect, kind: "effect", reference: actions.chargeCompensation },
    {
      handler: observation,
      kind: "observation",
      reference: actions.chargeCompensationObservation,
    },
    { handler: effect, kind: "effect", reference: actions.notifyForward },
    {
      handler: observation,
      kind: "observation",
      reference: actions.notifyForwardObservation,
    },
  ]
}

async function descriptor(descriptorId = "checkout", version = 1) {
  return sealSagaDescriptor(
    {
      descriptorId,
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 10,
          compensationAction: actions.chargeCompensation,
          compensationObservation: actions.chargeCompensationObservation,
          forwardAction: actions.chargeForward,
          forwardObservation: actions.chargeForwardObservation,
          inputSchemaChecksum: "a".repeat(64),
          irreversible: false,
          maxAttempts: 3,
          maxRetryDelayMs: 100,
          outputSchemaChecksum: "b".repeat(64),
          stepId: "charge",
          timeoutMs: 1_000,
        },
        {
          authorizationPolicyChecksum: "c".repeat(64),
          baseRetryDelayMs: 10,
          compensationAction: null,
          compensationObservation: null,
          forwardAction: actions.notifyForward,
          forwardObservation: actions.notifyForwardObservation,
          inputSchemaChecksum: "d".repeat(64),
          irreversible: true,
          maxAttempts: 2,
          maxRetryDelayMs: 100,
          outputSchemaChecksum: "e".repeat(64),
          stepId: "notify",
          timeoutMs: 1_000,
        },
      ],
      version,
    },
    digest,
  )
}

async function fixture() {
  const sealedDescriptor = await descriptor()
  const registry = await sealSagaHandlerRegistry(registrations(), digest)
  const stepInputChecksums = { charge: "7".repeat(64), notify: "8".repeat(64) }
  return {
    input: {
      capabilitySnapshotChecksum: "9".repeat(64),
      descriptor: sealedDescriptor,
      inputChecksum: "0".repeat(64),
      leaseKey: "saga:checkout-1",
      operationId: "checkout-operation",
      operationIdempotencyKey: "checkout-operation-key",
      registry,
      sagaId: "checkout-1",
      stepInputChecksums,
    },
    registry,
    sealedDescriptor,
    stepInputChecksums,
  }
}

function sagaForInput(
  input: Awaited<ReturnType<typeof fixture>>["input"],
  overrides: {
    readonly descriptor?: SagaRecord["descriptor"]
    readonly inputChecksum?: string
    readonly sagaId?: string
    readonly stepInputChecksums?: Readonly<Record<string, string>>
  } = {},
): SagaRecord {
  return createSagaRecord({
    deadlineAtMs: 10_000,
    descriptor: overrides.descriptor ?? input.descriptor,
    idempotencyKey: "checkout-saga-key",
    inputChecksum: overrides.inputChecksum ?? input.inputChecksum,
    sagaId: overrides.sagaId ?? input.sagaId,
    serverTimeMs: 1_000,
    stepInputChecksums: overrides.stepInputChecksums ?? input.stepInputChecksums,
  })
}

async function resealPlanStep(
  plan: OperationPlan,
  stepId: string,
  changes: Partial<OperationPlan["steps"][number]>,
): Promise<OperationPlan> {
  const { planChecksum: _planChecksum, schemaVersion: _schemaVersion, ...input } = plan
  return sealOperationPlan(
    {
      ...input,
      steps: input.steps.map((step) => (step.stepId === stepId ? { ...step, ...changes } : step)),
    },
    digest,
  )
}

describe("saga operation plan compiler", () => {
  it("seals every possible serial path without inventing ordinary dependency edges", async () => {
    const { input, stepInputChecksums } = await fixture()
    const plan = await sealSagaOperationPlan(input, digest)
    const byId = new Map(plan.steps.map((step) => [step.stepId, step] as const))
    const init = byId.get(SAGA_INIT_OPERATION_STEP_ID)
    const settlement = byId.get(SAGA_SETTLE_OPERATION_STEP_ID)
    const termination = byId.get(SAGA_TERMINATION_OPERATION_STEP_ID)
    const chargeForward = byId.get(sagaActionOperationStepId("charge", "forward"))
    const chargeCompensation = byId.get(sagaActionOperationStepId("charge", "compensation"))
    const notifyForward = byId.get(sagaActionOperationStepId("notify", "forward"))
    expect(plan).toMatchObject({
      idempotencyKey: input.operationIdempotencyKey,
      inputChecksum: input.inputChecksum,
      operationId: input.operationId,
      operationType: "saga:checkout@1",
      schemaVersion: 1,
    })
    expect(plan.steps).toHaveLength(6)
    expect(init).toMatchObject({ activation: "required", effectProtocol: "opaque" })
    expect(settlement).toMatchObject({
      activation: "required",
      completionRole: "settlement",
      effectProtocol: "opaque",
      retryClassification: "never",
    })
    expect(termination).toMatchObject({ activation: "conditional", effectProtocol: "opaque" })
    expect(chargeForward).toMatchObject({
      activation: "conditional",
      checkpoint: "reversible",
      dependsOn: [],
      effectProtocol: "saga_receipt",
      idempotencyKey: sagaActionIdempotencyKey("checkout-1", "charge", "forward"),
      inputChecksum: stepInputChecksums.charge,
    })
    expect(chargeCompensation).toMatchObject({
      activation: "conditional",
      checkpoint: "reversible",
      dependsOn: [],
      effectProtocol: "saga_receipt",
      idempotencyKey: sagaActionIdempotencyKey("checkout-1", "charge", "compensation"),
    })
    expect(chargeCompensation?.inputChecksum).not.toBe(stepInputChecksums.charge)
    expect(notifyForward).toMatchObject({
      activation: "conditional",
      checkpoint: "irreversible",
      effectProtocol: "saga_receipt",
      inputChecksum: stepInputChecksums.notify,
    })
    expect(byId.has(sagaActionOperationStepId("notify", "compensation"))).toBe(false)
    for (const step of plan.steps) {
      expect(step.preconditionChecksum).toMatch(/^[0-9a-f]{64}$/u)
      expect(step.postconditionChecksum).toMatch(/^[0-9a-f]{64}$/u)
      expect(Object.isFrozen(step)).toBe(true)
    }
    await expect(sealSagaOperationPlan(input, digest)).resolves.toEqual(plan)
  })

  it("binds descriptor artifacts and inputs without coupling to unrelated registry additions", async () => {
    const { input } = await fixture()
    const original = await sealSagaOperationPlan(input, digest)
    const extendedRegistry = await sealSagaHandlerRegistry(
      [
        ...registrations(),
        { handler: effect, kind: "effect", reference: reference("unrelated.forward", "f") },
      ],
      digest,
    )
    await expect(
      sealSagaOperationPlan({ ...input, registry: extendedRegistry }, digest),
    ).resolves.toEqual(original)
    const changedInput = await sealSagaOperationPlan(
      { ...input, stepInputChecksums: { ...input.stepInputChecksums, charge: "f".repeat(64) } },
      digest,
    )
    expect(changedInput.planChecksum).not.toBe(original.planChecksum)
    const changedCapability = await sealSagaOperationPlan(
      { ...input, capabilitySnapshotChecksum: "a".repeat(64) },
      digest,
    )
    expect(changedCapability.planChecksum).not.toBe(original.planChecksum)
  })

  it("compiles and binds one owned checksum snapshot even when the caller mutates its map", async () => {
    const { input } = await fixture()
    const initialStepInputChecksums = Object.freeze({
      charge: input.stepInputChecksums.charge,
      notify: input.stepInputChecksums.notify,
    })
    const mutableStepInputChecksums = input.stepInputChecksums as Record<string, string>
    let mutated = false
    const mutatingDigest: DigestFunction = async (bytes) => {
      if (!mutated) {
        mutated = true
        mutableStepInputChecksums.charge = "f".repeat(64)
      }
      return digest(bytes)
    }

    const plan = await sealSagaOperationPlan(input, mutatingDigest)
    const saga = sagaForInput(input, { stepInputChecksums: initialStepInputChecksums })
    expect(input.stepInputChecksums.charge).toBe("f".repeat(64))
    expect(
      plan.steps.find((step) => step.stepId === sagaActionOperationStepId("charge", "forward"))
        ?.inputChecksum,
    ).toBe(initialStepInputChecksums.charge)
    expect(() => assertTrustedSagaOperationPlan(plan, saga)).not.toThrow()

    const loaded = await loadOperationPlan(structuredClone(plan), digest)
    await verifySagaOperationPlan(loaded, saga, digest)
    expect(() => assertTrustedSagaOperationPlan(loaded, saga)).not.toThrow()
  })

  it("reads switching registry, descriptor, and scalar accessors exactly once", async () => {
    const { input } = await fixture()
    const otherDescriptor = await descriptor("switched", 2)
    let descriptorReads = 0
    let operationIdReads = 0
    let registryReads = 0
    const switchingInput = { ...input }
    Object.defineProperties(switchingInput, {
      descriptor: {
        enumerable: true,
        get: () => {
          descriptorReads += 1
          return descriptorReads === 1 ? input.descriptor : otherDescriptor
        },
      },
      operationId: {
        enumerable: true,
        get: () => {
          operationIdReads += 1
          return operationIdReads === 1 ? input.operationId : "switched-operation"
        },
      },
      registry: {
        enumerable: true,
        get: () => {
          registryReads += 1
          return registryReads === 1 ? input.registry : ({} as typeof input.registry)
        },
      },
    })

    const plan = await sealSagaOperationPlan(switchingInput, digest)
    expect({ descriptorReads, operationIdReads, registryReads }).toEqual({
      descriptorReads: 1,
      operationIdReads: 1,
      registryReads: 1,
    })
    expect(plan).toMatchObject({
      operationId: input.operationId,
      operationType: "saga:checkout@1",
    })
    expect(() => assertTrustedSagaOperationPlan(plan, sagaForInput(input))).not.toThrow()
  })

  it("normalizes hostile snapshot inputs to one stable configuration error", async () => {
    const { input } = await fixture()
    const throwingChecksums = Object.defineProperty(
      { notify: input.stepInputChecksums.notify },
      "charge",
      {
        enumerable: true,
        get: () => {
          throw new Error("private accessor detail")
        },
      },
    )
    const hostileInputs = [
      { ...input, stepInputChecksums: new Proxy({ ...input.stepInputChecksums }, {}) },
      {
        ...input,
        stepInputChecksums: { ...input.stepInputChecksums, uncloneable: () => undefined },
      },
      { ...input, stepInputChecksums: throwingChecksums },
      new Proxy(input, {
        get(target, property, receiver) {
          if (property === "descriptor") throw new Error("private proxy detail")
          return Reflect.get(target, property, receiver)
        },
      }),
    ]

    for (const hostileInput of hostileInputs) {
      await expect(
        sealSagaOperationPlan(hostileInput as typeof input, digest),
      ).rejects.toMatchObject({
        code: "ConfigurationError",
        message: "Saga operation-plan input could not be snapshotted.",
      })
    }
  })

  it("binds saga-plan provenance to the exact saga and revalidates generic loaded plans", async () => {
    const { input } = await fixture()
    const plan = await sealSagaOperationPlan(input, digest)
    const saga = sagaForInput(input)
    expect(() => assertTrustedSagaOperationPlan(plan, saga)).not.toThrow()

    const loaded = await loadOperationPlan(structuredClone(plan), digest)
    expect(() => assertTrustedSagaOperationPlan(loaded, saga)).toThrowError(
      expect.objectContaining({ code: "OperationInterventionRequiredError" }),
    )
    await verifySagaOperationPlan(loaded, saga, digest)
    expect(() => assertTrustedSagaOperationPlan(loaded, saga)).not.toThrow()

    const clonedPlan = structuredClone(plan)
    const firstStep = clonedPlan.steps[0] as OperationPlan["steps"][number]
    const reorderedStep = Object.fromEntries(
      Object.entries(firstStep).reverse(),
    ) as unknown as OperationPlan["steps"][number]
    const reorderedCandidate: OperationPlan = {
      ...clonedPlan,
      steps: [reorderedStep, ...clonedPlan.steps.slice(1)],
    }
    const reordered = await loadOperationPlan(reorderedCandidate, digest)
    await verifySagaOperationPlan(reordered, saga, digest)
    expect(() => assertTrustedSagaOperationPlan(reordered, saga)).not.toThrow()

    const otherDescriptor = await descriptor("checkout-other", 2)
    const wrongSagas = [
      sagaForInput(input, { descriptor: otherDescriptor }),
      sagaForInput(input, { inputChecksum: "a".repeat(64) }),
      sagaForInput(input, { sagaId: "checkout-other" }),
      sagaForInput(input, {
        stepInputChecksums: { ...input.stepInputChecksums, charge: "b".repeat(64) },
      }),
    ]
    for (const wrongSaga of wrongSagas) {
      expect(() => assertTrustedSagaOperationPlan(plan, wrongSaga)).toThrowError(
        expect.objectContaining({ code: "OperationInterventionRequiredError" }),
      )
      const genericLoaded = await loadOperationPlan(structuredClone(plan), digest)
      await expect(verifySagaOperationPlan(genericLoaded, wrongSaga, digest)).rejects.toMatchObject(
        {
          code: "OperationInterventionRequiredError",
        },
      )
    }
  })

  it("rejects every independently sealed change to canonical saga-derived plan values", async () => {
    const { input } = await fixture()
    const plan = await sealSagaOperationPlan(input, digest)
    const saga = sagaForInput(input)
    const changedPlans = await Promise.all([
      resealPlanStep(plan, SAGA_INIT_OPERATION_STEP_ID, {
        inputChecksum: "a".repeat(64),
      }),
      resealPlanStep(plan, SAGA_TERMINATION_OPERATION_STEP_ID, {
        preconditionChecksum: "b".repeat(64),
      }),
      resealPlanStep(plan, SAGA_SETTLE_OPERATION_STEP_ID, {
        postconditionChecksum: "c".repeat(64),
      }),
      resealPlanStep(plan, sagaActionOperationStepId("charge", "forward"), {
        preconditionChecksum: "d".repeat(64),
      }),
      resealPlanStep(plan, sagaActionOperationStepId("charge", "forward"), {
        postconditionChecksum: "e".repeat(64),
      }),
      resealPlanStep(plan, sagaActionOperationStepId("charge", "compensation"), {
        inputChecksum: "f".repeat(64),
      }),
    ])

    for (const changed of changedPlans) {
      await expect(verifySagaOperationPlan(changed, saga, digest)).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
      expect(() => assertTrustedSagaOperationPlan(changed, saga)).toThrowError(
        expect.objectContaining({ code: "OperationInterventionRequiredError" }),
      )
    }
  })

  it("fails closed for unverified plan objects and invalid verification digests", async () => {
    const { input } = await fixture()
    const plan = await sealSagaOperationPlan(input, digest)
    const saga = sagaForInput(input)

    await expect(
      verifySagaOperationPlan(structuredClone(plan), saga, digest),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

    const loaded = await loadOperationPlan(structuredClone(plan), digest)
    await expect(verifySagaOperationPlan(loaded, saga, () => "bad")).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
    await expect(
      verifySagaOperationPlan(loaded, saga, () => {
        throw new NozzleError("OperationInterventionRequiredError", "caller-controlled detail")
      }),
    ).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
      message: "The saga operation plan could not be integrity-verified.",
    })
    expect(() => assertTrustedSagaOperationPlan(loaded, saga)).toThrowError(
      expect.objectContaining({ code: "OperationInterventionRequiredError" }),
    )
  })

  it("rejects incomplete bindings, untrusted inputs, missing handlers, and invalid digests", async () => {
    const { input, sealedDescriptor } = await fixture()
    for (const stepInputChecksums of [
      { charge: "7".repeat(64) },
      { charge: "7".repeat(64), notify: "8".repeat(64), extra: "f".repeat(64) },
      { charge: "bad", notify: "8".repeat(64) },
      [] as never,
      null as never,
      undefined as never,
    ]) {
      await expect(
        sealSagaOperationPlan({ ...input, stepInputChecksums }, digest),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    }
    const partialRegistry = await sealSagaHandlerRegistry(registrations().slice(0, 5), digest)
    await expect(
      sealSagaOperationPlan({ ...input, registry: partialRegistry }, digest),
    ).rejects.toThrow(/observation handler/u)
    await expect(
      sealSagaOperationPlan(
        { ...input, descriptor: JSON.parse(JSON.stringify(sealedDescriptor)) },
        digest,
      ),
    ).rejects.toThrow(/sealed or loaded/u)
    await expect(
      sealSagaOperationPlan({ ...input, registry: {} as never }, digest),
    ).rejects.toThrow(/checksummed before use/u)
    for (const invalid of [
      { capabilitySnapshotChecksum: "bad" },
      { inputChecksum: "bad" },
      { leaseKey: "" },
      { operationId: "" },
      { operationIdempotencyKey: "" },
      { sagaId: "" },
      { sagaId: "x".repeat(513) },
    ]) {
      await expect(sealSagaOperationPlan({ ...input, ...invalid }, digest)).rejects.toMatchObject({
        code: "ConfigurationError",
      })
    }
    await expect(sealSagaOperationPlan(input, undefined as never)).rejects.toThrow(
      /digest is required/u,
    )
    await expect(sealSagaOperationPlan(input, () => "bad")).rejects.toThrow(/value checksum/u)
  })
})
