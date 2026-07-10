import {
  type DigestFunction,
  type SagaActionReference,
  sagaActionIdempotencyKey,
  sealSagaDescriptor,
} from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { sealSagaOperationPlan } from "../src/saga-plan.js"
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

async function descriptor() {
  return sealSagaDescriptor(
    {
      descriptorId: "checkout",
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
      version: 1,
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

  it("rejects incomplete bindings, untrusted inputs, missing handlers, and invalid digests", async () => {
    const { input, sealedDescriptor } = await fixture()
    for (const stepInputChecksums of [
      { charge: "7".repeat(64) },
      { charge: "7".repeat(64), notify: "8".repeat(64), extra: "f".repeat(64) },
      { charge: "bad", notify: "8".repeat(64) },
      [] as never,
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
