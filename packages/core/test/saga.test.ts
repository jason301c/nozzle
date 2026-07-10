import fc from "fast-check"
import { describe, expect, it } from "vitest"
import type { DigestFunction } from "../src/operation.js"
import {
  assertTrustedSagaDescriptor,
  encodeSagaDescriptorChecksumInput,
  loadSagaDescriptor,
  MAX_SAGA_STEPS,
  type SagaActionReference,
  type SagaDescriptorInput,
  type SagaStepDescriptorInput,
  sagaActionKey,
  sealSagaDescriptor,
  verifySagaDescriptorChecksum,
} from "../src/saga.js"

const digest: DigestFunction = async (input) => {
  const owned = new Uint8Array(input.byteLength)
  owned.set(input)
  const output = new Uint8Array(await crypto.subtle.digest("SHA-256", owned.buffer))
  return [...output].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function checksum(character: string): string {
  return character.repeat(64)
}

function action(actionId: string, character = "a", version = 1): SagaActionReference {
  return { actionId, artifactChecksum: checksum(character), version }
}

function step(
  stepId: string,
  overrides: Partial<SagaStepDescriptorInput> = {},
): SagaStepDescriptorInput {
  return {
    authorizationPolicyChecksum: null,
    baseRetryDelayMs: 100,
    compensationAction: action(`${stepId}.compensate`, "c"),
    compensationObservation: action(`${stepId}.observe-compensation`, "d"),
    forwardAction: action(`${stepId}.forward`, "a"),
    forwardObservation: action(`${stepId}.observe-forward`, "b"),
    inputSchemaChecksum: checksum("1"),
    irreversible: false,
    maxAttempts: 3,
    maxRetryDelayMs: 1_000,
    outputSchemaChecksum: checksum("2"),
    stepId,
    timeoutMs: 10_000,
    ...overrides,
  }
}

function irreversibleStep(stepId: string): SagaStepDescriptorInput {
  return step(stepId, {
    authorizationPolicyChecksum: checksum("e"),
    compensationAction: null,
    compensationObservation: null,
    irreversible: true,
  })
}

function input(steps: readonly SagaStepDescriptorInput[] = [step("reserve")]): SagaDescriptorInput {
  return { descriptorId: "transfer", steps, version: 1 }
}

function expectConfiguration(callback: () => unknown): void {
  expect(callback).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
}

async function expectIntervention(promise: Promise<unknown>): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
}

describe("immutable saga descriptors", () => {
  it("seals a deterministic serial descriptor without retaining caller ownership", async () => {
    const mutableAction = action("reserve.forward") as { actionId: string } & SagaActionReference
    const mutableStep = step("reserve", { forwardAction: mutableAction })
    const pending = sealSagaDescriptor(input([mutableStep, irreversibleStep("commit")]), digest)
    mutableAction.actionId = "mutated"
    const descriptor = await pending
    const nullPrototype = Object.assign(Object.create(null), input()) as SagaDescriptorInput
    await expect(sealSagaDescriptor(nullPrototype, digest)).resolves.toMatchObject({
      descriptorId: "transfer",
    })
    const repeated = await sealSagaDescriptor(
      input([step("reserve"), irreversibleStep("commit")]),
      digest,
    )

    expect(descriptor).toEqual(repeated)
    expect(descriptor).toMatchObject({
      cancellationMode: "compensate_confirmed",
      descriptorId: "transfer",
      executionMode: "serial",
      schemaVersion: 1,
      version: 1,
    })
    expect(descriptor.steps[0]?.forwardAction.actionId).toBe("reserve.forward")
    expect(Object.isFrozen(descriptor)).toBe(true)
    expect(Object.isFrozen(descriptor.steps)).toBe(true)
    expect(Object.isFrozen(descriptor.steps[0]?.forwardAction)).toBe(true)
    await expect(verifySagaDescriptorChecksum(descriptor, digest)).resolves.toBe(true)
    expect(() => assertTrustedSagaDescriptor(descriptor)).not.toThrow()
  })

  it("makes version, step order, action version, policy, and artifact changes checksum-visible", async () => {
    const baseline = await sealSagaDescriptor(input([step("a"), step("b")]), digest)
    const variants = [
      { ...input([step("a"), step("b")]), version: 2 },
      input([step("b"), step("a")]),
      input([step("a", { forwardAction: action("a.forward", "a", 2) }), step("b")]),
      input([step("a", { maxAttempts: 4 }), step("b")]),
      input([step("a", { inputSchemaChecksum: checksum("3") }), step("b")]),
    ]
    for (const variant of variants) {
      await expect(sealSagaDescriptor(variant, digest)).resolves.not.toMatchObject({
        descriptorChecksum: baseline.descriptorChecksum,
      })
    }
  })

  it("round-trips persisted descriptors and rejects envelope, body, and checksum corruption", async () => {
    const descriptor = await sealSagaDescriptor(input([step("a"), irreversibleStep("b")]), digest)
    await expect(loadSagaDescriptor(structuredClone(descriptor), digest)).resolves.toEqual(
      descriptor,
    )
    for (const malformed of [
      null,
      [],
      new Date(),
      { ...descriptor, extra: true },
      { ...descriptor, schemaVersion: 2 },
      { ...descriptor, executionMode: "parallel" },
      { ...descriptor, cancellationMode: "drop" },
    ]) {
      await expectIntervention(loadSagaDescriptor(malformed, digest))
    }
    await expectIntervention(loadSagaDescriptor({ ...descriptor, descriptorId: "" }, digest))
    await expectIntervention(
      loadSagaDescriptor({ ...descriptor, descriptorChecksum: checksum("f") }, digest),
    )
  })

  it("rejects malformed descriptors, IDs, checksums, and action references", async () => {
    const invalid: unknown[] = [
      null,
      [],
      { ...input(), extra: true },
      { ...input(), descriptorId: "" },
      { ...input(), descriptorId: " bad " },
      { ...input(), descriptorId: "bad\nvalue" },
      { ...input(), descriptorId: "\ud800" },
      { ...input(), descriptorId: "\udc00" },
      { ...input(), descriptorId: "😀".repeat(64) },
      { ...input(), version: 0 },
      { ...input(), version: 1.5 },
      { ...input(), steps: [] },
      { ...input(), steps: new Array(MAX_SAGA_STEPS + 1).fill(step("a")) },
      input([step("a"), step("a")]),
      input([step("a", { inputSchemaChecksum: "bad" })]),
      input([step("a", { forwardAction: null as never })]),
      input([step("a", { forwardAction: { ...action("a"), extra: true } as never })]),
      input([step("a", { forwardAction: action("", "a") })]),
      input([step("a", { forwardAction: action("a", "g") })]),
      input([step("a", { forwardAction: action("a", "a", 0) })]),
      input([{ ...step("a"), extra: true } as never]),
    ]
    for (const candidate of invalid) {
      await expect(
        sealSagaDescriptor(candidate as SagaDescriptorInput, digest),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    }
  })

  it("enforces bounded retry, timeout, reversibility, and last-only irreversible policy", async () => {
    const invalid = [
      input([step("a", { maxAttempts: 0 })]),
      input([step("a", { maxAttempts: 21 })]),
      input([step("a", { baseRetryDelayMs: -1 })]),
      input([step("a", { maxRetryDelayMs: 86_400_001 })]),
      input([step("a", { baseRetryDelayMs: 2, maxRetryDelayMs: 1 })]),
      input([step("a", { timeoutMs: 0 })]),
      input([irreversibleStep("a"), step("b")]),
      input(
        [irreversibleStep("a")].map((value) => ({ ...value, compensationAction: action("bad") })),
      ),
      input([step("a", { compensationAction: null })]),
      input([step("a", { compensationObservation: null })]),
      input([step("a", { authorizationPolicyChecksum: checksum("e") })]),
      input(
        [irreversibleStep("a")].map((value) => ({ ...value, authorizationPolicyChecksum: null })),
      ),
      input([step("a", { irreversible: "yes" as never })]),
    ]
    for (const candidate of invalid) {
      await expect(sealSagaDescriptor(candidate, digest)).rejects.toMatchObject({
        code: "ConfigurationError",
      })
    }
  })

  it("validates digest output, persisted digest failures, and untrusted descriptors", async () => {
    await expect(sealSagaDescriptor(input(), null as never)).rejects.toMatchObject({
      code: "ConfigurationError",
    })
    await expect(sealSagaDescriptor(input(), () => "bad")).rejects.toMatchObject({
      code: "ConfigurationError",
    })
    const descriptor = await sealSagaDescriptor(input(), digest)
    await expect(
      verifySagaDescriptorChecksum({ ...descriptor, descriptorChecksum: checksum("f") }, digest),
    ).resolves.toBe(false)
    await expect(verifySagaDescriptorChecksum({ ...descriptor, steps: [] }, digest)).resolves.toBe(
      false,
    )
    await expectIntervention(loadSagaDescriptor(descriptor, () => "bad"))
    expectConfiguration(() => assertTrustedSagaDescriptor(structuredClone(descriptor)))
  })

  it("encodes canonical checksum input and canonical versioned action keys", async () => {
    const encoded = encodeSagaDescriptorChecksumInput(input())
    expect(new TextDecoder().decode(encoded)).toContain('"domain":"nozzle.saga-descriptor.v1"')
    expect(sagaActionKey(action("transfer.forward", "a", 2))).toBe(
      `transfer.forward@2:${checksum("a")}`,
    )
    for (const invalid of [
      null,
      { ...action("a"), actionId: "" },
      { ...action("a"), artifactChecksum: "bad" },
      { ...action("a"), version: 0 },
    ]) {
      expectConfiguration(() => sagaActionKey(invalid as SagaActionReference))
    }
  })

  it("has deterministic checksums for generated valid serial descriptors", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.stringMatching(/^[a-z]{1,8}$/u), { maxLength: 8, minLength: 1 }),
        async (rawIds) => {
          const ids = [...new Set(rawIds)]
          fc.pre(ids.length > 0)
          const candidate = input(ids.map((id) => step(id)))
          const first = await sealSagaDescriptor(candidate, digest)
          const second = await sealSagaDescriptor(structuredClone(candidate), digest)
          expect(second.descriptorChecksum).toBe(first.descriptorChecksum)
        },
      ),
      { numRuns: 100 },
    )
  })

  it("copies checksum bytes before an asynchronous digest observes them", async () => {
    let observed: Uint8Array | undefined
    let release: (() => void) | undefined
    const waitingDigest: DigestFunction = async (bytes) => {
      await new Promise<void>((resolve) => {
        release = resolve
      })
      observed = bytes
      return checksum("a")
    }
    const candidate = input()
    const expected = encodeSagaDescriptorChecksumInput(candidate)
    const pending = sealSagaDescriptor(candidate, waitingDigest)
    const exposed = encodeSagaDescriptorChecksumInput(candidate)
    exposed.fill(0)
    release?.()
    await pending
    expect(observed).toEqual(expected)
  })
})
