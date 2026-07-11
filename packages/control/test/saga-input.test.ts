import { type DigestFunction, sealSagaDescriptor } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { loadSagaInvocationInput, sealSagaInvocationInput } from "../src/saga-input.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

async function descriptor(id = "durable-input") {
  return sealSagaDescriptor(
    {
      descriptorId: id,
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 10,
          compensationAction: {
            actionId: `${id}.compensate`,
            artifactChecksum: "3".repeat(64),
            version: 1,
          },
          compensationObservation: {
            actionId: `${id}.observe-compensation`,
            artifactChecksum: "4".repeat(64),
            version: 1,
          },
          forwardAction: {
            actionId: `${id}.forward`,
            artifactChecksum: "1".repeat(64),
            version: 1,
          },
          forwardObservation: {
            actionId: `${id}.observe-forward`,
            artifactChecksum: "2".repeat(64),
            version: 1,
          },
          inputSchemaChecksum: "5".repeat(64),
          irreversible: false,
          maxAttempts: 3,
          maxRetryDelayMs: 100,
          outputSchemaChecksum: "6".repeat(64),
          stepId: "write",
          timeoutMs: 1_000,
        },
      ],
      version: 1,
    },
    digest,
  )
}

describe("durable saga invocation input", () => {
  it("seals and reconstructs canonical overall and per-step input from the operation ledger", async () => {
    const sealedDescriptor = await descriptor()
    const stepInputs = Object.assign(Object.create(null) as Record<string, string>, {
      write: ' { "tenant": "a", "nested": { "z": 2, "a": [true, null] } } ',
    })
    const sealed = await sealSagaInvocationInput(
      {
        descriptor: sealedDescriptor,
        inputJson: ' { "request": 7, "mode": "write" } ',
        sagaId: "saga-input-1",
        stepInputJsons: stepInputs,
      },
      digest,
    )
    expect(sealed).toMatchObject({
      descriptorChecksum: sealedDescriptor.descriptorChecksum,
      descriptorId: sealedDescriptor.descriptorId,
      descriptorVersion: 1,
      inputJson: '{"mode":"write","request":7}',
      sagaId: "saga-input-1",
      schemaVersion: 1,
      stepInputJsons: { write: '{"nested":{"a":[true,null],"z":2},"tenant":"a"}' },
    })
    expect(sealed.inputChecksum).toMatch(/^[0-9a-f]{64}$/u)
    expect(sealed.stepInputChecksums.write).toMatch(/^[0-9a-f]{64}$/u)
    expect(Object.isFrozen(sealed)).toBe(true)
    expect(Object.isFrozen(sealed.stepInputJsons)).toBe(true)
    expect(Object.isFrozen(sealed.stepInputChecksums)).toBe(true)
    await expect(
      loadSagaInvocationInput(sealed.operationInputJson, sealedDescriptor, digest),
    ).resolves.toEqual(sealed)
    expect(await digest(new TextEncoder().encode(sealed.operationInputJson))).toBe(
      sealed.inputChecksum,
    )
  })

  it("rejects malformed source input before it reaches the operation ledger", async () => {
    const sealedDescriptor = await descriptor()
    const base = {
      descriptor: sealedDescriptor,
      inputJson: "{}",
      sagaId: "saga-input-1",
      stepInputJsons: { write: "{}" },
    }
    for (const override of [
      { inputJson: "" },
      { inputJson: "not-json" },
      { inputJson: JSON.stringify("x".repeat(1024 * 1024)) },
      { sagaId: "" },
      { sagaId: "x".repeat(513) },
      { stepInputJsons: [] as never },
      { stepInputJsons: {} },
      { stepInputJsons: { extra: "{}", write: "{}" } },
      { stepInputJsons: { write: "not-json" } },
    ]) {
      await expect(sealSagaInvocationInput({ ...base, ...override }, digest)).rejects.toMatchObject(
        {
          code: "ConfigurationError",
        },
      )
    }
    await expect(sealSagaInvocationInput(base, undefined as never)).rejects.toThrow(
      /digest is required/u,
    )
    let digestCalls = 0
    await expect(
      sealSagaInvocationInput(base, () => {
        digestCalls += 1
        return digestCalls === 1 ? "a".repeat(64) : "bad"
      }),
    ).rejects.toThrow(/lowercase SHA/u)
    expect(digestCalls).toBe(2)
    await expect(
      sealSagaInvocationInput(
        {
          ...base,
          inputJson: JSON.stringify("x".repeat(600_000)),
          stepInputJsons: { write: JSON.stringify("y".repeat(600_000)) },
        },
        digest,
      ),
    ).rejects.toThrow(/envelope exceeds/u)
    await expect(
      sealSagaInvocationInput(
        { ...base, descriptor: JSON.parse(JSON.stringify(sealedDescriptor)) },
        digest,
      ),
    ).rejects.toThrow(/sealed or loaded/u)
  })

  it("fails closed on persisted envelope drift, corruption, and descriptor substitution", async () => {
    const sealedDescriptor = await descriptor()
    const sealed = await sealSagaInvocationInput(
      {
        descriptor: sealedDescriptor,
        inputJson: "{}",
        sagaId: "saga-input-1",
        stepInputJsons: { write: '{"value":1}' },
      },
      digest,
    )
    const body = JSON.parse(sealed.operationInputJson) as Record<string, unknown>
    const candidates = [
      "not-json",
      ` ${sealed.operationInputJson}`,
      "[]",
      JSON.stringify({ ...body, extra: true }),
      JSON.stringify({ ...body, schemaVersion: 2 }),
      JSON.stringify({ ...body, descriptorChecksum: "f".repeat(64) }),
      JSON.stringify({ ...body, descriptorId: "other" }),
      JSON.stringify({ ...body, descriptorVersion: 2 }),
      JSON.stringify({ ...body, sagaId: "" }),
      JSON.stringify({ ...body, stepInputs: null }),
      JSON.stringify({ ...body, stepInputs: {} }),
    ]
    for (const candidate of candidates) {
      await expect(
        loadSagaInvocationInput(candidate, sealedDescriptor, digest),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
    const otherDescriptor = await descriptor("other-descriptor")
    await expect(
      loadSagaInvocationInput(sealed.operationInputJson, otherDescriptor, digest),
    ).rejects.toThrow(/contradicts its descriptor/u)
    await expect(
      loadSagaInvocationInput(sealed.operationInputJson, sealedDescriptor, undefined as never),
    ).rejects.toThrow(/digest is required/u)
    await expect(
      loadSagaInvocationInput(
        sealed.operationInputJson,
        JSON.parse(JSON.stringify(sealedDescriptor)),
        digest,
      ),
    ).rejects.toThrow(/sealed or loaded/u)
  })

  it("rejects deeply nested JSON that cannot be canonicalized safely", async () => {
    const sealedDescriptor = await descriptor()
    const nested = `${"[".repeat(20_000)}0${"]".repeat(20_000)}`
    await expect(
      sealSagaInvocationInput(
        {
          descriptor: sealedDescriptor,
          inputJson: nested,
          sagaId: "saga-input-1",
          stepInputJsons: { write: "{}" },
        },
        digest,
      ),
    ).rejects.toThrow(/canonicalized safely/u)
  })
})
