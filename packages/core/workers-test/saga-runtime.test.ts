import { describe, expect, it } from "vitest"
import type { DigestFunction } from "../src/operation.js"
import { loadSagaDescriptor, sealSagaDescriptor } from "../src/saga.js"

const digest: DigestFunction = async (input) => {
  const owned = new Uint8Array(input.byteLength)
  owned.set(input)
  const output = new Uint8Array(await crypto.subtle.digest("SHA-256", owned.buffer))
  return [...output].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

describe("saga descriptors in workerd", () => {
  it("seals and reloads versioned action references with Web Crypto", async () => {
    const reference = (actionId: string, character: string) => ({
      actionId,
      artifactChecksum: character.repeat(64),
      version: 1,
    })
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: "transfer",
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 100,
            compensationAction: reference("reserve.compensate", "c"),
            compensationObservation: reference("reserve.observe-compensation", "d"),
            forwardAction: reference("reserve.forward", "a"),
            forwardObservation: reference("reserve.observe-forward", "b"),
            inputSchemaChecksum: "11".repeat(32),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 1_000,
            outputSchemaChecksum: "22".repeat(32),
            stepId: "reserve",
            timeoutMs: 10_000,
          },
        ],
        version: 1,
      },
      digest,
    )

    await expect(loadSagaDescriptor(structuredClone(descriptor), digest)).resolves.toEqual(
      descriptor,
    )
  })
})
