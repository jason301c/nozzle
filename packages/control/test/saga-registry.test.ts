import { type DigestFunction, type SagaActionReference, sealSagaDescriptor } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import {
  assertTrustedSagaHandlerRegistry,
  type SagaHandlerRegistration,
  SagaHandlerRegistry,
  sealSagaHandlerRegistry,
} from "../src/saga-registry.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function reference(actionId: string, byte: string, version = 1): SagaActionReference {
  return { actionId, artifactChecksum: byte.repeat(64), version }
}

const references = {
  compensate: reference("transfer.compensate", "c"),
  forward: reference("transfer.forward", "a"),
  observeCompensation: reference("transfer.observe-compensation", "d"),
  observeForward: reference("transfer.observe-forward", "b"),
}

const effect = () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" as const })
const observation = () => ({ evidenceJson: "{}", outputJson: "{}", state: "applied" as const })

function registrations(): SagaHandlerRegistration[] {
  return [
    { handler: observation, kind: "observation", reference: references.observeForward },
    { handler: effect, kind: "effect", reference: references.compensate },
    {
      handler: observation,
      kind: "observation",
      reference: references.observeCompensation,
    },
    { handler: effect, kind: "effect", reference: references.forward },
  ]
}

async function descriptor() {
  return sealSagaDescriptor(
    {
      descriptorId: "transfer",
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 10,
          compensationAction: references.compensate,
          compensationObservation: references.observeCompensation,
          forwardAction: references.forward,
          forwardObservation: references.observeForward,
          inputSchemaChecksum: "1".repeat(64),
          irreversible: false,
          maxAttempts: 3,
          maxRetryDelayMs: 100,
          outputSchemaChecksum: "2".repeat(64),
          stepId: "transfer",
          timeoutMs: 1_000,
        },
      ],
      version: 1,
    },
    digest,
  )
}

describe("sealed saga handler registry", () => {
  it("binds immutable action versions to kinds, artifacts, and a deterministic manifest", async () => {
    const registry = await sealSagaHandlerRegistry(registrations(), digest)
    const reordered = await sealSagaHandlerRegistry([...registrations()].reverse(), digest)
    expect(registry.manifest).toEqual(reordered.manifest)
    expect(registry.manifest.handlers.map((entry) => entry.actionKey)).toEqual(
      [...registry.manifest.handlers.map((entry) => entry.actionKey)].sort(),
    )
    expect(registry.manifest.manifestChecksum).toMatch(/^[0-9a-f]{64}$/u)
    expect(Object.isFrozen(registry)).toBe(true)
    expect(Object.isFrozen(registry.manifest)).toBe(true)
    expect(Object.isFrozen(registry.manifest.handlers)).toBe(true)
    expect(registry.effect(references.forward)).toBe(effect)
    expect(registry.observation(references.observeForward)).toBe(observation)
    const sealedDescriptor = await descriptor()
    expect(() => registry.assertDescriptor(sealedDescriptor)).not.toThrow()
    expect(() => assertTrustedSagaHandlerRegistry(registry)).not.toThrow()
    await expect(
      Promise.resolve(
        registry.effect(references.forward)({
          action: references.forward,
          attemptId: "attempt",
          idempotencyKey: "key",
          inputJson: "{}",
          operationId: "operation",
          phase: "forward",
          proof: {
            acquisitionId: "acquisition",
            fencingToken: 1,
            holderId: "holder",
            leaseKey: "lease",
          },
          sagaId: "saga",
          signal: new AbortController().signal,
          stepId: "transfer",
          timeoutMs: 1_000,
        }),
      ),
    ).resolves.toMatchObject({ state: "confirmed" })
  })

  it("rejects missing handlers, kind mismatches, direct construction, and untrusted descriptors", async () => {
    const registry = await sealSagaHandlerRegistry(registrations(), digest)
    expect(() => registry.effect(references.observeForward)).toThrow(/effect handler/u)
    expect(() => registry.observation(references.forward)).toThrow(/observation handler/u)
    const partial = await sealSagaHandlerRegistry(registrations().slice(1), digest)
    const sealedDescriptor = await descriptor()
    expect(() => partial.assertDescriptor(sealedDescriptor)).toThrow(/observation handler/u)
    const untrusted = JSON.parse(JSON.stringify(sealedDescriptor))
    expect(() => registry.assertDescriptor(untrusted)).toThrow(/sealed or loaded/u)
    expect(
      () => new SagaHandlerRegistry(Symbol("forged"), new Map(), new Map(), registry.manifest),
    ).toThrow(/cannot be constructed directly/u)
    expect(() => assertTrustedSagaHandlerRegistry({} as SagaHandlerRegistry)).toThrow(
      /checksummed before use/u,
    )
  })

  it("rejects ambiguous versions and malformed registry inputs", async () => {
    await expect(
      sealSagaHandlerRegistry(
        [
          ...registrations(),
          {
            handler: effect,
            kind: "effect",
            reference: { ...references.forward, artifactChecksum: "e".repeat(64) },
          },
        ],
        digest,
      ),
    ).rejects.toThrow(/exactly one registered artifact/u)
    await expect(sealSagaHandlerRegistry([], digest)).rejects.toThrow(/between 1 and 2048/u)
    await expect(
      sealSagaHandlerRegistry(
        Array.from({ length: 2_049 }, () => registrations()[0] as SagaHandlerRegistration),
        digest,
      ),
    ).rejects.toThrow(/between 1 and 2048/u)
    await expect(
      sealSagaHandlerRegistry(
        [{ handler: effect, kind: "bad", reference: references.forward } as never],
        digest,
      ),
    ).rejects.toThrow(/registration is malformed/u)
    await expect(
      sealSagaHandlerRegistry(
        [{ handler: "bad", kind: "effect", reference: references.forward } as never],
        digest,
      ),
    ).rejects.toThrow(/registration is malformed/u)
    await expect(
      sealSagaHandlerRegistry(
        [
          {
            handler: effect,
            kind: "effect",
            reference: { ...references.forward, artifactChecksum: "bad" },
          },
        ],
        digest,
      ),
    ).rejects.toThrow(/lowercase SHA-256/u)
    await expect(sealSagaHandlerRegistry(registrations(), undefined as never)).rejects.toThrow(
      /digest is required/u,
    )
    await expect(sealSagaHandlerRegistry(registrations(), () => "bad")).rejects.toThrow(
      /manifest checksum/u,
    )
  })
})
