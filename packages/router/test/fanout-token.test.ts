import type { NozzleError } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import {
  createFanoutContinuation,
  type FanoutContinuationState,
  type FanoutCurrentIdentity,
} from "../src/fanout.js"
import {
  createFanoutToken,
  decodeFanoutToken,
  MAX_FANOUT_TOKEN_BYTES,
  NOZZLE_FANOUT_TOKEN_VERSION,
} from "../src/fanout-token.js"

const secret = Uint8Array.from({ length: 32 }, (_, index) => index)
const checksums = Object.freeze({
  manifestChecksum: "11".repeat(32),
  queryChecksum: "22".repeat(32),
  schemaChecksum: "33".repeat(32),
})
const current = Object.freeze({ ...checksums, shardIds: ["a"] }) satisfies FanoutCurrentIdentity

function state(): FanoutContinuationState {
  return createFanoutContinuation({
    budget: {
      maxBufferedBytes: 1_000,
      maxBufferedRows: 100,
      maxBytes: 1_000,
      maxConcurrency: 1,
      maxCostMicros: 1_000,
      maxCpuMs: 1_000,
      maxPages: 10,
      maxRows: 100,
      maxShards: 1,
      maxSubrequests: 100,
      timeoutMs: 1_000,
    },
    deadlineAtMs: 10_000,
    expiresAtMs: 9_000,
    ...checksums,
    nowMs: 1_000,
    order: [{ direction: "asc", immutable: true, kind: "string", nulls: "last" }],
    partialPolicy: "fail",
    shardIds: ["a"],
  })
}

function expectCode(promise: Promise<unknown>, code: NozzleError["code"]): Promise<void> {
  return expect(promise).rejects.toMatchObject({ code })
}

function ownedBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

function encode(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "")
}

async function encryptedRaw(bytes: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey("raw", ownedBuffer(secret), "AES-GCM", false, [
    "encrypt",
  ])
  const nonce = Uint8Array.from({ length: 12 }, (_, index) => index + 1)
  const additionalData = new TextEncoder().encode("nozzle-fanout:v1")
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      {
        additionalData: ownedBuffer(additionalData),
        iv: ownedBuffer(nonce),
        name: "AES-GCM",
        tagLength: 128,
      },
      key,
      ownedBuffer(bytes),
    ),
  )
  return `nzf1.${encode(nonce)}.${encode(ciphertext)}`
}

describe("encrypted fan-out continuation tokens", () => {
  it("round-trips sealed state without exposing raw ordering values", async () => {
    const initial = state()
    const sensitive = "raw-order-value-must-stay-private"
    const positioned = {
      ...initial,
      positions: { a: { orderValues: [sensitive], primaryKey: "private-primary-key" } },
    }
    const first = await createFanoutToken({ key: secret, nowMs: 2_000, state: positioned })
    const second = await createFanoutToken({ key: secret, nowMs: 2_000, state: positioned })

    expect(first).toMatch(/^nzf1\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]+$/u)
    expect(first).not.toBe(second)
    expect(first).not.toContain(sensitive)
    expect(first).not.toContain("private-primary-key")
    await expect(
      decodeFanoutToken({ current, key: secret, nowMs: 2_000, token: first }),
    ).resolves.toEqual(positioned)
    expect(NOZZLE_FANOUT_TOKEN_VERSION).toBe(1)
  })

  it("detects tampering, wrong keys, and identity drift", async () => {
    const token = await createFanoutToken({ key: secret, nowMs: 2_000, state: state() })
    const last = token.at(-1) as string
    const tampered = `${token.slice(0, -1)}${last === "A" ? "B" : "A"}`
    await expectCode(
      decodeFanoutToken({ current, key: secret, nowMs: 2_000, token: tampered }),
      "SessionTokenInvalidError",
    )
    await expectCode(
      decodeFanoutToken({
        current,
        key: Uint8Array.from(secret, (value) => value ^ 1),
        nowMs: 2_000,
        token,
      }),
      "SessionTokenInvalidError",
    )
    await expectCode(
      decodeFanoutToken({
        current: { ...current, manifestChecksum: "44".repeat(32) },
        key: secret,
        nowMs: 2_000,
        token,
      }),
      "RouteVersionConflictError",
    )
  })

  it("rejects malformed, noncanonical, unsupported, and oversized tokens", async () => {
    for (const token of [
      "",
      "nzf2.a.b",
      "nzf1.a.b.c",
      "nzf1.!.abcd",
      "nzf1.a.abcd",
      "nzf1.AB.AA",
      "nzf1.AA.AA",
      "x".repeat(MAX_FANOUT_TOKEN_BYTES + 1),
    ]) {
      await expectCode(
        decodeFanoutToken({ current, key: secret, nowMs: 2_000, token }),
        "SessionTokenInvalidError",
      )
    }
    await expectCode(
      decodeFanoutToken({ current, key: secret, nowMs: 2_000, token: null as never }),
      "SessionTokenInvalidError",
    )
  })

  it("maps encrypted invalid UTF-8, JSON, and state to stable token errors", async () => {
    for (const bytes of [
      Uint8Array.of(255),
      new TextEncoder().encode("not-json"),
      new TextEncoder().encode("{}"),
    ]) {
      await expectCode(
        decodeFanoutToken({
          current,
          key: secret,
          nowMs: 2_000,
          token: await encryptedRaw(bytes),
        }),
        "SessionTokenInvalidError",
      )
    }
  })

  it("enforces expiry, deadline, current-time, and token-size bounds", async () => {
    const token = await createFanoutToken({ key: secret, nowMs: 2_000, state: state() })
    await expectCode(
      decodeFanoutToken({ current, key: secret, nowMs: 9_000, token }),
      "SessionTokenInvalidError",
    )
    await expectCode(
      decodeFanoutToken({ current, key: secret, nowMs: 10_000, token }),
      "CapacityGuardError",
    )
    await expectCode(
      createFanoutToken({ key: secret, nowMs: 9_000, state: state() }),
      "SessionTokenInvalidError",
    )
    await expectCode(
      createFanoutToken({ key: secret, nowMs: 10_000, state: state() }),
      "CapacityGuardError",
    )
    await expectCode(
      createFanoutToken({ key: secret, nowMs: -1, state: state() }),
      "ConfigurationError",
    )
    await expectCode(
      decodeFanoutToken({ current, key: secret, nowMs: -1, token }),
      "ConfigurationError",
    )

    const initial = state()
    const oversized = {
      ...initial,
      positions: { a: { orderValues: ["x".repeat(MAX_FANOUT_TOKEN_BYTES)], primaryKey: "a" } },
    }
    await expectCode(
      createFanoutToken({ key: secret, nowMs: 2_000, state: oversized }),
      "CapacityGuardError",
    )
  })

  it("validates raw and nonextractable AES-256 key material", async () => {
    await expectCode(
      createFanoutToken({ key: new Uint8Array(31), nowMs: 2_000, state: state() }),
      "ConfigurationError",
    )
    const key = await crypto.subtle.importKey("raw", ownedBuffer(secret), "AES-GCM", false, [
      "decrypt",
      "encrypt",
    ])
    const token = await createFanoutToken({ key, nowMs: 2_000, state: state() })
    await expect(decodeFanoutToken({ current, key, nowMs: 2_000, token })).resolves.toEqual(state())

    const wrongAlgorithm = await crypto.subtle.importKey(
      "raw",
      ownedBuffer(secret),
      { hash: "SHA-256", name: "HMAC" },
      false,
      ["sign"],
    )
    const wrongLength = await crypto.subtle.importKey(
      "raw",
      ownedBuffer(secret.slice(0, 16)),
      "AES-GCM",
      false,
      ["encrypt"],
    )
    const extractable = await crypto.subtle.importKey("raw", ownedBuffer(secret), "AES-GCM", true, [
      "encrypt",
    ])
    const decryptOnly = await crypto.subtle.importKey(
      "raw",
      ownedBuffer(secret),
      "AES-GCM",
      false,
      ["decrypt"],
    )
    for (const invalid of [wrongAlgorithm, wrongLength, extractable, decryptOnly, {} as never]) {
      await expectCode(
        createFanoutToken({ key: invalid, nowMs: 2_000, state: state() }),
        "ConfigurationError",
      )
    }
  })
})
