import { describe, expect, it, vi } from "vitest"
import {
  createSessionToken,
  decodeSessionToken,
  NOZZLE_SESSION_TOKEN_VERSION,
  resolveRouteAwareSession,
} from "../src/session.js"

const secret = Uint8Array.from({ length: 32 }, (_, index) => index)
const payload = {
  d1Bookmark: "bookmark-a",
  fleetId: "fleet-a",
  issuedAtMs: 1_000,
  routeEpoch: 7,
  shardId: "shard-a",
} as const
const route = {
  bucketId: 42,
  partitionDigestHex: "11".repeat(32),
  partitionValue: "workspace-a",
  routeEpoch: 7,
  shardId: "shard-a",
} as const

function encode(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "")
}

function ownedBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

async function signedRaw(body: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    secret,
    { hash: "SHA-256", name: "HMAC" },
    false,
    ["sign"],
  )
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, ownedBuffer(body)))
  return `nz1.${encode(body)}.${encode(signature)}`
}

async function signedJson(value: unknown): Promise<string> {
  return signedRaw(new TextEncoder().encode(JSON.stringify(value)))
}

describe("route-aware Nozzle session tokens", () => {
  it("round-trips a deterministic, bounded, integrity-protected payload", async () => {
    const token = await createSessionToken(secret, payload)
    expect(token).toMatch(/^nz1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u)
    await expect(decodeSessionToken(secret, token)).resolves.toEqual({
      ...payload,
      version: NOZZLE_SESSION_TOKEN_VERSION,
    })
    await expect(createSessionToken(secret, payload)).resolves.toBe(token)
    await expect(createSessionToken(secret, { ...payload, fleetId: "fleet-😀" })).resolves.toMatch(
      /^nz1\./u,
    )
  })

  it("rejects tampering, wrong secrets, malformed segments, versions, and size abuse", async () => {
    const token = await createSessionToken(secret, payload)
    const [prefix, body, signature] = token.split(".") as [string, string, string]
    await expect(
      decodeSessionToken(secret, `${prefix}.${body}A.${signature}`),
    ).rejects.toMatchObject({
      code: "SessionTokenInvalidError",
    })
    await expect(
      decodeSessionToken(
        Uint8Array.from(secret, (value) => value ^ 1),
        token,
      ),
    ).rejects.toThrow("integrity check")
    await expect(decodeSessionToken(secret, `${prefix}.${body}.AA`)).rejects.toThrow(
      "signature is malformed",
    )
    for (const malformed of [
      "",
      "nz2.a.b",
      "nz1.a.b.c",
      "nz1.!.abcd",
      "nz1.a.abcd",
      "nz1.AB.AA",
      "x".repeat(8_193),
    ]) {
      await expect(decodeSessionToken(secret, malformed)).rejects.toMatchObject({
        code: "SessionTokenInvalidError",
      })
    }
  })

  it("rejects validly signed malformed UTF-8, JSON, shapes, and payload values", async () => {
    await expect(
      decodeSessionToken(secret, await signedRaw(Uint8Array.from([255]))),
    ).rejects.toThrow("UTF-8 JSON")
    await expect(
      decodeSessionToken(secret, await signedRaw(new TextEncoder().encode("not-json"))),
    ).rejects.toThrow("UTF-8 JSON")
    const valid = { ...payload, version: 1 }
    for (const invalid of [
      null,
      [],
      { ...valid, extra: true },
      { ...valid, version: 2 },
      { ...valid, fleetId: "" },
      { ...valid, fleetId: "\ud800" },
      { ...valid, fleetId: "\ud800\uffff" },
      { ...valid, fleetId: "\udc00" },
      { ...valid, shardId: "bad\nvalue" },
      { ...valid, d1Bookmark: "" },
      { ...valid, routeEpoch: 0 },
      { ...valid, routeEpoch: 1.5 },
      { ...valid, issuedAtMs: -1 },
      { ...valid, issuedAtMs: 1.5 },
    ]) {
      await expect(decodeSessionToken(secret, await signedJson(invalid))).rejects.toMatchObject({
        code: "SessionTokenInvalidError",
      })
    }
  })

  it("validates raw and CryptoKey signing material without leaking it", async () => {
    await expect(createSessionToken(new Uint8Array(31), payload)).rejects.toMatchObject({
      code: "ConfigurationError",
    })
    const key = await crypto.subtle.importKey(
      "raw",
      secret,
      { hash: "SHA-256", name: "HMAC" },
      false,
      ["sign", "verify"],
    )
    const token = await createSessionToken(key, payload)
    await expect(decodeSessionToken(key, token)).resolves.toMatchObject(payload)

    const wrongHash = await crypto.subtle.importKey(
      "raw",
      secret,
      { hash: "SHA-512", name: "HMAC" },
      false,
      ["sign", "verify"],
    )
    await expect(createSessionToken(wrongHash, payload)).rejects.toThrow("signing key is invalid")
    const extractable = await crypto.subtle.importKey(
      "raw",
      secret,
      { hash: "SHA-256", name: "HMAC" },
      true,
      ["sign", "verify"],
    )
    await expect(createSessionToken(extractable, payload)).rejects.toThrow("signing key is invalid")
    await expect(createSessionToken({} as never, payload)).rejects.toThrow("signing key is invalid")
  })

  it("reuses a bookmark only for the exact current shard and epoch", async () => {
    const token = await createSessionToken(secret, payload)
    const establish = vi.fn<() => Promise<string>>()
    await expect(
      resolveRouteAwareSession({
        currentRoute: route,
        establishFreshBookmark: establish,
        fleetId: "fleet-a",
        key: secret,
        nowMs: 2_000,
        token,
      }),
    ).resolves.toMatchObject({ d1Bookmark: "bookmark-a", moved: false, payload })
    expect(establish).not.toHaveBeenCalled()
  })

  it("establishes a fresh destination session after any route change", async () => {
    const token = await createSessionToken(secret, payload)
    const establish = vi.fn(async (destinationShardId: string) => {
      expect(destinationShardId).toBe("shard-b")
      return "bookmark-b"
    })
    const result = await resolveRouteAwareSession({
      currentRoute: { ...route, routeEpoch: 8, shardId: "shard-b" },
      establishFreshBookmark: establish,
      fleetId: "fleet-a",
      key: secret,
      nowMs: 2_000,
      token,
    })

    expect(establish).toHaveBeenCalledOnce()
    expect(establish.mock.calls[0]).toEqual(["shard-b"])
    expect(result).toMatchObject({
      d1Bookmark: "bookmark-b",
      moved: true,
      payload: { fleetId: "fleet-a", routeEpoch: 8, shardId: "shard-b" },
    })
    expect(result.replacementToken).toBeDefined()
    await expect(
      decodeSessionToken(secret, result.replacementToken as string),
    ).resolves.toMatchObject({
      d1Bookmark: "bookmark-b",
      routeEpoch: 8,
      shardId: "shard-b",
    })
  })

  it("rejects cross-fleet and route-epoch rollback", async () => {
    const token = await createSessionToken(secret, payload)
    const establish = vi.fn(async () => "bookmark-b")
    await expect(
      resolveRouteAwareSession({
        currentRoute: route,
        establishFreshBookmark: establish,
        fleetId: "fleet-b",
        key: secret,
        nowMs: 2_000,
        token,
      }),
    ).rejects.toMatchObject({ code: "SessionTokenInvalidError" })
    await expect(
      resolveRouteAwareSession({
        currentRoute: { ...route, routeEpoch: 6 },
        establishFreshBookmark: establish,
        fleetId: "fleet-a",
        key: secret,
        nowMs: 2_000,
        token,
      }),
    ).rejects.toMatchObject({ code: "RouteVersionConflictError" })
    expect(establish).not.toHaveBeenCalled()
  })
})
