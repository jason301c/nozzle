import { NozzleError } from "@nozzle/core"
import type { ScopedRoute } from "@nozzle/drizzle"

export const NOZZLE_SESSION_TOKEN_VERSION = 1 as const

const TOKEN_PREFIX = "nz1"
const MAX_TOKEN_BYTES = 8_192
const MAX_ID_BYTES = 255
const MAX_BOOKMARK_BYTES = 4_096
const MIN_SECRET_BYTES = 32

export interface NozzleSessionTokenPayload {
  readonly d1Bookmark: string
  readonly fleetId: string
  readonly issuedAtMs: number
  readonly routeEpoch: number
  readonly shardId: string
  readonly version: typeof NOZZLE_SESSION_TOKEN_VERSION
}

export type SessionSigningKey = CryptoKey | Uint8Array

export interface RouteAwareSessionResult {
  readonly d1Bookmark: string
  readonly moved: boolean
  readonly payload: NozzleSessionTokenPayload
  readonly replacementToken?: string
}

function invalidSession(message: string): never {
  throw new NozzleError("SessionTokenInvalidError", message)
}

function hasAsciiControl(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code <= 0x1f || code === 0x7f) return true
  }
  return false
}

function isWellFormedText(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false
    }
  }
  return true
}

function validText(value: unknown, maxBytes: number): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    isWellFormedText(value) &&
    new TextEncoder().encode(value).byteLength <= maxBytes &&
    !hasAsciiControl(value)
  )
}

function validatePayload(value: unknown): NozzleSessionTokenPayload {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return invalidSession("The Nozzle session payload is malformed.")
  }
  const record = value as Readonly<Record<string, unknown>>
  const expected = ["d1Bookmark", "fleetId", "issuedAtMs", "routeEpoch", "shardId", "version"]
  const keys = Object.keys(record)
  if (keys.length !== expected.length || expected.some((key) => !Object.hasOwn(record, key))) {
    return invalidSession("The Nozzle session payload fields are invalid.")
  }
  if (
    record.version !== NOZZLE_SESSION_TOKEN_VERSION ||
    !validText(record.fleetId, MAX_ID_BYTES) ||
    !validText(record.shardId, MAX_ID_BYTES) ||
    !validText(record.d1Bookmark, MAX_BOOKMARK_BYTES) ||
    !Number.isSafeInteger(record.routeEpoch) ||
    (record.routeEpoch as number) < 1 ||
    !Number.isSafeInteger(record.issuedAtMs) ||
    (record.issuedAtMs as number) < 0
  ) {
    return invalidSession("The Nozzle session payload values are invalid.")
  }
  return Object.freeze({
    d1Bookmark: record.d1Bookmark,
    fleetId: record.fleetId,
    issuedAtMs: record.issuedAtMs,
    routeEpoch: record.routeEpoch,
    shardId: record.shardId,
    version: NOZZLE_SESSION_TOKEN_VERSION,
  }) as NozzleSessionTokenPayload
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "")
}

function base64UrlDecode(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) return invalidSession("A session token segment is invalid.")
  const remainder = value.length % 4
  if (remainder === 1) return invalidSession("A session token segment has invalid padding.")
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - remainder) % 4)
  const binary = atob(base64)
  const decoded = Uint8Array.from(binary, (character) => character.charCodeAt(0))
  if (base64UrlEncode(decoded) !== value) {
    return invalidSession("A session token segment is not canonical base64url.")
  }
  return decoded
}

function ownedBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

async function hmacKey(key: SessionSigningKey, usage: "sign" | "verify"): Promise<CryptoKey> {
  if (key instanceof Uint8Array) {
    if (key.byteLength < MIN_SECRET_BYTES) {
      throw new NozzleError(
        "ConfigurationError",
        "A Nozzle session signing secret must contain at least 32 bytes.",
      )
    }
    return crypto.subtle.importKey(
      "raw",
      ownedBuffer(key),
      { hash: "SHA-256", name: "HMAC" },
      false,
      [usage],
    )
  }
  const algorithm = key instanceof CryptoKey ? (key.algorithm as HmacKeyAlgorithm) : undefined
  if (
    !(key instanceof CryptoKey) ||
    algorithm?.name !== "HMAC" ||
    algorithm.hash.name !== "SHA-256" ||
    key.extractable ||
    !key.usages.includes(usage)
  ) {
    throw new NozzleError("ConfigurationError", "The Nozzle session signing key is invalid.")
  }
  return key
}

function encodedPayload(payload: NozzleSessionTokenPayload): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({
      d1Bookmark: payload.d1Bookmark,
      fleetId: payload.fleetId,
      issuedAtMs: payload.issuedAtMs,
      routeEpoch: payload.routeEpoch,
      shardId: payload.shardId,
      version: payload.version,
    }),
  )
}

export async function createSessionToken(
  key: SessionSigningKey,
  input: Omit<NozzleSessionTokenPayload, "version">,
): Promise<string> {
  const payload = validatePayload({ ...input, version: NOZZLE_SESSION_TOKEN_VERSION })
  const body = encodedPayload(payload)
  const signingKey = await hmacKey(key, "sign")
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", signingKey, ownedBuffer(body)))
  return `${TOKEN_PREFIX}.${base64UrlEncode(body)}.${base64UrlEncode(signature)}`
}

export async function decodeSessionToken(
  key: SessionSigningKey,
  token: string,
): Promise<NozzleSessionTokenPayload> {
  if (
    typeof token !== "string" ||
    token.length === 0 ||
    new TextEncoder().encode(token).byteLength > MAX_TOKEN_BYTES
  ) {
    return invalidSession("The Nozzle session token is empty or too large.")
  }
  const segments = token.split(".")
  if (segments.length !== 3 || segments[0] !== TOKEN_PREFIX) {
    return invalidSession("The Nozzle session token version is unsupported.")
  }
  const body = base64UrlDecode(segments[1] as string)
  const signature = base64UrlDecode(segments[2] as string)
  if (signature.byteLength !== 32)
    return invalidSession("The session token signature is malformed.")
  const verifyingKey = await hmacKey(key, "verify")
  if (
    !(await crypto.subtle.verify("HMAC", verifyingKey, ownedBuffer(signature), ownedBuffer(body)))
  ) {
    return invalidSession("The Nozzle session token integrity check failed.")
  }
  let decoded: unknown
  try {
    decoded = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body))
  } catch {
    return invalidSession("The Nozzle session token payload is not valid UTF-8 JSON.")
  }
  return validatePayload(decoded)
}

export async function resolveRouteAwareSession(input: {
  readonly currentRoute: ScopedRoute
  readonly establishFreshBookmark: (destinationShardId: string) => Promise<string>
  readonly fleetId: string
  readonly key: SessionSigningKey
  readonly nowMs: number
  readonly token: string
}): Promise<RouteAwareSessionResult> {
  const payload = await decodeSessionToken(input.key, input.token)
  if (payload.fleetId !== input.fleetId) {
    return invalidSession("The Nozzle session token belongs to another fleet.")
  }
  if (input.currentRoute.routeEpoch < payload.routeEpoch) {
    throw new NozzleError(
      "RouteVersionConflictError",
      "The current route is older than the session token route.",
    )
  }
  if (
    input.currentRoute.shardId === payload.shardId &&
    input.currentRoute.routeEpoch === payload.routeEpoch
  ) {
    return Object.freeze({ d1Bookmark: payload.d1Bookmark, moved: false, payload })
  }
  const bookmark = await input.establishFreshBookmark(input.currentRoute.shardId)
  const replacementToken = await createSessionToken(input.key, {
    d1Bookmark: bookmark,
    fleetId: input.fleetId,
    issuedAtMs: input.nowMs,
    routeEpoch: input.currentRoute.routeEpoch,
    shardId: input.currentRoute.shardId,
  })
  return Object.freeze({
    d1Bookmark: bookmark,
    moved: true,
    payload: await decodeSessionToken(input.key, replacementToken),
    replacementToken,
  })
}
