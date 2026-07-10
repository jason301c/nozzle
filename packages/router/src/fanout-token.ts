import { NozzleError } from "@nozzle/core"
import {
  type FanoutContinuationState,
  type FanoutCurrentIdentity,
  loadFanoutContinuation,
  validateFanoutContinuationIdentity,
} from "./fanout.js"

export const NOZZLE_FANOUT_TOKEN_VERSION = 1 as const
export const MAX_FANOUT_TOKEN_BYTES = 8_192

export type FanoutEncryptionKey = CryptoKey | Uint8Array

const TOKEN_PREFIX = `nzf${NOZZLE_FANOUT_TOKEN_VERSION}`
const NONCE_BYTES = 12
const TAG_BYTES = 16
const AES_KEY_BYTES = 32
const AAD = new TextEncoder().encode(`nozzle-fanout:v${NOZZLE_FANOUT_TOKEN_VERSION}`)

function invalid(message: string): never {
  throw new NozzleError("SessionTokenInvalidError", message)
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function validTime(value: unknown, label: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    configuration(`${label} must be a non-negative safe integer.`)
  }
}

function assertLive(state: FanoutContinuationState, nowMs: number): void {
  if (nowMs >= state.deadlineAtMs) {
    throw new NozzleError("CapacityGuardError", "Fan-out operation deadline has elapsed.")
  }
  if (nowMs >= state.expiresAtMs) invalid("Fan-out continuation token has expired.")
}

function ownedBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return copy.buffer
}

async function aesKey(key: FanoutEncryptionKey, usage: "decrypt" | "encrypt"): Promise<CryptoKey> {
  if (key instanceof Uint8Array) {
    if (key.byteLength !== AES_KEY_BYTES) {
      configuration("A fan-out token encryption secret must contain exactly 32 bytes.")
    }
    return crypto.subtle.importKey("raw", ownedBuffer(key), "AES-GCM", false, [usage])
  }
  const algorithm = key instanceof CryptoKey ? (key.algorithm as AesKeyAlgorithm) : undefined
  if (
    !(key instanceof CryptoKey) ||
    algorithm?.name !== "AES-GCM" ||
    algorithm.length !== 256 ||
    key.extractable ||
    !key.usages.includes(usage)
  ) {
    configuration("The fan-out token encryption key is invalid.")
  }
  return key
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "")
}

function base64UrlDecode(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) invalid("A fan-out token segment is invalid.")
  const remainder = value.length % 4
  if (remainder === 1) invalid("A fan-out token segment has invalid padding.")
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - remainder) % 4)
  const decoded = Uint8Array.from(atob(base64), (character) => character.charCodeAt(0))
  if (base64UrlEncode(decoded) !== value) {
    invalid("A fan-out token segment is not canonical base64url.")
  }
  return decoded
}

function encodedState(state: FanoutContinuationState): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(state))
}

export async function createFanoutToken(input: {
  readonly key: FanoutEncryptionKey
  readonly nowMs: number
  readonly state: FanoutContinuationState
}): Promise<string> {
  validTime(input.nowMs, "Fan-out current time")
  const state = loadFanoutContinuation(input.state)
  assertLive(state, input.nowMs)
  const key = await aesKey(input.key, "encrypt")
  const nonce = crypto.getRandomValues(new Uint8Array(NONCE_BYTES))
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt(
      {
        additionalData: ownedBuffer(AAD),
        iv: ownedBuffer(nonce),
        name: "AES-GCM",
        tagLength: TAG_BYTES * 8,
      },
      key,
      ownedBuffer(encodedState(state)),
    ),
  )
  const token = `${TOKEN_PREFIX}.${base64UrlEncode(nonce)}.${base64UrlEncode(ciphertext)}`
  if (new TextEncoder().encode(token).byteLength > MAX_FANOUT_TOKEN_BYTES) {
    throw new NozzleError(
      "CapacityGuardError",
      "Fan-out continuation state exceeds the token size budget.",
    )
  }
  return token
}

export async function decodeFanoutToken(input: {
  readonly current: FanoutCurrentIdentity
  readonly key: FanoutEncryptionKey
  readonly nowMs: number
  readonly token: string
}): Promise<FanoutContinuationState> {
  validTime(input.nowMs, "Fan-out current time")
  if (
    typeof input.token !== "string" ||
    input.token.length === 0 ||
    new TextEncoder().encode(input.token).byteLength > MAX_FANOUT_TOKEN_BYTES
  ) {
    invalid("The fan-out continuation token is empty or too large.")
  }
  const segments = input.token.split(".")
  if (segments.length !== 3 || segments[0] !== TOKEN_PREFIX) {
    invalid("The fan-out continuation token version is unsupported.")
  }
  const nonce = base64UrlDecode(segments[1] as string)
  const ciphertext = base64UrlDecode(segments[2] as string)
  if (nonce.byteLength !== NONCE_BYTES || ciphertext.byteLength < TAG_BYTES) {
    invalid("The fan-out continuation token payload is malformed.")
  }
  const key = await aesKey(input.key, "decrypt")
  let plaintext: Uint8Array
  try {
    plaintext = new Uint8Array(
      await crypto.subtle.decrypt(
        {
          additionalData: ownedBuffer(AAD),
          iv: ownedBuffer(nonce),
          name: "AES-GCM",
          tagLength: TAG_BYTES * 8,
        },
        key,
        ownedBuffer(ciphertext),
      ),
    )
  } catch {
    return invalid("The fan-out continuation token integrity check failed.")
  }
  let decoded: unknown
  try {
    decoded = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(plaintext))
  } catch {
    return invalid("The fan-out continuation token payload is not valid UTF-8 JSON.")
  }
  let state: FanoutContinuationState
  try {
    state = loadFanoutContinuation(decoded)
  } catch {
    return invalid("The fan-out continuation token state is malformed.")
  }
  assertLive(state, input.nowMs)
  validateFanoutContinuationIdentity(state, input.current)
  return state
}
