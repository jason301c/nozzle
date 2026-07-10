import { NozzleError } from "./errors.js"

const HEADER = Uint8Array.of(0x4e, 0x5a, 0x48, 0x01)
const DOMAIN = new TextEncoder().encode("nozzle.partition.v1")
const BASE64URL_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

export const HASH_VERSION = 1 as const
export const DEFAULT_BUCKET_BITS = 16 as const
export const HIGH_SCALE_BUCKET_BITS = 20 as const

export type BucketBits = typeof DEFAULT_BUCKET_BITS | typeof HIGH_SCALE_BUCKET_BITS

export type PartitionKey =
  | { readonly type: "binary"; readonly value: Uint8Array }
  | { readonly type: "integer"; readonly value: number }
  | { readonly type: "string"; readonly value: string }
  | { readonly type: "uuid"; readonly value: string }

export interface PartitionHash {
  readonly bucketId: number
  readonly digest: Uint8Array
  readonly digestHex: string
  readonly preimage: Uint8Array
  readonly version: typeof HASH_VERSION
}

function invalidPartitionKey(message: string, details?: Readonly<Record<string, unknown>>): never {
  throw new NozzleError("PartitionKeyMismatchError", message, details ? { details } : undefined)
}

function concatBytes(parts: readonly Uint8Array[]): Uint8Array {
  const length = parts.reduce((total, part) => total + part.byteLength, 0)
  const result = new Uint8Array(length)
  let offset = 0
  for (const part of parts) {
    result.set(part, offset)
    offset += part.byteLength
  }
  return result
}

function frame(tag: number, value: Uint8Array): Uint8Array {
  const result = new Uint8Array(5 + value.byteLength)
  result[0] = tag
  new DataView(result.buffer).setUint32(1, value.byteLength, false)
  result.set(value, 5)
  return result
}

function assertWellFormedUtf16(value: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        invalidPartitionKey("String partition keys cannot contain unpaired UTF-16 surrogates.")
      }
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      invalidPartitionKey("String partition keys cannot contain unpaired UTF-16 surrogates.")
    }
  }
}

function encodeInteger(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || Object.is(value, -0)) {
    invalidPartitionKey("Integer partition keys must be JavaScript safe integers other than -0.", {
      value,
    })
  }
  const bytes = new Uint8Array(8)
  new DataView(bytes.buffer).setBigInt64(0, BigInt(value), false)
  return bytes
}

function encodeUuid(value: string): Uint8Array {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu.test(value)) {
    invalidPartitionKey("UUID partition keys must use the hyphenated 8-4-4-4-12 form.")
  }
  const hex = value.replaceAll("-", "").toLowerCase()
  const bytes = new Uint8Array(16)
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes
}

export function encodeCanonicalPartitionKey(key: PartitionKey): {
  readonly bytes: Uint8Array
  readonly typeTag: number
} {
  switch (key.type) {
    case "binary":
      if (!(key.value instanceof Uint8Array)) {
        return invalidPartitionKey("Binary partition keys must be Uint8Array values.")
      }
      return { bytes: key.value.slice(), typeTag: 0x04 }
    case "integer":
      return { bytes: encodeInteger(key.value), typeTag: 0x02 }
    case "string":
      if (typeof key.value !== "string") {
        return invalidPartitionKey("String partition keys must be strings.")
      }
      assertWellFormedUtf16(key.value)
      return { bytes: new TextEncoder().encode(key.value), typeTag: 0x01 }
    case "uuid":
      if (typeof key.value !== "string") {
        return invalidPartitionKey("UUID partition keys must be strings.")
      }
      return { bytes: encodeUuid(key.value), typeTag: 0x03 }
  }
}

export function encodeHashPreimage(key: PartitionKey, fleetSeed: Uint8Array): Uint8Array {
  if (!(fleetSeed instanceof Uint8Array) || fleetSeed.byteLength !== 32) {
    throw new NozzleError("ConfigurationError", "The fleet seed must contain exactly 32 bytes.", {
      details: { actualLength: fleetSeed?.byteLength },
    })
  }
  const canonical = encodeCanonicalPartitionKey(key)
  return concatBytes([
    HEADER,
    frame(0x01, DOMAIN),
    frame(0x02, fleetSeed),
    frame(0x03, Uint8Array.of(canonical.typeTag)),
    frame(0x04, canonical.bytes),
  ])
}

export function selectBucket(digest: Uint8Array, bucketBits: BucketBits): number {
  if (!(digest instanceof Uint8Array) || digest.byteLength !== 32) {
    throw new NozzleError("ConfigurationError", "A partition digest must contain 32 bytes.")
  }
  if (bucketBits !== DEFAULT_BUCKET_BITS && bucketBits !== HIGH_SCALE_BUCKET_BITS) {
    throw new NozzleError("ConfigurationError", "Bucket bits must be 16 or 20.", {
      details: { bucketBits },
    })
  }
  const prefix = new DataView(digest.buffer, digest.byteOffset, 4).getUint32(0, false)
  return prefix >>> (32 - bucketBits)
}

export function bytesToHex(bytes: Uint8Array): string {
  let result = ""
  for (const byte of bytes) result += byte.toString(16).padStart(2, "0")
  return result
}

export async function hashPartitionKey(
  key: PartitionKey,
  fleetSeed: Uint8Array,
  bucketBits: BucketBits = DEFAULT_BUCKET_BITS,
): Promise<PartitionHash> {
  const preimage = encodeHashPreimage(key, fleetSeed)
  const digestInput = new Uint8Array(preimage.byteLength)
  digestInput.set(preimage)
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", digestInput.buffer))
  return Object.freeze({
    bucketId: selectBucket(digest, bucketBits),
    digest,
    digestHex: bytesToHex(digest),
    preimage,
    version: HASH_VERSION,
  })
}

export function encodeFleetSeed(seed: Uint8Array): string {
  if (!(seed instanceof Uint8Array) || seed.byteLength !== 32) {
    throw new NozzleError("ConfigurationError", "The fleet seed must contain exactly 32 bytes.")
  }
  let output = ""
  for (let index = 0; index < seed.length; index += 3) {
    const a = seed[index] as number
    const b = seed[index + 1] as number
    const c = seed[index + 2] ?? 0
    const packed = (a << 16) | (b << 8) | c
    output += BASE64URL_ALPHABET[(packed >>> 18) & 0x3f]
    output += BASE64URL_ALPHABET[(packed >>> 12) & 0x3f]
    output += BASE64URL_ALPHABET[(packed >>> 6) & 0x3f]
    if (index + 2 < seed.length) output += BASE64URL_ALPHABET[packed & 0x3f]
  }
  return output
}

export function decodeFleetSeed(encoded: string): Uint8Array {
  if (typeof encoded !== "string" || !/^[A-Za-z0-9_-]{43}$/u.test(encoded)) {
    throw new NozzleError("ConfigurationError", "The fleet seed must be unpadded base64url.")
  }
  const output: number[] = []
  let buffer = 0
  let bits = 0
  for (const character of encoded) {
    const value = BASE64URL_ALPHABET.indexOf(character)
    buffer = (buffer << 6) | value
    bits += 6
    if (bits >= 8) {
      bits -= 8
      output.push((buffer >>> bits) & 0xff)
    }
  }
  const seed = Uint8Array.from(output)
  if ((buffer & ((1 << bits) - 1)) !== 0) {
    throw new NozzleError("ConfigurationError", "The fleet seed must decode to exactly 32 bytes.")
  }
  return seed
}

export function generateFleetSeed(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(32))
}
