import { NozzleError } from "./errors.js"
import {
  type BucketBits,
  bytesToHex,
  DEFAULT_BUCKET_BITS,
  HASH_VERSION,
  HIGH_SCALE_BUCKET_BITS,
  selectBucket,
} from "./hash.js"
import { OWNERSHIP_STATES, type OwnershipState } from "./ownership.js"

const MAGIC = Uint8Array.of(0x4e, 0x5a, 0x52, 0x4d)
const MAX_STRING_BYTES = 1_024
const MAX_SHARDS = 50_000
const MAX_DESCRIPTOR_TEXT_BYTES = 2 * 1_024 * 1_024
const MAX_RESERVED_BUCKETS = 65_536
const MAX_OVERRIDES = 65_536
const MAX_MANIFEST_PAYLOAD_BYTES = 24 * 1_024 * 1_024
const ROUTE_BYTES = 13
const RESERVED_ROUTE_BYTES = 17
const CHECKSUM_PATTERN = /^[0-9a-f]{64}$/u
const DIGEST_PATTERN = /^[0-9a-f]{64}$/u

const KIND_TO_CODE = { binding: 1, router: 2 } as const
const CODE_TO_KIND = [undefined, "binding", "router"] as const
const OWNERSHIP_TO_CODE = new Map(OWNERSHIP_STATES.map((state, index) => [state, index] as const))

export const ROUTE_MANIFEST_VERSION = 1 as const

export type RouteDestinationKind = keyof typeof KIND_TO_CODE

export interface SchemaCompatibilityRange {
  readonly maximum: number
  readonly minimum: number
}

export interface RouteShardDescriptor {
  readonly destination: string
  readonly id: string
  readonly jurisdiction: string
  readonly kind: RouteDestinationKind
  readonly schemaCompatibility: SchemaCompatibilityRange
}

export interface RouteOverride {
  readonly bucketId: number
  readonly digestHex: string
}

export interface ReservedBucketRoute {
  readonly bucketId: number
  readonly ownershipState: OwnershipState
  readonly routeEpoch: number
  readonly shardId: string
}

export interface RouteManifestSource {
  readonly bucketBits: BucketBits
  readonly bucketOwnershipStates: readonly OwnershipState[]
  readonly bucketRouteEpochs: readonly number[]
  readonly bucketToShard: readonly string[]
  readonly createdAtMs: number
  readonly environmentId: string
  readonly expiresAtMs: number
  readonly fleetId: string
  readonly hashVersion: typeof HASH_VERSION
  readonly overrides?: readonly RouteOverride[]
  readonly reservedBucketRoutes?: readonly ReservedBucketRoute[]
  readonly shards: readonly RouteShardDescriptor[]
  readonly topologyVersion: number
}

export interface RouteManifestCompatibility {
  readonly bucketBits: BucketBits
  readonly environmentId: string
  readonly fleetId: string
  readonly hashVersion: typeof HASH_VERSION
  readonly maximumTopologyVersion?: number
  readonly minimumTopologyVersion: number
  readonly nowMs: number
  readonly schemaVersion?: number
}

export interface ResolvedRoute {
  readonly bucketId: number
  readonly destination: string
  readonly destinationKind: RouteDestinationKind
  readonly environmentId: string
  readonly fleetId: string
  readonly hashVersion: typeof HASH_VERSION
  readonly jurisdiction: string
  readonly manifestVersion: typeof ROUTE_MANIFEST_VERSION
  readonly overridden: boolean
  readonly ownershipState: OwnershipState
  readonly routeEpoch: number
  readonly schemaCompatibility: SchemaCompatibilityRange
  readonly shardId: string
  readonly topologyVersion: number
}

interface NormalizedManifest {
  readonly bucketBits: BucketBits
  readonly bucketOwnershipCodes: Uint8Array
  readonly bucketRouteEpochs: Float64Array
  readonly bucketShardIndexes: Uint32Array
  readonly createdAtMs: number
  readonly environmentId: string
  readonly expiresAtMs: number
  readonly fleetId: string
  readonly overrides: readonly NormalizedOverride[]
  readonly reservedBucketIds: Uint32Array
  readonly reservedBucketOwnershipCodes: Uint8Array
  readonly reservedBucketRouteEpochs: Float64Array
  readonly reservedBucketShardIndexes: Uint32Array
  readonly shards: readonly RouteShardDescriptor[]
  readonly topologyVersion: number
}

interface NormalizedOverride {
  readonly bucketId: number
  readonly digest: Uint8Array
}

function configurationError(message: string, details?: Readonly<Record<string, unknown>>): never {
  throw new NozzleError("ConfigurationError", message, details ? { details } : undefined)
}

function routeError(message: string, details?: Readonly<Record<string, unknown>>): never {
  throw new NozzleError("RouteVersionConflictError", message, details ? { details } : undefined)
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function compareStrings(left: string, right: string): number {
  if (left < right) return -1
  if (left > right) return 1
  return 0
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const leftView = new DataView(left.buffer, left.byteOffset, left.byteLength)
  const rightView = new DataView(right.buffer, right.byteOffset, right.byteLength)
  for (let index = 0; index < left.byteLength; index += 1) {
    const difference = leftView.getUint8(index) - rightView.getUint8(index)
    if (difference !== 0) return difference
  }
  return 0
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  const leftView = new DataView(left.buffer, left.byteOffset, left.byteLength)
  const rightView = new DataView(right.buffer, right.byteOffset, right.byteLength)
  let difference = 0
  for (let index = 0; index < left.byteLength; index += 1) {
    difference |= leftView.getUint8(index) ^ rightView.getUint8(index)
  }
  return difference === 0
}

function assertSafeUnsigned(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    configurationError(`${name} must be a non-negative safe integer.`, { value })
  }
}

function assertUint32(value: unknown, name: string): asserts value is number {
  assertSafeUnsigned(value, name)
  if (value > 0xffff_ffff) configurationError(`${name} must fit in an unsigned 32-bit integer.`)
}

function assertBucketBits(bucketBits: number): asserts bucketBits is BucketBits {
  if (bucketBits !== DEFAULT_BUCKET_BITS && bucketBits !== HIGH_SCALE_BUCKET_BITS) {
    configurationError("Route manifest bucket bits must be 16 or 20.", { bucketBits })
  }
}

function assertText(value: string, name: string): Uint8Array {
  if (typeof value !== "string" || value.length === 0) {
    configurationError(`${name} must be a non-empty string.`)
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        configurationError(`${name} cannot contain unpaired UTF-16 surrogates.`)
      }
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      configurationError(`${name} cannot contain unpaired UTF-16 surrogates.`)
    }
  }
  const encoded = new TextEncoder().encode(value)
  if (encoded.byteLength > MAX_STRING_BYTES) {
    configurationError(`${name} exceeds the ${MAX_STRING_BYTES}-byte manifest limit.`)
  }
  return encoded
}

function decodeDigestHex(digestHex: unknown): Uint8Array {
  if (typeof digestHex !== "string" || !DIGEST_PATTERN.test(digestHex)) {
    configurationError("Route override digests must be 64 lowercase hexadecimal characters.")
  }
  const digest = new Uint8Array(32)
  for (let index = 0; index < digest.byteLength; index += 1) {
    digest[index] = Number.parseInt(digestHex.slice(index * 2, index * 2 + 2), 16)
  }
  return digest
}

function copyShard(shard: RouteShardDescriptor): {
  readonly descriptor: RouteShardDescriptor
  readonly textBytes: number
} {
  if (!isRecord(shard) || !isRecord(shard.schemaCompatibility)) {
    configurationError("Every shard descriptor must be a complete object.")
  }
  const id = assertText(shard.id, "Shard ID")
  const destination = assertText(shard.destination, "Shard destination")
  const jurisdiction = assertText(shard.jurisdiction, "Shard jurisdiction")
  if (shard.kind !== "binding" && shard.kind !== "router") {
    configurationError("Shard destination kind must be binding or router.", { kind: shard.kind })
  }
  assertUint32(shard.schemaCompatibility.minimum, "Minimum compatible schema version")
  assertUint32(shard.schemaCompatibility.maximum, "Maximum compatible schema version")
  if (shard.schemaCompatibility.minimum > shard.schemaCompatibility.maximum) {
    configurationError("A shard schema compatibility range cannot be inverted.", {
      maximum: shard.schemaCompatibility.maximum,
      minimum: shard.schemaCompatibility.minimum,
      shardId: shard.id,
    })
  }
  const schemaCompatibility = Object.freeze({
    maximum: shard.schemaCompatibility.maximum,
    minimum: shard.schemaCompatibility.minimum,
  })
  return {
    descriptor: Object.freeze({
      destination: shard.destination,
      id: shard.id,
      jurisdiction: shard.jurisdiction,
      kind: shard.kind,
      schemaCompatibility,
    }),
    textBytes: id.byteLength + destination.byteLength + jurisdiction.byteLength,
  }
}

function checkedManifestSize(
  current: number,
  increment: number,
  label: string,
  maximum = MAX_MANIFEST_PAYLOAD_BYTES,
): number {
  const next = current + increment
  if (!Number.isSafeInteger(next) || next > maximum) {
    configurationError(`${label} exceeds the ${maximum}-byte limit.`)
  }
  return next
}

function normalizeSource(source: RouteManifestSource): NormalizedManifest {
  if (!isRecord(source)) configurationError("A route manifest source must be an object.")
  if (source.hashVersion !== HASH_VERSION) {
    configurationError("Route manifests support hash version 1 only.", {
      hashVersion: source.hashVersion,
    })
  }
  assertBucketBits(source.bucketBits)
  assertText(source.fleetId, "Fleet ID")
  assertText(source.environmentId, "Environment ID")
  assertSafeUnsigned(source.topologyVersion, "Topology version")
  assertSafeUnsigned(source.createdAtMs, "Manifest creation time")
  assertSafeUnsigned(source.expiresAtMs, "Manifest expiry time")
  if (source.expiresAtMs <= source.createdAtMs) {
    configurationError("Manifest expiry must be later than its creation time.")
  }

  if (!Array.isArray(source.shards) || source.shards.length === 0) {
    configurationError("A route manifest must contain at least one shard descriptor.")
  }
  if (
    !Array.isArray(source.bucketToShard) ||
    !Array.isArray(source.bucketRouteEpochs) ||
    !Array.isArray(source.bucketOwnershipStates)
  ) {
    configurationError("Dense route manifest tables must be arrays.")
  }
  if (source.reservedBucketRoutes !== undefined && !Array.isArray(source.reservedBucketRoutes)) {
    configurationError("Reserved bucket routes must be an array when provided.")
  }
  if (source.overrides !== undefined && !Array.isArray(source.overrides)) {
    configurationError("Route overrides must be an array when provided.")
  }
  if (source.shards.length > MAX_SHARDS) {
    configurationError(`A route manifest cannot contain more than ${MAX_SHARDS} shards.`)
  }
  let descriptorTextBytes = 0
  const shards = source.shards
    .map((shard) => {
      const copied = copyShard(shard)
      descriptorTextBytes = checkedManifestSize(
        descriptorTextBytes,
        copied.textBytes,
        "Route manifest shard descriptors",
        MAX_DESCRIPTOR_TEXT_BYTES,
      )
      return copied.descriptor
    })
    .sort((left, right) => compareStrings(left.id, right.id))
  const shardIndexes = new Map<string, number>()
  for (let index = 0; index < shards.length; index += 1) {
    const shard = shards[index] as RouteShardDescriptor
    if (shardIndexes.has(shard.id)) {
      configurationError("Shard IDs must be unique.", { shardId: shard.id })
    }
    shardIndexes.set(shard.id, index)
  }

  const bucketCount = 2 ** source.bucketBits
  if (source.bucketToShard.length !== bucketCount) {
    configurationError("The dense bucket-to-shard table has the wrong length.", {
      actual: source.bucketToShard.length,
      expected: bucketCount,
    })
  }
  if (source.bucketRouteEpochs.length !== bucketCount) {
    configurationError("The dense bucket route-epoch table has the wrong length.", {
      actual: source.bucketRouteEpochs.length,
      expected: bucketCount,
    })
  }
  if (source.bucketOwnershipStates.length !== bucketCount) {
    configurationError("The dense bucket ownership-state table has the wrong length.", {
      actual: source.bucketOwnershipStates.length,
      expected: bucketCount,
    })
  }

  const bucketShardIndexes = new Uint32Array(bucketCount)
  const bucketRouteEpochs = new Float64Array(bucketCount)
  const bucketOwnershipCodes = new Uint8Array(bucketCount)
  const referencedShards = new Set<number>()
  for (let bucketId = 0; bucketId < bucketCount; bucketId += 1) {
    const shardId = source.bucketToShard[bucketId]
    const shardIndex = typeof shardId === "string" ? shardIndexes.get(shardId) : undefined
    if (shardIndex === undefined) {
      configurationError("Every bucket must reference a declared shard.", { bucketId, shardId })
    }
    const routeEpoch = source.bucketRouteEpochs[bucketId]
    assertSafeUnsigned(routeEpoch ?? Number.NaN, "Bucket route epoch")
    const ownershipState = source.bucketOwnershipStates[bucketId]
    const ownershipCode = ownershipState ? OWNERSHIP_TO_CODE.get(ownershipState) : undefined
    if (ownershipCode === undefined) {
      configurationError("Every bucket must contain a valid ownership state.", {
        bucketId,
        ownershipState,
      })
    }
    bucketShardIndexes[bucketId] = shardIndex
    bucketRouteEpochs[bucketId] = routeEpoch as number
    bucketOwnershipCodes[bucketId] = ownershipCode
    referencedShards.add(shardIndex)
  }
  const sourceReserved = source.reservedBucketRoutes ?? []
  if (sourceReserved.length > MAX_RESERVED_BUCKETS) {
    configurationError(
      `A route manifest cannot contain more than ${MAX_RESERVED_BUCKETS} reserved bucket routes.`,
    )
  }
  const reserved = sourceReserved
    .map((route): ReservedBucketRoute => {
      if (!isRecord(route)) configurationError("Every reserved bucket route must be an object.")
      assertUint32(route.bucketId, "Reserved bucket ID")
      if (route.bucketId < bucketCount) {
        configurationError("Reserved bucket IDs must be outside the dense hash-bucket namespace.", {
          bucketId: route.bucketId,
          minimum: bucketCount,
        })
      }
      const shardId = route.shardId
      const shardIndex = typeof shardId === "string" ? shardIndexes.get(shardId) : undefined
      if (shardIndex === undefined) {
        configurationError("Every reserved bucket must reference a declared shard.", {
          bucketId: route.bucketId,
          shardId,
        })
      }
      assertSafeUnsigned(route.routeEpoch, "Reserved bucket route epoch")
      const ownershipState = route.ownershipState as OwnershipState
      const ownershipCode = route.ownershipState ? OWNERSHIP_TO_CODE.get(ownershipState) : undefined
      if (ownershipCode === undefined) {
        configurationError("Every reserved bucket must contain a valid ownership state.", {
          bucketId: route.bucketId,
          ownershipState: route.ownershipState,
        })
      }
      referencedShards.add(shardIndex)
      return Object.freeze({
        bucketId: route.bucketId,
        ownershipState,
        routeEpoch: route.routeEpoch,
        shardId: shardId as string,
      })
    })
    .sort((left, right) => left.bucketId - right.bucketId)
  for (let index = 1; index < reserved.length; index += 1) {
    const route = reserved[index] as ReservedBucketRoute
    const previous = reserved[index - 1] as ReservedBucketRoute
    if (route.bucketId === previous.bucketId) {
      configurationError("Reserved bucket IDs must be unique.", {
        bucketId: route.bucketId,
      })
    }
  }
  const reservedBucketIds = new Uint32Array(reserved.length)
  const reservedBucketShardIndexes = new Uint32Array(reserved.length)
  const reservedBucketRouteEpochs = new Float64Array(reserved.length)
  const reservedBucketOwnershipCodes = new Uint8Array(reserved.length)
  for (let index = 0; index < reserved.length; index += 1) {
    const route = reserved[index] as ReservedBucketRoute
    reservedBucketIds[index] = route.bucketId
    reservedBucketShardIndexes[index] = shardIndexes.get(route.shardId) as number
    reservedBucketRouteEpochs[index] = route.routeEpoch
    reservedBucketOwnershipCodes[index] = OWNERSHIP_TO_CODE.get(route.ownershipState) as number
  }
  const reservedSet = new Set(reservedBucketIds)

  if (referencedShards.size !== shards.length) {
    configurationError(
      "Every shard descriptor must be referenced by a dense or reserved bucket route.",
    )
  }

  const sourceOverrides = source.overrides ?? []
  if (sourceOverrides.length > MAX_OVERRIDES) {
    configurationError(`A route manifest cannot contain more than ${MAX_OVERRIDES} overrides.`)
  }
  const overrides = sourceOverrides.map((override): NormalizedOverride => {
    if (!isRecord(override)) configurationError("Every route override must be an object.")
    assertUint32(override.bucketId, "Override bucket ID")
    if (!reservedSet.has(override.bucketId)) {
      configurationError("Every route override must target a declared reserved bucket.", {
        bucketId: override.bucketId,
      })
    }
    return Object.freeze({
      bucketId: override.bucketId,
      digest: decodeDigestHex(override.digestHex),
    })
  })
  overrides.sort((left, right) => compareBytes(left.digest, right.digest))
  const overriddenBuckets = new Set<number>()
  for (let index = 0; index < overrides.length; index += 1) {
    const override = overrides[index] as NormalizedOverride
    if (
      index > 0 &&
      compareBytes((overrides[index - 1] as NormalizedOverride).digest, override.digest) === 0
    ) {
      configurationError("Route override digests must be unique.", {
        digestHex: bytesToHex(override.digest),
      })
    }
    if (overriddenBuckets.has(override.bucketId)) {
      configurationError("A reserved bucket can belong to only one route override.", {
        bucketId: override.bucketId,
      })
    }
    overriddenBuckets.add(override.bucketId)
  }

  return {
    bucketBits: source.bucketBits,
    bucketOwnershipCodes,
    bucketRouteEpochs,
    bucketShardIndexes,
    createdAtMs: source.createdAtMs,
    environmentId: source.environmentId,
    expiresAtMs: source.expiresAtMs,
    fleetId: source.fleetId,
    overrides,
    reservedBucketIds,
    reservedBucketOwnershipCodes,
    reservedBucketRouteEpochs,
    reservedBucketShardIndexes,
    shards: Object.freeze(shards),
    topologyVersion: source.topologyVersion,
  }
}

class BinaryWriter {
  #buffer = new Uint8Array(1_024)
  #offset = 0
  #view = new DataView(this.#buffer.buffer)

  #reserve(length: number): void {
    const required = checkedManifestSize(this.#offset, length, "Route manifest payload")
    if (required <= this.#buffer.byteLength) return
    let capacity = this.#buffer.byteLength
    while (capacity < required) capacity *= 2
    const grown = new Uint8Array(Math.min(capacity, MAX_MANIFEST_PAYLOAD_BYTES))
    grown.set(this.#buffer)
    this.#buffer = grown
    this.#view = new DataView(grown.buffer)
  }

  bytes(value: Uint8Array): void {
    this.#reserve(value.byteLength)
    this.#buffer.set(value, this.#offset)
    this.#offset += value.byteLength
  }

  string(value: string, name: string): void {
    const encoded = assertText(value, name)
    this.uint16(encoded.byteLength)
    this.bytes(encoded)
  }

  uint8(value: number): void {
    this.#reserve(1)
    this.#buffer[this.#offset] = value
    this.#offset += 1
  }

  uint16(value: number): void {
    this.#reserve(2)
    this.#view.setUint16(this.#offset, value, false)
    this.#offset += 2
  }

  uint32(value: number): void {
    this.#reserve(4)
    this.#view.setUint32(this.#offset, value, false)
    this.#offset += 4
  }

  uint64(value: number): void {
    this.#reserve(8)
    this.#view.setBigUint64(this.#offset, BigInt(value), false)
    this.#offset += 8
  }

  finish(): Uint8Array {
    return this.#buffer.slice(0, this.#offset)
  }
}

class BinaryReader {
  readonly #bytes: Uint8Array
  readonly #view: DataView
  #offset = 0

  constructor(bytes: Uint8Array) {
    this.#bytes = bytes
    this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  }

  get remaining(): number {
    return this.#bytes.byteLength - this.#offset
  }

  bytes(length: number): Uint8Array {
    this.#assertAvailable(length)
    const output = this.#bytes.slice(this.#offset, this.#offset + length)
    this.#offset += length
    return output
  }

  #assertAvailable(length: number): void {
    if (!Number.isSafeInteger(length) || length < 0 || length > this.remaining) {
      routeError("The route manifest payload is truncated or malformed.")
    }
  }

  string(name: string): string {
    const encoded = this.bytes(this.uint16())
    let decoded: string
    try {
      decoded = new TextDecoder("utf-8", { fatal: true }).decode(encoded)
    } catch {
      return routeError(`${name} is not valid UTF-8.`)
    }
    const canonical = assertText(decoded, name)
    if (!bytesEqual(canonical, encoded)) routeError(`${name} does not use canonical UTF-8.`)
    return decoded
  }

  uint8(): number {
    this.#assertAvailable(1)
    const value = this.#view.getUint8(this.#offset)
    this.#offset += 1
    return value
  }

  uint16(): number {
    this.#assertAvailable(2)
    const value = this.#view.getUint16(this.#offset, false)
    this.#offset += 2
    return value
  }

  uint32(): number {
    this.#assertAvailable(4)
    const value = this.#view.getUint32(this.#offset, false)
    this.#offset += 4
    return value
  }

  uint64(name: string): number {
    this.#assertAvailable(8)
    const value = this.#view.getBigUint64(this.#offset, false)
    this.#offset += 8
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      routeError(`${name} exceeds the JavaScript safe-integer range.`)
    }
    return Number(value)
  }
}

function encodeNormalized(manifest: NormalizedManifest): Uint8Array {
  const writer = new BinaryWriter()
  writer.bytes(MAGIC)
  writer.uint8(ROUTE_MANIFEST_VERSION)
  writer.uint8(HASH_VERSION)
  writer.uint8(manifest.bucketBits)
  writer.uint8(0)
  writer.uint64(manifest.topologyVersion)
  writer.uint64(manifest.createdAtMs)
  writer.uint64(manifest.expiresAtMs)
  writer.string(manifest.fleetId, "Fleet ID")
  writer.string(manifest.environmentId, "Environment ID")

  writer.uint32(manifest.shards.length)
  for (const shard of manifest.shards) {
    writer.string(shard.id, "Shard ID")
    writer.uint8(KIND_TO_CODE[shard.kind])
    writer.string(shard.destination, "Shard destination")
    writer.string(shard.jurisdiction, "Shard jurisdiction")
    writer.uint32(shard.schemaCompatibility.minimum)
    writer.uint32(shard.schemaCompatibility.maximum)
  }

  writer.uint32(manifest.bucketShardIndexes.length)
  for (let bucketId = 0; bucketId < manifest.bucketShardIndexes.length; bucketId += 1) {
    writer.uint32(manifest.bucketShardIndexes[bucketId] as number)
    writer.uint64(manifest.bucketRouteEpochs[bucketId] as number)
    writer.uint8(manifest.bucketOwnershipCodes[bucketId] as number)
  }

  writer.uint32(manifest.reservedBucketIds.length)
  for (let index = 0; index < manifest.reservedBucketIds.length; index += 1) {
    writer.uint32(manifest.reservedBucketIds[index] as number)
    writer.uint32(manifest.reservedBucketShardIndexes[index] as number)
    writer.uint64(manifest.reservedBucketRouteEpochs[index] as number)
    writer.uint8(manifest.reservedBucketOwnershipCodes[index] as number)
  }
  writer.uint32(manifest.overrides.length)
  for (const override of manifest.overrides) {
    writer.bytes(override.digest)
    writer.uint32(override.bucketId)
  }
  return writer.finish()
}

function decodePayload(payload: Uint8Array): NormalizedManifest {
  const reader = new BinaryReader(payload)
  if (!bytesEqual(reader.bytes(MAGIC.byteLength), MAGIC)) {
    routeError("The route manifest magic bytes are invalid.")
  }
  if (reader.uint8() !== ROUTE_MANIFEST_VERSION) {
    routeError("The route manifest version is unsupported.")
  }
  if (reader.uint8() !== HASH_VERSION) routeError("The route manifest hash version is unsupported.")
  const bucketBits = reader.uint8()
  assertBucketBits(bucketBits)
  if (reader.uint8() !== 0) routeError("The route manifest reserved header byte must be zero.")
  const topologyVersion = reader.uint64("Topology version")
  const createdAtMs = reader.uint64("Manifest creation time")
  const expiresAtMs = reader.uint64("Manifest expiry time")
  const fleetId = reader.string("Fleet ID")
  const environmentId = reader.string("Environment ID")

  const shardCount = reader.uint32()
  if (shardCount === 0 || shardCount > MAX_SHARDS) {
    routeError("The route manifest shard count is outside the supported range.", { shardCount })
  }
  const shards: RouteShardDescriptor[] = []
  for (let index = 0; index < shardCount; index += 1) {
    const id = reader.string("Shard ID")
    if (index > 0 && compareStrings((shards[index - 1] as RouteShardDescriptor).id, id) >= 0) {
      routeError("Shard descriptors must be uniquely sorted by ID.")
    }
    const kind = CODE_TO_KIND[reader.uint8()]
    if (!kind) routeError("The route manifest contains an invalid destination kind.")
    const destination = reader.string("Shard destination")
    const jurisdiction = reader.string("Shard jurisdiction")
    const minimum = reader.uint32()
    const maximum = reader.uint32()
    if (minimum > maximum) routeError("A shard schema compatibility range is inverted.")
    shards.push(
      Object.freeze({
        destination,
        id,
        jurisdiction,
        kind,
        schemaCompatibility: Object.freeze({ maximum, minimum }),
      }),
    )
  }

  const bucketCount = reader.uint32()
  const expectedBucketCount = 2 ** bucketBits
  if (bucketCount !== expectedBucketCount) {
    routeError("The dense bucket table does not match the declared bucket space.", {
      actual: bucketCount,
      expected: expectedBucketCount,
    })
  }
  if (reader.remaining < bucketCount * ROUTE_BYTES + 8) {
    routeError("The route manifest dense bucket table is truncated.")
  }
  const bucketShardIndexes = new Uint32Array(bucketCount)
  const bucketRouteEpochs = new Float64Array(bucketCount)
  const bucketOwnershipCodes = new Uint8Array(bucketCount)
  const referencedShards = new Set<number>()
  for (let bucketId = 0; bucketId < bucketCount; bucketId += 1) {
    const shardIndex = reader.uint32()
    if (shardIndex >= shardCount) routeError("A bucket references an unknown shard descriptor.")
    const routeEpoch = reader.uint64("Bucket route epoch")
    const ownershipCode = reader.uint8()
    if (!OWNERSHIP_STATES[ownershipCode]) {
      routeError("A bucket contains an invalid ownership state.")
    }
    bucketShardIndexes[bucketId] = shardIndex
    bucketRouteEpochs[bucketId] = routeEpoch
    bucketOwnershipCodes[bucketId] = ownershipCode
    referencedShards.add(shardIndex)
  }
  const reservedCount = reader.uint32()
  if (
    reservedCount > MAX_RESERVED_BUCKETS ||
    reader.remaining < reservedCount * RESERVED_ROUTE_BYTES + 4
  ) {
    routeError("The route manifest reserved-bucket route table is malformed.")
  }
  const reservedBucketIds = new Uint32Array(reservedCount)
  const reservedBucketShardIndexes = new Uint32Array(reservedCount)
  const reservedBucketRouteEpochs = new Float64Array(reservedCount)
  const reservedBucketOwnershipCodes = new Uint8Array(reservedCount)
  for (let index = 0; index < reservedCount; index += 1) {
    const bucketId = reader.uint32()
    if (
      bucketId < bucketCount ||
      (index > 0 && bucketId <= (reservedBucketIds[index - 1] as number))
    ) {
      routeError(
        "Reserved bucket IDs must be uniquely sorted outside the dense hash-bucket namespace.",
      )
    }
    const shardIndex = reader.uint32()
    if (shardIndex >= shardCount) {
      routeError("A reserved bucket references an unknown shard descriptor.")
    }
    const routeEpoch = reader.uint64("Reserved bucket route epoch")
    const ownershipCode = reader.uint8()
    if (!OWNERSHIP_STATES[ownershipCode]) {
      routeError("A reserved bucket contains an invalid ownership state.")
    }
    reservedBucketIds[index] = bucketId
    reservedBucketShardIndexes[index] = shardIndex
    reservedBucketRouteEpochs[index] = routeEpoch
    reservedBucketOwnershipCodes[index] = ownershipCode
    referencedShards.add(shardIndex)
  }
  if (referencedShards.size !== shards.length) {
    routeError("Every shard descriptor must be referenced by a dense or reserved bucket route.")
  }
  const reserved = new Set(reservedBucketIds)

  const overrideCount = reader.uint32()
  if (
    overrideCount > Math.min(reservedCount, MAX_OVERRIDES) ||
    reader.remaining !== overrideCount * 36
  ) {
    routeError("The route manifest override table is malformed.")
  }
  const overrides: NormalizedOverride[] = []
  const overriddenBuckets = new Set<number>()
  for (let index = 0; index < overrideCount; index += 1) {
    const digest = reader.bytes(32)
    const bucketId = reader.uint32()
    if (!reserved.has(bucketId)) routeError("A route override targets a non-reserved bucket.")
    if (
      index > 0 &&
      compareBytes((overrides[index - 1] as NormalizedOverride).digest, digest) >= 0
    ) {
      routeError("Route override digests must be uniquely sorted in full-digest order.")
    }
    if (overriddenBuckets.has(bucketId)) {
      routeError("A reserved bucket is assigned to more than one route override.")
    }
    overriddenBuckets.add(bucketId)
    overrides.push(Object.freeze({ bucketId, digest }))
  }
  if (expiresAtMs <= createdAtMs) routeError("Manifest expiry must be later than creation time.")

  return {
    bucketBits,
    bucketOwnershipCodes,
    bucketRouteEpochs,
    bucketShardIndexes,
    createdAtMs,
    environmentId,
    expiresAtMs,
    fleetId,
    overrides: Object.freeze(overrides),
    reservedBucketIds,
    reservedBucketOwnershipCodes,
    reservedBucketRouteEpochs,
    reservedBucketShardIndexes,
    shards: Object.freeze(shards),
    topologyVersion,
  }
}

async function sha256(payload: Uint8Array): Promise<Uint8Array> {
  const input = new Uint8Array(payload.byteLength)
  input.set(payload)
  return new Uint8Array(await crypto.subtle.digest("SHA-256", input.buffer))
}

export async function computeRouteManifestChecksum(payload: Uint8Array): Promise<string> {
  if (!(payload instanceof Uint8Array))
    configurationError("A manifest payload must be a Uint8Array.")
  if (payload.byteLength > MAX_MANIFEST_PAYLOAD_BYTES) {
    configurationError(
      `The route manifest payload exceeds the ${MAX_MANIFEST_PAYLOAD_BYTES}-byte limit.`,
    )
  }
  return bytesToHex(await sha256(payload))
}

export async function verifyRouteManifestChecksum(
  payload: Uint8Array,
  expectedChecksum: string,
): Promise<void> {
  if (!(payload instanceof Uint8Array))
    configurationError("A manifest payload must be a Uint8Array.")
  if (payload.byteLength > MAX_MANIFEST_PAYLOAD_BYTES) {
    routeError(`The route manifest payload exceeds the ${MAX_MANIFEST_PAYLOAD_BYTES}-byte limit.`)
  }
  if (typeof expectedChecksum !== "string" || !CHECKSUM_PATTERN.test(expectedChecksum)) {
    routeError("A route manifest checksum must be 64 lowercase hexadecimal characters.")
  }
  const expected = decodeDigestHex(expectedChecksum)
  const actual = await sha256(payload)
  if (!bytesEqual(actual, expected)) routeError("The route manifest checksum does not match.")
}

export interface RouteManifest {
  readonly bucketBits: BucketBits
  readonly checksum: string
  readonly createdAtMs: number
  readonly environmentId: string
  readonly expiresAtMs: number
  readonly fleetId: string
  readonly hashVersion: typeof HASH_VERSION
  readonly manifestVersion: typeof ROUTE_MANIFEST_VERSION
  readonly shards: readonly RouteShardDescriptor[]
  readonly topologyVersion: number

  assertCompatible(requirements: RouteManifestCompatibility): void
  payload(): Uint8Array
  resolve(digest: Uint8Array): ResolvedRoute
}

class VerifiedRouteManifest implements RouteManifest {
  readonly bucketBits: BucketBits
  readonly checksum: string
  readonly createdAtMs: number
  readonly environmentId: string
  readonly expiresAtMs: number
  readonly fleetId: string
  readonly hashVersion = HASH_VERSION
  readonly manifestVersion = ROUTE_MANIFEST_VERSION
  readonly shards: readonly RouteShardDescriptor[]
  readonly topologyVersion: number

  readonly #bucketOwnershipCodes: Uint8Array
  readonly #bucketRouteEpochs: Float64Array
  readonly #bucketShardIndexes: Uint32Array
  readonly #overrides: readonly NormalizedOverride[]
  readonly #payload: Uint8Array
  readonly #reservedBucketIds: Uint32Array
  readonly #reservedBucketOwnershipCodes: Uint8Array
  readonly #reservedBucketRouteEpochs: Float64Array
  readonly #reservedBucketShardIndexes: Uint32Array

  constructor(manifest: NormalizedManifest, payload: Uint8Array, checksum: string) {
    this.bucketBits = manifest.bucketBits
    this.checksum = checksum
    this.createdAtMs = manifest.createdAtMs
    this.environmentId = manifest.environmentId
    this.expiresAtMs = manifest.expiresAtMs
    this.fleetId = manifest.fleetId
    this.shards = manifest.shards
    this.topologyVersion = manifest.topologyVersion
    this.#bucketOwnershipCodes = manifest.bucketOwnershipCodes
    this.#bucketRouteEpochs = manifest.bucketRouteEpochs
    this.#bucketShardIndexes = manifest.bucketShardIndexes
    this.#overrides = manifest.overrides
    this.#payload = payload.slice()
    this.#reservedBucketIds = manifest.reservedBucketIds
    this.#reservedBucketOwnershipCodes = manifest.reservedBucketOwnershipCodes
    this.#reservedBucketRouteEpochs = manifest.reservedBucketRouteEpochs
    this.#reservedBucketShardIndexes = manifest.reservedBucketShardIndexes
    Object.freeze(this)
  }

  payload(): Uint8Array {
    return this.#payload.slice()
  }

  assertCompatible(requirements: RouteManifestCompatibility): void {
    if (!isRecord(requirements)) {
      configurationError("Route manifest compatibility requirements must be an object.")
    }
    assertSafeUnsigned(requirements.nowMs, "Current time")
    assertSafeUnsigned(requirements.minimumTopologyVersion, "Minimum topology version")
    if (requirements.maximumTopologyVersion !== undefined) {
      assertSafeUnsigned(requirements.maximumTopologyVersion, "Maximum topology version")
      if (requirements.maximumTopologyVersion < requirements.minimumTopologyVersion) {
        configurationError("The accepted topology range cannot be inverted.")
      }
    }
    if (
      requirements.fleetId !== this.fleetId ||
      requirements.environmentId !== this.environmentId ||
      requirements.hashVersion !== this.hashVersion ||
      requirements.bucketBits !== this.bucketBits
    ) {
      routeError("The route manifest identity is incompatible with this caller.")
    }
    if (requirements.nowMs >= this.expiresAtMs) {
      throw new NozzleError("StaleRouteRejectedError", "The route manifest has expired.", {
        details: { expiresAtMs: this.expiresAtMs, nowMs: requirements.nowMs },
      })
    }
    if (this.topologyVersion < requirements.minimumTopologyVersion) {
      throw new NozzleError(
        "StaleRouteRejectedError",
        "The route manifest topology is older than the caller requires.",
        {
          details: {
            actual: this.topologyVersion,
            minimum: requirements.minimumTopologyVersion,
          },
        },
      )
    }
    if (
      requirements.maximumTopologyVersion !== undefined &&
      this.topologyVersion > requirements.maximumTopologyVersion
    ) {
      routeError("The route manifest topology is newer than the caller supports.", {
        actual: this.topologyVersion,
        maximum: requirements.maximumTopologyVersion,
      })
    }
    if (requirements.schemaVersion !== undefined) {
      assertUint32(requirements.schemaVersion, "Caller schema version")
      const incompatible = this.shards.find(
        (shard) =>
          requirements.schemaVersion !== undefined &&
          (requirements.schemaVersion < shard.schemaCompatibility.minimum ||
            requirements.schemaVersion > shard.schemaCompatibility.maximum),
      )
      if (incompatible) {
        throw new NozzleError(
          "SchemaDriftError",
          "The caller schema version is incompatible with a routed shard.",
          {
            details: {
              callerSchemaVersion: requirements.schemaVersion,
              shardId: incompatible.id,
            },
          },
        )
      }
    }
  }

  resolve(digest: Uint8Array): ResolvedRoute {
    if (!(digest instanceof Uint8Array) || digest.byteLength !== 32) {
      configurationError("Route resolution requires the full 32-byte partition digest.")
    }
    let lower = 0
    let upper = this.#overrides.length - 1
    let overriddenBucket: number | undefined
    while (lower <= upper) {
      const middle = Math.floor((lower + upper) / 2)
      const override = this.#overrides[middle] as NormalizedOverride
      const comparison = compareBytes(digest, override.digest)
      if (comparison === 0) {
        overriddenBucket = override.bucketId
        break
      }
      if (comparison < 0) upper = middle - 1
      else lower = middle + 1
    }

    const bucketId = overriddenBucket ?? selectBucket(digest, this.bucketBits)
    const reservedIndex =
      overriddenBucket === undefined
        ? -1
        : findSortedBucketIndex(this.#reservedBucketIds, overriddenBucket)
    const shardIndex =
      reservedIndex < 0
        ? (this.#bucketShardIndexes[bucketId] as number)
        : (this.#reservedBucketShardIndexes[reservedIndex] as number)
    const shard = this.shards[shardIndex] as RouteShardDescriptor
    const ownershipCode =
      reservedIndex < 0
        ? (this.#bucketOwnershipCodes[bucketId] as number)
        : (this.#reservedBucketOwnershipCodes[reservedIndex] as number)
    const ownershipState = OWNERSHIP_STATES[ownershipCode] as OwnershipState
    const routeEpoch =
      reservedIndex < 0
        ? (this.#bucketRouteEpochs[bucketId] as number)
        : (this.#reservedBucketRouteEpochs[reservedIndex] as number)
    return Object.freeze({
      bucketId,
      destination: shard.destination,
      destinationKind: shard.kind,
      environmentId: this.environmentId,
      fleetId: this.fleetId,
      hashVersion: this.hashVersion,
      jurisdiction: shard.jurisdiction,
      manifestVersion: this.manifestVersion,
      overridden: overriddenBucket !== undefined,
      ownershipState,
      routeEpoch,
      schemaCompatibility: shard.schemaCompatibility,
      shardId: shard.id,
      topologyVersion: this.topologyVersion,
    })
  }
}

function findSortedBucketIndex(bucketIds: Uint32Array, bucketId: number): number {
  let lower = 0
  let upper = bucketIds.length
  while (lower < upper) {
    const middle = Math.floor((lower + upper) / 2)
    const candidate = bucketIds[middle] as number
    if (candidate < bucketId) lower = middle + 1
    else upper = middle
  }
  return lower
}

export async function createRouteManifest(source: RouteManifestSource): Promise<RouteManifest> {
  const normalized = normalizeSource(source)
  const payload = encodeNormalized(normalized)
  const checksum = await computeRouteManifestChecksum(payload)
  return new VerifiedRouteManifest(normalized, payload, checksum)
}

export async function loadRouteManifest(
  payload: Uint8Array,
  expectedChecksum: string,
): Promise<RouteManifest> {
  if (!(payload instanceof Uint8Array))
    configurationError("A manifest payload must be a Uint8Array.")
  if (payload.byteLength > MAX_MANIFEST_PAYLOAD_BYTES) {
    routeError(`The route manifest payload exceeds the ${MAX_MANIFEST_PAYLOAD_BYTES}-byte limit.`)
  }
  const copy = payload.slice()
  await verifyRouteManifestChecksum(copy, expectedChecksum)
  const normalized = decodePayload(copy)
  return new VerifiedRouteManifest(normalized, copy, expectedChecksum)
}
