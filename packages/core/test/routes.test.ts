import fc from "fast-check"
import { describe, expect, it } from "vitest"
import { bytesToHex } from "../src/hash.js"
import type { OwnershipState } from "../src/ownership.js"
import {
  computeRouteManifestChecksum,
  createRouteManifest,
  loadRouteManifest,
  type ReservedBucketRoute,
  type RouteManifest,
  type RouteManifestSource,
  type RouteOverride,
  type RouteShardDescriptor,
  verifyRouteManifestChecksum,
} from "../src/routes.js"

const BUCKET_COUNT = 2 ** 16
const RESERVED_A = BUCKET_COUNT
const RESERVED_B = BUCKET_COUNT + 1
const digestA = `${"00".repeat(31)}01`
const digestB = `${"00".repeat(31)}02`

function shard(
  id: string,
  kind: "binding" | "router",
  minimum: number,
  maximum: number,
): RouteShardDescriptor {
  return {
    destination: kind === "binding" ? `DB_${id.toUpperCase()}` : `router-${id}`,
    id,
    jurisdiction: id === "a" ? "EU" : "WNAM",
    kind,
    schemaCompatibility: { maximum, minimum },
  }
}

function source(overrides: readonly RouteOverride[] = []): RouteManifestSource {
  const reservedBucketRoutes = [...new Set(overrides.map((override) => override.bucketId))].map(
    (bucketId): ReservedBucketRoute => ({
      bucketId,
      ownershipState: bucketId === RESERVED_B ? "read_only" : "writable",
      routeEpoch: bucketId - BUCKET_COUNT + 110,
      shardId: bucketId % 2 === 0 ? "a" : "b",
    }),
  )
  return {
    bucketBits: 16,
    bucketOwnershipStates: Array.from(
      { length: BUCKET_COUNT },
      (_, bucketId): OwnershipState => (bucketId === 101 ? "read_only" : "writable"),
    ),
    bucketRouteEpochs: Array.from({ length: BUCKET_COUNT }, (_, bucketId) => bucketId + 10),
    bucketToShard: Array.from({ length: BUCKET_COUNT }, (_, bucketId) =>
      bucketId % 2 === 0 ? "a" : "b",
    ),
    createdAtMs: 1_000,
    environmentId: "production",
    expiresAtMs: 2_000,
    fleetId: "fleet-example",
    hashVersion: 1,
    overrides,
    reservedBucketRoutes,
    shards: [shard("b", "router", 2, 4), shard("a", "binding", 1, 4)],
    topologyVersion: 7,
  }
}

function digestForBucket(bucketId: number): Uint8Array {
  const digest = new Uint8Array(32)
  digest[0] = bucketId >>> 8
  digest[1] = bucketId & 0xff
  return digest
}

async function expectMutatedPayloadFailure(
  manifest: RouteManifest,
  mutate: (payload: Uint8Array, view: DataView) => void,
  message?: string,
): Promise<void> {
  const payload = manifest.payload()
  mutate(payload, new DataView(payload.buffer, payload.byteOffset, payload.byteLength))
  const result = loadRouteManifest(payload, await computeRouteManifestChecksum(payload))
  if (message) await expect(result).rejects.toThrow(message)
  else await expect(result).rejects.toBeInstanceOf(Error)
}

describe("route manifests", () => {
  it("builds a deterministic canonical binary payload independent of descriptor input order", async () => {
    const firstSource = source([
      { bucketId: RESERVED_B, digestHex: digestB },
      { bucketId: RESERVED_A, digestHex: digestA },
    ])
    const secondSource: RouteManifestSource = {
      ...firstSource,
      overrides: [...(firstSource.overrides ?? [])].reverse(),
      reservedBucketRoutes: [...(firstSource.reservedBucketRoutes ?? [])].reverse(),
      shards: [...firstSource.shards].reverse(),
    }

    const first = await createRouteManifest(firstSource)
    const second = await createRouteManifest(secondSource)

    expect(first.checksum).toBe(second.checksum)
    expect(first.payload()).toEqual(second.payload())
    expect(first.payload().subarray(0, 8)).toEqual(
      Uint8Array.of(0x4e, 0x5a, 0x52, 0x4d, 1, 1, 16, 0),
    )
    expect(first.payload()).toHaveLength(852_199)
    expect(first.checksum).toBe("fe48ba13a05b7bb23b61d8b148c69c16f06f3fa7e41873f12b0efc10c83aac0b")
    expect(await computeRouteManifestChecksum(first.payload())).toBe(first.checksum)
  })

  it("supports the verified 20-bit bucket profile without changing resolution semantics", async () => {
    const bucketCount = 2 ** 20
    const manifest = await createRouteManifest({
      bucketBits: 20,
      bucketOwnershipStates: new Array<OwnershipState>(bucketCount).fill("writable"),
      bucketRouteEpochs: new Array<number>(bucketCount).fill(1),
      bucketToShard: new Array<string>(bucketCount).fill("a"),
      createdAtMs: 1,
      environmentId: "scale",
      expiresAtMs: 2,
      fleetId: "large-fleet",
      hashVersion: 1,
      shards: [shard("a", "binding", 1, 1)],
      topologyVersion: 1,
    })
    const digest = new Uint8Array(32)
    digest[0] = 0xff
    digest[1] = 0xff
    digest[2] = 0xf0
    expect(manifest.resolve(digest)).toMatchObject({
      bucketId: bucketCount - 1,
      overridden: false,
      shardId: "a",
    })
  })

  it("preserves well-formed supplementary Unicode in canonical identities", async () => {
    const input = source()
    const manifest = await createRouteManifest({ ...input, environmentId: "prod-😀" })
    const loaded = await loadRouteManifest(manifest.payload(), manifest.checksum)
    expect(loaded.environmentId).toBe("prod-😀")
  })

  it("loads only checksum-verified canonical payloads and isolates retained bytes", async () => {
    const created = await createRouteManifest(
      source([{ bucketId: RESERVED_A, digestHex: digestA }]),
    )
    const payload = created.payload()
    const loaded = await loadRouteManifest(payload, created.checksum)
    payload.fill(0)

    expect(loaded.payload()).toEqual(created.payload())
    expect(
      loaded.resolve(Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0))),
    ).toMatchObject({ bucketId: RESERVED_A, overridden: true, shardId: "a" })

    const tampered = created.payload()
    tampered[tampered.length - 1] = (tampered[tampered.length - 1] ?? 0) ^ 1
    await expect(loadRouteManifest(tampered, created.checksum)).rejects.toMatchObject({
      code: "RouteVersionConflictError",
    })
    await expect(verifyRouteManifestChecksum(created.payload(), "A".repeat(64))).rejects.toThrow(
      "lowercase hexadecimal",
    )

    const racingPayload = created.payload()
    const loading = loadRouteManifest(racingPayload, created.checksum)
    racingPayload[34] = 0x46
    const raced = await loading
    expect(raced.fleetId).toBe("fleet-example")
    expect(raced.payload()[34]).toBe(0x66)
  })

  it("uses a full-digest binary search before the dense bucket directory", async () => {
    const manifest = await createRouteManifest(
      source([
        { bucketId: RESERVED_B, digestHex: digestB },
        { bucketId: RESERVED_A, digestHex: digestA },
      ]),
    )
    const first = manifest.resolve(
      Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0)),
    )
    const second = manifest.resolve(
      Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 2 : 0)),
    )
    const sharedPrefixOnly = manifest.resolve(new Uint8Array(32))

    expect(first).toEqual({
      bucketId: RESERVED_A,
      destination: "DB_A",
      destinationKind: "binding",
      environmentId: "production",
      fleetId: "fleet-example",
      hashVersion: 1,
      jurisdiction: "EU",
      manifestVersion: 1,
      overridden: true,
      ownershipState: "writable",
      routeEpoch: 110,
      schemaCompatibility: { maximum: 4, minimum: 1 },
      shardId: "a",
      topologyVersion: 7,
    })
    expect(second).toMatchObject({
      bucketId: RESERVED_B,
      overridden: true,
      ownershipState: "read_only",
      routeEpoch: 111,
      shardId: "b",
    })
    expect(sharedPrefixOnly).toMatchObject({ bucketId: 0, overridden: false, shardId: "a" })
    expect(manifest.resolve(digestForBucket(100))).toMatchObject({
      bucketId: 100,
      overridden: false,
    })
  })

  it("allows a shard referenced only by a sparse reserved bucket route", async () => {
    const input = source([{ bucketId: RESERVED_B, digestHex: digestA }])
    const manifest = await createRouteManifest({
      ...input,
      bucketToShard: input.bucketToShard.map(() => "a"),
    })
    expect(
      manifest.resolve(Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0))),
    ).toMatchObject({
      bucketId: RESERVED_B,
      ownershipState: "read_only",
      routeEpoch: 111,
      shardId: "b",
    })
  })

  it("supports the full sparse uint32 reserved-bucket namespace", async () => {
    const maximumReservedBucket = 0xffff_ffff
    const manifest = await createRouteManifest(
      source([{ bucketId: maximumReservedBucket, digestHex: digestA }]),
    )
    expect(
      manifest.resolve(Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0))),
    ).toMatchObject({ bucketId: maximumReservedBucket, overridden: true, shardId: "b" })
  })

  it("rejects route structures that exceed bounded Worker-memory budgets", async () => {
    const oversizedPayload = new Uint8Array(24 * 1_024 * 1_024 + 1)
    await expect(computeRouteManifestChecksum(oversizedPayload)).rejects.toThrow("payload exceeds")
    await expect(verifyRouteManifestChecksum(oversizedPayload, "0".repeat(64))).rejects.toThrow(
      "payload exceeds",
    )
    await expect(loadRouteManifest(oversizedPayload, "0".repeat(64))).rejects.toThrow(
      "payload exceeds",
    )

    await expect(
      createRouteManifest({
        ...source(),
        reservedBucketRoutes: new Array<ReservedBucketRoute>(65_537).fill({
          bucketId: RESERVED_A,
          ownershipState: "writable",
          routeEpoch: 1,
          shardId: "a",
        }),
      }),
    ).rejects.toThrow("more than 65536 reserved bucket routes")
    await expect(
      createRouteManifest({
        ...source(),
        overrides: new Array<RouteOverride>(65_537).fill({
          bucketId: RESERVED_A,
          digestHex: digestA,
        }),
      }),
    ).rejects.toThrow("cannot contain more than 65536 overrides")

    const long = "x".repeat(1_024)
    const descriptors = Array.from({ length: 700 }, (_, index) => ({
      destination: long,
      id: `${index.toString().padStart(4, "0")}${long.slice(4)}`,
      jurisdiction: long,
      kind: "binding" as const,
      schemaCompatibility: { minimum: 1, maximum: 1 },
    }))
    await expect(createRouteManifest({ ...source(), shards: descriptors })).rejects.toThrow(
      "shard descriptors exceeds",
    )
  })

  it("resolves every generated bucket through the dense directory", async () => {
    const manifest = await createRouteManifest(source())
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: BUCKET_COUNT - 1 }), async (bucketId) => {
        const route = manifest.resolve(digestForBucket(bucketId))
        expect(route.bucketId).toBe(bucketId)
        expect(route.shardId).toBe(bucketId % 2 === 0 ? "a" : "b")
        expect(route.routeEpoch).toBe(bucketId + 10)
      }),
      { numRuns: 500 },
    )
  })

  it("copies and freezes caller-owned data at the validation boundary", async () => {
    const input = source([{ bucketId: RESERVED_A, digestHex: digestA }])
    const manifest = await createRouteManifest(input)
    ;(input.bucketToShard as string[])[0] = "b"
    ;(input.bucketRouteEpochs as number[])[0] = 999
    ;(input.bucketOwnershipStates as string[])[0] = "retired"
    ;(input.shards[1]?.schemaCompatibility as { minimum: number }).minimum = 99
    ;(input.overrides as RouteOverride[])[0] = { bucketId: 100, digestHex: digestB }
    ;(input.reservedBucketRoutes as ReservedBucketRoute[])[0] = {
      bucketId: RESERVED_B,
      ownershipState: "retired",
      routeEpoch: 999,
      shardId: "b",
    }
    const exported = manifest.payload()
    exported.fill(0)

    expect(manifest.resolve(new Uint8Array(32))).toMatchObject({
      ownershipState: "writable",
      routeEpoch: 10,
      schemaCompatibility: { maximum: 4, minimum: 1 },
      shardId: "a",
    })
    expect(
      manifest.resolve(Uint8Array.from({ length: 32 }, (_, index) => (index === 31 ? 1 : 0))),
    ).toMatchObject({ bucketId: RESERVED_A, overridden: true })
    expect(Object.isFrozen(manifest)).toBe(true)
    expect(Object.isFrozen(manifest.shards)).toBe(true)
    expect(Object.isFrozen(manifest.shards[0]?.schemaCompatibility)).toBe(true)
    expect(manifest.payload().some((byte) => byte !== 0)).toBe(true)
  })

  it("fails closed for expired, rolled-back, future, identity, and schema-incompatible manifests", async () => {
    const manifest = await createRouteManifest(source())
    const compatible = {
      bucketBits: 16 as const,
      environmentId: "production",
      fleetId: "fleet-example",
      hashVersion: 1 as const,
      maximumTopologyVersion: 7,
      minimumTopologyVersion: 7,
      nowMs: 1_999,
      schemaVersion: 3,
    }
    expect(() => manifest.assertCompatible(compatible)).not.toThrow()
    expect(() => manifest.assertCompatible({ ...compatible, nowMs: 2_000 })).toThrowError(
      expect.objectContaining({ code: "StaleRouteRejectedError" }),
    )
    expect(() =>
      manifest.assertCompatible({
        ...compatible,
        maximumTopologyVersion: 8,
        minimumTopologyVersion: 8,
      }),
    ).toThrowError(expect.objectContaining({ code: "StaleRouteRejectedError" }))
    expect(() =>
      manifest.assertCompatible({
        ...compatible,
        maximumTopologyVersion: 6,
        minimumTopologyVersion: 0,
      }),
    ).toThrowError(expect.objectContaining({ code: "RouteVersionConflictError" }))
    expect(() => manifest.assertCompatible({ ...compatible, fleetId: "another-fleet" })).toThrow(
      "identity is incompatible",
    )
    expect(() => manifest.assertCompatible({ ...compatible, environmentId: "staging" })).toThrow(
      "identity is incompatible",
    )
    expect(() => manifest.assertCompatible({ ...compatible, hashVersion: 2 as 1 })).toThrow(
      "identity is incompatible",
    )
    expect(() => manifest.assertCompatible({ ...compatible, bucketBits: 20 })).toThrow(
      "identity is incompatible",
    )
    expect(() => manifest.assertCompatible({ ...compatible, schemaVersion: 1 })).toThrowError(
      expect.objectContaining({ code: "SchemaDriftError" }),
    )
    expect(() => manifest.assertCompatible({ ...compatible, schemaVersion: 5 })).toThrowError(
      expect.objectContaining({ code: "SchemaDriftError" }),
    )
    expect(() =>
      manifest.assertCompatible({
        bucketBits: 16,
        environmentId: "production",
        fleetId: "fleet-example",
        hashVersion: 1,
        minimumTopologyVersion: 0,
        nowMs: 1_500,
      }),
    ).not.toThrow()
  })

  it.each([
    {
      name: "hash version",
      mutate: (value: RouteManifestSource) => ({ ...value, hashVersion: 2 as 1 }),
      message: "hash version 1",
    },
    {
      name: "bucket bits",
      mutate: (value: RouteManifestSource) => ({ ...value, bucketBits: 18 as 16 }),
      message: "16 or 20",
    },
    {
      name: "expiry",
      mutate: (value: RouteManifestSource) => ({ ...value, expiresAtMs: value.createdAtMs }),
      message: "later than",
    },
    {
      name: "negative topology",
      mutate: (value: RouteManifestSource) => ({ ...value, topologyVersion: -1 }),
      message: "non-negative safe integer",
    },
    {
      name: "empty fleet identity",
      mutate: (value: RouteManifestSource) => ({ ...value, fleetId: "" }),
      message: "non-empty string",
    },
    {
      name: "unpaired high surrogate",
      mutate: (value: RouteManifestSource) => ({ ...value, environmentId: "\ud800" }),
      message: "unpaired UTF-16",
    },
    {
      name: "unpaired low surrogate",
      mutate: (value: RouteManifestSource) => ({ ...value, environmentId: "\udc00" }),
      message: "unpaired UTF-16",
    },
    {
      name: "oversized identity",
      mutate: (value: RouteManifestSource) => ({ ...value, environmentId: "x".repeat(1_025) }),
      message: "1024-byte",
    },
    {
      name: "missing shards",
      mutate: (value: RouteManifestSource) => ({ ...value, shards: [] }),
      message: "at least one shard",
    },
    {
      name: "non-array shards",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        shards: {} as readonly RouteShardDescriptor[],
      }),
      message: "at least one shard",
    },
    {
      name: "excess shard descriptors",
      mutate: (value: RouteManifestSource) => {
        const shards = [shard("a", "binding", 1, 1)]
        shards.length = 50_001
        return { ...value, shards }
      },
      message: "more than 50000",
    },
    {
      name: "invalid destination kind",
      mutate: (value: RouteManifestSource) => {
        const first = value.shards[0] ?? shard("a", "binding", 1, 4)
        const second = value.shards[1] ?? shard("b", "router", 2, 4)
        return { ...value, shards: [{ ...first, kind: "direct" as "binding" }, second] }
      },
      message: "binding or router",
    },
    {
      name: "oversized schema version",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        shards: [
          shard("a", "binding", 0, 0x1_0000_0000),
          value.shards[0] ?? shard("b", "router", 2, 4),
        ],
      }),
      message: "unsigned 32-bit",
    },
    {
      name: "dense shard table length",
      mutate: (value: RouteManifestSource) => ({ ...value, bucketToShard: [] }),
      message: "wrong length",
    },
    {
      name: "dense epoch table length",
      mutate: (value: RouteManifestSource) => ({ ...value, bucketRouteEpochs: [] }),
      message: "wrong length",
    },
    {
      name: "dense state table length",
      mutate: (value: RouteManifestSource) => ({ ...value, bucketOwnershipStates: [] }),
      message: "wrong length",
    },
    {
      name: "unknown shard",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        bucketToShard: value.bucketToShard.map((id, index) => (index === 0 ? "missing" : id)),
      }),
      message: "declared shard",
    },
    {
      name: "missing shard entry",
      mutate: (value: RouteManifestSource) => {
        const bucketToShard = [...value.bucketToShard]
        delete bucketToShard[0]
        return { ...value, bucketToShard }
      },
      message: "declared shard",
    },
    {
      name: "missing route epoch",
      mutate: (value: RouteManifestSource) => {
        const bucketRouteEpochs = [...value.bucketRouteEpochs]
        delete bucketRouteEpochs[0]
        return { ...value, bucketRouteEpochs }
      },
      message: "non-negative safe integer",
    },
    {
      name: "negative route epoch",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        bucketRouteEpochs: value.bucketRouteEpochs.map((epoch, index) =>
          index === 0 ? -1 : epoch,
        ),
      }),
      message: "non-negative safe integer",
    },
    {
      name: "missing ownership state",
      mutate: (value: RouteManifestSource) => {
        const bucketOwnershipStates = [...value.bucketOwnershipStates]
        delete bucketOwnershipStates[0]
        return { ...value, bucketOwnershipStates }
      },
      message: "valid ownership state",
    },
    {
      name: "duplicate shard",
      mutate: (value: RouteManifestSource) => {
        const first = value.shards[0] ?? shard("a", "binding", 1, 4)
        return { ...value, shards: [first, first] }
      },
      message: "unique",
    },
    {
      name: "unreferenced shard",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        bucketToShard: value.bucketToShard.map(() => "a"),
      }),
      message: "referenced",
    },
    {
      name: "invalid state",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        bucketOwnershipStates: value.bucketOwnershipStates.map((state, index) =>
          index === 0 ? ("invalid" as "writable") : state,
        ),
      }),
      message: "valid ownership state",
    },
    {
      name: "inverted schema range",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        shards: [shard("a", "binding", 3, 2), value.shards[0] ?? shard("b", "router", 2, 4)],
      }),
      message: "cannot be inverted",
    },
    {
      name: "non-reserved override target",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        overrides: [{ bucketId: RESERVED_A, digestHex: digestA }],
        reservedBucketRoutes: [],
      }),
      message: "reserved bucket",
    },
    {
      name: "reserved bucket inside dense hash space",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: BUCKET_COUNT - 1,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: "a",
          },
        ],
      }),
      message: "outside the dense",
    },
    {
      name: "duplicate reserved bucket",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: "a",
          },
          {
            bucketId: RESERVED_A,
            ownershipState: "read_only" as const,
            routeEpoch: 2,
            shardId: "b",
          },
        ],
      }),
      message: "must be unique",
    },
    {
      name: "negative reserved bucket",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: -1,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: "a",
          },
        ],
      }),
      message: "non-negative safe integer",
    },
    {
      name: "reserved bucket above uint32",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: 0x1_0000_0000,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: "a",
          },
        ],
      }),
      message: "unsigned 32-bit",
    },
    {
      name: "reserved bucket unknown shard",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: "missing",
          },
        ],
      }),
      message: "declared shard",
    },
    {
      name: "reserved bucket non-string shard",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "writable" as const,
            routeEpoch: 1,
            shardId: 1 as unknown as string,
          },
        ],
      }),
      message: "declared shard",
    },
    {
      name: "reserved bucket negative epoch",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "writable" as const,
            routeEpoch: -1,
            shardId: "a",
          },
        ],
      }),
      message: "non-negative safe integer",
    },
    {
      name: "reserved bucket invalid state",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "invalid" as "writable",
            routeEpoch: 1,
            shardId: "a",
          },
        ],
      }),
      message: "valid ownership state",
    },
    {
      name: "reserved bucket missing state",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        reservedBucketRoutes: [
          {
            bucketId: RESERVED_A,
            ownershipState: "" as "writable",
            routeEpoch: 1,
            shardId: "a",
          },
        ],
      }),
      message: "valid ownership state",
    },
    {
      name: "truncated override digest",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        overrides: [{ bucketId: RESERVED_A, digestHex: "00".repeat(16) }],
        reservedBucketRoutes:
          source([{ bucketId: RESERVED_A, digestHex: digestA }]).reservedBucketRoutes ?? [],
      }),
      message: "64 lowercase",
    },
    {
      name: "non-string override digest",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        overrides: [{ bucketId: RESERVED_A, digestHex: 1 as unknown as string }],
        reservedBucketRoutes:
          source([{ bucketId: RESERVED_A, digestHex: digestA }]).reservedBucketRoutes ?? [],
      }),
      message: "64 lowercase",
    },
    {
      name: "duplicate override digest",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        overrides: [
          { bucketId: RESERVED_A, digestHex: digestA },
          { bucketId: RESERVED_B, digestHex: digestA },
        ],
        reservedBucketRoutes:
          source([
            { bucketId: RESERVED_A, digestHex: digestA },
            { bucketId: RESERVED_B, digestHex: digestB },
          ]).reservedBucketRoutes ?? [],
      }),
      message: "digests must be unique",
    },
    {
      name: "reused reserved bucket",
      mutate: (value: RouteManifestSource) => ({
        ...value,
        overrides: [
          { bucketId: RESERVED_A, digestHex: digestA },
          { bucketId: RESERVED_A, digestHex: digestB },
        ],
        reservedBucketRoutes:
          source([{ bucketId: RESERVED_A, digestHex: digestA }]).reservedBucketRoutes ?? [],
      }),
      message: "only one route override",
    },
  ])("rejects invalid $name", async ({ mutate, message }) => {
    await expect(createRouteManifest(mutate(source()))).rejects.toThrow(message)
  })

  it("rejects malformed resolution and compatibility inputs", async () => {
    const manifest = await createRouteManifest(source())
    expect(() => manifest.resolve(new Uint8Array(31))).toThrow("full 32-byte")
    expect(() => manifest.resolve([] as unknown as Uint8Array)).toThrow("full 32-byte")
    expect(() =>
      manifest.assertCompatible({
        bucketBits: 16,
        environmentId: "production",
        fleetId: "fleet-example",
        hashVersion: 1,
        maximumTopologyVersion: 1,
        minimumTopologyVersion: 2,
        nowMs: 1_500,
      }),
    ).toThrow("cannot be inverted")
    expect(() =>
      manifest.assertCompatible({
        bucketBits: 16,
        environmentId: "production",
        fleetId: "fleet-example",
        hashVersion: 1,
        minimumTopologyVersion: 0,
        nowMs: -1,
      }),
    ).toThrow("non-negative safe integer")
    await expect(computeRouteManifestChecksum("payload" as unknown as Uint8Array)).rejects.toThrow(
      "Uint8Array",
    )
    await expect(
      loadRouteManifest("payload" as unknown as Uint8Array, "0".repeat(64)),
    ).rejects.toThrow("Uint8Array")
    await expect(
      verifyRouteManifestChecksum(manifest.payload(), 1 as unknown as string),
    ).rejects.toThrow("lowercase hexadecimal")
    await expect(
      verifyRouteManifestChecksum("payload" as unknown as Uint8Array, "0".repeat(64)),
    ).rejects.toThrow("Uint8Array")
    expect(() =>
      manifest.assertCompatible(null as unknown as Parameters<typeof manifest.assertCompatible>[0]),
    ).toThrow("must be an object")
  })

  it("runtime-validates untyped manifest object boundaries without leaking TypeErrors", async () => {
    await expect(createRouteManifest(null as unknown as RouteManifestSource)).rejects.toThrow(
      "source must be an object",
    )

    const invalidSources: readonly [string, (value: RouteManifestSource) => RouteManifestSource][] =
      [
        [
          "bucket-to-shard table",
          (value) => ({ ...value, bucketToShard: {} as readonly string[] }),
        ],
        ["epoch table", (value) => ({ ...value, bucketRouteEpochs: {} as readonly number[] })],
        [
          "ownership table",
          (value) => ({ ...value, bucketOwnershipStates: {} as readonly OwnershipState[] }),
        ],
        [
          "reserved bucket routes",
          (value) => ({
            ...value,
            reservedBucketRoutes: {} as readonly ReservedBucketRoute[],
          }),
        ],
        ["overrides", (value) => ({ ...value, overrides: {} as readonly RouteOverride[] })],
        [
          "shard descriptor",
          (value) => ({ ...value, shards: [null as unknown as RouteShardDescriptor] }),
        ],
        [
          "schema range",
          (value) => ({
            ...value,
            shards: [
              {
                ...(value.shards[0] ?? shard("a", "binding", 1, 4)),
                schemaCompatibility: null as unknown as RouteShardDescriptor["schemaCompatibility"],
              },
            ],
          }),
        ],
        [
          "override object",
          (value) => ({
            ...value,
            overrides: [null as unknown as RouteOverride],
            reservedBucketRoutes:
              source([{ bucketId: RESERVED_A, digestHex: digestA }]).reservedBucketRoutes ?? [],
          }),
        ],
        [
          "reserved bucket route object",
          (value) => ({
            ...value,
            reservedBucketRoutes: [null as unknown as ReservedBucketRoute],
          }),
        ],
      ]
    for (const [name, mutate] of invalidSources) {
      await expect(createRouteManifest(mutate(source())), name).rejects.toMatchObject({
        code: "ConfigurationError",
      })
    }
  })

  it("rejects truncated and structurally non-canonical payloads even with matching checksums", async () => {
    const manifest = await createRouteManifest(source())
    const truncated = manifest.payload().subarray(0, 100)
    await expect(
      loadRouteManifest(truncated, await computeRouteManifestChecksum(truncated)),
    ).rejects.toMatchObject({ code: "RouteVersionConflictError" })

    const truncatedDenseTable = manifest.payload().subarray(0, 217)
    await expect(
      loadRouteManifest(
        truncatedDenseTable,
        await computeRouteManifestChecksum(truncatedDenseTable),
      ),
    ).rejects.toThrow("dense bucket table is truncated")

    const reserved = await createRouteManifest(
      source([{ bucketId: RESERVED_A, digestHex: digestA }]),
    )
    const truncatedReservedRoutes = reserved.payload().subarray(0, 852_100)
    await expect(
      loadRouteManifest(
        truncatedReservedRoutes,
        await computeRouteManifestChecksum(truncatedReservedRoutes),
      ),
    ).rejects.toThrow("reserved-bucket route table is malformed")

    const trailing = new Uint8Array(manifest.payload().length + 1)
    trailing.set(manifest.payload())
    await expect(
      loadRouteManifest(trailing, await computeRouteManifestChecksum(trailing)),
    ).rejects.toThrow("malformed")
  })

  it("rejects every malformed canonical binary section after checksum verification", async () => {
    const plain = await createRouteManifest(source())
    const overridden = await createRouteManifest(
      source([
        { bucketId: RESERVED_B, digestHex: digestB },
        { bucketId: RESERVED_A, digestHex: digestA },
      ]),
    )
    const plainCases: readonly [string, (payload: Uint8Array, view: DataView) => void, string][] = [
      ["magic", (payload) => (payload[0] = 0), "magic bytes"],
      ["manifest version", (payload) => (payload[4] = 2), "version is unsupported"],
      ["hash version", (payload) => (payload[5] = 2), "hash version is unsupported"],
      ["bucket bits", (payload) => (payload[6] = 18), "16 or 20"],
      ["reserved header", (payload) => (payload[7] = 1), "must be zero"],
      [
        "unsafe topology",
        (_, view) => view.setBigUint64(8, BigInt(Number.MAX_SAFE_INTEGER) + 1n, false),
        "safe-integer range",
      ],
      ["invalid UTF-8", (payload) => (payload[34] = 0xff), "not valid UTF-8"],
      [
        "non-canonical BOM",
        (payload) => payload.set(Uint8Array.of(0xef, 0xbb, 0xbf), 34),
        "canonical UTF-8",
      ],
      ["empty shard table", (_, view) => view.setUint32(59, 0, false), "shard count"],
      ["excess shard table", (_, view) => view.setUint32(59, 50_001, false), "shard count"],
      ["unsorted shards", (payload) => (payload[87] = 0x61), "uniquely sorted"],
      ["destination kind", (payload) => (payload[66] = 0), "destination kind"],
      [
        "schema range",
        (_, view) => {
          view.setUint32(77, 5, false)
          view.setUint32(81, 4, false)
        },
        "compatibility range is inverted",
      ],
      ["bucket count", (_, view) => view.setUint32(113, 1, false), "bucket table"],
      ["unknown shard", (_, view) => view.setUint32(117, 2, false), "unknown shard"],
      ["ownership state", (payload) => (payload[129] = 255), "ownership state"],
      [
        "unreferenced shard",
        (_, view) => {
          for (let bucketId = 1; bucketId < BUCKET_COUNT; bucketId += 2) {
            view.setUint32(117 + bucketId * 13, 0, false)
          }
        },
        "must be referenced",
      ],
      [
        "expired at creation",
        (_, view) => view.setBigUint64(24, 1_000n, false),
        "later than creation",
      ],
    ]
    for (const [name, mutate, message] of plainCases) {
      await expectMutatedPayloadFailure(plain, mutate, message).catch((error: unknown) => {
        throw new Error(`Malformed ${name} case did not fail as expected.`, { cause: error })
      })
    }

    const reservedCountOffset = 852_085
    const firstReservedOffset = 852_089
    const firstReservedShardOffset = 852_093
    const firstReservedEpochOffset = 852_097
    const firstReservedStateOffset = 852_105
    const secondReservedOffset = 852_106
    const overrideCountOffset = 852_123
    const firstOverrideOffset = 852_127
    const firstOverrideBucketOffset = 852_159
    const secondOverrideOffset = 852_163
    const secondOverrideBucketOffset = 852_195
    const overrideCases: readonly [
      string,
      (payload: Uint8Array, view: DataView) => void,
      string,
    ][] = [
      [
        "reserved count",
        (_, view) => view.setUint32(reservedCountOffset, 65_537, false),
        "reserved-bucket route table",
      ],
      [
        "reserved range",
        (_, view) => view.setUint32(firstReservedOffset, BUCKET_COUNT - 1, false),
        "outside the dense",
      ],
      [
        "reserved order",
        (_, view) => view.setUint32(secondReservedOffset, RESERVED_A, false),
        "uniquely sorted",
      ],
      [
        "reserved shard",
        (_, view) => view.setUint32(firstReservedShardOffset, 2, false),
        "unknown shard",
      ],
      [
        "reserved epoch",
        (_, view) =>
          view.setBigUint64(firstReservedEpochOffset, BigInt(Number.MAX_SAFE_INTEGER) + 1n, false),
        "safe-integer range",
      ],
      ["reserved state", (payload) => (payload[firstReservedStateOffset] = 255), "ownership state"],
      [
        "override count",
        (_, view) => view.setUint32(overrideCountOffset, 3, false),
        "override table",
      ],
      [
        "override target",
        (_, view) => view.setUint32(firstOverrideBucketOffset, RESERVED_A + 999, false),
        "non-reserved",
      ],
      [
        "override order",
        (payload) =>
          payload.copyWithin(secondOverrideOffset, firstOverrideOffset, firstOverrideOffset + 32),
        "uniquely sorted",
      ],
      [
        "override bucket reuse",
        (_, view) => view.setUint32(secondOverrideBucketOffset, RESERVED_A, false),
        "more than one",
      ],
    ]
    for (const [name, mutate, message] of overrideCases) {
      await expectMutatedPayloadFailure(overridden, mutate, message).catch((error: unknown) => {
        throw new Error(`Malformed ${name} case did not fail as expected.`, { cause: error })
      })
    }
  })

  it("keeps override lookup equivalent to a straightforward full-digest reference", async () => {
    const overridePairs = Array.from({ length: 64 }, (_, index) => {
      const digest = new Uint8Array(32)
      new DataView(digest.buffer).setUint32(28, index * 997, false)
      return { bucketId: RESERVED_A + 1_000 + index, digestHex: bytesToHex(digest) }
    })
    const manifest = await createRouteManifest(source(overridePairs))
    const reference = new Map(
      overridePairs.map((override) => [override.digestHex, override.bucketId]),
    )

    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 70_000 }), async (suffix) => {
        const digest = new Uint8Array(32)
        new DataView(digest.buffer).setUint32(28, suffix, false)
        const expected = reference.get(bytesToHex(digest))
        const resolved = manifest.resolve(digest)
        if (expected === undefined) {
          expect(resolved.overridden).toBe(false)
        } else {
          expect(resolved).toMatchObject({ bucketId: expected, overridden: true })
        }
      }),
      { numRuns: 500 },
    )
  })
})
