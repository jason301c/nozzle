import fc from "fast-check"
import { describe, expect, it } from "vitest"
import {
  bytesToHex,
  decodeFleetSeed,
  encodeCanonicalPartitionKey,
  encodeFleetSeed,
  encodeHashPreimage,
  generateFleetSeed,
  hashPartitionKey,
  type PartitionKey,
  selectBucket,
} from "../src/hash.js"

const seed = Uint8Array.from({ length: 32 }, (_, index) => index)

const vectors: readonly {
  readonly bucket16: number
  readonly bucket20: number
  readonly digest: string
  readonly key: PartitionKey
  readonly preimage: string
}[] = [
  {
    key: { type: "string", value: "Nozzle" },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f03000000010104000000064e6f7a7a6c65",
    digest: "9fd23fcbdea06e55380f6088c0d64bb3817419c58e68f079d574c0ea5a081cfc",
    bucket16: 40914,
    bucket20: 654627,
  },
  {
    key: { type: "string", value: "" },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0300000001010400000000",
    digest: "7e31065b8acb895534e163b1e3954bde943f5ac66453542dd94e2b7026640bf3",
    bucket16: 32305,
    bucket20: 516880,
  },
  {
    key: { type: "integer", value: 42 },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0300000001020400000008000000000000002a",
    digest: "c1a80fcae1a96f6f3b886d4cf206996dd4fadbfd9a74e054448dcef7625e539d",
    bucket16: 49576,
    bucket20: 793216,
  },
  {
    key: { type: "integer", value: -42 },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0300000001020400000008ffffffffffffffd6",
    digest: "4afea64fdf5125ab0b42c5e97ff953fb810b75fcb6e4436966647b1c86e231e7",
    bucket16: 19198,
    bucket20: 307178,
  },
  {
    key: { type: "uuid", value: "123e4567-e89b-12d3-a456-426614174000" },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0300000001030400000010123e4567e89b12d3a456426614174000",
    digest: "87420cf8cb82a7f16472c0d4aba55efbde8cea0b3f97ebfc8563612ad1840af9",
    bucket16: 34626,
    bucket20: 554016,
  },
  {
    key: { type: "binary", value: Uint8Array.of(0, 1, 254, 255) },
    preimage:
      "4e5a480101000000136e6f7a7a6c652e706172746974696f6e2e76310200000020000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f03000000010404000000040001feff",
    digest: "78190ce4ec9f17feccf8a36cc20aa3b5ccdc5284dd93b797d78ae8913dd917d8",
    bucket16: 30745,
    bucket20: 491920,
  },
]

describe("hash version 1", () => {
  it.each(vectors)("matches the published $key.type vector", async (vector) => {
    const hash16 = await hashPartitionKey(vector.key, seed, 16)
    const hash20 = await hashPartitionKey(vector.key, seed, 20)
    expect(bytesToHex(hash16.preimage)).toBe(vector.preimage)
    expect(hash16.digestHex).toBe(vector.digest)
    expect(hash16.bucketId).toBe(vector.bucket16)
    expect(hash20.bucketId).toBe(vector.bucket20)
    expect(hash16.version).toBe(1)
  })

  it("treats UUID hexadecimal case canonically", async () => {
    const lower = await hashPartitionKey(
      { type: "uuid", value: "123e4567-e89b-12d3-a456-426614174000" },
      seed,
    )
    const upper = await hashPartitionKey(
      { type: "uuid", value: "123E4567-E89B-12D3-A456-426614174000" },
      seed,
    )
    expect(upper.digestHex).toBe(lower.digestHex)
  })

  it("accepts well-formed UTF-16 surrogate pairs", () => {
    expect(bytesToHex(encodeCanonicalPartitionKey({ type: "string", value: "😀" }).bytes)).toBe(
      "f09f9880",
    )
  })

  it("copies binary key bytes before returning the canonical form", () => {
    const input = Uint8Array.of(1, 2, 3)
    const canonical = encodeCanonicalPartitionKey({ type: "binary", value: input })
    input[0] = 9
    expect([...canonical.bytes]).toEqual([1, 2, 3])
    expect(canonical.typeTag).toBe(4)
  })

  it.each([
    { type: "integer", value: Number.NaN },
    { type: "integer", value: Number.POSITIVE_INFINITY },
    { type: "integer", value: 1.5 },
    { type: "integer", value: Number.MAX_SAFE_INTEGER + 1 },
    { type: "integer", value: -0 },
    { type: "uuid", value: "not-a-uuid" },
    { type: "uuid", value: "123e4567e89b12d3a456426614174000" },
    { type: "string", value: "\ud800" },
    { type: "string", value: "\udc00" },
  ] as const)("rejects ambiguous $type input", (key) => {
    expect(() => encodeCanonicalPartitionKey(key)).toThrowError(
      expect.objectContaining({ code: "PartitionKeyMismatchError" }),
    )
  })

  it("rejects runtime values that contradict their declared type", () => {
    expect(() =>
      encodeCanonicalPartitionKey({ type: "binary", value: [] } as unknown as PartitionKey),
    ).toThrowError(expect.objectContaining({ code: "PartitionKeyMismatchError" }))
    expect(() =>
      encodeCanonicalPartitionKey({ type: "string", value: 1 } as unknown as PartitionKey),
    ).toThrowError(expect.objectContaining({ code: "PartitionKeyMismatchError" }))
    expect(() =>
      encodeCanonicalPartitionKey({ type: "uuid", value: 1 } as unknown as PartitionKey),
    ).toThrowError(expect.objectContaining({ code: "PartitionKeyMismatchError" }))
  })

  it("requires exact seed, digest, and bucket-space sizes", () => {
    expect(() => encodeHashPreimage({ type: "string", value: "x" }, new Uint8Array(31))).toThrow(
      "exactly 32 bytes",
    )
    expect(() => selectBucket(new Uint8Array(31), 16)).toThrow("32 bytes")
    expect(() => selectBucket("not-bytes" as unknown as Uint8Array, 16)).toThrow("32 bytes")
    expect(() =>
      encodeHashPreimage({ type: "string", value: "x" }, "not-bytes" as unknown as Uint8Array),
    ).toThrow("exactly 32 bytes")
    expect(() => selectBucket(new Uint8Array(32), 18 as 16)).toThrow("16 or 20")
  })

  it("round-trips fleet seeds using unpadded base64url", () => {
    const encoded = encodeFleetSeed(seed)
    expect(encoded).toHaveLength(43)
    expect([...decodeFleetSeed(encoded)]).toEqual([...seed])
    expect(generateFleetSeed()).toHaveLength(32)
    expect(() => encodeFleetSeed(new Uint8Array(2))).toThrow("exactly 32 bytes")
    expect(() => encodeFleetSeed("not-bytes" as unknown as Uint8Array)).toThrow("exactly 32 bytes")
    expect(() => decodeFleetSeed("not base64url")).toThrow("unpadded base64url")
    expect(() => decodeFleetSeed(1 as unknown as string)).toThrow("unpadded base64url")
    expect(() => decodeFleetSeed(`${encoded.slice(0, 42)}B`)).toThrow("exactly 32 bytes")
  })

  it("is deterministic and bounded for generated strings and safe integers", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.string().map((value): PartitionKey => ({ type: "string", value })),
          fc
            .integer({ min: Number.MIN_SAFE_INTEGER, max: Number.MAX_SAFE_INTEGER })
            .filter((value) => !Object.is(value, -0))
            .map((value): PartitionKey => ({ type: "integer", value })),
          fc.uint8Array().map((value): PartitionKey => ({ type: "binary", value })),
        ),
        async (key) => {
          const first = await hashPartitionKey(key, seed, 16)
          const second = await hashPartitionKey(key, seed, 16)
          expect(second.digestHex).toBe(first.digestHex)
          expect(second.bucketId).toBe(first.bucketId)
          expect(first.bucketId).toBeGreaterThanOrEqual(0)
          expect(first.bucketId).toBeLessThan(2 ** 16)
        },
      ),
      { numRuns: 200 },
    )
  })
})
