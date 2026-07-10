import { describe, expect, it } from "vitest"
import {
  decodeWireD1Result,
  encodeWireD1Result,
  MAX_ROUTER_RESULT_BYTES,
  MAX_ROUTER_RESULT_ROWS,
} from "../src/wire.js"

function result(overrides: Record<string, unknown> = {}) {
  return {
    meta: { changed_db: true, changes: 1, duration: 0.5, last_row_id: 1, served_by: null },
    results: [],
    success: true,
    ...overrides,
  } as never
}

describe("router D1 result wire codec", () => {
  it("round-trips every supported raw D1 value without type coercion", () => {
    const buffer = Uint8Array.from([0, 1, 255]).buffer
    const viewBuffer = Uint8Array.from([7, 8, 9]).buffer
    const encoded = encodeWireD1Result(
      result({
        results: [
          {
            arrayBuffer: buffer,
            boolean: false,
            bytes: Uint8Array.from([2, 3]),
            dataView: new DataView(viewBuffer, 1, 2),
            d1Array: [4, 5, 6],
            null: null,
            number: 1.5,
            string: "Nozzle",
          },
        ],
      }),
    )
    expect(encoded).toEqual({
      meta: { changed_db: true, changes: 1, duration: 0.5, last_row_id: 1, served_by: null },
      results: [
        {
          arrayBuffer: { hex: "0001ff", type: "blob" },
          boolean: false,
          bytes: { hex: "0203", type: "blob" },
          dataView: { hex: "0809", type: "blob" },
          d1Array: { hex: "040506", type: "blob" },
          null: null,
          number: 1.5,
          string: "Nozzle",
        },
      ],
      success: true,
    })
    const decoded = decodeWireD1Result(structuredClone(encoded))
    expect(decoded.meta).toEqual(encoded.meta)
    expect(decoded.results[0]).toMatchObject({
      boolean: false,
      null: null,
      number: 1.5,
      string: "Nozzle",
    })
    expect([
      ...((decoded.results[0] as Record<string, unknown>).arrayBuffer as Uint8Array),
    ]).toEqual([0, 1, 255])
    expect([...((decoded.results[0] as Record<string, unknown>).dataView as Uint8Array)]).toEqual([
      8, 9,
    ])
    expect(Object.isFrozen(encoded)).toBe(true)
    expect(Object.isFrozen(decoded)).toBe(true)
  })

  it("rejects malformed D1 result envelopes and unsupported result values", () => {
    for (const malformed of [
      result({ success: false }),
      result({ results: null }),
      result({ results: [null] }),
      result({ results: [{ value: undefined }] }),
      result({ results: [{ value: Number.NaN }] }),
      result({ results: [{ value: [0, 256] }] }),
      result({ results: [{ value: [0, 1.5] }] }),
      result({ meta: { binary: Uint8Array.of(1) } }),
    ]) {
      expect(() => encodeWireD1Result(malformed)).toThrow()
    }
    expect(() =>
      encodeWireD1Result(result({ results: new Array(MAX_ROUTER_RESULT_ROWS + 1).fill({}) })),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    expect(() =>
      encodeWireD1Result(
        result({
          results: [Object.fromEntries(new Array(257).fill(0).map((_, i) => [`c${i}`, i]))],
        }),
      ),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    expect(() =>
      encodeWireD1Result(result({ results: [{ value: "x".repeat(2 * 1024 * 1024 + 1) }] })),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    expect(() =>
      encodeWireD1Result(result({ results: [{ value: new Uint8Array(2 * 1024 * 1024 + 1) }] })),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
  })

  it("enforces aggregate result byte limits", () => {
    const chunk = "x".repeat(2 * 1024 * 1024)
    expect(() =>
      encodeWireD1Result(
        result({
          results: [
            Object.fromEntries(
              Array.from(
                { length: Math.ceil(MAX_ROUTER_RESULT_BYTES / chunk.length) + 1 },
                (_, i) => [`c${i}`, chunk],
              ),
            ),
          ],
        }),
      ),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
  })

  it("rejects malformed decoded envelopes, rows, metadata, and BLOB tags", () => {
    const valid = encodeWireD1Result(result({ results: [{ value: Uint8Array.of(1) }] }))
    for (const malformed of [
      null,
      { ...valid, extra: true },
      { ...valid, success: false },
      { ...valid, results: null },
      { ...valid, results: [null] },
      { ...valid, results: [{ value: { hex: "0", type: "blob" } }] },
      { ...valid, results: [{ value: { hex: "FF", type: "blob" } }] },
      { ...valid, results: [{ value: { hex: "00", type: "other" } }] },
      { ...valid, results: [{ value: { extra: true, hex: "00", type: "blob" } }] },
      { ...valid, results: [{ value: undefined }] },
      { ...valid, meta: { binary: { hex: "00", type: "blob" } } },
    ]) {
      expect(() => decodeWireD1Result(malformed)).toThrow()
    }
    expect(() =>
      decodeWireD1Result({
        ...valid,
        results: new Array(MAX_ROUTER_RESULT_ROWS + 1).fill({}),
      }),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    expect(() =>
      decodeWireD1Result({
        ...valid,
        results: [Object.fromEntries(new Array(257).fill(0).map((_, i) => [`c${i}`, i]))],
      }),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
  })

  it("rejects non-plain, symbolic, and hidden wire records", () => {
    expect(() => decodeWireD1Result(new Date())).toThrow("plain object")
    const symbol = { meta: {}, results: [], success: true } as Record<PropertyKey, unknown>
    symbol[Symbol("hidden")] = true
    expect(() => decodeWireD1Result(symbol)).toThrow("symbol")
    const hidden = { meta: {}, results: [], success: true }
    Object.defineProperty(hidden, "secret", { enumerable: false, value: true })
    expect(() => decodeWireD1Result(hidden)).toThrow("enumerable data properties")
  })
})
