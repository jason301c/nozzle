import type { NozzleError } from "@nozzle/core"
import fc from "fast-check"
import { describe, expect, it } from "vitest"
import {
  countFanout,
  type FanoutReductionShard,
  maxFanout,
  minFanout,
  reduceFanout,
  sumFanoutBigInts,
  sumFanoutNumbers,
} from "../src/fanout-reducer.js"

function success<T>(shardId: string, value: T): FanoutReductionShard<T> {
  return { kind: "success", shardId, value }
}

function failure<T>(shardId: string, errorCode = "timeout"): FanoutReductionShard<T> {
  return { errorCode, kind: "failure", shardId }
}

function expectCode(callback: () => unknown, code: NozzleError["code"]): void {
  expect(callback).toThrowError(expect.objectContaining({ code }))
}

describe("deterministic fan-out reducers", () => {
  it("invokes custom reducers in canonical shard order and finalizes once", () => {
    const calls: string[] = []
    const result = reduceFanout({
      finalize: (values) => values.join(","),
      initial: [] as string[],
      partialPolicy: "fail",
      reduce: (values, value: string, shardId) => {
        calls.push(shardId)
        return [...values, value]
      },
      shardIds: ["a", "b", "c"],
      shards: [success("c", "C"), success("a", "A"), success("b", "B")],
    })
    expect(calls).toEqual(["a", "b", "c"])
    expect(result).toEqual({
      complete: true,
      consistency: "best_effort_no_global_snapshot",
      failures: [],
      incomplete: false,
      value: "A,B,C",
    })
    expect(Object.isFrozen(result)).toBe(true)
  })

  it("returns explicit partial reductions or fails closed", () => {
    const shards = [success("a", 2), failure<number>("b"), success("c", 3)]
    const partial = reduceFanout({
      initial: 0,
      partialPolicy: "allow",
      reduce: (total, value: number) => total + value,
      shardIds: ["a", "b", "c"],
      shards,
    })
    expect(partial).toEqual({
      complete: false,
      consistency: "best_effort_no_global_snapshot",
      failures: [{ errorCode: "timeout", shardId: "b" }],
      incomplete: true,
      value: 5,
    })
    expectCode(
      () =>
        reduceFanout({
          initial: 0,
          partialPolicy: "fail",
          reduce: (total, value: number) => total + value,
          shardIds: ["a", "b", "c"],
          shards,
        }),
      "ShardUnavailableError",
    )
  })

  it("counts exactly across bigint, safe-number, and decimal-string partials", () => {
    expect(
      countFanout({
        partialPolicy: "fail",
        shardIds: ["a", "b", "c"],
        shards: [success("a", 2n), success("b", 3), success("c", "9007199254740993")],
      }).value,
    ).toBe(9_007_199_254_740_998n)
    for (const invalid of [
      -1n,
      -1,
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
      "-1",
      "01",
      "1.0",
      "1".repeat(129),
    ]) {
      expectCode(
        () =>
          countFanout({
            partialPolicy: "fail",
            shardIds: ["a"],
            shards: [success("a", invalid)],
          }),
        "ConfigurationError",
      )
    }
  })

  it("sums integers exactly and rejects lossy or noncanonical inputs", () => {
    expect(
      sumFanoutBigInts({
        partialPolicy: "fail",
        shardIds: ["a", "b", "c"],
        shards: [success("a", -2n), success("b", 3), success("c", "9007199254740993")],
      }).value,
    ).toBe(9_007_199_254_740_994n)
    for (const invalid of [
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
      "-0",
      "+1",
      "01",
      "1.0",
      "1".repeat(129),
    ]) {
      expectCode(
        () =>
          sumFanoutBigInts({
            partialPolicy: "fail",
            shardIds: ["a"],
            shards: [success("a", invalid)],
          }),
        "ConfigurationError",
      )
    }
  })

  it("uses a deterministic compensated floating-point sum", () => {
    const result = sumFanoutNumbers({
      partialPolicy: "fail",
      shardIds: ["a", "b", "c", "d"],
      shards: [success("d", -1e16), success("b", 1), success("a", 1e16), success("c", 2)],
    })
    expect(result.value).toBe(3)
    for (const invalid of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
      expectCode(
        () =>
          sumFanoutNumbers({
            partialPolicy: "fail",
            shardIds: ["a"],
            shards: [success("a", invalid)],
          }),
        "ConfigurationError",
      )
    }
    expectCode(
      () =>
        sumFanoutNumbers({
          partialPolicy: "fail",
          shardIds: ["a", "b"],
          shards: [success("a", Number.MAX_VALUE), success("b", Number.MAX_VALUE)],
        }),
      "ConfigurationError",
    )
  })

  it("computes numeric and UTF-8 extrema while following SQL null aggregate semantics", () => {
    const numeric = {
      kind: "number" as const,
      partialPolicy: "fail" as const,
      shardIds: ["a", "b", "c"],
      shards: [success("a", null), success("b", -2), success("c", 3)],
    }
    expect(minFanout(numeric).value).toBe(-2)
    expect(maxFanout(numeric).value).toBe(3)
    const decreasing = {
      kind: "number" as const,
      partialPolicy: "fail" as const,
      shardIds: ["a", "b"],
      shards: [success("a", 3), success("b", -2)],
    }
    expect(minFanout(decreasing).value).toBe(-2)
    expect(maxFanout(decreasing).value).toBe(3)
    const strings = {
      kind: "string" as const,
      partialPolicy: "fail" as const,
      shardIds: ["a", "b", "c"],
      shards: [success("a", "z"), success("b", "😀"), success("c", null)],
    }
    expect(minFanout(strings).value).toBe("z")
    expect(maxFanout(strings).value).toBe("😀")
    expect(
      minFanout({
        kind: "string",
        partialPolicy: "fail",
        shardIds: ["a"],
        shards: [success("a", null)],
      }).value,
    ).toBeNull()
    for (const invalid of [
      { kind: "number" as const, value: "1" },
      { kind: "number" as const, value: Number.NaN },
      { kind: "string" as const, value: 1 },
      { kind: "string" as const, value: "x".repeat(4_097) },
      { kind: "string" as const, value: "\ud800" },
    ]) {
      expectCode(
        () =>
          minFanout({
            kind: invalid.kind,
            partialPolicy: "fail",
            shardIds: ["a"],
            shards: [success("a", invalid.value)],
          }),
        "ConfigurationError",
      )
    }
  })

  it("rejects malformed reducer configuration and contradictory shard results", () => {
    const base = {
      initial: 0,
      partialPolicy: "fail" as const,
      reduce: (total: number, value: number) => total + value,
      shardIds: ["a", "b"],
      shards: [success("a", 1), success("b", 2)],
    }
    const invalid = [
      { ...base, reduce: null as never },
      { ...base, finalize: true as never },
      { ...base, partialPolicy: "other" as never },
      { ...base, shardIds: [] },
      { ...base, shardIds: ["b", "a"] },
      { ...base, shardIds: ["a", "a"] },
      { ...base, shardIds: ["a", "\ud800"] },
      { ...base, shardIds: ["a", "\udc00"] },
      { ...base, shardIds: ["a", "bad\nvalue"] },
      { ...base, shards: [success("a", 1)] },
      { ...base, shards: [success("a", 1), success("a", 2)] },
      { ...base, shards: [success("a", 1), success("c", 2)] },
      { ...base, shards: [null, success("b", 2)] as never },
      { ...base, shards: [{ kind: "other", shardId: "a" }, success("b", 2)] as never },
      { ...base, shards: [{ ...success("a", 1), extra: true }, success("b", 2)] },
      { ...base, shards: [failure<number>("a", ""), success("b", 2)] },
    ]
    for (const input of invalid) {
      expectCode(() => reduceFanout(input), "ConfigurationError")
    }
  })

  it("is invariant to result arrival order for associative custom reducers", () => {
    fc.assert(
      fc.property(fc.array(fc.integer(), { maxLength: 20, minLength: 1 }), (values) => {
        const shardIds = values.map((_, index) => index.toString().padStart(3, "0"))
        const shards = values.map((value, index) => success(shardIds[index] as string, value))
        const expected = reduceFanout({
          initial: 0,
          partialPolicy: "fail",
          reduce: (total, value: number) => total + value,
          shardIds,
          shards,
        }).value
        const reversed = reduceFanout({
          initial: 0,
          partialPolicy: "fail",
          reduce: (total, value: number) => total + value,
          shardIds,
          shards: [...shards].reverse(),
        }).value
        expect(reversed).toBe(expected)
      }),
      { numRuns: 200 },
    )
  })
})
