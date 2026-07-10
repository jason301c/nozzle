import { NozzleError } from "@nozzle/core"
import { describe, expect, it, vi } from "vitest"
import {
  createFanoutContinuation,
  type FanoutBudget,
  type FanoutContinuationState,
  type FanoutCurrentIdentity,
  type FanoutShardUsage,
  MAX_FANOUT_PAGE_ROWS,
} from "../src/fanout.js"
import {
  executeFanoutPage,
  type FanoutClock,
  type FanoutFetchRequest,
} from "../src/fanout-executor.js"

const checksums = Object.freeze({
  manifestChecksum: "11".repeat(32),
  queryChecksum: "22".repeat(32),
  schemaChecksum: "33".repeat(32),
})
const usage = Object.freeze({ costMicros: 1, cpuMs: 1, subrequests: 1 })
const defaultBudget = Object.freeze({
  maxBufferedBytes: 40,
  maxBufferedRows: 8,
  maxBytes: 100,
  maxConcurrency: 2,
  maxCostMicros: 100,
  maxCpuMs: 100,
  maxPages: 10,
  maxRows: 100,
  maxShards: 4,
  maxSubrequests: 100,
  timeoutMs: 100,
}) satisfies FanoutBudget

function state(
  input: {
    budget?: FanoutBudget
    partialPolicy?: "allow" | "fail"
    shardIds?: readonly string[]
  } = {},
): FanoutContinuationState {
  return createFanoutContinuation({
    budget: input.budget ?? defaultBudget,
    deadlineAtMs: 10_000,
    expiresAtMs: 9_000,
    ...checksums,
    nowMs: 1_000,
    order: [{ direction: "asc", immutable: true, kind: "number", nulls: "last" }],
    partialPolicy: input.partialPolicy ?? "fail",
    shardIds: input.shardIds ?? ["a", "b", "c", "d"],
  })
}

function current(shardIds: readonly string[]): FanoutCurrentIdentity {
  return { ...checksums, shardIds }
}

function manualClock(initialNowMs = 2_000): {
  readonly clock: FanoutClock
  readonly fire: () => void
  readonly pending: () => number
  readonly setNow: (value: number) => void
} {
  let nowMs = initialNowMs
  let nextHandle = 0
  const callbacks = new Map<number, () => void>()
  return {
    clock: {
      clearTimer: (handle) => {
        callbacks.delete(handle as number)
      },
      nowMs: () => nowMs,
      setTimer: (callback) => {
        nextHandle += 1
        callbacks.set(nextHandle, callback)
        return nextHandle
      },
    },
    fire: () => {
      const pending = [...callbacks.values()]
      callbacks.clear()
      for (const callback of pending) callback()
    },
    pending: () => callbacks.size,
    setNow: (value) => {
      nowMs = value
    },
  }
}

function baseInput<T>(
  fanoutState: FanoutContinuationState,
  fetchShard: (request: FanoutFetchRequest) => Promise<{
    readonly bookmark?: string
    readonly exhausted: boolean
    readonly rows: readonly {
      readonly byteSize: number
      readonly orderValues: readonly number[]
      readonly primaryKey: string
      readonly value: T
    }[]
    readonly usage: FanoutShardUsage
  }>,
  clock: FanoutClock,
) {
  return {
    clock,
    current: current(fanoutState.shardIds),
    estimateUsage: () => usage,
    fetchShard,
    pageSize: 4,
    state: fanoutState,
  }
}

function expectCode(promise: Promise<unknown>, code: NozzleError["code"]): Promise<void> {
  return expect(promise).rejects.toMatchObject({ code })
}

describe("bounded fan-out executor", () => {
  it("honors concurrency and splits buffer bounds across active shards", async () => {
    const fanoutState = state()
    const clock = manualClock()
    let active = 0
    let maximumActive = 0
    const requests: FanoutFetchRequest[] = []
    const result = await executeFanoutPage(
      baseInput(
        fanoutState,
        async (request) => {
          requests.push(request)
          active += 1
          maximumActive = Math.max(maximumActive, active)
          await Promise.resolve()
          active -= 1
          return {
            ...(request.shardId === "a" ? { bookmark: "bookmark-a" } : {}),
            exhausted: true,
            rows: [
              {
                byteSize: 1,
                orderValues: [request.shardId.charCodeAt(0)],
                primaryKey: request.shardId,
                value: request.shardId,
              },
            ],
            usage,
          }
        },
        clock.clock,
      ),
    )

    expect(maximumActive).toBe(2)
    expect(requests).toHaveLength(4)
    expect(requests.every((request) => request.maxBytes === 10 && request.rowLimit === 2)).toBe(
      true,
    )
    expect(result.rows.map((row) => row.value)).toEqual(["a", "b", "c", "d"])
    expect(result).toMatchObject({ complete: true, incomplete: false })
  })

  it("passes prior positions and bookmarks to keyset shard fetches", async () => {
    const initial = state({
      budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
      shardIds: ["a"],
    })
    const resumed = {
      ...initial,
      bookmarks: { a: "bookmark-a" },
      positions: { a: { orderValues: [1], primaryKey: "one" } },
    }
    const clock = manualClock()
    const fetchShard = vi.fn(async (request: FanoutFetchRequest) => {
      expect(request).toMatchObject({
        bookmark: "bookmark-a",
        position: { orderValues: [1], primaryKey: "one" },
      })
      return { exhausted: true, rows: [], usage }
    })
    const result = await executeFanoutPage(baseInput(resumed, fetchShard, clock.clock))
    expect(fetchShard).toHaveBeenCalledOnce()
    expect(result.complete).toBe(true)
  })

  it("times out a shard, returns no partial cursor, and ignores its late response", async () => {
    const fanoutState = state({
      budget: { ...defaultBudget, maxConcurrency: 2, maxShards: 2 },
      partialPolicy: "allow",
      shardIds: ["a", "b"],
    })
    const clock = manualClock()
    let resolveLate: ((value: never) => void) | undefined
    const execution = executeFanoutPage(
      baseInput(
        fanoutState,
        (request) => {
          if (request.shardId === "a") {
            return Promise.resolve({
              exhausted: true,
              rows: [{ byteSize: 1, orderValues: [1], primaryKey: "a", value: "a" }],
              usage,
            })
          }
          return new Promise((resolve) => {
            resolveLate = resolve as (value: never) => void
          })
        },
        clock.clock,
      ),
    )
    for (let attempt = 0; attempt < 10 && clock.pending() !== 1; attempt += 1) {
      await Promise.resolve()
    }
    expect(clock.pending()).toBe(1)
    clock.fire()
    const result = await execution
    expect(result).toMatchObject({
      complete: false,
      failures: [{ errorCode: "timeout", shardId: "b" }],
      incomplete: true,
    })
    expect(result.continuation).toBeUndefined()
    resolveLate?.({
      exhausted: true,
      rows: [{ byteSize: 1, orderValues: [2], primaryKey: "b", value: "late" }],
      usage,
    } as never)
    await Promise.resolve()
    expect(result.rows.map((row) => row.value)).toEqual(["a"])
  })

  it("cancels in-flight and not-yet-launched work after a required failure", async () => {
    const fanoutState = state({
      budget: { ...defaultBudget, maxConcurrency: 2, maxShards: 3 },
      shardIds: ["a", "b", "c"],
    })
    const clock = manualClock()
    const called: string[] = []
    let bSignal: AbortSignal | undefined
    const execution = executeFanoutPage({
      ...baseInput(
        fanoutState,
        (request) => {
          called.push(request.shardId)
          if (request.shardId === "a") return Promise.reject(new Error("permanent"))
          bSignal = request.signal
          return new Promise(() => undefined)
        },
        clock.clock,
      ),
      classifyFailure: () => "permanent_failure",
    })

    await expectCode(execution, "ShardUnavailableError")
    expect(called).toEqual(["a", "b"])
    expect(bSignal?.aborted).toBe(true)
  })

  it("classifies Nozzle errors and contains broken custom classifiers", async () => {
    const clock = manualClock()
    const nozzleState = state({
      budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
      partialPolicy: "allow",
      shardIds: ["a"],
    })
    const nozzleResult = await executeFanoutPage(
      baseInput(
        nozzleState,
        async () => {
          throw new NozzleError("ProviderRateLimitedError", "limited")
        },
        clock.clock,
      ),
    )
    expect(nozzleResult.failures[0]?.errorCode).toBe("ProviderRateLimitedError")

    for (const classifyFailure of [
      () => {
        throw new Error("classifier failed")
      },
      () => "\n",
    ]) {
      const genericResult = await executeFanoutPage({
        ...baseInput(
          nozzleState,
          async () => {
            throw new Error("secret")
          },
          clock.clock,
        ),
        classifyFailure,
      })
      expect(genericResult.failures[0]?.errorCode).toBe("shard_error")
    }
  })

  it("completes without fetching when every shard is already exhausted", async () => {
    const initial = state({
      budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
      shardIds: ["a"],
    })
    const exhausted = { ...initial, exhaustedShardIds: ["a"] }
    const clock = manualClock()
    const fetchShard = vi.fn(async () => ({ exhausted: true, rows: [], usage }))
    const result = await executeFanoutPage(baseInput(exhausted, fetchShard, clock.clock))
    expect(fetchShard).not.toHaveBeenCalled()
    expect(result.complete).toBe(true)
  })

  it("rejects impossible work before launching any shard", async () => {
    const clock = manualClock()
    const fetchShard = vi.fn(async () => ({ exhausted: true, rows: [], usage }))
    const twoShardBudget = { ...defaultBudget, maxConcurrency: 2, maxShards: 2 }
    const initial = state({ budget: twoShardBudget, shardIds: ["a", "b"] })
    const cases: readonly {
      readonly estimate?: FanoutShardUsage
      readonly pageSize?: number
      readonly state: FanoutContinuationState
    }[] = [
      { pageSize: 0, state: initial },
      { pageSize: MAX_FANOUT_PAGE_ROWS + 1, state: initial },
      { state: { ...initial, consumedPages: initial.budget.maxPages } },
      { state: { ...initial, consumedRows: initial.budget.maxRows } },
      { state: { ...initial, consumedBytes: initial.budget.maxBytes } },
      {
        state: state({
          budget: { ...twoShardBudget, maxBufferedRows: 1 },
          shardIds: ["a", "b"],
        }),
      },
      {
        state: state({
          budget: { ...twoShardBudget, maxBufferedBytes: 1 },
          shardIds: ["a", "b"],
        }),
      },
      { estimate: { ...usage, costMicros: 51 }, state: { ...initial, consumedCostMicros: 0 } },
      { estimate: { ...usage, cpuMs: 51 }, state: { ...initial, consumedCpuMs: 0 } },
      { estimate: { ...usage, subrequests: 51 }, state: { ...initial, consumedSubrequests: 0 } },
    ]
    for (const testCase of cases) {
      await expectCode(
        executeFanoutPage({
          ...baseInput(testCase.state, fetchShard, clock.clock),
          estimateUsage: () => testCase.estimate ?? usage,
          pageSize: testCase.pageSize ?? 4,
        }),
        testCase.pageSize !== undefined ? "ConfigurationError" : "CapacityGuardError",
      )
    }
    expect(fetchShard).not.toHaveBeenCalled()
  })

  it("rejects malformed estimates, clocks, actual usage, and clock rollback", async () => {
    const initial = state({
      budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
      shardIds: ["a"],
    })
    const clock = manualClock()
    const fetchShard = async () => ({ exhausted: true, rows: [], usage })
    await expectCode(
      executeFanoutPage({
        ...baseInput(initial, fetchShard, clock.clock),
        estimateUsage: () => null as never,
      }),
      "ConfigurationError",
    )
    await expectCode(
      executeFanoutPage({ ...baseInput(initial, fetchShard, null as never) }),
      "ConfigurationError",
    )
    const invalidTime = manualClock()
    invalidTime.setNow(-1)
    await expectCode(
      executeFanoutPage(baseInput(initial, fetchShard, invalidTime.clock)),
      "ConfigurationError",
    )
    const expired = manualClock(9_000)
    await expectCode(
      executeFanoutPage(baseInput(initial, fetchShard, expired.clock)),
      "SessionTokenInvalidError",
    )
    const overdue = manualClock(10_000)
    await expectCode(
      executeFanoutPage(baseInput(initial, fetchShard, overdue.clock)),
      "CapacityGuardError",
    )
    await expectCode(
      executeFanoutPage(
        baseInput(
          initial,
          async () => ({ exhausted: true, rows: [], usage: { ...usage, cpuMs: -1 } }),
          clock.clock,
        ),
      ),
      "ConfigurationError",
    )

    const rollback = manualClock()
    const rollbackFetch = async () => {
      rollback.setNow(1_999)
      return { exhausted: true, rows: [], usage }
    }
    await expectCode(
      executeFanoutPage(baseInput(initial, rollbackFetch, rollback.clock)),
      "ConfigurationError",
    )
  })

  it.each([
    [{ maxCostMicros: 2 }, { costMicros: 3, cpuMs: 1, subrequests: 1 }],
    [{ maxCpuMs: 2 }, { costMicros: 1, cpuMs: 3, subrequests: 1 }],
    [{ maxSubrequests: 2 }, { costMicros: 1, cpuMs: 1, subrequests: 3 }],
  ] as const)("reports actual usage drift that crosses a preflight budget", async (limit, actual) => {
    const limits = { ...defaultBudget, ...limit, maxConcurrency: 1, maxShards: 1 }
    const initial = state({ budget: limits, shardIds: ["a"] })
    const clock = manualClock()
    await expectCode(
      executeFanoutPage({
        ...baseInput(
          initial,
          async () => ({ exhausted: true, rows: [], usage: actual }),
          clock.clock,
        ),
        estimateUsage: () => usage,
      }),
      "CapacityGuardError",
    )
  })

  it("uses the Workers-compatible default clock and timers", async () => {
    const nowMs = Date.now()
    const dynamic = createFanoutContinuation({
      budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
      deadlineAtMs: nowMs + 10_000,
      expiresAtMs: nowMs + 9_000,
      ...checksums,
      nowMs,
      order: [{ direction: "asc", immutable: true, kind: "number", nulls: "last" }],
      partialPolicy: "fail",
      shardIds: ["a"],
    })
    const result = await executeFanoutPage({
      current: current(dynamic.shardIds),
      estimateUsage: () => usage,
      fetchShard: async () => ({ exhausted: true, rows: [], usage }),
      pageSize: 1,
      state: dynamic,
    })
    expect(result.complete).toBe(true)
  })
})
