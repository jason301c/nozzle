import type { NozzleError } from "@nozzle/core"
import fc from "fast-check"
import { describe, expect, it } from "vitest"
import {
  createFanoutContinuation,
  type FanoutBudget,
  type FanoutContinuationState,
  type FanoutOrderColumn,
  type FanoutPosition,
  type FanoutRow,
  type FanoutShardPage,
  loadFanoutContinuation,
  mergeFanoutPage as mergeFanoutPageRaw,
  referenceFanoutOrder,
} from "../src/fanout.js"

const order = [
  { direction: "asc", immutable: true, kind: "number", nulls: "last" },
  { direction: "desc", immutable: true, kind: "string", nulls: "first" },
] as const satisfies readonly FanoutOrderColumn[]

const checksums = Object.freeze({
  manifestChecksum: "11".repeat(32),
  queryChecksum: "22".repeat(32),
  schemaChecksum: "33".repeat(32),
})

const defaultBudget = Object.freeze({
  maxBufferedBytes: 10_000,
  maxBufferedRows: 1_000,
  maxBytes: 10_000,
  maxConcurrency: 4,
  maxCostMicros: 10_000,
  maxCpuMs: 10_000,
  maxPages: 100,
  maxRows: 1_000,
  maxShards: 10,
  maxSubrequests: 1_000,
  timeoutMs: 1_000,
}) satisfies FanoutBudget

const usage = Object.freeze({ costMicros: 1, cpuMs: 1, subrequests: 1 })

function createState(
  overrides: Partial<{
    budget: FanoutBudget
    deadlineAtMs: number
    expiresAtMs: number
    order: readonly FanoutOrderColumn[]
    partialPolicy: "allow" | "fail"
    shardIds: readonly string[]
  }> = {},
): FanoutContinuationState {
  return createFanoutContinuation({
    budget: overrides.budget ?? defaultBudget,
    deadlineAtMs: overrides.deadlineAtMs ?? 10_000,
    expiresAtMs: overrides.expiresAtMs ?? 9_000,
    manifestChecksum: checksums.manifestChecksum,
    nowMs: 1_000,
    order: overrides.order ?? order,
    partialPolicy: overrides.partialPolicy ?? "fail",
    queryChecksum: checksums.queryChecksum,
    schemaChecksum: checksums.schemaChecksum,
    shardIds: overrides.shardIds ?? ["a", "b"],
  })
}

function row<T>(
  primaryKey: string,
  orderValues: FanoutRow<T>["orderValues"],
  value: T,
  byteSize = 1,
): FanoutRow<T> {
  return { byteSize, orderValues, primaryKey, value }
}

function success<T>(
  shardId: string,
  rows: readonly FanoutRow<T>[],
  exhausted = true,
  bookmark?: string,
): Extract<FanoutShardPage<T>, { kind: "success" }> {
  return {
    ...(bookmark === undefined ? {} : { bookmark }),
    exhausted,
    kind: "success",
    rows,
    shardId,
    usage,
  }
}

function failure<T>(shardId: string, errorCode = "timeout"): FanoutShardPage<T> {
  return { errorCode, kind: "failure", shardId, usage }
}

function expectCode(callback: () => unknown, code: NozzleError["code"]): void {
  expect(callback).toThrowError(expect.objectContaining({ code }))
}

function mergeFanoutPage<T>(input: {
  readonly nowMs: number
  readonly pageSize: number
  readonly pages: readonly FanoutShardPage<T>[]
  readonly state: FanoutContinuationState
}) {
  return mergeFanoutPageRaw({
    ...input,
    current: { ...checksums, shardIds: input.state.shardIds },
  })
}

describe("bounded fan-out merge", () => {
  it("creates and reconstructs an immutable, sealed continuation", () => {
    const state = createState()
    expect(loadFanoutContinuation(structuredClone(state))).toEqual(state)
    expect(state).toMatchObject({
      consumedBytes: 0,
      consumedCostMicros: 0,
      consumedCpuMs: 0,
      consumedPages: 0,
      consumedRows: 0,
      consumedSubrequests: 0,
      deadlineAtMs: 10_000,
      expiresAtMs: 9_000,
      shardIds: ["a", "b"],
      version: 1,
    })
    expect(Object.isFrozen(state)).toBe(true)
    expect(Object.isFrozen(state.budget)).toBe(true)
    expect(Object.isFrozen(state.order[0])).toBe(true)
    const nullPrototypeBudget = Object.assign(Object.create(null), defaultBudget) as FanoutBudget
    expect(createState({ budget: nullPrototypeBudget }).budget).toEqual(defaultBudget)
  })

  it("merges uneven and empty shards in a stable total order", () => {
    const pages = [
      success("a", [
        row("2", [1, "b"], "a2"),
        row("1", [1, "a"], "a1"),
        row("3", [null, null], "a3"),
      ]),
      success("b", [row("1", [1, "b"], "b1"), row("2", [2, "z"], "b2")]),
      success("c", []),
    ]
    const state = createState({ shardIds: ["a", "b", "c"] })
    const result = mergeFanoutPage({ nowMs: 2_000, pageSize: 10, pages, state })
    expect(result.rows.map((item) => item.value)).toEqual(["a2", "b1", "a1", "b2", "a3"])
    expect(result).toMatchObject({
      complete: true,
      consistency: "best_effort_no_global_snapshot",
      failures: [],
      incomplete: false,
      usage: { bytes: 5, costMicros: 3, cpuMs: 3, pages: 1, rows: 5, subrequests: 3 },
    })
    expect(result.continuation).toBeUndefined()
    expect(Object.isFrozen(result.rows)).toBe(true)
    expect(referenceFanoutOrder({ order, pages })).toEqual(result.rows)
  })

  it("keeps explicit null placement independent of direction and compares UTF-8 bytes", () => {
    const descending = [
      { direction: "desc", immutable: true, kind: "string", nulls: "last" },
    ] as const satisfies readonly FanoutOrderColumn[]
    const pages = [
      success("a", [row("1", ["😀"], "emoji"), row("2", [null], "null")]),
      success("b", [row("1", ["z"], "ascii")]),
    ]
    expect(referenceFanoutOrder({ order: descending, pages }).map((item) => item.value)).toEqual([
      "emoji",
      "ascii",
      "null",
    ])
    expect(
      mergeFanoutPage({
        nowMs: 2_000,
        pageSize: 3,
        pages,
        state: createState({ order: descending }),
      }).rows.map((item) => item.value),
    ).toEqual(["emoji", "ascii", "null"])
  })

  it("paginates by emitted per-shard positions without duplication or omission", () => {
    const first = mergeFanoutPage({
      nowMs: 2_000,
      pageSize: 2,
      pages: [
        success("a", [row("1", [1, "a"], "a1"), row("2", [3, "a"], "a2")], false, "bookmark-a"),
        success("b", [row("1", [2, "a"], "b1")], true, "bookmark-b"),
      ],
      state: createState(),
    })
    expect(first.rows.map((item) => item.value)).toEqual(["a1", "b1"])
    expect(first.continuation).toMatchObject({
      bookmarks: { a: "bookmark-a", b: "bookmark-b" },
      exhaustedShardIds: ["b"],
      positions: { a: { primaryKey: "1" }, b: { primaryKey: "1" } },
    })

    const second = mergeFanoutPage({
      nowMs: 3_000,
      pageSize: 2,
      pages: [success("a", [row("2", [3, "a"], "a2")])],
      state: first.continuation as FanoutContinuationState,
    })
    expect(second.rows.map((item) => item.value)).toEqual(["a2"])
    expect(second.complete).toBe(true)
    expect(second.usage).toEqual({
      bytes: 3,
      costMicros: 3,
      cpuMs: 3,
      pages: 2,
      rows: 3,
      subrequests: 3,
    })
  })

  it("returns structured partial failures without a continuation, or fails closed", () => {
    const pages = [success("a", [row("1", [1, "x"], "a")]), failure<string>("b")]
    const partial = mergeFanoutPage({
      nowMs: 2_000,
      pageSize: 2,
      pages,
      state: createState({ partialPolicy: "allow" }),
    })
    expect(partial).toMatchObject({
      complete: false,
      failures: [{ errorCode: "timeout", shardId: "b" }],
      incomplete: true,
    })
    expect(partial.continuation).toBeUndefined()
    expectCode(
      () => mergeFanoutPage({ nowMs: 2_000, pageSize: 2, pages, state: createState() }),
      "ShardUnavailableError",
    )
  })

  it("rejects topology, query, and schema changes between pages", () => {
    const state = createState()
    const input = {
      nowMs: 2_000,
      pageSize: 1,
      pages: [success("a", []), success("b", [])],
      state,
    }
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, manifestChecksum: "44".repeat(32), shardIds: state.shardIds },
        }),
      "RouteVersionConflictError",
    )
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, shardIds: ["a", "c"] },
        }),
      "RouteVersionConflictError",
    )
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, queryChecksum: "44".repeat(32), shardIds: state.shardIds },
        }),
      "SessionTokenInvalidError",
    )
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, schemaChecksum: "44".repeat(32), shardIds: state.shardIds },
        }),
      "SchemaDriftError",
    )
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, extra: true, shardIds: state.shardIds } as never,
        }),
      "ConfigurationError",
    )
    expectCode(
      () =>
        mergeFanoutPageRaw({
          ...input,
          current: { ...checksums, shardIds: ["b", "a"] },
        }),
      "ConfigurationError",
    )
  })

  it.each([
    ["row_budget", { maxRows: 1 }, [row("1", [1, "a"], "a"), row("2", [2, "a"], "b")]],
    ["byte_budget", { maxBytes: 1 }, [row("1", [1, "a"], "a", 2)]],
    ["page_budget", { maxPages: 1 }, [row("1", [1, "a"], "a")]],
    ["cost_budget", { maxCostMicros: 2 }, [row("1", [1, "a"], "a")]],
    ["cpu_budget", { maxCpuMs: 2 }, [row("1", [1, "a"], "a")]],
    ["subrequest_budget", { maxSubrequests: 2 }, [row("1", [1, "a"], "a")]],
  ] as const)("stops continuation at the cumulative %s", (reason, budgetOverride, rows) => {
    const budget = { ...defaultBudget, ...budgetOverride }
    const result = mergeFanoutPage({
      nowMs: 2_000,
      pageSize: 2,
      pages: [success("a", rows, false), success("b", [row("9", [9, "z"], "later")], false)],
      state: createState({ budget }),
    })
    expect(result.reason).toBe(reason)
    expect(result.continuation).toBeUndefined()
  })

  it.each([
    ["row_budget", { maxRows: 1 }, row("1", [1, "a"], "a")],
    ["byte_budget", { maxBytes: 1 }, row("1", [1, "a"], "a")],
  ] as const)("closes a cursor that reaches its %s exactly", (reason, budgetOverride, onlyRow) => {
    const result = mergeFanoutPage({
      nowMs: 2_000,
      pageSize: 1,
      pages: [success("a", [onlyRow], false)],
      state: createState({
        budget: { ...defaultBudget, ...budgetOverride, maxConcurrency: 1, maxShards: 1 },
        shardIds: ["a"],
      }),
    })
    expect(result.reason).toBe(reason)
    expect(result.continuation).toBeUndefined()
  })

  it("rejects expired, overdue, and already-exhausted continuations", () => {
    const state = createState()
    expectCode(
      () => mergeFanoutPage({ nowMs: 9_000, pageSize: 1, pages: [], state }),
      "SessionTokenInvalidError",
    )
    expectCode(
      () => mergeFanoutPage({ nowMs: 10_000, pageSize: 1, pages: [], state }),
      "CapacityGuardError",
    )
    const exhausted = { ...state, consumedPages: state.budget.maxPages }
    expectCode(
      () => mergeFanoutPage({ nowMs: 2_000, pageSize: 1, pages: [], state: exhausted }),
      "ConfigurationError",
    )
    const deadlineFirst = createState({ deadlineAtMs: 8_000, expiresAtMs: 8_000 })
    expectCode(
      () => mergeFanoutPage({ nowMs: 8_000, pageSize: 1, pages: [], state: deadlineFirst }),
      "CapacityGuardError",
    )
  })

  it("enforces buffered row and byte bounds before merging", () => {
    const budget = { ...defaultBudget, maxBufferedBytes: 1, maxBufferedRows: 1 }
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 2,
          pages: [success("a", [row("1", [1, "a"], 1), row("2", [2, "a"], 2)]), success("b", [])],
          state: createState({ budget }),
        }),
      "CapacityGuardError",
    )
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 1,
          pages: [success("a", [row("1", [1, "a"], 1, 2)]), success("b", [])],
          state: createState({ budget }),
        }),
      "CapacityGuardError",
    )
  })

  it("rejects malformed configuration and persisted state with stable error families", () => {
    const invalidConfigurations: Array<() => unknown> = [
      () => createState({ shardIds: [] }),
      () => createState({ shardIds: ["b", "a"] }),
      () => createState({ shardIds: ["a", "a"] }),
      () => createState({ budget: { ...defaultBudget, maxConcurrency: 11 } }),
      () => createState({ budget: new Date() as never }),
      () =>
        createState({
          budget: { ...defaultBudget, maxConcurrency: 1, maxShards: 1 },
          shardIds: ["a", "b"],
        }),
      () => createState({ deadlineAtMs: 1_000 }),
      () => createState({ deadlineAtMs: 8_000, expiresAtMs: 9_000 }),
      () => createState({ expiresAtMs: 1_000 }),
      () => createState({ order: [] }),
      () => createState({ order: [{ direction: "other" }] as never }),
      () =>
        createState({
          order: [{ direction: "asc", immutable: false, kind: "number", nulls: "last" }] as never,
        }),
      () => createState({ partialPolicy: "other" as never }),
      () =>
        createFanoutContinuation({
          budget: defaultBudget,
          deadlineAtMs: 10_000,
          expiresAtMs: 9_000,
          manifestChecksum: "not-a-checksum",
          nowMs: 1_000,
          order,
          partialPolicy: "fail",
          queryChecksum: checksums.queryChecksum,
          schemaChecksum: checksums.schemaChecksum,
          shardIds: ["a"],
        }),
    ]
    for (const invalid of invalidConfigurations) expectCode(invalid, "ConfigurationError")

    const state = structuredClone(createState()) as unknown as Record<string, unknown>
    const invalidStates = [
      null,
      [],
      { ...state, extra: true },
      { ...state, version: 2 },
      { ...state, budget: { ...(state.budget as object), extra: true } },
      { ...state, budget: { ...(state.budget as object), maxRows: 0 } },
      {
        ...state,
        budget: { ...(state.budget as object), maxConcurrency: 1, maxShards: 1 },
      },
      { ...state, consumedRows: defaultBudget.maxRows + 1 },
      { ...state, partialPolicy: "other" },
      { ...state, positions: [] },
      { ...state, positions: { a: { orderValues: [1], primaryKey: "1" } } },
      { ...state, positions: { unknown: { orderValues: [1, "a"], primaryKey: "1" } } },
      { ...state, bookmarks: { unknown: "bookmark" } },
      { ...state, exhaustedShardIds: ["unknown"] },
      { ...state, exhaustedShardIds: ["b", "a"] },
      { ...state, exhaustedShardIds: null },
      { ...state, expiresAtMs: 10_001 },
      { ...state, queryChecksum: "not-a-checksum" },
    ]
    for (const invalid of invalidStates) {
      expectCode(() => loadFanoutContinuation(invalid), "OperationInterventionRequiredError")
    }
  })

  it("rejects contradictory, unbounded, and unordered shard pages", () => {
    const state = createState()
    const valid = [success("a", []), success("b", [])]
    const invalidPages: readonly (readonly FanoutShardPage<unknown>[])[] = [
      [],
      [success("a", [])],
      [success("a", []), success("a", [])],
      [success("a", []), success("c", [])],
      [failure("a", ""), success("b", [])],
      [{ ...success("a", []), usage: null as never }, success("b", [])],
      [success("a", [], false), success("b", [])],
      [success("a", [], true, ""), success("b", [])],
      [success("a", [row("1", [1], 1)]), success("b", [])],
      [success("a", [row("1", ["bad", "a"], 1)]), success("b", [])],
      [success("a", [row("1", [1, 2], 1)]), success("b", [])],
      [success("a", [row("", [1, "a"], 1)]), success("b", [])],
      [success("a", [row("2", [2, "a"], 2), row("1", [1, "a"], 1)]), success("b", [])],
    ]
    for (const pages of invalidPages) {
      expectCode(
        () => mergeFanoutPage({ nowMs: 2_000, pageSize: 1, pages, state }),
        "ConfigurationError",
      )
    }
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 1,
          pages: null as never,
          state,
        }),
      "ConfigurationError",
    )
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 1,
          pages: [null as never, success("b", [])],
          state,
        }),
      "ConfigurationError",
    )
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 1,
          pages: [{ ...success("a", []), kind: "other" } as never, success("b", [])],
          state,
        }),
      "ConfigurationError",
    )
    expectCode(
      () =>
        mergeFanoutPage({
          nowMs: 2_000,
          pageSize: 2,
          pages: [success("a", [row("2", [2, "a"], 2), row("1", [1, "a"], 1)]), success("b", [])],
          state,
        }),
      "ConfigurationError",
    )
    expect(() => mergeFanoutPage({ nowMs: 2_000, pageSize: 1, pages: valid, state })).not.toThrow()
  })

  it("differentially matches collect-and-sort and preserves keyset pagination", () => {
    const matrix = fc.array(
      fc.array(
        fc.record({
          first: fc.option(fc.integer({ max: 5, min: -5 }), { nil: null }),
          second: fc.option(fc.string({ maxLength: 4 }), { nil: null }),
        }),
        { maxLength: 8 },
      ),
      { maxLength: 5, minLength: 1 },
    )
    fc.assert(
      fc.property(matrix, fc.integer({ max: 6, min: 1 }), (valuesByShard, pageSize) => {
        const shardIds = valuesByShard.map((_, index) => `s${index}`)
        const pages = valuesByShard.map((values, shardIndex) => {
          const rows = values
            .map((value, rowIndex) =>
              row(`${rowIndex.toString().padStart(3, "0")}`, [value.first, value.second], {
                rowIndex,
                shardIndex,
              }),
            )
            .sort((left, right) => testCompare(left, right))
          return success(shardIds[shardIndex] as string, rows)
        })
        const expected = referenceFanoutOrder({ order, pages })
        let state = createState({
          budget: { ...defaultBudget, maxConcurrency: shardIds.length, maxShards: shardIds.length },
          shardIds,
        })
        const actual: (typeof expected)[number][] = []
        for (let call = 0; call < 100; call += 1) {
          const nextPages = pages
            .filter((page) => !state.exhaustedShardIds.includes(page.shardId))
            .map((page) => {
              const position = state.positions[page.shardId]
              const remaining = page.rows.filter(
                (candidate) =>
                  position === undefined || testComparePosition(position, candidate) < 0,
              )
              const nextRows = remaining.slice(0, pageSize)
              return success(page.shardId, nextRows, nextRows.length === remaining.length)
            })
          const result = mergeFanoutPage({ nowMs: 2_000, pageSize, pages: nextPages, state })
          actual.push(...result.rows)
          if (result.complete) break
          state = result.continuation as FanoutContinuationState
        }
        expect(actual).toEqual(expected)
      }),
      { numRuns: 200 },
    )
  })
})

function testCompare(left: FanoutRow<unknown>, right: FanoutRow<unknown>): number {
  const first = testValue(
    left.orderValues[0] as FanoutRow<unknown>["orderValues"][number],
    right.orderValues[0] as FanoutRow<unknown>["orderValues"][number],
    order[0],
  )
  if (first !== 0) return first
  const second = testValue(
    left.orderValues[1] as FanoutRow<unknown>["orderValues"][number],
    right.orderValues[1] as FanoutRow<unknown>["orderValues"][number],
    order[1],
  )
  return second === 0 ? Buffer.from(left.primaryKey).compare(Buffer.from(right.primaryKey)) : second
}

function testComparePosition(position: FanoutPosition, rowValue: FanoutRow<unknown>): number {
  return testCompare(
    {
      byteSize: 0,
      orderValues: position.orderValues,
      primaryKey: position.primaryKey,
      value: null,
    },
    rowValue,
  )
}

function testValue(
  left: FanoutRow<unknown>["orderValues"][number],
  right: FanoutRow<unknown>["orderValues"][number],
  column: FanoutOrderColumn,
): number {
  if (left === null || right === null) {
    if (left === right) return 0
    return left === null ? (column.nulls === "first" ? -1 : 1) : column.nulls === "first" ? 1 : -1
  }
  const compared =
    column.kind === "number"
      ? (left as number) - (right as number)
      : Buffer.from(left as string).compare(Buffer.from(right as string))
  return column.direction === "asc" ? compared : -compared
}
