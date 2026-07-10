import { NozzleError } from "@nozzle/core"

export type FanoutOrderValue = null | number | string

export const MAX_FANOUT_PAGE_ROWS = 1_000_000

export interface FanoutOrderColumn {
  readonly direction: "asc" | "desc"
  readonly immutable: true
  readonly kind: "number" | "string"
  readonly nulls: "first" | "last"
}

export interface FanoutCurrentIdentity {
  readonly manifestChecksum: string
  readonly queryChecksum: string
  readonly schemaChecksum: string
  readonly shardIds: readonly string[]
}

export interface FanoutRow<T> {
  readonly byteSize: number
  readonly orderValues: readonly FanoutOrderValue[]
  readonly primaryKey: string
  readonly value: T
}

export type FanoutShardPage<T> =
  | {
      readonly bookmark?: string
      readonly exhausted: boolean
      readonly kind: "success"
      readonly rows: readonly FanoutRow<T>[]
      readonly shardId: string
      readonly usage: FanoutShardUsage
    }
  | {
      readonly errorCode: string
      readonly kind: "failure"
      readonly shardId: string
      readonly usage: FanoutShardUsage
    }

export interface FanoutShardUsage {
  readonly costMicros: number
  readonly cpuMs: number
  readonly subrequests: number
}

export interface FanoutBudget {
  readonly maxBufferedBytes: number
  readonly maxBufferedRows: number
  readonly maxBytes: number
  readonly maxConcurrency: number
  readonly maxCostMicros: number
  readonly maxCpuMs: number
  readonly maxPages: number
  readonly maxRows: number
  readonly maxShards: number
  readonly maxSubrequests: number
  readonly timeoutMs: number
}

export interface FanoutPosition {
  readonly orderValues: readonly FanoutOrderValue[]
  readonly primaryKey: string
}

export interface FanoutContinuationState {
  readonly bookmarks: Readonly<Record<string, string>>
  readonly budget: FanoutBudget
  readonly consumedBytes: number
  readonly consumedCostMicros: number
  readonly consumedCpuMs: number
  readonly consumedPages: number
  readonly consumedRows: number
  readonly consumedSubrequests: number
  readonly deadlineAtMs: number
  readonly exhaustedShardIds: readonly string[]
  readonly expiresAtMs: number
  readonly manifestChecksum: string
  readonly order: readonly FanoutOrderColumn[]
  readonly partialPolicy: "allow" | "fail"
  readonly positions: Readonly<Record<string, FanoutPosition>>
  readonly queryChecksum: string
  readonly schemaChecksum: string
  readonly shardIds: readonly string[]
  readonly version: 1
}

export interface FanoutMergedRow<T> {
  readonly primaryKey: string
  readonly shardId: string
  readonly value: T
}

export interface FanoutFailure {
  readonly errorCode: string
  readonly shardId: string
}

export interface FanoutPageResult<T> {
  readonly complete: boolean
  readonly consistency: "best_effort_no_global_snapshot"
  readonly continuation?: FanoutContinuationState
  readonly failures: readonly FanoutFailure[]
  readonly incomplete: boolean
  readonly reason?:
    | "byte_budget"
    | "cost_budget"
    | "cpu_budget"
    | "page_budget"
    | "row_budget"
    | "subrequest_budget"
  readonly rows: readonly FanoutMergedRow<T>[]
  readonly usage: {
    readonly bytes: number
    readonly costMicros: number
    readonly cpuMs: number
    readonly pages: number
    readonly rows: number
    readonly subrequests: number
  }
}

interface HeapItem<T> {
  readonly page: Extract<FanoutShardPage<T>, { kind: "success" }>
  readonly row: FanoutRow<T>
  readonly rowIndex: number
}

const MAX_ORDER_COLUMNS = 16
const MAX_SHARDS = 10_000
const MAX_ROWS = MAX_FANOUT_PAGE_ROWS
const MAX_BYTES = 128 * 1024 * 1024
const MAX_PAGES = 10_000
const MAX_CPU_MS = 60_000
const MAX_COST_MICROS = 1_000_000_000_000
const MAX_SUBREQUESTS = 10_000
const MAX_TIMEOUT_MS = 300_000
const CHECKSUM = /^[0-9a-f]{64}$/u

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function exactRecord(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) return false
  const actual = Object.keys(value)
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key))
}

function nonEmpty(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function checksum(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
  }
}

function integer(
  value: unknown,
  label: string,
  minimum: number,
  maximum: number,
): asserts value is number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    configuration(`${label} must be an integer between ${minimum} and ${maximum}.`)
  }
}

function utf8Compare(left: string, right: string): number {
  const leftBytes = new TextEncoder().encode(left)
  const rightBytes = new TextEncoder().encode(right)
  const length = Math.min(leftBytes.length, rightBytes.length)
  for (let index = 0; index < length; index += 1) {
    const difference = (leftBytes[index] as number) - (rightBytes[index] as number)
    if (difference !== 0) return difference < 0 ? -1 : 1
  }
  return leftBytes.length === rightBytes.length ? 0 : leftBytes.length < rightBytes.length ? -1 : 1
}

export function compareFanoutOrderValues(
  left: FanoutOrderValue,
  right: FanoutOrderValue,
  column: FanoutOrderColumn,
): number {
  if (left === null || right === null) {
    if (left === right) return 0
    return left === null ? (column.nulls === "first" ? -1 : 1) : column.nulls === "first" ? 1 : -1
  }
  const result =
    column.kind === "number"
      ? (left as number) === (right as number)
        ? 0
        : (left as number) < (right as number)
          ? -1
          : 1
      : utf8Compare(left as string, right as string)
  return column.direction === "asc" ? result : -result
}

function validateOrderValue(value: FanoutOrderValue, column: FanoutOrderColumn): void {
  if (value === null) return
  if (column.kind === "number") {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      configuration("Fan-out numeric order value is malformed.")
    }
  } else if (typeof value !== "string") {
    configuration("Fan-out string order value is malformed.")
  }
}

function comparePosition(
  left: FanoutPosition,
  right: FanoutPosition,
  order: readonly FanoutOrderColumn[],
): number {
  for (let index = 0; index < order.length; index += 1) {
    const difference = compareFanoutOrderValues(
      left.orderValues[index] as FanoutOrderValue,
      right.orderValues[index] as FanoutOrderValue,
      order[index] as FanoutOrderColumn,
    )
    if (difference !== 0) return difference
  }
  return utf8Compare(left.primaryKey, right.primaryKey)
}

function compareRows<T>(
  left: FanoutRow<T>,
  leftShardId: string,
  right: FanoutRow<T>,
  rightShardId: string,
  order: readonly FanoutOrderColumn[],
): number {
  for (let index = 0; index < order.length; index += 1) {
    const difference = compareFanoutOrderValues(
      left.orderValues[index] as FanoutOrderValue,
      right.orderValues[index] as FanoutOrderValue,
      order[index] as FanoutOrderColumn,
    )
    if (difference !== 0) return difference
  }
  const shardDifference = utf8Compare(leftShardId, rightShardId)
  return shardDifference === 0 ? utf8Compare(left.primaryKey, right.primaryKey) : shardDifference
}

function rowPosition<T>(row: FanoutRow<T>): FanoutPosition {
  return Object.freeze({
    orderValues: Object.freeze([...row.orderValues]),
    primaryKey: row.primaryKey,
  })
}

function validatePosition(
  position: FanoutPosition,
  order: readonly FanoutOrderColumn[],
  label: string,
): void {
  if (
    !exactRecord(position, ["orderValues", "primaryKey"]) ||
    !Array.isArray(position.orderValues) ||
    position.orderValues.length !== order.length
  ) {
    intervention(`${label} is malformed.`)
  }
  nonEmpty(position.primaryKey, `${label} primary key`)
  for (let index = 0; index < order.length; index += 1) {
    validateOrderValue(
      position.orderValues[index] as FanoutOrderValue,
      order[index] as FanoutOrderColumn,
    )
  }
}

function validateOrder(order: readonly FanoutOrderColumn[]): void {
  if (!Array.isArray(order) || order.length < 1 || order.length > MAX_ORDER_COLUMNS) {
    configuration(`Fan-out order must contain between one and ${MAX_ORDER_COLUMNS} columns.`)
  }
  for (const column of order) {
    if (
      !exactRecord(column, ["direction", "immutable", "kind", "nulls"]) ||
      (column.direction !== "asc" && column.direction !== "desc") ||
      column.immutable !== true ||
      (column.kind !== "number" && column.kind !== "string") ||
      (column.nulls !== "first" && column.nulls !== "last")
    ) {
      configuration("Fan-out order column is malformed.")
    }
  }
}

function validateBudget(budget: FanoutBudget): void {
  if (
    !exactRecord(budget, [
      "maxBufferedBytes",
      "maxBufferedRows",
      "maxBytes",
      "maxConcurrency",
      "maxCostMicros",
      "maxCpuMs",
      "maxPages",
      "maxRows",
      "maxShards",
      "maxSubrequests",
      "timeoutMs",
    ])
  ) {
    configuration("Fan-out budget is malformed.")
  }
  integer(budget.maxBufferedRows, "Fan-out buffered-row budget", 1, MAX_ROWS)
  integer(budget.maxBufferedBytes, "Fan-out buffered-byte budget", 1, MAX_BYTES)
  integer(budget.maxShards, "Fan-out shard budget", 1, MAX_SHARDS)
  integer(budget.maxRows, "Fan-out row budget", 1, MAX_ROWS)
  integer(budget.maxBytes, "Fan-out byte budget", 1, MAX_BYTES)
  integer(budget.maxPages, "Fan-out page budget", 1, MAX_PAGES)
  integer(budget.maxConcurrency, "Fan-out concurrency budget", 1, MAX_SHARDS)
  integer(budget.timeoutMs, "Fan-out timeout", 1, MAX_TIMEOUT_MS)
  integer(budget.maxCpuMs, "Fan-out CPU budget", 1, MAX_CPU_MS)
  integer(budget.maxSubrequests, "Fan-out subrequest budget", 1, MAX_SUBREQUESTS)
  integer(budget.maxCostMicros, "Fan-out cost budget", 1, MAX_COST_MICROS)
  if (budget.maxConcurrency > budget.maxShards) {
    configuration("Fan-out concurrency cannot exceed the shard budget.")
  }
}

function canonicalShardIds(shardIds: readonly string[]): readonly string[] {
  if (!Array.isArray(shardIds) || shardIds.length === 0) {
    configuration("Fan-out shard membership must be non-empty.")
  }
  const validated = shardIds.map((shardId) => {
    nonEmpty(shardId, "Fan-out shard ID")
    return shardId
  })
  const canonical = [...new Set(validated)].sort(utf8Compare)
  if (canonical.length !== validated.length)
    configuration("Fan-out shard membership is duplicated.")
  if (canonical.some((shardId, index) => shardId !== validated[index])) {
    configuration("Fan-out shard membership is not canonical.")
  }
  return Object.freeze(canonical)
}

function frozenBudget(budget: FanoutBudget): FanoutBudget {
  return Object.freeze({
    maxBufferedBytes: budget.maxBufferedBytes,
    maxBufferedRows: budget.maxBufferedRows,
    maxBytes: budget.maxBytes,
    maxConcurrency: budget.maxConcurrency,
    maxCostMicros: budget.maxCostMicros,
    maxCpuMs: budget.maxCpuMs,
    maxPages: budget.maxPages,
    maxRows: budget.maxRows,
    maxShards: budget.maxShards,
    maxSubrequests: budget.maxSubrequests,
    timeoutMs: budget.timeoutMs,
  })
}

export function createFanoutContinuation(input: {
  readonly budget: FanoutBudget
  readonly deadlineAtMs: number
  readonly expiresAtMs: number
  readonly manifestChecksum: string
  readonly nowMs: number
  readonly order: readonly FanoutOrderColumn[]
  readonly partialPolicy: "allow" | "fail"
  readonly queryChecksum: string
  readonly schemaChecksum: string
  readonly shardIds: readonly string[]
}): FanoutContinuationState {
  validateBudget(input.budget)
  validateOrder(input.order)
  const shardIds = canonicalShardIds(input.shardIds)
  if (shardIds.length > input.budget.maxShards) {
    configuration("Fan-out shard membership exceeds its sealed budget.")
  }
  checksum(input.manifestChecksum, "Fan-out manifest checksum")
  checksum(input.queryChecksum, "Fan-out query checksum")
  checksum(input.schemaChecksum, "Fan-out schema checksum")
  integer(input.nowMs, "Fan-out current time", 0, Number.MAX_SAFE_INTEGER)
  integer(input.deadlineAtMs, "Fan-out deadline", 1, Number.MAX_SAFE_INTEGER)
  integer(input.expiresAtMs, "Fan-out expiry", 1, Number.MAX_SAFE_INTEGER)
  if (input.deadlineAtMs <= input.nowMs) configuration("Fan-out deadline must be in the future.")
  if (input.expiresAtMs <= input.nowMs) configuration("Fan-out expiry must be in the future.")
  if (input.expiresAtMs > input.deadlineAtMs) {
    configuration("Fan-out cursor expiry cannot exceed its operation deadline.")
  }
  if (input.partialPolicy !== "allow" && input.partialPolicy !== "fail") {
    configuration("Fan-out partial-result policy is unsupported.")
  }
  return Object.freeze({
    bookmarks: Object.freeze({}),
    budget: frozenBudget(input.budget),
    consumedBytes: 0,
    consumedCostMicros: 0,
    consumedCpuMs: 0,
    consumedPages: 0,
    consumedRows: 0,
    consumedSubrequests: 0,
    deadlineAtMs: input.deadlineAtMs,
    exhaustedShardIds: Object.freeze([]),
    expiresAtMs: input.expiresAtMs,
    manifestChecksum: input.manifestChecksum,
    order: Object.freeze(input.order.map((column) => Object.freeze({ ...column }))),
    partialPolicy: input.partialPolicy,
    positions: Object.freeze({}),
    queryChecksum: input.queryChecksum,
    schemaChecksum: input.schemaChecksum,
    shardIds,
    version: 1,
  })
}

function loadFanoutContinuationUnchecked(candidate: unknown): FanoutContinuationState {
  if (
    !exactRecord(candidate, [
      "bookmarks",
      "budget",
      "consumedBytes",
      "consumedCostMicros",
      "consumedCpuMs",
      "consumedPages",
      "consumedRows",
      "consumedSubrequests",
      "deadlineAtMs",
      "exhaustedShardIds",
      "expiresAtMs",
      "manifestChecksum",
      "order",
      "partialPolicy",
      "positions",
      "queryChecksum",
      "schemaChecksum",
      "shardIds",
      "version",
    ])
  ) {
    return intervention("Persisted fan-out continuation is malformed.")
  }
  const value = candidate as Partial<FanoutContinuationState>
  if (value.version !== 1) intervention("Persisted fan-out continuation version is unsupported.")
  validateBudget(value.budget as FanoutBudget)
  validateOrder(value.order as readonly FanoutOrderColumn[])
  const shardIds = canonicalShardIds(value.shardIds as readonly string[])
  if (shardIds.length > (value.budget as FanoutBudget).maxShards) {
    intervention("Persisted fan-out shard membership exceeds its budget.")
  }
  checksum(value.manifestChecksum, "Persisted fan-out manifest checksum")
  checksum(value.queryChecksum, "Persisted fan-out query checksum")
  checksum(value.schemaChecksum, "Persisted fan-out schema checksum")
  integer(value.expiresAtMs, "Persisted fan-out expiry", 1, Number.MAX_SAFE_INTEGER)
  integer(value.deadlineAtMs, "Persisted fan-out deadline", 1, Number.MAX_SAFE_INTEGER)
  if ((value.expiresAtMs as number) > (value.deadlineAtMs as number)) {
    intervention("Persisted fan-out expiry exceeds its operation deadline.")
  }
  integer(value.consumedBytes, "Persisted fan-out consumed bytes", 0, MAX_BYTES)
  integer(value.consumedCostMicros, "Persisted fan-out consumed cost", 0, MAX_COST_MICROS)
  integer(value.consumedCpuMs, "Persisted fan-out consumed CPU", 0, MAX_CPU_MS)
  integer(value.consumedPages, "Persisted fan-out consumed pages", 0, MAX_PAGES)
  integer(value.consumedRows, "Persisted fan-out consumed rows", 0, MAX_ROWS)
  integer(value.consumedSubrequests, "Persisted fan-out consumed subrequests", 0, MAX_SUBREQUESTS)
  if (
    (value.consumedBytes as number) > (value.budget as FanoutBudget).maxBytes ||
    (value.consumedCostMicros as number) > (value.budget as FanoutBudget).maxCostMicros ||
    (value.consumedCpuMs as number) > (value.budget as FanoutBudget).maxCpuMs ||
    (value.consumedPages as number) > (value.budget as FanoutBudget).maxPages ||
    (value.consumedRows as number) > (value.budget as FanoutBudget).maxRows ||
    (value.consumedSubrequests as number) > (value.budget as FanoutBudget).maxSubrequests
  ) {
    intervention("Persisted fan-out usage exceeds its sealed budget.")
  }
  if (value.partialPolicy !== "allow" && value.partialPolicy !== "fail") {
    intervention("Persisted fan-out partial-result policy is unsupported.")
  }
  if (
    typeof value.positions !== "object" ||
    value.positions === null ||
    Array.isArray(value.positions) ||
    typeof value.bookmarks !== "object" ||
    value.bookmarks === null ||
    Array.isArray(value.bookmarks)
  ) {
    intervention("Persisted fan-out per-shard state is malformed.")
  }
  const shardSet = new Set(shardIds)
  const positions: Record<string, FanoutPosition> = {}
  for (const [shardId, position] of Object.entries(value.positions)) {
    if (!shardSet.has(shardId)) intervention("Fan-out position references an unknown shard.")
    validatePosition(position, value.order as readonly FanoutOrderColumn[], "Fan-out position")
    positions[shardId] = Object.freeze({
      orderValues: Object.freeze([...position.orderValues]),
      primaryKey: position.primaryKey,
    })
  }
  const bookmarks: Record<string, string> = {}
  for (const [shardId, bookmark] of Object.entries(value.bookmarks)) {
    if (!shardSet.has(shardId)) intervention("Fan-out bookmark references an unknown shard.")
    nonEmpty(bookmark, "Fan-out bookmark")
    bookmarks[shardId] = bookmark
  }
  const exhaustedShardIds = canonicalShardIdsOrEmpty(value.exhaustedShardIds, "exhausted shard")
  if (exhaustedShardIds.some((shardId) => !shardSet.has(shardId))) {
    intervention("Fan-out exhaustion state references an unknown shard.")
  }
  return Object.freeze({
    bookmarks: Object.freeze(bookmarks),
    budget: frozenBudget(value.budget as FanoutBudget),
    consumedBytes: value.consumedBytes as number,
    consumedCostMicros: value.consumedCostMicros as number,
    consumedCpuMs: value.consumedCpuMs as number,
    consumedPages: value.consumedPages as number,
    consumedRows: value.consumedRows as number,
    consumedSubrequests: value.consumedSubrequests as number,
    deadlineAtMs: value.deadlineAtMs as number,
    exhaustedShardIds,
    expiresAtMs: value.expiresAtMs as number,
    manifestChecksum: value.manifestChecksum,
    order: Object.freeze(
      (value.order as readonly FanoutOrderColumn[]).map((column) => Object.freeze({ ...column })),
    ),
    partialPolicy: value.partialPolicy,
    positions: Object.freeze(positions),
    queryChecksum: value.queryChecksum,
    schemaChecksum: value.schemaChecksum,
    shardIds,
    version: 1,
  })
}

export function loadFanoutContinuation(candidate: unknown): FanoutContinuationState {
  try {
    return loadFanoutContinuationUnchecked(candidate)
  } catch (error) {
    if (error instanceof NozzleError && error.code === "OperationInterventionRequiredError") {
      throw error
    }
    return intervention("Persisted fan-out continuation is malformed.")
  }
}

function canonicalShardIdsOrEmpty(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value)) intervention(`Persisted fan-out ${label} membership is malformed.`)
  if (value.length === 0) return Object.freeze([])
  const canonical = value.map((shardId) => {
    nonEmpty(shardId, `Fan-out ${label} ID`)
    return shardId
  })
  const sorted = [...new Set(canonical)].sort(utf8Compare)
  if (
    sorted.length !== canonical.length ||
    sorted.some((shardId, index) => shardId !== canonical[index])
  ) {
    intervention(`Persisted fan-out ${label} membership is not canonical.`)
  }
  return Object.freeze(sorted)
}

function validateRow<T>(row: FanoutRow<T>, order: readonly FanoutOrderColumn[]): void {
  if (
    typeof row !== "object" ||
    row === null ||
    !Array.isArray(row.orderValues) ||
    row.orderValues.length !== order.length
  ) {
    configuration("Fan-out row ordering metadata is malformed.")
  }
  nonEmpty(row.primaryKey, "Fan-out row primary key")
  integer(row.byteSize, "Fan-out row byte size", 0, MAX_BYTES)
  for (let index = 0; index < order.length; index += 1) {
    validateOrderValue(
      row.orderValues[index] as FanoutOrderValue,
      order[index] as FanoutOrderColumn,
    )
  }
}

function validateUsage(usage: FanoutShardUsage): void {
  if (typeof usage !== "object" || usage === null) {
    configuration("Fan-out shard usage is required.")
  }
  integer(usage.costMicros, "Fan-out shard cost", 0, MAX_COST_MICROS)
  integer(usage.cpuMs, "Fan-out shard CPU", 0, MAX_CPU_MS)
  integer(usage.subrequests, "Fan-out shard subrequests", 0, MAX_SUBREQUESTS)
}

function validatePages<T>(
  state: FanoutContinuationState,
  pages: readonly FanoutShardPage<T>[],
  pageSize: number,
): void {
  if (!Array.isArray(pages)) configuration("Fan-out shard pages are required.")
  const exhausted = new Set(state.exhaustedShardIds)
  const expected = state.shardIds.filter((shardId) => !exhausted.has(shardId))
  const expectedSet = new Set(expected)
  if (pages.length !== expected.length) {
    configuration("Fan-out shard page membership is incomplete.")
  }
  const seen = new Set<string>()
  for (const page of pages) {
    if (typeof page !== "object" || page === null) configuration("Fan-out shard page is malformed.")
    nonEmpty(page.shardId, "Fan-out page shard ID")
    if (!expectedSet.has(page.shardId) || seen.has(page.shardId)) {
      configuration("Fan-out shard page membership is contradictory.")
    }
    seen.add(page.shardId)
    validateUsage(page.usage)
    if (page.kind === "failure") {
      nonEmpty(page.errorCode, "Fan-out shard error code")
      continue
    }
    if (
      page.kind !== "success" ||
      !Array.isArray(page.rows) ||
      typeof page.exhausted !== "boolean"
    ) {
      configuration("Fan-out shard success page is malformed.")
    }
    if (page.rows.length > pageSize || (page.rows.length === 0 && !page.exhausted)) {
      configuration("Fan-out shard page violates the bounded fetch contract.")
    }
    if (page.bookmark !== undefined) nonEmpty(page.bookmark, "Fan-out shard bookmark")
    let previous = state.positions[page.shardId]
    for (const row of page.rows) {
      validateRow(row, state.order)
      const position = rowPosition(row)
      if (previous !== undefined && comparePosition(previous, position, state.order) >= 0) {
        configuration("Fan-out shard page is not strictly keyset ordered.")
      }
      previous = position
    }
  }
  const successfulPages = pages.filter(
    (page): page is Extract<FanoutShardPage<T>, { kind: "success" }> => page.kind === "success",
  )
  const bufferedRows = successfulPages.reduce((total, page) => total + page.rows.length, 0)
  const bufferedBytes = successfulPages.reduce(
    (total, page) => total + page.rows.reduce((pageTotal, row) => pageTotal + row.byteSize, 0),
    0,
  )
  if (
    bufferedRows > state.budget.maxBufferedRows ||
    bufferedBytes > state.budget.maxBufferedBytes
  ) {
    throw new NozzleError("CapacityGuardError", "Fan-out input exceeds its buffer budget.")
  }
}

function heapPush<T>(
  heap: HeapItem<T>[],
  item: HeapItem<T>,
  order: readonly FanoutOrderColumn[],
): void {
  heap.push(item)
  let index = heap.length - 1
  while (index > 0) {
    const parent = Math.floor((index - 1) / 2)
    if (compareHeap(heap[parent] as HeapItem<T>, item, order) <= 0) break
    heap[index] = heap[parent] as HeapItem<T>
    index = parent
  }
  heap[index] = item
}

function heapPop<T>(
  heap: HeapItem<T>[],
  order: readonly FanoutOrderColumn[],
): HeapItem<T> | undefined {
  const first = heap[0]
  const last = heap.pop()
  if (first === undefined || last === undefined || heap.length === 0) return first
  let index = 0
  while (true) {
    const left = index * 2 + 1
    if (left >= heap.length) break
    const right = left + 1
    const child =
      right < heap.length &&
      compareHeap(heap[right] as HeapItem<T>, heap[left] as HeapItem<T>, order) < 0
        ? right
        : left
    if (compareHeap(last, heap[child] as HeapItem<T>, order) <= 0) break
    heap[index] = heap[child] as HeapItem<T>
    index = child
  }
  heap[index] = last
  return first
}

function compareHeap<T>(
  left: HeapItem<T>,
  right: HeapItem<T>,
  order: readonly FanoutOrderColumn[],
): number {
  return compareRows(left.row, left.page.shardId, right.row, right.page.shardId, order)
}

function referenceItems<T>(
  pages: readonly FanoutShardPage<T>[],
  order: readonly FanoutOrderColumn[],
): HeapItem<T>[] {
  return pages
    .filter(
      (page): page is Extract<FanoutShardPage<T>, { kind: "success" }> => page.kind === "success",
    )
    .flatMap((page) => page.rows.map((row, rowIndex) => ({ page, row, rowIndex })))
    .sort((left, right) => compareHeap(left, right, order))
}

export function referenceFanoutOrder<T>(input: {
  readonly order: readonly FanoutOrderColumn[]
  readonly pages: readonly FanoutShardPage<T>[]
}): readonly FanoutMergedRow<T>[] {
  validateOrder(input.order)
  const rows = referenceItems(input.pages, input.order).map((item) =>
    Object.freeze({
      primaryKey: item.row.primaryKey,
      shardId: item.page.shardId,
      value: item.row.value,
    }),
  )
  return Object.freeze(rows)
}

export function validateFanoutContinuationIdentity(
  state: FanoutContinuationState,
  current: FanoutCurrentIdentity,
): void {
  if (!exactRecord(current, ["manifestChecksum", "queryChecksum", "schemaChecksum", "shardIds"])) {
    configuration("Current fan-out identity is malformed.")
  }
  checksum(current.manifestChecksum, "Current fan-out manifest checksum")
  checksum(current.queryChecksum, "Current fan-out query checksum")
  checksum(current.schemaChecksum, "Current fan-out schema checksum")
  const shardIds = canonicalShardIds(current.shardIds)
  if (
    current.manifestChecksum !== state.manifestChecksum ||
    shardIds.length !== state.shardIds.length ||
    shardIds.some((shardId, index) => shardId !== state.shardIds[index])
  ) {
    throw new NozzleError(
      "RouteVersionConflictError",
      "Fan-out topology changed after the cursor was created.",
    )
  }
  if (current.queryChecksum !== state.queryChecksum) {
    throw new NozzleError("SessionTokenInvalidError", "Fan-out cursor belongs to another query.")
  }
  if (current.schemaChecksum !== state.schemaChecksum) {
    throw new NozzleError("SchemaDriftError", "Fan-out schema changed after cursor creation.")
  }
}

export function mergeFanoutPage<T>(input: {
  readonly current: FanoutCurrentIdentity
  readonly nowMs: number
  readonly pageSize: number
  readonly pages: readonly FanoutShardPage<T>[]
  readonly state: FanoutContinuationState
}): FanoutPageResult<T> {
  const state = loadFanoutContinuation(input.state)
  validateFanoutContinuationIdentity(state, input.current)
  integer(input.nowMs, "Fan-out current time", 0, Number.MAX_SAFE_INTEGER)
  if (input.nowMs >= state.deadlineAtMs) {
    throw new NozzleError("CapacityGuardError", "Fan-out operation deadline has elapsed.")
  }
  if (input.nowMs >= state.expiresAtMs) {
    throw new NozzleError("SessionTokenInvalidError", "Fan-out continuation has expired.")
  }
  integer(input.pageSize, "Fan-out page size", 1, MAX_ROWS)
  if (state.consumedPages >= state.budget.maxPages) {
    configuration("Fan-out page budget is already exhausted.")
  }
  validatePages(state, input.pages, input.pageSize)
  let batchCostMicros = 0
  let batchCpuMs = 0
  let batchSubrequests = 0
  for (const page of input.pages) {
    batchCostMicros = Math.min(
      state.budget.maxCostMicros + 1,
      batchCostMicros + page.usage.costMicros,
    )
    batchCpuMs = Math.min(state.budget.maxCpuMs + 1, batchCpuMs + page.usage.cpuMs)
    batchSubrequests = Math.min(
      state.budget.maxSubrequests + 1,
      batchSubrequests + page.usage.subrequests,
    )
  }
  const failures = Object.freeze(
    input.pages
      .filter(
        (page): page is Extract<FanoutShardPage<T>, { kind: "failure" }> => page.kind === "failure",
      )
      .map((page) => Object.freeze({ errorCode: page.errorCode, shardId: page.shardId })),
  )
  if (failures.length > 0 && state.partialPolicy === "fail") {
    throw new NozzleError("ShardUnavailableError", "A required fan-out shard failed.", {
      details: { failures },
    })
  }

  const successfulPages = input.pages.filter(
    (page): page is Extract<FanoutShardPage<T>, { kind: "success" }> => page.kind === "success",
  )
  const heap: HeapItem<T>[] = []
  for (const page of successfulPages) {
    const row = page.rows[0]
    if (row !== undefined) heapPush(heap, { page, row, rowIndex: 0 }, state.order)
  }
  const rows: FanoutMergedRow<T>[] = []
  const positions: Record<string, FanoutPosition> = { ...state.positions }
  const bookmarks: Record<string, string> = { ...state.bookmarks }
  for (const page of successfulPages) {
    if (page.bookmark !== undefined) bookmarks[page.shardId] = page.bookmark
  }
  let pageBytes = 0
  let reason: FanoutPageResult<T>["reason"]
  while (heap.length > 0 && rows.length < input.pageSize) {
    const item = heapPop(heap, state.order) as HeapItem<T>
    if (state.consumedRows + rows.length >= state.budget.maxRows) {
      reason = "row_budget"
      break
    }
    if (state.consumedBytes + pageBytes + item.row.byteSize > state.budget.maxBytes) {
      reason = "byte_budget"
      break
    }
    rows.push(
      Object.freeze({
        primaryKey: item.row.primaryKey,
        shardId: item.page.shardId,
        value: item.row.value,
      }),
    )
    pageBytes += item.row.byteSize
    positions[item.page.shardId] = rowPosition(item.row)
    const nextIndex = item.rowIndex + 1
    const next = item.page.rows[nextIndex]
    if (next !== undefined) {
      heapPush(heap, { page: item.page, row: next, rowIndex: nextIndex }, state.order)
    }
  }

  const exhausted = new Set(state.exhaustedShardIds)
  for (const page of successfulPages) {
    const emittedPosition = positions[page.shardId]
    const finalRow = page.rows.at(-1)
    const consumedWholePage =
      finalRow === undefined ||
      (emittedPosition !== undefined &&
        comparePosition(emittedPosition, rowPosition(finalRow), state.order) === 0)
    if (page.exhausted && consumedWholePage) exhausted.add(page.shardId)
  }
  const consumedRows = state.consumedRows + rows.length
  const consumedBytes = state.consumedBytes + pageBytes
  const consumedCostMicros = state.consumedCostMicros + batchCostMicros
  const consumedCpuMs = state.consumedCpuMs + batchCpuMs
  const consumedPages = state.consumedPages + 1
  const consumedSubrequests = state.consumedSubrequests + batchSubrequests
  const complete = failures.length === 0 && exhausted.size === state.shardIds.length
  const more = !complete && failures.length === 0
  if (reason === undefined && more && consumedPages >= state.budget.maxPages) {
    reason = "page_budget"
  }
  if (reason === undefined && more && consumedRows >= state.budget.maxRows) {
    reason = "row_budget"
  }
  if (reason === undefined && more && consumedBytes >= state.budget.maxBytes) {
    reason = "byte_budget"
  }
  if (reason === undefined && more && consumedCostMicros >= state.budget.maxCostMicros) {
    reason = "cost_budget"
  }
  if (reason === undefined && more && consumedCpuMs >= state.budget.maxCpuMs) {
    reason = "cpu_budget"
  }
  if (reason === undefined && more && consumedSubrequests >= state.budget.maxSubrequests) {
    reason = "subrequest_budget"
  }
  const continuation =
    more && reason === undefined
      ? Object.freeze({
          ...state,
          bookmarks: Object.freeze(bookmarks),
          consumedBytes,
          consumedCostMicros,
          consumedCpuMs,
          consumedPages,
          consumedRows,
          consumedSubrequests,
          exhaustedShardIds: Object.freeze([...exhausted].sort(utf8Compare)),
          positions: Object.freeze(positions),
        })
      : undefined
  return Object.freeze({
    complete,
    consistency: "best_effort_no_global_snapshot" as const,
    ...(continuation === undefined ? {} : { continuation }),
    failures,
    incomplete: failures.length > 0,
    ...(reason === undefined ? {} : { reason }),
    rows: Object.freeze(rows),
    usage: Object.freeze({
      bytes: consumedBytes,
      costMicros: consumedCostMicros,
      cpuMs: consumedCpuMs,
      pages: consumedPages,
      rows: consumedRows,
      subrequests: consumedSubrequests,
    }),
  })
}
