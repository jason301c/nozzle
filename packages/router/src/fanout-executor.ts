import { NozzleError } from "@nozzle/core"
import {
  type FanoutContinuationState,
  type FanoutCurrentIdentity,
  type FanoutPageResult,
  type FanoutPosition,
  type FanoutRow,
  type FanoutShardPage,
  type FanoutShardUsage,
  loadFanoutContinuation,
  MAX_FANOUT_PAGE_ROWS,
  mergeFanoutPage,
  validateFanoutContinuationIdentity,
} from "./fanout.js"

export interface FanoutFetchRequest {
  readonly bookmark?: string
  readonly maxBytes: number
  readonly position?: FanoutPosition
  readonly rowLimit: number
  readonly shardId: string
  readonly signal: AbortSignal
}

export interface FanoutFetchResult<T> {
  readonly bookmark?: string
  readonly exhausted: boolean
  readonly rows: readonly FanoutRow<T>[]
  readonly usage: FanoutShardUsage
}

export interface FanoutClock {
  readonly clearTimer: (handle: unknown) => void
  readonly nowMs: () => number
  readonly setTimer: (callback: () => void, delayMs: number) => unknown
}

export interface FanoutExecutorInput<T> {
  readonly classifyFailure?: (error: unknown) => string
  readonly clock?: FanoutClock
  readonly current: FanoutCurrentIdentity
  readonly estimateUsage: (shardId: string) => FanoutShardUsage
  readonly fetchShard: (request: FanoutFetchRequest) => Promise<FanoutFetchResult<T>>
  readonly pageSize: number
  readonly state: FanoutContinuationState
}

const defaultClock: FanoutClock = Object.freeze({
  clearTimer: (handle: unknown) => clearTimeout(handle as ReturnType<typeof setTimeout>),
  nowMs: () => Date.now(),
  setTimer: (callback: () => void, delayMs: number) => setTimeout(callback, delayMs),
})

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function capacity(message: string): never {
  throw new NozzleError("CapacityGuardError", message)
}

function validUsage(usage: FanoutShardUsage, label: string): void {
  if (
    typeof usage !== "object" ||
    usage === null ||
    !Number.isSafeInteger(usage.costMicros) ||
    usage.costMicros < 0 ||
    !Number.isSafeInteger(usage.cpuMs) ||
    usage.cpuMs < 0 ||
    !Number.isSafeInteger(usage.subrequests) ||
    usage.subrequests < 1
  ) {
    configuration(`${label} is malformed.`)
  }
}

function validClock(clock: FanoutClock): void {
  if (
    typeof clock !== "object" ||
    clock === null ||
    typeof clock.clearTimer !== "function" ||
    typeof clock.nowMs !== "function" ||
    typeof clock.setTimer !== "function"
  ) {
    configuration("Fan-out clock is malformed.")
  }
}

function liveNow(clock: FanoutClock, state: FanoutContinuationState): number {
  const nowMs = clock.nowMs()
  if (!Number.isSafeInteger(nowMs) || nowMs < 0)
    configuration("Fan-out clock returned invalid time.")
  if (nowMs >= state.deadlineAtMs) capacity("Fan-out operation deadline has elapsed.")
  if (nowMs >= state.expiresAtMs) {
    throw new NozzleError("SessionTokenInvalidError", "Fan-out continuation has expired.")
  }
  return nowMs
}

function failurePage<T>(
  shardId: string,
  errorCode: string,
  usage: FanoutShardUsage,
): FanoutShardPage<T> {
  return Object.freeze({ errorCode, kind: "failure", shardId, usage: Object.freeze({ ...usage }) })
}

function classify(
  error: unknown,
  classifier: FanoutExecutorInput<unknown>["classifyFailure"],
): string {
  let result: unknown
  try {
    result = classifier?.(error)
  } catch {
    result = undefined
  }
  if (typeof result === "string") {
    const code = result.trim()
    if (/^[A-Za-z][A-Za-z0-9_.:-]{0,127}$/u.test(code)) return code
  }
  return error instanceof NozzleError ? error.code : "shard_error"
}

export async function executeFanoutPage<T>(
  input: FanoutExecutorInput<T>,
): Promise<FanoutPageResult<T>> {
  const state = loadFanoutContinuation(input.state)
  validateFanoutContinuationIdentity(state, input.current)
  if (
    !Number.isSafeInteger(input.pageSize) ||
    input.pageSize < 1 ||
    input.pageSize > MAX_FANOUT_PAGE_ROWS
  ) {
    configuration(`Fan-out page size must be an integer between 1 and ${MAX_FANOUT_PAGE_ROWS}.`)
  }
  const clock = input.clock === undefined ? defaultClock : input.clock
  validClock(clock)
  const startedAtMs = liveNow(clock, state)
  if (state.consumedPages >= state.budget.maxPages) {
    capacity("Fan-out page budget is exhausted.")
  }
  if (state.consumedRows >= state.budget.maxRows) {
    capacity("Fan-out row budget is exhausted.")
  }
  if (state.consumedBytes >= state.budget.maxBytes) {
    capacity("Fan-out response-byte budget is exhausted.")
  }
  const exhausted = new Set(state.exhaustedShardIds)
  const shardIds = state.shardIds.filter((shardId) => !exhausted.has(shardId))
  if (shardIds.length > state.budget.maxBufferedRows) {
    capacity("Fan-out buffer cannot hold one candidate row per active shard.")
  }
  if (shardIds.length > state.budget.maxBufferedBytes) {
    capacity("Fan-out buffer cannot reserve one byte per active shard.")
  }
  const rowLimit =
    shardIds.length === 0
      ? input.pageSize
      : Math.min(input.pageSize, Math.floor(state.budget.maxBufferedRows / shardIds.length))
  const maxBytes =
    shardIds.length === 0
      ? state.budget.maxBufferedBytes
      : Math.floor(state.budget.maxBufferedBytes / shardIds.length)

  const estimates = new Map<string, FanoutShardUsage>()
  let estimatedCostMicros = 0
  let estimatedCpuMs = 0
  let estimatedSubrequests = 0
  for (const shardId of shardIds) {
    const estimate = input.estimateUsage(shardId)
    validUsage(estimate, "Fan-out shard usage estimate")
    estimates.set(shardId, Object.freeze({ ...estimate }))
    estimatedCostMicros = Math.min(
      state.budget.maxCostMicros + 1,
      estimatedCostMicros + estimate.costMicros,
    )
    estimatedCpuMs = Math.min(state.budget.maxCpuMs + 1, estimatedCpuMs + estimate.cpuMs)
    estimatedSubrequests = Math.min(
      state.budget.maxSubrequests + 1,
      estimatedSubrequests + estimate.subrequests,
    )
  }
  if (state.consumedCostMicros + estimatedCostMicros > state.budget.maxCostMicros) {
    capacity("Fan-out estimated cost exceeds the remaining budget.")
  }
  if (state.consumedCpuMs + estimatedCpuMs > state.budget.maxCpuMs) {
    capacity("Fan-out estimated CPU exceeds the remaining budget.")
  }
  if (state.consumedSubrequests + estimatedSubrequests > state.budget.maxSubrequests) {
    capacity("Fan-out estimated subrequests exceed the remaining budget.")
  }

  const results = new Map<string, FanoutShardPage<T>>()
  const controllers = new Set<AbortController>()
  let nextIndex = 0
  let stop = false

  const runShard = async (shardId: string): Promise<FanoutShardPage<T>> => {
    const estimate = estimates.get(shardId) as FanoutShardUsage
    const nowMs = liveNow(clock, state)
    const timeoutMs = Math.min(state.budget.timeoutMs, state.deadlineAtMs - nowMs)
    const controller = new AbortController()
    controllers.add(controller)
    let abortCode = "cancelled"
    let timer: unknown
    const aborted = new Promise<FanoutShardPage<T>>((resolve) => {
      controller.signal.addEventListener(
        "abort",
        () => resolve(failurePage(shardId, abortCode, estimate)),
        { once: true },
      )
    })
    const fetchAttempt: Promise<FanoutFetchResult<T>> = Promise.resolve().then(() =>
      input.fetchShard({
        ...(state.bookmarks[shardId] === undefined ? {} : { bookmark: state.bookmarks[shardId] }),
        maxBytes,
        ...(state.positions[shardId] === undefined ? {} : { position: state.positions[shardId] }),
        rowLimit,
        shardId,
        signal: controller.signal,
      }),
    )
    const fetched: Promise<FanoutShardPage<T>> = fetchAttempt.then(
      (result) => {
        validUsage(result.usage, "Fan-out shard actual usage")
        return Object.freeze({
          ...(result.bookmark === undefined ? {} : { bookmark: result.bookmark }),
          exhausted: result.exhausted,
          kind: "success" as const,
          rows: result.rows,
          shardId,
          usage: Object.freeze({ ...result.usage }),
        })
      },
      (error: unknown) => failurePage(shardId, classify(error, input.classifyFailure), estimate),
    )
    timer = clock.setTimer(() => {
      abortCode = "timeout"
      controller.abort()
    }, timeoutMs)
    try {
      return await Promise.race([fetched, aborted])
    } finally {
      clock.clearTimer(timer)
      controllers.delete(controller)
    }
  }

  const worker = async (): Promise<void> => {
    while (!stop) {
      const index = nextIndex
      nextIndex += 1
      const shardId = shardIds[index]
      if (shardId === undefined) return
      const page = await runShard(shardId)
      results.set(shardId, page)
      if (page.kind === "failure" && state.partialPolicy === "fail") {
        stop = true
        for (const controller of controllers) controller.abort()
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(state.budget.maxConcurrency, shardIds.length) }, async () =>
      worker(),
    ),
  )
  for (const shardId of shardIds) {
    if (!results.has(shardId)) {
      results.set(
        shardId,
        failurePage(shardId, "cancelled_before_launch", {
          costMicros: 0,
          cpuMs: 0,
          subrequests: 0,
        }),
      )
    }
  }
  let actualCostMicros = 0
  let actualCpuMs = 0
  let actualSubrequests = 0
  for (const page of results.values()) {
    actualCostMicros = Math.min(
      state.budget.maxCostMicros + 1,
      actualCostMicros + page.usage.costMicros,
    )
    actualCpuMs = Math.min(state.budget.maxCpuMs + 1, actualCpuMs + page.usage.cpuMs)
    actualSubrequests = Math.min(
      state.budget.maxSubrequests + 1,
      actualSubrequests + page.usage.subrequests,
    )
  }
  if (state.consumedCostMicros + actualCostMicros > state.budget.maxCostMicros) {
    capacity("Fan-out actual cost exceeded its budget.")
  }
  if (state.consumedCpuMs + actualCpuMs > state.budget.maxCpuMs) {
    capacity("Fan-out actual CPU exceeded its budget.")
  }
  if (state.consumedSubrequests + actualSubrequests > state.budget.maxSubrequests) {
    capacity("Fan-out actual subrequests exceeded its budget.")
  }
  const completedAtMs = liveNow(clock, state)
  if (completedAtMs < startedAtMs) configuration("Fan-out clock moved backwards.")
  return mergeFanoutPage({
    current: input.current,
    nowMs: completedAtMs,
    pageSize: input.pageSize,
    pages: shardIds.map((shardId) => results.get(shardId) as FanoutShardPage<T>),
    state,
  })
}
