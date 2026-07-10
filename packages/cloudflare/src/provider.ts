import { NozzleError } from "@nozzle/core"

export type D1Jurisdiction = "eu" | "fedramp"

export type D1LocationHint = "apac" | "eeur" | "enam" | "oc" | "weur" | "wnam"

export interface ObservedD1Database {
  readonly createdAt?: string
  readonly fileSize?: number
  readonly jurisdiction?: D1Jurisdiction
  readonly name: string
  readonly numTables?: number
  readonly uuid: string
  readonly version?: string
}

export interface D1ListPage {
  readonly databases: readonly ObservedD1Database[]
  readonly nextPage?: number
  readonly page: number
  readonly perPage: number
  readonly reportedTotalCount?: number
}

export interface CompleteD1Inventory {
  readonly complete: true
  readonly databases: readonly ObservedD1Database[]
  readonly pageCount: number
  readonly reportedTotalCount?: number
  readonly totalCount: number
}

export type ProviderAttemptDecision =
  | { readonly disposition: "success" }
  | { readonly disposition: "permanent_failure"; readonly status: number }
  | { readonly disposition: "retry"; readonly retryAfterMs: number; readonly status: number | null }
  | { readonly disposition: "unknown_outcome"; readonly status: number | null }

export interface DesiredD1Database {
  readonly jurisdiction?: D1Jurisdiction
  readonly locationHint?: D1LocationHint
  readonly name: string
}

export interface RecordedD1Database extends DesiredD1Database {
  readonly uuid: string
}

export type D1ReconciliationAction =
  | { readonly kind: "create"; readonly desired: DesiredD1Database }
  | {
      readonly candidate: ObservedD1Database
      readonly desired: DesiredD1Database
      readonly kind: "inspect_for_adoption"
    }
  | { readonly kind: "none"; readonly observed: ObservedD1Database }
  | {
      readonly kind: "quarantine_drift"
      readonly observed?: ObservedD1Database
      readonly reason:
        | "duplicate_name"
        | "immutable_jurisdiction_mismatch"
        | "recorded_identity_mismatch"
        | "recorded_resource_missing"
    }

const TRANSIENT_HTTP_STATUSES = new Set([408, 500, 502, 503, 504, 520, 521, 522, 523, 524])
const D1_LOCATION_HINTS = new Set<D1LocationHint>(["apac", "eeur", "enam", "oc", "weur", "wnam"])

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function providerError(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function configurationString(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function providerString(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    providerError(`${label} is malformed.`)
  }
}

function positiveInteger(value: unknown, label: string): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    providerError(`${label} is malformed.`)
  }
}

function optionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined
  providerString(value, label)
  return value
}

function optionalNonNegativeInteger(value: unknown, label: string): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    return providerError(`${label} is malformed.`)
  }
  return value
}

function validateObservedDatabase(value: unknown): asserts value is ObservedD1Database {
  if (!plainRecord(value)) return providerError("Observed D1 database is malformed.")
  providerString(value.name, "Observed D1 name")
  providerString(value.uuid, "Observed D1 UUID")
  if (
    value.jurisdiction !== undefined &&
    value.jurisdiction !== "eu" &&
    value.jurisdiction !== "fedramp"
  ) {
    return providerError("Observed D1 jurisdiction is unsupported.")
  }
  optionalString(value.createdAt, "Observed D1 creation time")
  optionalNonNegativeInteger(value.fileSize, "Observed D1 file size")
  optionalNonNegativeInteger(value.numTables, "Observed D1 table count")
  optionalString(value.version, "Observed D1 version")
}

export function decodeObservedD1Database(value: unknown): ObservedD1Database {
  if (!plainRecord(value)) return providerError("Cloudflare returned a malformed D1 database.")
  providerString(value.name, "Observed D1 name")
  providerString(value.uuid, "Observed D1 UUID")
  if (
    value.jurisdiction !== undefined &&
    value.jurisdiction !== "eu" &&
    value.jurisdiction !== "fedramp"
  ) {
    return providerError("Observed D1 jurisdiction is unsupported.")
  }
  const createdAt = optionalString(value.created_at, "Observed D1 creation time")
  const fileSize = optionalNonNegativeInteger(value.file_size, "Observed D1 file size")
  const numTables = optionalNonNegativeInteger(value.num_tables, "Observed D1 table count")
  const version = optionalString(value.version, "Observed D1 version")
  return Object.freeze({
    ...(createdAt === undefined ? {} : { createdAt }),
    ...(fileSize === undefined ? {} : { fileSize }),
    ...(value.jurisdiction === undefined
      ? {}
      : { jurisdiction: value.jurisdiction as D1Jurisdiction }),
    name: value.name,
    ...(numTables === undefined ? {} : { numTables }),
    uuid: value.uuid,
    ...(version === undefined ? {} : { version }),
  })
}

export function decodeD1ListPage(
  value: unknown,
  request: { readonly expectedPage: number; readonly perPage: number },
): D1ListPage {
  const { expectedPage, perPage } = request
  positiveInteger(expectedPage, "Expected D1 page")
  positiveInteger(perPage, "Requested D1 page size")
  if (perPage < 10 || perPage > 10_000) {
    return providerError("Requested D1 page size is unsupported.")
  }
  if (!plainRecord(value) || value.success !== true || !Array.isArray(value.result)) {
    return providerError("Cloudflare returned a malformed D1 list envelope.")
  }
  const databases = value.result.map(decodeObservedD1Database)
  if (databases.length > perPage) {
    return providerError("Cloudflare returned more D1 databases than the declared page size.")
  }
  let reportedTotalCount: number | undefined
  if (value.result_info !== undefined) {
    if (!plainRecord(value.result_info)) {
      return providerError("Cloudflare returned malformed D1 pagination metadata.")
    }
    if (value.result_info.page !== undefined && value.result_info.page !== expectedPage) {
      return providerError("Cloudflare returned an inconsistent D1 page number.")
    }
    if (value.result_info.per_page !== undefined && value.result_info.per_page !== perPage) {
      return providerError("Cloudflare returned an inconsistent D1 page size.")
    }
    if (value.result_info.count !== undefined && value.result_info.count !== databases.length) {
      return providerError("Cloudflare returned an inconsistent D1 page count.")
    }
    if (value.result_info.total_count !== undefined) {
      if (
        typeof value.result_info.total_count !== "number" ||
        !Number.isSafeInteger(value.result_info.total_count) ||
        value.result_info.total_count < 0
      ) {
        return providerError("Cloudflare returned a malformed D1 total count.")
      }
      reportedTotalCount = value.result_info.total_count
    }
  }
  const seen = new Set<string>()
  for (const database of databases) {
    if (seen.has(database.uuid))
      return providerError("Cloudflare repeated a D1 UUID within a page.")
    seen.add(database.uuid)
  }
  const nextPage = databases.length === perPage ? expectedPage + 1 : undefined
  return Object.freeze({
    databases: Object.freeze(databases),
    ...(nextPage === undefined ? {} : { nextPage }),
    page: expectedPage,
    perPage,
    ...(reportedTotalCount === undefined ? {} : { reportedTotalCount }),
  })
}

export function mergeD1ListPages(pages: readonly D1ListPage[]): CompleteD1Inventory {
  if (pages.length === 0) return providerError("D1 inventory observation has no pages.")
  const databases: ObservedD1Database[] = []
  const ids = new Set<string>()
  const first = pages[0] as D1ListPage
  positiveInteger(first.perPage, "Observed D1 page size")
  if (first.perPage < 10 || first.perPage > 10_000) {
    return providerError("Observed D1 page size is unsupported.")
  }
  const reportedTotalCount = first.reportedTotalCount
  if (
    reportedTotalCount !== undefined &&
    (!Number.isSafeInteger(reportedTotalCount) || reportedTotalCount < 0)
  ) {
    return providerError("Observed D1 total count is malformed.")
  }
  for (const [index, page] of pages.entries()) {
    const expectedPage = index + 1
    if (
      page.page !== expectedPage ||
      page.perPage !== first.perPage ||
      page.reportedTotalCount !== reportedTotalCount ||
      page.databases.length > page.perPage
    ) {
      return providerError("D1 inventory pages are inconsistent.")
    }
    const expectedNextPage = expectedPage < pages.length ? expectedPage + 1 : undefined
    if (page.nextPage !== expectedNextPage) {
      return providerError("D1 inventory pagination is incomplete or contains extra pages.")
    }
    for (const database of page.databases) {
      validateObservedDatabase(database)
      if (ids.has(database.uuid)) {
        return providerError("Cloudflare repeated a D1 UUID across inventory pages.")
      }
      ids.add(database.uuid)
      databases.push(database)
    }
  }
  if (reportedTotalCount !== undefined && databases.length !== reportedTotalCount) {
    return providerError("D1 inventory did not observe the declared total count.")
  }
  return Object.freeze({
    complete: true,
    databases: Object.freeze(databases),
    pageCount: pages.length,
    ...(reportedTotalCount === undefined ? {} : { reportedTotalCount }),
    totalCount: databases.length,
  })
}

function retryAfterMs(value: string | null | undefined): number {
  if (value === null || value === undefined || !/^[0-9]+$/u.test(value)) return 1_000
  const milliseconds = Number(value) * 1_000
  return Number.isSafeInteger(milliseconds) && milliseconds > 0 ? milliseconds : 1_000
}

export function computeProviderRetryDelay(input: {
  readonly attempt: number
  readonly baseDelayMs?: number
  readonly maximumDelayMs?: number
  readonly minimumDelayMs?: number
  readonly randomUnit: number
}): number {
  const baseDelayMs = input.baseDelayMs ?? 1_000
  const maximumDelayMs = input.maximumDelayMs ?? 30_000
  const minimumDelayMs = input.minimumDelayMs ?? 0
  if (!Number.isSafeInteger(input.attempt) || input.attempt < 0 || input.attempt > 31) {
    return configuration("Provider retry attempt must be an integer between zero and 31.")
  }
  if (!Number.isSafeInteger(baseDelayMs) || baseDelayMs < 1) {
    return configuration("Provider retry base delay must be a positive safe integer.")
  }
  if (!Number.isSafeInteger(maximumDelayMs) || maximumDelayMs < baseDelayMs) {
    return configuration("Provider retry maximum delay must be no smaller than the base delay.")
  }
  if (!Number.isSafeInteger(minimumDelayMs) || minimumDelayMs < 0) {
    return configuration("Provider retry minimum delay must be a non-negative safe integer.")
  }
  if (!Number.isFinite(input.randomUnit) || input.randomUnit < 0 || input.randomUnit >= 1) {
    return configuration("Provider retry jitter input must be in the range [0, 1).")
  }
  const exponential = Math.min(maximumDelayMs, baseDelayMs * 2 ** input.attempt)
  const jittered = Math.ceil(exponential * (0.5 + input.randomUnit))
  return Math.max(minimumDelayMs, jittered)
}

export function classifyProviderAttempt(input: {
  readonly mutating: boolean
  readonly retryAfter?: string | null
  readonly status: number | null
}): ProviderAttemptDecision {
  if (input.status === null) {
    return Object.freeze(
      input.mutating
        ? { disposition: "unknown_outcome", status: null }
        : { disposition: "retry", retryAfterMs: 1_000, status: null },
    )
  }
  if (!Number.isInteger(input.status) || input.status < 100 || input.status > 599) {
    configuration("Provider HTTP status is invalid.")
  }
  if (input.status >= 200 && input.status < 300) {
    return Object.freeze({ disposition: "success" })
  }
  if (input.status === 429) {
    return Object.freeze({
      disposition: "retry",
      retryAfterMs: retryAfterMs(input.retryAfter),
      status: input.status,
    })
  }
  if (TRANSIENT_HTTP_STATUSES.has(input.status)) {
    if (input.mutating) {
      return Object.freeze({ disposition: "unknown_outcome", status: input.status })
    }
    return Object.freeze({
      disposition: "retry",
      retryAfterMs: retryAfterMs(input.retryAfter),
      status: input.status,
    })
  }
  return Object.freeze({ disposition: "permanent_failure", status: input.status })
}

function validateDesired(desired: DesiredD1Database): void {
  configurationString(desired.name, "Desired D1 name")
  if (
    desired.jurisdiction !== undefined &&
    desired.jurisdiction !== "eu" &&
    desired.jurisdiction !== "fedramp"
  ) {
    configuration("Desired D1 jurisdiction is unsupported.")
  }
  if (desired.locationHint !== undefined && !D1_LOCATION_HINTS.has(desired.locationHint)) {
    configuration("Desired D1 location hint is unsupported.")
  }
  if (desired.jurisdiction !== undefined && desired.locationHint !== undefined) {
    configuration("D1 jurisdiction and location hint cannot both be authoritative.")
  }
}

function validateInventory(inventory: CompleteD1Inventory): void {
  if (
    !plainRecord(inventory) ||
    inventory.complete !== true ||
    !Array.isArray(inventory.databases) ||
    !Number.isSafeInteger(inventory.pageCount) ||
    inventory.pageCount < 1 ||
    !Number.isSafeInteger(inventory.totalCount) ||
    inventory.totalCount < 0 ||
    inventory.databases.length !== inventory.totalCount
  ) {
    providerError("D1 reconciliation requires a complete paginated observation.")
  }
  const ids = new Set<string>()
  for (const database of inventory.databases) {
    validateObservedDatabase(database)
    if (ids.has(database.uuid)) providerError("Observed D1 resources contain a duplicate UUID.")
    ids.add(database.uuid)
  }
}

export function planD1Reconciliation(input: {
  readonly desired: DesiredD1Database
  readonly inventory: CompleteD1Inventory
  readonly recorded?: RecordedD1Database
}): D1ReconciliationAction {
  validateDesired(input.desired)
  validateInventory(input.inventory)
  const byName = input.inventory.databases.filter(
    (database) => database.name === input.desired.name,
  )
  if (byName.length > 1) {
    return Object.freeze({ kind: "quarantine_drift", reason: "duplicate_name" })
  }
  const named = byName[0]
  if (input.recorded) {
    validateDesired(input.recorded)
    providerString(input.recorded.uuid, "Recorded D1 UUID")
    const identified = input.inventory.databases.find(
      (database) => database.uuid === input.recorded?.uuid,
    )
    if (!identified) {
      return Object.freeze({ kind: "quarantine_drift", reason: "recorded_resource_missing" })
    }
    if (
      input.recorded.name !== input.desired.name ||
      identified.name !== input.desired.name ||
      (named !== undefined && named.uuid !== identified.uuid)
    ) {
      return Object.freeze({
        kind: "quarantine_drift",
        observed: identified,
        reason: "recorded_identity_mismatch",
      })
    }
    if (
      input.recorded.jurisdiction !== input.desired.jurisdiction ||
      input.desired.jurisdiction !== identified.jurisdiction
    ) {
      return Object.freeze({
        kind: "quarantine_drift",
        observed: identified,
        reason: "immutable_jurisdiction_mismatch",
      })
    }
    return Object.freeze({ kind: "none", observed: identified })
  }
  if (!named) return Object.freeze({ desired: input.desired, kind: "create" })
  if (input.desired.jurisdiction !== named.jurisdiction) {
    return Object.freeze({
      kind: "quarantine_drift",
      observed: named,
      reason: "immutable_jurisdiction_mismatch",
    })
  }
  return Object.freeze({ candidate: named, desired: input.desired, kind: "inspect_for_adoption" })
}
