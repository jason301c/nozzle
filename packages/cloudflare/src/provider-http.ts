import { NozzleError } from "@nozzle/core"
import {
  type CompleteD1Inventory,
  classifyProviderAttempt,
  type D1DatabaseUpdate,
  type D1ListPage,
  type DesiredD1Database,
  decodeD1ListPage,
  decodeObservedD1Database,
  mergeD1ListPages,
  type ObservedD1Database,
  type ProviderAttemptDecision,
} from "./provider.js"

const CLOUDFLARE_API_ORIGIN = "https://api.cloudflare.com/client/v4"
const DEFAULT_D1_PAGE_SIZE = 1_000
const DEFAULT_MAX_INVENTORY_PAGES = 100
const DEFAULT_MAX_RESPONSE_BYTES = 10 * 1024 * 1024

export type D1ProviderEndpoint = "d1.create" | "d1.delete" | "d1.get" | "d1.list" | "d1.update"

export interface CloudflareRateLimitSnapshot {
  readonly quota?: number
  readonly remaining?: number
  readonly resetAfterMs?: number
  readonly retryAfterMs?: number
  readonly windowMs?: number
}

export interface ProviderAttemptEvidence {
  readonly bodyBytes: number
  readonly bodyState: "complete" | "not_received" | "too_large" | "unreadable"
  readonly cfRay?: string
  readonly completedAtMs: number
  readonly endpoint: D1ProviderEndpoint
  readonly rateLimit: CloudflareRateLimitSnapshot
  readonly responseChecksum?: string
  readonly startedAtMs: number
  readonly status: number | null
  readonly transportErrorKind?: "aborted" | "network"
}

export interface ProviderErrorSummary {
  readonly code: number
  readonly message: string
}

export type D1InventoryResult =
  | {
      readonly evidence: readonly ProviderAttemptEvidence[]
      readonly inventory: CompleteD1Inventory
      readonly kind: "complete"
    }
  | {
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: readonly ProviderAttemptEvidence[]
      readonly kind: "inconclusive"
      readonly reason:
        | "inconsistent_inventory"
        | "malformed_response"
        | "page_limit"
        | "provider_rejected"
        | "retry_required"
        | "transport_error"
    }

export type ProviderResourceObservation<T> =
  | { readonly evidence: ProviderAttemptEvidence; readonly kind: "absent" }
  | { readonly evidence: ProviderAttemptEvidence; readonly kind: "present"; readonly value: T }
  | {
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: ProviderAttemptEvidence
      readonly kind: "inconclusive"
      readonly reason:
        | "malformed_response"
        | "provider_rejected"
        | "retry_required"
        | "transport_error"
    }

type RejectedProviderDecision = Extract<
  ProviderAttemptDecision,
  { readonly disposition: "permanent_failure" | "retry" }
>

export type ProviderMutationResult<T> =
  | { readonly evidence: ProviderAttemptEvidence; readonly kind: "confirmed"; readonly value: T }
  | {
      readonly decision: RejectedProviderDecision
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: ProviderAttemptEvidence
      readonly kind: "rejected"
    }
  | {
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: ProviderAttemptEvidence
      readonly kind: "unknown"
      readonly reason: "ambiguous_status" | "malformed_response" | "transport_error"
    }

export interface CloudflareD1ProviderClient {
  createDatabase(
    desired: DesiredD1Database,
    options?: { readonly signal?: AbortSignal },
  ): Promise<ProviderMutationResult<ObservedD1Database>>
  deleteDatabase(
    databaseId: string,
    options?: { readonly signal?: AbortSignal },
  ): Promise<ProviderMutationResult<undefined>>
  getDatabase(
    databaseId: string,
    options?: { readonly signal?: AbortSignal },
  ): Promise<ProviderResourceObservation<ObservedD1Database>>
  listInventory(options?: { readonly signal?: AbortSignal }): Promise<D1InventoryResult>
  updateDatabase(
    databaseId: string,
    update: D1DatabaseUpdate,
    options?: { readonly signal?: AbortSignal },
  ): Promise<ProviderMutationResult<ObservedD1Database>>
}

export interface CloudflareD1ProviderClientOptions {
  readonly accountId: string
  readonly apiToken: string
  readonly fetch?: typeof globalThis.fetch
  readonly maxInventoryPages?: number
  readonly maxResponseBytes?: number
  readonly now?: () => number
  readonly perPage?: number
}

interface RawAttempt {
  readonly body?: unknown
  readonly evidence: ProviderAttemptEvidence
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function nonEmpty(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function boundedInteger(
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

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function isAbortError(value: unknown): boolean {
  return (
    (value instanceof DOMException && value.name === "AbortError") ||
    (plainRecord(value) && value.name === "AbortError")
  )
}

function safeProviderMessage(value: string): string {
  let output = ""
  for (const character of value) {
    const code = character.charCodeAt(0)
    output += code < 32 || code === 127 ? " " : character
    if (output.length >= 1_024) break
  }
  return output
}

function freezeErrors(value: unknown): readonly ProviderErrorSummary[] {
  if (!plainRecord(value) || !Array.isArray(value.errors)) return Object.freeze([])
  const errors: ProviderErrorSummary[] = []
  for (const candidate of value.errors) {
    if (
      !plainRecord(candidate) ||
      typeof candidate.code !== "number" ||
      !Number.isSafeInteger(candidate.code) ||
      typeof candidate.message !== "string" ||
      candidate.message.length === 0
    ) {
      continue
    }
    errors.push(
      Object.freeze({
        code: candidate.code,
        message: safeProviderMessage(candidate.message),
      }),
    )
  }
  return Object.freeze(errors)
}

function parseNonNegativeInteger(value: string): number | undefined {
  if (!/^[0-9]+$/u.test(value)) return undefined
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) ? parsed : undefined
}

function directiveValues(header: string | null | undefined, directive: string): number[] {
  if (header === null || header === undefined) return []
  const values: number[] = []
  const expression = new RegExp(`(?:^|[;,])\\s*${directive}=([0-9]+)(?=\\s*(?:[;,]|$))`, "giu")
  for (const match of header.matchAll(expression)) {
    const value = match[1] as string
    const parsed = parseNonNegativeInteger(value)
    if (parsed !== undefined) values.push(parsed)
  }
  return values
}

function minimum(values: readonly number[]): number | undefined {
  return values.length === 0 ? undefined : Math.min(...values)
}

function maximum(values: readonly number[]): number | undefined {
  return values.length === 0 ? undefined : Math.max(...values)
}

function secondsToMilliseconds(value: number | undefined): number | undefined {
  if (value === undefined || value > Math.floor(Number.MAX_SAFE_INTEGER / 1_000)) return undefined
  return value * 1_000
}

export function parseCloudflareRateLimit(input: {
  readonly rateLimit?: string | null
  readonly rateLimitPolicy?: string | null
  readonly retryAfter?: string | null
}): CloudflareRateLimitSnapshot {
  const remaining = minimum(directiveValues(input.rateLimit, "r"))
  const resetAfterMs = secondsToMilliseconds(maximum(directiveValues(input.rateLimit, "t")))
  const quota = minimum(directiveValues(input.rateLimitPolicy, "q"))
  const windowMs = secondsToMilliseconds(maximum(directiveValues(input.rateLimitPolicy, "w")))
  const retryAfterMs = secondsToMilliseconds(
    input.retryAfter === null || input.retryAfter === undefined
      ? undefined
      : parseNonNegativeInteger(input.retryAfter.trim()),
  )
  return Object.freeze({
    ...(quota === undefined ? {} : { quota }),
    ...(remaining === undefined ? {} : { remaining }),
    ...(resetAfterMs === undefined ? {} : { resetAfterMs }),
    ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
    ...(windowMs === undefined ? {} : { windowMs }),
  })
}

function bytesToHex(bytes: Uint8Array): string {
  let output = ""
  for (const byte of bytes) output += byte.toString(16).padStart(2, "0")
  return output
}

async function sha256(bytes: Uint8Array): Promise<string> {
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer)))
}

async function readBoundedBody(
  response: Response,
  maximumBytes: number,
): Promise<
  | { readonly bytes: Uint8Array; readonly kind: "complete"; readonly text: string }
  | { readonly bodyBytes: number; readonly kind: "too_large" | "unreadable" }
> {
  if (response.body === null) {
    return { bytes: new Uint8Array(), kind: "complete", text: "" }
  }
  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let bodyBytes = 0
  try {
    while (true) {
      const item = await reader.read()
      if (item.done) break
      bodyBytes += item.value.byteLength
      if (bodyBytes > maximumBytes) {
        await reader.cancel().catch(() => undefined)
        return { bodyBytes, kind: "too_large" }
      }
      chunks.push(item.value)
    }
  } catch {
    return { bodyBytes, kind: "unreadable" }
  }
  const bytes = new Uint8Array(bodyBytes)
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.byteLength
  }
  try {
    return {
      bytes,
      kind: "complete",
      text: new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    }
  } catch {
    return { bodyBytes, kind: "unreadable" }
  }
}

function decodeJson(text: string): unknown {
  if (text.length === 0) return undefined
  try {
    return JSON.parse(text) as unknown
  } catch {
    return undefined
  }
}

function envelopeResult(value: unknown): unknown {
  return plainRecord(value) && value.success === true ? value.result : undefined
}

function observationReason(
  decision: ProviderAttemptDecision,
): "provider_rejected" | "retry_required" {
  return decision.disposition === "retry" ? "retry_required" : "provider_rejected"
}

function classifyEvidence(
  evidence: ProviderAttemptEvidence,
  mutating: boolean,
): ProviderAttemptDecision {
  const retryAfterMs = evidence.rateLimit.retryAfterMs
  return classifyProviderAttempt({
    mutating,
    ...(retryAfterMs === undefined ? {} : { retryAfter: String(retryAfterMs / 1_000) }),
    status: evidence.status,
  })
}

function decodeDatabaseMutation(result: RawAttempt): ProviderMutationResult<ObservedD1Database> {
  const decision = classifyEvidence(result.evidence, true)
  if (decision.disposition === "unknown_outcome") {
    return Object.freeze({
      errors: freezeErrors(result.body),
      evidence: result.evidence,
      kind: "unknown",
      reason: result.evidence.status === null ? "transport_error" : "ambiguous_status",
    })
  }
  if (decision.disposition === "retry" || decision.disposition === "permanent_failure") {
    return Object.freeze({
      decision,
      errors: freezeErrors(result.body),
      evidence: result.evidence,
      kind: "rejected",
    })
  }
  const providerResult = envelopeResult(result.body)
  if (result.evidence.bodyState !== "complete" || providerResult === undefined) {
    return Object.freeze({
      errors: freezeErrors(result.body),
      evidence: result.evidence,
      kind: "unknown",
      reason: "malformed_response",
    })
  }
  try {
    return Object.freeze({
      evidence: result.evidence,
      kind: "confirmed",
      value: decodeObservedD1Database(providerResult),
    })
  } catch {
    return Object.freeze({
      errors: freezeErrors(result.body),
      evidence: result.evidence,
      kind: "unknown",
      reason: "malformed_response",
    })
  }
}

function databaseIdPath(databaseId: string): string {
  nonEmpty(databaseId, "Cloudflare D1 database ID")
  if (!/^[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}$/u.test(databaseId)) {
    return configuration("Cloudflare D1 database ID is malformed.")
  }
  return encodeURIComponent(databaseId)
}

function validateDesiredDatabase(desired: DesiredD1Database): void {
  nonEmpty(desired.name, "Desired D1 name")
  if (
    desired.jurisdiction !== undefined &&
    desired.jurisdiction !== "eu" &&
    desired.jurisdiction !== "fedramp"
  ) {
    configuration("Desired D1 jurisdiction is unsupported.")
  }
  if (
    desired.locationHint !== undefined &&
    !["apac", "eeur", "enam", "oc", "weur", "wnam"].includes(desired.locationHint)
  ) {
    configuration("Desired D1 location hint is unsupported.")
  }
  if (desired.jurisdiction !== undefined && desired.locationHint !== undefined) {
    configuration("D1 jurisdiction and location hint cannot both be supplied.")
  }
  if (
    desired.readReplication !== undefined &&
    (!plainRecord(desired.readReplication) ||
      (desired.readReplication.mode !== "auto" && desired.readReplication.mode !== "disabled"))
  ) {
    configuration("Desired D1 read replication is unsupported.")
  }
}

function validateDatabaseUpdate(update: D1DatabaseUpdate): void {
  if (
    !plainRecord(update) ||
    !plainRecord(update.readReplication) ||
    (update.readReplication.mode !== "auto" && update.readReplication.mode !== "disabled")
  ) {
    configuration("D1 read-replication update is unsupported.")
  }
}

export function createCloudflareD1ProviderClient(
  options: CloudflareD1ProviderClientOptions,
): CloudflareD1ProviderClient {
  if (!plainRecord(options)) configuration("Cloudflare provider options are required.")
  nonEmpty(options.accountId, "Cloudflare account ID")
  if (!/^[A-Fa-f0-9]{32}$/u.test(options.accountId)) {
    configuration("Cloudflare account ID must contain 32 hexadecimal characters.")
  }
  nonEmpty(options.apiToken, "Cloudflare API token")
  const apiToken = options.apiToken
  const perPage = options.perPage ?? DEFAULT_D1_PAGE_SIZE
  const maxInventoryPages = options.maxInventoryPages ?? DEFAULT_MAX_INVENTORY_PAGES
  const maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES
  boundedInteger(perPage, "D1 inventory page size", 10, 10_000)
  boundedInteger(maxInventoryPages, "D1 inventory page limit", 1, 10_000)
  boundedInteger(maxResponseBytes, "Provider response byte limit", 1_024, 32 * 1024 * 1024)
  const fetchImplementation = options.fetch ?? globalThis.fetch
  if (typeof fetchImplementation !== "function")
    configuration("A Fetch implementation is required.")
  const now = options.now ?? Date.now
  if (typeof now !== "function") configuration("A provider clock is required.")
  const accountPath = `${CLOUDFLARE_API_ORIGIN}/accounts/${options.accountId}/d1/database`

  async function attempt(
    endpoint: D1ProviderEndpoint,
    url: string,
    method: "DELETE" | "GET" | "PATCH" | "POST",
    body: string | undefined,
    signal: AbortSignal | undefined,
  ): Promise<RawAttempt> {
    const startedAtMs = now()
    const headers = new Headers({
      Accept: "application/json",
      Authorization: `Bearer ${apiToken}`,
    })
    if (body !== undefined) headers.set("Content-Type", "application/json")
    let response: Response
    try {
      response = await fetchImplementation(url, {
        ...(body === undefined ? {} : { body }),
        headers,
        method,
        redirect: "error",
        ...(signal === undefined ? {} : { signal }),
      })
    } catch (error) {
      return {
        evidence: Object.freeze({
          bodyBytes: 0,
          bodyState: "not_received",
          completedAtMs: now(),
          endpoint,
          rateLimit: Object.freeze({}),
          startedAtMs,
          status: null,
          transportErrorKind: isAbortError(error) ? "aborted" : "network",
        }),
      }
    }
    const rateLimit = parseCloudflareRateLimit({
      rateLimit: response.headers.get("Ratelimit"),
      rateLimitPolicy: response.headers.get("Ratelimit-Policy"),
      retryAfter: response.headers.get("retry-after"),
    })
    const cfRay = response.headers.get("cf-ray")
    const bodyResult = await readBoundedBody(response, maxResponseBytes)
    const completedAtMs = now()
    if (bodyResult.kind !== "complete") {
      return {
        evidence: Object.freeze({
          bodyBytes: bodyResult.bodyBytes,
          bodyState: bodyResult.kind,
          ...(cfRay === null || cfRay.length === 0 ? {} : { cfRay }),
          completedAtMs,
          endpoint,
          rateLimit,
          startedAtMs,
          status: response.status,
        }),
      }
    }
    return {
      body: decodeJson(bodyResult.text),
      evidence: Object.freeze({
        bodyBytes: bodyResult.bytes.byteLength,
        bodyState: "complete",
        ...(cfRay === null || cfRay.length === 0 ? {} : { cfRay }),
        completedAtMs,
        endpoint,
        rateLimit,
        responseChecksum: await sha256(bodyResult.bytes),
        startedAtMs,
        status: response.status,
      }),
    }
  }

  async function listInventory(
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<D1InventoryResult> {
    const evidence: ProviderAttemptEvidence[] = []
    const pages = []
    for (let page = 1; page <= maxInventoryPages; page += 1) {
      const url = new URL(accountPath)
      url.searchParams.set("page", String(page))
      url.searchParams.set("per_page", String(perPage))
      const result = await attempt("d1.list", url.href, "GET", undefined, callOptions.signal)
      evidence.push(result.evidence)
      const decision = classifyEvidence(result.evidence, false)
      if (decision.disposition !== "success") {
        return Object.freeze({
          errors: freezeErrors(result.body),
          evidence: Object.freeze(evidence),
          kind: "inconclusive",
          reason: result.evidence.status === null ? "transport_error" : observationReason(decision),
        })
      }
      if (result.evidence.bodyState !== "complete" || result.body === undefined) {
        return Object.freeze({
          errors: freezeErrors(result.body),
          evidence: Object.freeze(evidence),
          kind: "inconclusive",
          reason: "malformed_response",
        })
      }
      let decoded: D1ListPage
      try {
        decoded = decodeD1ListPage(result.body, { expectedPage: page, perPage })
      } catch {
        return Object.freeze({
          errors: freezeErrors(result.body),
          evidence: Object.freeze(evidence),
          kind: "inconclusive",
          reason: "malformed_response",
        })
      }
      pages.push(decoded)
      if (decoded.nextPage === undefined) {
        try {
          return Object.freeze({
            evidence: Object.freeze(evidence),
            inventory: mergeD1ListPages(pages),
            kind: "complete",
          })
        } catch {
          return Object.freeze({
            errors: Object.freeze([]),
            evidence: Object.freeze(evidence),
            kind: "inconclusive",
            reason: "inconsistent_inventory",
          })
        }
      }
    }
    return Object.freeze({
      errors: Object.freeze([]),
      evidence: Object.freeze(evidence),
      kind: "inconclusive",
      reason: "page_limit",
    })
  }

  async function getDatabase(
    databaseId: string,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<ProviderResourceObservation<ObservedD1Database>> {
    const result = await attempt(
      "d1.get",
      `${accountPath}/${databaseIdPath(databaseId)}`,
      "GET",
      undefined,
      callOptions.signal,
    )
    if (result.evidence.status === 404) {
      return Object.freeze({ evidence: result.evidence, kind: "absent" })
    }
    const decision = classifyEvidence(result.evidence, false)
    if (decision.disposition !== "success") {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason: result.evidence.status === null ? "transport_error" : observationReason(decision),
      })
    }
    const providerResult = envelopeResult(result.body)
    if (result.evidence.bodyState !== "complete" || providerResult === undefined) {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
    try {
      return Object.freeze({
        evidence: result.evidence,
        kind: "present",
        value: decodeObservedD1Database(providerResult),
      })
    } catch {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
  }

  async function createDatabase(
    desired: DesiredD1Database,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<ProviderMutationResult<ObservedD1Database>> {
    validateDesiredDatabase(desired)
    const body = JSON.stringify({
      ...(desired.jurisdiction === undefined ? {} : { jurisdiction: desired.jurisdiction }),
      name: desired.name,
      ...(desired.locationHint === undefined
        ? {}
        : { primary_location_hint: desired.locationHint }),
      ...(desired.readReplication === undefined
        ? {}
        : { read_replication: desired.readReplication }),
    })
    const result = await attempt("d1.create", accountPath, "POST", body, callOptions.signal)
    return decodeDatabaseMutation(result)
  }

  async function deleteDatabase(
    databaseId: string,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<ProviderMutationResult<undefined>> {
    const result = await attempt(
      "d1.delete",
      `${accountPath}/${databaseIdPath(databaseId)}`,
      "DELETE",
      undefined,
      callOptions.signal,
    )
    const decision = classifyEvidence(result.evidence, true)
    if (decision.disposition === "unknown_outcome") {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "unknown",
        reason: result.evidence.status === null ? "transport_error" : "ambiguous_status",
      })
    }
    if (decision.disposition === "retry" || decision.disposition === "permanent_failure") {
      return Object.freeze({
        decision,
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "rejected",
      })
    }
    if (result.evidence.bodyState !== "complete" || envelopeResult(result.body) === undefined) {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "unknown",
        reason: "malformed_response",
      })
    }
    return Object.freeze({ evidence: result.evidence, kind: "confirmed", value: undefined })
  }

  async function updateDatabase(
    databaseId: string,
    update: D1DatabaseUpdate,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<ProviderMutationResult<ObservedD1Database>> {
    validateDatabaseUpdate(update)
    const result = await attempt(
      "d1.update",
      `${accountPath}/${databaseIdPath(databaseId)}`,
      "PATCH",
      JSON.stringify({ read_replication: update.readReplication }),
      callOptions.signal,
    )
    return decodeDatabaseMutation(result)
  }

  return Object.freeze({
    createDatabase,
    deleteDatabase,
    getDatabase,
    listInventory,
    updateDatabase,
  })
}
