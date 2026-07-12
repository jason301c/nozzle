import { NozzleError } from "@nozzle/core"
import { classifyProviderAttempt } from "./provider.js"
import {
  type CloudflareRateLimitSnapshot,
  type ProviderErrorSummary,
  parseCloudflareRateLimit,
} from "./provider-http.js"
import {
  type ActiveWorkerDeploymentProof,
  createActiveWorkerDeploymentProof,
} from "./worker-deployment-proof.js"
import {
  createWorkerVersionArtifactProof,
  type WorkerVersionArtifactProof,
} from "./worker-version-proof.js"

export type { ActiveWorkerDeploymentProof } from "./worker-deployment-proof.js"
export type { WorkerVersionArtifactProof } from "./worker-version-proof.js"

const CLOUDFLARE_API_ORIGIN = "https://api.cloudflare.com/client/v4"
const DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024
const UUID = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/u
const CHECKSUM = /^[0-9a-f]{64}$/u
const UTC_DATE_TIME = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/u

export interface ActiveWorkerVersion {
  readonly versionId: string
  readonly weightBps: number
}

export interface ActiveWorkerDeployment {
  readonly createdAtMs: number
  readonly deploymentId: string
  readonly scriptName: string
  readonly versions: readonly ActiveWorkerVersion[]
}

export interface WorkerVersionArtifact {
  readonly artifactChecksum: string
  readonly scriptName: string
  readonly versionId: string
}

export interface WorkerDeploymentObservationEvidence {
  readonly bodyBytes: number
  readonly bodyState: "complete" | "not_received" | "too_large" | "unreadable"
  readonly cfRay?: string
  readonly completedAtMs: number
  readonly rateLimit: CloudflareRateLimitSnapshot
  readonly responseChecksum?: string
  readonly startedAtMs: number
  readonly status: number | null
  readonly transportErrorKind?: "aborted" | "network"
}

export type ActiveWorkerDeploymentObservation =
  | {
      readonly deployment: ActiveWorkerDeployment
      readonly evidence: WorkerDeploymentObservationEvidence
      readonly kind: "complete"
      readonly proof: ActiveWorkerDeploymentProof
    }
  | {
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: WorkerDeploymentObservationEvidence
      readonly kind: "inconclusive"
      readonly reason:
        | "malformed_response"
        | "missing_deployment"
        | "provider_rejected"
        | "retry_required"
        | "transport_error"
    }

export type WorkerVersionArtifactObservation =
  | {
      readonly artifact: WorkerVersionArtifact
      readonly evidence: WorkerDeploymentObservationEvidence
      readonly kind: "complete"
      readonly proof: WorkerVersionArtifactProof
    }
  | {
      readonly errors: readonly ProviderErrorSummary[]
      readonly evidence: WorkerDeploymentObservationEvidence
      readonly kind: "inconclusive"
      readonly reason:
        | "malformed_response"
        | "missing_artifact"
        | "provider_rejected"
        | "retry_required"
        | "transport_error"
    }

export interface CloudflareWorkerDeploymentClient {
  getActiveDeployment(
    scriptName: string,
    options?: { readonly signal?: AbortSignal },
  ): Promise<ActiveWorkerDeploymentObservation>
  getVersionArtifact(
    scriptName: string,
    versionId: string,
    options?: { readonly signal?: AbortSignal },
  ): Promise<WorkerVersionArtifactObservation>
}

export interface CloudflareWorkerDeploymentClientOptions {
  readonly accountId: string
  readonly apiToken: string
  readonly fetch?: typeof globalThis.fetch
  readonly maxResponseBytes?: number
  readonly now?: () => number
}

interface RawObservation {
  readonly body?: unknown
  readonly evidence: WorkerDeploymentObservationEvidence
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function nonEmpty(value: unknown, label: string, maximum?: number): asserts value is string {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    (maximum !== undefined && value.length > maximum)
  ) {
    configuration(`${label} is malformed.`)
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) configuration(`${label} is malformed.`)
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      configuration(`${label} is malformed.`)
    }
  }
}

function boundedInteger(
  value: unknown,
  label: string,
  minimum: number,
  maximum: number,
): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    configuration(`${label} is outside its supported range.`)
  }
}

function safeClock(now: () => number): number {
  const value = now()
  if (!Number.isSafeInteger(value) || value < 0) configuration("The provider clock is malformed.")
  return value
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
      Object.freeze({ code: candidate.code, message: safeProviderMessage(candidate.message) }),
    )
  }
  return Object.freeze(errors)
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

function leapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
}

function deploymentTime(value: unknown): number {
  if (typeof value !== "string") configuration("Worker deployment creation time is malformed.")
  const match = UTC_DATE_TIME.exec(value)
  if (match === null) configuration("Worker deployment creation time is malformed.")
  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const hour = Number(match[4])
  const minute = Number(match[5])
  const second = Number(match[6])
  const monthDays = [31, leapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
  if (
    year < 1970 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > (monthDays[month - 1] as number) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    configuration("Worker deployment creation time is malformed.")
  }
  const milliseconds = Number((match[7] ?? "").padEnd(3, "0").slice(0, 3))
  return Date.UTC(year, month - 1, day, hour, minute, second, milliseconds)
}

function uuid(value: unknown, label: string): string {
  if (typeof value !== "string" || !UUID.test(value)) configuration(`${label} is malformed.`)
  return value
}

function expectedArray(value: unknown, label: string): readonly unknown[] {
  if (!Array.isArray(value)) configuration(`${label} is malformed.`)
  return value
}

function deploymentPercentage(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    configuration("Worker deployment percentage is malformed.")
  }
  const scaled = value * 100
  const rounded = Math.round(scaled)
  if (
    value < 0.01 ||
    value > 100 ||
    Math.abs(scaled - rounded) > 1e-9 ||
    rounded < 1 ||
    rounded > 10_000
  ) {
    configuration("Worker deployment percentage cannot be represented in basis points.")
  }
  return rounded
}

function decodeActiveDeployment(value: unknown, scriptName: string): ActiveWorkerDeployment {
  if (!plainRecord(value) || value.strategy !== "percentage") {
    configuration("Active Worker deployment is malformed.")
  }
  const deploymentId = uuid(value.id, "Worker deployment ID")
  const createdAtMs = deploymentTime(value.created_on)
  const rawVersions = expectedArray(value.versions, "Active Worker deployment versions")
  if (rawVersions.length < 1 || rawVersions.length > 2) {
    configuration("An active Worker deployment must contain one or two versions.")
  }
  const versions: ActiveWorkerVersion[] = []
  const versionIds = new Set<string>()
  let totalWeight = 0
  for (const rawVersion of rawVersions) {
    if (!plainRecord(rawVersion)) configuration("Active Worker version is malformed.")
    const versionId = uuid(rawVersion.version_id, "Worker version ID")
    if (versionIds.has(versionId)) configuration("Active Worker versions must be unique.")
    versionIds.add(versionId)
    const weightBps = deploymentPercentage(rawVersion.percentage)
    totalWeight += weightBps
    versions.push(Object.freeze({ versionId, weightBps }))
  }
  if (totalWeight !== 10_000) {
    configuration("Active Worker deployment percentages must total 100 percent.")
  }
  versions.sort((left, right) => (left.versionId < right.versionId ? -1 : 1))
  return Object.freeze({
    createdAtMs,
    deploymentId,
    scriptName,
    versions: Object.freeze(versions),
  })
}

function decodeDeploymentEnvelope(
  value: unknown,
  scriptName: string,
): ActiveWorkerDeployment | undefined {
  if (!plainRecord(value) || value.success !== true || !plainRecord(value.result)) {
    configuration("Cloudflare returned a malformed deployment envelope.")
  }
  const deployments = expectedArray(value.result.deployments, "Worker deployment inventory")
  if (deployments.length === 0) return undefined
  return decodeActiveDeployment(deployments[0], scriptName)
}

function decodeVersionArtifactEnvelope(
  value: unknown,
  scriptName: string,
  expectedVersionId: string,
): WorkerVersionArtifact | undefined {
  if (!plainRecord(value) || value.success !== true || !plainRecord(value.result)) {
    configuration("Cloudflare returned a malformed Worker-version envelope.")
  }
  const versionId = uuid(value.result.id, "Worker version ID")
  if (versionId !== expectedVersionId) {
    configuration("Cloudflare returned the wrong Worker version.")
  }
  if (value.result.resources === undefined) return undefined
  if (!plainRecord(value.result.resources)) {
    configuration("Cloudflare returned malformed Worker-version resources.")
  }
  if (value.result.resources.script === undefined) return undefined
  if (!plainRecord(value.result.resources.script)) {
    configuration("Cloudflare returned malformed Worker-version script metadata.")
  }
  if (value.result.resources.script.etag === undefined) return undefined
  const artifactChecksum = value.result.resources.script.etag
  if (typeof artifactChecksum !== "string" || !CHECKSUM.test(artifactChecksum)) {
    configuration("Cloudflare returned a malformed Worker-version artifact checksum.")
  }
  return Object.freeze({ artifactChecksum, scriptName, versionId })
}

export function createCloudflareWorkerDeploymentClient(
  options: CloudflareWorkerDeploymentClientOptions,
): CloudflareWorkerDeploymentClient {
  if (!plainRecord(options)) configuration("Cloudflare Worker deployment options are required.")
  nonEmpty(options.accountId, "Cloudflare account ID")
  if (!/^[0-9a-fA-F]{32}$/u.test(options.accountId)) {
    configuration("Cloudflare account ID must contain 32 hexadecimal characters.")
  }
  nonEmpty(options.apiToken, "Cloudflare API token")
  const apiToken = options.apiToken
  const maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES
  boundedInteger(maxResponseBytes, "Worker deployment response byte limit", 1_024, 10 * 1024 * 1024)
  const fetchImplementation = options.fetch ?? globalThis.fetch
  if (typeof fetchImplementation !== "function")
    configuration("A Fetch implementation is required.")
  const now = options.now ?? Date.now
  if (typeof now !== "function") configuration("A provider clock is required.")
  const accountPath = `${CLOUDFLARE_API_ORIGIN}/accounts/${options.accountId}/workers/scripts`
  const accountId = options.accountId

  async function attempt(
    scriptName: string,
    suffix: string,
    signal: AbortSignal | undefined,
  ): Promise<RawObservation> {
    const startedAtMs = safeClock(now)
    let response: Response
    try {
      response = await fetchImplementation(
        `${accountPath}/${encodeURIComponent(scriptName)}/${suffix}`,
        {
          headers: new Headers({
            Accept: "application/json",
            Authorization: `Bearer ${apiToken}`,
          }),
          method: "GET",
          redirect: "error",
          ...(signal === undefined ? {} : { signal }),
        },
      )
    } catch (error) {
      const completedAtMs = safeClock(now)
      if (completedAtMs < startedAtMs) configuration("The provider clock moved backwards.")
      return {
        evidence: Object.freeze({
          bodyBytes: 0,
          bodyState: "not_received",
          completedAtMs,
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
    const completedAtMs = safeClock(now)
    if (completedAtMs < startedAtMs) configuration("The provider clock moved backwards.")
    if (bodyResult.kind !== "complete") {
      return {
        evidence: Object.freeze({
          bodyBytes: bodyResult.bodyBytes,
          bodyState: bodyResult.kind,
          ...(cfRay === null || cfRay.length === 0 ? {} : { cfRay }),
          completedAtMs,
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
        rateLimit,
        responseChecksum: await sha256(bodyResult.bytes),
        startedAtMs,
        status: response.status,
      }),
    }
  }

  async function getActiveDeployment(
    scriptName: string,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<ActiveWorkerDeploymentObservation> {
    nonEmpty(scriptName, "Cloudflare Worker script name", 255)
    if (!plainRecord(callOptions)) configuration("Worker deployment call options are malformed.")
    const result = await attempt(scriptName, "deployments", callOptions.signal)
    const retryAfterMs = result.evidence.rateLimit.retryAfterMs
    const decision = classifyProviderAttempt({
      mutating: false,
      ...(retryAfterMs === undefined ? {} : { retryAfter: String(retryAfterMs / 1_000) }),
      status: result.evidence.status,
    })
    if (decision.disposition !== "success") {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason:
          result.evidence.status === null
            ? "transport_error"
            : decision.disposition === "retry"
              ? "retry_required"
              : "provider_rejected",
      })
    }
    if (result.evidence.bodyState !== "complete" || result.body === undefined) {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
    try {
      const deployment = decodeDeploymentEnvelope(result.body, scriptName)
      if (deployment === undefined) {
        return Object.freeze({
          errors: freezeErrors(result.body),
          evidence: result.evidence,
          kind: "inconclusive",
          reason: "missing_deployment",
        })
      }
      return Object.freeze({
        deployment,
        evidence: result.evidence,
        kind: "complete",
        proof: createActiveWorkerDeploymentProof({
          accountId,
          deployment,
          evidence: result.evidence,
        }),
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

  async function getVersionArtifact(
    scriptName: string,
    versionId: string,
    callOptions: { readonly signal?: AbortSignal } = {},
  ): Promise<WorkerVersionArtifactObservation> {
    nonEmpty(scriptName, "Cloudflare Worker script name", 255)
    const normalizedVersionId = uuid(versionId, "Cloudflare Worker version ID")
    if (!plainRecord(callOptions)) configuration("Worker version call options are malformed.")
    const result = await attempt(
      scriptName,
      `versions/${encodeURIComponent(normalizedVersionId)}`,
      callOptions.signal,
    )
    const retryAfterMs = result.evidence.rateLimit.retryAfterMs
    const decision = classifyProviderAttempt({
      mutating: false,
      ...(retryAfterMs === undefined ? {} : { retryAfter: String(retryAfterMs / 1_000) }),
      status: result.evidence.status,
    })
    if (decision.disposition !== "success") {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason:
          result.evidence.status === null
            ? "transport_error"
            : decision.disposition === "retry"
              ? "retry_required"
              : "provider_rejected",
      })
    }
    if (result.evidence.bodyState !== "complete" || result.body === undefined) {
      return Object.freeze({
        errors: freezeErrors(result.body),
        evidence: result.evidence,
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
    try {
      const artifact = decodeVersionArtifactEnvelope(result.body, scriptName, normalizedVersionId)
      if (artifact === undefined) {
        return Object.freeze({
          errors: freezeErrors(result.body),
          evidence: result.evidence,
          kind: "inconclusive",
          reason: "missing_artifact",
        })
      }
      return Object.freeze({
        artifact,
        evidence: result.evidence,
        kind: "complete",
        proof: createWorkerVersionArtifactProof({ accountId, artifact, evidence: result.evidence }),
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

  return Object.freeze({ getActiveDeployment, getVersionArtifact })
}
