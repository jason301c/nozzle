import { NozzleError } from "@nozzle/core"
import type {
  SagaEffectHandler,
  SagaEffectHandlerResult,
  SagaHandlerRequest,
  SagaObservationHandler,
  SagaObservationHandlerRequest,
  SagaObservationHandlerResult,
} from "./saga-registry.js"

const MAX_JSON_BYTES = 1024 * 1024
const MAX_TIMEOUT_MS = 24 * 60 * 60 * 1_000
const TIMEOUT = Symbol("nozzle.saga-handler-timeout")

export type SagaEffectInvocationRequest = Omit<SagaHandlerRequest, "signal">
export type SagaObservationInvocationRequest = Omit<SagaObservationHandlerRequest, "signal">

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function exactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value)
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key))
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (!plainRecord(value)) return value
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) output[key] = canonicalValue(value[key])
  return output
}

function canonicalJson(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    return intervention(`${label} must be JSON text.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_JSON_BYTES) {
    return intervention(`${label} exceeds the one MiB durable limit.`)
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return intervention(`${label} is not valid JSON.`)
  }
  let json: string
  try {
    json = JSON.stringify(canonicalValue(parsed))
  } catch {
    return intervention(`${label} cannot be canonicalized safely.`)
  }
  return json
}

function validateTimeout(value: unknown): asserts value is number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > MAX_TIMEOUT_MS
  ) {
    configuration("Saga handler timeout is outside the supported range.")
  }
}

function localEvidence(reason: "exception" | "timeout"): string {
  return JSON.stringify({ kind: "local_handler_invocation", reason })
}

function unknownError(reason: "exception" | "timeout"): string {
  return JSON.stringify({ kind: "unknown_handler_outcome", reason })
}

function indeterminateError(reason: "exception" | "timeout"): string {
  return JSON.stringify({ kind: "indeterminate_handler_observation", reason })
}

function normalizeEffect(value: unknown): SagaEffectHandlerResult {
  if (!plainRecord(value) || typeof value.state !== "string") {
    return intervention("A saga effect handler returned a malformed result.")
  }
  if (value.state === "confirmed") {
    if (!exactKeys(value, ["evidenceJson", "outputJson", "state"])) {
      return intervention("A confirmed saga effect result has malformed fields.")
    }
    return Object.freeze({
      evidenceJson: canonicalJson(value.evidenceJson, "Saga effect evidence"),
      outputJson: canonicalJson(value.outputJson, "Saga effect output"),
      state: "confirmed",
    })
  }
  if (
    value.state !== "definitely_not_applied_retryable" &&
    value.state !== "definitely_not_applied_terminal" &&
    value.state !== "unknown"
  ) {
    return intervention("A saga effect handler returned an unsupported outcome.")
  }
  if (!exactKeys(value, ["errorJson", "evidenceJson", "state"])) {
    return intervention("A failed saga effect result has malformed fields.")
  }
  return Object.freeze({
    errorJson: canonicalJson(value.errorJson, "Saga effect error"),
    evidenceJson: canonicalJson(value.evidenceJson, "Saga effect evidence"),
    state: value.state,
  })
}

function normalizeObservation(value: unknown): SagaObservationHandlerResult {
  if (!plainRecord(value) || typeof value.state !== "string") {
    return intervention("A saga observation handler returned a malformed result.")
  }
  if (value.state === "applied") {
    if (!exactKeys(value, ["evidenceJson", "outputJson", "state"])) {
      return intervention("An applied saga observation result has malformed fields.")
    }
    return Object.freeze({
      evidenceJson: canonicalJson(value.evidenceJson, "Saga observation evidence"),
      outputJson: canonicalJson(value.outputJson, "Saga observation output"),
      state: "applied",
    })
  }
  if (value.state !== "indeterminate" && value.state !== "not_applied") {
    return intervention("A saga observation handler returned an unsupported outcome.")
  }
  if (!exactKeys(value, ["errorJson", "evidenceJson", "state"])) {
    return intervention("A non-applied saga observation result has malformed fields.")
  }
  return Object.freeze({
    errorJson: canonicalJson(value.errorJson, "Saga observation error"),
    evidenceJson: canonicalJson(value.evidenceJson, "Saga observation evidence"),
    state: value.state,
  })
}

async function invoke<T>(
  timeoutMs: number,
  handler: (signal: AbortSignal) => Promise<T> | T,
): Promise<{ readonly reason: "exception" | "timeout" } | { readonly value: T }> {
  validateTimeout(timeoutMs)
  const controller = new AbortController()
  let rejectTimeout!: (reason: typeof TIMEOUT) => void
  const timeout = new Promise<never>((_resolve, reject) => {
    rejectTimeout = reject
  })
  const timer = setTimeout(() => {
    controller.abort()
    rejectTimeout(TIMEOUT)
  }, timeoutMs)
  try {
    const value = await Promise.race([
      Promise.resolve().then(() => handler(controller.signal)),
      timeout,
    ])
    return Object.freeze({ value })
  } catch (error) {
    return Object.freeze({ reason: error === TIMEOUT ? "timeout" : "exception" })
  } finally {
    clearTimeout(timer)
  }
}

export async function invokeSagaEffectHandler(
  handler: SagaEffectHandler,
  request: SagaEffectInvocationRequest,
): Promise<SagaEffectHandlerResult> {
  if (typeof handler !== "function") configuration("A saga effect handler is required.")
  const invocation = await invoke(request.timeoutMs, (signal) => handler({ ...request, signal }))
  if ("value" in invocation) return normalizeEffect(invocation.value)
  return Object.freeze({
    errorJson: unknownError(invocation.reason),
    evidenceJson: localEvidence(invocation.reason),
    state: "unknown",
  })
}

export async function invokeSagaObservationHandler(
  handler: SagaObservationHandler,
  request: SagaObservationInvocationRequest,
): Promise<SagaObservationHandlerResult> {
  if (typeof handler !== "function") configuration("A saga observation handler is required.")
  const invocation = await invoke(request.timeoutMs, (signal) => handler({ ...request, signal }))
  if ("value" in invocation) return normalizeObservation(invocation.value)
  return Object.freeze({
    errorJson: indeterminateError(invocation.reason),
    evidenceJson: localEvidence(invocation.reason),
    state: "indeterminate",
  })
}
