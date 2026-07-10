import {
  NOZZLE_ERROR_CODES,
  NozzleError,
  type NozzleErrorCode,
  type SerializedNozzleError,
  serializeError,
} from "@nozzle/core"
import type { D1ResultLike, ExecutionPlan, ScopedPlanTransport } from "@nozzle/drizzle"
import {
  decodeWireD1Result,
  MAX_ROUTER_BATCH_STATEMENTS,
  ROUTER_PROTOCOL_VERSION,
  type WireD1Result,
} from "./wire.js"

export interface RouterExecuteRequest {
  readonly plan: ExecutionPlan
  readonly protocolVersion: typeof ROUTER_PROTOCOL_VERSION
}

export interface RouterBatchRequest {
  readonly plans: readonly ExecutionPlan[]
  readonly protocolVersion: typeof ROUTER_PROTOCOL_VERSION
}

export interface RouterSuccess<T> {
  readonly ok: true
  readonly protocolVersion: typeof ROUTER_PROTOCOL_VERSION
  readonly result: T
}

export interface RouterFailure {
  readonly error: SerializedNozzleError
  readonly ok: false
  readonly protocolVersion: typeof ROUTER_PROTOCOL_VERSION
}

export type RouterResponse<T> = RouterFailure | RouterSuccess<T>

export interface RouterServiceBinding {
  executeBatch(request: RouterBatchRequest): Promise<unknown>
  executePlan(request: RouterExecuteRequest): Promise<unknown>
}

function protocolError(message: string): never {
  throw new NozzleError("ShardUnavailableError", message)
}

function plainRecord(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return protocolError("The router returned a malformed response.")
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return protocolError("The router returned a non-plain response.")
  }
  return value as Readonly<Record<string, unknown>>
}

function exactKeys(record: Readonly<Record<string, unknown>>, keys: readonly string[]): void {
  const actual = Object.keys(record)
  if (actual.length !== keys.length || keys.some((key) => !Object.hasOwn(record, key))) {
    protocolError("The router response contains missing or unsupported fields.")
  }
}

function decodeRemoteError(value: unknown): NozzleError {
  const record = plainRecord(value)
  exactKeys(record, [
    "schemaVersion",
    "name",
    "code",
    "family",
    "message",
    "remediation",
    "retryable",
    "details",
  ])
  if (
    record.schemaVersion !== 1 ||
    record.name !== "NozzleError" ||
    typeof record.code !== "string" ||
    !NOZZLE_ERROR_CODES.includes(record.code as NozzleErrorCode) ||
    typeof record.message !== "string" ||
    record.message.length > 4_096 ||
    typeof record.remediation !== "string" ||
    record.remediation.length > 4_096 ||
    typeof record.retryable !== "boolean" ||
    typeof record.details !== "object" ||
    record.details === null ||
    Array.isArray(record.details)
  ) {
    return protocolError("The router returned a malformed error.")
  }
  const error = new NozzleError(record.code as NozzleErrorCode, record.message, {
    details: record.details as Readonly<Record<string, unknown>>,
    remediation: record.remediation,
    retryable: record.retryable,
  })
  if (record.family !== error.family) {
    return protocolError("The router returned an inconsistent error family.")
  }
  return error
}

function decodeEnvelope(value: unknown): readonly WireD1Result[] {
  const record = plainRecord(value)
  if (record.ok === true) {
    exactKeys(record, ["ok", "protocolVersion", "result"])
    if (record.protocolVersion !== ROUTER_PROTOCOL_VERSION || !Array.isArray(record.result)) {
      return protocolError("The router success response uses an unsupported protocol.")
    }
    if (record.result.length > MAX_ROUTER_BATCH_STATEMENTS) {
      throw new NozzleError("CapacityGuardError", "The router returned too many batch results.")
    }
    return record.result as readonly WireD1Result[]
  }
  if (record.ok === false) {
    exactKeys(record, ["error", "ok", "protocolVersion"])
    if (record.protocolVersion !== ROUTER_PROTOCOL_VERSION) {
      return protocolError("The router failure response uses an unsupported protocol.")
    }
    throw decodeRemoteError(record.error)
  }
  return protocolError("The router response does not declare success or failure.")
}

export function routerSuccess<T>(result: T): RouterSuccess<T> {
  return Object.freeze({ ok: true, protocolVersion: ROUTER_PROTOCOL_VERSION, result })
}

export function routerFailure(error: unknown): RouterFailure {
  return Object.freeze({
    error: serializeError(error),
    ok: false,
    protocolVersion: ROUTER_PROTOCOL_VERSION,
  })
}

export function createRouterTransport(service: RouterServiceBinding): ScopedPlanTransport {
  if (
    typeof service !== "object" ||
    service === null ||
    typeof service.executePlan !== "function" ||
    typeof service.executeBatch !== "function"
  ) {
    throw new NozzleError("ConfigurationError", "A router Service Binding is required.")
  }
  const execute = async (plan: ExecutionPlan): Promise<D1ResultLike> => {
    const response = await service.executePlan({ plan, protocolVersion: ROUTER_PROTOCOL_VERSION })
    const results = decodeEnvelope(response)
    if (results.length !== 1) return protocolError("The router returned an invalid result count.")
    return decodeWireD1Result(results[0])
  }
  return Object.freeze({
    async batch(plans: readonly ExecutionPlan[]): Promise<readonly D1ResultLike[]> {
      if (plans.length === 0 || plans.length > MAX_ROUTER_BATCH_STATEMENTS) {
        throw new NozzleError(
          "CapacityGuardError",
          "A router batch must contain between 1 and 49 plans.",
        )
      }
      const response = await service.executeBatch({
        plans,
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      })
      const results = decodeEnvelope(response)
      if (results.length !== plans.length) {
        return protocolError("The router returned an invalid batch result count.")
      }
      return Object.freeze(results.map(decodeWireD1Result))
    },
    execute,
  })
}
