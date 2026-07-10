export const NOZZLE_ERROR_CODES = [
  "ConfigurationError",
  "PartitionKeyMissingError",
  "PartitionKeyMismatchError",
  "TenantScopeRequiredError",
  "UnsafeQueryRequiredError",
  "ShardUnavailableError",
  "RouteVersionConflictError",
  "StaleRouteRejectedError",
  "SessionTokenInvalidError",
  "SchemaDriftError",
  "CapacityGuardError",
  "JurisdictionViolationError",
  "MigrationFailedError",
  "MovementVerificationError",
  "OperationResumeRequiredError",
  "OperationInterventionRequiredError",
  "CrossShardTransactionUnsupportedError",
  "ProviderRateLimitedError",
] as const

export type NozzleErrorCode = (typeof NOZZLE_ERROR_CODES)[number]

export type NozzleErrorFamily =
  | "capacity"
  | "configuration"
  | "jurisdiction"
  | "migration"
  | "movement"
  | "operation"
  | "partition"
  | "provider"
  | "route"
  | "schema"
  | "shard"
  | "transaction"
  | "unsafe-query"

interface ErrorDefinition {
  readonly family: NozzleErrorFamily
  readonly remediation: string
  readonly retryable: boolean
}

const ERROR_DEFINITIONS: Readonly<Record<NozzleErrorCode, ErrorDefinition>> = {
  CapacityGuardError: {
    family: "capacity",
    remediation: "Inspect the capacity plan and provide verified headroom before retrying.",
    retryable: false,
  },
  ConfigurationError: {
    family: "configuration",
    remediation: "Correct the reported configuration field and validate the configuration again.",
    retryable: false,
  },
  CrossShardTransactionUnsupportedError: {
    family: "transaction",
    remediation: "Use a single-shard batch or an explicit durable saga.",
    retryable: false,
  },
  JurisdictionViolationError: {
    family: "jurisdiction",
    remediation: "Choose a destination allowed by the fleet and tenant residency policies.",
    retryable: false,
  },
  MigrationFailedError: {
    family: "migration",
    remediation:
      "Inspect shard results, correct permanent failures, then resume the same operation.",
    retryable: false,
  },
  MovementVerificationError: {
    family: "movement",
    remediation:
      "Keep the source fenced, inspect verification evidence, and resume or recover forward.",
    retryable: false,
  },
  OperationInterventionRequiredError: {
    family: "operation",
    remediation:
      "Follow the operation recovery instructions and record an explicit operator decision.",
    retryable: false,
  },
  OperationResumeRequiredError: {
    family: "operation",
    remediation:
      "Resume using the existing operation ID instead of starting a conflicting operation.",
    retryable: true,
  },
  PartitionKeyMismatchError: {
    family: "partition",
    remediation: "Use the scoped partition value and the configured canonical partition-key type.",
    retryable: false,
  },
  PartitionKeyMissingError: {
    family: "partition",
    remediation: "Provide the configured partition key before building or executing the query.",
    retryable: false,
  },
  ProviderRateLimitedError: {
    family: "provider",
    remediation: "Honor the provider retry delay and resume the idempotent operation.",
    retryable: true,
  },
  RouteVersionConflictError: {
    family: "route",
    remediation: "Refresh the route manifest and retry only after confirming no mutation occurred.",
    retryable: true,
  },
  SchemaDriftError: {
    family: "schema",
    remediation:
      "Reconcile the reported semantic drift before running schema-sensitive operations.",
    retryable: false,
  },
  ShardUnavailableError: {
    family: "shard",
    remediation: "Inspect shard health and retry only within the operation's bounded retry policy.",
    retryable: true,
  },
  StaleRouteRejectedError: {
    family: "route",
    remediation: "Refresh the route manifest and safely retry the pre-mutation rejection.",
    retryable: true,
  },
  SessionTokenInvalidError: {
    family: "route",
    remediation: "Discard the token, resolve the current route, and establish a new session.",
    retryable: true,
  },
  TenantScopeRequiredError: {
    family: "partition",
    remediation: "Execute the query through nozzle.for(partitionKey).",
    retryable: false,
  },
  UnsafeQueryRequiredError: {
    family: "unsafe-query",
    remediation: "Use a supported scoped query or explicitly opt into the audited unsafe API.",
    retryable: false,
  },
}

export interface NozzleErrorOptions {
  readonly details?: Readonly<Record<string, unknown>>
  readonly remediation?: string
  readonly retryable?: boolean
}

export interface SerializedNozzleError {
  readonly schemaVersion: 1
  readonly name: "NozzleError"
  readonly code: NozzleErrorCode
  readonly family: NozzleErrorFamily
  readonly message: string
  readonly remediation: string
  readonly retryable: boolean
  readonly details: Readonly<Record<string, unknown>>
}

const SENSITIVE_KEY =
  /(?:authorization|cookie|credential|password|private|secret|session|token|api[-_]?key)/iu

function isPlainRecord(value: object): value is Record<string, unknown> {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

export function redact(value: unknown, depth = 0): unknown {
  if (depth > 8) return "[truncated]"
  if (value === null || typeof value === "boolean" || typeof value === "number") return value
  if (typeof value === "string") return value
  if (typeof value === "bigint") return value.toString(10)
  if (value instanceof Uint8Array) return `[bytes:${value.byteLength}]`
  if (Array.isArray(value)) return value.slice(0, 100).map((entry) => redact(entry, depth + 1))
  if (typeof value !== "object" || !isPlainRecord(value)) return "[unsupported]"

  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) {
    output[key] = SENSITIVE_KEY.test(key) ? "[redacted]" : redact(value[key], depth + 1)
  }
  return output
}

export class NozzleError extends Error {
  readonly code: NozzleErrorCode
  readonly details: Readonly<Record<string, unknown>>
  readonly family: NozzleErrorFamily
  readonly remediation: string
  readonly retryable: boolean

  constructor(code: NozzleErrorCode, message: string, options: NozzleErrorOptions = {}) {
    super(message)
    const definition = ERROR_DEFINITIONS[code]
    this.name = "NozzleError"
    this.code = code
    this.family = definition.family
    this.retryable = options.retryable ?? definition.retryable
    this.remediation = options.remediation ?? definition.remediation
    this.details = Object.freeze(
      (redact(options.details ?? {}) as Readonly<Record<string, unknown>>) ?? {},
    )
  }

  toJSON(): SerializedNozzleError {
    return {
      schemaVersion: 1,
      name: "NozzleError",
      code: this.code,
      family: this.family,
      message: this.message,
      remediation: this.remediation,
      retryable: this.retryable,
      details: this.details,
    }
  }
}

export function isNozzleError(value: unknown): value is NozzleError {
  return value instanceof NozzleError
}

export function serializeError(error: unknown): SerializedNozzleError {
  if (isNozzleError(error)) return error.toJSON()
  return new NozzleError(
    "OperationInterventionRequiredError",
    "Unexpected internal error.",
  ).toJSON()
}
