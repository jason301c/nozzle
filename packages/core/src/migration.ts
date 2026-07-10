import { NozzleError } from "./errors.js"

export type MigrationApplyState =
  | "applied"
  | "blocked_failed"
  | "pending"
  | "retryable_failed"
  | "running"
  | "unknown"

export type MigrationVerificationState = "failed" | "pending" | "unknown" | "verified"

export interface MigrationShardState {
  readonly apply: MigrationApplyState
  readonly canonicalSchemaChecksum?: string
  readonly ledgerChecksum?: string
  readonly verification: MigrationVerificationState
}

export interface MigrationHaltEvent {
  readonly controlSequence: number
  readonly failedShardId: string
  readonly fencingToken: number
}

export interface MigrationResumeAuthorization {
  readonly decisionChecksum: string
  readonly fencingToken: number
}

export interface MigrationOperation {
  readonly artifactChecksum: string
  readonly halt?: MigrationHaltEvent
  readonly operationId: string
  readonly requiredShardIds: readonly string[]
  readonly resume?: MigrationResumeAuthorization
  readonly shards: Readonly<Record<string, MigrationShardState>>
  readonly targetSchemaChecksum: string
}

export interface MigrationCompatibility {
  readonly activeApplicationSupportsTarget: boolean
  readonly activeRouterSupportsTarget: boolean
}

function nonEmpty(value: string, label: string): void {
  if (value.length === 0) {
    throw new NozzleError("ConfigurationError", `${label} must be non-empty.`)
  }
}

export function createMigrationOperation(input: {
  readonly artifactChecksum: string
  readonly operationId: string
  readonly requiredShardIds: readonly string[]
  readonly targetSchemaChecksum: string
}): MigrationOperation {
  nonEmpty(input.artifactChecksum, "Artifact checksum")
  nonEmpty(input.operationId, "Operation ID")
  nonEmpty(input.targetSchemaChecksum, "Target schema checksum")
  if (input.requiredShardIds.length === 0) {
    throw new NozzleError("ConfigurationError", "A migration requires at least one shard.")
  }
  const requiredShardIds = [...new Set(input.requiredShardIds)].sort()
  if (requiredShardIds.length !== input.requiredShardIds.length || requiredShardIds.includes("")) {
    throw new NozzleError("ConfigurationError", "Required shard IDs must be unique and non-empty.")
  }
  const shards: Record<string, MigrationShardState> = {}
  for (const shardId of requiredShardIds) {
    shards[shardId] = Object.freeze({ apply: "pending", verification: "pending" })
  }
  return Object.freeze({
    artifactChecksum: input.artifactChecksum,
    operationId: input.operationId,
    requiredShardIds: Object.freeze(requiredShardIds),
    shards: Object.freeze(shards),
    targetSchemaChecksum: input.targetSchemaChecksum,
  })
}

function updateShard(
  operation: MigrationOperation,
  shardId: string,
  update: (state: MigrationShardState) => MigrationShardState,
): MigrationOperation {
  const current = operation.shards[shardId]
  if (!current) {
    throw new NozzleError("MigrationFailedError", "Shard is not in the sealed migration set.", {
      details: { shardId },
    })
  }
  return Object.freeze({
    ...operation,
    shards: Object.freeze({ ...operation.shards, [shardId]: Object.freeze(update(current)) }),
  })
}

export function acceptMigrationShard(
  operation: MigrationOperation,
  shardId: string,
): MigrationOperation {
  if (operation.halt && !operation.resume) {
    throw new NozzleError(
      "MigrationFailedError",
      "No new shard work may start after the halt event.",
    )
  }
  return updateShard(operation, shardId, (state) => {
    if (state.apply !== "pending" && state.apply !== "retryable_failed") {
      throw new NozzleError("OperationResumeRequiredError", "Shard is not schedulable.")
    }
    return { apply: "running", verification: "pending" }
  })
}

export function authorizeMigrationResume(
  operation: MigrationOperation,
  input: { readonly decisionChecksum: string; readonly fencingToken: number },
): MigrationOperation {
  nonEmpty(input.decisionChecksum, "Resume decision checksum")
  if (!Number.isSafeInteger(input.fencingToken) || input.fencingToken < 1) {
    throw new NozzleError("ConfigurationError", "Resume fencing token must be a positive integer.")
  }
  if (!operation.halt) {
    throw new NozzleError(
      "OperationResumeRequiredError",
      "A migration without a halt cannot resume.",
    )
  }
  if (input.fencingToken <= operation.halt.fencingToken) {
    throw new NozzleError(
      "OperationResumeRequiredError",
      "Migration resume requires a newer controller fencing token.",
    )
  }
  if (operation.resume) {
    if (
      operation.resume.decisionChecksum === input.decisionChecksum &&
      operation.resume.fencingToken === input.fencingToken
    ) {
      return operation
    }
    throw new NozzleError(
      "OperationResumeRequiredError",
      "The active migration resume decision is immutable until another shard failure.",
    )
  }
  if (Object.values(operation.shards).some((state) => state.apply === "running")) {
    throw new NozzleError(
      "OperationResumeRequiredError",
      "Accepted migration work must settle before scheduling resumes.",
    )
  }
  return Object.freeze({ ...operation, resume: Object.freeze({ ...input }) })
}

export function recordMigrationApplied(
  operation: MigrationOperation,
  shardId: string,
  ledgerChecksum: string,
): MigrationOperation {
  nonEmpty(ledgerChecksum, "Ledger checksum")
  return updateShard(operation, shardId, (state) => {
    if (state.apply !== "running" && state.apply !== "unknown") {
      throw new NozzleError("OperationResumeRequiredError", "Shard was not running or unknown.")
    }
    return { apply: "applied", ledgerChecksum, verification: "pending" }
  })
}

export function recordMigrationVerified(
  operation: MigrationOperation,
  shardId: string,
  canonicalSchemaChecksum: string,
): MigrationOperation {
  nonEmpty(canonicalSchemaChecksum, "Canonical schema checksum")
  return updateShard(operation, shardId, (state) => {
    if (state.apply !== "applied" || state.ledgerChecksum !== operation.artifactChecksum) {
      throw new NozzleError("MigrationFailedError", "Shard application evidence is incomplete.")
    }
    if (canonicalSchemaChecksum !== operation.targetSchemaChecksum) {
      throw new NozzleError("SchemaDriftError", "Shard schema does not match the migration target.")
    }
    return { ...state, canonicalSchemaChecksum, verification: "verified" }
  })
}

export function recordMigrationFailure(
  operation: MigrationOperation,
  input: {
    readonly apply: "blocked_failed" | "retryable_failed" | "unknown"
    readonly controlSequence: number
    readonly fencingToken: number
    readonly shardId: string
    readonly verification?: "failed" | "unknown"
  },
): MigrationOperation {
  if (!Number.isSafeInteger(input.controlSequence) || input.controlSequence < 1) {
    throw new NozzleError("ConfigurationError", "Control sequence must be a positive integer.")
  }
  if (!Number.isSafeInteger(input.fencingToken) || input.fencingToken < 1) {
    throw new NozzleError("ConfigurationError", "Fencing token must be a positive integer.")
  }
  let next = updateShard(operation, input.shardId, (state) => ({
    ...state,
    apply: input.apply,
    verification: input.verification ?? (input.apply === "unknown" ? "unknown" : "failed"),
  }))
  if (next.resume) {
    const { resume: _resume, ...withoutResume } = next
    next = Object.freeze(withoutResume)
  }
  if (!next.halt) {
    next = Object.freeze({
      ...next,
      halt: Object.freeze({
        controlSequence: input.controlSequence,
        failedShardId: input.shardId,
        fencingToken: input.fencingToken,
      }),
    })
  }
  return next
}

export function migrationSucceeded(
  operation: MigrationOperation,
  compatibility: MigrationCompatibility,
): boolean {
  if (!compatibility.activeApplicationSupportsTarget || !compatibility.activeRouterSupportsTarget) {
    return false
  }
  return operation.requiredShardIds.every((shardId) => {
    const state = operation.shards[shardId]
    return (
      state?.apply === "applied" &&
      state.verification === "verified" &&
      state.ledgerChecksum === operation.artifactChecksum &&
      state.canonicalSchemaChecksum === operation.targetSchemaChecksum
    )
  })
}
