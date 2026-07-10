import { NozzleError } from "./errors.js"

export const MOVEMENT_PHASES = [
  "planned",
  "capturing",
  "copying",
  "replaying",
  "source_read_only",
  "tail_drained",
  "destination_writable",
  "route_published",
  "verified",
  "quarantined",
  "cleanup_authorized",
  "completed",
  "rollback_pending",
  "rolled_back",
] as const

export type MovementPhase = (typeof MOVEMENT_PHASES)[number]

export interface MovementTableCopyState {
  readonly bytesCopied: number
  readonly complete: boolean
  readonly cursor: string | null
  readonly rowsCopied: number
}

export interface MovementBlock {
  readonly controlSequence: number
  readonly errorChecksum: string
  readonly fencingToken: number
  readonly outcome: "definitely_not_applied" | "permanent" | "unknown"
  readonly phase: MovementPhase
}

export interface MovementRecoveryAuthorization {
  readonly decisionChecksum: string
  readonly fencingToken: number
}

export interface MovementOperation {
  readonly block?: MovementBlock
  readonly captureSchemaChecksum?: string
  readonly captureStartSequence?: number
  readonly cleanupAuthorizationChecksum?: string
  readonly cleanupFencingToken?: number
  readonly copy: Readonly<Record<string, MovementTableCopyState>>
  readonly destinationDigest?: string
  readonly destinationRowCount?: number
  readonly destinationShardId: string
  readonly operationId: string
  readonly partitionDigest: string
  readonly phase: MovementPhase
  readonly publishedRouteChecksum?: string
  readonly quarantineUntilServerTimeMs?: number
  readonly recovery?: MovementRecoveryAuthorization
  readonly replayedThroughSequence?: number
  readonly requiredTableIds: readonly string[]
  readonly sourceRouteEpoch: number
  readonly sourceShardId: string
  readonly tailSequence?: number
  readonly targetRouteEpoch: number
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function verification(message: string): never {
  throw new NozzleError("MovementVerificationError", message)
}

function nonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function nonNegative(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    configuration(`${label} must be a non-negative safe integer.`)
  }
}

function positive(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    configuration(`${label} must be a positive safe integer.`)
  }
}

function add(left: number, right: number, label: string): number {
  const result = left + right
  if (!Number.isSafeInteger(result)) configuration(`${label} exceeds the safe integer range.`)
  return result
}

function update(
  operation: MovementOperation,
  patch: Partial<MovementOperation>,
): MovementOperation {
  return Object.freeze({ ...operation, ...patch })
}

function assertRunnable(operation: MovementOperation): void {
  if (operation.block && !operation.recovery) {
    resume("Movement is blocked and requires a newer fenced recovery decision.")
  }
}

function requirePhase(operation: MovementOperation, ...allowed: readonly MovementPhase[]): void {
  assertRunnable(operation)
  if (!allowed.includes(operation.phase)) {
    resume(`Movement phase ${operation.phase} cannot perform this transition.`)
  }
}

export function createMovementOperation(input: {
  readonly destinationShardId: string
  readonly operationId: string
  readonly partitionDigest: string
  readonly requiredTableIds: readonly string[]
  readonly sourceRouteEpoch: number
  readonly sourceShardId: string
  readonly targetRouteEpoch: number
}): MovementOperation {
  nonEmpty(input.operationId, "Operation ID")
  nonEmpty(input.partitionDigest, "Partition digest")
  nonEmpty(input.sourceShardId, "Source shard ID")
  nonEmpty(input.destinationShardId, "Destination shard ID")
  if (input.sourceShardId === input.destinationShardId) {
    configuration("Movement source and destination shards must differ.")
  }
  nonNegative(input.sourceRouteEpoch, "Source route epoch")
  positive(input.targetRouteEpoch, "Target route epoch")
  if (input.targetRouteEpoch !== input.sourceRouteEpoch + 1) {
    configuration("Target route epoch must immediately follow the source route epoch.")
  }
  if (input.requiredTableIds.length === 0) {
    configuration("Movement requires at least one table.")
  }
  const uniqueTables = new Set<string>()
  const copy: Record<string, MovementTableCopyState> = {}
  for (const tableId of input.requiredTableIds) {
    nonEmpty(tableId, "Table ID")
    if (uniqueTables.has(tableId)) configuration("Movement table IDs must be unique.")
    uniqueTables.add(tableId)
    copy[tableId] = Object.freeze({
      bytesCopied: 0,
      complete: false,
      cursor: null,
      rowsCopied: 0,
    })
  }
  return Object.freeze({
    copy: Object.freeze(copy),
    destinationShardId: input.destinationShardId,
    operationId: input.operationId,
    partitionDigest: input.partitionDigest,
    phase: "planned",
    requiredTableIds: Object.freeze([...input.requiredTableIds]),
    sourceRouteEpoch: input.sourceRouteEpoch,
    sourceShardId: input.sourceShardId,
    targetRouteEpoch: input.targetRouteEpoch,
  })
}

export function startMovementCapture(
  operation: MovementOperation,
  input: { readonly schemaChecksum: string; readonly startSequence: number },
): MovementOperation {
  requirePhase(operation, "planned")
  nonEmpty(input.schemaChecksum, "Capture schema checksum")
  nonNegative(input.startSequence, "Capture start sequence")
  return update(operation, {
    captureSchemaChecksum: input.schemaChecksum,
    captureStartSequence: input.startSequence,
    phase: "capturing",
    replayedThroughSequence: input.startSequence,
  })
}

export function startMovementCopy(operation: MovementOperation): MovementOperation {
  requirePhase(operation, "capturing")
  return update(operation, { phase: "copying" })
}

export function recordMovementCopyPage(
  operation: MovementOperation,
  input: {
    readonly bytesCopied: number
    readonly complete: boolean
    readonly expectedCursor: string | null
    readonly nextCursor: string | null
    readonly rowsCopied: number
    readonly tableId: string
  },
): MovementOperation {
  requirePhase(operation, "copying")
  nonNegative(input.rowsCopied, "Copied row count")
  nonNegative(input.bytesCopied, "Copied byte count")
  const firstIncomplete = operation.requiredTableIds.find(
    (tableId) => !operation.copy[tableId]?.complete,
  )
  if (firstIncomplete !== input.tableId) {
    resume("Movement tables must copy once in declared dependency order.")
  }
  const current = operation.copy[input.tableId]
  if (!current || current.cursor !== input.expectedCursor) {
    resume("Movement copy cursor compare-and-swap precondition failed.")
  }
  if (input.complete) {
    if (input.nextCursor !== null) configuration("A completed copy page cannot retain a cursor.")
  } else {
    if (
      input.rowsCopied < 1 ||
      input.nextCursor === null ||
      input.nextCursor.length === 0 ||
      input.nextCursor === input.expectedCursor
    ) {
      configuration("An incomplete copy page requires rows and a new non-empty cursor.")
    }
  }
  const nextTable = Object.freeze({
    bytesCopied: add(current.bytesCopied, input.bytesCopied, "Copied bytes"),
    complete: input.complete,
    cursor: input.nextCursor,
    rowsCopied: add(current.rowsCopied, input.rowsCopied, "Copied rows"),
  })
  return update(operation, {
    copy: Object.freeze({ ...operation.copy, [input.tableId]: nextTable }),
  })
}

export function startMovementReplay(operation: MovementOperation): MovementOperation {
  requirePhase(operation, "copying")
  if (operation.requiredTableIds.some((tableId) => !operation.copy[tableId]?.complete)) {
    resume("Base copy must complete before journal replay starts.")
  }
  return update(operation, { phase: "replaying" })
}

export function recordMovementReplay(
  operation: MovementOperation,
  input: { readonly fromExclusive: number; readonly throughInclusive: number },
): MovementOperation {
  requirePhase(operation, "replaying")
  nonNegative(input.fromExclusive, "Replay start sequence")
  nonNegative(input.throughInclusive, "Replay end sequence")
  if (input.fromExclusive !== operation.replayedThroughSequence) {
    resume("Journal replay watermark compare-and-swap precondition failed.")
  }
  if (input.throughInclusive < input.fromExclusive) {
    configuration("Journal replay cannot move its watermark backward.")
  }
  if (input.throughInclusive === input.fromExclusive) return operation
  return update(operation, { replayedThroughSequence: input.throughInclusive })
}

export function fenceMovementSource(
  operation: MovementOperation,
  input: { readonly ownershipChecksum: string; readonly sourceFenceEpoch: number },
): MovementOperation {
  requirePhase(operation, "replaying")
  nonEmpty(input.ownershipChecksum, "Source ownership checksum")
  if (input.sourceFenceEpoch !== operation.targetRouteEpoch) {
    verification("Source read-only fence does not match the target route epoch.")
  }
  return update(operation, { phase: "source_read_only" })
}

export function drainMovementTail(
  operation: MovementOperation,
  input: {
    readonly fromExclusive: number
    readonly sourceReadOnlyVerified: boolean
    readonly tailEmptyVerified: boolean
    readonly throughInclusive: number
  },
): MovementOperation {
  requirePhase(operation, "source_read_only")
  nonNegative(input.fromExclusive, "Tail start sequence")
  nonNegative(input.throughInclusive, "Tail end sequence")
  if (input.fromExclusive !== operation.replayedThroughSequence) {
    resume("Tail replay watermark compare-and-swap precondition failed.")
  }
  if (input.throughInclusive < input.fromExclusive) {
    configuration("Tail replay cannot move its watermark backward.")
  }
  if (!input.sourceReadOnlyVerified || !input.tailEmptyVerified) {
    verification("Source fencing and an empty mutation tail must both be verified.")
  }
  return update(operation, {
    phase: "tail_drained",
    replayedThroughSequence: input.throughInclusive,
    tailSequence: input.throughInclusive,
  })
}

export function activateMovementDestination(
  operation: MovementOperation,
  input: {
    readonly destinationDigest: string
    readonly destinationFenceEpoch: number
    readonly destinationRowCount: number
    readonly sourceDigest: string
    readonly sourceRowCount: number
  },
): MovementOperation {
  requirePhase(operation, "tail_drained")
  nonEmpty(input.sourceDigest, "Source verification digest")
  nonEmpty(input.destinationDigest, "Destination verification digest")
  nonNegative(input.sourceRowCount, "Source row count")
  nonNegative(input.destinationRowCount, "Destination row count")
  if (
    input.destinationFenceEpoch !== operation.targetRouteEpoch ||
    input.sourceDigest !== input.destinationDigest ||
    input.sourceRowCount !== input.destinationRowCount
  ) {
    verification("Destination activation evidence does not match the fenced source.")
  }
  return update(operation, {
    destinationDigest: input.destinationDigest,
    destinationRowCount: input.destinationRowCount,
    phase: "destination_writable",
  })
}

export function publishMovementRoute(
  operation: MovementOperation,
  input: { readonly routeChecksum: string; readonly routeEpoch: number },
): MovementOperation {
  requirePhase(operation, "destination_writable")
  nonEmpty(input.routeChecksum, "Route checksum")
  if (input.routeEpoch !== operation.targetRouteEpoch) {
    verification("Published route epoch does not match the movement target.")
  }
  return update(operation, {
    phase: "route_published",
    publishedRouteChecksum: input.routeChecksum,
  })
}

export function verifyMovementRuntime(
  operation: MovementOperation,
  evidence: {
    readonly destinationAccepts: boolean
    readonly directPathPassed: boolean
    readonly routerPathPassed: boolean
    readonly sessionTransitionPassed: boolean
    readonly sourceRejects: boolean
  },
): MovementOperation {
  requirePhase(operation, "route_published")
  if (
    !evidence.destinationAccepts ||
    !evidence.directPathPassed ||
    !evidence.routerPathPassed ||
    !evidence.sessionTransitionPassed ||
    !evidence.sourceRejects
  ) {
    verification("Public runtime movement verification is incomplete.")
  }
  return update(operation, { phase: "verified" })
}

export function startMovementQuarantine(
  operation: MovementOperation,
  input: { readonly serverTimeMs: number; readonly untilServerTimeMs: number },
): MovementOperation {
  requirePhase(operation, "verified")
  nonNegative(input.serverTimeMs, "Server time")
  nonNegative(input.untilServerTimeMs, "Quarantine deadline")
  if (input.untilServerTimeMs <= input.serverTimeMs) {
    configuration("Movement quarantine deadline must be in the future.")
  }
  return update(operation, {
    phase: "quarantined",
    quarantineUntilServerTimeMs: input.untilServerTimeMs,
  })
}

export function authorizeMovementCleanup(
  operation: MovementOperation,
  input: {
    readonly authorizationChecksum: string
    readonly fencingToken: number
    readonly serverTimeMs: number
  },
): MovementOperation {
  requirePhase(operation, "quarantined")
  nonEmpty(input.authorizationChecksum, "Cleanup authorization checksum")
  positive(input.fencingToken, "Cleanup fencing token")
  nonNegative(input.serverTimeMs, "Server time")
  if (
    !Number.isSafeInteger(operation.quarantineUntilServerTimeMs) ||
    (operation.quarantineUntilServerTimeMs as number) < 0
  ) {
    verification("Persisted movement quarantine state is malformed.")
  }
  if (input.serverTimeMs < (operation.quarantineUntilServerTimeMs as number)) {
    resume("Movement quarantine safety window has not elapsed.")
  }
  return update(operation, {
    cleanupAuthorizationChecksum: input.authorizationChecksum,
    cleanupFencingToken: input.fencingToken,
    phase: "cleanup_authorized",
  })
}

export function completeMovement(
  operation: MovementOperation,
  evidence: {
    readonly captureJournalCompacted: boolean
    readonly destinationVerified: boolean
    readonly sourceApplicationRowsDeleted: boolean
    readonly sourcePartitionFenceRetained: boolean
  },
): MovementOperation {
  requirePhase(operation, "cleanup_authorized")
  if (
    !evidence.captureJournalCompacted ||
    !evidence.destinationVerified ||
    !evidence.sourceApplicationRowsDeleted ||
    !evidence.sourcePartitionFenceRetained
  ) {
    verification("Movement cleanup evidence is incomplete.")
  }
  return update(operation, { phase: "completed" })
}

export function requestMovementRollback(
  operation: MovementOperation,
  evidence: {
    readonly destinationReadOnlyVerified: boolean
    readonly destinationWritesObserved: number
  },
): MovementOperation {
  requirePhase(
    operation,
    "planned",
    "capturing",
    "copying",
    "replaying",
    "source_read_only",
    "tail_drained",
    "destination_writable",
  )
  nonNegative(evidence.destinationWritesObserved, "Observed destination writes")
  if (
    operation.phase === "destination_writable" &&
    (!evidence.destinationReadOnlyVerified || evidence.destinationWritesObserved !== 0)
  ) {
    verification("Destination activation cannot roll back without proving zero writes and fencing.")
  }
  return update(operation, { phase: "rollback_pending" })
}

export function completeMovementRollback(
  operation: MovementOperation,
  evidence: {
    readonly activeRouteEpoch: number
    readonly captureDisabled: boolean
    readonly destinationQuarantined: boolean
    readonly sourceWritableVerified: boolean
  },
): MovementOperation {
  requirePhase(operation, "rollback_pending")
  nonNegative(evidence.activeRouteEpoch, "Active route epoch")
  if (
    evidence.activeRouteEpoch !== operation.sourceRouteEpoch ||
    !evidence.captureDisabled ||
    !evidence.destinationQuarantined ||
    !evidence.sourceWritableVerified
  ) {
    verification("Movement rollback evidence is incomplete.")
  }
  return update(operation, { phase: "rolled_back" })
}

export function blockMovement(
  operation: MovementOperation,
  input: {
    readonly controlSequence: number
    readonly errorChecksum: string
    readonly fencingToken: number
    readonly outcome: MovementBlock["outcome"]
  },
): MovementOperation {
  if (operation.phase === "completed" || operation.phase === "rolled_back") {
    resume("A terminal movement cannot be blocked.")
  }
  nonEmpty(input.errorChecksum, "Movement error checksum")
  positive(input.controlSequence, "Control sequence")
  positive(input.fencingToken, "Movement fencing token")
  if (
    operation.block?.controlSequence === input.controlSequence &&
    operation.block.errorChecksum === input.errorChecksum &&
    operation.block.fencingToken === input.fencingToken &&
    operation.block.outcome === input.outcome
  ) {
    return operation
  }
  if (operation.block && input.controlSequence <= operation.block.controlSequence) {
    resume("A replacement movement block requires a newer control sequence.")
  }
  const { recovery: _recovery, ...withoutRecovery } = operation
  return Object.freeze({
    ...withoutRecovery,
    block: Object.freeze({ ...input, phase: operation.phase }),
  })
}

export function authorizeMovementRecovery(
  operation: MovementOperation,
  input: { readonly decisionChecksum: string; readonly fencingToken: number },
): MovementOperation {
  nonEmpty(input.decisionChecksum, "Recovery decision checksum")
  positive(input.fencingToken, "Recovery fencing token")
  if (!operation.block) resume("A movement without a block cannot recover.")
  if (input.fencingToken <= operation.block.fencingToken) {
    resume("Movement recovery requires a newer controller fencing token.")
  }
  if (operation.recovery) {
    if (
      operation.recovery.decisionChecksum === input.decisionChecksum &&
      operation.recovery.fencingToken === input.fencingToken
    ) {
      return operation
    }
    resume("The active movement recovery decision is immutable until another failure.")
  }
  return update(operation, { recovery: Object.freeze({ ...input }) })
}
