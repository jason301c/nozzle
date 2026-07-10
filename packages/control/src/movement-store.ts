import {
  activateMovementDestination,
  authorizeMovementCleanup,
  authorizeMovementRecovery,
  blockMovement,
  completeMovement,
  completeMovementRollback,
  createMovementOperation,
  drainMovementTail,
  fenceMovementSource,
  type LeaseProof,
  loadMovementOperation,
  type MovementOperation,
  NozzleError,
  publishMovementRoute,
  recordMovementCopyPage,
  recordMovementReplay,
  requestMovementRollback,
  startMovementCapture,
  startMovementCopy,
  startMovementQuarantine,
  startMovementReplay,
  verifyMovementRuntime,
} from "@nozzle/core"
import type { ControlDatabase, ControlRunResult } from "./database.js"

const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

export type MovementCommand =
  | {
      readonly input: Parameters<typeof activateMovementDestination>[1]
      readonly kind: "activate_destination"
    }
  | {
      readonly input: Parameters<typeof authorizeMovementCleanup>[1]
      readonly kind: "authorize_cleanup"
    }
  | {
      readonly input: Parameters<typeof authorizeMovementRecovery>[1]
      readonly kind: "authorize_recovery"
    }
  | {
      readonly input: Omit<Parameters<typeof blockMovement>[1], "controlSequence" | "fencingToken">
      readonly kind: "block"
    }
  | { readonly input: Parameters<typeof completeMovement>[1]; readonly kind: "complete" }
  | {
      readonly input: Parameters<typeof completeMovementRollback>[1]
      readonly kind: "complete_rollback"
    }
  | { readonly input: Parameters<typeof drainMovementTail>[1]; readonly kind: "drain_tail" }
  | { readonly input: Parameters<typeof fenceMovementSource>[1]; readonly kind: "fence_source" }
  | { readonly input: Parameters<typeof publishMovementRoute>[1]; readonly kind: "publish_route" }
  | {
      readonly input: Parameters<typeof recordMovementCopyPage>[1]
      readonly kind: "record_copy_page"
    }
  | { readonly input: Parameters<typeof recordMovementReplay>[1]; readonly kind: "record_replay" }
  | {
      readonly input: Parameters<typeof requestMovementRollback>[1]
      readonly kind: "request_rollback"
    }
  | { readonly input: Parameters<typeof startMovementCapture>[1]; readonly kind: "start_capture" }
  | { readonly kind: "start_copy" }
  | {
      readonly input: Parameters<typeof startMovementQuarantine>[1]
      readonly kind: "start_quarantine"
    }
  | { readonly kind: "start_replay" }
  | { readonly input: Parameters<typeof verifyMovementRuntime>[1]; readonly kind: "verify_runtime" }

interface MovementRow {
  readonly destination_shard_id: string
  readonly fleet_id: string
  readonly operation_id: string
  readonly partition_digest: string
  readonly phase: string
  readonly required_tables_json: string
  readonly source_route_epoch: number
  readonly source_shard_id: string
  readonly state_json: string
  readonly target_route_epoch: number
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function activeLeaseSql(offset: number): string {
  return `EXISTS (SELECT 1 FROM "nozzle_leases" WHERE "lease_key" = ?${offset} AND "holder_id" = ?${offset + 1} AND "acquisition_id" = ?${offset + 2} AND "fencing_token" = ?${offset + 3} AND "expires_at_ms" > ${SERVER_TIME_SQL})`
}

function proofValues(proof: LeaseProof): readonly [string, string, string, number] {
  return [proof.leaseKey, proof.holderId, proof.acquisitionId, proof.fencingToken]
}

function reportedChange(result: ControlRunResult): boolean {
  const changes = result.meta.changes
  if (!Number.isSafeInteger(changes) || (changes as number) < 0) {
    return intervention("Control D1 returned invalid movement mutation metadata.")
  }
  return changes === 1
}

function encode(operation: MovementOperation): string {
  return JSON.stringify(operation)
}

function samePlan(left: MovementOperation, right: MovementOperation): boolean {
  return (
    left.operationId === right.operationId &&
    left.partitionDigest === right.partitionDigest &&
    left.sourceShardId === right.sourceShardId &&
    left.destinationShardId === right.destinationShardId &&
    left.sourceRouteEpoch === right.sourceRouteEpoch &&
    left.targetRouteEpoch === right.targetRouteEpoch &&
    JSON.stringify(left.requiredTableIds) === JSON.stringify(right.requiredTableIds)
  )
}

function decode(row: MovementRow): MovementOperation {
  let value: unknown
  try {
    value = JSON.parse(row.state_json)
  } catch {
    return intervention("Persisted movement JSON is malformed.")
  }
  const operation = loadMovementOperation(value)
  if (
    operation.operationId !== row.operation_id ||
    operation.partitionDigest !== row.partition_digest ||
    operation.sourceShardId !== row.source_shard_id ||
    operation.destinationShardId !== row.destination_shard_id ||
    operation.sourceRouteEpoch !== row.source_route_epoch ||
    operation.targetRouteEpoch !== row.target_route_epoch ||
    operation.phase !== row.phase ||
    JSON.stringify(operation.requiredTableIds) !== row.required_tables_json
  ) {
    return intervention("Persisted movement columns disagree with the state record.")
  }
  return operation
}

function transition(
  operation: MovementOperation,
  command: MovementCommand,
  controlSequence: number,
  fencingToken: number,
): MovementOperation {
  switch (command.kind) {
    case "activate_destination":
      return activateMovementDestination(operation, command.input)
    case "authorize_cleanup":
      return authorizeMovementCleanup(operation, command.input)
    case "authorize_recovery":
      return authorizeMovementRecovery(operation, command.input)
    case "block":
      return blockMovement(operation, { ...command.input, controlSequence, fencingToken })
    case "complete":
      return completeMovement(operation, command.input)
    case "complete_rollback":
      return completeMovementRollback(operation, command.input)
    case "drain_tail":
      return drainMovementTail(operation, command.input)
    case "fence_source":
      return fenceMovementSource(operation, command.input)
    case "publish_route":
      return publishMovementRoute(operation, command.input)
    case "record_copy_page":
      return recordMovementCopyPage(operation, command.input)
    case "record_replay":
      return recordMovementReplay(operation, command.input)
    case "request_rollback":
      return requestMovementRollback(operation, command.input)
    case "start_capture":
      return startMovementCapture(operation, command.input)
    case "start_copy":
      return startMovementCopy(operation)
    case "start_quarantine":
      return startMovementQuarantine(operation, command.input)
    case "start_replay":
      return startMovementReplay(operation)
    case "verify_runtime":
      return verifyMovementRuntime(operation, command.input)
  }
}

export class D1MovementStore {
  readonly #database: ControlDatabase

  constructor(database: ControlDatabase) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function"
    ) {
      throw new NozzleError("ConfigurationError", "A control D1 database binding is required.")
    }
    this.#database = database
  }

  async create(input: {
    readonly fleetId: string
    readonly operation: MovementOperation
    readonly proof: LeaseProof
  }): Promise<MovementOperation> {
    if (typeof input.fleetId !== "string" || input.fleetId.trim().length === 0) {
      throw new NozzleError("ConfigurationError", "Fleet ID must be non-empty.")
    }
    const operation = loadMovementOperation(input.operation)
    const fresh = createMovementOperation(operation)
    if (encode(operation) !== encode(fresh)) {
      throw new NozzleError("ConfigurationError", "Only a fresh movement may be registered.")
    }
    await this.#database
      .prepare(
        `INSERT INTO "nozzle_movement_operations" ("operation_id", "fleet_id", "partition_digest", "source_shard_id", "destination_shard_id", "source_route_epoch", "target_route_epoch", "required_tables_json", "phase", "state_json", "created_at_ms", "updated_at_ms") SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'planned', ?9, ${SERVER_TIME_SQL}, ${SERVER_TIME_SQL} WHERE ${activeLeaseSql(10)} ON CONFLICT ("operation_id") DO NOTHING`,
      )
      .bind(
        operation.operationId,
        input.fleetId,
        operation.partitionDigest,
        operation.sourceShardId,
        operation.destinationShardId,
        operation.sourceRouteEpoch,
        operation.targetRouteEpoch,
        JSON.stringify(operation.requiredTableIds),
        encode(operation),
        ...proofValues(input.proof),
      )
      .run()
    const persisted = await this.load(operation.operationId)
    if (!persisted) return resume("The movement lease expired before registration completed.")
    if (!samePlan(persisted, operation)) {
      return resume("The operation ID is bound to an incompatible movement plan.")
    }
    return persisted
  }

  async #row(operationId: string): Promise<MovementRow | undefined> {
    const row = await this.#database
      .prepare(`SELECT * FROM "nozzle_movement_operations" WHERE "operation_id" = ?1`)
      .bind(operationId)
      .first<MovementRow>()
    return row ?? undefined
  }

  async load(operationId: string): Promise<MovementOperation | undefined> {
    if (typeof operationId !== "string" || operationId.trim().length === 0) {
      throw new NozzleError("ConfigurationError", "Operation ID must be non-empty.")
    }
    const row = await this.#row(operationId)
    return row ? decode(row) : undefined
  }

  async #sequence(proof: LeaseProof): Promise<number> {
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_control_sequence" SET "sequence" = "sequence" + 1 WHERE "singleton" = 1 AND ${activeLeaseSql(1)} RETURNING "sequence"`,
      )
      .bind(...proofValues(proof))
      .all<{ readonly sequence: number }>()
    const row = result.results[0]
    if (
      result.results.length !== 1 ||
      !row ||
      !Number.isSafeInteger(row.sequence) ||
      row.sequence < 1
    ) {
      return resume("Movement block could not allocate a fenced control sequence.")
    }
    return row.sequence
  }

  async apply(
    operationId: string,
    command: MovementCommand,
    proof: LeaseProof,
  ): Promise<MovementOperation> {
    const row = await this.#row(operationId)
    if (!row) return resume("The movement operation does not exist.")
    const current = decode(row)
    if (
      command.kind === "block" &&
      current.block?.errorChecksum === command.input.errorChecksum &&
      current.block.fencingToken === proof.fencingToken &&
      current.block.outcome === command.input.outcome &&
      current.block.phase === current.phase
    ) {
      return current
    }
    const next = transition(
      current,
      command,
      command.kind === "block" ? await this.#sequence(proof) : 0,
      command.kind === "block" ? proof.fencingToken : 0,
    )
    if (next === current) return current
    const nextJson = encode(loadMovementOperation(next))
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_movement_operations" SET "phase" = ?1, "state_json" = ?2, "updated_at_ms" = ${SERVER_TIME_SQL} WHERE "operation_id" = ?3 AND "state_json" = ?4 AND ${activeLeaseSql(5)}`,
      )
      .bind(next.phase, nextJson, operationId, row.state_json, ...proofValues(proof))
      .run()
    const changed = reportedChange(result)
    const persisted = await this.load(operationId)
    if (!persisted) return intervention("The movement disappeared after a durable transition.")
    if (encode(persisted) === nextJson) return persisted
    if (!changed)
      return resume("Movement transition lost its lease or compare-and-swap precondition.")
    return intervention("Control D1 did not persist the expected movement state.")
  }
}
