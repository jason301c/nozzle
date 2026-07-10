import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import { createMovementOperation, leaseProof } from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlDatabase,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1MovementStore, type MovementCommand } from "../src/movement-store.js"
import { controlSchemaSql } from "../src/schema.js"

class StatementAdapter implements ControlStatement {
  readonly #statement: StatementSync
  #values: Record<string, SQLInputValue> = {}

  constructor(statement: StatementSync) {
    this.#statement = statement
    this.#statement.setAllowBareNamedParameters(false)
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#values = Object.fromEntries(
      values.map((value, index) => [
        `?${index + 1}`,
        typeof value === "boolean"
          ? value
            ? 1
            : 0
          : value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : value,
      ]),
    )
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: this.#statement.all(this.#values) as T[], success: true }
  }

  async first<T>(): Promise<T | null> {
    return (this.#statement.get(this.#values) as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    const result = this.#statement.run(this.#values)
    return { meta: { changes: Number(result.changes) }, success: true }
  }
}

class DatabaseAdapter implements ControlDatabase {
  readonly database = new DatabaseSync(":memory:")

  constructor() {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(controlSchemaSql())
    this.database
      .prepare(
        `INSERT INTO "nozzle_fleets"
         ("fleet_id", "account_id_checksum", "environment_id", "bucket_bits", "hash_version",
          "fleet_seed", "state", "created_at_ms")
         VALUES ('fleet-a', 'account', 'production', 16, 1, ?, 'active', 1)`,
      )
      .run("a".repeat(43))
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

type FaultMode = "disappear" | "invalid_metadata" | "sequence" | "unchanged_one" | "unchanged_zero"

class FaultStatement implements ControlStatement {
  readonly #database: DatabaseSync
  readonly #delegate: ControlStatement
  readonly #mode: FaultMode
  readonly #sql: string

  constructor(database: DatabaseSync, delegate: ControlStatement, mode: FaultMode, sql: string) {
    this.#database = database
    this.#delegate = delegate
    this.#mode = mode
    this.#sql = sql
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#delegate.bind(...values)
    return this
  }

  all<T>(): Promise<ControlQueryResult<T>> {
    if (this.#mode === "sequence" && this.#sql.startsWith('UPDATE "nozzle_control_sequence"')) {
      return Promise.resolve({ meta: {}, results: [], success: true })
    }
    return this.#delegate.all<T>()
  }

  first<T>(): Promise<T | null> {
    return this.#delegate.first<T>()
  }

  async run(): Promise<ControlRunResult> {
    if (this.#sql.startsWith('UPDATE "nozzle_movement_operations"')) {
      if (this.#mode === "invalid_metadata") {
        return { meta: { changes: Number.NaN }, success: true }
      }
      if (this.#mode === "unchanged_one" || this.#mode === "unchanged_zero") {
        return { meta: { changes: this.#mode === "unchanged_one" ? 1 : 0 }, success: true }
      }
      if (this.#mode === "disappear") {
        const result = await this.#delegate.run()
        this.#database.exec(`DROP TRIGGER "nozzle_control_movement_delete";`)
        this.#database.exec(`DELETE FROM "nozzle_movement_operations";`)
        return result
      }
    }
    return this.#delegate.run()
  }
}

class FaultDatabase implements ControlDatabase {
  readonly #database: DatabaseAdapter
  readonly #mode: FaultMode

  constructor(database: DatabaseAdapter, mode: FaultMode) {
    this.#database = database
    this.#mode = mode
  }

  prepare(sql: string): ControlStatement {
    return new FaultStatement(this.#database.database, this.#database.prepare(sql), this.#mode, sql)
  }
}

function fresh(operationId = "movement-1") {
  return createMovementOperation({
    destinationShardId: "shard-b",
    operationId,
    partitionDigest: "digest",
    requiredTableIds: ["rows"],
    sourceRouteEpoch: 7,
    sourceShardId: "shard-a",
    targetRouteEpoch: 8,
  })
}

describe("D1MovementStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let movements: D1MovementStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    movements = new D1MovementStore(database)
    return () => database.close()
  })

  async function acquire(holder = "controller-a", acquisitionId = "acquisition-a") {
    const decision = await leases.acquire({
      acquisitionId,
      holderId: holder,
      leaseKey: "fleet-a:movement",
      ttlMs: 60_000,
    })
    if (!decision.acquired) throw new Error("fixture lease acquisition failed")
    return leaseProof(decision.record)
  }

  it("persists every cutover checkpoint under exact lease and state CAS", async () => {
    const proof = await acquire()
    let operation = await movements.create({ fleetId: "fleet-a", operation: fresh(), proof })
    const commands: MovementCommand[] = [
      { input: { schemaChecksum: "schema", startSequence: 10 }, kind: "start_capture" },
      { kind: "start_copy" },
      {
        input: {
          bytesCopied: 10,
          complete: true,
          expectedCursor: null,
          nextCursor: null,
          rowsCopied: 1,
          tableId: "rows",
        },
        kind: "record_copy_page",
      },
      { kind: "start_replay" },
      { input: { fromExclusive: 10, throughInclusive: 11 }, kind: "record_replay" },
      {
        input: { ownershipChecksum: "source-fence", sourceFenceEpoch: 8 },
        kind: "fence_source",
      },
      {
        input: {
          fromExclusive: 11,
          sourceReadOnlyVerified: true,
          tailEmptyVerified: true,
          throughInclusive: 12,
        },
        kind: "drain_tail",
      },
      {
        input: {
          destinationDigest: "rows",
          destinationFenceEpoch: 8,
          destinationRowCount: 1,
          sourceDigest: "rows",
          sourceRowCount: 1,
        },
        kind: "activate_destination",
      },
      { input: { routeChecksum: "route", routeEpoch: 8 }, kind: "publish_route" },
      {
        input: {
          destinationAccepts: true,
          directPathPassed: true,
          routerPathPassed: true,
          sessionTransitionPassed: true,
          sourceRejects: true,
        },
        kind: "verify_runtime",
      },
      { input: { serverTimeMs: 1, untilServerTimeMs: 2 }, kind: "start_quarantine" },
      {
        input: { authorizationChecksum: "cleanup", fencingToken: 1, serverTimeMs: 2 },
        kind: "authorize_cleanup",
      },
      {
        input: {
          captureJournalCompacted: true,
          destinationVerified: true,
          sourceApplicationRowsDeleted: true,
          sourcePartitionFenceRetained: true,
        },
        kind: "complete",
      },
    ]
    for (const command of commands)
      operation = await movements.apply(operation.operationId, command, proof)
    expect(operation.phase).toBe("completed")
    await expect(movements.load(operation.operationId)).resolves.toEqual(operation)
    await expect(
      movements.create({ fleetId: "fleet-a", operation: fresh(), proof }),
    ).resolves.toEqual(operation)
    expect(() =>
      database.database
        .prepare(`UPDATE "nozzle_movement_operations" SET "source_shard_id" = 'other'`)
        .run(),
    ).toThrow("NOZZLE_CONTROL_IMMUTABLE_MOVEMENT_PLAN")
    expect(() =>
      database.database.prepare(`DELETE FROM "nozzle_movement_operations"`).run(),
    ).toThrow("NOZZLE_CONTROL_MOVEMENT_PERSISTENT")
  })

  it("allocates one block sequence, replays idempotently, and fences stale recovery", async () => {
    const firstProof = await acquire()
    let operation = await movements.create({
      fleetId: "fleet-a",
      operation: fresh("movement-blocked"),
      proof: firstProof,
    })
    const block = {
      input: { errorChecksum: "failure", outcome: "unknown" as const },
      kind: "block" as const,
    }
    operation = await movements.apply(operation.operationId, block, firstProof)
    expect(operation.block).toMatchObject({ controlSequence: 1, fencingToken: 1 })
    await expect(movements.apply(operation.operationId, block, firstProof)).resolves.toEqual(
      operation,
    )
    expect(
      database.database.prepare(`SELECT "sequence" FROM "nozzle_control_sequence"`).get(),
    ).toEqual({ sequence: 1 })
    await expect(
      movements.apply(operation.operationId, { kind: "start_copy" }, firstProof),
    ).rejects.toThrow("requires a newer fenced recovery")

    await leases.release({ proof: firstProof })
    const secondProof = await acquire("controller-b", "acquisition-b")
    await expect(
      movements.apply(
        operation.operationId,
        { input: { decisionChecksum: "recovery", fencingToken: 2 }, kind: "authorize_recovery" },
        firstProof,
      ),
    ).rejects.toThrow("compare-and-swap")
    operation = await movements.apply(
      operation.operationId,
      { input: { decisionChecksum: "recovery", fencingToken: 2 }, kind: "authorize_recovery" },
      secondProof,
    )
    expect(operation.recovery?.fencingToken).toBe(2)
    await expect(
      movements.apply(
        operation.operationId,
        { input: { decisionChecksum: "recovery", fencingToken: 2 }, kind: "authorize_recovery" },
        secondProof,
      ),
    ).resolves.toEqual(operation)
  })

  it("persists the reversible rollback path before route publication", async () => {
    const proof = await acquire()
    let operation = await movements.create({
      fleetId: "fleet-a",
      operation: fresh("movement-rollback"),
      proof,
    })
    operation = await movements.apply(
      operation.operationId,
      {
        input: { destinationReadOnlyVerified: false, destinationWritesObserved: 0 },
        kind: "request_rollback",
      },
      proof,
    )
    operation = await movements.apply(
      operation.operationId,
      {
        input: {
          activeRouteEpoch: 7,
          captureDisabled: true,
          destinationQuarantined: true,
          sourceWritableVerified: true,
        },
        kind: "complete_rollback",
      },
      proof,
    )
    expect(operation.phase).toBe("rolled_back")
  })

  it("rejects malformed registration, missing operations, and incompatible replay", async () => {
    expect(() => new D1MovementStore(null as never)).toThrow("database binding")
    const proof = await acquire()
    await expect(movements.load("missing")).resolves.toBeUndefined()
    await expect(movements.load("")).rejects.toThrow("Operation ID")
    await expect(movements.apply("missing", { kind: "start_copy" }, proof)).rejects.toThrow(
      "does not exist",
    )
    await expect(movements.create({ fleetId: "", operation: fresh(), proof })).rejects.toThrow(
      "Fleet ID",
    )
    const created = await movements.create({ fleetId: "fleet-a", operation: fresh(), proof })
    const started = await movements.apply(
      created.operationId,
      { input: { schemaChecksum: "schema", startSequence: 0 }, kind: "start_capture" },
      proof,
    )
    await expect(
      movements.create({ fleetId: "fleet-a", operation: started, proof }),
    ).rejects.toThrow("fresh movement")
    await expect(
      movements.create({
        fleetId: "fleet-a",
        operation: createMovementOperation({
          destinationShardId: "shard-c",
          operationId: created.operationId,
          partitionDigest: "digest",
          requiredTableIds: ["rows"],
          sourceRouteEpoch: 7,
          sourceShardId: "shard-a",
          targetRouteEpoch: 8,
        }),
        proof,
      }),
    ).rejects.toThrow("incompatible movement plan")
    await expect(
      movements.create({ fleetId: "fleet-a", operation: fresh("other"), proof }),
    ).resolves.toMatchObject({ operationId: "other" })
  })

  it("fails closed on corrupt rows and impossible database outcomes", async () => {
    const proof = await acquire()
    const corrupt = await movements.create({
      fleetId: "fleet-a",
      operation: fresh("movement-corrupt"),
      proof,
    })
    database.database.exec("PRAGMA ignore_check_constraints = ON;")
    database.database
      .prepare(
        `UPDATE "nozzle_movement_operations" SET "state_json" = '{'
         WHERE "operation_id" = 'movement-corrupt'`,
      )
      .run()
    await expect(movements.load(corrupt.operationId)).rejects.toThrow("JSON is malformed")
    database.database
      .prepare(
        `UPDATE "nozzle_movement_operations" SET "state_json" = ?, "phase" = 'capturing'
         WHERE "operation_id" = 'movement-corrupt'`,
      )
      .run(JSON.stringify(corrupt))
    await expect(movements.load(corrupt.operationId)).rejects.toThrow("columns disagree")

    for (const mode of [
      "invalid_metadata",
      "unchanged_one",
      "unchanged_zero",
      "disappear",
      "sequence",
    ] as const) {
      const local = new DatabaseAdapter()
      try {
        const localLeases = new D1LeaseStore(local)
        const lease = await localLeases.acquire({
          acquisitionId: `acquisition-${mode}`,
          holderId: "controller",
          leaseKey: "fleet-a:movement",
          ttlMs: 60_000,
        })
        if (!lease.acquired) throw new Error("fixture")
        const localProof = leaseProof(lease.record)
        const normal = new D1MovementStore(local)
        const operation = await normal.create({
          fleetId: "fleet-a",
          operation: fresh(`movement-${mode}`),
          proof: localProof,
        })
        const faulty = new D1MovementStore(new FaultDatabase(local, mode))
        const action =
          mode === "sequence"
            ? faulty.apply(
                operation.operationId,
                { input: { errorChecksum: "failure", outcome: "unknown" }, kind: "block" },
                localProof,
              )
            : faulty.apply(
                operation.operationId,
                { input: { schemaChecksum: "schema", startSequence: 0 }, kind: "start_capture" },
                localProof,
              )
        await expect(action).rejects.toThrow()
      } finally {
        local.close()
      }
    }
  })

  it("rejects registration and block allocation after lease loss", async () => {
    const proof = await acquire()
    await leases.release({ proof })
    await expect(
      movements.create({ fleetId: "fleet-a", operation: fresh("expired-create"), proof }),
    ).rejects.toThrow("expired before registration")
    const nextProof = await acquire("controller-b", "acquisition-b")
    const operation = await movements.create({
      fleetId: "fleet-a",
      operation: fresh("stale-block"),
      proof: nextProof,
    })
    await expect(
      movements.apply(
        operation.operationId,
        { input: { errorChecksum: "failure", outcome: "unknown" }, kind: "block" },
        proof,
      ),
    ).rejects.toThrow("allocate a fenced control sequence")
  })
})
