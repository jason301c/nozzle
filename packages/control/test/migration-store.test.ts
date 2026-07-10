import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import { createMigrationOperation, leaseProof } from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlDatabase,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1MigrationStore } from "../src/migration-store.js"
import { controlSchemaSql } from "../src/schema.js"

class StatementAdapter implements ControlStatement {
  readonly #statement: StatementSync
  #values: Record<string, SQLInputValue> = {}

  constructor(statement: StatementSync) {
    this.#statement = statement
    this.#statement.setAllowBareNamedParameters(false)
    this.#statement.setReadBigInts(false)
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#values = {}
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] as ControlBindingValue
      this.#values[`?${index + 1}`] =
        typeof value === "boolean"
          ? value
            ? 1
            : 0
          : value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : value
    }
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
         VALUES ('fleet-a', 'account-checksum', 'production', 16, 1, ?, 'active', 1)`,
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

interface ScriptedOperationRow {
  readonly artifact_checksum: string
  readonly fleet_id: string
  readonly halt_control_sequence: number | null
  readonly halt_failed_shard_id: string | null
  readonly halt_fencing_token: number | null
  readonly operation_id: string
  readonly required_shards_json: string
  readonly resume_decision_checksum: string | null
  readonly resume_fencing_token: number | null
  readonly state: string
  readonly target_schema_checksum: string
}

interface ScriptedShardRow {
  readonly apply_state: string
  readonly canonical_schema_checksum: string | null
  readonly ledger_checksum: string | null
  readonly shard_id: string
  readonly verification_state: string
}

class ScriptedStatement implements ControlStatement {
  readonly #database: ScriptedDatabase
  readonly #sql: string

  constructor(database: ScriptedDatabase, sql: string) {
    this.#database = database
    this.#sql = sql
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: this.#database.rows as T[], success: true }
  }

  async first<T>(): Promise<T | null> {
    if (!this.#sql.includes('FROM "nozzle_migration_operations"')) return null
    return (this.#database.operations.shift() as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    return { meta: { changes: this.#database.changes }, success: true }
  }
}

class ScriptedDatabase implements ControlDatabase {
  readonly operations: (ScriptedOperationRow | null)[]
  readonly rows: ScriptedShardRow[]
  readonly changes: unknown

  constructor(input: {
    readonly changes?: unknown
    readonly operations: readonly (ScriptedOperationRow | null)[]
    readonly rows?: readonly ScriptedShardRow[]
  }) {
    this.operations = [...input.operations]
    this.rows = [...(input.rows ?? [])]
    this.changes = input.changes ?? 1
  }

  prepare(sql: string): ControlStatement {
    return new ScriptedStatement(this, sql)
  }
}

function scriptedOperation(overrides: Partial<ScriptedOperationRow> = {}): ScriptedOperationRow {
  return {
    artifact_checksum: "artifact-target",
    fleet_id: "fleet-a",
    halt_control_sequence: null,
    halt_failed_shard_id: null,
    halt_fencing_token: null,
    operation_id: "migration-1",
    required_shards_json: '["shard-a"]',
    resume_decision_checksum: null,
    resume_fencing_token: null,
    state: "running",
    target_schema_checksum: "schema-target",
    ...overrides,
  }
}

function scriptedShard(overrides: Partial<ScriptedShardRow> = {}): ScriptedShardRow {
  return {
    apply_state: "pending",
    canonical_schema_checksum: null,
    ledger_checksum: null,
    shard_id: "shard-a",
    verification_state: "pending",
    ...overrides,
  }
}

function fresh() {
  return createMigrationOperation({
    artifactChecksum: "artifact-target",
    operationId: "migration-1",
    requiredShardIds: ["shard-c", "shard-a", "shard-b"],
    targetSchemaChecksum: "schema-target",
  })
}

describe("D1MigrationStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let migrations: D1MigrationStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    migrations = new D1MigrationStore(database)
    return () => database.close()
  })

  async function acquire(holder = "controller-a", acquisition = "acquisition-a") {
    const decision = await leases.acquire({
      acquisitionId: acquisition,
      holderId: holder,
      leaseKey: "fleet-a:migration",
      ttlMs: 60_000,
    })
    if (!decision.acquired) throw new Error("fixture lease acquisition failed")
    return leaseProof(decision.record)
  }

  it("persists partial success, halts once, resumes under a newer fence, and converges forward", async () => {
    const firstProof = await acquire()
    let operation = await migrations.create({
      fleetId: "fleet-a",
      operation: fresh(),
      proof: firstProof,
    })
    expect(operation.requiredShardIds).toEqual(["shard-a", "shard-b", "shard-c"])

    operation = await migrations.accept(operation.operationId, "shard-a", firstProof)
    operation = await migrations.applied(
      operation.operationId,
      "shard-a",
      "artifact-target",
      firstProof,
    )
    operation = await migrations.verified(
      operation.operationId,
      "shard-a",
      "schema-target",
      firstProof,
    )
    expect(operation.shards["shard-a"]?.verification).toBe("verified")

    const replayed = await migrations.create({
      fleetId: "fleet-a",
      operation: fresh(),
      proof: firstProof,
    })
    expect(replayed.shards["shard-a"]?.verification).toBe("verified")
    operation = await migrations.accept(operation.operationId, "shard-b", firstProof)
    operation = await migrations.failed(operation.operationId, {
      apply: "retryable_failed",
      errorChecksum: "error-b",
      proof: firstProof,
      shardId: "shard-b",
    })
    expect(operation.halt).toEqual({
      controlSequence: 1,
      failedShardId: "shard-b",
      fencingToken: 1,
    })
    await expect(migrations.accept(operation.operationId, "shard-c", firstProof)).rejects.toThrow(
      "No new shard work",
    )
    expect(
      database.database
        .prepare(`SELECT "state" FROM "nozzle_fleets" WHERE "fleet_id" = 'fleet-a'`)
        .get(),
    ).toEqual({ state: "mixed_blocked" })

    await expect(
      migrations.resume(operation.operationId, {
        decisionChecksum: "not-authorized",
        proof: { ...firstProof, fencingToken: 2 },
      }),
    ).rejects.toThrow("lost its lease")

    await leases.release({ proof: firstProof })
    const secondProof = await acquire("controller-b", "acquisition-b")
    operation = await migrations.resume(operation.operationId, {
      decisionChecksum: "forward-recovery-approved",
      proof: secondProof,
    })
    expect(operation.resume).toEqual({
      decisionChecksum: "forward-recovery-approved",
      fencingToken: 2,
    })
    await expect(
      migrations.resume(operation.operationId, {
        decisionChecksum: "forward-recovery-approved",
        proof: secondProof,
      }),
    ).resolves.toEqual(operation)
    for (const shardId of ["shard-b", "shard-c"]) {
      operation = await migrations.accept(operation.operationId, shardId, secondProof)
      operation = await migrations.applied(
        operation.operationId,
        shardId,
        "artifact-target",
        secondProof,
      )
      operation = await migrations.verified(
        operation.operationId,
        shardId,
        "schema-target",
        secondProof,
      )
    }
    await expect(
      migrations.activate(
        operation.operationId,
        { activeApplicationSupportsTarget: false, activeRouterSupportsTarget: true },
        secondProof,
      ),
    ).resolves.toBe(false)
    await expect(
      migrations.activate(
        operation.operationId,
        { activeApplicationSupportsTarget: true, activeRouterSupportsTarget: true },
        secondProof,
      ),
    ).resolves.toBe(true)
    expect(
      database.database
        .prepare(`SELECT "state" FROM "nozzle_migration_operations" WHERE "operation_id" = ?`)
        .get(operation.operationId),
    ).toEqual({ state: "succeeded" })
    expect(
      database.database
        .prepare(`SELECT "state" FROM "nozzle_fleets" WHERE "fleet_id" = 'fleet-a'`)
        .get(),
    ).toEqual({ state: "active" })
    expect(
      database.database
        .prepare(
          `SELECT "attempts" FROM "nozzle_migrations"
           WHERE "operation_id" = ? AND "shard_id" = 'shard-a'`,
        )
        .get(operation.operationId),
    ).toEqual({ attempts: 1 })
  })

  it("lets accepted work settle after a halt and reconciles an unknown outcome", async () => {
    const proof = await acquire()
    let operation = await migrations.create({ fleetId: "fleet-a", operation: fresh(), proof })
    operation = await migrations.accept(operation.operationId, "shard-a", proof)
    operation = await migrations.accept(operation.operationId, "shard-b", proof)
    operation = await migrations.failed(operation.operationId, {
      apply: "unknown",
      errorChecksum: "unknown-a",
      proof,
      shardId: "shard-a",
      verification: "unknown",
    })
    operation = await migrations.applied(operation.operationId, "shard-a", "artifact-target", proof)
    operation = await migrations.verified(operation.operationId, "shard-a", "schema-target", proof)
    operation = await migrations.applied(operation.operationId, "shard-b", "artifact-target", proof)
    expect(operation.shards["shard-a"]?.verification).toBe("verified")
    expect(operation.shards["shard-b"]?.apply).toBe("applied")
    expect(operation.halt?.controlSequence).toBe(1)
  })

  it("rejects stale controllers, incompatible replay, and premature activation", async () => {
    const firstProof = await acquire()
    const operation = await migrations.create({
      fleetId: "fleet-a",
      operation: fresh(),
      proof: firstProof,
    })
    await leases.release({ proof: firstProof })
    await acquire("controller-b", "acquisition-b")
    await expect(
      migrations.accept(operation.operationId, "shard-a", firstProof),
    ).rejects.toMatchObject({
      code: "OperationResumeRequiredError",
    })
    await expect(
      migrations.create({
        fleetId: "fleet-a",
        operation: createMigrationOperation({
          artifactChecksum: "different",
          operationId: operation.operationId,
          requiredShardIds: operation.requiredShardIds,
          targetSchemaChecksum: operation.targetSchemaChecksum,
        }),
        proof: firstProof,
      }),
    ).rejects.toThrow("incompatible migration plan")
    await expect(
      migrations.activate(
        operation.operationId,
        { activeApplicationSupportsTarget: true, activeRouterSupportsTarget: true },
        firstProof,
      ),
    ).resolves.toBe(false)
  })

  it("validates constructor, registration identity, and missing operations", async () => {
    expect(() => new D1MigrationStore(null as never)).toThrow("database binding")
    const proof = await acquire()
    await expect(migrations.load("missing")).resolves.toBeUndefined()
    await expect(
      migrations.resume("missing", { decisionChecksum: "decision", proof }),
    ).rejects.toThrow("does not exist")
    await expect(
      migrations.create({ fleetId: "", operation: fresh(), proof }),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    const notFresh = await migrations.create({ fleetId: "fleet-a", operation: fresh(), proof })
    const running = await migrations.accept(notFresh.operationId, "shard-a", proof)
    await expect(
      migrations.create({ fleetId: "fleet-a", operation: running, proof }),
    ).rejects.toThrow("Only a fresh migration")
    await expect(migrations.accept("missing", "shard-a", proof)).rejects.toThrow("does not exist")
  })

  it("fails closed when persisted migration rows are malformed", async () => {
    const malformed: readonly {
      readonly operation: ScriptedOperationRow
      readonly rows?: readonly ScriptedShardRow[]
    }[] = [
      { operation: scriptedOperation({ required_shards_json: "{" }) },
      { operation: scriptedOperation({ required_shards_json: "null" }) },
      { operation: scriptedOperation({ required_shards_json: "[]" }) },
      { operation: scriptedOperation({ required_shards_json: '[""]' }) },
      { operation: scriptedOperation({ required_shards_json: '["a","a"]' }) },
      { operation: scriptedOperation({ required_shards_json: '["b","a"]' }) },
      { operation: scriptedOperation(), rows: [] },
      { operation: scriptedOperation(), rows: [scriptedShard({ shard_id: "other" })] },
      { operation: scriptedOperation(), rows: [scriptedShard({ apply_state: "bad" })] },
      { operation: scriptedOperation(), rows: [scriptedShard({ verification_state: "bad" })] },
      {
        operation: scriptedOperation({ halt_control_sequence: 1 }),
        rows: [scriptedShard()],
      },
      {
        operation: scriptedOperation({
          halt_control_sequence: 1,
          halt_failed_shard_id: "shard-a",
          halt_fencing_token: 2,
          resume_decision_checksum: "decision",
          resume_fencing_token: null,
        }),
        rows: [scriptedShard()],
      },
      {
        operation: scriptedOperation({
          halt_control_sequence: 1,
          halt_failed_shard_id: "shard-a",
          halt_fencing_token: 2,
          resume_decision_checksum: "decision",
          resume_fencing_token: 2,
        }),
        rows: [scriptedShard()],
      },
    ]
    for (const fixture of malformed) {
      const store = new D1MigrationStore(
        new ScriptedDatabase({
          operations: [fixture.operation],
          rows: fixture.rows ?? [scriptedShard()],
        }),
      )
      await expect(store.load("migration-1")).rejects.toMatchObject({
        code: "MigrationFailedError",
      })
    }
  })

  it("fails closed on invalid metadata and impossible post-mutation disappearance", async () => {
    const proof = {
      acquisitionId: "acquisition-b",
      fencingToken: 2,
      holderId: "controller-b",
      leaseKey: "fleet-a:migration",
    } as const
    const completeRow = scriptedOperation()
    const verifiedRow = scriptedShard({
      apply_state: "applied",
      canonical_schema_checksum: "schema-target",
      ledger_checksum: "artifact-target",
      verification_state: "verified",
    })
    for (const changes of [-1, Number.NaN]) {
      await expect(
        new D1MigrationStore(
          new ScriptedDatabase({ changes, operations: [completeRow], rows: [verifiedRow] }),
        ).activate(
          "migration-1",
          { activeApplicationSupportsTarget: true, activeRouterSupportsTarget: true },
          proof,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }

    await expect(
      new D1MigrationStore(
        new ScriptedDatabase({ operations: [completeRow, null], rows: [scriptedShard()] }),
      ).accept("migration-1", "shard-a", proof),
    ).rejects.toThrow("disappeared after a durable transition")

    const halted = scriptedOperation({
      halt_control_sequence: 1,
      halt_failed_shard_id: "shard-a",
      halt_fencing_token: 1,
      state: "mixed_blocked",
    })
    await expect(
      new D1MigrationStore(
        new ScriptedDatabase({ operations: [halted, null], rows: [scriptedShard()] }),
      ).resume("migration-1", { decisionChecksum: "decision", proof }),
    ).rejects.toThrow("disappeared after resume authorization")

    const expiredCreate = new D1MigrationStore(
      new ScriptedDatabase({ changes: 0, operations: [null] }),
    )
    await expect(
      expiredCreate.create({ fleetId: "fleet-a", operation: fresh(), proof }),
    ).rejects.toThrow("expired before registration")

    await expect(
      new D1MigrationStore(
        new ScriptedDatabase({
          operations: [completeRow, completeRow],
          rows: [scriptedShard()],
        }),
      ).accept("migration-1", "shard-a", proof),
    ).rejects.toThrow("did not persist the expected migration shard state")

    await expect(
      new D1MigrationStore(
        new ScriptedDatabase({
          operations: [halted, halted],
          rows: [scriptedShard()],
        }),
      ).resume("migration-1", { decisionChecksum: "decision", proof }),
    ).rejects.toThrow("did not persist the expected migration resume decision")

    for (const changes of [0, 1]) {
      const activation = new D1MigrationStore(
        new ScriptedDatabase({
          changes,
          operations: [completeRow, completeRow],
          rows: [verifiedRow],
        }),
      ).activate(
        "migration-1",
        { activeApplicationSupportsTarget: true, activeRouterSupportsTarget: true },
        proof,
      )
      if (changes === 0) await expect(activation).resolves.toBe(false)
      else await expect(activation).rejects.toThrow("did not persist migration activation")
    }
  })
})
