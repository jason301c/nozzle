import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import {
  generateMovementTransferSql,
  type MovementTransferTableSpec,
  movementTransferViewName,
} from "../src/movement-transfer.js"
import { generateShardGuardSql } from "../src/shard-guards.js"

const table: MovementTransferTableSpec = {
  columns: ["id", "workspace_id", "payload"],
  primaryColumns: ["id", "workspace_id"],
  tableName: "projects",
}
const token = "a".repeat(43)

function withDatabase(run: (database: DatabaseSync) => void): void {
  const database = new DatabaseSync(":memory:")
  try {
    run(database)
  } finally {
    database.close()
  }
}

function install(database: DatabaseSync): void {
  database.exec(`CREATE TABLE "projects" (
    "id" TEXT NOT NULL,
    "workspace_id" TEXT NOT NULL,
    "payload" TEXT NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL,
    PRIMARY KEY ("id", "workspace_id")
  );`)
  database.exec(
    generateShardGuardSql({
      schemaId: "application-v1",
      tables: [{ tableName: "projects", partitionColumn: "workspace_id", partitionType: "string" }],
    }).sql,
  )
  database.exec(generateMovementTransferSql({ tables: [table] }).sql)
  database
    .prepare(
      `INSERT INTO "nozzle_schema_state"
       ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
       VALUES ('application-v1', ?, 1, 'schema-operation', 1)`,
    )
    .run("ab".repeat(32))
}

function setOwnership(
  database: DatabaseSync,
  input: {
    readonly bucketId?: number
    readonly operationId?: string
    readonly role: "destination" | "source"
    readonly state: "copying" | "quarantined"
  },
): void {
  database
    .prepare(
      `INSERT INTO "nozzle_bucket_ownership"
       ("bucket_id", "route_epoch", "state", "movement_role", "operation_id",
        "fencing_token", "schema_version", "last_verified_checkpoint",
        "last_verified_at_ms", "updated_at_ms")
       VALUES (?, 8, ?, ?, ?, 2, 1, 'movement', 1, 1)`,
    )
    .run(input.bucketId ?? 42, input.state, input.role, input.operationId ?? "movement-1")
}

function issueCapability(
  database: DatabaseSync,
  input: {
    readonly capabilityToken?: string
    readonly mode: "cleanup_delete" | "delete" | "upsert"
    readonly operationId?: string
    readonly uses?: number
  },
): void {
  database
    .prepare(
      `INSERT INTO "nozzle_operation_write_capabilities"
       ("capability_token", "operation_id", "bucket_id", "table_id", "mode",
        "fencing_token", "expires_at_ms", "remaining_uses", "issued_at_ms")
       VALUES (?, ?, 42, 'projects', ?, 2, 9999999999999, ?, 1)`,
    )
    .run(
      input.capabilityToken ?? token,
      input.operationId ?? "movement-1",
      input.mode,
      input.uses ?? 1,
    )
}

function apply(
  database: DatabaseSync,
  input: {
    readonly capabilityToken?: string
    readonly hint: string
    readonly id?: string
    readonly payload?: string
    readonly workspace?: string
  },
): void {
  database
    .prepare(
      `INSERT INTO "${movementTransferViewName("projects")}"
       ("id", "workspace_id", "payload", "__nozzle_bucket",
        "__nozzle_capability_token", "__nozzle_mutation_hint")
       VALUES (?, ?, ?, 42, ?, ?)`,
    )
    .run(
      input.id ?? "project-1",
      input.workspace ?? "workspace-a",
      input.payload ?? "payload",
      input.capabilityToken ?? token,
      input.hint,
    )
}

describe("movement transfer capabilities", () => {
  it("upserts and deletes through a statement-local context without weakening base guards", () => {
    withDatabase((database) => {
      install(database)
      setOwnership(database, { role: "destination", state: "copying" })
      expect(() =>
        database
          .prepare(
            `INSERT INTO "projects" ("id", "workspace_id", "payload", "__nozzle_bucket")
             VALUES ('direct', 'workspace-a', 'blocked', 42)`,
          )
          .run(),
      ).toThrow(/NOZZLE_GUARD_OWNERSHIP/u)

      const failedToken = "f".repeat(43)
      issueCapability(database, {
        capabilityToken: failedToken,
        mode: "upsert",
      })
      expect(() =>
        database
          .prepare(
            `INSERT INTO "${movementTransferViewName("projects")}"
             ("id", "workspace_id", "payload", "__nozzle_bucket",
              "__nozzle_capability_token", "__nozzle_mutation_hint")
             VALUES ('invalid', 'workspace-a', NULL, 42, ?, 'upsert')`,
          )
          .run(failedToken),
      ).toThrow()
      expect(
        database
          .prepare(
            `SELECT "remaining_uses" FROM "nozzle_operation_write_capabilities"
             WHERE "capability_token" = ?`,
          )
          .get(failedToken),
      ).toEqual({ remaining_uses: 1 })
      expect(
        database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_write_context"`).get(),
      ).toEqual({ count: 0 })

      issueCapability(database, { mode: "upsert", uses: 2 })
      apply(database, { hint: "upsert", payload: "copied" })
      apply(database, { hint: "upsert", payload: "replayed" })
      expect(database.prepare(`SELECT * FROM "projects"`).get()).toEqual({
        __nozzle_bucket: 42,
        id: "project-1",
        payload: "replayed",
        workspace_id: "workspace-a",
      })
      expect(
        database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_write_context"`).get(),
      ).toEqual({
        count: 0,
      })
      expect(
        database
          .prepare(
            `SELECT "remaining_uses" FROM "nozzle_operation_write_capabilities"
             WHERE "capability_token" = ?`,
          )
          .get(token),
      ).toEqual({ remaining_uses: 0 })
      expect(() => apply(database, { hint: "upsert" })).toThrow(
        /NOZZLE_OPERATION_CAPABILITY_INVALID/u,
      )

      issueCapability(database, { capabilityToken: "b".repeat(43), mode: "delete" })
      apply(database, { capabilityToken: "b".repeat(43), hint: "delete" })
      expect(database.prepare(`SELECT count(*) AS "count" FROM "projects"`).get()).toEqual({
        count: 0,
      })
    })
  })

  it("supports quarantined source cleanup but fences wrong ownership and invalid requests", () => {
    withDatabase((database) => {
      install(database)
      setOwnership(database, { role: "source", state: "quarantined" })
      database.exec(`DROP TRIGGER "nozzle_guard_70726f6a65637473_insert";`)
      database
        .prepare(
          `INSERT INTO "projects" ("id", "workspace_id", "payload", "__nozzle_bucket")
           VALUES ('source', 'workspace-a', 'old', 42)`,
        )
        .run()
      issueCapability(database, { mode: "cleanup_delete" })
      apply(database, { hint: "delete", id: "source" })
      expect(database.prepare(`SELECT count(*) AS "count" FROM "projects"`).get()).toEqual({
        count: 0,
      })

      expect(() =>
        issueCapability(database, {
          capabilityToken: "c".repeat(43),
          mode: "upsert",
        }),
      ).toThrow(/NOZZLE_OPERATION_CAPABILITY_OWNERSHIP/u)
      expect(() => apply(database, { capabilityToken: "z".repeat(43), hint: "upsert" })).toThrow(
        /NOZZLE_OPERATION_CAPABILITY_INVALID/u,
      )
      expect(() => apply(database, { hint: "invalid" })).toThrow(/NOZZLE_OPERATION_MUTATION_HINT/u)
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_operation_write_context"
             ("singleton", "capability_token", "operation_id", "bucket_id", "table_id", "mode")
             VALUES (1, ?, 'movement-1', 42, 'projects', 'upsert')`,
          )
          .run("z".repeat(43)),
      ).toThrow(/NOZZLE_OPERATION_CAPABILITY_INVALID/u)
    })
  })

  it("keeps capability identity immutable and permits cleanup only after use or expiry", () => {
    withDatabase((database) => {
      install(database)
      setOwnership(database, { role: "destination", state: "copying" })
      issueCapability(database, { mode: "upsert" })
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operation_write_capabilities" SET "remaining_uses" = 5
             WHERE "capability_token" = ?`,
          )
          .run(token),
      ).toThrow(/NOZZLE_OPERATION_CAPABILITY_IMMUTABLE/u)
      expect(() =>
        database
          .prepare(`DELETE FROM "nozzle_operation_write_capabilities" WHERE "capability_token" = ?`)
          .run(token),
      ).toThrow(/NOZZLE_OPERATION_CAPABILITY_ACTIVE/u)
      apply(database, { hint: "upsert" })
      expect(
        database
          .prepare(`DELETE FROM "nozzle_operation_write_capabilities" WHERE "capability_token" = ?`)
          .run(token).changes,
      ).toBe(1)
      const contextToken = "d".repeat(43)
      issueCapability(database, {
        capabilityToken: contextToken,
        mode: "upsert",
      })
      database
        .prepare(
          `INSERT INTO "nozzle_operation_write_context"
           ("singleton", "capability_token", "operation_id", "bucket_id", "table_id", "mode")
           VALUES (1, ?, 'movement-1', 42, 'projects', 'upsert')`,
        )
        .run(contextToken)
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_operation_write_context" SET "table_id" = 'other'
             WHERE "singleton" = 1`,
          )
          .run(),
      ).toThrow(/NOZZLE_OPERATION_CONTEXT_IMMUTABLE/u)
      database.prepare(`DELETE FROM "nozzle_operation_write_context"`).run()
    })
  })

  it("generates deterministic quoted views and rejects malformed table metadata", () => {
    const beta = {
      columns: ['id"quoted', "value"],
      primaryColumns: ['id"quoted'],
      tableName: 'Beta"table',
    }
    const alpha = { columns: ["id"], primaryColumns: ["id"], tableName: "alpha" }
    const left = generateMovementTransferSql({ tables: [beta, alpha] })
    const right = generateMovementTransferSql({ tables: [alpha, beta] })
    expect(left).toEqual(right)
    expect(left.tables.map((entry) => entry.tableName)).toEqual(["alpha", 'Beta"table'])
    expect(left.sql).toContain('FROM "Beta""table"')
    expect(left.sql.endsWith("\n")).toBe(true)
    expect(Object.isFrozen(left)).toBe(true)
    expect(Object.isFrozen(left.tables[0]?.columns)).toBe(true)
    expect(movementTransferViewName("projects")).toBe("nozzle_operation_70726f6a65637473")

    const base = { tables: [table] }
    const invalid: unknown[] = [
      null,
      [],
      { tables: [] },
      { tables: Array.from({ length: 1_001 }, () => table) },
      { tables: [null] },
      { tables: [[]] },
      { tables: [{ ...table, tableName: "" }] },
      { tables: [{ ...table, tableName: undefined }] },
      { tables: [{ ...table, tableName: "\ud800" }] },
      { tables: [{ ...table, tableName: "\udc00" }] },
      { tables: [{ ...table, tableName: "bad\n" }] },
      { tables: [{ ...table, tableName: "x".repeat(256) }] },
      { tables: [{ ...table, tableName: "sqlite_bad" }] },
      { tables: [{ ...table, tableName: "nozzle_bad" }] },
      { tables: [{ ...table, columns: [] }] },
      { tables: [{ ...table, columns: ["id", "ID"] }] },
      { tables: [{ ...table, primaryColumns: [] }] },
      { tables: [{ ...table, primaryColumns: ["missing"] }] },
      { tables: [table, { ...table, tableName: "PROJECTS" }] },
    ]
    for (const input of invalid) {
      expect(() => generateMovementTransferSql(input as typeof base)).toThrow()
    }
    expect(() => movementTransferViewName("😀")).not.toThrow()
  })
})
