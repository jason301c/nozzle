import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import {
  generateShardGuardSql,
  ShardGuardSqlError,
  type ShardGuardSqlInput,
  type ShardGuardTableSpec,
} from "../src/shard-guards.js"

type SqlValue = bigint | Buffer | null | number | string

const SCHEMA_ID = "application-v1"
const SCHEMA_DIGEST = "ab".repeat(32)

function identifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function generate(input: unknown) {
  return generateShardGuardSql(input as ShardGuardSqlInput)
}

function withDatabase(run: (database: DatabaseSync) => void): void {
  const database = new DatabaseSync(":memory:")
  try {
    run(database)
  } finally {
    database.close()
  }
}

function createApplicationTable(database: DatabaseSync, spec: ShardGuardTableSpec): void {
  const affinity =
    spec.partitionType === "integer" ? "INTEGER" : spec.partitionType === "binary" ? "BLOB" : "TEXT"
  database.exec(`CREATE TABLE ${identifier(spec.tableName)} (
    "id" INTEGER PRIMARY KEY,
    ${identifier(spec.partitionColumn)} ${affinity} NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL,
    "payload" TEXT NOT NULL
  );`)
}

function activateSchema(database: DatabaseSync, schemaId = SCHEMA_ID): void {
  database
    .prepare(
      `INSERT INTO "nozzle_schema_state"
       ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
       VALUES (?, ?, 1, ?, ?)`,
    )
    .run(schemaId, SCHEMA_DIGEST, "operation-schema-1", 1)
}

function setOwnership(
  database: DatabaseSync,
  bucketId: number,
  state: "read_only" | "writable",
  routeEpoch = 1,
  fencingToken = 1,
  operationId = "operation-route-1",
): void {
  database
    .prepare(
      `INSERT INTO "nozzle_bucket_ownership"
       ("bucket_id", "route_epoch", "state", "movement_role", "operation_id", "fencing_token", "schema_version",
        "last_verified_checkpoint", "last_verified_at_ms", "updated_at_ms")
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT ("bucket_id") DO UPDATE SET
         "route_epoch" = excluded."route_epoch",
         "state" = excluded."state",
         "movement_role" = excluded."movement_role",
         "operation_id" = excluded."operation_id",
         "fencing_token" = excluded."fencing_token",
         "schema_version" = excluded."schema_version",
         "last_verified_checkpoint" = excluded."last_verified_checkpoint",
         "last_verified_at_ms" = excluded."last_verified_at_ms",
         "updated_at_ms" = excluded."updated_at_ms"`,
    )
    .run(bucketId, routeEpoch, state, "none", operationId, fencingToken, 1, "checkpoint-1", 1, 1)
}

function insertApplicationRow(
  database: DatabaseSync,
  spec: ShardGuardTableSpec,
  id: number,
  partitionValue: SqlValue,
  bucketId: number,
  payload = "initial",
): void {
  database
    .prepare(
      `INSERT INTO ${identifier(spec.tableName)}
       ("id", ${identifier(spec.partitionColumn)}, "__nozzle_bucket", "payload")
       VALUES (?, ?, ?, ?)`,
    )
    .run(id, partitionValue, bucketId, payload)
}

interface FenceValues {
  readonly binary?: Buffer
  readonly integer?: number
  readonly string?: string
  readonly uuid?: string
}

function insertFence(
  database: DatabaseSync,
  type: ShardGuardTableSpec["partitionType"],
  values: FenceValues,
  digest = "cd".repeat(32),
): void {
  database
    .prepare(
      `INSERT INTO "nozzle_partition_fences" (
        "hash_version", "partition_digest", "partition_type",
        "partition_binary", "partition_integer", "partition_string", "partition_uuid",
        "source_bucket_id", "source_route_epoch", "operation_id", "audit_event_id",
        "fenced_at_ms", "reason"
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      1,
      digest,
      type,
      values.binary ?? null,
      values.integer ?? null,
      values.string ?? null,
      values.uuid ?? null,
      7,
      1,
      "operation-move-1",
      "audit-event-1",
      2,
      "former-source",
    )
}

function install(database: DatabaseSync, tables: readonly ShardGuardTableSpec[]): string {
  for (const table of tables) createApplicationTable(database, table)
  const generated = generateShardGuardSql({ schemaId: SCHEMA_ID, tables })
  database.exec(generated.sql)
  return generated.sql
}

describe("generateShardGuardSql", () => {
  it("emits a deterministic, deeply immutable artifact in SQLite name order", () => {
    const beta = {
      tableName: "Beta",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    const alpha = {
      tableName: "alpha",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    const left = generateShardGuardSql({ schemaId: SCHEMA_ID, tables: [beta, alpha] })
    const right = generateShardGuardSql({ schemaId: SCHEMA_ID, tables: [alpha, beta] })

    expect(left).toEqual(right)
    expect(left.sql.endsWith("\n")).toBe(true)
    expect(left.tables.map(({ tableName }) => tableName)).toEqual(["alpha", "Beta"])
    expect(left.statements).toHaveLength(18)
    expect(Object.isFrozen(left)).toBe(true)
    expect(Object.isFrozen(left.tables)).toBe(true)
    expect(Object.isFrozen(left.tables[0])).toBe(true)
    expect(Object.isFrozen(left.statements)).toBe(true)
    expect(left.sql).not.toContain("tenant-fictional")
  })

  it("sorts prefix and non-ASCII identifiers by deterministic UTF-16 code units", () => {
    const generated = generateShardGuardSql({
      schemaId: SCHEMA_ID,
      tables: [
        { tableName: "aa", partitionColumn: "tenant_id", partitionType: "string" },
        { tableName: "😀", partitionColumn: "tenant_id", partitionType: "string" },
        { tableName: "a", partitionColumn: "tenant_id", partitionType: "string" },
      ],
    })

    expect(generated.tables.map(({ tableName }) => tableName)).toEqual(["a", "aa", "😀"])
  })

  it("quotes hostile but valid identifiers without executing injected SQL", () => {
    const spec = {
      tableName: 'orders"; DROP TABLE "victim',
      partitionColumn: 'tenant"; SELECT 1; --',
      partitionType: "string",
    } as const
    withDatabase((database) => {
      database.exec('CREATE TABLE "victim" ("id" INTEGER);')
      const sql = install(database, [spec])
      activateSchema(database)
      setOwnership(database, 7, "writable")
      insertApplicationRow(database, spec, 1, "tenant-fictional", 7)

      expect(sql).toContain('BEFORE INSERT ON "orders""; DROP TABLE ""victim"')
      expect(sql).toContain('NEW."tenant""; SELECT 1; --"')
      expect(database.prepare("SELECT count(*) AS count FROM victim").get()).toEqual({ count: 0 })
    })
  })

  it("creates only non-recursive BEFORE guard triggers with read-only bodies", () => {
    const spec = {
      tableName: "projects",
      partitionColumn: "workspace_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      const triggers = database
        .prepare(
          "SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ? ORDER BY name",
        )
        .all(spec.tableName) as { readonly name: string; readonly sql: string }[]

      expect(triggers).toHaveLength(3)
      for (const trigger of triggers) {
        expect(trigger.name).toMatch(/^nozzle_guard_[0-9a-f]+_(?:delete|insert|update)$/u)
        expect(trigger.sql).toMatch(/\bBEFORE (?:DELETE|INSERT|UPDATE)\b/u)
        expect(trigger.sql).not.toMatch(/\bAFTER\b/u)
        expect(trigger.sql.slice(trigger.sql.indexOf("BEGIN"))).not.toMatch(
          /\b(?:DELETE|INSERT|UPDATE)\s+(?:FROM|INTO|OR|"nozzle)/u,
        )
        expect(trigger.sql).toContain("SELECT CASE")
      }
    })
  })

  it.each([
    {
      partitionType: "string" as const,
      fenced: "tenant-a",
      other: "tenant-b",
      fence: { string: "tenant-a" },
    },
    {
      partitionType: "integer" as const,
      fenced: 17,
      other: 18,
      fence: { integer: 17 },
    },
    {
      partitionType: "binary" as const,
      fenced: Buffer.from([0, 1, 2]),
      other: Buffer.from([3, 4, 5]),
      fence: { binary: Buffer.from([0, 1, 2]) },
    },
    {
      partitionType: "uuid" as const,
      fenced: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
      other: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      fence: { uuid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
    },
  ])("blocks every mutation of a fenced $partitionType partition but not its bucket neighbors", ({
    partitionType,
    fenced,
    other,
    fence,
  }) => {
    const spec = {
      tableName: `records_${partitionType}`,
      partitionColumn: "tenant_id",
      partitionType,
    }
    withDatabase((database) => {
      install(database, [spec])
      activateSchema(database)
      setOwnership(database, 7, "writable")
      insertApplicationRow(database, spec, 1, fenced, 7)
      insertApplicationRow(database, spec, 2, other, 7)
      insertFence(database, partitionType, fence)

      expect(() => insertApplicationRow(database, spec, 3, fenced, 7)).toThrow(
        /NOZZLE_GUARD_PARTITION_FENCE/u,
      )
      expect(() =>
        database
          .prepare(`UPDATE ${identifier(spec.tableName)} SET "payload" = ? WHERE "id" = 1`)
          .run("changed"),
      ).toThrow(/NOZZLE_GUARD_PARTITION_FENCE/u)
      expect(() =>
        database.prepare(`DELETE FROM ${identifier(spec.tableName)} WHERE "id" = 1`).run(),
      ).toThrow(/NOZZLE_GUARD_PARTITION_FENCE/u)

      insertApplicationRow(database, spec, 3, other, 7)
      database
        .prepare(`UPDATE ${identifier(spec.tableName)} SET "payload" = ? WHERE "id" = 2`)
        .run("changed")
      database.prepare(`DELETE FROM ${identifier(spec.tableName)} WHERE "id" = 2`).run()
      expect(
        database
          .prepare(`SELECT "id", "payload" FROM ${identifier(spec.tableName)} ORDER BY "id"`)
          .all(),
      ).toEqual([
        { id: 1, payload: "initial" },
        { id: 3, payload: "initial" },
      ])
    })
  })

  it("keeps fence comparison type preserving across tables", () => {
    const stringSpec = {
      tableName: "string_records",
      partitionColumn: "partition_key",
      partitionType: "string",
    } as const
    const integerSpec = {
      tableName: "integer_records",
      partitionColumn: "partition_key",
      partitionType: "integer",
    } as const
    withDatabase((database) => {
      install(database, [stringSpec, integerSpec])
      activateSchema(database)
      setOwnership(database, 7, "writable")
      insertFence(database, "string", { string: "17" })

      expect(() => insertApplicationRow(database, stringSpec, 1, "17", 7)).toThrow(
        /NOZZLE_GUARD_PARTITION_FENCE/u,
      )
      expect(() => insertApplicationRow(database, integerSpec, 1, 17, 7)).not.toThrow()
    })
  })

  it("fails closed on inactive schema or non-writable local ownership before mutation", () => {
    const spec = {
      tableName: "documents",
      partitionColumn: "account_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      expect(() => insertApplicationRow(database, spec, 1, "account-a", 7)).toThrow(
        /NOZZLE_GUARD_SCHEMA/u,
      )

      activateSchema(database)
      expect(() => insertApplicationRow(database, spec, 1, "account-a", 7)).toThrow(
        /NOZZLE_GUARD_OWNERSHIP/u,
      )
      setOwnership(database, 7, "read_only")
      expect(() => insertApplicationRow(database, spec, 1, "account-a", 7)).toThrow(
        /NOZZLE_GUARD_OWNERSHIP/u,
      )

      setOwnership(database, 7, "writable", 2, 2, "operation-route-2")
      insertApplicationRow(database, spec, 1, "account-a", 7)
      setOwnership(database, 7, "read_only", 2, 2, "operation-route-2")
      expect(() =>
        database.prepare('UPDATE "documents" SET "payload" = ? WHERE "id" = 1').run("changed"),
      ).toThrow(/NOZZLE_GUARD_OWNERSHIP/u)
      expect(() => database.prepare('DELETE FROM "documents" WHERE "id" = 1').run()).toThrow(
        /NOZZLE_GUARD_OWNERSHIP/u,
      )
      expect(database.prepare('SELECT "payload" FROM "documents" WHERE "id" = 1').get()).toEqual({
        payload: "initial",
      })
    })
  })

  it("allows mixed-schema trigger rollout while requiring one active local schema", () => {
    const spec = {
      tableName: "mixed_schema",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      activateSchema(database, "application-v2")
      setOwnership(database, 7, "writable")

      expect(() => insertApplicationRow(database, spec, 1, "tenant-a", 7)).not.toThrow()
    })
  })

  it("rejects bucket and partition identity updates while allowing ordinary updates", () => {
    const spec = {
      tableName: "immutable_scope",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      activateSchema(database)
      setOwnership(database, 7, "writable")
      setOwnership(database, 8, "writable")
      insertApplicationRow(database, spec, 1, "tenant-a", 7)

      expect(() =>
        database.prepare('UPDATE "immutable_scope" SET "__nozzle_bucket" = 8 WHERE "id" = 1').run(),
      ).toThrow(/NOZZLE_GUARD_BUCKET_IMMUTABLE/u)
      expect(() =>
        database
          .prepare('UPDATE "immutable_scope" SET "tenant_id" = ? WHERE "id" = 1')
          .run("tenant-b"),
      ).toThrow(/NOZZLE_GUARD_PARTITION_IMMUTABLE/u)
      expect(() =>
        database
          .prepare('UPDATE "immutable_scope" SET "payload" = ? WHERE "id" = 1')
          .run("changed"),
      ).not.toThrow()
      expect(database.prepare('SELECT * FROM "immutable_scope"').get()).toEqual({
        __nozzle_bucket: 7,
        id: 1,
        payload: "changed",
        tenant_id: "tenant-a",
      })
    })
  })

  it("enforces exact partition identity even when an application column uses NOCASE", () => {
    const spec = {
      tableName: "nocase_scope",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      database.exec(`CREATE TABLE "nocase_scope" (
        "id" INTEGER PRIMARY KEY,
        "tenant_id" TEXT COLLATE NOCASE NOT NULL,
        "__nozzle_bucket" INTEGER NOT NULL,
        "payload" TEXT NOT NULL
      );`)
      database.exec(generateShardGuardSql({ schemaId: SCHEMA_ID, tables: [spec] }).sql)
      activateSchema(database)
      setOwnership(database, 7, "writable")
      insertApplicationRow(database, spec, 1, "tenant-a", 7)

      expect(() =>
        database
          .prepare('UPDATE "nocase_scope" SET "tenant_id" = ? WHERE "id" = 1')
          .run("TENANT-A"),
      ).toThrow(/NOZZLE_GUARD_PARTITION_IMMUTABLE/u)
    })
  })

  it("rejects malformed stored rows and runtime partition types", () => {
    const spec = {
      tableName: "integer_scope",
      partitionColumn: "tenant_id",
      partitionType: "integer",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      activateSchema(database)
      setOwnership(database, 7, "writable")
      expect(() => insertApplicationRow(database, spec, 1, "not-an-integer", 7)).toThrow(
        /NOZZLE_GUARD_PARTITION_TYPE/u,
      )
      expect(() => insertApplicationRow(database, spec, 1, 17, -1)).toThrow(
        /NOZZLE_GUARD_BUCKET_TYPE/u,
      )
      expect(() => insertApplicationRow(database, spec, 1, Number.MAX_SAFE_INTEGER + 1, 7)).toThrow(
        /NOZZLE_GUARD_PARTITION_TYPE/u,
      )
    })
  })

  it("stores canonical ownership movement and verification metadata", () => {
    const spec = {
      tableName: "ownership_metadata",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_bucket_ownership"
             ("bucket_id", "route_epoch", "state", "movement_role", "operation_id", "fencing_token",
              "schema_version", "last_verified_checkpoint", "last_verified_at_ms", "updated_at_ms")
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(7, 1, "copying", "destination", "operation-move-1", 4, 2, "copy-page-4", 10, 11),
      ).not.toThrow()
      expect(database.prepare('SELECT * FROM "nozzle_bucket_ownership"').get()).toEqual({
        bucket_id: 7,
        last_verified_at_ms: 10,
        last_verified_checkpoint: "copy-page-4",
        movement_role: "destination",
        operation_id: "operation-move-1",
        fencing_token: 4,
        route_epoch: 1,
        schema_version: 2,
        state: "copying",
        updated_at_ms: 11,
      })
    })
  })

  it("makes local ownership monotonic, fenced, persistent, and state-machine constrained", () => {
    const spec = {
      tableName: "ownership_fencing",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      setOwnership(database, 7, "writable", 5, 10, "operation-route-10")

      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "bucket_id" = 8 WHERE "bucket_id" = 7')
          .run(),
      ).toThrow(/NOZZLE_OWNERSHIP_BUCKET_IMMUTABLE/u)
      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "fencing_token" = 9 WHERE "bucket_id" = 7')
          .run(),
      ).toThrow(/NOZZLE_OWNERSHIP_FENCING_TOKEN/u)
      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "operation_id" = ? WHERE "bucket_id" = 7')
          .run("operation-route-11"),
      ).toThrow(/NOZZLE_OWNERSHIP_FENCING_TOKEN/u)
      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "route_epoch" = 4 WHERE "bucket_id" = 7')
          .run(),
      ).toThrow(/NOZZLE_OWNERSHIP_ROUTE_EPOCH/u)
      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "state" = ? WHERE "bucket_id" = 7')
          .run("copying"),
      ).toThrow(/NOZZLE_OWNERSHIP_TRANSITION/u)

      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "state" = ? WHERE "bucket_id" = 7')
          .run("read_only"),
      ).not.toThrow()
      expect(() =>
        database
          .prepare('UPDATE "nozzle_bucket_ownership" SET "state" = ? WHERE "bucket_id" = 7')
          .run("writable"),
      ).toThrow(/NOZZLE_OWNERSHIP_ROUTE_EPOCH/u)
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_bucket_ownership"
             SET "state" = 'writable', "route_epoch" = 6, "fencing_token" = 11,
                 "operation_id" = 'operation-route-11'
             WHERE "bucket_id" = 7`,
          )
          .run(),
      ).not.toThrow()
      expect(database.prepare('SELECT * FROM "nozzle_bucket_ownership"').get()).toMatchObject({
        bucket_id: 7,
        fencing_token: 11,
        operation_id: "operation-route-11",
        route_epoch: 6,
        state: "writable",
      })
      expect(() =>
        database.prepare('DELETE FROM "nozzle_bucket_ownership" WHERE "bucket_id" = 7').run(),
      ).toThrow(/NOZZLE_OWNERSHIP_PERSISTENT/u)
    })
  })

  it("makes partition fences immutable and persistent after creation", () => {
    const spec = {
      tableName: "persistent_fence",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      insertFence(database, "string", { string: "tenant-a" })

      expect(() =>
        database.prepare('UPDATE "nozzle_partition_fences" SET "reason" = ?').run("rewritten"),
      ).toThrow(/NOZZLE_PARTITION_FENCE_IMMUTABLE/u)
      expect(() => database.prepare('DELETE FROM "nozzle_partition_fences"').run()).toThrow(
        /NOZZLE_PARTITION_FENCE_PERSISTENT/u,
      )
      expect(database.prepare('SELECT "reason" FROM "nozzle_partition_fences"').get()).toEqual({
        reason: "former-source",
      })
    })
  })

  it("enforces lowercase full digests, hash version, exactly one typed value, and audit fields", () => {
    const spec = {
      tableName: "fence_constraints",
      partitionColumn: "tenant_id",
      partitionType: "string",
    } as const
    withDatabase((database) => {
      install(database, [spec])
      const insert = database.prepare(`INSERT INTO "nozzle_partition_fences" (
        "hash_version", "partition_digest", "partition_type",
        "partition_binary", "partition_integer", "partition_string", "partition_uuid",
        "source_bucket_id", "source_route_epoch", "operation_id", "audit_event_id",
        "fenced_at_ms", "reason"
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      const values = (
        hashVersion: number,
        digest: string,
        type: string,
        binary: Buffer | null,
        integer: number | null,
        string: string | null,
        uuid: string | null,
        operationId = "operation-1",
      ) =>
        [
          hashVersion,
          digest,
          type,
          binary,
          integer,
          string,
          uuid,
          7,
          1,
          operationId,
          "audit-1",
          1,
          "former-source",
        ] as const

      expect(() =>
        insert.run(...values(1, "AB".repeat(32), "string", null, null, "a", null)),
      ).toThrow()
      expect(() =>
        insert.run(...values(1, "ab".repeat(31), "string", null, null, "a", null)),
      ).toThrow()
      expect(() =>
        insert.run(...values(2, "ab".repeat(32), "string", null, null, "a", null)),
      ).toThrow()
      expect(() =>
        insert.run(...values(1, "ab".repeat(32), "string", null, 1, "a", null)),
      ).toThrow()
      expect(() =>
        insert.run(
          ...values(1, "ab".repeat(32), "integer", null, Number.MAX_SAFE_INTEGER + 1, null, null),
        ),
      ).toThrow()
      expect(() =>
        insert.run(...values(1, "ab".repeat(32), "uuid", null, null, null, "not-a-uuid")),
      ).toThrow()
      expect(() =>
        insert.run(...values(1, "ab".repeat(32), "string", null, null, "a", null, "")),
      ).toThrow()
      expect(() =>
        insert.run(
          ...values(
            1,
            "ab".repeat(32),
            "uuid",
            null,
            null,
            null,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          ),
        ),
      ).not.toThrow()
    })
  })

  it.each([
    undefined,
    null,
    [],
    "schema-v1",
  ])("rejects a non-object generator input: %j", (input) => {
    expect(() => generate(input)).toThrow(ShardGuardSqlError)
  })

  it.each([
    "",
    " leading-space",
    "contains space",
    "contains/slash",
    "contains'apostrophe",
    "a".repeat(129),
  ])("rejects malformed schema ID %j", (schemaId) => {
    expect(() =>
      generate({
        schemaId,
        tables: [{ tableName: "items", partitionColumn: "tenant_id", partitionType: "string" }],
      }),
    ).toThrow(/schemaId/u)
  })

  it.each([
    { tableName: "", partitionColumn: "tenant_id", partitionType: "string" },
    { tableName: "  ", partitionColumn: "tenant_id", partitionType: "string" },
    { tableName: "nozzle_shadow", partitionColumn: "tenant_id", partitionType: "string" },
    { tableName: "SQLITE_shadow", partitionColumn: "tenant_id", partitionType: "string" },
    { tableName: "items", partitionColumn: "Nozzle_tenant", partitionType: "string" },
    { tableName: "items", partitionColumn: "__NOZZLE_bucket", partitionType: "string" },
    { tableName: "items", partitionColumn: "tenant\u0000id", partitionType: "string" },
    { tableName: "items", partitionColumn: "\ud800", partitionType: "string" },
    { tableName: "items", partitionColumn: "\ud800\uffff", partitionType: "string" },
    { tableName: "items", partitionColumn: "\udc00", partitionType: "string" },
    { tableName: "x".repeat(256), partitionColumn: "tenant_id", partitionType: "string" },
    { tableName: "items", partitionColumn: "tenant_id", partitionType: "float" },
  ])("rejects malformed or reserved table specifications: %j", (table) => {
    expect(() => generate({ schemaId: SCHEMA_ID, tables: [table] })).toThrow(ShardGuardSqlError)
  })

  it("rejects empty, malformed, duplicate, and ASCII case-colliding table sets", () => {
    expect(() => generate({ schemaId: SCHEMA_ID, tables: [] })).toThrow(/at least one/u)
    expect(() => generate({ schemaId: SCHEMA_ID, tables: [null] })).toThrow(/must be an object/u)
    expect(() => generate({ schemaId: SCHEMA_ID, tables: new Array(1_001) })).toThrow(
      /more than 1000/u,
    )
    expect(() =>
      generate({
        schemaId: SCHEMA_ID,
        tables: [
          { tableName: "Orders", partitionColumn: "tenant_id", partitionType: "string" },
          { tableName: "orders", partitionColumn: "account_id", partitionType: "string" },
        ],
      }),
    ).toThrow(/ASCII case-insensitive/u)
    expect(() =>
      generate({
        schemaId: SCHEMA_ID,
        tables: [
          { tableName: "Ä", partitionColumn: "tenant_id", partitionType: "string" },
          { tableName: "ä", partitionColumn: "tenant_id", partitionType: "string" },
        ],
      }),
    ).not.toThrow()
  })
})
