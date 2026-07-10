import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import {
  generateMovementCaptureSql,
  type MovementCaptureSqlInput,
  type MovementCaptureTableSpec,
} from "../src/movement-capture.js"
import { generateShardGuardSql } from "../src/shard-guards.js"

const SCHEMA_ID = "application-v1"
const SCHEMA_CHECKSUM = "ab".repeat(32)

const records: MovementCaptureTableSpec = {
  partitionColumn: "workspace_id",
  partitionType: "string",
  primaryColumns: ["blob_id", "integer_id", "real_id", "text_id"],
  tableName: "records",
}

function quoted(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function withDatabase(run: (database: DatabaseSync) => void): void {
  const database = new DatabaseSync(":memory:")
  try {
    run(database)
  } finally {
    database.close()
  }
}

function install(database: DatabaseSync, spec: MovementCaptureTableSpec = records): void {
  const partitionAffinity =
    spec.partitionType === "integer" ? "INTEGER" : spec.partitionType === "binary" ? "BLOB" : "TEXT"
  database.exec(`CREATE TABLE ${quoted(spec.tableName)} (
    "blob_id" BLOB NOT NULL,
    "integer_id" INTEGER NOT NULL,
    "real_id" REAL NOT NULL,
    "text_id" TEXT NOT NULL,
    ${quoted(spec.partitionColumn)} ${partitionAffinity} NOT NULL,
    "payload" TEXT NOT NULL,
    "__nozzle_bucket" INTEGER NOT NULL,
    PRIMARY KEY ("blob_id", "integer_id", "real_id", "text_id")
  );`)
  database.exec(
    generateShardGuardSql({
      schemaId: SCHEMA_ID,
      tables: [
        {
          partitionColumn: spec.partitionColumn,
          partitionType: spec.partitionType,
          tableName: spec.tableName,
        },
      ],
    }).sql,
  )
  database.exec(generateMovementCaptureSql({ schemaId: SCHEMA_ID, tables: [spec] }).sql)
  database
    .prepare(
      `INSERT INTO "nozzle_schema_state"
       ("schema_id", "schema_digest", "active", "activated_operation_id", "activated_at_ms")
       VALUES (?, ?, 1, 'schema-operation', 1)`,
    )
    .run(SCHEMA_ID, SCHEMA_CHECKSUM)
  database
    .prepare(
      `INSERT INTO "nozzle_bucket_ownership"
       ("bucket_id", "route_epoch", "state", "movement_role", "operation_id",
        "fencing_token", "schema_version", "last_verified_checkpoint",
        "last_verified_at_ms", "updated_at_ms")
       VALUES (42, 7, 'writable', 'source', 'movement-1', 1, 1, 'ready', 1, 1)`,
    )
    .run()
}

function startCapture(
  database: DatabaseSync,
  input: {
    readonly operationId?: string
    readonly partition?: string
    readonly maximum?: number
  } = {},
): void {
  database
    .prepare(
      `INSERT INTO "nozzle_movement_captures"
       ("operation_id", "bucket_id", "scope_kind", "partition_type", "partition_binary",
        "partition_integer", "partition_string", "partition_uuid", "schema_id",
        "schema_checksum", "state", "start_sequence", "acknowledged_sequence",
        "max_pending_entries", "fencing_token", "created_at_ms", "updated_at_ms")
       VALUES (?, 42, 'partition', 'string', NULL, NULL, ?, NULL, ?, ?, 'active',
         0, 0, ?, 1, 1, 1)`,
    )
    .run(
      input.operationId ?? "movement-1",
      input.partition ?? "workspace-a",
      SCHEMA_ID,
      SCHEMA_CHECKSUM,
      input.maximum ?? 2,
    )
}

function insertRecord(
  database: DatabaseSync,
  textId: string,
  workspace: Buffer | number | string = "workspace-a",
): void {
  database
    .prepare(
      `INSERT INTO "records"
       ("blob_id", "integer_id", "real_id", "text_id", "workspace_id", "payload", "__nozzle_bucket")
       VALUES (?, 9007199254740991, 1.25, ?, ?, 'initial', 42)`,
    )
    .run(Buffer.from([0, 1, 255]), textId, workspace)
}

describe("movement key journal generation", () => {
  it("captures typed composite keys, applies backpressure, and retains an immutable outbox", () => {
    withDatabase((database) => {
      install(database)
      startCapture(database)

      insertRecord(database, "target")
      insertRecord(database, "neighbor", "workspace-b")
      database
        .prepare(`UPDATE "records" SET "payload" = 'changed' WHERE "text_id" = 'target'`)
        .run()

      const entries = database
        .prepare(
          `SELECT "sequence", "mutation_hint", "key_json", "key_shape_json", "schema_checksum"
           FROM "nozzle_movement_outbox" ORDER BY "sequence"`,
        )
        .all() as {
        readonly key_json: string
        readonly key_shape_json: string
        readonly mutation_hint: string
        readonly schema_checksum: string
        readonly sequence: number
      }[]
      expect(entries).toHaveLength(2)
      expect(entries.map((entry) => entry.mutation_hint)).toEqual(["upsert", "upsert"])
      expect(JSON.parse(entries[0]?.key_shape_json ?? "null")).toEqual(records.primaryColumns)
      expect(JSON.parse(entries[0]?.key_json ?? "null")).toEqual([
        { column: "blob_id", type: "blob", value: "0001ff" },
        { column: "integer_id", type: "integer", value: "9007199254740991" },
        { column: "real_id", type: "real", value: "1.25" },
        { column: "text_id", type: "text", value: "target" },
      ])
      expect(entries[0]?.schema_checksum).toBe(SCHEMA_CHECKSUM)

      expect(() =>
        database.prepare(`DELETE FROM "records" WHERE "text_id" = 'target'`).run(),
      ).toThrow(/NOZZLE_CAPTURE_BACKPRESSURE/u)
      expect(
        database.prepare(`SELECT "payload" FROM "records" WHERE "text_id" = 'target'`).get(),
      ).toEqual({
        payload: "changed",
      })

      database
        .prepare(
          `UPDATE "nozzle_movement_captures"
           SET "acknowledged_sequence" = 1, "updated_at_ms" = 2
           WHERE "operation_id" = 'movement-1'`,
        )
        .run()
      database.prepare(`DELETE FROM "records" WHERE "text_id" = 'target'`).run()
      expect(
        database
          .prepare(`SELECT "mutation_hint" FROM "nozzle_movement_outbox" ORDER BY "sequence"`)
          .all(),
      ).toEqual([
        { mutation_hint: "upsert" },
        { mutation_hint: "upsert" },
        { mutation_hint: "delete" },
      ])
      expect(() =>
        database
          .prepare(`UPDATE "nozzle_movement_outbox" SET "table_id" = 'other' WHERE "sequence" = 1`)
          .run(),
      ).toThrow(/NOZZLE_OUTBOX_IMMUTABLE/u)
      expect(() =>
        database.prepare(`DELETE FROM "nozzle_movement_outbox" WHERE "sequence" = 1`).run(),
      ).toThrow(/NOZZLE_OUTBOX_RETENTION/u)

      database
        .prepare(
          `UPDATE "nozzle_movement_captures"
           SET "state" = 'completed', "acknowledged_sequence" = 3, "updated_at_ms" = 3
           WHERE "operation_id" = 'movement-1'`,
        )
        .run()
      expect(database.prepare(`DELETE FROM "nozzle_movement_outbox"`).run().changes).toBe(3)
      expect(() => database.prepare(`DELETE FROM "nozzle_movement_captures"`).run()).toThrow(
        /NOZZLE_CAPTURE_PERSISTENT/u,
      )
    })
  })

  it.each([
    {
      other: Buffer.from([4, 5, 6]),
      partitionType: "binary" as const,
      value: Buffer.from([1, 2, 3]),
    },
    { other: 18, partitionType: "integer" as const, value: 17 },
    { other: "workspace-b", partitionType: "string" as const, value: "workspace-a" },
    {
      other: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      partitionType: "uuid" as const,
      value: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
    },
  ])("matches a $partitionType capture scope type-preservingly", ({
    other,
    partitionType,
    value,
  }) => {
    withDatabase((database) => {
      install(database, { ...records, partitionType })
      const columns = {
        binary: partitionType === "binary" ? value : null,
        integer: partitionType === "integer" ? value : null,
        string: partitionType === "string" ? value : null,
        uuid: partitionType === "uuid" ? String(value).toLowerCase() : null,
      }
      database
        .prepare(
          `INSERT INTO "nozzle_movement_captures"
           ("operation_id", "bucket_id", "scope_kind", "partition_type", "partition_binary",
            "partition_integer", "partition_string", "partition_uuid", "schema_id",
            "schema_checksum", "state", "start_sequence", "acknowledged_sequence",
            "max_pending_entries", "fencing_token", "created_at_ms", "updated_at_ms")
           VALUES ('typed-capture', 42, 'partition', ?, ?, ?, ?, ?, ?, ?, 'active',
             0, 0, 10, 1, 1, 1)`,
        )
        .run(
          partitionType,
          columns.binary,
          columns.integer,
          columns.string,
          columns.uuid,
          SCHEMA_ID,
          SCHEMA_CHECKSUM,
        )
      insertRecord(database, "target", value)
      insertRecord(database, "neighbor", other)
      expect(
        database.prepare(`SELECT count(*) AS "count" FROM "nozzle_movement_outbox"`).get(),
      ).toEqual({ count: 1 })
    })
  })

  it("captures every partition in an explicitly bucket-scoped movement", () => {
    withDatabase((database) => {
      install(database)
      database
        .prepare(
          `INSERT INTO "nozzle_movement_captures"
           ("operation_id", "bucket_id", "scope_kind", "partition_type", "partition_binary",
            "partition_integer", "partition_string", "partition_uuid", "schema_id",
            "schema_checksum", "state", "start_sequence", "acknowledged_sequence",
            "max_pending_entries", "fencing_token", "created_at_ms", "updated_at_ms")
           VALUES ('bucket-capture', 42, 'bucket', NULL, NULL, NULL, NULL, NULL, ?, ?,
             'active', 0, 0, 10, 1, 1, 1)`,
        )
        .run(SCHEMA_ID, SCHEMA_CHECKSUM)
      insertRecord(database, "first", "workspace-a")
      insertRecord(database, "second", "workspace-b")
      expect(
        database.prepare(`SELECT count(*) AS "count" FROM "nozzle_movement_outbox"`).get(),
      ).toEqual({ count: 2 })
    })
  })

  it("prevents primary-key changes and overlapping bucket or partition captures", () => {
    withDatabase((database) => {
      install(database)
      startCapture(database)
      insertRecord(database, "neighbor", "workspace-b")
      expect(() =>
        database
          .prepare(`UPDATE "records" SET "text_id" = 'changed' WHERE "text_id" = 'neighbor'`)
          .run(),
      ).toThrow(/NOZZLE_CAPTURE_PRIMARY_KEY_IMMUTABLE/u)

      startCapture(database, { operationId: "movement-2", partition: "workspace-b" })
      expect(() => startCapture(database, { operationId: "conflict" })).toThrow(
        /NOZZLE_CAPTURE_SCOPE_CONFLICT/u,
      )
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_movement_captures"
             ("operation_id", "bucket_id", "scope_kind", "partition_type", "partition_binary",
              "partition_integer", "partition_string", "partition_uuid", "schema_id",
              "schema_checksum", "state", "start_sequence", "acknowledged_sequence",
              "max_pending_entries", "fencing_token", "created_at_ms", "updated_at_ms")
             VALUES ('bucket-conflict', 42, 'bucket', NULL, NULL, NULL, NULL, NULL, ?, ?,
               'active', 0, 0, 10, 1, 1, 1)`,
          )
          .run(SCHEMA_ID, SCHEMA_CHECKSUM),
      ).toThrow(/NOZZLE_CAPTURE_SCOPE_CONFLICT/u)
    })
  })

  it("enforces capture acknowledgement, fencing, transition, and replay receipt invariants", () => {
    withDatabase((database) => {
      install(database)
      startCapture(database, { maximum: 10 })
      insertRecord(database, "target")
      for (const sql of [
        `UPDATE "nozzle_movement_captures" SET "bucket_id" = 43 WHERE "operation_id" = 'movement-1'`,
        `UPDATE "nozzle_movement_captures" SET "fencing_token" = 0 WHERE "operation_id" = 'movement-1'`,
        `UPDATE "nozzle_movement_captures" SET "updated_at_ms" = 0 WHERE "operation_id" = 'movement-1'`,
        `UPDATE "nozzle_movement_captures" SET "acknowledged_sequence" = 2 WHERE "operation_id" = 'movement-1'`,
        `UPDATE "nozzle_movement_captures" SET "state" = 'completed' WHERE "operation_id" = 'movement-1'`,
      ]) {
        expect(() => database.prepare(sql).run()).toThrow()
      }
      database
        .prepare(
          `UPDATE "nozzle_movement_captures"
           SET "state" = 'draining', "acknowledged_sequence" = 1, "fencing_token" = 2,
               "updated_at_ms" = 2 WHERE "operation_id" = 'movement-1'`,
        )
        .run()
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_movement_captures" SET "state" = 'active'
             WHERE "operation_id" = 'movement-1'`,
          )
          .run(),
      ).toThrow(/NOZZLE_CAPTURE_STATE_TRANSITION/u)
      database
        .prepare(
          `INSERT INTO "nozzle_movement_replay_receipts"
           ("operation_id", "source_sequence", "table_id", "key_json", "mutation_hint",
            "result_checksum", "applied_at_ms")
           VALUES ('movement-1', 1, 'records', '[]', 'upsert', 'result', 2)`,
        )
        .run()
      expect(() =>
        database
          .prepare(`UPDATE "nozzle_movement_replay_receipts" SET "result_checksum" = 'other'`)
          .run(),
      ).toThrow(/NOZZLE_REPLAY_RECEIPT_IMMUTABLE/u)
      expect(() => database.prepare(`DELETE FROM "nozzle_movement_replay_receipts"`).run()).toThrow(
        /NOZZLE_REPLAY_RECEIPT_PERSISTENT/u,
      )
      expect(
        database
          .prepare(
            `INSERT INTO "nozzle_movement_replay_receipts"
             ("operation_id", "source_sequence", "table_id", "key_json", "mutation_hint",
              "result_checksum", "applied_at_ms")
             VALUES ('movement-1', 1, 'records', '[]', 'upsert', 'result', 2)
             ON CONFLICT ("operation_id", "source_sequence") DO NOTHING`,
          )
          .run().changes,
      ).toBe(0)
    })
  })

  it("emits deterministic, immutable, safely quoted SQL for every partition type", () => {
    const tables: MovementCaptureTableSpec[] = [
      {
        partitionColumn: 'tenant"id',
        partitionType: "string",
        primaryColumns: ["id'quoted"],
        tableName: 'Beta"table',
      },
      {
        partitionColumn: "tenant_id",
        partitionType: "binary",
        primaryColumns: ["id"],
        tableName: "alpha",
      },
      {
        partitionColumn: "tenant_id",
        partitionType: "integer",
        primaryColumns: ["id"],
        tableName: "integer_table",
      },
      {
        partitionColumn: "tenant_id",
        partitionType: "uuid",
        primaryColumns: ["id"],
        tableName: "uuid_table",
      },
    ]
    const left = generateMovementCaptureSql({ schemaId: SCHEMA_ID, tables })
    const right = generateMovementCaptureSql({ schemaId: SCHEMA_ID, tables: [...tables].reverse() })
    expect(left).toEqual(right)
    expect(left.tables.map((table) => table.tableName)).toEqual([
      "alpha",
      'Beta"table',
      "integer_table",
      "uuid_table",
    ])
    expect(left.sql).toContain('ON "Beta""table"')
    expect(left.sql).toContain("'id''quoted'")
    expect(left.sql.endsWith("\n")).toBe(true)
    expect(Object.isFrozen(left)).toBe(true)
    expect(Object.isFrozen(left.statements)).toBe(true)
    expect(Object.isFrozen(left.tables)).toBe(true)
    expect(Object.isFrozen(left.tables[0]?.primaryColumns)).toBe(true)
    expect(
      generateMovementCaptureSql({
        schemaId: SCHEMA_ID,
        tables: [
          { ...records, tableName: "aa" },
          { ...records, tableName: "a" },
        ],
      }).tables.map((table) => table.tableName),
    ).toEqual(["a", "aa"])
  })

  it("rejects malformed capture specifications", () => {
    const base: MovementCaptureSqlInput = { schemaId: SCHEMA_ID, tables: [records] }
    const invalid: unknown[] = [
      null,
      [],
      { ...base, schemaId: "" },
      { ...base, schemaId: "a".repeat(129) },
      { ...base, tables: [] },
      { ...base, tables: Array.from({ length: 1_001 }, () => records) },
      { ...base, tables: [null] },
      { ...base, tables: [[]] },
      { ...base, tables: [{ ...records, tableName: "" }] },
      { ...base, tables: [{ ...records, tableName: undefined }] },
      { ...base, tables: [{ ...records, tableName: "\ud800" }] },
      { ...base, tables: [{ ...records, tableName: "\udc00" }] },
      { ...base, tables: [{ ...records, tableName: "bad\nname" }] },
      { ...base, tables: [{ ...records, tableName: "x".repeat(256) }] },
      { ...base, tables: [{ ...records, tableName: "sqlite_bad" }] },
      { ...base, tables: [{ ...records, tableName: "nozzle_bad" }] },
      { ...base, tables: [{ ...records, partitionType: "float" }] },
      { ...base, tables: [{ ...records, primaryColumns: null }] },
      { ...base, tables: [{ ...records, primaryColumns: [] }] },
      { ...base, tables: [{ ...records, primaryColumns: ["id", "ID"] }] },
      { ...base, tables: [records, { ...records, tableName: "RECORDS" }] },
    ]
    for (const input of invalid) {
      expect(() => generateMovementCaptureSql(input as MovementCaptureSqlInput)).toThrow()
    }
    expect(() =>
      generateMovementCaptureSql({
        schemaId: SCHEMA_ID,
        tables: [{ ...records, tableName: "😀" }],
      }),
    ).not.toThrow()
  })
})
