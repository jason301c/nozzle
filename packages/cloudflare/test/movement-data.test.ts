import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import { SchemaRegistry, type TableMetadata } from "@nozzle/drizzle"
import { blob, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import {
  compileMovementDelete,
  compileMovementPage,
  compileMovementReplayRead,
  compileMovementReplayReceipt,
  compileMovementUpsert,
  decodeMovementKey,
  decodeMovementPage,
} from "../src/movement-data.js"

const rows = sqliteTable(
  "move_rows",
  {
    id: blob({ mode: "buffer" }).notNull(),
    sequence: integer().notNull(),
    workspaceId: text("workspace_id").notNull(),
    payload: text().notNull(),
  },
  (table) => [primaryKey({ columns: [table.id, table.sequence] })],
)
const globalRows = sqliteTable("global_rows", { id: text().primaryKey() })
const registry = new SchemaRegistry({ partitionKey: "workspaceId", schema: { rows } })
const table = registry.table(rows)

function keyStorage(
  first: "blob" | "integer" | "real" | "text",
  second: "blob" | "integer" | "real" | "text",
): TableMetadata {
  return {
    ...table,
    primaryColumns: [
      { ...(table.primaryColumns[0] as (typeof table.primaryColumns)[number]), storageType: first },
      {
        ...(table.primaryColumns[1] as (typeof table.primaryColumns)[number]),
        storageType: second,
      },
    ],
  }
}

function sqliteBindings(values: readonly unknown[]): Record<string, SQLInputValue> {
  return Object.fromEntries(
    values.map((value, index) => [
      `?${index + 1}`,
      value instanceof Uint8Array ? Buffer.from(value) : (value as SQLInputValue),
    ]),
  )
}

function withDatabase(run: (database: DatabaseSync) => void): void {
  const database = new DatabaseSync(":memory:")
  try {
    database.exec(`CREATE TABLE "move_rows" (
      "id" BLOB NOT NULL,
      "sequence" INTEGER NOT NULL,
      "workspace_id" TEXT NOT NULL,
      "payload" TEXT NOT NULL,
      "__nozzle_bucket" INTEGER NOT NULL,
      PRIMARY KEY ("id", "sequence")
    );`)
    for (const [id, sequence, workspace, payload, bucket] of [
      [Buffer.from([1]), 1, "workspace-a", "a", 42],
      [Buffer.from([1]), 2, "workspace-a", "b", 42],
      [Buffer.from([2]), 1, "workspace-a", "c", 42],
      [Buffer.from([3]), 1, "workspace-b", "neighbor", 42],
      [Buffer.from([4]), 1, "workspace-a", "other-bucket", 43],
    ] as const) {
      database
        .prepare(
          `INSERT INTO "move_rows"
           ("id", "sequence", "workspace_id", "payload", "__nozzle_bucket")
           VALUES (?, ?, ?, ?, ?)`,
        )
        .run(id, sequence, workspace, payload, bucket)
    }
    run(database)
  } finally {
    database.close()
  }
}

describe("movement data compiler", () => {
  it("pages deterministically by typed composite key without offset", () => {
    withDatabase((database) => {
      const first = compileMovementPage({
        limit: 2,
        maxBytes: 10_000,
        scope: { bucketId: 42, partitionValue: "workspace-a" },
        table,
      })
      const firstRows = database.prepare(first.sql).all(sqliteBindings(first.params)) as Record<
        string,
        unknown
      >[]
      expect(first.sql).not.toContain("OFFSET")
      expect(firstRows.map((row) => row.payload)).toEqual(["a", "b"])
      const decodedFirst = decodeMovementPage(firstRows, 10_000)
      expect(decodedFirst.rows.map((row) => row.payload)).toEqual(["a", "b"])
      expect(decodedFirst.transferredBytes).toBeGreaterThan(0)
      const cursor = firstRows[1]?.__nozzle_cursor_json as string
      expect(decodedFirst.nextCursor).toBe(cursor)
      expect(JSON.parse(cursor)).toEqual([
        { column: "id", type: "blob", value: "01" },
        { column: "sequence", type: "integer", value: "2" },
      ])

      const second = compileMovementPage({
        afterKeyJson: cursor,
        limit: 2,
        maxBytes: 10_000,
        scope: { bucketId: 42, partitionValue: "workspace-a" },
        table,
      })
      expect(
        database
          .prepare(second.sql)
          .all(sqliteBindings(second.params))
          .map((row) => row.payload),
      ).toEqual(["c"])

      const bucketPage = compileMovementPage({
        limit: 10,
        maxBytes: 10_000,
        scope: { bucketId: 42 },
        table,
      })
      expect(
        database
          .prepare(bucketPage.sql)
          .all(sqliteBindings(bucketPage.params))
          .map((row) => row.payload),
      ).toEqual(["a", "b", "c", "neighbor"])

      const byteBounded = compileMovementPage({
        limit: 10,
        maxBytes: 500,
        scope: { bucketId: 42 },
        table,
      })
      const boundedRows = database.prepare(byteBounded.sql).all(sqliteBindings(byteBounded.params))
      expect(boundedRows).toHaveLength(1)
      expect(decodeMovementPage(boundedRows, 500).transferredBytes).toBeLessThanOrEqual(500)
    })
  })

  it("reads current source state from a typed journal key", () => {
    withDatabase((database) => {
      const key = JSON.stringify([
        { column: "id", type: "blob", value: "01" },
        { column: "sequence", type: "integer", value: "2" },
      ])
      const read = compileMovementReplayRead({ keyJson: key, sourceBucketId: 42, table })
      expect(database.prepare(read.sql).all(sqliteBindings(read.params))).toEqual([
        {
          __nozzle_bucket: 42,
          id: new Uint8Array([1]),
          payload: "b",
          sequence: 2,
          workspace_id: "workspace-a",
        },
      ])
      const absent = compileMovementReplayRead({
        keyJson: JSON.stringify([
          { column: "id", type: "blob", value: "ff" },
          { column: "sequence", type: "integer", value: "2" },
        ]),
        sourceBucketId: 42,
        table,
      })
      expect(database.prepare(absent.sql).all(sqliteBindings(absent.params))).toEqual([])
    })
  })

  it("compiles exact admin-view upsert, delete, and idempotent receipt statements", () => {
    const row = {
      __nozzle_bucket: 42,
      id: new Uint8Array([1]),
      payload: "payload",
      sequence: 2,
      workspace_id: "workspace-a",
    }
    const upsert = compileMovementUpsert({
      capabilityToken: "capability",
      destinationBucketId: 77,
      row,
      table,
    })
    expect(upsert.sql).toContain('INSERT INTO "nozzle_operation_6d6f76655f726f7773"')
    expect(upsert.params).toEqual([
      new Uint8Array([1]),
      2,
      "workspace-a",
      "payload",
      77,
      "capability",
      "upsert",
    ])
    const keyJson = JSON.stringify([
      { column: "id", type: "blob", value: "01" },
      { column: "sequence", type: "integer", value: "2" },
    ])
    const deletion = compileMovementDelete({
      capabilityToken: "capability",
      destinationBucketId: 77,
      keyJson,
      table,
    })
    expect(deletion.params).toEqual([new Uint8Array([1]), "2", 77, "capability", "delete"])
    const receipt = compileMovementReplayReceipt({
      appliedAtMs: 5,
      keyJson,
      mutationHint: "upsert",
      operationId: "movement-1",
      resultChecksum: "result",
      sourceSequence: 3,
      tableId: "move_rows",
    })
    expect(receipt.sql).toContain("ON CONFLICT")
    expect(receipt.params).toEqual(["movement-1", 3, "move_rows", keyJson, "upsert", "result", 5])
  })

  it("validates every typed key representation and rejects malformed evidence", () => {
    const twoColumn = (components: readonly unknown[]) => JSON.stringify(components)
    const valid = [
      {
        components: [
          { column: "id", type: "text", value: "text" },
          { column: "sequence", type: "real", value: "1.25" },
        ],
        metadata: keyStorage("text", "real"),
      },
      {
        components: [
          { column: "id", type: "integer", value: "-9223372036854775808" },
          { column: "sequence", type: "integer", value: "9223372036854775807" },
        ],
        metadata: keyStorage("integer", "integer"),
      },
    ]
    for (const entry of valid) {
      expect(decodeMovementKey(entry.metadata, twoColumn(entry.components))).toHaveLength(2)
    }
    expect(() =>
      compileMovementPage({
        afterKeyJson: twoColumn([
          { column: "id", type: "text", value: "text" },
          { column: "sequence", type: "real", value: "1.25" },
        ]),
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 42 },
        table: keyStorage("text", "real"),
      }),
    ).not.toThrow()
    expect(() =>
      compileMovementPage({
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 42 },
        table: {
          ...table,
          primaryColumns: [table.partitionColumn as NonNullable<(typeof table)["partitionColumn"]>],
        },
      }),
    ).not.toThrow()
    const malformed: unknown[] = [
      "{",
      "{}",
      "[]",
      twoColumn([null, null]),
      twoColumn([
        { column: "id", type: "blob", value: "01", extra: true },
        { column: "sequence", type: "integer", value: "1" },
      ]),
      twoColumn([
        { column: "wrong", type: "blob", value: "01" },
        { column: "sequence", type: "integer", value: "1" },
      ]),
      twoColumn([
        { column: "id", type: "bad", value: "01" },
        { column: "sequence", type: "integer", value: "1" },
      ]),
      twoColumn([
        { column: "id", type: "blob", value: 1 },
        { column: "sequence", type: "integer", value: "1" },
      ]),
      twoColumn([
        { column: "id", type: "text", value: "01" },
        { column: "sequence", type: "integer", value: "1" },
      ]),
      twoColumn([
        { column: "id", type: "blob", value: "0" },
        { column: "sequence", type: "integer", value: "1" },
      ]),
    ]
    for (const keyJson of malformed) {
      expect(() => decodeMovementKey(table, keyJson as string)).toThrow()
    }
    const malformedInteger = ["01", "-0", "9223372036854775808"]
    for (const value of malformedInteger) {
      expect(() =>
        decodeMovementKey(
          keyStorage("integer", "integer"),
          twoColumn([
            { column: "id", type: "integer", value },
            { column: "sequence", type: "integer", value: "1" },
          ]),
        ),
      ).toThrow()
    }
    for (const value of ["Infinity", " 1", ""]) {
      expect(() =>
        decodeMovementKey(
          keyStorage("real", "integer"),
          twoColumn([
            { column: "id", type: "real", value },
            { column: "sequence", type: "integer", value: "1" },
          ]),
        ),
      ).toThrow()
    }
  })

  it("rejects invalid scopes, rows, receipts, and table shapes", () => {
    for (const limit of [0, 1_001, Number.NaN]) {
      expect(() =>
        compileMovementPage({ limit, maxBytes: 10_000, scope: { bucketId: 42 }, table }),
      ).toThrow()
    }
    for (const maxBytes of [0, 8 * 1024 * 1024 + 1, Number.NaN]) {
      expect(() =>
        compileMovementPage({ limit: 1, maxBytes, scope: { bucketId: 42 }, table }),
      ).toThrow()
      expect(() => decodeMovementPage([], maxBytes)).toThrow()
    }
    expect(decodeMovementPage([], 1_000)).toEqual({ rows: [], transferredBytes: 0 })
    for (const rows of [
      null,
      [null],
      [{ __nozzle_cursor_json: "", __nozzle_row_bytes: 1 }],
      [{ __nozzle_cursor_json: "cursor", __nozzle_row_bytes: Number.NaN }],
      [{ __nozzle_cursor_json: "cursor", __nozzle_row_bytes: -1 }],
    ]) {
      expect(() => decodeMovementPage(rows as never, 1_000)).toThrow()
    }
    expect(() =>
      decodeMovementPage([{ __nozzle_cursor_json: "cursor", __nozzle_row_bytes: 1_001 }], 1_000),
    ).toThrow("byte budget")
    for (const bucketId of [-1, Number.NaN]) {
      expect(() =>
        compileMovementPage({ limit: 1, maxBytes: 10_000, scope: { bucketId }, table }),
      ).toThrow()
      expect(() =>
        compileMovementReplayRead({ keyJson: "[]", sourceBucketId: bucketId, table }),
      ).toThrow()
    }
    expect(() =>
      compileMovementUpsert({
        capabilityToken: "",
        destinationBucketId: 1,
        row: {},
        table,
      }),
    ).toThrow("capability token")
    for (const row of [null, {}, { id: new Uint8Array(), sequence: 1 }]) {
      expect(() =>
        compileMovementUpsert({
          capabilityToken: "cap",
          destinationBucketId: 1,
          row: row as never,
          table,
        }),
      ).toThrow()
    }
    const baseRow = {
      __nozzle_bucket: 42,
      id: new Uint8Array([1]),
      payload: "payload",
      sequence: 2,
      workspace_id: "workspace-a",
    }
    for (const value of [Number.NaN, Number.POSITIVE_INFINITY, {}, []]) {
      expect(() =>
        compileMovementUpsert({
          capabilityToken: "cap",
          destinationBucketId: 1,
          row: { ...baseRow, payload: value },
          table,
        }),
      ).toThrow()
    }
    expect(
      compileMovementUpsert({
        capabilityToken: "cap",
        destinationBucketId: 1,
        row: { ...baseRow, id: new Uint8Array([1]).buffer },
        table,
      }).params[0],
    ).toEqual(new Uint8Array([1]))
    expect(() =>
      compileMovementUpsert({
        capabilityToken: "cap",
        destinationBucketId: 1,
        row: Object.assign(Object.create(null), baseRow) as Readonly<Record<string, unknown>>,
        table,
      }),
    ).not.toThrow()

    const global = new SchemaRegistry({
      globalTables: [globalRows],
      partitionKey: "workspaceId",
      schema: { globalRows },
    }).table(globalRows)
    expect(() =>
      compileMovementPage({
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 1 },
        table: global,
      }),
    ).toThrow("sharded table")
    expect(() =>
      compileMovementPage({
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 1 },
        table: { ...table, primaryColumns: [] },
      }),
    ).toThrow("stable primary key")
    expect(() =>
      compileMovementPage({
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 1 },
        table: {
          ...table,
          columns: Array.from(
            { length: 97 },
            () => table.columns[0] as (typeof table.columns)[number],
          ),
        },
      }),
    ).toThrow("binding budget")
    expect(() =>
      compileMovementPage({
        limit: 1,
        maxBytes: 10_000,
        scope: { bucketId: 1, partitionValue: "workspace" },
        table: { ...table, partitionColumn: undefined } as unknown as TableMetadata,
      }),
    ).toThrow("partition metadata")

    const receipt = {
      appliedAtMs: 1,
      keyJson: "[]",
      mutationHint: "delete" as const,
      operationId: "operation",
      resultChecksum: "result",
      sourceSequence: 1,
      tableId: "table",
    }
    for (const invalid of [
      { ...receipt, operationId: "" },
      { ...receipt, tableId: "" },
      { ...receipt, keyJson: "" },
      { ...receipt, resultChecksum: "" },
      { ...receipt, appliedAtMs: -1 },
      { ...receipt, sourceSequence: 0 },
      { ...receipt, sourceSequence: Number.NaN },
    ]) {
      expect(() => compileMovementReplayReceipt(invalid)).toThrow()
    }
  })
})
