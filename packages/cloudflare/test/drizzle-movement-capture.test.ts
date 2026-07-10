import { SchemaRegistry } from "@nozzle/drizzle"
import { blob, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { generateDrizzleMovementCaptureSql } from "../src/drizzle-movement-capture.js"

const projects = sqliteTable(
  "projects",
  {
    id: blob({ mode: "buffer" }).notNull(),
    version: integer().notNull(),
    workspaceId: text("workspace_id").notNull(),
  },
  (table) => [primaryKey({ columns: [table.id, table.version] })],
)
const globalLog = sqliteTable("global_log", { id: text().primaryKey() })

describe("Drizzle movement capture compilation", () => {
  it("derives sharded tables, SQL names, and ordered composite primary keys", () => {
    const registry = new SchemaRegistry({
      globalTables: [globalLog],
      partitionKey: "workspaceId",
      schema: { projects, globalLog },
    })
    const generated = generateDrizzleMovementCaptureSql({
      partitionKeyType: "string",
      registry,
      schemaId: "application-v1",
    })
    expect(generated.tables).toEqual([
      {
        partitionColumn: "workspace_id",
        partitionType: "string",
        primaryColumns: ["id", "version"],
        tableName: "projects",
      },
    ])
    expect(generated.sql).not.toContain('ON "global_log"')
  })

  it("rejects malformed inputs and incompatible schema metadata", () => {
    const registry = new SchemaRegistry({ partitionKey: "workspaceId", schema: { projects } })
    expect(() => generateDrizzleMovementCaptureSql(null as never)).toThrow(
      "input must be an object",
    )
    expect(() =>
      generateDrizzleMovementCaptureSql({
        partitionKeyType: "string",
        registry: {} as never,
        schemaId: "v1",
      }),
    ).toThrow("schema registry")
    expect(() =>
      generateDrizzleMovementCaptureSql({
        partitionKeyType: "float" as never,
        registry,
        schemaId: "v1",
      }),
    ).toThrow("partition type is unsupported")
    expect(() =>
      generateDrizzleMovementCaptureSql({
        partitionKeyType: "integer",
        registry,
        schemaId: "v1",
      }),
    ).toThrow("does not match")
  })

  it("fails closed if a registry violates partition metadata invariants", () => {
    class BrokenRegistry extends SchemaRegistry {
      override tables(): ReturnType<SchemaRegistry["tables"]> {
        const table = super.tables()[0]
        if (!table) throw new Error("fixture")
        const { partitionColumn: _partition, ...rest } = table
        return [{ ...rest }]
      }
    }
    expect(() =>
      generateDrizzleMovementCaptureSql({
        partitionKeyType: "string",
        registry: new BrokenRegistry({ partitionKey: "workspaceId", schema: { projects } }),
        schemaId: "v1",
      }),
    ).toThrow("lacks partition metadata")

    expect(() =>
      generateDrizzleMovementCaptureSql({
        partitionKeyType: "string",
        registry: new SchemaRegistry({
          globalTables: [globalLog],
          partitionKey: "workspaceId",
          schema: { globalLog },
        }),
        schemaId: "v1",
      }),
    ).toThrow("tables must not be empty")
  })
})
