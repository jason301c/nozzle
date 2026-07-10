import { SchemaRegistry } from "@nozzle/drizzle"
import { primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { generateDrizzleMovementTransferSql } from "../src/drizzle-movement-transfer.js"

const projects = sqliteTable(
  "projects",
  {
    id: text().notNull(),
    workspaceId: text("workspace_id").notNull(),
    payload: text().notNull(),
  },
  (table) => [primaryKey({ columns: [table.id, table.workspaceId] })],
)
const globalLog = sqliteTable("global_log", { id: text().primaryKey() })

describe("Drizzle movement transfer compilation", () => {
  it("derives all application columns and keys from sharded tables only", () => {
    const generated = generateDrizzleMovementTransferSql({
      registry: new SchemaRegistry({
        globalTables: [globalLog],
        partitionKey: "workspaceId",
        schema: { projects, globalLog },
      }),
    })
    expect(generated.tables).toEqual([
      {
        columns: ["id", "workspace_id", "payload"],
        primaryColumns: ["id", "workspace_id"],
        tableName: "projects",
      },
    ])
  })

  it("rejects malformed registries and fleets without sharded tables", () => {
    expect(() => generateDrizzleMovementTransferSql(null as never)).toThrow(
      "input must be an object",
    )
    expect(() => generateDrizzleMovementTransferSql({ registry: {} as never })).toThrow(
      "requires a schema registry",
    )
    expect(() =>
      generateDrizzleMovementTransferSql({
        registry: new SchemaRegistry({
          globalTables: [globalLog],
          partitionKey: "workspaceId",
          schema: { globalLog },
        }),
      }),
    ).toThrow("tables must not be empty")
  })
})
