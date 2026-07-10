import { SchemaRegistry } from "@nozzle/drizzle"
import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { generateDrizzleShardGuardSql } from "../src/drizzle-shard-guards.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
})
const globalLog = sqliteTable("global_log", { message: text().notNull() })

describe("Drizzle shard guard compilation", () => {
  it("derives only sharded table and SQL partition names from the compiled registry", () => {
    const registry = new SchemaRegistry({
      schema: { projects, globalLog },
      partitionKey: "workspaceId",
      globalTables: [globalLog],
    })
    const generated = generateDrizzleShardGuardSql({
      registry,
      partitionKeyType: "string",
      schemaId: "application-v1",
    })

    expect(generated.tables).toEqual([
      { tableName: "projects", partitionColumn: "workspace_id", partitionType: "string" },
    ])
    expect(generated.sql).toContain('BEFORE INSERT ON "projects"')
    expect(generated.sql).not.toContain('BEFORE INSERT ON "global_log"')
  })

  it("accepts exact integer and binary Drizzle partition modes", () => {
    const integerRows = sqliteTable("integer_rows", {
      id: text().primaryKey(),
      workspaceId: integer("workspace_id").notNull(),
    })
    const binaryRows = sqliteTable("binary_rows", {
      id: text().primaryKey(),
      workspaceId: blob("workspace_id", { mode: "buffer" }).notNull(),
    })

    expect(
      generateDrizzleShardGuardSql({
        registry: new SchemaRegistry({
          schema: { integerRows },
          partitionKey: "workspaceId",
        }),
        partitionKeyType: "integer",
        schemaId: "integer-v1",
      }).tables[0],
    ).toMatchObject({ partitionType: "integer" })
    expect(
      generateDrizzleShardGuardSql({
        registry: new SchemaRegistry({
          schema: { binaryRows },
          partitionKey: "workspaceId",
        }),
        partitionKeyType: "binary",
        schemaId: "binary-v1",
      }).tables[0],
    ).toMatchObject({ partitionType: "binary" })
  })

  it("rejects malformed registries, partition types, and mismatched Drizzle modes", () => {
    const registry = new SchemaRegistry({ schema: { projects }, partitionKey: "workspaceId" })
    expect(() => generateDrizzleShardGuardSql(null as never)).toThrow("input must be an object")
    expect(() =>
      generateDrizzleShardGuardSql({
        registry: {} as never,
        partitionKeyType: "string",
        schemaId: "v1",
      }),
    ).toThrow("compiled Nozzle schema registry")
    expect(() =>
      generateDrizzleShardGuardSql({
        registry,
        partitionKeyType: "float" as never,
        schemaId: "v1",
      }),
    ).toThrow("partition-key type is unsupported")
    expect(() =>
      generateDrizzleShardGuardSql({
        registry,
        partitionKeyType: "integer",
        schemaId: "v1",
      }),
    ).toThrow("does not match the Drizzle column mode")
  })

  it("rejects fleets with no sharded tables instead of emitting unguarded metadata", () => {
    const registry = new SchemaRegistry({
      schema: { globalLog },
      partitionKey: "workspaceId",
      globalTables: [globalLog],
    })
    expect(() =>
      generateDrizzleShardGuardSql({
        registry,
        partitionKeyType: "string",
        schemaId: "global-only-v1",
      }),
    ).toThrow("at least one sharded table")
  })

  it("fails closed if a registry implementation violates its sharded-table invariant", () => {
    class BrokenRegistry extends SchemaRegistry {
      override tables(): ReturnType<SchemaRegistry["tables"]> {
        const table = super.tables()[0]
        if (!table) throw new Error("fixture requires one table")
        const { partitionColumn: _partitionColumn, ...withoutPartitionColumn } = table
        return [
          {
            ...withoutPartitionColumn,
          },
        ]
      }
    }
    const registry = new BrokenRegistry({ schema: { projects }, partitionKey: "workspaceId" })

    expect(() =>
      generateDrizzleShardGuardSql({
        registry,
        partitionKeyType: "string",
        schemaId: "broken-v1",
      }),
    ).toThrow("missing compiled partition metadata")
  })
})
