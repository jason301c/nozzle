import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { compilePlan } from "../src/compiler.js"
import { type D1ResultLike, decodeD1Result } from "../src/direct.js"
import { and, gte, lt, lte } from "../src/expression.js"
import { buildSelectPlan, buildUpdatePlan, type ScopedRoute } from "../src/plan.js"
import { SchemaRegistry } from "../src/schema.js"

const records = sqliteTable("compiler_edge_records", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
  note: text(),
  score: integer(),
})

const registry = new SchemaRegistry({ schema: { records }, partitionKey: "workspaceId" })
const route: ScopedRoute = {
  bucketId: 17,
  partitionDigestHex: "ab".repeat(32),
  partitionValue: "workspace-edge",
  routeEpoch: 4,
  shardId: "shard-edge",
}

describe("compiler edge cases", () => {
  it("compiles inclusive and exclusive numeric bounds in stable parameter order", () => {
    const lowerInclusive = gte(records.score, 10)
    const upperExclusive = lt(records.score, 20)
    const upperInclusive = lte(records.score, 19)
    const plan = buildSelectPlan(registry, {
      predicate: and(lowerInclusive, upperExclusive, upperInclusive),
      route,
      schemaId: "schema-edge-v1",
      table: records,
    })

    const compiled = compilePlan(plan)

    expect(Object.isFrozen(lowerInclusive)).toBe(true)
    expect(Object.isFrozen(upperExclusive)).toBe(true)
    expect(Object.isFrozen(upperInclusive)).toBe(true)
    expect(compiled.data.sql).toContain('("score" >= ?7 AND "score" < ?8 AND "score" <= ?9)')
    expect(compiled.data.params.slice(6)).toEqual([10, 20, 19])
  })

  it("sorts and binds every assignment in a multi-column update", () => {
    const plan = buildUpdatePlan(registry, {
      route,
      schemaId: "schema-edge-v1",
      table: records,
      values: { score: 21, name: "ordered" },
    })

    const compiled = compilePlan(plan)

    expect(compiled.data.sql).toContain('SET "name" = ?1, "score" = ?2')
    expect(compiled.data.params.slice(0, 2)).toEqual(["ordered", 21])
  })

  it("preserves SQL null for nullable selected columns without invoking a driver mapper", () => {
    const plan = buildSelectPlan(registry, {
      route,
      schemaId: "schema-edge-v1",
      table: records,
    })
    const data: D1ResultLike = {
      meta: Object.freeze({ rows_read: 1, rows_written: 0 }),
      results: Object.freeze([
        Object.freeze({
          id: "record-1",
          name: "nullable",
          note: null,
          score: 3,
          workspaceId: "workspace-edge",
        }),
      ]),
      success: true,
    }

    expect(decodeD1Result(plan, data)).toEqual([
      {
        id: "record-1",
        name: "nullable",
        note: null,
        score: 3,
        workspaceId: "workspace-edge",
      },
    ])
  })
})
