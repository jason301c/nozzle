import { sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import type { D1BindingValue } from "../src/compiler.js"
import {
  type D1DatabaseLike,
  type D1PreparedStatementLike,
  type D1ResultLike,
  executeDirect,
} from "../src/direct.js"
import { buildDeletePlan, buildSelectPlan } from "../src/plan.js"
import { SchemaRegistry } from "../src/schema.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
})
const registry = new SchemaRegistry({ schema: { projects }, partitionKey: "workspaceId" })
const PARTITION_DIGEST = "11".repeat(32)
const plan = buildSelectPlan(registry, {
  table: projects,
  schemaId: "v1",
  route: {
    bucketId: 1,
    partitionDigestHex: PARTITION_DIGEST,
    partitionValue: "workspace-fictional",
    routeEpoch: 2,
    shardId: "shard-fictional",
  },
})

class Statement implements D1PreparedStatementLike {
  readonly params: unknown[] = []
  constructor(readonly sql: string) {}
  bind(...values: readonly D1BindingValue[]): D1PreparedStatementLike {
    this.params.push(...values)
    return this
  }
}

class FakeDatabase implements D1DatabaseLike {
  readonly statements: Statement[] = []
  constructor(readonly results: readonly D1ResultLike[]) {}
  prepare(sql: string): D1PreparedStatementLike {
    const statement = new Statement(sql)
    this.statements.push(statement)
    return statement
  }
  async batch<T>(): Promise<readonly D1ResultLike<T>[]> {
    return this.results as readonly D1ResultLike<T>[]
  }
}

const meta = { rows_read: 1, rows_written: 0 }

describe("direct D1 execution", () => {
  it("executes authorization and data in one atomic batch", async () => {
    const data = {
      success: true,
      results: [{ id: "project-1", workspaceId: "workspace-fictional" }],
      meta,
    } as const
    const database = new FakeDatabase([{ success: true, results: [{ routeEpoch: 2 }], meta }, data])
    await expect(executeDirect(database, plan)).resolves.toEqual([
      { id: "project-1", workspaceId: "workspace-fictional" },
    ])
    expect(database.statements).toHaveLength(2)
    expect(database.statements[0]?.params).toEqual([1, 2, "v1", PARTITION_DIGEST])
    expect(database.statements[1]?.sql).toContain('FROM "projects"')
  })

  it("fails closed when local ownership rejects the route", async () => {
    const database = new FakeDatabase([
      { success: true, results: [], meta },
      { success: true, results: [], meta },
    ])
    await expect(executeDirect(database, plan)).rejects.toMatchObject({
      code: "StaleRouteRejectedError",
      details: { bucketId: 1, routeEpoch: 2, shardId: "shard-fictional" },
    })
  })

  it("rejects incomplete D1 batch results", async () => {
    const database = new FakeDatabase([{ success: true, results: [{ routeEpoch: 2 }], meta }])
    await expect(executeDirect(database, plan)).rejects.toMatchObject({
      code: "ShardUnavailableError",
    })
  })

  it("preserves D1 mutation metadata while decoding only selected rows", async () => {
    const mutation = buildDeletePlan(registry, {
      table: projects,
      schemaId: "v1",
      route: {
        bucketId: 1,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 2,
        shardId: "shard-fictional",
      },
    })
    const data = { success: true, results: [], meta: { rows_read: 2, rows_written: 1 } } as const
    const database = new FakeDatabase([{ success: true, results: [{ routeEpoch: 2 }], meta }, data])
    await expect(executeDirect(database, mutation)).resolves.toBe(data)
  })

  it("fails closed on malformed or incomplete selected rows", async () => {
    for (const results of [[null], [{ id: "project-1" }]]) {
      const database = new FakeDatabase([
        { success: true, results: [{ routeEpoch: 2 }], meta },
        { success: true, results, meta } as D1ResultLike,
      ])
      await expect(executeDirect(database, plan)).rejects.toMatchObject({
        code: "SchemaDriftError",
      })
    }
  })
})
