import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, expectTypeOf, it, vi } from "vitest"
import type { D1BindingValue } from "../src/compiler.js"
import type { D1DatabaseLike, D1PreparedStatementLike, D1ResultLike } from "../src/direct.js"
import { eq } from "../src/expression.js"
import type { ExecutionPlan } from "../src/plan.js"
import { SchemaRegistry } from "../src/schema.js"
import { createScopedDatabase, type PlannedQuery, type ScopedPlanTransport } from "../src/scoped.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
})
const typedRecords = sqliteTable("typed_records", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  active: integer({ mode: "boolean" }).notNull(),
  createdAt: integer("created_at", { mode: "timestamp_ms" }).notNull(),
  settings: text({ mode: "json" }).notNull(),
  payload: blob({ mode: "buffer" }).notNull(),
})
const registry = new SchemaRegistry({
  schema: { projects, typedRecords },
  partitionKey: "workspaceId",
})
const PARTITION_DIGEST = "11".repeat(32)
const meta = { rows_read: 0, rows_written: 0 }

class Statement implements D1PreparedStatementLike {
  readonly params: unknown[] = []
  constructor(readonly sql: string) {}
  bind(...values: readonly D1BindingValue[]): D1PreparedStatementLike {
    this.params.push(...values)
    return this
  }
}

class Database implements D1DatabaseLike {
  readonly batchSizes: number[] = []
  readonly statements: Statement[] = []
  results: readonly D1ResultLike[] = []

  prepare(sql: string): D1PreparedStatementLike {
    const statement = new Statement(sql)
    this.statements.push(statement)
    return statement
  }

  async batch<T>(
    statements: readonly D1PreparedStatementLike[],
  ): Promise<readonly D1ResultLike<T>[]> {
    this.batchSizes.push(statements.length)
    return this.results as readonly D1ResultLike<T>[]
  }
}

function create(
  database: Database,
  resolveRoute = vi.fn(async () => ({
    bucketId: 42,
    partitionDigestHex: PARTITION_DIGEST,
    partitionValue: "workspace-fictional",
    routeEpoch: 7,
    shardId: "shard-fictional",
  })),
  schemaId = "schema-v1",
) {
  return {
    db: createScopedDatabase({
      partitionKey: "workspaceId",
      registry,
      resolveDatabase: () => database,
      resolveRoute,
      schemaId,
    }),
    resolveRoute,
  }
}

describe("ScopedDatabase", () => {
  it("rejects public partition-key typing that disagrees with the schema registry", () => {
    const database = new Database()
    expect(() =>
      createScopedDatabase({
        partitionKey: "tenantId",
        registry,
        resolveDatabase: () => database,
        resolveRoute: async () => ({
          bucketId: 42,
          partitionDigestHex: PARTITION_DIGEST,
          partitionValue: "workspace-fictional",
          routeEpoch: 7,
          shardId: "shard-fictional",
        }),
        schemaId: "schema-v1",
      }),
    ).toThrow("partition key does not match")
  })

  it("rejects missing, conflicting, and incomplete execution backends", () => {
    const database = new Database()
    const resolveRoute = async () => ({
      bucketId: 42,
      partitionDigestHex: PARTITION_DIGEST,
      partitionValue: "workspace-fictional",
      routeEpoch: 7,
      shardId: "shard-fictional",
    })
    const base = {
      partitionKey: "workspaceId",
      registry,
      resolveRoute,
      schemaId: "schema-v1",
    } as const
    const validTransport: ScopedPlanTransport = {
      async batch(plans) {
        return plans.map(() => ({ success: true, results: [], meta }))
      },
      async execute() {
        return { success: true, results: [], meta }
      },
    }

    for (const options of [
      base,
      { ...base, resolveDatabase: () => database, transport: validTransport },
      { ...base, transport: null },
      { ...base, transport: {} },
      { ...base, transport: { execute() {} } },
      { ...base, transport: { batch() {} } },
    ]) {
      expect(() => createScopedDatabase(options as never)).toThrow(
        "requires exactly one direct database resolver or routed transport",
      )
    }
  })

  it("lists registered tables in immutable code-unit order", () => {
    const alpha = sqliteTable("alpha", {
      id: text().primaryKey(),
      workspaceId: text("workspace_id").notNull(),
    })
    const zulu = sqliteTable("zulu", {
      id: text().primaryKey(),
      workspaceId: text("workspace_id").notNull(),
    })
    const names = (schema: Record<string, unknown>) =>
      new SchemaRegistry({ partitionKey: "workspaceId", schema })
        .tables()
        .map((table) => table.tableName)

    const reverseInput = new SchemaRegistry({
      partitionKey: "workspaceId",
      schema: { zulu, alpha },
    }).tables()

    expect(reverseInput.map((table) => table.tableName)).toEqual(["alpha", "zulu"])
    expect(Object.isFrozen(reverseInput)).toBe(true)
    expect(names({ alpha, zulu })).toEqual(["alpha", "zulu"])
  })

  it("resolves and caches routing lazily on first execution", async () => {
    const database = new Database()
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      {
        success: true,
        results: [{ id: "project-1", workspaceId: "workspace-fictional", name: "Nozzle" }],
        meta,
      },
    ]
    const { db, resolveRoute } = create(database)
    const query = db.select().from(projects).where(eq(projects.id, "project-1")).limit(1)
    expect(resolveRoute).not.toHaveBeenCalled()

    const result = await query
    expect(result).toEqual([
      { id: "project-1", workspaceId: "workspace-fictional", name: "Nozzle" },
    ])
    expect(resolveRoute).toHaveBeenCalledTimes(1)
    expect(database.batchSizes).toEqual([2])

    const plan = await query.toPlan()
    expect(plan).toMatchObject({ operation: "select", bucketId: 42, routeEpoch: 7 })
    expect(resolveRoute).toHaveBeenCalledTimes(1)
  })

  it("returns Drizzle-shaped rows with boolean, timestamp, JSON, and BLOB decoding", async () => {
    const database = new Database()
    const createdAt = new Date("2026-01-02T03:04:05.678Z")
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      {
        success: true,
        results: [
          {
            id: "typed-1",
            workspaceId: "workspace-fictional",
            active: 1,
            createdAt: createdAt.getTime(),
            settings: '{"theme":"dark"}',
            payload: [0, 127, 255],
          },
        ],
        meta,
      },
    ]
    const { db } = create(database)

    await expect(db.select().from(typedRecords)).resolves.toEqual([
      {
        id: "typed-1",
        workspaceId: "workspace-fictional",
        active: true,
        createdAt,
        settings: { theme: "dark" },
        payload: Buffer.from([0, 127, 255]),
      },
    ])
  })

  it("fails closed when a selected value no longer matches its Drizzle schema", async () => {
    const database = new Database()
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      {
        success: true,
        results: [
          {
            id: "typed-1",
            workspaceId: "workspace-fictional",
            active: 1,
            createdAt: 1,
            settings: "not-json",
            payload: [],
          },
        ],
        meta,
      },
    ]
    const { db } = create(database)
    await expect(db.select().from(typedRecords)).rejects.toMatchObject({
      code: "SchemaDriftError",
    })
  })

  it("builds familiar insert, update, and delete plans", async () => {
    const database = new Database()
    const { db, resolveRoute } = create(database)
    const insert = db.insert(projects).values({ id: "project-1", name: "Nozzle" })
    const update = db.update(projects).set({ name: "Renamed" }).where(eq(projects.id, "project-1"))
    const deletion = db.delete(projects).where(eq(projects.id, "project-1"))

    await expect(insert.toPlan()).resolves.toMatchObject({
      operation: "insert",
      values: { workspace_id: "workspace-fictional", __nozzle_bucket: 42 },
    })
    await expect(update.toPlan()).resolves.toMatchObject({ operation: "update" })
    await expect(deletion.toPlan()).resolves.toMatchObject({ operation: "delete" })
    expect(resolveRoute).toHaveBeenCalledTimes(1)
  })

  it("executes direct insert, update, and delete mutations", async () => {
    const database = new Database()
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      { success: true, results: [], meta },
    ]
    const { db } = create(database)

    await expect(db.insert(projects).values({ id: "direct-1", name: "Direct" })).resolves.toEqual(
      database.results[1],
    )
    await expect(db.update(projects).set({ name: "Updated" })).resolves.toEqual(database.results[1])
    await expect(db.delete(projects)).resolves.toEqual(database.results[1])
    expect(database.batchSizes).toEqual([2, 2, 2])
  })

  it("executes routed reads, mutations, and complete batches", async () => {
    const mutationResult = { success: true, results: [], meta } as const
    const transport: ScopedPlanTransport = {
      batch: vi.fn(async (plans: readonly ExecutionPlan[]) => plans.map(() => mutationResult)),
      execute: vi.fn(async (plan: ExecutionPlan) =>
        plan.operation === "select"
          ? {
              success: true as const,
              results: [
                {
                  id: "routed-1",
                  workspaceId: "workspace-fictional",
                  name: "Routed",
                },
              ],
              meta,
            }
          : mutationResult,
      ),
    }
    const db = createScopedDatabase({
      partitionKey: "workspaceId",
      registry,
      resolveRoute: async () => ({
        bucketId: 42,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 7,
        shardId: "shard-fictional",
      }),
      schemaId: "schema-v1",
      transport,
    })

    await expect(db.select().from(projects)).resolves.toEqual([
      { id: "routed-1", workspaceId: "workspace-fictional", name: "Routed" },
    ])
    await expect(db.insert(projects).values({ id: "routed-1", name: "Routed" })).resolves.toEqual(
      mutationResult,
    )
    await expect(db.update(projects).set({ name: "Updated" })).resolves.toEqual(mutationResult)
    await expect(db.delete(projects)).resolves.toEqual(mutationResult)
    await expect(
      db.batch([
        db.insert(projects).values({ id: "routed-2", name: "Batch" }),
        db.delete(projects),
      ]),
    ).resolves.toEqual([mutationResult, mutationResult])
    expect(transport.execute).toHaveBeenCalledTimes(4)
    expect(transport.batch).toHaveBeenCalledTimes(1)
  })

  it("rejects an incomplete routed batch", async () => {
    const transport: ScopedPlanTransport = {
      async batch() {
        return []
      },
      async execute() {
        return { success: true, results: [], meta }
      },
    }
    const db = createScopedDatabase({
      partitionKey: "workspaceId",
      registry,
      resolveRoute: async () => ({
        bucketId: 42,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 7,
        shardId: "shard-fictional",
      }),
      schemaId: "schema-v1",
      transport,
    })

    await expect(db.batch([db.delete(projects)])).rejects.toMatchObject({
      code: "ShardUnavailableError",
    })
  })

  it("executes a portable multi-statement batch with one authorization statement", async () => {
    const database = new Database()
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      { success: true, results: [], meta },
      { success: true, results: [], meta },
    ]
    const { db } = create(database)
    const results = await db.batch([
      db.insert(projects).values({ id: "project-1", name: "Nozzle" }),
      db.update(projects).set({ name: "Renamed" }).where(eq(projects.id, "project-1")),
    ])
    expect(results).toHaveLength(2)
    expectTypeOf(results[0]).toEqualTypeOf<D1ResultLike>()
    expectTypeOf(results[1]).toEqualTypeOf<D1ResultLike>()
    expect(database.batchSizes).toEqual([3])
  })

  it("preserves decoded select result types and values through atomic batches", async () => {
    const database = new Database()
    database.results = [
      { success: true, results: [{ routeEpoch: 7 }], meta },
      {
        success: true,
        results: [{ id: "project-1", workspaceId: "workspace-fictional", name: "Nozzle" }],
        meta,
      },
    ]
    const { db } = create(database)
    const results = await db.batch([db.select().from(projects)])

    expectTypeOf(results[0]).toEqualTypeOf<
      readonly { id: string; name: string; workspaceId: string }[]
    >()
    expect(results[0]).toEqual([
      { id: "project-1", workspaceId: "workspace-fictional", name: "Nozzle" },
    ])
  })

  it("rejects empty, oversized, cross-route, stale, and incomplete batches", async () => {
    const database = new Database()
    const { db } = create(database)
    await expect(db.batch([])).rejects.toThrow("cannot be empty")

    const query = db.delete(projects)
    await expect(db.batch(Array.from({ length: 50 }, () => query))).rejects.toThrow(
      "cannot exceed 49",
    )

    const { db: foreignDb } = create(
      database,
      vi.fn(async () => ({
        bucketId: 42,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 7,
        shardId: "another-shard",
      })),
    )
    const foreignPlan = await foreignDb.delete(projects).toPlan()
    await expect(db.batch([query, planned(foreignPlan)])).rejects.toMatchObject({
      code: "CrossShardTransactionUnsupportedError",
    })

    const { db: collidingDb } = create(
      database,
      vi.fn(async () => ({
        bucketId: 42,
        partitionDigestHex: "22".repeat(32),
        partitionValue: "another-workspace",
        routeEpoch: 7,
        shardId: "shard-fictional",
      })),
    )
    const collidingPlan = await collidingDb.delete(projects).toPlan()
    await expect(db.batch([query, planned(collidingPlan)])).rejects.toMatchObject({
      code: "CrossShardTransactionUnsupportedError",
    })

    const { db: incompatibleDb } = create(
      database,
      vi.fn(async () => ({
        bucketId: 42,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 7,
        shardId: "shard-fictional",
      })),
      "schema-v2",
    )
    const incompatiblePlan = await incompatibleDb.delete(projects).toPlan()
    await expect(db.batch([query, planned(incompatiblePlan)])).rejects.toMatchObject({
      code: "CrossShardTransactionUnsupportedError",
    })

    const otherRegistry = new SchemaRegistry({
      schema: { projects },
      partitionKey: "workspaceId",
    })
    const otherDb = createScopedDatabase({
      partitionKey: "workspaceId",
      registry: otherRegistry,
      resolveDatabase: () => database,
      resolveRoute: async () => ({
        bucketId: 42,
        partitionDigestHex: PARTITION_DIGEST,
        partitionValue: "workspace-fictional",
        routeEpoch: 7,
        shardId: "shard-fictional",
      }),
      schemaId: "schema-v1",
    })
    const foreignRegistryPlan = await otherDb.delete(projects).toPlan()
    await expect(db.batch([query, planned(foreignRegistryPlan)])).rejects.toMatchObject({
      code: "UnsafeQueryRequiredError",
    })

    database.results = [
      { success: true, results: [], meta },
      { success: true, results: [], meta },
    ]
    await expect(db.batch([query])).rejects.toMatchObject({ code: "StaleRouteRejectedError" })

    database.results = [{ success: true, results: [{ routeEpoch: 7 }], meta }]
    await expect(db.batch([query])).rejects.toMatchObject({ code: "ShardUnavailableError" })
  })

  it("compares typed partition values without conflating encodings", async () => {
    const blobProjects = sqliteTable("blob_projects", {
      id: text().primaryKey(),
      workspaceId: blob("workspace_id", { mode: "buffer" }).notNull(),
    })
    const textProjects = sqliteTable("text_projects", {
      id: text().primaryKey(),
      workspaceId: text("workspace_id").notNull(),
    })
    const typedRegistry = new SchemaRegistry({
      partitionKey: "workspaceId",
      schema: { blobProjects, textProjects },
    })
    const result = { success: true, results: [], meta } as const
    const transport: ScopedPlanTransport = {
      async batch(plans) {
        return plans.map(() => result)
      },
      async execute() {
        return result
      },
    }
    const createTyped = (partitionValue: unknown) =>
      createScopedDatabase({
        partitionKey: "workspaceId",
        registry: typedRegistry,
        resolveRoute: async () => ({
          bucketId: 42,
          partitionDigestHex: PARTITION_DIGEST,
          partitionValue,
          routeEpoch: 7,
          shardId: "shard-fictional",
        }),
        schemaId: "schema-v1",
        transport,
      })
    const executor = createTyped(Buffer.from([1, 2]))
    const blobPlan = await executor.delete(blobProjects).toPlan()
    const sameBlobPlan = await createTyped(Buffer.from([1, 2]))
      .delete(blobProjects)
      .toPlan()
    const otherBlobPlan = await createTyped(Buffer.from([1, 3]))
      .delete(blobProjects)
      .toPlan()
    const textPlan = await createTyped("workspace-fictional").delete(textProjects).toPlan()
    const nullPlan = await createTyped(null).delete(textProjects).toPlan()

    await expect(executor.batch([planned(blobPlan), planned(sameBlobPlan)])).resolves.toEqual([
      result,
      result,
    ])
    for (const plans of [
      [blobPlan, otherBlobPlan],
      [textPlan, blobPlan],
      [nullPlan, blobPlan],
    ] as const) {
      await expect(executor.batch(plans.map(planned))).rejects.toMatchObject({
        code: "CrossShardTransactionUnsupportedError",
      })
    }
  })

  it("fails closed if a hostile batch changes shape after admission", async () => {
    const database = new Database()
    const { db } = create(database)
    let lengthReads = 0
    const queries = new Proxy([db.delete(projects)], {
      get(target, property, receiver) {
        if (property === "length") {
          lengthReads += 1
          return lengthReads <= 2 ? 1 : 0
        }
        return Reflect.get(target, property, receiver)
      },
    })

    await expect(db.batch(queries)).rejects.toThrow("cannot be empty")
    expect(lengthReads).toBe(3)
  })
})

function planned(plan: ExecutionPlan): PlannedQuery {
  const result = { success: true, results: [], meta } as const
  return {
    async execute() {
      return result
    },
    async toPlan() {
      return plan
    },
    // biome-ignore lint/suspicious/noThenProperty: The fixture implements the public awaitable query contract.
    then(onfulfilled, onrejected) {
      return this.execute().then(onfulfilled, onrejected)
    },
  }
}
