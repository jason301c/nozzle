import { sql } from "drizzle-orm"
import { blob, integer, numeric, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { compilePlan } from "../src/compiler.js"
import { and, eq, gt, inArray, isNotNull, isNull, ne, or } from "../src/expression.js"
import {
  buildDeletePlan,
  buildInsertPlan,
  buildSelectPlan,
  buildUpdatePlan,
  type ScopedRoute,
} from "../src/plan.js"
import { SchemaRegistry } from "../src/schema.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
  active: integer({ mode: "boolean" }).notNull().default(true),
  score: integer(),
})

const events = sqliteTable(
  "events",
  {
    workspaceId: text("workspace_id").notNull(),
    eventId: text("event_id").notNull(),
    label: text(),
  },
  (table) => [primaryKey({ columns: [table.workspaceId, table.eventId] })],
)

const users = sqliteTable("users", {
  id: text().primaryKey(),
  email: text().notNull(),
})

const binaryRows = sqliteTable("binary_rows", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  payload: blob().notNull(),
})

const prototypeRows = sqliteTable("prototype_rows", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  dangerous: text("__proto__").notNull(),
})

const schema = { projects, events, users, binaryRows, prototypeRows, ignored: "not-a-table" }
const registry = new SchemaRegistry({ schema, partitionKey: "workspaceId", globalTables: [users] })
const PARTITION_DIGEST = "11".repeat(32)
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: PARTITION_DIGEST,
  partitionValue: "workspace-fictional",
  routeEpoch: 7,
  shardId: "shard-fictional-a",
}

describe("Drizzle schema registry", () => {
  it("classifies tables and preserves primary/partition metadata", () => {
    const project = registry.table(projects)
    expect(project.classification).toBe("sharded")
    expect(project.partitionColumn?.dbName).toBe("workspace_id")
    expect(project.partitionColumn?.storageType).toBe("text")
    expect(project.primaryColumns.map((column) => column.propertyName)).toEqual(["id"])
    expect(registry.table(events).primaryColumns.map((column) => column.propertyName)).toEqual([
      "workspaceId",
      "eventId",
    ])
    expect(registry.table(users).classification).toBe("global")
    expect(registry.column(projects.name).dbName).toBe("name")
  })

  it("rejects schemas without tables, unknown globals, and unknown lookups", () => {
    expect(() => new SchemaRegistry({ schema, partitionKey: " " })).toThrow(
      "partition-key property cannot be empty",
    )
    expect(() => new SchemaRegistry({ schema: { value: 1 }, partitionKey: "workspaceId" })).toThrow(
      "does not contain any tables",
    )
    const outsider = sqliteTable("outsider", { id: text().primaryKey() })
    expect(
      () => new SchemaRegistry({ schema, partitionKey: "workspaceId", globalTables: [outsider] }),
    ).toThrow("global table is not present")
    expect(() => registry.table(outsider)).toThrow("not registered")
    expect(() => registry.column(outsider.id)).toThrowError(
      expect.objectContaining({ code: "UnsafeQueryRequiredError" }),
    )

    expect(
      () =>
        new SchemaRegistry({
          schema: { users },
          partitionKey: "workspaceId",
          globalTables: [users, users],
        }),
    ).toThrow("cannot be duplicated")

    const duplicateA = sqliteTable("duplicate_name", { id: text().primaryKey() })
    const duplicateB = sqliteTable("DUPLICATE_NAME", { id: text().primaryKey() })
    expect(
      () =>
        new SchemaRegistry({
          schema: { duplicateA, duplicateB },
          partitionKey: "workspaceId",
          globalTables: [duplicateA, duplicateB],
        }),
    ).toThrow("same SQL table name")
  })

  it("accepts keyless global tables because they are not moved by sharded identity", () => {
    const globalLog = sqliteTable("global_log", { message: text().notNull() })
    const localRegistry = new SchemaRegistry({
      schema: { globalLog },
      partitionKey: "workspaceId",
      globalTables: [globalLog],
    })
    expect(localRegistry.table(globalLog)).toMatchObject({
      classification: "global",
      primaryColumns: [],
    })
  })

  it("rejects missing, nullable, primary-key-free, and reserved schema structures", () => {
    const missingPartition = sqliteTable("missing_partition", { id: text().primaryKey() })
    expect(
      () => new SchemaRegistry({ schema: { missingPartition }, partitionKey: "workspaceId" }),
    ).toThrowError(expect.objectContaining({ code: "PartitionKeyMissingError" }))

    const nullablePartition = sqliteTable("nullable_partition", {
      id: text().primaryKey(),
      workspaceId: text(),
    })
    expect(
      () => new SchemaRegistry({ schema: { nullablePartition }, partitionKey: "workspaceId" }),
    ).toThrow("must be non-null")

    const missingPrimary = sqliteTable("missing_primary", {
      workspaceId: text().notNull(),
      value: text(),
    })
    expect(
      () => new SchemaRegistry({ schema: { missingPrimary }, partitionKey: "workspaceId" }),
    ).toThrow("explicit primary key")

    const nullablePrimary = sqliteTable(
      "nullable_primary",
      { id: text(), workspaceId: text("workspace_id").notNull() },
      (table) => [primaryKey({ columns: [table.id] })],
    )
    expect(
      () => new SchemaRegistry({ schema: { nullablePrimary }, partitionKey: "workspaceId" }),
    ).toThrow("primary-key column must be explicitly non-null")

    const numericPrimary = sqliteTable(
      "numeric_primary",
      { id: numeric().notNull(), workspaceId: text("workspace_id").notNull() },
      (table) => [primaryKey({ columns: [table.id] })],
    )
    expect(
      () => new SchemaRegistry({ schema: { numericPrimary }, partitionKey: "workspaceId" }),
    ).toThrow("deterministic SQLite storage affinity")

    const nozzleTable = sqliteTable("Nozzle_Application", {
      id: text().primaryKey(),
      workspaceId: text().notNull(),
    })
    expect(
      () => new SchemaRegistry({ schema: { nozzleTable }, partitionKey: "workspaceId" }),
    ).toThrow("cannot use Nozzle names")

    const reservedColumn = sqliteTable("reserved_column", {
      id: text().primaryKey(),
      workspaceId: text().notNull(),
      bad: text("__nozzle_bad"),
    })
    expect(
      () => new SchemaRegistry({ schema: { reservedColumn }, partitionKey: "workspaceId" }),
    ).toThrow("columns cannot use Nozzle names")

    const duplicateColumn = sqliteTable("duplicate_column", {
      id: text("identity").primaryKey(),
      alias: text("IDENTITY"),
      workspaceId: text().notNull(),
    })
    expect(
      () => new SchemaRegistry({ schema: { duplicateColumn }, partitionKey: "workspaceId" }),
    ).toThrow("same SQL column name")

    const mixedReservedColumn = sqliteTable("mixed_reserved_column", {
      id: text().primaryKey(),
      workspaceId: text().notNull(),
      bad: text("__Nozzle_Internal"),
    })
    expect(
      () => new SchemaRegistry({ schema: { mixedReservedColumn }, partitionKey: "workspaceId" }),
    ).toThrow("columns cannot use Nozzle names")
  })
})

describe("scoped structured plans", () => {
  it("injects scope outside arbitrary user OR predicates", () => {
    const plan = buildSelectPlan(registry, {
      table: projects,
      route,
      schemaId: "schema-v1",
      limit: 25,
      predicate: or(
        eq(projects.name, "alpha"),
        and(gt(projects.score, 5), isNotNull(projects.score), ne(projects.name, "blocked")),
      ),
    })
    const compiled = compilePlan(plan)

    expect(plan).toMatchObject({
      version: 1,
      operation: "select",
      table: "projects",
      partitionColumn: "workspace_id",
      partitionDigestHex: PARTITION_DIGEST,
      partitionValue: "workspace-fictional",
      bucketId: 42,
      routeEpoch: 7,
    })
    expect(compiled.data.sql).toBe(
      'SELECT "id" AS "id", "workspace_id" AS "workspaceId", "name" AS "name", "active" AS "active", "score" AS "score" FROM "projects" WHERE ("workspace_id" = ?1 AND "__nozzle_bucket" = ?2 AND EXISTS (SELECT 1 FROM "nozzle_bucket_ownership" WHERE "bucket_id" = ?3 AND "route_epoch" = ?4 AND "state" = \'writable\') AND EXISTS (SELECT 1 FROM "nozzle_schema_state" WHERE "schema_id" = ?5 AND "active" = 1) AND NOT EXISTS (SELECT 1 FROM "nozzle_partition_fences" WHERE "partition_digest" = ?6) AND ("name" = ?7 OR ("score" > ?8 AND "score" IS NOT NULL AND "name" != ?9))) LIMIT ?10',
    )
    expect(compiled.data.params).toEqual([
      "workspace-fictional",
      42,
      42,
      7,
      "schema-v1",
      PARTITION_DIGEST,
      "alpha",
      5,
      "blocked",
      25,
    ])
    expect(compiled.authorization.params).toEqual([42, 7, "schema-v1", PARTITION_DIGEST])
  })

  it("injects and validates partition and bucket values on inserts", () => {
    const plan = buildInsertPlan(registry, {
      table: projects,
      route,
      schemaId: "schema-v1",
      values: { id: "project-1", name: "Nozzle", active: false },
    })
    expect(plan.values).toEqual({
      id: "project-1",
      name: "Nozzle",
      active: 0,
      workspace_id: "workspace-fictional",
      __nozzle_bucket: 42,
    })
    expect(compilePlan(plan).data).toEqual({
      sql: 'INSERT INTO "projects" ("__nozzle_bucket", "active", "id", "name", "workspace_id") SELECT ?1, ?2, ?3, ?4, ?5 WHERE EXISTS (SELECT 1 FROM "nozzle_bucket_ownership" WHERE "bucket_id" = ?6 AND "route_epoch" = ?7 AND "state" = \'writable\') AND EXISTS (SELECT 1 FROM "nozzle_schema_state" WHERE "schema_id" = ?8 AND "active" = 1) AND NOT EXISTS (SELECT 1 FROM "nozzle_partition_fences" WHERE "partition_digest" = ?9)',
      params: [
        42,
        0,
        "project-1",
        "Nozzle",
        "workspace-fictional",
        42,
        7,
        "schema-v1",
        PARTITION_DIGEST,
      ],
    })

    expect(() =>
      buildInsertPlan(registry, {
        table: projects,
        route,
        schemaId: "schema-v1",
        values: { id: "project-1", name: "Nozzle", workspaceId: "another-workspace" },
      }),
    ).toThrowError(expect.objectContaining({ code: "PartitionKeyMismatchError" }))
  })

  it("encodes BLOB plan values immutably and restores typed D1 bindings", () => {
    const payload = Uint8Array.of(0x00, 0x7f, 0xff)
    const plan = buildInsertPlan(registry, {
      table: binaryRows,
      route,
      schemaId: "schema-v1",
      values: { id: "binary-1", payload },
    })
    payload[0] = 0xaa

    expect(plan.values.payload).toEqual({ type: "blob", hex: "007fff" })
    const compiledPayload = compilePlan(plan).data.params.find(
      (value): value is Uint8Array => value instanceof Uint8Array,
    )
    expect(compiledPayload).toEqual(Uint8Array.of(0x00, 0x7f, 0xff))
  })

  it("preserves own write values for SQL column names inherited from Object.prototype", () => {
    const insert = buildInsertPlan(registry, {
      table: prototypeRows,
      route,
      schemaId: "schema-v1",
      values: { dangerous: "safe", id: "prototype-1" },
    })
    const update = buildUpdatePlan(registry, {
      table: prototypeRows,
      route,
      schemaId: "schema-v1",
      values: { dangerous: "safer" },
    })

    expect(Object.hasOwn(insert.values, "__proto__")).toBe(true)
    expect(Reflect.get(insert.values, "__proto__")).toBe("safe")
    expect(compilePlan(insert).data.sql).toContain('"__proto__"')
    expect(Object.hasOwn(update.values, "__proto__")).toBe(true)
    expect(Reflect.get(update.values, "__proto__")).toBe("safer")
  })

  it("constrains update and delete plans even without a user predicate", () => {
    const update = buildUpdatePlan(registry, {
      table: projects,
      route,
      schemaId: "schema-v1",
      values: { name: "Renamed" },
    })
    const deletion = buildDeletePlan(registry, {
      table: projects,
      route,
      schemaId: "schema-v1",
      predicate: isNull(projects.score),
    })
    expect(compilePlan(update).data.sql).toContain(
      'WHERE ("workspace_id" = ?2 AND "__nozzle_bucket" = ?3',
    )
    expect(compilePlan(deletion).data.sql).toContain('AND "score" IS NULL)')
  })

  it("rejects identity mutation, empty writes, global tables, and invalid routes", () => {
    for (const values of [{ id: "new" }, { workspaceId: "new" }]) {
      expect(() =>
        buildUpdatePlan(registry, { table: projects, route, schemaId: "v1", values }),
      ).toThrow("cannot mutate identity columns")
    }
    expect(() =>
      buildUpdatePlan(registry, { table: projects, route, schemaId: "v1", values: {} }),
    ).toThrow("at least one value")
    expect(() => buildSelectPlan(registry, { table: users, route, schemaId: "v1" })).toThrowError(
      expect.objectContaining({ code: "TenantScopeRequiredError" }),
    )
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route: { ...route, bucketId: -1 },
        schemaId: "v1",
      }),
    ).toThrow("route bucket is invalid")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route: { ...route, bucketId: 0x1_0000_0000 },
        schemaId: "v1",
      }),
    ).toThrow("route bucket is invalid")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route: { ...route, routeEpoch: 0 },
        schemaId: "v1",
      }),
    ).toThrow("route epoch is invalid")
    expect(() =>
      buildSelectPlan(registry, { table: projects, route, schemaId: "v1", limit: 0 }),
    ).toThrow("positive integers")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route: { ...route, shardId: " " },
        schemaId: "v1",
      }),
    ).toThrow("route shard is invalid")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route: { ...route, partitionDigestHex: "AA" },
        schemaId: "v1",
      }),
    ).toThrow("partition digest must be 64 lowercase")
    expect(() => buildSelectPlan(registry, { table: projects, route, schemaId: " " })).toThrow(
      "schema identifier cannot be empty",
    )
  })

  it("rejects unknown values, columns, empty predicates, and cross-table predicates", () => {
    expect(() =>
      buildInsertPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: { id: "x", name: "x", unknown: true },
      }),
    ).toThrow("unknown column")
    expect(() =>
      buildInsertPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: null as never,
      }),
    ).toThrow("Write values must be a structured object")
    expect(() =>
      buildUpdatePlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: [] as never,
      }),
    ).toThrow("Write values must be a structured object")
    expect(() =>
      buildInsertPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: { id: "x", name: undefined },
      }),
    ).toThrow("Undefined cannot be bound")
    expect(() =>
      buildInsertPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: { id: "x", name: sql`unsafe` },
      }),
    ).toThrow("unsupported SQL encoding")
    expect(() =>
      buildUpdatePlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        values: { score: Number.MAX_SAFE_INTEGER + 1 },
      }),
    ).toThrow("must be JavaScript safe integers")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: eq(events.label, "cross-table"),
      }),
    ).toThrow("cannot cross tables")
    expect(() =>
      buildSelectPlan(registry, { table: projects, route, schemaId: "v1", predicate: and() }),
    ).toThrow("cannot be empty")
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: inArray(projects.name, []),
      }),
    ).toThrow("cannot be empty")
  })

  it("rejects forged predicate shapes and SQL-bearing operators before compilation", () => {
    const injection = {
      kind: "logical",
      operator: ")) OR 1=1 --",
      terms: [eq(projects.name, "safe")],
    }
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: injection as never,
      }),
    ).toThrowError(expect.objectContaining({ code: "UnsafeQueryRequiredError" }))

    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: {
          kind: "comparison",
          column: projects.name,
          operator: 'eq" OR 1=1 --',
          value: "safe",
        } as never,
      }),
    ).toThrow("comparison predicate operator is invalid")

    for (const predicate of [
      null,
      { kind: "unsupported" },
      { kind: "is-null", column: projects.name, negated: "false" },
      { kind: "logical", operator: "and", terms: "not-an-array" },
      { kind: "in", column: projects.name, values: "not-an-array" },
    ]) {
      expect(() =>
        buildSelectPlan(registry, {
          table: projects,
          route,
          schemaId: "v1",
          predicate: predicate as never,
        }),
      ).toThrowError(expect.objectContaining({ code: "UnsafeQueryRequiredError" }))
    }
  })

  it("rejects cyclic and unbounded predicate structures", () => {
    const cyclic: { kind: "logical"; operator: "and"; terms: unknown[] } = {
      kind: "logical",
      operator: "and",
      terms: [],
    }
    cyclic.terms.push(cyclic)
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: cyclic as never,
      }),
    ).toThrow("Cyclic predicates")

    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: and(...Array.from({ length: 129 }, () => isNull(projects.score))),
      }),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))

    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: inArray(
          projects.name,
          Array.from({ length: 101 }, (_, index) => `name-${index}`),
        ),
      }),
    ).toThrow("too many bound values")

    const oversizedSparseTerms = new Array(1_000_000)
    oversizedSparseTerms[999_999] = isNull(projects.score)
    expect(() =>
      buildSelectPlan(registry, {
        table: projects,
        route,
        schemaId: "v1",
        predicate: { kind: "logical", operator: "and", terms: oversizedSparseTerms } as never,
      }),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))

    const sparseTerms = new Array(2)
    sparseTerms[1] = isNull(projects.score)
    const sparseValues = new Array(2)
    sparseValues[1] = "name"
    for (const predicate of [
      { kind: "logical", operator: "and", terms: sparseTerms },
      { kind: "in", column: projects.name, values: sparseValues },
    ]) {
      expect(() =>
        buildSelectPlan(registry, {
          table: projects,
          route,
          schemaId: "v1",
          predicate: predicate as never,
        }),
      ).toThrow("cannot contain sparse entries")
    }
  })

  it("enforces D1's parameter ceiling", () => {
    const plan = buildSelectPlan(registry, {
      table: projects,
      route,
      schemaId: "v1",
      predicate: inArray(
        projects.name,
        Array.from({ length: 97 }, (_, index) => `name-${index}`),
      ),
    })
    expect(() => compilePlan(plan)).toThrowError(
      expect.objectContaining({ code: "CapacityGuardError" }),
    )
  })

  it("rejects structurally forged plans even when copied from a trusted plan", () => {
    const plan = buildDeletePlan(registry, { table: projects, route, schemaId: "v1" })
    expect(() => compilePlan({ ...plan })).toThrow("must be produced by the scoped")
  })
})
