import { blob, integer, real, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { and, eq, gt, inArray, isNotNull, isNull, or } from "../src/expression.js"
import {
  buildDeletePlan,
  buildInsertPlan,
  buildSelectPlan,
  buildUpdatePlan,
  decodeWireExecutionPlan,
  type ExecutionPlan,
  readWireExecutionPlanRouteHint,
  type ScopedRoute,
} from "../src/plan.js"
import { SchemaRegistry } from "../src/schema.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
  score: integer(),
  ratio: real(),
  settings: text({ mode: "json" }),
  payload: blob({ mode: "buffer" }),
})
const globalLog = sqliteTable("global_log", { message: text().notNull() })
const registry = new SchemaRegistry({
  schema: { globalLog, projects },
  partitionKey: "workspaceId",
  globalTables: [globalLog],
})
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: "11".repeat(32),
  partitionValue: "workspace-a",
  routeEpoch: 7,
  shardId: "shard-a",
}
const expectation = { route, schemaId: "application-v1" } as const

function wire<T extends ExecutionPlan>(plan: T): T {
  return structuredClone(plan)
}

function validPlans(): readonly ExecutionPlan[] {
  return [
    buildSelectPlan(registry, {
      limit: 10,
      predicate: and(
        eq(projects.name, "Nozzle"),
        or(gt(projects.score, 1), isNull(projects.score)),
        inArray(projects.id, ["a", "b"]),
        isNotNull(projects.name),
      ),
      route,
      schemaId: expectation.schemaId,
      table: projects,
    }),
    buildInsertPlan(registry, {
      route,
      schemaId: expectation.schemaId,
      table: projects,
      values: {
        id: "project-a",
        name: "Nozzle",
        payload: Buffer.from([0, 255]),
        ratio: 1.5,
        score: 1,
        settings: { theme: "dark" },
      },
    }),
    buildUpdatePlan(registry, {
      predicate: eq(projects.id, "project-a"),
      route,
      schemaId: expectation.schemaId,
      table: projects,
      values: { name: "Updated", score: null },
    }),
    buildDeletePlan(registry, {
      predicate: eq(projects.id, "project-a"),
      route,
      schemaId: expectation.schemaId,
      table: projects,
    }),
  ]
}

describe("wire execution-plan validation", () => {
  it("reconstructs every operation as a deeply trusted immutable plan", () => {
    for (const original of validPlans()) {
      const decoded = decodeWireExecutionPlan(registry, wire(original), expectation)
      expect(decoded).toEqual(original)
      expect(Object.isFrozen(decoded)).toBe(true)
      if ("values" in decoded) expect(Object.isFrozen(decoded.values)).toBe(true)
      if ("predicate" in decoded && decoded.predicate)
        expect(Object.isFrozen(decoded.predicate)).toBe(true)
    }
  })

  it("reconstructs plans with no optional limit or predicate fields", () => {
    const plans = [
      buildSelectPlan(registry, {
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
      buildUpdatePlan(registry, {
        route,
        schemaId: expectation.schemaId,
        table: projects,
        values: { name: "Updated" },
      }),
      buildDeletePlan(registry, {
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
    ]
    for (const original of plans) {
      expect(decodeWireExecutionPlan(registry, wire(original), expectation)).toEqual(original)
    }
  })

  it("extracts only a validated, cloned route hint", () => {
    const input = wire(validPlans()[0] as ExecutionPlan)
    const hint = readWireExecutionPlanRouteHint(registry, input)
    expect(hint).toEqual({ partitionValue: "workspace-a", table: "projects" })
    expect(Object.isFrozen(hint)).toBe(true)

    expect(() => readWireExecutionPlanRouteHint(registry, null)).toThrow("plain object")
    expect(() => readWireExecutionPlanRouteHint(registry, { table: 1 })).toThrow("table name")
    expect(() => readWireExecutionPlanRouteHint(registry, { table: "missing" })).toThrow(
      "unregistered table",
    )
    expect(() => readWireExecutionPlanRouteHint(registry, { table: "global_log" })).toThrow(
      "sharded table",
    )
    expect(() => readWireExecutionPlanRouteHint(registry, { table: "projects" })).toThrow(
      "missing its partition value",
    )
    expect(() =>
      readWireExecutionPlanRouteHint(registry, { partitionValue: 1, table: "projects" }),
    ).toThrow("wrong SQLite storage type")
  })

  it.each([
    ["version", 2],
    ["table", "global_log"],
    ["partitionColumn", "other"],
    ["schemaId", "other"],
    ["bucketId", 43],
    ["routeEpoch", 8],
    ["shardId", "shard-b"],
    ["partitionDigestHex", "22".repeat(32)],
  ] as const)("rejects a mismatched base %s", (field, value) => {
    const input = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    input[field] = value
    expect(() => decodeWireExecutionPlan(registry, input, expectation)).toThrow()
  })

  it("rejects malformed envelopes, fields, and partition values", () => {
    expect(() => decodeWireExecutionPlan(registry, null, expectation)).toThrow("plain object")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        Object.assign(Object.create(null), { table: 1 }),
        expectation,
      ),
    ).toThrow("table name")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        Object.assign(new Date(), { table: "projects" }),
        expectation,
      ),
    ).toThrow("plain object")
    const symbolPlan = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<
      PropertyKey,
      unknown
    >
    symbolPlan[Symbol("hidden")] = true
    expect(() => decodeWireExecutionPlan(registry, symbolPlan, expectation)).toThrow("symbol")
    const hidden = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    Object.defineProperty(hidden, "secret", { enumerable: false, value: true })
    expect(() => decodeWireExecutionPlan(registry, hidden, expectation)).toThrow(
      "enumerable data properties",
    )

    const extra = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    extra.extra = true
    expect(() => decodeWireExecutionPlan(registry, extra, expectation)).toThrow(
      "missing or unsupported fields",
    )
    const missing = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    delete missing.bucketId
    expect(() => decodeWireExecutionPlan(registry, missing, expectation)).toThrow("validated route")
    const partition = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    partition.partitionValue = "workspace-b"
    expect(() => decodeWireExecutionPlan(registry, partition, expectation)).toThrow(
      "not canonical for the route",
    )
    const operation = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    operation.operation = "merge"
    expect(() => decodeWireExecutionPlan(registry, operation, expectation)).toThrow(
      "operation is unsupported",
    )
  })

  it("rejects selected-column and limit tampering", () => {
    const plan = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    plan.columns = []
    expect(() => decodeWireExecutionPlan(registry, plan, expectation)).toThrow(
      "columns do not match",
    )

    for (const limit of [0, 1.5, "1"]) {
      const limited = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
      limited.limit = limit
      expect(() => decodeWireExecutionPlan(registry, limited, expectation)).toThrow(
        "positive safe integer",
      )
    }
    const columns = (wire(validPlans()[0] as ExecutionPlan) as unknown as { columns: unknown[] })
      .columns
    columns[0] = { column: "wrong", resultKey: "id" }
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...(wire(validPlans()[0] as ExecutionPlan) as object), columns },
        expectation,
      ),
    ).toThrow("columns do not match")
  })

  it("rejects predicate tampering and resource abuse", () => {
    const base = wire(validPlans()[0] as ExecutionPlan) as unknown as Record<string, unknown>
    for (const predicate of [null, [], { kind: "unknown" }]) {
      expect(() => decodeWireExecutionPlan(registry, { ...base, predicate }, expectation)).toThrow()
    }
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { column: "missing", kind: "is-null", negated: false } },
        expectation,
      ),
    ).toThrow("unregistered column")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: { column: "score", kind: "comparison", operator: "like", value: 1 },
        },
        expectation,
      ),
    ).toThrow("comparison operator")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { column: "score", kind: "in", values: [] } },
        expectation,
      ),
    ).toThrow("cannot be empty")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { column: "score", kind: "is-null", negated: 1 } },
        expectation,
      ),
    ).toThrow("flag is invalid")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { column: 1, kind: "is-null", negated: false } },
        expectation,
      ),
    ).toThrow("column is invalid")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: { column: "score", kind: "comparison", operator: 1, value: 1 },
        },
        expectation,
      ),
    ).toThrow("comparison operator")
    const tooMany = Array.from({ length: 101 }, () => 1)
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { column: "score", kind: "in", values: tooMany } },
        expectation,
      ),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))

    for (const terms of [null, []]) {
      expect(() =>
        decodeWireExecutionPlan(
          registry,
          { ...base, predicate: { kind: "logical", operator: "and", terms } },
          expectation,
        ),
      ).toThrow("cannot be empty")
    }
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...base, predicate: { kind: "logical", operator: "xor", terms: [{}] } },
        expectation,
      ),
    ).toThrow("logical operator")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: {
            kind: "logical",
            operator: "and",
            terms: Array.from({ length: 129 }, () => ({
              column: "score",
              kind: "is-null",
              negated: false,
            })),
          },
        },
        expectation,
      ),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: {
            kind: "logical",
            operator: "and",
            terms: Array.from({ length: 128 }, () => ({
              column: "score",
              kind: "is-null",
              negated: false,
            })),
          },
        },
        expectation,
      ),
    ).toThrow("too many nodes")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: {
            kind: "logical",
            operator: "and",
            terms: Array.from({ length: 101 }, () => ({
              column: "score",
              kind: "comparison",
              operator: "eq",
              value: 1,
            })),
          },
        },
        expectation,
      ),
    ).toThrow("too many bound values")
    const sparseTerms = new Array(1)
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: { kind: "logical", operator: "and", terms: sparseTerms },
        },
        expectation,
      ),
    ).toThrow("sparse entries")
    const sparseValues = new Array(1)
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...base,
          predicate: { column: "score", kind: "in", values: sparseValues },
        },
        expectation,
      ),
    ).toThrow("sparse entries")
    const cyclic: Record<string, unknown> = { kind: "logical", operator: "and", terms: [] }
    cyclic.terms = [cyclic]
    expect(() =>
      decodeWireExecutionPlan(registry, { ...base, predicate: cyclic }, expectation),
    ).toThrow("Cyclic")
    let deep: Record<string, unknown> = { column: "score", kind: "is-null", negated: false }
    for (let index = 0; index < 34; index += 1) {
      deep = { kind: "logical", operator: "and", terms: [deep] }
    }
    expect(() =>
      decodeWireExecutionPlan(registry, { ...base, predicate: deep }, expectation),
    ).toThrow("maximum depth")
  })

  it("rejects write-scope, identity, schema-column, and value tampering", () => {
    const insert = wire(validPlans()[1] as ExecutionPlan) as unknown as Record<string, unknown>
    for (const values of [null, {}, { __nozzle_bucket: 43, workspace_id: "workspace-a" }]) {
      expect(() => decodeWireExecutionPlan(registry, { ...insert, values }, expectation)).toThrow()
    }
    const unknownValues = { ...(insert.values as Record<string, unknown>) }
    delete unknownValues.score
    unknownValues.unknown = "value"
    expect(() =>
      decodeWireExecutionPlan(registry, { ...insert, values: unknownValues }, expectation),
    ).toThrow("unregistered column")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...insert, values: { ...(insert.values as object), score: "1" } },
        expectation,
      ),
    ).toThrow("wrong SQLite storage type")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        {
          ...insert,
          values: { ...(insert.values as object), settings: ' {"theme":"dark"}' },
        },
        expectation,
      ),
    ).toThrow("canonical driver form")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...insert, values: { ...(insert.values as object), settings: "{" } },
        expectation,
      ),
    ).toThrow("incompatible with its schema column")

    const insertValues = { ...(insert.values as Record<string, unknown>) }
    delete insertValues.workspace_id
    expect(() =>
      decodeWireExecutionPlan(registry, { ...insert, values: insertValues }, expectation),
    ).toThrow("expected partition scope")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...insert, values: { ...(insert.values as object), workspace_id: "workspace-b" } },
        expectation,
      ),
    ).toThrow("expected partition scope")
    const noBucket = { ...(insert.values as Record<string, unknown>) }
    delete noBucket.__nozzle_bucket
    expect(() =>
      decodeWireExecutionPlan(registry, { ...insert, values: noBucket }, expectation),
    ).toThrow("expected partition scope")

    const update = wire(validPlans()[2] as ExecutionPlan) as unknown as Record<string, unknown>
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...update, values: { workspace_id: "workspace-a" } },
        expectation,
      ),
    ).toThrow("identity columns")
    expect(() =>
      decodeWireExecutionPlan(registry, { ...update, values: { id: "other" } }, expectation),
    ).toThrow("identity columns")
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...update, values: { __nozzle_bucket: 42 } },
        expectation,
      ),
    ).toThrow("internal bucket")
  })

  it("rejects malformed tagged BLOBs without retaining caller objects", () => {
    const insert = wire(validPlans()[1] as ExecutionPlan) as unknown as Record<string, unknown>
    const values = insert.values as Record<string, unknown>
    for (const payload of [
      { hex: "0", type: "blob" },
      { hex: "FF", type: "blob" },
      { hex: "00", type: "other" },
      { hex: 1, type: "blob" },
      { extra: true, hex: "00", type: "blob" },
      { hex: "00".repeat(2 * 1024 * 1024 + 1), type: "blob" },
    ]) {
      expect(() =>
        decodeWireExecutionPlan(
          registry,
          { ...insert, values: { ...values, payload } },
          expectation,
        ),
      ).toThrow()
    }
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...insert, values: { ...values, payload: { hex: "", type: "blob" } } },
        expectation,
      ),
    ).not.toThrow()
  })

  it("enforces native builder predicate resource bounds and mapping failures", () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    expect(() =>
      buildSelectPlan(registry, {
        predicate: eq(projects.settings, circular),
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
    ).toThrow("column value is invalid")

    const comparisons = Array.from({ length: 101 }, () => eq(projects.score, 1))
    expect(() =>
      buildSelectPlan(registry, {
        predicate: and(...comparisons),
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
    ).toThrow("too many bound values")
    const nodes = Array.from({ length: 128 }, () => isNull(projects.score))
    expect(() =>
      buildSelectPlan(registry, {
        predicate: and(...nodes),
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
    ).toThrow("too many nodes")
    let deep = isNull(projects.score)
    for (let index = 0; index < 34; index += 1) deep = and(deep)
    expect(() =>
      buildSelectPlan(registry, {
        predicate: deep,
        route,
        schemaId: expectation.schemaId,
        table: projects,
      }),
    ).toThrow("maximum nesting depth")

    const oversizedString = "x".repeat(2 * 1024 * 1024 + 1)
    expect(() =>
      buildInsertPlan(registry, {
        route,
        schemaId: expectation.schemaId,
        table: projects,
        values: { id: "large-string", name: oversizedString },
      }),
    ).toThrow("string value exceeds 2 MiB")
    expect(() =>
      buildInsertPlan(registry, {
        route,
        schemaId: expectation.schemaId,
        table: projects,
        values: { id: "large-blob", name: "large", payload: Buffer.alloc(2 * 1024 * 1024 + 1) },
      }),
    ).toThrow("BLOB value exceeds 2 MiB")

    const insert = wire(validPlans()[1] as ExecutionPlan) as unknown as Record<string, unknown>
    expect(() =>
      decodeWireExecutionPlan(
        registry,
        { ...insert, values: { ...(insert.values as object), name: oversizedString } },
        expectation,
      ),
    ).toThrow("wire string value exceeds 2 MiB")
  })
})
