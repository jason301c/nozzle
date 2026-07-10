import { NozzleError } from "@nozzle/core"
import {
  buildInsertPlan,
  buildSelectPlan,
  type D1BindingValue,
  type D1DatabaseLike,
  type D1PreparedStatementLike,
  type D1ResultLike,
  type ExecutionPlan,
  type PlanValue,
  SchemaRegistry,
  type ScopedRoute,
} from "@nozzle/drizzle"
import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import { RouterLeaf } from "../src/leaf.js"
import {
  createRouterTransport,
  type RouterServiceBinding,
  routerFailure,
  routerSuccess,
} from "../src/transport.js"
import { encodeWireD1Result, ROUTER_PROTOCOL_VERSION } from "../src/wire.js"

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text().notNull(),
  active: integer({ mode: "boolean" }).notNull(),
})
const registry = new SchemaRegistry({ schema: { projects }, partitionKey: "workspaceId" })
const binaryRows = sqliteTable("binary_rows", {
  id: text().primaryKey(),
  workspaceId: blob("workspace_id", { mode: "buffer" }).notNull(),
})
const binaryRegistry = new SchemaRegistry({
  schema: { binaryRows },
  partitionKey: "workspaceId",
})
const routeA: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: "11".repeat(32),
  partitionValue: "workspace-a",
  routeEpoch: 7,
  shardId: "shard-a",
}
const routeB: ScopedRoute = {
  bucketId: 43,
  partitionDigestHex: "22".repeat(32),
  partitionValue: "workspace-b",
  routeEpoch: 8,
  shardId: "shard-a",
}

class Statement implements D1PreparedStatementLike {
  readonly sql: string
  params: readonly D1BindingValue[] = []

  constructor(sql: string) {
    this.sql = sql
  }

  bind(...values: readonly D1BindingValue[]): D1PreparedStatementLike {
    this.params = values
    return this
  }
}

function d1Result(results: readonly Record<string, unknown>[] = []): D1ResultLike {
  return { meta: { changes: 0 }, results, success: true }
}

class Database implements D1DatabaseLike {
  fail: unknown
  incomplete = false
  stale = false

  async batch(statements: readonly D1PreparedStatementLike[]): Promise<readonly D1ResultLike[]> {
    if (this.fail) throw this.fail
    const authorization = d1Result(this.stale ? [] : [{ routeEpoch: 7 }])
    const data = statements.slice(1).map(() =>
      d1Result([
        {
          active: 1,
          id: "project-a",
          name: "Nozzle",
          workspaceId: "workspace-a",
        },
      ]),
    )
    return this.incomplete ? [authorization] : [authorization, ...data]
  }

  prepare(sql: string): D1PreparedStatementLike {
    return new Statement(sql)
  }
}

function plan(route: ScopedRoute = routeA): ExecutionPlan {
  return buildSelectPlan(registry, {
    route,
    schemaId: "application-v1",
    table: projects,
  })
}

function leaf(database = new Database()): RouterLeaf {
  return new RouterLeaf({
    database,
    leafShardId: "shard-a",
    registry,
    resolveRoute: async ({ partitionValue }: { partitionValue: PlanValue }) =>
      partitionValue === "workspace-b" ? routeB : routeA,
    schemaId: "application-v1",
  })
}

function serviceReturning(value: unknown): RouterServiceBinding {
  return {
    async executeBatch() {
      return value
    },
    async executePlan() {
      return value
    },
  }
}

describe("safe router leaf and client transport", () => {
  it("rejects every malformed router leaf dependency", () => {
    const valid = {
      database: new Database(),
      leafShardId: "shard-a",
      registry,
      resolveRoute: async () => routeA,
      schemaId: "application-v1",
    }
    for (const options of [
      null,
      { ...valid, leafShardId: "" },
      { ...valid, leafShardId: 1 },
      { ...valid, schemaId: "" },
      { ...valid, schemaId: 1 },
      { ...valid, registry: {} },
      { ...valid, resolveRoute: null },
      { ...valid, database: { batch: valid.database.batch } },
      { ...valid, database: { prepare: valid.database.prepare } },
    ]) {
      expect(() => new RouterLeaf(options as never)).toThrow("options are invalid")
    }
  })

  it("executes one validated plan through the versioned client transport", async () => {
    const transport = createRouterTransport(leaf())
    await expect(transport.execute(plan())).resolves.toEqual({
      meta: { changes: 0 },
      results: [{ active: 1, id: "project-a", name: "Nozzle", workspaceId: "workspace-a" }],
      success: true,
    })
  })

  it("executes an atomic same-route batch and preserves result order", async () => {
    const transport = createRouterTransport(leaf())
    const plans = [
      plan(),
      buildInsertPlan(registry, {
        route: routeA,
        schemaId: "application-v1",
        table: projects,
        values: { active: true, id: "project-b", name: "Second" },
      }),
    ]
    await expect(transport.batch(plans)).resolves.toHaveLength(2)
  })

  it("compares binary partition scope by exact bytes in router batches", async () => {
    const binaryRouteA: ScopedRoute = {
      ...routeA,
      partitionDigestHex: "33".repeat(32),
      partitionValue: Uint8Array.of(1),
    }
    const binaryRouteB: ScopedRoute = {
      ...routeB,
      partitionDigestHex: "44".repeat(32),
      partitionValue: Uint8Array.of(2),
    }
    const binaryLeaf = new RouterLeaf({
      database: new Database(),
      leafShardId: "shard-a",
      registry: binaryRegistry,
      resolveRoute: async ({ partitionValue }) =>
        typeof partitionValue === "object" && partitionValue !== null && partitionValue.hex === "01"
          ? binaryRouteA
          : binaryRouteB,
      schemaId: "application-v1",
    })
    const binaryPlan = (route: ScopedRoute) =>
      buildSelectPlan(binaryRegistry, {
        route,
        schemaId: "application-v1",
        table: binaryRows,
      })

    await expect(
      binaryLeaf.executeBatch({
        plans: [binaryPlan(binaryRouteA), binaryPlan(binaryRouteA)],
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ ok: true })
    await expect(
      binaryLeaf.executeBatch({
        plans: [binaryPlan(binaryRouteA), binaryPlan(binaryRouteB)],
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "CrossShardTransactionUnsupportedError" },
      ok: false,
    })
  })

  it("fails closed on malformed requests, stale routes, and tampered plans", async () => {
    const router = leaf()
    await expect(router.executePlan(null)).resolves.toMatchObject({
      error: { code: "UnsafeQueryRequiredError" },
      ok: false,
    })
    await expect(router.executePlan(new Date())).resolves.toMatchObject({
      error: { code: "UnsafeQueryRequiredError" },
      ok: false,
    })
    await expect(router.executePlan({ plan: plan(), protocolVersion: 2 })).resolves.toMatchObject({
      error: { code: "UnsafeQueryRequiredError" },
      ok: false,
    })
    await expect(
      router.executePlan({
        plan: { ...structuredClone(plan()), bucketId: 99 },
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ error: { code: "UnsafeQueryRequiredError" }, ok: false })

    const wrongLeaf = new RouterLeaf({
      database: new Database(),
      leafShardId: "shard-b",
      registry,
      resolveRoute: async () => routeA,
      schemaId: "application-v1",
    })
    await expect(
      wrongLeaf.executePlan({ plan: plan(), protocolVersion: ROUTER_PROTOCOL_VERSION }),
    ).resolves.toMatchObject({ error: { code: "StaleRouteRejectedError" }, ok: false })

    const stale = new Database()
    stale.stale = true
    await expect(
      leaf(stale).executePlan({ plan: plan(), protocolVersion: ROUTER_PROTOCOL_VERSION }),
    ).resolves.toMatchObject({ error: { code: "StaleRouteRejectedError" }, ok: false })
    const staleBatch = new Database()
    staleBatch.stale = true
    await expect(
      leaf(staleBatch).executeBatch({
        plans: [plan(), plan()],
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ error: { code: "StaleRouteRejectedError" }, ok: false })
  })

  it("rejects empty, oversized, sparse, and cross-route batches", async () => {
    const router = leaf()
    for (const plans of [[], new Array(50).fill(plan())]) {
      await expect(
        router.executeBatch({ plans, protocolVersion: ROUTER_PROTOCOL_VERSION }),
      ).resolves.toMatchObject({ error: { code: "CapacityGuardError" }, ok: false })
    }
    const sparse = new Array(1)
    await expect(
      router.executeBatch({ plans: sparse, protocolVersion: ROUTER_PROTOCOL_VERSION }),
    ).resolves.toMatchObject({ error: { code: "UnsafeQueryRequiredError" }, ok: false })
    await expect(
      router.executeBatch({
        plans: [plan(routeA), plan(routeB)],
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      error: { code: "CrossShardTransactionUnsupportedError" },
      ok: false,
    })
  })

  it("turns unknown failures and incomplete D1 responses into stable failures", async () => {
    const failed = new Database()
    failed.fail = new Error("fictional-secret")
    await expect(
      leaf(failed).executePlan({ plan: plan(), protocolVersion: ROUTER_PROTOCOL_VERSION }),
    ).resolves.toMatchObject({
      error: { code: "OperationInterventionRequiredError", message: "Unexpected internal error." },
      ok: false,
    })
    const incomplete = new Database()
    incomplete.incomplete = true
    await expect(
      leaf(incomplete).executePlan({ plan: plan(), protocolVersion: ROUTER_PROTOCOL_VERSION }),
    ).resolves.toMatchObject({ error: { code: "ShardUnavailableError" }, ok: false })
    await expect(
      leaf(incomplete).executeBatch({
        plans: [plan(), plan()],
        protocolVersion: ROUTER_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({ error: { code: "ShardUnavailableError" }, ok: false })
  })

  it("reconstructs stable remote errors and rejects malformed responses", async () => {
    const remote = createRouterTransport(
      serviceReturning(routerFailure(new NozzleError("StaleRouteRejectedError", "stale"))),
    )
    await expect(remote.execute(plan())).rejects.toMatchObject({
      code: "StaleRouteRejectedError",
      message: "stale",
    })

    const wire = encodeWireD1Result(d1Result())
    for (const response of [
      null,
      new Date(),
      { ok: true, protocolVersion: 1, result: [wire], extra: true },
      { ok: true, protocolVersion: 2, result: [wire] },
      { ok: true, protocolVersion: 1, result: wire },
      { ok: false, protocolVersion: 2, error: routerFailure(new Error()).error },
      { ok: "yes", protocolVersion: 1, result: [wire] },
    ]) {
      await expect(
        createRouterTransport(serviceReturning(response)).execute(plan()),
      ).rejects.toMatchObject({
        code: "ShardUnavailableError",
      })
    }
    await expect(
      createRouterTransport(serviceReturning(routerSuccess([]))).execute(plan()),
    ).rejects.toThrow("invalid result count")

    const malformedError = structuredClone(
      routerFailure(new NozzleError("StaleRouteRejectedError", "stale")),
    ) as unknown as { error: Record<string, unknown> }
    malformedError.error.code = "NotARealError"
    await expect(
      createRouterTransport(serviceReturning(malformedError)).execute(plan()),
    ).rejects.toThrow("malformed error")
    const wrongFamily = structuredClone(
      routerFailure(new NozzleError("StaleRouteRejectedError", "stale")),
    ) as unknown as { error: Record<string, unknown> }
    wrongFamily.error.family = "provider"
    await expect(
      createRouterTransport(serviceReturning(wrongFamily)).execute(plan()),
    ).rejects.toThrow("inconsistent error family")
    await expect(
      createRouterTransport(
        serviceReturning(routerSuccess(new Array(50).fill(encodeWireD1Result(d1Result())))),
      ).execute(plan()),
    ).rejects.toMatchObject({ code: "CapacityGuardError" })
  })

  it("validates the Service Binding and client-side batch cardinality", async () => {
    expect(() => createRouterTransport(null as never)).toThrow("Service Binding")
    const transport = createRouterTransport(serviceReturning(routerSuccess([])))
    await expect(transport.batch([])).rejects.toMatchObject({ code: "CapacityGuardError" })
    await expect(transport.batch(new Array(50).fill(plan()))).rejects.toMatchObject({
      code: "CapacityGuardError",
    })
    await expect(transport.batch([plan()])).rejects.toThrow("invalid batch result count")
  })
})
