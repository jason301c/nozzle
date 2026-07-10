import { NozzleError } from "@nozzle/core"
import {
  compilePlan,
  type D1DatabaseLike,
  decodeWireExecutionPlan,
  type ExecutionPlan,
  type PlanValue,
  readWireExecutionPlanRouteHint,
  SchemaRegistry,
  type ScopedRoute,
} from "@nozzle/drizzle"
import {
  type RouterBatchRequest,
  type RouterExecuteRequest,
  type RouterFailure,
  type RouterSuccess,
  routerFailure,
  routerSuccess,
} from "./transport.js"
import {
  encodeWireD1Result,
  MAX_ROUTER_BATCH_STATEMENTS,
  ROUTER_PROTOCOL_VERSION,
  type WireD1Result,
} from "./wire.js"

export interface RouterRouteHint {
  readonly partitionValue: PlanValue
  readonly table: string
}

export interface RouterLeafOptions {
  readonly database: D1DatabaseLike
  readonly leafShardId: string
  readonly registry: SchemaRegistry
  readonly resolveRoute: (hint: RouterRouteHint) => Promise<ScopedRoute>
  readonly schemaId: string
}

export type RouterLeafResponse = RouterFailure | RouterSuccess<readonly WireD1Result[]>

function requestError(message: string): never {
  throw new NozzleError("UnsafeQueryRequiredError", message)
}

function requestRecord(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return requestError("A router request must be a plain object.")
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return requestError("A router request must be a plain object.")
  }
  return value as Readonly<Record<string, unknown>>
}

function assertRequestKeys(record: Readonly<Record<string, unknown>>, payload: string): void {
  const expected = new Set([payload, "protocolVersion"])
  const keys = Object.keys(record)
  if (
    keys.length !== expected.size ||
    keys.some((key) => !expected.has(key)) ||
    record.protocolVersion !== ROUTER_PROTOCOL_VERSION
  ) {
    requestError("A router request uses an unsupported protocol or envelope.")
  }
}

function planValuesEqual(left: PlanValue, right: PlanValue): boolean {
  if (typeof left === "object" && left !== null) {
    return typeof right === "object" && right !== null && left.hex === right.hex
  }
  return Object.is(left, right)
}

function assertOneScope(plans: readonly ExecutionPlan[]): void {
  const first = plans[0] as ExecutionPlan
  if (
    plans.some(
      (plan) =>
        plan.shardId !== first.shardId ||
        plan.bucketId !== first.bucketId ||
        plan.routeEpoch !== first.routeEpoch ||
        plan.schemaId !== first.schemaId ||
        plan.partitionDigestHex !== first.partitionDigestHex ||
        !planValuesEqual(plan.partitionValue, first.partitionValue),
    )
  ) {
    throw new NozzleError(
      "CrossShardTransactionUnsupportedError",
      "A router batch must target one validated partition route.",
    )
  }
}

export class RouterLeaf {
  readonly #options: RouterLeafOptions

  constructor(options: RouterLeafOptions) {
    if (
      typeof options !== "object" ||
      options === null ||
      typeof options.leafShardId !== "string" ||
      options.leafShardId.trim().length === 0 ||
      typeof options.schemaId !== "string" ||
      options.schemaId.trim().length === 0 ||
      !(options.registry instanceof SchemaRegistry) ||
      typeof options.resolveRoute !== "function" ||
      typeof options.database?.prepare !== "function" ||
      typeof options.database?.batch !== "function"
    ) {
      throw new NozzleError("ConfigurationError", "Router leaf options are invalid.")
    }
    this.#options = options
  }

  async #decodePlan(input: unknown): Promise<ExecutionPlan> {
    const hint = readWireExecutionPlanRouteHint(this.#options.registry, input)
    const route = await this.#options.resolveRoute(hint)
    if (route.shardId !== this.#options.leafShardId) {
      throw new NozzleError("StaleRouteRejectedError", "The route does not belong to this leaf.")
    }
    return decodeWireExecutionPlan(this.#options.registry, input, {
      route,
      schemaId: this.#options.schemaId,
    })
  }

  async executePlan(request: RouterExecuteRequest | unknown): Promise<RouterLeafResponse> {
    try {
      const record = requestRecord(request)
      assertRequestKeys(record, "plan")
      const plan = await this.#decodePlan(record.plan)
      const compiled = compilePlan(plan)
      const statements = [compiled.authorization, compiled.data].map((statement) =>
        this.#options.database.prepare(statement.sql).bind(...statement.params),
      )
      const results = await this.#options.database.batch(statements)
      if (results[0]?.results.length !== 1) {
        throw new NozzleError("StaleRouteRejectedError", "Shard ownership rejected the route.")
      }
      const data = results[1]
      if (!data || results.length !== 2) {
        throw new NozzleError("ShardUnavailableError", "D1 returned an incomplete router result.")
      }
      return routerSuccess(Object.freeze([encodeWireD1Result(data)]))
    } catch (error) {
      return routerFailure(error)
    }
  }

  async executeBatch(request: RouterBatchRequest | unknown): Promise<RouterLeafResponse> {
    try {
      const record = requestRecord(request)
      assertRequestKeys(record, "plans")
      if (
        !Array.isArray(record.plans) ||
        record.plans.length === 0 ||
        record.plans.length > MAX_ROUTER_BATCH_STATEMENTS
      ) {
        throw new NozzleError(
          "CapacityGuardError",
          "A router batch must contain between 1 and 49 plans.",
        )
      }
      for (let index = 0; index < record.plans.length; index += 1) {
        if (!Object.hasOwn(record.plans, index))
          return requestError("A router batch cannot be sparse.")
      }
      const plans = await Promise.all(record.plans.map((plan) => this.#decodePlan(plan)))
      assertOneScope(plans)
      const compiled = plans.map(compilePlan)
      const statements = [compiled[0]?.authorization, ...compiled.map((entry) => entry.data)]
        .filter((statement) => statement !== undefined)
        .map((statement) => this.#options.database.prepare(statement.sql).bind(...statement.params))
      const results = await this.#options.database.batch(statements)
      if (results[0]?.results.length !== 1) {
        throw new NozzleError("StaleRouteRejectedError", "Shard ownership rejected the batch.")
      }
      if (results.length !== plans.length + 1) {
        throw new NozzleError("ShardUnavailableError", "D1 returned an incomplete router batch.")
      }
      return routerSuccess(Object.freeze(results.slice(1).map(encodeWireD1Result)))
    } catch (error) {
      return routerFailure(error)
    }
  }
}
