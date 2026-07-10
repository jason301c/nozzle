import { NozzleError } from "@nozzle/core"
import type { InferInsertModel, InferSelectModel } from "drizzle-orm"
import type { AnySQLiteTable } from "drizzle-orm/sqlite-core"
import { compilePlan } from "./compiler.js"
import {
  type D1DatabaseLike,
  type D1ResultLike,
  decodeD1Result,
  executeDirect,
  type MutationPlan,
} from "./direct.js"
import type { PredicateInput } from "./expression.js"
import {
  assertExecutionPlanRegistry,
  buildDeletePlan,
  buildInsertPlan,
  buildSelectPlan,
  buildUpdatePlan,
  type DeletePlan,
  type ExecutionPlan,
  type InsertPlan,
  type ScopedRoute,
  type SelectPlan,
  type UpdatePlan,
} from "./plan.js"
import type { SchemaRegistry } from "./schema.js"

type ScopedInsert<TTable extends AnySQLiteTable, TPartitionKey extends string> = Omit<
  InferInsertModel<TTable>,
  TPartitionKey
> &
  Partial<Pick<InferInsertModel<TTable>, Extract<TPartitionKey, keyof InferInsertModel<TTable>>>>

function samePlanValue(
  left: ExecutionPlan["partitionValue"],
  right: ExecutionPlan["partitionValue"],
): boolean {
  if (typeof left === "object" && left !== null) {
    return typeof right === "object" && right !== null && left.hex === right.hex
  }
  return Object.is(left, right)
}

export interface PlannedQuery<TResult = unknown, TPlan extends ExecutionPlan = ExecutionPlan>
  extends PromiseLike<TResult> {
  execute(): Promise<TResult>
  toPlan(): Promise<TPlan>
}

export type PlannedQueryResult<TQuery> =
  TQuery extends PlannedQuery<infer TResult, ExecutionPlan> ? TResult : never

export type ScopedBatchResult<TQueries extends readonly PlannedQuery[]> = {
  readonly [TIndex in keyof TQueries]: PlannedQueryResult<TQueries[TIndex]>
}

class Query<TResult, TPlan extends ExecutionPlan> implements PlannedQuery<TResult, TPlan> {
  readonly #executePlan: (plan: TPlan) => Promise<TResult>
  readonly #plan: () => Promise<TPlan>
  #planPromise: Promise<TPlan> | undefined

  constructor(plan: () => Promise<TPlan>, executePlan: (plan: TPlan) => Promise<TResult>) {
    this.#plan = plan
    this.#executePlan = executePlan
  }

  toPlan(): Promise<TPlan> {
    this.#planPromise ??= this.#plan()
    return this.#planPromise
  }

  async execute(): Promise<TResult> {
    return this.#executePlan(await this.toPlan())
  }

  // biome-ignore lint/suspicious/noThenProperty: Drizzle-style query builders are intentionally awaitable.
  then<TResult1 = TResult, TResult2 = never>(
    onfulfilled?: ((value: TResult) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected)
  }
}

export class SelectQuery<TTable extends AnySQLiteTable> extends Query<
  readonly InferSelectModel<TTable>[],
  SelectPlan
> {
  readonly #create: (predicate?: PredicateInput, limit?: number) => SelectQuery<TTable>
  readonly #limit: number | undefined
  readonly #predicate: PredicateInput | undefined

  constructor(
    plan: () => Promise<SelectPlan>,
    executePlan: (plan: SelectPlan) => Promise<readonly InferSelectModel<TTable>[]>,
    create: (predicate?: PredicateInput, limit?: number) => SelectQuery<TTable>,
    predicate?: PredicateInput,
    limit?: number,
  ) {
    super(plan, executePlan)
    this.#create = create
    this.#predicate = predicate
    this.#limit = limit
  }

  where(predicate: PredicateInput): SelectQuery<TTable> {
    return this.#create(predicate, this.#limit)
  }

  limit(limit: number): SelectQuery<TTable> {
    return this.#create(this.#predicate, limit)
  }
}

export class UpdateQuery<TTable extends AnySQLiteTable> extends Query<D1ResultLike, UpdatePlan> {
  readonly #create: (predicate?: PredicateInput) => UpdateQuery<TTable>

  constructor(
    plan: () => Promise<UpdatePlan>,
    executePlan: (plan: UpdatePlan) => Promise<D1ResultLike>,
    create: (predicate?: PredicateInput) => UpdateQuery<TTable>,
  ) {
    super(plan, executePlan)
    this.#create = create
  }

  where(predicate: PredicateInput): UpdateQuery<TTable> {
    return this.#create(predicate)
  }
}

export class DeleteQuery<TTable extends AnySQLiteTable> extends Query<D1ResultLike, DeletePlan> {
  readonly #create: (predicate?: PredicateInput) => DeleteQuery<TTable>

  constructor(
    plan: () => Promise<DeletePlan>,
    executePlan: (plan: DeletePlan) => Promise<D1ResultLike>,
    create: (predicate?: PredicateInput) => DeleteQuery<TTable>,
  ) {
    super(plan, executePlan)
    this.#create = create
  }

  where(predicate: PredicateInput): DeleteQuery<TTable> {
    return this.#create(predicate)
  }
}

export interface ScopedDatabaseOptions<TPartitionKey extends string> {
  readonly partitionKey: TPartitionKey
  readonly registry: SchemaRegistry
  readonly resolveRoute: () => Promise<ScopedRoute>
  readonly schemaId: string
}

export interface ScopedPlanTransport {
  batch(plans: readonly ExecutionPlan[]): Promise<readonly D1ResultLike[]>
  execute(plan: ExecutionPlan): Promise<D1ResultLike>
}

export type ScopedDatabaseConnectionOptions<TPartitionKey extends string> =
  ScopedDatabaseOptions<TPartitionKey> &
    (
      | {
          readonly resolveDatabase: (shardId: string) => D1DatabaseLike
          readonly transport?: never
        }
      | {
          readonly resolveDatabase?: never
          readonly transport: ScopedPlanTransport
        }
    )

export class ScopedDatabase<TPartitionKey extends string> {
  readonly #options: ScopedDatabaseConnectionOptions<TPartitionKey>
  #route: Promise<ScopedRoute> | undefined

  constructor(options: ScopedDatabaseConnectionOptions<TPartitionKey>) {
    if (options.partitionKey !== options.registry.partitionKey) {
      throw new NozzleError(
        "ConfigurationError",
        "The scoped database partition key does not match its schema registry.",
      )
    }
    const direct = typeof options.resolveDatabase === "function"
    const routed =
      typeof options.transport === "object" &&
      options.transport !== null &&
      typeof options.transport.execute === "function" &&
      typeof options.transport.batch === "function"
    if (direct === routed) {
      throw new NozzleError(
        "ConfigurationError",
        "A scoped database requires exactly one direct database resolver or routed transport.",
      )
    }
    this.#options = options
  }

  #resolveRoute(): Promise<ScopedRoute> {
    this.#route ??= this.#options.resolveRoute()
    return this.#route
  }

  async #executeSelect<TResult>(plan: SelectPlan): Promise<readonly TResult[]> {
    if (this.#options.transport) {
      return decodeD1Result<TResult>(plan, await this.#options.transport.execute(plan))
    }
    return executeDirect<TResult>(this.#options.resolveDatabase(plan.shardId), plan)
  }

  async #executeMutation(plan: MutationPlan): Promise<D1ResultLike> {
    if (this.#options.transport) return this.#options.transport.execute(plan)
    return executeDirect(this.#options.resolveDatabase(plan.shardId), plan)
  }

  select(): {
    from: <TTable extends AnySQLiteTable>(table: TTable) => SelectQuery<TTable>
  } {
    return {
      from: <TTable extends AnySQLiteTable>(table: TTable): SelectQuery<TTable> => {
        const create = (predicate?: PredicateInput, limit?: number): SelectQuery<TTable> =>
          new SelectQuery<TTable>(
            async () =>
              buildSelectPlan(this.#options.registry, {
                table,
                route: await this.#resolveRoute(),
                schemaId: this.#options.schemaId,
                ...(predicate ? { predicate } : {}),
                ...(limit === undefined ? {} : { limit }),
              }),
            (plan) => this.#executeSelect<InferSelectModel<TTable>>(plan),
            create,
            predicate,
            limit,
          )
        return create()
      },
    }
  }

  insert<TTable extends AnySQLiteTable>(
    table: TTable,
  ): {
    values: (values: ScopedInsert<TTable, TPartitionKey>) => PlannedQuery<D1ResultLike, InsertPlan>
  } {
    return {
      values: (values) =>
        new Query<D1ResultLike, InsertPlan>(
          async () =>
            buildInsertPlan(this.#options.registry, {
              table,
              values,
              route: await this.#resolveRoute(),
              schemaId: this.#options.schemaId,
            }),
          (plan) => this.#executeMutation(plan),
        ),
    }
  }

  update<TTable extends AnySQLiteTable>(
    table: TTable,
  ): {
    set: (values: Partial<InferInsertModel<TTable>>) => UpdateQuery<TTable>
  } {
    return {
      set: (values) => {
        const create = (predicate?: PredicateInput): UpdateQuery<TTable> =>
          new UpdateQuery<TTable>(
            async () =>
              buildUpdatePlan(this.#options.registry, {
                table,
                values,
                route: await this.#resolveRoute(),
                schemaId: this.#options.schemaId,
                ...(predicate ? { predicate } : {}),
              }),
            (plan) => this.#executeMutation(plan),
            create,
          )
        return create()
      },
    }
  }

  delete<TTable extends AnySQLiteTable>(table: TTable): DeleteQuery<TTable> {
    const create = (predicate?: PredicateInput): DeleteQuery<TTable> =>
      new DeleteQuery<TTable>(
        async () =>
          buildDeletePlan(this.#options.registry, {
            table,
            route: await this.#resolveRoute(),
            schemaId: this.#options.schemaId,
            ...(predicate ? { predicate } : {}),
          }),
        (plan) => this.#executeMutation(plan),
        create,
      )
    return create()
  }

  async batch<const TQueries extends readonly PlannedQuery[]>(
    queries: TQueries,
  ): Promise<ScopedBatchResult<TQueries>> {
    if (queries.length === 0) {
      throw new NozzleError("ConfigurationError", "A scoped batch cannot be empty.")
    }
    if (queries.length > 49) {
      throw new NozzleError("CapacityGuardError", "A scoped batch cannot exceed 49 statements.")
    }
    const plans = await Promise.all(queries.map((query) => query.toPlan()))
    const first = plans[0]
    if (!first) throw new NozzleError("ConfigurationError", "A scoped batch cannot be empty.")
    for (const plan of plans) assertExecutionPlanRegistry(plan, this.#options.registry)
    if (
      plans.some(
        (plan) =>
          plan.shardId !== first.shardId ||
          plan.bucketId !== first.bucketId ||
          plan.routeEpoch !== first.routeEpoch ||
          plan.schemaId !== first.schemaId ||
          plan.partitionDigestHex !== first.partitionDigestHex ||
          !samePlanValue(plan.partitionValue, first.partitionValue),
      )
    ) {
      throw new NozzleError(
        "CrossShardTransactionUnsupportedError",
        "Every scoped batch statement must resolve to one partition, schema, shard, bucket, and route epoch.",
      )
    }

    let dataResults: readonly D1ResultLike[]
    if (this.#options.transport) {
      dataResults = await this.#options.transport.batch(plans)
      if (dataResults.length !== plans.length) {
        throw new NozzleError("ShardUnavailableError", "The router returned an incomplete batch.")
      }
    } else {
      const compiled = plans.map(compilePlan)
      const database = this.#options.resolveDatabase(first.shardId)
      const statements = [compiled[0]?.authorization, ...compiled.map((entry) => entry.data)]
        .filter((statement) => statement !== undefined)
        .map((statement) => database.prepare(statement.sql).bind(...statement.params))
      const results = await database.batch(statements)
      const authorization = results[0]
      if (authorization?.results.length !== 1) {
        throw new NozzleError(
          "StaleRouteRejectedError",
          "Shard ownership rejected the batch route.",
        )
      }
      if (results.length !== plans.length + 1) {
        throw new NozzleError("ShardUnavailableError", "D1 returned an incomplete batch result.")
      }
      dataResults = results.slice(1)
    }
    return plans.map((plan, index) =>
      decodeD1Result(plan, dataResults[index] as D1ResultLike),
    ) as ScopedBatchResult<TQueries>
  }
}

export function createScopedDatabase<TPartitionKey extends string>(
  options: ScopedDatabaseConnectionOptions<TPartitionKey>,
): ScopedDatabase<TPartitionKey> {
  return new ScopedDatabase(options)
}
