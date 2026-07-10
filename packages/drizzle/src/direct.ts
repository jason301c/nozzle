import { NozzleError } from "@nozzle/core"
import { compilePlan, type D1BindingValue } from "./compiler.js"
import { type ExecutionPlan, getExecutionPlanRegistry, type SelectPlan } from "./plan.js"

export type MutationPlan = Exclude<ExecutionPlan, SelectPlan>

export interface D1ResultLike<T = Record<string, unknown>> {
  readonly meta: Readonly<Record<string, unknown>>
  readonly results: readonly T[]
  readonly success: true
}

export interface D1PreparedStatementLike {
  bind(...values: readonly D1BindingValue[]): D1PreparedStatementLike
}

export interface D1DatabaseLike {
  batch(statements: readonly D1PreparedStatementLike[]): Promise<readonly D1ResultLike[]>
  prepare(sql: string): D1PreparedStatementLike
}

function decodeSelectedRows<T>(plan: SelectPlan, data: D1ResultLike): readonly T[] {
  const table = getExecutionPlanRegistry(plan).tableByName(plan.table)
  const columns = new Map(table.columns.map((column) => [column.dbName, column]))
  return data.results.map((rawRow) => {
    if (typeof rawRow !== "object" || rawRow === null || Array.isArray(rawRow)) {
      throw new NozzleError("SchemaDriftError", "D1 returned a malformed selected row.")
    }
    const row = rawRow as Readonly<Record<string, unknown>>
    const decoded: Record<string, unknown> = {}
    for (const selection of plan.columns) {
      const column = columns.get(selection.column)
      if (!column || !Object.hasOwn(row, selection.resultKey)) {
        throw new NozzleError("SchemaDriftError", "D1 omitted a selected schema column.", {
          details: { column: selection.column, table: plan.table },
        })
      }
      const value = row[selection.resultKey]
      let mapped: unknown
      try {
        mapped = value === null ? null : column.column.mapFromDriverValue(value)
      } catch {
        throw new NozzleError("SchemaDriftError", "D1 returned an incompatible column value.", {
          details: { column: selection.column, table: plan.table },
        })
      }
      Object.defineProperty(decoded, selection.resultKey, {
        configurable: true,
        enumerable: true,
        value: mapped,
        writable: true,
      })
    }
    return decoded as T
  })
}

export function decodeD1Result<T>(plan: SelectPlan, data: D1ResultLike): readonly T[]
export function decodeD1Result(plan: MutationPlan, data: D1ResultLike): D1ResultLike
export function decodeD1Result<T>(
  plan: ExecutionPlan,
  data: D1ResultLike,
): D1ResultLike | readonly T[]
export function decodeD1Result<T>(
  plan: ExecutionPlan,
  data: D1ResultLike,
): D1ResultLike | readonly T[] {
  return plan.operation === "select" ? decodeSelectedRows<T>(plan, data) : data
}

export function executeDirect<T>(database: D1DatabaseLike, plan: SelectPlan): Promise<readonly T[]>
export function executeDirect(database: D1DatabaseLike, plan: MutationPlan): Promise<D1ResultLike>
export function executeDirect<T>(
  database: D1DatabaseLike,
  plan: ExecutionPlan,
): Promise<D1ResultLike | readonly T[]>
export async function executeDirect<T = Record<string, unknown>>(
  database: D1DatabaseLike,
  plan: ExecutionPlan,
): Promise<D1ResultLike | readonly T[]> {
  const data = await executeDirectRaw(database, plan)
  return plan.operation === "select" ? decodeD1Result<T>(plan, data) : decodeD1Result(plan, data)
}

export async function executeDirectRaw(
  database: D1DatabaseLike,
  plan: ExecutionPlan,
): Promise<D1ResultLike> {
  const compiled = compilePlan(plan)
  const statements = [compiled.authorization, compiled.data].map((statement) =>
    database.prepare(statement.sql).bind(...statement.params),
  )
  const results = await database.batch(statements)
  const authorization = results[0]
  const data = results[1]
  if (authorization?.results.length !== 1) {
    throw new NozzleError("StaleRouteRejectedError", "Shard ownership rejected the route.", {
      details: { bucketId: plan.bucketId, routeEpoch: plan.routeEpoch, shardId: plan.shardId },
    })
  }
  if (!data) {
    throw new NozzleError("ShardUnavailableError", "D1 returned an incomplete batch result.")
  }
  return data
}
