import { NozzleError } from "@nozzle/core"
import {
  assertTrustedExecutionPlan,
  type ExecutionPlan,
  type PlanPredicate,
  type PlanValue,
} from "./plan.js"

export type D1BindingValue = boolean | null | number | string | Uint8Array

export interface CompiledStatement {
  readonly params: readonly D1BindingValue[]
  readonly sql: string
}

export interface CompiledPlan {
  readonly authorization: CompiledStatement
  readonly data: CompiledStatement
}

const OPERATOR_SQL = {
  eq: "=",
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
  ne: "!=",
} as const

function identifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function compareCodeUnits(left: string, right: string): number {
  if (left < right) return -1
  if (left > right) return 1
  return 0
}

class Parameters {
  readonly values: D1BindingValue[] = []

  bind(value: PlanValue): string {
    this.values.push(decodePlanValue(value))
    if (this.values.length > 100) {
      throw new NozzleError("CapacityGuardError", "A D1 statement cannot exceed 100 parameters.")
    }
    return `?${this.values.length}`
  }
}

function decodePlanValue(value: PlanValue): D1BindingValue {
  if (typeof value !== "object" || value === null) return value
  if (!/^(?:[0-9a-f]{2})*$/u.test(value.hex)) {
    throw new NozzleError("UnsafeQueryRequiredError", "A BLOB plan value is malformed.")
  }
  const bytes = new Uint8Array(value.hex.length / 2)
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.hex.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes
}

function compilePredicate(predicate: PlanPredicate, parameters: Parameters): string {
  if (predicate.kind === "logical") {
    return `(${predicate.terms.map((term) => compilePredicate(term, parameters)).join(` ${predicate.operator.toUpperCase()} `)})`
  }
  const column = identifier(predicate.column)
  if (predicate.kind === "is-null") return `${column} IS ${predicate.negated ? "NOT " : ""}NULL`
  if (predicate.kind === "in") {
    return `${column} IN (${predicate.values.map((value) => parameters.bind(value)).join(", ")})`
  }
  return `${column} ${OPERATOR_SQL[predicate.operator]} ${parameters.bind(predicate.value)}`
}

function ownershipExists(plan: ExecutionPlan, parameters: Parameters): string {
  const bucket = parameters.bind(plan.bucketId)
  const epoch = parameters.bind(plan.routeEpoch)
  const schema = parameters.bind(plan.schemaId)
  const partitionDigest = parameters.bind(plan.partitionDigestHex)
  return `EXISTS (SELECT 1 FROM "nozzle_bucket_ownership" WHERE "bucket_id" = ${bucket} AND "route_epoch" = ${epoch} AND "state" = 'writable') AND EXISTS (SELECT 1 FROM "nozzle_schema_state" WHERE "schema_id" = ${schema} AND "active" = 1) AND NOT EXISTS (SELECT 1 FROM "nozzle_partition_fences" WHERE "partition_digest" = ${partitionDigest})`
}

function whereClause(plan: ExecutionPlan, parameters: Parameters): string {
  const scope = `${identifier(plan.partitionColumn)} = ${parameters.bind(plan.partitionValue)} AND "__nozzle_bucket" = ${parameters.bind(plan.bucketId)}`
  const ownership = ownershipExists(plan, parameters)
  const predicate =
    "predicate" in plan && plan.predicate
      ? ` AND ${compilePredicate(plan.predicate, parameters)}`
      : ""
  return `(${scope} AND ${ownership}${predicate})`
}

function compileData(plan: ExecutionPlan): CompiledStatement {
  const parameters = new Parameters()
  let sql: string
  switch (plan.operation) {
    case "select": {
      const columns = plan.columns
        .map(({ column, resultKey }) => `${identifier(column)} AS ${identifier(resultKey)}`)
        .join(", ")
      const where = whereClause(plan, parameters)
      const limit = plan.limit === undefined ? "" : ` LIMIT ${parameters.bind(plan.limit)}`
      sql = `SELECT ${columns} FROM ${identifier(plan.table)} WHERE ${where}${limit}`
      break
    }
    case "insert": {
      const entries = Object.entries(plan.values).sort(([left], [right]) =>
        compareCodeUnits(left, right),
      )
      const columns = entries.map(([column]) => identifier(column)).join(", ")
      const values = entries.map(([, value]) => parameters.bind(value)).join(", ")
      sql = `INSERT INTO ${identifier(plan.table)} (${columns}) SELECT ${values} WHERE ${ownershipExists(plan, parameters)}`
      break
    }
    case "update": {
      const assignments = Object.entries(plan.values)
        .sort(([left], [right]) => compareCodeUnits(left, right))
        .map(([column, value]) => `${identifier(column)} = ${parameters.bind(value)}`)
        .join(", ")
      sql = `UPDATE ${identifier(plan.table)} SET ${assignments} WHERE ${whereClause(plan, parameters)}`
      break
    }
    case "delete":
      sql = `DELETE FROM ${identifier(plan.table)} WHERE ${whereClause(plan, parameters)}`
      break
  }
  return Object.freeze({ params: Object.freeze(parameters.values), sql })
}

export function compilePlan(plan: ExecutionPlan): CompiledPlan {
  assertTrustedExecutionPlan(plan)
  const authorizationParameters = new Parameters()
  const bucket = authorizationParameters.bind(plan.bucketId)
  const epoch = authorizationParameters.bind(plan.routeEpoch)
  const schema = authorizationParameters.bind(plan.schemaId)
  const partitionDigest = authorizationParameters.bind(plan.partitionDigestHex)
  return Object.freeze({
    authorization: Object.freeze({
      params: Object.freeze(authorizationParameters.values),
      sql: `SELECT "route_epoch" AS "routeEpoch" FROM "nozzle_bucket_ownership" WHERE "bucket_id" = ${bucket} AND "route_epoch" = ${epoch} AND "state" = 'writable' AND EXISTS (SELECT 1 FROM "nozzle_schema_state" WHERE "schema_id" = ${schema} AND "active" = 1) AND NOT EXISTS (SELECT 1 FROM "nozzle_partition_fences" WHERE "partition_digest" = ${partitionDigest}) LIMIT 1`,
    }),
    data: compileData(plan),
  })
}
