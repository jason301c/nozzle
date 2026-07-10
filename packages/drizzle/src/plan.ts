import { bytesToHex, NozzleError } from "@nozzle/core"
import type { AnySQLiteTable } from "drizzle-orm/sqlite-core"
import type { PredicateInput } from "./expression.js"
import type { ColumnMetadata, SchemaRegistry, TableMetadata } from "./schema.js"

export interface PlanBlob {
  readonly hex: string
  readonly type: "blob"
}

export type PlanValue = boolean | null | number | PlanBlob | string

export type PlanPredicate =
  | {
      readonly column: string
      readonly kind: "comparison"
      readonly operator: "eq" | "gt" | "gte" | "lt" | "lte" | "ne"
      readonly value: PlanValue
    }
  | { readonly column: string; readonly kind: "in"; readonly values: readonly PlanValue[] }
  | { readonly column: string; readonly kind: "is-null"; readonly negated: boolean }
  | {
      readonly kind: "logical"
      readonly operator: "and" | "or"
      readonly terms: readonly PlanPredicate[]
    }

export interface ScopedRoute {
  readonly bucketId: number
  readonly partitionDigestHex: string
  readonly partitionValue: unknown
  readonly routeEpoch: number
  readonly shardId: string
}

interface BasePlan {
  readonly bucketId: number
  readonly operation: "delete" | "insert" | "select" | "update"
  readonly partitionColumn: string
  readonly partitionDigestHex: string
  readonly partitionValue: PlanValue
  readonly routeEpoch: number
  readonly schemaId: string
  readonly shardId: string
  readonly table: string
  readonly version: 1
}

export interface SelectPlan extends BasePlan {
  readonly operation: "select"
  readonly columns: readonly { readonly column: string; readonly resultKey: string }[]
  readonly limit?: number
  readonly predicate?: PlanPredicate
}

export interface InsertPlan extends BasePlan {
  readonly operation: "insert"
  readonly values: Readonly<Record<string, PlanValue>>
}

export interface UpdatePlan extends BasePlan {
  readonly operation: "update"
  readonly predicate?: PlanPredicate
  readonly values: Readonly<Record<string, PlanValue>>
}

export interface DeletePlan extends BasePlan {
  readonly operation: "delete"
  readonly predicate?: PlanPredicate
}

export type ExecutionPlan = DeletePlan | InsertPlan | SelectPlan | UpdatePlan

const EXECUTION_PLAN_REGISTRIES = new WeakMap<object, SchemaRegistry>()
const COMPARISON_OPERATORS = new Set(["eq", "gt", "gte", "lt", "lte", "ne"])
const MAX_PREDICATE_BOUND_VALUES = 100
const MAX_PREDICATE_DEPTH = 32
const MAX_PREDICATE_NODES = 128
const MAX_D1_VALUE_BYTES = 2 * 1024 * 1024
const PARTITION_DIGEST_PATTERN = /^[0-9a-f]{64}$/u

function trustExecutionPlan<TPlan extends ExecutionPlan>(
  plan: TPlan,
  registry: SchemaRegistry,
): TPlan {
  EXECUTION_PLAN_REGISTRIES.set(plan, registry)
  return plan
}

export function assertTrustedExecutionPlan(plan: ExecutionPlan): void {
  if (!EXECUTION_PLAN_REGISTRIES.has(plan)) {
    throw new NozzleError(
      "UnsafeQueryRequiredError",
      "Execution plans must be produced by the scoped Nozzle query builder.",
    )
  }
}

export function assertExecutionPlanRegistry(plan: ExecutionPlan, registry: SchemaRegistry): void {
  assertTrustedExecutionPlan(plan)
  if (EXECUTION_PLAN_REGISTRIES.get(plan) !== registry) {
    throw new NozzleError(
      "UnsafeQueryRequiredError",
      "The execution plan belongs to a different Nozzle schema registry.",
    )
  }
}

export function getExecutionPlanRegistry(plan: ExecutionPlan): SchemaRegistry {
  assertTrustedExecutionPlan(plan)
  return EXECUTION_PLAN_REGISTRIES.get(plan) as SchemaRegistry
}

function isPrimitivePlanValue(value: unknown): value is Exclude<PlanValue, PlanBlob> {
  return (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value))
  )
}

function normalizeColumnValue(column: ColumnMetadata, value: unknown): PlanValue {
  if (value === undefined) {
    throw new NozzleError("ConfigurationError", "Undefined cannot be bound to D1.", {
      details: { column: column.propertyName },
    })
  }
  let mapped: unknown
  try {
    mapped = column.column.mapToDriverValue(value)
  } catch {
    throw new NozzleError("ConfigurationError", "The column value is invalid.", {
      details: { column: column.propertyName },
    })
  }
  if (
    typeof mapped === "number" &&
    column.column.getSQLType().toLowerCase().includes("int") &&
    !Number.isSafeInteger(mapped)
  ) {
    throw new NozzleError(
      "ConfigurationError",
      "SQLite integer values must be JavaScript safe integers.",
      { details: { column: column.propertyName } },
    )
  }
  if (mapped instanceof Uint8Array) {
    if (mapped.byteLength > MAX_D1_VALUE_BYTES) {
      throw new NozzleError("CapacityGuardError", "A D1 BLOB value exceeds 2 MiB.")
    }
    return Object.freeze({ hex: bytesToHex(mapped), type: "blob" })
  }
  if (
    typeof mapped === "string" &&
    new TextEncoder().encode(mapped).byteLength > MAX_D1_VALUE_BYTES
  ) {
    throw new NozzleError("CapacityGuardError", "A D1 string value exceeds 2 MiB.")
  }
  if (!isPrimitivePlanValue(mapped)) {
    throw new NozzleError(
      "UnsafeQueryRequiredError",
      "The value requires unsupported SQL encoding.",
      {
        details: { column: column.propertyName },
      },
    )
  }
  return mapped
}

function valuesEqual(left: PlanValue, right: PlanValue): boolean {
  if (typeof left === "object" && left !== null) {
    return typeof right === "object" && right !== null && left.hex === right.hex
  }
  return Object.is(left, right)
}

function assertScoped(table: TableMetadata): asserts table is TableMetadata & {
  readonly partitionColumn: ColumnMetadata
} {
  if (table.classification !== "sharded" || !table.partitionColumn) {
    throw new NozzleError("TenantScopeRequiredError", "Scoped queries require a sharded table.")
  }
}

function basePlan(
  table: TableMetadata,
  route: ScopedRoute,
  schemaId: string,
): Omit<BasePlan, "operation"> {
  assertScoped(table)
  if (!Number.isSafeInteger(route.bucketId) || route.bucketId < 0 || route.bucketId > 0xffff_ffff) {
    throw new NozzleError("RouteVersionConflictError", "The route bucket is invalid.")
  }
  if (!Number.isSafeInteger(route.routeEpoch) || route.routeEpoch < 1) {
    throw new NozzleError("RouteVersionConflictError", "The route epoch is invalid.")
  }
  if (typeof route.shardId !== "string" || route.shardId.trim().length === 0) {
    throw new NozzleError("RouteVersionConflictError", "The route shard is invalid.")
  }
  if (typeof schemaId !== "string" || schemaId.trim().length === 0) {
    throw new NozzleError("ConfigurationError", "The schema identifier cannot be empty.")
  }
  if (
    typeof route.partitionDigestHex !== "string" ||
    !PARTITION_DIGEST_PATTERN.test(route.partitionDigestHex)
  ) {
    throw new NozzleError(
      "RouteVersionConflictError",
      "The route partition digest must be 64 lowercase hexadecimal characters.",
    )
  }
  return {
    bucketId: route.bucketId,
    partitionColumn: table.partitionColumn.dbName,
    partitionDigestHex: route.partitionDigestHex,
    partitionValue: normalizeColumnValue(table.partitionColumn, route.partitionValue),
    routeEpoch: route.routeEpoch,
    schemaId,
    shardId: route.shardId,
    table: table.tableName,
    version: 1,
  }
}

interface PredicateTranslationState {
  readonly active: WeakSet<object>
  boundValues: number
  nodes: number
}

function unsafePredicate(message: string): never {
  throw new NozzleError("UnsafeQueryRequiredError", message)
}

function assertDensePredicateArray(values: readonly unknown[], label: string): void {
  for (let index = 0; index < values.length; index += 1) {
    if (!Object.hasOwn(values, index)) {
      unsafePredicate(`${label} cannot contain sparse entries.`)
    }
  }
}

function translatePredicateNode(
  registry: SchemaRegistry,
  table: TableMetadata,
  predicate: PredicateInput,
  state: PredicateTranslationState,
  depth: number,
): PlanPredicate {
  if (typeof predicate !== "object" || predicate === null || Array.isArray(predicate)) {
    return unsafePredicate("Predicates must be structured Nozzle predicate objects.")
  }
  if (depth > MAX_PREDICATE_DEPTH) {
    throw new NozzleError("CapacityGuardError", "A predicate exceeds the maximum nesting depth.")
  }
  state.nodes += 1
  if (state.nodes > MAX_PREDICATE_NODES) {
    throw new NozzleError("CapacityGuardError", "A predicate contains too many nodes.")
  }
  if (state.active.has(predicate)) {
    return unsafePredicate("Cyclic predicates are not supported.")
  }
  state.active.add(predicate)
  try {
    if (predicate.kind === "logical") {
      if (predicate.operator !== "and" && predicate.operator !== "or") {
        return unsafePredicate("A logical predicate operator is invalid.")
      }
      if (!Array.isArray(predicate.terms) || predicate.terms.length === 0) {
        return unsafePredicate("Logical predicates cannot be empty.")
      }
      if (predicate.terms.length > MAX_PREDICATE_NODES) {
        throw new NozzleError("CapacityGuardError", "A predicate contains too many terms.")
      }
      assertDensePredicateArray(predicate.terms, "Logical predicate terms")
      return Object.freeze({
        kind: "logical",
        operator: predicate.operator,
        terms: Object.freeze(
          predicate.terms.map((term) =>
            translatePredicateNode(registry, table, term, state, depth + 1),
          ),
        ),
      })
    }
    if (
      predicate.kind !== "comparison" &&
      predicate.kind !== "in" &&
      predicate.kind !== "is-null"
    ) {
      return unsafePredicate("The predicate kind is unsupported.")
    }
    const column = registry.column(predicate.column)
    if (!table.columns.includes(column)) {
      return unsafePredicate("Predicates cannot cross tables.")
    }
    if (predicate.kind === "comparison") {
      if (!COMPARISON_OPERATORS.has(predicate.operator)) {
        return unsafePredicate("A comparison predicate operator is invalid.")
      }
      state.boundValues += 1
      if (state.boundValues > MAX_PREDICATE_BOUND_VALUES) {
        throw new NozzleError("CapacityGuardError", "A predicate contains too many bound values.")
      }
      return Object.freeze({
        column: column.dbName,
        kind: "comparison",
        operator: predicate.operator,
        value: normalizeColumnValue(column, predicate.value),
      })
    }
    if (predicate.kind === "in") {
      if (!Array.isArray(predicate.values) || predicate.values.length === 0) {
        return unsafePredicate("IN predicates cannot be empty.")
      }
      assertDensePredicateArray(predicate.values, "IN predicate values")
      state.boundValues += predicate.values.length
      if (state.boundValues > MAX_PREDICATE_BOUND_VALUES) {
        throw new NozzleError("CapacityGuardError", "A predicate contains too many bound values.")
      }
      return Object.freeze({
        column: column.dbName,
        kind: "in",
        values: Object.freeze(predicate.values.map((value) => normalizeColumnValue(column, value))),
      })
    }
    if (typeof predicate.negated !== "boolean") {
      return unsafePredicate("An IS NULL predicate negation flag is invalid.")
    }
    return Object.freeze({ column: column.dbName, kind: "is-null", negated: predicate.negated })
  } finally {
    state.active.delete(predicate)
  }
}

function translatePredicate(
  registry: SchemaRegistry,
  table: TableMetadata,
  predicate: PredicateInput | undefined,
): PlanPredicate | undefined {
  if (predicate === undefined) return undefined
  return translatePredicateNode(
    registry,
    table,
    predicate,
    { active: new WeakSet(), boundValues: 0, nodes: 0 },
    0,
  )
}

function normalizeWriteValues(
  table: TableMetadata,
  values: Readonly<Record<string, unknown>>,
): Record<string, PlanValue> {
  if (typeof values !== "object" || values === null || Array.isArray(values)) {
    throw new NozzleError("ConfigurationError", "Write values must be a structured object.")
  }
  const output: Record<string, PlanValue> = {}
  for (const [propertyName, value] of Object.entries(values)) {
    const column = table.columns.find((candidate) => candidate.propertyName === propertyName)
    if (!column) {
      throw new NozzleError("ConfigurationError", "A write contains an unknown column.", {
        details: { column: propertyName, table: table.tableName },
      })
    }
    Object.defineProperty(output, column.dbName, {
      configurable: true,
      enumerable: true,
      value: normalizeColumnValue(column, value),
      writable: true,
    })
  }
  return output
}

export function buildSelectPlan<TTable extends AnySQLiteTable>(
  registry: SchemaRegistry,
  input: {
    readonly limit?: number
    readonly predicate?: PredicateInput
    readonly route: ScopedRoute
    readonly schemaId: string
    readonly table: TTable
  },
): SelectPlan {
  const table = registry.table(input.table)
  assertScoped(table)
  if (input.limit !== undefined && (!Number.isSafeInteger(input.limit) || input.limit < 1)) {
    throw new NozzleError("ConfigurationError", "Select limits must be positive integers.")
  }
  const predicate = translatePredicate(registry, table, input.predicate)
  return trustExecutionPlan(
    Object.freeze({
      ...basePlan(table, input.route, input.schemaId),
      operation: "select",
      columns: Object.freeze(
        table.columns.map((column) =>
          Object.freeze({ column: column.dbName, resultKey: column.propertyName }),
        ),
      ),
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(predicate ? { predicate } : {}),
    }),
    registry,
  )
}

export function buildInsertPlan<TTable extends AnySQLiteTable>(
  registry: SchemaRegistry,
  input: {
    readonly route: ScopedRoute
    readonly schemaId: string
    readonly table: TTable
    readonly values: Readonly<Record<string, unknown>>
  },
): InsertPlan {
  const table = registry.table(input.table)
  assertScoped(table)
  const values = normalizeWriteValues(table, input.values)
  const scopedPartition = normalizeColumnValue(table.partitionColumn, input.route.partitionValue)
  const suppliedPartition = values[table.partitionColumn.dbName]
  if (suppliedPartition !== undefined && !valuesEqual(suppliedPartition, scopedPartition)) {
    throw new NozzleError("PartitionKeyMismatchError", "Insert partition key does not match scope.")
  }
  values[table.partitionColumn.dbName] = scopedPartition
  values.__nozzle_bucket = input.route.bucketId
  return trustExecutionPlan(
    Object.freeze({
      ...basePlan(table, input.route, input.schemaId),
      operation: "insert",
      values: Object.freeze(values),
    }),
    registry,
  )
}

export function buildUpdatePlan<TTable extends AnySQLiteTable>(
  registry: SchemaRegistry,
  input: {
    readonly predicate?: PredicateInput
    readonly route: ScopedRoute
    readonly schemaId: string
    readonly table: TTable
    readonly values: Readonly<Record<string, unknown>>
  },
): UpdatePlan {
  const table = registry.table(input.table)
  assertScoped(table)
  if (typeof input.values !== "object" || input.values === null || Array.isArray(input.values)) {
    throw new NozzleError("ConfigurationError", "Write values must be a structured object.")
  }
  const forbidden = new Set([
    table.partitionColumn.propertyName,
    ...table.primaryColumns.map((column) => column.propertyName),
  ])
  const attempted = Object.keys(input.values).find((propertyName) => forbidden.has(propertyName))
  if (attempted) {
    throw new NozzleError(
      "UnsafeQueryRequiredError",
      "Scoped updates cannot mutate identity columns.",
      {
        details: { column: attempted },
      },
    )
  }
  const values = normalizeWriteValues(table, input.values)
  if (Object.keys(values).length === 0) {
    throw new NozzleError("ConfigurationError", "Updates require at least one value.")
  }
  const predicate = translatePredicate(registry, table, input.predicate)
  return trustExecutionPlan(
    Object.freeze({
      ...basePlan(table, input.route, input.schemaId),
      operation: "update",
      ...(predicate ? { predicate } : {}),
      values: Object.freeze(values),
    }),
    registry,
  )
}

export function buildDeletePlan<TTable extends AnySQLiteTable>(
  registry: SchemaRegistry,
  input: {
    readonly predicate?: PredicateInput
    readonly route: ScopedRoute
    readonly schemaId: string
    readonly table: TTable
  },
): DeletePlan {
  const table = registry.table(input.table)
  assertScoped(table)
  const predicate = translatePredicate(registry, table, input.predicate)
  return trustExecutionPlan(
    Object.freeze({
      ...basePlan(table, input.route, input.schemaId),
      operation: "delete",
      ...(predicate ? { predicate } : {}),
    }),
    registry,
  )
}

export interface WireExecutionPlanExpectation {
  readonly route: ScopedRoute
  readonly schemaId: string
}

const BASE_WIRE_KEYS = [
  "bucketId",
  "operation",
  "partitionColumn",
  "partitionDigestHex",
  "partitionValue",
  "routeEpoch",
  "schemaId",
  "shardId",
  "table",
  "version",
] as const

function wireUnsafe(message: string): never {
  throw new NozzleError("UnsafeQueryRequiredError", message)
}

function wireRecord(value: unknown, label: string): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return wireUnsafe(`${label} must be a plain object.`)
  }
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    return wireUnsafe(`${label} must be a plain object.`)
  }
  if (Object.getOwnPropertySymbols(value).length !== 0) {
    return wireUnsafe(`${label} cannot contain symbol properties.`)
  }
  for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
    if (!descriptor.enumerable || !("value" in descriptor)) {
      return wireUnsafe(`${label} must contain only enumerable data properties.`)
    }
  }
  return value as Readonly<Record<string, unknown>>
}

function assertWireKeys(
  record: Readonly<Record<string, unknown>>,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set([...required, ...optional])
  const keys = Object.keys(record)
  if (
    keys.some((key) => !allowed.has(key)) ||
    required.some((key) => !Object.hasOwn(record, key))
  ) {
    wireUnsafe("The wire execution plan contains missing or unsupported fields.")
  }
}

function cloneWireValue(value: unknown): PlanValue {
  if (typeof value === "string") {
    if (new TextEncoder().encode(value).byteLength > MAX_D1_VALUE_BYTES) {
      throw new NozzleError("CapacityGuardError", "A wire string value exceeds 2 MiB.")
    }
    return value
  }
  if (
    value === null ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value
  }
  const record = wireRecord(value, "A wire bound value")
  assertWireKeys(record, ["hex", "type"])
  if (
    record.type !== "blob" ||
    typeof record.hex !== "string" ||
    record.hex.length > MAX_D1_VALUE_BYTES * 2 ||
    !/^(?:[0-9a-f]{2})*$/u.test(record.hex)
  ) {
    return wireUnsafe("A wire BLOB value is malformed or too large.")
  }
  return Object.freeze({ hex: record.hex, type: "blob" })
}

function assertCanonicalColumnValue(column: ColumnMetadata, value: PlanValue): void {
  if (value !== null) {
    const sqlType = column.column.getSQLType().toLowerCase()
    const compatible = sqlType.includes("blob")
      ? typeof value === "object"
      : sqlType.includes("int")
        ? typeof value === "number" && Number.isSafeInteger(value)
        : sqlType.includes("real") || sqlType.includes("numeric")
          ? typeof value === "number" && Number.isFinite(value)
          : typeof value === "string"
    if (!compatible) wireUnsafe("A wire bound value has the wrong SQLite storage type.")
  }
  let driverValue: PlanValue | Uint8Array = value
  if (typeof value === "object" && value !== null) {
    const bytes = new Uint8Array(value.hex.length / 2)
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Number.parseInt(value.hex.slice(index * 2, index * 2 + 2), 16)
    }
    driverValue = bytes
  }
  let applicationValue: unknown
  try {
    applicationValue = driverValue === null ? null : column.column.mapFromDriverValue(driverValue)
  } catch {
    wireUnsafe("A wire bound value is incompatible with its schema column.")
  }
  const canonical = normalizeColumnValue(column, applicationValue)
  if (!valuesEqual(value, canonical)) {
    wireUnsafe("A wire bound value is not in canonical driver form.")
  }
}

export interface WireExecutionPlanRouteHint {
  readonly partitionValue: PlanValue
  readonly table: string
}

export function readWireExecutionPlanRouteHint(
  registry: SchemaRegistry,
  input: unknown,
): WireExecutionPlanRouteHint {
  const record = wireRecord(input, "The wire execution plan")
  if (typeof record.table !== "string") return wireUnsafe("The wire table name is invalid.")
  const table = registry.tableByName(record.table)
  if (table.classification !== "sharded" || !table.partitionColumn) {
    return wireUnsafe("Wire execution plans require a registered sharded table.")
  }
  if (!Object.hasOwn(record, "partitionValue")) {
    return wireUnsafe("The wire execution plan is missing its partition value.")
  }
  const partitionValue = cloneWireValue(record.partitionValue)
  assertCanonicalColumnValue(table.partitionColumn, partitionValue)
  return Object.freeze({ partitionValue, table: table.tableName })
}

interface WirePredicateState {
  boundValues: number
  nodes: number
  readonly active: WeakSet<object>
}

function decodeWirePredicate(
  value: unknown,
  table: TableMetadata,
  state: WirePredicateState,
  depth: number,
): PlanPredicate {
  const record = wireRecord(value, "A wire predicate")
  if (depth > MAX_PREDICATE_DEPTH) {
    throw new NozzleError("CapacityGuardError", "A wire predicate exceeds the maximum depth.")
  }
  state.nodes += 1
  if (state.nodes > MAX_PREDICATE_NODES) {
    throw new NozzleError("CapacityGuardError", "A wire predicate contains too many nodes.")
  }
  if (state.active.has(record)) return wireUnsafe("Cyclic wire predicates are not supported.")
  state.active.add(record)
  try {
    if (record.kind === "logical") {
      assertWireKeys(record, ["kind", "operator", "terms"])
      if (record.operator !== "and" && record.operator !== "or") {
        return wireUnsafe("A wire logical operator is invalid.")
      }
      if (!Array.isArray(record.terms) || record.terms.length === 0) {
        return wireUnsafe("Wire logical predicates cannot be empty.")
      }
      if (record.terms.length > MAX_PREDICATE_NODES) {
        throw new NozzleError("CapacityGuardError", "A wire predicate contains too many terms.")
      }
      assertDensePredicateArray(record.terms, "Wire logical predicate terms")
      return Object.freeze({
        kind: "logical",
        operator: record.operator,
        terms: Object.freeze(
          record.terms.map((term) => decodeWirePredicate(term, table, state, depth + 1)),
        ),
      })
    }
    if (record.kind !== "comparison" && record.kind !== "in" && record.kind !== "is-null") {
      return wireUnsafe("A wire predicate kind is unsupported.")
    }
    if (typeof record.column !== "string") {
      return wireUnsafe("A wire predicate column is invalid.")
    }
    const column = table.columns.find((candidate) => candidate.dbName === record.column)
    if (!column) return wireUnsafe("A wire predicate uses an unregistered column.")
    if (record.kind === "comparison") {
      assertWireKeys(record, ["column", "kind", "operator", "value"])
      if (typeof record.operator !== "string" || !COMPARISON_OPERATORS.has(record.operator)) {
        return wireUnsafe("A wire comparison operator is invalid.")
      }
      state.boundValues += 1
      if (state.boundValues > MAX_PREDICATE_BOUND_VALUES) {
        throw new NozzleError("CapacityGuardError", "A wire predicate has too many bound values.")
      }
      const bound = cloneWireValue(record.value)
      assertCanonicalColumnValue(column, bound)
      return Object.freeze({
        column: record.column,
        kind: "comparison",
        operator: record.operator as "eq" | "gt" | "gte" | "lt" | "lte" | "ne",
        value: bound,
      })
    }
    if (record.kind === "in") {
      assertWireKeys(record, ["column", "kind", "values"])
      if (!Array.isArray(record.values) || record.values.length === 0) {
        return wireUnsafe("A wire IN predicate cannot be empty.")
      }
      assertDensePredicateArray(record.values, "Wire IN predicate values")
      state.boundValues += record.values.length
      if (state.boundValues > MAX_PREDICATE_BOUND_VALUES) {
        throw new NozzleError("CapacityGuardError", "A wire predicate has too many bound values.")
      }
      const values = record.values.map((entry) => {
        const bound = cloneWireValue(entry)
        assertCanonicalColumnValue(column, bound)
        return bound
      })
      return Object.freeze({
        column: record.column,
        kind: "in",
        values: Object.freeze(values),
      })
    }
    assertWireKeys(record, ["column", "kind", "negated"])
    if (typeof record.negated !== "boolean") {
      return wireUnsafe("A wire IS NULL predicate flag is invalid.")
    }
    return Object.freeze({ column: record.column, kind: "is-null", negated: record.negated })
  } finally {
    state.active.delete(record)
  }
}

function decodeWireValues(
  value: unknown,
  table: TableMetadata,
  operation: "insert" | "update",
  expectation: WireExecutionPlanExpectation,
): Readonly<Record<string, PlanValue>> {
  const record = wireRecord(value, "Wire write values")
  const entries = Object.entries(record)
  if (entries.length === 0 || entries.length > table.columns.length + 1) {
    return wireUnsafe("Wire write values are empty or exceed the schema column limit.")
  }
  const output: Record<string, PlanValue> = {}
  for (const [columnName, rawValue] of entries) {
    if (columnName === "__nozzle_bucket") {
      if (operation !== "insert" || rawValue !== expectation.route.bucketId) {
        return wireUnsafe("The wire internal bucket value is invalid.")
      }
      output[columnName] = expectation.route.bucketId
      continue
    }
    const column = table.columns.find((candidate) => candidate.dbName === columnName)
    if (!column) return wireUnsafe("Wire write values contain an unregistered column.")
    if (
      operation === "update" &&
      (column === table.partitionColumn || table.primaryColumns.includes(column))
    ) {
      return wireUnsafe("Wire updates cannot mutate identity columns.")
    }
    const bound = cloneWireValue(rawValue)
    assertCanonicalColumnValue(column, bound)
    Object.defineProperty(output, columnName, {
      configurable: true,
      enumerable: true,
      value: bound,
      writable: true,
    })
  }
  if (operation === "insert") {
    const partitionColumn = table.partitionColumn as ColumnMetadata
    const expectedPartition = normalizeColumnValue(
      partitionColumn,
      expectation.route.partitionValue,
    )
    const actualPartition = output[partitionColumn.dbName]
    if (
      actualPartition === undefined ||
      !valuesEqual(actualPartition, expectedPartition) ||
      output.__nozzle_bucket !== expectation.route.bucketId
    ) {
      return wireUnsafe("A wire insert does not contain the expected partition scope.")
    }
  }
  return Object.freeze(output)
}

function assertWireBase(
  record: Readonly<Record<string, unknown>>,
  table: TableMetadata,
  expectation: WireExecutionPlanExpectation,
): PlanValue {
  if (
    record.version !== 1 ||
    record.table !== table.tableName ||
    record.partitionColumn !== table.partitionColumn?.dbName ||
    record.schemaId !== expectation.schemaId ||
    record.bucketId !== expectation.route.bucketId ||
    record.routeEpoch !== expectation.route.routeEpoch ||
    record.shardId !== expectation.route.shardId ||
    record.partitionDigestHex !== expectation.route.partitionDigestHex
  ) {
    wireUnsafe("The wire execution plan does not match the leaf's validated route and schema.")
  }
  const expectedPartition = normalizeColumnValue(
    table.partitionColumn as ColumnMetadata,
    expectation.route.partitionValue,
  )
  const partitionValue = cloneWireValue(record.partitionValue)
  if (!valuesEqual(partitionValue, expectedPartition)) {
    wireUnsafe("The wire execution plan partition value is not canonical for the route.")
  }
  return expectedPartition
}

export function decodeWireExecutionPlan(
  registry: SchemaRegistry,
  input: unknown,
  expectation: WireExecutionPlanExpectation,
): ExecutionPlan {
  const record = wireRecord(input, "The wire execution plan")
  if (typeof record.table !== "string") return wireUnsafe("The wire table name is invalid.")
  const table = registry.tableByName(record.table)
  if (table.classification !== "sharded" || !table.partitionColumn) {
    return wireUnsafe("Wire execution plans require a registered sharded table.")
  }
  const partitionValue = assertWireBase(record, table, expectation)
  const predicate = Object.hasOwn(record, "predicate")
    ? decodeWirePredicate(
        record.predicate,
        table,
        { active: new WeakSet(), boundValues: 0, nodes: 0 },
        0,
      )
    : undefined
  let plan: ExecutionPlan
  switch (record.operation) {
    case "select": {
      assertWireKeys(record, [...BASE_WIRE_KEYS, "columns"], ["limit", "predicate"])
      if (!Array.isArray(record.columns) || record.columns.length !== table.columns.length) {
        return wireUnsafe("Wire select columns do not match the registered schema.")
      }
      const columns = record.columns.map((entry, index) => {
        const selection = wireRecord(entry, "A wire selected column")
        assertWireKeys(selection, ["column", "resultKey"])
        const expected = table.columns[index]
        if (
          !expected ||
          selection.column !== expected.dbName ||
          selection.resultKey !== expected.propertyName
        ) {
          return wireUnsafe("Wire select columns do not match the registered schema.")
        }
        return Object.freeze({ column: expected.dbName, resultKey: expected.propertyName })
      })
      if (
        record.limit !== undefined &&
        (!Number.isSafeInteger(record.limit) || (record.limit as number) < 1)
      ) {
        return wireUnsafe("A wire select limit must be a positive safe integer.")
      }
      plan = Object.freeze({
        ...(record as unknown as BasePlan),
        operation: "select",
        partitionValue,
        columns: Object.freeze(columns),
        ...(record.limit === undefined ? {} : { limit: record.limit as number }),
        ...(predicate ? { predicate } : {}),
      })
      break
    }
    case "insert":
      assertWireKeys(record, [...BASE_WIRE_KEYS, "values"])
      plan = Object.freeze({
        ...(record as unknown as BasePlan),
        operation: "insert",
        partitionValue,
        values: decodeWireValues(record.values, table, "insert", expectation),
      })
      break
    case "update":
      assertWireKeys(record, [...BASE_WIRE_KEYS, "values"], ["predicate"])
      plan = Object.freeze({
        ...(record as unknown as BasePlan),
        operation: "update",
        partitionValue,
        ...(predicate ? { predicate } : {}),
        values: decodeWireValues(record.values, table, "update", expectation),
      })
      break
    case "delete":
      assertWireKeys(record, BASE_WIRE_KEYS, ["predicate"])
      plan = Object.freeze({
        ...(record as unknown as BasePlan),
        operation: "delete",
        partitionValue,
        ...(predicate ? { predicate } : {}),
      })
      break
    default:
      return wireUnsafe("The wire execution operation is unsupported.")
  }
  return trustExecutionPlan(plan, registry)
}
