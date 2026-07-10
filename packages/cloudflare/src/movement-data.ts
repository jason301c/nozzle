import { NozzleError } from "@nozzle/core"
import type { CompiledStatement, D1BindingValue, TableMetadata } from "@nozzle/drizzle"
import { movementTransferViewName } from "./movement-transfer.js"

export type MovementKeyType = "blob" | "integer" | "real" | "text"

export interface MovementKeyComponent {
  readonly column: string
  readonly type: MovementKeyType
  readonly value: string
}

export interface MovementPageScope {
  readonly bucketId: number
  readonly partitionValue?: D1BindingValue
}

const MAX_PAGE_ROWS = 1_000
const MAX_PAGE_BYTES = 8 * 1024 * 1024
const MAX_MOVEMENT_COLUMNS = 96
const INTEGER_MIN = -9_223_372_036_854_775_808n
const INTEGER_MAX = 9_223_372_036_854_775_807n

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function verification(message: string): never {
  throw new NozzleError("MovementVerificationError", message)
}

function quote(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function nonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.length === 0) configuration(`${label} must be non-empty.`)
}

function nonNegativeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    configuration(`${label} must be a non-negative safe integer.`)
  }
}

function tableShape(table: TableMetadata): void {
  if (table.classification !== "sharded") configuration("Movement requires a sharded table.")
  if (table.primaryColumns.length === 0) configuration("Movement requires a stable primary key.")
  if (table.columns.length > MAX_MOVEMENT_COLUMNS) {
    throw new NozzleError("CapacityGuardError", "Movement table exceeds the D1 binding budget.")
  }
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function decodeHex(value: string): Uint8Array {
  if (!/^(?:[0-9a-f]{2})+$/u.test(value)) verification("Movement BLOB key is malformed.")
  const bytes = new Uint8Array(value.length / 2)
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes
}

function validateInteger(value: string): void {
  if (!/^-?(?:0|[1-9][0-9]*)$/u.test(value) || value === "-0") {
    verification("Movement integer key is not canonical.")
  }
  const integer = BigInt(value)
  if (integer < INTEGER_MIN || integer > INTEGER_MAX) {
    verification("Movement integer key exceeds SQLite's signed range.")
  }
}

function validateReal(value: string): void {
  if (value.trim() !== value || value.length === 0 || !Number.isFinite(Number(value))) {
    verification("Movement real key is malformed.")
  }
}

export function decodeMovementKey(
  table: TableMetadata,
  keyJson: string,
): readonly MovementKeyComponent[] {
  tableShape(table)
  nonEmpty(keyJson, "Movement key JSON")
  let decoded: unknown
  try {
    decoded = JSON.parse(keyJson)
  } catch {
    return verification("Movement key JSON is malformed.")
  }
  if (!Array.isArray(decoded) || decoded.length !== table.primaryColumns.length) {
    return verification("Movement key shape does not match the table primary key.")
  }
  const components: MovementKeyComponent[] = []
  for (let index = 0; index < decoded.length; index += 1) {
    const candidate = decoded[index]
    const expected = table.primaryColumns[index] as (typeof table.primaryColumns)[number]
    if (!plainRecord(candidate)) {
      return verification("Movement key component is malformed.")
    }
    const keys = Object.keys(candidate).sort()
    if (keys.join(",") !== "column,type,value") {
      return verification("Movement key component fields are invalid.")
    }
    if (
      candidate.column !== expected.dbName ||
      typeof candidate.type !== "string" ||
      !(["blob", "integer", "real", "text"] as const).includes(candidate.type as MovementKeyType) ||
      typeof candidate.value !== "string"
    ) {
      return verification("Movement key component identity or type is invalid.")
    }
    const type = candidate.type as MovementKeyType
    if (type !== expected.storageType) {
      return verification("Movement key type does not match the registered SQLite affinity.")
    }
    if (type === "blob") decodeHex(candidate.value)
    else if (type === "integer") validateInteger(candidate.value)
    else if (type === "real") validateReal(candidate.value)
    components.push(Object.freeze({ column: expected.dbName, type, value: candidate.value }))
  }
  return Object.freeze(components)
}

function keyBinding(component: MovementKeyComponent): D1BindingValue {
  return component.type === "blob" ? decodeHex(component.value) : component.value
}

function parameterExpression(component: MovementKeyComponent, parameter: string): string {
  if (component.type === "integer") return `CAST(${parameter} AS INTEGER)`
  if (component.type === "real") return `CAST(${parameter} AS REAL)`
  return parameter
}

function exactKeyTerm(component: MovementKeyComponent, parameter: string): string {
  const column = quote(component.column)
  const expression = parameterExpression(component, parameter)
  const collate = component.type === "text" ? " COLLATE BINARY" : ""
  return `typeof(${column}) = '${component.type}' AND ${column}${collate} IS ${expression}${collate}`
}

function greaterKeyTerm(component: MovementKeyComponent, parameter: string): string {
  const column = quote(component.column)
  const expression = parameterExpression(component, parameter)
  const collate = component.type === "text" ? " COLLATE BINARY" : ""
  return `${column}${collate} > ${expression}${collate}`
}

function appendKeyBindings(
  params: D1BindingValue[],
  components: readonly MovementKeyComponent[],
): readonly string[] {
  return components.map((component) => {
    params.push(keyBinding(component))
    return `?${params.length}`
  })
}

function exactKeyPredicate(
  components: readonly MovementKeyComponent[],
  parameters: readonly string[],
): string {
  return components
    .map((component, index) => exactKeyTerm(component, parameters[index] as string))
    .join(" AND ")
}

function keysetPredicate(
  components: readonly MovementKeyComponent[],
  parameters: readonly string[],
): string {
  return components
    .map((component, index) => {
      const equalPrefix = components
        .slice(0, index)
        .map((prefix, prefixIndex) => exactKeyTerm(prefix, parameters[prefixIndex] as string))
      return `(${[...equalPrefix, greaterKeyTerm(component, parameters[index] as string)].join(" AND ")})`
    })
    .join(" OR ")
}

function encodedValue(column: string): string {
  const value = quote(column)
  return `CASE typeof(${value}) WHEN 'blob' THEN lower(hex(${value})) WHEN 'integer' THEN printf('%lld', ${value}) WHEN 'real' THEN printf('%.17g', ${value}) WHEN 'text' THEN ${value} ELSE NULL END`
}

function cursorExpression(table: TableMetadata): string {
  return `json_array(${table.primaryColumns
    .map(
      (column) =>
        `json_object('column', '${column.dbName.replaceAll("'", "''")}', 'type', typeof(${quote(column.dbName)}), 'value', ${encodedValue(column.dbName)})`,
    )
    .join(", ")})`
}

function selectedColumns(table: TableMetadata): string {
  return [...table.columns.map((column) => quote(column.dbName)), '"__nozzle_bucket"'].join(", ")
}

export function compileMovementPage(input: {
  readonly afterKeyJson?: string
  readonly limit: number
  readonly maxBytes: number
  readonly scope: MovementPageScope
  readonly table: TableMetadata
}): CompiledStatement {
  tableShape(input.table)
  if (!Number.isSafeInteger(input.limit) || input.limit < 1 || input.limit > MAX_PAGE_ROWS) {
    configuration(`Movement page limit must be between 1 and ${MAX_PAGE_ROWS}.`)
  }
  if (
    !Number.isSafeInteger(input.maxBytes) ||
    input.maxBytes < 1 ||
    input.maxBytes > MAX_PAGE_BYTES
  ) {
    configuration(`Movement page byte budget must be between 1 and ${MAX_PAGE_BYTES}.`)
  }
  nonNegativeInteger(input.scope.bucketId, "Movement bucket ID")
  const params: D1BindingValue[] = [input.scope.bucketId]
  const predicates = [`"__nozzle_bucket" = ?1`]
  if (input.scope.partitionValue !== undefined) {
    const partition = input.table.partitionColumn
    if (!partition) configuration("Partition movement requires partition metadata.")
    params.push(input.scope.partitionValue)
    predicates.push(`${quote(partition.dbName)} IS ?${params.length}`)
  }
  if (input.afterKeyJson !== undefined) {
    const components = decodeMovementKey(input.table, input.afterKeyJson)
    const parameters = appendKeyBindings(params, components)
    predicates.push(`(${keysetPredicate(components, parameters)})`)
  }
  params.push(input.limit)
  const limitParameter = `?${params.length}`
  params.push(input.maxBytes)
  const byteParameter = `?${params.length}`
  const order = input.table.primaryColumns
    .map(
      (column) => `${quote(column.dbName)}${column.dataType === "string" ? " COLLATE BINARY" : ""}`,
    )
    .join(", ")
  const selected = selectedColumns(input.table)
  const cursor = cursorExpression(input.table)
  const rowBytes = `${64 * (input.table.columns.length + 2)} + ${[
    ...input.table.columns.map((column) => quote(column.dbName)),
    '"__nozzle_bucket"',
  ]
    .map((column) => `COALESCE(length(CAST(${column} AS BLOB)), 0)`)
    .join(" + ")}`
  return Object.freeze({
    params: Object.freeze(params),
    sql: `WITH "nozzle_candidates" AS (SELECT ${selected}, ${cursor} AS "__nozzle_cursor_json", ${rowBytes} AS "__nozzle_row_bytes" FROM ${quote(input.table.tableName)} WHERE ${predicates.join(" AND ")} ORDER BY ${order} LIMIT ${limitParameter}), "nozzle_bounded" AS (SELECT *, sum("__nozzle_row_bytes") OVER (ORDER BY ${order} ROWS UNBOUNDED PRECEDING) AS "__nozzle_cumulative_bytes", row_number() OVER (ORDER BY ${order}) AS "__nozzle_row_number" FROM "nozzle_candidates") SELECT ${selected}, "__nozzle_cursor_json", "__nozzle_row_bytes" FROM "nozzle_bounded" WHERE "__nozzle_cumulative_bytes" <= ${byteParameter} OR "__nozzle_row_number" = 1 ORDER BY ${order}`,
  })
}

export interface DecodedMovementPage<T extends Readonly<Record<string, unknown>>> {
  readonly nextCursor?: string
  readonly rows: readonly T[]
  readonly transferredBytes: number
}

export function decodeMovementPage<T extends Readonly<Record<string, unknown>>>(
  rows: readonly unknown[],
  maxBytes: number,
): DecodedMovementPage<T> {
  if (!Array.isArray(rows)) verification("Movement page result is malformed.")
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_PAGE_BYTES) {
    configuration(`Movement page byte budget must be between 1 and ${MAX_PAGE_BYTES}.`)
  }
  let transferredBytes = 0
  let nextCursor: string | undefined
  const decoded: T[] = []
  for (const candidate of rows) {
    if (!plainRecord(candidate)) verification("Movement page row is malformed.")
    const cursor = candidate.__nozzle_cursor_json
    const rowBytes = candidate.__nozzle_row_bytes
    if (
      typeof cursor !== "string" ||
      cursor.length === 0 ||
      !Number.isSafeInteger(rowBytes) ||
      (rowBytes as number) < 0
    ) {
      verification("Movement page metadata is malformed.")
    }
    if ((rowBytes as number) > maxBytes - transferredBytes) {
      throw new NozzleError("CapacityGuardError", "Movement page exceeded its byte budget.")
    }
    transferredBytes += rowBytes as number
    const { __nozzle_cursor_json: _cursor, __nozzle_row_bytes: _bytes, ...row } = candidate
    decoded.push(Object.freeze(row) as T)
    nextCursor = cursor
  }
  return Object.freeze({
    ...(nextCursor === undefined ? {} : { nextCursor }),
    rows: Object.freeze(decoded),
    transferredBytes,
  })
}

export function compileMovementReplayRead(input: {
  readonly keyJson: string
  readonly sourceBucketId: number
  readonly table: TableMetadata
}): CompiledStatement {
  tableShape(input.table)
  nonNegativeInteger(input.sourceBucketId, "Source bucket ID")
  const components = decodeMovementKey(input.table, input.keyJson)
  const params: D1BindingValue[] = [input.sourceBucketId]
  const parameters = appendKeyBindings(params, components)
  return Object.freeze({
    params: Object.freeze(params),
    sql: `SELECT ${selectedColumns(input.table)} FROM ${quote(input.table.tableName)} WHERE "__nozzle_bucket" = ?1 AND ${exactKeyPredicate(components, parameters)} LIMIT 2`,
  })
}

function normalizeRowValue(value: unknown): D1BindingValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    value instanceof Uint8Array
  ) {
    if (typeof value === "number" && !Number.isFinite(value)) {
      return verification("Movement row contains a non-finite number.")
    }
    return value
  }
  if (value instanceof ArrayBuffer) return new Uint8Array(value.slice(0))
  return verification("Movement row contains an unsupported D1 value.")
}

function commandSql(viewName: string, columns: readonly string[], parameterCount: number): string {
  return `INSERT INTO ${quote(viewName)} (${columns.map(quote).join(", ")}) VALUES (${Array.from({ length: parameterCount }, (_, index) => `?${index + 1}`).join(", ")})`
}

export function compileMovementUpsert(input: {
  readonly capabilityToken: string
  readonly destinationBucketId: number
  readonly row: Readonly<Record<string, unknown>>
  readonly table: TableMetadata
}): CompiledStatement {
  tableShape(input.table)
  nonEmpty(input.capabilityToken, "Movement capability token")
  nonNegativeInteger(input.destinationBucketId, "Destination bucket ID")
  if (!plainRecord(input.row)) verification("Movement source row is malformed.")
  const expected = new Set([
    ...input.table.columns.map((column) => column.dbName),
    "__nozzle_bucket",
  ])
  if (
    Object.keys(input.row).length !== expected.size ||
    [...expected].some((name) => !Object.hasOwn(input.row, name))
  ) {
    verification("Movement source row does not exactly match the registered table.")
  }
  const columns = [
    ...input.table.columns.map((column) => column.dbName),
    "__nozzle_bucket",
    "__nozzle_capability_token",
    "__nozzle_mutation_hint",
  ]
  const params: D1BindingValue[] = input.table.columns.map((column) =>
    normalizeRowValue(input.row[column.dbName]),
  )
  params.push(input.destinationBucketId, input.capabilityToken, "upsert")
  return Object.freeze({
    params: Object.freeze(params),
    sql: commandSql(movementTransferViewName(input.table.tableName), columns, params.length),
  })
}

export function compileMovementDelete(input: {
  readonly capabilityToken: string
  readonly destinationBucketId: number
  readonly keyJson: string
  readonly table: TableMetadata
}): CompiledStatement {
  tableShape(input.table)
  nonEmpty(input.capabilityToken, "Movement capability token")
  nonNegativeInteger(input.destinationBucketId, "Destination bucket ID")
  const components = decodeMovementKey(input.table, input.keyJson)
  const columns = [
    ...components.map((component) => component.column),
    "__nozzle_bucket",
    "__nozzle_capability_token",
    "__nozzle_mutation_hint",
  ]
  const params = components.map(keyBinding)
  params.push(input.destinationBucketId, input.capabilityToken, "delete")
  const values = [
    ...components.map((component, index) => parameterExpression(component, `?${index + 1}`)),
    ...params.slice(components.length).map((_, index) => `?${components.length + index + 1}`),
  ]
  return Object.freeze({
    params: Object.freeze(params),
    sql: `INSERT INTO ${quote(movementTransferViewName(input.table.tableName))} (${columns.map(quote).join(", ")}) VALUES (${values.join(", ")})`,
  })
}

export function compileMovementReplayReceipt(input: {
  readonly appliedAtMs: number
  readonly keyJson: string
  readonly mutationHint: "delete" | "upsert"
  readonly operationId: string
  readonly resultChecksum: string
  readonly sourceSequence: number
  readonly tableId: string
}): CompiledStatement {
  nonEmpty(input.operationId, "Movement operation ID")
  nonEmpty(input.tableId, "Movement table ID")
  nonEmpty(input.keyJson, "Movement key JSON")
  nonEmpty(input.resultChecksum, "Movement result checksum")
  nonNegativeInteger(input.appliedAtMs, "Movement receipt time")
  if (!Number.isSafeInteger(input.sourceSequence) || input.sourceSequence < 1) {
    configuration("Movement source sequence must be a positive safe integer.")
  }
  return Object.freeze({
    params: Object.freeze([
      input.operationId,
      input.sourceSequence,
      input.tableId,
      input.keyJson,
      input.mutationHint,
      input.resultChecksum,
      input.appliedAtMs,
    ]),
    sql: `INSERT INTO "nozzle_movement_replay_receipts" ("operation_id", "source_sequence", "table_id", "key_json", "mutation_hint", "result_checksum", "applied_at_ms") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT ("operation_id", "source_sequence") DO NOTHING`,
  })
}
