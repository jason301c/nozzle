import { NozzleError } from "@nozzle/core"

export const MOVEMENT_TRANSFER_SCHEMA_VERSION = 1 as const

export interface MovementTransferTableSpec {
  readonly columns: readonly string[]
  readonly primaryColumns: readonly string[]
  readonly tableName: string
}

export interface GeneratedMovementTransferSql {
  readonly schemaVersion: typeof MOVEMENT_TRANSFER_SCHEMA_VERSION
  readonly sql: string
  readonly statements: readonly string[]
  readonly tables: readonly MovementTransferTableSpec[]
}

const MAX_IDENTIFIER_BYTES = 255
const MAX_TABLES = 1_000

function fail(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function fold(value: string): string {
  return value.replace(/[A-Z]/gu, (character) => character.toLowerCase())
}

function identifier(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    fail(`${label} must be a non-empty string.`)
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) fail(`${label} is not well-formed UTF-16.`)
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      fail(`${label} is not well-formed UTF-16.`)
    } else if (code <= 0x1f || code === 0x7f) {
      fail(`${label} cannot contain ASCII control characters.`)
    }
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTIFIER_BYTES) {
    fail(`${label} is too long.`)
  }
  if (fold(value).startsWith("sqlite_") || fold(value).startsWith("nozzle_")) {
    fail(`${label} uses a reserved identifier.`)
  }
  return value
}

function quote(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function encodedName(tableName: string): string {
  let output = ""
  for (const byte of new TextEncoder().encode(tableName)) {
    output += byte.toString(16).padStart(2, "0")
  }
  return output
}

export function movementTransferViewName(tableName: string): string {
  return `nozzle_operation_${encodedName(identifier(tableName, "tableName"))}`
}

function validateNameList(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value) || value.length === 0) fail(`${label} must not be empty.`)
  const names = new Set<string>()
  const output = value.map((candidate) => {
    const name = identifier(candidate, label)
    const key = fold(name)
    if (names.has(key)) fail(`${label} must be unique.`)
    names.add(key)
    return name
  })
  return Object.freeze(output)
}

function validateTables(value: unknown): readonly MovementTransferTableSpec[] {
  if (!Array.isArray(value) || value.length === 0) fail("tables must not be empty.")
  if (value.length > MAX_TABLES) fail("tables exceeds the supported limit.")
  const tableNames = new Set<string>()
  const output: MovementTransferTableSpec[] = []
  for (const candidate of value) {
    if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
      fail("Every movement transfer table must be an object.")
    }
    const record = candidate as Partial<MovementTransferTableSpec>
    const tableName = identifier(record.tableName, "tableName")
    const columns = validateNameList(record.columns, "columns")
    const primaryColumns = validateNameList(record.primaryColumns, "primaryColumns")
    const columnNames = new Set(columns.map(fold))
    if (primaryColumns.some((name) => !columnNames.has(fold(name)))) {
      fail("Every primary column must be present in columns.")
    }
    const key = fold(tableName)
    if (tableNames.has(key)) fail("Movement transfer table names must be unique.")
    tableNames.add(key)
    output.push(Object.freeze({ columns, primaryColumns, tableName }))
  }
  output.sort((left, right) => {
    const leftName = fold(left.tableName)
    const rightName = fold(right.tableName)
    return leftName < rightName ? -1 : 1
  })
  return Object.freeze(output)
}

function viewStatement(spec: MovementTransferTableSpec): string {
  const projected = spec.columns.map(quote).join(", ")
  return `CREATE VIEW ${quote(movementTransferViewName(spec.tableName))} AS
SELECT ${projected}, "__nozzle_bucket",
  CAST(NULL AS TEXT) AS "__nozzle_capability_token",
  CAST(NULL AS TEXT) AS "__nozzle_mutation_hint"
FROM ${quote(spec.tableName)} WHERE 0;`
}

function keyPredicate(spec: MovementTransferTableSpec): string {
  return spec.primaryColumns
    .map((name) => {
      const base = `${quote(spec.tableName)}.${quote(name)}`
      const incoming = `NEW.${quote(name)}`
      return `typeof(${base}) = typeof(${incoming}) AND ${base} IS ${incoming}`
    })
    .join(" AND ")
}

function triggerStatement(spec: MovementTransferTableSpec): string {
  const table = quote(spec.tableName)
  const view = quote(movementTransferViewName(spec.tableName))
  const columns = [...spec.columns, "__nozzle_bucket"]
  const columnSql = columns.map(quote).join(", ")
  const incomingSql = columns.map((name) => `NEW.${quote(name)}`).join(", ")
  const conflicts = spec.primaryColumns.map(quote).join(", ")
  const primary = new Set(spec.primaryColumns.map(fold))
  const assignments = columns
    .filter((name) => !primary.has(fold(name)))
    .map((name) => `${quote(name)} = excluded.${quote(name)}`)
    .join(", ")
  const tableLiteral = literal(spec.tableName)
  const trigger = quote(`nozzle_operation_${encodedName(spec.tableName)}_apply`)
  return `CREATE TRIGGER ${trigger}
INSTEAD OF INSERT ON ${view}
FOR EACH ROW
BEGIN
  SELECT CASE WHEN NEW."__nozzle_mutation_hint" NOT IN ('upsert', 'delete')
    THEN RAISE(ABORT, 'NOZZLE_OPERATION_MUTATION_HINT') END;
  SELECT CASE WHEN NOT EXISTS (
    SELECT 1 FROM "nozzle_operation_write_capabilities" AS "capability"
    WHERE "capability"."capability_token" = NEW."__nozzle_capability_token"
      AND "capability"."bucket_id" = NEW."__nozzle_bucket"
      AND "capability"."table_id" = ${tableLiteral}
      AND "capability"."remaining_uses" > 0
      AND "capability"."expires_at_ms" > CAST(unixepoch('subsec') * 1000 AS INTEGER)
      AND ((NEW."__nozzle_mutation_hint" = 'upsert' AND "capability"."mode" = 'upsert')
        OR (NEW."__nozzle_mutation_hint" = 'delete'
          AND "capability"."mode" IN ('delete', 'cleanup_delete')))
  ) THEN RAISE(ABORT, 'NOZZLE_OPERATION_CAPABILITY_INVALID') END;
  INSERT INTO "nozzle_operation_write_context"
    ("singleton", "capability_token", "operation_id", "bucket_id", "table_id", "mode")
  SELECT 1, "capability_token", "operation_id", "bucket_id", "table_id", "mode"
  FROM "nozzle_operation_write_capabilities"
  WHERE "capability_token" = NEW."__nozzle_capability_token";
  INSERT INTO ${table} (${columnSql})
  SELECT ${incomingSql} WHERE NEW."__nozzle_mutation_hint" = 'upsert'
  ON CONFLICT (${conflicts}) DO UPDATE SET ${assignments};
  DELETE FROM ${table}
  WHERE NEW."__nozzle_mutation_hint" = 'delete'
    AND ${table}."__nozzle_bucket" = NEW."__nozzle_bucket"
    AND ${keyPredicate(spec)};
  DELETE FROM "nozzle_operation_write_context" WHERE "singleton" = 1;
END;`
}

export function generateMovementTransferSql(input: {
  readonly tables: readonly MovementTransferTableSpec[]
}): GeneratedMovementTransferSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    fail("Movement transfer input must be an object.")
  }
  const tables = validateTables(input.tables)
  const statements: string[] = []
  for (const table of tables) statements.push(viewStatement(table), triggerStatement(table))
  const frozenStatements = Object.freeze(statements)
  return Object.freeze({
    schemaVersion: MOVEMENT_TRANSFER_SCHEMA_VERSION,
    sql: `${frozenStatements.join("\n\n")}\n`,
    statements: frozenStatements,
    tables,
  })
}
