import { NozzleError } from "@nozzle/core"

export const MOVEMENT_CAPTURE_SCHEMA_VERSION = 1 as const
export const MOVEMENT_KEY_ENCODING_VERSION = 1 as const

export type MovementCapturePartitionType = "binary" | "integer" | "string" | "uuid"

export interface MovementCaptureTableSpec {
  readonly partitionColumn: string
  readonly partitionType: MovementCapturePartitionType
  readonly primaryColumns: readonly string[]
  readonly tableName: string
}

export interface MovementCaptureSqlInput {
  readonly schemaId: string
  readonly tables: readonly MovementCaptureTableSpec[]
}

export interface GeneratedMovementCaptureSql {
  readonly keyEncodingVersion: typeof MOVEMENT_KEY_ENCODING_VERSION
  readonly schemaId: string
  readonly schemaVersion: typeof MOVEMENT_CAPTURE_SCHEMA_VERSION
  readonly sql: string
  readonly statements: readonly string[]
  readonly tables: readonly MovementCaptureTableSpec[]
}

const MAX_IDENTIFIER_BYTES = 255
const MAX_SCHEMA_ID_BYTES = 128
const MAX_TABLES = 1_000
const PARTITION_TYPES = new Set<MovementCapturePartitionType>([
  "binary",
  "integer",
  "string",
  "uuid",
])

function fail(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function fold(value: string): string {
  let output = ""
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    output += code >= 65 && code <= 90 ? String.fromCharCode(code + 32) : value.charAt(index)
  }
  return output
}

function compare(left: string, right: string): number {
  const length = Math.min(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index)
    if (difference !== 0) return difference
  }
  return left.length - right.length
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
  const folded = fold(value)
  if (folded.startsWith("sqlite_") || folded.startsWith("nozzle_")) {
    fail(`${label} uses a reserved identifier.`)
  }
  return value
}

function schemaId(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]*$/u.test(value)) {
    fail("schemaId is malformed.")
  }
  if (new TextEncoder().encode(value).byteLength > MAX_SCHEMA_ID_BYTES) {
    fail("schemaId is too long.")
  }
  return value
}

function quote(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function triggerName(tableName: string, operation: "delete" | "insert" | "update", stage: string) {
  let encoded = ""
  for (const byte of new TextEncoder().encode(tableName)) {
    encoded += byte.toString(16).padStart(2, "0")
  }
  return quote(`nozzle_capture_${encoded}_${stage}_${operation}`)
}

function column(reference: "NEW" | "OLD", name: string): string {
  return `${reference}.${quote(name)}`
}

function typedScopeComparison(spec: MovementCaptureTableSpec, reference: "NEW" | "OLD"): string {
  const value = column(reference, spec.partitionColumn)
  switch (spec.partitionType) {
    case "binary":
      return `"nozzle_capture"."partition_binary" IS ${value}`
    case "integer":
      return `"nozzle_capture"."partition_integer" IS ${value}`
    case "string":
      return `"nozzle_capture"."partition_string" COLLATE BINARY IS ${value} COLLATE BINARY`
    case "uuid":
      return `"nozzle_capture"."partition_uuid" COLLATE BINARY IS lower(${value}) COLLATE BINARY`
  }
}

function captureMatch(
  spec: MovementCaptureTableSpec,
  schema: string,
  reference: "NEW" | "OLD",
): string {
  return `"nozzle_capture"."bucket_id" = ${column(reference, "__nozzle_bucket")}
    AND "nozzle_capture"."schema_id" = ${literal(schema)}
    AND "nozzle_capture"."state" IN ('active', 'draining')
    AND ("nozzle_capture"."scope_kind" = 'bucket'
      OR ("nozzle_capture"."partition_type" = '${spec.partitionType}'
        AND ${typedScopeComparison(spec, reference)}))`
}

function primaryTypeExpression(spec: MovementCaptureTableSpec, reference: "NEW" | "OLD"): string {
  return spec.primaryColumns
    .map((name) => `typeof(${column(reference, name)}) NOT IN ('blob', 'integer', 'real', 'text')`)
    .join(" OR ")
}

function immutablePrimaryExpression(spec: MovementCaptureTableSpec): string {
  return spec.primaryColumns
    .map((name) => {
      const oldValue = column("OLD", name)
      const newValue = column("NEW", name)
      return `typeof(${newValue}) <> typeof(${oldValue}) OR NOT (${newValue} IS ${oldValue})`
    })
    .join(" OR ")
}

function encodedPrimaryValue(value: string): string {
  return `CASE typeof(${value})
      WHEN 'blob' THEN lower(hex(${value}))
      WHEN 'integer' THEN printf('%lld', ${value})
      WHEN 'real' THEN printf('%.17g', ${value})
      WHEN 'text' THEN ${value}
      ELSE NULL
    END`
}

function keyJson(spec: MovementCaptureTableSpec, reference: "NEW" | "OLD"): string {
  const components = spec.primaryColumns.map((name) => {
    const value = column(reference, name)
    return `json_object('column', ${literal(name)}, 'type', typeof(${value}), 'value', ${encodedPrimaryValue(value)})`
  })
  return `json_array(${components.join(", ")})`
}

function preflightTrigger(
  spec: MovementCaptureTableSpec,
  schema: string,
  operation: "delete" | "insert" | "update",
): string {
  const reference = operation === "delete" ? "OLD" : "NEW"
  const primaryType = primaryTypeExpression(spec, reference)
  const immutable =
    operation === "update"
      ? `
  SELECT CASE WHEN ${immutablePrimaryExpression(spec)}
    THEN RAISE(ABORT, 'NOZZLE_CAPTURE_PRIMARY_KEY_IMMUTABLE') END;`
      : ""
  return `CREATE TRIGGER ${triggerName(spec.tableName, operation, "preflight")}
BEFORE ${operation.toUpperCase()} ON ${quote(spec.tableName)}
FOR EACH ROW
BEGIN
  SELECT CASE WHEN ${primaryType}
    THEN RAISE(ABORT, 'NOZZLE_CAPTURE_PRIMARY_KEY_TYPE') END;${immutable}
  SELECT CASE WHEN EXISTS (
    SELECT 1 FROM "nozzle_movement_captures" AS "nozzle_capture"
    WHERE ${captureMatch(spec, schema, reference)}
      AND (SELECT count(*) FROM "nozzle_movement_outbox" AS "nozzle_outbox"
        WHERE "nozzle_outbox"."operation_id" = "nozzle_capture"."operation_id"
          AND "nozzle_outbox"."sequence" > "nozzle_capture"."acknowledged_sequence")
        >= "nozzle_capture"."max_pending_entries"
  ) THEN RAISE(ABORT, 'NOZZLE_CAPTURE_BACKPRESSURE') END;
END;`
}

function journalTrigger(
  spec: MovementCaptureTableSpec,
  schema: string,
  operation: "delete" | "insert" | "update",
): string {
  const reference = operation === "delete" ? "OLD" : "NEW"
  const hint = operation === "delete" ? "delete" : "upsert"
  const shape = JSON.stringify(spec.primaryColumns)
  return `CREATE TRIGGER ${triggerName(spec.tableName, operation, "journal")}
AFTER ${operation.toUpperCase()} ON ${quote(spec.tableName)}
FOR EACH ROW
BEGIN
  INSERT INTO "nozzle_movement_outbox"
    ("operation_id", "bucket_id", "table_id", "mutation_hint", "key_json",
     "key_shape_json", "schema_id", "schema_checksum", "key_encoding_version", "created_at_ms")
  SELECT "nozzle_capture"."operation_id", ${column(reference, "__nozzle_bucket")},
    ${literal(spec.tableName)}, '${hint}', ${keyJson(spec, reference)}, ${literal(shape)},
    "nozzle_capture"."schema_id", "nozzle_capture"."schema_checksum",
    ${MOVEMENT_KEY_ENCODING_VERSION}, CAST(unixepoch('subsec') * 1000 AS INTEGER)
  FROM "nozzle_movement_captures" AS "nozzle_capture"
  WHERE ${captureMatch(spec, schema, reference)};
END;`
}

const INTERNAL_STATEMENTS = Object.freeze([
  `CREATE TABLE IF NOT EXISTS "nozzle_movement_captures" (
  "operation_id" TEXT PRIMARY KEY NOT NULL CHECK (length(trim("operation_id")) > 0),
  "bucket_id" INTEGER NOT NULL CHECK ("bucket_id" BETWEEN 0 AND 4294967295),
  "scope_kind" TEXT NOT NULL CHECK ("scope_kind" IN ('bucket', 'partition')),
  "partition_type" TEXT CHECK ("partition_type" IN ('binary', 'integer', 'string', 'uuid')),
  "partition_binary" BLOB,
  "partition_integer" INTEGER,
  "partition_string" TEXT,
  "partition_uuid" TEXT,
  "schema_id" TEXT NOT NULL CHECK (length("schema_id") BETWEEN 1 AND 128),
  "schema_checksum" TEXT NOT NULL CHECK (length("schema_checksum") = 64 AND "schema_checksum" = lower("schema_checksum") AND "schema_checksum" NOT GLOB '*[^0-9a-f]*'),
  "state" TEXT NOT NULL CHECK ("state" IN ('active', 'draining', 'completed', 'aborted')),
  "start_sequence" INTEGER NOT NULL CHECK ("start_sequence" >= 0),
  "acknowledged_sequence" INTEGER NOT NULL CHECK ("acknowledged_sequence" >= "start_sequence"),
  "max_pending_entries" INTEGER NOT NULL CHECK ("max_pending_entries" >= 1),
  "fencing_token" INTEGER NOT NULL CHECK ("fencing_token" >= 1),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= "created_at_ms"),
  CHECK (
    ("scope_kind" = 'bucket' AND "partition_type" IS NULL AND "partition_binary" IS NULL AND "partition_integer" IS NULL AND "partition_string" IS NULL AND "partition_uuid" IS NULL)
    OR ("scope_kind" = 'partition' AND (
      ("partition_type" = 'binary' AND typeof("partition_binary") = 'blob' AND "partition_integer" IS NULL AND "partition_string" IS NULL AND "partition_uuid" IS NULL)
      OR ("partition_type" = 'integer' AND "partition_binary" IS NULL AND typeof("partition_integer") = 'integer' AND "partition_string" IS NULL AND "partition_uuid" IS NULL)
      OR ("partition_type" = 'string' AND "partition_binary" IS NULL AND "partition_integer" IS NULL AND typeof("partition_string") = 'text' AND "partition_uuid" IS NULL)
      OR ("partition_type" = 'uuid' AND "partition_binary" IS NULL AND "partition_integer" IS NULL AND "partition_string" IS NULL AND typeof("partition_uuid") = 'text' AND length("partition_uuid") = 36 AND "partition_uuid" = lower("partition_uuid") AND substr("partition_uuid", 9, 1) = '-' AND substr("partition_uuid", 14, 1) = '-' AND substr("partition_uuid", 19, 1) = '-' AND substr("partition_uuid", 24, 1) = '-' AND length(replace("partition_uuid", '-', '')) = 32 AND replace("partition_uuid", '-', '') NOT GLOB '*[^0-9a-f]*')
    ))
  )
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_capture_insert"
BEFORE INSERT ON "nozzle_movement_captures"
FOR EACH ROW
WHEN NEW."state" IN ('active', 'draining')
  AND EXISTS (
    SELECT 1 FROM "nozzle_movement_captures" AS "existing"
    WHERE "existing"."bucket_id" = NEW."bucket_id"
      AND "existing"."state" IN ('active', 'draining')
      AND ("existing"."scope_kind" = 'bucket' OR NEW."scope_kind" = 'bucket' OR (
        "existing"."partition_type" = NEW."partition_type"
        AND "existing"."partition_binary" IS NEW."partition_binary"
        AND "existing"."partition_integer" IS NEW."partition_integer"
        AND "existing"."partition_string" IS NEW."partition_string"
        AND "existing"."partition_uuid" IS NEW."partition_uuid"
      ))
  )
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CAPTURE_SCOPE_CONFLICT'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_capture_update"
BEFORE UPDATE ON "nozzle_movement_captures"
FOR EACH ROW
BEGIN
  SELECT CASE
    WHEN NEW."operation_id" IS NOT OLD."operation_id"
      OR NEW."bucket_id" IS NOT OLD."bucket_id"
      OR NEW."scope_kind" IS NOT OLD."scope_kind"
      OR NEW."partition_type" IS NOT OLD."partition_type"
      OR NEW."partition_binary" IS NOT OLD."partition_binary"
      OR NEW."partition_integer" IS NOT OLD."partition_integer"
      OR NEW."partition_string" IS NOT OLD."partition_string"
      OR NEW."partition_uuid" IS NOT OLD."partition_uuid"
      OR NEW."schema_id" IS NOT OLD."schema_id"
      OR NEW."schema_checksum" IS NOT OLD."schema_checksum"
      OR NEW."start_sequence" IS NOT OLD."start_sequence"
      OR NEW."max_pending_entries" IS NOT OLD."max_pending_entries"
      OR NEW."created_at_ms" IS NOT OLD."created_at_ms"
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_PLAN_IMMUTABLE')
    WHEN NEW."fencing_token" < OLD."fencing_token"
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_FENCING_TOKEN')
    WHEN NEW."updated_at_ms" < OLD."updated_at_ms"
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_TIME_ROLLBACK')
    WHEN NEW."acknowledged_sequence" < OLD."acknowledged_sequence"
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_ACK_ROLLBACK')
    WHEN NEW."acknowledged_sequence" > COALESCE(
      (SELECT max("sequence") FROM "nozzle_movement_outbox" WHERE "operation_id" = OLD."operation_id"),
      OLD."start_sequence")
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_ACK_UNKNOWN')
    WHEN NOT (
      NEW."state" = OLD."state"
      OR (OLD."state" = 'active' AND NEW."state" IN ('draining', 'completed', 'aborted'))
      OR (OLD."state" = 'draining' AND NEW."state" IN ('completed', 'aborted'))
    ) THEN RAISE(ABORT, 'NOZZLE_CAPTURE_STATE_TRANSITION')
    WHEN NEW."state" = 'completed' AND NEW."acknowledged_sequence" < COALESCE(
      (SELECT max("sequence") FROM "nozzle_movement_outbox" WHERE "operation_id" = OLD."operation_id"),
      OLD."start_sequence")
      THEN RAISE(ABORT, 'NOZZLE_CAPTURE_TAIL_PENDING')
  END;
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_capture_delete"
BEFORE DELETE ON "nozzle_movement_captures"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_CAPTURE_PERSISTENT'); END;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_movement_outbox" (
  "sequence" INTEGER PRIMARY KEY AUTOINCREMENT,
  "operation_id" TEXT NOT NULL REFERENCES "nozzle_movement_captures" ("operation_id"),
  "bucket_id" INTEGER NOT NULL CHECK ("bucket_id" BETWEEN 0 AND 4294967295),
  "table_id" TEXT NOT NULL CHECK (length(trim("table_id")) > 0),
  "mutation_hint" TEXT NOT NULL CHECK ("mutation_hint" IN ('upsert', 'delete')),
  "key_json" TEXT NOT NULL CHECK (json_valid("key_json") AND json_type("key_json") = 'array'),
  "key_shape_json" TEXT NOT NULL CHECK (json_valid("key_shape_json") AND json_type("key_shape_json") = 'array'),
  "schema_id" TEXT NOT NULL,
  "schema_checksum" TEXT NOT NULL CHECK (length("schema_checksum") = 64),
  "key_encoding_version" INTEGER NOT NULL CHECK ("key_encoding_version" = 1),
  "created_at_ms" INTEGER NOT NULL CHECK ("created_at_ms" >= 0)
);`,
  `CREATE INDEX IF NOT EXISTS "nozzle_movement_outbox_operation_sequence" ON "nozzle_movement_outbox" ("operation_id", "sequence");`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_outbox_update"
BEFORE UPDATE ON "nozzle_movement_outbox"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_OUTBOX_IMMUTABLE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_outbox_delete"
BEFORE DELETE ON "nozzle_movement_outbox"
WHEN NOT EXISTS (
  SELECT 1 FROM "nozzle_movement_captures" AS "capture"
  WHERE "capture"."operation_id" = OLD."operation_id"
    AND "capture"."state" IN ('completed', 'aborted')
    AND OLD."sequence" <= "capture"."acknowledged_sequence"
)
BEGIN SELECT RAISE(ABORT, 'NOZZLE_OUTBOX_RETENTION'); END;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_movement_replay_receipts" (
  "operation_id" TEXT NOT NULL,
  "source_sequence" INTEGER NOT NULL CHECK ("source_sequence" >= 1),
  "table_id" TEXT NOT NULL,
  "key_json" TEXT NOT NULL CHECK (json_valid("key_json")),
  "mutation_hint" TEXT NOT NULL CHECK ("mutation_hint" IN ('upsert', 'delete')),
  "result_checksum" TEXT NOT NULL,
  "applied_at_ms" INTEGER NOT NULL CHECK ("applied_at_ms" >= 0),
  PRIMARY KEY ("operation_id", "source_sequence")
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_replay_receipt_update"
BEFORE UPDATE ON "nozzle_movement_replay_receipts"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_REPLAY_RECEIPT_IMMUTABLE'); END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_replay_receipt_delete"
BEFORE DELETE ON "nozzle_movement_replay_receipts"
BEGIN SELECT RAISE(ABORT, 'NOZZLE_REPLAY_RECEIPT_PERSISTENT'); END;`,
])

function validateTables(value: unknown): readonly MovementCaptureTableSpec[] {
  if (!Array.isArray(value) || value.length === 0) fail("tables must not be empty.")
  if (value.length > MAX_TABLES) fail("tables exceeds the supported limit.")
  const names = new Set<string>()
  const output: MovementCaptureTableSpec[] = []
  for (const candidate of value) {
    if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
      fail("Every movement capture table must be an object.")
    }
    const record = candidate as Partial<MovementCaptureTableSpec>
    const tableName = identifier(record.tableName, "tableName")
    const partitionColumn = identifier(record.partitionColumn, "partitionColumn")
    if (!PARTITION_TYPES.has(record.partitionType as MovementCapturePartitionType)) {
      fail("partitionType is unsupported.")
    }
    if (!Array.isArray(record.primaryColumns) || record.primaryColumns.length === 0) {
      fail("primaryColumns must not be empty.")
    }
    const primaryNames = new Set<string>()
    const primaryColumns = record.primaryColumns.map((value) => {
      const name = identifier(value, "primaryColumn")
      const folded = fold(name)
      if (primaryNames.has(folded)) fail("primaryColumns must be unique.")
      primaryNames.add(folded)
      return name
    })
    const foldedName = fold(tableName)
    if (names.has(foldedName)) fail("Movement capture table names must be unique.")
    names.add(foldedName)
    output.push(
      Object.freeze({
        partitionColumn,
        partitionType: record.partitionType as MovementCapturePartitionType,
        primaryColumns: Object.freeze(primaryColumns),
        tableName,
      }),
    )
  }
  output.sort((left, right) => compare(fold(left.tableName), fold(right.tableName)))
  return Object.freeze(output)
}

export function generateMovementCaptureSql(
  input: MovementCaptureSqlInput,
): GeneratedMovementCaptureSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    fail("Movement capture input must be an object.")
  }
  const validatedSchemaId = schemaId(input.schemaId)
  const tables = validateTables(input.tables)
  const statements = [...INTERNAL_STATEMENTS]
  for (const table of tables) {
    statements.push(
      preflightTrigger(table, validatedSchemaId, "insert"),
      preflightTrigger(table, validatedSchemaId, "update"),
      preflightTrigger(table, validatedSchemaId, "delete"),
      journalTrigger(table, validatedSchemaId, "insert"),
      journalTrigger(table, validatedSchemaId, "update"),
      journalTrigger(table, validatedSchemaId, "delete"),
    )
  }
  const frozenStatements = Object.freeze(statements)
  return Object.freeze({
    keyEncodingVersion: MOVEMENT_KEY_ENCODING_VERSION,
    schemaId: validatedSchemaId,
    schemaVersion: MOVEMENT_CAPTURE_SCHEMA_VERSION,
    sql: `${frozenStatements.join("\n\n")}\n`,
    statements: frozenStatements,
    tables,
  })
}
