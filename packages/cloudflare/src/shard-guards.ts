import { NozzleError } from "@nozzle/core"

export const SHARD_GUARD_SCHEMA_VERSION = 1 as const
export const SHARD_GUARD_HASH_VERSION = 1 as const

export type ShardPartitionType = "binary" | "integer" | "string" | "uuid"

export interface ShardGuardTableSpec {
  /** SQL name of an existing sharded application table. */
  readonly tableName: string
  /** SQL name of the table's NOT NULL partition column. */
  readonly partitionColumn: string
  readonly partitionType: ShardPartitionType
}

export interface ShardGuardSqlInput {
  /** Immutable application schema identity accepted by this guard generation. */
  readonly schemaId: string
  readonly tables: readonly ShardGuardTableSpec[]
}

export interface GeneratedShardGuardSql {
  readonly hashVersion: typeof SHARD_GUARD_HASH_VERSION
  readonly schemaId: string
  readonly schemaVersion: typeof SHARD_GUARD_SCHEMA_VERSION
  /** Statements are ordered so application-table triggers follow internal metadata. */
  readonly statements: readonly string[]
  /** The same statements joined into one deterministic migration artifact. */
  readonly sql: string
  readonly tables: readonly ShardGuardTableSpec[]
}

export class ShardGuardSqlError extends NozzleError {
  constructor(message: string) {
    super("ConfigurationError", message)
    this.name = "ShardGuardSqlError"
  }
}

const INTERNAL_BUCKET_COLUMN = "__nozzle_bucket"
const MAX_IDENTIFIER_BYTES = 255
const MAX_SCHEMA_ID_BYTES = 128
const MAX_SHARDED_TABLES = 1_000
const MAX_SAFE_PARTITION_INTEGER = 9_007_199_254_740_991
const PARTITION_TYPES = new Set<ShardPartitionType>(["binary", "integer", "string", "uuid"])

const INTERNAL_SCHEMA_STATEMENTS = Object.freeze([
  `CREATE TABLE IF NOT EXISTS "nozzle_bucket_ownership" (
  "bucket_id" INTEGER PRIMARY KEY NOT NULL CHECK ("bucket_id" BETWEEN 0 AND 4294967295),
  "route_epoch" INTEGER NOT NULL CHECK ("route_epoch" >= 0),
  "state" TEXT NOT NULL CHECK ("state" IN ('unassigned', 'preparing', 'copying', 'catching_up', 'writable', 'read_only', 'retired', 'quarantined', 'intervention_required')),
  "movement_role" TEXT NOT NULL CHECK ("movement_role" IN ('none', 'source', 'destination')),
  "operation_id" TEXT NOT NULL CHECK (length(trim("operation_id")) > 0),
  "fencing_token" INTEGER NOT NULL CHECK ("fencing_token" >= 1),
  "schema_version" INTEGER NOT NULL CHECK ("schema_version" >= 0),
  "last_verified_checkpoint" TEXT NOT NULL CHECK (length(trim("last_verified_checkpoint")) > 0),
  "last_verified_at_ms" INTEGER NOT NULL CHECK ("last_verified_at_ms" >= 0),
  "updated_at_ms" INTEGER NOT NULL CHECK ("updated_at_ms" >= 0)
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_guard_ownership_update"
BEFORE UPDATE ON "nozzle_bucket_ownership"
FOR EACH ROW
BEGIN
  SELECT CASE
    WHEN NEW."bucket_id" IS NOT OLD."bucket_id" THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_BUCKET_IMMUTABLE')
    WHEN NEW."fencing_token" < OLD."fencing_token" THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_FENCING_TOKEN')
    WHEN NEW."operation_id" IS NOT OLD."operation_id" AND NEW."fencing_token" <= OLD."fencing_token" THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_FENCING_TOKEN')
    WHEN NEW."route_epoch" < OLD."route_epoch" THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_ROUTE_EPOCH')
    WHEN NEW."state" = 'writable' AND OLD."state" <> 'writable' AND NEW."route_epoch" <= OLD."route_epoch" THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_ROUTE_EPOCH')
    WHEN NOT (
      NEW."state" = OLD."state"
      OR (OLD."state" = 'unassigned' AND NEW."state" IN ('preparing', 'writable', 'retired'))
      OR (OLD."state" = 'preparing' AND NEW."state" IN ('copying', 'quarantined', 'retired', 'intervention_required'))
      OR (OLD."state" = 'copying' AND NEW."state" IN ('catching_up', 'quarantined', 'intervention_required'))
      OR (OLD."state" = 'catching_up' AND NEW."state" IN ('writable', 'quarantined', 'intervention_required'))
      OR (OLD."state" = 'read_only' AND NEW."state" IN ('writable', 'quarantined', 'intervention_required'))
      OR (OLD."state" = 'writable' AND NEW."state" IN ('read_only', 'quarantined', 'intervention_required'))
      OR (OLD."state" = 'quarantined' AND NEW."state" IN ('preparing', 'retired', 'intervention_required'))
      OR (OLD."state" = 'intervention_required' AND NEW."state" IN ('preparing', 'retired'))
    ) THEN RAISE(ABORT, 'NOZZLE_OWNERSHIP_TRANSITION')
  END;
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_guard_ownership_delete"
BEFORE DELETE ON "nozzle_bucket_ownership"
FOR EACH ROW
BEGIN
  SELECT RAISE(ABORT, 'NOZZLE_OWNERSHIP_PERSISTENT');
END;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_schema_state" (
  "schema_id" TEXT PRIMARY KEY NOT NULL CHECK (length("schema_id") BETWEEN 1 AND 128),
  "schema_digest" TEXT NOT NULL CHECK (length("schema_digest") = 64 AND "schema_digest" = lower("schema_digest") AND "schema_digest" NOT GLOB '*[^0-9a-f]*'),
  "active" INTEGER NOT NULL CHECK ("active" IN (0, 1)),
  "activated_operation_id" TEXT NOT NULL CHECK (length(trim("activated_operation_id")) > 0),
  "activated_at_ms" INTEGER NOT NULL CHECK ("activated_at_ms" >= 0)
);`,
  `CREATE UNIQUE INDEX IF NOT EXISTS "nozzle_schema_state_one_active" ON "nozzle_schema_state" ("active") WHERE "active" = 1;`,
  `CREATE TABLE IF NOT EXISTS "nozzle_partition_fences" (
  "hash_version" INTEGER NOT NULL CHECK ("hash_version" = 1),
  "partition_digest" TEXT NOT NULL CHECK (length("partition_digest") = 64 AND "partition_digest" = lower("partition_digest") AND "partition_digest" NOT GLOB '*[^0-9a-f]*'),
  "partition_type" TEXT NOT NULL CHECK ("partition_type" IN ('binary', 'integer', 'string', 'uuid')),
  "partition_binary" BLOB,
  "partition_integer" INTEGER,
  "partition_string" TEXT,
  "partition_uuid" TEXT,
  "source_bucket_id" INTEGER NOT NULL CHECK ("source_bucket_id" BETWEEN 0 AND 4294967295),
  "source_route_epoch" INTEGER NOT NULL CHECK ("source_route_epoch" >= 0),
  "operation_id" TEXT NOT NULL CHECK (length(trim("operation_id")) > 0),
  "audit_event_id" TEXT NOT NULL CHECK (length(trim("audit_event_id")) > 0),
  "fenced_at_ms" INTEGER NOT NULL CHECK ("fenced_at_ms" >= 0),
  "reason" TEXT NOT NULL CHECK (length(trim("reason")) > 0),
  PRIMARY KEY ("hash_version", "partition_digest"),
  CHECK (
    ("partition_type" = 'binary' AND typeof("partition_binary") = 'blob' AND "partition_integer" IS NULL AND "partition_string" IS NULL AND "partition_uuid" IS NULL)
    OR ("partition_type" = 'integer' AND "partition_binary" IS NULL AND typeof("partition_integer") = 'integer' AND "partition_integer" BETWEEN -9007199254740991 AND 9007199254740991 AND "partition_string" IS NULL AND "partition_uuid" IS NULL)
    OR ("partition_type" = 'string' AND "partition_binary" IS NULL AND "partition_integer" IS NULL AND typeof("partition_string") = 'text' AND "partition_uuid" IS NULL)
    OR ("partition_type" = 'uuid' AND "partition_binary" IS NULL AND "partition_integer" IS NULL AND "partition_string" IS NULL AND typeof("partition_uuid") = 'text' AND length("partition_uuid") = 36 AND "partition_uuid" = lower("partition_uuid") AND substr("partition_uuid", 9, 1) = '-' AND substr("partition_uuid", 14, 1) = '-' AND substr("partition_uuid", 19, 1) = '-' AND substr("partition_uuid", 24, 1) = '-' AND length(replace("partition_uuid", '-', '')) = 32 AND replace("partition_uuid", '-', '') NOT GLOB '*[^0-9a-f]*')
  )
);`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_guard_partition_fence_update"
BEFORE UPDATE ON "nozzle_partition_fences"
FOR EACH ROW
BEGIN
  SELECT RAISE(ABORT, 'NOZZLE_PARTITION_FENCE_IMMUTABLE');
END;`,
  `CREATE TRIGGER IF NOT EXISTS "nozzle_guard_partition_fence_delete"
BEFORE DELETE ON "nozzle_partition_fences"
FOR EACH ROW
BEGIN
  SELECT RAISE(ABORT, 'NOZZLE_PARTITION_FENCE_PERSISTENT');
END;`,
  `CREATE INDEX IF NOT EXISTS "nozzle_partition_fences_binary" ON "nozzle_partition_fences" ("partition_binary") WHERE "hash_version" = 1 AND "partition_type" = 'binary';`,
  `CREATE INDEX IF NOT EXISTS "nozzle_partition_fences_integer" ON "nozzle_partition_fences" ("partition_integer") WHERE "hash_version" = 1 AND "partition_type" = 'integer';`,
  `CREATE INDEX IF NOT EXISTS "nozzle_partition_fences_string" ON "nozzle_partition_fences" ("partition_string" COLLATE BINARY) WHERE "hash_version" = 1 AND "partition_type" = 'string';`,
  `CREATE INDEX IF NOT EXISTS "nozzle_partition_fences_uuid" ON "nozzle_partition_fences" ("partition_uuid" COLLATE BINARY) WHERE "hash_version" = 1 AND "partition_type" = 'uuid';`,
])

function fail(message: string): never {
  throw new ShardGuardSqlError(message)
}

function asciiFold(value: string): string {
  let output = ""
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    output += code >= 65 && code <= 90 ? String.fromCharCode(code + 32) : value.charAt(index)
  }
  return output
}

function compareCodeUnits(left: string, right: string): number {
  const length = Math.min(left.length, right.length)
  for (let index = 0; index < length; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index)
    if (difference !== 0) return difference
  }
  return left.length - right.length
}

function assertWellFormed(value: string, label: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        fail(`${label} cannot contain an unpaired UTF-16 surrogate.`)
      }
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      fail(`${label} cannot contain an unpaired UTF-16 surrogate.`)
    }
  }
}

function validateIdentifier(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    fail(`${label} must be a non-empty, non-whitespace string.`)
  }
  assertWellFormed(value, label)
  const byteLength = new TextEncoder().encode(value).byteLength
  if (byteLength > MAX_IDENTIFIER_BYTES) {
    fail(`${label} cannot exceed ${MAX_IDENTIFIER_BYTES} UTF-8 bytes.`)
  }
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code <= 0x1f || code === 0x7f) {
      fail(`${label} cannot contain ASCII control characters.`)
    }
  }
  const folded = asciiFold(value)
  if (
    folded.startsWith("sqlite_") ||
    folded.startsWith("nozzle_") ||
    folded.startsWith("__nozzle_")
  ) {
    fail(`${label} uses a reserved SQLite or Nozzle identifier.`)
  }
  return value
}

function validateSchemaId(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._:-]*$/u.test(value)) {
    fail(
      "schemaId must begin with an ASCII alphanumeric and contain only ASCII alphanumerics, '.', '_', ':', or '-'.",
    )
  }
  if (new TextEncoder().encode(value).byteLength > MAX_SCHEMA_ID_BYTES) {
    fail(`schemaId cannot exceed ${MAX_SCHEMA_ID_BYTES} UTF-8 bytes.`)
  }
  return value
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function triggerIdentifier(tableName: string, operation: "delete" | "insert" | "update"): string {
  const bytes = new TextEncoder().encode(tableName)
  let encoded = ""
  for (const byte of bytes) encoded += byte.toString(16).padStart(2, "0")
  return quoteIdentifier(`nozzle_guard_${encoded}_${operation}`)
}

function recordColumn(reference: "NEW" | "OLD", column: string): string {
  return `${reference}.${quoteIdentifier(column)}`
}

function validUuidExpression(value: string): string {
  return `typeof(${value}) = 'text' AND length(${value}) = 36 AND substr(${value}, 9, 1) = '-' AND substr(${value}, 14, 1) = '-' AND substr(${value}, 19, 1) = '-' AND substr(${value}, 24, 1) = '-' AND length(replace(${value}, '-', '')) = 32 AND lower(replace(${value}, '-', '')) NOT GLOB '*[^0-9a-f]*'`
}

function validPartitionExpression(spec: ShardGuardTableSpec, reference: "NEW" | "OLD"): string {
  const value = recordColumn(reference, spec.partitionColumn)
  if (spec.partitionType === "uuid") return validUuidExpression(value)
  const sqliteType =
    spec.partitionType === "string"
      ? "text"
      : spec.partitionType === "binary"
        ? "blob"
        : spec.partitionType
  const integerRange =
    spec.partitionType === "integer"
      ? ` AND ${value} BETWEEN -${MAX_SAFE_PARTITION_INTEGER} AND ${MAX_SAFE_PARTITION_INTEGER}`
      : ""
  return `typeof(${value}) = '${sqliteType}'${integerRange}`
}

function validBucketExpression(reference: "NEW" | "OLD"): string {
  const bucket = recordColumn(reference, INTERNAL_BUCKET_COLUMN)
  return `typeof(${bucket}) = 'integer' AND ${bucket} BETWEEN 0 AND 4294967295`
}

function writableOwnershipExpression(reference: "NEW" | "OLD"): string {
  const bucket = recordColumn(reference, INTERNAL_BUCKET_COLUMN)
  return `EXISTS (SELECT 1 FROM "nozzle_bucket_ownership" AS "nozzle_owner" WHERE "nozzle_owner"."bucket_id" = ${bucket} AND "nozzle_owner"."state" = 'writable')`
}

function activeSchemaExpression(): string {
  return `EXISTS (SELECT 1 FROM "nozzle_schema_state" AS "nozzle_schema" WHERE "nozzle_schema"."active" = 1)`
}

function fenceExpression(spec: ShardGuardTableSpec, reference: "NEW" | "OLD"): string {
  const value = recordColumn(reference, spec.partitionColumn)
  let comparison: string
  switch (spec.partitionType) {
    case "binary":
      comparison = `"nozzle_fence"."partition_binary" = ${value}`
      break
    case "integer":
      comparison = `"nozzle_fence"."partition_integer" = ${value}`
      break
    case "string":
      comparison = `"nozzle_fence"."partition_string" COLLATE BINARY = ${value} COLLATE BINARY`
      break
    case "uuid":
      comparison = `"nozzle_fence"."partition_uuid" COLLATE BINARY = lower(${value}) COLLATE BINARY`
      break
  }
  return `EXISTS (SELECT 1 FROM "nozzle_partition_fences" AS "nozzle_fence" WHERE "nozzle_fence"."hash_version" = ${SHARD_GUARD_HASH_VERSION} AND "nozzle_fence"."partition_type" = '${spec.partitionType}' AND ${comparison})`
}

function caseStatement(conditions: readonly [expression: string, error: string][]): string {
  const cases = conditions
    .map(([expression, error]) => `    WHEN ${expression} THEN RAISE(ABORT, '${error}')`)
    .join("\n")
  return `  SELECT CASE\n${cases}\n  END;`
}

function insertTrigger(spec: ShardGuardTableSpec): string {
  const conditions: readonly [string, string][] = [
    [`NOT (${validBucketExpression("NEW")})`, "NOZZLE_GUARD_BUCKET_TYPE"],
    [`NOT (${validPartitionExpression(spec, "NEW")})`, "NOZZLE_GUARD_PARTITION_TYPE"],
    [`NOT (${activeSchemaExpression()})`, "NOZZLE_GUARD_SCHEMA"],
    [`NOT (${writableOwnershipExpression("NEW")})`, "NOZZLE_GUARD_OWNERSHIP"],
    [fenceExpression(spec, "NEW"), "NOZZLE_GUARD_PARTITION_FENCE"],
  ]
  return `CREATE TRIGGER ${triggerIdentifier(spec.tableName, "insert")}
BEFORE INSERT ON ${quoteIdentifier(spec.tableName)}
FOR EACH ROW
BEGIN
${caseStatement(conditions)}
END;`
}

function updateTrigger(spec: ShardGuardTableSpec): string {
  const oldBucket = recordColumn("OLD", INTERNAL_BUCKET_COLUMN)
  const newBucket = recordColumn("NEW", INTERNAL_BUCKET_COLUMN)
  const oldPartition = recordColumn("OLD", spec.partitionColumn)
  const newPartition = recordColumn("NEW", spec.partitionColumn)
  const conditions: readonly [string, string][] = [
    [
      `NOT (${validBucketExpression("OLD")}) OR NOT (${validBucketExpression("NEW")})`,
      "NOZZLE_GUARD_BUCKET_TYPE",
    ],
    [
      `NOT (${validPartitionExpression(spec, "OLD")}) OR NOT (${validPartitionExpression(spec, "NEW")})`,
      "NOZZLE_GUARD_PARTITION_TYPE",
    ],
    [`${newBucket} IS NOT ${oldBucket}`, "NOZZLE_GUARD_BUCKET_IMMUTABLE"],
    [
      `NOT (${newPartition} COLLATE BINARY IS ${oldPartition} COLLATE BINARY)`,
      "NOZZLE_GUARD_PARTITION_IMMUTABLE",
    ],
    [`NOT (${activeSchemaExpression()})`, "NOZZLE_GUARD_SCHEMA"],
    [`NOT (${writableOwnershipExpression("OLD")})`, "NOZZLE_GUARD_OWNERSHIP"],
    [`NOT (${writableOwnershipExpression("NEW")})`, "NOZZLE_GUARD_OWNERSHIP"],
    [fenceExpression(spec, "OLD"), "NOZZLE_GUARD_PARTITION_FENCE"],
    [fenceExpression(spec, "NEW"), "NOZZLE_GUARD_PARTITION_FENCE"],
  ]
  return `CREATE TRIGGER ${triggerIdentifier(spec.tableName, "update")}
BEFORE UPDATE ON ${quoteIdentifier(spec.tableName)}
FOR EACH ROW
BEGIN
${caseStatement(conditions)}
END;`
}

function deleteTrigger(spec: ShardGuardTableSpec): string {
  const conditions: readonly [string, string][] = [
    [`NOT (${validBucketExpression("OLD")})`, "NOZZLE_GUARD_BUCKET_TYPE"],
    [`NOT (${validPartitionExpression(spec, "OLD")})`, "NOZZLE_GUARD_PARTITION_TYPE"],
    [`NOT (${activeSchemaExpression()})`, "NOZZLE_GUARD_SCHEMA"],
    [`NOT (${writableOwnershipExpression("OLD")})`, "NOZZLE_GUARD_OWNERSHIP"],
    [fenceExpression(spec, "OLD"), "NOZZLE_GUARD_PARTITION_FENCE"],
  ]
  return `CREATE TRIGGER ${triggerIdentifier(spec.tableName, "delete")}
BEFORE DELETE ON ${quoteIdentifier(spec.tableName)}
FOR EACH ROW
BEGIN
${caseStatement(conditions)}
END;`
}

function validateTables(value: unknown): readonly ShardGuardTableSpec[] {
  if (!Array.isArray(value) || value.length === 0) {
    fail("tables must contain at least one sharded table specification.")
  }
  if (value.length > MAX_SHARDED_TABLES) {
    fail(`tables cannot contain more than ${MAX_SHARDED_TABLES} sharded table specifications.`)
  }
  const tableNames = new Set<string>()
  const validated: ShardGuardTableSpec[] = []
  for (const candidate of value) {
    if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
      fail("Every sharded table specification must be an object.")
    }
    const record = candidate as Partial<ShardGuardTableSpec>
    const tableName = validateIdentifier(record.tableName, "tableName")
    const partitionColumn = validateIdentifier(record.partitionColumn, "partitionColumn")
    if (
      typeof record.partitionType !== "string" ||
      !PARTITION_TYPES.has(record.partitionType as ShardPartitionType)
    ) {
      fail("partitionType must be one of binary, integer, string, or uuid.")
    }
    const collisionKey = asciiFold(tableName)
    if (tableNames.has(collisionKey)) {
      fail("Sharded table names must be unique under SQLite ASCII case-insensitive matching.")
    }
    tableNames.add(collisionKey)
    validated.push(
      Object.freeze({
        tableName,
        partitionColumn,
        partitionType: record.partitionType as ShardPartitionType,
      }),
    )
  }
  validated.sort((left, right) => {
    return compareCodeUnits(asciiFold(left.tableName), asciiFold(right.tableName))
  })
  return Object.freeze(validated)
}

export function generateShardGuardSql(input: ShardGuardSqlInput): GeneratedShardGuardSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    fail("Shard guard input must be an object.")
  }
  const schemaId = validateSchemaId(input.schemaId)
  const tables = validateTables(input.tables)
  const statements = [...INTERNAL_SCHEMA_STATEMENTS]
  for (const table of tables) {
    statements.push(insertTrigger(table), updateTrigger(table), deleteTrigger(table))
  }
  const frozenStatements = Object.freeze(statements)
  return Object.freeze({
    hashVersion: SHARD_GUARD_HASH_VERSION,
    schemaId,
    schemaVersion: SHARD_GUARD_SCHEMA_VERSION,
    statements: frozenStatements,
    sql: `${frozenStatements.join("\n\n")}\n`,
    tables,
  })
}
