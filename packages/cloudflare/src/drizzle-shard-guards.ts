import { NozzleError, type PartitionKeyType } from "@nozzle/core"
import { SchemaRegistry } from "@nozzle/drizzle"
import { type GeneratedShardGuardSql, generateShardGuardSql } from "./shard-guards.js"

export interface DrizzleShardGuardSqlInput {
  readonly partitionKeyType: PartitionKeyType
  readonly registry: SchemaRegistry
  readonly schemaId: string
}

const PARTITION_DATA_TYPES: Readonly<Record<PartitionKeyType, string>> = {
  binary: "buffer",
  integer: "number",
  string: "string",
  uuid: "string",
}
const PARTITION_KEY_TYPES = new Set<PartitionKeyType>(["binary", "integer", "string", "uuid"])

export function generateDrizzleShardGuardSql(
  input: DrizzleShardGuardSqlInput,
): GeneratedShardGuardSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new NozzleError("ConfigurationError", "Drizzle shard guard input must be an object.")
  }
  if (!(input.registry instanceof SchemaRegistry)) {
    throw new NozzleError(
      "ConfigurationError",
      "Drizzle shard guards require a compiled Nozzle schema registry.",
    )
  }
  if (!PARTITION_KEY_TYPES.has(input.partitionKeyType)) {
    throw new NozzleError("ConfigurationError", "The partition-key type is unsupported.")
  }
  const tables = input.registry
    .tables()
    .filter((table) => table.classification === "sharded")
    .map((table) => {
      const partitionColumn = table.partitionColumn
      if (!partitionColumn) {
        throw new NozzleError(
          "PartitionKeyMissingError",
          "A sharded table is missing compiled partition metadata.",
          { details: { table: table.tableName } },
        )
      }
      if (partitionColumn.dataType !== PARTITION_DATA_TYPES[input.partitionKeyType]) {
        throw new NozzleError(
          "ConfigurationError",
          "The declared partition-key type does not match the Drizzle column mode.",
          {
            details: {
              actual: partitionColumn.dataType,
              expected: PARTITION_DATA_TYPES[input.partitionKeyType],
              table: table.tableName,
            },
          },
        )
      }
      return {
        partitionColumn: partitionColumn.dbName,
        partitionType: input.partitionKeyType,
        tableName: table.tableName,
      }
    })

  return generateShardGuardSql({ schemaId: input.schemaId, tables })
}
