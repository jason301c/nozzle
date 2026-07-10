import { NozzleError, type PartitionKeyType } from "@nozzle/core"
import { SchemaRegistry } from "@nozzle/drizzle"
import { type GeneratedMovementCaptureSql, generateMovementCaptureSql } from "./movement-capture.js"

const PARTITION_TYPES = new Set<PartitionKeyType>(["binary", "integer", "string", "uuid"])
const PARTITION_DATA_TYPES: Readonly<Record<PartitionKeyType, string>> = {
  binary: "buffer",
  integer: "number",
  string: "string",
  uuid: "string",
}

export function generateDrizzleMovementCaptureSql(input: {
  readonly partitionKeyType: PartitionKeyType
  readonly registry: SchemaRegistry
  readonly schemaId: string
}): GeneratedMovementCaptureSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new NozzleError("ConfigurationError", "Drizzle movement capture input must be an object.")
  }
  if (!(input.registry instanceof SchemaRegistry)) {
    throw new NozzleError("ConfigurationError", "Movement capture requires a schema registry.")
  }
  if (!PARTITION_TYPES.has(input.partitionKeyType)) {
    throw new NozzleError("ConfigurationError", "Movement capture partition type is unsupported.")
  }
  const tables = input.registry
    .tables()
    .filter((table) => table.classification === "sharded")
    .map((table) => {
      const partition = table.partitionColumn
      if (!partition) {
        throw new NozzleError(
          "PartitionKeyMissingError",
          "Movement table lacks partition metadata.",
        )
      }
      if (partition.dataType !== PARTITION_DATA_TYPES[input.partitionKeyType]) {
        throw new NozzleError(
          "ConfigurationError",
          "Movement partition type does not match the Drizzle column mode.",
        )
      }
      return {
        partitionColumn: partition.dbName,
        partitionType: input.partitionKeyType,
        primaryColumns: table.primaryColumns.map((column) => column.dbName),
        tableName: table.tableName,
      }
    })
  return generateMovementCaptureSql({ schemaId: input.schemaId, tables })
}
