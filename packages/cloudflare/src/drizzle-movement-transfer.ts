import { NozzleError } from "@nozzle/core"
import { SchemaRegistry } from "@nozzle/drizzle"
import {
  type GeneratedMovementTransferSql,
  generateMovementTransferSql,
} from "./movement-transfer.js"

export function generateDrizzleMovementTransferSql(input: {
  readonly registry: SchemaRegistry
}): GeneratedMovementTransferSql {
  if (typeof input !== "object" || input === null || Array.isArray(input)) {
    throw new NozzleError(
      "ConfigurationError",
      "Drizzle movement transfer input must be an object.",
    )
  }
  if (!(input.registry instanceof SchemaRegistry)) {
    throw new NozzleError("ConfigurationError", "Movement transfer requires a schema registry.")
  }
  const tables = input.registry
    .tables()
    .filter((table) => table.classification === "sharded")
    .map((table) => ({
      columns: table.columns.map((column) => column.dbName),
      primaryColumns: table.primaryColumns.map((column) => column.dbName),
      tableName: table.tableName,
    }))
  return generateMovementTransferSql({ tables })
}
