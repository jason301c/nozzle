import { NozzleError } from "@nozzle/core"
import { type AnyColumn, getTableColumns, getTableName, isTable } from "drizzle-orm"
import { type AnySQLiteTable, getTableConfig, type SQLiteColumn } from "drizzle-orm/sqlite-core"

export type TableClass = "global" | "sharded"
export type SQLiteStorageType = "blob" | "integer" | "numeric" | "real" | "text"

export interface ColumnMetadata {
  readonly column: SQLiteColumn
  readonly dataType: string
  readonly dbName: string
  readonly notNull: boolean
  readonly primary: boolean
  readonly propertyName: string
  readonly sqlType: string
  readonly storageType: SQLiteStorageType
}

export interface TableMetadata<TTable extends AnySQLiteTable = AnySQLiteTable> {
  readonly classification: TableClass
  readonly columns: readonly ColumnMetadata[]
  readonly partitionColumn?: ColumnMetadata
  readonly primaryColumns: readonly ColumnMetadata[]
  readonly table: TTable
  readonly tableName: string
}

export interface CompileSchemaOptions<TSchema extends Record<string, unknown>> {
  readonly globalTables?: readonly AnySQLiteTable[]
  readonly partitionKey: string
  readonly schema: TSchema
}

function sqliteIdentifierKey(identifier: string): string {
  return identifier.replace(/[A-Z]/gu, (character) => character.toLowerCase())
}

function reservedIdentifier(identifier: string): boolean {
  const key = sqliteIdentifierKey(identifier)
  return key.startsWith("__nozzle_") || key.startsWith("nozzle_")
}

function sqliteStorageType(sqlType: string): SQLiteStorageType {
  const type = sqlType.toUpperCase()
  if (type.includes("INT")) return "integer"
  if (type.includes("CHAR") || type.includes("CLOB") || type.includes("TEXT")) return "text"
  if (type.includes("BLOB") || type.length === 0) return "blob"
  if (type.includes("REAL") || type.includes("FLOA") || type.includes("DOUB")) return "real"
  return "numeric"
}

export class SchemaRegistry<TSchema extends Record<string, unknown> = Record<string, unknown>> {
  readonly #columns = new WeakMap<object, ColumnMetadata>()
  readonly #tables = new WeakMap<object, TableMetadata>()
  readonly #tablesByName = new Map<string, TableMetadata>()
  readonly partitionKey: string
  readonly schema: TSchema

  constructor(options: CompileSchemaOptions<TSchema>) {
    if (typeof options.partitionKey !== "string" || options.partitionKey.trim().length === 0) {
      throw new NozzleError("ConfigurationError", "The partition-key property cannot be empty.")
    }
    this.partitionKey = options.partitionKey
    this.schema = options.schema
    const globalTableInput = options.globalTables ?? []
    const globalTables = new Set(globalTableInput)
    if (globalTables.size !== globalTableInput.length) {
      throw new NozzleError("ConfigurationError", "Global table declarations cannot be duplicated.")
    }
    const discovered = Object.values(options.schema).filter(isTable)
    if (discovered.length === 0) {
      throw new NozzleError("ConfigurationError", "The Drizzle schema does not contain any tables.")
    }
    for (const value of globalTables) {
      if (!discovered.includes(value)) {
        throw new NozzleError("ConfigurationError", "A global table is not present in the schema.")
      }
    }

    const tableNames = new Set<string>()
    for (const table of discovered) {
      const sqliteTable = table as AnySQLiteTable
      const tableName = getTableName(sqliteTable)
      const tableNameKey = sqliteIdentifierKey(tableName)
      if (tableNames.has(tableNameKey)) {
        throw new NozzleError(
          "ConfigurationError",
          "The Drizzle schema cannot declare the same SQL table name more than once.",
          { details: { tableName } },
        )
      }
      tableNames.add(tableNameKey)
      if (reservedIdentifier(tableName)) {
        throw new NozzleError("ConfigurationError", "Application tables cannot use Nozzle names.", {
          details: { tableName },
        })
      }
      const columnsRecord = getTableColumns(sqliteTable)
      const columnNames = new Set<string>()
      const columns = Object.entries(columnsRecord).map(([propertyName, column]) => {
        if (reservedIdentifier(propertyName) || reservedIdentifier(column.name)) {
          throw new NozzleError(
            "ConfigurationError",
            "Application columns cannot use Nozzle names.",
            {
              details: { column: propertyName, tableName },
            },
          )
        }
        const columnNameKey = sqliteIdentifierKey(column.name)
        if (columnNames.has(columnNameKey)) {
          throw new NozzleError(
            "ConfigurationError",
            "A table cannot map two properties to the same SQL column name.",
            { details: { column: column.name, tableName } },
          )
        }
        columnNames.add(columnNameKey)
        const sqlType = column.getSQLType()
        const metadata: ColumnMetadata = Object.freeze({
          column: column as SQLiteColumn,
          dataType: column.dataType,
          dbName: column.name,
          notNull: column.notNull,
          primary: column.primary,
          propertyName,
          sqlType,
          storageType: sqliteStorageType(sqlType),
        })
        this.#columns.set(column, metadata)
        return metadata
      })
      const config = getTableConfig(sqliteTable)
      const primarySet = new Set<AnyColumn>([
        ...config.columns.filter((column) => column.primary),
        ...config.primaryKeys.flatMap((primaryKey) => primaryKey.columns),
      ])
      const primaryColumns = columns.filter((metadata) => primarySet.has(metadata.column))
      const classification: TableClass = globalTables.has(sqliteTable) ? "global" : "sharded"
      if (classification === "sharded" && primaryColumns.length === 0) {
        throw new NozzleError(
          "ConfigurationError",
          "Every sharded Nozzle table needs an explicit primary key.",
          {
            details: { tableName },
          },
        )
      }
      if (classification === "sharded" && primaryColumns.some((column) => !column.notNull)) {
        throw new NozzleError(
          "ConfigurationError",
          "Every sharded primary-key column must be explicitly non-null.",
          { details: { tableName } },
        )
      }
      if (
        classification === "sharded" &&
        primaryColumns.some((column) => column.storageType === "numeric")
      ) {
        throw new NozzleError(
          "ConfigurationError",
          "Sharded primary-key columns require a deterministic SQLite storage affinity.",
          { details: { tableName } },
        )
      }
      const partitionColumn = columns.find((column) => column.propertyName === options.partitionKey)
      if (classification === "sharded" && !partitionColumn) {
        throw new NozzleError(
          "PartitionKeyMissingError",
          "A sharded table is missing the partition key.",
          {
            details: { partitionKey: options.partitionKey, tableName },
          },
        )
      }
      if (classification === "sharded" && !partitionColumn?.notNull) {
        throw new NozzleError("ConfigurationError", "A sharded partition key must be non-null.", {
          details: { partitionKey: options.partitionKey, tableName },
        })
      }

      const metadata = Object.freeze({
        classification,
        columns: Object.freeze(columns),
        ...(partitionColumn ? { partitionColumn } : {}),
        primaryColumns: Object.freeze(primaryColumns),
        table: sqliteTable,
        tableName,
      })
      this.#tables.set(sqliteTable, metadata)
      this.#tablesByName.set(tableName, metadata)
    }
  }

  table<TTable extends AnySQLiteTable>(table: TTable): TableMetadata<TTable> {
    const metadata = this.#tables.get(table)
    if (!metadata) {
      throw new NozzleError(
        "ConfigurationError",
        "The table is not registered in this Nozzle schema.",
      )
    }
    return metadata as TableMetadata<TTable>
  }

  tableByName(tableName: string): TableMetadata {
    const metadata = this.#tablesByName.get(tableName)
    if (!metadata) {
      throw new NozzleError("UnsafeQueryRequiredError", "The plan uses an unregistered table name.")
    }
    return metadata
  }

  tables(): readonly TableMetadata[] {
    return Object.freeze(
      [...this.#tablesByName.keys()]
        .sort()
        .map((tableName) => this.#tablesByName.get(tableName) as TableMetadata),
    )
  }

  column(column: AnyColumn): ColumnMetadata {
    const metadata = this.#columns.get(column)
    if (!metadata) {
      throw new NozzleError(
        "UnsafeQueryRequiredError",
        "The predicate uses an unregistered column.",
      )
    }
    return metadata
  }
}
