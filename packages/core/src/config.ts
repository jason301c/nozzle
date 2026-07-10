import { NozzleError } from "./errors.js"
import { type BucketBits, DEFAULT_BUCKET_BITS, HIGH_SCALE_BUCKET_BITS } from "./hash.js"

export type PlacementMode = "auto" | "custom" | "dedicated" | "directory" | "hash" | "time"
export type TopologyMode = "auto" | "direct" | "router"
export type PartitionKeyType = "binary" | "integer" | "string" | "uuid"

export interface NozzleConfigInput<TSchema> {
  readonly bucketBits?: BucketBits
  readonly globalTables?: readonly unknown[]
  readonly mode?: PlacementMode
  readonly partitionKey: string
  readonly partitionKeyType?: PartitionKeyType
  readonly placement?: { readonly mode: PlacementMode }
  readonly schema: TSchema
  readonly topology?: { readonly mode: TopologyMode }
}

export interface NozzleConfig<TSchema> {
  readonly bucketBits: BucketBits
  readonly globalTables: readonly unknown[]
  readonly partitionKey: string
  readonly partitionKeyType: PartitionKeyType
  readonly placement: { readonly mode: PlacementMode }
  readonly schema: TSchema
  readonly topology: { readonly mode: TopologyMode }
}

export function defineNozzle<TSchema>(input: NozzleConfigInput<TSchema>): NozzleConfig<TSchema> {
  if (!input || typeof input !== "object") {
    throw new NozzleError("ConfigurationError", "Nozzle configuration must be an object.")
  }
  if (typeof input.partitionKey !== "string" || input.partitionKey.trim().length === 0) {
    throw new NozzleError("ConfigurationError", "partitionKey must be a non-empty string.")
  }
  if (input.partitionKey.startsWith("__nozzle_") || input.partitionKey.startsWith("nozzle_")) {
    throw new NozzleError("ConfigurationError", "partitionKey uses a reserved Nozzle identifier.")
  }

  const placementMode = input.placement?.mode ?? input.mode ?? "auto"
  if (input.mode && input.placement && input.mode !== input.placement.mode) {
    throw new NozzleError("ConfigurationError", "mode and placement.mode disagree.")
  }
  const bucketBits = input.bucketBits ?? DEFAULT_BUCKET_BITS
  if (bucketBits !== DEFAULT_BUCKET_BITS && bucketBits !== HIGH_SCALE_BUCKET_BITS) {
    throw new NozzleError("ConfigurationError", "bucketBits must be 16 or 20.")
  }
  const globalTables = input.globalTables ? [...input.globalTables] : []
  if (new Set(globalTables).size !== globalTables.length) {
    throw new NozzleError("ConfigurationError", "globalTables cannot contain duplicates.")
  }

  return Object.freeze({
    bucketBits,
    globalTables: Object.freeze(globalTables),
    partitionKey: input.partitionKey,
    partitionKeyType: input.partitionKeyType ?? "string",
    placement: Object.freeze({ mode: placementMode }),
    schema: input.schema,
    topology: Object.freeze({ mode: input.topology?.mode ?? "auto" }),
  })
}
