import { NozzleError } from "./errors.js"

export interface PlacementLoad {
  readonly query: number
  readonly storage: number
  readonly write: number
}

export interface PlacementShard {
  readonly jurisdiction: string
  readonly location?: string
  readonly load: PlacementLoad
  readonly schemaCompatible: boolean
  readonly shardId: string
  readonly state: "active" | "quarantined" | "retired"
}

export interface PlacementPolicy {
  readonly locationPenalty: number
  readonly movementPenalty: number
  readonly stopPlacementAt: number
  readonly targetOccupancy: number
}

export interface PlacementScore {
  readonly locationPenalty: number
  readonly maximumLoad: number
  readonly movementPenalty: number
  readonly score: number
  readonly shardId: string
}

export interface PlacementDecision {
  readonly candidates: readonly PlacementScore[]
  readonly selected: PlacementScore
}

const DEFAULT_POLICY: PlacementPolicy = Object.freeze({
  locationPenalty: 0.02,
  movementPenalty: 0.05,
  stopPlacementAt: 0.8,
  targetOccupancy: 0.6,
})

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function finiteFraction(value: number, label: string): void {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    configuration(`${label} must be a finite fraction in the supported range.`)
  }
}

function nonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function validateLoad(load: PlacementLoad, label: string): void {
  if (typeof load !== "object" || load === null) configuration(`${label} is required.`)
  finiteFraction(load.query, `${label} query load`)
  finiteFraction(load.storage, `${label} storage load`)
  finiteFraction(load.write, `${label} write load`)
}

function validatePolicy(policy: PlacementPolicy): void {
  finiteFraction(policy.targetOccupancy, "Target occupancy")
  finiteFraction(policy.stopPlacementAt, "Stop-placement threshold")
  if (policy.targetOccupancy >= policy.stopPlacementAt) {
    configuration("Target occupancy must be below the stop-placement threshold.")
  }
  finiteFraction(policy.locationPenalty, "Location penalty")
  finiteFraction(policy.movementPenalty, "Movement penalty")
}

function projectedLoad(current: PlacementLoad, addition: PlacementLoad): PlacementLoad {
  return Object.freeze({
    query: current.query + addition.query,
    storage: current.storage + addition.storage,
    write: current.write + addition.write,
  })
}

function maximumLoad(load: PlacementLoad): number {
  return Math.max(load.query, load.storage, load.write)
}

function compare(left: PlacementScore, right: PlacementScore): number {
  if (left.score !== right.score) return left.score - right.score
  if (left.maximumLoad !== right.maximumLoad) return left.maximumLoad - right.maximumLoad
  return left.shardId < right.shardId ? -1 : 1
}

export function planPlacement(input: {
  readonly additionalLoad: PlacementLoad
  readonly currentShardId?: string
  readonly permittedJurisdictions: readonly string[]
  readonly policy?: PlacementPolicy
  readonly preferredLocations?: readonly string[]
  readonly shards: readonly PlacementShard[]
}): PlacementDecision {
  validateLoad(input.additionalLoad, "Additional load")
  const policy = input.policy ?? DEFAULT_POLICY
  validatePolicy(policy)
  if (!Array.isArray(input.shards) || input.shards.length === 0) {
    throw new NozzleError("CapacityGuardError", "Placement requires at least one observed shard.")
  }
  if (!Array.isArray(input.permittedJurisdictions) || input.permittedJurisdictions.length === 0) {
    configuration("At least one permitted jurisdiction is required.")
  }
  const jurisdictions = new Set<string>()
  for (const jurisdiction of input.permittedJurisdictions) {
    nonEmpty(jurisdiction, "Permitted jurisdiction")
    if (jurisdictions.has(jurisdiction)) configuration("Permitted jurisdictions must be unique.")
    jurisdictions.add(jurisdiction)
  }
  const locations = new Set<string>()
  for (const location of input.preferredLocations ?? []) {
    nonEmpty(location, "Preferred location")
    if (locations.has(location)) configuration("Preferred locations must be unique.")
    locations.add(location)
  }
  if (input.currentShardId !== undefined) nonEmpty(input.currentShardId, "Current shard ID")

  const shardIds = new Set<string>()
  let jurisdictionMatch = false
  const candidates: PlacementScore[] = []
  for (const shard of input.shards) {
    nonEmpty(shard.shardId, "Shard ID")
    nonEmpty(shard.jurisdiction, "Shard jurisdiction")
    if (shardIds.has(shard.shardId)) configuration("Shard IDs must be unique.")
    shardIds.add(shard.shardId)
    validateLoad(shard.load, `Shard ${shard.shardId}`)
    if (!(["active", "quarantined", "retired"] as const).includes(shard.state)) {
      configuration("Shard state is invalid.")
    }
    if (!jurisdictions.has(shard.jurisdiction)) continue
    jurisdictionMatch = true
    if (shard.state !== "active" || !shard.schemaCompatible) continue
    const projected = projectedLoad(shard.load, input.additionalLoad)
    const maximum = maximumLoad(projected)
    if (maximum >= policy.stopPlacementAt) continue
    const movePenalty =
      input.currentShardId === undefined || input.currentShardId === shard.shardId
        ? 0
        : policy.movementPenalty
    const locationPenalty =
      locations.size === 0 || (shard.location !== undefined && locations.has(shard.location))
        ? 0
        : policy.locationPenalty
    const aboveTarget = Math.max(0, maximum - policy.targetOccupancy)
    candidates.push(
      Object.freeze({
        locationPenalty,
        maximumLoad: maximum,
        movementPenalty: movePenalty,
        score: maximum + aboveTarget * 2 + movePenalty + locationPenalty,
        shardId: shard.shardId,
      }),
    )
  }
  candidates.sort(compare)
  const selected = candidates[0]
  if (!selected) {
    if (!jurisdictionMatch) {
      throw new NozzleError(
        "JurisdictionViolationError",
        "No observed shard satisfies the permitted jurisdictions.",
      )
    }
    throw new NozzleError(
      "CapacityGuardError",
      "No permitted active schema-compatible shard has placement headroom.",
    )
  }
  return Object.freeze({ candidates: Object.freeze(candidates), selected })
}
