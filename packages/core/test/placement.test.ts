import { describe, expect, it } from "vitest"
import { type PlacementShard, planPlacement } from "../src/placement.js"

const additionalLoad = { query: 0.05, storage: 0.05, write: 0.05 } as const

function shard(overrides: Partial<PlacementShard> = {}): PlacementShard {
  return {
    jurisdiction: "eu",
    load: { query: 0.2, storage: 0.2, write: 0.2 },
    location: "weur",
    schemaCompatible: true,
    shardId: "shard-a",
    state: "active",
    ...overrides,
  }
}

describe("deterministic constrained placement", () => {
  it("filters hard constraints and emits a stable readable score order", () => {
    const decision = planPlacement({
      additionalLoad,
      currentShardId: "shard-b",
      permittedJurisdictions: ["eu"],
      preferredLocations: ["weur"],
      shards: [
        shard({ shardId: "shard-z", state: "quarantined" }),
        shard({ jurisdiction: "fedramp", shardId: "shard-fed" }),
        shard({ schemaCompatible: false, shardId: "shard-schema" }),
        shard({ load: { query: 0.76, storage: 0.2, write: 0.2 }, shardId: "shard-full" }),
        shard({ location: "eeur", shardId: "shard-c" }),
        shard({ shardId: "shard-b" }),
        shard({ shardId: "shard-a" }),
      ],
    })
    expect(decision.selected).toMatchObject({
      locationPenalty: 0,
      movementPenalty: 0,
      shardId: "shard-b",
    })
    expect(decision.candidates.map((candidate) => candidate.shardId)).toEqual([
      "shard-b",
      "shard-a",
      "shard-c",
    ])
    expect(Object.isFrozen(decision.candidates)).toBe(true)
  })

  it("uses load, penalties, and shard ID as deterministic tie breakers", () => {
    const decision = planPlacement({
      additionalLoad: { query: 0, storage: 0, write: 0 },
      permittedJurisdictions: ["eu"],
      policy: {
        locationPenalty: 0,
        movementPenalty: 0,
        stopPlacementAt: 0.9,
        targetOccupancy: 0.6,
      },
      shards: [
        shard({ load: { query: 0.3, storage: 0.2, write: 0.2 }, shardId: "same-b" }),
        shard({ load: { query: 0.3, storage: 0.2, write: 0.2 }, shardId: "same-a" }),
        shard({ load: { query: 0.4, storage: 0.2, write: 0.2 }, shardId: "higher" }),
      ],
    })
    expect(decision.candidates.map(({ shardId }) => shardId)).toEqual([
      "same-a",
      "same-b",
      "higher",
    ])
    expect(
      planPlacement({
        additionalLoad: { query: 0, storage: 0, write: 0 },
        permittedJurisdictions: ["eu"],
        shards: [shard({ shardId: "same-a" }), shard({ shardId: "same-b" })],
      }).candidates.map(({ shardId }) => shardId),
    ).toEqual(["same-a", "same-b"])
    expect(
      planPlacement({
        additionalLoad: { query: 0, storage: 0, write: 0 },
        currentShardId: "current",
        permittedJurisdictions: ["eu"],
        policy: {
          locationPenalty: 0,
          movementPenalty: 0.05,
          stopPlacementAt: 0.9,
          targetOccupancy: 0.6,
        },
        shards: [
          shard({ load: { query: 0.3, storage: 0.2, write: 0.2 }, shardId: "current" }),
          shard({ load: { query: 0.25, storage: 0.2, write: 0.2 }, shardId: "move" }),
        ],
      }).selected.shardId,
    ).toBe("move")
  })

  it("distinguishes jurisdiction failure from capacity and compatibility failure", () => {
    expect(() =>
      planPlacement({
        additionalLoad,
        permittedJurisdictions: ["fedramp"],
        shards: [shard()],
      }),
    ).toThrowError(expect.objectContaining({ code: "JurisdictionViolationError" }))
    for (const shards of [
      [shard({ schemaCompatible: false })],
      [shard({ state: "retired" })],
      [shard({ load: { query: 0.79, storage: 0.2, write: 0.2 } })],
    ]) {
      expect(() =>
        planPlacement({ additionalLoad, permittedJurisdictions: ["eu"], shards }),
      ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
    }
  })

  it("rejects malformed policy, load, shard, and preference inputs", () => {
    const base = {
      additionalLoad,
      permittedJurisdictions: ["eu"] as readonly string[],
      shards: [shard()] as readonly PlacementShard[],
    }
    const invalid: unknown[] = [
      { ...base, additionalLoad: null },
      { ...base, additionalLoad: { ...additionalLoad, query: Number.NaN } },
      { ...base, additionalLoad: { ...additionalLoad, query: -1 } },
      { ...base, additionalLoad: { ...additionalLoad, query: 2 } },
      { ...base, shards: [] },
      { ...base, permittedJurisdictions: [] },
      { ...base, permittedJurisdictions: [""] },
      { ...base, permittedJurisdictions: ["eu", "eu"] },
      { ...base, preferredLocations: [""] },
      { ...base, preferredLocations: ["weur", "weur"] },
      { ...base, currentShardId: "" },
      { ...base, shards: [shard({ shardId: "" })] },
      { ...base, shards: [shard({ jurisdiction: "" })] },
      { ...base, shards: [shard(), shard()] },
      { ...base, shards: [shard({ state: "bad" as never })] },
      {
        ...base,
        policy: {
          locationPenalty: 0,
          movementPenalty: 0,
          stopPlacementAt: 0.6,
          targetOccupancy: 0.6,
        },
      },
      {
        ...base,
        policy: {
          locationPenalty: Number.NaN,
          movementPenalty: 0,
          stopPlacementAt: 0.8,
          targetOccupancy: 0.6,
        },
      },
      {
        ...base,
        policy: {
          locationPenalty: 0,
          movementPenalty: -1,
          stopPlacementAt: 0.8,
          targetOccupancy: 0.6,
        },
      },
    ]
    for (const input of invalid) {
      expect(() => planPlacement(input as Parameters<typeof planPlacement>[0])).toThrow()
    }
  })

  it("rejects non-finite projected capacity", () => {
    expect(() =>
      planPlacement({
        additionalLoad: { query: 1, storage: 1, write: 1 },
        permittedJurisdictions: ["eu"],
        shards: [shard({ load: { query: 1, storage: 1, write: 1 } })],
      }),
    ).toThrowError(expect.objectContaining({ code: "CapacityGuardError" }))
  })
})
