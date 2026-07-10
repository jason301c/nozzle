import { describe, expect, it } from "vitest"
import {
  acceptMigrationShard,
  createMigrationOperation,
  migrationSucceeded,
  recordMigrationApplied,
  recordMigrationFailure,
  recordMigrationVerified,
} from "../src/migration.js"

const target = "schema-target"
const artifact = "artifact-target"

function fresh() {
  return createMigrationOperation({
    artifactChecksum: artifact,
    operationId: "migration-1",
    requiredShardIds: ["shard-b", "shard-a"],
    targetSchemaChecksum: target,
  })
}

function applyAndVerify(operation: ReturnType<typeof fresh>, shardId: string) {
  return recordMigrationVerified(
    recordMigrationApplied(acceptMigrationShard(operation, shardId), shardId, artifact),
    shardId,
    target,
  )
}

describe("fleet migration oracle", () => {
  it("succeeds only after every sealed shard verifies and callers are compatible", () => {
    const initial = fresh()
    expect(initial.requiredShardIds).toEqual(["shard-a", "shard-b"])
    const one = applyAndVerify(initial, "shard-a")
    expect(
      migrationSucceeded(one, {
        activeApplicationSupportsTarget: true,
        activeRouterSupportsTarget: true,
      }),
    ).toBe(false)
    const complete = applyAndVerify(one, "shard-b")
    expect(
      migrationSucceeded(complete, {
        activeApplicationSupportsTarget: true,
        activeRouterSupportsTarget: true,
      }),
    ).toBe(true)
    expect(
      migrationSucceeded(complete, {
        activeApplicationSupportsTarget: false,
        activeRouterSupportsTarget: true,
      }),
    ).toBe(false)
    expect(
      migrationSucceeded(complete, {
        activeApplicationSupportsTarget: true,
        activeRouterSupportsTarget: false,
      }),
    ).toBe(false)
  })

  it("persists the first halt event and stops later scheduling", () => {
    const running = acceptMigrationShard(fresh(), "shard-a")
    const failed = recordMigrationFailure(running, {
      apply: "retryable_failed",
      controlSequence: 10,
      fencingToken: 4,
      shardId: "shard-a",
    })
    expect(failed.halt).toEqual({
      controlSequence: 10,
      failedShardId: "shard-a",
      fencingToken: 4,
    })
    expect(failed.shards["shard-a"]).toEqual({
      apply: "retryable_failed",
      verification: "failed",
    })
    expect(() => acceptMigrationShard(failed, "shard-b")).toThrow("No new shard work")

    const secondFailure = recordMigrationFailure(failed, {
      apply: "blocked_failed",
      controlSequence: 11,
      fencingToken: 4,
      shardId: "shard-b",
      verification: "unknown",
    })
    expect(secondFailure.halt).toBe(failed.halt)
    expect(secondFailure.shards["shard-b"]?.verification).toBe("unknown")
  })

  it("reconciles an unknown outcome through observed immutable evidence", () => {
    const unknown = recordMigrationFailure(acceptMigrationShard(fresh(), "shard-a"), {
      apply: "unknown",
      controlSequence: 1,
      fencingToken: 1,
      shardId: "shard-a",
    })
    expect(unknown.shards["shard-a"]?.verification).toBe("unknown")
    const observed = recordMigrationApplied(unknown, "shard-a", artifact)
    const verified = recordMigrationVerified(observed, "shard-a", target)
    expect(verified.shards["shard-a"]?.verification).toBe("verified")
  })

  it("rejects incomplete or contradictory migration evidence", () => {
    const operation = fresh()
    expect(() => recordMigrationApplied(operation, "shard-a", artifact)).toThrow(
      "not running or unknown",
    )
    expect(() => recordMigrationVerified(operation, "shard-a", target)).toThrow(
      "application evidence is incomplete",
    )
    const appliedWrongLedger = recordMigrationApplied(
      acceptMigrationShard(operation, "shard-a"),
      "shard-a",
      "different-artifact",
    )
    expect(() => recordMigrationVerified(appliedWrongLedger, "shard-a", target)).toThrow(
      "application evidence is incomplete",
    )
    const applied = recordMigrationApplied(
      acceptMigrationShard(operation, "shard-a"),
      "shard-a",
      artifact,
    )
    expect(() => recordMigrationVerified(applied, "shard-a", "wrong-schema")).toThrowError(
      expect.objectContaining({ code: "SchemaDriftError" }),
    )
    expect(() =>
      acceptMigrationShard(acceptMigrationShard(operation, "shard-a"), "shard-a"),
    ).toThrow("not schedulable")
    expect(() => acceptMigrationShard(operation, "not-sealed")).toThrow("sealed migration set")
  })

  it.each([
    () =>
      createMigrationOperation({
        artifactChecksum: "",
        operationId: "x",
        requiredShardIds: ["a"],
        targetSchemaChecksum: "s",
      }),
    () =>
      createMigrationOperation({
        artifactChecksum: "a",
        operationId: "",
        requiredShardIds: ["a"],
        targetSchemaChecksum: "s",
      }),
    () =>
      createMigrationOperation({
        artifactChecksum: "a",
        operationId: "x",
        requiredShardIds: ["a"],
        targetSchemaChecksum: "",
      }),
    () =>
      createMigrationOperation({
        artifactChecksum: "a",
        operationId: "x",
        requiredShardIds: [],
        targetSchemaChecksum: "s",
      }),
    () =>
      createMigrationOperation({
        artifactChecksum: "a",
        operationId: "x",
        requiredShardIds: ["a", "a"],
        targetSchemaChecksum: "s",
      }),
    () =>
      createMigrationOperation({
        artifactChecksum: "a",
        operationId: "x",
        requiredShardIds: [""],
        targetSchemaChecksum: "s",
      }),
  ])("rejects an invalid sealed operation", (build) => {
    expect(build).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
  })

  it("validates halt fencing coordinates and evidence checksums", () => {
    const running = acceptMigrationShard(fresh(), "shard-a")
    expect(() =>
      recordMigrationFailure(running, {
        apply: "blocked_failed",
        controlSequence: 0,
        fencingToken: 1,
        shardId: "shard-a",
      }),
    ).toThrow("Control sequence")
    expect(() =>
      recordMigrationFailure(running, {
        apply: "blocked_failed",
        controlSequence: 1,
        fencingToken: 0,
        shardId: "shard-a",
      }),
    ).toThrow("Fencing token")
    expect(() => recordMigrationApplied(running, "shard-a", "")).toThrow("Ledger checksum")
    const applied = recordMigrationApplied(running, "shard-a", artifact)
    expect(() => recordMigrationVerified(applied, "shard-a", "")).toThrow(
      "Canonical schema checksum",
    )
  })
})
