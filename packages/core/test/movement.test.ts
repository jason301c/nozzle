import { describe, expect, it } from "vitest"
import {
  activateMovementDestination,
  authorizeMovementCleanup,
  authorizeMovementRecovery,
  blockMovement,
  completeMovement,
  completeMovementRollback,
  createMovementOperation,
  drainMovementTail,
  fenceMovementSource,
  type MovementOperation,
  publishMovementRoute,
  recordMovementCopyPage,
  recordMovementReplay,
  requestMovementRollback,
  startMovementCapture,
  startMovementCopy,
  startMovementQuarantine,
  startMovementReplay,
  verifyMovementRuntime,
} from "../src/index.js"

function planned(): MovementOperation {
  return createMovementOperation({
    destinationShardId: "shard-b",
    operationId: "movement-1",
    partitionDigest: "partition-digest",
    requiredTableIds: ["parents", "children"],
    sourceRouteEpoch: 7,
    sourceShardId: "shard-a",
    targetRouteEpoch: 8,
  })
}

function copying(): MovementOperation {
  return startMovementCopy(
    startMovementCapture(planned(), { schemaChecksum: "schema-a", startSequence: 10 }),
  )
}

function copied(): MovementOperation {
  let operation = copying()
  operation = recordMovementCopyPage(operation, {
    bytesCopied: 20,
    complete: true,
    expectedCursor: null,
    nextCursor: null,
    rowsCopied: 2,
    tableId: "parents",
  })
  return recordMovementCopyPage(operation, {
    bytesCopied: 30,
    complete: true,
    expectedCursor: null,
    nextCursor: null,
    rowsCopied: 3,
    tableId: "children",
  })
}

function destinationWritable(): MovementOperation {
  let operation = startMovementReplay(copied())
  operation = recordMovementReplay(operation, { fromExclusive: 10, throughInclusive: 12 })
  operation = fenceMovementSource(operation, {
    ownershipChecksum: "source-read-only",
    sourceFenceEpoch: 8,
  })
  operation = drainMovementTail(operation, {
    fromExclusive: 12,
    sourceReadOnlyVerified: true,
    tailEmptyVerified: true,
    throughInclusive: 13,
  })
  return activateMovementDestination(operation, {
    destinationDigest: "row-digest",
    destinationFenceEpoch: 8,
    destinationRowCount: 5,
    sourceDigest: "row-digest",
    sourceRowCount: 5,
  })
}

function published(): MovementOperation {
  return publishMovementRoute(destinationWritable(), {
    routeChecksum: "route-8",
    routeEpoch: 8,
  })
}

const runtimeEvidence = {
  destinationAccepts: true,
  directPathPassed: true,
  routerPathPassed: true,
  sessionTransitionPassed: true,
  sourceRejects: true,
} as const

describe("online movement protocol", () => {
  it("copies in dependency order and converges through fenced cutover and cleanup", () => {
    const created = planned()
    expect(created.requiredTableIds).toEqual(["parents", "children"])
    expect(Object.isFrozen(created.copy)).toBe(true)

    let operation = startMovementCapture(created, {
      schemaChecksum: "schema-a",
      startSequence: 10,
    })
    operation = startMovementCopy(operation)
    operation = recordMovementCopyPage(operation, {
      bytesCopied: 20,
      complete: false,
      expectedCursor: null,
      nextCursor: "parent-cursor-1",
      rowsCopied: 2,
      tableId: "parents",
    })
    operation = recordMovementCopyPage(operation, {
      bytesCopied: 0,
      complete: true,
      expectedCursor: "parent-cursor-1",
      nextCursor: null,
      rowsCopied: 0,
      tableId: "parents",
    })
    operation = recordMovementCopyPage(operation, {
      bytesCopied: 30,
      complete: true,
      expectedCursor: null,
      nextCursor: null,
      rowsCopied: 3,
      tableId: "children",
    })
    operation = startMovementReplay(operation)
    operation = recordMovementReplay(operation, { fromExclusive: 10, throughInclusive: 12 })
    expect(recordMovementReplay(operation, { fromExclusive: 12, throughInclusive: 12 })).toBe(
      operation,
    )
    operation = fenceMovementSource(operation, {
      ownershipChecksum: "source-read-only",
      sourceFenceEpoch: 8,
    })
    operation = drainMovementTail(operation, {
      fromExclusive: 12,
      sourceReadOnlyVerified: true,
      tailEmptyVerified: true,
      throughInclusive: 13,
    })
    operation = activateMovementDestination(operation, {
      destinationDigest: "row-digest",
      destinationFenceEpoch: 8,
      destinationRowCount: 5,
      sourceDigest: "row-digest",
      sourceRowCount: 5,
    })
    operation = publishMovementRoute(operation, { routeChecksum: "route-8", routeEpoch: 8 })
    operation = verifyMovementRuntime(operation, runtimeEvidence)
    operation = startMovementQuarantine(operation, {
      serverTimeMs: 1_000,
      untilServerTimeMs: 2_000,
    })
    operation = authorizeMovementCleanup(operation, {
      authorizationChecksum: "irreversible-cleanup",
      fencingToken: 4,
      serverTimeMs: 2_000,
    })
    operation = completeMovement(operation, {
      captureJournalCompacted: true,
      destinationVerified: true,
      sourceApplicationRowsDeleted: true,
      sourcePartitionFenceRetained: true,
    })
    expect(operation).toMatchObject({
      cleanupAuthorizationChecksum: "irreversible-cleanup",
      destinationDigest: "row-digest",
      destinationRowCount: 5,
      phase: "completed",
      publishedRouteChecksum: "route-8",
      tailSequence: 13,
    })
  })

  it("rejects malformed immutable movement plans", () => {
    const base = {
      destinationShardId: "shard-b",
      operationId: "movement-1",
      partitionDigest: "digest",
      requiredTableIds: ["table-a"],
      sourceRouteEpoch: 1,
      sourceShardId: "shard-a",
      targetRouteEpoch: 2,
    } as const
    const invalid = [
      { ...base, operationId: "" },
      { ...base, operationId: undefined as never },
      { ...base, partitionDigest: " " },
      { ...base, sourceShardId: "" },
      { ...base, destinationShardId: "" },
      { ...base, destinationShardId: "shard-a" },
      { ...base, sourceRouteEpoch: Number.NaN },
      { ...base, sourceRouteEpoch: -1 },
      { ...base, targetRouteEpoch: Number.NaN },
      { ...base, targetRouteEpoch: 0 },
      { ...base, targetRouteEpoch: 3 },
      { ...base, requiredTableIds: [] },
      { ...base, requiredTableIds: [""] },
      { ...base, requiredTableIds: ["table-a", "table-a"] },
    ]
    for (const input of invalid) expect(() => createMovementOperation(input)).toThrow()
  })

  it("enforces keyset cursor CAS, dependency order, progress, and safe counters", () => {
    const operation = copying()
    const page = {
      bytesCopied: 1,
      complete: false,
      expectedCursor: null,
      nextCursor: "cursor-1",
      rowsCopied: 1,
      tableId: "parents",
    } as const
    expect(() => recordMovementCopyPage(operation, { ...page, tableId: "children" })).toThrow(
      "dependency order",
    )
    expect(() => recordMovementCopyPage(operation, { ...page, expectedCursor: "wrong" })).toThrow(
      "cursor compare-and-swap",
    )
    const missingCopy = Object.freeze({ ...operation, copy: Object.freeze({}) })
    expect(() => recordMovementCopyPage(missingCopy, page)).toThrow("cursor compare-and-swap")
    expect(() =>
      recordMovementCopyPage(operation, { ...page, complete: true, nextCursor: "cursor" }),
    ).toThrow("completed copy page")
    for (const invalid of [
      { ...page, rowsCopied: 0 },
      { ...page, nextCursor: null },
      { ...page, nextCursor: "" },
      { ...page, expectedCursor: "cursor-1", nextCursor: "cursor-1" },
    ]) {
      const current =
        invalid.expectedCursor === "cursor-1" ? recordMovementCopyPage(operation, page) : operation
      expect(() => recordMovementCopyPage(current, invalid)).toThrow("incomplete copy page")
    }
    for (const value of [Number.NaN, -1]) {
      expect(() => recordMovementCopyPage(operation, { ...page, rowsCopied: value })).toThrow(
        "Copied row count",
      )
      expect(() => recordMovementCopyPage(operation, { ...page, bytesCopied: value })).toThrow(
        "Copied byte count",
      )
    }

    const maximumRows = recordMovementCopyPage(operation, {
      ...page,
      rowsCopied: Number.MAX_SAFE_INTEGER,
    })
    expect(() =>
      recordMovementCopyPage(maximumRows, {
        ...page,
        complete: true,
        expectedCursor: "cursor-1",
        nextCursor: null,
      }),
    ).toThrow("Copied rows exceeds")
    const maximumBytes = recordMovementCopyPage(operation, {
      ...page,
      bytesCopied: Number.MAX_SAFE_INTEGER,
    })
    expect(() =>
      recordMovementCopyPage(maximumBytes, {
        ...page,
        bytesCopied: 1,
        complete: true,
        expectedCursor: "cursor-1",
        nextCursor: null,
        rowsCopied: 0,
      }),
    ).toThrow("Copied bytes exceeds")

    expect(() => startMovementReplay(operation)).toThrow("Base copy must complete")
    expect(() =>
      recordMovementCopyPage(copied(), {
        ...page,
        complete: true,
        nextCursor: null,
        rowsCopied: 0,
      }),
    ).toThrow("dependency order")
  })

  it("fails closed on replay, fence, tail, and activation evidence gaps", () => {
    const replaying = startMovementReplay(copied())
    for (const value of [Number.NaN, -1]) {
      expect(() =>
        recordMovementReplay(replaying, { fromExclusive: value, throughInclusive: 10 }),
      ).toThrow()
      expect(() =>
        recordMovementReplay(replaying, { fromExclusive: 10, throughInclusive: value }),
      ).toThrow()
    }
    expect(() =>
      recordMovementReplay(replaying, { fromExclusive: 9, throughInclusive: 10 }),
    ).toThrow("watermark compare-and-swap")
    expect(() =>
      recordMovementReplay(replaying, { fromExclusive: 10, throughInclusive: 9 }),
    ).toThrow("cannot move")
    expect(() =>
      fenceMovementSource(replaying, { ownershipChecksum: "checksum", sourceFenceEpoch: 9 }),
    ).toThrow("read-only fence")

    const sourceReadOnly = fenceMovementSource(replaying, {
      ownershipChecksum: "checksum",
      sourceFenceEpoch: 8,
    })
    expect(() =>
      drainMovementTail(sourceReadOnly, {
        fromExclusive: 9,
        sourceReadOnlyVerified: true,
        tailEmptyVerified: true,
        throughInclusive: 10,
      }),
    ).toThrow("watermark compare-and-swap")
    expect(() =>
      drainMovementTail(sourceReadOnly, {
        fromExclusive: 10,
        sourceReadOnlyVerified: true,
        tailEmptyVerified: true,
        throughInclusive: 9,
      }),
    ).toThrow("cannot move")
    for (const evidence of [
      { sourceReadOnlyVerified: false, tailEmptyVerified: true },
      { sourceReadOnlyVerified: true, tailEmptyVerified: false },
    ]) {
      expect(() =>
        drainMovementTail(sourceReadOnly, {
          fromExclusive: 10,
          throughInclusive: 10,
          ...evidence,
        }),
      ).toThrow("must both be verified")
    }

    const drained = drainMovementTail(sourceReadOnly, {
      fromExclusive: 10,
      sourceReadOnlyVerified: true,
      tailEmptyVerified: true,
      throughInclusive: 10,
    })
    const activation = {
      destinationDigest: "digest",
      destinationFenceEpoch: 8,
      destinationRowCount: 5,
      sourceDigest: "digest",
      sourceRowCount: 5,
    } as const
    for (const invalid of [
      { ...activation, destinationFenceEpoch: 9 },
      { ...activation, destinationDigest: "different" },
      { ...activation, destinationRowCount: 4 },
    ]) {
      expect(() => activateMovementDestination(drained, invalid)).toThrow("does not match")
    }
  })

  it("requires exact route, runtime, quarantine, and cleanup evidence", () => {
    const writable = destinationWritable()
    expect(() => publishMovementRoute(writable, { routeChecksum: "route", routeEpoch: 9 })).toThrow(
      "route epoch",
    )
    const routePublished = published()
    for (const key of Object.keys(runtimeEvidence) as (keyof typeof runtimeEvidence)[]) {
      expect(() =>
        verifyMovementRuntime(routePublished, { ...runtimeEvidence, [key]: false }),
      ).toThrow("verification is incomplete")
    }
    const verified = verifyMovementRuntime(routePublished, runtimeEvidence)
    for (const input of [
      { serverTimeMs: Number.NaN, untilServerTimeMs: 2 },
      { serverTimeMs: -1, untilServerTimeMs: 2 },
      { serverTimeMs: 2, untilServerTimeMs: Number.NaN },
      { serverTimeMs: 2, untilServerTimeMs: -1 },
      { serverTimeMs: 2, untilServerTimeMs: 2 },
      { serverTimeMs: 2, untilServerTimeMs: 1 },
    ]) {
      expect(() => startMovementQuarantine(verified, input)).toThrow()
    }
    const quarantined = startMovementQuarantine(verified, {
      serverTimeMs: 2,
      untilServerTimeMs: 3,
    })
    for (const quarantineUntilServerTimeMs of [undefined, -1]) {
      expect(() =>
        authorizeMovementCleanup(
          { ...quarantined, quarantineUntilServerTimeMs } as MovementOperation,
          {
            authorizationChecksum: "authorization",
            fencingToken: 1,
            serverTimeMs: 3,
          },
        ),
      ).toThrow("quarantine state is malformed")
    }
    expect(() =>
      authorizeMovementCleanup(quarantined, {
        authorizationChecksum: "authorization",
        fencingToken: 1,
        serverTimeMs: 2,
      }),
    ).toThrow("has not elapsed")
    for (const input of [
      { authorizationChecksum: "", fencingToken: 1, serverTimeMs: 3 },
      { authorizationChecksum: "authorization", fencingToken: 0, serverTimeMs: 3 },
      { authorizationChecksum: "authorization", fencingToken: Number.NaN, serverTimeMs: 3 },
    ]) {
      expect(() => authorizeMovementCleanup(quarantined, input)).toThrow()
    }
    const authorized = authorizeMovementCleanup(quarantined, {
      authorizationChecksum: "authorization",
      fencingToken: 1,
      serverTimeMs: 3,
    })
    const cleanup = {
      captureJournalCompacted: true,
      destinationVerified: true,
      sourceApplicationRowsDeleted: true,
      sourcePartitionFenceRetained: true,
    } as const
    for (const key of Object.keys(cleanup) as (keyof typeof cleanup)[]) {
      expect(() => completeMovement(authorized, { ...cleanup, [key]: false })).toThrow(
        "evidence is incomplete",
      )
    }
  })

  it("rolls back only before publication and proves a single writable owner", () => {
    const rollback = requestMovementRollback(planned(), {
      destinationReadOnlyVerified: false,
      destinationWritesObserved: 0,
    })
    expect(() =>
      completeMovementRollback(rollback, {
        activeRouteEpoch: Number.NaN,
        captureDisabled: true,
        destinationQuarantined: true,
        sourceWritableVerified: true,
      }),
    ).toThrow("Active route epoch")
    const rollbackEvidence = {
      activeRouteEpoch: 7,
      captureDisabled: true,
      destinationQuarantined: true,
      sourceWritableVerified: true,
    } as const
    for (const invalid of [
      { ...rollbackEvidence, activeRouteEpoch: 8 },
      { ...rollbackEvidence, captureDisabled: false },
      { ...rollbackEvidence, destinationQuarantined: false },
      { ...rollbackEvidence, sourceWritableVerified: false },
    ]) {
      expect(() => completeMovementRollback(rollback, invalid)).toThrow("evidence is incomplete")
    }
    expect(completeMovementRollback(rollback, rollbackEvidence).phase).toBe("rolled_back")

    const writable = destinationWritable()
    expect(() =>
      requestMovementRollback(writable, {
        destinationReadOnlyVerified: false,
        destinationWritesObserved: 0,
      }),
    ).toThrow("zero writes and fencing")
    expect(() =>
      requestMovementRollback(writable, {
        destinationReadOnlyVerified: true,
        destinationWritesObserved: 1,
      }),
    ).toThrow("zero writes and fencing")
    expect(
      requestMovementRollback(writable, {
        destinationReadOnlyVerified: true,
        destinationWritesObserved: 0,
      }).phase,
    ).toBe("rollback_pending")
    expect(() =>
      requestMovementRollback(published(), {
        destinationReadOnlyVerified: true,
        destinationWritesObserved: 0,
      }),
    ).toThrow("cannot perform")
  })

  it("blocks failures and requires immutable newer fenced recovery decisions", () => {
    const operation = planned()
    const blockInput = {
      controlSequence: 1,
      errorChecksum: "copy-failure",
      fencingToken: 2,
      outcome: "unknown",
    } as const
    const blocked = blockMovement(operation, blockInput)
    expect(blocked.block?.phase).toBe("planned")
    expect(blockMovement(blocked, blockInput)).toBe(blocked)
    expect(() =>
      startMovementCapture(blocked, { schemaChecksum: "schema", startSequence: 0 }),
    ).toThrow("requires a newer fenced recovery")
    expect(() =>
      authorizeMovementRecovery(operation, { decisionChecksum: "d", fencingToken: 3 }),
    ).toThrow("without a block")
    expect(() =>
      authorizeMovementRecovery(blocked, { decisionChecksum: "d", fencingToken: 2 }),
    ).toThrow("newer controller")
    const recovered = authorizeMovementRecovery(blocked, {
      decisionChecksum: "recover-forward",
      fencingToken: 3,
    })
    expect(
      authorizeMovementRecovery(recovered, {
        decisionChecksum: "recover-forward",
        fencingToken: 3,
      }),
    ).toBe(recovered)
    expect(() =>
      authorizeMovementRecovery(recovered, { decisionChecksum: "different", fencingToken: 3 }),
    ).toThrow("decision is immutable")
    expect(() =>
      authorizeMovementRecovery(recovered, {
        decisionChecksum: "recover-forward",
        fencingToken: 4,
      }),
    ).toThrow("decision is immutable")
    expect(
      startMovementCapture(recovered, { schemaChecksum: "schema", startSequence: 0 }).phase,
    ).toBe("capturing")

    for (const replacement of [
      { ...blockInput, errorChecksum: "different" },
      { ...blockInput, fencingToken: 3 },
      { ...blockInput, outcome: "permanent" as const },
    ]) {
      expect(() => blockMovement(blocked, replacement)).toThrow("newer control sequence")
    }
    const reblocked = blockMovement(recovered, {
      ...blockInput,
      controlSequence: 2,
      errorChecksum: "second-failure",
      fencingToken: 3,
    })
    expect(reblocked.recovery).toBeUndefined()
    expect(() => blockMovement(reblocked, { ...blockInput, controlSequence: 1 })).toThrow(
      "newer control sequence",
    )

    for (const input of [
      { ...blockInput, errorChecksum: "" },
      { ...blockInput, controlSequence: 0 },
      { ...blockInput, controlSequence: Number.NaN },
      { ...blockInput, fencingToken: 0 },
    ]) {
      expect(() => blockMovement(operation, input)).toThrow()
    }

    const completed = completeMovement(
      authorizeMovementCleanup(
        startMovementQuarantine(verifyMovementRuntime(published(), runtimeEvidence), {
          serverTimeMs: 1,
          untilServerTimeMs: 2,
        }),
        { authorizationChecksum: "auth", fencingToken: 1, serverTimeMs: 2 },
      ),
      {
        captureJournalCompacted: true,
        destinationVerified: true,
        sourceApplicationRowsDeleted: true,
        sourcePartitionFenceRetained: true,
      },
    )
    const rolledBack = completeMovementRollback(
      requestMovementRollback(planned(), {
        destinationReadOnlyVerified: false,
        destinationWritesObserved: 0,
      }),
      {
        activeRouteEpoch: 7,
        captureDisabled: true,
        destinationQuarantined: true,
        sourceWritableVerified: true,
      },
    )
    expect(() => blockMovement(completed, blockInput)).toThrow("terminal movement")
    expect(() => blockMovement(rolledBack, blockInput)).toThrow("terminal movement")
  })

  it("rejects transitions from the wrong phase and malformed stage evidence", () => {
    expect(() => startMovementCopy(planned())).toThrow("cannot perform")
    expect(() => startMovementCapture(planned(), { schemaChecksum: "", startSequence: 0 })).toThrow(
      "Capture schema checksum",
    )
    expect(() =>
      startMovementCapture(planned(), { schemaChecksum: "schema", startSequence: Number.NaN }),
    ).toThrow("Capture start sequence")
    expect(() =>
      fenceMovementSource(startMovementReplay(copied()), {
        ownershipChecksum: "",
        sourceFenceEpoch: 8,
      }),
    ).toThrow("Source ownership checksum")
    const drained = drainMovementTail(
      fenceMovementSource(startMovementReplay(copied()), {
        ownershipChecksum: "checksum",
        sourceFenceEpoch: 8,
      }),
      {
        fromExclusive: 10,
        sourceReadOnlyVerified: true,
        tailEmptyVerified: true,
        throughInclusive: 10,
      },
    )
    for (const input of [
      {
        destinationDigest: "",
        destinationFenceEpoch: 8,
        destinationRowCount: 0,
        sourceDigest: "digest",
        sourceRowCount: 0,
      },
      {
        destinationDigest: "digest",
        destinationFenceEpoch: 8,
        destinationRowCount: 0,
        sourceDigest: "",
        sourceRowCount: 0,
      },
      {
        destinationDigest: "digest",
        destinationFenceEpoch: 8,
        destinationRowCount: -1,
        sourceDigest: "digest",
        sourceRowCount: 0,
      },
      {
        destinationDigest: "digest",
        destinationFenceEpoch: 8,
        destinationRowCount: 0,
        sourceDigest: "digest",
        sourceRowCount: -1,
      },
    ]) {
      expect(() => activateMovementDestination(drained, input)).toThrow()
    }
    expect(() =>
      publishMovementRoute(destinationWritable(), { routeChecksum: "", routeEpoch: 8 }),
    ).toThrow("Route checksum")
    expect(() =>
      requestMovementRollback(planned(), {
        destinationReadOnlyVerified: false,
        destinationWritesObserved: -1,
      }),
    ).toThrow("Observed destination writes")
    expect(() =>
      authorizeMovementRecovery(
        blockMovement(planned(), {
          controlSequence: 1,
          errorChecksum: "error",
          fencingToken: 1,
          outcome: "permanent",
        }),
        { decisionChecksum: "", fencingToken: 2 },
      ),
    ).toThrow("Recovery decision checksum")
  })
})
