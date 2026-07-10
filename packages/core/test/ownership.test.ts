import fc from "fast-check"
import { describe, expect, it } from "vitest"
import { OwnershipModel, assertWriteAuthorized, type OwnershipRecord } from "../src/ownership.js"

function makeWritable(
  model: OwnershipModel,
  bucketId: number,
  shardId: string,
  epoch: number,
): void {
  model.transition({
    bucketId,
    shardId,
    operationId: `provision-${shardId}`,
    from: "unassigned",
    to: "writable",
    movementRole: "none",
    routeEpoch: epoch,
  })
}

function prepareDestination(
  model: OwnershipModel,
  bucketId: number,
  shardId: string,
  epoch: number,
): void {
  const operationId = `move-${bucketId}`
  model.transition({
    bucketId,
    shardId,
    operationId,
    from: "unassigned",
    to: "preparing",
    movementRole: "destination",
    routeEpoch: epoch,
  })
  model.transition({
    bucketId,
    shardId,
    operationId,
    from: "preparing",
    to: "copying",
    movementRole: "destination",
    routeEpoch: epoch,
  })
  model.transition({
    bucketId,
    shardId,
    operationId,
    from: "copying",
    to: "catching_up",
    movementRole: "destination",
    routeEpoch: epoch,
  })
}

describe("OwnershipModel", () => {
  it("performs a fenced cutover with one writable owner", () => {
    const model = new OwnershipModel()
    makeWritable(model, 7, "source", 1)
    prepareDestination(model, 7, "destination", 1)

    model.transition({
      bucketId: 7,
      shardId: "source",
      operationId: "move-7",
      from: "writable",
      to: "read_only",
      movementRole: "source",
      routeEpoch: 1,
    })
    const destination = model.transition({
      bucketId: 7,
      shardId: "destination",
      operationId: "move-7",
      from: "catching_up",
      to: "writable",
      movementRole: "destination",
      routeEpoch: 2,
    })

    expect(model.records().filter((record) => record.state === "writable")).toEqual([destination])
    expect(() => assertWriteAuthorized(model.get(7, "source"), 1)).toThrowError(
      expect.objectContaining({ code: "StaleRouteRejectedError" }),
    )
    expect(() => assertWriteAuthorized(destination, 1)).toThrow("route epoch is stale")
    expect(() => assertWriteAuthorized(destination, 2)).not.toThrow()
  })

  it("rejects a second writable owner before mutation", () => {
    const model = new OwnershipModel()
    makeWritable(model, 1, "source", 1)
    prepareDestination(model, 1, "destination", 1)
    expect(() =>
      model.transition({
        bucketId: 1,
        shardId: "destination",
        operationId: "move-1",
        from: "catching_up",
        to: "writable",
        movementRole: "destination",
        routeEpoch: 2,
      }),
    ).toThrow("two writable owners")
    expect(model.get(1, "destination")?.state).toBe("catching_up")
  })

  it("makes exact duplicate transitions idempotent", () => {
    const model = new OwnershipModel()
    makeWritable(model, 1, "a", 1)
    const transition = {
      bucketId: 1,
      shardId: "a",
      operationId: "move-a",
      from: "writable" as const,
      to: "read_only" as const,
      movementRole: "source" as const,
      routeEpoch: 1,
    }
    const first = model.transition(transition)
    const second = model.transition({ ...transition, from: "read_only", to: "read_only" })
    expect(second).toBe(first)
    expect(() =>
      model.transition({ ...transition, from: "read_only", to: "read_only", routeEpoch: 2 }),
    ).toThrow("duplicate transition did not match")
  })

  it("rejects illegal, stale, and mismatched transitions", () => {
    const model = new OwnershipModel()
    makeWritable(model, 2, "a", 2)
    expect(() =>
      model.transition({
        bucketId: 2,
        shardId: "a",
        operationId: "x",
        from: "copying",
        to: "catching_up",
        movementRole: "none",
        routeEpoch: 2,
      }),
    ).toThrow("precondition failed")
    expect(() =>
      model.transition({
        bucketId: 2,
        shardId: "a",
        operationId: "x",
        from: "writable",
        to: "read_only",
        movementRole: "source",
        routeEpoch: 1,
      }),
    ).toThrow("cannot decrease")
    model.transition({
      bucketId: 2,
      shardId: "a",
      operationId: "x",
      from: "writable",
      to: "read_only",
      movementRole: "source",
      routeEpoch: 2,
    })
    expect(() =>
      model.transition({
        bucketId: 2,
        shardId: "a",
        operationId: "x",
        from: "read_only",
        to: "writable",
        movementRole: "source",
        routeEpoch: 2,
      }),
    ).toThrow("must advance the bucket route epoch")
    expect(() =>
      model.transition({
        bucketId: 3,
        shardId: "b",
        operationId: "x",
        from: "unassigned",
        to: "copying",
        movementRole: "destination",
        routeEpoch: 1,
      }),
    ).toThrow("Illegal ownership transition")
  })

  it("validates records, identity, epochs, sorting, and terminal states", () => {
    const duplicate: OwnershipRecord = {
      bucketId: 1,
      shardId: "a",
      operationId: "x",
      routeEpoch: 1,
      state: "writable",
      movementRole: "none",
    }
    expect(() => new OwnershipModel([duplicate, duplicate])).toThrow("Duplicate ownership record")
    expect(() => new OwnershipModel([duplicate, { ...duplicate, shardId: "b" }])).toThrow(
      "two writable owners",
    )
    expect(() => new OwnershipModel([{ ...duplicate, bucketId: -1 }])).toThrow(
      "non-negative safe integers",
    )
    expect(() => new OwnershipModel([{ ...duplicate, shardId: "" }])).toThrow("must be non-empty")

    const model = new OwnershipModel()
    makeWritable(model, 10, "z", 1)
    makeWritable(model, 2, "a", 1)
    expect(model.records().map((record) => record.bucketId)).toEqual([2, 10])
    expect(() =>
      model.transition({
        bucketId: 2,
        shardId: "a",
        operationId: "x",
        from: "writable",
        to: "read_only",
        movementRole: "source",
        routeEpoch: -1,
      }),
    ).toThrow("non-negative integers")
  })

  it("rejects absent write authorization", () => {
    expect(() => assertWriteAuthorized(undefined, 1)).toThrow("not the writable owner")
  })

  it("preserves the single-writer invariant for arbitrary distinct shard IDs", () => {
    fc.assert(
      fc.property(
        fc.nat({ max: 65_535 }),
        fc.string({ minLength: 1 }),
        fc.string({ minLength: 1 }),
        (bucketId, left, right) => {
          fc.pre(left !== right)
          const model = new OwnershipModel()
          makeWritable(model, bucketId, left, 1)
          expect(() => makeWritable(model, bucketId, right, 2)).toThrow()
          expect(model.records().filter((record) => record.state === "writable")).toHaveLength(1)
        },
      ),
      { numRuns: 200 },
    )
  })
})
