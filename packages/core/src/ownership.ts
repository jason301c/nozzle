import { NozzleError } from "./errors.js"

export const OWNERSHIP_STATES = [
  "unassigned",
  "preparing",
  "copying",
  "catching_up",
  "read_only",
  "writable",
  "quarantined",
  "retired",
  "intervention_required",
] as const

export type OwnershipState = (typeof OWNERSHIP_STATES)[number]
export type MovementRole = "destination" | "none" | "source"

export interface OwnershipRecord {
  readonly bucketId: number
  readonly movementRole: MovementRole
  readonly operationId: string
  readonly routeEpoch: number
  readonly shardId: string
  readonly state: OwnershipState
}

export interface OwnershipTransition {
  readonly bucketId: number
  readonly from: OwnershipState
  readonly movementRole: MovementRole
  readonly operationId: string
  readonly routeEpoch: number
  readonly shardId: string
  readonly to: OwnershipState
}

const ALLOWED_TRANSITIONS: Readonly<Record<OwnershipState, ReadonlySet<OwnershipState>>> = {
  catching_up: new Set(["writable", "quarantined", "intervention_required"]),
  copying: new Set(["catching_up", "quarantined", "intervention_required"]),
  intervention_required: new Set(["preparing", "retired"]),
  preparing: new Set(["copying", "quarantined", "retired", "intervention_required"]),
  quarantined: new Set(["preparing", "retired", "intervention_required"]),
  read_only: new Set(["writable", "quarantined", "intervention_required"]),
  retired: new Set(),
  unassigned: new Set(["preparing", "writable", "retired"]),
  writable: new Set(["read_only", "quarantined", "intervention_required"]),
}

function recordKey(bucketId: number, shardId: string): string {
  return `${bucketId}:${shardId.length}:${shardId}`
}

function assertIdentity(bucketId: number, shardId: string, operationId: string): void {
  if (!Number.isSafeInteger(bucketId) || bucketId < 0) {
    throw new NozzleError("ConfigurationError", "Bucket IDs must be non-negative safe integers.")
  }
  if (shardId.length === 0 || operationId.length === 0) {
    throw new NozzleError("ConfigurationError", "Shard and operation IDs must be non-empty.")
  }
}

export class OwnershipModel {
  readonly #records: Map<string, OwnershipRecord>

  constructor(records: readonly OwnershipRecord[] = []) {
    this.#records = new Map()
    for (const record of records) {
      assertIdentity(record.bucketId, record.shardId, record.operationId)
      const key = recordKey(record.bucketId, record.shardId)
      if (this.#records.has(key)) {
        throw new NozzleError("RouteVersionConflictError", "Duplicate ownership record.")
      }
      this.#records.set(key, Object.freeze({ ...record }))
    }
    this.assertInvariants()
  }

  records(): readonly OwnershipRecord[] {
    return [...this.#records.values()].sort(
      (left, right) => left.bucketId - right.bucketId || left.shardId.localeCompare(right.shardId),
    )
  }

  get(bucketId: number, shardId: string): OwnershipRecord | undefined {
    return this.#records.get(recordKey(bucketId, shardId))
  }

  transition(transition: OwnershipTransition): OwnershipRecord {
    assertIdentity(transition.bucketId, transition.shardId, transition.operationId)
    if (!Number.isSafeInteger(transition.routeEpoch) || transition.routeEpoch < 0) {
      throw new NozzleError(
        "RouteVersionConflictError",
        "Route epochs must be non-negative integers.",
      )
    }

    const key = recordKey(transition.bucketId, transition.shardId)
    const current = this.#records.get(key)
    const currentState = current?.state ?? "unassigned"
    if (currentState !== transition.from) {
      throw new NozzleError(
        "OperationResumeRequiredError",
        "Ownership transition precondition failed.",
        {
          details: { actual: currentState, expected: transition.from },
        },
      )
    }
    if (current && current.routeEpoch > transition.routeEpoch) {
      throw new NozzleError("RouteVersionConflictError", "Route epochs cannot decrease.", {
        details: { currentEpoch: current.routeEpoch, requestedEpoch: transition.routeEpoch },
      })
    }

    if (transition.to === currentState) {
      if (
        current &&
        current.operationId === transition.operationId &&
        current.routeEpoch === transition.routeEpoch &&
        current.movementRole === transition.movementRole
      ) {
        return current
      }
      throw new NozzleError("OperationResumeRequiredError", "A duplicate transition did not match.")
    }
    if (!ALLOWED_TRANSITIONS[currentState].has(transition.to)) {
      throw new NozzleError(
        "OperationInterventionRequiredError",
        `Illegal ownership transition from ${currentState} to ${transition.to}.`,
      )
    }

    if (transition.to === "writable") {
      const maximumEpoch = this.records()
        .filter((record) => record.bucketId === transition.bucketId)
        .reduce((maximum, record) => Math.max(maximum, record.routeEpoch), 0)
      if (transition.routeEpoch <= maximumEpoch) {
        throw new NozzleError(
          "RouteVersionConflictError",
          "A writable transition must advance the bucket route epoch.",
          { details: { maximumEpoch, requestedEpoch: transition.routeEpoch } },
        )
      }
      const writable = this.records().find(
        (record) => record.bucketId === transition.bucketId && record.state === "writable",
      )
      if (writable && writable.shardId !== transition.shardId) {
        throw new NozzleError(
          "RouteVersionConflictError",
          "A bucket cannot have two writable owners.",
          { details: { bucketId: transition.bucketId } },
        )
      }
    }

    const next = Object.freeze({
      bucketId: transition.bucketId,
      movementRole: transition.movementRole,
      operationId: transition.operationId,
      routeEpoch: transition.routeEpoch,
      shardId: transition.shardId,
      state: transition.to,
    })
    this.#records.set(key, next)
    this.assertInvariants()
    return next
  }

  assertInvariants(): void {
    const writableBuckets = new Set<number>()
    for (const record of this.#records.values()) {
      if (record.state !== "writable") continue
      if (writableBuckets.has(record.bucketId)) {
        throw new NozzleError("RouteVersionConflictError", "A bucket has two writable owners.", {
          details: { bucketId: record.bucketId },
        })
      }
      writableBuckets.add(record.bucketId)
    }
  }
}

export function assertWriteAuthorized(
  record: OwnershipRecord | undefined,
  expectedRouteEpoch: number,
): asserts record is OwnershipRecord {
  if (record?.state !== "writable") {
    throw new NozzleError("StaleRouteRejectedError", "The shard is not the writable owner.", {
      details: { expectedRouteEpoch, state: record?.state ?? "missing" },
    })
  }
  if (record.routeEpoch !== expectedRouteEpoch) {
    throw new NozzleError("StaleRouteRejectedError", "The route epoch is stale.", {
      details: { actualRouteEpoch: record.routeEpoch, expectedRouteEpoch },
    })
  }
}
