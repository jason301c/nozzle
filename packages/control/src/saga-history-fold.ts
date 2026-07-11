import { type AuditEvent, type DigestFunction, loadAuditEvent, NozzleError } from "@nozzle/core"
import {
  loadSagaHistoryAnchor,
  SAGA_HISTORY_PAGE_ROW_LIMIT,
  type SagaHistoryAnchor,
  type SagaHistoryAuditRow,
  type SagaHistoryPage,
} from "./saga-history.js"

const AUDIT_TRANSITION_FOLD_DOMAIN = "nozzle.saga-history.audit-transition-fold.v1"
const EMPTY_FOLD_CHECKSUM = "0".repeat(64)
const CHECKSUM = /^[0-9a-f]{64}$/u

const AUDIT_ROW_KEYS = [
  "event_hash",
  "event_json",
  "sequence",
] as const satisfies readonly (keyof SagaHistoryAuditRow)[]

export interface SagaHistoryAuditProof {
  readonly auditEventCount: number
  readonly auditHeadEventHash: string
  readonly auditHeadSequence: number
  readonly environmentId: string
  readonly operationCreationEventHash: string
  readonly operationId: string
  readonly operationInputChecksum: string
  readonly operationPlanChecksum: string
  readonly operationTransitionCount: number
  readonly operationTransitionFoldChecksum: string
  readonly schemaVersion: 1
}

interface SagaHistoryAuditFoldState {
  creationEventHash: string | null
  nextSequence: number
  operationTransitionCount: number
  operationTransitionFoldChecksum: string
  previousEventHash: string | null
  previousServerTimeMs: number | null
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  )
}

function exactRecord<Row extends object>(
  value: unknown,
  keys: readonly (keyof Row)[],
): value is Row {
  return (
    plainRecord(value) &&
    Object.keys(value).length === keys.length &&
    keys.every((key) => Object.hasOwn(value, key))
  )
}

function denseArray(value: unknown): value is readonly unknown[] {
  return (
    Array.isArray(value) &&
    Object.keys(value).length === value.length &&
    Object.keys(value).every((key, index) => key === String(index))
  )
}

function capturedAuditPage(value: unknown): SagaHistoryPage<SagaHistoryAuditRow, number> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention("Saga audit fold page could not be captured safely.")
  }
  if (
    !exactRecord<SagaHistoryPage<SagaHistoryAuditRow, number>>(snapshot, [
      "complete",
      "nextCursor",
      "rows",
    ])
  ) {
    return intervention("Saga audit fold page fields are malformed.")
  }
  if (typeof snapshot.complete !== "boolean") {
    return intervention("Saga audit fold page completion metadata is malformed.")
  }
  if (!denseArray(snapshot.rows)) {
    return intervention("Saga audit fold page rows are not a dense array.")
  }
  if (
    snapshot.rows.length === 0 ||
    snapshot.rows.length > SAGA_HISTORY_PAGE_ROW_LIMIT ||
    !snapshot.rows.every((row) => exactRecord<SagaHistoryAuditRow>(row, AUDIT_ROW_KEYS))
  ) {
    return intervention("Saga audit fold page row envelope is malformed.")
  }
  const last = snapshot.rows.at(-1) as SagaHistoryAuditRow
  if (snapshot.complete) {
    if (snapshot.nextCursor !== null) {
      return intervention("A complete saga audit fold page retained a cursor.")
    }
  } else if (
    snapshot.rows.length !== SAGA_HISTORY_PAGE_ROW_LIMIT ||
    snapshot.nextCursor !== last.sequence
  ) {
    return intervention("An incomplete saga audit fold page has contradictory pagination.")
  }
  return Object.freeze({
    complete: snapshot.complete,
    nextCursor: snapshot.nextCursor,
    rows: Object.freeze(snapshot.rows.map((row) => Object.freeze(row))),
  })
}

function frame(parts: readonly string[]): Uint8Array {
  const encoded = [AUDIT_TRANSITION_FOLD_DOMAIN, ...parts].map((part) =>
    new TextEncoder().encode(part),
  )
  const length = encoded.reduce((total, part) => total + 4 + part.byteLength, 0)
  const output = new Uint8Array(length)
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function foldChecksum(
  digest: DigestFunction,
  previous: string,
  event: AuditEvent,
): Promise<string> {
  const checksum = await digest(
    frame([previous, event.sequence.toString(10), event.eventHash]).slice(),
  )
  if (typeof checksum !== "string" || !CHECKSUM.test(checksum)) {
    return configuration("Saga audit fold digest must return a lowercase SHA-256 checksum.")
  }
  return checksum
}

function parsedAuditEvent(row: SagaHistoryAuditRow): unknown {
  try {
    return JSON.parse(row.event_json)
  } catch {
    return intervention("Saga audit fold event JSON is invalid.")
  }
}

/**
 * Incrementally verifies the pinned environment audit chain and reduces this operation's
 * transition-linked events to a constant-size ordered checksum. It is intentionally internal
 * and does not reconstruct either projection or grant terminal mutation authority.
 */
export class SagaHistoryAuditFolder {
  readonly #anchor: SagaHistoryAnchor
  readonly #digest: DigestFunction
  #appending = false
  #complete = false
  #creationEventHash: string | null = null
  #nextSequence = 1
  #operationTransitionCount = 0
  #operationTransitionFoldChecksum = EMPTY_FOLD_CHECKSUM
  #previousEventHash: string | null = null
  #previousServerTimeMs: number | null = null

  constructor(inputAnchor: SagaHistoryAnchor, digest: DigestFunction) {
    if (typeof digest !== "function") configuration("A saga audit fold digest is required.")
    this.#anchor = loadSagaHistoryAnchor(inputAnchor)
    this.#digest = digest
  }

  async #foldOperationEvent(event: AuditEvent, state: SagaHistoryAuditFoldState): Promise<void> {
    if (event.eventType === "operation.created") {
      if (state.creationEventHash !== null || state.operationTransitionCount !== 0) {
        return intervention("The saga operation creation audit event is duplicated or reordered.")
      }
      if (
        event.stepId !== null ||
        event.fencingToken !== null ||
        event.payloadChecksum !== this.#anchor.operationInputChecksum
      ) {
        return intervention("The saga operation creation audit event contradicts its anchor.")
      }
      state.creationEventHash = event.eventHash
      return
    }
    if (state.creationEventHash === null) {
      return intervention("A saga operation transition audit event precedes operation creation.")
    }
    if (event.stepId === null || event.fencingToken === null) {
      return intervention("A saga operation transition audit event lacks its fenced step.")
    }
    if (state.operationTransitionCount >= this.#anchor.operationTransitionCount) {
      return intervention("Saga operation audit history exceeds its anchored transition count.")
    }
    state.operationTransitionFoldChecksum = await foldChecksum(
      this.#digest,
      state.operationTransitionFoldChecksum,
      event,
    )
    state.operationTransitionCount += 1
  }

  async #foldRow(row: SagaHistoryAuditRow, state: SagaHistoryAuditFoldState): Promise<void> {
    const event = await loadAuditEvent(parsedAuditEvent(row), this.#digest)
    if (JSON.stringify(event) !== row.event_json) {
      return intervention("Saga audit fold event JSON is not canonical.")
    }
    if (row.sequence !== state.nextSequence || event.sequence !== row.sequence) {
      return intervention("Saga audit fold sequence is incomplete or contradictory.")
    }
    if (row.event_hash !== event.eventHash) {
      return intervention("Saga audit fold row hash contradicts its event body.")
    }
    if (
      event.environmentId !== this.#anchor.environmentId ||
      event.sequence > this.#anchor.auditHeadSequence
    ) {
      return intervention("Saga audit fold event lies outside its anchored environment history.")
    }
    if (event.previousHash !== state.previousEventHash) {
      return intervention("Saga audit fold event does not extend the exact previous hash.")
    }
    if (state.previousServerTimeMs !== null && event.serverTimeMs < state.previousServerTimeMs) {
      return intervention("Saga audit fold server time decreases.")
    }
    if (event.operationId === this.#anchor.operationId) {
      await this.#foldOperationEvent(event, state)
    }
    state.nextSequence += 1
    state.previousEventHash = event.eventHash
    state.previousServerTimeMs = event.serverTimeMs
  }

  async append(inputPage: SagaHistoryPage<SagaHistoryAuditRow, number>): Promise<void> {
    if (this.#complete) configuration("Saga audit history is already completely folded.")
    if (this.#appending) configuration("A saga audit history page is already being folded.")
    this.#appending = true
    try {
      const page = capturedAuditPage(inputPage)
      const state: SagaHistoryAuditFoldState = {
        creationEventHash: this.#creationEventHash,
        nextSequence: this.#nextSequence,
        operationTransitionCount: this.#operationTransitionCount,
        operationTransitionFoldChecksum: this.#operationTransitionFoldChecksum,
        previousEventHash: this.#previousEventHash,
        previousServerTimeMs: this.#previousServerTimeMs,
      }
      for (const row of page.rows) await this.#foldRow(row, state)
      if (!page.complete) {
        if (state.nextSequence > this.#anchor.auditHeadSequence) {
          return intervention("Saga audit fold page failed to close at its anchored head.")
        }
      } else {
        if (
          state.nextSequence !== this.#anchor.auditHeadSequence + 1 ||
          state.previousEventHash !== this.#anchor.auditHeadEventHash ||
          state.creationEventHash === null ||
          state.operationTransitionCount !== this.#anchor.operationTransitionCount
        ) {
          return intervention("Saga audit fold does not reconcile with its complete anchor.")
        }
        this.#complete = true
      }
      this.#creationEventHash = state.creationEventHash
      this.#nextSequence = state.nextSequence
      this.#operationTransitionCount = state.operationTransitionCount
      this.#operationTransitionFoldChecksum = state.operationTransitionFoldChecksum
      this.#previousEventHash = state.previousEventHash
      this.#previousServerTimeMs = state.previousServerTimeMs
    } finally {
      this.#appending = false
    }
  }

  proof(): SagaHistoryAuditProof {
    if (!this.#complete || this.#creationEventHash === null) {
      return resume("Saga audit history requires more verified pages.")
    }
    return Object.freeze({
      auditEventCount: this.#anchor.auditHeadSequence,
      auditHeadEventHash: this.#anchor.auditHeadEventHash,
      auditHeadSequence: this.#anchor.auditHeadSequence,
      environmentId: this.#anchor.environmentId,
      operationCreationEventHash: this.#creationEventHash,
      operationId: this.#anchor.operationId,
      operationInputChecksum: this.#anchor.operationInputChecksum,
      operationPlanChecksum: this.#anchor.operationPlanChecksum,
      operationTransitionCount: this.#operationTransitionCount,
      operationTransitionFoldChecksum: this.#operationTransitionFoldChecksum,
      schemaVersion: 1,
    })
  }
}
