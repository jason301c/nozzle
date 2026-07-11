import { type AuditEvent, type DigestFunction, encodeAuditEventChecksumInput } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import type {
  SagaHistoryAnchor,
  SagaHistoryAuditRow,
  SagaHistoryPage,
} from "../src/saga-history.js"
import { SagaHistoryAuditFolder, type SagaHistoryAuditProof } from "../src/saga-history-fold.js"

const digest: DigestFunction = async (input) => {
  const copy = input.slice()
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

type AuditOverrides = Partial<
  Omit<AuditEvent, "eventHash" | "previousHash" | "schemaVersion" | "sequence">
> & {
  readonly previousHash?: string | null
  readonly sequence?: number
}

async function event(
  previous: AuditEvent | undefined,
  overrides: AuditOverrides,
): Promise<AuditEvent> {
  const candidate: Omit<AuditEvent, "eventHash"> = {
    actorChecksum: "actor-checksum",
    environmentId: "environment-a",
    eventType: "step.attempt.accepted",
    fencingToken: 1,
    idempotencyKey: `audit-${(previous?.sequence ?? 0) + 1}`,
    operationId: "other-operation",
    payloadChecksum: "payload-checksum",
    previousHash: previous?.eventHash ?? null,
    schemaVersion: 1,
    sequence: (previous?.sequence ?? 0) + 1,
    serverTimeMs: (previous?.serverTimeMs ?? 0) + 1,
    stepId: "other-step",
    ...overrides,
  }
  return Object.freeze({
    ...candidate,
    eventHash: await digest(encodeAuditEventChecksumInput(candidate)),
  })
}

function row(value: AuditEvent, json = JSON.stringify(value)): SagaHistoryAuditRow {
  return Object.freeze({ event_hash: value.eventHash, event_json: json, sequence: value.sequence })
}

function page(
  rows: readonly SagaHistoryAuditRow[],
  complete: boolean,
): SagaHistoryPage<SagaHistoryAuditRow, number> {
  return Object.freeze({
    complete,
    nextCursor: complete ? null : (rows.at(-1)?.sequence as number),
    rows: Object.freeze([...rows]),
  })
}

function anchor(
  events: readonly AuditEvent[],
  overrides: Partial<SagaHistoryAnchor> = {},
): SagaHistoryAnchor {
  const head = events.at(-1) as AuditEvent
  return Object.freeze({
    auditHeadEventHash: head.eventHash,
    auditHeadSequence: head.sequence,
    environmentId: "environment-a",
    operationId: "operation-a",
    operationInputChecksum: "operation-input-checksum",
    operationPlanChecksum: "operation-plan-checksum",
    operationStatus: "running",
    operationTransitionCount: 2,
    operationTransitionLastAuditSequence: head.sequence,
    operationTransitionLastId: "transition-2",
    operationUpdatedAtMs: 20,
    sagaAttemptCount: 0,
    sagaAttemptLastAcceptedAtMs: null,
    sagaAttemptLastId: null,
    sagaDescriptorChecksum: "saga-descriptor-checksum",
    sagaEffectCount: 1,
    sagaId: "saga-a",
    sagaInputChecksum: "saga-input-checksum",
    sagaLastEffectId: "effect-0",
    sagaRecordChecksum: "saga-record-checksum",
    sagaStateVersion: 0,
    sagaStatus: "failed",
    sagaUpdatedAtMs: 21,
    schemaVersion: 1,
    ...overrides,
  })
}

async function history(): Promise<readonly AuditEvent[]> {
  const first = await event(undefined, {
    eventType: "operation.created",
    fencingToken: null,
    operationId: "other-operation",
    stepId: null,
  })
  const second = await event(first, {
    eventType: "operation.created",
    fencingToken: null,
    idempotencyKey: "operation-a:created",
    operationId: "operation-a",
    payloadChecksum: "operation-input-checksum",
    stepId: null,
  })
  const third = await event(second, {
    eventType: "saga.initialized",
    idempotencyKey: "transition-1",
    operationId: "operation-a",
    stepId: "saga:init",
  })
  const fourth = await event(third, {})
  const fifth = await event(fourth, {
    eventType: "saga.action.started",
    idempotencyKey: "transition-2",
    operationId: "operation-a",
    stepId: "saga:forward:write",
  })
  return Object.freeze([first, second, third, fourth, fifth])
}

async function fold(events: readonly AuditEvent[], inputAnchor = anchor(events)) {
  const folder = new SagaHistoryAuditFolder(inputAnchor, digest)
  const rows = events.map((value) => row(value))
  for (let index = 0; index < rows.length; index += 2) {
    const next = rows.slice(index, index + 2)
    await folder.append(page(next, index + 2 >= rows.length))
  }
  return folder
}

function expectIntervention(promise: Promise<unknown>, message: RegExp) {
  return expect(promise).rejects.toMatchObject({
    code: "OperationInterventionRequiredError",
    message: expect.stringMatching(message),
  })
}

describe("saga history audit fold", () => {
  it("stream-verifies the pinned chain and emits a constant-size operation audit proof", async () => {
    const events = await history()
    const folder = new SagaHistoryAuditFolder(anchor(events), digest)
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )

    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    expect(() => folder.proof()).toThrowError(
      expect.objectContaining({ code: "OperationResumeRequiredError" }),
    )
    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )

    const proof: SagaHistoryAuditProof = folder.proof()
    expect(proof).toMatchObject({
      auditEventCount: 5,
      auditHeadEventHash: events[4]?.eventHash,
      auditHeadSequence: 5,
      environmentId: "environment-a",
      operationCreationEventHash: events[1]?.eventHash,
      operationId: "operation-a",
      operationInputChecksum: "operation-input-checksum",
      operationPlanChecksum: "operation-plan-checksum",
      operationTransitionCount: 2,
      operationTransitionFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      schemaVersion: 1,
    })
    expect(Object.isFrozen(proof)).toBe(true)
    await expect(folder.append(page([row(events[4] as AuditEvent)], true))).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "Saga audit history is already completely folded.",
    })
  })

  it("owns the anchor and page snapshots before checksum work begins", async () => {
    const events = await history()
    const mutableAnchor = { ...anchor(events) }
    const folder = new SagaHistoryAuditFolder(mutableAnchor, digest)
    mutableAnchor.operationId = "changed-operation"

    const mutableRows = events.map((value) => ({ ...row(value) }))
    const mutablePage = {
      complete: false,
      nextCursor: 2,
      rows: mutableRows.slice(0, 2),
    }
    const pending = folder.append(mutablePage)
    mutablePage.complete = true
    mutablePage.nextCursor = 99
    const firstMutableRow = mutablePage.rows[0]
    if (firstMutableRow === undefined) throw new Error("Missing test audit row")
    firstMutableRow.event_json = "{}"
    await expect(pending).resolves.toBeUndefined()

    await folder.append(page(mutableRows.slice(2, 4), false))
    await folder.append(page(mutableRows.slice(4), true))
    expect(folder.proof().operationId).toBe("operation-a")
  })

  it("applies each page atomically so a rejected page can be retried exactly", async () => {
    const events = await history()
    const folder = new SagaHistoryAuditFolder(anchor(events), digest)
    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    await expectIntervention(
      folder.append(
        page(
          [row(events[2] as AuditEvent), { ...row(events[3] as AuditEvent), event_hash: "wrong" }],
          false,
        ),
      ),
      /row hash/u,
    )
    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )
    expect(folder.proof().operationTransitionCount).toBe(2)
  })

  it("rejects overlapping page folds before a delayed checksum can stale-write state", async () => {
    const events = await history()
    let releaseDigest: () => void = () => undefined
    let markDigestStarted: () => void = () => undefined
    const digestGate = new Promise<void>((resolve) => {
      releaseDigest = resolve
    })
    const digestStarted = new Promise<void>((resolve) => {
      markDigestStarted = resolve
    })
    let digestCalls = 0
    const delayedDigest: DigestFunction = async (input) => {
      digestCalls += 1
      if (digestCalls === 1) {
        markDigestStarted()
        await digestGate
      }
      return digest(input)
    }
    const folder = new SagaHistoryAuditFolder(anchor(events), delayedDigest)
    const firstPage = page(
      events.slice(0, 2).map((value) => row(value)),
      false,
    )
    const delayedAppend = folder.append(firstPage)
    await digestStarted
    await expect(folder.append(firstPage)).rejects.toMatchObject({
      code: "ConfigurationError",
      message: "A saga audit history page is already being folded.",
    })
    releaseDigest()
    await delayedAppend

    await folder.append(
      page(
        events.slice(2, 4).map((value) => row(value)),
        false,
      ),
    )
    await folder.append(
      page(
        events.slice(4).map((value) => row(value)),
        true,
      ),
    )
    expect(folder.proof()).toMatchObject({
      auditHeadSequence: 5,
      operationTransitionCount: 2,
    })
  })

  it("requires a valid anchor and digest", async () => {
    const events = await history()
    expect(
      () => new SagaHistoryAuditFolder(anchor(events), undefined as unknown as DigestFunction),
    ).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
    expect(
      () =>
        new SagaHistoryAuditFolder(
          { ...anchor(events), schemaVersion: 2 } as unknown as SagaHistoryAnchor,
          digest,
        ),
    ).toThrowError(expect.objectContaining({ code: "OperationInterventionRequiredError" }))

    let digestCalls = 0
    const malformedFoldDigest: DigestFunction = async (input) => {
      digestCalls += 1
      return digestCalls === 4 ? "not-a-checksum" : digest(input)
    }
    const folder = new SagaHistoryAuditFolder(anchor(events), malformedFoldDigest)
    await folder.append(
      page(
        events.slice(0, 2).map((value) => row(value)),
        false,
      ),
    )
    await expect(
      folder.append(
        page(
          events.slice(2, 4).map((value) => row(value)),
          false,
        ),
      ),
    ).rejects.toMatchObject({
      code: "ConfigurationError",
      message: expect.stringMatching(/digest/u),
    })
  })

  it("rejects malformed page ownership and pagination envelopes", async () => {
    const events = await history()
    const validRow = row(events[0] as AuditEvent)
    const cases: readonly [unknown, RegExp][] = [
      [
        { complete: true, nextCursor: null, rows: [{ ...validRow, event_json: () => "bad" }] },
        /captured safely/u,
      ],
      [{ complete: true, extra: true, nextCursor: null, rows: [validRow] }, /fields/u],
      [{ complete: "yes", nextCursor: null, rows: [validRow] }, /completion metadata/u],
      [{ complete: true, nextCursor: null, rows: {} }, /dense array/u],
      [{ complete: true, nextCursor: null, rows: [] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [validRow, validRow, validRow] }, /row envelope/u],
      [{ complete: true, nextCursor: null, rows: [{ ...validRow, extra: true }] }, /row envelope/u],
      [{ complete: true, nextCursor: 1, rows: [validRow] }, /retained a cursor/u],
      [{ complete: false, nextCursor: 1, rows: [validRow] }, /pagination/u],
      [
        { complete: false, nextCursor: 99, rows: [validRow, row(events[1] as AuditEvent)] },
        /pagination/u,
      ],
    ]
    for (const [candidate, message] of cases) {
      const folder = new SagaHistoryAuditFolder(anchor(events), digest)
      await expectIntervention(
        folder.append(candidate as SagaHistoryPage<SagaHistoryAuditRow, number>),
        message,
      )
    }
  })

  it("rejects checksum-valid audit rows that break the pinned chain", async () => {
    const events = await history()
    const first = events[0] as AuditEvent
    const second = events[1] as AuditEvent
    const wrongEnvironment = await event(undefined, { environmentId: "environment-b" })
    const wrongPrevious = await event(undefined, { previousHash: "unexpected-previous" })
    const reordered = JSON.stringify({
      eventHash: first.eventHash,
      actorChecksum: first.actorChecksum,
      environmentId: first.environmentId,
      eventType: first.eventType,
      fencingToken: first.fencingToken,
      idempotencyKey: first.idempotencyKey,
      operationId: first.operationId,
      payloadChecksum: first.payloadChecksum,
      previousHash: first.previousHash,
      schemaVersion: first.schemaVersion,
      sequence: first.sequence,
      serverTimeMs: first.serverTimeMs,
      stepId: first.stepId,
    })
    const cases: readonly [SagaHistoryAuditRow, RegExp][] = [
      [{ ...row(first), event_json: "{" }, /event JSON is invalid/u],
      [{ ...row(first), sequence: 2 }, /sequence/u],
      [{ ...row(second), sequence: 1 }, /sequence/u],
      [{ ...row(first), event_hash: "wrong-row-hash" }, /row hash/u],
      [row(wrongEnvironment), /environment history/u],
      [row(wrongPrevious), /previous hash/u],
      [row(first, reordered), /not canonical/u],
    ]
    for (const [candidate, message] of cases) {
      const folder = new SagaHistoryAuditFolder(anchor(events), digest)
      await expectIntervention(folder.append(page([candidate], true)), message)
    }

    const decreasing = await event(first, { serverTimeMs: 0 })
    const decreasingFolder = new SagaHistoryAuditFolder(
      anchor([first, decreasing], {
        auditHeadEventHash: decreasing.eventHash,
        auditHeadSequence: 2,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 2,
      }),
      digest,
    )
    await expectIntervention(
      decreasingFolder.append(page([row(first), row(decreasing)], true)),
      /server time/u,
    )

    const beyond = await event(first, {})
    const beyondFolder = new SagaHistoryAuditFolder(
      anchor([first], {
        auditHeadSequence: 1,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 1,
      }),
      digest,
    )
    await expectIntervention(
      beyondFolder.append(page([row(first), row(beyond)], false)),
      /environment history/u,
    )
  })

  it("requires exactly one anchored creation before every fenced transition event", async () => {
    const other = await event(undefined, {})
    const transition = await event(other, {
      operationId: "operation-a",
      stepId: "saga:init",
    })
    const transitionBeforeCreation = new SagaHistoryAuditFolder(
      anchor([other, transition], {
        auditHeadEventHash: transition.eventHash,
        auditHeadSequence: transition.sequence,
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: transition.sequence,
      }),
      digest,
    )
    await expectIntervention(
      transitionBeforeCreation.append(page([row(other), row(transition)], true)),
      /precedes operation creation/u,
    )

    const creation = await event(undefined, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: "operation-input-checksum",
      stepId: null,
    })
    const duplicate = await event(creation, {
      eventType: "operation.created",
      fencingToken: null,
      operationId: "operation-a",
      payloadChecksum: "operation-input-checksum",
      stepId: null,
    })
    const duplicateFolder = new SagaHistoryAuditFolder(
      anchor([creation, duplicate], {
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 2,
      }),
      digest,
    )
    await expectIntervention(
      duplicateFolder.append(page([row(creation), row(duplicate)], true)),
      /duplicated or reordered/u,
    )

    for (const overrides of [
      { stepId: "unexpected-step" },
      { fencingToken: 1 },
      { payloadChecksum: "wrong-input" },
    ] as const) {
      const contradictory = await event(undefined, {
        eventType: "operation.created",
        fencingToken: null,
        operationId: "operation-a",
        payloadChecksum: "operation-input-checksum",
        stepId: null,
        ...overrides,
      })
      const folder = new SagaHistoryAuditFolder(
        anchor([contradictory], {
          operationTransitionCount: 1,
          operationTransitionLastAuditSequence: 1,
        }),
        digest,
      )
      await expectIntervention(
        folder.append(page([row(contradictory)], true)),
        /contradicts its anchor/u,
      )
    }

    for (const overrides of [{ stepId: null }, { fencingToken: null }] as const) {
      const malformed = await event(creation, {
        operationId: "operation-a",
        stepId: "saga:init",
        ...overrides,
      })
      const folder = new SagaHistoryAuditFolder(
        anchor([creation, malformed], {
          operationTransitionCount: 1,
          operationTransitionLastAuditSequence: 2,
        }),
        digest,
      )
      await expectIntervention(
        folder.append(page([row(creation), row(malformed)], true)),
        /lacks its fenced step/u,
      )
    }

    const one = await event(creation, { operationId: "operation-a" })
    const two = await event(one, { operationId: "operation-a" })
    const overflowFolder = new SagaHistoryAuditFolder(
      anchor([creation, one, two], {
        operationTransitionCount: 1,
        operationTransitionLastAuditSequence: 3,
      }),
      digest,
    )
    await overflowFolder.append(page([row(creation), row(one)], false))
    await expectIntervention(overflowFolder.append(page([row(two)], true)), /exceeds/u)
  })

  it("refuses incomplete pages and complete folds that disagree with any anchor head", async () => {
    const events = await history()
    const firstTwo = events.slice(0, 2).map((value) => row(value))
    const didNotClose = new SagaHistoryAuditFolder(
      anchor(events, { auditHeadSequence: 2, operationTransitionLastAuditSequence: 2 }),
      digest,
    )
    await expectIntervention(didNotClose.append(page(firstTwo, false)), /failed to close/u)

    const reconciliationCases: readonly SagaHistoryAnchor[] = [
      anchor(events),
      anchor(events, { auditHeadEventHash: "wrong-head" }),
      anchor(events, { operationId: "missing-operation", operationTransitionCount: 2 }),
      anchor(events, { operationTransitionCount: 3 }),
    ]
    const suppliedRows = [events.slice(0, 4), events, events, events]
    for (let index = 0; index < reconciliationCases.length; index += 1) {
      const candidateAnchor = reconciliationCases[index] as SagaHistoryAnchor
      const candidateEvents = suppliedRows[index] as readonly AuditEvent[]
      const folder = new SagaHistoryAuditFolder(candidateAnchor, digest)
      const candidateRows = candidateEvents.map((value) => row(value))
      for (let offset = 0; offset < candidateRows.length - 2; offset += 2) {
        await folder.append(page(candidateRows.slice(offset, offset + 2), false))
      }
      const finalOffset = Math.max(0, candidateRows.length - (candidateRows.length % 2 || 2))
      await expectIntervention(
        folder.append(page(candidateRows.slice(finalOffset), true)),
        /does not reconcile/u,
      )
    }
  })

  it("produces the same ordered transition digest across different page boundaries", async () => {
    const events = await history()
    const streamed = await fold(events)
    const differentlyPaged = new SagaHistoryAuditFolder(anchor(events), digest)
    const rows = events.map((value) => row(value))
    await differentlyPaged.append(page(rows.slice(0, 2), false))
    await differentlyPaged.append(page(rows.slice(2, 4), false))
    await differentlyPaged.append(page(rows.slice(4), true))
    expect(differentlyPaged.proof().operationTransitionFoldChecksum).toBe(
      streamed.proof().operationTransitionFoldChecksum,
    )
  })
})
