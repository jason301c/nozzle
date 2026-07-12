import type { DigestFunction, SagaActionPhase } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import type {
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import {
  canonicalSagaReceiptJson,
  SAGA_ATTEMPT_ERROR_DOMAIN,
  SAGA_ATTEMPT_EVIDENCE_DOMAIN,
  SAGA_ATTEMPT_INPUT_DOMAIN,
  SAGA_ATTEMPT_OUTPUT_DOMAIN,
  SAGA_OUTCOME_ERROR_REFERENCE_JSON,
  SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON,
  SAGA_OUTCOME_OUTPUT_REFERENCE_JSON,
  type SagaAttemptIdentityRow,
  type SagaAttemptOutcomeRow,
  type SagaAttemptOutcomeState,
  type SagaAttemptPayloadRow,
  type SagaAttemptPurpose,
  sagaAttemptAcceptanceChecksum,
  sagaAttemptOutcomeChecksum,
  sagaReceiptPayloadChecksum,
} from "../src/saga-attempt-codec.js"
import {
  D1SagaHistoryReader,
  type SagaHistoryAnchor,
  type SagaHistoryAttemptCursor,
  type SagaHistoryPage,
} from "../src/saga-history.js"
import { SagaHistoryAttemptFolder } from "../src/saga-history-fold.js"

const OPERATION_ID = "attempt-fold-operation"
const SAGA_ID = "attempt-fold-saga"
const SAGA_STEP_ID = "write"
const LEASE_KEY = `saga:${SAGA_ID}`

const digest: DigestFunction = async (input) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", input.slice().buffer))
  return Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join("")
}

type ScriptedKind = "all" | "first"

interface ScriptedCall {
  readonly kind: ScriptedKind
  readonly result: unknown
  readonly sql: string
}

class ScriptedStatement implements ControlStatement {
  readonly #database: ScriptedDatabase
  readonly #sql: string

  constructor(database: ScriptedDatabase, sql: string) {
    this.#database = database
    this.#sql = sql
  }

  bind(..._values: readonly unknown[]): ControlStatement {
    return this
  }

  async first<T>(): Promise<T | null> {
    return this.#database.take("first", this.#sql) as T | null
  }

  async all<T>(): Promise<{
    readonly meta: Readonly<Record<string, unknown>>
    readonly results: readonly T[]
    readonly success: boolean
  }> {
    return this.#database.take("all", this.#sql) as {
      readonly meta: Readonly<Record<string, unknown>>
      readonly results: readonly T[]
      readonly success: boolean
    }
  }

  async run(): Promise<ControlRunResult> {
    throw new Error("unexpected run")
  }
}

class ScriptedDatabase implements TransactionalControlDatabase {
  readonly #calls: ScriptedCall[]

  constructor(calls: readonly ScriptedCall[]) {
    this.#calls = [...calls]
  }

  prepare(sql: string): ControlStatement {
    return new ScriptedStatement(this, sql)
  }

  async batch(_statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    throw new Error("unexpected batch")
  }

  take(kind: ScriptedKind, sql: string): unknown {
    const call = this.#calls.shift()
    if (call === undefined || call.kind !== kind || !sql.includes(call.sql)) {
      throw new Error(`unexpected ${kind} query: ${sql}`)
    }
    return structuredClone(call.result)
  }

  expectComplete(): void {
    expect(this.#calls).toEqual([])
  }
}

interface AttemptSpec {
  readonly acquisitionId?: string
  readonly acceptedAtMs: number
  readonly actionKey?: string
  readonly attemptId: string
  readonly causalAttemptId?: string | null
  readonly companion?: boolean
  readonly completedAtMs?: number
  readonly fencingToken?: number
  readonly idempotencyKey?: string
  readonly holderId?: string
  readonly leaseKey?: string
  readonly operationStepId?: string
  readonly outcome?: SagaAttemptOutcomeState | "accepted"
  readonly phase?: SagaActionPhase
  readonly protocolVersion?: 1 | 2
  readonly purpose?: SagaAttemptPurpose
  readonly sagaStepId?: string
}

interface AttemptFixture {
  readonly identity: SagaAttemptIdentityRow
  readonly outcome: SagaAttemptOutcomeRow | null
  readonly payloads: readonly SagaAttemptPayloadRow[]
}

function actionKey(purpose: SagaAttemptPurpose, phase: SagaActionPhase): string {
  return `${purpose}-${phase}-action@1:${(phase === "forward" ? "a" : "b").repeat(64)}`
}

function idempotencyKey(purpose: SagaAttemptPurpose, phase: SagaActionPhase): string {
  return `saga-${phase}${purpose === "observation" ? ":observation" : ""}`
}

async function attemptFixture(spec: AttemptSpec): Promise<AttemptFixture> {
  const phase = spec.phase ?? "forward"
  const purpose = spec.purpose ?? "effect"
  const sagaStepId = spec.sagaStepId ?? SAGA_STEP_ID
  const inputJson = canonicalSagaReceiptJson(
    JSON.stringify({ attempt: spec.attemptId, phase, purpose }),
    "Test input",
    false,
  )
  const inputChecksum = await sagaReceiptPayloadChecksum(
    digest,
    SAGA_ATTEMPT_INPUT_DOMAIN,
    inputJson,
  )
  const fencingToken = spec.fencingToken ?? 1
  const identityWithoutReceipt = Object.freeze({
    acquisitionId: spec.acquisitionId ?? `acquisition-${fencingToken}`,
    actionKey: spec.actionKey ?? actionKey(purpose, phase),
    attemptId: spec.attemptId,
    causalAttemptId: spec.causalAttemptId ?? null,
    fencingToken,
    holderId: spec.holderId ?? `holder-${fencingToken}`,
    idempotencyKey: spec.idempotencyKey ?? idempotencyKey(purpose, phase),
    inputChecksum,
    inputJson,
    leaseKey: spec.leaseKey ?? LEASE_KEY,
    operationId: OPERATION_ID,
    operationStepId: spec.operationStepId ?? `saga:${phase}:${sagaStepId}`,
    phase,
    purpose,
    sagaId: SAGA_ID,
    sagaStepId,
  })
  const acceptanceChecksum = await sagaAttemptAcceptanceChecksum(digest, identityWithoutReceipt)
  const identity: SagaAttemptIdentityRow = {
    acceptance_checksum: acceptanceChecksum,
    accepted_at_ms: spec.acceptedAtMs,
    acquisition_id: identityWithoutReceipt.acquisitionId,
    action_key: identityWithoutReceipt.actionKey,
    attempt_id: spec.attemptId,
    causal_attempt_id: identityWithoutReceipt.causalAttemptId,
    fencing_token: fencingToken,
    holder_id: identityWithoutReceipt.holderId,
    idempotency_key: identityWithoutReceipt.idempotencyKey,
    input_checksum: inputChecksum,
    input_json: inputJson,
    lease_key: identityWithoutReceipt.leaseKey,
    operation_id: OPERATION_ID,
    operation_step_id: identityWithoutReceipt.operationStepId,
    phase,
    protocol_classified_at_ms: spec.acceptedAtMs,
    protocol_version: spec.protocolVersion ?? 2,
    purpose,
    saga_id: SAGA_ID,
    saga_step_id: sagaStepId,
  }
  const state = spec.outcome ?? "confirmed"
  if (state === "accepted") return { identity, outcome: null, payloads: [] }
  const evidenceJson = JSON.stringify({ evidence: spec.attemptId })
  const confirmed = state === "confirmed"
  const valueJson = JSON.stringify({ [confirmed ? "output" : "error"]: spec.attemptId })
  const evidenceChecksum = await sagaReceiptPayloadChecksum(
    digest,
    SAGA_ATTEMPT_EVIDENCE_DOMAIN,
    evidenceJson,
  )
  const valueChecksum = await sagaReceiptPayloadChecksum(
    digest,
    confirmed ? SAGA_ATTEMPT_OUTPUT_DOMAIN : SAGA_ATTEMPT_ERROR_DOMAIN,
    valueJson,
  )
  const outcomeChecksum = await sagaAttemptOutcomeChecksum(
    digest,
    acceptanceChecksum,
    state,
    evidenceChecksum,
    evidenceJson,
    valueChecksum,
    valueJson,
  )
  const companion = spec.companion ?? false
  const outcome: SagaAttemptOutcomeRow = {
    attempt_id: spec.attemptId,
    completed_at_ms: spec.completedAtMs ?? spec.acceptedAtMs + 1,
    error_checksum: confirmed ? null : valueChecksum,
    error_json: confirmed ? null : companion ? SAGA_OUTCOME_ERROR_REFERENCE_JSON : valueJson,
    evidence_checksum: evidenceChecksum,
    evidence_json: companion ? SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON : evidenceJson,
    outcome_checksum: outcomeChecksum,
    output_checksum: confirmed ? valueChecksum : null,
    output_json: confirmed ? (companion ? SAGA_OUTCOME_OUTPUT_REFERENCE_JSON : valueJson) : null,
    state,
  }
  const payloads: SagaAttemptPayloadRow[] = companion
    ? [
        {
          attempt_id: spec.attemptId,
          payload_checksum: evidenceChecksum,
          payload_json: evidenceJson,
          payload_kind: "evidence",
        },
        {
          attempt_id: spec.attemptId,
          payload_checksum: valueChecksum,
          payload_json: valueJson,
          payload_kind: confirmed ? "output" : "error",
        },
      ]
    : []
  return { identity, outcome, payloads }
}

function allResult(rows: readonly unknown[]): unknown {
  return { meta: {}, results: rows, success: true }
}

function readerFor(fixtures: readonly AttemptFixture[]): {
  readonly database: ScriptedDatabase
  readonly reader: D1SagaHistoryReader
} {
  const calls = fixtures.flatMap((fixture): ScriptedCall[] => [
    {
      kind: "first",
      result: fixture.outcome,
      sql: 'FROM "nozzle_saga_action_attempt_outcomes"',
    },
    {
      kind: "all",
      result: allResult(fixture.payloads),
      sql: 'FROM "nozzle_saga_action_attempt_outcome_payloads"',
    },
  ])
  const database = new ScriptedDatabase(calls)
  return { database, reader: new D1SagaHistoryReader(database) }
}

function anchor(fixtures: readonly AttemptFixture[]): SagaHistoryAnchor {
  const last = fixtures.at(-1)?.identity
  return {
    auditHeadEventHash: "attempt-audit-head",
    auditHeadSequence: 1,
    environmentId: "attempt-environment",
    operationId: OPERATION_ID,
    operationInputChecksum: "attempt-operation-input",
    operationPlanChecksum: "attempt-operation-plan",
    operationStatus: "failed",
    operationTransitionCount: 1,
    operationTransitionLastAuditSequence: 1,
    operationTransitionLastId: "attempt-transition",
    operationUpdatedAtMs: 100,
    sagaAttemptCount: fixtures.length,
    sagaAttemptLastAcceptedAtMs: last?.accepted_at_ms ?? null,
    sagaAttemptLastId: last?.attempt_id ?? null,
    sagaDescriptorChecksum: "attempt-descriptor",
    sagaEffectCount: 1,
    sagaId: SAGA_ID,
    sagaInputChecksum: "attempt-saga-input",
    sagaLastEffectId: "attempt-effect",
    sagaRecordChecksum: "attempt-record",
    sagaStateVersion: 0,
    sagaStatus: "failed",
    sagaUpdatedAtMs: 100,
    schemaVersion: 1,
  }
}

function page(
  rows: readonly SagaAttemptIdentityRow[],
  complete: boolean,
): SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor> {
  const last = rows.at(-1)
  return {
    complete,
    nextCursor:
      complete || last === undefined
        ? null
        : { acceptedAtMs: last.accepted_at_ms, attemptId: last.attempt_id },
    rows,
  }
}

async function happyAttempts(): Promise<readonly AttemptFixture[]> {
  return [
    await attemptFixture({ acceptedAtMs: 1, attemptId: "forward-confirmed", protocolVersion: 1 }),
    await attemptFixture({
      acceptedAtMs: 2,
      attemptId: "compensation-confirmed",
      causalAttemptId: "forward-confirmed",
      companion: true,
      phase: "compensation",
    }),
    await attemptFixture({ acceptedAtMs: 3, attemptId: "forward-unknown", outcome: "unknown" }),
    await attemptFixture({ acceptedAtMs: 4, attemptId: "forward-accepted", outcome: "accepted" }),
    await attemptFixture({
      acceptedAtMs: 5,
      attemptId: "observe-unknown",
      causalAttemptId: "forward-unknown",
      companion: true,
      fencingToken: 2,
      outcome: "not_applied",
      purpose: "observation",
    }),
    await attemptFixture({
      acceptedAtMs: 6,
      attemptId: "observe-accepted",
      causalAttemptId: "forward-accepted",
      fencingToken: 2,
      outcome: "indeterminate",
      purpose: "observation",
    }),
  ]
}

async function fold(fixtures: readonly AttemptFixture[]) {
  const { database, reader } = readerFor(fixtures)
  const folder = new SagaHistoryAttemptFolder(anchor(fixtures), reader, digest)
  for (let offset = 0; offset < fixtures.length; offset += 2) {
    const rows = fixtures.slice(offset, offset + 2).map((fixture) => fixture.identity)
    await folder.append(page(rows, offset + rows.length === fixtures.length))
  }
  database.expectComplete()
  return folder.proof()
}

describe("SagaHistoryAttemptFolder", () => {
  it("folds inline, companion, accepted, effect, observation, and compensation receipts", async () => {
    const fixtures = await happyAttempts()
    const proof = await fold(fixtures)
    expect(proof).toMatchObject({
      attemptCount: 6,
      attemptFoldChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      attemptLastAcceptedAtMs: 6,
      attemptLastId: "observe-accepted",
      operationId: OPERATION_ID,
      sagaId: SAGA_ID,
      schemaVersion: 1,
    })
    expect(proof.attempts.map((attempt) => attempt.state)).toEqual([
      "confirmed",
      "confirmed",
      "unknown",
      "accepted",
      "not_applied",
      "indeterminate",
    ])
    expect(proof.attempts[1]).toMatchObject({
      causalAttemptId: "forward-confirmed",
      phase: "compensation",
      valueChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
    })
  })

  it("folds the exact empty terminal attempt stream", async () => {
    const { database, reader } = readerFor([])
    const emptyAnchor = anchor([])
    const folder = new SagaHistoryAttemptFolder(emptyAnchor, reader, digest)
    expect(() => folder.proof()).toThrowError(/requires more verified pages/u)
    await folder.append(page([], true))
    expect(folder.proof()).toMatchObject({
      attemptCount: 0,
      attemptLastAcceptedAtMs: null,
      attemptLastId: null,
      attempts: [],
    })
    await expect(folder.append(page([], true))).rejects.toMatchObject({
      code: "ConfigurationError",
    })
    database.expectComplete()
  })

  it("point-loads accepted receipts and rejects orphaned or out-of-anchor payload state", async () => {
    const accepted = await attemptFixture({
      acceptedAtMs: 1,
      attemptId: "accepted-only",
      outcome: "accepted",
    })
    const acceptedReader = readerFor([accepted])
    await expect(
      acceptedReader.reader.attemptRecord(anchor([accepted]), accepted.identity, digest),
    ).resolves.toMatchObject({ attemptId: "accepted-only", state: "accepted" })
    acceptedReader.database.expectComplete()

    const orphanReader = readerFor([
      {
        ...accepted,
        payloads: [
          {
            attempt_id: "accepted-only",
            payload_checksum: "a".repeat(64),
            payload_json: "{}",
            payload_kind: "evidence",
          },
        ],
      },
    ])
    await expect(
      orphanReader.reader.attemptRecord(anchor([accepted]), accepted.identity, digest),
    ).rejects.toMatchObject({ message: expect.stringMatching(/orphaned/u) })
    orphanReader.database.expectComplete()

    const outside = { ...accepted.identity, accepted_at_ms: 2, protocol_classified_at_ms: 2 }
    const outsideReader = readerFor([])
    await expect(
      outsideReader.reader.attemptRecord(anchor([accepted]), outside, digest),
    ).rejects.toMatchObject({ message: expect.stringMatching(/outside/u) })
    outsideReader.database.expectComplete()
  })

  it("rejects missing, incompatible, late, stale-fence, or misbound causal receipts", async () => {
    const baseCause = await attemptFixture({
      acceptedAtMs: 1,
      attemptId: "cause",
      outcome: "unknown",
    })
    const invalidObservations: AttemptFixture[][] = [
      [
        await attemptFixture({
          acceptedAtMs: 1,
          attemptId: "missing-cause-observation",
          causalAttemptId: "missing",
          fencingToken: 2,
          purpose: "observation",
        }),
      ],
      [
        baseCause,
        await attemptFixture({
          acceptedAtMs: 2,
          attemptId: "stale-observation",
          causalAttemptId: "cause",
          fencingToken: 1,
          purpose: "observation",
        }),
      ],
      [
        baseCause,
        await attemptFixture({
          acceptedAtMs: 2,
          attemptId: "misbound-observation",
          causalAttemptId: "cause",
          fencingToken: 2,
          idempotencyKey: "wrong-observation-key",
          purpose: "observation",
        }),
      ],
      [
        await attemptFixture({ acceptedAtMs: 1, attemptId: "confirmed-cause" }),
        await attemptFixture({
          acceptedAtMs: 2,
          attemptId: "confirmed-observation",
          causalAttemptId: "confirmed-cause",
          fencingToken: 2,
          purpose: "observation",
        }),
      ],
      [
        await attemptFixture({
          acceptedAtMs: 2,
          attemptId: "late-cause",
          fencingToken: 2,
          outcome: "unknown",
        }),
        await attemptFixture({
          acceptedAtMs: 1,
          attemptId: "early-observation",
          causalAttemptId: "late-cause",
          fencingToken: 2,
          purpose: "observation",
        }),
      ].sort((left, right) => left.identity.accepted_at_ms - right.identity.accepted_at_ms),
    ]
    for (const fixtures of invalidObservations) {
      await expect(fold(fixtures)).rejects.toMatchObject({
        message: expect.stringMatching(/causal effect/u),
      })
    }

    const invalidCompensations: AttemptFixture[][] = [
      [
        await attemptFixture({
          acceptedAtMs: 1,
          attemptId: "missing-forward-compensation",
          causalAttemptId: "missing",
          phase: "compensation",
        }),
      ],
      [
        await attemptFixture({
          acceptedAtMs: 1,
          attemptId: "unknown-forward",
          outcome: "unknown",
        }),
        await attemptFixture({
          acceptedAtMs: 2,
          attemptId: "unknown-compensation",
          causalAttemptId: "unknown-forward",
          phase: "compensation",
        }),
      ],
    ]
    for (const fixtures of invalidCompensations) {
      await expect(fold(fixtures)).rejects.toMatchObject({
        message: expect.stringMatching(/confirmed forward/u),
      })
    }

    const lateObservationCause = await attemptFixture({
      acceptedAtMs: 1,
      attemptId: "late-completing-observation-cause",
      completedAtMs: 10,
      outcome: "unknown",
    })
    const prematureObservation = await attemptFixture({
      acceptedAtMs: 2,
      attemptId: "premature-observation",
      causalAttemptId: lateObservationCause.identity.attempt_id,
      fencingToken: 2,
      purpose: "observation",
    })
    await expect(fold([lateObservationCause, prematureObservation])).rejects.toMatchObject({
      message: expect.stringMatching(/causal effect/u),
    })

    const lateForwardCause = await attemptFixture({
      acceptedAtMs: 1,
      attemptId: "late-completing-forward-cause",
      completedAtMs: 10,
    })
    const prematureCompensation = await attemptFixture({
      acceptedAtMs: 2,
      attemptId: "premature-compensation",
      causalAttemptId: lateForwardCause.identity.attempt_id,
      phase: "compensation",
    })
    await expect(fold([lateForwardCause, prematureCompensation])).rejects.toMatchObject({
      message: expect.stringMatching(/confirmed forward/u),
    })
  })

  it("rejects changed action, lease, fence, operation-step, order, and anchor bindings", async () => {
    const first = await attemptFixture({ acceptedAtMs: 1, attemptId: "first" })
    const changedAction = await attemptFixture({
      acceptedAtMs: 2,
      actionKey: "changed-action",
      attemptId: "changed-action",
    })
    await expect(fold([first, changedAction])).rejects.toMatchObject({
      message: expect.stringMatching(/binding changed/u),
    })

    const changedLease = await attemptFixture({
      acceptedAtMs: 2,
      attemptId: "changed-lease",
      leaseKey: "other-lease",
    })
    await expect(fold([first, changedLease])).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })

    const changedFence = await attemptFixture({
      acceptedAtMs: 2,
      attemptId: "changed-fence",
      fencingToken: 1,
      holderId: "other-holder",
    })
    await expect(fold([first, changedFence])).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })

    const sameTime = [
      await attemptFixture({ acceptedAtMs: 3, attemptId: "same-time-a", fencingToken: 2 }),
      await attemptFixture({ acceptedAtMs: 3, attemptId: "same-time-b", fencingToken: 1 }),
    ]
    await expect(fold(sameTime)).resolves.toMatchObject({ attemptCount: 2 })
    const regressedFence = await attemptFixture({
      acceptedAtMs: 4,
      attemptId: "regressed-fence",
      fencingToken: 1,
    })
    await expect(fold([...sameTime, regressedFence])).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })

    const wrongStep = await attemptFixture({
      acceptedAtMs: 1,
      attemptId: "wrong-step",
      operationStepId: "saga:forward:other",
    })
    await expect(fold([wrongStep])).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })

    const { reader } = readerFor([])
    const folder = new SagaHistoryAttemptFolder(anchor([first]), reader, digest)
    await expect(
      folder.append(page([{ ...first.identity, accepted_at_ms: 2 }], true)),
    ).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
    expect(
      () => new SagaHistoryAttemptFolder(anchor([first]), {} as D1SagaHistoryReader, digest),
    ).toThrowError(/reader is required/u)
    expect(
      () =>
        new SagaHistoryAttemptFolder(
          anchor([first]),
          reader,
          undefined as unknown as DigestFunction,
        ),
    ).toThrowError(/digest is required/u)
  })

  it("rejects malformed page capture and contradictory pagination", async () => {
    const fixture = await attemptFixture({ acceptedAtMs: 1, attemptId: "page-attempt" })
    const { reader } = readerFor([])
    const inputAnchor = anchor([fixture])
    const valid = page([fixture.identity], true)
    const malformed: unknown[] = [
      new Proxy(valid, {
        ownKeys: () => {
          throw new Error("capture")
        },
      }),
      { ...valid, extra: true },
      { complete: false, nextCursor: null, rows: [] },
      { ...valid, nextCursor: { acceptedAtMs: 1, attemptId: "page-attempt" } },
      {
        complete: false,
        nextCursor: { acceptedAtMs: 1, attemptId: "page-attempt" },
        rows: [fixture.identity],
      },
    ]
    for (const input of malformed) {
      await expect(
        new SagaHistoryAttemptFolder(inputAnchor, reader, digest).append(
          input as SagaHistoryPage<SagaAttemptIdentityRow, SagaHistoryAttemptCursor>,
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("serializes page folding and rejects premature or contradictory closure", async () => {
    const fixtures = [
      await attemptFixture({ acceptedAtMs: 1, attemptId: "serial-a" }),
      await attemptFixture({ acceptedAtMs: 2, attemptId: "serial-b" }),
    ]
    let markStarted: () => void = () => undefined
    let release: () => void = () => undefined
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    let calls = 0
    const delayedDigest: DigestFunction = async (input) => {
      calls += 1
      if (calls === 1) {
        markStarted()
        await gate
      }
      return digest(input)
    }
    const delayedReader = readerFor(fixtures)
    const delayedFolder = new SagaHistoryAttemptFolder(
      anchor(fixtures),
      delayedReader.reader,
      delayedDigest,
    )
    const pending = delayedFolder.append(
      page(
        fixtures.map((fixture) => fixture.identity),
        true,
      ),
    )
    await started
    await expect(delayedFolder.append(page([], true))).rejects.toMatchObject({
      code: "ConfigurationError",
      message: expect.stringMatching(/already being folded/u),
    })
    release()
    await pending
    expect(delayedFolder.proof().attemptCount).toBe(2)
    delayedReader.database.expectComplete()

    const incompleteReader = readerFor(fixtures)
    await expect(
      new SagaHistoryAttemptFolder(anchor(fixtures), incompleteReader.reader, digest).append(
        page(
          fixtures.map((fixture) => fixture.identity),
          false,
        ),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/failed to close/u) })
    incompleteReader.database.expectComplete()

    const earlyFixtures = await happyAttempts()
    const earlyReader = readerFor(earlyFixtures.slice(0, 2))
    await expect(
      new SagaHistoryAttemptFolder(anchor(earlyFixtures), earlyReader.reader, digest).append(
        page(
          earlyFixtures.slice(0, 2).map((fixture) => fixture.identity),
          true,
        ),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })
    earlyReader.database.expectComplete()

    const countReader = readerFor(fixtures)
    const countAnchor = { ...anchor(fixtures), sagaAttemptCount: 3 }
    await expect(
      new SagaHistoryAttemptFolder(countAnchor, countReader.reader, digest).append(
        page(
          fixtures.map((fixture) => fixture.identity),
          true,
        ),
      ),
    ).rejects.toMatchObject({ message: expect.stringMatching(/does not reconcile/u) })
    countReader.database.expectComplete()
  })
})
