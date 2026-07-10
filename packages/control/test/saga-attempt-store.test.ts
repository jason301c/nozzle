import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type DigestFunction,
  leaseProof,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore, operationTransitionIdentity } from "../src/operation-store.js"
import {
  D1SagaAttemptStore,
  sagaActionInputChecksum,
  sagaObservationIdempotencyKey,
} from "../src/saga-attempt-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  type SagaEffectContext,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
import { controlSchemaSql } from "../src/schema.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

class StatementAdapter implements ControlStatement {
  readonly #statement: StatementSync
  #values: Record<string, SQLInputValue> = {}

  constructor(statement: StatementSync) {
    this.#statement = statement
    this.#statement.setAllowBareNamedParameters(false)
    this.#statement.setReadBigInts(false)
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#values = {}
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] as ControlBindingValue
      this.#values[`?${index + 1}`] =
        typeof value === "boolean"
          ? value
            ? 1
            : 0
          : value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : value
    }
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: this.#statement.all(this.#values) as T[], success: true }
  }

  async first<T>(): Promise<T | null> {
    return (this.#statement.get(this.#values) as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    const result = this.#statement.run(this.#values)
    return { meta: { changes: Number(result.changes) }, success: true }
  }
}

class DatabaseAdapter implements TransactionalControlDatabase {
  readonly database = new DatabaseSync(":memory:")

  constructor() {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(controlSchemaSql())
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    this.database.exec("BEGIN IMMEDIATE;")
    try {
      const results: ControlRunResult[] = []
      for (const statement of statements) results.push(await statement.run())
      this.database.exec("COMMIT;")
      return results
    } catch (error) {
      this.database.exec("ROLLBACK;")
      throw error
    }
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

class FixedStatement implements ControlStatement {
  readonly #row: unknown
  readonly #runResult: ControlRunResult

  constructor(input: { readonly row?: unknown; readonly runResult?: ControlRunResult }) {
    this.#row = input.row ?? null
    this.#runResult = input.runResult ?? { meta: { changes: 0 }, success: true }
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: [], success: true }
  }

  async first<T>(): Promise<T | null> {
    return this.#row as T | null
  }

  async run(): Promise<ControlRunResult> {
    return this.#runResult
  }
}

interface AttemptFaults {
  readonly attemptRow?: unknown | (() => unknown)
  readonly runResult?: ControlRunResult
  readonly sagaRow?: unknown
}

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #faults: AttemptFaults

  constructor(base: DatabaseAdapter, faults: AttemptFaults) {
    this.#base = base
    this.#faults = faults
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    if (
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"') &&
      this.#faults.attemptRow !== undefined
    ) {
      return new FixedStatement({
        row:
          typeof this.#faults.attemptRow === "function"
            ? this.#faults.attemptRow()
            : this.#faults.attemptRow,
      })
    }
    if (
      sql.includes('SELECT "operation_id", "record_json" FROM "nozzle_sagas"') &&
      this.#faults.sagaRow !== undefined
    ) {
      return new FixedStatement({ row: this.#faults.sagaRow })
    }
    if (sql.includes('INSERT INTO "nozzle_saga_action_') && this.#faults.runResult !== undefined) {
      return new FixedStatement({ runResult: this.#faults.runResult })
    }
    return this.#base.prepare(sql)
  }
}

function rawAttempt(database: DatabaseSync, attemptId: string): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "attempt".*, "outcome"."state", "outcome"."evidence_checksum",
              "outcome"."evidence_json", "outcome"."output_checksum",
              "outcome"."output_json", "outcome"."error_checksum",
              "outcome"."error_json", "outcome"."outcome_checksum",
              "outcome"."completed_at_ms"
       FROM "nozzle_saga_action_attempts" AS "attempt"
       LEFT JOIN "nozzle_saga_action_attempt_outcomes" AS "outcome" USING ("attempt_id")
       WHERE "attempt"."attempt_id" = ?`,
    )
    .get(attemptId) as Record<string, unknown>
}

async function framedChecksum(domain: string, parts: readonly string[]): Promise<string> {
  const encoded = [domain, ...parts].map((part) => new TextEncoder().encode(part))
  const output = new Uint8Array(encoded.reduce((length, part) => length + 4 + part.byteLength, 0))
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return digest(output)
}

async function acceptanceForRow(row: Record<string, unknown>): Promise<string> {
  return framedChecksum(
    "nozzle.saga-action-acceptance.v1",
    [
      "attempt_id",
      "saga_id",
      "operation_id",
      "operation_step_id",
      "saga_step_id",
      "phase",
      "purpose",
      "action_key",
      "idempotency_key",
      "input_checksum",
      "input_json",
      "lease_key",
      "holder_id",
      "acquisition_id",
    ]
      .map((key) => row[key] as string)
      .concat(String(row.fencing_token)),
  )
}

describe("D1SagaAttemptStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let operations: D1OperationStore
  let sagaStore: D1SagaStore
  let attempts: D1SagaAttemptStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    operations = new D1OperationStore(database, digest)
    sagaStore = new D1SagaStore(database, digest)
    attempts = new D1SagaAttemptStore(database, digest)
    return () => database.close()
  })

  async function fixture(suffix: string, actionInputJson = '{"b":2,"a":1}') {
    const operationId = `attempt-operation-${suffix}`
    const sagaId = `attempt-saga-${suffix}`
    const leaseKey = `saga:${sagaId}`
    const actionStepId = sagaActionOperationStepId("a", "forward")
    const capabilitySnapshotJson = JSON.stringify({ runtime: "saga-attempt-v1" })
    const operationInputJson = JSON.stringify({ sagaId })
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: `${operationId}:key`,
        inputChecksum: await digest(new TextEncoder().encode(operationInputJson)),
        operationId,
        operationType: "saga",
        steps: [SAGA_INIT_OPERATION_STEP_ID, actionStepId].map((stepId) => ({
          checkpoint: "reversible" as const,
          dependsOn: [],
          effectProtocol: stepId === actionStepId ? ("saga_receipt" as const) : ("opaque" as const),
          idempotencyKey: `${operationId}:${stepId}:key`,
          inputChecksum: `${operationId}:${stepId}:input`,
          leaseKey,
          postconditionChecksum: `${operationId}:${stepId}:postcondition`,
          preconditionChecksum: `${operationId}:${stepId}:precondition`,
          recoveryInstructions: "Recover from the append-only saga action receipt.",
          retryClassification: "reconcile_first" as const,
          stepId,
        })),
      },
      digest,
    )
    await operations.create({
      actorChecksum: "saga-attempt-actor",
      capabilitySnapshotJson,
      environmentId: "production",
      idempotencyScope: `saga-attempt-${suffix}`,
      inputJson: operationInputJson,
      plan,
      requiredShardIds: ["shard-a"],
    })
    const acquired = await leases.acquire({
      acquisitionId: `attempt-acquisition-${suffix}`,
      holderId: `attempt-controller-${suffix}`,
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Saga attempt fixture lease failed.")
    let proof = leaseProof(acquired.record)
    const context = (
      effectId: string,
      stepId: string,
      transitionId: string,
    ): SagaEffectContext => ({ effectId, operationId, proof, stepId, transitionId })
    const beginOperation = async (stepId: string, attemptId: string) => {
      await operations.beginStep({
        actorChecksum: "saga-attempt-actor",
        attemptId,
        idempotencyKey: `${operationId}:${stepId}:key`,
        observedPreconditionChecksum: `${operationId}:${stepId}:precondition`,
        operationId,
        proof,
        stepId,
      })
    }

    const initAttempt = `${sagaId}:init:1`
    await beginOperation(SAGA_INIT_OPERATION_STEP_ID, initAttempt)
    await operations.completeStep({
      actorChecksum: "saga-attempt-actor",
      attemptId: initAttempt,
      observedPostconditionChecksum: `${operationId}:${SAGA_INIT_OPERATION_STEP_ID}:postcondition`,
      operationId,
      proof,
      resultChecksum: `${sagaId}:init:result`,
      stepId: SAGA_INIT_OPERATION_STEP_ID,
    })
    const descriptor = await sealSagaDescriptor(
      {
        descriptorId: `attempt-descriptor-${suffix}`,
        steps: [
          {
            authorizationPolicyChecksum: null,
            baseRetryDelayMs: 10,
            compensationAction: {
              actionId: "a.compensate",
              artifactChecksum: "cc".repeat(32),
              version: 1,
            },
            compensationObservation: {
              actionId: "a.observe-compensation",
              artifactChecksum: "dd".repeat(32),
              version: 1,
            },
            forwardAction: {
              actionId: "a.forward",
              artifactChecksum: "aa".repeat(32),
              version: 1,
            },
            forwardObservation: {
              actionId: "a.observe-forward",
              artifactChecksum: "bb".repeat(32),
              version: 1,
            },
            inputSchemaChecksum: "11".repeat(32),
            irreversible: false,
            maxAttempts: 3,
            maxRetryDelayMs: 100,
            outputSchemaChecksum: "22".repeat(32),
            stepId: "a",
            timeoutMs: 1_000,
          },
        ],
        version: 1,
      },
      digest,
    )
    let saga = await sagaStore.create({
      deadlineAtMs: 10_000,
      descriptor,
      effect: context(
        `${sagaId}:create`,
        SAGA_INIT_OPERATION_STEP_ID,
        operationTransitionIdentity("succeeded", [
          operationId,
          SAGA_INIT_OPERATION_STEP_ID,
          initAttempt,
        ]),
      ),
      evidenceChecksum: `${sagaId}:create:evidence`,
      idempotencyKey: `${sagaId}:key`,
      inputChecksum: `${sagaId}:input`,
      sagaId,
      serverTimeMs: 1_000,
      stepInputChecksums: { a: await sagaActionInputChecksum(actionInputJson, digest) },
    })

    const actionAttemptId = `${sagaId}:a:forward:1`
    await beginOperation(actionStepId, actionAttemptId)
    saga = (
      await sagaStore.beginAction({
        attemptId: actionAttemptId,
        effect: context(
          `${actionAttemptId}:begin`,
          actionStepId,
          operationTransitionIdentity("accepted", [operationId, actionStepId, actionAttemptId]),
        ),
        evidenceChecksum: `${actionAttemptId}:accepted`,
        idempotencyKey: saga.steps.a?.forward.idempotencyKey as string,
        phase: "forward",
        sagaId,
        serverTimeMs: 1_001,
        stepId: "a",
      })
    ).saga

    const makeUnknown = async (acceptedOutcomeChecksum: string) => {
      await operations.failStep({
        actorChecksum: "saga-attempt-actor",
        attemptId: actionAttemptId,
        errorChecksum: acceptedOutcomeChecksum,
        operationId,
        outcome: "unknown",
        proof,
        stepId: actionStepId,
      })
      saga = await sagaStore.recordActionFailure({
        attemptId: actionAttemptId,
        effect: context(
          `${actionAttemptId}:unknown`,
          actionStepId,
          operationTransitionIdentity("failed", [operationId, actionStepId, actionAttemptId]),
        ),
        errorChecksum: acceptedOutcomeChecksum,
        evidenceChecksum: acceptedOutcomeChecksum,
        outcome: "unknown",
        phase: "forward",
        sagaId,
        serverTimeMs: 1_002,
        stepId: "a",
      })
      await leases.release({ proof })
      const reacquired = await leases.acquire({
        acquisitionId: `${sagaId}:observation-acquisition`,
        holderId: `${sagaId}:observation-controller`,
        leaseKey,
        ttlMs: 60_000,
      })
      if (!reacquired.acquired) throw new Error("Saga observation lease failed.")
      proof = leaseProof(reacquired.record)
    }

    return {
      actionAttemptId,
      actionInputJson,
      actionStepId,
      context,
      makeUnknown,
      operationId,
      proof: () => proof,
      saga: () => saga,
      sagaId,
    }
  }

  it("persists canonical accepted input and a confirmed append-only outcome", async () => {
    const run = await fixture("confirmed")
    const accepted = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    expect(accepted).toMatchObject({
      actionKey: `a.forward@1:${"aa".repeat(32)}`,
      inputJson: '{"a":1,"b":2}',
      operationId: run.operationId,
      operationStepId: run.actionStepId,
      state: "accepted",
    })
    expect(await attempts.get(run.actionAttemptId)).toEqual(accepted)
    await expect(
      attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      }),
    ).resolves.toEqual(accepted)

    const input = {
      attemptId: run.actionAttemptId,
      evidenceJson: '{"z":2,"a":1}',
      outputJson: '{"result":true}',
      proof: run.proof(),
      state: "confirmed" as const,
    }
    const completed = await attempts.complete(input)
    expect(completed).toMatchObject({
      evidenceJson: '{"a":1,"z":2}',
      outputJson: '{"result":true}',
      state: "confirmed",
    })
    await expect(attempts.complete(input)).resolves.toEqual(completed)
    await expect(attempts.complete({ ...input, outputJson: '{"result":false}' })).rejects.toThrow(
      /contradicts durable evidence/u,
    )
    expect(() =>
      database.database
        .prepare(`UPDATE "nozzle_saga_action_attempts" SET "input_checksum" = 'rewritten'`)
        .run(),
    ).toThrow(/SAGA_ATTEMPT_IMMUTABLE/u)
    expect(() =>
      database.database.prepare(`DELETE FROM "nozzle_saga_action_attempts"`).run(),
    ).toThrow(/SAGA_ATTEMPT_PERSISTENT/u)
  })

  it("records every effect failure classification and rejects indeterminate effects", async () => {
    for (const state of ["failed", "not_applied", "unknown"] as const) {
      const run = await fixture(`effect-${state}`)
      await attempts.accept({
        attemptId: run.actionAttemptId,
        inputJson: run.actionInputJson,
        phase: "forward",
        proof: run.proof(),
        purpose: "effect",
        sagaId: run.sagaId,
        sagaStepId: "a",
      })
      await expect(
        attempts.complete({
          attemptId: run.actionAttemptId,
          errorJson: JSON.stringify({ state }),
          evidenceJson: JSON.stringify({ source: "handler" }),
          proof: run.proof(),
          state,
        }),
      ).resolves.toMatchObject({ state })
    }

    const incompatible = await fixture("effect-indeterminate")
    await attempts.accept({
      attemptId: incompatible.actionAttemptId,
      inputJson: incompatible.actionInputJson,
      phase: "forward",
      proof: incompatible.proof(),
      purpose: "effect",
      sagaId: incompatible.sagaId,
      sagaStepId: "a",
    })
    await expect(
      attempts.complete({
        attemptId: incompatible.actionAttemptId,
        errorJson: "{}",
        evidenceJson: "{}",
        proof: incompatible.proof(),
        state: "indeterminate",
      }),
    ).rejects.toThrow(/incompatible/u)
  })

  it("binds observation attempts to the registered oracle under a newer fence", async () => {
    const run = await fixture("observation")
    const effect = await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const unknown = await attempts.complete({
      attemptId: run.actionAttemptId,
      errorJson: '{"dispatch":"unknown"}',
      evidenceJson: '{"transport":"lost"}',
      proof: run.proof(),
      state: "unknown",
    })
    if (unknown.state === "accepted") throw new Error("Expected terminal unknown receipt.")
    await run.makeUnknown(unknown.outcomeChecksum)

    const observationAttemptId = `${run.sagaId}:a:observe:1`
    const observation = await attempts.accept({
      attemptId: observationAttemptId,
      inputJson: JSON.stringify({ originalAttemptId: effect.attemptId }),
      phase: "forward",
      proof: run.proof(),
      purpose: "observation",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    expect(observation).toMatchObject({
      actionKey: `a.observe-forward@1:${"bb".repeat(32)}`,
      idempotencyKey: sagaObservationIdempotencyKey(effect.idempotencyKey),
      purpose: "observation",
      state: "accepted",
    })
    await expect(
      attempts.complete({
        attemptId: observationAttemptId,
        errorJson: '{"reason":"ambiguous"}',
        evidenceJson: '{"lookup":"complete"}',
        proof: run.proof(),
        state: "indeterminate",
      }),
    ).resolves.toMatchObject({ state: "indeterminate" })

    const wrong = await fixture("observation-unknown")
    const wrongEffect = await attempts.accept({
      attemptId: wrong.actionAttemptId,
      inputJson: wrong.actionInputJson,
      phase: "forward",
      proof: wrong.proof(),
      purpose: "effect",
      sagaId: wrong.sagaId,
      sagaStepId: "a",
    })
    const wrongUnknown = await attempts.complete({
      attemptId: wrong.actionAttemptId,
      errorJson: "{}",
      evidenceJson: "{}",
      proof: wrong.proof(),
      state: "unknown",
    })
    if (wrongUnknown.state === "accepted") throw new Error("Expected terminal unknown receipt.")
    await wrong.makeUnknown(wrongUnknown.outcomeChecksum)
    const wrongObservationId = `${wrong.sagaId}:a:observe:1`
    await attempts.accept({
      attemptId: wrongObservationId,
      inputJson: JSON.stringify({ originalAttemptId: wrongEffect.attemptId }),
      phase: "forward",
      proof: wrong.proof(),
      purpose: "observation",
      sagaId: wrong.sagaId,
      sagaStepId: "a",
    })
    await expect(
      attempts.complete({
        attemptId: wrongObservationId,
        errorJson: "{}",
        evidenceJson: "{}",
        proof: wrong.proof(),
        state: "unknown",
      }),
    ).rejects.toThrow(/incompatible/u)
  })

  it("validates receipt inputs and fails closed before an ineligible dispatch", async () => {
    expect(() => new D1SagaAttemptStore(null as never, digest)).toThrow(/binding is required/u)
    expect(() => new D1SagaAttemptStore(database, null as never)).toThrow(/digest/u)
    await expect(sagaActionInputChecksum("{}", null as never)).rejects.toThrow(/digest/u)
    await expect(sagaActionInputChecksum("{}", async () => "")).rejects.toThrow(/non-empty/u)
    expect(() => sagaObservationIdempotencyKey("")).toThrow(/non-empty/u)
    await expect(attempts.get("")).rejects.toThrow(/non-empty/u)
    await expect(attempts.get("x".repeat(513))).rejects.toThrow(/receipt limit/u)

    const run = await fixture("validation")
    const base = {
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward" as const,
      proof: run.proof(),
      purpose: "effect" as const,
      sagaId: run.sagaId,
      sagaStepId: "a",
    }
    await expect(attempts.accept({ ...base, phase: "bad" as never })).rejects.toThrow(
      /phase is invalid/u,
    )
    await expect(attempts.accept({ ...base, purpose: "bad" as never })).rejects.toThrow(
      /purpose is invalid/u,
    )
    await expect(
      attempts.accept({ ...base, proof: { ...run.proof(), fencingToken: 0 } }),
    ).rejects.toThrow(/positive safe integer/u)
    await expect(attempts.accept({ ...base, inputJson: "" })).rejects.toThrow(/must be JSON/u)
    await expect(attempts.accept({ ...base, inputJson: "{" })).rejects.toThrow(/not valid JSON/u)
    await expect(
      attempts.accept({ ...base, inputJson: JSON.stringify("x".repeat(1024 * 1024)) }),
    ).rejects.toThrow(/one MiB/u)
    await expect(
      attempts.accept({ ...base, attemptId: `${run.actionAttemptId}:other` }),
    ).rejects.toThrow(/not eligible/u)
    await expect(attempts.accept({ ...base, inputJson: '{"different":true}' })).rejects.toThrow(
      /sealed durable checksum/u,
    )
    await expect(attempts.accept({ ...base, sagaStepId: "missing" })).rejects.toThrow(
      /action state is missing/u,
    )

    const missing = { ...base, attemptId: "missing", sagaId: "missing-saga" }
    await expect(attempts.accept(missing)).rejects.toThrow(/saga does not exist/u)
    for (const [sagaRow, message] of [
      [{ operation_id: null, record_json: "{}" }, /binding is malformed/u],
      [{ operation_id: run.operationId, record_json: null }, /binding is malformed/u],
      [{ operation_id: run.operationId, record_json: "{" }, /not valid JSON/u],
      [
        {
          operation_id: run.operationId,
          record_json: ` ${JSON.stringify(run.saga())}`,
        },
        /not canonical/u,
      ],
    ] as const) {
      const faulted = new D1SagaAttemptStore(new FaultDatabase(database, { sagaRow }), digest)
      await expect(faulted.accept(base)).rejects.toThrow(message)
    }

    await expect(
      attempts.complete({
        attemptId: "missing",
        errorJson: "{}",
        evidenceJson: "{}",
        proof: run.proof(),
        state: "failed",
      }),
    ).rejects.toThrow(/never durably accepted/u)
    await expect(
      attempts.complete({
        attemptId: run.actionAttemptId,
        errorJson: "{}",
        evidenceJson: "{}",
        proof: run.proof(),
        state: "bad" as never,
      }),
    ).rejects.toThrow(/outcome is invalid/u)
  })

  it("rejects every malformed or contradictory persisted saga receipt shape", async () => {
    const run = await fixture("persisted-fault")
    await attempts.accept({
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof(),
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const acceptedRow = rawAttempt(database.database, run.actionAttemptId)
    for (const [change, message] of [
      [{ attempt_id: "other" }, /identity is malformed/u],
      [{ phase: "other" }, /identity is malformed/u],
      [{ input_json: null }, /action input is malformed/u],
      [{ input_json: "{" }, /action input is not valid JSON/u],
      [{ input_json: ` ${acceptedRow.input_json as string}` }, /action input is not canonical/u],
      [{ input_json: JSON.stringify("x".repeat(1024 * 1024)) }, /one MiB/u],
      [{ input_checksum: "wrong" }, /acceptance checksums do not match/u],
      [{ evidence_checksum: "partial" }, /partial outcome/u],
    ] as const) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: { ...acceptedRow, ...change } }),
        digest,
      )
      await expect(faulted.get(run.actionAttemptId)).rejects.toThrow(message)
    }

    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"source":"handler"}',
      outputJson: '{"ok":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    const completedRow = rawAttempt(database.database, run.actionAttemptId)
    for (const [change, message] of [
      [{ state: "other" }, /state is unsupported/u],
      [{ completed_at_ms: null }, /terminal saga attempt is incomplete/u],
      [{ evidence_json: null }, /action evidence is malformed/u],
      [{ evidence_json: "{" }, /action evidence is not valid JSON/u],
      [{ evidence_json: ` ${completedRow.evidence_json as string}` }, /evidence is not canonical/u],
      [{ output_checksum: null }, /value checksum is missing/u],
      [{ output_json: null }, /action output is malformed/u],
      [{ output_json: "{" }, /action output is not valid JSON/u],
      [{ output_json: ` ${completedRow.output_json as string}` }, /output is not canonical/u],
      [{ outcome_checksum: "wrong" }, /outcome checksums do not match/u],
    ] as const) {
      const faulted = new D1SagaAttemptStore(
        new FaultDatabase(database, { attemptRow: { ...completedRow, ...change } }),
        digest,
      )
      await expect(faulted.get(run.actionAttemptId)).rejects.toThrow(message)
    }
  })

  it("fails closed on rejected writes, stale fences, and contradictory committed outcomes", async () => {
    const run = await fixture("write-fault")
    const acceptInput = {
      attemptId: run.actionAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward" as const,
      proof: run.proof(),
      purpose: "effect" as const,
      sagaId: run.sagaId,
      sagaStepId: "a",
    }
    const malformed = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 2 }, success: true },
      }),
      digest,
    )
    await expect(malformed.accept(acceptInput)).rejects.toThrow(/malformed saga-receipt/u)
    const rejected = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(rejected.accept(acceptInput)).rejects.toThrow(/not accepted/u)

    await attempts.accept(acceptInput)
    const acceptedRow = rawAttempt(database.database, run.actionAttemptId)
    const alteredRow: Record<string, unknown> = {
      ...acceptedRow,
      operation_step_id: "other-step",
    }
    alteredRow.acceptance_checksum = await acceptanceForRow(alteredRow)
    const contradictoryAccept = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        attemptRow: alteredRow,
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(contradictoryAccept.accept(acceptInput)).rejects.toThrow(
      /contradictory immutable input/u,
    )

    await expect(
      attempts.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: "{}",
        outputJson: "{}",
        proof: { ...run.proof(), acquisitionId: "other-acquisition" },
        state: "confirmed",
      }),
    ).rejects.toThrow(/fenced by a different lease owner/u)

    const outcomeRejected = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        runResult: { meta: { changes: 0 }, success: true },
      }),
      digest,
    )
    await expect(
      outcomeRejected.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: "{}",
        outputJson: "{}",
        proof: run.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/outcome was not committed/u)

    const acceptedBefore = rawAttempt(database.database, run.actionAttemptId)
    await attempts.complete({
      attemptId: run.actionAttemptId,
      evidenceJson: '{"actual":true}',
      outputJson: '{"actual":true}',
      proof: run.proof(),
      state: "confirmed",
    })
    const actualCompleted = rawAttempt(database.database, run.actionAttemptId)
    let reads = 0
    const contradictoryOutcome = new D1SagaAttemptStore(
      new FaultDatabase(database, {
        attemptRow: () => (reads++ === 0 ? acceptedBefore : actualCompleted),
        runResult: { meta: { changes: 1 }, success: true },
      }),
      digest,
    )
    await expect(
      contradictoryOutcome.complete({
        attemptId: run.actionAttemptId,
        evidenceJson: '{"requested":true}',
        outputJson: '{"requested":true}',
        proof: run.proof(),
        state: "confirmed",
      }),
    ).rejects.toThrow(/committed saga outcome contradicts/u)
  })
})
