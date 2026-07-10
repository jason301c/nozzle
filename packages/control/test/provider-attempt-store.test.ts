import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import { type DigestFunction, leaseProof, sealOperationPlan } from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlDatabase,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore } from "../src/operation-store.js"
import {
  type AcceptProviderAttemptInput,
  D1ProviderAttemptStore,
} from "../src/provider-attempt-store.js"
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

class StaticStatement implements ControlStatement {
  readonly #row: unknown
  readonly #runResult: ControlRunResult

  constructor(row: unknown, runResult: ControlRunResult) {
    this.#row = row
    this.#runResult = runResult
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

class StaticDatabase implements ControlDatabase {
  readonly #rows: unknown[]
  readonly #runResult: ControlRunResult
  #read = 0

  constructor(
    rows: unknown | readonly unknown[],
    runResult: ControlRunResult = { meta: { changes: 1 }, success: true },
  ) {
    this.#rows = Array.isArray(rows) ? [...rows] : [rows]
    this.#runResult = runResult
  }

  prepare(sql: string): ControlStatement {
    const row = sql.includes("SELECT")
      ? this.#rows[Math.min(this.#read++, this.#rows.length - 1)]
      : null
    return new StaticStatement(row, this.#runResult)
  }
}

function rawAttempt(database: DatabaseSync, attemptId: string): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "attempt"."attempt_id", "attempt"."operation_id", "attempt"."step_id",
              "attempt"."target_checksum", "attempt"."actor_checksum", "attempt"."purpose",
              "attempt"."endpoint",
              "attempt"."mutating", "attempt"."request_checksum",
              "attempt"."acceptance_checksum", "attempt"."lease_key",
              "attempt"."holder_id", "attempt"."acquisition_id",
              "attempt"."fencing_token", "attempt"."accepted_at_ms",
              "outcome"."state", "outcome"."evidence_json", "outcome"."result_json",
              "outcome"."error_json", "outcome"."outcome_checksum",
              "outcome"."completed_at_ms"
       FROM "nozzle_provider_attempts" AS "attempt"
       LEFT JOIN "nozzle_provider_attempt_outcomes" AS "outcome" USING ("attempt_id")
       WHERE "attempt"."attempt_id" = ?`,
    )
    .get(attemptId) as Record<string, unknown>
}

describe("D1ProviderAttemptStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let operations: D1OperationStore
  let store: D1ProviderAttemptStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    operations = new D1OperationStore(database, digest)
    store = new D1ProviderAttemptStore(database, digest)
    return () => database.close()
  })

  const startAttempt = async (suffix: string) => {
    const leaseKey = `provider:${suffix}`
    const inputJson = JSON.stringify({ operation: `provider-${suffix}` })
    const capabilitySnapshotJson = JSON.stringify({ target: "cloudflare-d1" })
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: `operation-key-${suffix}`,
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId: `operation-${suffix}`,
        operationType: "provider-test",
        steps: [
          {
            checkpoint: "reversible",
            dependsOn: [],
            effectProtocol: "provider_receipt",
            idempotencyKey: `step-key-${suffix}`,
            inputChecksum: `step-input-${suffix}`,
            leaseKey,
            postconditionChecksum: `postcondition-${suffix}`,
            preconditionChecksum: `precondition-${suffix}`,
            recoveryInstructions: "Inspect the provider receipt.",
            retryClassification: "reconcile_first",
            stepId: "provider-call",
          },
        ],
      },
      digest,
    )
    await operations.create({
      actorChecksum: "creation-actor",
      capabilitySnapshotJson,
      environmentId: "production",
      idempotencyScope: `scope-${suffix}`,
      inputJson,
      plan,
      requiredShardIds: [],
    })
    const acquired = await leases.acquire({
      acquisitionId: `acquisition-${suffix}`,
      holderId: `controller-${suffix}`,
      leaseKey,
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    const attemptId = `attempt-${suffix}`
    await operations.beginStep({
      actorChecksum: "attempt-actor",
      attemptId,
      idempotencyKey: `step-key-${suffix}`,
      observedPreconditionChecksum: `precondition-${suffix}`,
      operationId: plan.operationId,
      proof,
      stepId: "provider-call",
    })
    const acceptance: AcceptProviderAttemptInput = {
      actorChecksum: "attempt-actor",
      attemptId,
      endpoint: "POST /accounts/{account_id}/d1/database",
      mutating: true,
      operationId: plan.operationId,
      purpose: "effect",
      proof,
      requestChecksum: `request-${suffix}`,
      stepId: "provider-call",
      targetChecksum: "cloudflare-account-target",
    }
    return { acceptance, attemptId, plan, proof }
  }

  it("persists immutable acceptance and confirmed-outcome receipts with exact replay", async () => {
    const fixture = await startAttempt("confirmed")
    const accepted = await store.accept(fixture.acceptance)
    expect(accepted).toMatchObject({
      actorChecksum: "attempt-actor",
      attemptId: fixture.attemptId,
      state: "accepted",
      targetChecksum: "cloudflare-account-target",
    })
    expect(accepted.acceptanceChecksum).toHaveLength(64)
    expect(Object.isFrozen(accepted)).toBe(true)
    await expect(store.accept(fixture.acceptance)).resolves.toEqual(accepted)

    const completed = await store.complete({
      attemptId: fixture.attemptId,
      evidenceJson: '{"cfRay":"ray-1","status":200}',
      proof: fixture.proof,
      resultJson: '{"name":"nozzle-db","uuid":"00000000-0000-4000-8000-000000000001"}',
      state: "confirmed",
    })
    expect(completed).toMatchObject({
      evidenceJson: '{"cfRay":"ray-1","status":200}',
      resultJson: '{"name":"nozzle-db","uuid":"00000000-0000-4000-8000-000000000001"}',
      state: "confirmed",
    })
    if (completed.state !== "confirmed") throw new Error("Fixture outcome was not confirmed.")
    expect(completed.outcomeChecksum).toHaveLength(64)
    await expect(
      operations.completeStep({
        actorChecksum: "attempt-actor",
        attemptId: fixture.attemptId,
        observedPostconditionChecksum: "postcondition-confirmed",
        operationId: fixture.plan.operationId,
        proof: fixture.proof,
        resultChecksum: "wrong-provider-outcome",
        stepId: "provider-call",
      }),
    ).rejects.toThrow(/provider outcome receipt contradicts/u)
    await expect(
      operations.completeStep({
        actorChecksum: "attempt-actor",
        attemptId: fixture.attemptId,
        observedPostconditionChecksum: "postcondition-confirmed",
        operationId: fixture.plan.operationId,
        proof: fixture.proof,
        resultChecksum: completed.outcomeChecksum,
        stepId: "provider-call",
      }),
    ).resolves.toMatchObject({ steps: { "provider-call": { state: "succeeded" } } })
    await leases.release({ proof: fixture.proof })
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: completed.evidenceJson,
        proof: fixture.proof,
        resultJson: completed.resultJson,
        state: "confirmed",
      }),
    ).resolves.toEqual(completed)
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: '{"status":201}',
        proof: fixture.proof,
        resultJson: '{"uuid":"different"}',
        state: "confirmed",
      }),
    ).rejects.toThrow(/contradicts durable evidence/u)
    expect(() =>
      database.database.exec(
        `UPDATE "nozzle_provider_attempts" SET "endpoint" = 'other' WHERE "attempt_id" = '${fixture.attemptId}'`,
      ),
    ).toThrow(/PROVIDER_ATTEMPT_IMMUTABLE/u)
    expect(() =>
      database.database.exec(
        `UPDATE "nozzle_provider_attempt_outcomes" SET "state" = 'unknown' WHERE "attempt_id" = '${fixture.attemptId}'`,
      ),
    ).toThrow(/PROVIDER_OUTCOME_IMMUTABLE/u)
    expect(() =>
      database.database.exec(
        `DELETE FROM "nozzle_provider_attempt_outcomes" WHERE "attempt_id" = '${fixture.attemptId}'`,
      ),
    ).toThrow(/PROVIDER_OUTCOME_PERSISTENT/u)
    expect(() =>
      database.database.exec(
        `DELETE FROM "nozzle_provider_attempts" WHERE "attempt_id" = '${fixture.attemptId}'`,
      ),
    ).toThrow(/PROVIDER_ATTEMPT_PERSISTENT/u)
  })

  it("records definite rejection and ambiguous outcome as distinct terminal receipts", async () => {
    const rejectedFixture = await startAttempt("rejected")
    await store.accept(rejectedFixture.acceptance)
    const rejected = await store.complete({
      attemptId: rejectedFixture.attemptId,
      errorJson: '{"codes":[10000],"kind":"permission_denied"}',
      evidenceJson: '{"status":403}',
      proof: rejectedFixture.proof,
      state: "rejected",
    })
    if (rejected.state !== "rejected") throw new Error("Fixture outcome was not rejected.")
    await expect(
      operations.failStep({
        actorChecksum: "attempt-actor",
        attemptId: rejectedFixture.attemptId,
        errorChecksum: rejected.outcomeChecksum,
        operationId: rejectedFixture.plan.operationId,
        outcome: "definitely_not_applied",
        proof: rejectedFixture.proof,
        stepId: "provider-call",
      }),
    ).resolves.toMatchObject({ steps: { "provider-call": { state: "retryable_failed" } } })

    const unknownFixture = await startAttempt("unknown")
    await store.accept(unknownFixture.acceptance)
    const unknown = await store.complete({
      attemptId: unknownFixture.attemptId,
      errorJson: '{"kind":"transport_after_dispatch"}',
      evidenceJson: '{"responseChecksum":null}',
      proof: unknownFixture.proof,
      state: "unknown",
    })
    if (unknown.state !== "unknown") throw new Error("Fixture outcome was not unknown.")
    await expect(
      operations.failStep({
        actorChecksum: "attempt-actor",
        attemptId: unknownFixture.attemptId,
        errorChecksum: unknown.outcomeChecksum,
        operationId: unknownFixture.plan.operationId,
        outcome: "unknown",
        proof: unknownFixture.proof,
        stepId: "provider-call",
      }),
    ).resolves.toMatchObject({ steps: { "provider-call": { state: "unknown" } } })

    await expect(
      operations.reconcileStep({
        actorChecksum: "reconciliation-actor",
        evidenceChecksum: "unreceipted-observation",
        observedPostconditionChecksum: "postcondition-unknown",
        operationId: unknownFixture.plan.operationId,
        outcome: "applied",
        proof: unknownFixture.proof,
        reconciliationId: "unreceipted-reconciliation",
        resultChecksum: "unreceipted-result",
        stepId: "provider-call",
      }),
    ).rejects.toThrow(/lacks receipt requirements/u)
    await expect(
      store.accept({
        ...unknownFixture.acceptance,
        attemptId: "premature-observation",
        endpoint: "GET /accounts/{account_id}/d1/database",
        mutating: false,
        purpose: "reconciliation",
        requestChecksum: "premature-observation-request",
      }),
    ).rejects.toThrow(/not accepted under the active lease/u)
    await leases.release({ proof: unknownFixture.proof })
    const observationLease = await leases.acquire({
      acquisitionId: "observation-acquisition",
      holderId: "observation-controller",
      leaseKey: unknownFixture.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!observationLease.acquired) throw new Error("Observation lease acquisition failed.")
    const observationProof = leaseProof(observationLease.record)
    const observationAttemptId = "observation-unknown"
    const observation = await store.accept({
      ...unknownFixture.acceptance,
      attemptId: observationAttemptId,
      endpoint: "GET /accounts/{account_id}/d1/database",
      mutating: false,
      purpose: "reconciliation",
      proof: observationProof,
      requestChecksum: "observation-request",
    })
    const observed = await store.complete({
      attemptId: observation.attemptId,
      evidenceJson: '{"matches":[{"uuid":"00000000-0000-4000-8000-000000000003"}]}',
      proof: observationProof,
      resultJson: '{"uuid":"00000000-0000-4000-8000-000000000003"}',
      state: "confirmed",
    })
    if (observed.state !== "confirmed") throw new Error("Observation was not confirmed.")
    await expect(
      operations.reconcileStep({
        actorChecksum: "reconciliation-actor",
        evidenceChecksum: observed.outcomeChecksum,
        observationAttemptId,
        observedPostconditionChecksum: "postcondition-unknown",
        operationId: unknownFixture.plan.operationId,
        outcome: "applied",
        proof: observationProof,
        reconciliationId: "received-reconciliation",
        resultChecksum: "reconciled-provider-result",
        stepId: "provider-call",
      }),
    ).resolves.toMatchObject({ steps: { "provider-call": { state: "succeeded" } } })
  })

  it("rejects contradictory receipt replay and outcomes under stale or different fences", async () => {
    const fixture = await startAttempt("fenced")
    await store.accept(fixture.acceptance)
    await expect(
      operations.completeStep({
        actorChecksum: "attempt-actor",
        attemptId: fixture.attemptId,
        observedPostconditionChecksum: "postcondition-fenced",
        operationId: fixture.plan.operationId,
        proof: fixture.proof,
        resultChecksum: "missing-outcome",
        stepId: "provider-call",
      }),
    ).rejects.toThrow(/no terminal outcome receipt/u)
    await expect(
      store.accept({ ...fixture.acceptance, targetChecksum: "other-target" }),
    ).rejects.toThrow(/contradictory immutable input/u)
    await leases.release({ proof: fixture.proof })
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        errorJson: '{"kind":"late"}',
        evidenceJson: '{"status":500}',
        proof: fixture.proof,
        state: "unknown",
      }),
    ).rejects.toThrow(/not committed under the active lease/u)
    const newer = await leases.acquire({
      acquisitionId: "newer-acquisition",
      holderId: "newer-controller",
      leaseKey: fixture.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!newer.acquired) throw new Error("Fixture lease reacquisition failed.")
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        errorJson: '{"kind":"late"}',
        evidenceJson: '{"status":500}',
        proof: leaseProof(newer.record),
        state: "unknown",
      }),
    ).rejects.toThrow(/different lease owner/u)
    await expect(store.get(fixture.attemptId)).resolves.toMatchObject({ state: "accepted" })
    await expect(
      operations.recoverRunningStep({
        actorChecksum: "recovery-actor",
        operationId: fixture.plan.operationId,
        proof: leaseProof(newer.record),
        recoveryId: "accepted-recovery",
        stepId: "provider-call",
      }),
    ).resolves.toMatchObject({ steps: { "provider-call": { state: "unknown" } } })
  })

  it("requires an accepted running step and validates bounded structured evidence", async () => {
    await expect(store.get("missing-attempt")).resolves.toBeUndefined()
    await expect(
      store.accept({
        actorChecksum: "actor",
        attemptId: "not-running",
        endpoint: "POST /resource",
        mutating: true,
        operationId: "missing-operation",
        purpose: "effect",
        proof: {
          acquisitionId: "missing-acquisition",
          fencingToken: 1,
          holderId: "missing-holder",
          leaseKey: "missing-lease",
        },
        requestChecksum: "request",
        stepId: "missing-step",
        targetChecksum: "target",
      }),
    ).rejects.toThrow(/not accepted under the active lease/u)
    await expect(
      store.complete({
        attemptId: "missing-attempt",
        evidenceJson: "{}",
        proof: {
          acquisitionId: "missing-acquisition",
          fencingToken: 1,
          holderId: "missing-holder",
          leaseKey: "missing-lease",
        },
        resultJson: "{}",
        state: "confirmed",
      }),
    ).rejects.toThrow(/never durably accepted/u)

    const fixture = await startAttempt("validation")
    await store.accept(fixture.acceptance)
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: "not-json",
        proof: fixture.proof,
        resultJson: "{}",
        state: "confirmed",
      }),
    ).rejects.toThrow(/not valid JSON/u)
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: "{}",
        proof: fixture.proof,
        resultJson: "x".repeat(1024 * 1024 + 1),
        state: "confirmed",
      }),
    ).rejects.toThrow(/one MiB/u)
  })

  it("validates receipt identity, proof, input bounds, and mutation metadata", async () => {
    expect(() => new D1ProviderAttemptStore(null as never, digest)).toThrow(/database binding/u)
    expect(() => new D1ProviderAttemptStore(database, null as never)).toThrow(/digest/u)
    await expect(store.get("")).rejects.toThrow(/must be non-empty/u)
    await expect(store.get("x".repeat(513))).rejects.toThrow(/durable receipt limit/u)

    const fixture = await startAttempt("input-validation")
    await expect(store.accept({ ...fixture.acceptance, mutating: "yes" as never })).rejects.toThrow(
      /mutating flag/u,
    )
    await expect(
      store.accept({ ...fixture.acceptance, purpose: "side_effect" as never }),
    ).rejects.toThrow(/purpose is invalid/u)
    await expect(
      store.accept({
        ...fixture.acceptance,
        attemptId: "mutating-observation",
        purpose: "reconciliation",
      }),
    ).rejects.toThrow(/reconciliation attempts must be non-mutating/u)
    await expect(
      store.accept({
        ...fixture.acceptance,
        proof: { ...fixture.proof, fencingToken: 0 },
      }),
    ).rejects.toThrow(/positive safe integer/u)
    await expect(store.accept({ ...fixture.acceptance, mutating: false })).resolves.toMatchObject({
      mutating: false,
      state: "accepted",
    })
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: "{}",
        proof: fixture.proof,
        resultJson: "{}",
        state: "invalid" as never,
      }),
    ).rejects.toThrow(/outcome is invalid/u)
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: "" as never,
        proof: fixture.proof,
        resultJson: "{}",
        state: "confirmed",
      }),
    ).rejects.toThrow(/must be JSON text/u)
    await expect(
      store.complete({
        attemptId: fixture.attemptId,
        evidenceJson: null as never,
        proof: fixture.proof,
        resultJson: "{}",
        state: "confirmed",
      }),
    ).rejects.toThrow(/must be JSON text/u)

    for (const changes of [undefined, -1, 2]) {
      const malformed = new D1ProviderAttemptStore(
        new StaticDatabase(null, {
          meta: { changes: changes as number },
          success: true,
        }),
        digest,
      )
      await expect(malformed.accept(fixture.acceptance)).rejects.toThrow(
        /malformed provider-receipt mutation metadata/u,
      )
    }
  })

  it("fails closed on malformed or contradictory persisted receipts", async () => {
    const acceptedFixture = await startAttempt("persisted-accepted")
    await store.accept(acceptedFixture.acceptance)
    const acceptedRow = rawAttempt(database.database, acceptedFixture.attemptId)

    const identityCases: readonly [string, unknown][] = [
      ["attempt_id", null],
      ["attempt_id", ""],
      ["operation_id", null],
      ["operation_id", ""],
      ["step_id", null],
      ["step_id", ""],
      ["target_checksum", null],
      ["target_checksum", ""],
      ["actor_checksum", null],
      ["actor_checksum", ""],
      ["purpose", null],
      ["purpose", "future-purpose"],
      ["endpoint", null],
      ["endpoint", ""],
      ["mutating", 2],
      ["request_checksum", null],
      ["request_checksum", ""],
      ["acceptance_checksum", null],
      ["acceptance_checksum", ""],
      ["lease_key", null],
      ["lease_key", ""],
      ["holder_id", null],
      ["holder_id", ""],
      ["acquisition_id", null],
      ["acquisition_id", ""],
      ["fencing_token", null],
      ["fencing_token", 0],
      ["accepted_at_ms", null],
      ["accepted_at_ms", -1],
    ]
    for (const [field, value] of identityCases) {
      const malformed = new D1ProviderAttemptStore(
        new StaticDatabase({ ...acceptedRow, [field]: value }),
        digest,
      )
      await expect(malformed.get(acceptedFixture.attemptId)).rejects.toThrow(
        /identity is malformed/u,
      )
    }

    await expect(
      new D1ProviderAttemptStore(
        new StaticDatabase({ ...acceptedRow, acceptance_checksum: "wrong" }),
        digest,
      ).get(acceptedFixture.attemptId),
    ).rejects.toThrow(/acceptance checksum does not match/u)
    for (const field of [
      "evidence_json",
      "result_json",
      "error_json",
      "outcome_checksum",
      "completed_at_ms",
    ]) {
      await expect(
        new D1ProviderAttemptStore(
          new StaticDatabase({ ...acceptedRow, [field]: field.endsWith("_ms") ? 1 : "{}" }),
          digest,
        ).get(acceptedFixture.attemptId),
      ).rejects.toThrow(/partial outcome data/u)
    }
    await expect(
      new D1ProviderAttemptStore(
        new StaticDatabase({ ...acceptedRow, state: "future-state" }),
        digest,
      ).get(acceptedFixture.attemptId),
    ).rejects.toThrow(/state is unsupported/u)

    const terminalFixture = await startAttempt("persisted-terminal")
    await store.accept(terminalFixture.acceptance)
    const terminalAcceptedRow = rawAttempt(database.database, terminalFixture.attemptId)
    await store.complete({
      attemptId: terminalFixture.attemptId,
      evidenceJson: '{"status":200}',
      proof: terminalFixture.proof,
      resultJson: '{"uuid":"terminal"}',
      state: "confirmed",
    })
    const terminalRow = rawAttempt(database.database, terminalFixture.attemptId)
    const incompleteCases: readonly [string, unknown][] = [
      ["completed_at_ms", null],
      ["completed_at_ms", -1],
      ["outcome_checksum", null],
      ["outcome_checksum", ""],
      ["evidence_json", null],
    ]
    for (const [field, value] of incompleteCases) {
      await expect(
        new D1ProviderAttemptStore(
          new StaticDatabase({ ...terminalRow, [field]: value }),
          digest,
        ).get(terminalFixture.attemptId),
      ).rejects.toThrow(/terminal provider attempt is incomplete/u)
    }
    for (const [field, value] of [
      ["evidence_json", 1],
      ["evidence_json", "not-json"],
      ["result_json", null],
      ["result_json", "not-json"],
    ] as const) {
      await expect(
        new D1ProviderAttemptStore(
          new StaticDatabase({ ...terminalRow, [field]: value }),
          digest,
        ).get(terminalFixture.attemptId),
      ).rejects.toThrow(/malformed|not valid JSON/u)
    }
    await expect(
      new D1ProviderAttemptStore(
        new StaticDatabase({ ...terminalRow, outcome_checksum: "wrong" }),
        digest,
      ).get(terminalFixture.attemptId),
    ).rejects.toThrow(/outcome checksum does not match/u)

    const contradictory = new D1ProviderAttemptStore(
      new StaticDatabase([terminalAcceptedRow, terminalRow]),
      digest,
    )
    await expect(
      contradictory.complete({
        attemptId: terminalFixture.attemptId,
        evidenceJson: '{"status":201}',
        proof: terminalFixture.proof,
        resultJson: '{"uuid":"different"}',
        state: "confirmed",
      }),
    ).rejects.toThrow(/committed provider outcome contradicts/u)
  })
})
