import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type D1ResourceIdentity,
  type DigestFunction,
  leaseProof,
  sealOperationPlan,
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
import { D1OperationStore } from "../src/operation-store.js"
import { type D1ResourceEffectContext, D1ResourceStore } from "../src/resource-store.js"
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
  readonly #allResult: ControlQueryResult<unknown>
  readonly #row: unknown
  readonly #runResult: ControlRunResult

  constructor(input: {
    readonly allResult?: ControlQueryResult<unknown>
    readonly row?: unknown
    readonly runResult?: ControlRunResult
  }) {
    this.#allResult = input.allResult ?? { meta: {}, results: [], success: true }
    this.#row = input.row ?? null
    this.#runResult = input.runResult ?? { meta: { changes: 0 }, success: true }
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return this.#allResult as ControlQueryResult<T>
  }

  async first<T>(): Promise<T | null> {
    return this.#row as T | null
  }

  async run(): Promise<ControlRunResult> {
    return this.#runResult
  }
}

interface DatabaseFaults {
  readonly batchMode?: "commit_then_throw" | "effect_only" | "throw"
  readonly batchResults?: readonly ControlRunResult[]
  readonly effectRow?: unknown
  readonly effectRowAfterBatch?: () => unknown
  readonly resourceRow?: unknown
  readonly resourceRowAfterBatch?: () => unknown
  readonly transitionResult?: ControlQueryResult<unknown>
}

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #faults: DatabaseFaults
  #batched = false

  constructor(base: DatabaseAdapter, faults: DatabaseFaults) {
    this.#base = base
    this.#faults = faults
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#faults.batchMode === "throw") throw new Error("injected resource batch failure")
    if (this.#faults.batchMode === "effect_only") {
      const first = statements[0]
      if (!first) throw new Error("missing effect statement")
      const effectResult = await this.#base.batch([first])
      this.#batched = true
      return [effectResult[0] as ControlRunResult, { meta: { changes: 1 }, success: true }]
    }
    if (this.#faults.batchMode === "commit_then_throw") {
      await this.#base.batch(statements)
      this.#batched = true
      throw new Error("injected post-commit failure")
    }
    if (this.#faults.batchResults !== undefined) {
      this.#batched = true
      return this.#faults.batchResults
    }
    const results = await this.#base.batch(statements)
    this.#batched = true
    return results
  }

  prepare(sql: string): ControlStatement {
    if (sql.includes('FROM "nozzle_d1_resources" AS "resource"')) {
      if (this.#batched && this.#faults.resourceRowAfterBatch !== undefined) {
        return new FixedStatement({ row: this.#faults.resourceRowAfterBatch() })
      }
      if (this.#faults.resourceRow !== undefined) {
        return new FixedStatement({ row: this.#faults.resourceRow })
      }
    }
    if (sql.includes('SELECT * FROM "nozzle_operation_effects"')) {
      if (this.#batched && this.#faults.effectRowAfterBatch !== undefined) {
        return new FixedStatement({ row: this.#faults.effectRowAfterBatch() })
      }
      if (this.#faults.effectRow !== undefined) {
        return new FixedStatement({ row: this.#faults.effectRow })
      }
    }
    if (
      sql.includes('SELECT "transition"."transition_id"') &&
      this.#faults.transitionResult !== undefined
    ) {
      return new FixedStatement({ allResult: this.#faults.transitionResult })
    }
    return this.#base.prepare(sql)
  }
}

function rawResource(database: DatabaseSync, resourceId = "resource-a"): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "resource".*,
              "effect"."effect_id" AS "effect_id",
              "effect"."resource_kind" AS "effect_resource_kind",
              "effect"."resource_id" AS "effect_resource_id",
              "effect"."to_state_version" AS "effect_to_state_version",
              "effect"."evidence_checksum" AS "effect_evidence_checksum",
              "effect"."record_checksum" AS "effect_record_checksum",
              "effect"."record_json" AS "effect_record_json"
       FROM "nozzle_d1_resources" AS "resource"
       LEFT JOIN "nozzle_operation_effects" AS "effect"
         ON "effect"."effect_id" = "resource"."last_effect_id"
       WHERE "resource"."resource_id" = ?`,
    )
    .get(resourceId) as Record<string, unknown>
}

function rawEffect(database: DatabaseSync, effectId: string): Record<string, unknown> {
  return database
    .prepare(`SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?`)
    .get(effectId) as Record<string, unknown>
}

describe("D1ResourceStore", () => {
  let database: DatabaseAdapter
  let leases: D1LeaseStore
  let operations: D1OperationStore
  let resources: D1ResourceStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    leases = new D1LeaseStore(database)
    operations = new D1OperationStore(database, digest)
    resources = new D1ResourceStore(database, digest)
    return () => database.close()
  })

  const completedEffect = async (suffix: string): Promise<D1ResourceEffectContext> => {
    const operationId = `resource-operation-${suffix}`
    const inputJson = JSON.stringify({ resourceEffect: suffix })
    const capabilitySnapshotJson = JSON.stringify({ provider: "cloudflare-d1" })
    const plan = await sealOperationPlan(
      {
        capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
        idempotencyKey: `resource-operation-key-${suffix}`,
        inputChecksum: await digest(new TextEncoder().encode(inputJson)),
        operationId,
        operationType: "d1-resource-effect",
        steps: [
          {
            checkpoint: "reversible",
            dependsOn: [],
            effectProtocol: "opaque",
            idempotencyKey: `resource-step-key-${suffix}`,
            inputChecksum: `resource-step-input-${suffix}`,
            leaseKey: "resource:resource-a",
            postconditionChecksum: `resource-postcondition-${suffix}`,
            preconditionChecksum: `resource-precondition-${suffix}`,
            recoveryInstructions: "Reconcile the resource projection.",
            retryClassification: "reconcile_first",
            stepId: "materialize",
          },
        ],
      },
      digest,
    )
    await operations.create({
      actorChecksum: "resource-actor",
      capabilitySnapshotJson,
      environmentId: "production",
      idempotencyScope: `resource-scope-${suffix}`,
      inputJson,
      plan,
      requiredShardIds: ["shard-a"],
    })
    const acquired = await leases.acquire({
      acquisitionId: `resource-acquisition-${suffix}`,
      holderId: `resource-controller-${suffix}`,
      leaseKey: "resource:resource-a",
      ttlMs: 60_000,
    })
    if (!acquired.acquired) throw new Error("Fixture resource lease acquisition failed.")
    const proof = leaseProof(acquired.record)
    await operations.beginStep({
      actorChecksum: "resource-actor",
      attemptId: `resource-attempt-${suffix}`,
      idempotencyKey: `resource-step-key-${suffix}`,
      observedPreconditionChecksum: `resource-precondition-${suffix}`,
      operationId,
      proof,
      stepId: "materialize",
    })
    await operations.completeStep({
      actorChecksum: "resource-actor",
      attemptId: `resource-attempt-${suffix}`,
      observedPostconditionChecksum: `resource-postcondition-${suffix}`,
      operationId,
      proof,
      resultChecksum: `resource-result-${suffix}`,
      stepId: "materialize",
    })
    return {
      effectId: `resource-effect-${suffix}`,
      operationId,
      proof,
      stepId: "materialize",
    }
  }

  const identity = (creationOperationId: string): D1ResourceIdentity => ({
    creationOperationId,
    databaseName: "nozzle-production-shard-a-generation-a",
    desiredJurisdiction: "eu",
    environmentId: "production",
    fleetId: "fleet-a",
    generationId: "generation-a",
    intentChecksum: "resource-intent-a",
    resourceId: "resource-a",
    shardId: "shard-a",
    targetChecksum: "cloudflare-account-a",
  })

  const release = async (context: D1ResourceEffectContext): Promise<void> => {
    await leases.release({ proof: context.proof })
  }

  const createResource = async () => {
    const effect = await completedEffect("plan")
    const record = await resources.create({ effect, identity: identity(effect.operationId) })
    return { effect, record }
  }

  it("persists planned intent with an append-only succeeded-operation effect", async () => {
    const { effect, record } = await createResource()
    expect(record).toMatchObject({
      lifecycle: "planned",
      resourceId: "resource-a",
      stateVersion: 0,
    })
    expect(await resources.get("resource-a")).toEqual(record)
    expect(Object.isFrozen(record)).toBe(true)
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_effects"`).get(),
    ).toEqual({ count: 1 })
    await release(effect)
    await expect(
      resources.create({ effect, identity: identity(effect.operationId) }),
    ).resolves.toEqual(record)
    await expect(
      resources.create({
        effect: { ...effect, effectId: "resource-effect-plan-alternate" },
        identity: identity(effect.operationId),
      }),
    ).resolves.toEqual(record)
    expect(() =>
      database.database.prepare(`UPDATE "nozzle_d1_resources" SET "lifecycle" = 'abandoned'`).run(),
    ).toThrow(/D1_RESOURCE_EFFECT_REQUIRED/u)
    expect(() => database.database.prepare(`DELETE FROM "nozzle_d1_resources"`).run()).toThrow(
      /D1_RESOURCE_PERSISTENT/u,
    )
    expect(() =>
      database.database
        .prepare(`UPDATE "nozzle_operation_effects" SET "effect_kind" = 'other'`)
        .run(),
    ).toThrow(/OPERATION_EFFECT_IMMUTABLE/u)
    expect(() => database.database.prepare(`DELETE FROM "nozzle_operation_effects"`).run()).toThrow(
      /OPERATION_EFFECT_PERSISTENT/u,
    )
  })

  it("persists the full stable lifecycle with fresh recovery and deletion observations", async () => {
    let { effect, record } = await createResource()
    await release(effect)

    effect = await completedEffect("register")
    const registerInput = {
      binding: {
        attributionEvidenceChecksum: "resource-attribution-a",
        databaseId: "11111111-2222-4333-8444-555555555555",
        databaseName: record.databaseName,
        jurisdiction: record.desiredJurisdiction,
        providerResultChecksum: "resource-provider-result-a",
      },
      effect,
      expectedStateVersion: record.stateVersion,
      resourceId: record.resourceId,
    } as const
    record = await resources.register(registerInput)
    await release(effect)
    await expect(resources.register(registerInput)).resolves.toEqual(record)
    await expect(
      resources.register({
        ...registerInput,
        effect: { ...effect, effectId: "resource-effect-register-alternate" },
      }),
    ).resolves.toEqual(record)

    effect = await completedEffect("observe-ready")
    const readyObservationInput = {
      effect,
      expectedStateVersion: record.stateVersion,
      observation: {
        databaseId: record.binding?.databaseId as string,
        databaseName: record.databaseName,
        evidenceChecksum: "resource-observation-ready",
        jurisdiction: record.desiredJurisdiction,
        observationOperationId: effect.operationId,
        presence: "present",
      },
      resourceId: record.resourceId,
    } as const
    record = await resources.observe(readyObservationInput)
    await release(effect)
    await expect(resources.observe(readyObservationInput)).resolves.toEqual(record)
    await expect(
      resources.observe({
        ...readyObservationInput,
        effect: { ...effect, effectId: "resource-effect-observe-ready-alternate" },
      }),
    ).resolves.toEqual(record)

    for (const [suffix, action] of [
      ["ready", { kind: "mark_ready" }],
      ["quarantine-first", { kind: "quarantine" }],
    ] as const) {
      effect = await completedEffect(suffix)
      record = await resources.transition({
        action,
        effect,
        evidenceChecksum: `resource-${suffix}`,
        expectedStateVersion: record.stateVersion,
        resourceId: record.resourceId,
      })
      await release(effect)
      await expect(
        resources.transition({
          action,
          effect,
          evidenceChecksum: `resource-${suffix}`,
          expectedStateVersion: record.stateVersion - 1,
          resourceId: record.resourceId,
        }),
      ).resolves.toEqual(record)
    }

    effect = await completedEffect("observe-recovery")
    record = await resources.observe({
      effect,
      expectedStateVersion: record.stateVersion,
      observation: {
        databaseId: record.binding?.databaseId as string,
        databaseName: record.databaseName,
        evidenceChecksum: "resource-observation-recovery",
        jurisdiction: record.desiredJurisdiction,
        observationOperationId: effect.operationId,
        presence: "present",
      },
      resourceId: record.resourceId,
    })
    await release(effect)

    for (const [suffix, action] of [
      ["recover", { kind: "recover_ready" }],
      ["quarantine-second", { kind: "quarantine" }],
      ["retire", { kind: "retire" }],
    ] as const) {
      effect = await completedEffect(suffix)
      record = await resources.transition({
        action,
        effect,
        evidenceChecksum: `resource-${suffix}`,
        expectedStateVersion: record.stateVersion,
        resourceId: record.resourceId,
      })
      await release(effect)
    }

    effect = await completedEffect("observe-absent")
    record = await resources.observe({
      effect,
      expectedStateVersion: record.stateVersion,
      observation: {
        databaseId: record.binding?.databaseId as string,
        evidenceChecksum: "resource-observation-absent",
        observationOperationId: effect.operationId,
        presence: "absent",
      },
      resourceId: record.resourceId,
    })
    await release(effect)

    effect = await completedEffect("deleted")
    record = await resources.transition({
      action: { kind: "confirm_deleted" },
      effect,
      evidenceChecksum: "resource-deleted",
      expectedStateVersion: record.stateVersion,
      resourceId: record.resourceId,
    })
    await release(effect)

    expect(record).toMatchObject({
      binding: { databaseId: "11111111-2222-4333-8444-555555555555" },
      lastObservation: { presence: "absent" },
      lifecycle: "deleted",
      stateVersion: 10,
    })
    expect(
      database.database.prepare(`SELECT count(*) AS "count" FROM "nozzle_operation_effects"`).get(),
    ).toEqual({ count: 11 })
  })

  it("requires a unique succeeded transition under the active exact lease", async () => {
    const effect = await completedEffect("expired")
    await release(effect)
    await expect(
      resources.create({ effect, identity: identity(effect.operationId) }),
    ).rejects.toThrow(/no succeeded transition under the active lease/u)

    const missingContext: D1ResourceEffectContext = {
      effectId: "missing-effect",
      operationId: "missing-operation",
      proof: {
        acquisitionId: "missing-acquisition",
        fencingToken: 1,
        holderId: "missing-holder",
        leaseKey: "missing-lease",
      },
      stepId: "missing-step",
    }
    await expect(
      resources.register({
        binding: {
          attributionEvidenceChecksum: "attribution",
          databaseId: "11111111-2222-4333-8444-555555555555",
          databaseName: "database",
          jurisdiction: "global",
          providerResultChecksum: "result",
        },
        effect: missingContext,
        expectedStateVersion: 0,
        resourceId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)
    await expect(
      resources.observe({
        effect: missingContext,
        expectedStateVersion: 0,
        observation: {
          databaseId: "11111111-2222-4333-8444-555555555555",
          evidenceChecksum: "evidence",
          observationOperationId: "observation",
          presence: "absent",
        },
        resourceId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)
    await expect(
      resources.transition({
        action: { kind: "quarantine" },
        effect: missingContext,
        evidenceChecksum: "evidence",
        expectedStateVersion: 0,
        resourceId: "missing",
      }),
    ).rejects.toThrow(/does not exist/u)
  })

  it("rejects invalid bindings, contexts, intent conflicts, and contradictory effect replay", async () => {
    expect(() => new D1ResourceStore(null as never, digest)).toThrow(/transactional/u)
    expect(() => new D1ResourceStore(database, null as never)).toThrow(/digest/u)
    await expect(resources.get("")).rejects.toThrow(/non-empty/u)
    await expect(resources.get("x".repeat(513))).rejects.toThrow(/identity limit/u)

    const effect = await completedEffect("validation")
    await expect(
      resources.create({ effect, identity: identity("different-operation") }),
    ).rejects.toThrow(/creation operation does not match/u)
    await expect(
      resources.create({
        effect: { ...effect, effectId: "" },
        identity: identity(effect.operationId),
      }),
    ).rejects.toThrow(/non-empty/u)
    await expect(
      resources.create({
        effect: { ...effect, proof: { ...effect.proof, fencingToken: 0 } },
        identity: identity(effect.operationId),
      }),
    ).rejects.toThrow(/positive safe integer/u)

    const record = await resources.create({ effect, identity: identity(effect.operationId) })
    await expect(
      resources.transition({
        action: { kind: "quarantine" },
        effect,
        evidenceChecksum: "other-effect-kind",
        expectedStateVersion: record.stateVersion,
        resourceId: record.resourceId,
      }),
    ).rejects.toThrow(/contradicts its immutable receipt/u)
    await expect(
      resources.create({
        effect: { ...effect, effectId: "different-effect" },
        identity: { ...identity(effect.operationId), databaseName: "other-name" },
      }),
    ).rejects.toThrow(/contradictory immutable intent/u)
  })

  it("fails closed on malformed resource projections and missing effect joins", async () => {
    const { effect } = await createResource()
    await release(effect)
    const row = rawResource(database.database)
    for (const [change, message] of [
      [{ resource_id: null }, /columns are malformed/u],
      [{ record_json: null }, /JSON is malformed/u],
      [{ record_json: "{" }, /JSON is invalid/u],
      [{ record_json: "[]" }, /persisted D1 resource record is malformed/iu],
      [{ record_json: ` ${row.record_json as string}` }, /not canonical/u],
      [{ lifecycle: "ready" }, /contradict the canonical record/u],
      [{ record_checksum: "wrong" }, /contradict the canonical record/u],
      [{ effect_id: null }, /lacks its exact operation-effect receipt/u],
    ] as const) {
      const faulted = new D1ResourceStore(
        new FaultDatabase(database, { resourceRow: { ...row, ...change } }),
        digest,
      )
      await expect(faulted.get("resource-a")).rejects.toThrow(message)
    }
  })

  it("fails closed on malformed, contradictory, or unprojected operation-effect receipts", async () => {
    const { effect } = await createResource()
    const resourceRow = rawResource(database.database)
    const effectRow = rawEffect(database.database, effect.effectId)
    for (const [change, message] of [
      [{ effect_id: null }, /receipt is malformed/u],
      [{ record_checksum: "wrong" }, /contradicts its resource record/u],
    ] as const) {
      const faulted = new D1ResourceStore(
        new FaultDatabase(database, { effectRow: { ...effectRow, ...change } }),
        digest,
      )
      await expect(
        faulted.create({ effect, identity: identity(effect.operationId) }),
      ).rejects.toThrow(message)
    }

    const unprojected = new D1ResourceStore(
      new FaultDatabase(database, { effectRow, resourceRow: null }),
      digest,
    )
    await expect(
      unprojected.create({ effect, identity: identity(effect.operationId) }),
    ).rejects.toThrow(/not reflected in the resource projection/u)

    const alteredRecord = JSON.parse(effectRow.record_json as string) as Record<string, unknown>
    alteredRecord.databaseName = "contradictory-name"
    const alteredEffect = {
      ...effectRow,
      record_json: JSON.stringify(alteredRecord),
    }
    const constantDigest: DigestFunction = async () => effectRow.record_checksum as string
    const contradictory = new D1ResourceStore(
      new FaultDatabase(database, { effectRow: alteredEffect, resourceRow }),
      constantDigest,
    )
    await expect(
      contradictory.create({ effect, identity: identity(effect.operationId) }),
    ).rejects.toThrow(/contradicts the current resource projection/u)
    await release(effect)
  })

  it("rejects malformed or ambiguous succeeded-transition queries", async () => {
    const { effect, record } = await createResource()
    await release(effect)
    for (const transitionResult of [
      { meta: {}, results: [], success: false },
      {
        meta: {},
        results: [{ transition_id: "one" }, { transition_id: "two" }],
        success: true,
      },
      { meta: {}, results: [{ transition_id: "" }], success: true },
    ] satisfies readonly ControlQueryResult<unknown>[]) {
      const context = await completedEffect(`transition-fault-${transitionResult.results.length}`)
      const faulted = new D1ResourceStore(new FaultDatabase(database, { transitionResult }), digest)
      await expect(
        faulted.transition({
          action: { kind: "quarantine" },
          effect: context,
          evidenceChecksum: "faulted-transition",
          expectedStateVersion: record.stateVersion,
          resourceId: record.resourceId,
        }),
      ).rejects.toThrow(/malformed|ambiguous/u)
      await release(context)
    }
  })

  it("fails closed on incomplete, malformed, missing, or thrown mutation batches", async () => {
    const { effect, record } = await createResource()
    await release(effect)
    const cases: readonly [string, DatabaseFaults, RegExp][] = [
      ["incomplete", { batchResults: [] }, /incomplete resource mutation batch/u],
      [
        "metadata",
        {
          batchResults: [
            { meta: { changes: 1 }, success: false },
            { meta: { changes: 1 }, success: true },
          ],
        },
        /malformed resource mutation metadata/u,
      ],
      [
        "missing-receipt",
        {
          batchResults: [
            { meta: { changes: 1 }, success: true },
            { meta: { changes: 1 }, success: true },
          ],
        },
        /operation-effect receipt is missing/u,
      ],
      ["throw", { batchMode: "throw" }, /injected resource batch failure/u],
    ]
    for (const [suffix, faults, message] of cases) {
      const context = await completedEffect(`batch-${suffix}`)
      const faulted = new D1ResourceStore(new FaultDatabase(database, faults), digest)
      await expect(
        faulted.transition({
          action: { kind: "quarantine" },
          effect: context,
          evidenceChecksum: `batch-${suffix}`,
          expectedStateVersion: record.stateVersion,
          resourceId: record.resourceId,
        }),
      ).rejects.toThrow(message)
      await release(context)
    }
  })

  it("returns the committed winner when the client loses the post-commit response", async () => {
    const { effect, record } = await createResource()
    await release(effect)
    const context = await completedEffect("commit-then-throw")
    const faulted = new D1ResourceStore(
      new FaultDatabase(database, { batchMode: "commit_then_throw" }),
      digest,
    )
    await expect(
      faulted.transition({
        action: { kind: "quarantine" },
        effect: context,
        evidenceChecksum: "committed-winner",
        expectedStateVersion: record.stateVersion,
        resourceId: record.resourceId,
      }),
    ).resolves.toMatchObject({ lifecycle: "quarantined", stateVersion: 1 })
  })

  it("detects an effect committed without its resource projection", async () => {
    const context = await completedEffect("effect-only-create")
    const faulted = new D1ResourceStore(
      new FaultDatabase(database, { batchMode: "effect_only" }),
      digest,
    )
    await expect(
      faulted.create({
        effect: context,
        identity: {
          ...identity(context.operationId),
          databaseName: "nozzle-production-shard-b-generation-a",
          generationId: "generation-b",
          resourceId: "resource-b",
          shardId: "shard-b",
        },
      }),
    ).rejects.toThrow(/did not become durably visible/u)
  })

  it("detects a resource update that did not accompany its committed effect", async () => {
    const { effect, record } = await createResource()
    await release(effect)
    const context = await completedEffect("effect-only-update")
    const faulted = new D1ResourceStore(
      new FaultDatabase(database, { batchMode: "effect_only" }),
      digest,
    )
    await expect(
      faulted.transition({
        action: { kind: "quarantine" },
        effect: context,
        evidenceChecksum: "effect-only-update",
        expectedStateVersion: record.stateVersion,
        resourceId: record.resourceId,
      }),
    ).rejects.toThrow(/concurrent resource mutation committed a contradictory state/u)
  })
})
