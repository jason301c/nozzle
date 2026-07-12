import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import { type DigestFunction, NozzleError } from "@nozzle/core"
import { afterEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import {
  D1ReaderBarrierStore,
  type ReaderBarrierCapability,
  type VerifyReaderBarrierInput,
  verifyReaderDeploymentBarrier,
} from "../src/reader-barrier-store.js"
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
  #batchTail: Promise<unknown> = Promise.resolve()

  constructor() {
    this.database.exec("PRAGMA foreign_keys = ON;")
    this.database.exec(controlSchemaSql())
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const execute = async (): Promise<readonly ControlRunResult[]> => {
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
    const result = this.#batchTail.then(execute, execute)
    this.#batchTail = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

class FailingStatement implements ControlStatement {
  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    throw new Error("Injected statement failure")
  }

  async first<T>(): Promise<T | null> {
    throw new Error("Injected statement failure")
  }

  async run(): Promise<ControlRunResult> {
    throw new Error("Injected statement failure")
  }
}

class FaultDatabase implements TransactionalControlDatabase {
  readonly #delegate: DatabaseAdapter
  readonly #failAt: number | undefined
  readonly #loseResponse: boolean

  constructor(
    delegate: DatabaseAdapter,
    input: { readonly failAt?: number; readonly loseResponse?: boolean },
  ) {
    this.#delegate = delegate
    this.#failAt = input.failAt
    this.#loseResponse = input.loseResponse === true
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const injected =
      this.#failAt === undefined
        ? statements
        : statements.map((statement, index) =>
            index === this.#failAt ? new FailingStatement() : statement,
          )
    const results = await this.#delegate.batch(injected)
    if (this.#loseResponse) throw new Error("Injected lost response")
    return results
  }

  prepare(sql: string): ControlStatement {
    return this.#delegate.prepare(sql)
  }
}

interface QueryFaults {
  readonly attestationResult?: unknown
  readonly barrierRow?: unknown
  readonly partialBarrierRow?: unknown
}

class QueryFaultStatement implements ControlStatement {
  readonly #delegate: ControlStatement
  readonly #faults: QueryFaults
  readonly #sql: string

  constructor(sql: string, delegate: ControlStatement, faults: QueryFaults) {
    this.#delegate = delegate
    this.#faults = faults
    this.#sql = sql
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#delegate.bind(...values)
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    if (
      this.#sql.includes('FROM "nozzle_reader_version_attestations"') &&
      Object.hasOwn(this.#faults, "attestationResult")
    ) {
      return this.#faults.attestationResult as ControlQueryResult<T>
    }
    return this.#delegate.all<T>()
  }

  async first<T>(): Promise<T | null> {
    if (
      this.#sql.includes('FROM "nozzle_saga_outcome_payload_activations" AS "activation"') &&
      Object.hasOwn(this.#faults, "barrierRow")
    ) {
      return this.#faults.barrierRow as T | null
    }
    if (
      this.#sql.includes('FROM "nozzle_reader_barriers" WHERE "protocol_version" = 1') &&
      Object.hasOwn(this.#faults, "partialBarrierRow")
    ) {
      return this.#faults.partialBarrierRow as T | null
    }
    return this.#delegate.first<T>()
  }

  run(): Promise<ControlRunResult> {
    return this.#delegate.run()
  }
}

class QueryFaultDatabase implements TransactionalControlDatabase {
  readonly #delegate: DatabaseAdapter
  readonly #faults: QueryFaults

  constructor(delegate: DatabaseAdapter, faults: QueryFaults) {
    this.#delegate = delegate
    this.#faults = faults
  }

  batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#delegate.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return new QueryFaultStatement(sql, this.#delegate.prepare(sql), this.#faults)
  }
}

class MetadataDatabase implements TransactionalControlDatabase {
  readonly #delegate: DatabaseAdapter
  readonly #results: unknown

  constructor(delegate: DatabaseAdapter, results: unknown) {
    this.#delegate = delegate
    this.#results = results
  }

  async batch(): Promise<readonly ControlRunResult[]> {
    return this.#results as readonly ControlRunResult[]
  }

  prepare(sql: string): ControlStatement {
    return this.#delegate.prepare(sql)
  }
}

class RacingDatabase implements TransactionalControlDatabase {
  readonly #competitor: ReaderBarrierCapability
  readonly #delegate: DatabaseAdapter
  #raced = false

  constructor(delegate: DatabaseAdapter, competitor: ReaderBarrierCapability) {
    this.#delegate = delegate
    this.#competitor = competitor
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (!this.#raced) {
      this.#raced = true
      await new D1ReaderBarrierStore(this.#delegate, digest).activate(this.#competitor)
    }
    return this.#delegate.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return this.#delegate.prepare(sql)
  }
}

class PartialRacingDatabase implements TransactionalControlDatabase {
  readonly #beforeBatch: () => void
  readonly #delegate: DatabaseAdapter
  #raced = false

  constructor(delegate: DatabaseAdapter, beforeBatch: () => void) {
    this.#beforeBatch = beforeBatch
    this.#delegate = delegate
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (!this.#raced) {
      this.#raced = true
      this.#beforeBatch()
    }
    return this.#delegate.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return this.#delegate.prepare(sql)
  }
}

const databases: DatabaseAdapter[] = []
afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

function database(): DatabaseAdapter {
  const value = new DatabaseAdapter()
  databases.push(value)
  return value
}

function evidence(suffix = "a"): VerifyReaderBarrierInput {
  return {
    attestations: [
      {
        artifactChecksum: "3".repeat(64),
        controlSchemaMax: 5,
        controlSchemaMin: 4,
        outcomePayloadReaderMax: 1,
        outcomePayloadReaderMin: 1,
        scriptName: `router-${suffix}`,
        versionId: `router-version-${suffix}`,
      },
      {
        artifactChecksum: "2".repeat(64),
        controlSchemaMax: 6,
        controlSchemaMin: 5,
        outcomePayloadReaderMax: 2,
        outcomePayloadReaderMin: 1,
        scriptName: `controller-${suffix}`,
        versionId: `controller-version-${suffix}-new`,
      },
      {
        artifactChecksum: "1".repeat(64),
        controlSchemaMax: 5,
        controlSchemaMin: 1,
        outcomePayloadReaderMax: 1,
        outcomePayloadReaderMin: 1,
        scriptName: `controller-${suffix}`,
        versionId: `controller-version-${suffix}-old`,
      },
    ],
    deployments: [
      {
        deploymentId: `router-deployment-${suffix}`,
        scriptName: `router-${suffix}`,
        versions: [{ versionId: `router-version-${suffix}`, weightBps: 10_000 }],
      },
      {
        deploymentId: `controller-deployment-${suffix}`,
        scriptName: `controller-${suffix}`,
        versions: [
          { versionId: `controller-version-${suffix}-old`, weightBps: 7_500 },
          { versionId: `controller-version-${suffix}-new`, weightBps: 2_500 },
        ],
      },
    ],
    expectedScriptNames: [`router-${suffix}`, `controller-${suffix}`],
  }
}

async function capability(suffix = "a"): Promise<ReaderBarrierCapability> {
  return verifyReaderDeploymentBarrier(evidence(suffix), digest)
}

function count(database: DatabaseSync, table: string): number {
  return (database.prepare(`SELECT count(*) AS "count" FROM "${table}"`).get() as { count: number })
    .count
}

function rawBarrierRow(database: DatabaseSync): Record<string, unknown> {
  return database
    .prepare(
      `SELECT "activation"."protocol_version", "activation"."reader_barrier_checksum",
              "activation"."activated_at_ms", "barrier"."barrier_checksum",
              "barrier"."inventory_checksum", "barrier"."barrier_json",
              "barrier"."verified_at_ms"
       FROM "nozzle_saga_outcome_payload_activations" AS "activation"
       JOIN "nozzle_reader_barriers" AS "barrier" USING ("protocol_version")`,
    )
    .get() as Record<string, unknown>
}

function rawAttestations(database: DatabaseSync): Record<string, unknown>[] {
  return database
    .prepare(
      `SELECT "script_name", "version_id", "artifact_checksum", "control_schema_min",
              "control_schema_max", "outcome_payload_reader_min", "outcome_payload_reader_max",
              "attestation_checksum",
              "attestation_json", "registered_at_ms"
       FROM "nozzle_reader_version_attestations" ORDER BY "script_name", "version_id"`,
    )
    .all() as Record<string, unknown>[]
}

function insertAttestations(
  database: DatabaseSync,
  rows: readonly Record<string, unknown>[],
): void {
  const statement = database.prepare(
    `INSERT INTO "nozzle_reader_version_attestations"
     ("script_name", "version_id", "artifact_checksum", "control_schema_min",
      "control_schema_max", "outcome_payload_reader_min", "outcome_payload_reader_max",
      "attestation_checksum", "attestation_json", "registered_at_ms")
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
  const columns = [
    "script_name",
    "version_id",
    "artifact_checksum",
    "control_schema_min",
    "control_schema_max",
    "outcome_payload_reader_min",
    "outcome_payload_reader_max",
    "attestation_checksum",
    "attestation_json",
    "registered_at_ms",
  ] as const
  for (const row of rows) {
    statement.run(...columns.map((column) => row[column] as SQLInputValue))
  }
}

function insertBarrier(database: DatabaseSync, row: Readonly<Record<string, unknown>>): void {
  database
    .prepare(
      `INSERT INTO "nozzle_reader_barriers"
       ("protocol_version", "barrier_checksum", "inventory_checksum", "barrier_json",
        "verified_at_ms")
       VALUES (?, ?, ?, ?, ?)`,
    )
    .run(
      row.protocol_version as SQLInputValue,
      row.barrier_checksum as SQLInputValue,
      row.inventory_checksum as SQLInputValue,
      row.barrier_json as SQLInputValue,
      row.verified_at_ms as SQLInputValue,
    )
}

async function activatedBase(suffix: string): Promise<DatabaseAdapter> {
  const base = database()
  await new D1ReaderBarrierStore(base, digest).activate(await capability(suffix))
  return base
}

describe("reader deployment barrier", () => {
  it("canonically activates every traffic-bearing reader version and exactly replays", async () => {
    const base = database()
    const input = evidence()
    const proof = await verifyReaderDeploymentBarrier(input, digest)
    expect(Object.isFrozen(proof)).toBe(true)
    expect(Object.keys(proof)).toEqual([])
    const store = new D1ReaderBarrierStore(base, digest)
    const activated = await store.activate(proof)

    expect(activated).toMatchObject({
      activeDeployments: [
        {
          scriptName: "controller-a",
          versions: [
            { versionId: "controller-version-a-new", weightBps: 2_500 },
            { versionId: "controller-version-a-old", weightBps: 7_500 },
          ],
        },
        { scriptName: "router-a" },
      ],
      expectedScriptNames: ["controller-a", "router-a"],
      protocolVersion: 1,
    })
    expect(
      activated.attestations.map(({ scriptName, versionId }) => [scriptName, versionId]),
    ).toEqual([
      ["controller-a", "controller-version-a-new"],
      ["controller-a", "controller-version-a-old"],
      ["router-a", "router-version-a"],
    ])
    expect(activated.activatedAtMs).toBeGreaterThanOrEqual(activated.verifiedAtMs)
    expect(Object.isFrozen(activated)).toBe(true)
    expect(Object.isFrozen(activated.activeDeployments)).toBe(true)
    expect(Object.isFrozen(activated.activeDeployments[0]?.versions)).toBe(true)
    expect(Object.isFrozen(activated.attestations)).toBe(true)
    expect(count(base.database, "nozzle_reader_version_attestations")).toBe(3)
    expect(count(base.database, "nozzle_reader_barriers")).toBe(1)
    expect(count(base.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
    await expect(store.get()).resolves.toEqual(activated)
    await expect(store.activate(proof)).resolves.toEqual(activated)

    const reordered = await verifyReaderDeploymentBarrier(
      {
        attestations: [...input.attestations].reverse(),
        deployments: [...input.deployments].reverse(),
        expectedScriptNames: [...input.expectedScriptNames].reverse(),
      },
      digest,
    )
    await expect(store.activate(reordered)).resolves.toEqual(activated)
  })

  it("uses D1 BINARY UTF-8 ordering for non-BMP names and prefix version IDs", async () => {
    const astral = "\u{10000}"
    const privateUse = "\uE000"
    const proof = await verifyReaderDeploymentBarrier(
      {
        attestations: [
          {
            artifactChecksum: "1".repeat(64),
            controlSchemaMax: 5,
            controlSchemaMin: 5,
            outcomePayloadReaderMax: 1,
            outcomePayloadReaderMin: 1,
            scriptName: astral,
            versionId: "astral-version",
          },
          ...["version", "v"].map((versionId, index) => ({
            artifactChecksum: `${index + 2}`.repeat(64),
            controlSchemaMax: 5,
            controlSchemaMin: 5,
            outcomePayloadReaderMax: 1,
            outcomePayloadReaderMin: 1,
            scriptName: privateUse,
            versionId,
          })),
        ],
        deployments: [
          {
            deploymentId: "astral-deployment",
            scriptName: astral,
            versions: [{ versionId: "astral-version", weightBps: 10_000 }],
          },
          {
            deploymentId: "private-deployment",
            scriptName: privateUse,
            versions: [
              { versionId: "version", weightBps: 5_000 },
              { versionId: "v", weightBps: 5_000 },
            ],
          },
        ],
        expectedScriptNames: [astral, privateUse],
      },
      digest,
    )
    const base = database()
    const activated = await new D1ReaderBarrierStore(base, digest).activate(proof)
    expect(activated.expectedScriptNames).toEqual([privateUse, astral])
    expect(activated.activeDeployments[0]?.versions.map(({ versionId }) => versionId)).toEqual([
      "v",
      "version",
    ])
  })

  it("persists the sealed 256-script and 512-active-version boundary below row limits", async () => {
    const expectedScriptNames = Array.from(
      { length: 256 },
      (_, index) => `reader-${index.toString().padStart(3, "0")}`,
    )
    const deployments = expectedScriptNames.map((scriptName) => ({
      deploymentId: `${scriptName}-deployment`,
      scriptName,
      versions: [
        { versionId: `${scriptName}-a`, weightBps: 5_000 },
        { versionId: `${scriptName}-b`, weightBps: 5_000 },
      ],
    }))
    const attestations = deployments.flatMap((deployment) =>
      deployment.versions.map((version, index) => ({
        artifactChecksum: (index === 0 ? "a" : "b").repeat(64),
        controlSchemaMax: 5,
        controlSchemaMin: 5,
        outcomePayloadReaderMax: 1,
        outcomePayloadReaderMin: 1,
        scriptName: deployment.scriptName,
        versionId: version.versionId,
      })),
    )
    const proof = await verifyReaderDeploymentBarrier(
      {
        attestations: attestations.reverse(),
        deployments: deployments.reverse(),
        expectedScriptNames: expectedScriptNames.reverse(),
      },
      digest,
    )
    const base = database()
    const activated = await new D1ReaderBarrierStore(base, digest).activate(proof)
    expect(activated.activeDeployments).toHaveLength(256)
    expect(activated.attestations).toHaveLength(512)
    expect(count(base.database, "nozzle_reader_version_attestations")).toBe(512)
    const sizes = base.database
      .prepare(
        `SELECT length(CAST("barrier_json" AS BLOB)) AS "barrier_bytes",
                (SELECT max(length(CAST("attestation_json" AS BLOB)))
                 FROM "nozzle_reader_version_attestations") AS "attestation_bytes"
         FROM "nozzle_reader_barriers"`,
      )
      .get() as { attestation_bytes: number; barrier_bytes: number }
    expect(sizes.attestation_bytes).toBeLessThanOrEqual(65_536)
    expect(sizes.barrier_bytes).toBeLessThanOrEqual(1_048_576)
  })

  it("recovers a committed activation after losing the batch response", async () => {
    const base = database()
    const store = new D1ReaderBarrierStore(new FaultDatabase(base, { loseResponse: true }), digest)
    await expect(store.activate(await capability("lost"))).resolves.toMatchObject({
      protocolVersion: 1,
    })
    expect(count(base.database, "nozzle_reader_barriers")).toBe(1)
  })

  it.each([0, 1, 2, 3])("rolls back a failure at barrier batch statement %i", async (failAt) => {
    const base = database()
    const store = new D1ReaderBarrierStore(new FaultDatabase(base, { failAt }), digest)
    await expect(store.activate(await capability(`rollback-${failAt}`))).rejects.toMatchObject({
      code: "OperationResumeRequiredError",
    })
    expect(count(base.database, "nozzle_reader_version_attestations")).toBe(0)
    expect(count(base.database, "nozzle_reader_barriers")).toBe(0)
    expect(count(base.database, "nozzle_saga_outcome_payload_activations")).toBe(0)
  })

  it("allows one exact concurrent barrier and rejects a contradictory contender", async () => {
    const base = database()
    const store = new D1ReaderBarrierStore(base, digest)
    const outcomes = await Promise.allSettled([
      store.activate(await capability("winner")),
      store.activate(await capability("contender")),
    ])
    expect(outcomes.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1)
    const rejected = outcomes.find((outcome) => outcome.status === "rejected")
    expect(rejected).toMatchObject({
      reason: expect.objectContaining({ code: "OperationInterventionRequiredError" }),
    })
    expect(count(base.database, "nozzle_reader_barriers")).toBe(1)
  })

  it("rejects a contradictory barrier that wins between the initial read and batch", async () => {
    const base = database()
    const store = new D1ReaderBarrierStore(
      new RacingDatabase(base, await capability("racing-winner")),
      digest,
    )
    await expect(store.activate(await capability("racing-loser"))).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
    expect(count(base.database, "nozzle_reader_barriers")).toBe(1)
  })

  it("rejects an immutable partial attestation that contradicts verified evidence", async () => {
    const input = evidence("partial-attestation")
    const conflicting = structuredClone(input)
    ;(conflicting.attestations[0] as { artifactChecksum: string }).artifactChecksum = "f".repeat(64)
    const source = database()
    await new D1ReaderBarrierStore(source, digest).activate(
      await verifyReaderDeploymentBarrier(conflicting, digest),
    )
    const base = database()
    insertAttestations(base.database, rawAttestations(source.database))

    await expect(
      new D1ReaderBarrierStore(base, digest).activate(
        await verifyReaderDeploymentBarrier(input, digest),
      ),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    expect(count(base.database, "nozzle_saga_outcome_payload_activations")).toBe(0)
  })

  it("rejects a partial row that aliases an expected attestation checksum", async () => {
    const source = await activatedBase("partial-checksum-alias")
    const [row] = rawAttestations(source.database)
    const body = JSON.parse(row?.attestation_json as string) as Record<string, unknown>
    const aliased = {
      ...row,
      attestation_json: JSON.stringify({
        ...body,
        scriptName: "unrelated-reader",
        versionId: "unrelated-version",
      }),
      script_name: "unrelated-reader",
      version_id: "unrelated-version",
    }
    const base = database()
    insertAttestations(base.database, [aliased])

    await expect(
      new D1ReaderBarrierStore(base, digest).activate(await capability("partial-checksum-alias")),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
  })

  it("rejects a contradictory or incomplete immutable partial barrier", async () => {
    const contradictorySource = await activatedBase("partial-barrier-other")
    const contradictory = database()
    insertBarrier(contradictory.database, rawBarrierRow(contradictorySource.database))
    await expect(
      new D1ReaderBarrierStore(contradictory, digest).activate(
        await capability("partial-barrier-target"),
      ),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

    const exactSource = await activatedBase("partial-barrier-exact")
    const incomplete = database()
    insertBarrier(incomplete.database, rawBarrierRow(exactSource.database))
    await expect(
      new D1ReaderBarrierStore(incomplete, digest).activate(
        await capability("partial-barrier-exact"),
      ),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
  })

  it("finishes activation from an exact, complete, temporally valid partial barrier", async () => {
    const source = await activatedBase("partial-barrier-resume")
    const base = database()
    insertAttestations(base.database, rawAttestations(source.database))
    insertBarrier(base.database, rawBarrierRow(source.database))

    await expect(
      new D1ReaderBarrierStore(base, digest).activate(await capability("partial-barrier-resume")),
    ).resolves.toMatchObject({ protocolVersion: 1 })
    expect(count(base.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
  })

  it("rejects partial attestations registered in the future or after their barrier", async () => {
    const source = await activatedBase("partial-time")
    const rows = rawAttestations(source.database)
    const future = database()
    insertAttestations(future.database, [
      { ...rows[0], registered_at_ms: Number.MAX_SAFE_INTEGER },
      ...rows.slice(1),
    ])
    await expect(
      new D1ReaderBarrierStore(future, digest).activate(await capability("partial-time")),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

    const afterBarrier = database()
    insertAttestations(afterBarrier.database, rows)
    insertBarrier(afterBarrier.database, {
      ...rawBarrierRow(source.database),
      verified_at_ms: 0,
    })
    await expect(
      new D1ReaderBarrierStore(afterBarrier, digest).activate(await capability("partial-time")),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
  })

  it("reconciles contradictory partial state written between preflight and batch", async () => {
    const input = evidence("partial-race")
    const conflicting = structuredClone(input)
    ;(conflicting.attestations[0] as { artifactChecksum: string }).artifactChecksum = "e".repeat(64)
    const source = database()
    await new D1ReaderBarrierStore(source, digest).activate(
      await verifyReaderDeploymentBarrier(conflicting, digest),
    )
    const [row] = rawAttestations(source.database).filter(
      (candidate) => candidate.script_name === "router-partial-race",
    )
    const base = database()
    const racing = new PartialRacingDatabase(base, () => {
      insertAttestations(base.database, [row as Record<string, unknown>])
    })
    await expect(
      new D1ReaderBarrierStore(racing, digest).activate(
        await verifyReaderDeploymentBarrier(input, digest),
      ),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    expect(count(base.database, "nozzle_reader_barriers")).toBe(0)
  })

  it("rejects fake, cloned, serialized, primitive, and contradictory capabilities", async () => {
    const base = database()
    const store = new D1ReaderBarrierStore(base, digest)
    const proof = await capability("opaque")
    for (const fake of [
      {},
      structuredClone(proof),
      JSON.parse(JSON.stringify(proof)),
      null,
      "reader-barrier",
    ]) {
      await expect(store.activate(fake as ReaderBarrierCapability)).rejects.toMatchObject({
        code: "ConfigurationError",
      })
    }
    await store.activate(proof)
    await expect(store.activate(await capability("other"))).rejects.toMatchObject({
      code: "OperationInterventionRequiredError",
    })
  })

  it("captures caller evidence before hashing begins", async () => {
    const input = evidence("capture")
    let mutated = false
    const mutatingDigest: DigestFunction = async (value) => {
      if (!mutated) {
        mutated = true
        ;(input.expectedScriptNames as string[])[0] = "switched"
        ;(input.attestations[0] as { artifactChecksum: string }).artifactChecksum = "f".repeat(64)
      }
      return digest(value)
    }
    const proof = await verifyReaderDeploymentBarrier(input, mutatingDigest)
    const base = database()
    await expect(
      new D1ReaderBarrierStore(base, mutatingDigest).activate(proof),
    ).resolves.toMatchObject({ expectedScriptNames: ["controller-capture", "router-capture"] })
    expect(mutated).toBe(true)
  })

  it("rejects evidence that cannot be captured and invalid digest boundaries", async () => {
    await expect(
      verifyReaderDeploymentBarrier(new Proxy(evidence(), {}), digest),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    await expect(verifyReaderDeploymentBarrier(evidence(), null as never)).rejects.toThrow(
      /digest function/u,
    )
    await expect(verifyReaderDeploymentBarrier(evidence(), async () => "invalid")).rejects.toThrow(
      /lowercase SHA-256/u,
    )
    await expect(
      verifyReaderDeploymentBarrier(evidence(), async () => "a".repeat(64)),
    ).rejects.toThrow(/checksums must be unique/u)
  })

  it.each([
    ["root type", []],
    ["root shape", { ...evidence(), extra: true }],
    ["expected scripts type", { ...evidence(), expectedScriptNames: {} }],
    ["empty expected scripts", { ...evidence(), expectedScriptNames: [], deployments: [] }],
    [
      "too many expected scripts",
      {
        ...evidence(),
        expectedScriptNames: Array.from({ length: 257 }, (_, index) => `s-${index}`),
      },
    ],
    ["malformed expected script", { ...evidence(), expectedScriptNames: [""] }],
    ["oversized expected script", { ...evidence(), expectedScriptNames: ["x".repeat(256)] }],
    ["unpaired high surrogate", { ...evidence(), expectedScriptNames: ["\ud800"] }],
    ["unpaired low surrogate", { ...evidence(), expectedScriptNames: ["\udc00"] }],
    [
      "duplicate expected script",
      { ...evidence(), expectedScriptNames: ["controller-a", "controller-a"] },
    ],
    ["deployments type", { ...evidence(), deployments: {} }],
    ["deployment count", { ...evidence(), deployments: [evidence().deployments[0]] }],
    [
      "deployment shape",
      {
        ...evidence(),
        deployments: [{ ...evidence().deployments[0], extra: true }, evidence().deployments[1]],
      },
    ],
    [
      "deployment ID",
      {
        ...evidence(),
        deployments: [
          { ...evidence().deployments[0], deploymentId: "" },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "versions type",
      {
        ...evidence(),
        deployments: [{ ...evidence().deployments[0], versions: {} }, evidence().deployments[1]],
      },
    ],
    [
      "versions count",
      {
        ...evidence(),
        deployments: [{ ...evidence().deployments[0], versions: [] }, evidence().deployments[1]],
      },
    ],
    [
      "too many active versions",
      {
        ...evidence(),
        deployments: [
          {
            ...evidence().deployments[0],
            versions: [
              { versionId: "one", weightBps: 3_000 },
              { versionId: "two", weightBps: 3_000 },
              { versionId: "three", weightBps: 4_000 },
            ],
          },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "version shape",
      {
        ...evidence(),
        deployments: [
          {
            ...evidence().deployments[0],
            versions: [{ versionId: "v", weightBps: 10_000, extra: true }],
          },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "version ID",
      {
        ...evidence(),
        deployments: [
          { ...evidence().deployments[0], versions: [{ versionId: "", weightBps: 10_000 }] },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "version weight type",
      {
        ...evidence(),
        deployments: [
          {
            ...evidence().deployments[0],
            versions: [{ versionId: "router-version-a", weightBps: 1.5 }],
          },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "version weight maximum",
      {
        ...evidence(),
        deployments: [
          {
            ...evidence().deployments[0],
            versions: [{ versionId: "router-version-a", weightBps: 10_001 }],
          },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "version weight sum",
      {
        ...evidence(),
        deployments: [
          {
            ...evidence().deployments[0],
            versions: [{ versionId: "router-version-a", weightBps: 9_999 }],
          },
          evidence().deployments[1],
        ],
      },
    ],
    [
      "duplicate active version",
      {
        ...evidence(),
        deployments: [
          evidence().deployments[0],
          {
            ...evidence().deployments[1],
            versions: [
              { versionId: "controller-version-a-old", weightBps: 5_000 },
              { versionId: "controller-version-a-old", weightBps: 5_000 },
            ],
          },
        ],
      },
    ],
    [
      "deployment inventory mismatch",
      {
        ...evidence(),
        deployments: [
          { ...evidence().deployments[0], scriptName: "controller-a" },
          evidence().deployments[1],
        ],
      },
    ],
    ["attestations type", { ...evidence(), attestations: {} }],
    ["attestation count", { ...evidence(), attestations: evidence().attestations.slice(1) }],
    [
      "attestation shape",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], extra: true },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "artifact checksum",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], artifactChecksum: "bad" },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "schema integer",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], controlSchemaMin: 0 },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "schema range",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], controlSchemaMax: 4, controlSchemaMin: 5 },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "schema current support",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], controlSchemaMax: 4 },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "schema minimum above current",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], controlSchemaMax: 6, controlSchemaMin: 6 },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "reader version",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], outcomePayloadReaderMin: 0 },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "reader protocol range",
      {
        ...evidence(),
        attestations: [
          {
            ...evidence().attestations[0],
            outcomePayloadReaderMax: 1,
            outcomePayloadReaderMin: 2,
          },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "reader protocol excludes version one",
      {
        ...evidence(),
        attestations: [
          {
            ...evidence().attestations[0],
            outcomePayloadReaderMax: 2,
            outcomePayloadReaderMin: 2,
          },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "attestation script",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], scriptName: "" },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "inactive attestation",
      {
        ...evidence(),
        attestations: [
          { ...evidence().attestations[0], versionId: "inactive" },
          ...evidence().attestations.slice(1),
        ],
      },
    ],
    [
      "duplicate attestation",
      {
        ...evidence(),
        attestations: [
          evidence().attestations[0],
          evidence().attestations[0],
          evidence().attestations[2],
        ],
      },
    ],
  ] as const)("rejects malformed %s evidence", async (_label, input) => {
    await expect(
      verifyReaderDeploymentBarrier(input as unknown as VerifyReaderBarrierInput, digest),
    ).rejects.toBeInstanceOf(NozzleError)
  })

  it("validates construction and reports an absent activation", async () => {
    const base = database()
    await expect(new D1ReaderBarrierStore(base, digest).get()).resolves.toBeUndefined()
    expect(() => new D1ReaderBarrierStore(null as never, digest)).toThrow(/transactional/u)
    expect(() => new D1ReaderBarrierStore({ prepare() {} } as never, digest)).toThrow(
      /transactional/u,
    )
    expect(() => new D1ReaderBarrierStore(base, null as never)).toThrow(/digest function/u)
  })

  it("rejects malformed partial-barrier query rows and every identity contradiction", async () => {
    const source = await activatedBase("partial-barrier-query")
    const row = rawBarrierRow(source.database)
    const malformed: readonly unknown[] = [
      [],
      new Proxy(row, {}),
      { ...row, verified_at_ms: -1 },
      { ...row, protocol_version: 2 },
      { ...row, barrier_checksum: "9".repeat(64) },
      { ...row, inventory_checksum: "9".repeat(64) },
      { ...row, barrier_json: "{}" },
    ]
    for (const partialBarrierRow of malformed) {
      const base = database()
      await expect(
        new D1ReaderBarrierStore(
          new QueryFaultDatabase(base, { partialBarrierRow }),
          digest,
        ).activate(await capability("partial-barrier-query")),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects malformed partial-attestation results and every row contradiction", async () => {
    const source = await activatedBase("partial-attestation-query")
    const [row] = rawAttestations(source.database)
    const valid = { ...row, observed_at_ms: Date.now() }
    const malformedResults: readonly unknown[] = [
      [],
      { meta: {}, results: [], success: false },
      { meta: {}, results: null, success: true },
      { meta: {}, results: [new Proxy(valid, {})], success: true },
      { meta: {}, results: [{ ...valid, registered_at_ms: -1 }], success: true },
      { meta: {}, results: [{ ...valid, observed_at_ms: -1 }], success: true },
      { meta: {}, results: [{ ...valid, script_name: null }], success: true },
      { meta: {}, results: [{ ...valid, version_id: null }], success: true },
      { meta: {}, results: [{ ...valid, script_name: "not-active" }], success: true },
      { meta: {}, results: [{ ...valid, artifact_checksum: "9".repeat(64) }], success: true },
      { meta: {}, results: [{ ...valid, control_schema_min: 2 }], success: true },
      { meta: {}, results: [{ ...valid, control_schema_max: 4 }], success: true },
      { meta: {}, results: [{ ...valid, outcome_payload_reader_min: 2 }], success: true },
      { meta: {}, results: [{ ...valid, outcome_payload_reader_max: 9 }], success: true },
      { meta: {}, results: [{ ...valid, attestation_checksum: "9".repeat(64) }], success: true },
      { meta: {}, results: [{ ...valid, attestation_json: "{}" }], success: true },
    ]
    for (const attestationResult of malformedResults) {
      const base = database()
      await expect(
        new D1ReaderBarrierStore(
          new QueryFaultDatabase(base, { attestationResult }),
          digest,
        ).activate(await capability("partial-attestation-query")),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects malformed batch metadata without trusting an apparent response", async () => {
    const valid = (changes: number): ControlRunResult => ({ meta: { changes }, success: true })
    const malformed: readonly unknown[] = [
      null,
      [],
      [valid(0), valid(0), valid(0)],
      [null, valid(0), valid(0), valid(0)],
      [{ meta: null, success: true }, valid(0), valid(0), valid(0)],
      [{ meta: { changes: 0 }, success: false }, valid(0), valid(0), valid(0)],
      [{ meta: { changes: 0.5 }, success: true }, valid(0), valid(0), valid(0)],
      [{ meta: { changes: -1 }, success: true }, valid(0), valid(0), valid(0)],
      [valid(513), valid(0), valid(0), valid(0)],
      [valid(0), valid(2), valid(0), valid(0)],
      [valid(0), valid(0), valid(0), valid(1)],
    ]
    for (const [index, results] of malformed.entries()) {
      const base = database()
      const store = new D1ReaderBarrierStore(new MetadataDatabase(base, results), digest)
      await expect(store.activate(await capability(`metadata-${index}`))).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
      expect(count(base.database, "nozzle_reader_barriers")).toBe(0)
    }
  })

  it("rejects malformed persisted barrier heads before trusting attestations", async () => {
    const base = await activatedBase("malformed-head")
    const row = rawBarrierRow(base.database)
    const parsed = JSON.parse(row.barrier_json as string) as Record<string, unknown>
    const malformed = [
      [],
      new Proxy(row, {}),
      { ...row, protocol_version: 2 },
      { ...row, reader_barrier_checksum: null },
      { ...row, reader_barrier_checksum: "bad" },
      { ...row, barrier_checksum: null },
      { ...row, barrier_checksum: "bad" },
      { ...row, inventory_checksum: null },
      { ...row, inventory_checksum: "bad" },
      { ...row, reader_barrier_checksum: "9".repeat(64) },
      { ...row, activated_at_ms: -1 },
      { ...row, verified_at_ms: -1 },
      { ...row, activated_at_ms: 0, verified_at_ms: 1 },
      { ...row, barrier_json: null },
      { ...row, barrier_json: JSON.stringify("x".repeat(1_048_576)) },
      { ...row, barrier_json: "{" },
      { ...row, barrier_json: "[]" },
      { ...row, barrier_json: JSON.stringify({ ...parsed, attestations: {} }) },
    ]
    for (const barrierRow of malformed) {
      await expect(
        new D1ReaderBarrierStore(new QueryFaultDatabase(base, { barrierRow }), digest).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects malformed persisted attestation queries and rows", async () => {
    const base = await activatedBase("malformed-attestation")
    const rows = rawAttestations(base.database)
    const verifiedAtMs = rawBarrierRow(base.database).verified_at_ms as number
    const validResult = { meta: {}, results: rows, success: true }
    const malformedResults: readonly unknown[] = [
      [],
      new Proxy(validResult, {}),
      { ...validResult, success: false },
      { ...validResult, results: null },
      {
        ...validResult,
        results: [new Proxy(rows[0] as Record<string, unknown>, {}), ...rows.slice(1)],
      },
      { ...validResult, results: [{ ...rows[0], registered_at_ms: -1 }, ...rows.slice(1)] },
      {
        ...validResult,
        results: [{ ...rows[0], registered_at_ms: verifiedAtMs + 1 }, ...rows.slice(1)],
      },
      { ...validResult, results: [{ ...rows[0], artifact_checksum: null }, ...rows.slice(1)] },
      { ...validResult, results: [{ ...rows[0], control_schema_max: -1 }, ...rows.slice(1)] },
      { ...validResult, results: [{ ...rows[0], control_schema_min: -1 }, ...rows.slice(1)] },
      {
        ...validResult,
        results: [{ ...rows[0], outcome_payload_reader_min: -1 }, ...rows.slice(1)],
      },
      {
        ...validResult,
        results: [{ ...rows[0], outcome_payload_reader_max: -1 }, ...rows.slice(1)],
      },
      { ...validResult, results: [{ ...rows[0], script_name: null }, ...rows.slice(1)] },
      { ...validResult, results: [{ ...rows[0], version_id: null }, ...rows.slice(1)] },
      { ...validResult, results: [{ ...rows[0], artifact_checksum: "bad" }, ...rows.slice(1)] },
      { ...validResult, results: rows.slice(1) },
      {
        ...validResult,
        results: [{ ...rows[0], attestation_checksum: "9".repeat(64) }, ...rows.slice(1)],
      },
      { ...validResult, results: [{ ...rows[0], attestation_json: "{}" }, ...rows.slice(1)] },
    ]
    for (const attestationResult of malformedResults) {
      await expect(
        new D1ReaderBarrierStore(new QueryFaultDatabase(base, { attestationResult }), digest).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("rejects canonical barrier checksum, inventory, and JSON contradictions", async () => {
    const base = await activatedBase("canonical-contradiction")
    const row = rawBarrierRow(base.database)
    const parsed = JSON.parse(row.barrier_json as string) as Record<string, unknown>
    const reorderedJson = JSON.stringify({
      schemaVersion: parsed.schemaVersion,
      protocolVersion: parsed.protocolVersion,
      expectedScriptNames: parsed.expectedScriptNames,
      attestations: parsed.attestations,
      activeDeployments: parsed.activeDeployments,
    })
    const contradictions = [
      { ...row, barrier_json: reorderedJson },
      {
        ...row,
        barrier_checksum: "9".repeat(64),
        reader_barrier_checksum: "9".repeat(64),
      },
      { ...row, inventory_checksum: "9".repeat(64) },
    ]
    for (const barrierRow of contradictions) {
      await expect(
        new D1ReaderBarrierStore(new QueryFaultDatabase(base, { barrierRow }), digest).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })
})
