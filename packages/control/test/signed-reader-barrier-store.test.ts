import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  createCloudflareWorkerDeploymentClient,
  createReaderDeploymentVerifier,
  type ReaderVersionAttestationStatement,
  readerVersionAttestationSigningBytes,
  type VerifiedReaderDeploymentCapability,
  verifiedReaderDeploymentEvidence,
} from "@nozzle/cloudflare"
import { type DigestFunction, NozzleError } from "@nozzle/core"
import { beforeAll, describe, expect, it, vi } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { controlSchemaSql } from "../src/schema.js"
import { D1SignedReaderBarrierStore } from "../src/signed-reader-barrier-store.js"

const accountId = "a".repeat(32)
const scriptName = "nozzle-controller"
const deploymentId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const versionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const versionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const audience = "nozzle:fictional-control"

let privateKey: CryptoKey
let publicKeyBase64Url: string

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function base64Url(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")
}

beforeAll(async () => {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair
  privateKey = pair.privateKey
  publicKeyBase64Url = base64Url(
    new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)),
  )
})

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

class BatchFaultDatabase implements TransactionalControlDatabase {
  readonly #delegate: DatabaseAdapter
  readonly #failAt: number | undefined
  readonly #loseResponse: boolean
  readonly #results: unknown

  constructor(
    delegate: DatabaseAdapter,
    input: {
      readonly failAt?: number
      readonly loseResponse?: boolean
      readonly results?: unknown
    },
  ) {
    this.#delegate = delegate
    this.#failAt = input.failAt
    this.#loseResponse = input.loseResponse === true
    this.#results = input.results
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#results !== undefined) return this.#results as readonly ControlRunResult[]
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
  readonly serverTimeRow?: unknown
  readonly verificationRow?: unknown
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

  all<T>(): Promise<ControlQueryResult<T>> {
    return this.#delegate.all<T>()
  }

  async first<T>(): Promise<T | null> {
    if (
      this.#sql.includes('FROM "nozzle_reader_barrier_verifications" WHERE') &&
      Object.hasOwn(this.#faults, "verificationRow")
    ) {
      return this.#faults.verificationRow as T | null
    }
    if (
      this.#sql.includes('AS "verified_at_ms"') &&
      !this.#sql.includes("FROM") &&
      Object.hasOwn(this.#faults, "serverTimeRow")
    ) {
      return this.#faults.serverTimeRow as T | null
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

class RacingDatabase implements TransactionalControlDatabase {
  readonly #competitor: VerifiedReaderDeploymentCapability
  readonly #delegate: DatabaseAdapter
  #raced = false

  constructor(delegate: DatabaseAdapter, competitor: VerifiedReaderDeploymentCapability) {
    this.#competitor = competitor
    this.#delegate = delegate
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (!this.#raced) {
      this.#raced = true
      await new D1SignedReaderBarrierStore(this.#delegate, digest).activate(this.#competitor)
    }
    return this.#delegate.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return this.#delegate.prepare(sql)
  }
}

function response(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200 })
}

async function signedCapability(
  suffix = "a",
  artifactOffset = 0,
): Promise<VerifiedReaderDeploymentCapability> {
  const sourceVariant =
    [...suffix].reduce((total, character) => total + character.charCodeAt(0), 0) % 50
  const artifactA = String(1 + artifactOffset).repeat(64)
  const artifactB = String(2 + artifactOffset).repeat(64)
  let time = 1_000
  const client = createCloudflareWorkerDeploymentClient({
    accountId,
    apiToken: `fictional-${suffix}-token`,
    fetch: (async (input: string | URL | Request) => {
      const url = String(input)
      if (url.endsWith("/deployments")) {
        return response({
          errors: [],
          messages: [],
          result: {
            deployments: [
              {
                created_on: "2026-07-12T00:00:00.000Z",
                id: deploymentId,
                strategy: "percentage",
                versions: [
                  { percentage: 75, version_id: versionB },
                  { percentage: 25, version_id: versionA },
                ],
              },
            ],
          },
          success: true,
        })
      }
      const versionId = url.slice(url.lastIndexOf("/") + 1)
      return response({
        errors: [],
        messages: [],
        result: {
          id: versionId,
          resources: { script: { etag: versionId === versionA ? artifactA : artifactB } },
        },
        success: true,
      })
    }) as typeof globalThis.fetch,
    now: () => time++,
  })
  const deployment = await client.getActiveDeployment(scriptName)
  if (deployment.kind !== "complete") throw new Error("Expected deployment evidence.")
  const artifacts = await Promise.all(
    deployment.deployment.versions.map(({ versionId }) =>
      client.getVersionArtifact(scriptName, versionId),
    ),
  )
  const statements = deployment.deployment.versions.map(
    ({ versionId }): ReaderVersionAttestationStatement => ({
      artifactChecksum: versionId === versionA ? artifactA : artifactB,
      audience,
      controlSchemaMax: 6,
      controlSchemaMin: 5,
      expiresAtMs: 2_000 + sourceVariant,
      issuedAtMs: 900 + sourceVariant,
      keyId: "fictional-release-key",
      outcomePayloadReaderMax: 1,
      outcomePayloadReaderMin: 1,
      schemaVersion: 1,
      scriptName,
      versionId,
    }),
  )
  const attestations = await Promise.all(
    statements.map(async (statement) => ({
      signature: base64Url(
        new Uint8Array(
          await crypto.subtle.sign(
            { name: "Ed25519" },
            privateKey,
            readerVersionAttestationSigningBytes(statement),
          ),
        ),
      ),
      statement,
    })),
  )
  const verifier = await createReaderDeploymentVerifier({
    accountId,
    audience,
    maxAttestationValidityMs: 2_000,
    maxObservationAgeMs: 200,
    maxObservationWindowMs: 100,
    now: () => 1_100,
    trustedKeys: [{ keyId: "fictional-release-key", publicKeyBase64Url }],
  })
  return verifier.verify({
    artifactProofs: artifacts.map((artifact) => {
      if (artifact.kind !== "complete") throw new Error("Expected artifact evidence.")
      return artifact.proof
    }),
    attestations,
    deploymentProofs: [deployment.proof],
    expectedScriptNames: [scriptName],
  })
}

interface RawVerificationRow {
  readonly evidence_json: string
  readonly protocol_version: number
  readonly reader_barrier_checksum: string
  readonly verification_checksum: string
  readonly verified_at_ms: number
}

function count(database: DatabaseSync, table: string): number {
  return (database.prepare(`SELECT count(*) AS "count" FROM "${table}"`).get() as { count: number })
    .count
}

function rawVerification(database: DatabaseSync): RawVerificationRow {
  return database
    .prepare(
      `SELECT "protocol_version", "reader_barrier_checksum", "verification_checksum",
              "evidence_json", "verified_at_ms"
       FROM "nozzle_reader_barrier_verifications"`,
    )
    .get() as unknown as RawVerificationRow
}

function copyRows(
  source: DatabaseSync,
  target: DatabaseSync,
  table: string,
  columns: readonly string[],
): void {
  const names = columns.map((column) => `"${column}"`).join(", ")
  const placeholders = columns.map(() => "?").join(", ")
  const rows = source.prepare(`SELECT ${names} FROM "${table}"`).all() as Record<
    string,
    SQLInputValue
  >[]
  const statement = target.prepare(`INSERT INTO "${table}" (${names}) VALUES (${placeholders})`)
  for (const row of rows) statement.run(...columns.map((column) => row[column] as SQLInputValue))
}

function copySignedPartial(source: DatabaseSync, target: DatabaseSync): void {
  copyRows(source, target, "nozzle_reader_version_attestations", [
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
  ])
  copyRows(source, target, "nozzle_reader_barriers", [
    "protocol_version",
    "barrier_checksum",
    "inventory_checksum",
    "barrier_json",
    "verified_at_ms",
  ])
  copyRows(source, target, "nozzle_reader_barrier_verifications", [
    "protocol_version",
    "reader_barrier_checksum",
    "verification_checksum",
    "evidence_json",
    "verified_at_ms",
  ])
}

function canonical(value: unknown): string {
  const normalize = (entry: unknown): unknown => {
    if (Array.isArray(entry)) return entry.map(normalize)
    if (entry !== null && typeof entry === "object") {
      const result: Record<string, unknown> = {}
      for (const key of Object.keys(entry).sort()) {
        result[key] = normalize((entry as Record<string, unknown>)[key])
      }
      return result
    }
    return entry
  }
  return JSON.stringify(normalize(value))
}

async function domainDigest(domain: string, value: string): Promise<string> {
  const parts = [domain, value].map((part) => new TextEncoder().encode(part))
  const framed = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0))
  const view = new DataView(framed.buffer)
  let offset = 0
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    framed.set(part, offset)
    offset += part.byteLength
  }
  return digest(framed)
}

function sourceFromBody(body: Record<string, unknown>): Record<string, unknown> {
  return {
    accountId: body.accountId,
    artifacts: body.artifacts,
    attestations: body.attestations,
    audience: body.audience,
    deployments: body.deployments,
    expectedScriptNames: body.expectedScriptNames,
    observedFromMs: body.observedFromMs,
    observedThroughMs: body.observedThroughMs,
    schemaVersion: body.sourceSchemaVersion,
    verifiedAtMs: body.sourceVerifiedAtMs,
  }
}

async function rewriteVerification(
  row: RawVerificationRow,
  mutate: (body: Record<string, unknown>) => void,
  recomputeSource = true,
): Promise<RawVerificationRow> {
  const body = JSON.parse(row.evidence_json) as Record<string, unknown>
  mutate(body)
  if (recomputeSource) {
    body.signedEvidenceChecksum = await domainDigest(
      "nozzle.reader-barrier-signed-evidence.v1",
      canonical(sourceFromBody(body)),
    )
  }
  const evidenceJson = canonical(body)
  return {
    evidence_json: evidenceJson,
    protocol_version: row.protocol_version,
    reader_barrier_checksum: body.readerBarrierChecksum as string,
    verification_checksum: await domainDigest(
      "nozzle.reader-barrier-verification.v1",
      evidenceJson,
    ),
    verified_at_ms: body.verifiedAtMs as number,
  }
}

async function withFirstClone<T>(snapshot: unknown, action: () => Promise<T>): Promise<T> {
  const spy = vi
    .spyOn(globalThis, "structuredClone")
    .mockImplementationOnce(() => snapshot as never)
  try {
    return await action()
  } finally {
    spy.mockRestore()
  }
}

describe("signed reader-barrier store", () => {
  it("atomically persists, reloads, and exactly replays signed deployment evidence", async () => {
    const database = new DatabaseAdapter()
    try {
      const store = new D1SignedReaderBarrierStore(database, digest)
      await expect(store.get()).resolves.toBeUndefined()
      const capability = await signedCapability()
      const activated = await store.activate(capability)

      expect(activated).toMatchObject({
        accountId,
        activeDeployments: [
          {
            deploymentId,
            scriptName,
            versions: [
              { versionId: versionA, weightBps: 2_500 },
              { versionId: versionB, weightBps: 7_500 },
            ],
          },
        ],
        audience,
        protocolVersion: 1,
        signedEvidenceChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
        verificationChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
      })
      expect(Object.isFrozen(activated)).toBe(true)
      await expect(store.get()).resolves.toEqual(activated)
      await expect(store.activate(capability)).resolves.toEqual(activated)

      const persisted = database.database
        .prepare(
          `SELECT "evidence_json", "verification_checksum"
           FROM "nozzle_reader_barrier_verifications"`,
        )
        .get() as { evidence_json: string; verification_checksum: string }
      const evidence = JSON.parse(persisted.evidence_json) as Record<string, unknown>
      expect(evidence).toMatchObject({
        accountId,
        audience,
        protocolVersion: 1,
        sourceSchemaVersion: 1,
      })
      expect(JSON.stringify(evidence)).toBe(persisted.evidence_json)
      expect(persisted.verification_checksum).toBe(activated.verificationChecksum)
      expect(
        database.database
          .prepare(`SELECT count(*) AS "count" FROM "nozzle_reader_version_attestations"`)
          .get(),
      ).toEqual({ count: 2 })
      expect(
        database.database
          .prepare(`SELECT count(*) AS "count" FROM "nozzle_reader_barrier_verifications"`)
          .get(),
      ).toEqual({ count: 1 })
      expect(
        database.database
          .prepare(`SELECT count(*) AS "count" FROM "nozzle_saga_outcome_payload_activations"`)
          .get(),
      ).toEqual({ count: 1 })
    } finally {
      database.close()
    }
  })

  it("recovers an exact commit after losing the five-statement batch response", async () => {
    const database = new DatabaseAdapter()
    try {
      const store = new D1SignedReaderBarrierStore(
        new BatchFaultDatabase(database, { loseResponse: true }),
        digest,
      )
      await expect(store.activate(await signedCapability("lost-response"))).resolves.toMatchObject({
        protocolVersion: 1,
      })
      expect(count(database.database, "nozzle_reader_barrier_verifications")).toBe(1)
      expect(count(database.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
    } finally {
      database.close()
    }
  })

  it.each([
    0, 1, 2, 3, 4,
  ])("rolls back a failure at signed activation batch statement %i", async (failAt) => {
    const database = new DatabaseAdapter()
    try {
      const store = new D1SignedReaderBarrierStore(
        new BatchFaultDatabase(database, { failAt }),
        digest,
      )
      await expect(
        store.activate(await signedCapability(`rollback-${failAt}`)),
      ).rejects.toMatchObject({ code: "OperationResumeRequiredError" })
      expect(count(database.database, "nozzle_reader_version_attestations")).toBe(0)
      expect(count(database.database, "nozzle_reader_barriers")).toBe(0)
      expect(count(database.database, "nozzle_reader_barrier_verifications")).toBe(0)
      expect(count(database.database, "nozzle_saga_outcome_payload_activations")).toBe(0)
    } finally {
      database.close()
    }
  })

  it("converges exact contenders and rejects signed or normalized contradictory winners", async () => {
    const exactDatabase = new DatabaseAdapter()
    const contradictoryDatabase = new DatabaseAdapter()
    const racingDatabase = new DatabaseAdapter()
    try {
      const exact = await signedCapability("exact-contender")
      const exactStore = new D1SignedReaderBarrierStore(exactDatabase, digest)
      const exactOutcomes = await Promise.all([
        exactStore.activate(exact),
        exactStore.activate(exact),
      ])
      expect(exactOutcomes[0]).toEqual(exactOutcomes[1])

      const contradictoryStore = new D1SignedReaderBarrierStore(contradictoryDatabase, digest)
      await contradictoryStore.activate(await signedCapability("signed-winner"))
      await expect(
        contradictoryStore.activate(await signedCapability("signed-contender")),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const racingStore = new D1SignedReaderBarrierStore(
        new RacingDatabase(racingDatabase, await signedCapability("racing-winner", 2)),
        digest,
      )
      await expect(
        racingStore.activate(await signedCapability("racing-contender")),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      expect(count(racingDatabase.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
    } finally {
      exactDatabase.close()
      contradictoryDatabase.close()
      racingDatabase.close()
    }
  })

  it("rejects every malformed signed batch result before trusting its apparent outcome", async () => {
    const valid = (changes: number): ControlRunResult => ({ meta: { changes }, success: true })
    const five = (): ControlRunResult[] => [valid(0), valid(0), valid(0), valid(0), valid(0)]
    const malformed: unknown[] = [null, [], five().slice(0, 4)]
    const badResult = five()
    badResult[0] = null as never
    malformed.push(badResult)
    const badMeta = five()
    badMeta[0] = { meta: null as never, success: true }
    malformed.push(badMeta)
    const unsuccessful = five()
    unsuccessful[0] = { meta: { changes: 0 }, success: false }
    malformed.push(unsuccessful)
    for (const changes of [0.5, -1]) {
      const result = five()
      result[0] = { meta: { changes }, success: true }
      malformed.push(result)
    }
    for (const [index, changes] of [
      [0, 513],
      [1, 2],
      [4, 1],
    ] as const) {
      const result = five()
      result[index] = valid(changes)
      malformed.push(result)
    }

    for (const [index, results] of malformed.entries()) {
      const database = new DatabaseAdapter()
      try {
        const store = new D1SignedReaderBarrierStore(
          new BatchFaultDatabase(database, { results }),
          digest,
        )
        await expect(
          store.activate(await signedCapability(`metadata-${index}`)),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
        expect(count(database.database, "nozzle_reader_barrier_verifications")).toBe(0)
      } finally {
        database.close()
      }
    }
  })

  it("finishes an exact immutable partial verification and rejects a contradictory one", async () => {
    const source = new DatabaseAdapter()
    const exact = new DatabaseAdapter()
    const contradictory = new DatabaseAdapter()
    try {
      const capability = await signedCapability("partial-source")
      const expected = await new D1SignedReaderBarrierStore(source, digest).activate(capability)
      copySignedPartial(source.database, exact.database)
      copySignedPartial(source.database, contradictory.database)

      await expect(new D1SignedReaderBarrierStore(exact, digest).get()).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
      await expect(
        new D1SignedReaderBarrierStore(exact, digest).activate(capability),
      ).resolves.toMatchObject({
        barrierChecksum: expected.barrierChecksum,
        signedEvidenceChecksum: expected.signedEvidenceChecksum,
        verificationChecksum: expected.verificationChecksum,
      })
      await expect(
        new D1SignedReaderBarrierStore(contradictory, digest).activate(
          await signedCapability("partial-contender"),
        ),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      expect(count(contradictory.database, "nozzle_saga_outcome_payload_activations")).toBe(0)
    } finally {
      source.close()
      exact.close()
      contradictory.close()
    }
  })

  it("fails closed on missing, malformed, or regressed authoritative D1 time", async () => {
    const faults: readonly unknown[] = [null, [], { verified_at_ms: "now" }, { verified_at_ms: -1 }]
    for (const [index, serverTimeRow] of faults.entries()) {
      const database = new DatabaseAdapter()
      try {
        const store = new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(database, { serverTimeRow }),
          digest,
        )
        await expect(store.activate(await signedCapability(`time-${index}`))).rejects.toMatchObject(
          {
            code: "OperationInterventionRequiredError",
          },
        )
      } finally {
        database.close()
      }
    }

    const regressed = new DatabaseAdapter()
    try {
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(regressed, { serverTimeRow: { verified_at_ms: 1_000 } }),
          digest,
        ).activate(await signedCapability("regressed-time")),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    } finally {
      regressed.close()
    }
  })

  it("captures opaque evidence and rejects every noncanonical JSON boundary", async () => {
    const capability = await signedCapability("canonical-source")
    const trusted = structuredClone(
      verifiedReaderDeploymentEvidence(capability),
    ) as unknown as Record<string, unknown>
    const sparse = new Array(2) as unknown[]
    sparse[0] = scriptName
    let deep: Record<string, unknown> = { leaf: true }
    for (let index = 0; index < 18; index += 1) deep = { nested: deep }
    const tooManyMembers = Object.fromEntries(
      Array.from({ length: 16_385 }, (_, index) => [`member${index}`, null]),
    )
    const malformed: readonly unknown[] = [
      [],
      { ...trusted, extra: true },
      { ...trusted, schemaVersion: 2 },
      { ...trusted, accountId: "not-an-account" },
      { ...trusted, accountId: "A".repeat(32) },
      { ...trusted, audience: "" },
      { ...trusted, observedFromMs: -1 },
      { ...trusted, observedFromMs: 1.5 },
      { ...trusted, artifacts: [() => undefined] },
      { ...trusted, artifacts: [new Date(0)] },
      { ...trusted, artifacts: [{ missing: undefined }] },
      { ...trusted, artifacts: [deep] },
      { ...trusted, artifacts: [tooManyMembers] },
      { ...trusted, expectedScriptNames: sparse },
      { ...trusted, expectedScriptNames: Array.from({ length: 16_385 }, () => null) },
      { ...trusted, audience: "unpaired-high-\ud800" },
      { ...trusted, audience: "unpaired-low-\udc00" },
      { ...trusted, artifacts: [{ "bad-key-\udc00": null }] },
    ]

    for (const snapshot of malformed) {
      const database = new DatabaseAdapter()
      try {
        await expect(
          withFirstClone(snapshot, () =>
            new D1SignedReaderBarrierStore(database, digest).activate(capability),
          ),
        ).rejects.toMatchObject({ code: "ConfigurationError" })
      } finally {
        database.close()
      }
    }

    const cloneFailure = new DatabaseAdapter()
    const spy = vi.spyOn(globalThis, "structuredClone").mockImplementationOnce(() => {
      throw new Error("Injected clone failure")
    })
    try {
      await expect(
        new D1SignedReaderBarrierStore(cloneFailure, digest).activate(capability),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    } finally {
      spy.mockRestore()
      cloneFailure.close()
    }

    const paired = new DatabaseAdapter()
    try {
      await expect(
        withFirstClone({ ...trusted, audience: "paired-😀" }, () =>
          new D1SignedReaderBarrierStore(paired, digest).activate(capability),
        ),
      ).resolves.toMatchObject({ audience: "paired-😀" })
    } finally {
      paired.close()
    }
  })

  it("enforces independent byte limits for signed source and persisted verification evidence", async () => {
    const capability = await signedCapability("evidence-size")
    const trusted = structuredClone(
      verifiedReaderDeploymentEvidence(capability),
    ) as unknown as Record<string, unknown>
    const currentAudience = trusted.audience as string
    const baseBytes = new TextEncoder().encode(JSON.stringify(trusted)).byteLength
    const snapshotAtBytes = (bytes: number): Record<string, unknown> => ({
      ...trusted,
      audience: "x".repeat(bytes - baseBytes + currentAudience.length),
    })

    const oversizedSource = new DatabaseAdapter()
    try {
      await expect(
        withFirstClone(snapshotAtBytes(1_048_577), () =>
          new D1SignedReaderBarrierStore(oversizedSource, digest).activate(capability),
        ),
      ).rejects.toThrow(/signed reader evidence exceeds/u)
    } finally {
      oversizedSource.close()
    }

    const oversizedVerification = new DatabaseAdapter()
    try {
      await expect(
        withFirstClone(snapshotAtBytes(1_048_575), () =>
          new D1SignedReaderBarrierStore(oversizedVerification, digest).activate(capability),
        ),
      ).rejects.toThrow(/reader-barrier verification exceeds/iu)
    } finally {
      oversizedVerification.close()
    }
  })

  it("rejects invalid source digests across both short-circuit paths", async () => {
    for (const malformedDigest of [
      async () => null as never,
      async () => "A".repeat(64),
    ] satisfies readonly DigestFunction[]) {
      const database = new DatabaseAdapter()
      try {
        await expect(
          new D1SignedReaderBarrierStore(database, malformedDigest).activate(
            await signedCapability("bad-digest"),
          ),
        ).rejects.toMatchObject({ code: "ConfigurationError" })
      } finally {
        database.close()
      }
    }
  })

  it("rejects malformed persisted verification rows and every outer/body contradiction", async () => {
    const database = new DatabaseAdapter()
    try {
      await new D1SignedReaderBarrierStore(database, digest).activate(
        await signedCapability("persisted-malformed"),
      )
      const row = rawVerification(database.database)
      const withBody = (
        mutate: (body: Record<string, unknown>) => void,
        pretty = false,
      ): RawVerificationRow => {
        const body = JSON.parse(row.evidence_json) as Record<string, unknown>
        mutate(body)
        return { ...row, evidence_json: pretty ? JSON.stringify(body, null, 2) : canonical(body) }
      }
      const malformed: readonly unknown[] = [
        [],
        new Proxy(row, {}),
        { ...row, protocol_version: 2 },
        { ...row, reader_barrier_checksum: null },
        { ...row, reader_barrier_checksum: "bad" },
        { ...row, verification_checksum: null },
        { ...row, verification_checksum: "bad" },
        { ...row, verified_at_ms: "now" },
        { ...row, verified_at_ms: -1 },
        { ...row, evidence_json: null },
        { ...row, evidence_json: "" },
        { ...row, evidence_json: "x".repeat(1_048_577) },
        { ...row, evidence_json: "{" },
        { ...row, evidence_json: "[]" },
        withBody(() => undefined, true),
        withBody((body) => {
          body.schemaVersion = 2
        }),
        withBody((body) => {
          body.sourceSchemaVersion = 2
        }),
        withBody((body) => {
          body.protocolVersion = 2
        }),
        withBody((body) => {
          body.readerBarrierChecksum = "f".repeat(64)
        }),
        withBody((body) => {
          body.verifiedAtMs = (body.verifiedAtMs as number) + 1
        }),
        withBody((body) => {
          body.accountId = null
        }),
        withBody((body) => {
          body.accountId = "bad"
        }),
        withBody((body) => {
          body.audience = null
        }),
        withBody((body) => {
          body.audience = ""
        }),
        withBody((body) => {
          body.sourceVerifiedAtMs = (body.verifiedAtMs as number) + 1
        }),
        withBody((body) => {
          body.signedEvidenceChecksum = null
        }),
        withBody((body) => {
          body.signedEvidenceChecksum = "bad"
        }),
        withBody((body) => {
          body.unexpected = true
        }),
        withBody((body) => {
          body.observedFromMs = -1
        }),
      ]

      for (const verificationRow of malformed) {
        await expect(
          new D1SignedReaderBarrierStore(
            new QueryFaultDatabase(database, { verificationRow }),
            digest,
          ).get(),
        ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      }
    } finally {
      database.close()
    }
  })

  it("detects either persisted checksum contradiction and invalid reload digests", async () => {
    const database = new DatabaseAdapter()
    try {
      await new D1SignedReaderBarrierStore(database, digest).activate(
        await signedCapability("persisted-checksum"),
      )
      const row = rawVerification(database.database)
      const body = JSON.parse(row.evidence_json) as Record<string, unknown>
      body.signedEvidenceChecksum = "f".repeat(64)
      const badSource = { ...row, evidence_json: canonical(body) }
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(database, { verificationRow: badSource }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(database, {
            verificationRow: { ...row, verification_checksum: "f".repeat(64) },
          }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const badSignedDigest: DigestFunction = async (input) =>
        new TextDecoder().decode(input).includes("nozzle.reader-barrier-signed-evidence.v1")
          ? (null as never)
          : digest(input)
      await expect(
        new D1SignedReaderBarrierStore(database, badSignedDigest).get(),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    } finally {
      database.close()
    }
  })

  it("reconciles signed evidence exhaustively against normalized activation state", async () => {
    const empty = new DatabaseAdapter()
    const primary = new DatabaseAdapter()
    const different = new DatabaseAdapter()
    try {
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(empty, { verificationRow: {} }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const primaryCapability = await signedCapability("reconcile-primary")
      await new D1SignedReaderBarrierStore(primary, digest).activate(primaryCapability)
      await new D1SignedReaderBarrierStore(different, digest).activate(
        await signedCapability("reconcile-different", 2),
      )
      const primaryRow = rawVerification(primary.database)
      const differentRow = rawVerification(different.database)

      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(primary, { verificationRow: null }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(primary, { verificationRow: differentRow }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const invalidNormalized = await rewriteVerification(primaryRow, (body) => {
        body.expectedScriptNames = []
      })
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(primary, { verificationRow: invalidNormalized }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const differentSource = await rewriteVerification(
        differentRow,
        (body) => {
          body.readerBarrierChecksum = primaryRow.reader_barrier_checksum
          body.verifiedAtMs = primaryRow.verified_at_ms
        },
        false,
      )
      await expect(
        new D1SignedReaderBarrierStore(
          new QueryFaultDatabase(primary, { verificationRow: differentSource }),
          digest,
        ).get(),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })

      const store = new D1SignedReaderBarrierStore(primary, digest)
      const get = vi.spyOn(store, "get").mockResolvedValueOnce(undefined)
      try {
        await expect(store.activate(primaryCapability)).rejects.toMatchObject({
          code: "OperationInterventionRequiredError",
        })
      } finally {
        get.mockRestore()
      }
    } finally {
      empty.close()
      primary.close()
      different.close()
    }
  })

  it("rejects configuration without a real database, digest, or verified capability", async () => {
    const database = new DatabaseAdapter()
    try {
      expect(() => new D1SignedReaderBarrierStore(null as never, digest)).toThrow(/transactional/u)
      expect(() => new D1SignedReaderBarrierStore({ prepare() {} } as never, digest)).toThrow(
        /transactional/u,
      )
      expect(() => new D1SignedReaderBarrierStore(database, null as never)).toThrow(/digest/u)
      await expect(
        new D1SignedReaderBarrierStore(database, digest).activate({}),
      ).rejects.toBeInstanceOf(NozzleError)
    } finally {
      database.close()
    }
  })
})
