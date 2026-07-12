import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  type ReaderVersionAttestationStatement,
  readerVersionAttestationSigningBytes,
} from "@nozzle/cloudflare"
import { type DigestFunction, NozzleError } from "@nozzle/core"
import { beforeAll, describe, expect, it } from "vitest"
import {
  createReaderDeploymentController,
  type ReaderDeploymentControllerOptions,
} from "../src/controller.js"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { controlSchemaSql } from "../src/schema.js"

const accountId = "a".repeat(32)
const audience = "nozzle:fictional-controller"
const scriptName = "nozzle-reader"
const deploymentId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const versionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const artifactChecksum = "1".repeat(64)

let privateKey: CryptoKey
let publicKeyBase64Url: string

const digest: DigestFunction = async (input) => {
  const bytes = new Uint8Array(input.byteLength)
  bytes.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))
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
    for (const [index, value] of values.entries()) {
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

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status })
}

interface RecordedFetch {
  readonly calls: readonly Readonly<{ authorization: string | null; url: string }>[]
  readonly fetch: typeof globalThis.fetch
}

function recordedFetch(input: {
  readonly artifactStatus?: number
  readonly deploymentStatus?: number
  readonly driftAfterRequest?: number
  readonly nowTick: () => void
}): RecordedFetch {
  const calls: { authorization: string | null; url: string }[] = []
  const fetchImplementation: typeof globalThis.fetch = async (request, init) => {
    const signal = init?.signal
    if (signal?.aborted === true) throw new DOMException("Aborted", "AbortError")
    const url = String(request)
    calls.push({ authorization: new Headers(init?.headers).get("authorization"), url })
    input.nowTick()
    if (url.endsWith("/deployments")) {
      const status = input.deploymentStatus ?? 200
      if (status !== 200) return response({ errors: [], messages: [], success: false }, status)
      const drifted =
        input.driftAfterRequest !== undefined && calls.length >= input.driftAfterRequest
      return response({
        errors: [],
        messages: [],
        result: {
          deployments: [
            {
              created_on: "2026-07-12T00:00:00.000Z",
              id: drifted ? "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee" : deploymentId,
              strategy: "percentage",
              versions: [{ percentage: 100, version_id: versionId }],
            },
          ],
        },
        success: true,
      })
    }
    const status = input.artifactStatus ?? 200
    if (status !== 200) return response({ errors: [], messages: [], success: false }, status)
    return response({
      errors: [],
      messages: [],
      result: {
        id: versionId,
        resources: { script: { etag: artifactChecksum } },
      },
      success: true,
    })
  }
  return { calls, fetch: fetchImplementation }
}

async function options(
  database: TransactionalControlDatabase,
  input: Readonly<{
    artifactStatus?: number
    deploymentStatus?: number
    driftAfterRequest?: number
    maxExternalSubrequests?: number
  }> = {},
): Promise<{
  readonly controller: ReaderDeploymentControllerOptions
  readonly requests: RecordedFetch
}> {
  const baseTimeMs = Date.now() - 1_000
  let tick = 0
  const requests = recordedFetch({
    ...(input.artifactStatus === undefined ? {} : { artifactStatus: input.artifactStatus }),
    ...(input.deploymentStatus === undefined ? {} : { deploymentStatus: input.deploymentStatus }),
    ...(input.driftAfterRequest === undefined
      ? {}
      : { driftAfterRequest: input.driftAfterRequest }),
    nowTick: () => {
      tick += 1
    },
  })
  const statement: ReaderVersionAttestationStatement = {
    artifactChecksum,
    audience,
    controlSchemaMax: 6,
    controlSchemaMin: 5,
    expiresAtMs: baseTimeMs + 60_000,
    issuedAtMs: baseTimeMs - 100,
    keyId: "fictional-controller-release-key",
    outcomePayloadReaderMax: 1,
    outcomePayloadReaderMin: 1,
    schemaVersion: 1,
    scriptName,
    versionId,
  }
  const signature = base64Url(
    new Uint8Array(
      await crypto.subtle.sign(
        { name: "Ed25519" },
        privateKey,
        readerVersionAttestationSigningBytes(statement),
      ),
    ),
  )
  return {
    controller: {
      accountId,
      apiToken: "fictional-controller-token",
      attestations: [{ signature, statement }],
      audience,
      database,
      digest,
      expectedScriptNames: [scriptName],
      fetch: requests.fetch,
      maxAttestationValidityMs: 120_000,
      maxExternalSubrequests: input.maxExternalSubrequests ?? 50,
      maxObservationAgeMs: 1_000,
      maxObservationWindowMs: 1_000,
      maxStabilityWindowMs: 5_000,
      now: () => baseTimeMs + tick * 10,
      trustedKeys: [{ keyId: statement.keyId, publicKeyBase64Url }],
    },
    requests,
  }
}

function count(database: DatabaseSync, table: string): number {
  return (database.prepare(`SELECT count(*) AS "count" FROM "${table}"`).get() as { count: number })
    .count
}

describe("user-owned reader deployment controller", () => {
  it("owns the three-round activation and continuous exact compatibility check", async () => {
    const database = new DatabaseAdapter()
    try {
      const fixture = await options(database)
      const controller = await createReaderDeploymentController(fixture.controller)
      const activated = await controller.activate()
      expect(activated).toMatchObject({
        accountId,
        audience,
        stability: { maxStabilityWindowMs: 5_000 },
      })
      expect(Object.isFrozen(activated)).toBe(true)
      expect(fixture.requests.calls).toHaveLength(6)
      await expect(
        controller.assertCompatible({ signal: new AbortController().signal }),
      ).resolves.toMatchObject({ accountId })
      expect(fixture.requests.calls).toHaveLength(8)
      expect(
        fixture.requests.calls.every(
          ({ authorization }) => authorization === "Bearer fictional-controller-token",
        ),
      ).toBe(true)
      expect(JSON.stringify(activated)).not.toContain("fictional-controller-token")
      expect(count(database.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
    } finally {
      database.close()
    }
  })

  it("fails closed when the immediate post-activation observation drifts", async () => {
    const database = new DatabaseAdapter()
    try {
      const fixture = await options(database, { driftAfterRequest: 5 })
      const controller = await createReaderDeploymentController(fixture.controller)
      await expect(controller.activate()).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
      expect(count(database.database, "nozzle_saga_outcome_payload_activations")).toBe(1)
    } finally {
      database.close()
    }
  })

  it("classifies retryable and intervention observations without provider or token leakage", async () => {
    for (const [field, status, code] of [
      ["deploymentStatus", 429, "OperationResumeRequiredError"],
      ["deploymentStatus", 403, "OperationInterventionRequiredError"],
      ["artifactStatus", 429, "OperationResumeRequiredError"],
    ] as const) {
      const database = new DatabaseAdapter()
      try {
        const fixture = await options(database, { [field]: status })
        const controller = await createReaderDeploymentController(fixture.controller)
        const error = await controller.activate().catch((reason: unknown) => reason)
        expect(error).toMatchObject({ code })
        expect(String(error)).not.toContain("fictional-controller-token")
        expect(count(database.database, "nozzle_saga_outcome_payload_activations")).toBe(0)
      } finally {
        database.close()
      }
    }
  })

  it("checks the declared external-subrequest capability before any API read", async () => {
    const database = new DatabaseAdapter()
    try {
      const fixture = await options(database, { maxExternalSubrequests: 8 })
      const controller = await createReaderDeploymentController(fixture.controller)
      await expect(controller.activate()).rejects.toThrow(/requires at most 9/u)
      expect(fixture.requests.calls).toHaveLength(0)

      const assertionFixture = await options(database, { maxExternalSubrequests: 2 })
      const assertionController = await createReaderDeploymentController(
        assertionFixture.controller,
      )
      await expect(assertionController.assertCompatible()).rejects.toThrow(/requires at most 3/u)
      expect(assertionFixture.requests.calls).toHaveLength(0)
    } finally {
      database.close()
    }
  })

  it("requires an activation and maps an aborted observation to a resumable result", async () => {
    const database = new DatabaseAdapter()
    try {
      const fixture = await options(database)
      const controller = await createReaderDeploymentController(fixture.controller)
      await expect(controller.assertCompatible()).rejects.toThrow(/has not been activated/u)
      const abort = new AbortController()
      abort.abort()
      await expect(controller.assertCompatible({ signal: abort.signal })).rejects.toMatchObject({
        code: "OperationResumeRequiredError",
      })
      await expect(controller.assertCompatible({ signal: null } as never)).rejects.toThrow(
        /abort signal/u,
      )
      await expect(controller.assertCompatible({ extra: true } as never)).rejects.toThrow(
        /unsupported shape/u,
      )
    } finally {
      database.close()
    }
  })

  it("captures configuration and rejects malformed shapes and bounds before construction", async () => {
    const database = new DatabaseAdapter()
    try {
      const fixture = await options(database)
      const valid = fixture.controller
      const sparse = new Array(2) as string[]
      sparse[0] = scriptName
      const malformed: readonly unknown[] = [
        null,
        [],
        { ...valid, extra: true },
        { ...valid, expectedScriptNames: [] },
        {
          ...valid,
          expectedScriptNames: Array.from({ length: 257 }, (_, index) => `reader-${index}`),
        },
        { ...valid, expectedScriptNames: [""] },
        { ...valid, expectedScriptNames: ["x".repeat(256)] },
        { ...valid, expectedScriptNames: ["unpaired-\ud800"] },
        { ...valid, expectedScriptNames: [scriptName, scriptName] },
        { ...valid, expectedScriptNames: sparse },
        { ...valid, expectedScriptNames: [() => undefined] },
        { ...valid, attestations: [() => undefined] },
        { ...valid, attestations: [] },
        { ...valid, attestations: Array.from({ length: 513 }, () => valid.attestations[0]) },
        { ...valid, trustedKeys: [() => undefined] },
        { ...valid, maxExternalSubrequests: 0 },
        { ...valid, maxExternalSubrequests: 10_000_001 },
        { ...valid, maxExternalSubrequests: 1.5 },
        { ...valid, maxStabilityWindowMs: 0 },
        { ...valid, maxStabilityWindowMs: 300_001 },
        { ...valid, maxStabilityWindowMs: 1.5 },
        { ...valid, fetch: "fetch" },
        { ...valid, now: "now" },
        { ...valid, database: null },
        { ...valid, digest: null },
      ]
      for (const candidate of malformed) {
        await expect(
          createReaderDeploymentController(candidate as ReaderDeploymentControllerOptions),
        ).rejects.toBeInstanceOf(NozzleError)
      }

      const { fetch: _fetch, now: _now, ...withoutRuntimeOverrides } = valid
      await expect(createReaderDeploymentController(withoutRuntimeOverrides)).resolves.toEqual({
        activate: expect.any(Function),
        assertCompatible: expect.any(Function),
      })
    } finally {
      database.close()
    }
  })
})
