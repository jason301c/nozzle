import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import { type LeaseProof, leaseProof } from "@nozzle/core"
import { beforeEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlDatabase,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { controlSchemaSql } from "../src/schema.js"

class StatementAdapter implements ControlStatement {
  readonly #statement: StatementSync
  #values: Record<string, SQLInputValue> = {}

  constructor(statement: StatementSync) {
    this.#statement = statement
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#statement.setAllowBareNamedParameters(false)
    this.#statement.setReadBigInts(false)
    this.#values = {}
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] as ControlBindingValue
      if (typeof value === "boolean") {
        this.#values[`?${index + 1}`] = value ? 1 : 0
        continue
      }
      this.#values[`?${index + 1}`] = value instanceof ArrayBuffer ? new Uint8Array(value) : value
    }
    return this
  }

  async first<T>(): Promise<T | null> {
    return (this.#statement.get(this.#values) as T | undefined) ?? null
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return {
      meta: {},
      results: this.#statement.all(this.#values) as T[],
      success: true,
    }
  }

  async run(): Promise<ControlRunResult> {
    const result = this.#statement.run(this.#values)
    return { meta: { changes: Number(result.changes) }, success: true }
  }
}

class DatabaseAdapter implements ControlDatabase {
  readonly database = new DatabaseSync(":memory:")

  constructor() {
    this.database.exec(controlSchemaSql())
  }

  close(): void {
    this.database.close()
  }

  prepare(sql: string): ControlStatement {
    return new StatementAdapter(this.database.prepare(sql))
  }
}

class ScriptedStatement implements ControlStatement {
  readonly #changes: unknown
  readonly #row: unknown

  constructor(row: unknown, changes: unknown) {
    this.#row = row
    this.#changes = changes
  }

  bind(): ControlStatement {
    return this
  }

  async first<T>(): Promise<T | null> {
    return this.#row as T | null
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: [], success: true }
  }

  async run(): Promise<ControlRunResult> {
    return { meta: { changes: this.#changes }, success: true }
  }
}

class ScriptedDatabase implements ControlDatabase {
  constructor(
    readonly row: unknown,
    readonly changes: unknown,
  ) {}

  prepare(): ControlStatement {
    return new ScriptedStatement(this.row, this.changes)
  }
}

function leaseRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    acquisition_id: "acquisition-a",
    expires_at_ms: 1_000,
    fencing_token: 1,
    holder_id: "controller-a",
    lease_key: "lease-a",
    now_ms: 100,
    ...overrides,
  }
}

describe("D1LeaseStore", () => {
  let database: DatabaseAdapter
  let store: D1LeaseStore

  beforeEach(() => {
    database = new DatabaseAdapter()
    store = new D1LeaseStore(database)
    return () => database.close()
  })

  it("acquires, replays, contends, authorizes, renews, and releases with monotonic tokens", async () => {
    const first = await store.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:migration",
      ttlMs: 10_000,
    })
    expect(first).toMatchObject({ acquired: true, replayed: false, record: { fencingToken: 1 } })
    if (!first.acquired) throw new Error("fixture acquisition failed")
    const proof = leaseProof(first.record)
    await expect(store.authorize(proof)).resolves.toEqual(first.record)
    await expect(store.authorizeAt(proof)).resolves.toMatchObject({
      record: first.record,
      serverTimeMs: expect.any(Number),
    })

    await expect(
      store.acquire({
        acquisitionId: "acquisition-a",
        holderId: "controller-a",
        leaseKey: "fleet-a:migration",
        ttlMs: 10_000,
      }),
    ).resolves.toMatchObject({ acquired: true, replayed: true, record: { fencingToken: 1 } })
    await expect(
      store.acquire({
        acquisitionId: "acquisition-b",
        holderId: "controller-b",
        leaseKey: "fleet-a:migration",
        ttlMs: 10_000,
      }),
    ).resolves.toMatchObject({ acquired: false, currentFencingToken: 1, reason: "held" })

    await expect(store.renew({ proof, ttlMs: 20_000 })).resolves.toMatchObject({
      renewed: true,
      record: { fencingToken: 1 },
    })
    await expect(store.release({ proof })).resolves.toMatchObject({ released: true })
    await expect(store.authorize(proof)).rejects.toMatchObject({
      code: "OperationResumeRequiredError",
    })

    const second = await store.acquire({
      acquisitionId: "acquisition-b",
      holderId: "controller-b",
      leaseKey: "fleet-a:migration",
      ttlMs: 10_000,
    })
    expect(second).toMatchObject({ acquired: true, replayed: false, record: { fencingToken: 2 } })
    await expect(store.renew({ proof, ttlMs: 10_000 })).resolves.toEqual({
      reason: "fenced",
      renewed: false,
    })
    await expect(store.release({ proof })).resolves.toEqual({ reason: "fenced", released: false })
  })

  it("allows exactly one winner under concurrent acquisition", async () => {
    const decisions = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        store.acquire({
          acquisitionId: `acquisition-${index}`,
          holderId: `controller-${index}`,
          leaseKey: "fleet-a:reconcile",
          ttlMs: 10_000,
        }),
      ),
    )
    expect(decisions.filter((decision) => decision.acquired)).toHaveLength(1)
    expect(decisions.filter((decision) => !decision.acquired)).toHaveLength(7)
    await expect(store.get("fleet-a:reconcile")).resolves.toMatchObject({ fencingToken: 1 })
  })

  it("rejects expired proofs and reacquires with a higher fencing token", async () => {
    const acquired = await store.acquire({
      acquisitionId: "acquisition-a",
      holderId: "controller-a",
      leaseKey: "fleet-a:move",
      ttlMs: 10_000,
    })
    if (!acquired.acquired) throw new Error("fixture acquisition failed")
    const proof = leaseProof(acquired.record)
    database.database
      .prepare(`UPDATE "nozzle_leases" SET "expires_at_ms" = 0 WHERE "lease_key" = ?`)
      .run("fleet-a:move")

    await expect(store.authorize(proof)).rejects.toThrow("expired")
    await expect(store.renew({ proof, ttlMs: 10_000 })).resolves.toEqual({
      reason: "expired",
      renewed: false,
    })
    const next = await store.acquire({
      acquisitionId: "acquisition-b",
      holderId: "controller-b",
      leaseKey: "fleet-a:move",
      ttlMs: 10_000,
    })
    expect(next).toMatchObject({ acquired: true, record: { fencingToken: 2 } })
  })

  it("validates bindings, inputs, and stale proofs with stable errors", async () => {
    expect(() => new D1LeaseStore(null as never)).toThrow("database binding")
    await expect(
      store.acquire({ acquisitionId: "", holderId: "holder", leaseKey: "lease", ttlMs: 1 }),
    ).rejects.toMatchObject({ code: "ConfigurationError" })
    const stale: LeaseProof = {
      acquisitionId: "missing",
      fencingToken: 1,
      holderId: "missing",
      leaseKey: "missing",
    }
    await expect(store.authorize(stale)).rejects.toMatchObject({
      code: "OperationResumeRequiredError",
    })
    await expect(store.get("missing")).resolves.toBeUndefined()
  })

  it("fails closed on malformed D1 time, lease rows, and mutation metadata", async () => {
    for (const row of [
      null,
      leaseRow({ now_ms: -1 }),
      leaseRow({ now_ms: 1.5 }),
      leaseRow({
        acquisition_id: null,
        expires_at_ms: null,
        fencing_token: null,
        holder_id: "impossible",
        lease_key: null,
      }),
      leaseRow({ lease_key: "other" }),
      leaseRow({ fencing_token: 0 }),
      leaseRow({ expires_at_ms: -1 }),
      leaseRow({ acquisition_id: null }),
    ]) {
      await expect(
        new D1LeaseStore(new ScriptedDatabase(row, 1)).get("lease-a"),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
    const absent = leaseRow({
      acquisition_id: null,
      expires_at_ms: null,
      fencing_token: null,
      holder_id: null,
      lease_key: null,
    })
    for (const changes of [undefined, -1, 1.5, 2]) {
      await expect(
        new D1LeaseStore(new ScriptedDatabase(absent, changes)).acquire({
          acquisitionId: "acquisition-a",
          holderId: "controller-a",
          leaseKey: "lease-a",
          ttlMs: 100,
        }),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
  })

  it("bounds compare-and-swap retries for acquisition, renewal, and release", async () => {
    const absent = leaseRow({
      acquisition_id: null,
      expires_at_ms: null,
      fencing_token: null,
      holder_id: null,
      lease_key: null,
    })
    const failingAcquire = new D1LeaseStore(new ScriptedDatabase(absent, 0))
    await expect(
      failingAcquire.acquire({
        acquisitionId: "acquisition-a",
        holderId: "controller-a",
        leaseKey: "lease-a",
        ttlMs: 100,
      }),
    ).rejects.toThrow("acquisition exceeded")

    const proof: LeaseProof = {
      acquisitionId: "acquisition-a",
      fencingToken: 1,
      holderId: "controller-a",
      leaseKey: "lease-a",
    }
    const failingUpdate = new D1LeaseStore(new ScriptedDatabase(leaseRow(), 0))
    await expect(failingUpdate.renew({ proof, ttlMs: 2_000 })).rejects.toThrow("renewal exceeded")
    await expect(failingUpdate.release({ proof })).rejects.toThrow("release exceeded")
  })
})
