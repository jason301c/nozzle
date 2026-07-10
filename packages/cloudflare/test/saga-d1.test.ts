import type { DigestFunction, SagaActionReference } from "@nozzle/core"
import {
  buildInsertPlan,
  type D1BindingValue,
  SchemaRegistry,
  type ScopedRoute,
} from "@nozzle/drizzle"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { describe, expect, it } from "vitest"
import {
  D1SagaAtomicAdapter,
  type D1SagaAtomicApplyInput,
  type D1SagaExpectedEffect,
  type D1SagaTarget,
  type SagaD1Database,
  type SagaD1PreparedStatement,
  type SagaD1Session,
  sealD1SagaExpectedEffect,
} from "../src/saga-d1.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

const projects = sqliteTable("projects", {
  id: text().primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  value: integer().notNull(),
})
const registry = new SchemaRegistry({ partitionKey: "workspaceId", schema: { projects } })
const route: ScopedRoute = {
  bucketId: 42,
  partitionDigestHex: "a".repeat(64),
  partitionValue: "tenant-a",
  routeEpoch: 7,
  shardId: "shard-a",
}
const action: SagaActionReference = {
  actionId: "write.forward",
  artifactChecksum: "b".repeat(64),
  version: 1,
}

class Statement implements SagaD1PreparedStatement {
  readonly sql: string
  values: readonly D1BindingValue[] = []
  readonly session: Session

  constructor(sql: string, session: Session) {
    this.sql = sql
    this.session = session
  }

  bind(...values: readonly D1BindingValue[]): SagaD1PreparedStatement {
    this.values = values
    return this
  }

  async first<T>(): Promise<T | null> {
    if (this.session.firstError) throw this.session.firstError
    return (this.session.rows.shift() ?? null) as T | null
  }
}

class Session implements SagaD1Session {
  batchError: Error | undefined
  firstError: Error | undefined
  readonly rows: unknown[]
  readonly statements: Statement[] = []
  batched: readonly SagaD1PreparedStatement[] = []

  constructor(rows: unknown[]) {
    this.rows = rows
  }

  async batch(statements: readonly SagaD1PreparedStatement[]): Promise<readonly unknown[]> {
    this.batched = statements
    if (this.batchError) throw this.batchError
    return statements.map(() => ({ success: true }))
  }

  prepare(sql: string): SagaD1PreparedStatement {
    const statement = new Statement(sql, this)
    this.statements.push(statement)
    return statement
  }
}

class Database implements SagaD1Database {
  readonly sessions: Session[] = []
  readonly rows: unknown[]
  batchError: Error | undefined
  firstError: Error | undefined

  constructor(rows: unknown[] = []) {
    this.rows = rows
  }

  withSession(constraint: "first-primary"): SagaD1Session {
    expect(constraint).toBe("first-primary")
    const session = new Session(this.rows)
    session.batchError = this.batchError
    session.firstError = this.firstError
    this.sessions.push(session)
    return session
  }
}

function target(database: SagaD1Database): D1SagaTarget {
  return {
    bucketId: route.bucketId,
    database,
    partitionDigest: route.partitionDigestHex,
    routeEpoch: route.routeEpoch,
    schemaId: "schema-v1",
    shardId: route.shardId,
  }
}

function input(database: SagaD1Database): D1SagaAtomicApplyInput {
  return {
    action,
    attemptAcceptanceChecksum: "c".repeat(64),
    attemptId: "attempt-1",
    idempotencyKey: "saga:write:forward",
    inputJson: ' { "value": 1 } ',
    mutations: [
      buildInsertPlan(registry, {
        route,
        schemaId: "schema-v1",
        table: projects,
        values: { id: "project-1", value: 1 },
      }),
    ],
    operationId: "operation-1",
    outputJson: ' { "id": "project-1" } ',
    phase: "forward",
    sagaId: "saga-1",
    stepId: "write",
    target: target(database),
  }
}

function row(expected: D1SagaExpectedEffect, overrides: Record<string, unknown> = {}) {
  return {
    action_key: expected.actionKey,
    applied_at_ms: 123,
    attempt_id: expected.attemptId,
    bucket_id: expected.bucketId,
    control_acceptance_checksum: expected.attemptAcceptanceChecksum,
    format_version: 1,
    idempotency_key: expected.idempotencyKey,
    input_checksum: expected.inputChecksum,
    mutation_checksum: expected.mutationChecksum,
    operation_id: expected.operationId,
    output_checksum: expected.outputChecksum,
    output_json: expected.outputJson,
    partition_digest: expected.partitionDigest,
    phase: expected.phase,
    receipt_checksum: expected.receiptChecksum,
    route_epoch: expected.routeEpoch,
    saga_id: expected.sagaId,
    schema_id: expected.schemaId,
    shard_id: expected.shardId,
    step_id: expected.stepId,
    ...overrides,
  }
}

describe("D1 shard-local saga atomic adapter", () => {
  it("batches trusted mutations before a plain receipt insert and confirms from primary", async () => {
    const database = new Database()
    const applyInput = input(database)
    const expected = await sealD1SagaExpectedEffect(applyInput, digest)
    database.rows.push(row(expected))
    const result = await new D1SagaAtomicAdapter(digest).apply(applyInput)
    expect(result).toMatchObject({ kind: "applied", receipt: expected })
    expect(result.kind === "applied" && result.receipt.appliedAtMs).toBe(123)
    expect(database.sessions).toHaveLength(2)
    const batch = database.sessions[0]?.batched as Statement[]
    expect(batch).toHaveLength(3)
    expect(batch[2]?.sql).toContain('INSERT INTO "nozzle_saga_action_receipts"')
    expect(batch[2]?.sql).not.toContain("ON CONFLICT")
    expect(database.sessions[1]?.statements[0]?.sql).toContain('WHERE "idempotency_key" = ?1')
  })

  it("uses receipt presence as authority after batch errors and absence as not applied", async () => {
    const committedDatabase = new Database()
    const committedInput = input(committedDatabase)
    const expected = await sealD1SagaExpectedEffect(committedInput, digest)
    committedDatabase.batchError = new Error("lost response")
    committedDatabase.rows.push(row(expected))
    await expect(new D1SagaAtomicAdapter(digest).apply(committedInput)).resolves.toMatchObject({
      kind: "applied",
    })

    const absentDatabase = new Database()
    await expect(new D1SagaAtomicAdapter(digest).apply(input(absentDatabase))).resolves.toEqual({
      evidenceJson: '{"kind":"d1_primary_receipt_absent"}',
      kind: "not_applied",
    })
  })

  it("fails observation closed on mismatched, malformed, or unavailable receipts", async () => {
    const database = new Database()
    const applyInput = input(database)
    const expected = await sealD1SagaExpectedEffect(applyInput, digest)
    for (const bad of [
      row(expected, { format_version: 2 }),
      row(expected, { applied_at_ms: -1 }),
      row(expected, { receipt_checksum: "d".repeat(64) }),
    ]) {
      database.rows.push(bad)
      await expect(
        new D1SagaAtomicAdapter(digest).observe(applyInput.target, expected),
      ).resolves.toMatchObject({
        kind: "indeterminate",
      })
    }
    database.firstError = new Error("read failed")
    await expect(
      new D1SagaAtomicAdapter(digest).observe(applyInput.target, expected),
    ).resolves.toMatchObject({
      kind: "indeterminate",
    })
    await expect(
      new D1SagaAtomicAdapter(digest).observe({ ...applyInput.target, shardId: "other" }, expected),
    ).rejects.toThrow(/physical effect target/u)
  })

  it("rejects corrupted expected-effect evidence before querying D1", async () => {
    const database = new Database()
    const applyInput = input(database)
    const expected = await sealD1SagaExpectedEffect(applyInput, digest)
    const corruptions: unknown[] = [
      undefined,
      { ...expected, bucketId: -1 },
      { ...expected, routeEpoch: -1 },
      { ...expected, phase: "invalid" },
      { ...expected, outputJson: ' { "id": "project-1" } ' },
      { ...expected, outputChecksum: "0".repeat(64) },
      { ...expected, receiptChecksum: "0".repeat(64) },
    ]
    for (const corrupted of corruptions) {
      await expect(
        new D1SagaAtomicAdapter(digest).observe(applyInput.target, corrupted as never),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    }
    const compensation = { ...applyInput, phase: "compensation" as const }
    const compensationExpected = await sealD1SagaExpectedEffect(compensation, digest)
    await expect(
      new D1SagaAtomicAdapter(digest).observe(compensation.target, compensationExpected),
    ).resolves.toMatchObject({ kind: "not_applied" })
    expect(database.sessions).toHaveLength(1)
  })

  it("rejects untrusted, empty, oversized, malformed, or cross-route actions", async () => {
    const database = new Database()
    const base = input(database)
    const untrusted = JSON.parse(JSON.stringify(base.mutations[0]))
    const cases: D1SagaAtomicApplyInput[] = [
      undefined as never,
      { ...base, mutations: undefined as never },
      { ...base, mutations: [] },
      { ...base, mutations: Array.from({ length: 65 }, () => base.mutations[0] as never) },
      { ...base, mutations: [untrusted] },
      { ...base, inputJson: "not-json" },
      { ...base, outputJson: "" },
      { ...base, outputJson: JSON.stringify("x".repeat(1024 * 1024)) },
      { ...base, phase: "bad" as never },
      { ...base, attemptAcceptanceChecksum: "bad" },
      { ...base, attemptId: "" },
      { ...base, attemptId: "x".repeat(513) },
      { ...base, target: undefined as never },
      { ...base, target: { ...base.target, bucketId: -1 } },
      { ...base, target: { ...base.target, routeEpoch: -1 } },
      { ...base, target: { ...base.target, database: {} as never } },
      { ...base, target: { ...base.target, partitionDigest: "bad" } },
      { ...base, target: { ...base.target, shardId: "other" } },
    ]
    for (const candidate of cases) {
      await expect(sealD1SagaExpectedEffect(candidate, digest)).rejects.toMatchObject({
        code: expect.stringMatching(/ConfigurationError|UnsafeQueryRequiredError/u),
      })
    }
    await expect(
      sealD1SagaExpectedEffect({ ...base, inputJson: '[{"z":1}]' }, digest),
    ).resolves.toMatchObject({ idempotencyKey: base.idempotencyKey })
    expect(() => sealD1SagaExpectedEffect(base, undefined as never)).toThrow(/digest/u)
    await expect(sealD1SagaExpectedEffect(base, () => "bad")).rejects.toThrow(/lowercase SHA/u)
    expect(() => new D1SagaAtomicAdapter(undefined as never)).toThrow(/digest/u)
  })
})
