import { env } from "cloudflare:test"
import { beforeAll, describe, expect, it } from "vitest"
import { D1SagaHistoryReader } from "../src/saga-history.js"

declare global {
  namespace Cloudflare {
    interface Env {
      DB: D1Database
    }
  }
}

async function run(sql: string, ...values: unknown[]): Promise<void> {
  await env.DB.prepare(sql)
    .bind(...values)
    .run()
}

beforeAll(async () => {
  for (const statement of [
    `CREATE TABLE "nozzle_operations" (
      "operation_id" TEXT PRIMARY KEY, "environment_id" TEXT, "input_checksum" TEXT,
      "plan_checksum" TEXT, "status" TEXT, "updated_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_sagas" (
      "saga_id" TEXT PRIMARY KEY, "operation_id" TEXT, "descriptor_checksum" TEXT,
      "input_checksum" TEXT, "state_version" INTEGER, "status" TEXT,
      "last_effect_id" TEXT, "record_checksum" TEXT, "updated_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_audit_log" (
      "environment_id" TEXT, "sequence" INTEGER, "event_hash" TEXT, "event_json" TEXT
    )`,
    `CREATE TABLE "nozzle_operation_transitions" (
      "transition_id" TEXT PRIMARY KEY, "operation_id" TEXT, "step_id" TEXT,
      "from_record_json" TEXT, "to_record_json" TEXT, "from_operation_status" TEXT,
      "to_operation_status" TEXT, "audit_event_hash" TEXT, "fencing_token" INTEGER,
      "lease_key" TEXT, "holder_id" TEXT, "acquisition_id" TEXT, "created_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_irreversible_authorization_receipts" (
      "transition_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "authorization_id" TEXT,
      "authorization_checksum" TEXT, "classified_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_operation_effects" (
      "effect_id" TEXT PRIMARY KEY, "transition_id" TEXT, "operation_id" TEXT,
      "step_id" TEXT, "resource_kind" TEXT, "resource_id" TEXT, "effect_kind" TEXT,
      "from_state_version" INTEGER, "to_state_version" INTEGER, "evidence_checksum" TEXT,
      "record_checksum" TEXT, "record_json" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "created_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_saga_action_attempts" (
      "attempt_id" TEXT PRIMARY KEY, "causal_attempt_id" TEXT, "saga_id" TEXT,
      "operation_id" TEXT, "operation_step_id" TEXT, "saga_step_id" TEXT, "phase" TEXT,
      "purpose" TEXT, "action_key" TEXT, "idempotency_key" TEXT, "input_checksum" TEXT,
      "input_json" TEXT, "acceptance_checksum" TEXT, "lease_key" TEXT, "holder_id" TEXT,
      "acquisition_id" TEXT, "fencing_token" INTEGER, "accepted_at_ms" INTEGER
    )`,
    `CREATE TABLE "nozzle_saga_action_attempt_protocols" (
      "attempt_id" TEXT PRIMARY KEY, "protocol_version" INTEGER, "classified_at_ms" INTEGER
    )`,
  ]) {
    await env.DB.prepare(statement).run()
  }
  await run(
    `INSERT INTO "nozzle_operations" VALUES (?, ?, ?, ?, ?, ?)`,
    "history-operation",
    "history-environment",
    "operation-input",
    "operation-plan",
    "running",
    11,
  )
  await run(
    `INSERT INTO "nozzle_sagas" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    "history-saga",
    "history-operation",
    "saga-descriptor",
    "saga-input",
    2,
    "failed",
    "effect-2",
    "saga-record-2",
    12,
  )
  await run(
    `INSERT INTO "nozzle_audit_log" VALUES (?, ?, ?, ?)`,
    "history-environment",
    1,
    "audit-1",
    '{"sequence":1}',
  )
  await run(
    `INSERT INTO "nozzle_operation_transitions" VALUES
     (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    "transition-1",
    "history-operation",
    "saga:init",
    '{"state":"running"}',
    '{"state":"succeeded"}',
    "running",
    "running",
    "audit-1",
    1,
    "saga:history-saga",
    "history-holder",
    "history-acquisition",
    1,
  )
  for (let version = 0; version <= 2; version += 1) {
    await run(
      `INSERT INTO "nozzle_operation_effects" VALUES
       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      `effect-${version}`,
      "transition-1",
      "history-operation",
      version === 0 ? "saga:init" : "saga:forward:a",
      "saga",
      "history-saga",
      version === 0 ? "create" : "action:forward:success",
      version === 0 ? null : version - 1,
      version,
      `evidence-${version}`,
      `record-${version}`,
      JSON.stringify({ stateVersion: version }),
      "saga:history-saga",
      "history-holder",
      "history-acquisition",
      1,
      version,
    )
  }
})

describe("real workerd D1 saga history paging", () => {
  it("captures and walks the stable terminal anchor with D1 query results", async () => {
    const reader = new D1SagaHistoryReader(env.DB)
    const anchor = await reader.captureAnchor("history-operation", "history-saga")
    expect(anchor).toMatchObject({
      auditHeadSequence: 1,
      operationTransitionCount: 1,
      sagaAttemptCount: 0,
      sagaEffectCount: 3,
      sagaStateVersion: 2,
    })
    await expect(reader.assertAnchorCurrent(anchor)).resolves.toBeUndefined()
    await expect(reader.auditPage(anchor)).resolves.toMatchObject({ complete: true })
    await expect(reader.transitionPage(anchor)).resolves.toMatchObject({ complete: true })
    const effects = await reader.effectPage(anchor)
    expect(effects).toMatchObject({ complete: false, nextCursor: 1 })
    await expect(reader.effectPage(anchor, effects.nextCursor as number)).resolves.toMatchObject({
      complete: true,
      nextCursor: null,
    })
    await expect(reader.attemptIdentityPage(anchor)).resolves.toEqual({
      complete: true,
      nextCursor: null,
      rows: [],
    })
  })
})
