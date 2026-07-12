import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import {
  CONTROL_SCHEMA_STATEMENTS,
  controlSchemaSql,
  controlSchemaVersionTwoSql,
} from "../src/schema.js"

const operationId = "operation-auth"
const stepId = "step-auth"
const leaseKey = "lease-auth"
const planChecksum = "plan-checksum"
const inputChecksum = "input-checksum"

const stepPlan = Object.freeze({
  activation: "required",
  checkpoint: "irreversible",
  completionRole: "work",
  dependsOn: [],
  effectProtocol: "saga_receipt",
  idempotencyKey: "step-idempotency",
  inputChecksum,
  leaseKey,
  postconditionChecksum: "postcondition",
  preconditionChecksum: "precondition",
  recoveryInstructions: "Inspect the exact provider receipt.",
  retryClassification: "reconcile_first",
  stepId,
})

const operationPlan = Object.freeze({
  capabilitySnapshotChecksum: "capability",
  idempotencyKey: "operation-idempotency",
  inputChecksum: "operation-input",
  operationId,
  operationType: "test:authorization.v1",
  planChecksum,
  schemaVersion: 1,
  steps: [stepPlan],
})

const pendingRecord = JSON.stringify({
  costCounters: {},
  progressCounters: {},
  startedAttempts: 0,
  state: "pending",
})

function authorization(
  overrides: Readonly<Record<string, unknown>> = {},
): Readonly<Record<string, unknown>> {
  return {
    actorChecksum: "actor-checksum",
    authorizationChecksum: "authorization-checksum",
    authorizationId: "authorization-id",
    decisionChecksum: "decision-checksum",
    fencingToken: 1,
    holderId: "holder-auth",
    leaseAcquisitionId: "acquisition-auth",
    leaseKey,
    operationId,
    planChecksum,
    sealedAtServerTimeMs: 1,
    schemaVersion: 1,
    stepId,
    stepInputChecksum: inputChecksum,
    ...overrides,
  }
}

function runningRecord(
  body: Readonly<Record<string, unknown>> | null = authorization(),
  checksum = "authorization-checksum",
  attemptId = "attempt-auth",
): string {
  return JSON.stringify({
    activeAttemptId: attemptId,
    authorizationChecksum: checksum,
    costCounters: {},
    fencingToken: 1,
    ...(body === null ? {} : { irreversibleAuthorization: body }),
    progressCounters: {},
    startedAttempts: 1,
    state: "running",
  })
}

function failedRecord(body = authorization(), checksum = "authorization-checksum"): string {
  return JSON.stringify({
    authorizationChecksum: checksum,
    costCounters: {},
    errorChecksum: "retryable-error",
    fencingToken: 1,
    irreversibleAuthorization: body,
    lastAttemptId: "attempt-auth",
    progressCounters: {},
    startedAttempts: 1,
    state: "retryable_failed",
  })
}

function legacyFailedRecord(checksum = "authorization-checksum"): string {
  return JSON.stringify({
    authorizationChecksum: checksum,
    costCounters: {},
    errorChecksum: "retryable-error",
    fencingToken: 1,
    lastAttemptId: "attempt-auth",
    progressCounters: {},
    startedAttempts: 1,
    state: "retryable_failed",
  })
}

function databaseAtVersion(version: 2 | 3): DatabaseSync {
  const database = new DatabaseSync(":memory:")
  database.exec("PRAGMA foreign_keys = OFF;")
  database.exec(version === 2 ? controlSchemaVersionTwoSql() : controlSchemaSql())
  return database
}

function seedOperation(
  database: DatabaseSync,
  recordJson = pendingRecord,
  selectedStepPlan: Readonly<Record<string, unknown>> = stepPlan,
): void {
  const selectedOperationPlan = { ...operationPlan, steps: [selectedStepPlan] }
  database
    .prepare(
      `INSERT INTO "nozzle_operations"
       ("operation_id", "environment_id", "operation_type", "idempotency_scope",
        "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
        "capability_snapshot_checksum", "capability_snapshot_json", "required_shards_json",
        "status", "created_at_ms", "updated_at_ms")
       VALUES (?, 'production', 'test:authorization.v1', 'test', 'operation-idempotency',
         'operation-input', '{}', ?, ?, 'capability', '{}', '[]', 'running', 1, 1)`,
    )
    .run(operationId, planChecksum, JSON.stringify(selectedOperationPlan))
  const record = JSON.parse(recordJson) as {
    readonly fencingToken?: number
    readonly state: string
  }
  database
    .prepare(
      `INSERT INTO "nozzle_operation_steps"
       ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json", "record_json",
        "state", "fencing_token", "updated_at_ms")
       VALUES (?, ?, 'step-idempotency', ?, ?, ?, ?, ?, 1)`,
    )
    .run(
      operationId,
      stepId,
      leaseKey,
      JSON.stringify(selectedStepPlan),
      recordJson,
      record.state,
      record.fencingToken ?? null,
    )
  database
    .prepare(
      `INSERT INTO "nozzle_leases"
       ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
        "updated_at_ms")
       VALUES (?, 'holder-auth', 'acquisition-auth', 1, 9000000000000000, 1)`,
    )
    .run(leaseKey)
}

function insertTransition(
  database: DatabaseSync,
  input: {
    readonly fromRecord?: string
    readonly id?: string
    readonly toRecord: string
  },
): void {
  const id = input.id ?? "transition-auth"
  database
    .prepare(
      `INSERT INTO "nozzle_operation_transitions"
       ("transition_id", "operation_id", "step_id", "from_record_json", "to_record_json",
        "from_operation_status", "to_operation_status", "audit_event_hash", "fencing_token",
        "lease_key", "holder_id", "acquisition_id", "created_at_ms")
       VALUES (?, ?, ?, ?, ?, 'running', 'running', ?, 1, ?, 'holder-auth',
         'acquisition-auth', 10)`,
    )
    .run(
      id,
      operationId,
      stepId,
      input.fromRecord ?? pendingRecord,
      input.toRecord,
      `audit-${id}`,
      leaseKey,
    )
}

function persistStep(
  database: DatabaseSync,
  transitionId: string,
  fromRecord: string,
  toRecord: string,
): void {
  const record = JSON.parse(toRecord) as { readonly fencingToken?: number; readonly state: string }
  database
    .prepare(
      `UPDATE "nozzle_operation_steps"
       SET "record_json" = ?, "state" = ?, "fencing_token" = ?, "updated_at_ms" = 10
       WHERE "operation_id" = ? AND "step_id" = ? AND "record_json" = ?
         AND EXISTS (
           SELECT 1 FROM "nozzle_operation_transitions"
           WHERE "transition_id" = ? AND "from_record_json" = ? AND "to_record_json" = ?
         )`,
    )
    .run(
      toRecord,
      record.state,
      record.fencingToken ?? null,
      operationId,
      stepId,
      fromRecord,
      transitionId,
      fromRecord,
      toRecord,
    )
}

function versionThreeStart(): number {
  return CONTROL_SCHEMA_STATEMENTS.findIndex((statement) =>
    statement.startsWith('CREATE TABLE IF NOT EXISTS "nozzle_irreversible_authorization_receipts"'),
  )
}

function mapperIndex(): number {
  return CONTROL_SCHEMA_STATEMENTS.findIndex((statement) =>
    statement.startsWith(
      'CREATE TRIGGER IF NOT EXISTS "nozzle_control_irreversible_authorization_receipt_classify_v3"',
    ),
  )
}

function insertProviderEffect(database: DatabaseSync, attemptId = "attempt-auth"): void {
  database
    .prepare(
      `INSERT INTO "nozzle_provider_attempts"
       ("attempt_id", "operation_id", "step_id", "target_checksum", "actor_checksum",
        "purpose", "endpoint", "mutating", "request_checksum", "acceptance_checksum",
        "lease_key", "holder_id", "acquisition_id", "fencing_token", "accepted_at_ms")
       VALUES (?, ?, ?, 'target', 'actor', 'effect', '/accounts/example/d1/database', 1,
         'request', 'acceptance', ?, 'holder-auth', 'acquisition-auth', 1, 11)`,
    )
    .run(attemptId, operationId, stepId, leaseKey)
}

function insertSagaEffect(database: DatabaseSync, attemptId = "attempt-auth"): void {
  database
    .prepare(
      `INSERT INTO "nozzle_saga_action_attempts"
       ("attempt_id", "causal_attempt_id", "saga_id", "operation_id", "operation_step_id",
        "saga_step_id", "phase", "purpose", "action_key", "idempotency_key",
        "input_checksum", "input_json", "acceptance_checksum", "lease_key", "holder_id",
        "acquisition_id", "fencing_token", "accepted_at_ms")
       VALUES (?, NULL, 'saga-auth', ?, ?, 'auth', 'forward', 'effect',
         'auth.forward@1:artifact', 'step-idempotency', ?, '{}', 'acceptance', ?,
         'holder-auth', 'acquisition-auth', 1, 11)`,
    )
    .run(attemptId, operationId, stepId, inputChecksum, leaseKey)
}

function isolateAuthorizationSagaGuard(database: DatabaseSync): void {
  database.exec(`DROP TRIGGER "nozzle_control_saga_attempt_insert_v2";
    DROP TRIGGER "nozzle_control_saga_protocol_classify_v2";`)
}

describe("control schema irreversible authorization protocol", () => {
  it("accepts an exact full authorization only after v3 and derives an immutable receipt", () => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      const record = runningRecord()
      expect(() => insertTransition(database, { toRecord: record })).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "transition_id", "authorization_id", "authorization_checksum",
                    "protocol_version", "classified_at_ms"
             FROM "nozzle_irreversible_authorization_receipts"`,
          )
          .get(),
      ).toEqual({
        authorization_checksum: "authorization-checksum",
        authorization_id: "authorization-id",
        classified_at_ms: 10,
        protocol_version: 2,
        transition_id: "transition-auth",
      })
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_irreversible_authorization_receipts"
             SET "authorization_checksum" = 'other'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_RECEIPT_IMMUTABLE")
      expect(() =>
        database.prepare(`DELETE FROM "nozzle_irreversible_authorization_receipts"`).run(),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_RECEIPT_IMMUTABLE")
    } finally {
      database.close()
    }
  })

  it("rejects checksum-only dispatch after v3 publication", () => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      expect(() => insertTransition(database, { toRecord: runningRecord(null) })).toThrow(
        "NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_REQUIRED",
      )
    } finally {
      database.close()
    }
  })

  it("quarantines a checksum-only retryable step after v3 publication", () => {
    const database = databaseAtVersion(3)
    try {
      const legacyFailed = legacyFailedRecord()
      seedOperation(database, legacyFailed)
      const replacement = authorization({
        authorizationChecksum: "replacement-checksum",
        authorizationId: "replacement-id",
        decisionChecksum: "replacement-decision",
      })
      expect(() =>
        insertTransition(database, {
          fromRecord: legacyFailed,
          id: "transition-legacy-retry",
          toRecord: runningRecord(replacement, "replacement-checksum", "attempt-retry"),
        }),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_REQUIRED")
    } finally {
      database.close()
    }
  })

  it("rejects full bodies before publication while classifying old-worker dispatch as protocol one", () => {
    const database = databaseAtVersion(2)
    try {
      seedOperation(database)
      const start = versionThreeStart()
      const mapper = mapperIndex()
      expect(start).toBeGreaterThan(0)
      expect(mapper).toBeGreaterThan(start)
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(start, mapper + 1)) {
        database.exec(statement)
      }

      expect(() =>
        insertTransition(database, {
          id: "transition-legacy",
          toRecord: runningRecord(null),
        }),
      ).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "authorization_id", "protocol_version"
             FROM "nozzle_irreversible_authorization_receipts"
             WHERE "transition_id" = 'transition-legacy'`,
          )
          .get(),
      ).toEqual({ authorization_id: null, protocol_version: 1 })
      expect(() =>
        insertTransition(database, {
          id: "transition-new-worker-too-early",
          toRecord: runningRecord(),
        }),
      ).toThrow(/NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_(?:FENCED|REQUIRED)/u)

      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(mapper + 1)) database.exec(statement)
      expect(
        database
          .prepare(
            `SELECT "schema_version" FROM "nozzle_control_schema_versions"
             ORDER BY "schema_version"`,
          )
          .all(),
      ).toEqual([
        { schema_version: 1 },
        { schema_version: 2 },
        { schema_version: 3 },
        { schema_version: 4 },
        { schema_version: 5 },
      ])
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_irreversible_authorization_receipts"
             WHERE "transition_id" = 'transition-legacy'`,
          )
          .get(),
      ).toEqual({ protocol_version: 1 })
    } finally {
      database.close()
    }
  })

  it("backfills a preexisting v2 irreversible dispatch conservatively as protocol one", () => {
    const database = databaseAtVersion(2)
    try {
      seedOperation(database)
      insertTransition(database, { toRecord: runningRecord(null) })
      database.exec(controlSchemaSql())
      expect(
        database
          .prepare(
            `SELECT "authorization_id", "authorization_checksum", "protocol_version"
             FROM "nozzle_irreversible_authorization_receipts"`,
          )
          .get(),
      ).toEqual({
        authorization_checksum: "authorization-checksum",
        authorization_id: null,
        protocol_version: 1,
      })
    } finally {
      database.close()
    }
  })

  it("classifies an old-worker checksum-only retry during the prepublication window", () => {
    const database = databaseAtVersion(2)
    try {
      const legacyFailed = legacyFailedRecord()
      seedOperation(database, legacyFailed)
      const start = versionThreeStart()
      const mapper = mapperIndex()
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(start, mapper + 1)) {
        database.exec(statement)
      }
      expect(() =>
        insertTransition(database, {
          fromRecord: legacyFailed,
          id: "transition-migration-retry",
          toRecord: runningRecord(null, "replacement-checksum", "attempt-retry"),
        }),
      ).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "authorization_id", "authorization_checksum", "protocol_version"
             FROM "nozzle_irreversible_authorization_receipts"
             WHERE "transition_id" = 'transition-migration-retry'`,
          )
          .get(),
      ).toEqual({
        authorization_checksum: "replacement-checksum",
        authorization_id: null,
        protocol_version: 1,
      })
    } finally {
      database.close()
    }
  })

  it("requires the current protocol-two dispatch receipt before external effect acceptance", () => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      const running = runningRecord()
      insertTransition(database, { toRecord: running })
      persistStep(database, "transition-auth", pendingRecord, running)

      expect(() => insertProviderEffect(database)).not.toThrow()
      isolateAuthorizationSagaGuard(database)
      expect(() => insertSagaEffect(database)).not.toThrow()
    } finally {
      database.close()
    }
  })

  it("quarantines a legacy checksum-only running step from post-v3 external dispatch", () => {
    const database = databaseAtVersion(2)
    try {
      seedOperation(database)
      const legacyRunning = runningRecord(null)
      insertTransition(database, { toRecord: legacyRunning })
      persistStep(database, "transition-auth", pendingRecord, legacyRunning)
      database.exec(controlSchemaSql())

      expect(() => insertProviderEffect(database)).toThrow(
        "NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_RECEIPT_REQUIRED",
      )
      isolateAuthorizationSagaGuard(database)
      expect(() => insertSagaEffect(database)).toThrow(
        "NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_RECEIPT_REQUIRED",
      )
    } finally {
      database.close()
    }
  })

  it.each([
    ["unknown body field", { unexpected: "field" }, "authorization-checksum"],
    ["schema version", { schemaVersion: 2 }, "authorization-checksum"],
    ["actor type", { actorChecksum: 1 }, "authorization-checksum"],
    ["authorization ID", { authorizationId: "" }, "authorization-checksum"],
    ["operation", { operationId: "other-operation" }, "authorization-checksum"],
    ["plan", { planChecksum: "other-plan" }, "authorization-checksum"],
    ["step", { stepId: "other-step" }, "authorization-checksum"],
    ["step input", { stepInputChecksum: "other-input" }, "authorization-checksum"],
    ["lease", { leaseKey: "other-lease" }, "authorization-checksum"],
    ["holder", { holderId: "other-holder" }, "authorization-checksum"],
    ["acquisition", { leaseAcquisitionId: "other-acquisition" }, "authorization-checksum"],
    ["fence", { fencingToken: 2 }, "authorization-checksum"],
    ["non-integer fence", { fencingToken: 1.5 }, "authorization-checksum"],
    ["future seal time", { sealedAtServerTimeMs: 9000000000000000 }, "authorization-checksum"],
    ["non-integer seal time", { sealedAtServerTimeMs: 1.5 }, "authorization-checksum"],
    ["top-level checksum", {}, "different-checksum"],
    ["body size", { actorChecksum: "x".repeat(65_536) }, "authorization-checksum"],
  ] as const)("rejects an authorization with a mismatched %s", (_label, overrides, checksum) => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      expect(() =>
        insertTransition(database, {
          toRecord: runningRecord(authorization(overrides), checksum),
        }),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_FENCED")
    } finally {
      database.close()
    }
  })

  it("rejects duplicate authorization keys even when every required key is present", () => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      const duplicated = runningRecord().replace(
        '"actorChecksum":"actor-checksum"',
        '"actorChecksum":"actor-checksum","actorChecksum":"duplicate"',
      )
      expect(() => insertTransition(database, { toRecord: duplicated })).toThrow(
        "NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_FENCED",
      )
    } finally {
      database.close()
    }
  })

  it("preserves a body on progress and permits replacement only on retry dispatch", () => {
    const database = databaseAtVersion(3)
    try {
      seedOperation(database)
      const firstRunning = runningRecord()
      insertTransition(database, { id: "transition-first", toRecord: firstRunning })
      persistStep(database, "transition-first", pendingRecord, firstRunning)

      const firstFailed = failedRecord()
      insertTransition(database, {
        fromRecord: firstRunning,
        id: "transition-failed",
        toRecord: firstFailed,
      })
      persistStep(database, "transition-failed", firstRunning, firstFailed)

      const removedBody = JSON.stringify({
        authorizationChecksum: "authorization-checksum",
        costCounters: {},
        errorChecksum: "retryable-error",
        fencingToken: 1,
        lastAttemptId: "attempt-auth",
        progressCounters: {},
        startedAttempts: 1,
        state: "failed",
      })
      expect(() =>
        insertTransition(database, {
          fromRecord: firstFailed,
          id: "transition-removes-body",
          toRecord: removedBody,
        }),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_NOT_PRESERVED")

      const replacement = authorization({
        authorizationChecksum: "replacement-checksum",
        authorizationId: "replacement-id",
        decisionChecksum: "replacement-decision",
      })
      const secondRunning = runningRecord(replacement, "replacement-checksum", "attempt-retry")
      expect(() =>
        insertTransition(database, {
          fromRecord: firstFailed,
          id: "transition-retry",
          toRecord: secondRunning,
        }),
      ).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "authorization_id", "authorization_checksum", "protocol_version"
             FROM "nozzle_irreversible_authorization_receipts"
             WHERE "transition_id" = 'transition-retry'`,
          )
          .get(),
      ).toEqual({
        authorization_checksum: "replacement-checksum",
        authorization_id: "replacement-id",
        protocol_version: 2,
      })
    } finally {
      database.close()
    }
  })

  it("rejects a retryable-to-running transition when the sealed plan forbids retries", () => {
    const database = databaseAtVersion(3)
    try {
      const failed = failedRecord()
      seedOperation(database, failed, { ...stepPlan, retryClassification: "never" })
      expect(() =>
        insertTransition(database, {
          fromRecord: failed,
          id: "transition-forbidden-retry",
          toRecord: runningRecord(),
        }),
      ).toThrow("NOZZLE_CONTROL_IRREVERSIBLE_AUTHORIZATION_RETRY_FORBIDDEN")
    } finally {
      database.close()
    }
  })
})
