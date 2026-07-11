import { DatabaseSync } from "node:sqlite"
import { describe, expect, it } from "vitest"
import { CONTROL_SCHEMA_STATEMENTS, controlSchemaSql } from "../src/schema.js"

const descriptorJson = JSON.stringify({
  steps: [
    {
      compensationAction: {
        actionId: "a.compensate",
        artifactChecksum: "artifact",
        version: 1,
      },
      compensationObservation: {
        actionId: "a.observe-compensation",
        artifactChecksum: "artifact",
        version: 1,
      },
      forwardAction: { actionId: "a.forward", artifactChecksum: "artifact", version: 1 },
      forwardObservation: {
        actionId: "a.observe-forward",
        artifactChecksum: "artifact",
        version: 1,
      },
      stepId: "a",
    },
  ],
})

function schemaStatement(prefix: string): string {
  const statement = CONTROL_SCHEMA_STATEMENTS.find((candidate) => candidate.startsWith(prefix))
  if (!statement) throw new Error(`Missing control schema statement: ${prefix}`)
  return statement
}

function installTables(database: DatabaseSync, tableNames: readonly string[]): void {
  for (const tableName of tableNames) {
    database.exec(schemaStatement(`CREATE TABLE IF NOT EXISTS "${tableName}"`))
  }
}

function markSchemaVersionTwoPublished(database: DatabaseSync): void {
  database.exec(`INSERT INTO "nozzle_control_schema_versions"
    ("schema_version", "published_at_ms") VALUES (1, 1), (2, 2);`)
}

function insertAttempt(
  database: DatabaseSync,
  input: {
    readonly actionKey?: string
    readonly attemptId: string
    readonly causalAttemptId: string | null
    readonly fencingToken: number
    readonly idempotencyKey?: string
    readonly operationStepId?: string
    readonly phase?: "compensation" | "forward"
    readonly purpose: "effect" | "observation"
  },
): void {
  const actionKey =
    input.actionKey ??
    {
      "effect:compensation": "a.compensate@1:artifact",
      "effect:forward": "a.forward@1:artifact",
      "observation:compensation": "a.observe-compensation@1:artifact",
      "observation:forward": "a.observe-forward@1:artifact",
    }[`${input.purpose}:${input.phase ?? "forward"}`]
  const idempotencyKey =
    input.idempotencyKey ??
    (input.purpose === "observation" ? "idempotency-key:observation" : "idempotency-key")
  database
    .prepare(
      `INSERT INTO "nozzle_saga_action_attempts"
       ("attempt_id", "causal_attempt_id", "saga_id", "operation_id", "operation_step_id",
        "saga_step_id", "phase", "purpose", "action_key", "idempotency_key",
        "input_checksum", "input_json", "acceptance_checksum", "lease_key", "holder_id",
        "acquisition_id", "fencing_token", "accepted_at_ms")
       VALUES (?, ?, 'saga-a', 'operation-a', ?, 'a', ?, ?,
         ?, ?, 'input-checksum', '{}', 'acceptance-checksum',
         'saga:lease', 'controller', 'acquisition', ?, 1)`,
    )
    .run(
      input.attemptId,
      input.causalAttemptId,
      input.operationStepId ?? `saga:${input.phase ?? "forward"}:a`,
      input.phase ?? "forward",
      input.purpose,
      actionKey,
      idempotencyKey,
      input.fencingToken,
    )
}

function outcomeProtocolDatabase(): DatabaseSync {
  const database = new DatabaseSync(":memory:")
  database.exec("PRAGMA foreign_keys = OFF;")
  installTables(database, [
    "nozzle_control_schema_versions",
    "nozzle_leases",
    "nozzle_saga_action_attempts",
    "nozzle_saga_action_attempt_outcomes",
    "nozzle_saga_action_attempt_protocols",
  ])
  markSchemaVersionTwoPublished(database)
  database.exec(
    schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_outcome_insert_v2"'),
  )
  database.exec(
    schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_protocol_classify_v2"'),
  )
  database
    .prepare(
      `INSERT INTO "nozzle_leases"
       ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
        "updated_at_ms")
       VALUES ('saga:lease', 'controller', 'acquisition', 2, 9000000000000000, 1)`,
    )
    .run()
  return database
}

function insertObservationOutcome(
  database: DatabaseSync,
  attemptId: string,
  state: "confirmed" | "failed" | "indeterminate" | "not_applied" | "unknown",
): void {
  const confirmed = state === "confirmed"
  database
    .prepare(
      `INSERT INTO "nozzle_saga_action_attempt_outcomes"
       ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
        "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
       VALUES (?, ?, 'evidence', '{}', ?, ?, ?, ?, 'outcome', 2)`,
    )
    .run(
      attemptId,
      state,
      confirmed ? "output" : null,
      confirmed ? "{}" : null,
      confirmed ? null : "error",
      confirmed ? null : "{}",
    )
}

interface ObservationBindingFixture {
  readonly causeOutcome: boolean
  readonly operationErrorChecksum: string
  readonly operationLastAttemptId: string
  readonly sagaErrorChecksum: string
}

function observationBindingDatabase(
  input: ObservationBindingFixture,
  options: { readonly installGuards?: boolean; readonly published?: boolean } = {},
): DatabaseSync {
  const database = new DatabaseSync(":memory:")
  database.exec("PRAGMA foreign_keys = OFF;")
  installTables(database, [
    "nozzle_control_schema_versions",
    "nozzle_operations",
    "nozzle_operation_steps",
    "nozzle_leases",
    "nozzle_sagas",
    "nozzle_saga_action_attempts",
    "nozzle_saga_action_attempt_outcomes",
    "nozzle_saga_action_attempt_protocols",
  ])
  if (options.published !== false) markSchemaVersionTwoPublished(database)
  database
    .prepare(
      `INSERT INTO "nozzle_operations"
       ("operation_id", "environment_id", "operation_type", "idempotency_scope",
        "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
        "capability_snapshot_checksum", "capability_snapshot_json", "required_shards_json",
        "status", "created_at_ms", "updated_at_ms")
       VALUES ('operation-a', 'production', 'saga:transfer.v1', 'saga', 'saga-key',
         'saga-input', '{}', 'plan', '{}', 'capability', '{}', '[]', 'reconciling', 1, 1)`,
    )
    .run()
  database
    .prepare(
      `INSERT INTO "nozzle_operation_steps"
       ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json", "record_json",
        "state", "fencing_token", "updated_at_ms")
       VALUES ('operation-a', 'saga:forward:a', 'operation-step-key', 'saga:lease', ?, ?,
         'unknown', 1, 1)`,
    )
    .run(
      JSON.stringify({
        effectProtocol: "saga_receipt",
        idempotencyKey: "operation-step-key",
        leaseKey: "saga:lease",
        stepId: "saga:forward:a",
      }),
      JSON.stringify({
        errorChecksum: input.operationErrorChecksum,
        fencingToken: 1,
        lastAttemptId: input.operationLastAttemptId,
        state: "unknown",
      }),
    )
  database
    .prepare(
      `INSERT INTO "nozzle_leases"
       ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
        "updated_at_ms")
       VALUES ('saga:lease', 'controller', 'acquisition', 2, 9000000000000000, 1)`,
    )
    .run()

  const descriptorChecksum = "d".repeat(64)
  const sagaRecord = JSON.stringify({
    deadlineAtMs: 10_000,
    descriptor: {
      descriptorChecksum,
      descriptorId: "transfer",
      version: 1,
    },
    idempotencyKey: "saga-key",
    inputChecksum: "saga-input",
    sagaId: "saga-a",
    stateVersion: 2,
    status: "running",
    steps: {
      a: {
        compensation: { state: "pending" },
        forward: {
          errorChecksum: input.sagaErrorChecksum,
          idempotencyKey: "idempotency-key",
          lastAttemptId: "effect-a",
          state: "unknown",
        },
        inputChecksum: "input-checksum",
      },
    },
    terminationCause: null,
    terminationRequestedAtMs: null,
  })
  database
    .prepare(
      `INSERT INTO "nozzle_sagas"
       ("saga_id", "operation_id", "descriptor_id", "descriptor_version",
        "descriptor_checksum", "descriptor_json", "idempotency_key", "input_checksum",
        "deadline_at_ms", "status", "commitment", "termination_cause",
        "termination_requested_at_ms", "state_version", "last_evidence_checksum",
        "last_effect_id", "record_checksum", "record_json", "created_at_ms", "updated_at_ms")
       VALUES ('saga-a', 'operation-a', 'transfer', 1, ?, ?, 'saga-key', 'saga-input',
         10000, 'running', 'possible', NULL, NULL, 2, 'evidence', 'effect-row', 'record', ?, 1, 1)`,
    )
    .run(descriptorChecksum, descriptorJson, sagaRecord)

  insertAttempt(database, {
    attemptId: "effect-a",
    causalAttemptId: null,
    fencingToken: 1,
    purpose: "effect",
  })
  if (input.causeOutcome) {
    database
      .prepare(
        `INSERT INTO "nozzle_saga_action_attempt_outcomes"
         ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
          "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
         VALUES ('effect-a', 'unknown', 'effect-evidence', '{}', NULL, NULL, 'effect-error', '{}',
           'effect-outcome', 2)`,
      )
      .run()
  }
  if (options.installGuards !== false) {
    database.exec(
      schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_attempt_insert_v2"'),
    )
    database.exec(
      schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_protocol_classify_v2"'),
    )
  }
  return database
}

function acceptObservation(
  database: DatabaseSync,
  operationStepId?: string,
  binding: { readonly actionKey?: string; readonly idempotencyKey?: string } = {},
): void {
  insertAttempt(database, {
    ...binding,
    attemptId: "observation-a",
    causalAttemptId: "effect-a",
    fencingToken: 2,
    ...(operationStepId === undefined ? {} : { operationStepId }),
    purpose: "observation",
  })
}

function compensationBindingDatabase(
  input: {
    readonly causeOperationStepId?: string
    readonly causeOutcomeState?: "confirmed" | "unknown"
    readonly causeOutputChecksum?: string
    readonly sagaResultChecksum?: string
  } = {},
): DatabaseSync {
  const database = new DatabaseSync(":memory:")
  database.exec("PRAGMA foreign_keys = OFF;")
  installTables(database, [
    "nozzle_control_schema_versions",
    "nozzle_operations",
    "nozzle_operation_steps",
    "nozzle_leases",
    "nozzle_sagas",
    "nozzle_saga_action_attempts",
    "nozzle_saga_action_attempt_outcomes",
    "nozzle_saga_action_attempt_protocols",
  ])
  markSchemaVersionTwoPublished(database)
  database.exec(`INSERT INTO "nozzle_operations"
    ("operation_id", "environment_id", "operation_type", "idempotency_scope",
     "idempotency_key", "input_checksum", "input_json", "plan_checksum", "plan_json",
     "capability_snapshot_checksum", "capability_snapshot_json", "required_shards_json",
     "status", "created_at_ms", "updated_at_ms")
    VALUES ('operation-a', 'production', 'saga:transfer.v1', 'saga', 'saga-key',
      'saga-input', '{}', 'plan', '{}', 'capability', '{}', '[]', 'running', 1, 1);`)
  database
    .prepare(
      `INSERT INTO "nozzle_operation_steps"
       ("operation_id", "step_id", "idempotency_key", "lease_key", "plan_json", "record_json",
        "state", "fencing_token", "updated_at_ms")
       VALUES ('operation-a', 'saga:compensation:a', 'compensation-key', 'saga:lease', ?, ?,
         'running', 2, 1)`,
    )
    .run(
      JSON.stringify({
        effectProtocol: "saga_receipt",
        idempotencyKey: "compensation-key",
        leaseKey: "saga:lease",
        stepId: "saga:compensation:a",
      }),
      JSON.stringify({ activeAttemptId: "compensation-a", fencingToken: 2, state: "running" }),
    )
  database.exec(`INSERT INTO "nozzle_leases"
    ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
     "updated_at_ms")
    VALUES ('saga:lease', 'controller', 'acquisition', 2, 9000000000000000, 1);`)

  const descriptorChecksum = "d".repeat(64)
  const sagaRecord = JSON.stringify({
    deadlineAtMs: 10_000,
    descriptor: { descriptorChecksum, descriptorId: "transfer", version: 1 },
    idempotencyKey: "saga-key",
    inputChecksum: "saga-input",
    sagaId: "saga-a",
    stateVersion: 2,
    status: "compensating",
    steps: {
      a: {
        compensation: {
          activeAttemptId: "compensation-a",
          idempotencyKey: "idempotency-key",
          state: "running",
        },
        forward: {
          idempotencyKey: "idempotency-key",
          lastAttemptId: "effect-a",
          resultChecksum: input.sagaResultChecksum ?? "forward-output",
          state: "succeeded",
        },
        inputChecksum: "input-checksum",
      },
    },
    terminationCause: "cancellation",
    terminationRequestedAtMs: 2,
  })
  database
    .prepare(
      `INSERT INTO "nozzle_sagas"
       ("saga_id", "operation_id", "descriptor_id", "descriptor_version",
        "descriptor_checksum", "descriptor_json", "idempotency_key", "input_checksum",
        "deadline_at_ms", "status", "commitment", "termination_cause",
        "termination_requested_at_ms", "state_version", "last_evidence_checksum",
        "last_effect_id", "record_checksum", "record_json", "created_at_ms", "updated_at_ms")
       VALUES ('saga-a', 'operation-a', 'transfer', 1, ?, ?, 'saga-key', 'saga-input',
         10000, 'compensating', 'possible', 'cancellation', 2, 2, 'evidence', 'effect-row',
         'record', ?, 1, 2)`,
    )
    .run(descriptorChecksum, descriptorJson, sagaRecord)
  insertAttempt(database, {
    attemptId: "effect-a",
    causalAttemptId: null,
    fencingToken: 1,
    operationStepId: input.causeOperationStepId ?? "saga:forward:a",
    purpose: "effect",
  })
  const confirmed = (input.causeOutcomeState ?? "confirmed") === "confirmed"
  database
    .prepare(
      `INSERT INTO "nozzle_saga_action_attempt_outcomes"
       ("attempt_id", "state", "evidence_checksum", "evidence_json", "output_checksum",
        "output_json", "error_checksum", "error_json", "outcome_checksum", "completed_at_ms")
       VALUES ('effect-a', ?, 'effect-evidence', '{}', ?, ?, ?, ?, 'effect-outcome', 2)`,
    )
    .run(
      confirmed ? "confirmed" : "unknown",
      confirmed ? (input.causeOutputChecksum ?? "forward-output") : null,
      confirmed ? "{}" : null,
      confirmed ? null : "unknown-error",
      confirmed ? null : "{}",
    )
  database.exec(
    schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_attempt_insert_v2"'),
  )
  database.exec(
    schemaStatement('CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_protocol_classify_v2"'),
  )
  return database
}

function acceptCompensation(database: DatabaseSync): void {
  insertAttempt(database, {
    attemptId: "compensation-a",
    causalAttemptId: "effect-a",
    fencingToken: 2,
    phase: "compensation",
    purpose: "effect",
  })
}

describe("control schema saga protocol guards", () => {
  it.each([
    "confirmed",
    "failed",
    "not_applied",
    "unknown",
  ] as const)("accepts an effect %s outcome through raw SQL", (state) => {
    const database = outcomeProtocolDatabase()
    try {
      insertAttempt(database, {
        attemptId: `effect-${state}`,
        causalAttemptId: null,
        fencingToken: 2,
        purpose: "effect",
      })
      expect(() => insertObservationOutcome(database, `effect-${state}`, state)).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = ?`,
          )
          .get(`effect-${state}`),
      ).toEqual({ protocol_version: 2 })
    } finally {
      database.close()
    }
  })

  it("rejects an effect indeterminate outcome through raw SQL", () => {
    const database = outcomeProtocolDatabase()
    try {
      insertAttempt(database, {
        attemptId: "effect-indeterminate",
        causalAttemptId: null,
        fencingToken: 2,
        purpose: "effect",
      })
      expect(() =>
        insertObservationOutcome(database, "effect-indeterminate", "indeterminate"),
      ).toThrow("NOZZLE_CONTROL_SAGA_OUTCOME_FENCED")
    } finally {
      database.close()
    }
  })

  it.each([
    "confirmed",
    "not_applied",
    "indeterminate",
  ] as const)("accepts an observation %s outcome through raw SQL", (state) => {
    const database = outcomeProtocolDatabase()
    try {
      insertAttempt(database, {
        attemptId: `observation-${state}`,
        causalAttemptId: "effect-a",
        fencingToken: 2,
        purpose: "observation",
      })
      expect(() => insertObservationOutcome(database, `observation-${state}`, state)).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = ?`,
          )
          .get(`observation-${state}`),
      ).toEqual({ protocol_version: 2 })
    } finally {
      database.close()
    }
  })

  it.each([
    "failed",
    "unknown",
  ] as const)("rejects an observation %s outcome through raw SQL", (state) => {
    const database = outcomeProtocolDatabase()
    try {
      insertAttempt(database, {
        attemptId: `observation-${state}`,
        causalAttemptId: "effect-a",
        fencingToken: 2,
        purpose: "observation",
      })
      expect(() => insertObservationOutcome(database, `observation-${state}`, state)).toThrow(
        "NOZZLE_CONTROL_SAGA_OUTCOME_FENCED",
      )
    } finally {
      database.close()
    }
  })

  it("labels legacy causally weak attempts as protocol one without rewriting their outcomes", () => {
    const database = new DatabaseSync(":memory:")
    database.exec("PRAGMA foreign_keys = OFF;")
    try {
      installTables(database, [
        "nozzle_leases",
        "nozzle_saga_action_attempts",
        "nozzle_saga_action_attempt_outcomes",
      ])
      database.exec(`INSERT INTO "nozzle_leases"
        ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms",
         "updated_at_ms")
        VALUES ('saga:lease', 'controller', 'acquisition', 2, 9000000000000000, 1);`)
      for (const attemptId of ["legacy-allowed", "legacy-invalid", "legacy-open"]) {
        insertAttempt(database, {
          attemptId,
          causalAttemptId: "missing-effect",
          fencingToken: 2,
          purpose: "observation",
        })
      }
      insertObservationOutcome(database, "legacy-invalid", "failed")

      const protocolGuardIndex = CONTROL_SCHEMA_STATEMENTS.findIndex((statement) =>
        statement.startsWith(
          'CREATE TRIGGER IF NOT EXISTS "nozzle_control_saga_protocol_compensation_insert_v2"',
        ),
      )
      if (protocolGuardIndex < 0) throw new Error("Missing protocol classification guard.")
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(0, protocolGuardIndex + 1)) {
        database.exec(statement)
      }
      expect(() =>
        database
          .prepare(
            `INSERT INTO "nozzle_saga_action_attempt_protocols"
             ("attempt_id", "protocol_version", "classified_at_ms")
             VALUES ('legacy-open', 2, 1)`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_PROTOCOL_FENCED")
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(protocolGuardIndex + 1)) {
        database.exec(statement)
      }

      expect(
        database
          .prepare(
            `SELECT "attempt_id", "protocol_version"
             FROM "nozzle_saga_action_attempt_protocols" ORDER BY "attempt_id"`,
          )
          .all(),
      ).toEqual([
        { attempt_id: "legacy-allowed", protocol_version: 1 },
        { attempt_id: "legacy-invalid", protocol_version: 1 },
        { attempt_id: "legacy-open", protocol_version: 1 },
      ])
      expect(
        database
          .prepare(
            `SELECT "state" FROM "nozzle_saga_action_attempt_outcomes"
             WHERE "attempt_id" = 'legacy-invalid'`,
          )
          .get(),
      ).toEqual({ state: "failed" })
      expect(() => insertObservationOutcome(database, "legacy-open", "failed")).toThrow(
        "NOZZLE_CONTROL_SAGA_OUTCOME_FENCED",
      )
      expect(() => insertObservationOutcome(database, "legacy-allowed", "confirmed")).not.toThrow()
      expect(() =>
        database
          .prepare(
            `UPDATE "nozzle_saga_action_attempt_protocols" SET "protocol_version" = 2
             WHERE "attempt_id" = 'legacy-open'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_PROTOCOL_IMMUTABLE")
      expect(() =>
        database
          .prepare(
            `DELETE FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = 'legacy-open'`,
          )
          .run(),
      ).toThrow("NOZZLE_CONTROL_SAGA_PROTOCOL_IMMUTABLE")
    } finally {
      database.close()
    }
  })

  it("refuses publication when an interrupted installer already misclassified a weak row", () => {
    const database = new DatabaseSync(":memory:")
    database.exec("PRAGMA foreign_keys = OFF;")
    try {
      installTables(database, [
        "nozzle_saga_action_attempts",
        "nozzle_saga_action_attempt_protocols",
      ])
      insertAttempt(database, {
        attemptId: "legacy-weak-misclassified",
        causalAttemptId: "missing-effect",
        fencingToken: 2,
        operationStepId: "noncanonical-step",
        purpose: "observation",
      })
      database.exec(`INSERT INTO "nozzle_saga_action_attempt_protocols"
        ("attempt_id", "protocol_version", "classified_at_ms")
        VALUES ('legacy-weak-misclassified', 2, 1);`)

      expect(() => database.exec(controlSchemaSql())).toThrow("CHECK constraint failed")
      expect(
        database
          .prepare(
            `SELECT count(*) AS "count" FROM "nozzle_control_schema_versions"
             WHERE "schema_version" = 2`,
          )
          .get(),
      ).toEqual({ count: 0 })
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = 'legacy-weak-misclassified'`,
          )
          .get(),
      ).toEqual({ protocol_version: 2 })
    } finally {
      database.close()
    }
  })

  it("refuses publication when an interrupted installer has an orphan protocol-two row", () => {
    const database = new DatabaseSync(":memory:")
    database.exec("PRAGMA foreign_keys = OFF;")
    try {
      installTables(database, ["nozzle_saga_action_attempt_protocols"])
      database.exec(`INSERT INTO "nozzle_saga_action_attempt_protocols"
        ("attempt_id", "protocol_version", "classified_at_ms")
        VALUES ('orphan-protocol-two', 2, 1);`)

      expect(() => database.exec(controlSchemaSql())).toThrow("CHECK constraint failed")
      expect(
        database
          .prepare(
            `SELECT count(*) AS "count" FROM "nozzle_control_schema_versions"
             WHERE "schema_version" = 2`,
          )
          .get(),
      ).toEqual({ count: 0 })
    } finally {
      database.close()
    }
  })

  it("installs protocol immutability before auditing a legacy mapping", () => {
    const database = new DatabaseSync(":memory:")
    database.exec("PRAGMA foreign_keys = OFF;")
    try {
      installTables(database, [
        "nozzle_saga_action_attempts",
        "nozzle_saga_action_attempt_protocols",
      ])
      insertAttempt(database, {
        attemptId: "legacy-immutable",
        causalAttemptId: "missing-effect",
        fencingToken: 2,
        purpose: "observation",
      })
      database.exec(`INSERT INTO "nozzle_saga_action_attempt_protocols"
        ("attempt_id", "protocol_version", "classified_at_ms")
        VALUES ('legacy-immutable', 1, 1);`)

      const identityAuditIndex = CONTROL_SCHEMA_STATEMENTS.findIndex(
        (statement) =>
          statement.includes('LEFT JOIN "nozzle_saga_action_attempts" AS "attempt"') &&
          statement.includes('"protocol"."classified_at_ms" <> "attempt"."accepted_at_ms"'),
      )
      if (identityAuditIndex < 0) throw new Error("Missing protocol identity audit.")
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(0, identityAuditIndex)) {
        database.exec(statement)
      }

      expect(() =>
        database.exec(`UPDATE "nozzle_saga_action_attempt_protocols"
          SET "protocol_version" = 2 WHERE "attempt_id" = 'legacy-immutable';`),
      ).toThrow("NOZZLE_CONTROL_SAGA_PROTOCOL_IMMUTABLE")
      expect(() =>
        database.exec(`DELETE FROM "nozzle_saga_action_attempt_protocols"
          WHERE "attempt_id" = 'legacy-immutable';`),
      ).toThrow("NOZZLE_CONTROL_SAGA_PROTOCOL_IMMUTABLE")

      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(identityAuditIndex)) {
        database.exec(statement)
      }
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = 'legacy-immutable'`,
          )
          .get(),
      ).toEqual({ protocol_version: 1 })
    } finally {
      database.close()
    }
  })

  it("keeps a settled migration-window attempt on protocol one across publication", () => {
    const database = observationBindingDatabase(
      {
        causeOutcome: true,
        operationErrorChecksum: "effect-outcome",
        operationLastAttemptId: "effect-a",
        sagaErrorChecksum: "effect-error",
      },
      { installGuards: false, published: false },
    )
    try {
      const mapperGuardIndex = CONTROL_SCHEMA_STATEMENTS.reduce(
        (lastIndex, statement, index) =>
          statement.includes("nozzle_control_saga_protocol_classify_v2") ? index : lastIndex,
        -1,
      )
      if (mapperGuardIndex < 0) throw new Error("Missing protocol mapper definition guard.")
      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(0, mapperGuardIndex + 1)) {
        database.exec(statement)
      }
      acceptObservation(database)
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = 'observation-a'`,
          )
          .get(),
      ).toEqual({ protocol_version: 1 })
      insertObservationOutcome(database, "observation-a", "confirmed")
      database.exec(`UPDATE "nozzle_leases"
        SET "holder_id" = NULL, "acquisition_id" = NULL, "expires_at_ms" = 2,
            "updated_at_ms" = 2
        WHERE "lease_key" = 'saga:lease';`)

      for (const statement of CONTROL_SCHEMA_STATEMENTS.slice(mapperGuardIndex + 1)) {
        database.exec(statement)
      }
      expect(() => database.exec(controlSchemaSql())).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "attempt_id", "protocol_version"
             FROM "nozzle_saga_action_attempt_protocols" ORDER BY "attempt_id"`,
          )
          .all(),
      ).toEqual([
        { attempt_id: "effect-a", protocol_version: 1 },
        { attempt_id: "observation-a", protocol_version: 1 },
      ])
      expect(
        database
          .prepare(
            `SELECT "schema_version" FROM "nozzle_control_schema_versions"
             ORDER BY "schema_version"`,
          )
          .all(),
      ).toEqual([{ schema_version: 1 }, { schema_version: 2 }])
    } finally {
      database.close()
    }
  })

  it("accepts an observation only when both ledgers bind the exact unknown receipt", () => {
    const database = observationBindingDatabase({
      causeOutcome: true,
      operationErrorChecksum: "effect-outcome",
      operationLastAttemptId: "effect-a",
      sagaErrorChecksum: "effect-error",
    })
    try {
      expect(() => acceptObservation(database)).not.toThrow()
    } finally {
      database.close()
    }
  })

  it.each([
    ["descriptor action", { actionKey: "different-action" }],
    ["action idempotency", { idempotencyKey: "different-key" }],
  ] as const)("rejects an observation with a mismatched %s binding", (_name, binding) => {
    const database = observationBindingDatabase({
      causeOutcome: true,
      operationErrorChecksum: "effect-outcome",
      operationLastAttemptId: "effect-a",
      sagaErrorChecksum: "effect-error",
    })
    try {
      expect(() => acceptObservation(database, undefined, binding)).toThrow(
        "NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED",
      )
    } finally {
      database.close()
    }
  })

  it.each([
    ["generic attempt identity", "different-effect", "effect-outcome", "effect-error"],
    ["generic receipt checksum", "effect-a", "different-outcome", "effect-error"],
    ["saga error checksum", "effect-a", "effect-outcome", "different-error"],
  ] as const)("rejects an observation with a mismatched %s", (_name, operationLastAttemptId, operationErrorChecksum, sagaErrorChecksum) => {
    const database = observationBindingDatabase({
      causeOutcome: true,
      operationErrorChecksum,
      operationLastAttemptId,
      sagaErrorChecksum,
    })
    try {
      expect(() => acceptObservation(database)).toThrow("NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED")
    } finally {
      database.close()
    }
  })

  it.each([
    ["canonical operation step ID", "", "saga:a:forward"],
    [
      "plan step ID",
      `UPDATE "nozzle_operation_steps"
       SET "plan_json" = json_set("plan_json", '$.stepId', 'saga:forward:other')`,
      undefined,
    ],
    [
      "plan idempotency key",
      `UPDATE "nozzle_operation_steps"
       SET "plan_json" = json_set("plan_json", '$.idempotencyKey', 'other-key')`,
      undefined,
    ],
    [
      "plan lease key",
      `UPDATE "nozzle_operation_steps"
       SET "plan_json" = json_set("plan_json", '$.leaseKey', 'other-lease')`,
      undefined,
    ],
    [
      "effect protocol",
      `UPDATE "nozzle_operation_steps"
       SET "plan_json" = json_set("plan_json", '$.effectProtocol', 'opaque')`,
      undefined,
    ],
    [
      "record state column",
      `UPDATE "nozzle_operation_steps"
       SET "record_json" = json_set("record_json", '$.state', 'running')`,
      undefined,
    ],
    [
      "record fencing column",
      `UPDATE "nozzle_operation_steps"
       SET "record_json" = json_set("record_json", '$.fencingToken', 2)`,
      undefined,
    ],
  ] as const)("rejects an observation with mismatched %s metadata", (_name, mutation, operationStepId) => {
    const database = observationBindingDatabase({
      causeOutcome: true,
      operationErrorChecksum: "effect-outcome",
      operationLastAttemptId: "effect-a",
      sagaErrorChecksum: "effect-error",
    })
    try {
      if (mutation.length > 0) database.exec(mutation)
      expect(() => acceptObservation(database, operationStepId)).toThrow(
        "NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED",
      )
    } finally {
      database.close()
    }
  })

  it("permits recovery when the accepted cause receipt was lost before persistence", () => {
    const database = observationBindingDatabase({
      causeOutcome: false,
      operationErrorChecksum: "acceptance-checksum",
      operationLastAttemptId: "effect-a",
      sagaErrorChecksum: "acceptance-checksum",
    })
    try {
      expect(() => acceptObservation(database)).not.toThrow()
    } finally {
      database.close()
    }
  })

  it.each([
    ["generic acceptance checksum", "other", "acceptance-checksum"],
    ["saga acceptance checksum", "acceptance-checksum", "other"],
  ] as const)("rejects lost-receipt recovery with a mismatched %s", (_name, operationErrorChecksum, sagaErrorChecksum) => {
    const database = observationBindingDatabase({
      causeOutcome: false,
      operationErrorChecksum,
      operationLastAttemptId: "effect-a",
      sagaErrorChecksum,
    })
    try {
      expect(() => acceptObservation(database)).toThrow("NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED")
    } finally {
      database.close()
    }
  })

  it("accepts compensation only from the exact confirmed forward receipt", () => {
    const database = compensationBindingDatabase()
    try {
      expect(() => acceptCompensation(database)).not.toThrow()
      expect(
        database
          .prepare(
            `SELECT "protocol_version" FROM "nozzle_saga_action_attempt_protocols"
             WHERE "attempt_id" = 'compensation-a'`,
          )
          .get(),
      ).toEqual({ protocol_version: 2 })
    } finally {
      database.close()
    }
  })

  it.each([
    ["unconfirmed receipt", { causeOutcomeState: "unknown" }],
    ["receipt output", { causeOutputChecksum: "other-output" }],
    ["forward result", { sagaResultChecksum: "other-result" }],
    ["forward operation step", { causeOperationStepId: "saga:a:forward" }],
  ] as const)("rejects compensation with a mismatched %s", (_name, fixture) => {
    const database = compensationBindingDatabase(fixture)
    try {
      expect(() => acceptCompensation(database)).toThrow("NOZZLE_CONTROL_SAGA_ATTEMPT_FENCED")
    } finally {
      database.close()
    }
  })
})
