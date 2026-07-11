import { DatabaseSync, type SQLInputValue, type StatementSync } from "node:sqlite"
import {
  appendAuditEvent,
  type DigestFunction,
  leaseProof,
  type OperationStepPlanInput,
  sagaActionIdempotencyKey,
  sealOperationPlan,
  sealSagaDescriptor,
} from "@nozzle/core"
import { afterEach, describe, expect, it } from "vitest"
import type {
  ControlBindingValue,
  ControlQueryResult,
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "../src/database.js"
import { D1LeaseStore } from "../src/lease-store.js"
import { D1OperationStore, operationTransitionIdentity } from "../src/operation-store.js"
import { D1SagaAttemptStore, sagaActionInputChecksum } from "../src/saga-attempt-store.js"
import {
  D1SagaCoordinatorStore,
  type InitializeSagaInput,
  type RequestCoordinatedSagaTerminationInput,
} from "../src/saga-coordinator-store.js"
import {
  D1SagaStore,
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "../src/saga-store.js"
import { controlSchemaSql } from "../src/schema.js"

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

async function sagaRecordChecksum(recordJson: string): Promise<string> {
  const domain = new TextEncoder().encode("nozzle.saga-record.v1")
  const record = new TextEncoder().encode(recordJson)
  const framed = new Uint8Array(8 + domain.byteLength + record.byteLength)
  const view = new DataView(framed.buffer)
  view.setUint32(0, domain.byteLength, false)
  framed.set(domain, 4)
  view.setUint32(4 + domain.byteLength, record.byteLength, false)
  framed.set(record, 8 + domain.byteLength)
  return digest(framed)
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
  readonly #row: unknown

  constructor(row: unknown) {
    this.#row = row
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
    return { meta: { changes: 0 }, success: true }
  }
}

class FixedRowsStatement implements ControlStatement {
  readonly #rows: readonly unknown[]

  constructor(rows: readonly unknown[]) {
    this.#rows = rows
  }

  bind(): ControlStatement {
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return { meta: {}, results: this.#rows as T[], success: true }
  }

  async first<T>(): Promise<T | null> {
    return (this.#rows[0] as T | undefined) ?? null
  }

  async run(): Promise<ControlRunResult> {
    return { meta: { changes: 0 }, success: true }
  }
}

type BatchFault =
  | { readonly kind: "commit_then_throw" }
  | { readonly index: number; readonly kind: "rollback_before" }
  | { readonly kind: "return"; readonly results: readonly ControlRunResult[] }

class FaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #fault: BatchFault

  constructor(base: DatabaseAdapter, fault: BatchFault) {
    this.#base = base
    this.#fault = fault
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    if (this.#fault.kind === "return") return this.#fault.results
    if (this.#fault.kind === "commit_then_throw") {
      await this.#base.batch(statements)
      throw new Error("injected post-commit response loss")
    }
    this.#base.database.exec("BEGIN IMMEDIATE;")
    try {
      const results: ControlRunResult[] = []
      for (let index = 0; index < statements.length; index += 1) {
        if (index === this.#fault.index) throw new Error("injected coupled statement failure")
        results.push(await (statements[index] as ControlStatement).run())
      }
      this.#base.database.exec("COMMIT;")
      return results
    } catch (error) {
      this.#base.database.exec("ROLLBACK;")
      throw error
    }
  }

  prepare(sql: string): ControlStatement {
    return this.#base.prepare(sql)
  }
}

class StaleSagaAttemptReadDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #firstRow: unknown
  #pending = true

  constructor(base: DatabaseAdapter, firstRow: unknown) {
    this.#base = base
    this.#firstRow = firstRow
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    if (
      this.#pending &&
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"') &&
      sql.includes('LEFT JOIN "nozzle_saga_action_attempt_outcomes" AS "outcome"')
    ) {
      this.#pending = false
      return new FixedStatement(this.#firstRow)
    }
    return this.#base.prepare(sql)
  }
}

class AdvanceBeforeReadStatement implements ControlStatement {
  readonly #advance: () => Promise<void>
  #delegate: ControlStatement
  #pending = true

  constructor(delegate: ControlStatement, advance: () => Promise<void>) {
    this.#advance = advance
    this.#delegate = delegate
  }

  bind(...values: readonly ControlBindingValue[]): ControlStatement {
    this.#delegate = this.#delegate.bind(...values)
    return this
  }

  async all<T>(): Promise<ControlQueryResult<T>> {
    return this.#delegate.all<T>()
  }

  async first<T>(): Promise<T | null> {
    if (this.#pending) {
      this.#pending = false
      await this.#advance()
    }
    return this.#delegate.first<T>()
  }

  async run(): Promise<ControlRunResult> {
    return this.#delegate.run()
  }
}

class AdvanceAfterCommitDatabase implements TransactionalControlDatabase {
  readonly #advance: () => Promise<void>
  readonly #base: DatabaseAdapter
  #armed = false
  #advanced = false

  constructor(base: DatabaseAdapter, advance: () => Promise<void>) {
    this.#advance = advance
    this.#base = base
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const results = await this.#base.batch(statements)
    if (!this.#advanced) this.#armed = true
    return results
  }

  prepare(sql: string): ControlStatement {
    const statement = this.#base.prepare(sql)
    if (
      this.#armed &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#armed = false
      this.#advanced = true
      return new AdvanceBeforeReadStatement(statement, this.#advance)
    }
    return statement
  }
}

class AdvanceBeforeInitializationReceiptReadDatabase implements TransactionalControlDatabase {
  readonly #advance: () => Promise<void>
  readonly #base: DatabaseAdapter
  #pending = true

  constructor(base: DatabaseAdapter, advance: () => Promise<void>) {
    this.#advance = advance
    this.#base = base
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    const statement = this.#base.prepare(sql)
    if (
      this.#pending &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#pending = false
      return new AdvanceBeforeReadStatement(statement, this.#advance)
    }
    return statement
  }
}

class ServerTimeDatabase implements TransactionalControlDatabase {
  readonly #base: TransactionalControlDatabase
  readonly #serverTimeMs: number

  constructor(base: TransactionalControlDatabase, serverTimeMs: number) {
    this.#base = base
    this.#serverTimeMs = serverTimeMs
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    return this.#base.prepare(
      sql.replaceAll(
        "CAST(unixepoch('subsec') * 1000 AS INTEGER)",
        this.#serverTimeMs.toString(10),
      ),
    )
  }
}

class AdvanceBeforeTerminationReceiptReadDatabase implements TransactionalControlDatabase {
  readonly #advance: () => Promise<void>
  readonly #base: TransactionalControlDatabase
  #pending = true

  constructor(base: TransactionalControlDatabase, advance: () => Promise<void>) {
    this.#advance = advance
    this.#base = base
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    return this.#base.batch(statements)
  }

  prepare(sql: string): ControlStatement {
    const statement = this.#base.prepare(sql)
    if (
      this.#pending &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#pending = false
      return new AdvanceBeforeReadStatement(statement, this.#advance)
    }
    return statement
  }
}

class FailFirstTerminationVerificationReadDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  #armed = false

  constructor(base: DatabaseAdapter) {
    this.#base = base
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const results = await this.#base.batch(statements)
    this.#armed = true
    return results
  }

  prepare(sql: string): ControlStatement {
    if (
      this.#armed &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#armed = false
      throw new Error("injected first post-commit termination receipt read failure")
    }
    return this.#base.prepare(sql)
  }
}

type QueryFault =
  | { readonly kind: "audit_snapshot"; readonly row: unknown }
  | { readonly kind: "saga_projection"; readonly row: unknown }
  | { readonly kind: "termination_projection"; readonly row: unknown }
  | { readonly kind: "termination_winner_projection"; readonly row: unknown }
  | {
      readonly kind:
        | "audit_missing"
        | "audit_mismatch"
        | "classified_audit_mismatch"
        | "classified_audit_missing"
        | "classified_duplicate_transition"
        | "classified_effect_mismatch"
        | "classified_from_record_mismatch"
        | "classified_invalid_step_json"
        | "classified_noncanonical_step"
        | "classified_prior_mismatch"
        | "classified_prior_missing"
        | "classified_saga_checksum_mismatch"
        | "classified_status_mismatch"
        | "classified_transition_mismatch"
        | "effect_missing"
        | "effect_mismatch"
        | "initialized_audit_mismatch"
        | "initialized_audit_missing"
        | "initialized_effect_mismatch"
        | "initialized_effect_missing"
        | "initialized_saga_missing"
        | "initialized_step_result_mismatch"
        | "initialized_transition_disappears"
        | "initialized_transition_history_mismatch"
        | "initialized_transition_identity_mismatch"
        | "initialized_transition_missing"
        | "observation_cause_missing"
        | "observation_saga_missing"
        | "operation_missing"
        | "saga_missing"
        | "saga_regressed"
        | "transition_mismatch"
    }
  | { readonly kind: "saga_operation_mismatch" }
  | {
      readonly kind:
        | "settlement_effect_missing"
        | "settlement_saga_missing"
        | "settlement_transition_missing"
        | "termination_audit_mismatch"
        | "termination_audit_missing"
        | "termination_effect_ambiguous"
        | "termination_effect_mismatch"
        | "termination_effect_missing"
        | "termination_effect_without_termination"
        | "termination_existing_step_incomplete"
        | "termination_prior_mismatch"
        | "termination_prior_missing"
        | "termination_projection_missing"
        | "termination_step_mismatch"
        | "termination_step_missing"
        | "termination_transition_disappears"
        | "termination_transition_mismatch"
        | "termination_transition_missing"
        | "termination_winner_projection_missing"
    }

class QueryFaultDatabase implements TransactionalControlDatabase {
  readonly #base: DatabaseAdapter
  readonly #fault: QueryFault
  #batched = false
  #initializationTransitionReads = 0
  #sagaAttemptReads = 0
  #terminationSagaReads = 0
  #terminationTransitionReads = 0

  constructor(base: DatabaseAdapter, fault: QueryFault) {
    this.#base = base
    this.#fault = fault
  }

  async batch(statements: readonly ControlStatement[]): Promise<readonly ControlRunResult[]> {
    const results = await this.#base.batch(statements)
    this.#batched = true
    return results
  }

  prepare(sql: string): ControlStatement {
    if (
      this.#fault.kind === "termination_transition_disappears" &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#terminationTransitionReads += 1
      if (this.#terminationTransitionReads === 2) return new FixedStatement(null)
    }
    if (
      (this.#fault.kind === "termination_transition_mismatch" ||
        this.#fault.kind === "termination_transition_missing") &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      if (this.#fault.kind === "termination_transition_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_transitions"
           WHERE "step_id" = ? ORDER BY rowid DESC LIMIT 1`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID) as Record<string, unknown>
      return new FixedStatement({ ...row, holder_id: "wrong-holder" })
    }
    if (
      this.#fault.kind === "termination_existing_step_incomplete" &&
      sql.includes('FROM "nozzle_operation_steps" WHERE "operation_id" = ?1 ORDER BY "step_id"')
    ) {
      const rows = this.#base.database
        .prepare(
          `SELECT "operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
                  "record_json", "state", "fencing_token", "updated_at_ms"
           FROM "nozzle_operation_steps" ORDER BY "step_id"`,
        )
        .all() as Record<string, unknown>[]
      return new FixedRowsStatement(
        rows.map((row) =>
          row.step_id === SAGA_TERMINATION_OPERATION_STEP_ID
            ? {
                ...row,
                fencing_token: null,
                record_json: JSON.stringify({
                  costCounters: {},
                  progressCounters: {},
                  startedAttempts: 0,
                  state: "pending",
                }),
                state: "pending",
              }
            : row,
        ),
      )
    }
    if (
      (this.#fault.kind === "termination_step_mismatch" ||
        this.#fault.kind === "termination_step_missing") &&
      sql.includes('FROM "nozzle_operation_steps" WHERE "operation_id" = ?1 ORDER BY "step_id"')
    ) {
      const rows = this.#base.database
        .prepare(
          `SELECT "operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
                  "record_json", "state", "fencing_token", "updated_at_ms"
           FROM "nozzle_operation_steps" ORDER BY "step_id"`,
        )
        .all() as Record<string, unknown>[]
      return new FixedRowsStatement(
        rows.flatMap((row) => {
          if (row.step_id !== SAGA_TERMINATION_OPERATION_STEP_ID) return [row]
          if (this.#fault.kind === "termination_step_missing") return []
          const record = JSON.parse(row.record_json as string) as Record<string, unknown>
          return [{ ...row, record_json: JSON.stringify({ ...record, resultChecksum: "wrong" }) }]
        }),
      )
    }
    if (
      (this.#fault.kind === "termination_audit_mismatch" ||
        this.#fault.kind === "termination_audit_missing") &&
      sql.includes('SELECT "event_json" FROM "nozzle_audit_log"') &&
      sql.includes('"event_hash" = ?2')
    ) {
      if (this.#fault.kind === "termination_audit_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT "event_json" FROM "nozzle_audit_log"
           ORDER BY "sequence" LIMIT 1`,
        )
        .get() as Record<string, unknown>
      return new FixedStatement(row)
    }
    if (
      (this.#fault.kind === "termination_effect_ambiguous" ||
        this.#fault.kind === "termination_effect_mismatch" ||
        this.#fault.kind === "termination_effect_missing" ||
        this.#fault.kind === "termination_effect_without_termination") &&
      sql.includes('WHERE "transition_id" = ?1') &&
      sql.includes("\"resource_kind\" = 'saga'") &&
      sql.includes('"resource_id" = ?2')
    ) {
      if (this.#fault.kind === "termination_effect_missing") return new FixedRowsStatement([])
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_effects"
           WHERE "step_id" = ? ORDER BY rowid DESC LIMIT 1`,
        )
        .get(
          this.#fault.kind === "termination_effect_without_termination"
            ? SAGA_INIT_OPERATION_STEP_ID
            : SAGA_TERMINATION_OPERATION_STEP_ID,
        ) as Record<string, unknown>
      if (this.#fault.kind === "termination_effect_without_termination") {
        return new FixedRowsStatement([row])
      }
      if (this.#fault.kind === "termination_effect_ambiguous") {
        return new FixedRowsStatement([row, row])
      }
      return new FixedRowsStatement([{ ...row, record_checksum: "wrong-checksum" }])
    }
    if (
      (this.#fault.kind === "termination_prior_mismatch" ||
        this.#fault.kind === "termination_prior_missing") &&
      sql.includes('"to_state_version" = ?2') &&
      sql.includes("\"resource_kind\" = 'saga'")
    ) {
      if (this.#fault.kind === "termination_prior_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_effects"
           WHERE "step_id" = ? ORDER BY rowid DESC LIMIT 1`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID) as Record<string, unknown>
      return new FixedStatement(row)
    }
    if (
      this.#fault.kind === "termination_projection_missing" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "termination_projection" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(this.#fault.row)
    }
    if (
      (this.#fault.kind === "termination_winner_projection" ||
        this.#fault.kind === "termination_winner_projection_missing") &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      this.#terminationSagaReads += 1
      if (this.#terminationSagaReads === 2) {
        return new FixedStatement(
          this.#fault.kind === "termination_winner_projection" ? this.#fault.row : null,
        )
      }
    }
    if (
      (this.#fault.kind === "initialized_transition_history_mismatch" ||
        this.#fault.kind === "initialized_transition_identity_mismatch" ||
        this.#fault.kind === "initialized_transition_missing") &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      if (this.#fault.kind === "initialized_transition_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_transitions"
           WHERE "step_id" = ? ORDER BY rowid DESC LIMIT 1`,
        )
        .get(SAGA_INIT_OPERATION_STEP_ID) as Record<string, unknown>
      return new FixedStatement(
        this.#fault.kind === "initialized_transition_identity_mismatch"
          ? { ...row, lease_key: "saga:wrong" }
          : { ...row, from_record_json: row.to_record_json },
      )
    }
    if (
      this.#fault.kind === "initialized_transition_disappears" &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      this.#initializationTransitionReads += 1
      if (this.#initializationTransitionReads === 2) return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "initialized_saga_missing" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "initialized_step_result_mismatch" &&
      sql.includes('FROM "nozzle_operation_steps" WHERE "operation_id" = ?1 ORDER BY "step_id"')
    ) {
      const rows = this.#base.database
        .prepare(
          `SELECT "operation_id", "step_id", "idempotency_key", "lease_key", "plan_json",
                  "record_json", "state", "fencing_token", "updated_at_ms"
           FROM "nozzle_operation_steps" ORDER BY "step_id"`,
        )
        .all() as Record<string, unknown>[]
      return new FixedRowsStatement(
        rows.map((row) => {
          if (row.step_id !== SAGA_INIT_OPERATION_STEP_ID) return row
          const record = JSON.parse(row.record_json as string) as Record<string, unknown>
          return {
            ...row,
            record_json: JSON.stringify({ ...record, resultChecksum: "wrong-result" }),
          }
        }),
      )
    }
    if (
      (this.#fault.kind === "initialized_audit_mismatch" ||
        this.#fault.kind === "initialized_audit_missing") &&
      sql.includes('SELECT "event_json" FROM "nozzle_audit_log"') &&
      sql.includes('"event_hash" = ?2')
    ) {
      if (this.#fault.kind === "initialized_audit_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT "event_json" FROM "nozzle_audit_log"
           ORDER BY "sequence" DESC LIMIT 1 OFFSET 1`,
        )
        .get() as Record<string, unknown>
      return new FixedStatement(row)
    }
    if (
      (this.#fault.kind === "initialized_effect_mismatch" ||
        this.#fault.kind === "initialized_effect_missing") &&
      sql === 'SELECT * FROM "nozzle_operation_effects" WHERE "effect_id" = ?1'
    ) {
      if (this.#fault.kind === "initialized_effect_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_effects"
           WHERE "step_id" = ? ORDER BY rowid DESC LIMIT 1`,
        )
        .get(SAGA_INIT_OPERATION_STEP_ID) as Record<string, unknown>
      return new FixedStatement({ ...row, evidence_checksum: "wrong-evidence" })
    }
    if (
      (this.#fault.kind === "classified_from_record_mismatch" ||
        this.#fault.kind === "classified_status_mismatch" ||
        this.#fault.kind === "classified_duplicate_transition" ||
        this.#fault.kind === "classified_invalid_step_json" ||
        this.#fault.kind === "classified_noncanonical_step" ||
        this.#fault.kind === "classified_transition_mismatch") &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      const row = this.#base.database
        .prepare(`SELECT * FROM "nozzle_operation_transitions" ORDER BY rowid DESC LIMIT 1`)
        .get() as Record<string, unknown>
      if (this.#fault.kind === "classified_status_mismatch") {
        return new FixedStatement({ ...row, from_operation_status: "paused" })
      }
      if (this.#fault.kind === "classified_duplicate_transition") {
        return new FixedStatement(row)
      }
      if (this.#fault.kind === "classified_invalid_step_json") {
        return new FixedStatement({ ...row, from_record_json: "{" })
      }
      if (this.#fault.kind === "classified_noncanonical_step") {
        return new FixedStatement({
          ...row,
          from_record_json: ` ${row.from_record_json as string}`,
        })
      }
      if (this.#fault.kind === "classified_transition_mismatch") {
        return new FixedStatement({ ...row, holder_id: "wrong-holder" })
      }
      const from = JSON.parse(row.from_record_json as string) as Record<string, unknown>
      return new FixedStatement({
        ...row,
        from_record_json: JSON.stringify({
          ...from,
          startedAttempts: (from.startedAttempts as number) + 1,
        }),
      })
    }
    if (
      (this.#fault.kind === "classified_effect_mismatch" ||
        this.#fault.kind === "classified_saga_checksum_mismatch") &&
      sql.includes('WHERE "transition_id" = ?1') &&
      sql.includes("\"resource_kind\" = 'saga'")
    ) {
      const row = this.#base.database
        .prepare(`SELECT * FROM "nozzle_operation_effects" ORDER BY rowid DESC LIMIT 1`)
        .get() as Record<string, unknown>
      return new FixedStatement(
        this.#fault.kind === "classified_effect_mismatch"
          ? { ...row, effect_kind: "wrong-kind" }
          : { ...row, record_checksum: "wrong-checksum" },
      )
    }
    if (
      (this.#fault.kind === "classified_prior_missing" ||
        this.#fault.kind === "classified_prior_mismatch") &&
      sql.includes('"to_state_version" = ?2') &&
      sql.includes("\"resource_kind\" = 'saga'")
    ) {
      if (this.#fault.kind === "classified_prior_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT * FROM "nozzle_operation_effects"
           WHERE "resource_kind" = 'saga' ORDER BY "to_state_version" DESC LIMIT 1 OFFSET 1`,
        )
        .get() as Record<string, unknown>
      return new FixedStatement({ ...row, operation_id: "wrong-operation" })
    }
    if (
      (this.#fault.kind === "classified_audit_missing" ||
        this.#fault.kind === "classified_audit_mismatch") &&
      sql.includes('SELECT "event_json" FROM "nozzle_audit_log"')
    ) {
      if (this.#fault.kind === "classified_audit_missing") return new FixedStatement(null)
      const row = this.#base.database
        .prepare(
          `SELECT "event_json" FROM "nozzle_audit_log"
           ORDER BY "sequence" DESC LIMIT 1 OFFSET 1`,
        )
        .get() as Record<string, unknown>
      return new FixedStatement(row)
    }
    if (
      this.#fault.kind === "observation_cause_missing" &&
      sql.includes('FROM "nozzle_saga_action_attempts" AS "attempt"')
    ) {
      this.#sagaAttemptReads += 1
      if (this.#sagaAttemptReads === 2) return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "observation_saga_missing" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "settlement_saga_missing" &&
      sql.includes('FROM "nozzle_sagas" AS "saga"')
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "settlement_transition_missing" &&
      sql === 'SELECT * FROM "nozzle_operation_transitions" WHERE "transition_id" = ?1'
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "settlement_effect_missing" &&
      sql.includes('WHERE "transition_id" = ?1') &&
      sql.includes("\"resource_kind\" = 'saga'")
    ) {
      return new FixedStatement(null)
    }
    if (
      this.#fault.kind === "audit_snapshot" &&
      sql.includes('AS "now_ms"') &&
      sql.includes("nozzle_audit_log")
    ) {
      return new FixedStatement(this.#fault.row)
    }
    if (
      this.#fault.kind === "saga_operation_mismatch" &&
      sql === 'SELECT "operation_id" FROM "nozzle_sagas" WHERE "saga_id" = ?1'
    ) {
      return new FixedStatement({ operation_id: "another-operation" })
    }
    if (this.#batched) {
      if (this.#fault.kind === "transition_mismatch" && sql.includes("operation_transitions")) {
        const row = this.#base.database
          .prepare(`SELECT * FROM "nozzle_operation_transitions" ORDER BY rowid DESC LIMIT 1`)
          .get() as Record<string, unknown>
        return new FixedStatement({ ...row, step_id: "wrong-step" })
      }
      if (this.#fault.kind === "effect_missing" && sql.includes("nozzle_operation_effects")) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "effect_mismatch" && sql.includes("nozzle_operation_effects")) {
        const row = this.#base.database
          .prepare(`SELECT * FROM "nozzle_operation_effects" ORDER BY rowid DESC LIMIT 1`)
          .get() as Record<string, unknown>
        return new FixedStatement({ ...row, effect_kind: "wrong-kind" })
      }
      if (
        this.#fault.kind === "operation_missing" &&
        sql.includes('FROM "nozzle_operations" WHERE "operation_id"')
      ) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "saga_missing" && sql.includes('FROM "nozzle_sagas" AS "saga"')) {
        return new FixedStatement(null)
      }
      if (this.#fault.kind === "saga_regressed" && sql.includes('FROM "nozzle_sagas" AS "saga"')) {
        const current = this.#base.database
          .prepare(`SELECT * FROM "nozzle_sagas" ORDER BY rowid DESC LIMIT 1`)
          .get() as Record<string, unknown>
        const effect = this.#base.database
          .prepare(
            `SELECT * FROM "nozzle_operation_effects"
             WHERE "resource_kind" = 'saga' AND "resource_id" = ?
               AND "to_state_version" = ?`,
          )
          .get(current.saga_id as string, (current.state_version as number) - 1) as Record<
          string,
          unknown
        >
        const record = JSON.parse(effect.record_json as string) as {
          readonly stateVersion: number
          readonly status: string
          readonly terminationCause: string | null
          readonly terminationRequestedAtMs: number | null
        }
        return new FixedStatement({
          ...current,
          commitment: "none",
          effect_evidence_checksum: effect.evidence_checksum,
          effect_id: effect.effect_id,
          effect_operation_id: effect.operation_id,
          effect_record_checksum: effect.record_checksum,
          effect_record_json: effect.record_json,
          effect_resource_id: effect.resource_id,
          effect_resource_kind: effect.resource_kind,
          effect_to_state_version: effect.to_state_version,
          last_effect_id: effect.effect_id,
          last_evidence_checksum: effect.evidence_checksum,
          record_checksum: effect.record_checksum,
          record_json: effect.record_json,
          state_version: record.stateVersion,
          status: record.status,
          termination_cause: record.terminationCause,
          termination_requested_at_ms: record.terminationRequestedAtMs,
        })
      }
      if (this.#fault.kind === "saga_projection" && sql.includes('FROM "nozzle_sagas" AS "saga"')) {
        return new FixedStatement(this.#fault.row)
      }
      if (
        this.#fault.kind === "audit_missing" &&
        sql.includes('SELECT "event_json" FROM "nozzle_audit_log"')
      ) {
        return new FixedStatement(null)
      }
      if (
        this.#fault.kind === "audit_mismatch" &&
        sql.includes('SELECT "event_json" FROM "nozzle_audit_log"')
      ) {
        const row = this.#base.database
          .prepare(
            `SELECT "event_json" FROM "nozzle_audit_log"
             ORDER BY "sequence" DESC LIMIT 1 OFFSET 1`,
          )
          .get() as Record<string, unknown>
        return new FixedStatement(row)
      }
    }
    return this.#base.prepare(sql)
  }
}

interface Fixture {
  readonly actionInputJson: string
  readonly attempts: D1SagaAttemptStore
  readonly base: DatabaseAdapter
  readonly coordinator: D1SagaCoordinatorStore
  readonly initialize: InitializeSagaInput
  readonly leases: D1LeaseStore
  readonly operationId: string
  readonly operations: D1OperationStore
  readonly proof: ReturnType<typeof leaseProof>
  readonly sagaId: string
  readonly sagas: D1SagaStore
}

const databases: DatabaseAdapter[] = []
afterEach(() => {
  for (const database of databases.splice(0)) database.close()
})

async function fixture(
  suffix: string,
  fault?: BatchFault,
  queryFault?: QueryFault,
  omitActionPlan = false,
  omitTerminationPlan = false,
): Promise<Fixture> {
  const base = new DatabaseAdapter()
  databases.push(base)
  const database =
    queryFault !== undefined
      ? new QueryFaultDatabase(base, queryFault)
      : fault === undefined
        ? base
        : new FaultDatabase(base, fault)
  const operations = new D1OperationStore(base, digest)
  const leases = new D1LeaseStore(base)
  const sagas = new D1SagaStore(base, digest)
  const attempts = new D1SagaAttemptStore(base, digest)
  const coordinator = new D1SagaCoordinatorStore(database, digest)
  const operationId = `coordinator-operation-${suffix}`
  const sagaId = `coordinator-saga-${suffix}`
  const leaseKey = `saga:${sagaId}`
  const forwardStepId = sagaActionOperationStepId("a", "forward")
  const compensationStepId = sagaActionOperationStepId("a", "compensation")
  const descriptor = await sealSagaDescriptor(
    {
      descriptorId: `coordinator-descriptor-${suffix}`,
      steps: [
        {
          authorizationPolicyChecksum: null,
          baseRetryDelayMs: 0,
          compensationAction: {
            actionId: "a.compensate",
            artifactChecksum: "cc".repeat(32),
            version: 1,
          },
          compensationObservation: {
            actionId: "a.observe-compensation",
            artifactChecksum: "dd".repeat(32),
            version: 1,
          },
          forwardAction: {
            actionId: "a.forward",
            artifactChecksum: "aa".repeat(32),
            version: 1,
          },
          forwardObservation: {
            actionId: "a.observe-forward",
            artifactChecksum: "bb".repeat(32),
            version: 1,
          },
          inputSchemaChecksum: "11".repeat(32),
          irreversible: false,
          maxAttempts: 3,
          maxRetryDelayMs: 100,
          outputSchemaChecksum: "22".repeat(32),
          stepId: "a",
          timeoutMs: 1_000,
        },
      ],
      version: 1,
    },
    digest,
  )
  const actionInputJson = '{"value":1}'
  const actionInputChecksum = await sagaActionInputChecksum(actionInputJson, digest)
  const capabilitySnapshotJson = '{"runtime":"coordinator-v1"}'
  const operationInputJson = JSON.stringify({ sagaId })
  const plan = await sealOperationPlan(
    {
      capabilitySnapshotChecksum: await digest(new TextEncoder().encode(capabilitySnapshotJson)),
      idempotencyKey: `${operationId}:key`,
      inputChecksum: await digest(new TextEncoder().encode(operationInputJson)),
      operationId,
      operationType: `saga:${descriptor.descriptorId}@1`,
      steps: (
        [
          {
            checkpoint: "reversible",
            idempotencyKey: `${operationId}:init:key`,
            inputChecksum: `${operationId}:init:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:init:postcondition`,
            preconditionChecksum: `${operationId}:init:precondition`,
            recoveryInstructions: "Create the saga projection through the coupled coordinator.",
            retryClassification: "idempotent",
            stepId: SAGA_INIT_OPERATION_STEP_ID,
          },
          {
            checkpoint: "reversible",
            completionRole: "settlement",
            idempotencyKey: `${operationId}:settle:key`,
            inputChecksum: `${operationId}:settle:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:settle:postcondition`,
            preconditionChecksum: `${operationId}:settle:precondition`,
            recoveryInstructions: "Settle only from the terminal saga projection.",
            retryClassification: "never",
            stepId: SAGA_SETTLE_OPERATION_STEP_ID,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            idempotencyKey: `${operationId}:termination:key`,
            inputChecksum: `${operationId}:termination:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:termination:postcondition`,
            preconditionChecksum: `${operationId}:termination:precondition`,
            recoveryInstructions: "Materialize termination under the saga lease.",
            retryClassification: "idempotent",
            stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            effectProtocol: "saga_receipt",
            idempotencyKey: sagaActionIdempotencyKey(sagaId, "a", "forward"),
            inputChecksum: actionInputChecksum,
            leaseKey,
            postconditionChecksum: `${operationId}:forward:postcondition`,
            preconditionChecksum: `${operationId}:forward:precondition`,
            recoveryInstructions: "Recover the exact forward action receipt.",
            retryClassification: "reconcile_first",
            stepId: forwardStepId,
          },
          {
            activation: "conditional",
            checkpoint: "reversible",
            effectProtocol: "saga_receipt",
            idempotencyKey: sagaActionIdempotencyKey(sagaId, "a", "compensation"),
            inputChecksum: `${operationId}:compensation:input`,
            leaseKey,
            postconditionChecksum: `${operationId}:compensation:postcondition`,
            preconditionChecksum: `${operationId}:compensation:precondition`,
            recoveryInstructions: "Recover the exact compensation receipt.",
            retryClassification: "reconcile_first",
            stepId: compensationStepId,
          },
        ] as const satisfies readonly OperationStepPlanInput[]
      ).filter(
        (step) =>
          (!omitActionPlan || step.stepId !== forwardStepId) &&
          (!omitTerminationPlan || step.stepId !== SAGA_TERMINATION_OPERATION_STEP_ID),
      ),
    },
    digest,
  )
  await operations.create({
    actorChecksum: "coordinator-test-actor",
    capabilitySnapshotJson,
    environmentId: "production",
    idempotencyScope: `coordinator-${suffix}`,
    inputJson: operationInputJson,
    plan,
    requiredShardIds: ["shard-a"],
  })
  const acquired = await leases.acquire({
    acquisitionId: `coordinator-acquisition-${suffix}`,
    holderId: `coordinator-controller-${suffix}`,
    leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Coordinator fixture lease acquisition failed.")
  const proof = leaseProof(acquired.record)
  const initAttemptId = `${sagaId}:init:1`
  await operations.beginStep({
    actorChecksum: "coordinator-test-actor",
    attemptId: initAttemptId,
    idempotencyKey: `${operationId}:init:key`,
    observedPreconditionChecksum: `${operationId}:init:precondition`,
    operationId,
    proof,
    stepId: SAGA_INIT_OPERATION_STEP_ID,
  })
  return {
    actionInputJson,
    attempts,
    base,
    coordinator,
    initialize: {
      actorChecksum: "coordinator-test-actor",
      attemptId: initAttemptId,
      deadlineAtMs: 8_000_000_000_000_000,
      descriptor,
      evidenceChecksum: `${sagaId}:init:evidence`,
      idempotencyKey: `${sagaId}:key`,
      inputChecksum: `${sagaId}:input`,
      observedPostconditionChecksum: `${operationId}:init:postcondition`,
      operationId,
      proof,
      resultChecksum: `${sagaId}:init:result`,
      sagaId,
      stepInputChecksums: { a: actionInputChecksum },
    },
    leases,
    operationId,
    operations,
    proof,
    sagaId,
    sagas,
  }
}

async function terminalReceipt(
  run: Fixture,
  state: "confirmed" | "failed" | "not_applied" | "unknown",
  attemptId = `${run.sagaId}:a:forward:1`,
  proof = run.proof,
  phase: "compensation" | "forward" = "forward",
) {
  await run.coordinator.beginAction(actionInput(run, attemptId, proof, phase))
  await run.attempts.accept({
    attemptId,
    inputJson: run.actionInputJson,
    phase,
    proof,
    purpose: "effect",
    sagaId: run.sagaId,
    sagaStepId: "a",
  })
  return state === "confirmed"
    ? run.attempts.complete({
        attemptId,
        evidenceJson: JSON.stringify({ attemptId, source: "provider" }),
        outputJson: JSON.stringify({ attemptId, value: "created" }),
        proof,
        state,
      })
    : run.attempts.complete({
        attemptId,
        errorJson: JSON.stringify({ attemptId, code: state }),
        evidenceJson: JSON.stringify({ attemptId, source: "provider" }),
        proof,
        state,
      })
}

function actionInput(
  run: Fixture,
  attemptId = `${run.sagaId}:a:forward:1`,
  proof = run.proof,
  phase: "compensation" | "forward" = "forward",
) {
  return {
    actorChecksum: "coordinator-test-actor",
    attemptId,
    operationId: run.operationId,
    phase,
    proof,
    sagaId: run.sagaId,
    stepId: "a",
  }
}

async function reacquire(run: Fixture, proof: Fixture["proof"], suffix: string) {
  await run.leases.release({ proof })
  const acquired = await run.leases.acquire({
    acquisitionId: `${run.sagaId}:${suffix}:acquisition`,
    holderId: `${run.sagaId}:${suffix}:controller`,
    leaseKey: proof.leaseKey,
    ttlMs: 60_000,
  })
  if (!acquired.acquired) throw new Error("Expected saga coordinator lease reacquisition.")
  return leaseProof(acquired.record)
}

async function unknownEffectObservation(
  run: Fixture,
  input: {
    readonly effectProof: Fixture["proof"]
    readonly phase?: "compensation" | "forward"
    readonly sequence: number
    readonly state: "confirmed" | "indeterminate" | "not_applied"
  },
) {
  const phase = input.phase ?? "forward"
  const effectAttemptId = `${run.sagaId}:a:${phase}:${input.sequence}`
  const effectReceipt = await terminalReceipt(
    run,
    "unknown",
    effectAttemptId,
    input.effectProof,
    phase,
  )
  if (effectReceipt.state !== "unknown") throw new Error("Expected an unknown saga effect.")
  await run.coordinator.settleActionFromReceipt(
    actionInput(run, effectAttemptId, input.effectProof, phase),
  )
  const observationProof = await reacquire(
    run,
    input.effectProof,
    `${phase}:observation:${input.sequence}`,
  )
  const observationAttemptId = `${effectAttemptId}:observation`
  await run.attempts.accept({
    attemptId: observationAttemptId,
    inputJson: JSON.stringify({ effectAttemptId }),
    phase,
    proof: observationProof,
    purpose: "observation",
    sagaId: run.sagaId,
    sagaStepId: "a",
  })
  const observationReceipt =
    input.state === "confirmed"
      ? await run.attempts.complete({
          attemptId: observationAttemptId,
          evidenceJson: JSON.stringify({ effectAttemptId, source: "observation" }),
          outputJson: JSON.stringify({ effectAttemptId, observed: "applied" }),
          proof: observationProof,
          state: input.state,
        })
      : await run.attempts.complete({
          attemptId: observationAttemptId,
          errorJson: JSON.stringify({ effectAttemptId, observed: input.state }),
          evidenceJson: JSON.stringify({ effectAttemptId, source: "observation" }),
          proof: observationProof,
          state: input.state,
        })
  return { effectAttemptId, observationAttemptId, observationProof, observationReceipt }
}

function terminationInput(
  run: Fixture,
  proof: Fixture["proof"] = run.proof,
  cause: "cancellation" | "timeout" = "cancellation",
  requestSuffix: string = cause,
): RequestCoordinatedSagaTerminationInput {
  return {
    actorChecksum: "coordinator-test-actor",
    cause,
    operationId: run.operationId,
    proof,
    requestChecksum: `${run.sagaId}:termination:${requestSuffix}:checksum`,
    requestId: `${run.sagaId}:termination:${requestSuffix}:request`,
    sagaId: run.sagaId,
  }
}

async function requestTermination(run: Fixture, proof: Fixture["proof"]) {
  return run.coordinator.requestTermination(terminationInput(run, proof))
}

type RecoveryMode = "accepted_unknown" | "not_dispatched"
type RecoveryPhase = "compensation" | "forward"

async function recoverableAction(
  run: Fixture,
  mode: RecoveryMode,
  phase: RecoveryPhase,
  proofSuffix: string,
) {
  await run.coordinator.initializeSaga(run.initialize)
  if (phase === "compensation") {
    const forwardAttemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", forwardAttemptId)
    await requestTermination(run, run.proof)
    await run.coordinator.settleActionFromReceipt(actionInput(run, forwardAttemptId))
  }
  const attemptId = `${run.sagaId}:a:${phase}:1`
  await run.coordinator.beginAction(actionInput(run, attemptId, run.proof, phase))
  const receipt =
    mode === "accepted_unknown"
      ? await run.attempts.accept({
          attemptId,
          inputJson: run.actionInputJson,
          phase,
          proof: run.proof,
          purpose: "effect",
          sagaId: run.sagaId,
          sagaStepId: "a",
        })
      : undefined
  const recoveryProof = await reacquire(run, run.proof, proofSuffix)
  return Object.freeze({
    attemptId,
    phase,
    receipt,
    recovery: {
      ...actionInput(run, attemptId, recoveryProof, phase),
      recoveryId: `${attemptId}:recovery`,
    },
    recoveryProof,
  })
}

function count(database: DatabaseSync, table: string): number {
  return (database.prepare(`SELECT count(*) AS "count" FROM "${table}"`).get() as { count: number })
    .count
}

function coupledProjectionRows(database: DatabaseSync) {
  return {
    audit: database.prepare(`SELECT * FROM "nozzle_audit_log" ORDER BY rowid`).all(),
    effects: database.prepare(`SELECT * FROM "nozzle_operation_effects" ORDER BY rowid`).all(),
    operations: database.prepare(`SELECT * FROM "nozzle_operations" ORDER BY rowid`).all(),
    sagas: database.prepare(`SELECT * FROM "nozzle_sagas" ORDER BY rowid`).all(),
    steps: database.prepare(`SELECT * FROM "nozzle_operation_steps" ORDER BY rowid`).all(),
    transitions: database
      .prepare(`SELECT * FROM "nozzle_operation_transitions" ORDER BY rowid`)
      .all(),
  }
}

function acceptedAttemptRow(database: DatabaseSync, attemptId: string): Record<string, unknown> {
  const row = database
    .prepare(
      `SELECT "attempt".*, "protocol"."protocol_version",
              "protocol"."classified_at_ms" AS "protocol_classified_at_ms"
       FROM "nozzle_saga_action_attempts" AS "attempt"
       JOIN "nozzle_saga_action_attempt_protocols" AS "protocol" USING ("attempt_id")
       WHERE "attempt"."attempt_id" = ?`,
    )
    .get(attemptId) as Record<string, unknown>
  return {
    ...row,
    completed_at_ms: null,
    error_checksum: null,
    error_json: null,
    evidence_checksum: null,
    evidence_json: null,
    outcome_checksum: null,
    output_checksum: null,
    output_json: null,
    state: null,
  }
}

function leaseExpiration(run: Fixture): number {
  const row = run.base.database
    .prepare(`SELECT "expires_at_ms" FROM "nozzle_leases" WHERE "lease_key" = ?`)
    .get(run.proof.leaseKey) as { readonly expires_at_ms: number }
  return row.expires_at_ms
}

async function contradictoryTerminationProjection(run: Fixture): Promise<Record<string, unknown>> {
  const row = run.base.database
    .prepare(
      `SELECT "saga".*,
              "effect"."effect_id" AS "effect_id",
              "effect"."resource_kind" AS "effect_resource_kind",
              "effect"."resource_id" AS "effect_resource_id",
              "effect"."operation_id" AS "effect_operation_id",
              "effect"."to_state_version" AS "effect_to_state_version",
              "effect"."evidence_checksum" AS "effect_evidence_checksum",
              "effect"."record_checksum" AS "effect_record_checksum",
              "effect"."record_json" AS "effect_record_json"
       FROM "nozzle_sagas" AS "saga"
       JOIN "nozzle_operation_effects" AS "effect"
         ON "effect"."effect_id" = "saga"."last_effect_id"
       WHERE "saga"."saga_id" = ?`,
    )
    .get(run.sagaId) as Record<string, unknown>
  const record = JSON.parse(row.record_json as string) as Record<string, unknown>
  const inputChecksum = `${record.inputChecksum as string}:contradictory`
  const recordJson = JSON.stringify({ ...record, inputChecksum })
  const recordChecksum = await sagaRecordChecksum(recordJson)
  return {
    ...row,
    effect_record_checksum: recordChecksum,
    effect_record_json: recordJson,
    input_checksum: inputChecksum,
    record_checksum: recordChecksum,
    record_json: recordJson,
  }
}

function initialTerminationProjection(run: Fixture): Record<string, unknown> {
  const current = run.base.database
    .prepare(`SELECT * FROM "nozzle_sagas" WHERE "saga_id" = ?`)
    .get(run.sagaId) as Record<string, unknown>
  const effect = run.base.database
    .prepare(
      `SELECT * FROM "nozzle_operation_effects"
       WHERE "resource_kind" = 'saga' AND "resource_id" = ? AND "to_state_version" = 0`,
    )
    .get(run.sagaId) as Record<string, unknown>
  const record = JSON.parse(effect.record_json as string) as {
    readonly stateVersion: number
    readonly status: string
    readonly terminationCause: string | null
    readonly terminationRequestedAtMs: number | null
  }
  return {
    ...current,
    commitment: "none",
    effect_evidence_checksum: effect.evidence_checksum,
    effect_id: effect.effect_id,
    effect_operation_id: effect.operation_id,
    effect_record_checksum: effect.record_checksum,
    effect_record_json: effect.record_json,
    effect_resource_id: effect.resource_id,
    effect_resource_kind: effect.resource_kind,
    effect_to_state_version: effect.to_state_version,
    last_effect_id: effect.effect_id,
    last_evidence_checksum: effect.evidence_checksum,
    record_checksum: effect.record_checksum,
    record_json: effect.record_json,
    state_version: record.stateVersion,
    status: record.status,
    termination_cause: record.terminationCause,
    termination_requested_at_ms: record.terminationRequestedAtMs,
  }
}

describe("D1SagaCoordinatorStore", () => {
  it("atomically classifies a confirmed terminal receipt across both ledgers", async () => {
    const run = await fixture("settle-confirmed")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    if (receipt.state !== "confirmed") throw new Error("Expected a confirmed receipt.")

    const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.steps.a?.forward).toMatchObject({
      lastAttemptId: attemptId,
      resultChecksum: receipt.outputChecksum,
      state: "succeeded",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({
      lastAttemptId: attemptId,
      resultChecksum: receipt.outcomeChecksum,
      state: "succeeded",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)

    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).resolves.toEqual(settled)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("keeps retryable not-applied receipts retryable until the sealed budget is exhausted", async () => {
    const run = await fixture("settle-retries")
    await run.coordinator.initializeSaga(run.initialize)

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const attemptId = `${run.sagaId}:a:forward:${attempt}`
      const receipt = await terminalReceipt(run, "not_applied", attemptId)
      if (receipt.state === "accepted" || receipt.state === "confirmed") {
        throw new Error("Expected a failed receipt.")
      }
      const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
      const action = settled.steps.a?.forward
      const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
        "saga:forward:a"
      ]
      if (attempt < 3) {
        expect(action).toMatchObject({
          attempts: attempt,
          errorChecksum: receipt.errorChecksum,
          state: "retryable_failed",
        })
        expect(operationAction).toMatchObject({
          errorChecksum: receipt.outcomeChecksum,
          state: "retryable_failed",
        })
      } else {
        expect(settled).toMatchObject({ status: "failed", terminationCause: "failure" })
        expect(action).toMatchObject({
          attempts: 3,
          errorChecksum: receipt.errorChecksum,
          state: "failed",
        })
        expect(operationAction).toMatchObject({
          resultChecksum: receipt.outcomeChecksum,
          state: "succeeded",
        })
        const effect = run.base.database
          .prepare(
            `SELECT "effect_kind" FROM "nozzle_operation_effects"
             WHERE "operation_id" = ? AND "step_id" = 'saga:forward:a'
             ORDER BY "created_at_ms" DESC, rowid DESC LIMIT 1`,
          )
          .get(run.operationId) as { effect_kind: string }
        expect(effect.effect_kind).toBe("action:forward:failure:definitely_not_applied_terminal")
      }
      await expect(
        run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
      ).resolves.toEqual(settled)
      if (attempt === 2) {
        await expect(
          run.coordinator.settleActionFromReceipt(actionInput(run, `${run.sagaId}:a:forward:1`)),
        ).resolves.toEqual(settled)
      }
    }
  })

  it.each([
    ["failed", "failed", "succeeded", "failed"],
    ["unknown", "unknown", "unknown", "running"],
  ] as const)("maps a %s receipt without confusing protocol classification with business success", async (receiptState, sagaState, operationState, sagaStatus) => {
    const run = await fixture(`settle-${receiptState}`)
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, receiptState, attemptId)
    if (receipt.state === "accepted" || receipt.state === "confirmed") {
      throw new Error("Expected a failed receipt.")
    }

    const settled = await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.status).toBe(sagaStatus)
    expect(settled.steps.a?.forward).toMatchObject({
      errorChecksum: receipt.errorChecksum,
      state: sagaState,
    })
    const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
      "saga:forward:a"
    ]
    expect(operationAction).toMatchObject(
      operationState === "succeeded"
        ? { resultChecksum: receipt.outcomeChecksum, state: operationState }
        : { errorChecksum: receipt.outcomeChecksum, state: operationState },
    )
    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).resolves.toEqual(settled)
  })

  it("rejects direct generic classification without diverging either ledger", async () => {
    const run = await fixture("settle-active-divergence")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    if (receipt.state !== "confirmed") throw new Error("Expected a confirmed receipt.")
    await expect(
      run.operations.completeStep({
        actorChecksum: "coordinator-test-actor",
        attemptId,
        observedPostconditionChecksum: `${run.operationId}:forward:postcondition`,
        operationId: run.operationId,
        proof: run.proof,
        resultChecksum: receipt.outcomeChecksum,
        stepId: "saga:forward:a",
      }),
    ).rejects.toThrow(/must be consumed through D1SagaCoordinatorStore/u)
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"]?.state,
    ).toBe("running")
    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, attemptId)),
    ).resolves.toMatchObject({ status: "succeeded" })
  })

  it("fails closed on corrupted active projections before either classification path", async () => {
    const effect = await fixture("settle-corrupt-active")
    await effect.coordinator.initializeSaga(effect.initialize)
    const effectAttemptId = `${effect.sagaId}:a:forward:1`
    await terminalReceipt(effect, "confirmed", effectAttemptId)
    const sagaRow = effect.base.database
      .prepare(`SELECT "last_effect_id", "record_json" FROM "nozzle_sagas" WHERE "saga_id" = ?`)
      .get(effect.sagaId) as { last_effect_id: string; record_json: string }
    const sagaRecord = JSON.parse(sagaRow.record_json) as {
      steps: { a: { forward: Record<string, unknown> } }
    }
    sagaRecord.steps.a.forward.activeAttemptId = "wrong-attempt"
    sagaRecord.steps.a.forward.lastAttemptId = "wrong-attempt"
    const recordJson = JSON.stringify(sagaRecord)
    const recordChecksum = await sagaRecordChecksum(recordJson)
    effect.base.database.exec(
      `DROP TRIGGER "nozzle_control_operation_effect_update";
       DROP TRIGGER "nozzle_control_saga_update";`,
    )
    effect.base.database
      .prepare(
        `UPDATE "nozzle_operation_effects" SET "record_json" = ?, "record_checksum" = ?
         WHERE "effect_id" = ?`,
      )
      .run(recordJson, recordChecksum, sagaRow.last_effect_id)
    effect.base.database
      .prepare(
        `UPDATE "nozzle_sagas" SET "record_json" = ?, "record_checksum" = ?
         WHERE "saga_id" = ?`,
      )
      .run(recordJson, recordChecksum, effect.sagaId)
    await expect(
      effect.coordinator.settleActionFromReceipt(actionInput(effect, effectAttemptId)),
    ).rejects.toThrow(/contradicts the active coupled attempt/u)

    const observation = await fixture("observe-corrupt-active")
    await observation.coordinator.initializeSaga(observation.initialize)
    const observed = await unknownEffectObservation(observation, {
      effectProof: observation.proof,
      sequence: 1,
      state: "confirmed",
    })
    const stepRow = observation.base.database
      .prepare(
        `SELECT "record_json" FROM "nozzle_operation_steps"
         WHERE "operation_id" = ? AND "step_id" = 'saga:forward:a'`,
      )
      .get(observation.operationId) as { record_json: string }
    const stepRecord = JSON.parse(stepRow.record_json) as Record<string, unknown>
    stepRecord.errorChecksum = "wrong-error"
    observation.base.database.exec(`DROP TRIGGER "nozzle_control_step_state_update";`)
    observation.base.database
      .prepare(
        `UPDATE "nozzle_operation_steps" SET "record_json" = ?
         WHERE "operation_id" = ? AND "step_id" = 'saga:forward:a'`,
      )
      .run(JSON.stringify(stepRecord), observation.operationId)
    await expect(
      observation.coordinator.settleObservationFromReceipt(
        actionInput(observation, observed.observationAttemptId, observed.observationProof),
      ),
    ).rejects.toThrow(/contradicts the unknown coupled action/u)
  })

  it("rejects observation-only outcomes and missing exact replay evidence", async () => {
    const observation = await fixture("settle-observation")
    await observation.coordinator.initializeSaga(observation.initialize)
    const effectAttemptId = `${observation.sagaId}:a:forward:1`
    await terminalReceipt(observation, "unknown", effectAttemptId)
    await observation.coordinator.settleActionFromReceipt(actionInput(observation, effectAttemptId))
    await observation.leases.release({ proof: observation.proof })
    const reacquired = await observation.leases.acquire({
      acquisitionId: `${observation.sagaId}:observation-acquisition`,
      holderId: `${observation.sagaId}:observation-controller`,
      leaseKey: observation.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!reacquired.acquired) throw new Error("Expected the observation lease.")
    const observationProof = leaseProof(reacquired.record)
    const observationAttemptId = `${effectAttemptId}:observation`
    await observation.attempts.accept({
      attemptId: observationAttemptId,
      inputJson: '{"observe":true}',
      phase: "forward",
      proof: observationProof,
      purpose: "observation",
      sagaId: observation.sagaId,
      sagaStepId: "a",
    })
    await observation.attempts.complete({
      attemptId: observationAttemptId,
      errorJson: '{"code":"indeterminate"}',
      evidenceJson: '{"source":"provider"}',
      proof: observationProof,
      state: "indeterminate",
    })
    await expect(
      observation.coordinator.settleActionFromReceipt({
        ...actionInput(observation, observationAttemptId),
        proof: observationProof,
      }),
    ).rejects.toThrow(/cannot be indeterminate/u)

    const replay = await fixture("settle-replay-evidence")
    await replay.coordinator.initializeSaga(replay.initialize)
    const attemptId = `${replay.sagaId}:a:forward:1`
    await terminalReceipt(replay, "confirmed", attemptId)
    await replay.coordinator.settleActionFromReceipt(actionInput(replay, attemptId))
    for (const [kind, message] of [
      ["settlement_saga_missing", /saga does not exist/u],
      ["settlement_transition_missing", /exact operation transition/u],
      ["settlement_effect_missing", /exact coupled effect receipt/u],
    ] as const) {
      const faulted = new D1SagaCoordinatorStore(
        new QueryFaultDatabase(replay.base, { kind }),
        digest,
      )
      await expect(faulted.settleActionFromReceipt(actionInput(replay, attemptId))).rejects.toThrow(
        message,
      )
    }
    for (const [kind, message] of [
      ["classified_invalid_step_json", /invalid JSON/u],
      ["classified_noncanonical_step", /not canonical/u],
      ["classified_transition_mismatch", /contradictory operation transition/u],
      ["classified_effect_mismatch", /exact coupled effect receipt/u],
      ["classified_saga_checksum_mismatch", /historical saga effect record is contradictory/u],
      ["classified_prior_missing", /lacks its prior immutable saga version/u],
      ["classified_prior_mismatch", /contradictory saga version chain/u],
      ["classified_from_record_mismatch", /history contradicts/u],
      ["classified_status_mismatch", /history contradicts/u],
      ["classified_audit_missing", /lacks its exact audit event/u],
      ["classified_audit_mismatch", /audit event is contradictory/u],
    ] as const) {
      const faulted = new D1SagaCoordinatorStore(
        new QueryFaultDatabase(replay.base, { kind }),
        digest,
      )
      await expect(faulted.settleActionFromReceipt(actionInput(replay, attemptId))).rejects.toThrow(
        message,
      )
    }

    const duplicate = await fixture("settle-duplicate-history")
    await duplicate.coordinator.initializeSaga(duplicate.initialize)
    const duplicateAttemptId = `${duplicate.sagaId}:a:forward:1`
    await terminalReceipt(duplicate, "not_applied", duplicateAttemptId)
    await duplicate.coordinator.settleActionFromReceipt(actionInput(duplicate, duplicateAttemptId))
    const faulted = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(duplicate.base, { kind: "classified_duplicate_transition" }),
      digest,
    )
    await expect(
      faulted.settleActionFromReceipt(actionInput(duplicate, duplicateAttemptId)),
    ).rejects.toThrow(/contradictory operation transitions/u)
  })

  it("requires an exact terminal effect receipt and exact coupled attempt identity", async () => {
    const missing = await fixture("settle-missing")
    await missing.coordinator.initializeSaga(missing.initialize)
    await expect(missing.coordinator.settleActionFromReceipt(actionInput(missing))).rejects.toThrow(
      /not durably accepted/u,
    )

    const accepted = await fixture("settle-accepted")
    await accepted.coordinator.initializeSaga(accepted.initialize)
    const attemptId = `${accepted.sagaId}:a:forward:1`
    await accepted.coordinator.beginAction(actionInput(accepted, attemptId))
    await accepted.attempts.accept({
      attemptId,
      inputJson: accepted.actionInputJson,
      phase: "forward",
      proof: accepted.proof,
      purpose: "effect",
      sagaId: accepted.sagaId,
      sagaStepId: "a",
    })
    await expect(
      accepted.coordinator.settleActionFromReceipt(actionInput(accepted, attemptId)),
    ).rejects.toThrow(/no terminal receipt/u)

    const terminal = await terminalReceipt(accepted, "confirmed", attemptId)
    expect(terminal.state).toBe("confirmed")
    await expect(
      accepted.coordinator.settleActionFromReceipt({
        ...actionInput(accepted, attemptId),
        operationId: "another-operation",
      }),
    ).rejects.toThrow(/different action/u)
    await expect(
      accepted.coordinator.settleActionFromReceipt({
        ...actionInput(accepted, attemptId),
        proof: { ...accepted.proof, acquisitionId: "another-acquisition" },
      }),
    ).rejects.toThrow(/cannot be consumed under this lease fence/u)
  })

  it("recovers an exact terminal settlement after losing the D1 response", async () => {
    const run = await fixture("settle-lost-response")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const receipt = await terminalReceipt(run, "confirmed", attemptId)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { kind: "commit_then_throw" }),
      digest,
    )

    const settled = await faulted.settleActionFromReceipt(actionInput(run, attemptId))
    expect(settled.steps.a?.forward).toMatchObject({
      resultChecksum: receipt.state === "confirmed" ? receipt.outputChecksum : undefined,
      state: "succeeded",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("returns the current saga when a committed receipt advances before readback", async () => {
    const run = await fixture("settle-readback-advance")
    await run.coordinator.initializeSaga(run.initialize)
    const firstAttemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "not_applied", firstAttemptId)
    const nextAttemptId = `${run.sagaId}:a:forward:2`
    const coordinator = new D1SagaCoordinatorStore(
      new AdvanceAfterCommitDatabase(run.base, async () => {
        await run.coordinator.beginAction(actionInput(run, nextAttemptId))
      }),
      digest,
    )

    const current = await coordinator.settleActionFromReceipt(actionInput(run, firstAttemptId))

    expect(current.steps.a?.forward).toMatchObject({
      activeAttemptId: nextAttemptId,
      attempts: 2,
      state: "running",
    })
    await expect(run.sagas.get(run.sagaId)).resolves.toEqual(current)
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({ activeAttemptId: nextAttemptId, state: "running" })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(4)
  })

  it("recomputes a begin decision when its committed action advances before readback", async () => {
    const run = await fixture("begin-readback-advance")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const coordinator = new D1SagaCoordinatorStore(
      new AdvanceAfterCommitDatabase(run.base, async () => {
        await run.attempts.accept({
          attemptId,
          inputJson: run.actionInputJson,
          phase: "forward",
          proof: run.proof,
          purpose: "effect",
          sagaId: run.sagaId,
          sagaStepId: "a",
        })
        await run.attempts.complete({
          attemptId,
          evidenceJson: '{"source":"interleaving-provider"}',
          outputJson: '{"created":true}',
          proof: run.proof,
          state: "confirmed",
        })
        await run.coordinator.settleActionFromReceipt(actionInput(run, attemptId))
      }),
      digest,
    )

    const decision = await coordinator.beginAction(actionInput(run, attemptId))

    expect(decision).toMatchObject({ disposition: "replay_success" })
    expect(decision.saga.steps.a?.forward.state).toBe("succeeded")
    await expect(run.sagas.get(run.sagaId)).resolves.toEqual(decision.saga)
  })

  it("rolls terminal settlement back as one unit when an interior statement fails", async () => {
    const run = await fixture("settle-rollback")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", attemptId)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index: 3, kind: "rollback_before" }),
      digest,
    )

    await expect(faulted.settleActionFromReceipt(actionInput(run, attemptId))).rejects.toThrow(
      /Settling a saga action exceeded/u,
    )
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("running")
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"]?.state,
    ).toBe("running")
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)
  })

  it("atomically recovers an accepted attempt without an outcome as unknown", async () => {
    const run = await fixture("recover-accepted")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, attemptId))
    const receipt = await run.attempts.accept({
      attemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const recoveryProof = await reacquire(run, run.proof, "accepted-recovery")
    const recovery = {
      ...actionInput(run, attemptId, recoveryProof),
      recoveryId: `${attemptId}:recovery`,
    }

    const recovered = await run.coordinator.recoverActionAfterCrash(recovery)
    expect(recovered.steps.a?.forward).toMatchObject({
      errorChecksum: receipt.acceptanceChecksum,
      lastAttemptId: attemptId,
      state: "unknown",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({
      errorChecksum: receipt.acceptanceChecksum,
      lastAttemptId: attemptId,
      state: "unknown",
    })
    await expect(run.coordinator.recoverActionAfterCrash(recovery)).resolves.toEqual(recovered)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("fails closed when immutable recovery replay evidence is missing or contradictory", async () => {
    const run = await fixture("recover-replay-evidence")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, attemptId))
    await run.attempts.accept({
      attemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const recoveryProof = await reacquire(run, run.proof, "replay-evidence")
    const recovery = {
      ...actionInput(run, attemptId, recoveryProof),
      recoveryId: `${attemptId}:recovery`,
    }
    await run.coordinator.recoverActionAfterCrash(recovery)

    for (const [kind, message] of [
      ["classified_transition_mismatch", /contradictory operation transition/u],
      ["classified_audit_missing", /lacks its exact audit event/u],
      ["classified_audit_mismatch", /audit event is contradictory/u],
      ["settlement_effect_missing", /lacks its exact coupled effect receipt/u],
      ["classified_prior_missing", /lacks its prior immutable saga version/u],
      ["classified_effect_mismatch", /history contradicts/u],
    ] as const) {
      const faulted = new D1SagaCoordinatorStore(new QueryFaultDatabase(run.base, { kind }), digest)
      await expect(faulted.recoverActionAfterCrash(recovery)).rejects.toThrow(message)
    }
  })

  it("fails closed on mismatched saga crash-recovery identity, state, and fence", async () => {
    const identity = await fixture("recover-wrong-identity")
    await identity.coordinator.initializeSaga(identity.initialize)
    const identityAttemptId = `${identity.sagaId}:a:forward:1`
    await identity.coordinator.beginAction(actionInput(identity, identityAttemptId))
    await identity.attempts.accept({
      attemptId: identityAttemptId,
      inputJson: identity.actionInputJson,
      phase: "forward",
      proof: identity.proof,
      purpose: "effect",
      sagaId: identity.sagaId,
      sagaStepId: "a",
    })
    const identityProof = await reacquire(identity, identity.proof, "wrong-identity")
    await expect(
      identity.coordinator.recoverActionAfterCrash({
        ...actionInput(identity, identityAttemptId, identityProof),
        operationId: "another-operation",
        recoveryId: `${identityAttemptId}:recovery`,
      }),
    ).rejects.toThrow(/belongs to a different action/u)

    const missingSaga = await fixture("recover-missing-saga")
    await missingSaga.coordinator.initializeSaga(missingSaga.initialize)
    const missingSagaAttemptId = `${missingSaga.sagaId}:a:forward:1`
    await missingSaga.coordinator.beginAction(actionInput(missingSaga, missingSagaAttemptId))
    const missingSagaProof = await reacquire(missingSaga, missingSaga.proof, "missing-saga")
    const missingSagaCoordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(missingSaga.base, { kind: "observation_saga_missing" }),
      digest,
    )
    await expect(
      missingSagaCoordinator.recoverActionAfterCrash({
        ...actionInput(missingSaga, missingSagaAttemptId, missingSagaProof),
        recoveryId: `${missingSagaAttemptId}:recovery`,
      }),
    ).rejects.toThrow(/saga does not exist/u)

    const active = await fixture("recover-wrong-active")
    await active.coordinator.initializeSaga(active.initialize)
    const activeAttemptId = `${active.sagaId}:a:forward:1`
    await active.coordinator.beginAction(actionInput(active, activeAttemptId))
    const activeProof = await reacquire(active, active.proof, "wrong-active")
    await expect(
      active.coordinator.recoverActionAfterCrash({
        ...actionInput(active, "another-attempt", activeProof),
        recoveryId: `${activeAttemptId}:recovery`,
      }),
    ).rejects.toThrow(/contradicts the active coupled attempt/u)
    await expect(
      active.coordinator.recoverActionAfterCrash({
        ...actionInput(active, activeAttemptId, {
          ...activeProof,
          leaseKey: "another-lease",
        }),
        recoveryId: `${activeAttemptId}:wrong-fence`,
      }),
    ).rejects.toThrow(/contradictory dispatch fence/u)
  })

  it("revalidates protocol-one receipts before first recovery and settlement", async () => {
    const markProtocolOne = (run: Fixture, attemptId: string) => {
      run.base.database.exec('DROP TRIGGER "nozzle_control_saga_protocol_update";')
      run.base.database
        .prepare(
          `UPDATE "nozzle_saga_action_attempt_protocols" SET "protocol_version" = 1
           WHERE "attempt_id" = ?`,
        )
        .run(attemptId)
    }

    const recovery = await fixture("recover-protocol-one")
    await recovery.coordinator.initializeSaga(recovery.initialize)
    const recoveryAttemptId = `${recovery.sagaId}:a:forward:1`
    await recovery.coordinator.beginAction(actionInput(recovery, recoveryAttemptId))
    await recovery.attempts.accept({
      attemptId: recoveryAttemptId,
      inputJson: recovery.actionInputJson,
      phase: "forward",
      proof: recovery.proof,
      purpose: "effect",
      sagaId: recovery.sagaId,
      sagaStepId: "a",
    })
    markProtocolOne(recovery, recoveryAttemptId)
    const recoveryProof = await reacquire(recovery, recovery.proof, "protocol-one")
    await expect(
      recovery.coordinator.recoverActionAfterCrash({
        ...actionInput(recovery, recoveryAttemptId, recoveryProof),
        recoveryId: `${recoveryAttemptId}:recovery`,
      }),
    ).resolves.toMatchObject({ steps: { a: { forward: { state: "unknown" } } } })

    const action = await fixture("settle-protocol-one")
    await action.coordinator.initializeSaga(action.initialize)
    const actionAttemptId = `${action.sagaId}:a:forward:1`
    await terminalReceipt(action, "confirmed", actionAttemptId)
    markProtocolOne(action, actionAttemptId)
    await expect(
      action.coordinator.settleActionFromReceipt(actionInput(action, actionAttemptId)),
    ).resolves.toMatchObject({ status: "succeeded" })

    const observation = await fixture("observe-protocol-one")
    await observation.coordinator.initializeSaga(observation.initialize)
    const observed = await unknownEffectObservation(observation, {
      effectProof: observation.proof,
      sequence: 1,
      state: "confirmed",
    })
    markProtocolOne(observation, observed.observationAttemptId)
    await expect(
      observation.coordinator.settleObservationFromReceipt(
        actionInput(observation, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toMatchObject({ status: "succeeded" })
  })

  it("atomically proves a missing acceptance receipt was not dispatched", async () => {
    const run = await fixture("recover-not-dispatched")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, attemptId))
    const recoveryProof = await reacquire(run, run.proof, "not-dispatched-recovery")
    const recovery = {
      ...actionInput(run, attemptId, recoveryProof),
      recoveryId: `${attemptId}:recovery`,
    }

    const recovered = await run.coordinator.recoverActionAfterCrash(recovery)
    const sagaAction = recovered.steps.a?.forward
    const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
      "saga:forward:a"
    ]
    expect(sagaAction).toMatchObject({ lastAttemptId: attemptId, state: "retryable_failed" })
    expect(operationAction).toMatchObject({ lastAttemptId: attemptId, state: "retryable_failed" })
    expect(operationAction?.reconciliationEvidenceChecksum).toBe(sagaAction?.errorChecksum)
    await expect(run.coordinator.recoverActionAfterCrash(recovery)).resolves.toEqual(recovered)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it.each([
    "accepted_unknown",
    "not_dispatched",
  ] as const)("atomically recovers a compensation %s across both ledgers", async (mode) => {
    const run = await fixture(`recover-compensation-${mode}`)
    const prepared = await recoverableAction(run, mode, "compensation", `compensation-${mode}`)

    const recovered = await run.coordinator.recoverActionAfterCrash(prepared.recovery)
    const sagaAction = recovered.steps.a?.compensation
    const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
      "saga:compensation:a"
    ]
    expect(sagaAction).toMatchObject({
      lastAttemptId: prepared.attemptId,
      state: mode === "accepted_unknown" ? "unknown" : "retryable_failed",
    })
    expect(operationAction).toMatchObject({
      lastAttemptId: prepared.attemptId,
      state: mode === "accepted_unknown" ? "unknown" : "retryable_failed",
    })
    if (mode === "accepted_unknown") {
      expect(prepared.receipt).toMatchObject({ state: "accepted" })
      expect(sagaAction?.errorChecksum).toBe(prepared.receipt?.acceptanceChecksum)
      expect(operationAction?.errorChecksum).toBe(prepared.receipt?.acceptanceChecksum)
    } else {
      expect(operationAction?.reconciliationEvidenceChecksum).toBe(sagaAction?.errorChecksum)
    }
    expect(
      run.base.database
        .prepare(
          `SELECT "effect_kind" FROM "nozzle_operation_effects"
             WHERE "resource_kind" = 'saga' ORDER BY "to_state_version" DESC LIMIT 1`,
        )
        .get(),
    ).toEqual({
      effect_kind: `action:compensation:recovery:${
        mode === "accepted_unknown" ? "unknown" : "not-dispatched"
      }`,
    })
    await expect(run.coordinator.recoverActionAfterCrash(prepared.recovery)).resolves.toEqual(
      recovered,
    )
  })

  it.each([
    ["forward", "accepted_unknown"],
    ["forward", "not_dispatched"],
    ["compensation", "accepted_unknown"],
    ["compensation", "not_dispatched"],
  ] as const)("recovers a %s %s after losing the committed D1 response", async (phase, mode) => {
    const run = await fixture(`recover-lost-${phase}-${mode}`)
    const prepared = await recoverableAction(run, mode, phase, `lost-${phase}-${mode}`)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { kind: "commit_then_throw" }),
      digest,
    )

    await expect(faulted.recoverActionAfterCrash(prepared.recovery)).resolves.toMatchObject({
      steps: {
        a: {
          [phase]: {
            state: mode === "accepted_unknown" ? "unknown" : "retryable_failed",
          },
        },
      },
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps[`saga:${phase}:a`],
    ).toMatchObject({ state: mode === "accepted_unknown" ? "unknown" : "retryable_failed" })
  })

  it.each(
    (["forward", "compensation"] as const).flatMap((phase) =>
      (["accepted_unknown", "not_dispatched"] as const).flatMap((mode) =>
        [0, 1, 2, 3, 4, 5].map((index) => [phase, mode, index] as const),
      ),
    ),
  )("rolls %s %s recovery back when batch statement %i fails", async (phase, mode, index) => {
    const run = await fixture(`recover-rollback-${phase}-${mode}-${index}`)
    const prepared = await recoverableAction(run, mode, phase, `rollback-${phase}-${mode}-${index}`)
    const effectsBefore = count(run.base.database, "nozzle_operation_effects")
    const transitionsBefore = count(run.base.database, "nozzle_operation_transitions")
    const auditBefore = count(run.base.database, "nozzle_audit_log")
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index, kind: "rollback_before" }),
      digest,
    )

    await expect(faulted.recoverActionAfterCrash(prepared.recovery)).rejects.toThrow(
      /Recovering a saga action exceeded/u,
    )
    expect((await run.sagas.get(run.sagaId))?.steps.a?.[phase].state).toBe("running")
    expect(
      (await run.operations.get(run.operationId))?.operation.steps[`saga:${phase}:a`]?.state,
    ).toBe("running")
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(effectsBefore)
    expect(count(run.base.database, "nozzle_operation_transitions")).toBe(transitionsBefore)
    expect(count(run.base.database, "nozzle_audit_log")).toBe(auditBefore)
  })

  it("terminally classifies an exhausted no-dispatch recovery in both ledgers", async () => {
    const run = await fixture("recover-not-dispatched-exhausted")
    await run.coordinator.initializeSaga(run.initialize)
    let proof = run.proof
    for (let sequence = 1; sequence <= 3; sequence += 1) {
      const attemptId = `${run.sagaId}:a:forward:${sequence}`
      await run.coordinator.beginAction(actionInput(run, attemptId, proof))
      const recoveryProof = await reacquire(run, proof, `exhausted-${sequence}`)
      const recovered = await run.coordinator.recoverActionAfterCrash({
        ...actionInput(run, attemptId, recoveryProof),
        recoveryId: `${attemptId}:recovery`,
      })
      if (sequence < 3) {
        expect(recovered.steps.a?.forward.state).toBe("retryable_failed")
      } else {
        expect(recovered).toMatchObject({
          status: "failed",
          steps: { a: { forward: { state: "failed" } } },
        })
        expect(
          (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
        ).toMatchObject({ state: "succeeded" })
      }
      proof = recoveryProof
    }
  })

  it("routes a terminal crash receipt back through terminal settlement", async () => {
    const run = await fixture("recover-terminal")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", attemptId)
    const recoveryProof = await reacquire(run, run.proof, "terminal-recovery")

    await expect(
      run.coordinator.recoverActionAfterCrash({
        ...actionInput(run, attemptId, recoveryProof),
        recoveryId: `${attemptId}:recovery`,
      }),
    ).resolves.toMatchObject({ status: "succeeded" })
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("succeeded")
  })

  it("rechecks dispatch absence inside the coupled recovery batch", async () => {
    const run = await fixture("recover-absence-race")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, attemptId))
    const accepted = await run.attempts.accept({
      attemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    const recoveryProof = await reacquire(run, run.proof, "absence-race")
    const racing = new D1SagaCoordinatorStore(
      new StaleSagaAttemptReadDatabase(run.base, null),
      digest,
    )

    const recovered = await racing.recoverActionAfterCrash({
      ...actionInput(run, attemptId, recoveryProof),
      recoveryId: `${attemptId}:recovery`,
    })
    expect(recovered.steps.a?.forward).toMatchObject({
      errorChecksum: accepted.acceptanceChecksum,
      state: "unknown",
    })
    expect(
      run.base.database
        .prepare(
          `SELECT "effect_kind" FROM "nozzle_operation_effects"
           WHERE "resource_kind" = 'saga' ORDER BY "to_state_version" DESC LIMIT 1`,
        )
        .get(),
    ).toEqual({ effect_kind: "action:forward:recovery:unknown" })
  })

  it("rechecks accepted-without-outcome evidence inside the coupled recovery batch", async () => {
    const run = await fixture("recover-outcome-race")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", attemptId)
    const staleAccepted = acceptedAttemptRow(run.base.database, attemptId)
    const recoveryProof = await reacquire(run, run.proof, "outcome-race")
    const racing = new D1SagaCoordinatorStore(
      new StaleSagaAttemptReadDatabase(run.base, staleAccepted),
      digest,
    )

    await expect(
      racing.recoverActionAfterCrash({
        ...actionInput(run, attemptId, recoveryProof),
        recoveryId: `${attemptId}:recovery`,
      }),
    ).resolves.toMatchObject({ status: "succeeded" })
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("succeeded")
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(3)
  })

  it("replays a retryable action receipt after the next attempt begins", async () => {
    const run = await fixture("replay-old-action")
    await run.coordinator.initializeSaga(run.initialize)
    const firstAttemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "not_applied", firstAttemptId)
    await run.coordinator.settleActionFromReceipt(actionInput(run, firstAttemptId))
    const nextAttemptId = `${run.sagaId}:a:forward:2`
    await run.coordinator.beginAction(actionInput(run, nextAttemptId))
    const current = await run.sagas.get(run.sagaId)

    await expect(
      run.coordinator.settleActionFromReceipt(actionInput(run, firstAttemptId)),
    ).resolves.toEqual(current)
  })

  it("replays a compensation receipt after the next compensation begins", async () => {
    const run = await fixture("replay-old-compensation")
    await run.coordinator.initializeSaga(run.initialize)
    const forwardAttemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, forwardAttemptId))
    await run.attempts.accept({
      attemptId: forwardAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    await requestTermination(run, run.proof)
    await run.attempts.complete({
      attemptId: forwardAttemptId,
      evidenceJson: '{"source":"forward"}',
      outputJson: '{"applied":true}',
      proof: run.proof,
      state: "confirmed",
    })
    await run.coordinator.settleActionFromReceipt(actionInput(run, forwardAttemptId))
    const firstCompensationId = `${run.sagaId}:a:compensation:1`
    await terminalReceipt(run, "not_applied", firstCompensationId, run.proof, "compensation")
    await run.coordinator.settleActionFromReceipt(
      actionInput(run, firstCompensationId, run.proof, "compensation"),
    )
    const nextCompensationId = `${run.sagaId}:a:compensation:2`
    await run.coordinator.beginAction(
      actionInput(run, nextCompensationId, run.proof, "compensation"),
    )
    const current = await run.sagas.get(run.sagaId)

    await expect(
      run.coordinator.settleActionFromReceipt(
        actionInput(run, firstCompensationId, run.proof, "compensation"),
      ),
    ).resolves.toEqual(current)
  })

  it("atomically applies a confirmed observation across both ledgers", async () => {
    const run = await fixture("observe-applied")
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state: "confirmed",
    })
    if (observed.observationReceipt.state !== "confirmed") {
      throw new Error("Expected a confirmed saga observation.")
    }

    const settled = await run.coordinator.settleObservationFromReceipt(
      actionInput(run, observed.observationAttemptId, observed.observationProof),
    )
    expect(settled).toMatchObject({ status: "succeeded" })
    expect(settled.steps.a?.forward).toMatchObject({
      lastAttemptId: observed.effectAttemptId,
      observationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      resultChecksum: observed.observationReceipt.outputChecksum,
      state: "succeeded",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({
      lastAttemptId: observed.effectAttemptId,
      reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      resultChecksum: observed.observationReceipt.outcomeChecksum,
      state: "succeeded",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(4)
    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toEqual(settled)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(4)
    await expect(
      run.coordinator.settleActionFromReceipt(
        actionInput(run, observed.effectAttemptId, run.proof),
      ),
    ).resolves.toEqual(settled)
    expect(await run.sagas.get(run.sagaId)).toEqual(settled)
  })

  it("settles an observation after a newer-fence takeover and replays its immutable proof", async () => {
    const run = await fixture("observe-takeover")
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state: "confirmed",
    })
    const takeoverProof = await reacquire(run, observed.observationProof, "observation-takeover")

    const settled = await run.coordinator.settleObservationFromReceipt(
      actionInput(run, observed.observationAttemptId, takeoverProof),
    )
    expect(settled).toMatchObject({ status: "succeeded" })
    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toEqual(settled)
  })

  it("keeps observed non-application retryable until its sealed budget is exhausted", async () => {
    const run = await fixture("observe-not-applied")
    await run.coordinator.initializeSaga(run.initialize)
    let effectProof = run.proof

    for (let sequence = 1; sequence <= 3; sequence += 1) {
      const observed = await unknownEffectObservation(run, {
        effectProof,
        sequence,
        state: "not_applied",
      })
      if (
        observed.observationReceipt.state === "accepted" ||
        observed.observationReceipt.state === "confirmed"
      ) {
        throw new Error("Expected a non-applied saga observation.")
      }
      const settled = await run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      )
      const action = settled.steps.a?.forward
      const operationAction = (await run.operations.get(run.operationId))?.operation.steps[
        "saga:forward:a"
      ]
      if (sequence < 3) {
        expect(action).toMatchObject({
          attempts: sequence,
          observationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
          state: "retryable_failed",
        })
        expect(operationAction).toMatchObject({
          reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
          state: "retryable_failed",
        })
      } else {
        expect(settled).toMatchObject({ status: "failed", terminationCause: "failure" })
        expect(action).toMatchObject({ attempts: 3, state: "failed" })
        expect(operationAction).toMatchObject({
          reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
          resultChecksum: observed.observationReceipt.outcomeChecksum,
          state: "succeeded",
        })
      }
      await expect(
        run.coordinator.settleObservationFromReceipt(
          actionInput(run, observed.observationAttemptId, observed.observationProof),
        ),
      ).resolves.toEqual(settled)
      effectProof = observed.observationProof
    }
  })

  it("makes an indeterminate observation require intervention in both ledgers", async () => {
    const run = await fixture("observe-indeterminate")
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state: "indeterminate",
    })
    if (observed.observationReceipt.state !== "indeterminate") {
      throw new Error("Expected an indeterminate saga observation.")
    }

    const settled = await run.coordinator.settleObservationFromReceipt(
      actionInput(run, observed.observationAttemptId, observed.observationProof),
    )
    expect(settled).toMatchObject({ status: "intervention_required" })
    expect(settled.steps.a?.forward).toMatchObject({
      observationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      state: "intervention_required",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({
      reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      state: "intervention_required",
    })
    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toEqual(settled)
  })

  it.each([
    ["confirmed", "succeeded", "succeeded", "cancelled"],
    ["indeterminate", "intervention_required", "intervention_required", "intervention_required"],
  ] as const)("atomically maps a %s compensation observation across both ledgers", async (state, sagaActionState, operationActionState, sagaStatus) => {
    const run = await fixture(`observe-compensation-${state}`)
    await run.coordinator.initializeSaga(run.initialize)
    const forwardAttemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, forwardAttemptId))
    await run.attempts.accept({
      attemptId: forwardAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    await requestTermination(run, run.proof)
    await run.attempts.complete({
      attemptId: forwardAttemptId,
      evidenceJson: '{"source":"forward"}',
      outputJson: '{"applied":true}',
      proof: run.proof,
      state: "confirmed",
    })
    await run.coordinator.settleActionFromReceipt(actionInput(run, forwardAttemptId))

    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      phase: "compensation",
      sequence: 1,
      state,
    })
    if (observed.observationReceipt.state === "accepted") {
      throw new Error("Expected a terminal compensation observation.")
    }
    const settled = await run.coordinator.settleObservationFromReceipt(
      actionInput(run, observed.observationAttemptId, observed.observationProof, "compensation"),
    )
    expect(settled).toMatchObject({ status: sagaStatus, terminationCause: "cancellation" })
    expect(settled.steps.a?.compensation).toMatchObject({
      observationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      state: sagaActionState,
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:compensation:a"],
    ).toMatchObject({
      reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
      state: operationActionState,
    })
    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof, "compensation"),
      ),
    ).resolves.toEqual(settled)
  })

  it("classifies exhausted compensation observations without claiming business success", async () => {
    const run = await fixture("observe-compensation")
    await run.coordinator.initializeSaga(run.initialize)
    const forwardAttemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, forwardAttemptId))
    await run.attempts.accept({
      attemptId: forwardAttemptId,
      inputJson: run.actionInputJson,
      phase: "forward",
      proof: run.proof,
      purpose: "effect",
      sagaId: run.sagaId,
      sagaStepId: "a",
    })
    await requestTermination(run, run.proof)
    const forwardReceipt = await run.attempts.complete({
      attemptId: forwardAttemptId,
      evidenceJson: '{"source":"forward"}',
      outputJson: '{"applied":true}',
      proof: run.proof,
      state: "confirmed",
    })
    await run.coordinator.settleActionFromReceipt(actionInput(run, forwardAttemptId))
    expect((await run.sagas.get(run.sagaId))?.status).toBe("compensating")

    let effectProof = run.proof
    for (let sequence = 1; sequence <= 3; sequence += 1) {
      const observed = await unknownEffectObservation(run, {
        effectProof,
        phase: "compensation",
        sequence,
        state: "not_applied",
      })
      if (observed.observationReceipt.state !== "not_applied") {
        throw new Error("Expected a non-applied compensation observation.")
      }
      const settled = await run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof, "compensation"),
      )
      if (sequence < 3) {
        expect(settled.steps.a?.compensation.state).toBe("retryable_failed")
      } else {
        expect(settled).toMatchObject({
          status: "intervention_required",
          terminationCause: "cancellation",
        })
        expect(settled.steps.a?.compensation).toMatchObject({
          state: "intervention_required",
        })
        expect(
          (await run.operations.get(run.operationId))?.operation.steps["saga:compensation:a"],
        ).toMatchObject({
          reconciliationEvidenceChecksum: observed.observationReceipt.outcomeChecksum,
          resultChecksum: observed.observationReceipt.outcomeChecksum,
          state: "succeeded",
        })
      }
      effectProof = observed.observationProof
    }
    expect(forwardReceipt.state).toBe("confirmed")
  })

  it("requires an exact compatible observation receipt and active observation fence", async () => {
    const missing = await fixture("observe-missing")
    await missing.coordinator.initializeSaga(missing.initialize)
    await expect(
      missing.coordinator.settleObservationFromReceipt(actionInput(missing)),
    ).rejects.toThrow(/not durably accepted/u)

    for (const state of ["failed", "unknown"] as const) {
      const incompatible = await fixture(`observe-incompatible-${state}`)
      await incompatible.coordinator.initializeSaga(incompatible.initialize)
      const attemptId = `${incompatible.sagaId}:a:forward:1`
      await terminalReceipt(incompatible, state, attemptId)
      await expect(
        incompatible.coordinator.settleObservationFromReceipt(actionInput(incompatible, attemptId)),
      ).rejects.toThrow(/incompatible terminal outcome/u)
    }

    const identity = await fixture("observe-identity")
    await identity.coordinator.initializeSaga(identity.initialize)
    const observedIdentity = await unknownEffectObservation(identity, {
      effectProof: identity.proof,
      sequence: 1,
      state: "confirmed",
    })
    await expect(
      identity.coordinator.settleObservationFromReceipt({
        ...actionInput(
          identity,
          observedIdentity.observationAttemptId,
          observedIdentity.observationProof,
        ),
        operationId: "another-operation",
      }),
    ).rejects.toThrow(/different action/u)

    const fence = await fixture("observe-fence")
    await fence.coordinator.initializeSaga(fence.initialize)
    const observedFence = await unknownEffectObservation(fence, {
      effectProof: fence.proof,
      sequence: 1,
      state: "confirmed",
    })
    await expect(
      fence.coordinator.settleObservationFromReceipt({
        ...actionInput(fence, observedFence.observationAttemptId, observedFence.observationProof),
        proof: { ...observedFence.observationProof, acquisitionId: "another-acquisition" },
      }),
    ).rejects.toThrow(/cannot be consumed under this lease fence/u)
  })

  it("fails closed when observation causality or saga visibility is missing", async () => {
    for (const [suffix, kind, message] of [
      ["cause", "observation_cause_missing", /no exact unknown causal effect/u],
      ["saga", "observation_saga_missing", /saga does not exist/u],
    ] as const) {
      const run = await fixture(`observe-${suffix}-missing`)
      await run.coordinator.initializeSaga(run.initialize)
      const observed = await unknownEffectObservation(run, {
        effectProof: run.proof,
        sequence: 1,
        state: "confirmed",
      })
      const faulted = new D1SagaCoordinatorStore(new QueryFaultDatabase(run.base, { kind }), digest)
      await expect(
        faulted.settleObservationFromReceipt(
          actionInput(run, observed.observationAttemptId, observed.observationProof),
        ),
      ).rejects.toThrow(message)
    }
  })

  it("replays an old observation from immutable history after the next attempt begins", async () => {
    const run = await fixture("observe-old-attempt")
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state: "not_applied",
    })
    const observedSettlement = await run.coordinator.settleObservationFromReceipt(
      actionInput(run, observed.observationAttemptId, observed.observationProof),
    )
    const nextAttemptId = `${run.sagaId}:a:forward:2`
    await run.coordinator.beginAction(actionInput(run, nextAttemptId, observed.observationProof))
    const current = await run.sagas.get(run.sagaId)

    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toEqual(current)
    expect(await run.sagas.get(run.sagaId)).not.toEqual(observedSettlement)
  })

  it("rejects split generic reconciliation before either ledger can diverge", async () => {
    const run = await fixture("observe-divergent")
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state: "confirmed",
    })
    if (observed.observationReceipt.state !== "confirmed") {
      throw new Error("Expected a confirmed observation receipt.")
    }
    await expect(
      run.operations.reconcileStep({
        actorChecksum: "coordinator-test-actor",
        evidenceChecksum: observed.observationReceipt.outcomeChecksum,
        observationAttemptId: observed.observationAttemptId,
        observedPostconditionChecksum: `${run.operationId}:forward:postcondition`,
        operationId: run.operationId,
        outcome: "applied",
        proof: observed.observationProof,
        reconciliationId: observed.observationAttemptId,
        resultChecksum: observed.observationReceipt.outcomeChecksum,
        stepId: "saga:forward:a",
      }),
    ).rejects.toThrow(/must be consumed through D1SagaCoordinatorStore/u)
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"]?.state,
    ).toBe("unknown")
    await expect(
      run.coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toMatchObject({ status: "succeeded" })
  })

  it.each([
    ["confirmed", "succeeded"],
    ["not_applied", "retryable_failed"],
    ["indeterminate", "intervention_required"],
  ] as const)("recovers a %s observation after losing the committed D1 response", async (state, actionState) => {
    const run = await fixture(`observe-lost-response-${state}`)
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state,
    })
    const coordinator = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { kind: "commit_then_throw" }),
      digest,
    )

    await expect(
      coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).resolves.toMatchObject({ steps: { a: { forward: { state: actionState } } } })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(4)
  })

  it.each(
    (["confirmed", "not_applied", "indeterminate"] as const).flatMap((state) =>
      [0, 1, 2, 3, 4, 5].map((index) => [state, index] as const),
    ),
  )("rolls a %s observation back when batch statement %i fails", async (state, index) => {
    const run = await fixture(`observe-rollback-${state}-${index}`)
    await run.coordinator.initializeSaga(run.initialize)
    const observed = await unknownEffectObservation(run, {
      effectProof: run.proof,
      sequence: 1,
      state,
    })
    const effectsBefore = count(run.base.database, "nozzle_operation_effects")
    const transitionsBefore = count(run.base.database, "nozzle_operation_transitions")
    const auditBefore = count(run.base.database, "nozzle_audit_log")
    const coordinator = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index, kind: "rollback_before" }),
      digest,
    )

    await expect(
      coordinator.settleObservationFromReceipt(
        actionInput(run, observed.observationAttemptId, observed.observationProof),
      ),
    ).rejects.toThrow(/Settling a saga observation exceeded/u)
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("unknown")
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"]?.state,
    ).toBe("unknown")
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(effectsBefore)
    expect(count(run.base.database, "nozzle_operation_transitions")).toBe(transitionsBefore)
    expect(count(run.base.database, "nozzle_audit_log")).toBe(auditBefore)
  })

  it("atomically records a cancellation and replays its exact coupled receipt", async () => {
    const run = await fixture("termination-cancellation")
    await run.coordinator.initializeSaga(run.initialize)
    const input = terminationInput(run)

    const cancelled = await run.coordinator.requestTermination(input)

    expect(cancelled).toMatchObject({
      sagaId: run.sagaId,
      stateVersion: 1,
      status: "cancelled",
      terminationCause: "cancellation",
    })
    expect(cancelled.terminationRequestedAtMs).toEqual(expect.any(Number))
    expect(
      (await run.operations.get(run.operationId))?.operation.steps[
        SAGA_TERMINATION_OPERATION_STEP_ID
      ],
    ).toMatchObject({ startedAttempts: 1, state: "succeeded" })
    expect(
      run.base.database
        .prepare(
          `SELECT "effect_kind" FROM "nozzle_operation_effects"
           WHERE "step_id" = ?`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID),
    ).toEqual({ effect_kind: "termination:cancellation" })
    const committed = coupledProjectionRows(run.base.database)
    await expect(run.coordinator.requestTermination(input)).resolves.toEqual(cancelled)
    expect(coupledProjectionRows(run.base.database)).toEqual(committed)
  })

  it("replays termination after action and compensation progress, restart, and takeover", async () => {
    const run = await fixture("termination-progress-replay")
    await run.coordinator.initializeSaga(run.initialize)
    const forwardAttemptId = `${run.sagaId}:a:forward:1`
    await terminalReceipt(run, "confirmed", forwardAttemptId)
    const input = terminationInput(run)
    await run.coordinator.requestTermination(input)
    await run.coordinator.settleActionFromReceipt(actionInput(run, forwardAttemptId))
    const afterForward = await run.sagas.get(run.sagaId)
    const restarted = new D1SagaCoordinatorStore(run.base, digest)

    await expect(restarted.requestTermination(input)).resolves.toEqual(afterForward)

    const compensationAttemptId = `${run.sagaId}:a:compensation:1`
    await terminalReceipt(run, "confirmed", compensationAttemptId, run.proof, "compensation")
    await run.coordinator.settleActionFromReceipt(
      actionInput(run, compensationAttemptId, run.proof, "compensation"),
    )
    const compensated = await run.sagas.get(run.sagaId)
    expect(compensated).toMatchObject({ status: "cancelled", terminationCause: "cancellation" })
    const takeoverProof = await reacquire(run, run.proof, "termination-replay")
    const beforeReplay = coupledProjectionRows(run.base.database)

    await expect(
      new D1SagaCoordinatorStore(run.base, digest).requestTermination({
        ...input,
        proof: takeoverProof,
      }),
    ).resolves.toEqual(compensated)
    expect(coupledProjectionRows(run.base.database)).toEqual(beforeReplay)
  })

  it("uses authoritative lease time to reject a premature timeout and accept the deadline", async () => {
    const run = await fixture("termination-timeout")
    const deadlineAtMs = leaseExpiration(run) - 20_000
    await run.coordinator.initializeSaga({ ...run.initialize, deadlineAtMs })
    const input = terminationInput(run, run.proof, "timeout")
    const premature = new D1SagaCoordinatorStore(
      new ServerTimeDatabase(run.base, deadlineAtMs - 1),
      digest,
    )

    await expect(premature.requestTermination(input)).rejects.toThrow(
      /authoritative saga deadline has not expired/u,
    )
    expect(await run.sagas.get(run.sagaId)).toMatchObject({
      stateVersion: 0,
      terminationCause: null,
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps[
        SAGA_TERMINATION_OPERATION_STEP_ID
      ]?.state,
    ).toBe("pending")

    const atDeadline = new D1SagaCoordinatorStore(
      new ServerTimeDatabase(run.base, deadlineAtMs),
      digest,
    )
    await expect(atDeadline.requestTermination(input)).resolves.toMatchObject({
      status: "timed_out",
      terminationCause: "timeout",
      terminationRequestedAtMs: deadlineAtMs,
    })
  })

  it("preserves the first explicit cause across cancellation and timeout request identities", async () => {
    const run = await fixture("termination-first-cause")
    await run.coordinator.initializeSaga(run.initialize)
    const cancelled = await run.coordinator.requestTermination(terminationInput(run))
    const rows = coupledProjectionRows(run.base.database)

    await expect(
      run.coordinator.requestTermination(terminationInput(run, run.proof, "timeout")),
    ).resolves.toEqual(cancelled)
    expect(cancelled.terminationCause).toBe("cancellation")
    expect(coupledProjectionRows(run.base.database)).toEqual(rows)
  })

  it.each([
    ["cancellation", "timeout"],
    ["timeout", "cancellation"],
  ] as const)("converges concurrent %s and %s requests on the first committed cause", async (winnerCause, contenderCause) => {
    const run = await fixture(`termination-concurrent-${winnerCause}`)
    const deadlineAtMs = leaseExpiration(run) - 20_000
    await run.coordinator.initializeSaga({ ...run.initialize, deadlineAtMs })
    const serverTimeMs = deadlineAtMs + 1
    const winnerCoordinator = new D1SagaCoordinatorStore(
      new ServerTimeDatabase(run.base, serverTimeMs),
      digest,
    )
    const winnerInput = terminationInput(run, run.proof, winnerCause)
    let winner: unknown
    const interleaved = new AdvanceBeforeTerminationReceiptReadDatabase(run.base, async () => {
      winner = await winnerCoordinator.requestTermination(winnerInput)
    })
    const contender = new D1SagaCoordinatorStore(
      new ServerTimeDatabase(interleaved, serverTimeMs + 1),
      digest,
    )

    const converged = await contender.requestTermination(
      terminationInput(run, run.proof, contenderCause),
    )

    expect(converged).toEqual(winner)
    expect(converged.terminationCause).toBe(winnerCause)
    expect(
      run.base.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
             WHERE "step_id" = ?`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID),
    ).toEqual({ count: 1 })
    expect(
      run.base.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_effects"
             WHERE "step_id" = ?`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID),
    ).toEqual({ count: 1 })
  })

  it("converges the same concurrent request despite differing authoritative read times", async () => {
    const run = await fixture("termination-concurrent-same-request")
    await run.coordinator.initializeSaga(run.initialize)
    const winnerTimeMs = leaseExpiration(run) - 20_000
    const contenderTimeMs = winnerTimeMs + 1_000
    const input = terminationInput(run)
    let winner: unknown
    const interleaved = new AdvanceBeforeTerminationReceiptReadDatabase(run.base, async () => {
      winner = await new D1SagaCoordinatorStore(
        new ServerTimeDatabase(run.base, winnerTimeMs),
        digest,
      ).requestTermination(input)
    })
    const contender = new D1SagaCoordinatorStore(
      new ServerTimeDatabase(interleaved, contenderTimeMs),
      digest,
    )

    const converged = await contender.requestTermination(input)

    expect(converged).toEqual(winner)
    expect(converged.terminationRequestedAtMs).toBe(winnerTimeMs)
    expect(
      run.base.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_transitions"
           WHERE "step_id" = ?`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID),
    ).toEqual({ count: 1 })
  })

  it("recovers an exact termination after the committed D1 response is lost", async () => {
    const run = await fixture("termination-lost-response")
    await run.coordinator.initializeSaga(run.initialize)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { kind: "commit_then_throw" }),
      digest,
    )

    await expect(faulted.requestTermination(terminationInput(run))).resolves.toMatchObject({
      status: "cancelled",
      terminationCause: "cancellation",
    })
    expect(
      run.base.database
        .prepare(
          `SELECT count(*) AS "count" FROM "nozzle_operation_effects"
           WHERE "step_id" = ?`,
        )
        .get(SAGA_TERMINATION_OPERATION_STEP_ID),
    ).toEqual({ count: 1 })
  })

  it("recovers the committed winner after its first verification read fails", async () => {
    const run = await fixture("termination-verification-read-loss")
    await run.coordinator.initializeSaga(run.initialize)
    const coordinator = new D1SagaCoordinatorStore(
      new FailFirstTerminationVerificationReadDatabase(run.base),
      digest,
    )

    await expect(coordinator.requestTermination(terminationInput(run))).resolves.toMatchObject({
      status: "cancelled",
      terminationCause: "cancellation",
    })
  })

  it("rethrows a pre-commit coordinator failure when no termination receipt won", async () => {
    const run = await fixture("termination-no-winner")
    await run.coordinator.initializeSaga(run.initialize)
    const coordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, {
        kind: "audit_snapshot",
        row: { event_json: null, now_ms: -1 },
      }),
      digest,
    )

    await expect(coordinator.requestTermination(terminationInput(run))).rejects.toThrow(
      /malformed authoritative saga coordinator time/u,
    )
    expect(await run.sagas.get(run.sagaId)).toMatchObject({ terminationCause: null })
  })

  it("validates the typed termination request before deriving durable identity", async () => {
    const run = await fixture("termination-validation")
    await run.coordinator.initializeSaga(run.initialize)

    await expect(
      run.coordinator.requestTermination({
        ...terminationInput(run),
        cause: "future" as never,
      }),
    ).rejects.toThrow(/cause is unsupported/u)
    for (const [field, value] of [
      ["actorChecksum", "\udc00"],
      ["operationId", "\ud800x"],
      ["requestChecksum", "\ud801"],
      ["requestId", "\ud800"],
      ["sagaId", "saga-\udc01"],
    ] as const) {
      await expect(
        run.coordinator.requestTermination({
          ...terminationInput(run),
          [field]: value,
        }),
      ).rejects.toThrow(/unpaired UTF-16 surrogates/u)
    }
    await expect(
      run.coordinator.requestTermination({
        ...terminationInput(run),
        requestId: `${run.sagaId}:termination:\ud83d\ude80:request`,
      }),
    ).resolves.toMatchObject({ terminationCause: "cancellation" })
  })

  it("binds one request identity to its first checksum and cause", async () => {
    const run = await fixture("termination-request-identity")
    await run.coordinator.initializeSaga(run.initialize)
    const input = terminationInput(run, run.proof, "cancellation", "stable-request")
    await run.coordinator.requestTermination(input)

    await expect(
      run.coordinator.requestTermination({
        ...input,
        requestChecksum: `${input.requestChecksum}:different`,
      }),
    ).rejects.toThrow(/immutable operation step|operation-effect receipt/u)
    await expect(
      run.coordinator.requestTermination({ ...input, cause: "timeout" }),
    ).rejects.toThrow(/predates its immutable deadline|immutable saga intent/u)
  })

  it("rejects oversized operation and saga identities before hashing them", async () => {
    const run = await fixture("termination-bounded-resource-identity")
    let digestCalled = false
    const unusedDigest: DigestFunction = async () => {
      digestCalled = true
      return "unused"
    }
    const coordinator = new D1SagaCoordinatorStore(run.base, unusedDigest)

    await expect(
      coordinator.requestTermination({
        ...terminationInput(run),
        operationId: "o".repeat(513),
      }),
    ).rejects.toThrow(/Operation ID exceeds/u)
    await expect(
      coordinator.requestTermination({
        ...terminationInput(run),
        sagaId: "s".repeat(513),
      }),
    ).rejects.toThrow(/Saga ID exceeds/u)
    expect(digestCalled).toBe(false)
  })

  it("rejects an operation plan without the canonical termination step", async () => {
    const run = await fixture("termination-plan-missing", undefined, undefined, false, true)
    await run.coordinator.initializeSaga(run.initialize)

    await expect(run.coordinator.requestTermination(terminationInput(run))).rejects.toThrow(
      /lacks its canonical sealed operation step/u,
    )
  })

  it("rejects a termination effect without a predecessor or request time", async () => {
    const run = await fixture("termination-effect-without-request")
    await run.coordinator.initializeSaga(run.initialize)
    const input = terminationInput(run)
    await run.coordinator.requestTermination(input)
    const restarted = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, { kind: "termination_effect_without_termination" }),
      digest,
    )

    await expect(restarted.requestTermination(input)).rejects.toThrow(
      /no valid predecessor or request time/u,
    )
  })

  it("fails closed when a termination transition disappears during exact verification", async () => {
    const run = await fixture("termination-transition-disappears")
    await run.coordinator.initializeSaga(run.initialize)
    const input = terminationInput(run)
    await run.coordinator.requestTermination(input)
    const restarted = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, { kind: "termination_transition_disappears" }),
      digest,
    )

    await expect(restarted.requestTermination(input)).rejects.toThrow(
      /lost its immutable transition/u,
    )
  })

  it("fails closed when an explicit concurrent winner loses its authoritative projection", async () => {
    for (const [kind, row, message] of [
      [
        "termination_winner_projection_missing",
        undefined,
        /winner has no current saga projection/u,
      ],
      [
        "termination_winner_projection",
        "initial",
        /has no explicit termination request to replay/u,
      ],
      ["termination_existing_step_incomplete", undefined, /lacks its canonical operation outcome/u],
    ] as const) {
      const run = await fixture(`termination-winner-${kind}`)
      await run.coordinator.initializeSaga(run.initialize)
      await run.coordinator.requestTermination(terminationInput(run))
      const fault =
        row === "initial"
          ? ({ kind, row: initialTerminationProjection(run) } as const)
          : ({ kind } as const)
      const restarted = new D1SagaCoordinatorStore(new QueryFaultDatabase(run.base, fault), digest)

      await expect(
        restarted.requestTermination(
          terminationInput(run, run.proof, "timeout", "competing-request"),
        ),
      ).rejects.toThrow(message)
    }
  })

  it("returns authoritative absent, failed, and successful saga states without mutation", async () => {
    const absent = await fixture("termination-saga-absent")
    await expect(absent.coordinator.requestTermination(terminationInput(absent))).rejects.toThrow(
      /saga does not exist/u,
    )

    const failed = await fixture("termination-saga-failed")
    await failed.coordinator.initializeSaga(failed.initialize)
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const attemptId = `${failed.sagaId}:a:forward:${attempt}`
      await terminalReceipt(failed, "not_applied", attemptId)
      await failed.coordinator.settleActionFromReceipt(actionInput(failed, attemptId))
    }
    const failedSaga = await failed.sagas.get(failed.sagaId)
    await expect(failed.coordinator.requestTermination(terminationInput(failed))).resolves.toEqual(
      failedSaga,
    )

    const succeeded = await fixture("termination-saga-succeeded")
    await succeeded.coordinator.initializeSaga(succeeded.initialize)
    const attemptId = `${succeeded.sagaId}:a:forward:1`
    await terminalReceipt(succeeded, "confirmed", attemptId)
    await succeeded.coordinator.settleActionFromReceipt(actionInput(succeeded, attemptId))
    const succeededSaga = await succeeded.sagas.get(succeeded.sagaId)
    await expect(
      succeeded.coordinator.requestTermination(terminationInput(succeeded)),
    ).resolves.toEqual(succeededSaga)
  })

  it.each([
    0, 1, 2, 3, 4, 5,
  ])("rolls every termination row back when coupled statement %i fails", async (index) => {
    const run = await fixture(`termination-rollback-${index}`)
    await run.coordinator.initializeSaga(run.initialize)
    const before = coupledProjectionRows(run.base.database)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index, kind: "rollback_before" }),
      digest,
    )

    await expect(faulted.requestTermination(terminationInput(run))).rejects.toThrow(
      /bounded coupled retry budget/u,
    )
    expect(coupledProjectionRows(run.base.database)).toEqual(before)
    expect(await run.sagas.get(run.sagaId)).toMatchObject({ terminationCause: null })
  })

  it("fails closed on missing or contradictory immutable termination history", async () => {
    for (const [kind, message] of [
      ["termination_transition_missing", /lacks its immutable receipt/u],
      ["termination_transition_mismatch", /contradictory operation transition/u],
      ["termination_effect_missing", /lacks its exact coupled effect receipt/u],
      ["termination_effect_ambiguous", /ambiguous coupled effect receipts/u],
      ["termination_effect_mismatch", /historical saga effect record is contradictory/u],
      ["termination_audit_missing", /lacks its exact audit event/u],
      ["termination_audit_mismatch", /transition receipt|audit event/u],
      ["termination_prior_missing", /lacks its prior immutable saga version/u],
      ["termination_prior_mismatch", /contradictory saga version chain/u],
      ["termination_step_missing", /step membership|operation step|sealed plan/u],
      ["termination_step_mismatch", /immutable operation step/u],
      ["termination_projection_missing", /has no current projection/u],
    ] as const) {
      const run = await fixture(`termination-history-${kind}`)
      await run.coordinator.initializeSaga(run.initialize)
      const input = terminationInput(run)
      await run.coordinator.requestTermination(input)
      const restarted = new D1SagaCoordinatorStore(
        new QueryFaultDatabase(run.base, { kind }),
        digest,
      )

      await expect(restarted.requestTermination(input)).rejects.toThrow(message)
    }
  })

  it("fails closed on a self-consistent projection that contradicts termination intent", async () => {
    const run = await fixture("termination-projection-mismatch")
    await run.coordinator.initializeSaga(run.initialize)
    const input = terminationInput(run)
    await run.coordinator.requestTermination(input)
    const row = await contradictoryTerminationProjection(run)
    const restarted = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, { kind: "termination_projection", row }),
      digest,
    )

    await expect(restarted.requestTermination(input)).rejects.toThrow(/immutable saga intent/u)
  })

  it("atomically initializes both ledgers and begins an action with synchronized attempts", async () => {
    const run = await fixture("atomic")
    const initialized = await run.coordinator.initializeSaga(run.initialize)
    expect(initialized).toMatchObject({ sagaId: run.sagaId, stateVersion: 0, status: "planned" })
    expect((await run.operations.get(run.operationId))?.operation.steps["saga:init"]).toMatchObject(
      {
        state: "succeeded",
      },
    )
    expect(await run.sagas.get(run.sagaId)).toEqual(initialized)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(1)
    await expect(run.coordinator.initializeSaga(run.initialize)).resolves.toEqual(initialized)
    await expect(
      run.coordinator.initializeSaga({ ...run.initialize, idempotencyKey: "contradictory" }),
    ).rejects.toThrow(/replay contradicts/u)

    const attemptId = `${run.sagaId}:a:forward:1`
    const begun = await run.coordinator.beginAction(actionInput(run, attemptId))
    expect(begun).toMatchObject({ disposition: "execute" })
    expect(begun.saga.steps.a?.forward).toMatchObject({
      activeAttemptId: attemptId,
      attempts: 1,
      state: "running",
    })
    expect(
      (await run.operations.get(run.operationId))?.operation.steps["saga:forward:a"],
    ).toMatchObject({ activeAttemptId: attemptId, startedAttempts: 1, state: "running" })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)

    await expect(run.coordinator.beginAction(actionInput(run, attemptId))).resolves.toMatchObject({
      disposition: "in_progress",
    })
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(2)
  })

  it("recovers an exact commit when the D1 response is lost", async () => {
    const run = await fixture("lost-response", { kind: "commit_then_throw" })
    await expect(run.coordinator.initializeSaga(run.initialize)).resolves.toMatchObject({
      sagaId: run.sagaId,
      stateVersion: 0,
    })
    expect(count(run.base.database, "nozzle_sagas")).toBe(1)
    expect(count(run.base.database, "nozzle_operation_effects")).toBe(1)
  })

  it("replays exact initialization history after progress, restart, and lease takeover", async () => {
    const run = await fixture("advanced-initialization-replay")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    await run.coordinator.beginAction(actionInput(run, attemptId))
    const advanced = await run.sagas.get(run.sagaId)
    expect(advanced).toMatchObject({ stateVersion: 1, status: "running" })
    const takeoverProof = await reacquire(run, run.proof, "initialization-replay")
    const beforeReplay = coupledProjectionRows(run.base.database)
    const restarted = new D1SagaCoordinatorStore(run.base, digest)

    await expect(
      restarted.initializeSaga({ ...run.initialize, proof: takeoverProof }),
    ).resolves.toEqual(advanced)
    expect(coupledProjectionRows(run.base.database)).toEqual(beforeReplay)
  })

  it("converges on one concurrent initialization winner after stale outer reads", async () => {
    const run = await fixture("concurrent-initialization-winner")
    let winner: unknown
    const outer = new D1SagaCoordinatorStore(
      new AdvanceBeforeInitializationReceiptReadDatabase(run.base, async () => {
        winner = await run.coordinator.initializeSaga(run.initialize)
      }),
      digest,
    )

    const replayed = await outer.initializeSaga(run.initialize)
    expect(replayed).toEqual(winner)
    const transitionId = operationTransitionIdentity("succeeded", [
      run.operationId,
      SAGA_INIT_OPERATION_STEP_ID,
      run.initialize.attemptId,
    ])
    expect(
      run.base.database
        .prepare(
          `SELECT
             (SELECT count(*) FROM "nozzle_operation_transitions"
              WHERE "transition_id" = ?1) AS "transitions",
             (SELECT count(*) FROM "nozzle_operation_effects"
              WHERE "transition_id" = ?1) AS "effects",
             (SELECT count(*) FROM "nozzle_audit_log"
              WHERE "event_hash" = (
                SELECT "audit_event_hash" FROM "nozzle_operation_transitions"
                WHERE "transition_id" = ?1
              )) AS "audits"`,
        )
        .get({ "?1": transitionId }),
    ).toEqual({ audits: 1, effects: 1, transitions: 1 })
  })

  it.each([
    0, 1, 2, 3, 4, 5,
  ])("rolls every initialization row back when coupled statement %i fails", async (index) => {
    const run = await fixture(`rollback-${index}`, { index, kind: "rollback_before" })
    const before = coupledProjectionRows(run.base.database)
    const operationBefore = await run.operations.get(run.operationId)

    await expect(run.coordinator.initializeSaga(run.initialize)).rejects.toThrow(
      /bounded coupled retry budget/u,
    )
    expect(coupledProjectionRows(run.base.database)).toEqual(before)
    expect(await run.operations.get(run.operationId)).toEqual(operationBefore)
    expect(await run.sagas.get(run.sagaId)).toBeUndefined()
  })

  it.each([
    ["initialized_transition_missing", /exists without its immutable receipt/u],
    ["initialized_transition_history_mismatch", /transition receipt is contradictory/u],
    ["initialized_transition_identity_mismatch", /contradictory operation transition/u],
    ["initialized_transition_disappears", /lost its immutable transition/u],
    ["initialized_saga_missing", /has no current projection/u],
    ["initialized_step_result_mismatch", /immutable operation step/u],
    ["initialized_audit_missing", /lacks its exact audit event/u],
    ["initialized_audit_mismatch", /active step attempt|audit event is contradictory/u],
    ["initialized_effect_missing", /operation-effect receipt is missing or contradictory/u],
    ["initialized_effect_mismatch", /operation-effect receipt is missing or contradictory/u],
  ] as const)("fails closed on %s initialization history", async (kind, message) => {
    const run = await fixture(`initialization-history-${kind}`)
    await run.coordinator.initializeSaga(run.initialize)
    const restarted = new D1SagaCoordinatorStore(new QueryFaultDatabase(run.base, { kind }), digest)

    await expect(restarted.initializeSaga(run.initialize)).rejects.toThrow(message)
  })

  it("fails closed on contradictory receipts, projections, and audit visibility", async () => {
    for (const [kind, message] of [
      ["transition_mismatch", /transition receipt is contradictory/u],
      ["effect_missing", /operation-effect receipt is missing/u],
      ["effect_mismatch", /operation-effect receipt is missing/u],
      ["operation_missing", /operation projection does not match/u],
      ["saga_missing", /saga projection does not match/u],
      ["audit_missing", /lacks its exact audit event/u],
      ["audit_mismatch", /audit event is contradictory/u],
    ] as const) {
      const run = await fixture(`verify-${kind}`, undefined, { kind })
      await expect(run.coordinator.initializeSaga(run.initialize)).rejects.toThrow(message)
    }
  })

  it("rejects a projection that regresses behind its exact coupled receipt", async () => {
    const run = await fixture("verify-saga-regressed")
    await run.coordinator.initializeSaga(run.initialize)
    const attemptId = `${run.sagaId}:a:forward:1`
    const coordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, { kind: "saga_regressed" }),
      digest,
    )

    await expect(coordinator.beginAction(actionInput(run, attemptId))).rejects.toThrow(
      /does not descend from its receipt/u,
    )
  })

  it("rejects a divergent same-version projection after an exact coupled receipt", async () => {
    const run = await fixture("verify-saga-divergent")
    await run.coordinator.initializeSaga(run.initialize)
    const row = run.base.database
      .prepare(
        `SELECT "saga".*,
                "effect"."effect_id" AS "effect_id",
                "effect"."resource_kind" AS "effect_resource_kind",
                "effect"."resource_id" AS "effect_resource_id",
                "effect"."operation_id" AS "effect_operation_id",
                "effect"."to_state_version" AS "effect_to_state_version",
                "effect"."evidence_checksum" AS "effect_evidence_checksum",
                "effect"."record_checksum" AS "effect_record_checksum",
                "effect"."record_json" AS "effect_record_json"
         FROM "nozzle_sagas" AS "saga"
         JOIN "nozzle_operation_effects" AS "effect"
           ON "effect"."effect_id" = "saga"."last_effect_id"
         WHERE "saga"."saga_id" = ?`,
      )
      .get(run.sagaId) as Record<string, unknown>
    const record = JSON.parse(row.record_json as string) as Record<string, unknown>
    record.stateVersion = 1
    record.status = "running"
    const recordJson = JSON.stringify(record)
    const recordChecksum = await sagaRecordChecksum(recordJson)
    const projection = {
      ...row,
      effect_record_checksum: recordChecksum,
      effect_record_json: recordJson,
      effect_to_state_version: 1,
      record_checksum: recordChecksum,
      record_json: recordJson,
      state_version: 1,
      status: "running",
    }
    const coordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(run.base, { kind: "saga_projection", row: projection }),
      digest,
    )

    await expect(
      coordinator.beginAction(actionInput(run, `${run.sagaId}:a:forward:1`)),
    ).rejects.toThrow(/does not descend from its receipt/u)
  })

  it("validates the authoritative audit snapshot before building a coupled batch", async () => {
    const malformedClock = await fixture("audit-clock", undefined, {
      kind: "audit_snapshot",
      row: { event_json: null, now_ms: -1 },
    })
    await expect(
      malformedClock.coordinator.initializeSaga(malformedClock.initialize),
    ).rejects.toThrow(/malformed authoritative/u)

    const malformedJson = await fixture("audit-json", undefined, {
      kind: "audit_snapshot",
      row: { event_json: "{", now_ms: 100 },
    })
    await expect(
      malformedJson.coordinator.initializeSaga(malformedJson.initialize),
    ).rejects.toThrow(/invalid JSON/u)

    const otherEnvironment = await appendAuditEvent(
      undefined,
      {
        actorChecksum: "other-actor",
        environmentId: "another-environment",
        eventType: "test.event",
        fencingToken: null,
        idempotencyKey: "other-event",
        operationId: "other-operation",
        payloadChecksum: "other-payload",
        serverTimeMs: 1,
        stepId: null,
      },
      digest,
    )
    const wrongEnvironment = await fixture("audit-environment", undefined, {
      kind: "audit_snapshot",
      row: { event_json: JSON.stringify(otherEnvironment), now_ms: 100 },
    })
    await expect(
      wrongEnvironment.coordinator.initializeSaga(wrongEnvironment.initialize),
    ).rejects.toThrow(/another environment/u)

    const absentHead = await fixture("audit-absent-head", undefined, {
      kind: "audit_snapshot",
      row: { event_json: null, now_ms: 100 },
    })
    await expect(absentHead.coordinator.initializeSaga(absentHead.initialize)).rejects.toThrow(
      /bounded coupled retry budget/u,
    )
  })

  it("rejects missing, fenced, and divergent coupled state before dispatch", async () => {
    const missingOperation = await fixture("missing-operation")
    await expect(
      missingOperation.coordinator.initializeSaga({
        ...missingOperation.initialize,
        operationId: "missing",
      }),
    ).rejects.toThrow(/operation does not exist/u)
    await expect(
      missingOperation.coordinator.beginAction(actionInput(missingOperation)),
    ).rejects.toThrow(/saga does not exist/u)

    const missingAction = await fixture("missing-action")
    await missingAction.coordinator.initializeSaga(missingAction.initialize)
    await expect(
      missingAction.coordinator.beginAction({ ...actionInput(missingAction), stepId: "missing" }),
    ).rejects.toThrow(/action does not exist/u)

    const missingPlan = await fixture("missing-plan", undefined, undefined, true)
    await missingPlan.coordinator.initializeSaga(missingPlan.initialize)
    await expect(missingPlan.coordinator.beginAction(actionInput(missingPlan))).rejects.toThrow(
      /lacks an operation step/u,
    )

    const wrongOperation = await fixture("wrong-saga-operation")
    await wrongOperation.coordinator.initializeSaga(wrongOperation.initialize)
    const wrongOperationCoordinator = new D1SagaCoordinatorStore(
      new QueryFaultDatabase(wrongOperation.base, { kind: "saga_operation_mismatch" }),
      digest,
    )
    await expect(
      wrongOperationCoordinator.beginAction(actionInput(wrongOperation)),
    ).rejects.toThrow(/different operation/u)

    const divergent = await fixture("divergent")
    await divergent.coordinator.initializeSaga(divergent.initialize)
    const attemptId = `${divergent.sagaId}:a:forward:1`
    await divergent.operations.beginStep({
      actorChecksum: "coordinator-test-actor",
      attemptId,
      idempotencyKey: sagaActionIdempotencyKey(divergent.sagaId, "a", "forward"),
      observedPreconditionChecksum: `${divergent.operationId}:forward:precondition`,
      operationId: divergent.operationId,
      proof: divergent.proof,
      stepId: sagaActionOperationStepId("a", "forward"),
    })
    await expect(
      divergent.coordinator.beginAction(actionInput(divergent, attemptId)),
    ).rejects.toThrow(/begin decisions diverged/u)

    const fenced = await fixture("fenced-initialization")
    await fenced.leases.release({ proof: fenced.proof })
    const reacquired = await fenced.leases.acquire({
      acquisitionId: `${fenced.sagaId}:new-acquisition`,
      holderId: `${fenced.sagaId}:new-controller`,
      leaseKey: fenced.proof.leaseKey,
      ttlMs: 60_000,
    })
    if (!reacquired.acquired) throw new Error("Expected coordinator lease reacquisition.")
    await expect(
      fenced.coordinator.initializeSaga({
        ...fenced.initialize,
        proof: leaseProof(reacquired.record),
      }),
    ).rejects.toThrow(/fenced by a newer/u)
  })

  it("bounds a repeatedly rolled-back action begin", async () => {
    const run = await fixture("begin-rollback")
    await run.coordinator.initializeSaga(run.initialize)
    const faulted = new D1SagaCoordinatorStore(
      new FaultDatabase(run.base, { index: 3, kind: "rollback_before" }),
      digest,
    )
    await expect(faulted.beginAction(actionInput(run))).rejects.toThrow(
      /Beginning a saga action exceeded/u,
    )
    expect((await run.sagas.get(run.sagaId))?.steps.a?.forward.state).toBe("pending")
  })

  it("rejects invalid dependencies and malformed D1 batch metadata", async () => {
    expect(() => new D1SagaCoordinatorStore(null as never, digest)).toThrow(/transactional/u)
    const database = new DatabaseAdapter()
    databases.push(database)
    expect(() => new D1SagaCoordinatorStore(database, null as never)).toThrow(/digest/u)
    expect(() => new D1SagaCoordinatorStore({ prepare() {} } as never, digest)).toThrow(
      /transactional/u,
    )

    const validation = await fixture("validation")
    await expect(
      validation.coordinator.initializeSaga({ ...validation.initialize, actorChecksum: "" }),
    ).rejects.toThrow(/non-empty/u)
    await expect(
      validation.coordinator.initializeSaga({
        ...validation.initialize,
        attemptId: "x".repeat(513),
      }),
    ).rejects.toThrow(/identity limit/u)

    const incomplete = await fixture("incomplete", { kind: "return", results: [] })
    await expect(incomplete.coordinator.initializeSaga(incomplete.initialize)).rejects.toThrow(
      /incomplete coupled/u,
    )
    const malformed = await fixture("malformed", {
      kind: "return",
      results: Array.from({ length: 6 }, () => ({
        meta: { changes: 2 },
        success: true,
      })),
    })
    await expect(malformed.coordinator.initializeSaga(malformed.initialize)).rejects.toThrow(
      /malformed coupled/u,
    )
    for (const [suffix, result] of [
      ["false", { meta: { changes: 1 }, success: false }],
      ["fraction", { meta: { changes: 0.5 }, success: true }],
      ["negative", { meta: { changes: -1 }, success: true }],
    ] as const) {
      const faulted = await fixture(`metadata-${suffix}`, {
        kind: "return",
        results: Array.from({ length: 6 }, () => result),
      })
      await expect(faulted.coordinator.initializeSaga(faulted.initialize)).rejects.toThrow(
        /malformed coupled/u,
      )
    }
  })
})
