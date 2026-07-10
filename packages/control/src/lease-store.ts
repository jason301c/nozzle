import {
  assertLeaseAuthorized,
  decideLeaseAcquisition,
  decideLeaseRelease,
  decideLeaseRenewal,
  type FencedLeaseRecord,
  type LeaseAcquisitionDecision,
  type LeaseProof,
  type LeaseReleaseDecision,
  type LeaseRenewalDecision,
  type LeaseWriteCondition,
  NozzleError,
} from "@nozzle/core"
import type { ControlDatabase, ControlRunResult } from "./database.js"

const MAX_CAS_ATTEMPTS = 16
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

interface LeaseSnapshot {
  readonly record: FencedLeaseRecord | undefined
  readonly serverTimeMs: number
}

interface LeaseSnapshotRow {
  readonly acquisition_id: string | null
  readonly expires_at_ms: number | null
  readonly fencing_token: number | null
  readonly holder_id: string | null
  readonly lease_key: string | null
  readonly now_ms: number
}

function databaseError(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function changed(result: ControlRunResult): boolean {
  const changes = result.meta.changes
  if (!Number.isSafeInteger(changes) || (changes as number) < 0 || (changes as number) > 1) {
    return databaseError("Control D1 returned invalid mutation metadata.")
  }
  return changes === 1
}

function validateSnapshotRow(
  row: LeaseSnapshotRow | null,
  expectedLeaseKey: string,
): LeaseSnapshot {
  if (!row || !Number.isSafeInteger(row.now_ms) || row.now_ms < 0) {
    return databaseError("Control D1 returned invalid authoritative server time.")
  }
  if (row.lease_key === null) {
    if (
      row.holder_id !== null ||
      row.acquisition_id !== null ||
      row.fencing_token !== null ||
      row.expires_at_ms !== null
    ) {
      return databaseError("Control D1 returned a malformed absent lease row.")
    }
    return Object.freeze({ record: undefined, serverTimeMs: row.now_ms })
  }
  if (
    row.lease_key !== expectedLeaseKey ||
    !Number.isSafeInteger(row.fencing_token) ||
    (row.fencing_token as number) < 1 ||
    !Number.isSafeInteger(row.expires_at_ms) ||
    (row.expires_at_ms as number) < 0 ||
    (row.holder_id === null) !== (row.acquisition_id === null)
  ) {
    return databaseError("Control D1 returned malformed lease state.")
  }
  return Object.freeze({
    record: Object.freeze({
      acquisitionId: row.acquisition_id,
      expiresAtServerTimeMs: row.expires_at_ms as number,
      fencingToken: row.fencing_token as number,
      holderId: row.holder_id,
      leaseKey: row.lease_key,
    }),
    serverTimeMs: row.now_ms,
  })
}

export class D1LeaseStore {
  readonly #database: ControlDatabase

  constructor(database: ControlDatabase) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function"
    ) {
      throw new NozzleError("ConfigurationError", "A control D1 database binding is required.")
    }
    this.#database = database
  }

  async #snapshot(leaseKey: string): Promise<LeaseSnapshot> {
    const row = await this.#database
      .prepare(
        `SELECT
          ${SERVER_TIME_SQL} AS "now_ms",
          "lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms"
         FROM (SELECT 1) AS "clock"
         LEFT JOIN "nozzle_leases" ON "lease_key" = ?1
         LIMIT 1`,
      )
      .bind(leaseKey)
      .first<LeaseSnapshotRow>()
    return validateSnapshotRow(row, leaseKey)
  }

  async #apply(condition: LeaseWriteCondition, record: FencedLeaseRecord): Promise<boolean> {
    if (condition.kind === "insert_if_absent") {
      const result = await this.#database
        .prepare(
          `INSERT INTO "nozzle_leases"
           ("lease_key", "holder_id", "acquisition_id", "fencing_token", "expires_at_ms", "updated_at_ms")
           VALUES (?1, ?2, ?3, ?4, ?5, ${SERVER_TIME_SQL})
           ON CONFLICT ("lease_key") DO NOTHING`,
        )
        .bind(
          record.leaseKey,
          record.holderId,
          record.acquisitionId,
          record.fencingToken,
          record.expiresAtServerTimeMs,
        )
        .run()
      return changed(result)
    }
    const serverPredicate =
      condition.serverTimeRequirement === "expired_or_released"
        ? `AND ("holder_id" IS NULL OR "expires_at_ms" <= ${SERVER_TIME_SQL})`
        : condition.serverTimeRequirement === "unexpired"
          ? `AND "holder_id" IS NOT NULL AND "expires_at_ms" > ${SERVER_TIME_SQL}`
          : ""
    const result = await this.#database
      .prepare(
        `UPDATE "nozzle_leases"
         SET "holder_id" = ?1,
             "acquisition_id" = ?2,
             "fencing_token" = ?3,
             "expires_at_ms" = ?4,
             "updated_at_ms" = ${SERVER_TIME_SQL}
         WHERE "lease_key" = ?5
           AND "fencing_token" = ?6
           AND "holder_id" IS ?7
           AND "acquisition_id" IS ?8
           AND "expires_at_ms" = ?9
           ${serverPredicate}`,
      )
      .bind(
        record.holderId,
        record.acquisitionId,
        record.fencingToken,
        record.expiresAtServerTimeMs,
        condition.leaseKey,
        condition.fencingToken,
        condition.holderId,
        condition.acquisitionId,
        condition.expiresAtServerTimeMs,
      )
      .run()
    return changed(result)
  }

  async acquire(input: {
    readonly acquisitionId: string
    readonly holderId: string
    readonly leaseKey: string
    readonly ttlMs: number
  }): Promise<LeaseAcquisitionDecision> {
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt += 1) {
      const snapshot = await this.#snapshot(input.leaseKey)
      const decision = decideLeaseAcquisition(snapshot.record, {
        ...input,
        serverTimeMs: snapshot.serverTimeMs,
      })
      if (!decision.acquired || decision.condition === null) return decision
      if (await this.#apply(decision.condition, decision.record)) return decision
    }
    return databaseError("Lease acquisition exceeded the bounded compare-and-swap retry budget.")
  }

  async renew(input: {
    readonly proof: LeaseProof
    readonly ttlMs: number
  }): Promise<LeaseRenewalDecision> {
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt += 1) {
      const snapshot = await this.#snapshot(input.proof.leaseKey)
      const decision = decideLeaseRenewal(snapshot.record, {
        ...input,
        serverTimeMs: snapshot.serverTimeMs,
      })
      if (!decision.renewed || decision.condition === null) return decision
      if (await this.#apply(decision.condition, decision.record)) return decision
    }
    return databaseError("Lease renewal exceeded the bounded compare-and-swap retry budget.")
  }

  async release(input: { readonly proof: LeaseProof }): Promise<LeaseReleaseDecision> {
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt += 1) {
      const snapshot = await this.#snapshot(input.proof.leaseKey)
      const decision = decideLeaseRelease(snapshot.record, {
        ...input,
        serverTimeMs: snapshot.serverTimeMs,
      })
      if (!decision.released) return decision
      if (await this.#apply(decision.condition, decision.record)) return decision
    }
    return databaseError("Lease release exceeded the bounded compare-and-swap retry budget.")
  }

  async authorize(proof: LeaseProof): Promise<FencedLeaseRecord> {
    const snapshot = await this.#snapshot(proof.leaseKey)
    assertLeaseAuthorized(snapshot.record, proof, snapshot.serverTimeMs)
    return snapshot.record
  }

  async get(leaseKey: string): Promise<FencedLeaseRecord | undefined> {
    return (await this.#snapshot(leaseKey)).record
  }
}
