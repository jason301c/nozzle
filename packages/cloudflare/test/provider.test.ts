import { describe, expect, it } from "vitest"
import {
  type CompleteD1Inventory,
  classifyProviderAttempt,
  computeProviderRetryDelay,
  type D1ListPage,
  decodeD1ListPage,
  mergeD1ListPages,
  type ObservedD1Database,
  planD1Reconciliation,
} from "../src/provider.js"

const request = { expectedPage: 1, perPage: 10 }

function uuid(value: string): string {
  let hash = 2166136261
  for (const character of value) {
    hash ^= character.charCodeAt(0)
    hash = Math.imul(hash, 16777619)
  }
  return `00000000-0000-4000-8000-${(hash >>> 0).toString(16).padStart(12, "0")}`
}

function envelope(result: readonly unknown[], resultInfo?: unknown): Record<string, unknown> {
  return {
    result,
    ...(resultInfo === undefined ? {} : { result_info: resultInfo }),
    success: true,
  }
}

function observed(
  id: string,
  name = `database-${id}`,
  jurisdiction?: "eu" | "fedramp",
): ObservedD1Database {
  return {
    ...(jurisdiction === undefined ? {} : { jurisdiction }),
    name,
    uuid: uuid(id),
  }
}

function inventory(databases: readonly ObservedD1Database[]): CompleteD1Inventory {
  return {
    complete: true,
    databases,
    pageCount: 1,
    totalCount: databases.length,
  }
}

describe("Cloudflare D1 inventory decoding", () => {
  it("normalizes provider fields and continues a full page without trusting optional metadata", () => {
    const result = Array.from({ length: 10 }, (_, index) => ({
      created_at: "2026-07-10T00:00:00.000Z",
      file_size: index,
      jurisdiction: index === 0 ? "eu" : undefined,
      name: `database-${index}`,
      num_tables: index,
      read_replication: index === 0 ? { mode: "auto" } : undefined,
      uuid: uuid(`uuid-${index}`),
      version: "production",
    }))
    const page = decodeD1ListPage(
      envelope(result, { count: 10, page: 1, per_page: 10, total_count: 10 }),
      request,
    )

    expect(page).toEqual({
      databases: result.map((database) => ({
        createdAt: database.created_at,
        fileSize: database.file_size,
        ...(database.jurisdiction === undefined ? {} : { jurisdiction: database.jurisdiction }),
        name: database.name,
        numTables: database.num_tables,
        ...(database.read_replication === undefined
          ? {}
          : { readReplication: database.read_replication }),
        uuid: database.uuid,
        version: database.version,
      })),
      nextPage: 2,
      page: 1,
      perPage: 10,
      reportedTotalCount: 10,
    })
    expect(Object.isFrozen(page)).toBe(true)
    expect(Object.isFrozen(page.databases)).toBe(true)
  })

  it("accepts schema-compliant omitted metadata and finishes only on a short page", () => {
    expect(decodeD1ListPage(envelope([{ name: "one", uuid: uuid("uuid-1") }]), request)).toEqual({
      databases: [{ name: "one", uuid: uuid("uuid-1") }],
      page: 1,
      perPage: 10,
    })
    expect(
      decodeD1ListPage(envelope([{ name: "one", uuid: uuid("uuid-1") }], {}), request),
    ).toEqual({
      databases: [{ name: "one", uuid: uuid("uuid-1") }],
      page: 1,
      perPage: 10,
    })
    expect(
      decodeD1ListPage(
        envelope([{ jurisdiction: null, name: "global", uuid: uuid("global") }]),
        request,
      ),
    ).toEqual({
      databases: [{ name: "global", uuid: uuid("global") }],
      page: 1,
      perPage: 10,
    })
  })

  it("rejects malformed requests and envelopes", () => {
    for (const invalidRequest of [
      { expectedPage: 0, perPage: 10 },
      { expectedPage: 1, perPage: 9 },
      { expectedPage: 1, perPage: 10_001 },
    ]) {
      expect(() => decodeD1ListPage(envelope([]), invalidRequest)).toThrow()
    }
    for (const value of [
      null,
      [],
      { result: [], success: false },
      { result: {}, success: true },
      Object.create({ result: [], success: true }),
    ]) {
      expect(() => decodeD1ListPage(value, request)).toThrow(/malformed D1 list envelope/u)
    }
  })

  it("rejects inconsistent optional pagination fields", () => {
    for (const resultInfo of [
      [],
      { page: 2 },
      { per_page: 11 },
      { count: 2 },
      { total_count: -1 },
      { total_count: 1.5 },
      { total_count: "1" },
    ]) {
      expect(() =>
        decodeD1ListPage(envelope([{ name: "one", uuid: uuid("uuid-1") }], resultInfo), request),
      ).toThrow()
    }
  })

  it("rejects malformed observed database fields and oversized or duplicate pages", () => {
    const valid = { name: "one", uuid: uuid("uuid-1") }
    for (const invalid of [
      null,
      [],
      { ...valid, name: " " },
      { ...valid, uuid: " " },
      { ...valid, uuid: "a".repeat(32) },
      { ...valid, uuid: "00000000-0000-0000-0000-00000000000z" },
      { ...valid, jurisdiction: "moon" },
      { ...valid, created_at: "" },
      { ...valid, file_size: -1 },
      { ...valid, file_size: 0.5 },
      { ...valid, num_tables: Number.POSITIVE_INFINITY },
      { ...valid, read_replication: null },
      { ...valid, read_replication: { mode: "future" } },
      { ...valid, version: 1 },
    ]) {
      expect(() => decodeD1ListPage(envelope([invalid as never]), request)).toThrow()
    }
    expect(() =>
      decodeD1ListPage(
        envelope(Array.from({ length: 11 }, (_, index) => observed(`${index}`))),
        request,
      ),
    ).toThrow(/more D1 databases/u)
    expect(() => decodeD1ListPage(envelope([valid, valid]), request)).toThrow(/repeated a D1 UUID/u)
  })
})

describe("complete D1 inventory assembly", () => {
  it("assembles contiguous pages and verifies the provider total", () => {
    const first = decodeD1ListPage(
      envelope(
        Array.from({ length: 10 }, (_, index) => observed(`${index}`)),
        { count: 10, page: 1, per_page: 10, total_count: 11 },
      ),
      request,
    )
    const second = decodeD1ListPage(
      envelope([observed("10")], { count: 1, page: 2, per_page: 10, total_count: 11 }),
      { expectedPage: 2, perPage: 10 },
    )

    const complete = mergeD1ListPages([first, second])
    expect(complete).toEqual({
      complete: true,
      databases: [...first.databases, ...second.databases],
      pageCount: 2,
      reportedTotalCount: 11,
      totalCount: 11,
    })
    expect(Object.isFrozen(complete)).toBe(true)
    expect(Object.isFrozen(complete.databases)).toBe(true)
  })

  it("assembles an inventory when Cloudflare omits result_info", () => {
    const complete = mergeD1ListPages([decodeD1ListPage(envelope([observed("one")]), request)])
    expect(complete).toEqual({
      complete: true,
      databases: [observed("one")],
      pageCount: 1,
      totalCount: 1,
    })
  })

  it("rejects incomplete, discontinuous, drifting, duplicate, and malformed page sets", () => {
    const full: D1ListPage = {
      databases: Array.from({ length: 10 }, (_, index) => observed(`${index}`)),
      nextPage: 2,
      page: 1,
      perPage: 10,
      reportedTotalCount: 11,
    }
    const final: D1ListPage = {
      databases: [observed("10")],
      page: 2,
      perPage: 10,
      reportedTotalCount: 11,
    }
    for (const pages of [
      [],
      [{ ...full, perPage: 9 }],
      [{ ...full, perPage: 10_001 }],
      [{ ...full, reportedTotalCount: -1 }],
      [{ ...full, page: 2 }, final],
      [full, { ...final, perPage: 11 }],
      [full, { ...final, reportedTotalCount: 12 }],
      [{ ...full, databases: [...full.databases, observed("overflow")] }, final],
      [{ ...full, nextPage: undefined }, final],
      [full, { ...final, nextPage: 3 }],
      [full, { ...final, databases: [observed("0")] }],
      [full, { ...final, databases: [] }],
    ]) {
      expect(() => mergeD1ListPages(pages as readonly D1ListPage[])).toThrow()
    }
  })

  it("revalidates caller-constructed observed objects", () => {
    const valid = observed("valid")
    for (const invalid of [
      null,
      [],
      { ...valid, name: "" },
      { ...valid, uuid: "" },
      { ...valid, jurisdiction: "moon" },
      { ...valid, createdAt: "" },
      { ...valid, fileSize: -1 },
      { ...valid, numTables: 0.1 },
      { ...valid, readReplication: { mode: "future" } },
      { ...valid, version: "" },
    ]) {
      const page = {
        databases: [invalid],
        page: 1,
        perPage: 10,
      } as never
      expect(() => mergeD1ListPages([page])).toThrow()
    }
  })
})

describe("provider attempt policy", () => {
  it("distinguishes confirmed, retryable, rejected, and ambiguous outcomes", () => {
    expect(classifyProviderAttempt({ mutating: false, status: 200 })).toEqual({
      disposition: "success",
    })
    expect(classifyProviderAttempt({ mutating: true, status: 299 })).toEqual({
      disposition: "success",
    })
    expect(classifyProviderAttempt({ mutating: false, status: null })).toEqual({
      disposition: "retry",
      retryAfterMs: 1_000,
      status: null,
    })
    expect(classifyProviderAttempt({ mutating: true, status: null })).toEqual({
      disposition: "unknown_outcome",
      status: null,
    })
    expect(classifyProviderAttempt({ mutating: false, status: 400 })).toEqual({
      disposition: "permanent_failure",
      status: 400,
    })
  })

  it("honors numeric Retry-After for definite rate-limit rejections", () => {
    expect(classifyProviderAttempt({ mutating: true, retryAfter: "30", status: 429 })).toEqual({
      disposition: "retry",
      retryAfterMs: 30_000,
      status: 429,
    })
    for (const retryAfter of [undefined, null, "", "1.5", "-1", "0", "99999999999999999"]) {
      const input =
        retryAfter === undefined
          ? { mutating: false, status: 429 }
          : { mutating: false, retryAfter, status: 429 }
      expect(classifyProviderAttempt(input)).toEqual({
        disposition: "retry",
        retryAfterMs: 1_000,
        status: 429,
      })
    }
  })

  it("never blindly retries an ambiguous mutation", () => {
    for (const status of [408, 500, 502, 503, 504, 520, 521, 522, 523, 524]) {
      expect(classifyProviderAttempt({ mutating: true, status })).toEqual({
        disposition: "unknown_outcome",
        status,
      })
      expect(classifyProviderAttempt({ mutating: false, retryAfter: "2", status })).toEqual({
        disposition: "retry",
        retryAfterMs: 2_000,
        status,
      })
    }
  })

  it("rejects impossible HTTP statuses", () => {
    for (const status of [99, 600, 200.5, Number.NaN]) {
      expect(() => classifyProviderAttempt({ mutating: false, status })).toThrow(
        /HTTP status is invalid/u,
      )
    }
  })
})

describe("provider backoff", () => {
  it("uses bounded exponential full-width jitter and a server floor", () => {
    expect(computeProviderRetryDelay({ attempt: 0, randomUnit: 0 })).toBe(500)
    expect(computeProviderRetryDelay({ attempt: 1, randomUnit: 0.5 })).toBe(2_000)
    expect(computeProviderRetryDelay({ attempt: 31, randomUnit: 0.999 })).toBe(30_000)
    expect(computeProviderRetryDelay({ attempt: 1, minimumDelayMs: 90_000, randomUnit: 0 })).toBe(
      90_000,
    )
    expect(
      computeProviderRetryDelay({
        attempt: 3,
        baseDelayMs: 10,
        maximumDelayMs: 20,
        randomUnit: 0.25,
      }),
    ).toBe(15)
  })

  it("rejects invalid retry configuration", () => {
    for (const invalid of [
      { attempt: -1, randomUnit: 0 },
      { attempt: 32, randomUnit: 0 },
      { attempt: 0.5, randomUnit: 0 },
      { attempt: 0, baseDelayMs: 0, randomUnit: 0 },
      { attempt: 0, baseDelayMs: 10, maximumDelayMs: 9, randomUnit: 0 },
      { attempt: 0, minimumDelayMs: -1, randomUnit: 0 },
      { attempt: 0, minimumDelayMs: 0.5, randomUnit: 0 },
      { attempt: 0, randomUnit: -0.1 },
      { attempt: 0, randomUnit: 1 },
      { attempt: 0, randomUnit: Number.NaN },
    ]) {
      expect(() => computeProviderRetryDelay(invalid)).toThrow()
    }
  })
})

describe("D1 desired-recorded-observed reconciliation", () => {
  it("creates only after a complete inventory proves the deterministic name absent", () => {
    const desired = { locationHint: "oc" as const, name: "nozzle-production-001" }
    expect(planD1Reconciliation({ desired, inventory: inventory([observed("other")]) })).toEqual({
      desired,
      kind: "create",
    })
  })

  it("requires inspection before adopting an unrecorded deterministic-name candidate", () => {
    const desired = { jurisdiction: "eu" as const, name: "nozzle-production-001" }
    const candidate = observed("candidate", desired.name, "eu")
    expect(planD1Reconciliation({ desired, inventory: inventory([candidate]) })).toEqual({
      candidate,
      desired,
      kind: "inspect_for_adoption",
    })
  })

  it("accepts only an exact recorded identity and immutable jurisdiction", () => {
    const desired = { jurisdiction: "fedramp" as const, name: "nozzle-production-001" }
    const database = observed("recorded", desired.name, "fedramp")
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([database]),
        recorded: { ...desired, uuid: database.uuid },
      }),
    ).toEqual({ kind: "none", observed: database })
  })

  it("inspects list summaries before planning a read-replication update", () => {
    const desired = {
      name: "nozzle-production-001",
      readReplication: { mode: "auto" as const },
    }
    const summary = observed("recorded", desired.name)
    const recorded = { ...desired, uuid: summary.uuid }
    expect(planD1Reconciliation({ desired, inventory: inventory([summary]), recorded })).toEqual({
      desired,
      kind: "inspect_recorded",
      observed: summary,
      recorded,
    })

    const disabled = { ...summary, readReplication: { mode: "disabled" as const } }
    expect(planD1Reconciliation({ desired, inventory: inventory([disabled]), recorded })).toEqual({
      databaseId: summary.uuid,
      kind: "update_read_replication",
      readReplication: desired.readReplication,
    })
    const automatic = { ...summary, readReplication: { mode: "auto" as const } }
    expect(planD1Reconciliation({ desired, inventory: inventory([automatic]), recorded })).toEqual({
      kind: "none",
      observed: automatic,
    })
  })

  it("quarantines duplicate names and missing or conflicting recorded identities", () => {
    const desired = { name: "nozzle-production-001" }
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([observed("one", desired.name), observed("two", desired.name)]),
      }),
    ).toEqual({ kind: "quarantine_drift", reason: "duplicate_name" })
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([]),
        recorded: { ...desired, uuid: uuid("missing") },
      }),
    ).toEqual({ kind: "quarantine_drift", reason: "recorded_resource_missing" })

    const identified = observed("recorded", "wrong-name")
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([identified, observed("other", desired.name)]),
        recorded: { ...desired, uuid: identified.uuid },
      }),
    ).toEqual({
      kind: "quarantine_drift",
      observed: identified,
      reason: "recorded_identity_mismatch",
    })
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([observed("recorded", desired.name)]),
        recorded: { name: "old-name", uuid: uuid("recorded") },
      }),
    ).toEqual({
      kind: "quarantine_drift",
      observed: observed("recorded", desired.name),
      reason: "recorded_identity_mismatch",
    })
  })

  it("quarantines immutable jurisdiction drift before adoption or continued use", () => {
    const desired = { jurisdiction: "eu" as const, name: "nozzle-production-001" }
    const globalDatabase = observed("database", desired.name)
    expect(planD1Reconciliation({ desired, inventory: inventory([globalDatabase]) })).toEqual({
      kind: "quarantine_drift",
      observed: globalDatabase,
      reason: "immutable_jurisdiction_mismatch",
    })
    expect(
      planD1Reconciliation({
        desired,
        inventory: inventory([globalDatabase]),
        recorded: { jurisdiction: "fedramp", name: desired.name, uuid: globalDatabase.uuid },
      }),
    ).toEqual({
      kind: "quarantine_drift",
      observed: globalDatabase,
      reason: "immutable_jurisdiction_mismatch",
    })
  })

  it("rejects malformed desired, recorded, and incomplete observed state", () => {
    const base = { desired: { name: "valid" }, inventory: inventory([]) }
    for (const desired of [
      { name: "" },
      { jurisdiction: "moon", name: "valid" },
      { locationHint: "moon", name: "valid" },
      { jurisdiction: "eu", locationHint: "oc", name: "valid" },
      { name: "valid", readReplication: { mode: "future" } },
    ]) {
      expect(() => planD1Reconciliation({ ...base, desired } as never)).toThrow()
    }
    for (const invalidInventory of [
      null,
      {},
      { complete: false, databases: [], pageCount: 1, totalCount: 0 },
      { complete: true, databases: {}, pageCount: 1, totalCount: 0 },
      { complete: true, databases: [], pageCount: 0, totalCount: 0 },
      { complete: true, databases: [], pageCount: 1, totalCount: -1 },
      { complete: true, databases: [], pageCount: 1, totalCount: 1 },
    ]) {
      expect(() => planD1Reconciliation({ ...base, inventory: invalidInventory } as never)).toThrow(
        /complete paginated observation/u,
      )
    }
    expect(() =>
      planD1Reconciliation({
        ...base,
        inventory: inventory([observed("duplicate"), observed("duplicate")]),
      }),
    ).toThrow(/duplicate UUID/u)
    expect(() =>
      planD1Reconciliation({
        ...base,
        recorded: { name: "valid", uuid: "" },
      }),
    ).toThrow(/Recorded D1 UUID/u)
  })
})
