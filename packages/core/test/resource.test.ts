import { describe, expect, it } from "vitest"
import {
  bindD1Resource,
  createD1ResourceRecord,
  type D1ResourceIdentity,
  type D1ResourceRecord,
  transitionD1Resource,
} from "../src/resource.js"

const databaseId = "11111111-2222-4333-8444-555555555555"

function identity(overrides: Partial<D1ResourceIdentity> = {}): D1ResourceIdentity {
  return {
    creationOperationId: "operation-provision-a",
    databaseName: "nozzle-production-shard-a-generation-a",
    desiredJurisdiction: "eu",
    environmentId: "production",
    fleetId: "fleet-a",
    generationId: "generation-a",
    intentChecksum: "intent-a",
    resourceId: "resource-a",
    shardId: "shard-a",
    targetChecksum: "cloudflare-account-a",
    ...overrides,
  }
}

function advance(
  record: D1ResourceRecord,
  kind:
    | "activate"
    | "begin_delete"
    | "begin_provisioning"
    | "confirm_deleted"
    | "intervene"
    | "quarantine"
    | "register"
    | "retire",
): D1ResourceRecord {
  return transitionD1Resource(record, {
    action: { kind },
    evidenceChecksum: `evidence-${kind}`,
    expectedStateVersion: record.stateVersion,
  })
}

function provisioning(): D1ResourceRecord {
  return advance(createD1ResourceRecord(identity()), "begin_provisioning")
}

function bound(): D1ResourceRecord {
  const record = provisioning()
  return bindD1Resource(record, {
    attributionEvidenceChecksum: "attribution-a",
    databaseId,
    databaseName: record.databaseName,
    expectedStateVersion: record.stateVersion,
    jurisdiction: record.desiredJurisdiction,
    providerResultChecksum: "provider-result-a",
  })
}

describe("D1 resource lifecycle", () => {
  it("seals immutable logical identity before a provider UUID exists", () => {
    const record = createD1ResourceRecord(identity())
    expect(record).toEqual({
      ...identity(),
      lastEvidenceChecksum: "intent-a",
      lifecycle: "planned",
      stateVersion: 0,
    })
    expect(record.binding).toBeUndefined()
    expect(Object.isFrozen(record)).toBe(true)
  })

  it("binds exact provider identity once and replays only exact evidence", () => {
    const record = provisioning()
    const input = {
      attributionEvidenceChecksum: "attribution-a",
      databaseId,
      databaseName: record.databaseName,
      expectedStateVersion: record.stateVersion,
      jurisdiction: record.desiredJurisdiction,
      providerResultChecksum: "provider-result-a",
    } as const
    const attributed = bindD1Resource(record, input)
    expect(attributed).toMatchObject({
      binding: {
        attributionEvidenceChecksum: "attribution-a",
        databaseId,
        jurisdiction: "eu",
      },
      lifecycle: "provisioning",
      stateVersion: 2,
    })
    expect(Object.isFrozen(attributed)).toBe(true)
    expect(Object.isFrozen(attributed.binding)).toBe(true)
    expect(bindD1Resource(attributed, input)).toBe(attributed)
    expect(() =>
      bindD1Resource(attributed, { ...input, databaseId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" }),
    ).toThrow(/contradictory provider identity/u)
  })

  it("runs the guarded registration, activation, retirement, and deletion path", () => {
    const registered = advance(bound(), "register")
    const active = advance(registered, "activate")
    const quarantined = advance(active, "quarantine")
    const retired = advance(quarantined, "retire")
    const deleting = advance(retired, "begin_delete")
    const deleted = advance(deleting, "confirm_deleted")
    expect([
      registered.lifecycle,
      active.lifecycle,
      quarantined.lifecycle,
      retired.lifecycle,
      deleting.lifecycle,
      deleted.lifecycle,
    ]).toEqual(["registered", "active", "quarantined", "retired", "deleting", "deleted"])
    expect(deleted.binding?.databaseId).toBe(databaseId)
    expect(deleted.stateVersion).toBe(8)
  })

  it("can quarantine pre-active resources or stop any nonterminal lifecycle for intervention", () => {
    for (const record of [
      createD1ResourceRecord(identity()),
      provisioning(),
      advance(bound(), "register"),
    ]) {
      expect(advance(record, "quarantine").lifecycle).toBe("quarantined")
    }
    for (const record of [
      createD1ResourceRecord(identity()),
      provisioning(),
      advance(bound(), "register"),
      advance(advance(bound(), "register"), "activate"),
      advance(advance(advance(bound(), "register"), "activate"), "quarantine"),
      advance(advance(advance(advance(bound(), "register"), "activate"), "quarantine"), "retire"),
      advance(
        advance(advance(advance(advance(bound(), "register"), "activate"), "quarantine"), "retire"),
        "begin_delete",
      ),
    ]) {
      expect(advance(record, "intervene").lifecycle).toBe("intervention_required")
    }
  })

  it("rejects stale, premature, contradictory, and malformed bindings", () => {
    const record = provisioning()
    const base = {
      attributionEvidenceChecksum: "attribution-a",
      databaseId,
      databaseName: record.databaseName,
      expectedStateVersion: record.stateVersion,
      jurisdiction: record.desiredJurisdiction,
      providerResultChecksum: "provider-result-a",
    } as const
    for (const input of [
      { ...base, expectedStateVersion: 9 },
      { ...base, databaseId: "not-a-uuid" },
      { ...base, databaseName: "" },
      { ...base, jurisdiction: "moon" },
      { ...base, attributionEvidenceChecksum: "" },
      { ...base, providerResultChecksum: "" },
    ]) {
      expect(() => bindD1Resource(record, input as never)).toThrow()
    }
    expect(() =>
      bindD1Resource(createD1ResourceRecord(identity()), { ...base, expectedStateVersion: 0 }),
    ).toThrow(/only while provisioning/u)
    expect(() => bindD1Resource(record, { ...base, databaseName: "other" })).toThrow(
      /does not match/u,
    )
    expect(() => bindD1Resource(record, { ...base, jurisdiction: "fedramp" })).toThrow(
      /does not match/u,
    )
    expect(() => bindD1Resource(record, { ...base, databaseId: "" })).toThrow(/non-empty/u)
  })

  it("rejects illegal lifecycle edges, stale versions, missing evidence, and missing binding", () => {
    const planned = createD1ResourceRecord(identity())
    for (const kind of [
      "activate",
      "begin_delete",
      "confirm_deleted",
      "register",
      "retire",
    ] as const) {
      expect(() => advance(planned, kind)).toThrow(/not valid/u)
    }
    expect(() =>
      transitionD1Resource(planned, {
        action: { kind: "begin_provisioning" },
        evidenceChecksum: "evidence",
        expectedStateVersion: 2,
      }),
    ).toThrow(/stale state version/u)
    expect(() =>
      transitionD1Resource(planned, {
        action: { kind: "begin_provisioning" },
        evidenceChecksum: "",
        expectedStateVersion: 0,
      }),
    ).toThrow(/non-empty/u)
    expect(() => advance(provisioning(), "register")).toThrow(/without verified/u)
    const deleted = advance(
      advance(advance(advance(advance(bound(), "register"), "activate"), "quarantine"), "retire"),
      "begin_delete",
    )
    const terminal = advance(deleted, "confirm_deleted")
    expect(() => advance(terminal, "intervene")).toThrow(/not valid/u)
    expect(() => advance(advance(planned, "intervene"), "quarantine")).toThrow(/not valid/u)
  })

  it("fails closed on malformed persisted identity, lifecycle, binding, and versions", () => {
    for (const value of ["", null]) {
      for (const field of [
        "resourceId",
        "generationId",
        "fleetId",
        "environmentId",
        "shardId",
        "targetChecksum",
        "creationOperationId",
        "intentChecksum",
        "databaseName",
      ]) {
        expect(() => createD1ResourceRecord({ ...identity(), [field]: value } as never)).toThrow()
      }
    }
    expect(() =>
      createD1ResourceRecord(identity({ desiredJurisdiction: "moon" as never })),
    ).toThrow(/unsupported/u)

    const valid = bound()
    for (const corrupt of [
      { ...valid, stateVersion: -1 },
      { ...valid, stateVersion: 0.5 },
      { ...valid, lastEvidenceChecksum: "" },
      { ...valid, lifecycle: "future" },
      { ...valid, binding: { ...valid.binding, databaseId: "bad" } },
      { ...valid, binding: { ...valid.binding, databaseName: "other" } },
      { ...valid, binding: { ...valid.binding, jurisdiction: "fedramp" } },
      { ...valid, binding: { ...valid.binding, attributionEvidenceChecksum: "" } },
      { ...valid, binding: { ...valid.binding, providerResultChecksum: "" } },
      { ...valid, stateVersion: Number.MAX_SAFE_INTEGER },
    ]) {
      expect(() => advance(corrupt as never, "register")).toThrow()
    }
    expect(() =>
      advance(
        { ...createD1ResourceRecord(identity()), lifecycle: "active" } as D1ResourceRecord,
        "quarantine",
      ),
    ).toThrow(/requires a provider binding/u)
  })
})
