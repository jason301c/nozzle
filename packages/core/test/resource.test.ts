import { describe, expect, it } from "vitest"
import {
  createD1ResourceRecord,
  type D1ResourceIdentity,
  type D1ResourceLifecycleAction,
  type D1ResourceObservation,
  type D1ResourceRecord,
  loadD1ResourceRecord,
  observeD1Resource,
  registerD1Resource,
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

function advance(record: D1ResourceRecord, action: D1ResourceLifecycleAction): D1ResourceRecord {
  return transitionD1Resource(record, {
    action,
    evidenceChecksum: `evidence-${action.kind}`,
    expectedStateVersion: record.stateVersion,
  })
}

function registered(): D1ResourceRecord {
  const record = createD1ResourceRecord(identity())
  return registerD1Resource(record, {
    attributionEvidenceChecksum: "attribution-a",
    databaseId,
    databaseName: record.databaseName,
    expectedStateVersion: record.stateVersion,
    jurisdiction: record.desiredJurisdiction,
    providerResultChecksum: "provider-result-a",
  })
}

function presentObservation(
  record: D1ResourceRecord,
  overrides: Partial<Extract<D1ResourceObservation, { presence: "present" }>> = {},
): D1ResourceRecord {
  return observeD1Resource(record, {
    databaseId,
    databaseName: record.databaseName,
    evidenceChecksum: "observation-present",
    expectedStateVersion: record.stateVersion,
    jurisdiction: record.desiredJurisdiction,
    observationOperationId: "operation-observe-present",
    presence: "present",
    ...overrides,
  })
}

function ready(): D1ResourceRecord {
  return advance(presentObservation(registered()), { kind: "mark_ready" })
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
    expect(record.lastObservation).toBeUndefined()
    expect(Object.isFrozen(record)).toBe(true)
  })

  it("registers exact provider identity once and replays only exact evidence", () => {
    const planned = createD1ResourceRecord(identity())
    const input = {
      attributionEvidenceChecksum: "attribution-a",
      databaseId,
      databaseName: planned.databaseName,
      expectedStateVersion: planned.stateVersion,
      jurisdiction: planned.desiredJurisdiction,
      providerResultChecksum: "provider-result-a",
    } as const
    const record = registerD1Resource(planned, input)
    expect(record).toMatchObject({
      binding: {
        attributionEvidenceChecksum: "attribution-a",
        databaseId,
        jurisdiction: "eu",
      },
      lifecycle: "registered",
      stateVersion: 1,
    })
    expect(Object.isFrozen(record)).toBe(true)
    expect(Object.isFrozen(record.binding)).toBe(true)
    expect(registerD1Resource(record, input)).toBe(record)
    expect(() =>
      registerD1Resource(record, {
        ...input,
        databaseId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
      }),
    ).toThrow(/contradictory provider identity/u)
  })

  it("keeps desired, recorded, and observed provider state distinct", () => {
    const record = registered()
    const drifted = presentObservation(record, {
      databaseName: "renamed-out-of-band",
      jurisdiction: "fedramp",
    })
    expect(drifted).toMatchObject({
      binding: { databaseName: record.databaseName, jurisdiction: "eu" },
      databaseName: record.databaseName,
      desiredJurisdiction: "eu",
      lastObservation: {
        databaseName: "renamed-out-of-band",
        jurisdiction: "fedramp",
        observedAtStateVersion: 2,
        presence: "present",
      },
    })
    expect(() => advance(drifted, { kind: "mark_ready" })).toThrow(/matching provider evidence/u)
    expect(advance(drifted, { kind: "quarantine" }).lifecycle).toBe("quarantined")
  })

  it("runs the stable ready, quarantine, retirement, and deletion projection", () => {
    const active = ready()
    const quarantined = advance(active, { kind: "quarantine" })
    expect(() => advance(quarantined, { kind: "recover_ready" })).toThrow(/recovery evidence/u)
    const recoveryObservation = presentObservation(quarantined, {
      evidenceChecksum: "observation-recovery",
      observationOperationId: "operation-observe-recovery",
    })
    const recovered = advance(recoveryObservation, { kind: "recover_ready" })
    const requarantined = advance(recovered, { kind: "quarantine" })
    const retired = advance(requarantined, { kind: "retire" })
    const absent = observeD1Resource(retired, {
      databaseId,
      evidenceChecksum: "observation-absent",
      expectedStateVersion: retired.stateVersion,
      observationOperationId: "operation-observe-absent",
      presence: "absent",
    })
    const deleted = advance(absent, { kind: "confirm_deleted" })
    expect([
      active.lifecycle,
      quarantined.lifecycle,
      recovered.lifecycle,
      retired.lifecycle,
      absent.lastObservation?.presence,
      deleted.lifecycle,
    ]).toEqual(["ready", "quarantined", "ready", "retired", "absent", "deleted"])
    expect(deleted.binding?.databaseId).toBe(databaseId)
    expect(deleted.stateVersion).toBe(10)
  })

  it("recovers a quarantined binding as registered and abandons only pristine intent", () => {
    const quarantined = advance(registered(), { kind: "quarantine" })
    expect(advance(quarantined, { kind: "recover_registered" }).lifecycle).toBe("registered")
    const abandoned = advance(createD1ResourceRecord(identity()), { kind: "abandon" })
    expect(abandoned).toMatchObject({ lifecycle: "abandoned", stateVersion: 1 })
    expect(() => advance(abandoned, { kind: "quarantine" })).toThrow(/not valid/u)
  })

  it("replays an exact observation and rejects stale or malformed observations", () => {
    const record = registered()
    const input = {
      databaseId,
      databaseName: record.databaseName,
      evidenceChecksum: "observation-present",
      expectedStateVersion: record.stateVersion,
      jurisdiction: record.desiredJurisdiction,
      observationOperationId: "operation-observe-present",
      presence: "present",
    } as const
    const observed = observeD1Resource(record, input)
    expect(observeD1Resource(observed, input)).toBe(observed)
    expect(Object.isFrozen(observed.lastObservation)).toBe(true)
    expect(() =>
      observeD1Resource(observed, {
        ...input,
        evidenceChecksum: "new-observation",
        expectedStateVersion: record.stateVersion,
      }),
    ).toThrow(/stale state version/u)
    for (const invalid of [
      { ...input, databaseId: "bad" },
      { ...input, databaseId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" },
      { ...input, databaseName: "" },
      { ...input, evidenceChecksum: "" },
      { ...input, jurisdiction: "moon" },
      { ...input, observationOperationId: "" },
    ]) {
      expect(() => observeD1Resource(record, invalid as never)).toThrow()
    }
    expect(() => observeD1Resource(record, { ...input, presence: "future" } as never)).toThrow(
      /presence is unsupported/u,
    )
    expect(() =>
      observeD1Resource(record, {
        databaseId,
        evidenceChecksum: "premature-absence",
        expectedStateVersion: record.stateVersion,
        observationOperationId: "operation-premature-absence",
        presence: "absent",
      }),
    ).toThrow(/only after resource retirement/u)
  })

  it("rejects stale, premature, contradictory, and malformed registration", () => {
    const record = createD1ResourceRecord(identity())
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
      { ...base, databaseId: "AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE" },
      { ...base, databaseName: "" },
      { ...base, jurisdiction: "moon" },
      { ...base, attributionEvidenceChecksum: "" },
      { ...base, providerResultChecksum: "" },
    ]) {
      expect(() => registerD1Resource(record, input as never)).toThrow()
    }
    expect(() =>
      registerD1Resource(advance(record, { kind: "quarantine" }), {
        ...base,
        expectedStateVersion: 1,
      }),
    ).toThrow(/Only a planned/u)
    expect(() => registerD1Resource(record, { ...base, databaseName: "other" })).toThrow(
      /contradicts/u,
    )
    expect(() => registerD1Resource(record, { ...base, jurisdiction: "fedramp" })).toThrow(
      /contradicts/u,
    )
  })

  it("rejects illegal lifecycle edges and deletion without authoritative absence", () => {
    const planned = createD1ResourceRecord(identity())
    for (const kind of [
      "confirm_deleted",
      "mark_ready",
      "recover_ready",
      "recover_registered",
      "retire",
    ] as const) {
      expect(() => advance(planned, { kind })).toThrow(/not valid/u)
    }
    expect(() =>
      transitionD1Resource(planned, {
        action: { kind: "quarantine" },
        evidenceChecksum: "evidence",
        expectedStateVersion: 2,
      }),
    ).toThrow(/stale state version/u)
    expect(() =>
      transitionD1Resource(planned, {
        action: { kind: "quarantine" },
        evidenceChecksum: "",
        expectedStateVersion: 0,
      }),
    ).toThrow(/non-empty/u)
    expect(() => advance(registered(), { kind: "mark_ready" })).toThrow(/matching provider/u)
    const retired = advance(advance(registered(), { kind: "quarantine" }), { kind: "retire" })
    expect(() => advance(retired, { kind: "confirm_deleted" })).toThrow(/absence evidence/u)
    expect(() => advance(ready(), { kind: "abandon" })).toThrow(/not valid/u)

    const unboundQuarantine = advance(planned, { kind: "quarantine" })
    for (const kind of ["recover_ready", "recover_registered", "retire"] as const) {
      expect(() => advance(unboundQuarantine, { kind })).toThrow(/unbound|lacks matching/u)
    }
  })

  it("rejects observations on unmaterialized and terminal resources", () => {
    const input = {
      databaseId,
      evidenceChecksum: "absence",
      expectedStateVersion: 0,
      observationOperationId: "observe",
      presence: "absent",
    } as const
    expect(() => observeD1Resource(createD1ResourceRecord(identity()), input)).toThrow(
      /before provider registration/u,
    )
    const abandoned = advance(createD1ResourceRecord(identity()), { kind: "abandon" })
    expect(() => observeD1Resource(abandoned, { ...input, expectedStateVersion: 1 })).toThrow(
      /before provider registration/u,
    )
    const retired = advance(advance(registered(), { kind: "quarantine" }), { kind: "retire" })
    const absent = observeD1Resource(retired, {
      ...input,
      expectedStateVersion: retired.stateVersion,
    })
    const deleted = advance(absent, { kind: "confirm_deleted" })
    expect(() =>
      observeD1Resource(deleted, {
        ...input,
        evidenceChecksum: "later-absence",
        expectedStateVersion: deleted.stateVersion,
      }),
    ).toThrow(/cannot accept/u)
  })

  it("fails closed on malformed persisted identity, lifecycle, binding, observation, and versions", () => {
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

    const valid = presentObservation(registered())
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
      { ...valid, lastObservation: { ...valid.lastObservation, databaseId: "bad" } },
      { ...valid, lastObservation: { ...valid.lastObservation, databaseId: "other" } },
      { ...valid, lastObservation: { ...valid.lastObservation, evidenceChecksum: "" } },
      { ...valid, lastObservation: { ...valid.lastObservation, observationOperationId: "" } },
      { ...valid, lastObservation: { ...valid.lastObservation, observedAtStateVersion: 0 } },
      { ...valid, lastObservation: { ...valid.lastObservation, observedAtStateVersion: 99 } },
    ]) {
      expect(() => advance(corrupt as never, { kind: "mark_ready" })).toThrow()
    }
    for (const corrupt of [
      { ...createD1ResourceRecord(identity()), stateVersion: 1 },
      { ...createD1ResourceRecord(identity()), lastEvidenceChecksum: "other" },
    ]) {
      expect(() => advance(corrupt as D1ResourceRecord, { kind: "quarantine" })).toThrow(
        /original materialization intent/u,
      )
    }
    expect(() =>
      advance(
        {
          ...advance(registered(), { kind: "quarantine" }),
          stateVersion: Number.MAX_SAFE_INTEGER,
        } as D1ResourceRecord,
        { kind: "recover_registered" },
      ),
    ).toThrow(/version overflowed/u)
    expect(() =>
      advance(
        {
          ...createD1ResourceRecord(identity()),
          binding: registered().binding,
        } as D1ResourceRecord,
        { kind: "quarantine" },
      ),
    ).toThrow(/cannot retain provider state/u)
    expect(() =>
      advance({ ...registered(), lifecycle: "ready" } as D1ResourceRecord, { kind: "quarantine" }),
    ).toThrow(/lacks a matching recent/u)
    expect(() =>
      advance({ ...registered(), lifecycle: "deleted" } as D1ResourceRecord, {
        kind: "quarantine",
      }),
    ).toThrow(/lacks authoritative absence/u)
    expect(() =>
      advance(
        { ...createD1ResourceRecord(identity()), lifecycle: "registered" } as D1ResourceRecord,
        { kind: "quarantine" },
      ),
    ).toThrow(/requires a provider binding/u)
    expect(() =>
      advance(
        {
          ...valid,
          lastObservation: { ...valid.lastObservation, presence: "absent" },
        } as D1ResourceRecord,
        { kind: "mark_ready" },
      ),
    ).toThrow(/invalid resource lifecycle/u)
  })

  it("loads only exact persisted resource shapes and freezes nested provider evidence", () => {
    const planned = createD1ResourceRecord(identity())
    expect(loadD1ResourceRecord(JSON.parse(JSON.stringify(planned)))).toEqual(planned)
    const present = ready()
    const loaded = loadD1ResourceRecord(JSON.parse(JSON.stringify(present)))
    expect(loaded).toEqual(present)
    expect(Object.isFrozen(loaded)).toBe(true)
    expect(Object.isFrozen(loaded.binding)).toBe(true)
    expect(Object.isFrozen(loaded.lastObservation)).toBe(true)

    for (const malformed of [
      null,
      [],
      { ...present, future: true },
      { ...present, binding: null },
      { ...present, binding: { ...present.binding, future: true } },
      { ...present, lastObservation: null },
      { ...present, lastObservation: { ...present.lastObservation, future: true } },
      { ...present, lastObservation: { presence: "absent", future: true } },
      { ...present, lastObservation: { ...present.lastObservation, presence: "future" } },
      { ...present, resourceId: "" },
    ]) {
      expect(() => loadD1ResourceRecord(malformed)).toThrow(/persisted|Persisted/u)
    }

    const retired = advance(advance(ready(), { kind: "quarantine" }), { kind: "retire" })
    const absent = observeD1Resource(retired, {
      databaseId,
      evidenceChecksum: "loader-absence",
      expectedStateVersion: retired.stateVersion,
      observationOperationId: "loader-observation",
      presence: "absent",
    })
    expect(loadD1ResourceRecord(JSON.parse(JSON.stringify(absent)))).toEqual(absent)
  })
})
