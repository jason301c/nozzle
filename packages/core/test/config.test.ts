import { describe, expect, it } from "vitest"
import { defineNozzle } from "../src/config.js"

describe("defineNozzle", () => {
  it("keeps the beginner configuration small and explicit", () => {
    const schema = { projects: {} }
    const users = {}
    const config = defineNozzle({
      schema,
      mode: "auto",
      partitionKey: "workspaceId",
      globalTables: [users],
    })

    expect(config).toEqual({
      bucketBits: 16,
      globalTables: [users],
      partitionKey: "workspaceId",
      partitionKeyType: "string",
      placement: { mode: "auto" },
      schema,
      topology: { mode: "auto" },
    })
    expect(Object.isFrozen(config)).toBe(true)
    expect(Object.isFrozen(config.globalTables)).toBe(true)
  })

  it("separates placement mode from runtime topology", () => {
    const config = defineNozzle({
      bucketBits: 20,
      schema: {},
      partitionKey: "tenantId",
      partitionKeyType: "uuid",
      placement: { mode: "dedicated" },
      topology: { mode: "router" },
    })
    expect(config.placement.mode).toBe("dedicated")
    expect(config.topology.mode).toBe("router")
    expect(config.bucketBits).toBe(20)
  })

  it.each([
    () => defineNozzle(null as never),
    () => defineNozzle({ schema: {}, partitionKey: "" }),
    () => defineNozzle({ schema: {}, partitionKey: "__nozzle_bucket" }),
    () => defineNozzle({ schema: {}, partitionKey: "nozzle_internal" }),
    () => defineNozzle({ schema: {}, partitionKey: "tenant", bucketBits: 18 as 16 }),
    () =>
      defineNozzle({
        schema: {},
        partitionKey: "tenant",
        mode: "auto",
        placement: { mode: "hash" },
      }),
  ])("rejects an invalid configuration", (build) => {
    expect(build).toThrowError(expect.objectContaining({ code: "ConfigurationError" }))
  })

  it("rejects duplicate global table identities", () => {
    const table = {}
    expect(() =>
      defineNozzle({ schema: {}, partitionKey: "tenant", globalTables: [table, table] }),
    ).toThrow("cannot contain duplicates")
  })
})
