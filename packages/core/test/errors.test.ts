import { describe, expect, it } from "vitest"
import { isNozzleError, NozzleError, redact, serializeError } from "../src/errors.js"

describe("NozzleError", () => {
  it("provides stable identity, defaults, and serialization", () => {
    const error = new NozzleError("ProviderRateLimitedError", "Cloudflare asked us to wait.", {
      details: { retryAfterSeconds: 4 },
    })

    expect(error).toBeInstanceOf(Error)
    expect(isNozzleError(error)).toBe(true)
    expect(error.toJSON()).toEqual({
      schemaVersion: 1,
      name: "NozzleError",
      code: "ProviderRateLimitedError",
      family: "provider",
      message: "Cloudflare asked us to wait.",
      remediation: "Honor the provider retry delay and resume the idempotent operation.",
      retryable: true,
      details: { retryAfterSeconds: 4 },
    })
  })

  it("supports explicit retry and remediation policy", () => {
    const error = new NozzleError("MigrationFailedError", "Canary failed.", {
      remediation: "Repair the fictional canary.",
      retryable: true,
    })

    expect(error.retryable).toBe(true)
    expect(error.remediation).toBe("Repair the fictional canary.")
  })

  it("redacts nested sensitive values deterministically", () => {
    const details = {
      z: { authorization: "fictional-authorization-value", safe: "visible" },
      api_key: "fictional-api-key",
      bytes: Uint8Array.of(1, 2, 3),
      count: 3n,
      list: [{ sessionToken: "fictional-session" }],
      nil: null,
      unsupported: new Date(0),
    }

    expect(redact(details)).toEqual({
      api_key: "[redacted]",
      bytes: "[bytes:3]",
      count: "3",
      list: [{ sessionToken: "[redacted]" }],
      nil: null,
      unsupported: "[unsupported]",
      z: { authorization: "[redacted]", safe: "visible" },
    })
    expect(
      JSON.stringify(new NozzleError("ConfigurationError", "Invalid.", { details })),
    ).not.toContain("fictional")
  })

  it("bounds arrays and recursive detail depth", () => {
    const recursive: Record<string, unknown> = {}
    let cursor = recursive
    for (let index = 0; index < 10; index += 1) {
      const next: Record<string, unknown> = {}
      cursor.next = next
      cursor = next
    }
    expect(JSON.stringify(redact(recursive))).toContain("[truncated]")
    expect(redact(Array.from({ length: 110 }, (_, index) => index))).toHaveLength(100)
  })

  it("serializes unknown errors without exposing their message", () => {
    const serialized = serializeError(new Error("fictional-secret-value"))
    expect(serialized.code).toBe("OperationInterventionRequiredError")
    expect(JSON.stringify(serialized)).not.toContain("fictional-secret-value")
    expect(isNozzleError({ code: "ConfigurationError" })).toBe(false)
  })

  it("preserves the public serialization of known Nozzle errors", () => {
    const error = new NozzleError("ConfigurationError", "The fictional field is invalid.", {
      details: {
        field: "region",
        password: "fictional-password",
      },
    })

    const serialized = serializeError(error)

    expect(serialized).toEqual(error.toJSON())
    expect(serialized).toMatchObject({
      code: "ConfigurationError",
      details: {
        field: "region",
        password: "[redacted]",
      },
      family: "configuration",
      message: "The fictional field is invalid.",
      retryable: false,
    })
    expect(JSON.stringify(serialized)).not.toContain("fictional-password")
  })
})
