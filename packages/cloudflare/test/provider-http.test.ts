import { describe, expect, it } from "vitest"
import {
  type CloudflareD1ProviderClientOptions,
  createCloudflareD1ProviderClient,
  parseCloudflareRateLimit,
} from "../src/provider-http.js"

const accountId = "a".repeat(32)
const databaseId = "b".repeat(32)

function jsonResponse(
  body: unknown,
  status = 200,
  headers: Readonly<Record<string, string>> = {},
): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json", ...headers },
    status,
  })
}

function providerResponse(result: unknown, status = 200): Response {
  return jsonResponse({ errors: [], messages: [], result, success: true }, status)
}

function fetchFrom(
  handler: (url: string, init: RequestInit) => Response | Promise<Response>,
): typeof globalThis.fetch {
  return (async (input: string | URL | Request, init?: RequestInit) => {
    if (init === undefined) throw new Error("Expected request initialization.")
    return handler(String(input), init)
  }) as typeof globalThis.fetch
}

function client(
  handler: (url: string, init: RequestInit) => Response | Promise<Response>,
  overrides: Partial<CloudflareD1ProviderClientOptions> = {},
) {
  let time = 1_000
  return createCloudflareD1ProviderClient({
    accountId,
    apiToken: "test-token",
    fetch: fetchFrom(handler),
    now: () => time++,
    perPage: 10,
    ...overrides,
  })
}

function database(index: number, jurisdiction?: "eu" | "fedramp") {
  return {
    created_at: "2026-07-10T00:00:00.000Z",
    file_size: index,
    ...(jurisdiction === undefined ? {} : { jurisdiction }),
    name: `database-${index}`,
    num_tables: index,
    uuid: String(index).padStart(32, "0"),
    version: "production",
  }
}

describe("Cloudflare rate-limit headers", () => {
  it("takes the most conservative values across policies", () => {
    expect(
      parseCloudflareRateLimit({
        rateLimit: '"default";r=50;t=30, "secondary";r=7;t=60',
        rateLimitPolicy: '"default";q=100;w=60, "secondary";q=20;w=120',
        retryAfter: " 45 ",
      }),
    ).toEqual({
      quota: 20,
      remaining: 7,
      resetAfterMs: 60_000,
      retryAfterMs: 45_000,
      windowMs: 120_000,
    })
  })

  it("ignores absent, malformed, and unsafe values conservatively", () => {
    expect(parseCloudflareRateLimit({})).toEqual({})
    expect(
      parseCloudflareRateLimit({
        rateLimit: '"default";r=-1;t=nope;r=999999999999999999999999999999999999',
        rateLimitPolicy: '"default";q=;w=999999999999999999999999999999999999',
        retryAfter: "invalid",
      }),
    ).toEqual({})
    expect(
      parseCloudflareRateLimit({
        rateLimit: null,
        rateLimitPolicy: null,
        retryAfter: null,
      }),
    ).toEqual({})
    expect(parseCloudflareRateLimit({ retryAfter: "0" })).toEqual({ retryAfterMs: 0 })
  })
})

describe("Cloudflare D1 provider configuration", () => {
  it("fails closed on malformed credentials and limits", () => {
    const valid = { accountId, apiToken: "token", fetch: fetchFrom(() => providerResponse({})) }
    for (const options of [
      null,
      { ...valid, accountId: "" },
      { ...valid, accountId: "not-an-account" },
      { ...valid, apiToken: "" },
      { ...valid, perPage: 9 },
      { ...valid, perPage: 10_001 },
      { ...valid, maxInventoryPages: 0 },
      { ...valid, maxInventoryPages: 10_001 },
      { ...valid, maxResponseBytes: 1_023 },
      { ...valid, maxResponseBytes: 32 * 1024 * 1024 + 1 },
      { ...valid, fetch: 1 },
      { ...valid, now: 1 },
    ]) {
      expect(() => createCloudflareD1ProviderClient(options as never)).toThrow()
    }
  })

  it("captures credentials at construction and supports safe defaults", async () => {
    expect(
      createCloudflareD1ProviderClient({ accountId, apiToken: "uses-platform-defaults" }),
    ).toBeDefined()
    const calls: RequestInit[] = []
    const mutable = {
      accountId,
      apiToken: "original-token",
      fetch: fetchFrom((_url, init) => {
        calls.push(init)
        return providerResponse(database(1))
      }),
    }
    const provider = createCloudflareD1ProviderClient(mutable)
    mutable.apiToken = "replacement-token"
    await provider.getDatabase(databaseId)
    expect(new Headers(calls[0]?.headers).get("authorization")).toBe("Bearer original-token")
  })
})

describe("Cloudflare D1 inventory transport", () => {
  it("paginates every page, exact-filters locally later, and captures redacted evidence", async () => {
    const calls: { readonly init: RequestInit; readonly url: string }[] = []
    const responses = [
      jsonResponse(
        {
          result: Array.from({ length: 10 }, (_, index) => database(index)),
          result_info: { count: 10, page: 1, per_page: 10, total_count: 11 },
          success: true,
        },
        200,
        {
          "cf-ray": "ray-one",
          Ratelimit: '"default";r=50;t=30',
          "Ratelimit-Policy": '"default";q=100;w=60',
        },
      ),
      jsonResponse({
        result: [database(10, "eu")],
        result_info: { count: 1, page: 2, per_page: 10, total_count: 11 },
        success: true,
      }),
    ]
    const provider = client((url, init) => {
      calls.push({ init, url })
      const response = responses.shift()
      if (!response) throw new Error("Unexpected request.")
      return response
    })

    const result = await provider.listInventory()
    expect(result.kind).toBe("complete")
    if (result.kind !== "complete") throw new Error("Expected complete inventory.")
    expect(result.inventory.totalCount).toBe(11)
    expect(result.inventory.databases[10]).toMatchObject({
      jurisdiction: "eu",
      name: "database-10",
    })
    expect(calls.map((call) => new URL(call.url).searchParams.get("page"))).toEqual(["1", "2"])
    expect(calls.map((call) => new URL(call.url).searchParams.get("per_page"))).toEqual([
      "10",
      "10",
    ])
    expect(calls.every((call) => call.init.method === "GET")).toBe(true)
    expect(new Headers(calls[0]?.init.headers).get("authorization")).toBe("Bearer test-token")
    expect(result.evidence[0]).toMatchObject({
      bodyState: "complete",
      cfRay: "ray-one",
      endpoint: "d1.list",
      rateLimit: {
        quota: 100,
        remaining: 50,
        resetAfterMs: 30_000,
        windowMs: 60_000,
      },
      status: 200,
    })
    expect(result.evidence[0]?.responseChecksum).toMatch(/^[0-9a-f]{64}$/u)
    expect(JSON.stringify(result)).not.toContain("test-token")
  })

  it("classifies transport, retryable, and definite rejection without hidden retries", async () => {
    const cases = [
      {
        expected: "transport_error",
        response: () => Promise.reject(new TypeError("network failed")),
      },
      {
        expected: "retry_required",
        response: () => jsonResponse({ errors: [], success: false }, 429),
      },
      {
        expected: "provider_rejected",
        response: () => jsonResponse({ errors: [], success: false }, 403),
      },
    ] as const
    for (const item of cases) {
      let calls = 0
      const provider = client(() => {
        calls += 1
        return item.response()
      })
      const result = await provider.listInventory()
      expect(result).toMatchObject({ kind: "inconclusive", reason: item.expected })
      expect(calls).toBe(1)
    }
  })

  it("rejects malformed envelopes and inventory rows", async () => {
    for (const response of [
      new Response("not-json", { status: 200 }),
      jsonResponse({ result: [], success: false }),
      jsonResponse({ result: [{ name: "missing-uuid" }], success: true }),
    ]) {
      const provider = client(() => response)
      await expect(provider.listInventory()).resolves.toMatchObject({
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
  })

  it("detects count drift across a paginated observation", async () => {
    const responses = [
      jsonResponse({
        result: Array.from({ length: 10 }, (_, index) => database(index)),
        result_info: { count: 10, page: 1, per_page: 10, total_count: 11 },
        success: true,
      }),
      jsonResponse({
        result: [database(10)],
        result_info: { count: 1, page: 2, per_page: 10, total_count: 12 },
        success: true,
      }),
    ]
    const provider = client(() => responses.shift() as Response)
    await expect(provider.listInventory()).resolves.toMatchObject({
      kind: "inconclusive",
      reason: "inconsistent_inventory",
    })
  })

  it("stops at a configured defensive page limit", async () => {
    let calls = 0
    const provider = client(
      () => {
        calls += 1
        return jsonResponse({
          result: Array.from({ length: 10 }, (_, index) => database(index + calls * 10)),
          success: true,
        })
      },
      { maxInventoryPages: 2 },
    )
    await expect(provider.listInventory()).resolves.toMatchObject({
      kind: "inconclusive",
      reason: "page_limit",
    })
    expect(calls).toBe(2)
  })
})

describe("Cloudflare D1 exact observation", () => {
  it("returns a decoded resource only from a structured success envelope", async () => {
    const provider = client(() => providerResponse(database(1, "fedramp")))
    const result = await provider.getDatabase(databaseId, { signal: new AbortController().signal })
    expect(result).toMatchObject({
      kind: "present",
      value: {
        fileSize: 1,
        jurisdiction: "fedramp",
        name: "database-1",
        numTables: 1,
      },
    })
  })

  it("treats only an exact authenticated 404 as absence", async () => {
    const provider = client(() => jsonResponse({ errors: [], success: false }, 404))
    await expect(provider.getDatabase(databaseId)).resolves.toMatchObject({ kind: "absent" })
  })

  it("keeps transport, throttling, and rejection observations inconclusive", async () => {
    const cases = [
      { reason: "transport_error", response: () => Promise.reject(new Error("network")) },
      {
        reason: "retry_required",
        response: () => jsonResponse({ errors: [], success: false }, 429),
      },
      {
        reason: "provider_rejected",
        response: () => jsonResponse({ errors: [], success: false }, 403),
      },
    ] as const
    for (const item of cases) {
      const provider = client(() => item.response())
      await expect(provider.getDatabase(databaseId)).resolves.toMatchObject({
        kind: "inconclusive",
        reason: item.reason,
      })
    }
  })

  it("keeps missing or malformed success bodies inconclusive", async () => {
    for (const response of [
      new Response(null, { status: 200 }),
      jsonResponse({ result: {}, success: false }),
      providerResponse({ name: "missing-uuid" }),
    ]) {
      const provider = client(() => response)
      await expect(provider.getDatabase(databaseId)).resolves.toMatchObject({
        kind: "inconclusive",
        reason: "malformed_response",
      })
    }
  })
})

describe("Cloudflare D1 mutation transport", () => {
  it("creates once with the exact supported structured request", async () => {
    const calls: { readonly init: RequestInit; readonly url: string }[] = []
    const provider = client((url, init) => {
      calls.push({ init, url })
      return providerResponse(database(1, "eu"))
    })
    const result = await provider.createDatabase({ jurisdiction: "eu", name: "database-1" })
    expect(result).toMatchObject({ kind: "confirmed", value: { jurisdiction: "eu" } })
    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe(
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database`,
    )
    expect(calls[0]?.init.method).toBe("POST")
    expect(calls[0]?.init.body).toBe(JSON.stringify({ jurisdiction: "eu", name: "database-1" }))
    expect(new Headers(calls[0]?.init.headers).get("content-type")).toBe("application/json")
  })

  it("sends a location hint only as creation intent", async () => {
    let body: BodyInit | null | undefined
    const provider = client((_url, init) => {
      body = init.body
      return providerResponse(database(1))
    })
    await provider.createDatabase({ locationHint: "oc", name: "database-1" })
    expect(body).toBe(JSON.stringify({ name: "database-1", primary_location_hint: "oc" }))
  })

  it("rejects malformed or contradictory desired state before a request", async () => {
    let calls = 0
    const provider = client(() => {
      calls += 1
      return providerResponse(database(1))
    })
    for (const desired of [
      { name: "" },
      { jurisdiction: "moon", name: "database" },
      { locationHint: "moon", name: "database" },
      { jurisdiction: "eu", locationHint: "oc", name: "database" },
    ]) {
      await expect(provider.createDatabase(desired as never)).rejects.toThrow()
    }
    expect(calls).toBe(0)
  })

  it("marks lost responses, ambiguous status, and malformed success as unknown", async () => {
    const cases = [
      { reason: "transport_error", response: () => Promise.reject(new Error("lost")) },
      { reason: "ambiguous_status", response: () => jsonResponse({}, 503) },
      { reason: "malformed_response", response: () => new Response("not-json", { status: 200 }) },
      {
        reason: "malformed_response",
        response: () => jsonResponse({ result: database(1), success: false }),
      },
      { reason: "malformed_response", response: () => providerResponse({ name: "missing-uuid" }) },
    ] as const
    for (const item of cases) {
      let calls = 0
      const provider = client(() => {
        calls += 1
        return item.response()
      })
      await expect(provider.createDatabase({ name: "database" })).resolves.toMatchObject({
        kind: "unknown",
        reason: item.reason,
      })
      expect(calls).toBe(1)
    }
  })

  it("returns definite rate-limit and permanent rejections with safe error summaries", async () => {
    const longMessage = `bad\u0000message\u007f${"x".repeat(2_000)}`
    const responseBody = {
      errors: [
        { code: 1000, message: longMessage },
        { code: "bad", message: "ignored" },
        { code: 1001, message: "" },
        null,
      ],
      success: false,
    }
    const throttled = client(() => jsonResponse(responseBody, 429, { "retry-after": "30" }))
    const retry = await throttled.createDatabase({ name: "database" })
    expect(retry).toMatchObject({
      decision: { disposition: "retry", retryAfterMs: 30_000, status: 429 },
      kind: "rejected",
    })
    if (retry.kind !== "rejected") throw new Error("Expected rejection.")
    expect(retry.errors).toHaveLength(1)
    expect(retry.errors[0]?.message).not.toContain("\u0000")
    expect(retry.errors[0]?.message).not.toContain("\u007f")
    expect(retry.errors[0]?.message).toHaveLength(1_024)
    expect(Object.isFrozen(retry.errors)).toBe(true)

    const forbidden = client(() => jsonResponse(responseBody, 403))
    await expect(forbidden.createDatabase({ name: "database" })).resolves.toMatchObject({
      decision: { disposition: "permanent_failure", status: 403 },
      kind: "rejected",
    })
  })

  it("requires a safe exact identity and confirms only a structured delete success", async () => {
    const calls: RequestInit[] = []
    const provider = client((_url, init) => {
      calls.push(init)
      return providerResponse({})
    })
    await expect(provider.deleteDatabase("")).rejects.toThrow()
    await expect(provider.deleteDatabase("../unsafe")).rejects.toThrow()
    await expect(provider.deleteDatabase(databaseId)).resolves.toMatchObject({ kind: "confirmed" })
    expect(calls).toHaveLength(1)
    expect(calls[0]?.method).toBe("DELETE")
    expect(calls[0]?.body).toBeUndefined()
  })

  it("classifies delete rejection, ambiguity, and malformed success without retrying", async () => {
    const cases = [
      { kind: "unknown", reason: "transport_error", response: () => Promise.reject(new Error()) },
      { kind: "unknown", reason: "ambiguous_status", response: () => jsonResponse({}, 500) },
      { kind: "rejected", response: () => jsonResponse({}, 429) },
      { kind: "rejected", response: () => jsonResponse({}, 403) },
      { kind: "unknown", reason: "malformed_response", response: () => jsonResponse({}, 200) },
    ] as const
    for (const item of cases) {
      let calls = 0
      const provider = client(() => {
        calls += 1
        return item.response()
      })
      const result = await provider.deleteDatabase(databaseId)
      expect(result).toMatchObject({
        kind: item.kind,
        ...("reason" in item ? { reason: item.reason } : {}),
      })
      expect(calls).toBe(1)
    }
  })

  it("records aborts, bounded bodies, unreadable streams, and empty bodies without secrets", async () => {
    const aborted = client(() => Promise.reject(new DOMException("aborted", "AbortError")))
    const abortResult = await aborted.createDatabase({ name: "database" })
    expect(abortResult).toMatchObject({
      evidence: { bodyState: "not_received", transportErrorKind: "aborted" },
      kind: "unknown",
    })
    const crossRealmAbort = client(() =>
      Promise.reject(Object.assign(Object.create(null) as object, { name: "AbortError" })),
    )
    await expect(crossRealmAbort.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { transportErrorKind: "aborted" },
    })
    const otherDomException = client(() => Promise.reject(new DOMException("bad", "SyntaxError")))
    await expect(otherDomException.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { transportErrorKind: "network" },
    })

    const tooLarge = client(
      () => new Response("x".repeat(1_025), { headers: { "cf-ray": "large-ray" }, status: 200 }),
      { maxResponseBytes: 1_024 },
    )
    await expect(tooLarge.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { bodyBytes: 1_025, bodyState: "too_large", cfRay: "large-ray" },
      kind: "unknown",
      reason: "malformed_response",
    })

    const cancelFailure = client(
      () =>
        new Response(
          new ReadableStream({
            cancel() {
              throw new Error("cancel failed")
            },
            start(controller) {
              controller.enqueue(new Uint8Array(1_025))
            },
          }),
          { status: 200 },
        ),
      { maxResponseBytes: 1_024 },
    )
    await expect(cancelFailure.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { bodyState: "too_large" },
      kind: "unknown",
    })

    const invalidUtf8 = client(() => new Response(new Uint8Array([0xff]), { status: 200 }))
    await expect(invalidUtf8.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { bodyBytes: 1, bodyState: "unreadable" },
      kind: "unknown",
    })

    const unreadable = client(
      () =>
        new Response(
          new ReadableStream({
            pull(controller) {
              controller.error(new Error("stream failed"))
            },
          }),
          { headers: { "cf-ray": "" }, status: 200 },
        ),
    )
    await expect(unreadable.createDatabase({ name: "database" })).resolves.toMatchObject({
      evidence: { bodyState: "unreadable" },
      kind: "unknown",
    })

    const empty = client(() => new Response(null, { status: 200 }))
    const emptyResult = await empty.createDatabase({ name: "database" })
    expect(emptyResult).toMatchObject({
      evidence: { bodyBytes: 0, bodyState: "complete" },
      kind: "unknown",
    })
    expect(emptyResult.evidence.responseChecksum).toBe(
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )
    expect(JSON.stringify(emptyResult)).not.toContain("test-token")
  })
})
