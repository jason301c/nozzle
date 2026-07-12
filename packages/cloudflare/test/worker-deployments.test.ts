import { describe, expect, it } from "vitest"
import {
  type CloudflareWorkerDeploymentClientOptions,
  createCloudflareWorkerDeploymentClient,
} from "../src/worker-deployments.js"

const accountId = "a".repeat(32)
const deploymentId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const versionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const versionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const versionC = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

function jsonResponse(
  body: unknown,
  status = 200,
  headers: Readonly<Record<string, string>> = {},
): Response {
  return new Response(JSON.stringify(body), { headers, status })
}

function rawResponse(
  body: BodyInit | null,
  status = 200,
  headers: Readonly<Record<string, string>> = {},
): Response {
  return new Response(body, { headers, status })
}

function envelope(deployments: readonly unknown[]): unknown {
  return { errors: [], messages: [], result: { deployments }, success: true }
}

function deployment(
  versions: readonly unknown[] | undefined = [{ percentage: 100, version_id: versionA }],
  overrides: Readonly<Record<string, unknown>> = {},
): unknown {
  return {
    created_on: "2026-07-12T00:00:00.123Z",
    id: deploymentId,
    strategy: "percentage",
    versions,
    ...overrides,
  }
}

function versionEnvelope(overrides: Readonly<Record<string, unknown>> = {}): unknown {
  return {
    errors: [],
    messages: [],
    result: {
      id: versionA,
      resources: { script: { etag: "1".repeat(64) } },
      ...overrides,
    },
    success: true,
  }
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
  overrides: Partial<CloudflareWorkerDeploymentClientOptions> = {},
) {
  let time = 1_000
  return createCloudflareWorkerDeploymentClient({
    accountId,
    apiToken: "fictional-token",
    fetch: fetchFrom(handler),
    now: () => time++,
    ...overrides,
  })
}

describe("Cloudflare Worker deployment client configuration", () => {
  it("rejects malformed credentials, transport limits, and clocks", () => {
    const valid = {
      accountId,
      apiToken: "fictional-token",
      fetch: fetchFrom(() => jsonResponse(envelope([deployment()]))),
    }
    for (const options of [
      null,
      { ...valid, accountId: "" },
      { ...valid, accountId: "not-an-account" },
      { ...valid, apiToken: "" },
      { ...valid, maxResponseBytes: 1_023 },
      { ...valid, maxResponseBytes: 10 * 1024 * 1024 + 1 },
      { ...valid, fetch: 1 },
      { ...valid, now: 1 },
    ]) {
      expect(() => createCloudflareWorkerDeploymentClient(options as never)).toThrow()
    }
  })

  it("captures credentials, encodes script identity, and passes the exact signal", async () => {
    const calls: { init: RequestInit; url: string }[] = []
    const options = Object.assign(Object.create(null) as Record<string, unknown>, {
      accountId,
      apiToken: "original-token",
      fetch: fetchFrom((url, init) => {
        calls.push({ init, url })
        return jsonResponse(envelope([deployment()]))
      }),
      now: () => 1,
    }) as unknown as CloudflareWorkerDeploymentClientOptions
    const provider = createCloudflareWorkerDeploymentClient(options)
    ;(options as { apiToken: string }).apiToken = "replacement-token"
    const abort = new AbortController()
    await provider.getActiveDeployment("reader name/edge", { signal: abort.signal })

    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe(
      `${"https://api.cloudflare.com/client/v4/accounts"}/${accountId}/workers/scripts/reader%20name%2Fedge/deployments`,
    )
    expect(new Headers(calls[0]?.init.headers).get("authorization")).toBe("Bearer original-token")
    expect(new Headers(calls[0]?.init.headers).get("accept")).toBe("application/json")
    expect(calls[0]?.init).toMatchObject({ method: "GET", redirect: "error", signal: abort.signal })
  })

  it("supports platform defaults and rejects malformed call boundaries", async () => {
    expect(
      createCloudflareWorkerDeploymentClient({ accountId, apiToken: "platform-token" }),
    ).toBeDefined()
    const provider = client(() => jsonResponse(envelope([deployment()])))
    for (const scriptName of ["", "x".repeat(256), "\ud800", "\udc00"]) {
      await expect(provider.getActiveDeployment(scriptName)).rejects.toThrow()
    }
    await expect(provider.getActiveDeployment("reader", null as never)).rejects.toThrow()
    await expect(provider.getActiveDeployment("reader-\u{10000}")).resolves.toMatchObject({
      kind: "complete",
    })
    for (const versionId of ["", "not-a-version", versionA.toUpperCase()]) {
      await expect(provider.getVersionArtifact("reader", versionId)).rejects.toThrow()
    }
    await expect(provider.getVersionArtifact("", versionA)).rejects.toThrow()
    await expect(provider.getVersionArtifact("reader", versionA, null as never)).rejects.toThrow()
  })
})

describe("Cloudflare Worker-version artifact observation", () => {
  it("normalizes, freezes, and account-binds the documented script etag", async () => {
    const calls: { init: RequestInit; url: string }[] = []
    const abort = new AbortController()
    const provider = client((url, init) => {
      calls.push({ init, url })
      return jsonResponse(versionEnvelope(), 200, { "cf-ray": "fictional-version-ray" })
    })
    const result = await provider.getVersionArtifact("reader name/edge", versionA, {
      signal: abort.signal,
    })

    expect(result).toMatchObject({
      artifact: {
        artifactChecksum: "1".repeat(64),
        scriptName: "reader name/edge",
        versionId: versionA,
      },
      evidence: { bodyState: "complete", cfRay: "fictional-version-ray", status: 200 },
      kind: "complete",
    })
    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe(
      `${"https://api.cloudflare.com/client/v4/accounts"}/${accountId}/workers/scripts/reader%20name%2Fedge/versions/${versionA}`,
    )
    expect(calls[0]?.init).toMatchObject({ method: "GET", redirect: "error", signal: abort.signal })
    expect(new Headers(calls[0]?.init.headers).get("authorization")).toBe("Bearer fictional-token")
    expect(Object.isFrozen(result)).toBe(true)
    if (result.kind !== "complete") throw new Error("Expected a complete artifact observation.")
    expect(Object.isFrozen(result.artifact)).toBe(true)
    expect(Object.isFrozen(result.proof)).toBe(true)
    expect(Object.keys(result.proof)).toEqual([])
  })

  it("distinguishes absent optional artifact metadata", async () => {
    for (const result of [
      { id: versionA, resources: undefined },
      { id: versionA, resources: {} },
      { id: versionA, resources: { script: {} } },
    ]) {
      await expect(
        client(() => jsonResponse(versionEnvelope(result))).getVersionArtifact("reader", versionA),
      ).resolves.toMatchObject({ kind: "inconclusive", reason: "missing_artifact" })
    }
  })

  it("rejects malformed version envelopes, identities, resources, and etags", async () => {
    const malformed: readonly unknown[] = [
      [],
      { result: { id: versionA }, success: false },
      { result: null, success: true },
      versionEnvelope({ id: "bad-version" }),
      versionEnvelope({ id: versionB }),
      versionEnvelope({ resources: null }),
      versionEnvelope({ resources: { script: null } }),
      versionEnvelope({ resources: { script: { etag: null } } }),
      versionEnvelope({ resources: { script: { etag: "bad" } } }),
      versionEnvelope({ resources: { script: { etag: "A".repeat(64) } } }),
    ]
    for (const body of malformed) {
      await expect(
        client(() => jsonResponse(body)).getVersionArtifact("reader", versionA),
      ).resolves.toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
    }
  })

  it("keeps retry, rejection, transport, and bounded-body failures inconclusive", async () => {
    await expect(
      client(() =>
        jsonResponse({ errors: [{ code: 1000, message: "try later" }], success: false }, 429, {
          "retry-after": "1",
        }),
      ).getVersionArtifact("reader", versionA),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "retry_required" })
    await expect(
      client(() => jsonResponse({ errors: [], success: false }, 403)).getVersionArtifact(
        "reader",
        versionA,
      ),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "provider_rejected" })
    await expect(
      client(() => {
        throw new Error("network unavailable")
      }).getVersionArtifact("reader", versionA),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "transport_error" })
    await expect(
      client(() => rawResponse(null)).getVersionArtifact("reader", versionA),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
    await expect(
      client(() => rawResponse(new Uint8Array(1_025)), {
        maxResponseBytes: 1_024,
      }).getVersionArtifact("reader", versionA),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
  })
})

describe("Cloudflare active Worker deployment observation", () => {
  it("normalizes and freezes one active version with transport evidence", async () => {
    const body = JSON.stringify(envelope([deployment()]))
    const provider = client(() =>
      rawResponse(body, 200, {
        "cf-ray": "fictional-ray",
        Ratelimit: '"default";r=9;t=30',
        "Ratelimit-Policy": '"default";q=10;w=60',
      }),
    )
    const result = await provider.getActiveDeployment("reader")

    expect(result).toMatchObject({
      deployment: {
        createdAtMs: Date.UTC(2026, 6, 12, 0, 0, 0, 123),
        deploymentId,
        scriptName: "reader",
        versions: [{ versionId: versionA, weightBps: 10_000 }],
      },
      evidence: {
        bodyBytes: new TextEncoder().encode(body).byteLength,
        bodyState: "complete",
        cfRay: "fictional-ray",
        completedAtMs: 1_001,
        rateLimit: { quota: 10, remaining: 9, resetAfterMs: 30_000, windowMs: 60_000 },
        responseChecksum: expect.stringMatching(/^[0-9a-f]{64}$/u),
        startedAtMs: 1_000,
        status: 200,
      },
      kind: "complete",
    })
    expect(Object.isFrozen(result)).toBe(true)
    if (result.kind !== "complete") throw new Error("Expected a complete deployment.")
    expect(Object.isFrozen(result.deployment)).toBe(true)
    expect(Object.isFrozen(result.deployment.versions)).toBe(true)
    expect(Object.isFrozen(result.deployment.versions[0])).toBe(true)
    expect(Object.isFrozen(result.evidence)).toBe(true)
    expect(Object.isFrozen(result.proof)).toBe(true)
    expect(Object.keys(result.proof)).toEqual([])
  })

  it("normalizes exact gradual percentages and canonical version order", async () => {
    const provider = client(() =>
      jsonResponse(
        envelope([
          deployment(
            [
              { percentage: 66.67, version_id: versionB },
              { percentage: 33.33, version_id: versionA },
            ],
            { created_on: "2024-02-29T23:59:59.123456789Z" },
          ),
        ]),
      ),
    )
    await expect(provider.getActiveDeployment("reader")).resolves.toMatchObject({
      deployment: {
        createdAtMs: Date.UTC(2024, 1, 29, 23, 59, 59, 123),
        versions: [
          { versionId: versionA, weightBps: 3_333 },
          { versionId: versionB, weightBps: 6_667 },
        ],
      },
      kind: "complete",
    })

    const alreadyOrdered = client(() =>
      jsonResponse(
        envelope([
          deployment(
            [
              { percentage: 50, version_id: versionA },
              { percentage: 50, version_id: versionB },
            ],
            { created_on: "2000-02-29T00:00:00Z" },
          ),
        ]),
      ),
    )
    await expect(alreadyOrdered.getActiveDeployment("reader")).resolves.toMatchObject({
      deployment: {
        createdAtMs: Date.UTC(2000, 1, 29),
        versions: [{ versionId: versionA }, { versionId: versionB }],
      },
    })
  })

  it("distinguishes an empty active-deployment inventory", async () => {
    const result = await client(() => jsonResponse(envelope([]))).getActiveDeployment("reader")
    expect(result).toMatchObject({ kind: "inconclusive", reason: "missing_deployment" })
    if (result.kind !== "inconclusive") throw new Error("Expected inconclusive evidence.")
    expect(result.errors).toEqual([])
    expect(Object.isFrozen(result.errors)).toBe(true)
  })

  it("rejects every malformed deployment identity, time, version, and percentage", async () => {
    const malformed: readonly unknown[] = [
      [],
      { result: { deployments: [deployment()] }, success: false },
      { result: null, success: true },
      { result: { deployments: {} }, success: true },
      envelope([null]),
      envelope([deployment(undefined, { strategy: "random" })]),
      envelope([deployment(undefined, { id: "not-a-deployment" })]),
      envelope([deployment(undefined, { id: deploymentId.toUpperCase() })]),
      envelope([deployment(undefined, { created_on: null })]),
      envelope([deployment(undefined, { created_on: "not-a-time" })]),
      envelope([deployment(undefined, { created_on: "1969-12-31T23:59:59Z" })]),
      envelope([deployment(undefined, { created_on: "2026-00-01T00:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2026-13-01T00:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2026-01-00T00:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2023-02-29T00:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2100-02-29T00:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2026-01-01T24:00:00Z" })]),
      envelope([deployment(undefined, { created_on: "2026-01-01T00:60:00Z" })]),
      envelope([deployment(undefined, { created_on: "2026-01-01T00:00:60Z" })]),
      envelope([deployment(undefined, { versions: {} })]),
      envelope([deployment([])]),
      envelope([
        deployment([
          { percentage: 30, version_id: versionA },
          { percentage: 30, version_id: versionB },
          { percentage: 40, version_id: versionC },
        ]),
      ]),
      envelope([deployment([null])]),
      envelope([deployment([{ percentage: 100, version_id: "bad-version" }])]),
      envelope([
        deployment([
          { percentage: 50, version_id: versionA },
          { percentage: 50, version_id: versionA },
        ]),
      ]),
      envelope([deployment([{ percentage: null, version_id: versionA }])]),
      envelope([deployment([{ percentage: 0, version_id: versionA }])]),
      envelope([deployment([{ percentage: 100.01, version_id: versionA }])]),
      envelope([deployment([{ percentage: 33.333, version_id: versionA }])]),
      envelope([deployment([{ percentage: 99.99, version_id: versionA }])]),
    ]
    for (const body of malformed) {
      const result = await client(() => jsonResponse(body)).getActiveDeployment("reader")
      expect(result).toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
    }

    const infinite = `{"errors":[],"messages":[],"result":{"deployments":[{"created_on":"2026-07-12T00:00:00Z","id":"${deploymentId}","strategy":"percentage","versions":[{"percentage":1e400,"version_id":"${versionA}"}]}]},"success":true}`
    await expect(
      client(() => rawResponse(infinite)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
  })
})

describe("Cloudflare Worker deployment transport failures", () => {
  it("classifies retryable, rejected, and transport outcomes without leaking credentials", async () => {
    const longMessage = `provider\u0000${"x".repeat(1_100)}`
    const retry = await client(() =>
      jsonResponse(
        {
          errors: [
            null,
            { code: "bad", message: "ignored" },
            { code: 1.5, message: "ignored" },
            { code: 1000, message: 7 },
            { code: 1001, message: "" },
            { code: 1002, message: longMessage },
          ],
          messages: [],
          result: null,
          success: false,
        },
        429,
        { "retry-after": "2" },
      ),
    ).getActiveDeployment("reader")
    expect(retry).toMatchObject({
      errors: [{ code: 1002 }],
      evidence: { rateLimit: { retryAfterMs: 2_000 }, status: 429 },
      kind: "inconclusive",
      reason: "retry_required",
    })
    if (retry.kind !== "inconclusive") throw new Error("Expected a retry result.")
    expect(retry.errors[0]?.message).toHaveLength(1_024)
    expect(retry.errors[0]?.message).not.toContain("\u0000")
    expect(JSON.stringify(retry)).not.toContain("fictional-token")

    await expect(
      client(() => jsonResponse({ errors: {}, success: false }, 403)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "provider_rejected" })
    await expect(
      client(() => jsonResponse({ errors: [], success: false }, 500)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "retry_required" })
    await expect(
      client(() => {
        throw new Error("network unavailable")
      }).getActiveDeployment("reader"),
    ).resolves.toMatchObject({
      evidence: { bodyState: "not_received", status: null, transportErrorKind: "network" },
      kind: "inconclusive",
      reason: "transport_error",
    })
  })

  it("classifies DOM and cross-realm shaped aborts", async () => {
    for (const error of [new DOMException("aborted", "AbortError"), { name: "AbortError" }]) {
      await expect(
        client(() => {
          throw error
        }).getActiveDeployment("reader"),
      ).resolves.toMatchObject({
        evidence: { transportErrorKind: "aborted" },
        kind: "inconclusive",
        reason: "transport_error",
      })
    }
  })

  it("bounds, cancels, and validates response streams before decoding", async () => {
    const oversized = new ReadableStream<Uint8Array>({
      cancel() {
        throw new Error("injected cancel failure")
      },
      start(controller) {
        controller.enqueue(new Uint8Array(1_025))
      },
    })
    await expect(
      client(() => rawResponse(oversized, 200, { "cf-ray": "failure-ray" }), {
        maxResponseBytes: 1_024,
      }).getActiveDeployment("reader"),
    ).resolves.toMatchObject({
      evidence: { bodyBytes: 1_025, bodyState: "too_large", cfRay: "failure-ray" },
      kind: "inconclusive",
      reason: "malformed_response",
    })

    let pulled = false
    const unreadable = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!pulled) {
          pulled = true
          controller.enqueue(new TextEncoder().encode("{"))
        } else {
          controller.error(new Error("injected read failure"))
        }
      },
    })
    await expect(
      client(() => rawResponse(unreadable)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({
      evidence: { bodyBytes: 1, bodyState: "unreadable" },
      kind: "inconclusive",
      reason: "malformed_response",
    })

    await expect(
      client(() => rawResponse(new Uint8Array([0xff]))).getActiveDeployment("reader"),
    ).resolves.toMatchObject({
      evidence: { bodyState: "unreadable" },
      kind: "inconclusive",
      reason: "malformed_response",
    })
    await expect(
      client(() => rawResponse(null)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({
      evidence: { bodyBytes: 0, bodyState: "complete" },
      kind: "inconclusive",
      reason: "malformed_response",
    })
    await expect(
      client(() => rawResponse("{")).getActiveDeployment("reader"),
    ).resolves.toMatchObject({ kind: "inconclusive", reason: "malformed_response" })
  })

  it("assembles a valid multi-chunk response", async () => {
    const body = JSON.stringify(envelope([deployment()]))
    const midpoint = Math.floor(body.length / 2)
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(body.slice(0, midpoint)))
        controller.enqueue(new TextEncoder().encode(body.slice(midpoint)))
        controller.close()
      },
    })
    await expect(
      client(() => rawResponse(stream)).getActiveDeployment("reader"),
    ).resolves.toMatchObject({ kind: "complete" })
  })

  it("rejects invalid and regressing provider clocks on every path", async () => {
    await expect(
      client(() => jsonResponse(envelope([deployment()])), { now: () => -1 }).getActiveDeployment(
        "reader",
      ),
    ).rejects.toThrow(/clock/u)
    await expect(
      client(() => jsonResponse(envelope([deployment()])), { now: () => 1.5 }).getActiveDeployment(
        "reader",
      ),
    ).rejects.toThrow(/clock/u)

    for (const fetch of [
      fetchFrom(() => jsonResponse(envelope([deployment()]))),
      fetchFrom(() => {
        throw new Error("network")
      }),
    ]) {
      const times = [2, 1]
      const provider = createCloudflareWorkerDeploymentClient({
        accountId,
        apiToken: "fictional-token",
        fetch,
        now: () => times.shift() as number,
      })
      await expect(provider.getActiveDeployment("reader")).rejects.toThrow(/backwards/u)
    }
  })
})
