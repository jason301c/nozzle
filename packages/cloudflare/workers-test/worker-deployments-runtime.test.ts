import { describe, expect, it } from "vitest"
import { createCloudflareWorkerDeploymentClient } from "../src/worker-deployments.js"

describe("real workerd Worker-deployment transport", () => {
  it("normalizes the documented active gradual-deployment response", async () => {
    let observedAuthorization: string | null = null
    let time = 1_000
    const fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      observedAuthorization = new Headers(init?.headers).get("authorization")
      return new Response(
        JSON.stringify({
          errors: [],
          messages: [],
          result: {
            deployments: [
              {
                created_on: "2026-07-12T00:00:00.000Z",
                id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                strategy: "percentage",
                versions: [
                  {
                    percentage: 90,
                    version_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                  },
                  {
                    percentage: 10,
                    version_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                  },
                ],
              },
            ],
          },
          success: true,
        }),
        { headers: { "content-type": "application/json" }, status: 200 },
      )
    }) as typeof globalThis.fetch
    const client = createCloudflareWorkerDeploymentClient({
      accountId: "a".repeat(32),
      apiToken: "fictional-workerd-token",
      fetch,
      now: () => time++,
    })

    await expect(client.getActiveDeployment("nozzle-controller")).resolves.toMatchObject({
      deployment: {
        deploymentId: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        scriptName: "nozzle-controller",
        versions: [
          { versionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", weightBps: 1_000 },
          { versionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", weightBps: 9_000 },
        ],
      },
      evidence: { bodyState: "complete", status: 200 },
      kind: "complete",
    })
    expect(observedAuthorization).toBe("Bearer fictional-workerd-token")
  })
})
