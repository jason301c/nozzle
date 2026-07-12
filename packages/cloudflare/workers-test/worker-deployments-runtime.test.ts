import { describe, expect, it } from "vitest"
import {
  createReaderDeploymentVerifier,
  readerVersionAttestationSigningBytes,
  verifiedReaderDeploymentEvidence,
} from "../src/reader-attestations.js"
import { createCloudflareWorkerDeploymentClient } from "../src/worker-deployments.js"

function base64Url(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")
}

function activeResponse(): Response {
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
}

describe("real workerd Worker-deployment transport", () => {
  it("normalizes the documented active gradual-deployment response", async () => {
    let observedAuthorization: string | null = null
    let time = 1_000
    const fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      observedAuthorization = new Headers(init?.headers).get("authorization")
      return activeResponse()
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

  it("verifies active-version Ed25519 attestations with opaque account-bound proofs", async () => {
    let time = 1_000
    const client = createCloudflareWorkerDeploymentClient({
      accountId: "a".repeat(32),
      apiToken: "fictional-workerd-token",
      fetch: (async () => activeResponse()) as typeof globalThis.fetch,
      now: () => time++,
    })
    const observation = await client.getActiveDeployment("nozzle-controller")
    if (observation.kind !== "complete") throw new Error("Expected a complete observation.")
    const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
      "sign",
      "verify",
    ])) as CryptoKeyPair
    const publicKeyBase64Url = base64Url(
      new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)),
    )
    const statements = observation.deployment.versions.map(({ versionId }, index) => ({
      artifactChecksum: (index === 0 ? "1" : "2").repeat(64),
      audience: "nozzle:fictional-workerd",
      controlSchemaMax: 5,
      controlSchemaMin: 5,
      expiresAtMs: 1_500,
      issuedAtMs: 900,
      keyId: "workerd-release-key",
      outcomePayloadReaderMax: 1,
      outcomePayloadReaderMin: 1,
      schemaVersion: 1 as const,
      scriptName: "nozzle-controller",
      versionId,
    }))
    const attestations = await Promise.all(
      statements.map(async (statement) => ({
        signature: base64Url(
          new Uint8Array(
            await crypto.subtle.sign(
              { name: "Ed25519" },
              pair.privateKey,
              readerVersionAttestationSigningBytes(statement),
            ),
          ),
        ),
        statement,
      })),
    )
    const verifier = await createReaderDeploymentVerifier({
      accountId: "a".repeat(32),
      audience: "nozzle:fictional-workerd",
      maxAttestationValidityMs: 1_000,
      maxObservationAgeMs: 200,
      maxObservationWindowMs: 100,
      now: () => 1_100,
      trustedKeys: [{ keyId: "workerd-release-key", publicKeyBase64Url }],
    })
    const capability = await verifier.verify({
      attestations,
      deploymentProofs: [observation.proof],
      expectedScriptNames: ["nozzle-controller"],
    })

    expect(verifiedReaderDeploymentEvidence(capability)).toMatchObject({
      attestations: [
        { versionId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
        { versionId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" },
      ],
      audience: "nozzle:fictional-workerd",
      verifiedAtMs: 1_100,
    })
  })
})
