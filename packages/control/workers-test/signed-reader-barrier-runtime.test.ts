import { env } from "cloudflare:workers"
import {
  createCloudflareWorkerDeploymentClient,
  createReaderDeploymentVerifier,
  readerVersionAttestationSigningBytes,
  verifyReaderDeploymentStability,
} from "@nozzle/cloudflare"
import type { DigestFunction } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { CONTROL_SCHEMA_STATEMENTS } from "../src/schema.js"
import { D1SignedReaderBarrierStore } from "../src/signed-reader-barrier-store.js"

declare global {
  namespace Cloudflare {
    interface Env {
      SIGNED_BARRIER_DB: D1Database
    }
  }
}

const accountId = "a".repeat(32)
const scriptName = "nozzle-controller"
const versionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const deploymentId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const artifactChecksum = "1".repeat(64)

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function base64Url(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")
}

function response(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status: 200,
  })
}

describe("real workerd signed reader deployment barrier", () => {
  it("verifies Ed25519 deployment evidence and activates it through a real D1 batch", async () => {
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.SIGNED_BARRIER_DB.prepare(statement).run()
    }
    const baseTimeMs = Date.now() - 1_000
    let observedAtMs = baseTimeMs
    const client = createCloudflareWorkerDeploymentClient({
      accountId,
      apiToken: "fictional-workerd-signed-token",
      fetch: (async (input: string | URL | Request) => {
        const url = String(input)
        if (url.endsWith("/deployments")) {
          return response({
            errors: [],
            messages: [],
            result: {
              deployments: [
                {
                  created_on: "2026-07-12T00:00:00.000Z",
                  id: deploymentId,
                  strategy: "percentage",
                  versions: [{ percentage: 100, version_id: versionId }],
                },
              ],
            },
            success: true,
          })
        }
        return response({
          errors: [],
          messages: [],
          result: { id: versionId, resources: { script: { etag: artifactChecksum } } },
          success: true,
        })
      }) as typeof globalThis.fetch,
      now: () => observedAtMs++,
    })
    const deployment = await client.getActiveDeployment(scriptName)
    const artifact = await client.getVersionArtifact(scriptName, versionId)
    if (deployment.kind !== "complete" || artifact.kind !== "complete") {
      throw new Error("Expected complete fictional Cloudflare observations.")
    }
    const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
      "sign",
      "verify",
    ])) as CryptoKeyPair
    const publicKeyBase64Url = base64Url(
      new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)),
    )
    const statement = {
      artifactChecksum,
      audience: "nozzle:fictional-workerd-signed",
      controlSchemaMax: 6,
      controlSchemaMin: 5,
      expiresAtMs: baseTimeMs + 1_500,
      issuedAtMs: baseTimeMs - 100,
      keyId: "fictional-workerd-release-key",
      outcomePayloadReaderMax: 1,
      outcomePayloadReaderMin: 1,
      schemaVersion: 1 as const,
      scriptName,
      versionId,
    }
    const signature = base64Url(
      new Uint8Array(
        await crypto.subtle.sign(
          { name: "Ed25519" },
          pair.privateKey,
          readerVersionAttestationSigningBytes(statement),
        ),
      ),
    )
    let verifiedAtMs = baseTimeMs + 100
    const verifier = await createReaderDeploymentVerifier({
      accountId,
      audience: statement.audience,
      maxAttestationValidityMs: 2_000,
      maxObservationAgeMs: 200,
      maxObservationWindowMs: 100,
      now: () => verifiedAtMs,
      trustedKeys: [{ keyId: statement.keyId, publicKeyBase64Url }],
    })
    const before = await verifier.verify({
      artifactProofs: [artifact.proof],
      attestations: [{ signature, statement }],
      deploymentProofs: [deployment.proof],
      expectedScriptNames: [scriptName],
    })
    observedAtMs = baseTimeMs + 200
    verifiedAtMs = baseTimeMs + 300
    const secondDeployment = await client.getActiveDeployment(scriptName)
    const secondArtifact = await client.getVersionArtifact(scriptName, versionId)
    if (secondDeployment.kind !== "complete" || secondArtifact.kind !== "complete") {
      throw new Error("Expected complete fictional Cloudflare reobservations.")
    }
    const after = await verifier.verify({
      artifactProofs: [secondArtifact.proof],
      attestations: [{ signature, statement }],
      deploymentProofs: [secondDeployment.proof],
      expectedScriptNames: [scriptName],
    })
    const capability = verifyReaderDeploymentStability(before, after, 5_000)
    const store = new D1SignedReaderBarrierStore(env.SIGNED_BARRIER_DB, digest)
    const activated = await store.activate(capability)

    expect(activated).toMatchObject({
      accountId,
      activeDeployments: [
        {
          deploymentId,
          scriptName,
          versions: [{ versionId, weightBps: 10_000 }],
        },
      ],
      audience: statement.audience,
      protocolVersion: 1,
    })
    await expect(store.get()).resolves.toEqual(activated)
    await expect(
      env.SIGNED_BARRIER_DB.prepare(
        `SELECT
           (SELECT count(*) FROM "nozzle_reader_version_attestations") AS "attestations",
           (SELECT count(*) FROM "nozzle_reader_barrier_verifications") AS "verifications",
           (SELECT count(*) FROM "nozzle_saga_outcome_payload_activations") AS "activations"`,
      ).first(),
    ).resolves.toEqual({ activations: 1, attestations: 1, verifications: 1 })
  })
})
