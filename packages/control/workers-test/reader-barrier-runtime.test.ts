import { env } from "cloudflare:workers"
import type { DigestFunction } from "@nozzle/core"
import { describe, expect, it } from "vitest"
import { D1ReaderBarrierStore, verifyReaderDeploymentBarrier } from "../src/reader-barrier-store.js"
import { CONTROL_SCHEMA_STATEMENTS } from "../src/schema.js"

declare global {
  namespace Cloudflare {
    interface Env {
      BARRIER_DB: D1Database
    }
  }
}

const digest: DigestFunction = async (input) => {
  const copy = new Uint8Array(input.byteLength)
  copy.set(input)
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer))
  return [...hash].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

describe("real workerd reader deployment barrier", () => {
  it("activates an exact gradual deployment through one transactional D1 batch", async () => {
    for (const statement of CONTROL_SCHEMA_STATEMENTS) {
      await env.BARRIER_DB.prepare(statement).run()
    }
    const capability = await verifyReaderDeploymentBarrier(
      {
        attestations: [
          {
            artifactChecksum: "1".repeat(64),
            controlSchemaMax: 5,
            controlSchemaMin: 5,
            outcomePayloadReaderMax: 1,
            outcomePayloadReaderMin: 1,
            scriptName: "nozzle-controller",
            versionId: "version-old",
          },
          {
            artifactChecksum: "2".repeat(64),
            controlSchemaMax: 6,
            controlSchemaMin: 5,
            outcomePayloadReaderMax: 2,
            outcomePayloadReaderMin: 1,
            scriptName: "nozzle-controller",
            versionId: "version-new",
          },
        ],
        deployments: [
          {
            deploymentId: "deployment-active",
            scriptName: "nozzle-controller",
            versions: [
              { versionId: "version-old", weightBps: 9_000 },
              { versionId: "version-new", weightBps: 1_000 },
            ],
          },
        ],
        expectedScriptNames: ["nozzle-controller"],
      },
      digest,
    )
    const store = new D1ReaderBarrierStore(env.BARRIER_DB, digest)
    const activated = await store.activate(capability)

    expect(activated).toMatchObject({
      activeDeployments: [
        {
          deploymentId: "deployment-active",
          versions: [
            { versionId: "version-new", weightBps: 1_000 },
            { versionId: "version-old", weightBps: 9_000 },
          ],
        },
      ],
      protocolVersion: 1,
    })
    await expect(store.get()).resolves.toEqual(activated)
    await expect(
      env.BARRIER_DB.prepare(
        `SELECT
           (SELECT count(*) FROM "nozzle_reader_version_attestations") AS "attestations",
           (SELECT count(*) FROM "nozzle_reader_barriers") AS "barriers",
           (SELECT count(*) FROM "nozzle_saga_outcome_payload_activations") AS "activations"`,
      ).first(),
    ).resolves.toEqual({ activations: 1, attestations: 2, barriers: 1 })
  })
})
