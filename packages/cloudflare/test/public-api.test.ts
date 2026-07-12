import { describe, expect, it } from "vitest"
import * as exports from "../src/index.js"

describe("@nozzle/cloudflare public API", () => {
  it("exports signed deployment verification without exposing proof minting", () => {
    expect(exports.createCloudflareWorkerDeploymentClient).toBeTypeOf("function")
    expect(exports.createReaderDeploymentVerifier).toBeTypeOf("function")
    expect(exports.readerVersionAttestationSigningBytes).toBeTypeOf("function")
    expect(exports.verifiedReaderDeploymentEvidence).toBeTypeOf("function")
    expect(exports).not.toHaveProperty("createActiveWorkerDeploymentProof")
    expect(exports).not.toHaveProperty("activeWorkerDeploymentProofState")
    expect(exports).not.toHaveProperty("createWorkerVersionArtifactProof")
    expect(exports).not.toHaveProperty("workerVersionArtifactProofState")
  })
})
