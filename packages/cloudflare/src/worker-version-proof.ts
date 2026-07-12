import type {
  WorkerDeploymentObservationEvidence,
  WorkerVersionArtifact,
} from "./worker-deployments.js"

export type WorkerVersionArtifactProof = object

export interface WorkerVersionArtifactProofState {
  readonly accountId: string
  readonly artifact: WorkerVersionArtifact
  readonly evidence: WorkerDeploymentObservationEvidence
}

const proofStates = new WeakMap<object, WorkerVersionArtifactProofState>()

export function createWorkerVersionArtifactProof(
  state: WorkerVersionArtifactProofState,
): WorkerVersionArtifactProof {
  const proof = Object.freeze({})
  proofStates.set(proof, state)
  return proof
}

export function workerVersionArtifactProofState(
  proof: WorkerVersionArtifactProof,
): WorkerVersionArtifactProofState | undefined {
  return typeof proof === "object" && proof !== null ? proofStates.get(proof) : undefined
}
