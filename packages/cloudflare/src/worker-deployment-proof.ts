import type {
  ActiveWorkerDeployment,
  WorkerDeploymentObservationEvidence,
} from "./worker-deployments.js"

export type ActiveWorkerDeploymentProof = object

export interface ActiveWorkerDeploymentProofState {
  readonly accountId: string
  readonly deployment: ActiveWorkerDeployment
  readonly evidence: WorkerDeploymentObservationEvidence
}

const states = new WeakMap<object, ActiveWorkerDeploymentProofState>()

export function createActiveWorkerDeploymentProof(
  state: ActiveWorkerDeploymentProofState,
): ActiveWorkerDeploymentProof {
  const proof = Object.freeze({})
  states.set(proof, Object.freeze({ ...state }))
  return proof
}

export function activeWorkerDeploymentProofState(
  proof: ActiveWorkerDeploymentProof,
): ActiveWorkerDeploymentProofState | undefined {
  return typeof proof === "object" && proof !== null ? states.get(proof) : undefined
}
