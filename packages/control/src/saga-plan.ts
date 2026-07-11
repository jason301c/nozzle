import {
  assertTrustedSagaDescriptor,
  createOperationRecord,
  type DigestFunction,
  NozzleError,
  type OperationPlan,
  type OperationStepPlanInput,
  type SagaActionPhase,
  type SagaDescriptor,
  type SagaRecord,
  sagaActionIdempotencyKey,
  sealOperationPlan,
} from "@nozzle/core"
import { assertTrustedSagaHandlerRegistry, type SagaHandlerRegistry } from "./saga-registry.js"
import {
  SAGA_INIT_OPERATION_STEP_ID,
  SAGA_SETTLE_OPERATION_STEP_ID,
  SAGA_TERMINATION_OPERATION_STEP_ID,
  sagaActionOperationStepId,
} from "./saga-store.js"

const PLAN_VALUE_DOMAIN = "nozzle.saga-operation-plan-value.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const MAX_IDENTITY_BYTES = 512

interface SagaOperationPlanIdentity {
  readonly descriptorChecksum: string
  readonly inputChecksum: string
  readonly sagaId: string
  readonly stepInputChecksumsJson: string
}

const TRUSTED_SAGA_OPERATION_PLANS = new WeakMap<OperationPlan, SagaOperationPlanIdentity>()

export interface SealSagaOperationPlanInput {
  readonly capabilitySnapshotChecksum: string
  readonly descriptor: SagaDescriptor
  readonly inputChecksum: string
  readonly leaseKey: string
  readonly operationId: string
  readonly operationIdempotencyKey: string
  readonly registry: SagaHandlerRegistry
  readonly sagaId: string
  readonly stepInputChecksums: Readonly<Record<string, string>>
}

type BuildSagaOperationPlanInput = Omit<SealSagaOperationPlanInput, "registry">

type SagaOperationPlanInputSnapshot = Readonly<SealSagaOperationPlanInput>

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function snapshotStepInputChecksums(value: unknown): Readonly<Record<string, string>> {
  const cloned = structuredClone(value)
  if (typeof cloned !== "object" || cloned === null || Array.isArray(cloned)) {
    return cloned as Readonly<Record<string, string>>
  }
  return Object.freeze(Object.fromEntries(Object.entries(cloned)))
}

function snapshotSagaOperationPlanInput(
  input: SealSagaOperationPlanInput,
): SagaOperationPlanInputSnapshot {
  try {
    const capabilitySnapshotChecksum = input.capabilitySnapshotChecksum
    const descriptor = input.descriptor
    const inputChecksum = input.inputChecksum
    const leaseKey = input.leaseKey
    const operationId = input.operationId
    const operationIdempotencyKey = input.operationIdempotencyKey
    const registry = input.registry
    const sagaId = input.sagaId
    const stepInputChecksums = snapshotStepInputChecksums(input.stepInputChecksums)
    return Object.freeze({
      capabilitySnapshotChecksum,
      descriptor,
      inputChecksum,
      leaseKey,
      operationId,
      operationIdempotencyKey,
      registry,
      sagaId,
      stepInputChecksums,
    })
  } catch {
    configuration("Saga operation-plan input could not be snapshotted.")
  }
}

function boundedText(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    configuration(`${label} exceeds the durable saga identity limit.`)
  }
}

function checksum(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
  }
}

function frame(parts: readonly string[]): Uint8Array {
  const encoded = [PLAN_VALUE_DOMAIN, ...parts].map((part) => new TextEncoder().encode(part))
  const length = encoded.reduce((total, part) => total + 4 + part.byteLength, 0)
  const output = new Uint8Array(length)
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of encoded) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

async function valueChecksum(
  digest: DigestFunction,
  descriptorChecksum: string,
  sagaId: string,
  stepId: string,
  valueKind: string,
  inputChecksum: string,
): Promise<string> {
  const value = await digest(
    frame([descriptorChecksum, sagaId, stepId, valueKind, inputChecksum]).slice(),
  )
  checksum(value, "Saga operation plan value checksum")
  return value
}

function validateStepInputs(
  descriptor: SagaDescriptor,
  stepInputChecksums: Readonly<Record<string, string>>,
): void {
  if (
    typeof stepInputChecksums !== "object" ||
    stepInputChecksums === null ||
    Array.isArray(stepInputChecksums)
  ) {
    configuration("Saga step input checksums are malformed.")
  }
  const expected = descriptor.steps.map((step) => step.stepId).sort()
  const actual = Object.keys(stepInputChecksums).sort()
  if (
    expected.length !== actual.length ||
    expected.some((stepId, index) => stepId !== actual[index])
  ) {
    configuration("Saga step input checksums do not match the descriptor.")
  }
  for (const stepId of expected) checksum(stepInputChecksums[stepId], "Saga step input checksum")
}

function stepInputChecksumsJson(
  descriptor: SagaDescriptor,
  stepInputChecksums: Readonly<Record<string, string>>,
): string {
  return JSON.stringify(
    descriptor.steps.map((step) => [step.stepId, stepInputChecksums[step.stepId]] as const),
  )
}

function bindSagaOperationPlan(
  plan: OperationPlan,
  input: Pick<
    BuildSagaOperationPlanInput,
    "descriptor" | "inputChecksum" | "sagaId" | "stepInputChecksums"
  >,
): OperationPlan {
  TRUSTED_SAGA_OPERATION_PLANS.set(
    plan,
    Object.freeze({
      descriptorChecksum: input.descriptor.descriptorChecksum,
      inputChecksum: input.inputChecksum,
      sagaId: input.sagaId,
      stepInputChecksumsJson: stepInputChecksumsJson(input.descriptor, input.stepInputChecksums),
    }),
  )
  return plan
}

async function actionStep(
  input: BuildSagaOperationPlanInput,
  step: SagaDescriptor["steps"][number],
  phase: SagaActionPhase,
  digest: DigestFunction,
): Promise<OperationStepPlanInput> {
  const operationStepId = sagaActionOperationStepId(step.stepId, phase)
  const stepInputChecksum = input.stepInputChecksums[step.stepId] as string
  const staticInputChecksum =
    phase === "forward"
      ? stepInputChecksum
      : await valueChecksum(
          digest,
          input.descriptor.descriptorChecksum,
          input.sagaId,
          operationStepId,
          "compensation-input",
          stepInputChecksum,
        )
  return Object.freeze({
    activation: "conditional",
    checkpoint: phase === "forward" && step.irreversible ? "irreversible" : "reversible",
    dependsOn: [],
    effectProtocol: "saga_receipt",
    idempotencyKey: sagaActionIdempotencyKey(input.sagaId, step.stepId, phase),
    inputChecksum: staticInputChecksum,
    leaseKey: input.leaseKey,
    postconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      operationStepId,
      "postcondition",
      step.outputSchemaChecksum,
    ),
    preconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      operationStepId,
      "precondition",
      step.inputSchemaChecksum,
    ),
    recoveryInstructions:
      "Resume only from the durable saga projection and exact append-only action receipt.",
    retryClassification: "reconcile_first",
    stepId: operationStepId,
  })
}

async function buildSagaOperationPlan(
  input: BuildSagaOperationPlanInput,
  digest: DigestFunction,
): Promise<OperationPlan> {
  if (typeof digest !== "function") configuration("A saga operation-plan digest is required.")
  assertTrustedSagaDescriptor(input.descriptor)
  boundedText(input.operationId, "Operation ID")
  boundedText(input.operationIdempotencyKey, "Operation idempotency key")
  boundedText(input.sagaId, "Saga ID")
  boundedText(input.leaseKey, "Saga lease key")
  checksum(input.capabilitySnapshotChecksum, "Capability snapshot checksum")
  checksum(input.inputChecksum, "Saga input checksum")
  validateStepInputs(input.descriptor, input.stepInputChecksums)
  const init: OperationStepPlanInput = Object.freeze({
    checkpoint: "reversible",
    dependsOn: [],
    effectProtocol: "opaque",
    idempotencyKey: `${input.operationIdempotencyKey}:init`,
    inputChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_INIT_OPERATION_STEP_ID,
      "input",
      input.inputChecksum,
    ),
    leaseKey: input.leaseKey,
    postconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_INIT_OPERATION_STEP_ID,
      "postcondition",
      input.descriptor.descriptorChecksum,
    ),
    preconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_INIT_OPERATION_STEP_ID,
      "precondition",
      input.inputChecksum,
    ),
    recoveryInstructions:
      "Reconstruct the saga projection from its exact operation-effect receipt.",
    retryClassification: "idempotent",
    stepId: SAGA_INIT_OPERATION_STEP_ID,
  })
  const termination: OperationStepPlanInput = Object.freeze({
    activation: "conditional",
    checkpoint: "reversible",
    dependsOn: [],
    effectProtocol: "opaque",
    idempotencyKey: `${input.operationIdempotencyKey}:termination`,
    inputChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_TERMINATION_OPERATION_STEP_ID,
      "input",
      input.inputChecksum,
    ),
    leaseKey: input.leaseKey,
    postconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_TERMINATION_OPERATION_STEP_ID,
      "postcondition",
      input.descriptor.descriptorChecksum,
    ),
    preconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_TERMINATION_OPERATION_STEP_ID,
      "precondition",
      input.inputChecksum,
    ),
    recoveryInstructions:
      "Resume the durable cancellation or timeout request under the saga lease.",
    retryClassification: "idempotent",
    stepId: SAGA_TERMINATION_OPERATION_STEP_ID,
  })
  const settlement: OperationStepPlanInput = Object.freeze({
    checkpoint: "reversible",
    completionRole: "settlement",
    dependsOn: [],
    effectProtocol: "opaque",
    idempotencyKey: `${input.operationIdempotencyKey}:settle`,
    inputChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_SETTLE_OPERATION_STEP_ID,
      "input",
      input.inputChecksum,
    ),
    leaseKey: input.leaseKey,
    postconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_SETTLE_OPERATION_STEP_ID,
      "postcondition",
      input.descriptor.descriptorChecksum,
    ),
    preconditionChecksum: await valueChecksum(
      digest,
      input.descriptor.descriptorChecksum,
      input.sagaId,
      SAGA_SETTLE_OPERATION_STEP_ID,
      "precondition",
      input.inputChecksum,
    ),
    recoveryInstructions:
      "Settle only from a terminal checksum-verified saga projection after every conditional path is classified.",
    retryClassification: "never",
    stepId: SAGA_SETTLE_OPERATION_STEP_ID,
  })
  const actions: OperationStepPlanInput[] = []
  for (const step of input.descriptor.steps) {
    actions.push(await actionStep(input, step, "forward", digest))
    if (!step.irreversible) actions.push(await actionStep(input, step, "compensation", digest))
  }
  return sealOperationPlan(
    {
      capabilitySnapshotChecksum: input.capabilitySnapshotChecksum,
      idempotencyKey: input.operationIdempotencyKey,
      inputChecksum: input.inputChecksum,
      operationId: input.operationId,
      operationType: `saga:${input.descriptor.descriptorId}@${input.descriptor.version}`,
      steps: [init, settlement, termination, ...actions],
    },
    digest,
  )
}

export async function sealSagaOperationPlan(
  input: SealSagaOperationPlanInput,
  digest: DigestFunction,
): Promise<OperationPlan> {
  const snapshot = snapshotSagaOperationPlanInput(input)
  assertTrustedSagaHandlerRegistry(snapshot.registry)
  snapshot.registry.assertDescriptor(snapshot.descriptor)
  return bindSagaOperationPlan(await buildSagaOperationPlan(snapshot, digest), snapshot)
}

interface SagaPlanBindingSnapshot {
  readonly descriptor: SagaDescriptor
  readonly inputChecksum: string
  readonly sagaId: string
  readonly stepInputChecksums: Readonly<Record<string, string>>
}

function snapshotSagaPlanBinding(saga: SagaRecord): SagaPlanBindingSnapshot {
  const descriptor = saga.descriptor
  const inputChecksum = saga.inputChecksum
  const sagaId = saga.sagaId
  const steps = saga.steps
  const stepInputChecksums = Object.freeze(
    Object.fromEntries(
      descriptor.steps.map((step) => [step.stepId, steps[step.stepId]?.inputChecksum as string]),
    ),
  )
  return Object.freeze({ descriptor, inputChecksum, sagaId, stepInputChecksums })
}

export function assertTrustedSagaOperationPlan(plan: OperationPlan, saga: SagaRecord): void {
  let verified = false
  try {
    const snapshot = snapshotSagaPlanBinding(saga)
    assertTrustedSagaDescriptor(snapshot.descriptor)
    const identity = TRUSTED_SAGA_OPERATION_PLANS.get(plan)
    verified =
      identity !== undefined &&
      identity.descriptorChecksum === snapshot.descriptor.descriptorChecksum &&
      identity.inputChecksum === snapshot.inputChecksum &&
      identity.sagaId === snapshot.sagaId &&
      identity.stepInputChecksumsJson ===
        stepInputChecksumsJson(snapshot.descriptor, snapshot.stepInputChecksums)
  } catch {
    verified = false
  }
  if (!verified) intervention("The operation plan is not integrity-verified for this exact saga.")
}

export async function verifySagaOperationPlan(
  plan: OperationPlan,
  saga: SagaRecord,
  digest: DigestFunction,
): Promise<void> {
  let canonical = false
  try {
    createOperationRecord(plan)
    const snapshot = snapshotSagaPlanBinding(saga)
    const leaseKey = plan.steps[0]?.leaseKey as string
    const buildInput: BuildSagaOperationPlanInput = {
      capabilitySnapshotChecksum: plan.capabilitySnapshotChecksum,
      descriptor: snapshot.descriptor,
      inputChecksum: snapshot.inputChecksum,
      leaseKey,
      operationId: plan.operationId,
      operationIdempotencyKey: plan.idempotencyKey,
      sagaId: snapshot.sagaId,
      stepInputChecksums: snapshot.stepInputChecksums,
    }
    const expected = await buildSagaOperationPlan(buildInput, digest)
    canonical = expected.planChecksum === plan.planChecksum
    if (canonical) bindSagaOperationPlan(plan, buildInput)
  } catch {
    intervention("The saga operation plan could not be integrity-verified.")
  }
  if (!canonical) intervention("The operation plan contradicts the canonical saga plan.")
}
