import {
  assertTrustedSagaDescriptor,
  type DigestFunction,
  NozzleError,
  type SagaDescriptor,
} from "@nozzle/core"
import { sagaActionInputChecksum } from "./saga-attempt-store.js"

const MAX_INPUT_BYTES = 1024 * 1024
const MAX_IDENTITY_BYTES = 512
const CHECKSUM = /^[0-9a-f]{64}$/u

export interface SagaInvocationInput {
  readonly descriptorChecksum: string
  readonly descriptorId: string
  readonly descriptorVersion: number
  readonly inputChecksum: string
  readonly inputJson: string
  readonly operationInputJson: string
  readonly sagaId: string
  readonly schemaVersion: 1
  readonly stepInputChecksums: Readonly<Record<string, string>>
  readonly stepInputJsons: Readonly<Record<string, string>>
}

export interface SealSagaInvocationInput {
  readonly descriptor: SagaDescriptor
  readonly inputJson: string
  readonly sagaId: string
  readonly stepInputJsons: Readonly<Record<string, string>>
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (!plainRecord(value)) return value
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value).sort()) output[key] = canonicalValue(value[key])
  return output
}

function boundedIdentity(value: unknown, label: string, persisted: boolean): string {
  const fail = persisted ? intervention : configuration
  if (typeof value !== "string" || value.trim().length === 0) {
    return fail(`${label} must be non-empty.`)
  }
  if (new TextEncoder().encode(value).byteLength > MAX_IDENTITY_BYTES) {
    return fail(`${label} exceeds the durable saga identity limit.`)
  }
  return value
}

function canonicalJson(
  value: unknown,
  label: string,
  persisted: boolean,
): {
  readonly json: string
  readonly value: unknown
} {
  const fail = persisted ? intervention : configuration
  if (typeof value !== "string" || value.length === 0) return fail(`${label} must be JSON text.`)
  if (new TextEncoder().encode(value).byteLength > MAX_INPUT_BYTES) {
    return fail(`${label} exceeds the one MiB durable limit.`)
  }
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return fail(`${label} is not valid JSON.`)
  }
  let json: string
  let canonical: unknown
  try {
    canonical = canonicalValue(parsed)
    json = JSON.stringify(canonical)
  } catch {
    return fail(`${label} cannot be canonicalized safely.`)
  }
  if (persisted && json !== value) return intervention(`${label} is not canonical.`)
  return Object.freeze({ json, value: canonical })
}

function exactStepIds(
  descriptor: SagaDescriptor,
  value: unknown,
  persisted: boolean,
): asserts value is Record<string, unknown> {
  const fail = persisted ? intervention : configuration
  if (!plainRecord(value)) return fail("Saga step inputs are malformed.")
  const expected = descriptor.steps.map((step) => step.stepId).sort()
  const actual = Object.keys(value).sort()
  if (
    expected.length !== actual.length ||
    expected.some((stepId, index) => stepId !== actual[index])
  ) {
    return fail("Saga step inputs do not match the descriptor.")
  }
}

async function digestJson(json: string, digest: DigestFunction): Promise<string> {
  const value = await digest(new TextEncoder().encode(json))
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration("Saga invocation digest must return a lowercase SHA-256 checksum.")
  }
  return value
}

async function materialize(
  descriptor: SagaDescriptor,
  envelope: {
    readonly descriptorChecksum: string
    readonly descriptorId: string
    readonly descriptorVersion: number
    readonly input: unknown
    readonly sagaId: string
    readonly schemaVersion: 1
    readonly stepInputs: Record<string, unknown>
  },
  digest: DigestFunction,
): Promise<SagaInvocationInput> {
  const stepInputJsons: Record<string, string> = {}
  const stepInputChecksums: Record<string, string> = {}
  for (const step of descriptor.steps) {
    const json = JSON.stringify(canonicalValue(envelope.stepInputs[step.stepId]))
    stepInputJsons[step.stepId] = json
    stepInputChecksums[step.stepId] = await sagaActionInputChecksum(json, digest)
  }
  const operationInputJson = JSON.stringify(canonicalValue(envelope))
  if (new TextEncoder().encode(operationInputJson).byteLength > MAX_INPUT_BYTES) {
    configuration("Saga invocation envelope exceeds the one MiB operation limit.")
  }
  return Object.freeze({
    descriptorChecksum: descriptor.descriptorChecksum,
    descriptorId: descriptor.descriptorId,
    descriptorVersion: descriptor.version,
    inputChecksum: await digestJson(operationInputJson, digest),
    inputJson: JSON.stringify(canonicalValue(envelope.input)),
    operationInputJson,
    sagaId: envelope.sagaId,
    schemaVersion: 1,
    stepInputChecksums: Object.freeze(stepInputChecksums),
    stepInputJsons: Object.freeze(stepInputJsons),
  })
}

export async function sealSagaInvocationInput(
  input: SealSagaInvocationInput,
  digest: DigestFunction,
): Promise<SagaInvocationInput> {
  if (typeof digest !== "function") configuration("A saga invocation digest is required.")
  assertTrustedSagaDescriptor(input.descriptor)
  const sagaId = boundedIdentity(input.sagaId, "Saga ID", false)
  const invocation = canonicalJson(input.inputJson, "Saga input", false)
  exactStepIds(input.descriptor, input.stepInputJsons, false)
  const stepInputs: Record<string, unknown> = {}
  for (const step of input.descriptor.steps) {
    stepInputs[step.stepId] = canonicalJson(
      input.stepInputJsons[step.stepId],
      `Saga step ${step.stepId} input`,
      false,
    ).value
  }
  return materialize(
    input.descriptor,
    {
      descriptorChecksum: input.descriptor.descriptorChecksum,
      descriptorId: input.descriptor.descriptorId,
      descriptorVersion: input.descriptor.version,
      input: invocation.value,
      sagaId,
      schemaVersion: 1,
      stepInputs,
    },
    digest,
  )
}

export async function loadSagaInvocationInput(
  operationInputJson: string,
  descriptor: SagaDescriptor,
  digest: DigestFunction,
): Promise<SagaInvocationInput> {
  if (typeof digest !== "function") configuration("A saga invocation digest is required.")
  assertTrustedSagaDescriptor(descriptor)
  const persisted = canonicalJson(operationInputJson, "Persisted saga invocation", true)
  if (
    !plainRecord(persisted.value) ||
    Object.keys(persisted.value).length !== 7 ||
    ![
      "descriptorChecksum",
      "descriptorId",
      "descriptorVersion",
      "input",
      "sagaId",
      "schemaVersion",
      "stepInputs",
    ].every((key) => Object.hasOwn(persisted.value as Record<string, unknown>, key))
  ) {
    return intervention("Persisted saga invocation envelope is malformed.")
  }
  const envelope = persisted.value
  if (
    envelope.schemaVersion !== 1 ||
    envelope.descriptorChecksum !== descriptor.descriptorChecksum ||
    envelope.descriptorId !== descriptor.descriptorId ||
    envelope.descriptorVersion !== descriptor.version
  ) {
    return intervention("Persisted saga invocation contradicts its descriptor.")
  }
  const sagaId = boundedIdentity(envelope.sagaId, "Persisted saga ID", true)
  exactStepIds(descriptor, envelope.stepInputs, true)
  return materialize(
    descriptor,
    {
      descriptorChecksum: descriptor.descriptorChecksum,
      descriptorId: descriptor.descriptorId,
      descriptorVersion: descriptor.version,
      input: envelope.input,
      sagaId,
      schemaVersion: 1,
      stepInputs: envelope.stepInputs,
    },
    digest,
  )
}
