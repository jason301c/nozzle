import { NozzleError } from "./errors.js"
import type { DigestFunction } from "./operation.js"

export const SAGA_DESCRIPTOR_SCHEMA_VERSION = 1 as const
export const MAX_SAGA_STEPS = 256

const DESCRIPTOR_DOMAIN = "nozzle.saga-descriptor.v1"
const MAX_ID_BYTES = 255
const MAX_RETRY_ATTEMPTS = 20
const MAX_DURATION_MS = 24 * 60 * 60 * 1_000
const CHECKSUM = /^[0-9a-f]{64}$/u
const TRUSTED_DESCRIPTORS = new WeakSet<SagaDescriptor>()
const UTF8 = new TextEncoder()

export interface SagaActionReference {
  readonly actionId: string
  readonly artifactChecksum: string
  readonly version: number
}

export interface SagaStepDescriptorInput {
  readonly authorizationPolicyChecksum: string | null
  readonly baseRetryDelayMs: number
  readonly compensationAction: SagaActionReference | null
  readonly compensationObservation: SagaActionReference | null
  readonly forwardAction: SagaActionReference
  readonly forwardObservation: SagaActionReference
  readonly inputSchemaChecksum: string
  readonly irreversible: boolean
  readonly maxAttempts: number
  readonly maxRetryDelayMs: number
  readonly outputSchemaChecksum: string
  readonly stepId: string
  readonly timeoutMs: number
}

export interface SagaDescriptorInput {
  readonly descriptorId: string
  readonly steps: readonly SagaStepDescriptorInput[]
  readonly version: number
}

export interface SagaStepDescriptor extends SagaStepDescriptorInput {
  readonly compensationAction: SagaActionReference | null
  readonly compensationObservation: SagaActionReference | null
  readonly forwardAction: SagaActionReference
  readonly forwardObservation: SagaActionReference
}

export interface SagaDescriptor {
  readonly cancellationMode: "compensate_confirmed"
  readonly descriptorChecksum: string
  readonly descriptorId: string
  readonly executionMode: "serial"
  readonly schemaVersion: typeof SAGA_DESCRIPTOR_SCHEMA_VERSION
  readonly steps: readonly SagaStepDescriptor[]
  readonly version: number
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function exactRecord(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) return false
  const actual = Object.keys(value)
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key))
}

function isWellFormedText(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false
    }
  }
  return true
}

function hasAsciiControl(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code <= 0x1f || code === 0x7f) return true
  }
  return false
}

function validId(value: unknown, label: string): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.trim() !== value ||
    !isWellFormedText(value) ||
    hasAsciiControl(value) ||
    UTF8.encode(value).byteLength > MAX_ID_BYTES
  ) {
    configuration(`${label} is malformed.`)
  }
}

function validChecksum(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
  }
}

function integer(
  value: unknown,
  label: string,
  minimum: number,
  maximum: number,
): asserts value is number {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    configuration(`${label} must be an integer between ${minimum} and ${maximum}.`)
  }
}

function actionReference(value: unknown, label: string): SagaActionReference {
  if (!exactRecord(value, ["actionId", "artifactChecksum", "version"])) {
    configuration(`${label} is malformed.`)
  }
  validId(value.actionId, `${label} action ID`)
  validChecksum(value.artifactChecksum, `${label} artifact checksum`)
  integer(value.version, `${label} version`, 1, Number.MAX_SAFE_INTEGER)
  return Object.freeze({
    actionId: value.actionId,
    artifactChecksum: value.artifactChecksum,
    version: value.version,
  })
}

function stepDescriptor(value: unknown, index: number, finalIndex: number): SagaStepDescriptor {
  if (
    !exactRecord(value, [
      "authorizationPolicyChecksum",
      "baseRetryDelayMs",
      "compensationAction",
      "compensationObservation",
      "forwardAction",
      "forwardObservation",
      "inputSchemaChecksum",
      "irreversible",
      "maxAttempts",
      "maxRetryDelayMs",
      "outputSchemaChecksum",
      "stepId",
      "timeoutMs",
    ])
  ) {
    configuration("Saga step descriptor fields are malformed.")
  }
  validId(value.stepId, "Saga step ID")
  validChecksum(value.inputSchemaChecksum, "Saga step input schema checksum")
  validChecksum(value.outputSchemaChecksum, "Saga step output schema checksum")
  if (typeof value.irreversible !== "boolean")
    configuration("Saga step reversibility is malformed.")
  integer(value.maxAttempts, "Saga step maximum attempts", 1, MAX_RETRY_ATTEMPTS)
  integer(value.baseRetryDelayMs, "Saga step base retry delay", 0, MAX_DURATION_MS)
  integer(value.maxRetryDelayMs, "Saga step maximum retry delay", 0, MAX_DURATION_MS)
  integer(value.timeoutMs, "Saga step timeout", 1, MAX_DURATION_MS)
  if (value.baseRetryDelayMs > value.maxRetryDelayMs) {
    configuration("Saga step base retry delay cannot exceed its maximum retry delay.")
  }
  const forwardAction = actionReference(value.forwardAction, "Saga forward action")
  const forwardObservation = actionReference(value.forwardObservation, "Saga forward observation")
  let compensationAction: SagaActionReference | null = null
  let compensationObservation: SagaActionReference | null = null
  let authorizationPolicyChecksum: string | null = null
  if (value.irreversible) {
    if (index !== finalIndex) configuration("An irreversible saga step must be last.")
    if (value.compensationAction !== null || value.compensationObservation !== null) {
      configuration("An irreversible saga step cannot declare compensation actions.")
    }
    validChecksum(
      value.authorizationPolicyChecksum,
      "Irreversible saga authorization policy checksum",
    )
    authorizationPolicyChecksum = value.authorizationPolicyChecksum
  } else {
    if (value.authorizationPolicyChecksum !== null) {
      configuration("A reversible saga step cannot declare irreversible authorization policy.")
    }
    compensationAction = actionReference(value.compensationAction, "Saga compensation action")
    compensationObservation = actionReference(
      value.compensationObservation,
      "Saga compensation observation",
    )
  }
  return Object.freeze({
    authorizationPolicyChecksum,
    baseRetryDelayMs: value.baseRetryDelayMs,
    compensationAction,
    compensationObservation,
    forwardAction,
    forwardObservation,
    inputSchemaChecksum: value.inputSchemaChecksum,
    irreversible: value.irreversible,
    maxAttempts: value.maxAttempts,
    maxRetryDelayMs: value.maxRetryDelayMs,
    outputSchemaChecksum: value.outputSchemaChecksum,
    stepId: value.stepId,
    timeoutMs: value.timeoutMs,
  })
}

function normalizedInput(candidate: unknown): SagaDescriptorInput {
  if (!exactRecord(candidate, ["descriptorId", "steps", "version"])) {
    configuration("Saga descriptor fields are malformed.")
  }
  validId(candidate.descriptorId, "Saga descriptor ID")
  integer(candidate.version, "Saga descriptor version", 1, Number.MAX_SAFE_INTEGER)
  if (
    !Array.isArray(candidate.steps) ||
    candidate.steps.length < 1 ||
    candidate.steps.length > MAX_SAGA_STEPS
  ) {
    configuration(`A saga descriptor requires between 1 and ${MAX_SAGA_STEPS} steps.`)
  }
  const finalIndex = candidate.steps.length - 1
  const steps = candidate.steps.map((step, index) => stepDescriptor(step, index, finalIndex))
  const stepIds = new Set<string>()
  for (const step of steps) {
    if (stepIds.has(step.stepId)) configuration("Saga step IDs must be unique.")
    stepIds.add(step.stepId)
  }
  return Object.freeze({
    descriptorId: candidate.descriptorId,
    steps: Object.freeze(steps),
    version: candidate.version,
  })
}

export function encodeSagaDescriptorChecksumInput(input: SagaDescriptorInput): Uint8Array {
  const normalized = normalizedInput(input)
  return UTF8.encode(
    JSON.stringify({
      cancellationMode: "compensate_confirmed",
      descriptorId: normalized.descriptorId,
      domain: DESCRIPTOR_DOMAIN,
      executionMode: "serial",
      schemaVersion: SAGA_DESCRIPTOR_SCHEMA_VERSION,
      steps: normalized.steps,
      version: normalized.version,
    }),
  )
}

async function descriptorChecksum(
  input: SagaDescriptorInput,
  digest: DigestFunction,
): Promise<string> {
  if (typeof digest !== "function") configuration("A saga descriptor digest function is required.")
  const bytes = encodeSagaDescriptorChecksumInput(input)
  const owned = new Uint8Array(bytes.byteLength)
  owned.set(bytes)
  const checksum = await digest(owned)
  validChecksum(checksum, "Saga descriptor checksum")
  return checksum
}

export async function sealSagaDescriptor(
  input: SagaDescriptorInput,
  digest: DigestFunction,
): Promise<SagaDescriptor> {
  const normalized = normalizedInput(input)
  const checksum = await descriptorChecksum(normalized, digest)
  const descriptor = Object.freeze({
    cancellationMode: "compensate_confirmed" as const,
    descriptorChecksum: checksum,
    descriptorId: normalized.descriptorId,
    executionMode: "serial" as const,
    schemaVersion: SAGA_DESCRIPTOR_SCHEMA_VERSION,
    steps: normalized.steps as readonly SagaStepDescriptor[],
    version: normalized.version,
  })
  TRUSTED_DESCRIPTORS.add(descriptor)
  return descriptor
}

export async function loadSagaDescriptor(
  candidate: unknown,
  digest: DigestFunction,
): Promise<SagaDescriptor> {
  if (
    !exactRecord(candidate, [
      "cancellationMode",
      "descriptorChecksum",
      "descriptorId",
      "executionMode",
      "schemaVersion",
      "steps",
      "version",
    ]) ||
    candidate.cancellationMode !== "compensate_confirmed" ||
    candidate.executionMode !== "serial" ||
    candidate.schemaVersion !== SAGA_DESCRIPTOR_SCHEMA_VERSION
  ) {
    return intervention("Persisted saga descriptor envelope is malformed.")
  }
  let descriptor: SagaDescriptor
  try {
    descriptor = await sealSagaDescriptor(
      {
        descriptorId: candidate.descriptorId as string,
        steps: candidate.steps as readonly SagaStepDescriptorInput[],
        version: candidate.version as number,
      },
      digest,
    )
  } catch {
    return intervention("Persisted saga descriptor body is malformed.")
  }
  if (candidate.descriptorChecksum !== descriptor.descriptorChecksum) {
    return intervention("Persisted saga descriptor checksum does not match its body.")
  }
  return descriptor
}

export async function verifySagaDescriptorChecksum(
  descriptor: SagaDescriptor,
  digest: DigestFunction,
): Promise<boolean> {
  try {
    const expected = await descriptorChecksum(
      {
        descriptorId: descriptor.descriptorId,
        steps: descriptor.steps,
        version: descriptor.version,
      },
      digest,
    )
    return expected === descriptor.descriptorChecksum
  } catch {
    return false
  }
}

export function assertTrustedSagaDescriptor(descriptor: SagaDescriptor): void {
  if (!TRUSTED_DESCRIPTORS.has(descriptor)) {
    configuration("Saga descriptor must be sealed or loaded before use.")
  }
}

export function sagaActionKey(reference: SagaActionReference): string {
  const action = actionReference(reference, "Saga action reference")
  return `${action.actionId}@${action.version}:${action.artifactChecksum}`
}
