import {
  assertTrustedSagaDescriptor,
  type DigestFunction,
  type LeaseProof,
  NozzleError,
  type SagaActionPhase,
  type SagaActionReference,
  type SagaDescriptor,
  sagaActionKey,
} from "@nozzle/core"

const REGISTRY_DOMAIN = "nozzle.saga-handler-registry.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const MAX_HANDLERS = 2_048
const TRUSTED_REGISTRIES = new WeakSet<SagaHandlerRegistry>()
const REGISTRY_TOKEN = Symbol("nozzle.saga-handler-registry")

export interface SagaHandlerRequest {
  readonly action: SagaActionReference
  readonly attemptId: string
  readonly idempotencyKey: string
  readonly inputJson: string
  readonly operationId: string
  readonly phase: SagaActionPhase
  readonly proof: LeaseProof
  readonly sagaId: string
  readonly signal: AbortSignal
  readonly stepId: string
  readonly timeoutMs: number
}

export interface SagaObservationHandlerRequest extends SagaHandlerRequest {
  readonly effectAttemptId: string
  readonly effectErrorJson: string
  readonly effectIdempotencyKey: string
}

export type SagaEffectHandlerResult =
  | { readonly evidenceJson: string; readonly outputJson: string; readonly state: "confirmed" }
  | {
      readonly errorJson: string
      readonly evidenceJson: string
      readonly state:
        | "definitely_not_applied_retryable"
        | "definitely_not_applied_terminal"
        | "unknown"
    }

export type SagaObservationHandlerResult =
  | { readonly evidenceJson: string; readonly outputJson: string; readonly state: "applied" }
  | {
      readonly errorJson: string
      readonly evidenceJson: string
      readonly state: "indeterminate" | "not_applied"
    }

export type SagaEffectHandler = (
  request: SagaHandlerRequest,
) => Promise<SagaEffectHandlerResult> | SagaEffectHandlerResult

export type SagaObservationHandler = (
  request: SagaObservationHandlerRequest,
) => Promise<SagaObservationHandlerResult> | SagaObservationHandlerResult

export type SagaHandlerRegistration =
  | {
      readonly handler: SagaEffectHandler
      readonly kind: "effect"
      readonly reference: SagaActionReference
    }
  | {
      readonly handler: SagaObservationHandler
      readonly kind: "observation"
      readonly reference: SagaActionReference
    }

export interface SagaHandlerManifestEntry {
  readonly actionId: string
  readonly actionKey: string
  readonly artifactChecksum: string
  readonly kind: "effect" | "observation"
  readonly version: number
}

export interface SagaHandlerManifest {
  readonly handlers: readonly SagaHandlerManifestEntry[]
  readonly manifestChecksum: string
  readonly schemaVersion: 1
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function checkedChecksum(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
  }
}

function identity(reference: SagaActionReference): string {
  return `${reference.actionId}@${reference.version}`
}

function manifestBytes(entries: readonly SagaHandlerManifestEntry[]): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify({ domain: REGISTRY_DOMAIN, handlers: entries, schemaVersion: 1 }),
  )
}

function entry(registration: SagaHandlerRegistration, actionKey: string): SagaHandlerManifestEntry {
  return Object.freeze({
    actionId: registration.reference.actionId,
    actionKey,
    artifactChecksum: registration.reference.artifactChecksum,
    kind: registration.kind,
    version: registration.reference.version,
  })
}

export class SagaHandlerRegistry {
  readonly #effectHandlers: ReadonlyMap<string, SagaEffectHandler>
  readonly #observationHandlers: ReadonlyMap<string, SagaObservationHandler>
  readonly manifest: SagaHandlerManifest

  constructor(
    token: symbol,
    effectHandlers: ReadonlyMap<string, SagaEffectHandler>,
    observationHandlers: ReadonlyMap<string, SagaObservationHandler>,
    manifest: SagaHandlerManifest,
  ) {
    if (token !== REGISTRY_TOKEN)
      configuration("A saga handler registry cannot be constructed directly.")
    this.#effectHandlers = effectHandlers
    this.#observationHandlers = observationHandlers
    this.manifest = manifest
    Object.freeze(this)
  }

  effect(reference: SagaActionReference): SagaEffectHandler {
    assertTrustedSagaHandlerRegistry(this)
    const handler = this.#effectHandlers.get(sagaActionKey(reference))
    if (handler === undefined) configuration("The saga effect handler is not registered.")
    return handler
  }

  observation(reference: SagaActionReference): SagaObservationHandler {
    assertTrustedSagaHandlerRegistry(this)
    const handler = this.#observationHandlers.get(sagaActionKey(reference))
    if (handler === undefined) configuration("The saga observation handler is not registered.")
    return handler
  }

  assertDescriptor(descriptor: SagaDescriptor): void {
    assertTrustedSagaHandlerRegistry(this)
    assertTrustedSagaDescriptor(descriptor)
    for (const step of descriptor.steps) {
      this.effect(step.forwardAction)
      this.observation(step.forwardObservation)
      if (step.compensationAction !== null) this.effect(step.compensationAction)
      if (step.compensationObservation !== null) this.observation(step.compensationObservation)
    }
  }
}

export function assertTrustedSagaHandlerRegistry(
  registry: SagaHandlerRegistry,
): asserts registry is SagaHandlerRegistry {
  if (!TRUSTED_REGISTRIES.has(registry)) {
    configuration("The saga handler registry must be checksummed before use.")
  }
}

export async function sealSagaHandlerRegistry(
  registrations: readonly SagaHandlerRegistration[],
  digest: DigestFunction,
): Promise<SagaHandlerRegistry> {
  if (
    !Array.isArray(registrations) ||
    registrations.length < 1 ||
    registrations.length > MAX_HANDLERS
  ) {
    configuration(`A saga handler registry requires between 1 and ${MAX_HANDLERS} handlers.`)
  }
  if (typeof digest !== "function") configuration("A saga registry digest is required.")
  const effectHandlers = new Map<string, SagaEffectHandler>()
  const observationHandlers = new Map<string, SagaObservationHandler>()
  const identities = new Set<string>()
  const entries: SagaHandlerManifestEntry[] = []
  for (const registration of registrations) {
    if (
      (registration.kind !== "effect" && registration.kind !== "observation") ||
      typeof registration.handler !== "function"
    ) {
      configuration("A saga handler registration is malformed.")
    }
    const actionKey = sagaActionKey(registration.reference)
    const versionIdentity = identity(registration.reference)
    if (identities.has(versionIdentity)) {
      configuration("A saga action ID and version must identify exactly one registered artifact.")
    }
    identities.add(versionIdentity)
    if (registration.kind === "effect") effectHandlers.set(actionKey, registration.handler)
    else observationHandlers.set(actionKey, registration.handler)
    entries.push(entry(registration, actionKey))
  }
  entries.sort((left, right) => (left.actionKey < right.actionKey ? -1 : 1))
  const frozenEntries = Object.freeze(entries)
  const checksum = await digest(manifestBytes(frozenEntries).slice())
  checkedChecksum(checksum, "Saga handler manifest checksum")
  const manifest: SagaHandlerManifest = Object.freeze({
    handlers: frozenEntries,
    manifestChecksum: checksum,
    schemaVersion: 1,
  })
  const registry = new SagaHandlerRegistry(
    REGISTRY_TOKEN,
    effectHandlers,
    observationHandlers,
    manifest,
  )
  TRUSTED_REGISTRIES.add(registry)
  return registry
}
