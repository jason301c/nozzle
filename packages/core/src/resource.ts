import { NozzleError } from "./errors.js"

export const D1_RESOURCE_LIFECYCLES = [
  "planned",
  "registered",
  "ready",
  "quarantined",
  "retired",
  "deleted",
  "abandoned",
] as const

export type D1ResourceLifecycle = (typeof D1_RESOURCE_LIFECYCLES)[number]
export type D1ResourceJurisdiction = "eu" | "fedramp" | "global"

export interface D1ResourceIdentity {
  readonly creationOperationId: string
  readonly databaseName: string
  readonly desiredJurisdiction: D1ResourceJurisdiction
  readonly environmentId: string
  readonly fleetId: string
  readonly generationId: string
  readonly intentChecksum: string
  readonly resourceId: string
  readonly shardId: string
  readonly targetChecksum: string
}

export interface D1ResourceBinding {
  readonly attributionEvidenceChecksum: string
  readonly databaseId: string
  readonly databaseName: string
  readonly jurisdiction: D1ResourceJurisdiction
  readonly providerResultChecksum: string
}

export type D1ResourceObservationValue =
  | {
      readonly databaseId: string
      readonly databaseName: string
      readonly evidenceChecksum: string
      readonly jurisdiction: D1ResourceJurisdiction
      readonly observationOperationId: string
      readonly presence: "present"
    }
  | {
      readonly databaseId: string
      readonly evidenceChecksum: string
      readonly observationOperationId: string
      readonly presence: "absent"
    }

export type D1ResourceObservation = D1ResourceObservationValue & {
  readonly observedAtStateVersion: number
}

export type ObserveD1ResourceInput = D1ResourceObservationValue & {
  readonly expectedStateVersion: number
}

export interface D1ResourceRecord extends D1ResourceIdentity {
  readonly binding?: D1ResourceBinding
  readonly lastEvidenceChecksum: string
  readonly lastObservation?: D1ResourceObservation
  readonly lifecycle: D1ResourceLifecycle
  readonly stateVersion: number
}

export type D1ResourceLifecycleAction =
  | { readonly kind: "abandon" }
  | { readonly kind: "confirm_deleted" }
  | { readonly kind: "mark_ready" }
  | { readonly kind: "quarantine" }
  | { readonly kind: "recover_ready" }
  | { readonly kind: "recover_registered" }
  | { readonly kind: "retire" }

const D1_UUID_PATTERN = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/u

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function text(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.trim().length === 0) {
    configuration(`${label} must be non-empty.`)
  }
}

function jurisdiction(value: unknown, label: string): asserts value is D1ResourceJurisdiction {
  if (value !== "global" && value !== "eu" && value !== "fedramp") {
    configuration(`${label} is unsupported.`)
  }
}

function version(value: unknown): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    intervention("The persisted D1 resource state version is malformed.")
  }
}

function validateIdentity(input: D1ResourceIdentity): void {
  text(input.resourceId, "D1 resource ID")
  text(input.generationId, "D1 resource generation ID")
  text(input.fleetId, "D1 resource fleet ID")
  text(input.environmentId, "D1 resource environment ID")
  text(input.shardId, "D1 resource shard ID")
  text(input.targetChecksum, "D1 resource target checksum")
  text(input.creationOperationId, "D1 resource creation operation ID")
  text(input.intentChecksum, "D1 resource intent checksum")
  text(input.databaseName, "D1 resource database name")
  jurisdiction(input.desiredJurisdiction, "Desired D1 resource jurisdiction")
}

function validateBinding(binding: D1ResourceBinding, record: D1ResourceIdentity): void {
  text(binding.databaseId, "Bound D1 database ID")
  if (!D1_UUID_PATTERN.test(binding.databaseId)) {
    intervention("The bound D1 database ID is malformed.")
  }
  text(binding.databaseName, "Bound D1 database name")
  jurisdiction(binding.jurisdiction, "Bound D1 database jurisdiction")
  text(binding.attributionEvidenceChecksum, "D1 attribution evidence checksum")
  text(binding.providerResultChecksum, "D1 provider result checksum")
  if (
    binding.databaseName !== record.databaseName ||
    binding.jurisdiction !== record.desiredJurisdiction
  ) {
    intervention("The bound D1 database contradicts the immutable resource intent.")
  }
}

function validateObservation(
  observation: D1ResourceObservationValue,
  binding: D1ResourceBinding | undefined,
): void {
  if (binding === undefined) {
    intervention("A D1 resource observation cannot exist before provider registration.")
  }
  text(observation.databaseId, "Observed D1 database ID")
  if (!D1_UUID_PATTERN.test(observation.databaseId)) {
    intervention("The observed D1 database ID is malformed.")
  }
  if (observation.databaseId !== binding.databaseId) {
    intervention("The D1 resource observation belongs to a different provider database.")
  }
  text(observation.evidenceChecksum, "D1 observation evidence checksum")
  text(observation.observationOperationId, "D1 observation operation ID")
  if (observation.presence === "present") {
    text(observation.databaseName, "Observed D1 database name")
    jurisdiction(observation.jurisdiction, "Observed D1 database jurisdiction")
  } else if (observation.presence !== "absent") {
    intervention("The persisted D1 resource observation presence is unsupported.")
  }
}

function observationMatchesIntent(record: D1ResourceRecord): boolean {
  const observation = record.lastObservation
  return (
    observation?.presence === "present" &&
    observation.databaseName === record.databaseName &&
    observation.jurisdiction === record.desiredJurisdiction
  )
}

function validateRecord(record: D1ResourceRecord): void {
  validateIdentity(record)
  version(record.stateVersion)
  text(record.lastEvidenceChecksum, "D1 resource evidence checksum")
  if (!D1_RESOURCE_LIFECYCLES.includes(record.lifecycle)) {
    intervention("The persisted D1 resource lifecycle is unsupported.")
  }
  if (record.binding !== undefined) validateBinding(record.binding, record)
  if (record.lastObservation !== undefined) {
    validateObservation(record.lastObservation, record.binding)
    if (
      !Number.isSafeInteger(record.lastObservation.observedAtStateVersion) ||
      record.lastObservation.observedAtStateVersion < 1 ||
      record.lastObservation.observedAtStateVersion > record.stateVersion
    ) {
      intervention("The persisted D1 resource observation version is malformed.")
    }
  }
  if (
    (record.lifecycle === "registered" ||
      record.lifecycle === "ready" ||
      record.lifecycle === "retired" ||
      record.lifecycle === "deleted") &&
    record.binding === undefined
  ) {
    intervention("The persisted D1 resource lifecycle requires a provider binding.")
  }
  if (
    (record.lifecycle === "planned" || record.lifecycle === "abandoned") &&
    (record.binding !== undefined || record.lastObservation !== undefined)
  ) {
    intervention("An unmaterialized D1 resource cannot retain provider state.")
  }
  if (
    record.lastObservation?.presence === "absent" &&
    record.lifecycle !== "retired" &&
    record.lifecycle !== "deleted"
  ) {
    intervention("Authoritative D1 absence is attached to an invalid resource lifecycle.")
  }
  if (
    record.lifecycle === "ready" &&
    (!observationMatchesIntent(record) ||
      (record.lastObservation?.observedAtStateVersion !== record.stateVersion &&
        record.lastObservation?.observedAtStateVersion !== record.stateVersion - 1))
  ) {
    intervention("A ready D1 resource lacks a matching recent provider observation.")
  }
  if (
    record.lifecycle === "deleted" &&
    (record.lastObservation?.presence !== "absent" ||
      record.lastObservation.observedAtStateVersion !== record.stateVersion - 1)
  ) {
    intervention("A deleted D1 resource lacks authoritative absence evidence.")
  }
}

function increment(value: number): number {
  const next = value + 1
  if (!Number.isSafeInteger(next)) intervention("The D1 resource state version overflowed.")
  return next
}

function exactBinding(left: D1ResourceBinding, right: D1ResourceBinding): boolean {
  return (
    left.databaseId === right.databaseId &&
    left.databaseName === right.databaseName &&
    left.jurisdiction === right.jurisdiction &&
    left.attributionEvidenceChecksum === right.attributionEvidenceChecksum &&
    left.providerResultChecksum === right.providerResultChecksum
  )
}

function exactObservation(
  left: D1ResourceObservationValue,
  right: D1ResourceObservationValue,
): boolean {
  return (
    left.presence === right.presence &&
    left.databaseId === right.databaseId &&
    left.evidenceChecksum === right.evidenceChecksum &&
    left.observationOperationId === right.observationOperationId &&
    (left.presence === "absent" ||
      (right.presence === "present" &&
        left.databaseName === right.databaseName &&
        left.jurisdiction === right.jurisdiction))
  )
}

export function createD1ResourceRecord(input: D1ResourceIdentity): D1ResourceRecord {
  validateIdentity(input)
  return Object.freeze({
    ...input,
    lastEvidenceChecksum: input.intentChecksum,
    lifecycle: "planned",
    stateVersion: 0,
  })
}

export function registerD1Resource(
  record: D1ResourceRecord,
  input: D1ResourceBinding & { readonly expectedStateVersion: number },
): D1ResourceRecord {
  validateRecord(record)
  const requested: D1ResourceBinding = Object.freeze({
    attributionEvidenceChecksum: input.attributionEvidenceChecksum,
    databaseId: input.databaseId,
    databaseName: input.databaseName,
    jurisdiction: input.jurisdiction,
    providerResultChecksum: input.providerResultChecksum,
  })
  validateBinding(requested, record)
  if (record.binding !== undefined) {
    if (exactBinding(record.binding, requested)) return record
    return intervention("The D1 resource is already bound to contradictory provider identity.")
  }
  if (record.stateVersion !== input.expectedStateVersion) {
    return resume("The D1 resource registration was based on a stale state version.")
  }
  if (record.lifecycle !== "planned") {
    return resume("Only a planned D1 resource can register provider identity.")
  }
  return Object.freeze({
    ...record,
    binding: requested,
    lastEvidenceChecksum: requested.attributionEvidenceChecksum,
    lifecycle: "registered",
    stateVersion: increment(record.stateVersion),
  })
}

export function observeD1Resource(
  record: D1ResourceRecord,
  input: ObserveD1ResourceInput,
): D1ResourceRecord {
  validateRecord(record)
  const requested: D1ResourceObservationValue =
    input.presence === "present"
      ? Object.freeze({
          databaseId: input.databaseId,
          databaseName: input.databaseName,
          evidenceChecksum: input.evidenceChecksum,
          jurisdiction: input.jurisdiction,
          observationOperationId: input.observationOperationId,
          presence: input.presence,
        })
      : Object.freeze({
          databaseId: input.databaseId,
          evidenceChecksum: input.evidenceChecksum,
          observationOperationId: input.observationOperationId,
          presence: input.presence,
        })
  validateObservation(requested, record.binding)
  if (record.lastObservation !== undefined && exactObservation(record.lastObservation, requested)) {
    return record
  }
  if (record.stateVersion !== input.expectedStateVersion) {
    return resume("The D1 resource observation was based on a stale state version.")
  }
  if (
    record.lifecycle === "planned" ||
    record.lifecycle === "abandoned" ||
    record.lifecycle === "deleted"
  ) {
    return resume(`A ${record.lifecycle} D1 resource cannot accept a new provider observation.`)
  }
  if (requested.presence === "absent" && record.lifecycle !== "retired") {
    return resume("Authoritative D1 absence can be recorded only after resource retirement.")
  }
  const stateVersion = increment(record.stateVersion)
  const observation: D1ResourceObservation = Object.freeze({
    ...requested,
    observedAtStateVersion: stateVersion,
  })
  return Object.freeze({
    ...record,
    lastEvidenceChecksum: observation.evidenceChecksum,
    lastObservation: observation,
    stateVersion,
  })
}

function requiredLifecycle(
  current: D1ResourceLifecycle,
  allowed: readonly D1ResourceLifecycle[],
  action: string,
): void {
  if (!allowed.includes(current)) {
    resume(`D1 resource action ${action} is not valid from lifecycle ${current}.`)
  }
}

export function transitionD1Resource(
  record: D1ResourceRecord,
  input: {
    readonly action: D1ResourceLifecycleAction
    readonly evidenceChecksum: string
    readonly expectedStateVersion: number
  },
): D1ResourceRecord {
  validateRecord(record)
  text(input.evidenceChecksum, "D1 resource transition evidence checksum")
  if (record.stateVersion !== input.expectedStateVersion) {
    return resume("The D1 resource transition was based on a stale state version.")
  }
  let lifecycle: D1ResourceLifecycle
  switch (input.action.kind) {
    case "mark_ready":
      requiredLifecycle(record.lifecycle, ["registered"], input.action.kind)
      if (
        !observationMatchesIntent(record) ||
        record.lastObservation?.observedAtStateVersion !== record.stateVersion
      ) {
        return intervention("A D1 resource cannot become ready without matching provider evidence.")
      }
      lifecycle = "ready"
      break
    case "quarantine":
      requiredLifecycle(record.lifecycle, ["planned", "registered", "ready"], input.action.kind)
      lifecycle = "quarantined"
      break
    case "recover_registered":
      requiredLifecycle(record.lifecycle, ["quarantined"], input.action.kind)
      if (record.binding === undefined) {
        return intervention("An unbound quarantined D1 resource cannot recover as registered.")
      }
      lifecycle = "registered"
      break
    case "recover_ready":
      requiredLifecycle(record.lifecycle, ["quarantined"], input.action.kind)
      if (
        record.binding === undefined ||
        !observationMatchesIntent(record) ||
        record.lastObservation?.observedAtStateVersion !== record.stateVersion
      ) {
        return intervention("A quarantined D1 resource lacks matching recovery evidence.")
      }
      lifecycle = "ready"
      break
    case "retire":
      requiredLifecycle(record.lifecycle, ["quarantined"], input.action.kind)
      if (record.binding === undefined) {
        return intervention("An unbound quarantined D1 resource cannot be retired.")
      }
      lifecycle = "retired"
      break
    case "confirm_deleted":
      requiredLifecycle(record.lifecycle, ["retired"], input.action.kind)
      if (
        record.binding === undefined ||
        record.lastObservation?.presence !== "absent" ||
        record.lastObservation.observedAtStateVersion !== record.stateVersion
      ) {
        return intervention(
          "D1 deletion requires a retained binding and authoritative absence evidence.",
        )
      }
      lifecycle = "deleted"
      break
    case "abandon":
      requiredLifecycle(record.lifecycle, ["planned"], input.action.kind)
      lifecycle = "abandoned"
      break
  }
  return Object.freeze({
    ...record,
    lastEvidenceChecksum: input.evidenceChecksum,
    lifecycle,
    stateVersion: increment(record.stateVersion),
  })
}
