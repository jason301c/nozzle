import { NozzleError } from "./errors.js"

export const D1_RESOURCE_LIFECYCLES = [
  "planned",
  "provisioning",
  "registered",
  "active",
  "quarantined",
  "retired",
  "deleting",
  "deleted",
  "intervention_required",
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

export interface D1ResourceRecord extends D1ResourceIdentity {
  readonly binding?: D1ResourceBinding
  readonly lastEvidenceChecksum: string
  readonly lifecycle: D1ResourceLifecycle
  readonly stateVersion: number
}

export type D1ResourceLifecycleAction =
  | { readonly kind: "activate" }
  | { readonly kind: "begin_delete" }
  | { readonly kind: "begin_provisioning" }
  | { readonly kind: "confirm_deleted" }
  | { readonly kind: "intervene" }
  | { readonly kind: "quarantine" }
  | { readonly kind: "register" }
  | { readonly kind: "retire" }

const D1_UUID_PATTERN = /^[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}$/u

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

function validateRecord(record: D1ResourceRecord): void {
  validateIdentity(record)
  version(record.stateVersion)
  text(record.lastEvidenceChecksum, "D1 resource evidence checksum")
  if (!D1_RESOURCE_LIFECYCLES.includes(record.lifecycle)) {
    intervention("The persisted D1 resource lifecycle is unsupported.")
  }
  if (record.binding !== undefined) {
    text(record.binding.databaseId, "Bound D1 database ID")
    if (!D1_UUID_PATTERN.test(record.binding.databaseId)) {
      intervention("The bound D1 database ID is malformed.")
    }
    text(record.binding.databaseName, "Bound D1 database name")
    jurisdiction(record.binding.jurisdiction, "Bound D1 database jurisdiction")
    text(record.binding.attributionEvidenceChecksum, "D1 attribution evidence checksum")
    text(record.binding.providerResultChecksum, "D1 provider result checksum")
    if (
      record.binding.databaseName !== record.databaseName ||
      record.binding.jurisdiction !== record.desiredJurisdiction
    ) {
      intervention("The bound D1 database contradicts the immutable resource intent.")
    }
  }
  if (
    (record.lifecycle === "registered" ||
      record.lifecycle === "active" ||
      record.lifecycle === "retired" ||
      record.lifecycle === "deleting" ||
      record.lifecycle === "deleted") &&
    record.binding === undefined
  ) {
    intervention("The persisted D1 resource lifecycle requires a provider binding.")
  }
}

function increment(value: number): number {
  const next = value + 1
  if (!Number.isSafeInteger(next)) intervention("The D1 resource state version overflowed.")
  return next
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

export function bindD1Resource(
  record: D1ResourceRecord,
  input: {
    readonly attributionEvidenceChecksum: string
    readonly databaseId: string
    readonly databaseName: string
    readonly expectedStateVersion: number
    readonly jurisdiction: D1ResourceJurisdiction
    readonly providerResultChecksum: string
  },
): D1ResourceRecord {
  validateRecord(record)
  text(input.databaseId, "Bound D1 database ID")
  if (!D1_UUID_PATTERN.test(input.databaseId)) {
    return configuration("Bound D1 database ID must use canonical UUID form.")
  }
  text(input.databaseName, "Bound D1 database name")
  jurisdiction(input.jurisdiction, "Bound D1 database jurisdiction")
  text(input.attributionEvidenceChecksum, "D1 attribution evidence checksum")
  text(input.providerResultChecksum, "D1 provider result checksum")
  const requested = Object.freeze({
    attributionEvidenceChecksum: input.attributionEvidenceChecksum,
    databaseId: input.databaseId,
    databaseName: input.databaseName,
    jurisdiction: input.jurisdiction,
    providerResultChecksum: input.providerResultChecksum,
  })
  if (record.binding !== undefined) {
    if (
      record.binding.databaseId === requested.databaseId &&
      record.binding.databaseName === requested.databaseName &&
      record.binding.jurisdiction === requested.jurisdiction &&
      record.binding.attributionEvidenceChecksum === requested.attributionEvidenceChecksum &&
      record.binding.providerResultChecksum === requested.providerResultChecksum
    ) {
      return record
    }
    return intervention("The D1 resource is already bound to contradictory provider identity.")
  }
  if (record.stateVersion !== input.expectedStateVersion) {
    return resume("The D1 resource binding was based on a stale state version.")
  }
  if (record.lifecycle !== "provisioning") {
    return resume("A D1 resource can be bound only while provisioning.")
  }
  if (
    requested.databaseName !== record.databaseName ||
    requested.jurisdiction !== record.desiredJurisdiction
  ) {
    return intervention("The observed D1 resource does not match the immutable creation intent.")
  }
  return Object.freeze({
    ...record,
    binding: requested,
    lastEvidenceChecksum: input.attributionEvidenceChecksum,
    stateVersion: increment(record.stateVersion),
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
    case "begin_provisioning":
      requiredLifecycle(record.lifecycle, ["planned"], input.action.kind)
      lifecycle = "provisioning"
      break
    case "register":
      requiredLifecycle(record.lifecycle, ["provisioning"], input.action.kind)
      if (record.binding === undefined) {
        return intervention("A D1 resource cannot register without verified provider identity.")
      }
      lifecycle = "registered"
      break
    case "activate":
      requiredLifecycle(record.lifecycle, ["registered"], input.action.kind)
      lifecycle = "active"
      break
    case "quarantine":
      requiredLifecycle(
        record.lifecycle,
        ["planned", "provisioning", "registered", "active"],
        input.action.kind,
      )
      lifecycle = "quarantined"
      break
    case "retire":
      requiredLifecycle(record.lifecycle, ["quarantined"], input.action.kind)
      lifecycle = "retired"
      break
    case "begin_delete":
      requiredLifecycle(record.lifecycle, ["retired"], input.action.kind)
      lifecycle = "deleting"
      break
    case "confirm_deleted":
      requiredLifecycle(record.lifecycle, ["deleting"], input.action.kind)
      lifecycle = "deleted"
      break
    case "intervene":
      requiredLifecycle(
        record.lifecycle,
        ["planned", "provisioning", "registered", "active", "quarantined", "retired", "deleting"],
        input.action.kind,
      )
      lifecycle = "intervention_required"
      break
  }
  return Object.freeze({
    ...record,
    lastEvidenceChecksum: input.evidenceChecksum,
    lifecycle,
    stateVersion: increment(record.stateVersion),
  })
}
