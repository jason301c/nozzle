import { NozzleError } from "./errors.js"

const OPERATION_PLAN_DOMAIN = "nozzle.operation-plan.v1"
const IRREVERSIBLE_AUTHORIZATION_DOMAIN = "nozzle.irreversible-authorization.v1"
const AUDIT_EVENT_DOMAIN = "nozzle.audit-event.v1"
const TRUSTED_OPERATION_PLANS = new WeakSet<OperationPlan>()
const TRUSTED_IRREVERSIBLE_AUTHORIZATIONS = new WeakSet<IrreversibleAuthorization>()
const OPERATION_PLAN_INPUT_KEYS = new Set([
  "capabilitySnapshotChecksum",
  "idempotencyKey",
  "inputChecksum",
  "operationId",
  "operationType",
  "steps",
])
const PERSISTED_OPERATION_PLAN_KEYS = new Set([
  ...OPERATION_PLAN_INPUT_KEYS,
  "planChecksum",
  "schemaVersion",
])
const OPERATION_STEP_PLAN_INPUT_KEYS = new Set([
  "activation",
  "checkpoint",
  "completionRole",
  "dependsOn",
  "effectProtocol",
  "idempotencyKey",
  "inputChecksum",
  "leaseKey",
  "postconditionChecksum",
  "preconditionChecksum",
  "recoveryInstructions",
  "retryClassification",
  "stepId",
])
const IRREVERSIBLE_AUTHORIZATION_KEYS = new Set([
  "actorChecksum",
  "authorizationChecksum",
  "authorizationId",
  "decisionChecksum",
  "fencingToken",
  "holderId",
  "leaseAcquisitionId",
  "leaseKey",
  "operationId",
  "planChecksum",
  "sealedAtServerTimeMs",
  "schemaVersion",
  "stepId",
  "stepInputChecksum",
])
const IRREVERSIBLE_AUTHORIZATION_INPUT_KEYS = new Set([
  "actorChecksum",
  "authorizationId",
  "decisionChecksum",
  "lease",
  "leaseProof",
  "sealedAtServerTimeMs",
  "stepId",
])
const AUDIT_EVENT_INPUT_KEYS = new Set([
  "actorChecksum",
  "environmentId",
  "eventType",
  "fencingToken",
  "idempotencyKey",
  "operationId",
  "payloadChecksum",
  "serverTimeMs",
  "stepId",
])
const FENCED_LEASE_RECORD_KEYS = new Set([
  "acquisitionId",
  "expiresAtServerTimeMs",
  "fencingToken",
  "holderId",
  "leaseKey",
])
const LEASE_PROOF_KEYS = new Set(["acquisitionId", "fencingToken", "holderId", "leaseKey"])

export const OPERATION_STEP_STATES = [
  "pending",
  "running",
  "retryable_failed",
  "unknown",
  "succeeded",
  "failed",
  "intervention_required",
  "not_required",
] as const

export const MAX_IRREVERSIBLE_AUTHORIZATION_BYTES = 64 * 1_024

export type OperationStepState = (typeof OPERATION_STEP_STATES)[number]
export type RetryClassification = "idempotent" | "never" | "reconcile_first"
export type CheckpointKind = "irreversible" | "reversible"
export type EffectProtocol = "opaque" | "provider_receipt" | "saga_receipt"
export type StepActivation = "conditional" | "required"
export type StepCompletionRole = "settlement" | "work"
export type DigestFunction = (input: Uint8Array) => Promise<string> | string

export interface OperationStepPlanInput {
  readonly activation?: StepActivation
  readonly checkpoint: CheckpointKind
  readonly completionRole?: StepCompletionRole
  readonly dependsOn?: readonly string[]
  readonly effectProtocol?: EffectProtocol
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly leaseKey: string
  readonly postconditionChecksum: string
  readonly preconditionChecksum: string
  readonly recoveryInstructions: string
  readonly retryClassification: RetryClassification
  readonly stepId: string
}

export interface OperationPlanInput {
  readonly capabilitySnapshotChecksum: string
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly operationId: string
  readonly operationType: string
  readonly steps: readonly OperationStepPlanInput[]
}

export interface OperationStepPlan extends OperationStepPlanInput {
  readonly activation: StepActivation
  readonly completionRole: StepCompletionRole
  readonly dependsOn: readonly string[]
  readonly effectProtocol: EffectProtocol
}

export interface OperationPlan {
  readonly capabilitySnapshotChecksum: string
  readonly idempotencyKey: string
  readonly inputChecksum: string
  readonly operationId: string
  readonly operationType: string
  readonly planChecksum: string
  readonly schemaVersion: 1
  readonly steps: readonly OperationStepPlan[]
}

export interface OperationStepRecord {
  readonly activeAttemptId?: string
  readonly authorizationChecksum?: string
  readonly costCounters: Readonly<Record<string, number>>
  readonly errorChecksum?: string
  readonly fencingToken?: number
  readonly irreversibleAuthorization?: IrreversibleAuthorization
  readonly lastAttemptId?: string
  readonly progressCounters: Readonly<Record<string, number>>
  readonly reconciliationEvidenceChecksum?: string
  readonly resultChecksum?: string
  readonly startedAttempts: number
  readonly state: OperationStepState
}

export interface OperationRecord {
  readonly plan: OperationPlan
  readonly steps: Readonly<Record<string, OperationStepRecord>>
}

export type OperationStatus =
  | "failed"
  | "intervention_required"
  | "paused"
  | "planned"
  | "reconciling"
  | "running"
  | "succeeded"

export interface LeaseProof {
  readonly acquisitionId: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseKey: string
}

export interface FencedLeaseRecord {
  readonly acquisitionId: string | null
  readonly expiresAtServerTimeMs: number
  readonly fencingToken: number
  readonly holderId: string | null
  readonly leaseKey: string
}

export type LeaseWriteCondition =
  | { readonly kind: "insert_if_absent" }
  | {
      readonly acquisitionId: string | null
      readonly expiresAtServerTimeMs: number
      readonly fencingToken: number
      readonly holderId: string | null
      readonly kind: "replace_exact"
      readonly leaseKey: string
      readonly serverTimeRequirement: "expired_or_released" | "unexpired" | "none"
    }

export type LeaseAcquisitionDecision =
  | {
      readonly acquired: false
      readonly currentFencingToken: number
      readonly reason: "held"
      readonly retryAtServerTimeMs: number
    }
  | {
      readonly acquired: true
      readonly condition: LeaseWriteCondition | null
      readonly record: FencedLeaseRecord
      readonly replayed: boolean
    }

export type LeaseRenewalDecision =
  | { readonly reason: "expired" | "fenced"; readonly renewed: false }
  | {
      readonly condition: LeaseWriteCondition | null
      readonly record: FencedLeaseRecord
      readonly renewed: true
      readonly replayed: boolean
    }

export type LeaseReleaseDecision =
  | { readonly reason: "fenced"; readonly released: false }
  | {
      readonly condition: LeaseWriteCondition
      readonly record: FencedLeaseRecord
      readonly released: true
    }

export interface IrreversibleAuthorization {
  readonly actorChecksum: string
  readonly authorizationChecksum: string
  readonly authorizationId: string
  readonly decisionChecksum: string
  readonly fencingToken: number
  readonly holderId: string
  readonly leaseAcquisitionId: string
  readonly leaseKey: string
  readonly operationId: string
  readonly planChecksum: string
  readonly sealedAtServerTimeMs: number
  readonly stepId: string
  readonly stepInputChecksum: string
  readonly schemaVersion: 1
}

export interface StepInvocationRequest {
  readonly attemptId: string
  readonly idempotencyKey: string
  readonly irreversibleAuthorization?: IrreversibleAuthorization
  readonly lease: FencedLeaseRecord
  readonly leaseProof: LeaseProof
  readonly observedPreconditionChecksum: string
  readonly serverTimeMs: number
  readonly stepId: string
}

export type StepInvocationDecision =
  | { readonly disposition: "blocked"; readonly operation: OperationRecord }
  | { readonly disposition: "execute"; readonly operation: OperationRecord }
  | { readonly disposition: "in_progress"; readonly operation: OperationRecord }
  | { readonly disposition: "reconcile"; readonly operation: OperationRecord }
  | {
      readonly disposition: "replay"
      readonly operation: OperationRecord
      readonly resultChecksum: string
    }

export interface CounterDeltas {
  readonly cost?: Readonly<Record<string, number>>
  readonly progress?: Readonly<Record<string, number>>
}

export type AtomicStepOutcome =
  | {
      readonly observedPostconditionChecksum: string
      readonly resultChecksum: string
      readonly state: "succeeded"
    }
  | { readonly errorChecksum: string; readonly state: "failed" }
  | { readonly evidenceChecksum: string; readonly state: "intervention_required" }

export interface AtomicStepOutcomeInput {
  readonly attemptId: string
  readonly idempotencyKey: string
  readonly leaseProof: LeaseProof
  readonly observedPreconditionChecksum: string
  readonly outcome: AtomicStepOutcome
  readonly stepId: string
}

export type StepFailureOutcome = "definitely_not_applied" | "permanent" | "unknown"
export type StepReconciliationOutcome = "applied" | "indeterminate" | "not_applied"

export interface AuditEventInput {
  readonly actorChecksum: string
  readonly environmentId: string
  readonly eventType: string
  readonly fencingToken: number | null
  readonly idempotencyKey: string
  readonly operationId: string
  readonly payloadChecksum: string
  readonly serverTimeMs: number
  readonly stepId: string | null
}

export interface AuditEvent extends AuditEventInput {
  readonly eventHash: string
  readonly previousHash: string | null
  readonly schemaVersion: 1
  readonly sequence: number
}

function configurationError(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function resumeError(message: string, details?: Readonly<Record<string, unknown>>): never {
  throw new NozzleError("OperationResumeRequiredError", message, details ? { details } : undefined)
}

function interventionError(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function captureConfiguration<T>(value: T, message: string): T {
  try {
    return structuredClone(value)
  } catch {
    return configurationError(message)
  }
}

function capturePersisted<T>(value: T, message: string): T {
  try {
    return structuredClone(value)
  } catch {
    return interventionError(message)
  }
}

function isWellFormedUtf16(value: string): boolean {
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

function assertWellFormedString(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    configurationError(`${label} must be non-empty.`)
  }
  if (!isWellFormedUtf16(value)) {
    configurationError(`${label} cannot contain unpaired UTF-16 surrogates.`)
  }
}

function assertServerTime(value: number, label = "Server time"): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    configurationError(`${label} must be a non-negative safe integer.`)
  }
}

function assertPositiveSafeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    configurationError(`${label} must be a positive safe integer.`)
  }
}

function checkedAdd(left: number, right: number, label: string): number {
  const result = left + right
  if (!Number.isSafeInteger(result)) configurationError(`${label} exceeds the safe integer range.`)
  return result
}

function normalizeStep(step: OperationStepPlanInput): OperationStepPlan {
  if (
    !plainRecord(step) ||
    !Object.keys(step).every((key) => OPERATION_STEP_PLAN_INPUT_KEYS.has(key))
  ) {
    configurationError("Operation step contains unknown fields.")
  }
  assertWellFormedString(step.stepId, "Step ID")
  assertWellFormedString(step.idempotencyKey, "Step idempotency key")
  assertWellFormedString(step.inputChecksum, "Step input checksum")
  assertWellFormedString(step.leaseKey, "Step lease key")
  assertWellFormedString(step.preconditionChecksum, "Step precondition checksum")
  assertWellFormedString(step.postconditionChecksum, "Step postcondition checksum")
  assertWellFormedString(step.recoveryInstructions, "Step recovery instructions")
  if (!(["irreversible", "reversible"] as const).includes(step.checkpoint)) {
    configurationError("Step checkpoint kind is invalid.")
  }
  if (!(["idempotent", "never", "reconcile_first"] as const).includes(step.retryClassification)) {
    configurationError("Step retry classification is invalid.")
  }
  const activation = step.activation ?? "required"
  if (!(["conditional", "required"] as const).includes(activation)) {
    configurationError("Step activation is invalid.")
  }
  const completionRole = step.completionRole ?? "work"
  if (!(["settlement", "work"] as const).includes(completionRole)) {
    configurationError("Step completion role is invalid.")
  }
  const effectProtocol = step.effectProtocol ?? "opaque"
  if (!(["opaque", "provider_receipt", "saga_receipt"] as const).includes(effectProtocol)) {
    configurationError("Step effect protocol is invalid.")
  }
  const sourceDependencies = step.dependsOn ?? []
  if (!exactDenseArray(sourceDependencies)) {
    configurationError("Step dependencies must be an array.")
  }
  const dependsOn = [...new Set(sourceDependencies)].sort()
  if (dependsOn.length !== sourceDependencies.length || dependsOn.includes("")) {
    configurationError("Step dependencies must be unique and non-empty.")
  }
  for (const dependency of dependsOn) assertWellFormedString(dependency, "Step dependency")
  return Object.freeze({
    activation,
    checkpoint: step.checkpoint,
    completionRole,
    dependsOn: Object.freeze(dependsOn),
    effectProtocol,
    idempotencyKey: step.idempotencyKey,
    inputChecksum: step.inputChecksum,
    leaseKey: step.leaseKey,
    postconditionChecksum: step.postconditionChecksum,
    preconditionChecksum: step.preconditionChecksum,
    recoveryInstructions: step.recoveryInstructions,
    retryClassification: step.retryClassification,
    stepId: step.stepId,
  })
}

function normalizePlan(input: OperationPlanInput): Omit<OperationPlan, "planChecksum"> {
  if (
    !plainRecord(input) ||
    Object.keys(input).length !== OPERATION_PLAN_INPUT_KEYS.size ||
    !Object.keys(input).every((key) => OPERATION_PLAN_INPUT_KEYS.has(key))
  ) {
    configurationError("Operation plan input fields are malformed.")
  }
  assertWellFormedString(input.operationId, "Operation ID")
  assertWellFormedString(input.operationType, "Operation type")
  assertWellFormedString(input.idempotencyKey, "Operation idempotency key")
  assertWellFormedString(input.inputChecksum, "Operation input checksum")
  assertWellFormedString(input.capabilitySnapshotChecksum, "Capability snapshot checksum")
  if (!exactDenseArray(input.steps)) configurationError("Operation plan steps must be an array.")
  if (input.steps.length === 0) configurationError("An operation requires at least one step.")

  const steps = input.steps.map(normalizeStep).sort((left, right) => {
    if (left.stepId < right.stepId) return -1
    if (left.stepId > right.stepId) return 1
    return 0
  })
  const stepIds = new Set<string>()
  const idempotencyKeys = new Set<string>()
  for (const step of steps) {
    if (stepIds.has(step.stepId)) configurationError("Operation step IDs must be unique.")
    if (idempotencyKeys.has(step.idempotencyKey)) {
      configurationError("Operation step idempotency keys must be unique.")
    }
    stepIds.add(step.stepId)
    idempotencyKeys.add(step.idempotencyKey)
  }
  if (!steps.some((step) => step.activation === "required")) {
    configurationError("An operation requires at least one required step.")
  }
  const settlements = steps.filter((step) => step.completionRole === "settlement")
  if (settlements.length > 1) {
    configurationError("An operation can declare at most one settlement step.")
  }
  if (settlements[0]?.activation === "conditional") {
    configurationError("An operation settlement step must be required.")
  }
  for (const step of steps) {
    if (step.dependsOn.includes(step.stepId)) configurationError("A step cannot depend on itself.")
    for (const dependency of step.dependsOn) {
      if (!stepIds.has(dependency)) configurationError("A step dependency does not exist.")
    }
  }

  const completed = new Set<string>()
  const visiting = new Set<string>()
  const byId = new Map(steps.map((step) => [step.stepId, step] as const))
  const visit = (stepId: string): void => {
    if (completed.has(stepId)) return
    if (visiting.has(stepId)) configurationError("Operation step dependencies contain a cycle.")
    visiting.add(stepId)
    const step = byId.get(stepId) as OperationStepPlan
    for (const dependency of step.dependsOn) visit(dependency)
    visiting.delete(stepId)
    completed.add(stepId)
  }
  for (const step of steps) visit(step.stepId)

  return Object.freeze({
    capabilitySnapshotChecksum: input.capabilitySnapshotChecksum,
    idempotencyKey: input.idempotencyKey,
    inputChecksum: input.inputChecksum,
    operationId: input.operationId,
    operationType: input.operationType,
    schemaVersion: 1,
    steps: Object.freeze(steps),
  })
}

function frameStrings(domain: string, values: readonly string[]): Uint8Array {
  const encoder = new TextEncoder()
  const parts = [domain, ...values].map((value) => encoder.encode(value))
  let length = 0
  for (const part of parts) {
    length = checkedAdd(length, 4 + part.byteLength, "Checksum input length")
  }
  const output = new Uint8Array(length)
  const view = new DataView(output.buffer)
  let offset = 0
  for (const part of parts) {
    view.setUint32(offset, part.byteLength, false)
    offset += 4
    output.set(part, offset)
    offset += part.byteLength
  }
  return output
}

function planChecksumValues(plan: Omit<OperationPlan, "planChecksum">): readonly string[] {
  const values = [
    String(plan.schemaVersion),
    plan.operationId,
    plan.operationType,
    plan.idempotencyKey,
    plan.inputChecksum,
    plan.capabilitySnapshotChecksum,
    String(plan.steps.length),
  ]
  for (const step of plan.steps) {
    values.push(
      step.stepId,
      step.idempotencyKey,
      step.inputChecksum,
      step.leaseKey,
      step.preconditionChecksum,
      step.postconditionChecksum,
      step.activation,
      step.completionRole,
      step.effectProtocol,
      step.retryClassification,
      step.checkpoint,
      step.recoveryInstructions,
      String(step.dependsOn.length),
      ...step.dependsOn,
    )
  }
  return values
}

async function digestChecksum(input: Uint8Array, digest: DigestFunction, label: string) {
  const checksum = await digest(input.slice())
  assertWellFormedString(checksum, label)
  return checksum
}

export function encodeOperationPlanChecksumInput(input: OperationPlanInput): Uint8Array {
  const plan = normalizePlan(
    captureConfiguration(input, "Operation plan input could not be captured safely."),
  )
  return frameStrings(OPERATION_PLAN_DOMAIN, planChecksumValues(plan))
}

export async function sealOperationPlan(
  input: OperationPlanInput,
  digest: DigestFunction,
): Promise<OperationPlan> {
  const plan = normalizePlan(
    captureConfiguration(input, "Operation plan input could not be captured safely."),
  )
  const planChecksum = await digestChecksum(
    frameStrings(OPERATION_PLAN_DOMAIN, planChecksumValues(plan)),
    digest,
    "Operation plan checksum",
  )
  const sealed = Object.freeze({ ...plan, planChecksum })
  TRUSTED_OPERATION_PLANS.add(sealed)
  return sealed
}

export async function loadOperationPlan(
  candidate: OperationPlan,
  digest: DigestFunction,
): Promise<OperationPlan> {
  const snapshot: unknown = capturePersisted(
    candidate,
    "The persisted operation plan could not be captured safely.",
  )
  persistedInvariant(plainRecord(snapshot), "The persisted operation plan is malformed.")
  persistedInvariant(
    Object.keys(snapshot).length === PERSISTED_OPERATION_PLAN_KEYS.size &&
      Object.keys(snapshot).every((key) => PERSISTED_OPERATION_PLAN_KEYS.has(key)),
    "The persisted operation plan fields are malformed.",
  )
  if (snapshot.schemaVersion !== 1) {
    interventionError("The persisted operation plan version is unsupported.")
  }
  assertWellFormedString(snapshot.planChecksum as string, "Operation plan checksum")
  const planChecksum = snapshot.planChecksum as string
  const normalized = normalizePlan({
    capabilitySnapshotChecksum: snapshot.capabilitySnapshotChecksum as string,
    idempotencyKey: snapshot.idempotencyKey as string,
    inputChecksum: snapshot.inputChecksum as string,
    operationId: snapshot.operationId as string,
    operationType: snapshot.operationType as string,
    steps: snapshot.steps as readonly OperationStepPlanInput[],
  })
  const actual = await digestChecksum(
    frameStrings(OPERATION_PLAN_DOMAIN, planChecksumValues(normalized)),
    digest,
    "Operation plan checksum",
  )
  if (actual !== planChecksum) {
    interventionError("The persisted operation plan checksum does not match its contents.")
  }
  const loaded = Object.freeze({ ...normalized, planChecksum })
  TRUSTED_OPERATION_PLANS.add(loaded)
  return loaded
}

export function assertResumeCompatible(existing: OperationPlan, requested: OperationPlan): void {
  if (existing.operationId !== requested.operationId) {
    resumeError("The requested operation ID does not match the persisted operation.")
  }
  if (existing.idempotencyKey !== requested.idempotencyKey) {
    resumeError("The operation ID is already bound to a different idempotency key.")
  }
  if (existing.planChecksum !== requested.planChecksum) {
    resumeError("The operation ID is already bound to an incompatible immutable plan.", {
      existingPlanChecksum: existing.planChecksum,
      requestedPlanChecksum: requested.planChecksum,
    })
  }
}

function freezeCounters(
  counters: Readonly<Record<string, number>>,
): Readonly<Record<string, number>> {
  const output: Record<string, number> = {}
  for (const key of Object.keys(counters).sort()) {
    assertWellFormedString(key, "Counter name")
    const value = counters[key]
    if (value === undefined || !Number.isSafeInteger(value) || value < 0) {
      configurationError("Counter values must be non-negative safe integers.")
    }
    output[key] = value
  }
  return Object.freeze(output)
}

function incrementCounters(
  current: Readonly<Record<string, number>>,
  delta: Readonly<Record<string, number>> | undefined,
): Readonly<Record<string, number>> {
  if (!delta) return current
  const validated = freezeCounters(delta)
  const output: Record<string, number> = { ...current }
  for (const key of Object.keys(validated)) {
    output[key] = checkedAdd(output[key] ?? 0, validated[key] as number, `Counter ${key}`)
  }
  return Object.freeze(output)
}

function initialStep(): OperationStepRecord {
  return Object.freeze({
    costCounters: Object.freeze({}),
    progressCounters: Object.freeze({}),
    startedAttempts: 0,
    state: "pending",
  })
}

export function createOperationRecord(plan: OperationPlan): OperationRecord {
  assertWellFormedString(plan.planChecksum, "Operation plan checksum")
  if (!TRUSTED_OPERATION_PLANS.has(plan)) {
    interventionError("The operation plan must be sealed or integrity-verified before use.")
  }
  const steps: Record<string, OperationStepRecord> = {}
  for (const step of plan.steps) steps[step.stepId] = initialStep()
  return Object.freeze({ plan, steps: Object.freeze(steps) })
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function exactDenseArray(value: unknown): value is readonly unknown[] {
  if (!Array.isArray(value)) return false
  const keys = Object.keys(value)
  return keys.length === value.length && keys.every((key, index) => key === String(index))
}

function exactRecordKeys(value: unknown, expectedKeys: ReadonlySet<string>): boolean {
  return (
    plainRecord(value) &&
    Object.keys(value).length === expectedKeys.size &&
    Object.keys(value).every((key) => expectedKeys.has(key))
  )
}

function persistedInvariant(condition: boolean, message: string): asserts condition {
  if (!condition) interventionError(message)
}

function persistedOptionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined
  persistedInvariant(
    typeof value === "string" && value.trim().length > 0 && isWellFormedUtf16(value),
    `${label} is malformed.`,
  )
  return value
}

function loadPersistedCounters(value: unknown, label: string): Readonly<Record<string, number>> {
  persistedInvariant(plainRecord(value), `${label} are malformed.`)
  const counters: Record<string, number> = {}
  for (const key of Object.keys(value).sort()) {
    persistedInvariant(
      key.trim().length > 0 && isWellFormedUtf16(key),
      `${label} contain an empty name.`,
    )
    const counter = value[key]
    persistedInvariant(
      typeof counter === "number" && Number.isSafeInteger(counter) && counter >= 0,
      `${label} contain a malformed value.`,
    )
    counters[key] = counter
  }
  return Object.freeze(counters)
}

const PERSISTED_STEP_RECORD_KEYS = new Set([
  "activeAttemptId",
  "authorizationChecksum",
  "costCounters",
  "errorChecksum",
  "fencingToken",
  "irreversibleAuthorization",
  "lastAttemptId",
  "progressCounters",
  "reconciliationEvidenceChecksum",
  "resultChecksum",
  "startedAttempts",
  "state",
])

function loadPersistedStepRecord(
  value: unknown,
  planStep: OperationStepPlan,
  loadedAuthorization?: IrreversibleAuthorization,
): OperationStepRecord {
  persistedInvariant(plainRecord(value), "A persisted operation step record is malformed.")
  persistedInvariant(
    Object.keys(value).every((key) => PERSISTED_STEP_RECORD_KEYS.has(key)),
    "A persisted operation step record contains unknown fields.",
  )
  persistedInvariant(
    typeof value.state === "string" &&
      (OPERATION_STEP_STATES as readonly string[]).includes(value.state),
    "A persisted operation step state is unsupported.",
  )
  persistedInvariant(
    typeof value.startedAttempts === "number" &&
      Number.isSafeInteger(value.startedAttempts) &&
      value.startedAttempts >= 0,
    "A persisted operation attempt count is malformed.",
  )
  const activeAttemptId = persistedOptionalString(value.activeAttemptId, "Active attempt ID")
  const authorizationChecksum = persistedOptionalString(
    value.authorizationChecksum,
    "Authorization checksum",
  )
  const authorizationCandidate = value.irreversibleAuthorization
  persistedInvariant(
    (authorizationCandidate === undefined) === (loadedAuthorization === undefined),
    "A persisted irreversible authorization body is incomplete or unexpected.",
  )
  if (loadedAuthorization !== undefined) {
    persistedInvariant(
      authorizationChecksum === loadedAuthorization.authorizationChecksum,
      "A persisted irreversible authorization body contradicts its checksum.",
    )
  }
  const errorChecksum = persistedOptionalString(value.errorChecksum, "Step error checksum")
  const lastAttemptId = persistedOptionalString(value.lastAttemptId, "Last attempt ID")
  const reconciliationEvidenceChecksum = persistedOptionalString(
    value.reconciliationEvidenceChecksum,
    "Reconciliation evidence checksum",
  )
  const resultChecksum = persistedOptionalString(value.resultChecksum, "Step result checksum")
  const fencingToken = value.fencingToken
  persistedInvariant(
    fencingToken === undefined ||
      (typeof fencingToken === "number" && Number.isSafeInteger(fencingToken) && fencingToken >= 1),
    "A persisted operation fencing token is malformed.",
  )
  const costCounters = loadPersistedCounters(value.costCounters, "Persisted cost counters")
  const progressCounters = loadPersistedCounters(
    value.progressCounters,
    "Persisted progress counters",
  )
  const state = value.state as OperationStepState
  persistedInvariant(
    state !== "retryable_failed" || planStep.retryClassification !== "never",
    "A persisted never-retry step cannot remain retryable.",
  )
  const pending = state === "pending"
  const neverAttempted = pending || state === "not_required"
  persistedInvariant(
    neverAttempted === (value.startedAttempts === 0),
    "Persisted unattempted state contradicts the attempt count.",
  )
  persistedInvariant(
    neverAttempted === (lastAttemptId === undefined) &&
      neverAttempted === (fencingToken === undefined),
    "Persisted attempt identity is incomplete or unexpected.",
  )
  persistedInvariant(
    (state === "running") === (activeAttemptId !== undefined),
    "Persisted active-attempt state is inconsistent.",
  )
  if (state === "running") {
    persistedInvariant(
      activeAttemptId === lastAttemptId,
      "Persisted active and last attempt identities disagree.",
    )
  }
  persistedInvariant(
    (state === "succeeded") === (resultChecksum !== undefined),
    "Persisted result state is inconsistent.",
  )
  persistedInvariant(
    reconciliationEvidenceChecksum === undefined ||
      state === "succeeded" ||
      state === "retryable_failed" ||
      state === "failed" ||
      state === "not_required" ||
      state === "intervention_required",
    "Persisted reconciliation evidence is attached to an invalid state.",
  )
  persistedInvariant(
    errorChecksum === undefined ||
      state === "unknown" ||
      state === "retryable_failed" ||
      state === "failed" ||
      state === "succeeded" ||
      state === "intervention_required",
    "Persisted error evidence is attached to an invalid state.",
  )
  if (state === "retryable_failed" || state === "failed") {
    persistedInvariant(
      errorChecksum !== undefined || reconciliationEvidenceChecksum !== undefined,
      "Persisted failed state has no failure or reconciliation evidence.",
    )
  }
  if (state === "intervention_required") {
    persistedInvariant(
      reconciliationEvidenceChecksum !== undefined,
      "Persisted intervention state has no reconciliation evidence.",
    )
  }
  if (state === "not_required") {
    persistedInvariant(
      planStep.activation === "conditional" && reconciliationEvidenceChecksum !== undefined,
      "A not-required step lacks its conditional decision evidence.",
    )
  }
  if (planStep.checkpoint === "reversible") {
    persistedInvariant(
      authorizationChecksum === undefined && loadedAuthorization === undefined,
      "A reversible step persisted irreversible authorization.",
    )
  } else {
    persistedInvariant(
      neverAttempted === (authorizationChecksum === undefined) &&
        (loadedAuthorization === undefined || !neverAttempted),
      "Persisted irreversible authorization state is inconsistent.",
    )
  }
  if (neverAttempted) {
    persistedInvariant(
      Object.keys(costCounters).length === 0 && Object.keys(progressCounters).length === 0,
      "An unattempted step persisted progress or cost counters.",
    )
  }
  return Object.freeze({
    ...(activeAttemptId === undefined ? {} : { activeAttemptId }),
    ...(authorizationChecksum === undefined ? {} : { authorizationChecksum }),
    costCounters,
    ...(errorChecksum === undefined ? {} : { errorChecksum }),
    ...(fencingToken === undefined ? {} : { fencingToken }),
    ...(loadedAuthorization === undefined
      ? {}
      : { irreversibleAuthorization: loadedAuthorization }),
    ...(lastAttemptId === undefined ? {} : { lastAttemptId }),
    progressCounters,
    ...(reconciliationEvidenceChecksum === undefined ? {} : { reconciliationEvidenceChecksum }),
    ...(resultChecksum === undefined ? {} : { resultChecksum }),
    startedAttempts: value.startedAttempts,
    state,
  })
}

export async function loadOperationRecord(
  candidate: unknown,
  digest: DigestFunction,
): Promise<OperationRecord> {
  const snapshot: unknown = capturePersisted(
    candidate,
    "The persisted operation record could not be captured safely.",
  )
  persistedInvariant(plainRecord(snapshot), "The persisted operation record is malformed.")
  persistedInvariant(
    Object.keys(snapshot).every((key) => key === "plan" || key === "steps"),
    "The persisted operation record contains unknown fields.",
  )
  persistedInvariant(plainRecord(snapshot.plan), "The persisted operation plan is malformed.")
  persistedInvariant(plainRecord(snapshot.steps), "The persisted operation steps are malformed.")
  const plan = await loadOperationPlan(snapshot.plan as unknown as OperationPlan, digest)
  const expectedStepIds = plan.steps.map((step) => step.stepId)
  const persistedStepIds = Object.keys(snapshot.steps).sort()
  persistedInvariant(
    expectedStepIds.length === persistedStepIds.length &&
      expectedStepIds.every((stepId, index) => stepId === persistedStepIds[index]),
    "Persisted operation step membership does not match the immutable plan.",
  )
  const steps: Record<string, OperationStepRecord> = {}
  for (const planStep of plan.steps) {
    const candidate = snapshot.steps[planStep.stepId]
    const authorizationCandidate = plainRecord(candidate)
      ? candidate.irreversibleAuthorization
      : undefined
    const authorization =
      authorizationCandidate === undefined
        ? undefined
        : await loadIrreversibleAuthorization(
            authorizationCandidate as IrreversibleAuthorization,
            digest,
          )
    if (
      authorization !== undefined &&
      (authorization.operationId !== plan.operationId ||
        authorization.planChecksum !== plan.planChecksum ||
        authorization.stepId !== planStep.stepId ||
        authorization.stepInputChecksum !== planStep.inputChecksum ||
        authorization.leaseKey !== planStep.leaseKey)
    ) {
      interventionError(
        "A persisted irreversible authorization is bound to a different immutable plan.",
      )
    }
    const step = loadPersistedStepRecord(candidate, planStep, authorization)
    if (authorization !== undefined && authorization.fencingToken !== step.fencingToken) {
      interventionError(
        "A persisted irreversible authorization is bound to a different operation fence.",
      )
    }
    steps[planStep.stepId] = step
  }
  return Object.freeze({ plan, steps: Object.freeze(steps) })
}

function getPlanStep(operation: OperationRecord, stepId: string): OperationStepPlan {
  const step = operation.plan.steps.find((candidate) => candidate.stepId === stepId)
  if (!step) resumeError("The step is not part of the immutable operation plan.", { stepId })
  return step
}

function getStepRecord(operation: OperationRecord, stepId: string): OperationStepRecord {
  const step = operation.steps[stepId]
  if (!step) resumeError("The step has no persisted operation record.", { stepId })
  return step
}

function updateStep(
  operation: OperationRecord,
  stepId: string,
  step: OperationStepRecord,
): OperationRecord {
  return Object.freeze({
    ...operation,
    steps: Object.freeze({ ...operation.steps, [stepId]: Object.freeze(step) }),
  })
}

export function operationStatus(operation: OperationRecord): OperationStatus {
  const states = Object.values(operation.steps).map((step) => step.state)
  const settlement = operation.plan.steps.find((step) => step.completionRole === "settlement")
  if (settlement !== undefined) {
    const state = operation.steps[settlement.stepId]?.state
    if (state === "succeeded") return "succeeded"
    if (state === "intervention_required") return "intervention_required"
    if (state === "failed") return "failed"
    if (state === "unknown") return "reconciling"
    if (states.includes("unknown")) return "reconciling"
    if (state === "running" || states.includes("running")) return "running"
    return states.every((candidate) => candidate === "pending") ? "planned" : "paused"
  }
  if (states.every((state) => state === "succeeded" || state === "not_required")) {
    return "succeeded"
  }
  if (states.includes("intervention_required")) return "intervention_required"
  if (states.includes("failed")) return "failed"
  if (states.includes("unknown")) return "reconciling"
  if (states.includes("running")) return "running"
  if (states.every((state) => state === "pending")) return "planned"
  return "paused"
}

function validateLeaseRecord(record: FencedLeaseRecord): void {
  assertWellFormedString(record.leaseKey, "Lease key")
  assertPositiveSafeInteger(record.fencingToken, "Lease fencing token")
  assertServerTime(record.expiresAtServerTimeMs, "Lease expiry")
  if ((record.holderId === null) !== (record.acquisitionId === null)) {
    configurationError("Lease holder and acquisition identity must both be present or absent.")
  }
  if (record.holderId !== null) assertWellFormedString(record.holderId, "Lease holder ID")
  if (record.acquisitionId !== null) {
    assertWellFormedString(record.acquisitionId, "Lease acquisition ID")
  }
}

function validateLeaseRequest(input: {
  readonly acquisitionId: string
  readonly holderId: string
  readonly leaseKey: string
  readonly serverTimeMs: number
  readonly ttlMs: number
}): void {
  assertWellFormedString(input.leaseKey, "Lease key")
  assertWellFormedString(input.holderId, "Lease holder ID")
  assertWellFormedString(input.acquisitionId, "Lease acquisition ID")
  assertServerTime(input.serverTimeMs)
  assertPositiveSafeInteger(input.ttlMs, "Lease TTL")
  checkedAdd(input.serverTimeMs, input.ttlMs, "Lease expiry")
}

function exactLeaseCondition(
  current: FencedLeaseRecord,
  serverTimeRequirement: "expired_or_released" | "unexpired" | "none",
): LeaseWriteCondition {
  return Object.freeze({
    acquisitionId: current.acquisitionId,
    expiresAtServerTimeMs: current.expiresAtServerTimeMs,
    fencingToken: current.fencingToken,
    holderId: current.holderId,
    kind: "replace_exact",
    leaseKey: current.leaseKey,
    serverTimeRequirement,
  })
}

export function decideLeaseAcquisition(
  current: FencedLeaseRecord | undefined,
  input: {
    readonly acquisitionId: string
    readonly holderId: string
    readonly leaseKey: string
    readonly serverTimeMs: number
    readonly ttlMs: number
  },
): LeaseAcquisitionDecision {
  validateLeaseRequest(input)
  if (current) {
    validateLeaseRecord(current)
    if (current.leaseKey !== input.leaseKey) configurationError("Lease keys do not match.")
    const active = current.holderId !== null && current.expiresAtServerTimeMs > input.serverTimeMs
    if (active) {
      if (current.holderId === input.holderId && current.acquisitionId === input.acquisitionId) {
        return Object.freeze({ acquired: true, condition: null, record: current, replayed: true })
      }
      return Object.freeze({
        acquired: false,
        currentFencingToken: current.fencingToken,
        reason: "held",
        retryAtServerTimeMs: current.expiresAtServerTimeMs,
      })
    }
  }

  const fencingToken = checkedAdd(current?.fencingToken ?? 0, 1, "Lease fencing token")
  const record: FencedLeaseRecord = Object.freeze({
    acquisitionId: input.acquisitionId,
    expiresAtServerTimeMs: checkedAdd(input.serverTimeMs, input.ttlMs, "Lease expiry"),
    fencingToken,
    holderId: input.holderId,
    leaseKey: input.leaseKey,
  })
  return Object.freeze({
    acquired: true,
    condition: current
      ? exactLeaseCondition(current, "expired_or_released")
      : Object.freeze({ kind: "insert_if_absent" }),
    record,
    replayed: false,
  })
}

function proofMatches(record: FencedLeaseRecord | undefined, proof: LeaseProof): boolean {
  return (
    record !== undefined &&
    record.leaseKey === proof.leaseKey &&
    record.holderId === proof.holderId &&
    record.acquisitionId === proof.acquisitionId &&
    record.fencingToken === proof.fencingToken
  )
}

function validateLeaseProof(proof: LeaseProof): void {
  assertWellFormedString(proof.leaseKey, "Lease proof key")
  assertWellFormedString(proof.holderId, "Lease proof holder ID")
  assertWellFormedString(proof.acquisitionId, "Lease proof acquisition ID")
  assertPositiveSafeInteger(proof.fencingToken, "Lease proof fencing token")
}

export function leaseProof(record: FencedLeaseRecord): LeaseProof {
  validateLeaseRecord(record)
  if (record.holderId === null || record.acquisitionId === null) {
    resumeError("A released lease cannot produce an authorization proof.")
  }
  return Object.freeze({
    acquisitionId: record.acquisitionId,
    fencingToken: record.fencingToken,
    holderId: record.holderId,
    leaseKey: record.leaseKey,
  })
}

export function decideLeaseRenewal(
  current: FencedLeaseRecord | undefined,
  input: {
    readonly proof: LeaseProof
    readonly serverTimeMs: number
    readonly ttlMs: number
  },
): LeaseRenewalDecision {
  validateLeaseProof(input.proof)
  assertServerTime(input.serverTimeMs)
  assertPositiveSafeInteger(input.ttlMs, "Lease TTL")
  if (current) validateLeaseRecord(current)
  if (!proofMatches(current, input.proof))
    return Object.freeze({ reason: "fenced", renewed: false })
  if (!current || current.expiresAtServerTimeMs <= input.serverTimeMs) {
    return Object.freeze({ reason: "expired", renewed: false })
  }
  const requestedExpiry = checkedAdd(input.serverTimeMs, input.ttlMs, "Lease expiry")
  const expiresAtServerTimeMs = Math.max(current.expiresAtServerTimeMs, requestedExpiry)
  if (expiresAtServerTimeMs === current.expiresAtServerTimeMs) {
    return Object.freeze({ renewed: true, condition: null, record: current, replayed: true })
  }
  const record = Object.freeze({ ...current, expiresAtServerTimeMs })
  return Object.freeze({
    renewed: true,
    condition: exactLeaseCondition(current, "unexpired"),
    record,
    replayed: false,
  })
}

export function decideLeaseRelease(
  current: FencedLeaseRecord | undefined,
  input: { readonly proof: LeaseProof; readonly serverTimeMs: number },
): LeaseReleaseDecision {
  validateLeaseProof(input.proof)
  assertServerTime(input.serverTimeMs)
  if (current) validateLeaseRecord(current)
  if (!current || !proofMatches(current, input.proof))
    return Object.freeze({ reason: "fenced", released: false })
  const record: FencedLeaseRecord = Object.freeze({
    acquisitionId: null,
    expiresAtServerTimeMs: input.serverTimeMs,
    fencingToken: current.fencingToken,
    holderId: null,
    leaseKey: current.leaseKey,
  })
  return Object.freeze({
    released: true,
    condition: exactLeaseCondition(current, "none"),
    record,
  })
}

export function assertLeaseAuthorized(
  current: FencedLeaseRecord | undefined,
  proof: LeaseProof,
  serverTimeMs: number,
): asserts current is FencedLeaseRecord {
  validateLeaseProof(proof)
  assertServerTime(serverTimeMs)
  if (current) validateLeaseRecord(current)
  if (!proofMatches(current, proof)) {
    resumeError("The lease proof was fenced by a different lease owner or token.")
  }
  if (!current || current.expiresAtServerTimeMs <= serverTimeMs) {
    resumeError("The lease proof has expired according to authoritative server time.")
  }
}

function authorizationChecksumValues(
  authorization: Omit<IrreversibleAuthorization, "authorizationChecksum">,
): readonly string[] {
  return [
    String(authorization.schemaVersion),
    authorization.authorizationId,
    authorization.operationId,
    authorization.planChecksum,
    authorization.stepId,
    authorization.stepInputChecksum,
    authorization.leaseKey,
    authorization.holderId,
    authorization.leaseAcquisitionId,
    String(authorization.fencingToken),
    authorization.actorChecksum,
    authorization.decisionChecksum,
    String(authorization.sealedAtServerTimeMs),
  ]
}

export function encodeIrreversibleAuthorizationChecksumInput(
  authorization: Omit<IrreversibleAuthorization, "authorizationChecksum">,
): Uint8Array {
  return frameStrings(IRREVERSIBLE_AUTHORIZATION_DOMAIN, authorizationChecksumValues(authorization))
}

export async function sealIrreversibleAuthorization(
  plan: OperationPlan,
  input: {
    readonly actorChecksum: string
    readonly authorizationId: string
    readonly decisionChecksum: string
    readonly lease: FencedLeaseRecord
    readonly leaseProof: LeaseProof
    readonly sealedAtServerTimeMs: number
    readonly stepId: string
  },
  digest: DigestFunction,
): Promise<IrreversibleAuthorization> {
  if (!TRUSTED_OPERATION_PLANS.has(plan)) {
    interventionError("Irreversible authorization requires a sealed or integrity-verified plan.")
  }
  const captured = captureConfiguration(
    input,
    "Irreversible authorization input could not be captured safely.",
  )
  if (
    !plainRecord(captured) ||
    Object.keys(captured).length !== IRREVERSIBLE_AUTHORIZATION_INPUT_KEYS.size ||
    !Object.keys(captured).every((key) => IRREVERSIBLE_AUTHORIZATION_INPUT_KEYS.has(key))
  ) {
    configurationError("Irreversible authorization input fields are malformed.")
  }
  if (
    !exactRecordKeys(captured.lease, FENCED_LEASE_RECORD_KEYS) ||
    !exactRecordKeys(captured.leaseProof, LEASE_PROOF_KEYS)
  ) {
    configurationError("Irreversible authorization lease fields are malformed.")
  }
  assertWellFormedString(captured.authorizationId, "Authorization ID")
  assertWellFormedString(captured.actorChecksum, "Authorization actor checksum")
  assertWellFormedString(captured.decisionChecksum, "Authorization decision checksum")
  const step = plan.steps.find((candidate) => candidate.stepId === captured.stepId)
  if (!step) configurationError("The authorized step is not part of the operation plan.")
  if (step.checkpoint !== "irreversible") {
    configurationError("Irreversible authorization can only seal an irreversible step.")
  }
  assertLeaseAuthorized(captured.lease, captured.leaseProof, captured.sealedAtServerTimeMs)
  if (step.leaseKey !== captured.lease.leaseKey)
    configurationError("Step and lease keys do not match.")

  const authorization = Object.freeze({
    actorChecksum: captured.actorChecksum,
    authorizationId: captured.authorizationId,
    decisionChecksum: captured.decisionChecksum,
    fencingToken: captured.leaseProof.fencingToken,
    holderId: captured.leaseProof.holderId,
    leaseAcquisitionId: captured.leaseProof.acquisitionId,
    leaseKey: captured.leaseProof.leaseKey,
    operationId: plan.operationId,
    planChecksum: plan.planChecksum,
    sealedAtServerTimeMs: captured.sealedAtServerTimeMs,
    schemaVersion: 1 as const,
    stepId: step.stepId,
    stepInputChecksum: step.inputChecksum,
  })
  if (
    new TextEncoder().encode(JSON.stringify(authorization)).byteLength >
    MAX_IRREVERSIBLE_AUTHORIZATION_BYTES
  ) {
    configurationError("Irreversible authorization exceeds the 64 KiB receipt limit.")
  }
  const authorizationChecksum = await digestChecksum(
    encodeIrreversibleAuthorizationChecksumInput(authorization),
    digest,
    "Irreversible authorization checksum",
  )
  const sealed = Object.freeze({ ...authorization, authorizationChecksum })
  if (
    new TextEncoder().encode(JSON.stringify(sealed)).byteLength >
    MAX_IRREVERSIBLE_AUTHORIZATION_BYTES
  ) {
    configurationError("Irreversible authorization exceeds the 64 KiB receipt limit.")
  }
  TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.add(sealed)
  return sealed
}

export async function verifyIrreversibleAuthorizationChecksum(
  authorization: IrreversibleAuthorization,
  digest: DigestFunction,
): Promise<boolean> {
  const { authorizationChecksum, ...unsigned } = authorization
  const actual = await digestChecksum(
    encodeIrreversibleAuthorizationChecksumInput(unsigned),
    digest,
    "Irreversible authorization checksum",
  )
  return actual === authorizationChecksum
}

export async function loadIrreversibleAuthorization(
  candidate: IrreversibleAuthorization,
  digest: DigestFunction,
): Promise<IrreversibleAuthorization> {
  const snapshot: unknown = capturePersisted(
    candidate,
    "The persisted irreversible authorization could not be captured safely.",
  )
  persistedInvariant(
    plainRecord(snapshot) &&
      Object.keys(snapshot).length === IRREVERSIBLE_AUTHORIZATION_KEYS.size &&
      Object.keys(snapshot).every((key) => IRREVERSIBLE_AUTHORIZATION_KEYS.has(key)),
    "The persisted irreversible authorization fields are malformed.",
  )
  if (snapshot.schemaVersion !== 1) {
    interventionError("The persisted irreversible authorization version is unsupported.")
  }
  assertWellFormedString(snapshot.actorChecksum as string, "Authorization actor checksum")
  assertWellFormedString(snapshot.authorizationChecksum as string, "Authorization checksum")
  assertWellFormedString(snapshot.authorizationId as string, "Authorization ID")
  assertWellFormedString(snapshot.decisionChecksum as string, "Authorization decision checksum")
  assertPositiveSafeInteger(snapshot.fencingToken as number, "Authorization fencing token")
  assertWellFormedString(snapshot.holderId as string, "Authorization holder ID")
  assertWellFormedString(
    snapshot.leaseAcquisitionId as string,
    "Authorization lease acquisition ID",
  )
  assertWellFormedString(snapshot.leaseKey as string, "Authorization lease key")
  assertWellFormedString(snapshot.operationId as string, "Authorization operation ID")
  assertWellFormedString(snapshot.planChecksum as string, "Authorization plan checksum")
  assertServerTime(snapshot.sealedAtServerTimeMs as number, "Authorization seal time")
  assertWellFormedString(snapshot.stepId as string, "Authorization step ID")
  assertWellFormedString(snapshot.stepInputChecksum as string, "Authorization step input checksum")
  const authorization: IrreversibleAuthorization = {
    actorChecksum: snapshot.actorChecksum as string,
    authorizationId: snapshot.authorizationId as string,
    decisionChecksum: snapshot.decisionChecksum as string,
    fencingToken: snapshot.fencingToken as number,
    holderId: snapshot.holderId as string,
    leaseAcquisitionId: snapshot.leaseAcquisitionId as string,
    leaseKey: snapshot.leaseKey as string,
    operationId: snapshot.operationId as string,
    planChecksum: snapshot.planChecksum as string,
    sealedAtServerTimeMs: snapshot.sealedAtServerTimeMs as number,
    schemaVersion: 1,
    stepId: snapshot.stepId as string,
    stepInputChecksum: snapshot.stepInputChecksum as string,
    authorizationChecksum: snapshot.authorizationChecksum as string,
  }
  persistedInvariant(
    new TextEncoder().encode(JSON.stringify(authorization)).byteLength <=
      MAX_IRREVERSIBLE_AUTHORIZATION_BYTES,
    "The persisted irreversible authorization exceeds the 64 KiB receipt limit.",
  )
  if (!(await verifyIrreversibleAuthorizationChecksum(authorization, digest))) {
    interventionError("The persisted irreversible authorization checksum does not match.")
  }
  const loaded = Object.freeze(authorization)
  TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.add(loaded)
  return loaded
}

function assertAuthorizationForInvocation(
  operation: OperationRecord,
  step: OperationStepPlan,
  request: StepInvocationRequest,
): IrreversibleAuthorization | undefined {
  if (step.checkpoint === "reversible") {
    if (request.irreversibleAuthorization) {
      configurationError("A reversible step must not consume irreversible authorization.")
    }
    return undefined
  }
  const authorization = request.irreversibleAuthorization
  if (!authorization) interventionError("The irreversible step has no sealed authorization.")
  if (
    authorization.operationId !== operation.plan.operationId ||
    authorization.planChecksum !== operation.plan.planChecksum ||
    authorization.stepId !== step.stepId ||
    authorization.stepInputChecksum !== step.inputChecksum
  ) {
    interventionError("The irreversible authorization is bound to a different immutable plan.")
  }
  if (
    authorization.leaseKey !== request.leaseProof.leaseKey ||
    authorization.holderId !== request.leaseProof.holderId ||
    authorization.leaseAcquisitionId !== request.leaseProof.acquisitionId ||
    authorization.fencingToken !== request.leaseProof.fencingToken
  ) {
    resumeError("The irreversible authorization was sealed under a different lease fence.")
  }
  if (authorization.sealedAtServerTimeMs > request.serverTimeMs) {
    interventionError("The irreversible authorization is from a future server timestamp.")
  }
  if (!TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.has(authorization)) {
    interventionError(
      "The irreversible authorization must be sealed or integrity-verified before use.",
    )
  }
  assertWellFormedString(authorization.authorizationChecksum, "Authorization checksum")
  return authorization
}

function authorizationRecordFields(
  authorization: IrreversibleAuthorization | undefined,
): Pick<OperationStepRecord, "authorizationChecksum" | "irreversibleAuthorization"> {
  return authorization === undefined
    ? {}
    : {
        authorizationChecksum: authorization.authorizationChecksum,
        irreversibleAuthorization: authorization,
      }
}

function retainedAuthorizationFields(
  operation: OperationRecord,
  step: OperationStepPlan,
  record: OperationStepRecord,
): Pick<OperationStepRecord, "authorizationChecksum" | "irreversibleAuthorization"> {
  const authorization = record.irreversibleAuthorization
  const authorizationExpected = step.checkpoint === "irreversible" && record.startedAttempts > 0
  if (
    authorizationExpected !== (authorization !== undefined) ||
    (authorization === undefined) !== (record.authorizationChecksum === undefined) ||
    (authorization !== undefined &&
      (!TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.has(authorization) ||
        authorization.authorizationChecksum !== record.authorizationChecksum ||
        authorization.operationId !== operation.plan.operationId ||
        authorization.planChecksum !== operation.plan.planChecksum ||
        authorization.stepId !== step.stepId ||
        authorization.stepInputChecksum !== step.inputChecksum ||
        authorization.leaseKey !== step.leaseKey ||
        authorization.fencingToken !== record.fencingToken))
  ) {
    interventionError("The irreversible authorization record is incomplete or contradictory.")
  }
  return authorizationRecordFields(authorization)
}

function assertReplayAuthorization(
  operation: OperationRecord,
  step: OperationStepPlan,
  record: OperationStepRecord,
  supplied: IrreversibleAuthorization | undefined,
): void {
  if (record.state === "pending") return
  if (record.state === "retryable_failed") {
    retainedAuthorizationFields(operation, step, record)
    return
  }
  if (supplied === undefined) return
  if (step.checkpoint === "reversible") {
    configurationError("A reversible step must not consume irreversible authorization.")
  }
  const retained = retainedAuthorizationFields(operation, step, record).irreversibleAuthorization
  if (
    !TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.has(supplied) ||
    retained === undefined ||
    ![...IRREVERSIBLE_AUTHORIZATION_KEYS].every(
      (key) =>
        supplied[key as keyof IrreversibleAuthorization] ===
        retained[key as keyof IrreversibleAuthorization],
    )
  ) {
    interventionError("A replayed step supplied contradictory irreversible authorization.")
  }
}

function assertDependenciesSucceeded(operation: OperationRecord, step: OperationStepPlan): void {
  for (const dependency of step.dependsOn) {
    if (operation.steps[dependency]?.state !== "succeeded") {
      resumeError("A step dependency has not succeeded.", { dependency, stepId: step.stepId })
    }
  }
}

export function markOperationStepNotRequired(
  operation: OperationRecord,
  input: {
    readonly evidenceChecksum: string
    readonly stepId: string
  },
): OperationRecord {
  assertWellFormedString(input.evidenceChecksum, "Conditional-step decision evidence checksum")
  const planStep = getPlanStep(operation, input.stepId)
  const record = getStepRecord(operation, input.stepId)
  if (planStep.activation !== "conditional") {
    configurationError("Only a conditional operation step can become not required.")
  }
  if (record.state === "not_required") {
    if (record.reconciliationEvidenceChecksum === input.evidenceChecksum) return operation
    interventionError("A duplicate conditional-step decision contradicts durable evidence.")
  }
  if (record.state !== "pending") {
    resumeError("Only an unattempted pending step can become not required.")
  }
  return updateStep(
    operation,
    input.stepId,
    Object.freeze({
      costCounters: record.costCounters,
      progressCounters: record.progressCounters,
      reconciliationEvidenceChecksum: input.evidenceChecksum,
      startedAttempts: 0,
      state: "not_required",
    }),
  )
}

function atomicTerminalStepRecord(
  attemptId: string,
  fencingToken: number,
  outcome: AtomicStepOutcome,
): OperationStepRecord {
  const common = {
    costCounters: Object.freeze({}),
    fencingToken,
    lastAttemptId: attemptId,
    progressCounters: Object.freeze({}),
    startedAttempts: 1,
  }
  if (outcome.state === "succeeded") {
    return Object.freeze({
      ...common,
      resultChecksum: outcome.resultChecksum,
      state: outcome.state,
    })
  }
  if (outcome.state === "failed") {
    return Object.freeze({ ...common, errorChecksum: outcome.errorChecksum, state: outcome.state })
  }
  return Object.freeze({
    ...common,
    reconciliationEvidenceChecksum: outcome.evidenceChecksum,
    state: outcome.state,
  })
}

function exactAtomicTerminalReplay(
  record: OperationStepRecord,
  expected: OperationStepRecord,
): boolean {
  return ![
    Object.keys(record).length !== Object.keys(expected).length,
    record.activeAttemptId !== undefined,
    record.authorizationChecksum !== undefined,
    Object.keys(record.costCounters).length !== 0,
    record.errorChecksum !== expected.errorChecksum,
    record.fencingToken !== expected.fencingToken,
    record.irreversibleAuthorization !== undefined,
    record.lastAttemptId !== expected.lastAttemptId,
    Object.keys(record.progressCounters).length !== 0,
    record.reconciliationEvidenceChecksum !== expected.reconciliationEvidenceChecksum,
    record.resultChecksum !== expected.resultChecksum,
    record.startedAttempts !== expected.startedAttempts,
    record.state !== expected.state,
  ].includes(true)
}

function assertPristineAtomicStep(record: OperationStepRecord): void {
  if (
    [
      Object.keys(record).length !== 4,
      record.startedAttempts !== 0,
      Object.keys(record.costCounters).length !== 0,
      Object.keys(record.progressCounters).length !== 0,
      record.activeAttemptId !== undefined,
      record.authorizationChecksum !== undefined,
      record.errorChecksum !== undefined,
      record.fencingToken !== undefined,
      record.irreversibleAuthorization !== undefined,
      record.lastAttemptId !== undefined,
      record.reconciliationEvidenceChecksum !== undefined,
      record.resultChecksum !== undefined,
    ].includes(true)
  ) {
    interventionError("The pending atomic step record contains contradictory attempt evidence.")
  }
}

/**
 * Commits a coordinator-owned step whose acceptance and outcome share one persistence transaction.
 * The persistence caller must verify the complete proof against the active lease in that transaction.
 */
export function recordAtomicStepOutcome(
  operation: OperationRecord,
  input: AtomicStepOutcomeInput,
): OperationRecord {
  assertWellFormedString(input.stepId, "Step ID")
  assertWellFormedString(input.attemptId, "Attempt ID")
  assertWellFormedString(input.idempotencyKey, "Step idempotency key")
  assertWellFormedString(input.observedPreconditionChecksum, "Observed precondition checksum")
  validateLeaseProof(input.leaseProof)
  const planStep = getPlanStep(operation, input.stepId)
  const record = getStepRecord(operation, input.stepId)
  if (planStep.checkpoint !== "reversible") {
    configurationError("An atomic internal outcome requires a reversible operation step.")
  }
  if (planStep.effectProtocol !== "opaque") {
    configurationError("An atomic internal outcome cannot bypass a receipt-owned step.")
  }
  if (input.idempotencyKey !== planStep.idempotencyKey) {
    resumeError("The atomic step ID is already bound to a different idempotency key.")
  }
  if (input.observedPreconditionChecksum !== planStep.preconditionChecksum) {
    resumeError("The atomic step precondition does not match the sealed operation plan.")
  }
  if (input.leaseProof.leaseKey !== planStep.leaseKey) {
    resumeError("The atomic step outcome was recorded under the wrong lease key.")
  }
  assertDependenciesSucceeded(operation, planStep)
  if (!plainRecord(input.outcome)) configurationError("Atomic step outcome is malformed.")

  const outcome = input.outcome as AtomicStepOutcome
  if (outcome.state === "succeeded") {
    assertWellFormedString(outcome.resultChecksum, "Atomic step result checksum")
    assertWellFormedString(
      outcome.observedPostconditionChecksum,
      "Observed atomic step postcondition checksum",
    )
    if (outcome.observedPostconditionChecksum !== planStep.postconditionChecksum) {
      interventionError("The atomic step postcondition contradicts the sealed operation plan.")
    }
  } else if (outcome.state === "failed") {
    assertWellFormedString(outcome.errorChecksum, "Atomic step error checksum")
  } else if (outcome.state === "intervention_required") {
    assertWellFormedString(outcome.evidenceChecksum, "Atomic step intervention evidence checksum")
  } else {
    configurationError("Atomic step outcome state is invalid.")
  }

  const next = atomicTerminalStepRecord(input.attemptId, input.leaseProof.fencingToken, outcome)
  if (
    record.state === "succeeded" ||
    record.state === "failed" ||
    record.state === "intervention_required"
  ) {
    if (exactAtomicTerminalReplay(record, next)) return operation
    interventionError("A duplicate atomic step outcome contradicts durable terminal evidence.")
  }
  if (record.state !== "pending") {
    resumeError("Only a pending atomic step can record its first terminal outcome.")
  }
  assertPristineAtomicStep(record)
  return updateStep(operation, input.stepId, next)
}

export function beginOperationStep(
  operation: OperationRecord,
  request: StepInvocationRequest,
): StepInvocationDecision {
  assertWellFormedString(request.stepId, "Step ID")
  assertWellFormedString(request.attemptId, "Attempt ID")
  assertWellFormedString(request.idempotencyKey, "Step idempotency key")
  const planStep = getPlanStep(operation, request.stepId)
  const record = getStepRecord(operation, request.stepId)
  if (request.idempotencyKey !== planStep.idempotencyKey) {
    resumeError("The step ID is already bound to a different idempotency key.")
  }
  assertReplayAuthorization(operation, planStep, record, request.irreversibleAuthorization)
  if (record.state === "succeeded") {
    if (!record.resultChecksum) interventionError("A successful step has no result checksum.")
    return Object.freeze({
      disposition: "replay",
      operation,
      resultChecksum: record.resultChecksum,
    })
  }
  if (record.state === "unknown") {
    return Object.freeze({ disposition: "reconcile", operation })
  }
  if (record.state === "running") {
    return Object.freeze({
      disposition: record.activeAttemptId === request.attemptId ? "in_progress" : "reconcile",
      operation,
    })
  }
  if (record.state === "failed" || record.state === "intervention_required") {
    return Object.freeze({ disposition: "blocked", operation })
  }
  if (record.state === "not_required") {
    return Object.freeze({ disposition: "blocked", operation })
  }

  assertWellFormedString(request.observedPreconditionChecksum, "Observed precondition checksum")
  if (request.observedPreconditionChecksum !== planStep.preconditionChecksum) {
    resumeError("The step precondition does not match the sealed operation plan.")
  }
  assertDependenciesSucceeded(operation, planStep)
  if (planStep.leaseKey !== request.leaseProof.leaseKey) {
    resumeError("The step was invoked under the wrong lease key.")
  }
  assertLeaseAuthorized(request.lease, request.leaseProof, request.serverTimeMs)
  const irreversibleAuthorization = assertAuthorizationForInvocation(operation, planStep, request)
  const next: OperationStepRecord = Object.freeze({
    ...authorizationRecordFields(irreversibleAuthorization),
    activeAttemptId: request.attemptId,
    costCounters: record.costCounters,
    fencingToken: request.leaseProof.fencingToken,
    lastAttemptId: request.attemptId,
    progressCounters: record.progressCounters,
    startedAttempts: checkedAdd(record.startedAttempts, 1, "Step attempt counter"),
    state: "running",
  })
  return Object.freeze({
    disposition: "execute",
    operation: updateStep(operation, request.stepId, next),
  })
}

function assertActiveAttempt(
  record: OperationStepRecord,
  attemptId: string,
): asserts record is OperationStepRecord & {
  readonly activeAttemptId: string
  readonly fencingToken: number
  readonly lastAttemptId: string
} {
  assertWellFormedString(attemptId, "Attempt ID")
  if (record.state !== "running" || record.activeAttemptId !== attemptId) {
    resumeError("The result does not match the active step attempt.")
  }
  if (!record.fencingToken || !record.lastAttemptId) {
    interventionError("The active step attempt has incomplete fencing metadata.")
  }
}

function withCounterDeltas(
  record: OperationStepRecord,
  deltas: CounterDeltas | undefined,
): Pick<OperationStepRecord, "costCounters" | "progressCounters"> {
  return {
    costCounters: incrementCounters(record.costCounters, deltas?.cost),
    progressCounters: incrementCounters(record.progressCounters, deltas?.progress),
  }
}

export function recordStepSuccess(
  operation: OperationRecord,
  input: {
    readonly attemptId: string
    readonly counters?: CounterDeltas
    readonly observedPostconditionChecksum: string
    readonly resultChecksum: string
    readonly stepId: string
  },
): OperationRecord {
  const planStep = getPlanStep(operation, input.stepId)
  const record = getStepRecord(operation, input.stepId)
  assertWellFormedString(input.resultChecksum, "Step result checksum")
  assertWellFormedString(input.observedPostconditionChecksum, "Observed postcondition checksum")
  if (record.state === "succeeded") {
    if (
      record.resultChecksum === input.resultChecksum &&
      input.observedPostconditionChecksum === planStep.postconditionChecksum
    ) {
      return operation
    }
    interventionError("A duplicate step success contradicts the persisted logical result.")
  }
  assertActiveAttempt(record, input.attemptId)
  if (input.observedPostconditionChecksum !== planStep.postconditionChecksum) {
    interventionError("The step postcondition does not match the sealed operation plan.")
  }
  const next: OperationStepRecord = Object.freeze({
    ...retainedAuthorizationFields(operation, planStep, record),
    ...withCounterDeltas(record, input.counters),
    fencingToken: record.fencingToken,
    lastAttemptId: input.attemptId,
    resultChecksum: input.resultChecksum,
    startedAttempts: record.startedAttempts,
    state: "succeeded",
  })
  return updateStep(operation, input.stepId, next)
}

export function recordStepFailure(
  operation: OperationRecord,
  input: {
    readonly attemptId: string
    readonly counters?: CounterDeltas
    readonly errorChecksum: string
    readonly outcome: StepFailureOutcome
    readonly stepId: string
  },
): OperationRecord {
  const planStep = getPlanStep(operation, input.stepId)
  const record = getStepRecord(operation, input.stepId)
  assertActiveAttempt(record, input.attemptId)
  assertWellFormedString(input.errorChecksum, "Step error checksum")
  if (!(["definitely_not_applied", "permanent", "unknown"] as const).includes(input.outcome)) {
    configurationError("Step failure outcome is invalid.")
  }
  let state: OperationStepState
  if (input.outcome === "unknown") state = "unknown"
  else if (input.outcome === "permanent" || planStep.retryClassification === "never") {
    state = "failed"
  } else state = "retryable_failed"
  const next: OperationStepRecord = Object.freeze({
    ...retainedAuthorizationFields(operation, planStep, record),
    ...withCounterDeltas(record, input.counters),
    errorChecksum: input.errorChecksum,
    fencingToken: record.fencingToken,
    lastAttemptId: input.attemptId,
    startedAttempts: record.startedAttempts,
    state,
  })
  return updateStep(operation, input.stepId, next)
}

function recoverRunningStepAfterCrash(
  operation: OperationRecord,
  stepId: string,
  state: "retryable_failed" | "unknown",
  errorChecksum?: string,
): OperationRecord {
  const planStep = getPlanStep(operation, stepId)
  const record = getStepRecord(operation, stepId)
  if (record.state !== "running") {
    resumeError("Only a running step can be recovered as an unknown crash outcome.")
  }
  if (!record.activeAttemptId || !record.fencingToken || !record.lastAttemptId) {
    interventionError("A running step has incomplete crash-recovery metadata.")
  }
  const recovered: OperationStepRecord = {
    ...retainedAuthorizationFields(operation, planStep, record),
    costCounters: record.costCounters,
    ...(errorChecksum === undefined ? {} : { errorChecksum }),
    fencingToken: record.fencingToken,
    lastAttemptId: record.lastAttemptId,
    progressCounters: record.progressCounters,
    startedAttempts: record.startedAttempts,
    state:
      state === "retryable_failed" && planStep.retryClassification === "never" ? "failed" : state,
  }
  return updateStep(operation, stepId, recovered)
}

export function markRunningStepNotDispatchedAfterCrash(
  operation: OperationRecord,
  stepId: string,
  evidenceChecksum: string,
): OperationRecord {
  assertWellFormedString(evidenceChecksum, "Provider dispatch-absence evidence checksum")
  const recovered = recoverRunningStepAfterCrash(operation, stepId, "retryable_failed")
  const record = getStepRecord(recovered, stepId)
  return updateStep(
    recovered,
    stepId,
    Object.freeze({ ...record, reconciliationEvidenceChecksum: evidenceChecksum }),
  )
}

export function markRunningStepUnknownAfterCrash(
  operation: OperationRecord,
  stepId: string,
  errorChecksum?: string,
): OperationRecord {
  if (errorChecksum !== undefined) {
    assertWellFormedString(errorChecksum, "Crash-recovery error checksum")
  }
  return recoverRunningStepAfterCrash(operation, stepId, "unknown", errorChecksum)
}

export function markRunningStepsUnknownAfterCrash(operation: OperationRecord): OperationRecord {
  let next = operation
  for (const [stepId, record] of Object.entries(operation.steps)) {
    if (record.state === "running") next = markRunningStepUnknownAfterCrash(next, stepId)
  }
  return next
}

function reconciledStepRecord(
  operation: OperationRecord,
  planStep: OperationStepPlan,
  record: OperationStepRecord,
  fencingToken: number,
  lastAttemptId: string,
  counters: CounterDeltas | undefined,
  evidenceChecksum: string,
  state: OperationStepState,
  resultChecksum?: string,
): OperationStepRecord {
  return Object.freeze({
    ...retainedAuthorizationFields(operation, planStep, record),
    ...withCounterDeltas(record, counters),
    ...(record.errorChecksum ? { errorChecksum: record.errorChecksum } : {}),
    fencingToken,
    lastAttemptId,
    reconciliationEvidenceChecksum: evidenceChecksum,
    ...(resultChecksum ? { resultChecksum } : {}),
    startedAttempts: record.startedAttempts,
    state,
  })
}

export function recordStepReconciliation(
  operation: OperationRecord,
  input: {
    readonly counters?: CounterDeltas
    readonly evidenceChecksum: string
    readonly observedPostconditionChecksum?: string
    readonly outcome: StepReconciliationOutcome
    readonly resultChecksum?: string
    readonly stepId: string
  },
): OperationRecord {
  const planStep = getPlanStep(operation, input.stepId)
  const record = getStepRecord(operation, input.stepId)
  if (record.state !== "unknown") resumeError("Only an unknown step outcome can be reconciled.")
  if (!record.fencingToken || !record.lastAttemptId) {
    interventionError("The unknown step has incomplete reconciliation metadata.")
  }
  assertWellFormedString(input.evidenceChecksum, "Reconciliation evidence checksum")
  if (!(["applied", "indeterminate", "not_applied"] as const).includes(input.outcome)) {
    configurationError("Step reconciliation outcome is invalid.")
  }

  let state: OperationStepState
  let resultChecksum: string | undefined
  if (input.outcome === "applied") {
    if (!input.resultChecksum || !input.observedPostconditionChecksum) {
      interventionError("Applied reconciliation requires result and postcondition evidence.")
    }
    assertWellFormedString(input.resultChecksum, "Step result checksum")
    if (input.observedPostconditionChecksum !== planStep.postconditionChecksum) {
      interventionError("Reconciliation found a contradictory step postcondition.")
    }
    state = "succeeded"
    resultChecksum = input.resultChecksum
  } else if (input.outcome === "indeterminate") {
    state = "intervention_required"
  } else if (planStep.retryClassification === "never") {
    state = "failed"
  } else {
    state = "retryable_failed"
  }

  const next = reconciledStepRecord(
    operation,
    planStep,
    record,
    record.fencingToken,
    record.lastAttemptId,
    input.counters,
    input.evidenceChecksum,
    state,
    resultChecksum,
  )
  return updateStep(operation, input.stepId, next)
}

/**
 * Records terminal non-application for a saga-receipt step. Generic success means the receipt was
 * classified; the coupled saga projection remains the authority for the business outcome.
 */
function assertSagaClassificationMetadata(
  record: OperationStepRecord,
): asserts record is OperationStepRecord & {
  readonly fencingToken: number
  readonly lastAttemptId: string
} {
  if (!record.fencingToken || !record.lastAttemptId) {
    interventionError("The saga step has incomplete classification metadata.")
  }
}

export function recordSagaStepTerminalClassification(
  operation: OperationRecord,
  input: {
    readonly counters?: CounterDeltas
    readonly outcome: "not_applied"
    readonly receiptOutcomeChecksum: string
    readonly stepId: string
  },
): OperationRecord {
  const planStep = getPlanStep(operation, input.stepId)
  if (planStep.effectProtocol !== "saga_receipt") {
    configurationError("Terminal saga classification requires a saga-receipt step.")
  }
  if (input.outcome !== "not_applied") {
    configurationError("Terminal saga classification outcome is invalid.")
  }
  assertWellFormedString(input.receiptOutcomeChecksum, "Terminal saga receipt-outcome checksum")
  const record = getStepRecord(operation, input.stepId)
  if (
    record.state === "succeeded" ||
    record.state === "unknown" ||
    record.state === "retryable_failed"
  ) {
    assertSagaClassificationMetadata(record)
  }
  retainedAuthorizationFields(operation, planStep, record)
  loadPersistedStepRecord(record, planStep, record.irreversibleAuthorization)
  if (record.state === "succeeded") {
    if (
      record.reconciliationEvidenceChecksum === input.receiptOutcomeChecksum &&
      record.resultChecksum === input.receiptOutcomeChecksum
    ) {
      return operation
    }
    interventionError("A duplicate terminal saga classification contradicts durable evidence.")
  }
  if (record.state !== "unknown" && record.state !== "retryable_failed") {
    resumeError("Only an unknown or retryable saga step can receive terminal classification.")
  }
  assertSagaClassificationMetadata(record)
  if (
    record.state === "retryable_failed" &&
    input.receiptOutcomeChecksum !== (record.reconciliationEvidenceChecksum ?? record.errorChecksum)
  ) {
    interventionError(
      "The terminal saga classification contradicts durable non-application evidence.",
    )
  }

  return updateStep(
    operation,
    input.stepId,
    reconciledStepRecord(
      operation,
      planStep,
      record,
      record.fencingToken,
      record.lastAttemptId,
      input.counters,
      input.receiptOutcomeChecksum,
      "succeeded",
      input.receiptOutcomeChecksum,
    ),
  )
}

function auditChecksumValues(event: Omit<AuditEvent, "eventHash">): readonly string[] {
  return [
    String(event.schemaVersion),
    String(event.sequence),
    event.previousHash ?? "",
    event.serverTimeMs.toString(10),
    event.environmentId,
    event.actorChecksum,
    event.operationId,
    event.stepId ?? "",
    event.idempotencyKey,
    event.eventType,
    event.payloadChecksum,
    event.fencingToken?.toString(10) ?? "",
  ]
}

export function encodeAuditEventChecksumInput(event: Omit<AuditEvent, "eventHash">): Uint8Array {
  return frameStrings(AUDIT_EVENT_DOMAIN, auditChecksumValues(event))
}

function validateAuditInput(input: AuditEventInput): void {
  assertWellFormedString(input.actorChecksum, "Audit actor checksum")
  assertWellFormedString(input.environmentId, "Audit environment ID")
  assertWellFormedString(input.eventType, "Audit event type")
  assertWellFormedString(input.idempotencyKey, "Audit idempotency key")
  assertWellFormedString(input.operationId, "Audit operation ID")
  assertWellFormedString(input.payloadChecksum, "Audit payload checksum")
  if (input.stepId !== null) assertWellFormedString(input.stepId, "Audit step ID")
  if (input.fencingToken !== null) {
    assertPositiveSafeInteger(input.fencingToken, "Audit fencing token")
  }
  assertServerTime(input.serverTimeMs, "Audit server time")
}

export async function appendAuditEvent(
  previous: AuditEvent | undefined,
  input: AuditEventInput,
  digest: DigestFunction,
): Promise<AuditEvent> {
  const captured = captureConfiguration(
    { input, previous },
    "Audit append input could not be captured safely.",
  )
  if (
    !plainRecord(captured.input) ||
    Object.keys(captured.input).length !== AUDIT_EVENT_INPUT_KEYS.size ||
    !Object.keys(captured.input).every((key) => AUDIT_EVENT_INPUT_KEYS.has(key))
  ) {
    configurationError("Audit input fields are malformed.")
  }
  validateAuditInput(captured.input)
  const verifiedPrevious =
    captured.previous === undefined
      ? undefined
      : await loadAuditEventSnapshot(captured.previous, digest)
  if (verifiedPrevious && captured.input.serverTimeMs < verifiedPrevious.serverTimeMs) {
    configurationError("Audit server time cannot decrease.")
  }
  const sequence = checkedAdd(verifiedPrevious?.sequence ?? 0, 1, "Audit sequence")
  const event = Object.freeze({
    actorChecksum: captured.input.actorChecksum,
    environmentId: captured.input.environmentId,
    eventType: captured.input.eventType,
    fencingToken: captured.input.fencingToken,
    idempotencyKey: captured.input.idempotencyKey,
    operationId: captured.input.operationId,
    payloadChecksum: captured.input.payloadChecksum,
    previousHash: verifiedPrevious?.eventHash ?? null,
    schemaVersion: 1 as const,
    sequence,
    serverTimeMs: captured.input.serverTimeMs,
    stepId: captured.input.stepId,
  })
  const eventHash = await digestChecksum(
    encodeAuditEventChecksumInput(event),
    digest,
    "Audit event checksum",
  )
  return Object.freeze({ ...event, eventHash })
}

const PERSISTED_AUDIT_EVENT_KEYS = new Set([
  "actorChecksum",
  "environmentId",
  "eventHash",
  "eventType",
  "fencingToken",
  "idempotencyKey",
  "operationId",
  "payloadChecksum",
  "previousHash",
  "schemaVersion",
  "sequence",
  "serverTimeMs",
  "stepId",
])

async function loadAuditEventSnapshot(
  snapshot: unknown,
  digest: DigestFunction,
): Promise<AuditEvent> {
  persistedInvariant(plainRecord(snapshot), "The persisted audit event is malformed.")
  persistedInvariant(
    Object.keys(snapshot).length === PERSISTED_AUDIT_EVENT_KEYS.size &&
      Object.keys(snapshot).every((key) => PERSISTED_AUDIT_EVENT_KEYS.has(key)),
    "The persisted audit event fields are malformed.",
  )
  persistedInvariant(
    snapshot.schemaVersion === 1,
    "The persisted audit event version is unsupported.",
  )
  persistedInvariant(
    typeof snapshot.sequence === "number" &&
      Number.isSafeInteger(snapshot.sequence) &&
      snapshot.sequence >= 1,
    "The persisted audit sequence is malformed.",
  )
  persistedInvariant(
    typeof snapshot.serverTimeMs === "number" &&
      Number.isSafeInteger(snapshot.serverTimeMs) &&
      snapshot.serverTimeMs >= 0,
    "The persisted audit server time is malformed.",
  )
  const actorChecksum = persistedOptionalString(snapshot.actorChecksum, "Audit actor checksum")
  const environmentId = persistedOptionalString(snapshot.environmentId, "Audit environment ID")
  const eventHash = persistedOptionalString(snapshot.eventHash, "Audit event checksum")
  const eventType = persistedOptionalString(snapshot.eventType, "Audit event type")
  const idempotencyKey = persistedOptionalString(snapshot.idempotencyKey, "Audit idempotency key")
  const operationId = persistedOptionalString(snapshot.operationId, "Audit operation ID")
  const payloadChecksum = persistedOptionalString(
    snapshot.payloadChecksum,
    "Audit payload checksum",
  )
  const previousHash =
    snapshot.previousHash === null
      ? null
      : persistedOptionalString(snapshot.previousHash, "Previous audit checksum")
  const stepId =
    snapshot.stepId === null ? null : persistedOptionalString(snapshot.stepId, "Audit step ID")
  persistedInvariant(
    actorChecksum !== undefined &&
      environmentId !== undefined &&
      eventHash !== undefined &&
      eventType !== undefined &&
      idempotencyKey !== undefined &&
      operationId !== undefined &&
      payloadChecksum !== undefined &&
      previousHash !== undefined &&
      stepId !== undefined,
    "The persisted audit event is incomplete.",
  )
  const fencingToken = snapshot.fencingToken
  persistedInvariant(
    fencingToken === null ||
      (typeof fencingToken === "number" && Number.isSafeInteger(fencingToken) && fencingToken >= 1),
    "The persisted audit fencing token is malformed.",
  )
  const event: Omit<AuditEvent, "eventHash"> = Object.freeze({
    actorChecksum,
    environmentId,
    eventType,
    fencingToken,
    idempotencyKey,
    operationId,
    payloadChecksum,
    previousHash,
    schemaVersion: 1,
    sequence: snapshot.sequence,
    serverTimeMs: snapshot.serverTimeMs,
    stepId,
  })
  const actual = await digestChecksum(
    encodeAuditEventChecksumInput(event),
    digest,
    "Audit event checksum",
  )
  persistedInvariant(actual === eventHash, "The persisted audit event checksum does not match.")
  return Object.freeze({ ...event, eventHash })
}

export async function loadAuditEvent(
  candidate: unknown,
  digest: DigestFunction,
): Promise<AuditEvent> {
  const snapshot: unknown = capturePersisted(
    candidate,
    "The persisted audit event could not be captured safely.",
  )
  return loadAuditEventSnapshot(snapshot, digest)
}

export async function verifyAuditChain(
  events: readonly AuditEvent[],
  digest: DigestFunction,
): Promise<boolean> {
  let snapshot: unknown
  try {
    snapshot = structuredClone(events)
  } catch {
    return false
  }
  if (!exactDenseArray(snapshot)) return false
  let previous: AuditEvent | undefined
  for (const candidate of snapshot) {
    let event: AuditEvent
    try {
      event = await loadAuditEventSnapshot(candidate, digest)
    } catch {
      return false
    }
    if (event.sequence !== (previous?.sequence ?? 0) + 1) return false
    if (event.previousHash !== (previous?.eventHash ?? null)) return false
    if (previous && event.serverTimeMs < previous.serverTimeMs) return false
    previous = event
  }
  return true
}
