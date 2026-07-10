import { NozzleError } from "./errors.js"

const OPERATION_PLAN_DOMAIN = "nozzle.operation-plan.v1"
const IRREVERSIBLE_AUTHORIZATION_DOMAIN = "nozzle.irreversible-authorization.v1"
const AUDIT_EVENT_DOMAIN = "nozzle.audit-event.v1"
const TRUSTED_OPERATION_PLANS = new WeakSet<OperationPlan>()
const TRUSTED_IRREVERSIBLE_AUTHORIZATIONS = new WeakSet<IrreversibleAuthorization>()

export const OPERATION_STEP_STATES = [
  "pending",
  "running",
  "retryable_failed",
  "unknown",
  "succeeded",
  "failed",
  "intervention_required",
] as const

export type OperationStepState = (typeof OPERATION_STEP_STATES)[number]
export type RetryClassification = "idempotent" | "never" | "reconcile_first"
export type CheckpointKind = "irreversible" | "reversible"
export type DigestFunction = (input: Uint8Array) => Promise<string> | string

export interface OperationStepPlanInput {
  readonly checkpoint: CheckpointKind
  readonly dependsOn?: readonly string[]
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
  readonly dependsOn: readonly string[]
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
  const dependsOn = [...new Set(step.dependsOn ?? [])].sort()
  if (dependsOn.length !== (step.dependsOn ?? []).length || dependsOn.includes("")) {
    configurationError("Step dependencies must be unique and non-empty.")
  }
  for (const dependency of dependsOn) assertWellFormedString(dependency, "Step dependency")
  return Object.freeze({ ...step, dependsOn: Object.freeze(dependsOn) })
}

function normalizePlan(input: OperationPlanInput): Omit<OperationPlan, "planChecksum"> {
  assertWellFormedString(input.operationId, "Operation ID")
  assertWellFormedString(input.operationType, "Operation type")
  assertWellFormedString(input.idempotencyKey, "Operation idempotency key")
  assertWellFormedString(input.inputChecksum, "Operation input checksum")
  assertWellFormedString(input.capabilitySnapshotChecksum, "Capability snapshot checksum")
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
  const plan = normalizePlan(input)
  return frameStrings(OPERATION_PLAN_DOMAIN, planChecksumValues(plan))
}

export async function sealOperationPlan(
  input: OperationPlanInput,
  digest: DigestFunction,
): Promise<OperationPlan> {
  const plan = normalizePlan(input)
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
  if (candidate.schemaVersion !== 1) {
    interventionError("The persisted operation plan version is unsupported.")
  }
  assertWellFormedString(candidate.planChecksum, "Operation plan checksum")
  const normalized = normalizePlan({
    capabilitySnapshotChecksum: candidate.capabilitySnapshotChecksum,
    idempotencyKey: candidate.idempotencyKey,
    inputChecksum: candidate.inputChecksum,
    operationId: candidate.operationId,
    operationType: candidate.operationType,
    steps: candidate.steps,
  })
  const actual = await digestChecksum(
    frameStrings(OPERATION_PLAN_DOMAIN, planChecksumValues(normalized)),
    digest,
    "Operation plan checksum",
  )
  if (actual !== candidate.planChecksum) {
    interventionError("The persisted operation plan checksum does not match its contents.")
  }
  const loaded = Object.freeze({ ...normalized, planChecksum: candidate.planChecksum })
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
  "lastAttemptId",
  "progressCounters",
  "reconciliationEvidenceChecksum",
  "resultChecksum",
  "startedAttempts",
  "state",
])

function loadPersistedStepRecord(value: unknown, planStep: OperationStepPlan): OperationStepRecord {
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
  const pending = state === "pending"
  persistedInvariant(
    pending === (value.startedAttempts === 0),
    "Persisted pending state contradicts the attempt count.",
  )
  persistedInvariant(
    pending === (lastAttemptId === undefined) && pending === (fencingToken === undefined),
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
  if (planStep.checkpoint === "reversible") {
    persistedInvariant(
      authorizationChecksum === undefined,
      "A reversible step persisted irreversible authorization.",
    )
  } else {
    persistedInvariant(
      pending === (authorizationChecksum === undefined),
      "Persisted irreversible authorization state is inconsistent.",
    )
  }
  if (pending) {
    persistedInvariant(
      Object.keys(costCounters).length === 0 && Object.keys(progressCounters).length === 0,
      "A pending step persisted progress or cost counters.",
    )
  }
  return Object.freeze({
    ...(activeAttemptId === undefined ? {} : { activeAttemptId }),
    ...(authorizationChecksum === undefined ? {} : { authorizationChecksum }),
    costCounters,
    ...(errorChecksum === undefined ? {} : { errorChecksum }),
    ...(fencingToken === undefined ? {} : { fencingToken }),
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
  persistedInvariant(plainRecord(candidate), "The persisted operation record is malformed.")
  persistedInvariant(
    Object.keys(candidate).every((key) => key === "plan" || key === "steps"),
    "The persisted operation record contains unknown fields.",
  )
  persistedInvariant(plainRecord(candidate.plan), "The persisted operation plan is malformed.")
  persistedInvariant(plainRecord(candidate.steps), "The persisted operation steps are malformed.")
  const plan = await loadOperationPlan(candidate.plan as unknown as OperationPlan, digest)
  const expectedStepIds = plan.steps.map((step) => step.stepId)
  const persistedStepIds = Object.keys(candidate.steps).sort()
  persistedInvariant(
    expectedStepIds.length === persistedStepIds.length &&
      expectedStepIds.every((stepId, index) => stepId === persistedStepIds[index]),
    "Persisted operation step membership does not match the immutable plan.",
  )
  const steps: Record<string, OperationStepRecord> = {}
  for (const planStep of plan.steps) {
    steps[planStep.stepId] = loadPersistedStepRecord(candidate.steps[planStep.stepId], planStep)
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
  if (states.every((state) => state === "succeeded")) return "succeeded"
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
  assertWellFormedString(input.authorizationId, "Authorization ID")
  assertWellFormedString(input.actorChecksum, "Authorization actor checksum")
  assertWellFormedString(input.decisionChecksum, "Authorization decision checksum")
  const step = plan.steps.find((candidate) => candidate.stepId === input.stepId)
  if (!step) configurationError("The authorized step is not part of the operation plan.")
  if (step.checkpoint !== "irreversible") {
    configurationError("Irreversible authorization can only seal an irreversible step.")
  }
  assertLeaseAuthorized(input.lease, input.leaseProof, input.sealedAtServerTimeMs)
  if (step.leaseKey !== input.lease.leaseKey)
    configurationError("Step and lease keys do not match.")

  const authorization = Object.freeze({
    actorChecksum: input.actorChecksum,
    authorizationId: input.authorizationId,
    decisionChecksum: input.decisionChecksum,
    fencingToken: input.leaseProof.fencingToken,
    holderId: input.leaseProof.holderId,
    leaseAcquisitionId: input.leaseProof.acquisitionId,
    leaseKey: input.leaseProof.leaseKey,
    operationId: plan.operationId,
    planChecksum: plan.planChecksum,
    sealedAtServerTimeMs: input.sealedAtServerTimeMs,
    schemaVersion: 1 as const,
    stepId: step.stepId,
    stepInputChecksum: step.inputChecksum,
  })
  const authorizationChecksum = await digestChecksum(
    encodeIrreversibleAuthorizationChecksumInput(authorization),
    digest,
    "Irreversible authorization checksum",
  )
  const sealed = Object.freeze({ ...authorization, authorizationChecksum })
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
  if (candidate.schemaVersion !== 1) {
    interventionError("The persisted irreversible authorization version is unsupported.")
  }
  assertWellFormedString(candidate.actorChecksum, "Authorization actor checksum")
  assertWellFormedString(candidate.authorizationChecksum, "Authorization checksum")
  assertWellFormedString(candidate.authorizationId, "Authorization ID")
  assertWellFormedString(candidate.decisionChecksum, "Authorization decision checksum")
  assertPositiveSafeInteger(candidate.fencingToken, "Authorization fencing token")
  assertWellFormedString(candidate.holderId, "Authorization holder ID")
  assertWellFormedString(candidate.leaseAcquisitionId, "Authorization lease acquisition ID")
  assertWellFormedString(candidate.leaseKey, "Authorization lease key")
  assertWellFormedString(candidate.operationId, "Authorization operation ID")
  assertWellFormedString(candidate.planChecksum, "Authorization plan checksum")
  assertServerTime(candidate.sealedAtServerTimeMs, "Authorization seal time")
  assertWellFormedString(candidate.stepId, "Authorization step ID")
  assertWellFormedString(candidate.stepInputChecksum, "Authorization step input checksum")
  if (!(await verifyIrreversibleAuthorizationChecksum(candidate, digest))) {
    interventionError("The persisted irreversible authorization checksum does not match.")
  }
  const loaded = Object.freeze({ ...candidate })
  TRUSTED_IRREVERSIBLE_AUTHORIZATIONS.add(loaded)
  return loaded
}

function assertAuthorizationForInvocation(
  operation: OperationRecord,
  step: OperationStepPlan,
  request: StepInvocationRequest,
): string | undefined {
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
  return authorization.authorizationChecksum
}

function assertDependenciesSucceeded(operation: OperationRecord, step: OperationStepPlan): void {
  for (const dependency of step.dependsOn) {
    if (operation.steps[dependency]?.state !== "succeeded") {
      resumeError("A step dependency has not succeeded.", { dependency, stepId: step.stepId })
    }
  }
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

  assertWellFormedString(request.observedPreconditionChecksum, "Observed precondition checksum")
  if (request.observedPreconditionChecksum !== planStep.preconditionChecksum) {
    resumeError("The step precondition does not match the sealed operation plan.")
  }
  assertDependenciesSucceeded(operation, planStep)
  if (planStep.leaseKey !== request.leaseProof.leaseKey) {
    resumeError("The step was invoked under the wrong lease key.")
  }
  assertLeaseAuthorized(request.lease, request.leaseProof, request.serverTimeMs)
  const authorizationChecksum = assertAuthorizationForInvocation(operation, planStep, request)
  const next: OperationStepRecord = Object.freeze({
    ...(authorizationChecksum ? { authorizationChecksum } : {}),
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
    ...(record.authorizationChecksum
      ? { authorizationChecksum: record.authorizationChecksum }
      : {}),
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
    ...(record.authorizationChecksum
      ? { authorizationChecksum: record.authorizationChecksum }
      : {}),
    ...withCounterDeltas(record, input.counters),
    errorChecksum: input.errorChecksum,
    fencingToken: record.fencingToken,
    lastAttemptId: input.attemptId,
    startedAttempts: record.startedAttempts,
    state,
  })
  return updateStep(operation, input.stepId, next)
}

export function markRunningStepUnknownAfterCrash(
  operation: OperationRecord,
  stepId: string,
): OperationRecord {
  const record = getStepRecord(operation, stepId)
  if (record.state !== "running") {
    resumeError("Only a running step can be recovered as an unknown crash outcome.")
  }
  if (!record.activeAttemptId || !record.fencingToken || !record.lastAttemptId) {
    interventionError("A running step has incomplete crash-recovery metadata.")
  }
  const unknown: OperationStepRecord = {
    ...(record.authorizationChecksum
      ? { authorizationChecksum: record.authorizationChecksum }
      : {}),
    costCounters: record.costCounters,
    fencingToken: record.fencingToken,
    lastAttemptId: record.lastAttemptId,
    progressCounters: record.progressCounters,
    startedAttempts: record.startedAttempts,
    state: "unknown",
  }
  return updateStep(operation, stepId, unknown)
}

export function markRunningStepsUnknownAfterCrash(operation: OperationRecord): OperationRecord {
  let next = operation
  for (const [stepId, record] of Object.entries(operation.steps)) {
    if (record.state === "running") next = markRunningStepUnknownAfterCrash(next, stepId)
  }
  return next
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

  const next: OperationStepRecord = Object.freeze({
    ...(record.authorizationChecksum
      ? { authorizationChecksum: record.authorizationChecksum }
      : {}),
    ...withCounterDeltas(record, input.counters),
    ...(record.errorChecksum ? { errorChecksum: record.errorChecksum } : {}),
    fencingToken: record.fencingToken,
    lastAttemptId: record.lastAttemptId,
    reconciliationEvidenceChecksum: input.evidenceChecksum,
    ...(resultChecksum ? { resultChecksum } : {}),
    startedAttempts: record.startedAttempts,
    state,
  })
  return updateStep(operation, input.stepId, next)
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
  validateAuditInput(input)
  if (previous && input.serverTimeMs < previous.serverTimeMs) {
    configurationError("Audit server time cannot decrease.")
  }
  const sequence = checkedAdd(previous?.sequence ?? 0, 1, "Audit sequence")
  const event = Object.freeze({
    ...input,
    previousHash: previous?.eventHash ?? null,
    schemaVersion: 1 as const,
    sequence,
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

export async function loadAuditEvent(
  candidate: unknown,
  digest: DigestFunction,
): Promise<AuditEvent> {
  persistedInvariant(plainRecord(candidate), "The persisted audit event is malformed.")
  persistedInvariant(
    Object.keys(candidate).every((key) => PERSISTED_AUDIT_EVENT_KEYS.has(key)),
    "The persisted audit event contains unknown fields.",
  )
  persistedInvariant(
    candidate.schemaVersion === 1,
    "The persisted audit event version is unsupported.",
  )
  persistedInvariant(
    typeof candidate.sequence === "number" &&
      Number.isSafeInteger(candidate.sequence) &&
      candidate.sequence >= 1,
    "The persisted audit sequence is malformed.",
  )
  persistedInvariant(
    typeof candidate.serverTimeMs === "number" &&
      Number.isSafeInteger(candidate.serverTimeMs) &&
      candidate.serverTimeMs >= 0,
    "The persisted audit server time is malformed.",
  )
  const actorChecksum = persistedOptionalString(candidate.actorChecksum, "Audit actor checksum")
  const environmentId = persistedOptionalString(candidate.environmentId, "Audit environment ID")
  const eventHash = persistedOptionalString(candidate.eventHash, "Audit event checksum")
  const eventType = persistedOptionalString(candidate.eventType, "Audit event type")
  const idempotencyKey = persistedOptionalString(candidate.idempotencyKey, "Audit idempotency key")
  const operationId = persistedOptionalString(candidate.operationId, "Audit operation ID")
  const payloadChecksum = persistedOptionalString(
    candidate.payloadChecksum,
    "Audit payload checksum",
  )
  const previousHash =
    candidate.previousHash === null
      ? null
      : persistedOptionalString(candidate.previousHash, "Previous audit checksum")
  const stepId =
    candidate.stepId === null ? null : persistedOptionalString(candidate.stepId, "Audit step ID")
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
  const fencingToken = candidate.fencingToken
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
    sequence: candidate.sequence,
    serverTimeMs: candidate.serverTimeMs,
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

export async function verifyAuditChain(
  events: readonly AuditEvent[],
  digest: DigestFunction,
): Promise<boolean> {
  let previous: AuditEvent | undefined
  for (const event of events) {
    if (event.sequence !== (previous?.sequence ?? 0) + 1) return false
    if (event.previousHash !== (previous?.eventHash ?? null)) return false
    if (previous && event.serverTimeMs < previous.serverTimeMs) return false
    const { eventHash, ...unsigned } = event
    const actual = await digestChecksum(
      encodeAuditEventChecksumInput(unsigned),
      digest,
      "Audit event checksum",
    )
    if (actual !== eventHash) return false
    previous = event
  }
  return true
}
