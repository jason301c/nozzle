import {
  type VerifiedReaderDeploymentCapability,
  type VerifiedReaderDeploymentEvidence,
  type VerifiedReaderDeploymentStabilityCapability,
  type VerifiedReaderDeploymentStabilityEvidence,
  verifiedReaderDeploymentEvidence,
  verifiedReaderDeploymentStabilityEvidence,
} from "@nozzle/cloudflare"
import { type DigestFunction, NozzleError } from "@nozzle/core"
import type {
  ControlRunResult,
  ControlStatement,
  TransactionalControlDatabase,
} from "./database.js"
import {
  type ActiveReaderDeploymentInput,
  type CanonicalReaderBarrierState,
  D1ReaderBarrierStore,
  type ReaderBarrierCapability,
  type ReaderBarrierReceipt,
  type ReaderVersionAttestationInput,
  readerBarrierCapabilityState,
  verifyReaderDeploymentBarrier,
} from "./reader-barrier-store.js"

const SIGNED_EVIDENCE_DOMAIN = "nozzle.reader-barrier-signed-evidence.v1"
const STABILITY_IDENTITY_DOMAIN = "nozzle.reader-barrier-stability-identity.v1"
const VERIFICATION_DOMAIN = "nozzle.reader-barrier-verification.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const ACCOUNT_ID = /^[0-9a-f]{32}$/u
const MAX_EVIDENCE_BYTES = 1_048_576
const MAX_JSON_DEPTH = 16
const MAX_JSON_MEMBERS = 16_384
const MAX_STABILITY_WINDOW_MS = 5 * 60 * 1_000
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

type JsonValue = boolean | null | number | string | JsonObject | readonly JsonValue[]
interface JsonObject {
  readonly [key: string]: JsonValue
}

interface SignedSourceState {
  readonly evidence: VerifiedReaderDeploymentEvidence
  readonly evidenceChecksum: string
  readonly maxStabilityWindowMs: number
  readonly priorArtifactObservations: JsonValue
  readonly priorDeploymentObservations: JsonValue
  readonly priorEvidenceChecksum: string
  readonly priorObservedFromMs: number
  readonly priorObservedThroughMs: number
  readonly priorVerifiedAtMs: number
  readonly stabilityIdentityChecksum: string
}

interface VerificationState {
  readonly accountId: string
  readonly audience: string
  readonly evidenceJson: string
  readonly readerBarrierChecksum: string
  readonly signedEvidenceChecksum: string
  readonly sourceEvidence: VerifiedReaderDeploymentEvidence
  readonly sourceVerifiedAtMs: number
  readonly stability?: VerificationStabilityState
  readonly verificationChecksum: string
  readonly verifiedAtMs: number
}

interface VerificationStabilityState {
  readonly identityChecksum: string
  readonly maxStabilityWindowMs: number
  readonly priorEvidenceChecksum: string
  readonly priorObservedFromMs: number
  readonly priorObservedThroughMs: number
  readonly priorVerifiedAtMs: number
}

interface VerificationRow {
  readonly evidence_json: unknown
  readonly protocol_version: unknown
  readonly reader_barrier_checksum: unknown
  readonly verification_checksum: unknown
  readonly verified_at_ms: unknown
}

interface ServerTimeRow {
  readonly verified_at_ms: unknown
}

export interface SignedReaderBarrierReceipt extends ReaderBarrierReceipt {
  readonly accountId: string
  readonly audience: string
  readonly signedEvidenceChecksum: string
  readonly sourceVerifiedAtMs: number
  readonly stability?: Readonly<{
    readonly identityChecksum: string
    readonly maxStabilityWindowMs: number
    readonly priorSignedEvidenceChecksum: string
    readonly priorObservedFromMs: number
    readonly priorObservedThroughMs: number
    readonly priorVerifiedAtMs: number
  }>
  readonly verificationChecksum: string
}

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
}

function resume(message: string): never {
  throw new NozzleError("OperationResumeRequiredError", message)
}

function plainRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) === Object.prototype
  )
}

function exactRecord(
  value: unknown,
  keys: readonly string[],
  label: string,
): Record<string, unknown> {
  if (!plainRecord(value)) configuration(`${label} must be a plain object.`)
  const actual = Object.keys(value).sort()
  const expected = [...keys].sort()
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    configuration(`${label} has an unsupported shape.`)
  }
  return value
}

function captured<T>(value: T, label: string): T {
  try {
    return structuredClone(value)
  } catch {
    return configuration(`${label} could not be captured safely.`)
  }
}

function capturedPersisted<T extends object>(value: T, label: string): T {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return intervention(`${label} could not be captured safely.`)
  }
  if (!plainRecord(snapshot)) return intervention(`${label} is malformed.`)
  return snapshot as T
}

function wellFormed(value: string, label: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index)
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1)
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        configuration(`${label} cannot contain an unpaired UTF-16 surrogate.`)
      }
      index += 1
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      configuration(`${label} cannot contain an unpaired UTF-16 surrogate.`)
    }
  }
}

function canonicalValue(value: unknown, label: string, depth = 0): JsonValue {
  if (depth > MAX_JSON_DEPTH) configuration(`${label} exceeds the JSON depth limit.`)
  if (value === null || typeof value === "boolean") return value
  if (typeof value === "string") {
    wellFormed(value, label)
    return value
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      configuration(`${label} contains a noncanonical number.`)
    }
    return value
  }
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length || value.length > MAX_JSON_MEMBERS) {
      configuration(`${label} must be a bounded dense array.`)
    }
    return Object.freeze(
      value.map((entry, index) => canonicalValue(entry, `${label}[${index}]`, depth + 1)),
    )
  }
  if (!plainRecord(value)) configuration(`${label} contains a non-JSON value.`)
  const keys = Object.keys(value).sort()
  if (keys.length > MAX_JSON_MEMBERS) configuration(`${label} has too many object members.`)
  const output: Record<string, JsonValue> = {}
  for (const key of keys) {
    wellFormed(key, `${label} key`)
    const entry = value[key]
    if (entry === undefined) configuration(`${label} contains an undefined value.`)
    output[key] = canonicalValue(entry, `${label}.${key}`, depth + 1)
  }
  return Object.freeze(output)
}

function canonicalJson(value: unknown, label: string): string {
  return JSON.stringify(canonicalValue(value, label))
}

function frame(domain: string, value: string): Uint8Array {
  const parts = [domain, value].map((part) => new TextEncoder().encode(part))
  const output = new Uint8Array(parts.reduce((total, part) => total + 4 + part.byteLength, 0))
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

async function digestText(
  digest: DigestFunction,
  domain: string,
  value: string,
  label: string,
): Promise<string> {
  const result = await digest(frame(domain, value).slice())
  if (typeof result !== "string" || !CHECKSUM.test(result)) {
    configuration(`${label} digest must return a lowercase SHA-256 checksum.`)
  }
  return result
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength
}

function persistedInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    return intervention(`${label} is malformed.`)
  }
  return value as number
}

function persistedChecksum(value: unknown, label: string): string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    return intervention(`${label} is malformed.`)
  }
  return value
}

function persistedText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) return intervention(`${label} is malformed.`)
  return value
}

function parsedJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value) as unknown
  } catch {
    return intervention(`${label} is not valid JSON.`)
  }
}

function normalizedInput(evidence: VerifiedReaderDeploymentEvidence): {
  readonly attestations: readonly ReaderVersionAttestationInput[]
  readonly deployments: readonly ActiveReaderDeploymentInput[]
  readonly expectedScriptNames: readonly string[]
} {
  return {
    attestations: evidence.attestations.map((attestation) => ({
      artifactChecksum: attestation.artifactChecksum,
      controlSchemaMax: attestation.controlSchemaMax,
      controlSchemaMin: attestation.controlSchemaMin,
      outcomePayloadReaderMax: attestation.outcomePayloadReaderMax,
      outcomePayloadReaderMin: attestation.outcomePayloadReaderMin,
      scriptName: attestation.scriptName,
      versionId: attestation.versionId,
    })),
    deployments: evidence.deployments.map((deployment) => ({
      deploymentId: deployment.deploymentId,
      scriptName: deployment.scriptName,
      versions: deployment.versions.map((version) => ({ ...version })),
    })),
    expectedScriptNames: [...evidence.expectedScriptNames],
  }
}

function sourceEvidence(value: unknown, label: string): VerifiedReaderDeploymentEvidence {
  const root = exactRecord(
    value,
    [
      "accountId",
      "artifacts",
      "attestations",
      "audience",
      "deployments",
      "expectedScriptNames",
      "observedFromMs",
      "observedThroughMs",
      "schemaVersion",
      "verifiedAtMs",
    ],
    label,
  )
  if (
    root.schemaVersion !== 1 ||
    typeof root.accountId !== "string" ||
    !ACCOUNT_ID.test(root.accountId) ||
    typeof root.audience !== "string" ||
    root.audience.length === 0
  ) {
    configuration(`${label} has a malformed identity.`)
  }
  canonicalValue(value, label)
  return value as VerifiedReaderDeploymentEvidence
}

function stabilityIdentity(evidence: VerifiedReaderDeploymentEvidence): JsonValue {
  return [
    evidence.schemaVersion,
    evidence.accountId,
    evidence.audience,
    evidence.expectedScriptNames,
    evidence.deployments.map((deployment) => [
      deployment.scriptName,
      deployment.deploymentId,
      deployment.createdAtMs,
      deployment.versions.map((version) => [version.versionId, version.weightBps]),
    ]),
    evidence.artifacts.map((artifact) => [
      artifact.scriptName,
      artifact.versionId,
      artifact.artifactChecksum,
    ]),
    evidence.attestations.map((attestation) => [
      attestation.scriptName,
      attestation.versionId,
      attestation.artifactChecksum,
      attestation.controlSchemaMin,
      attestation.controlSchemaMax,
      attestation.outcomePayloadReaderMin,
      attestation.outcomePayloadReaderMax,
      attestation.issuedAtMs,
      attestation.expiresAtMs,
      attestation.keyId,
      attestation.publicKeyBase64Url,
      attestation.signature,
    ]),
  ]
}

function ownedStabilityEvidence(
  capability: VerifiedReaderDeploymentStabilityCapability,
): VerifiedReaderDeploymentStabilityEvidence {
  const snapshot = captured(
    verifiedReaderDeploymentStabilityEvidence(capability),
    "Verified reader-deployment stability evidence",
  )
  const root = exactRecord(
    snapshot,
    [
      "after",
      "before",
      "firstVerifiedAtMs",
      "maxStabilityWindowMs",
      "observedFromMs",
      "observedThroughMs",
      "schemaVersion",
      "verifiedAtMs",
    ],
    "Verified reader-deployment stability evidence",
  )
  const before = sourceEvidence(root.before, "First signed reader observation")
  const after = sourceEvidence(root.after, "Second signed reader observation")
  if (
    root.schemaVersion !== 1 ||
    root.firstVerifiedAtMs !== before.verifiedAtMs ||
    root.observedFromMs !== before.observedFromMs ||
    root.observedThroughMs !== after.observedThroughMs ||
    root.verifiedAtMs !== after.verifiedAtMs ||
    !Number.isSafeInteger(root.maxStabilityWindowMs) ||
    (root.maxStabilityWindowMs as number) < 1 ||
    (root.maxStabilityWindowMs as number) > MAX_STABILITY_WINDOW_MS ||
    after.observedFromMs < before.verifiedAtMs ||
    after.verifiedAtMs - before.observedFromMs > (root.maxStabilityWindowMs as number) ||
    canonicalJson(stabilityIdentity(before), "First reader stability identity") !==
      canonicalJson(stabilityIdentity(after), "Second reader stability identity")
  ) {
    configuration("Verified reader-deployment stability evidence is contradictory.")
  }
  return snapshot
}

async function stableSourceState(
  capability: VerifiedReaderDeploymentStabilityCapability,
  digest: DigestFunction,
): Promise<SignedSourceState> {
  const stable = ownedStabilityEvidence(capability)
  const evidence = stable.after
  const beforeJson = canonicalJson(stable.before, "First signed reader observation")
  const evidenceJson = canonicalJson(evidence, "Verified signed reader evidence")
  if (
    byteLength(beforeJson) > MAX_EVIDENCE_BYTES ||
    byteLength(evidenceJson) > MAX_EVIDENCE_BYTES
  ) {
    configuration("Verified signed reader evidence exceeds its byte limit.")
  }
  const identityJson = canonicalJson(stabilityIdentity(evidence), "Reader stability identity")
  return Object.freeze({
    evidence,
    evidenceChecksum: await digestText(
      digest,
      SIGNED_EVIDENCE_DOMAIN,
      evidenceJson,
      "Signed reader evidence",
    ),
    priorArtifactObservations: canonicalValue(
      stable.before.artifacts.map(({ artifactChecksum, observation, scriptName, versionId }) => ({
        artifactChecksum,
        observation,
        scriptName,
        versionId,
      })),
      "First reader artifact observations",
    ),
    priorDeploymentObservations: canonicalValue(
      stable.before.deployments.map(
        ({ createdAtMs, deploymentId, observation, scriptName, versions }) => ({
          createdAtMs,
          deploymentId,
          observation,
          scriptName,
          versions,
        }),
      ),
      "First reader deployment observations",
    ),
    priorEvidenceChecksum: await digestText(
      digest,
      SIGNED_EVIDENCE_DOMAIN,
      beforeJson,
      "First signed reader evidence",
    ),
    priorObservedFromMs: stable.before.observedFromMs,
    priorObservedThroughMs: stable.before.observedThroughMs,
    priorVerifiedAtMs: stable.before.verifiedAtMs,
    maxStabilityWindowMs: stable.maxStabilityWindowMs,
    stabilityIdentityChecksum: await digestText(
      digest,
      STABILITY_IDENTITY_DOMAIN,
      identityJson,
      "Reader stability identity",
    ),
  })
}

async function currentIdentityChecksum(
  capability: VerifiedReaderDeploymentCapability,
  digest: DigestFunction,
): Promise<string> {
  const evidence = sourceEvidence(
    captured(verifiedReaderDeploymentEvidence(capability), "Current signed reader observation"),
    "Current signed reader observation",
  )
  const evidenceJson = canonicalJson(evidence, "Current signed reader observation")
  if (byteLength(evidenceJson) > MAX_EVIDENCE_BYTES) {
    configuration("Current signed reader observation exceeds its byte limit.")
  }
  return digestText(
    digest,
    STABILITY_IDENTITY_DOMAIN,
    canonicalJson(stabilityIdentity(evidence), "Current reader stability identity"),
    "Current reader stability identity",
  )
}

function verificationBody(
  source: SignedSourceState,
  barrierChecksum: string,
  verifiedAtMs: number,
): JsonObject {
  const evidence = source.evidence
  return {
    accountId: evidence.accountId,
    artifacts: canonicalValue(evidence.artifacts, "Signed reader artifacts"),
    attestations: canonicalValue(evidence.attestations, "Signed reader attestations"),
    audience: evidence.audience,
    deployments: canonicalValue(evidence.deployments, "Signed reader deployments"),
    expectedScriptNames: canonicalValue(
      evidence.expectedScriptNames,
      "Signed reader script inventory",
    ),
    observedFromMs: evidence.observedFromMs,
    observedThroughMs: evidence.observedThroughMs,
    maxStabilityWindowMs: source.maxStabilityWindowMs,
    priorArtifactObservations: source.priorArtifactObservations,
    priorDeploymentObservations: source.priorDeploymentObservations,
    priorObservedFromMs: source.priorObservedFromMs,
    priorObservedThroughMs: source.priorObservedThroughMs,
    priorSignedEvidenceChecksum: source.priorEvidenceChecksum,
    priorVerifiedAtMs: source.priorVerifiedAtMs,
    protocolVersion: 1,
    readerBarrierChecksum: barrierChecksum,
    schemaVersion: 1,
    signedEvidenceChecksum: source.evidenceChecksum,
    sourceSchemaVersion: 1,
    sourceVerifiedAtMs: evidence.verifiedAtMs,
    stabilityIdentityChecksum: source.stabilityIdentityChecksum,
    stabilitySchemaVersion: 1,
    verifiedAtMs,
  }
}

async function createVerificationState(
  source: SignedSourceState,
  barrierChecksum: string,
  verifiedAtMs: number,
  digest: DigestFunction,
): Promise<VerificationState> {
  const evidenceJson = canonicalJson(
    verificationBody(source, barrierChecksum, verifiedAtMs),
    "Reader-barrier verification",
  )
  if (byteLength(evidenceJson) > MAX_EVIDENCE_BYTES) {
    configuration("Reader-barrier verification exceeds its byte limit.")
  }
  return Object.freeze({
    accountId: source.evidence.accountId,
    audience: source.evidence.audience,
    evidenceJson,
    readerBarrierChecksum: barrierChecksum,
    signedEvidenceChecksum: source.evidenceChecksum,
    sourceEvidence: source.evidence,
    sourceVerifiedAtMs: source.evidence.verifiedAtMs,
    stability: Object.freeze({
      identityChecksum: source.stabilityIdentityChecksum,
      maxStabilityWindowMs: source.maxStabilityWindowMs,
      priorEvidenceChecksum: source.priorEvidenceChecksum,
      priorObservedFromMs: source.priorObservedFromMs,
      priorObservedThroughMs: source.priorObservedThroughMs,
      priorVerifiedAtMs: source.priorVerifiedAtMs,
    }),
    verificationChecksum: await digestText(
      digest,
      VERIFICATION_DOMAIN,
      evidenceJson,
      "Reader-barrier verification",
    ),
    verifiedAtMs,
  })
}

function assertFreshActivationTime(source: SignedSourceState, verifiedAtMs: number): void {
  if (verifiedAtMs < source.evidence.verifiedAtMs) {
    intervention("Control D1 time precedes the signed reader verification time.")
  }
  if (verifiedAtMs - source.priorObservedFromMs > source.maxStabilityWindowMs) {
    intervention("The reader deployment stability window expired before Control activation.")
  }
}

function sourceFromVerificationBody(
  body: Record<string, unknown>,
): VerifiedReaderDeploymentEvidence {
  return {
    accountId: body.accountId as string,
    artifacts: body.artifacts as VerifiedReaderDeploymentEvidence["artifacts"],
    attestations: body.attestations as VerifiedReaderDeploymentEvidence["attestations"],
    audience: body.audience as string,
    deployments: body.deployments as VerifiedReaderDeploymentEvidence["deployments"],
    expectedScriptNames:
      body.expectedScriptNames as VerifiedReaderDeploymentEvidence["expectedScriptNames"],
    observedFromMs: body.observedFromMs as number,
    observedThroughMs: body.observedThroughMs as number,
    schemaVersion: body.sourceSchemaVersion as 1,
    verifiedAtMs: body.sourceVerifiedAtMs as number,
  }
}

function observationBounds(
  value: unknown,
  label: string,
): {
  readonly completedAtMs: number
  readonly startedAtMs: number
} {
  const observation = exactRecord(
    value,
    [
      "bodyBytes",
      "bodyState",
      ...(plainRecord(value) && Object.hasOwn(value, "cfRay") ? ["cfRay"] : []),
      "completedAtMs",
      "rateLimit",
      "responseChecksum",
      "startedAtMs",
      "status",
    ],
    label,
  )
  const bodyBytes = persistedInteger(observation.bodyBytes, `${label} body bytes`)
  const startedAtMs = persistedInteger(observation.startedAtMs, `${label} start time`)
  const completedAtMs = persistedInteger(observation.completedAtMs, `${label} completion time`)
  if (
    bodyBytes > MAX_EVIDENCE_BYTES ||
    observation.bodyState !== "complete" ||
    observation.status !== 200 ||
    completedAtMs < startedAtMs ||
    !plainRecord(observation.rateLimit) ||
    typeof observation.responseChecksum !== "string" ||
    !CHECKSUM.test(observation.responseChecksum) ||
    (Object.hasOwn(observation, "cfRay") &&
      (typeof observation.cfRay !== "string" || observation.cfRay.length === 0))
  ) {
    intervention(`${label} is malformed.`)
  }
  return { completedAtMs, startedAtMs }
}

async function decodeStabilityState(
  root: Record<string, unknown>,
  source: VerifiedReaderDeploymentEvidence,
  digest: DigestFunction,
): Promise<VerificationStabilityState | undefined> {
  if (!Object.hasOwn(root, "stabilitySchemaVersion")) return undefined
  try {
    if (root.stabilitySchemaVersion !== 1) configuration("Unsupported stability schema.")
    const priorArtifacts = root.priorArtifactObservations
    const priorDeployments = root.priorDeploymentObservations
    if (
      !Array.isArray(priorArtifacts) ||
      Object.keys(priorArtifacts).length !== priorArtifacts.length ||
      priorArtifacts.length !== source.artifacts.length ||
      !Array.isArray(priorDeployments) ||
      Object.keys(priorDeployments).length !== priorDeployments.length ||
      priorDeployments.length !== source.deployments.length ||
      priorArtifacts.length === 0 ||
      priorDeployments.length === 0
    ) {
      configuration("Prior observation inventory is malformed.")
    }
    let observedFromMs = Number.MAX_SAFE_INTEGER
    let observedThroughMs = 0
    for (const [index, value] of priorArtifacts.entries()) {
      const row = exactRecord(
        value,
        ["artifactChecksum", "observation", "scriptName", "versionId"],
        "Prior reader artifact observation",
      )
      const expected = source.artifacts[index]
      if (
        expected === undefined ||
        row.artifactChecksum !== expected.artifactChecksum ||
        row.scriptName !== expected.scriptName ||
        row.versionId !== expected.versionId
      ) {
        configuration("Prior reader artifact identity is contradictory.")
      }
      const bounds = observationBounds(row.observation, "Prior reader artifact observation")
      observedFromMs = Math.min(observedFromMs, bounds.startedAtMs)
      observedThroughMs = Math.max(observedThroughMs, bounds.completedAtMs)
    }
    for (const [index, value] of priorDeployments.entries()) {
      const row = exactRecord(
        value,
        ["createdAtMs", "deploymentId", "observation", "scriptName", "versions"],
        "Prior reader deployment observation",
      )
      const expected = source.deployments[index]
      if (
        expected === undefined ||
        row.createdAtMs !== expected.createdAtMs ||
        row.deploymentId !== expected.deploymentId ||
        row.scriptName !== expected.scriptName ||
        canonicalJson(row.versions, "Prior reader deployment versions") !==
          canonicalJson(expected.versions, "Current reader deployment versions")
      ) {
        configuration("Prior reader deployment identity is contradictory.")
      }
      const bounds = observationBounds(row.observation, "Prior reader deployment observation")
      observedFromMs = Math.min(observedFromMs, bounds.startedAtMs)
      observedThroughMs = Math.max(observedThroughMs, bounds.completedAtMs)
    }
    const priorObservedFromMs = persistedInteger(
      root.priorObservedFromMs,
      "Prior reader observation start",
    )
    const priorObservedThroughMs = persistedInteger(
      root.priorObservedThroughMs,
      "Prior reader observation completion",
    )
    const priorVerifiedAtMs = persistedInteger(
      root.priorVerifiedAtMs,
      "Prior reader verification time",
    )
    const maxStabilityWindowMs = persistedInteger(
      root.maxStabilityWindowMs,
      "Reader deployment stability window",
    )
    if (
      maxStabilityWindowMs < 1 ||
      maxStabilityWindowMs > MAX_STABILITY_WINDOW_MS ||
      priorObservedFromMs !== observedFromMs ||
      priorObservedThroughMs !== observedThroughMs ||
      priorObservedThroughMs > priorVerifiedAtMs ||
      priorVerifiedAtMs > source.observedFromMs ||
      (root.verifiedAtMs as number) - priorObservedFromMs > maxStabilityWindowMs
    ) {
      configuration("Prior reader observation time order is contradictory.")
    }
    const priorEvidenceChecksum = persistedChecksum(
      root.priorSignedEvidenceChecksum,
      "Prior signed reader evidence checksum",
    )
    const identityChecksum = persistedChecksum(
      root.stabilityIdentityChecksum,
      "Reader stability identity checksum",
    )
    if (
      (await digestText(
        digest,
        STABILITY_IDENTITY_DOMAIN,
        canonicalJson(stabilityIdentity(source), "Persisted reader stability identity"),
        "Persisted reader stability identity",
      )) !== identityChecksum
    ) {
      configuration("Reader stability identity checksum is contradictory.")
    }
    return Object.freeze({
      identityChecksum,
      maxStabilityWindowMs,
      priorEvidenceChecksum,
      priorObservedFromMs,
      priorObservedThroughMs,
      priorVerifiedAtMs,
    })
  } catch {
    return intervention("Persisted reader-deployment stability evidence is malformed.")
  }
}

async function decodeVerificationRow(
  rawRow: VerificationRow,
  digest: DigestFunction,
): Promise<VerificationState> {
  const row = capturedPersisted(rawRow, "Persisted signed reader verification")
  if (row.protocol_version !== 1) {
    return intervention("Persisted signed reader verification protocol is malformed.")
  }
  const readerBarrierChecksum = persistedChecksum(
    row.reader_barrier_checksum,
    "Persisted signed reader barrier checksum",
  )
  const verificationChecksum = persistedChecksum(
    row.verification_checksum,
    "Persisted signed reader verification checksum",
  )
  const verifiedAtMs = persistedInteger(
    row.verified_at_ms,
    "Persisted signed reader verification time",
  )
  const evidenceJson = persistedText(row.evidence_json, "Persisted signed reader verification JSON")
  if (byteLength(evidenceJson) > MAX_EVIDENCE_BYTES) {
    return intervention("Persisted signed reader verification exceeds its byte limit.")
  }
  const body = parsedJson(evidenceJson, "Persisted signed reader verification")
  let root: Record<string, unknown>
  let canonical: string
  try {
    const stabilityKeys =
      plainRecord(body) && Object.hasOwn(body, "stabilitySchemaVersion")
        ? [
            "maxStabilityWindowMs",
            "priorArtifactObservations",
            "priorDeploymentObservations",
            "priorObservedFromMs",
            "priorObservedThroughMs",
            "priorSignedEvidenceChecksum",
            "priorVerifiedAtMs",
            "stabilityIdentityChecksum",
            "stabilitySchemaVersion",
          ]
        : []
    root = exactRecord(
      body,
      [
        "accountId",
        "artifacts",
        "attestations",
        "audience",
        "deployments",
        "expectedScriptNames",
        "observedFromMs",
        "observedThroughMs",
        ...stabilityKeys,
        "protocolVersion",
        "readerBarrierChecksum",
        "schemaVersion",
        "signedEvidenceChecksum",
        "sourceSchemaVersion",
        "sourceVerifiedAtMs",
        "verifiedAtMs",
      ],
      "Persisted signed reader verification",
    )
    canonical = canonicalJson(root, "Persisted signed reader verification")
  } catch {
    return intervention("Persisted signed reader verification is malformed.")
  }
  if (
    canonical !== evidenceJson ||
    root.schemaVersion !== 1 ||
    root.sourceSchemaVersion !== 1 ||
    root.protocolVersion !== 1 ||
    root.readerBarrierChecksum !== readerBarrierChecksum ||
    root.verifiedAtMs !== verifiedAtMs ||
    typeof root.accountId !== "string" ||
    !ACCOUNT_ID.test(root.accountId) ||
    typeof root.audience !== "string" ||
    root.audience.length === 0
  ) {
    return intervention("Persisted signed reader verification is noncanonical or contradictory.")
  }
  const sourceVerifiedAtMs = persistedInteger(
    root.sourceVerifiedAtMs,
    "Persisted source reader verification time",
  )
  if (sourceVerifiedAtMs > verifiedAtMs) {
    return intervention("Persisted signed reader verification time order is contradictory.")
  }
  const signedEvidenceChecksum = persistedChecksum(
    root.signedEvidenceChecksum,
    "Persisted signed reader evidence checksum",
  )
  const sourceEvidence = sourceFromVerificationBody(root)
  const sourceJson = canonicalJson(sourceEvidence, "Persisted signed reader source evidence")
  if (
    (await digestText(
      digest,
      SIGNED_EVIDENCE_DOMAIN,
      sourceJson,
      "Persisted signed reader evidence",
    )) !== signedEvidenceChecksum ||
    (await digestText(
      digest,
      VERIFICATION_DOMAIN,
      evidenceJson,
      "Persisted reader-barrier verification",
    )) !== verificationChecksum
  ) {
    return intervention("Persisted signed reader verification checksum is contradictory.")
  }
  const stability = await decodeStabilityState(root, sourceEvidence, digest)
  return Object.freeze({
    accountId: root.accountId,
    audience: root.audience,
    evidenceJson,
    readerBarrierChecksum,
    signedEvidenceChecksum,
    sourceEvidence,
    sourceVerifiedAtMs,
    ...(stability === undefined ? {} : { stability }),
    verificationChecksum,
    verifiedAtMs,
  })
}

function receipt(
  normalized: ReaderBarrierReceipt,
  verification: VerificationState,
): SignedReaderBarrierReceipt {
  return Object.freeze({
    ...normalized,
    accountId: verification.accountId,
    audience: verification.audience,
    signedEvidenceChecksum: verification.signedEvidenceChecksum,
    sourceVerifiedAtMs: verification.sourceVerifiedAtMs,
    ...(verification.stability === undefined
      ? {}
      : {
          stability: Object.freeze({
            identityChecksum: verification.stability.identityChecksum,
            maxStabilityWindowMs: verification.stability.maxStabilityWindowMs,
            priorObservedFromMs: verification.stability.priorObservedFromMs,
            priorObservedThroughMs: verification.stability.priorObservedThroughMs,
            priorSignedEvidenceChecksum: verification.stability.priorEvidenceChecksum,
            priorVerifiedAtMs: verification.stability.priorVerifiedAtMs,
          }),
        }),
    verificationChecksum: verification.verificationChecksum,
  })
}

function signedMutationResults(results: readonly ControlRunResult[]): void {
  if (!Array.isArray(results) || results.length !== 5) {
    intervention("Control D1 returned an incomplete signed reader-barrier batch result.")
  }
  for (const [index, result] of results.entries()) {
    if (!plainRecord(result) || !plainRecord(result.meta)) {
      intervention("Control D1 returned malformed signed reader-barrier mutation metadata.")
    }
    const changes = result.meta.changes
    const maximum = index === 0 ? 512 : index < 4 ? 1 : 0
    if (
      result.success !== true ||
      !Number.isSafeInteger(changes) ||
      (changes as number) < 0 ||
      (changes as number) > maximum
    ) {
      intervention("Control D1 returned malformed signed reader-barrier mutation metadata.")
    }
  }
}

function mutationStatements(
  database: TransactionalControlDatabase,
  barrier: CanonicalReaderBarrierState,
  source: SignedSourceState,
  verification: VerificationState,
): readonly ControlStatement[] {
  return [
    database
      .prepare(
        `INSERT INTO "nozzle_reader_version_attestations"
         ("script_name", "version_id", "artifact_checksum", "control_schema_min",
          "control_schema_max", "outcome_payload_reader_min", "outcome_payload_reader_max",
          "attestation_checksum", "attestation_json", "registered_at_ms")
         SELECT json_extract("entry"."value", '$.scriptName'),
                json_extract("entry"."value", '$.versionId'),
                json_extract("entry"."value", '$.artifactChecksum'),
                json_extract("entry"."value", '$.controlSchemaMin'),
                json_extract("entry"."value", '$.controlSchemaMax'),
                json_extract("entry"."value", '$.outcomePayloadReaderMin'),
                json_extract("entry"."value", '$.outcomePayloadReaderMax'),
                json_extract("entry"."value", '$.attestationChecksum'),
                json_extract("entry"."value", '$.attestationJson'), ?2
         FROM json_each(?1) AS "entry"
         WHERE NOT EXISTS (
           SELECT 1 FROM "nozzle_saga_outcome_payload_activations"
           WHERE "protocol_version" = 1
         )
         ON CONFLICT ("script_name", "version_id") DO NOTHING`,
      )
      .bind(barrier.attestationMutationJson, verification.verifiedAtMs),
    database
      .prepare(
        `INSERT INTO "nozzle_reader_barriers"
         ("protocol_version", "barrier_checksum", "inventory_checksum", "barrier_json",
          "verified_at_ms")
         SELECT 1, ?2, ?3, ?4, ?6
         WHERE NOT EXISTS (
           SELECT 1 FROM "nozzle_saga_outcome_payload_activations"
           WHERE "protocol_version" = 1
         )
         AND (
           SELECT count(*)
           FROM json_each(?1) AS "expected"
           JOIN "nozzle_reader_version_attestations" AS "attestation"
             ON "attestation"."script_name" = json_extract("expected"."value", '$.scriptName')
            AND "attestation"."version_id" = json_extract("expected"."value", '$.versionId')
            AND "attestation"."artifact_checksum" =
              json_extract("expected"."value", '$.artifactChecksum')
            AND "attestation"."control_schema_min" =
              json_extract("expected"."value", '$.controlSchemaMin')
            AND "attestation"."control_schema_max" =
              json_extract("expected"."value", '$.controlSchemaMax')
            AND "attestation"."outcome_payload_reader_min" =
              json_extract("expected"."value", '$.outcomePayloadReaderMin')
            AND "attestation"."outcome_payload_reader_max" =
              json_extract("expected"."value", '$.outcomePayloadReaderMax')
            AND "attestation"."attestation_checksum" =
              json_extract("expected"."value", '$.attestationChecksum')
            AND "attestation"."attestation_json" =
              json_extract("expected"."value", '$.attestationJson')
         ) = ?5
         ON CONFLICT ("protocol_version") DO NOTHING`,
      )
      .bind(
        barrier.attestationMutationJson,
        barrier.barrierChecksum,
        barrier.inventoryChecksum,
        barrier.barrierJson,
        barrier.attestations.length,
        verification.verifiedAtMs,
      ),
    database
      .prepare(
        `INSERT INTO "nozzle_reader_barrier_verifications"
         ("protocol_version", "reader_barrier_checksum", "verification_checksum",
          "evidence_json", "verified_at_ms")
         SELECT 1, ?1, ?2, ?3, ?4
         FROM "nozzle_reader_barriers" AS "barrier"
         WHERE "barrier"."protocol_version" = 1
           AND "barrier"."barrier_checksum" = ?1
           AND "barrier"."verified_at_ms" = ?4
           AND NOT EXISTS (
             SELECT 1 FROM "nozzle_saga_outcome_payload_activations"
             WHERE "protocol_version" = 1
           )
         ON CONFLICT ("protocol_version") DO NOTHING`,
      )
      .bind(
        barrier.barrierChecksum,
        verification.verificationChecksum,
        verification.evidenceJson,
        verification.verifiedAtMs,
      ),
    database
      .prepare(
        `INSERT INTO "nozzle_saga_outcome_payload_activations"
         ("protocol_version", "reader_barrier_checksum", "activated_at_ms")
         SELECT "verification"."protocol_version", "verification"."reader_barrier_checksum",
                ${SERVER_TIME_SQL}
         FROM "nozzle_reader_barrier_verifications" AS "verification"
         JOIN "nozzle_reader_barriers" AS "barrier"
           ON "barrier"."protocol_version" = "verification"."protocol_version"
          AND "barrier"."barrier_checksum" = "verification"."reader_barrier_checksum"
         WHERE "verification"."protocol_version" = 1
           AND "verification"."reader_barrier_checksum" = ?1
           AND "verification"."verification_checksum" = ?2
           AND "verification"."evidence_json" = ?3
           AND "verification"."verified_at_ms" = ?4
           AND ${SERVER_TIME_SQL} - ?5 <= ?6
         ON CONFLICT ("protocol_version") DO NOTHING`,
      )
      .bind(
        barrier.barrierChecksum,
        verification.verificationChecksum,
        verification.evidenceJson,
        verification.verifiedAtMs,
        source.priorObservedFromMs,
        source.maxStabilityWindowMs,
      ),
    database
      .prepare(
        `INSERT INTO "nozzle_control_schema_versions" ("schema_version", "published_at_ms")
         SELECT 0, 0
         WHERE NOT EXISTS (
           SELECT 1
           FROM "nozzle_saga_outcome_payload_activations" AS "activation"
           JOIN "nozzle_reader_barriers" AS "barrier"
             ON "barrier"."protocol_version" = "activation"."protocol_version"
            AND "barrier"."barrier_checksum" = "activation"."reader_barrier_checksum"
           JOIN "nozzle_reader_barrier_verifications" AS "verification"
             ON "verification"."protocol_version" = "activation"."protocol_version"
            AND "verification"."reader_barrier_checksum" = "barrier"."barrier_checksum"
           WHERE "activation"."protocol_version" = 1
             AND "barrier"."barrier_checksum" = ?1
             AND "barrier"."verified_at_ms" = ?4
             AND "verification"."verification_checksum" = ?2
             AND "verification"."evidence_json" = ?3
             AND "verification"."verified_at_ms" = ?4
             AND "activation"."activated_at_ms" >= ?4
             AND "activation"."activated_at_ms" - ?5 <= ?6
         )`,
      )
      .bind(
        barrier.barrierChecksum,
        verification.verificationChecksum,
        verification.evidenceJson,
        verification.verifiedAtMs,
        source.priorObservedFromMs,
        source.maxStabilityWindowMs,
      ),
  ]
}

export class D1SignedReaderBarrierStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction
  readonly #normalized: D1ReaderBarrierStore

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional Control D1 binding is required for signed reader activation.")
    }
    if (typeof digest !== "function") {
      configuration("A signed reader-barrier digest function is required.")
    }
    this.#database = database
    this.#digest = digest
    this.#normalized = new D1ReaderBarrierStore(database, digest)
  }

  async #rawVerification(): Promise<VerificationRow | null> {
    return this.#database
      .prepare(
        `SELECT "protocol_version", "reader_barrier_checksum", "verification_checksum",
                "evidence_json", "verified_at_ms"
         FROM "nozzle_reader_barrier_verifications" WHERE "protocol_version" = 1`,
      )
      .first<VerificationRow>()
  }

  async #serverTime(): Promise<number> {
    const raw = await this.#database
      .prepare(`SELECT ${SERVER_TIME_SQL} AS "verified_at_ms"`)
      .first<ServerTimeRow>()
    if (raw === null) return intervention("Control D1 omitted its reader verification time.")
    const row = capturedPersisted(raw, "Control D1 reader verification time")
    return persistedInteger(row.verified_at_ms, "Control D1 reader verification time")
  }

  async get(): Promise<SignedReaderBarrierReceipt | undefined> {
    const normalized = await this.#normalized.get()
    const rawVerification = await this.#rawVerification()
    if (normalized === undefined) {
      if (rawVerification !== null) {
        return intervention("Signed reader verification exists without its activation.")
      }
      return undefined
    }
    if (rawVerification === null) {
      return intervention("Signed reader activation is missing its verification evidence.")
    }
    const verification = await decodeVerificationRow(rawVerification, this.#digest)
    if (
      verification.readerBarrierChecksum !== normalized.barrierChecksum ||
      verification.verifiedAtMs !== normalized.verifiedAtMs
    ) {
      return intervention("Signed reader verification contradicts its normalized barrier.")
    }
    let capability: ReaderBarrierCapability
    try {
      capability = await verifyReaderDeploymentBarrier(
        normalizedInput(verification.sourceEvidence),
        this.#digest,
      )
    } catch {
      return intervention("Persisted signed reader evidence failed normalized verification.")
    }
    if (readerBarrierCapabilityState(capability).barrierChecksum !== normalized.barrierChecksum) {
      return intervention("Persisted signed reader evidence contradicts its reader barrier.")
    }
    return receipt(normalized, verification)
  }

  async assertCurrent(
    capability: VerifiedReaderDeploymentCapability,
  ): Promise<SignedReaderBarrierReceipt> {
    const observedIdentityChecksum = await currentIdentityChecksum(capability, this.#digest)
    const active = await this.get()
    if (active === undefined) {
      return intervention("Signed reader deployment has not been activated.")
    }
    let expectedIdentityChecksum = active.stability?.identityChecksum
    if (expectedIdentityChecksum === undefined) {
      const rawVerification = await this.#rawVerification()
      if (rawVerification === null) {
        return intervention("Signed reader activation is missing its verification evidence.")
      }
      const verification = await decodeVerificationRow(rawVerification, this.#digest)
      expectedIdentityChecksum = await digestText(
        this.#digest,
        STABILITY_IDENTITY_DOMAIN,
        canonicalJson(
          stabilityIdentity(verification.sourceEvidence),
          "Activated reader stability identity",
        ),
        "Activated reader stability identity",
      )
    }
    if (observedIdentityChecksum !== expectedIdentityChecksum) {
      return intervention("Active reader deployment drifted from its verified activation.")
    }
    return active
  }

  async activate(
    stabilityCapability: VerifiedReaderDeploymentStabilityCapability,
  ): Promise<SignedReaderBarrierReceipt> {
    const source = await stableSourceState(stabilityCapability, this.#digest)
    const normalizedCapability = await verifyReaderDeploymentBarrier(
      normalizedInput(source.evidence),
      this.#digest,
    )
    const barrier = readerBarrierCapabilityState(normalizedCapability)
    const existingNormalized = await this.#normalized.get()
    if (existingNormalized !== undefined) {
      const existing = await this.get()
      if (
        existing === undefined ||
        existing.barrierChecksum !== barrier.barrierChecksum ||
        (existing.stability === undefined
          ? existing.signedEvidenceChecksum !== source.evidenceChecksum
          : existing.stability.identityChecksum !== source.stabilityIdentityChecksum)
      ) {
        return intervention("Signed reader activation is bound to other verification evidence.")
      }
      return existing
    }
    await this.#normalized.assertCompatible(normalizedCapability)
    const partial = await this.#rawVerification()
    if (partial !== null) {
      const raced = (await this.get()) as SignedReaderBarrierReceipt
      if (
        raced.stability === undefined ||
        raced.barrierChecksum !== barrier.barrierChecksum ||
        raced.stability.identityChecksum !== source.stabilityIdentityChecksum
      ) {
        return intervention("Signed reader activation is bound to other verification evidence.")
      }
      return raced
    }
    const activationTime = await this.#serverTime()
    assertFreshActivationTime(source, activationTime)
    const verification = await createVerificationState(
      source,
      barrier.barrierChecksum,
      activationTime,
      this.#digest,
    )
    let results: readonly ControlRunResult[] | undefined
    try {
      results = await this.#database.batch(
        mutationStatements(this.#database, barrier, source, verification),
      )
    } catch {
      // Exact immutable state below decides whether the signed activation committed.
    }
    if (results !== undefined) signedMutationResults(results)
    const activated = await this.get()
    if (activated === undefined) {
      await this.#normalized.assertCompatible(normalizedCapability)
      return resume("Signed reader activation did not produce an immutable receipt; retry safely.")
    }
    if (
      activated.barrierChecksum !== barrier.barrierChecksum ||
      activated.stability?.identityChecksum !== source.stabilityIdentityChecksum
    ) {
      return intervention("Signed reader activation is bound to other verification evidence.")
    }
    return activated
  }
}
