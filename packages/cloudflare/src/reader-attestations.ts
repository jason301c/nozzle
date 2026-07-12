import { NozzleError } from "@nozzle/core"
import {
  type ActiveWorkerDeploymentProof,
  type ActiveWorkerDeploymentProofState,
  activeWorkerDeploymentProofState,
} from "./worker-deployment-proof.js"
import type { WorkerDeploymentObservationEvidence } from "./worker-deployments.js"
import {
  type WorkerVersionArtifactProof,
  type WorkerVersionArtifactProofState,
  workerVersionArtifactProofState,
} from "./worker-version-proof.js"

const SIGNATURE_DOMAIN = "nozzle.reader-version-attestation-signature.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const BASE64URL = /^[A-Za-z0-9_-]+$/u
const MAX_READERS = 256
const MAX_ACTIVE_VERSIONS = MAX_READERS * 2
const MAX_TRUST_KEYS = 64
const MAX_OBSERVATION_TIME_MS = 5 * 60 * 1_000
const MAX_ATTESTATION_VALIDITY_MS = 30 * 24 * 60 * 60 * 1_000
const REQUIRED_CONTROL_SCHEMA_MAX = 6
const REQUIRED_CONTROL_SCHEMA_MIN = 5

export interface ReaderVersionAttestationStatement {
  readonly artifactChecksum: string
  readonly audience: string
  readonly controlSchemaMax: number
  readonly controlSchemaMin: number
  readonly expiresAtMs: number
  readonly issuedAtMs: number
  readonly keyId: string
  readonly outcomePayloadReaderMax: number
  readonly outcomePayloadReaderMin: number
  readonly schemaVersion: 1
  readonly scriptName: string
  readonly versionId: string
}

export interface SignedReaderVersionAttestation {
  readonly signature: string
  readonly statement: ReaderVersionAttestationStatement
}

export interface ReaderAttestationTrustKey {
  readonly keyId: string
  readonly publicKeyBase64Url: string
}

export interface ReaderDeploymentVerifierOptions {
  readonly accountId: string
  readonly audience: string
  readonly maxAttestationValidityMs: number
  readonly maxObservationAgeMs: number
  readonly maxObservationWindowMs: number
  readonly now?: () => number
  readonly trustedKeys: readonly ReaderAttestationTrustKey[]
}

export interface ReaderDeploymentVerificationInput {
  readonly artifactProofs: readonly WorkerVersionArtifactProof[]
  readonly attestations: readonly SignedReaderVersionAttestation[]
  readonly deploymentProofs: readonly ActiveWorkerDeploymentProof[]
  readonly expectedScriptNames: readonly string[]
}

export interface VerifiedReaderAttestation {
  readonly artifactChecksum: string
  readonly controlSchemaMax: number
  readonly controlSchemaMin: number
  readonly expiresAtMs: number
  readonly issuedAtMs: number
  readonly keyId: string
  readonly outcomePayloadReaderMax: number
  readonly outcomePayloadReaderMin: number
  readonly publicKeyBase64Url: string
  readonly signature: string
  readonly scriptName: string
  readonly versionId: string
}

export interface VerifiedReaderArtifact {
  readonly artifactChecksum: string
  readonly observation: WorkerDeploymentObservationEvidence
  readonly scriptName: string
  readonly versionId: string
}

export interface VerifiedReaderDeployment {
  readonly createdAtMs: number
  readonly deploymentId: string
  readonly observation: WorkerDeploymentObservationEvidence
  readonly scriptName: string
  readonly versions: readonly {
    readonly versionId: string
    readonly weightBps: number
  }[]
}

export interface VerifiedReaderDeploymentEvidence {
  readonly accountId: string
  readonly artifacts: readonly VerifiedReaderArtifact[]
  readonly attestations: readonly VerifiedReaderAttestation[]
  readonly audience: string
  readonly deployments: readonly VerifiedReaderDeployment[]
  readonly expectedScriptNames: readonly string[]
  readonly observedFromMs: number
  readonly observedThroughMs: number
  readonly schemaVersion: 1
  readonly verifiedAtMs: number
}

export type VerifiedReaderDeploymentCapability = object

export interface ReaderDeploymentVerifier {
  verify(input: ReaderDeploymentVerificationInput): Promise<VerifiedReaderDeploymentCapability>
}

interface CanonicalStatement {
  readonly statement: ReaderVersionAttestationStatement
  readonly signingBytes: Uint8Array<ArrayBuffer>
}

const capabilityStates = new WeakMap<object, VerifiedReaderDeploymentEvidence>()

function configuration(message: string): never {
  throw new NozzleError("ConfigurationError", message)
}

function intervention(message: string): never {
  throw new NozzleError("OperationInterventionRequiredError", message)
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

function exactArray(value: unknown, label: string): readonly unknown[] {
  if (!Array.isArray(value) || Object.keys(value).length !== value.length) {
    configuration(`${label} must be a dense array.`)
  }
  return value
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

function identifier(value: unknown, label: string, maximum: number): string {
  if (typeof value !== "string" || value.trim() === "" || value.length > maximum) {
    configuration(`${label} must contain between 1 and ${maximum} characters.`)
  }
  wellFormed(value, label)
  return value
}

function checksum(value: unknown, label: string): string {
  if (typeof value !== "string" || !CHECKSUM.test(value)) {
    configuration(`${label} must be a lowercase SHA-256 checksum.`)
  }
  return value
}

function safeInteger(value: unknown, label: string, minimum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum) {
    configuration(`${label} must be a safe integer of at least ${minimum}.`)
  }
  return value as number
}

function boundedPolicy(value: unknown, label: string, maximum: number): number {
  const parsed = safeInteger(value, label, 1)
  if (parsed > maximum) configuration(`${label} exceeds its fail-closed maximum.`)
  return parsed
}

function binaryTextOrder(left: string, right: string): number {
  const leftBytes = new TextEncoder().encode(left)
  const rightBytes = new TextEncoder().encode(right)
  const length = Math.min(leftBytes.length, rightBytes.length)
  for (let index = 0; index < length; index += 1) {
    const difference = (leftBytes[index] as number) - (rightBytes[index] as number)
    if (difference !== 0) return difference
  }
  return leftBytes.length - rightBytes.length
}

function frame(value: string): Uint8Array<ArrayBuffer> {
  const parts = [SIGNATURE_DOMAIN, value].map((part) => new TextEncoder().encode(part))
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

function canonicalStatement(input: ReaderVersionAttestationStatement): CanonicalStatement {
  const body = exactRecord(
    input,
    [
      "artifactChecksum",
      "audience",
      "controlSchemaMax",
      "controlSchemaMin",
      "expiresAtMs",
      "issuedAtMs",
      "keyId",
      "outcomePayloadReaderMax",
      "outcomePayloadReaderMin",
      "schemaVersion",
      "scriptName",
      "versionId",
    ],
    "Reader-version attestation statement",
  )
  if (body.schemaVersion !== 1) configuration("Reader-version attestation schema is unsupported.")
  const artifactChecksum = checksum(body.artifactChecksum, "Reader artifact checksum")
  const audience = identifier(body.audience, "Reader attestation audience", 512)
  const controlSchemaMax = safeInteger(body.controlSchemaMax, "Maximum Control schema", 1)
  const controlSchemaMin = safeInteger(body.controlSchemaMin, "Minimum Control schema", 1)
  if (controlSchemaMax < controlSchemaMin) configuration("Control schema range is inverted.")
  const expiresAtMs = safeInteger(body.expiresAtMs, "Reader attestation expiry", 1)
  const issuedAtMs = safeInteger(body.issuedAtMs, "Reader attestation issuance", 0)
  if (expiresAtMs <= issuedAtMs) configuration("Reader attestation validity interval is empty.")
  const keyId = identifier(body.keyId, "Reader attestation key ID", 128)
  const outcomePayloadReaderMax = safeInteger(
    body.outcomePayloadReaderMax,
    "Maximum outcome-payload reader protocol",
    1,
  )
  const outcomePayloadReaderMin = safeInteger(
    body.outcomePayloadReaderMin,
    "Minimum outcome-payload reader protocol",
    1,
  )
  if (outcomePayloadReaderMax < outcomePayloadReaderMin) {
    configuration("Outcome-payload reader range is inverted.")
  }
  const scriptName = identifier(body.scriptName, "Reader script name", 255)
  const versionId = identifier(body.versionId, "Reader version ID", 128)
  const statement = Object.freeze({
    artifactChecksum,
    audience,
    controlSchemaMax,
    controlSchemaMin,
    expiresAtMs,
    issuedAtMs,
    keyId,
    outcomePayloadReaderMax,
    outcomePayloadReaderMin,
    schemaVersion: 1 as const,
    scriptName,
    versionId,
  })
  return Object.freeze({ statement, signingBytes: frame(JSON.stringify(statement)) })
}

export function readerVersionAttestationSigningBytes(
  input: ReaderVersionAttestationStatement,
): Uint8Array<ArrayBuffer> {
  return canonicalStatement(
    captured(input, "Reader-version attestation statement"),
  ).signingBytes.slice()
}

function encodeBase64Url(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")
}

function base64UrlBytes(
  value: unknown,
  expectedBytes: number,
  label: string,
): Uint8Array<ArrayBuffer> {
  if (typeof value !== "string" || !BASE64URL.test(value)) {
    configuration(`${label} must be unpadded base64url.`)
  }
  let binary: string
  try {
    const base64 = value.replace(/-/gu, "+").replace(/_/gu, "/")
    binary = atob(`${base64}${"=".repeat((4 - (base64.length % 4)) % 4)}`)
  } catch {
    return configuration(`${label} must be unpadded base64url.`)
  }
  const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0))
  if (bytes.byteLength !== expectedBytes || encodeBase64Url(bytes) !== value) {
    configuration(`${label} has the wrong length or a noncanonical encoding.`)
  }
  return bytes
}

function proofKey(scriptName: string, versionId: string): string {
  return `${scriptName.length}:${scriptName}:${versionId.length}:${versionId}`
}

function capturedVerificationInput(input: ReaderDeploymentVerificationInput): {
  readonly artifactProofs: readonly WorkerVersionArtifactProof[]
  readonly attestations: readonly SignedReaderVersionAttestation[]
  readonly deploymentProofs: readonly ActiveWorkerDeploymentProof[]
  readonly expectedScriptNames: readonly string[]
} {
  const root = exactRecord(
    input,
    ["artifactProofs", "attestations", "deploymentProofs", "expectedScriptNames"],
    "Reader deployment verification input",
  )
  const rawArtifactProofs = exactArray(root.artifactProofs, "Worker-version artifact proofs")
  const rawAttestations = root.attestations
  const rawProofs = exactArray(root.deploymentProofs, "Active Worker deployment proofs")
  const rawExpected = root.expectedScriptNames
  return Object.freeze({
    artifactProofs: Object.freeze([...rawArtifactProofs] as WorkerVersionArtifactProof[]),
    attestations: Object.freeze(
      captured(
        rawAttestations,
        "Signed reader-version attestations",
      ) as SignedReaderVersionAttestation[],
    ),
    deploymentProofs: Object.freeze([...rawProofs] as ActiveWorkerDeploymentProof[]),
    expectedScriptNames: Object.freeze(
      captured(rawExpected, "Expected reader scripts") as string[],
    ),
  })
}

function observedArtifactProof(
  proof: WorkerVersionArtifactProof,
  accountId: string,
): WorkerVersionArtifactProofState {
  const state = workerVersionArtifactProofState(proof)
  if (state === undefined) configuration("A live Worker-version artifact proof is required.")
  if (state.accountId !== accountId) {
    intervention("A Worker-version artifact proof belongs to another Cloudflare account.")
  }
  const { evidence } = state
  if (
    evidence.status !== 200 ||
    evidence.bodyState !== "complete" ||
    typeof evidence.responseChecksum !== "string" ||
    !CHECKSUM.test(evidence.responseChecksum) ||
    !Number.isSafeInteger(evidence.startedAtMs) ||
    evidence.startedAtMs < 0 ||
    !Number.isSafeInteger(evidence.completedAtMs) ||
    evidence.completedAtMs < evidence.startedAtMs
  ) {
    intervention("Worker-version artifact proof evidence is malformed.")
  }
  return state
}

function observedProof(
  proof: ActiveWorkerDeploymentProof,
  accountId: string,
): ActiveWorkerDeploymentProofState {
  const state = activeWorkerDeploymentProofState(proof)
  if (state === undefined) configuration("A live active-Worker deployment proof is required.")
  if (state.accountId !== accountId) {
    intervention("An active Worker deployment proof belongs to another Cloudflare account.")
  }
  const { evidence } = state
  if (
    evidence.status !== 200 ||
    evidence.bodyState !== "complete" ||
    typeof evidence.responseChecksum !== "string" ||
    !CHECKSUM.test(evidence.responseChecksum) ||
    !Number.isSafeInteger(evidence.startedAtMs) ||
    evidence.startedAtMs < 0 ||
    !Number.isSafeInteger(evidence.completedAtMs) ||
    evidence.completedAtMs < evidence.startedAtMs
  ) {
    intervention("Active Worker deployment proof evidence is malformed.")
  }
  return state
}

export async function createReaderDeploymentVerifier(
  options: ReaderDeploymentVerifierOptions,
): Promise<ReaderDeploymentVerifier> {
  if (!plainRecord(options)) configuration("Reader deployment verifier options are required.")
  const accountId = identifier(options.accountId, "Cloudflare account ID", 32)
  if (!/^[0-9a-fA-F]{32}$/u.test(accountId)) {
    configuration("Cloudflare account ID must contain 32 hexadecimal characters.")
  }
  const audience = identifier(options.audience, "Reader attestation audience", 512)
  const maxAttestationValidityMs = boundedPolicy(
    options.maxAttestationValidityMs,
    "Maximum reader-attestation validity",
    MAX_ATTESTATION_VALIDITY_MS,
  )
  const maxObservationAgeMs = boundedPolicy(
    options.maxObservationAgeMs,
    "Maximum deployment-observation age",
    MAX_OBSERVATION_TIME_MS,
  )
  const maxObservationWindowMs = boundedPolicy(
    options.maxObservationWindowMs,
    "Maximum deployment-observation window",
    MAX_OBSERVATION_TIME_MS,
  )
  const now = options.now ?? Date.now
  if (typeof now !== "function") configuration("A reader deployment verifier clock is required.")
  const subtle = globalThis.crypto?.subtle
  if (typeof subtle !== "object" || subtle === null) {
    configuration("A Web Crypto implementation is required for reader attestation verification.")
  }
  const keyInput = exactArray(
    captured(options.trustedKeys, "Reader attestation trust keys"),
    "Reader attestation trust keys",
  )
  if (keyInput.length < 1 || keyInput.length > MAX_TRUST_KEYS) {
    configuration(`Reader attestation trust must contain between 1 and ${MAX_TRUST_KEYS} keys.`)
  }
  const rawKeys: {
    readonly keyId: string
    readonly publicKey: Uint8Array
    readonly publicKeyBase64Url: string
  }[] = []
  const keyIds = new Set<string>()
  for (const candidate of keyInput) {
    const key = exactRecord(candidate, ["keyId", "publicKeyBase64Url"], "Reader trust key")
    const keyId = identifier(key.keyId, "Reader attestation key ID", 128)
    if (keyIds.has(keyId)) configuration("Reader attestation key IDs must be unique.")
    keyIds.add(keyId)
    const publicKey = base64UrlBytes(key.publicKeyBase64Url, 32, "Ed25519 public key")
    rawKeys.push(
      Object.freeze({ keyId, publicKey, publicKeyBase64Url: encodeBase64Url(publicKey) }),
    )
  }
  let imported: readonly CryptoKey[]
  try {
    imported = await Promise.all(
      rawKeys.map(({ publicKey }) =>
        subtle.importKey("raw", publicKey.slice(), { name: "Ed25519" }, false, ["verify"]),
      ),
    )
  } catch {
    return configuration("Reader attestation public keys could not be imported as Ed25519 keys.")
  }
  const keys = new Map(
    rawKeys.map(({ keyId, publicKeyBase64Url }, index) => [
      keyId,
      Object.freeze({ cryptoKey: imported[index] as CryptoKey, publicKeyBase64Url }),
    ]),
  )

  async function verify(
    untrustedInput: ReaderDeploymentVerificationInput,
  ): Promise<VerifiedReaderDeploymentCapability> {
    const input = capturedVerificationInput(untrustedInput)
    const expectedInput = exactArray(input.expectedScriptNames, "Expected reader scripts")
    if (expectedInput.length < 1 || expectedInput.length > MAX_READERS) {
      configuration(`Expected reader scripts must contain between 1 and ${MAX_READERS} entries.`)
    }
    const expectedScriptNames = expectedInput
      .map((value) => identifier(value, "Reader script name", 255))
      .sort(binaryTextOrder)
    if (new Set(expectedScriptNames).size !== expectedScriptNames.length) {
      configuration("Expected reader scripts must be unique.")
    }
    if (input.deploymentProofs.length !== expectedScriptNames.length) {
      intervention("Active Worker deployment proofs do not cover every expected reader.")
    }
    const deployments: VerifiedReaderDeployment[] = []
    const observedScripts = new Set<string>()
    const activeKeys = new Set<string>()
    let observedFromMs = Number.MAX_SAFE_INTEGER
    let observedThroughMs = 0
    for (const proof of input.deploymentProofs) {
      const state = observedProof(proof, accountId)
      const { deployment, evidence } = state
      if (observedScripts.has(deployment.scriptName)) {
        intervention("Active Worker deployment proofs contain a duplicate reader script.")
      }
      observedScripts.add(deployment.scriptName)
      observedFromMs = Math.min(observedFromMs, evidence.startedAtMs)
      observedThroughMs = Math.max(observedThroughMs, evidence.completedAtMs)
      const versions = deployment.versions.map(({ versionId, weightBps }) => {
        const key = proofKey(deployment.scriptName, versionId)
        if (activeKeys.has(key)) {
          intervention("Active Worker deployment proofs contain a duplicate reader version.")
        }
        activeKeys.add(key)
        return Object.freeze({ versionId, weightBps })
      })
      versions.sort((left, right) => binaryTextOrder(left.versionId, right.versionId))
      deployments.push(
        Object.freeze({
          createdAtMs: deployment.createdAtMs,
          deploymentId: deployment.deploymentId,
          observation: evidence,
          scriptName: deployment.scriptName,
          versions: Object.freeze(versions),
        }),
      )
    }
    deployments.sort((left, right) => binaryTextOrder(left.scriptName, right.scriptName))
    if (
      deployments.some((deployment, index) => deployment.scriptName !== expectedScriptNames[index])
    ) {
      intervention("Active Worker deployments do not match the expected reader inventory.")
    }
    if (activeKeys.size > MAX_ACTIVE_VERSIONS) {
      intervention("Active Worker deployments exceed the reader-version proof bound.")
    }
    if (input.artifactProofs.length !== activeKeys.size) {
      intervention("Worker-version artifact proofs do not cover every active version exactly once.")
    }
    const artifacts: VerifiedReaderArtifact[] = []
    const artifactChecksums = new Map<string, string>()
    for (const proof of input.artifactProofs) {
      const state = observedArtifactProof(proof, accountId)
      const { artifact, evidence } = state
      const key = proofKey(artifact.scriptName, artifact.versionId)
      if (artifactChecksums.has(key)) {
        intervention("Worker-version artifact proofs contain a duplicate active version.")
      }
      if (!activeKeys.has(key)) {
        intervention("A Worker-version artifact proof does not name an active version.")
      }
      observedFromMs = Math.min(observedFromMs, evidence.startedAtMs)
      observedThroughMs = Math.max(observedThroughMs, evidence.completedAtMs)
      artifactChecksums.set(key, artifact.artifactChecksum)
      artifacts.push(
        Object.freeze({
          artifactChecksum: artifact.artifactChecksum,
          observation: evidence,
          scriptName: artifact.scriptName,
          versionId: artifact.versionId,
        }),
      )
    }
    artifacts.sort((left, right) => {
      const scriptOrder = binaryTextOrder(left.scriptName, right.scriptName)
      return scriptOrder === 0 ? binaryTextOrder(left.versionId, right.versionId) : scriptOrder
    })
    const verifiedAtMs = safeInteger(now(), "Reader deployment verification time", 0)
    if (
      observedThroughMs > verifiedAtMs ||
      verifiedAtMs - observedThroughMs > maxObservationAgeMs ||
      observedThroughMs - observedFromMs > maxObservationWindowMs
    ) {
      intervention("Active Worker deployment observations are stale, future, or unconverged.")
    }
    const attestationInput = exactArray(input.attestations, "Signed reader-version attestations")
    if (attestationInput.length !== activeKeys.size) {
      intervention("Signed reader attestations do not cover every active version exactly once.")
    }
    const attestations: VerifiedReaderAttestation[] = []
    const attestationKeys = new Set<string>()
    await Promise.all(
      attestationInput.map(async (candidate) => {
        const envelope = exactRecord(
          candidate,
          ["signature", "statement"],
          "Signed reader-version attestation",
        )
        const canonical = canonicalStatement(
          envelope.statement as ReaderVersionAttestationStatement,
        )
        const { statement } = canonical
        const key = proofKey(statement.scriptName, statement.versionId)
        if (attestationKeys.has(key)) {
          intervention("Signed reader attestations contain a duplicate active version.")
        }
        attestationKeys.add(key)
        if (!activeKeys.has(key)) {
          intervention("A signed reader attestation does not name an active version.")
        }
        if (artifactChecksums.get(key) !== statement.artifactChecksum) {
          intervention("A signed reader attestation does not match the live Worker artifact.")
        }
        if (
          statement.audience !== audience ||
          statement.issuedAtMs > observedFromMs ||
          statement.expiresAtMs <= verifiedAtMs ||
          statement.expiresAtMs - statement.issuedAtMs > maxAttestationValidityMs ||
          statement.controlSchemaMin > REQUIRED_CONTROL_SCHEMA_MIN ||
          statement.controlSchemaMax < REQUIRED_CONTROL_SCHEMA_MAX ||
          statement.outcomePayloadReaderMin !== 1 ||
          statement.outcomePayloadReaderMax < 1
        ) {
          intervention("A signed reader attestation is out of scope, stale, or incompatible.")
        }
        const trustedKey = keys.get(statement.keyId)
        if (trustedKey === undefined) {
          intervention("A signed reader attestation uses an untrusted key ID.")
        }
        const signature = base64UrlBytes(envelope.signature, 64, "Ed25519 signature")
        let valid: boolean
        try {
          valid = await subtle.verify(
            { name: "Ed25519" },
            trustedKey.cryptoKey,
            signature,
            canonical.signingBytes,
          )
        } catch {
          return intervention("A reader attestation signature could not be verified.")
        }
        if (!valid) intervention("A reader attestation signature is invalid.")
        attestations.push(
          Object.freeze({
            artifactChecksum: statement.artifactChecksum,
            controlSchemaMax: statement.controlSchemaMax,
            controlSchemaMin: statement.controlSchemaMin,
            expiresAtMs: statement.expiresAtMs,
            issuedAtMs: statement.issuedAtMs,
            keyId: statement.keyId,
            outcomePayloadReaderMax: statement.outcomePayloadReaderMax,
            outcomePayloadReaderMin: statement.outcomePayloadReaderMin,
            publicKeyBase64Url: trustedKey.publicKeyBase64Url,
            signature: encodeBase64Url(signature),
            scriptName: statement.scriptName,
            versionId: statement.versionId,
          }),
        )
      }),
    )
    attestations.sort((left, right) => {
      const scriptOrder = binaryTextOrder(left.scriptName, right.scriptName)
      return scriptOrder === 0 ? binaryTextOrder(left.versionId, right.versionId) : scriptOrder
    })
    const evidence = Object.freeze({
      accountId,
      artifacts: Object.freeze(artifacts),
      attestations: Object.freeze(attestations),
      audience,
      deployments: Object.freeze(deployments),
      expectedScriptNames: Object.freeze(expectedScriptNames),
      observedFromMs,
      observedThroughMs,
      schemaVersion: 1 as const,
      verifiedAtMs,
    })
    const capability = Object.freeze({})
    capabilityStates.set(capability, evidence)
    return capability
  }

  return Object.freeze({ verify })
}

export function verifiedReaderDeploymentEvidence(
  capability: VerifiedReaderDeploymentCapability,
): VerifiedReaderDeploymentEvidence {
  if (typeof capability !== "object" || capability === null) {
    configuration("A live verified reader-deployment capability is required.")
  }
  const state = capabilityStates.get(capability)
  if (state === undefined)
    configuration("A live verified reader-deployment capability is required.")
  return state
}
