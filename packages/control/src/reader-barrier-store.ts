import { type DigestFunction, NozzleError } from "@nozzle/core"
import type { ControlRunResult, TransactionalControlDatabase } from "./database.js"

const ATTESTATION_DOMAIN = "nozzle.reader-version-attestation.v1"
const INVENTORY_DOMAIN = "nozzle.reader-deployment-inventory.v1"
const BARRIER_DOMAIN = "nozzle.reader-barrier.v1"
const CHECKSUM = /^[0-9a-f]{64}$/u
const MAX_READERS = 256
const MAX_ACTIVE_VERSIONS = MAX_READERS * 2
const MAX_BARRIER_BYTES = 1_048_576
const REQUIRED_CONTROL_SCHEMA_VERSION = 5
const SERVER_TIME_SQL = `CAST(unixepoch('subsec') * 1000 AS INTEGER)`

export interface ReaderVersionAttestationInput {
  readonly artifactChecksum: string
  readonly controlSchemaMax: number
  readonly controlSchemaMin: number
  readonly outcomePayloadReaderMax: number
  readonly outcomePayloadReaderMin: number
  readonly scriptName: string
  readonly versionId: string
}

export interface ActiveReaderVersionInput {
  readonly versionId: string
  readonly weightBps: number
}

export interface ActiveReaderDeploymentInput {
  readonly deploymentId: string
  readonly scriptName: string
  readonly versions: readonly ActiveReaderVersionInput[]
}

export interface VerifyReaderBarrierInput {
  readonly attestations: readonly ReaderVersionAttestationInput[]
  readonly deployments: readonly ActiveReaderDeploymentInput[]
  readonly expectedScriptNames: readonly string[]
}

export type ReaderBarrierCapability = object

export interface ReaderBarrierReceipt {
  readonly activatedAtMs: number
  readonly activeDeployments: readonly ActiveReaderDeploymentInput[]
  readonly attestations: readonly (ReaderVersionAttestationInput & {
    readonly attestationChecksum: string
  })[]
  readonly barrierChecksum: string
  readonly expectedScriptNames: readonly string[]
  readonly inventoryChecksum: string
  readonly protocolVersion: 1
  readonly verifiedAtMs: number
}

interface CanonicalAttestation extends ReaderVersionAttestationInput {
  readonly attestationChecksum: string
  readonly attestationJson: string
}

export interface CanonicalReaderBarrierState {
  readonly activeDeployments: readonly ActiveReaderDeploymentInput[]
  readonly attestationMutationJson: string
  readonly attestations: readonly CanonicalAttestation[]
  readonly barrierChecksum: string
  readonly barrierJson: string
  readonly expectedScriptNames: readonly string[]
  readonly inventoryChecksum: string
}

interface BarrierRow {
  readonly activated_at_ms: unknown
  readonly barrier_checksum: unknown
  readonly barrier_json: unknown
  readonly inventory_checksum: unknown
  readonly protocol_version: unknown
  readonly reader_barrier_checksum: unknown
  readonly verified_at_ms: unknown
}

interface AttestationRow {
  readonly artifact_checksum: unknown
  readonly attestation_checksum: unknown
  readonly attestation_json: unknown
  readonly control_schema_max: unknown
  readonly control_schema_min: unknown
  readonly outcome_payload_reader_max: unknown
  readonly outcome_payload_reader_min: unknown
  readonly registered_at_ms: unknown
  readonly script_name: unknown
  readonly version_id: unknown
}

interface PartialBarrierRow {
  readonly barrier_checksum: unknown
  readonly barrier_json: unknown
  readonly inventory_checksum: unknown
  readonly protocol_version: unknown
  readonly verified_at_ms: unknown
}

interface PartialAttestationRow extends AttestationRow {
  readonly observed_at_ms: unknown
}

const capabilityStates = new WeakMap<object, CanonicalReaderBarrierState>()

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

function capturedInput(input: VerifyReaderBarrierInput): VerifyReaderBarrierInput {
  try {
    return structuredClone(input)
  } catch {
    return configuration("Reader deployment evidence could not be captured safely.")
  }
}

function capturedPersisted<T extends object>(value: T, label: string): T {
  let captured: unknown
  try {
    captured = structuredClone(value)
  } catch {
    return intervention(`${label} could not be captured safely.`)
  }
  if (!plainRecord(captured)) return intervention(`${label} is malformed.`)
  return captured as T
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

function identifier(value: unknown, label: string, maximum = 255): string {
  if (typeof value !== "string" || value.trim() === "" || value.length > maximum) {
    configuration(`${label} must contain between 1 and ${maximum} characters.`)
  }
  wellFormed(value, label)
  return value
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

function bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}

function freezeDeployment(
  deploymentId: string,
  scriptName: string,
  versions: readonly ActiveReaderVersionInput[],
): ActiveReaderDeploymentInput {
  return Object.freeze({
    deploymentId,
    scriptName,
    versions: Object.freeze(versions.map((version) => Object.freeze({ ...version }))),
  })
}

async function canonicalBarrier(
  input: VerifyReaderBarrierInput,
  digest: DigestFunction,
): Promise<CanonicalReaderBarrierState> {
  const root = exactRecord(
    input,
    ["attestations", "deployments", "expectedScriptNames"],
    "Reader deployment evidence",
  )
  const expectedInput = exactArray(root.expectedScriptNames, "Expected reader scripts")
  if (expectedInput.length < 1 || expectedInput.length > MAX_READERS) {
    configuration(`Expected reader scripts must contain between 1 and ${MAX_READERS} entries.`)
  }
  const expectedScriptNames = expectedInput
    .map((value) => identifier(value, "Reader script name"))
    .sort(binaryTextOrder)
  if (new Set(expectedScriptNames).size !== expectedScriptNames.length) {
    configuration("Expected reader scripts must not contain duplicates.")
  }

  const deploymentInput = exactArray(root.deployments, "Active reader deployments")
  if (deploymentInput.length !== expectedScriptNames.length) {
    configuration("Active reader deployments do not cover every expected script exactly once.")
  }
  const deployments: ActiveReaderDeploymentInput[] = []
  const activeKeys = new Set<string>()
  for (const candidate of deploymentInput) {
    const deployment = exactRecord(
      candidate,
      ["deploymentId", "scriptName", "versions"],
      "Active reader deployment",
    )
    const deploymentId = identifier(deployment.deploymentId, "Reader deployment ID", 128)
    const scriptName = identifier(deployment.scriptName, "Reader script name")
    const versionInput = exactArray(deployment.versions, "Active reader versions")
    if (versionInput.length < 1 || versionInput.length > 2) {
      configuration("An active reader deployment must contain one or two traffic-bearing versions.")
    }
    const versions: ActiveReaderVersionInput[] = []
    let totalWeight = 0
    for (const versionCandidate of versionInput) {
      const version = exactRecord(
        versionCandidate,
        ["versionId", "weightBps"],
        "Active reader version",
      )
      const versionId = identifier(version.versionId, "Reader version ID", 128)
      const weightBps = safeInteger(version.weightBps, "Reader version weight", 1)
      if (weightBps > 10_000)
        configuration("Reader version weight cannot exceed 10000 basis points.")
      const key = `${scriptName.length}:${scriptName}:${versionId.length}:${versionId}`
      if (activeKeys.has(key)) configuration("Active reader versions must not contain duplicates.")
      activeKeys.add(key)
      totalWeight += weightBps
      versions.push(Object.freeze({ versionId, weightBps }))
    }
    if (totalWeight !== 10_000) {
      configuration("Traffic-bearing reader-version weights must sum to 10000 basis points.")
    }
    versions.sort((left, right) => binaryTextOrder(left.versionId, right.versionId))
    deployments.push(freezeDeployment(deploymentId, scriptName, versions))
  }
  deployments.sort((left, right) => binaryTextOrder(left.scriptName, right.scriptName))
  if (
    !sameStrings(
      deployments.map((deployment) => deployment.scriptName),
      expectedScriptNames,
    )
  ) {
    configuration("Active reader deployments do not match the expected script inventory.")
  }
  const attestationInput = exactArray(root.attestations, "Reader version attestations")
  if (attestationInput.length !== activeKeys.size) {
    configuration("Reader attestations must cover every active version exactly once.")
  }
  const attestations: CanonicalAttestation[] = []
  const attestationKeys = new Set<string>()
  for (const candidate of attestationInput) {
    const attestation = exactRecord(
      candidate,
      [
        "artifactChecksum",
        "controlSchemaMax",
        "controlSchemaMin",
        "outcomePayloadReaderMax",
        "outcomePayloadReaderMin",
        "scriptName",
        "versionId",
      ],
      "Reader version attestation",
    )
    const artifactChecksum = checksum(attestation.artifactChecksum, "Reader artifact checksum")
    const controlSchemaMax = safeInteger(
      attestation.controlSchemaMax,
      "Reader maximum Control schema",
      1,
    )
    const controlSchemaMin = safeInteger(
      attestation.controlSchemaMin,
      "Reader minimum Control schema",
      1,
    )
    if (
      controlSchemaMax < controlSchemaMin ||
      controlSchemaMin > REQUIRED_CONTROL_SCHEMA_VERSION ||
      controlSchemaMax < REQUIRED_CONTROL_SCHEMA_VERSION
    ) {
      configuration("An active reader does not support the current Control schema.")
    }
    const outcomePayloadReaderMax = safeInteger(
      attestation.outcomePayloadReaderMax,
      "Maximum saga-outcome payload reader protocol",
      1,
    )
    const outcomePayloadReaderMin = safeInteger(
      attestation.outcomePayloadReaderMin,
      "Minimum saga-outcome payload reader protocol",
      1,
    )
    if (outcomePayloadReaderMax < outcomePayloadReaderMin || outcomePayloadReaderMin !== 1) {
      configuration("An active reader does not support saga-outcome payload protocol 1.")
    }
    const scriptName = identifier(attestation.scriptName, "Reader script name")
    const versionId = identifier(attestation.versionId, "Reader version ID", 128)
    const key = `${scriptName.length}:${scriptName}:${versionId.length}:${versionId}`
    if (attestationKeys.has(key)) configuration("Reader attestations must not contain duplicates.")
    attestationKeys.add(key)
    if (!activeKeys.has(key)) configuration("A reader attestation does not name an active version.")
    const attestationJson = JSON.stringify({
      artifactChecksum,
      controlSchemaMax,
      controlSchemaMin,
      outcomePayloadReaderMax,
      outcomePayloadReaderMin,
      schemaVersion: 1,
      scriptName,
      versionId,
    })
    attestations.push(
      Object.freeze({
        artifactChecksum,
        attestationChecksum: await digestText(
          digest,
          ATTESTATION_DOMAIN,
          attestationJson,
          "Reader attestation",
        ),
        attestationJson,
        controlSchemaMax,
        controlSchemaMin,
        outcomePayloadReaderMax,
        outcomePayloadReaderMin,
        scriptName,
        versionId,
      }),
    )
  }
  if (
    new Set(attestations.map(({ attestationChecksum }) => attestationChecksum)).size !==
    attestations.length
  ) {
    configuration("Reader attestation checksums must be unique.")
  }
  attestations.sort((left, right) => {
    const scriptOrder = binaryTextOrder(left.scriptName, right.scriptName)
    return scriptOrder === 0 ? binaryTextOrder(left.versionId, right.versionId) : scriptOrder
  })
  const activeDeployments = Object.freeze(deployments)
  const frozenExpected = Object.freeze(expectedScriptNames)
  const frozenAttestations = Object.freeze(attestations)
  const inventoryJson = JSON.stringify({
    activeDeployments,
    expectedScriptNames: frozenExpected,
    schemaVersion: 1,
  })
  const inventoryChecksum = await digestText(
    digest,
    INVENTORY_DOMAIN,
    inventoryJson,
    "Reader inventory",
  )
  const barrierJson = JSON.stringify({
    activeDeployments,
    attestations: frozenAttestations.map((attestation) => ({
      attestationChecksum: attestation.attestationChecksum,
      scriptName: attestation.scriptName,
      versionId: attestation.versionId,
    })),
    expectedScriptNames: frozenExpected,
    protocolVersion: 1,
    schemaVersion: 1,
  })
  const attestationMutationJson = JSON.stringify(
    frozenAttestations.map((attestation) => ({
      artifactChecksum: attestation.artifactChecksum,
      attestationChecksum: attestation.attestationChecksum,
      attestationJson: attestation.attestationJson,
      controlSchemaMax: attestation.controlSchemaMax,
      controlSchemaMin: attestation.controlSchemaMin,
      outcomePayloadReaderMax: attestation.outcomePayloadReaderMax,
      outcomePayloadReaderMin: attestation.outcomePayloadReaderMin,
      scriptName: attestation.scriptName,
      versionId: attestation.versionId,
    })),
  )
  return Object.freeze({
    activeDeployments,
    attestationMutationJson,
    attestations: frozenAttestations,
    barrierChecksum: await digestText(digest, BARRIER_DOMAIN, barrierJson, "Reader barrier"),
    barrierJson,
    expectedScriptNames: frozenExpected,
    inventoryChecksum,
  })
}

export async function verifyReaderDeploymentBarrier(
  input: VerifyReaderBarrierInput,
  digest: DigestFunction,
): Promise<ReaderBarrierCapability> {
  if (typeof digest !== "function") configuration("A reader-barrier digest function is required.")
  const state = await canonicalBarrier(capturedInput(input), digest)
  const capability = Object.freeze({})
  capabilityStates.set(capability, state)
  return capability
}

export function readerBarrierCapabilityState(
  capability: ReaderBarrierCapability,
): CanonicalReaderBarrierState {
  if (typeof capability !== "object" || capability === null) {
    configuration("A live reader-barrier capability is required.")
  }
  const state = capabilityStates.get(capability)
  if (state === undefined) configuration("A live reader-barrier capability is required.")
  return state
}

function persistedInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    return intervention(`${label} is malformed.`)
  }
  return value as number
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

function receipt(
  state: CanonicalReaderBarrierState,
  activatedAtMs: number,
  verifiedAtMs: number,
): ReaderBarrierReceipt {
  return Object.freeze({
    activatedAtMs,
    activeDeployments: state.activeDeployments,
    attestations: Object.freeze(
      state.attestations.map((attestation) =>
        Object.freeze({
          artifactChecksum: attestation.artifactChecksum,
          attestationChecksum: attestation.attestationChecksum,
          controlSchemaMax: attestation.controlSchemaMax,
          controlSchemaMin: attestation.controlSchemaMin,
          outcomePayloadReaderMax: attestation.outcomePayloadReaderMax,
          outcomePayloadReaderMin: attestation.outcomePayloadReaderMin,
          scriptName: attestation.scriptName,
          versionId: attestation.versionId,
        }),
      ),
    ),
    barrierChecksum: state.barrierChecksum,
    expectedScriptNames: state.expectedScriptNames,
    inventoryChecksum: state.inventoryChecksum,
    protocolVersion: 1,
    verifiedAtMs,
  })
}

function mutationResults(results: readonly ControlRunResult[]): void {
  if (!Array.isArray(results) || results.length !== 4) {
    intervention("Control D1 returned an incomplete reader-barrier batch result.")
  }
  for (const [index, result] of results.entries()) {
    if (!plainRecord(result) || !plainRecord(result.meta)) {
      intervention("Control D1 returned malformed reader-barrier mutation metadata.")
    }
    const changes = result.meta.changes
    const maximum = index === 0 ? MAX_ACTIVE_VERSIONS : index < 3 ? 1 : 0
    if (
      result.success !== true ||
      !Number.isSafeInteger(changes) ||
      (changes as number) < 0 ||
      (changes as number) > maximum
    ) {
      intervention("Control D1 returned malformed reader-barrier mutation metadata.")
    }
  }
}

export class D1ReaderBarrierStore {
  readonly #database: TransactionalControlDatabase
  readonly #digest: DigestFunction

  constructor(database: TransactionalControlDatabase, digest: DigestFunction) {
    if (
      typeof database !== "object" ||
      database === null ||
      typeof database.prepare !== "function" ||
      typeof database.batch !== "function"
    ) {
      configuration("A transactional Control D1 binding is required for reader activation.")
    }
    if (typeof digest !== "function") configuration("A reader-barrier digest function is required.")
    this.#database = database
    this.#digest = digest
  }

  async #assertCompatiblePartialState(state: CanonicalReaderBarrierState): Promise<void> {
    const rawBarrier = await this.#database
      .prepare(
        `SELECT "protocol_version", "barrier_checksum", "inventory_checksum", "barrier_json",
                "verified_at_ms"
         FROM "nozzle_reader_barriers" WHERE "protocol_version" = 1`,
      )
      .first<PartialBarrierRow>()
    let barrierVerifiedAtMs: number | undefined
    if (rawBarrier !== null) {
      const barrier = capturedPersisted(rawBarrier, "Partial reader barrier")
      barrierVerifiedAtMs = persistedInteger(
        barrier.verified_at_ms,
        "Partial reader-barrier verification time",
      )
      if (
        barrier.protocol_version !== 1 ||
        barrier.barrier_checksum !== state.barrierChecksum ||
        barrier.inventory_checksum !== state.inventoryChecksum ||
        barrier.barrier_json !== state.barrierJson
      ) {
        intervention("A partial reader barrier contradicts the verified deployment evidence.")
      }
    }
    const rawAttestations = await this.#database
      .prepare(
        `SELECT "attestation"."script_name", "attestation"."version_id",
                "attestation"."artifact_checksum", "attestation"."control_schema_min",
                "attestation"."control_schema_max",
                "attestation"."outcome_payload_reader_min",
                "attestation"."outcome_payload_reader_max",
                "attestation"."attestation_checksum", "attestation"."attestation_json",
                "attestation"."registered_at_ms",
                ${SERVER_TIME_SQL} AS "observed_at_ms"
         FROM "nozzle_reader_version_attestations" AS "attestation"
         JOIN json_each(?1) AS "expected"
           ON (
             (
               "attestation"."script_name" = json_extract("expected"."value", '$.scriptName')
               AND "attestation"."version_id" = json_extract("expected"."value", '$.versionId')
             )
             OR "attestation"."attestation_checksum" =
               json_extract("expected"."value", '$.attestationChecksum')
           )
         ORDER BY "attestation"."script_name", "attestation"."version_id"`,
      )
      .bind(state.attestationMutationJson)
      .all<PartialAttestationRow>()
    const result = capturedPersisted(rawAttestations, "Partial reader attestations")
    if (result.success !== true || !Array.isArray(result.results)) {
      intervention("Control D1 returned malformed partial reader attestations.")
    }
    if (barrierVerifiedAtMs !== undefined && result.results.length !== state.attestations.length) {
      intervention("A partial reader barrier is missing its attested reader versions.")
    }
    const expected = new Map(
      state.attestations.map((attestation) => [
        `${attestation.scriptName.length}:${attestation.scriptName}:${attestation.versionId.length}:${attestation.versionId}`,
        attestation,
      ]),
    )
    for (const rawAttestation of result.results) {
      const attestation = capturedPersisted(rawAttestation, "Partial reader attestation")
      const registeredAtMs = persistedInteger(
        attestation.registered_at_ms,
        "Partial reader registration time",
      )
      const observedAtMs = persistedInteger(
        attestation.observed_at_ms,
        "Partial reader observation time",
      )
      if (
        registeredAtMs > observedAtMs ||
        (barrierVerifiedAtMs !== undefined && registeredAtMs > barrierVerifiedAtMs)
      ) {
        intervention("A partial reader attestation has an impossible registration time.")
      }
      const scriptName = persistedText(attestation.script_name, "Partial reader script name")
      const versionId = persistedText(attestation.version_id, "Partial reader version ID")
      const key = `${scriptName.length}:${scriptName}:${versionId.length}:${versionId}`
      const trusted = expected.get(key)
      if (
        trusted === undefined ||
        attestation.artifact_checksum !== trusted.artifactChecksum ||
        attestation.control_schema_min !== trusted.controlSchemaMin ||
        attestation.control_schema_max !== trusted.controlSchemaMax ||
        attestation.outcome_payload_reader_min !== trusted.outcomePayloadReaderMin ||
        attestation.outcome_payload_reader_max !== trusted.outcomePayloadReaderMax ||
        attestation.attestation_checksum !== trusted.attestationChecksum ||
        attestation.attestation_json !== trusted.attestationJson
      ) {
        intervention("A partial reader attestation contradicts the verified deployment evidence.")
      }
    }
  }

  async get(): Promise<ReaderBarrierReceipt | undefined> {
    const rawRow = await this.#database
      .prepare(
        `SELECT "activation"."protocol_version", "activation"."reader_barrier_checksum",
                "activation"."activated_at_ms", "barrier"."barrier_checksum",
                "barrier"."inventory_checksum", "barrier"."barrier_json",
                "barrier"."verified_at_ms"
         FROM "nozzle_saga_outcome_payload_activations" AS "activation"
         LEFT JOIN "nozzle_reader_barriers" AS "barrier"
           ON "barrier"."protocol_version" = "activation"."protocol_version"
         WHERE "activation"."protocol_version" = 1`,
      )
      .first<BarrierRow>()
    if (rawRow === null) return undefined
    const row = capturedPersisted(rawRow, "Persisted reader barrier")
    if (
      row.protocol_version !== 1 ||
      typeof row.reader_barrier_checksum !== "string" ||
      !CHECKSUM.test(row.reader_barrier_checksum) ||
      typeof row.barrier_checksum !== "string" ||
      !CHECKSUM.test(row.barrier_checksum) ||
      typeof row.inventory_checksum !== "string" ||
      !CHECKSUM.test(row.inventory_checksum) ||
      row.reader_barrier_checksum !== row.barrier_checksum
    ) {
      return intervention("Persisted reader-barrier identity is malformed or contradictory.")
    }
    const activatedAtMs = persistedInteger(row.activated_at_ms, "Persisted activation time")
    const verifiedAtMs = persistedInteger(row.verified_at_ms, "Persisted barrier verification time")
    if (activatedAtMs < verifiedAtMs) {
      return intervention("Persisted reader-barrier time order is contradictory.")
    }
    const barrierJson = persistedText(row.barrier_json, "Persisted reader barrier JSON")
    if (bytes(barrierJson) > MAX_BARRIER_BYTES) {
      return intervention("Persisted reader barrier exceeds its byte limit.")
    }
    const barrier = parsedJson(barrierJson, "Persisted reader barrier")
    if (!plainRecord(barrier)) return intervention("Persisted reader barrier is malformed.")
    const references = barrier.attestations
    if (!Array.isArray(references) || Object.keys(references).length !== references.length) {
      return intervention("Persisted reader-barrier attestation references are malformed.")
    }
    const keyJson = JSON.stringify(references)
    const rawAttestations = await this.#database
      .prepare(
        `SELECT "attestation"."script_name", "attestation"."version_id",
                "attestation"."artifact_checksum", "attestation"."control_schema_min",
                "attestation"."control_schema_max",
                "attestation"."outcome_payload_reader_min",
                "attestation"."outcome_payload_reader_max",
                "attestation"."attestation_checksum", "attestation"."attestation_json",
                "attestation"."registered_at_ms"
         FROM "nozzle_reader_version_attestations" AS "attestation"
         JOIN json_each(?1) AS "expected"
           ON "attestation"."script_name" = json_extract("expected"."value", '$.scriptName')
          AND "attestation"."version_id" = json_extract("expected"."value", '$.versionId')
         ORDER BY "attestation"."script_name", "attestation"."version_id"`,
      )
      .bind(keyJson)
      .all<AttestationRow>()
    const result = capturedPersisted(rawAttestations, "Persisted reader attestations")
    if (result.success !== true || !Array.isArray(result.results)) {
      return intervention("Control D1 returned malformed reader attestations.")
    }
    const attestations: ReaderVersionAttestationInput[] = []
    for (const rawAttestation of result.results) {
      const attestation = capturedPersisted(rawAttestation, "Persisted reader attestation")
      const registeredAtMs = persistedInteger(
        attestation.registered_at_ms,
        "Persisted reader registration time",
      )
      if (registeredAtMs > verifiedAtMs) {
        return intervention("Persisted reader attestation was registered after its barrier.")
      }
      attestations.push({
        artifactChecksum: persistedText(
          attestation.artifact_checksum,
          "Persisted reader artifact checksum",
        ),
        controlSchemaMax: persistedInteger(
          attestation.control_schema_max,
          "Persisted maximum Control schema",
        ),
        controlSchemaMin: persistedInteger(
          attestation.control_schema_min,
          "Persisted minimum Control schema",
        ),
        outcomePayloadReaderMax: persistedInteger(
          attestation.outcome_payload_reader_max,
          "Persisted maximum outcome-payload reader protocol",
        ),
        outcomePayloadReaderMin: persistedInteger(
          attestation.outcome_payload_reader_min,
          "Persisted minimum outcome-payload reader protocol",
        ),
        scriptName: persistedText(attestation.script_name, "Persisted reader script name"),
        versionId: persistedText(attestation.version_id, "Persisted reader version ID"),
      })
    }
    let state: CanonicalReaderBarrierState
    try {
      state = await canonicalBarrier(
        {
          attestations,
          deployments: barrier.activeDeployments as readonly ActiveReaderDeploymentInput[],
          expectedScriptNames: barrier.expectedScriptNames as readonly string[],
        },
        this.#digest,
      )
    } catch {
      return intervention("Persisted reader barrier failed canonical verification.")
    }
    if (
      state.barrierJson !== barrierJson ||
      state.barrierChecksum !== row.barrier_checksum ||
      state.inventoryChecksum !== row.inventory_checksum ||
      state.attestations.some((expected, index) => {
        const persisted = result.results[index] as AttestationRow
        return (
          expected.attestationChecksum !== persisted.attestation_checksum ||
          expected.attestationJson !== persisted.attestation_json
        )
      })
    ) {
      return intervention("Persisted reader barrier contradicts its canonical evidence.")
    }
    return receipt(state, activatedAtMs, verifiedAtMs)
  }

  async activate(capability: ReaderBarrierCapability): Promise<ReaderBarrierReceipt> {
    const state = readerBarrierCapabilityState(capability)
    const existing = await this.get()
    if (existing !== undefined) {
      if (existing.barrierChecksum !== state.barrierChecksum) {
        return intervention("Saga-outcome payload activation is bound to another reader barrier.")
      }
      return existing
    }
    await this.#assertCompatiblePartialState(state)
    const statements = [
      this.#database
        .prepare(
          `INSERT INTO "nozzle_reader_version_attestations"
           ("script_name", "version_id", "artifact_checksum", "control_schema_min",
            "control_schema_max", "outcome_payload_reader_min", "outcome_payload_reader_max",
            "attestation_checksum",
            "attestation_json", "registered_at_ms")
           SELECT json_extract("entry"."value", '$.scriptName'),
                  json_extract("entry"."value", '$.versionId'),
                  json_extract("entry"."value", '$.artifactChecksum'),
                  json_extract("entry"."value", '$.controlSchemaMin'),
                  json_extract("entry"."value", '$.controlSchemaMax'),
                  json_extract("entry"."value", '$.outcomePayloadReaderMin'),
                  json_extract("entry"."value", '$.outcomePayloadReaderMax'),
                  json_extract("entry"."value", '$.attestationChecksum'),
                  json_extract("entry"."value", '$.attestationJson'), ${SERVER_TIME_SQL}
           FROM json_each(?1) AS "entry"
           WHERE NOT EXISTS (
             SELECT 1 FROM "nozzle_saga_outcome_payload_activations"
             WHERE "protocol_version" = 1
           )
           ON CONFLICT ("script_name", "version_id") DO NOTHING`,
        )
        .bind(state.attestationMutationJson),
      this.#database
        .prepare(
          `INSERT INTO "nozzle_reader_barriers"
           ("protocol_version", "barrier_checksum", "inventory_checksum", "barrier_json",
            "verified_at_ms")
           SELECT 1, ?2, ?3, ?4, ${SERVER_TIME_SQL}
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
          state.attestationMutationJson,
          state.barrierChecksum,
          state.inventoryChecksum,
          state.barrierJson,
          state.attestations.length,
        ),
      this.#database
        .prepare(
          `INSERT INTO "nozzle_saga_outcome_payload_activations"
           ("protocol_version", "reader_barrier_checksum", "activated_at_ms")
           SELECT 1, ?1, ${SERVER_TIME_SQL}
           FROM "nozzle_reader_barriers"
           WHERE "protocol_version" = 1 AND "barrier_checksum" = ?1
           ON CONFLICT ("protocol_version") DO NOTHING`,
        )
        .bind(state.barrierChecksum),
      this.#database
        .prepare(
          `INSERT INTO "nozzle_control_schema_versions" ("schema_version", "published_at_ms")
           SELECT 0, 0
           WHERE NOT EXISTS (
             SELECT 1
             FROM "nozzle_saga_outcome_payload_activations" AS "activation"
             JOIN "nozzle_reader_barriers" AS "barrier"
               ON "barrier"."protocol_version" = "activation"."protocol_version"
              AND "barrier"."barrier_checksum" = "activation"."reader_barrier_checksum"
             WHERE "activation"."protocol_version" = 1
               AND "barrier"."barrier_checksum" = ?1
           )`,
        )
        .bind(state.barrierChecksum),
    ]
    let results: readonly ControlRunResult[] | undefined
    try {
      results = await this.#database.batch(statements)
    } catch {
      // The immutable receipt below decides whether the activation committed.
    }
    if (results !== undefined) mutationResults(results)
    const activated = await this.get()
    if (activated === undefined) {
      await this.#assertCompatiblePartialState(state)
      return resume("Reader-barrier activation did not produce an immutable receipt; retry safely.")
    }
    if (activated.barrierChecksum !== state.barrierChecksum) {
      return intervention("Saga-outcome payload activation is bound to another reader barrier.")
    }
    return activated
  }

  async assertCompatible(capability: ReaderBarrierCapability): Promise<void> {
    await this.#assertCompatiblePartialState(readerBarrierCapabilityState(capability))
  }
}
