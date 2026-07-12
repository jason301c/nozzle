import {
  type ActiveWorkerDeploymentObservation,
  createCloudflareWorkerDeploymentClient,
  createReaderDeploymentVerifier,
  type ReaderAttestationTrustKey,
  type SignedReaderVersionAttestation,
  type VerifiedReaderDeploymentCapability,
  verifyReaderDeploymentStability,
  type WorkerVersionArtifactObservation,
} from "@nozzle/cloudflare"
import { type DigestFunction, NozzleError } from "@nozzle/core"
import type { TransactionalControlDatabase } from "./database.js"
import {
  D1SignedReaderBarrierStore,
  type SignedReaderBarrierReceipt,
} from "./signed-reader-barrier-store.js"

const MAX_EXTERNAL_SUBREQUESTS = 10_000_000
const MAX_READER_ATTESTATIONS = 512
const MAX_READERS = 256
const MAX_STABILITY_WINDOW_MS = 5 * 60 * 1_000
const REQUEST_CONCURRENCY = 6

export interface ReaderDeploymentControllerOptions {
  readonly accountId: string
  readonly apiToken: string
  readonly attestations: readonly SignedReaderVersionAttestation[]
  readonly audience: string
  readonly database: TransactionalControlDatabase
  readonly digest: DigestFunction
  readonly expectedScriptNames: readonly string[]
  readonly fetch?: typeof globalThis.fetch
  readonly maxAttestationValidityMs: number
  readonly maxExternalSubrequests: number
  readonly maxObservationAgeMs: number
  readonly maxObservationWindowMs: number
  readonly maxStabilityWindowMs: number
  readonly now?: () => number
  readonly trustedKeys: readonly ReaderAttestationTrustKey[]
}

export interface ReaderDeploymentControllerCallOptions {
  readonly signal?: AbortSignal
}

export interface ReaderDeploymentController {
  activate(options?: ReaderDeploymentControllerCallOptions): Promise<SignedReaderBarrierReceipt>
  assertCompatible(
    options?: ReaderDeploymentControllerCallOptions,
  ): Promise<SignedReaderBarrierReceipt>
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

function exactRecord(value: unknown, keys: readonly string[], label: string): void {
  if (!plainRecord(value)) configuration(`${label} must be a plain object.`)
  const actual = Object.keys(value).sort()
  const expected = [...keys].sort()
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    configuration(`${label} has an unsupported shape.`)
  }
}

function capturedArray<T>(value: readonly T[], label: string): readonly T[] {
  let snapshot: unknown
  try {
    snapshot = structuredClone(value)
  } catch {
    return configuration(`${label} could not be captured safely.`)
  }
  if (!Array.isArray(snapshot) || Object.keys(snapshot).length !== snapshot.length) {
    return configuration(`${label} must be a dense array.`)
  }
  return Object.freeze(snapshot as T[])
}

function scriptInventory(value: readonly string[]): readonly string[] {
  const scripts = capturedArray(value, "Expected reader scripts")
  if (scripts.length < 1 || scripts.length > MAX_READERS) {
    configuration(`Expected reader scripts must contain between 1 and ${MAX_READERS} entries.`)
  }
  for (const scriptName of scripts) {
    if (typeof scriptName !== "string" || scriptName.trim() === "" || scriptName.length > 255) {
      configuration("Each expected reader script must contain between 1 and 255 characters.")
    }
    try {
      encodeURIComponent(scriptName)
    } catch {
      configuration("Expected reader script names must contain well-formed UTF-16.")
    }
  }
  if (new Set(scripts).size !== scripts.length) {
    configuration("Expected reader scripts must be unique.")
  }
  return scripts
}

function boundedInteger(value: unknown, label: string, maximum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > maximum) {
    configuration(`${label} must be a safe integer between 1 and ${maximum}.`)
  }
  return value as number
}

function callSignal(
  options: ReaderDeploymentControllerCallOptions | undefined,
): AbortSignal | undefined {
  if (options === undefined) return undefined
  exactRecord(
    options,
    Object.hasOwn(options, "signal") ? ["signal"] : [],
    "Reader controller call options",
  )
  const signal = options.signal
  if (
    signal !== undefined &&
    (typeof signal !== "object" ||
      signal === null ||
      typeof signal.aborted !== "boolean" ||
      typeof signal.addEventListener !== "function")
  ) {
    configuration("Reader controller abort signal is malformed.")
  }
  return signal
}

async function mapBounded<T, Result>(
  values: readonly T[],
  mapper: (value: T) => Promise<Result>,
): Promise<readonly Result[]> {
  const output: Result[] = []
  for (let offset = 0; offset < values.length; offset += REQUEST_CONCURRENCY) {
    output.push(
      ...(await Promise.all(values.slice(offset, offset + REQUEST_CONCURRENCY).map(mapper))),
    )
  }
  return output
}

function inconclusive(label: string, reason: string): never {
  if (reason === "retry_required" || reason === "transport_error") {
    return resume(`${label} is inconclusive and must be retried (${reason}).`)
  }
  return intervention(`${label} is inconclusive and requires intervention (${reason}).`)
}

function completeDeployment(
  observation: ActiveWorkerDeploymentObservation,
): Extract<ActiveWorkerDeploymentObservation, { readonly kind: "complete" }> {
  if (observation.kind !== "complete") {
    return inconclusive("Active Worker deployment observation", observation.reason)
  }
  return observation
}

function completeArtifact(
  observation: WorkerVersionArtifactObservation,
): Extract<WorkerVersionArtifactObservation, { readonly kind: "complete" }> {
  if (observation.kind !== "complete") {
    return inconclusive("Worker-version artifact observation", observation.reason)
  }
  return observation
}

export async function createReaderDeploymentController(
  options: ReaderDeploymentControllerOptions,
): Promise<ReaderDeploymentController> {
  const optionalKeys = plainRecord(options)
    ? [
        ...(Object.hasOwn(options, "fetch") ? ["fetch"] : []),
        ...(Object.hasOwn(options, "now") ? ["now"] : []),
      ]
    : []
  exactRecord(
    options,
    [
      "accountId",
      "apiToken",
      "attestations",
      "audience",
      "database",
      "digest",
      "expectedScriptNames",
      "maxAttestationValidityMs",
      "maxExternalSubrequests",
      "maxObservationAgeMs",
      "maxObservationWindowMs",
      "maxStabilityWindowMs",
      "trustedKeys",
      ...optionalKeys,
    ],
    "Reader deployment controller options",
  )
  const accountId = options.accountId
  const apiToken = options.apiToken
  const attestations = capturedArray(options.attestations, "Signed reader attestations")
  if (attestations.length < 1 || attestations.length > MAX_READER_ATTESTATIONS) {
    configuration(
      `Signed reader attestations must contain between 1 and ${MAX_READER_ATTESTATIONS} entries.`,
    )
  }
  const audience = options.audience
  const database = options.database
  const digest = options.digest
  const expectedScriptNames = scriptInventory(options.expectedScriptNames)
  const fetchImplementation = options.fetch
  const maxAttestationValidityMs = options.maxAttestationValidityMs
  const maxExternalSubrequests = boundedInteger(
    options.maxExternalSubrequests,
    "External subrequest budget",
    MAX_EXTERNAL_SUBREQUESTS,
  )
  const maxObservationAgeMs = options.maxObservationAgeMs
  const maxObservationWindowMs = options.maxObservationWindowMs
  const maxStabilityWindowMs = boundedInteger(
    options.maxStabilityWindowMs,
    "Reader deployment stability window",
    MAX_STABILITY_WINDOW_MS,
  )
  const now = options.now
  const trustedKeys = capturedArray(options.trustedKeys, "Reader attestation trust keys")

  const client = createCloudflareWorkerDeploymentClient({
    accountId,
    apiToken,
    ...(fetchImplementation === undefined ? {} : { fetch: fetchImplementation }),
    ...(now === undefined ? {} : { now }),
  })
  const verifier = await createReaderDeploymentVerifier({
    accountId,
    audience,
    maxAttestationValidityMs,
    maxObservationAgeMs,
    maxObservationWindowMs,
    ...(now === undefined ? {} : { now }),
    trustedKeys,
  })
  const store = new D1SignedReaderBarrierStore(database, digest)

  function assertSubrequestBudget(rounds: number): void {
    const required = expectedScriptNames.length * 3 * rounds
    if (required > maxExternalSubrequests) {
      configuration(
        `Reader verification requires at most ${required} external subrequests, exceeding the declared budget of ${maxExternalSubrequests}.`,
      )
    }
  }

  async function observe(
    signal: AbortSignal | undefined,
  ): Promise<VerifiedReaderDeploymentCapability> {
    const deployments = await mapBounded(expectedScriptNames, async (scriptName) =>
      completeDeployment(
        await client.getActiveDeployment(scriptName, signal === undefined ? {} : { signal }),
      ),
    )
    const artifactTargets = deployments.flatMap(({ deployment }) =>
      deployment.versions.map(({ versionId }) => ({
        scriptName: deployment.scriptName,
        versionId,
      })),
    )
    const artifacts = await mapBounded(artifactTargets, async ({ scriptName, versionId }) =>
      completeArtifact(
        await client.getVersionArtifact(
          scriptName,
          versionId,
          signal === undefined ? {} : { signal },
        ),
      ),
    )
    return verifier.verify({
      artifactProofs: artifacts.map(({ proof }) => proof),
      attestations,
      deploymentProofs: deployments.map(({ proof }) => proof),
      expectedScriptNames,
    })
  }

  return Object.freeze({
    async activate(
      callOptions?: ReaderDeploymentControllerCallOptions,
    ): Promise<SignedReaderBarrierReceipt> {
      const signal = callSignal(callOptions)
      assertSubrequestBudget(3)
      const before = await observe(signal)
      const after = await observe(signal)
      await store.activate(verifyReaderDeploymentStability(before, after, maxStabilityWindowMs))
      return store.assertCurrent(await observe(signal))
    },
    async assertCompatible(
      callOptions?: ReaderDeploymentControllerCallOptions,
    ): Promise<SignedReaderBarrierReceipt> {
      const signal = callSignal(callOptions)
      assertSubrequestBudget(1)
      return store.assertCurrent(await observe(signal))
    },
  })
}
