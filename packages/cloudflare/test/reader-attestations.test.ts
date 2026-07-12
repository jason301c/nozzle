import { afterEach, beforeAll, describe, expect, it, vi } from "vitest"
import {
  createReaderDeploymentVerifier,
  type ReaderAttestationTrustKey,
  type ReaderDeploymentVerificationInput,
  type ReaderDeploymentVerifierOptions,
  type ReaderVersionAttestationStatement,
  readerVersionAttestationSigningBytes,
  type SignedReaderVersionAttestation,
  type VerifiedReaderDeploymentCapability,
  verifiedReaderDeploymentEvidence,
  verifiedReaderDeploymentStabilityEvidence,
  verifyReaderDeploymentStability,
} from "../src/reader-attestations.js"
import {
  type ActiveWorkerDeploymentProofState,
  createActiveWorkerDeploymentProof,
} from "../src/worker-deployment-proof.js"
import {
  createWorkerVersionArtifactProof,
  type WorkerVersionArtifactProofState,
} from "../src/worker-version-proof.js"

const accountId = "a".repeat(32)
const audience = "nozzle:fictional-environment"
const controllerScript = "nozzle-controller"
const routerScript = "nozzle-router"
const controllerDeployment = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const routerDeployment = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
const versionA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const versionB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const versionC = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

let privateKey: CryptoKey
let trustKey: ReaderAttestationTrustKey

function base64Url(bytes: Uint8Array): string {
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, "-").replace(/\//gu, "_").replace(/=+$/u, "")
}

beforeAll(async () => {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
    "sign",
    "verify",
  ])) as CryptoKeyPair
  privateKey = pair.privateKey
  trustKey = Object.freeze({
    keyId: "release-key",
    publicKeyBase64Url: base64Url(
      new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey)),
    ),
  })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function statement(
  scriptName = controllerScript,
  versionId = versionA,
  overrides: Readonly<Partial<ReaderVersionAttestationStatement>> = {},
): ReaderVersionAttestationStatement {
  return {
    artifactChecksum: "1".repeat(64),
    audience,
    controlSchemaMax: 6,
    controlSchemaMin: 5,
    expiresAtMs: 1_500,
    issuedAtMs: 900,
    keyId: "release-key",
    outcomePayloadReaderMax: 1,
    outcomePayloadReaderMin: 1,
    schemaVersion: 1,
    scriptName,
    versionId,
    ...overrides,
  }
}

async function signed(
  value: ReaderVersionAttestationStatement,
  key: CryptoKey = privateKey,
): Promise<SignedReaderVersionAttestation> {
  const signature = await crypto.subtle.sign(
    { name: "Ed25519" },
    key,
    readerVersionAttestationSigningBytes(value),
  )
  return Object.freeze({ signature: base64Url(new Uint8Array(signature)), statement: value })
}

function proof(
  scriptName = controllerScript,
  versions: ActiveWorkerDeploymentProofState["deployment"]["versions"] | undefined = [
    { versionId: versionA, weightBps: 10_000 },
  ],
  overrides: {
    readonly accountId?: string
    readonly deploymentId?: string
    readonly evidence?: Readonly<Partial<ActiveWorkerDeploymentProofState["evidence"]>>
  } = {},
) {
  return createActiveWorkerDeploymentProof({
    accountId: overrides.accountId ?? accountId,
    deployment: Object.freeze({
      createdAtMs: 800,
      deploymentId: overrides.deploymentId ?? controllerDeployment,
      scriptName,
      versions: Object.freeze(versions.map((version) => Object.freeze({ ...version }))),
    }),
    evidence: Object.freeze({
      bodyBytes: 100,
      bodyState: "complete",
      completedAtMs: 1_001,
      rateLimit: Object.freeze({}),
      responseChecksum: "2".repeat(64),
      startedAtMs: 1_000,
      status: 200,
      ...overrides.evidence,
    }),
  })
}

function artifactProof(
  scriptName = controllerScript,
  versionId = versionA,
  artifactChecksum = "1".repeat(64),
  overrides: {
    readonly accountId?: string
    readonly evidence?: Readonly<Partial<WorkerVersionArtifactProofState["evidence"]>>
  } = {},
) {
  return createWorkerVersionArtifactProof({
    accountId: overrides.accountId ?? accountId,
    artifact: Object.freeze({ artifactChecksum, scriptName, versionId }),
    evidence: Object.freeze({
      bodyBytes: 100,
      bodyState: "complete",
      completedAtMs: 1_001,
      rateLimit: Object.freeze({}),
      responseChecksum: "4".repeat(64),
      startedAtMs: 1_000,
      status: 200,
      ...overrides.evidence,
    }),
  })
}

async function verifier(overrides: Readonly<Partial<ReaderDeploymentVerifierOptions>> = {}) {
  return createReaderDeploymentVerifier({
    accountId,
    audience,
    maxAttestationValidityMs: 1_000,
    maxObservationAgeMs: 200,
    maxObservationWindowMs: 100,
    now: () => 1_100,
    trustedKeys: [trustKey],
    ...overrides,
  })
}

async function validInput(timeOffset = 0): Promise<ReaderDeploymentVerificationInput> {
  return {
    artifactProofs: [
      artifactProof(controllerScript, versionA, "1".repeat(64), {
        evidence: { completedAtMs: 1_001 + timeOffset, startedAtMs: 1_000 + timeOffset },
      }),
      artifactProof(controllerScript, versionB, "2".repeat(64), {
        evidence: { completedAtMs: 1_001 + timeOffset, startedAtMs: 1_000 + timeOffset },
      }),
      artifactProof(routerScript, versionC, "3".repeat(64), {
        evidence: {
          completedAtMs: 1_003 + timeOffset,
          startedAtMs: 1_002 + timeOffset,
        },
      }),
    ],
    attestations: [
      await signed(statement(controllerScript, versionA)),
      await signed(
        statement(controllerScript, versionB, {
          artifactChecksum: "2".repeat(64),
          controlSchemaMax: 6,
          controlSchemaMin: 4,
          outcomePayloadReaderMax: 2,
        }),
      ),
      await signed(statement(routerScript, versionC, { artifactChecksum: "3".repeat(64) })),
    ],
    deploymentProofs: [
      proof(
        controllerScript,
        [
          { versionId: versionB, weightBps: 7_500 },
          { versionId: versionA, weightBps: 2_500 },
        ],
        {
          evidence: {
            completedAtMs: 1_001 + timeOffset,
            startedAtMs: 1_000 + timeOffset,
          },
        },
      ),
      proof(routerScript, [{ versionId: versionC, weightBps: 10_000 }], {
        deploymentId: routerDeployment,
        evidence: {
          completedAtMs: 1_003 + timeOffset,
          startedAtMs: 1_002 + timeOffset,
        },
      }),
    ],
    expectedScriptNames: [routerScript, controllerScript],
  }
}

async function verifiedAt(timeOffset = 0): Promise<VerifiedReaderDeploymentCapability> {
  return (await verifier({ now: () => 1_100 + timeOffset })).verify(await validInput(timeOffset))
}

function requiredAttestation(
  input: ReaderDeploymentVerificationInput,
  index: number,
): SignedReaderVersionAttestation {
  const value = input.attestations[index]
  if (value === undefined) throw new Error("Missing test attestation.")
  return value
}

describe("reader-version attestation signing format", () => {
  it("produces stable owned domain-framed bytes", () => {
    const input = statement()
    const reordered = {
      versionId: input.versionId,
      scriptName: input.scriptName,
      schemaVersion: input.schemaVersion,
      outcomePayloadReaderMin: input.outcomePayloadReaderMin,
      outcomePayloadReaderMax: input.outcomePayloadReaderMax,
      keyId: input.keyId,
      issuedAtMs: input.issuedAtMs,
      expiresAtMs: input.expiresAtMs,
      controlSchemaMin: input.controlSchemaMin,
      controlSchemaMax: input.controlSchemaMax,
      audience: input.audience,
      artifactChecksum: input.artifactChecksum,
    }
    const first = readerVersionAttestationSigningBytes(input)
    const second = readerVersionAttestationSigningBytes(reordered)
    expect(first).toEqual(second)
    first.fill(0)
    expect(readerVersionAttestationSigningBytes(input)).toEqual(second)
    expect(readerVersionAttestationSigningBytes(statement("reader-\u{10000}"))).toBeInstanceOf(
      Uint8Array,
    )
  })

  it("rejects unsafe capture and every malformed signed field boundary", () => {
    const valid = statement()
    const malformed: readonly unknown[] = [
      new Proxy(valid, {}),
      [],
      { ...valid, extra: true },
      { ...valid, schemaVersion: 2 },
      { ...valid, artifactChecksum: "bad" },
      { ...valid, audience: "" },
      { ...valid, audience: "x".repeat(513) },
      { ...valid, audience: "\ud800" },
      { ...valid, audience: "\udc00" },
      { ...valid, controlSchemaMax: 0 },
      { ...valid, controlSchemaMin: 0 },
      { ...valid, controlSchemaMax: 4, controlSchemaMin: 5 },
      { ...valid, expiresAtMs: 0 },
      { ...valid, issuedAtMs: -1 },
      { ...valid, expiresAtMs: 900 },
      { ...valid, keyId: "" },
      { ...valid, keyId: "x".repeat(129) },
      { ...valid, outcomePayloadReaderMax: 0 },
      { ...valid, outcomePayloadReaderMin: 0 },
      { ...valid, outcomePayloadReaderMax: 1, outcomePayloadReaderMin: 2 },
      { ...valid, scriptName: "" },
      { ...valid, versionId: "" },
    ]
    for (const value of malformed) {
      expect(() =>
        readerVersionAttestationSigningBytes(value as unknown as ReaderVersionAttestationStatement),
      ).toThrow()
    }
  })
})

describe("reader deployment verifier configuration", () => {
  it("rejects malformed accounts, policies, clocks, and trust registries", async () => {
    const valid = {
      accountId,
      audience,
      maxAttestationValidityMs: 1_000,
      maxObservationAgeMs: 100,
      maxObservationWindowMs: 100,
      now: () => 1_100,
      trustedKeys: [trustKey],
    }
    const tooManyKeys = Array.from({ length: 65 }, (_, index) => ({
      ...trustKey,
      keyId: `key-${index}`,
    }))
    for (const options of [
      null,
      { ...valid, accountId: "" },
      { ...valid, accountId: "not-an-account" },
      { ...valid, audience: "" },
      { ...valid, maxAttestationValidityMs: 0 },
      { ...valid, maxAttestationValidityMs: 30 * 24 * 60 * 60 * 1_000 + 1 },
      { ...valid, maxObservationAgeMs: 0 },
      { ...valid, maxObservationAgeMs: 300_001 },
      { ...valid, maxObservationWindowMs: 0 },
      { ...valid, maxObservationWindowMs: 300_001 },
      { ...valid, now: 1 },
      { ...valid, trustedKeys: new Proxy([trustKey], {}) },
      { ...valid, trustedKeys: {} },
      { ...valid, trustedKeys: [] },
      { ...valid, trustedKeys: tooManyKeys },
      { ...valid, trustedKeys: [{ ...trustKey, extra: true }] },
      { ...valid, trustedKeys: [{ ...trustKey, keyId: "" }] },
      { ...valid, trustedKeys: [trustKey, trustKey] },
      { ...valid, trustedKeys: [{ ...trustKey, publicKeyBase64Url: "!" }] },
      { ...valid, trustedKeys: [{ ...trustKey, publicKeyBase64Url: "A" }] },
      {
        ...valid,
        trustedKeys: [{ ...trustKey, publicKeyBase64Url: `${"A".repeat(42)}B` }],
      },
    ]) {
      await expect(createReaderDeploymentVerifier(options as never)).rejects.toThrow()
    }
    const { now: _now, ...platformClock } = valid
    await expect(createReaderDeploymentVerifier(platformClock)).resolves.toBeDefined()
  })

  it("reports unsupported Web Crypto and Ed25519 import failures", async () => {
    const nativeCrypto = globalThis.crypto
    vi.stubGlobal("crypto", {})
    await expect(verifier()).rejects.toThrow(/Web Crypto/u)

    vi.stubGlobal("crypto", {
      subtle: {
        importKey: async () => {
          throw new Error("injected import failure")
        },
      },
    })
    await expect(verifier()).rejects.toThrow(/could not be imported/u)
    vi.stubGlobal("crypto", nativeCrypto)
  })
})

describe("signed active-reader convergence", () => {
  it("verifies, canonically orders, freezes, and opaquely retains exact evidence", async () => {
    const input = await validInput()
    const capability = await (await verifier()).verify({
      artifactProofs: [...input.artifactProofs].reverse(),
      attestations: [...input.attestations].reverse(),
      deploymentProofs: [...input.deploymentProofs].reverse(),
      expectedScriptNames: input.expectedScriptNames,
    })
    expect(Object.isFrozen(capability)).toBe(true)
    expect(Object.keys(capability)).toEqual([])
    const evidence = verifiedReaderDeploymentEvidence(capability)
    expect(evidence).toMatchObject({
      accountId,
      artifacts: [
        {
          artifactChecksum: "1".repeat(64),
          observation: { responseChecksum: "4".repeat(64) },
          scriptName: controllerScript,
          versionId: versionA,
        },
        {
          artifactChecksum: "2".repeat(64),
          scriptName: controllerScript,
          versionId: versionB,
        },
        {
          artifactChecksum: "3".repeat(64),
          scriptName: routerScript,
          versionId: versionC,
        },
      ],
      attestations: [
        {
          expiresAtMs: 1_500,
          issuedAtMs: 900,
          keyId: "release-key",
          publicKeyBase64Url: trustKey.publicKeyBase64Url,
          scriptName: controllerScript,
          signature: expect.stringMatching(/^[A-Za-z0-9_-]{86}$/u),
          versionId: versionA,
        },
        { keyId: "release-key", scriptName: controllerScript, versionId: versionB },
        { keyId: "release-key", scriptName: routerScript, versionId: versionC },
      ],
      audience,
      deployments: [
        {
          createdAtMs: 800,
          deploymentId: controllerDeployment,
          observation: { responseChecksum: "2".repeat(64) },
          scriptName: controllerScript,
          versions: [
            { versionId: versionA, weightBps: 2_500 },
            { versionId: versionB, weightBps: 7_500 },
          ],
        },
        { deploymentId: routerDeployment, scriptName: routerScript },
      ],
      expectedScriptNames: [controllerScript, routerScript],
      observedFromMs: 1_000,
      observedThroughMs: 1_003,
      schemaVersion: 1,
      verifiedAtMs: 1_100,
    })
    expect(Object.isFrozen(evidence)).toBe(true)
    expect(Object.isFrozen(evidence.artifacts)).toBe(true)
    expect(Object.isFrozen(evidence.artifacts[0])).toBe(true)
    expect(Object.isFrozen(evidence.attestations)).toBe(true)
    expect(Object.isFrozen(evidence.deployments)).toBe(true)
    expect(Object.isFrozen(evidence.deployments[0]?.versions)).toBe(true)

    for (const fake of [{}, structuredClone(capability), null, "verified"]) {
      expect(() => verifiedReaderDeploymentEvidence(fake as never)).toThrow(/live verified/u)
    }
  })

  it("captures signed inputs and proof arrays before asynchronous verification", async () => {
    const input = await validInput()
    const mutableAttestations = [...input.attestations]
    const mutableArtifactProofs = [...input.artifactProofs]
    const mutableProofs = [...input.deploymentProofs]
    const verifying = (await verifier()).verify({
      artifactProofs: mutableArtifactProofs,
      attestations: mutableAttestations,
      deploymentProofs: mutableProofs,
      expectedScriptNames: [...input.expectedScriptNames],
    })
    mutableAttestations.splice(0)
    mutableArtifactProofs.splice(0)
    mutableProofs.splice(0)
    await expect(verifying).resolves.toBeDefined()
  })

  it("accepts overlapping key rotation and rejects a removed signing key", async () => {
    const nextPair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, [
      "sign",
      "verify",
    ])) as CryptoKeyPair
    const nextTrustKey = {
      keyId: "next-release-key",
      publicKeyBase64Url: base64Url(
        new Uint8Array(await crypto.subtle.exportKey("raw", nextPair.publicKey)),
      ),
    }
    const nextStatement = statement(controllerScript, versionA, {
      keyId: nextTrustKey.keyId,
    })
    const nextAttestation = await signed(nextStatement, nextPair.privateKey)
    const input = {
      artifactProofs: [artifactProof()],
      attestations: [nextAttestation],
      deploymentProofs: [proof()],
      expectedScriptNames: [controllerScript],
    }
    await expect(
      (await verifier({ trustedKeys: [trustKey, nextTrustKey] })).verify(input),
    ).resolves.toBeDefined()
    await expect((await verifier()).verify(input)).rejects.toThrow(/untrusted key ID/u)
  })

  it("verifies the sealed 256-script and 512-active-version boundary", async () => {
    const scriptNames = Array.from(
      { length: 256 },
      (_, index) => `reader-${index.toString().padStart(3, "0")}`,
    )
    const deploymentProofs = scriptNames.map((scriptName, scriptIndex) =>
      proof(
        scriptName,
        [0, 1].map((offset) => ({
          versionId: `00000000-0000-4000-8000-${String(scriptIndex * 2 + offset).padStart(12, "0")}`,
          weightBps: 5_000,
        })),
        {
          deploymentId: `10000000-0000-4000-8000-${String(scriptIndex).padStart(12, "0")}`,
        },
      ),
    )
    const statements = scriptNames.flatMap((scriptName, scriptIndex) =>
      [0, 1].map((offset) =>
        statement(
          scriptName,
          `00000000-0000-4000-8000-${String(scriptIndex * 2 + offset).padStart(12, "0")}`,
          { artifactChecksum: ((scriptIndex * 2 + offset) % 16).toString(16).repeat(64) },
        ),
      ),
    )
    const attestations = await Promise.all(statements.map((value) => signed(value)))
    const artifactProofs = statements.map((value) =>
      artifactProof(value.scriptName, value.versionId, value.artifactChecksum),
    )
    let verifiedAtMs = 1_100
    const activeVerifier = await verifier({ now: () => verifiedAtMs })
    const capability = await activeVerifier.verify({
      artifactProofs: [...artifactProofs].reverse(),
      attestations: [...attestations].reverse(),
      deploymentProofs: [...deploymentProofs].reverse(),
      expectedScriptNames: [...scriptNames].reverse(),
    })
    const evidence = verifiedReaderDeploymentEvidence(capability)
    expect(evidence.deployments).toHaveLength(256)
    expect(evidence.attestations).toHaveLength(512)

    verifiedAtMs = 1_300
    const reobserved = await activeVerifier.verify({
      artifactProofs: statements.map((value) =>
        artifactProof(value.scriptName, value.versionId, value.artifactChecksum, {
          evidence: { completedAtMs: 1_201, startedAtMs: 1_200 },
        }),
      ),
      attestations,
      deploymentProofs: scriptNames.map((scriptName, scriptIndex) =>
        proof(
          scriptName,
          [0, 1].map((offset) => ({
            versionId: `00000000-0000-4000-8000-${String(scriptIndex * 2 + offset).padStart(12, "0")}`,
            weightBps: 5_000,
          })),
          {
            deploymentId: `10000000-0000-4000-8000-${String(scriptIndex).padStart(12, "0")}`,
            evidence: { completedAtMs: 1_201, startedAtMs: 1_200 },
          },
        ),
      ),
      expectedScriptNames: scriptNames,
    })
    expect(
      verifiedReaderDeploymentStabilityEvidence(
        verifyReaderDeploymentStability(capability, reobserved, 400),
      ).after.attestations,
    ).toHaveLength(512)
  })

  it("rejects malformed input envelopes, inventories, and proof identities", async () => {
    const input = await validInput()
    const verify = (await verifier()).verify
    const sparseProofs = [...input.deploymentProofs]
    delete sparseProofs[0]
    const malformed: readonly unknown[] = [
      [],
      { ...input, extra: true },
      { ...input, attestations: new Proxy([...input.attestations], {}) },
      { ...input, deploymentProofs: {} },
      { ...input, deploymentProofs: sparseProofs },
      { ...input, expectedScriptNames: [] },
      {
        ...input,
        expectedScriptNames: Array.from({ length: 257 }, (_, index) => `reader-${index}`),
      },
      { ...input, expectedScriptNames: [controllerScript, controllerScript] },
      { ...input, expectedScriptNames: [""] },
      { ...input, deploymentProofs: input.deploymentProofs.slice(1) },
      { ...input, deploymentProofs: [{}, input.deploymentProofs[1]] },
      { ...input, deploymentProofs: [null, input.deploymentProofs[1]] },
      {
        ...input,
        deploymentProofs: [structuredClone(input.deploymentProofs[0]), input.deploymentProofs[1]],
      },
    ]
    for (const candidate of malformed) {
      await expect(verify(candidate as ReaderDeploymentVerificationInput)).rejects.toThrow()
    }
  })

  it("rejects account, duplicate-script, inventory, and version-bound contradictions", async () => {
    const input = await validInput()
    const verify = (await verifier()).verify
    await expect(
      verify({
        ...input,
        deploymentProofs: [
          proof(controllerScript, undefined, { accountId: "b".repeat(32) }),
          input.deploymentProofs[1] as object,
        ],
      }),
    ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    await expect(
      verify({
        ...input,
        deploymentProofs: [proof(controllerScript), proof(controllerScript)],
      }),
    ).rejects.toThrow(/duplicate reader script/u)
    await expect(
      verify({ ...input, expectedScriptNames: [controllerScript, "wrong-reader"] }),
    ).rejects.toThrow(/expected reader inventory/u)

    const duplicatedVersion = proof(controllerScript, [
      { versionId: versionA, weightBps: 5_000 },
      { versionId: versionA, weightBps: 5_000 },
    ])
    await expect(
      verify({
        artifactProofs: [],
        attestations: input.attestations.slice(0, 1),
        deploymentProofs: [duplicatedVersion],
        expectedScriptNames: [controllerScript],
      }),
    ).rejects.toThrow(/duplicate reader version/u)

    const oversizedVersions = Array.from({ length: 513 }, (_, index) => ({
      versionId: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      weightBps: index === 0 ? 9_488 : 1,
    }))
    await expect(
      verify({
        artifactProofs: [],
        attestations: [],
        deploymentProofs: [proof(controllerScript, oversizedVersions)],
        expectedScriptNames: [controllerScript],
      }),
    ).rejects.toThrow(/exceed the reader-version proof bound/u)
  })

  it("requires one live account-bound artifact proof for every active version", async () => {
    const input = await validInput()
    const verify = (await verifier()).verify
    await expect(
      verify({ ...input, artifactProofs: input.artifactProofs.slice(1) }),
    ).rejects.toThrow(/cover every active version/u)
    for (const fake of [{}, null, "artifact", structuredClone(input.artifactProofs[0])]) {
      await expect(
        verify({ ...input, artifactProofs: [fake as object, ...input.artifactProofs.slice(1)] }),
      ).rejects.toThrow(/live Worker-version artifact proof/u)
    }
    await expect(
      verify({
        ...input,
        artifactProofs: [
          artifactProof(controllerScript, versionA, "1".repeat(64), {
            accountId: "b".repeat(32),
          }),
          ...input.artifactProofs.slice(1),
        ],
      }),
    ).rejects.toThrow(/another Cloudflare account/u)
    await expect(
      verify({
        ...input,
        artifactProofs: [
          artifactProof(controllerScript, versionA),
          artifactProof(controllerScript, versionA),
          input.artifactProofs[2] as object,
        ],
      }),
    ).rejects.toThrow(/duplicate active version/u)
    await expect(
      verify({
        ...input,
        artifactProofs: [
          artifactProof(controllerScript, versionC),
          ...input.artifactProofs.slice(1),
        ],
      }),
    ).rejects.toThrow(/does not name an active version/u)
  })

  it("rejects malformed artifact evidence and signed-to-live checksum contradictions", async () => {
    const input = await validInput()
    const malformedEvidence: readonly Readonly<
      Partial<WorkerVersionArtifactProofState["evidence"]>
    >[] = [
      { status: 201 },
      { bodyState: "unreadable" },
      { responseChecksum: null as never },
      { responseChecksum: "bad" },
      { startedAtMs: 1.5 },
      { startedAtMs: -1 },
      { completedAtMs: 1.5 },
      { completedAtMs: 999 },
    ]
    for (const evidence of malformedEvidence) {
      await expect(
        (await verifier()).verify({
          artifactProofs: [artifactProof(controllerScript, versionA, "1".repeat(64), { evidence })],
          attestations: input.attestations.slice(0, 1),
          deploymentProofs: [proof()],
          expectedScriptNames: [controllerScript],
        }),
      ).rejects.toThrow(/artifact proof evidence is malformed/u)
    }
    await expect(
      (await verifier()).verify({
        ...input,
        artifactProofs: [
          artifactProof(controllerScript, versionA, "9".repeat(64)),
          ...input.artifactProofs.slice(1),
        ],
      }),
    ).rejects.toThrow(/does not match the live Worker artifact/u)
  })

  it("includes artifact reads in freshness and convergence windows", async () => {
    const input = await validInput()
    for (const item of [
      { evidence: { completedAtMs: 1_101 }, now: () => 1_100 },
      { evidence: { completedAtMs: 1_001, startedAtMs: 900 }, now: () => 1_100 },
      { evidence: { completedAtMs: 1_001 }, now: () => 1_202 },
    ]) {
      await expect(
        (
          await verifier({ maxObservationAgeMs: 200, maxObservationWindowMs: 100, now: item.now })
        ).verify({
          artifactProofs: [
            artifactProof(controllerScript, versionA, "1".repeat(64), {
              evidence: item.evidence,
            }),
          ],
          attestations: input.attestations.slice(0, 1),
          deploymentProofs: [proof()],
          expectedScriptNames: [controllerScript],
        }),
      ).rejects.toThrow(/stale, future, or unconverged/u)
    }
  })

  it("rejects every malformed trusted proof-evidence field", async () => {
    const input = await validInput()
    const firstAttestations = input.attestations.slice(0, 1)
    const malformedEvidence: readonly Readonly<
      Partial<ActiveWorkerDeploymentProofState["evidence"]>
    >[] = [
      { status: 201 },
      { bodyState: "unreadable" },
      { responseChecksum: null as never },
      { responseChecksum: "bad" },
      { startedAtMs: 1.5 },
      { startedAtMs: -1 },
      { completedAtMs: 1.5 },
      { completedAtMs: 999 },
    ]
    for (const evidence of malformedEvidence) {
      await expect(
        (await verifier()).verify({
          artifactProofs: input.artifactProofs.slice(0, 1),
          attestations: firstAttestations,
          deploymentProofs: [proof(controllerScript, undefined, { evidence })],
          expectedScriptNames: [controllerScript],
        }),
      ).rejects.toThrow(/proof evidence is malformed/u)
    }
  })

  it("rejects future, stale, and over-wide observation windows", async () => {
    const input = await validInput()
    const cases = [
      {
        now: () => 1_000,
        proofs: [proof(controllerScript, undefined, { evidence: { completedAtMs: 1_001 } })],
      },
      { now: () => 1_202, proofs: [proof()] },
      {
        now: () => 1_100,
        proofs: [proof(controllerScript, undefined, { evidence: { completedAtMs: 1_101 } })],
      },
    ]
    for (const item of cases) {
      await expect(
        (
          await verifier({ maxObservationAgeMs: 200, maxObservationWindowMs: 100, now: item.now })
        ).verify({
          artifactProofs: input.artifactProofs.slice(0, 1),
          attestations: input.attestations.slice(0, 1),
          deploymentProofs: item.proofs,
          expectedScriptNames: [controllerScript],
        }),
      ).rejects.toThrow(/stale, future, or unconverged/u)
    }

    await expect(
      (await verifier({ maxObservationWindowMs: 50 })).verify({
        artifactProofs: input.artifactProofs.slice(0, 2),
        attestations: input.attestations.slice(0, 2),
        deploymentProofs: [
          proof(
            controllerScript,
            [
              { versionId: versionA, weightBps: 5_000 },
              { versionId: versionB, weightBps: 5_000 },
            ],
            { evidence: { completedAtMs: 1_101, startedAtMs: 1_000 } },
          ),
        ],
        expectedScriptNames: [controllerScript],
      }),
    ).rejects.toThrow(/stale, future, or unconverged/u)
  })

  it("rejects missing, duplicate, inactive, scoped, incompatible, and untrusted attestations", async () => {
    const input = await validInput()
    const first = requiredAttestation(input, 0)
    const second = requiredAttestation(input, 1)
    const third = requiredAttestation(input, 2)
    const verify = (await verifier()).verify
    await expect(verify({ ...input, attestations: input.attestations.slice(1) })).rejects.toThrow(
      /cover every active version/u,
    )
    await expect(
      verify({
        ...input,
        attestations: [first, first, third],
      }),
    ).rejects.toThrow(/duplicate active version/u)
    await expect(
      verify({
        ...input,
        attestations: [await signed(statement(controllerScript, versionC)), second, third],
      }),
    ).rejects.toThrow(/does not name an active version/u)

    const incompatible: readonly Readonly<Partial<ReaderVersionAttestationStatement>>[] = [
      { audience: "another-audience" },
      { issuedAtMs: 1_001 },
      { expiresAtMs: 1_100 },
      { expiresAtMs: 2_000 },
      { controlSchemaMin: 6, controlSchemaMax: 6 },
      { controlSchemaMax: 5, controlSchemaMin: 5 },
      { outcomePayloadReaderMin: 2, outcomePayloadReaderMax: 2 },
    ]
    for (const override of incompatible) {
      const changed = await signed(statement(controllerScript, versionA, override))
      await expect(
        verify({
          ...input,
          attestations: [changed, second, third],
        }),
      ).rejects.toThrow(/out of scope, stale, or incompatible/u)
    }

    const untrusted = await signed(statement(controllerScript, versionA, { keyId: "unknown-key" }))
    await expect(
      verify({
        ...input,
        attestations: [untrusted, second, third],
      }),
    ).rejects.toThrow(/untrusted key ID/u)
  })

  it("rejects malformed, noncanonical, invalid, and failed Ed25519 verification", async () => {
    const input = await validInput()
    const first = requiredAttestation(input, 0)
    const malformed = ["!", "A", "A".repeat(86)]
    for (const signature of malformed) {
      await expect(
        (await verifier()).verify({
          ...input,
          attestations: [{ ...first, signature }, ...input.attestations.slice(1)],
        }),
      ).rejects.toThrow()
    }
    const invalid = `${first.signature.startsWith("A") ? "B" : "A"}${first.signature.slice(1)}`
    await expect(
      (await verifier()).verify({
        ...input,
        attestations: [{ ...first, signature: invalid }, ...input.attestations.slice(1)],
      }),
    ).rejects.toThrow(/signature is invalid/u)

    const nativeCrypto = globalThis.crypto
    vi.stubGlobal("crypto", {
      subtle: {
        importKey: async () => ({}) as CryptoKey,
        verify: async () => {
          throw new Error("injected verify failure")
        },
      },
    })
    const failing = await verifier({ trustedKeys: [trustKey] })
    await expect(
      failing.verify({
        artifactProofs: [artifactProof()],
        attestations: [{ signature: `${"A".repeat(85)}Q`, statement: statement() }],
        deploymentProofs: [proof()],
        expectedScriptNames: [controllerScript],
      }),
    ).rejects.toThrow(/could not be verified/u)
    vi.stubGlobal("crypto", nativeCrypto)
  })
})

describe("reader deployment activation stability", () => {
  it("requires an ordered exact second observation and retains both immutable proofs", async () => {
    const before = await verifiedAt()
    const after = await verifiedAt(200)
    const capability = verifyReaderDeploymentStability(before, after, 400)

    expect(Object.keys(capability)).toEqual([])
    expect(Object.isFrozen(capability)).toBe(true)
    const evidence = verifiedReaderDeploymentStabilityEvidence(capability)
    expect(evidence).toMatchObject({
      after: { observedFromMs: 1_200, observedThroughMs: 1_203, verifiedAtMs: 1_300 },
      before: { observedFromMs: 1_000, observedThroughMs: 1_003, verifiedAtMs: 1_100 },
      firstVerifiedAtMs: 1_100,
      maxStabilityWindowMs: 400,
      observedFromMs: 1_000,
      observedThroughMs: 1_203,
      schemaVersion: 1,
      verifiedAtMs: 1_300,
    })
    expect(Object.isFrozen(evidence)).toBe(true)
    expect(Object.isFrozen(evidence.before)).toBe(true)
    expect(Object.isFrozen(evidence.after)).toBe(true)

    for (const fake of [{}, structuredClone(capability), null, "stable"]) {
      expect(() => verifiedReaderDeploymentStabilityEvidence(fake as never)).toThrow(
        /live verified/u,
      )
    }
  })

  it("rejects malformed policy and overlapping, reversed, stale, or fake observations", async () => {
    const before = await verifiedAt()
    const after = await verifiedAt(200)
    for (const maximum of [0, 300_001, 1.5]) {
      expect(() => verifyReaderDeploymentStability(before, after, maximum)).toThrow(/window/u)
    }
    expect(() => verifyReaderDeploymentStability(before, before, 400)).toThrow(/overlapping/u)
    expect(() => verifyReaderDeploymentStability(after, before, 400)).toThrow(/overlapping/u)
    expect(() => verifyReaderDeploymentStability(before, after, 299)).toThrow(/too old/u)
    for (const fake of [{}, structuredClone(before), null, "verified"]) {
      expect(() => verifyReaderDeploymentStability(fake as never, after, 400)).toThrow(
        /live verified/u,
      )
      expect(() => verifyReaderDeploymentStability(before, fake as never, 400)).toThrow(
        /live verified/u,
      )
    }
  })

  it("rejects changes to deployment, artifact, attestation, or expected-reader identity", async () => {
    const before = await verifiedAt()
    const base = await validInput(200)
    const controllerVersions = [
      { versionId: versionB, weightBps: 7_500 },
      { versionId: versionA, weightBps: 2_500 },
    ]
    const evidence = { completedAtMs: 1_201, startedAtMs: 1_200 }
    const changedDeployment = {
      ...base,
      deploymentProofs: [
        proof(controllerScript, controllerVersions, {
          deploymentId: "ffffffff-ffff-4fff-8fff-ffffffffffff",
          evidence,
        }),
        base.deploymentProofs[1] as (typeof base.deploymentProofs)[number],
      ],
    }
    const changedArtifact = {
      ...base,
      artifactProofs: [
        artifactProof(controllerScript, versionA, "5".repeat(64), { evidence }),
        ...base.artifactProofs.slice(1),
      ],
      attestations: [
        await signed(statement(controllerScript, versionA, { artifactChecksum: "5".repeat(64) })),
        ...base.attestations.slice(1),
      ],
    }
    const changedAttestation = {
      ...base,
      attestations: [
        await signed(statement(controllerScript, versionA, { issuedAtMs: 901 })),
        ...base.attestations.slice(1),
      ],
    }
    const changedInventory = {
      artifactProofs: base.artifactProofs.slice(0, 2),
      attestations: base.attestations.slice(0, 2),
      deploymentProofs: base.deploymentProofs.slice(0, 1),
      expectedScriptNames: [controllerScript],
    }
    const activeVerifier = await verifier({ now: () => 1_300 })
    for (const input of [
      changedDeployment,
      changedArtifact,
      changedAttestation,
      changedInventory,
    ]) {
      const after = await activeVerifier.verify(input)
      expect(() => verifyReaderDeploymentStability(before, after, 400)).toThrow(/changed/u)
    }
  })
})
