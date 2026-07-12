import { readFileSync } from "node:fs"
import { describe, expect, it } from "vitest"
import * as control from "../src/index.js"

describe("@nozzle/control public API", () => {
  it("exposes saga protocol identities without the mutable projection store", () => {
    const exports = control as Readonly<Record<string, unknown>>

    expect(exports).not.toHaveProperty("D1SagaStore")
    expect(exports).not.toHaveProperty("D1SagaAttemptStore")
    expect(exports).not.toHaveProperty("D1SagaCoordinatorStore")
    expect(exports).not.toHaveProperty("createInternalSagaOperationStore")
    expect(exports).not.toHaveProperty("loadSagaAttemptRecordRow")
    expect(exports).not.toHaveProperty("SAGA_ATTEMPT_ROW_SELECT")
    for (const internal of [
      "acceptedSagaAttemptRecord",
      "D1ReaderBarrierStore",
      "D1SignedReaderBarrierStore",
      "D1SagaHistoryReader",
      "D1SagaTerminalStore",
      "finalizeSagaHistoryProof",
      "loadSagaTerminalCapability",
      "loadSagaHistoryAnchor",
      "loadSagaAttemptIdentityRow",
      "loadSagaAttemptOutcomeRow",
      "loadVerifiedSagaHistoryFinalState",
      "mintSagaTerminalCapability",
      "modelTerminalSagaBranches",
      "SagaHistoryAuditFolder",
      "SagaHistoryTransitionFolder",
      "SAGA_ATTEMPT_IDENTITY_ROW_SELECT",
      "SAGA_ATTEMPT_OUTCOME_ROW_SELECT",
      "SAGA_ATTEMPT_PAYLOAD_ROW_SELECT",
      "SAGA_HISTORY_PAGE_MAX_BYTES",
      "SAGA_HISTORY_PAGE_ROW_LIMIT",
      "SAGA_OUTCOME_ERROR_REFERENCE_JSON",
      "SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON",
      "SAGA_OUTCOME_OUTPUT_REFERENCE_JSON",
      "verifyReaderDeploymentBarrier",
      "readerBarrierCapabilityState",
    ]) {
      expect(exports).not.toHaveProperty(internal)
    }
    expect(exports).toMatchObject({
      SAGA_INIT_OPERATION_STEP_ID: "saga:init",
      SAGA_SETTLE_OPERATION_STEP_ID: "saga:settle",
      SAGA_TERMINATION_OPERATION_STEP_ID: "saga:termination",
      sagaActionOperationStepId: expect.any(Function),
    })
  })

  it("publishes only the audited package root rather than internal store subpaths", () => {
    const manifest = JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8"),
    ) as {
      readonly exports: Readonly<Record<string, unknown>>
    }

    expect(Object.keys(manifest.exports)).toEqual(["."])
  })
})
