import { describe, expect, it } from "vitest"
import * as control from "../src/index.js"

describe("@nozzle/control public API", () => {
  it("exposes saga protocol identities without the mutable projection store", () => {
    const exports = control as Readonly<Record<string, unknown>>

    expect(exports).not.toHaveProperty("D1SagaStore")
    expect(exports).not.toHaveProperty("loadSagaAttemptRecordRow")
    expect(exports).not.toHaveProperty("SAGA_ATTEMPT_ROW_SELECT")
    for (const internal of [
      "acceptedSagaAttemptRecord",
      "D1SagaHistoryReader",
      "loadSagaAttemptIdentityRow",
      "loadSagaAttemptOutcomeRow",
      "SAGA_ATTEMPT_IDENTITY_ROW_SELECT",
      "SAGA_ATTEMPT_OUTCOME_ROW_SELECT",
      "SAGA_ATTEMPT_PAYLOAD_ROW_SELECT",
      "SAGA_HISTORY_PAGE_MAX_BYTES",
      "SAGA_HISTORY_PAGE_ROW_LIMIT",
      "SAGA_OUTCOME_ERROR_REFERENCE_JSON",
      "SAGA_OUTCOME_EVIDENCE_REFERENCE_JSON",
      "SAGA_OUTCOME_OUTPUT_REFERENCE_JSON",
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
})
