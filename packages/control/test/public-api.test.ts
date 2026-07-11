import { describe, expect, it } from "vitest"
import * as control from "../src/index.js"

describe("@nozzle/control public API", () => {
  it("exposes saga protocol identities without the mutable projection store", () => {
    const exports = control as Readonly<Record<string, unknown>>

    expect(exports).not.toHaveProperty("D1SagaStore")
    expect(exports).toMatchObject({
      SAGA_INIT_OPERATION_STEP_ID: "saga:init",
      SAGA_SETTLE_OPERATION_STEP_ID: "saga:settle",
      SAGA_TERMINATION_OPERATION_STEP_ID: "saga:termination",
      sagaActionOperationStepId: expect.any(Function),
    })
  })
})
