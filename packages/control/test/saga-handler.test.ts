import type { SagaActionReference } from "@nozzle/core"
import { afterEach, describe, expect, it, vi } from "vitest"
import {
  invokeSagaEffectHandler,
  invokeSagaObservationHandler,
  type SagaEffectInvocationRequest,
  type SagaObservationInvocationRequest,
} from "../src/saga-handler.js"

const action: SagaActionReference = {
  actionId: "handler.action",
  artifactChecksum: "a".repeat(64),
  version: 1,
}

function effectRequest(
  overrides: Partial<SagaEffectInvocationRequest> = {},
): SagaEffectInvocationRequest {
  return {
    action,
    attemptId: "attempt-1",
    idempotencyKey: "action-key",
    inputJson: "{}",
    operationId: "operation-1",
    phase: "forward",
    proof: {
      acquisitionId: "acquisition-1",
      fencingToken: 1,
      holderId: "controller-1",
      leaseKey: "saga:one",
    },
    sagaId: "saga-1",
    stepId: "write",
    timeoutMs: 1_000,
    ...overrides,
  }
}

function observationRequest(
  overrides: Partial<SagaObservationInvocationRequest> = {},
): SagaObservationInvocationRequest {
  return {
    ...effectRequest(),
    effectAttemptId: "effect-attempt-1",
    effectErrorJson: '{"kind":"unknown"}',
    effectIdempotencyKey: "action-key",
    ...overrides,
  }
}

afterEach(() => vi.useRealTimers())

describe("saga handler invocation boundary", () => {
  it("canonicalizes every supported effect and observation result", async () => {
    for (const state of [
      "definitely_not_applied_retryable",
      "definitely_not_applied_terminal",
      "unknown",
    ] as const) {
      const result = await invokeSagaEffectHandler(
        () => ({
          errorJson: ' { "z": 2, "a": [3, { "y": true, "x": null }] } ',
          evidenceJson: '{"source":"adapter"}',
          state,
        }),
        effectRequest(),
      )
      expect(result).toEqual({
        errorJson: '{"a":[3,{"x":null,"y":true}],"z":2}',
        evidenceJson: '{"source":"adapter"}',
        state,
      })
      expect(Object.isFrozen(result)).toBe(true)
    }
    await expect(
      invokeSagaEffectHandler(async ({ signal }) => {
        expect(signal.aborted).toBe(false)
        return {
          evidenceJson: '{"provider":"d1"}',
          outputJson: ' { "written": true } ',
          state: "confirmed",
        }
      }, effectRequest()),
    ).resolves.toEqual({
      evidenceJson: '{"provider":"d1"}',
      outputJson: '{"written":true}',
      state: "confirmed",
    })
    for (const state of ["indeterminate", "not_applied"] as const) {
      await expect(
        invokeSagaObservationHandler(
          () => ({ errorJson: '{"reason":"absent"}', evidenceJson: "{}", state }),
          observationRequest(),
        ),
      ).resolves.toEqual({ errorJson: '{"reason":"absent"}', evidenceJson: "{}", state })
    }
    await expect(
      invokeSagaObservationHandler(
        () => ({ evidenceJson: "{}", outputJson: '{"value":1}', state: "applied" }),
        observationRequest(),
      ),
    ).resolves.toEqual({ evidenceJson: "{}", outputJson: '{"value":1}', state: "applied" })
    const nullPrototype = Object.assign(Object.create(null) as Record<string, unknown>, {
      evidenceJson: "{}",
      outputJson: "{}",
      state: "confirmed",
    })
    await expect(
      invokeSagaEffectHandler(() => nullPrototype as never, effectRequest()),
    ).resolves.toEqual({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" })
  })

  it("classifies thrown and timed-out effects as unknown without exposing exceptions", async () => {
    const thrown = await invokeSagaEffectHandler(() => {
      throw new Error("private provider detail")
    }, effectRequest())
    expect(thrown).toEqual({
      errorJson: '{"kind":"unknown_handler_outcome","reason":"exception"}',
      evidenceJson: '{"kind":"local_handler_invocation","reason":"exception"}',
      state: "unknown",
    })
    await expect(
      invokeSagaEffectHandler(async () => Promise.reject(new Error("secret")), effectRequest()),
    ).resolves.toEqual(thrown)

    vi.useFakeTimers()
    let signal: AbortSignal | undefined
    const pending = invokeSagaEffectHandler(
      ({ signal: received }) => {
        signal = received
        return new Promise(() => undefined)
      },
      effectRequest({ timeoutMs: 10 }),
    )
    await vi.advanceTimersByTimeAsync(10)
    await expect(pending).resolves.toEqual({
      errorJson: '{"kind":"unknown_handler_outcome","reason":"timeout"}',
      evidenceJson: '{"kind":"local_handler_invocation","reason":"timeout"}',
      state: "unknown",
    })
    expect(signal?.aborted).toBe(true)
  })

  it("classifies observation exceptions and timeouts as indeterminate", async () => {
    const thrown = await invokeSagaObservationHandler(() => {
      throw new Error("private observation detail")
    }, observationRequest())
    expect(thrown).toEqual({
      errorJson: '{"kind":"indeterminate_handler_observation","reason":"exception"}',
      evidenceJson: '{"kind":"local_handler_invocation","reason":"exception"}',
      state: "indeterminate",
    })
    vi.useFakeTimers()
    const pending = invokeSagaObservationHandler(
      () => new Promise(() => undefined),
      observationRequest({ timeoutMs: 5 }),
    )
    await vi.advanceTimersByTimeAsync(5)
    await expect(pending).resolves.toEqual({
      errorJson: '{"kind":"indeterminate_handler_observation","reason":"timeout"}',
      evidenceJson: '{"kind":"local_handler_invocation","reason":"timeout"}',
      state: "indeterminate",
    })
  })

  it("rejects malformed handler contracts and non-durable JSON", async () => {
    for (const value of [
      null,
      {},
      { state: "future" },
      { evidenceJson: "{}", outputJson: "{}", state: "confirmed", extra: true },
      { errorJson: "{}", state: "unknown" },
      { errorJson: "{}", evidenceJson: "{}", state: "unknown", extra: true },
    ]) {
      await expect(
        invokeSagaEffectHandler(() => value as never, effectRequest()),
      ).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
    }
    for (const value of [
      null,
      {},
      { state: "future" },
      { evidenceJson: "{}", outputJson: "{}", state: "applied", extra: true },
      { errorJson: "{}", state: "not_applied" },
      { errorJson: "{}", evidenceJson: "{}", state: "indeterminate", extra: true },
    ]) {
      await expect(
        invokeSagaObservationHandler(() => value as never, observationRequest()),
      ).rejects.toMatchObject({ code: "OperationInterventionRequiredError" })
    }
    const deeplyNested = `${"[".repeat(20_000)}0${"]".repeat(20_000)}`
    for (const result of [
      { evidenceJson: "not-json", outputJson: "{}", state: "confirmed" as const },
      { evidenceJson: "{}", outputJson: "", state: "confirmed" as const },
      { evidenceJson: "{}", outputJson: deeplyNested, state: "confirmed" as const },
      {
        errorJson: JSON.stringify("x".repeat(1024 * 1024)),
        evidenceJson: "{}",
        state: "unknown" as const,
      },
    ]) {
      await expect(invokeSagaEffectHandler(() => result, effectRequest())).rejects.toMatchObject({
        code: "OperationInterventionRequiredError",
      })
    }
  })

  it("rejects missing handlers and unsupported timeout values", async () => {
    await expect(invokeSagaEffectHandler(undefined as never, effectRequest())).rejects.toThrow(
      /effect handler is required/u,
    )
    await expect(
      invokeSagaObservationHandler(undefined as never, observationRequest()),
    ).rejects.toThrow(/observation handler is required/u)
    for (const timeoutMs of [0, 1.5, Number.MAX_SAFE_INTEGER, "1" as never]) {
      await expect(
        invokeSagaEffectHandler(
          () => ({ evidenceJson: "{}", outputJson: "{}", state: "confirmed" }),
          effectRequest({ timeoutMs }),
        ),
      ).rejects.toMatchObject({ code: "ConfigurationError" })
    }
  })
})
