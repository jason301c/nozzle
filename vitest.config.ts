import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    coverage: {
      exclude: ["**/*.test.ts", "**/dist/**"],
      include: ["packages/*/src/**/*.ts"],
      provider: "v8",
      reporter: ["text", "json", "json-summary", "lcov"],
      thresholds: {
        100: true,
      },
    },
    include: ["packages/*/test/**/*.test.ts"],
    passWithNoTests: false,
    restoreMocks: true,
  },
})
