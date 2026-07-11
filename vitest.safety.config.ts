import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    coverage: {
      exclude: ["**/*.test.ts", "**/dist/**"],
      include: ["packages/*/src/**/*.ts"],
      provider: "v8",
      reporter: ["text", "json", "json-summary"],
      thresholds: {
        branches: 100,
        functions: 100,
        lines: 100,
        statements: 100,
      },
    },
    include: [
      "packages/cloudflare/test/**/*.test.ts",
      "packages/control/test/**/*.test.ts",
      "packages/core/test/**/*.test.ts",
      "packages/drizzle/test/**/*.test.ts",
      "packages/router/test/**/*.test.ts",
    ],
    passWithNoTests: false,
    restoreMocks: true,
  },
})
