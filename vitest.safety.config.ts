import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    coverage: {
      exclude: ["**/*.test.ts", "**/dist/**"],
      include: [
        "packages/core/src/hash.ts",
        "packages/core/src/migration.ts",
        "packages/core/src/operation.ts",
        "packages/core/src/ownership.ts",
        "packages/core/src/routes.ts",
      ],
      provider: "v8",
      reporter: ["text", "json-summary"],
      thresholds: {
        branches: 100,
        functions: 100,
        lines: 100,
        statements: 100,
      },
    },
    include: ["packages/core/test/**/*.test.ts"],
    passWithNoTests: false,
    restoreMocks: true,
  },
})
