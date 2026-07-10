import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    coverage: {
      exclude: ["**/*.test.ts", "**/dist/**"],
      include: [
        "packages/cloudflare/src/drizzle-shard-guards.ts",
        "packages/cloudflare/src/drizzle-movement-capture.ts",
        "packages/cloudflare/src/drizzle-movement-transfer.ts",
        "packages/cloudflare/src/movement-capture.ts",
        "packages/cloudflare/src/movement-data.ts",
        "packages/cloudflare/src/movement-transfer.ts",
        "packages/cloudflare/src/shard-guards.ts",
        "packages/control/src/lease-store.ts",
        "packages/control/src/migration-store.ts",
        "packages/core/src/hash.ts",
        "packages/core/src/migration.ts",
        "packages/core/src/movement.ts",
        "packages/core/src/operation.ts",
        "packages/core/src/ownership.ts",
        "packages/core/src/routes.ts",
        "packages/drizzle/src/plan.ts",
        "packages/router/src/leaf.ts",
        "packages/router/src/session.ts",
        "packages/router/src/transport.ts",
        "packages/router/src/wire.ts",
      ],
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
