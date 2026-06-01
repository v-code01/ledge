import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // One shared server child process; run the suite in a single worker so the
    // beforeAll/afterAll lifecycle owns exactly one server.
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 180_000,
    include: ["test/**/*.test.ts"],
  },
});
