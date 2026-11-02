// Model-output: Claude Opus 5.5
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// Only the tests in src/. Not dist/*.test.js, which are tsc's compiled copies of them, and
		// not the copies inside other checkouts, e.g. git worktrees under .claude/.
		include: ["src/**/*.test.ts"],
	},
});
