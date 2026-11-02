// Model-output: Claude Opus 5.5
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// Only the tests in src/, not copies of them elsewhere, e.g. in git worktrees under .claude/.
		include: ["src/**/*.test.ts"],
	},
});
