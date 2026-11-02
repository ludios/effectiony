// Model-output: Claude Opus 5.5
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		// dist/ contains tsc's output, which includes compiled copies of the tests in src/;
		// running those copies too would run every test twice.
		exclude: [...configDefaults.exclude, "dist/**"],
	},
});
