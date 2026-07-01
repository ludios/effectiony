// Slop-provider: Claude Opus 4.8

import { describe, expect, test } from "vitest";
import { createQueue, each, run, sleep, spawn } from "effection";
import type { Operation, Stream } from "effection";
import { batch } from "./batch.ts";

// ----------------------------------------------------------------------------
// test sources
// ----------------------------------------------------------------------------

/**
 * A live, unbounded, never-closing source backed by a queue. `push` enqueues a
 * value from outside any operation; because the queue buffers, values pushed
 * before a subscriber pulls are not dropped, which keeps timing assertions
 * about *when* a batch is emitted independent of subscription races.
 *
 * @returns the `stream` (typed `Stream<T, never>`) and a `push` to feed it.
 */
function make_source<T>(): { stream: Stream<T, never>; push: (value: T) => void } {
	const queue = createQueue<T, never>();
	const stream: Stream<T, never> = {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			return queue;
		},
	};
	return { stream, push: (value) => queue.add(value) };
}

/**
 * A source that violates the `Stream<T, never>` contract by ending on its very
 * first `next()`. Used to prove `batch` surfaces the contract breach.
 *
 * @returns a stream whose first pull reports `done`.
 */
function ended_source(): Stream<number, never> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			return {
				// oxlint-disable-next-line eslint/require-yield
				*next() {
					return { done: true, value: undefined as never };
				},
			};
		},
	};
}

/**
 * A source that emits exactly one value and then violates the contract by
 * ending, so the breach happens *after* a batch has started collecting.
 *
 * @param value - the single value emitted before the source ends.
 * @returns a stream that yields `value` once, then reports `done`.
 */
function one_then_ended_source(value: number): Stream<number, never> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			let sent = false;
			return {
				// oxlint-disable-next-line eslint/require-yield
				*next() {
					if (!sent) {
						sent = true;
						return { done: false, value };
					}
					return { done: true, value: undefined as never };
				},
			};
		},
	};
}

// ----------------------------------------------------------------------------
// consumers
// ----------------------------------------------------------------------------

/**
 * Collect exactly `n` batches via the `each` loop, then break — the loop the
 * public API is meant for. Each batch is copied so later teardown can't mutate
 * an already-yielded array.
 *
 * @param stream - the batched stream to drain.
 * @param n - the number of batches to collect before breaking.
 * @returns the first `n` batches as plain arrays.
 */
function* take_batches<T>(stream: Stream<Readonly<T[]>, never>, n: number): Operation<T[][]> {
	const out: T[][] = [];
	for (const b of yield* each(stream)) {
		out.push([...b]);
		if (out.length >= n) {
			break;
		}
		yield* each.next();
	}
	return out;
}

// ----------------------------------------------------------------------------
// timing: batches wait for more items until maxTime
// ----------------------------------------------------------------------------

describe("maxTime batching", () => {
	test("does not emit a batch until maxTime elapses", async () => {
		const src = make_source<string>();
		const maxTime = 100;
		let emitted = 0;
		await run(function* () {
			const consumer = yield* spawn(function* () {
				for (const _b of yield* each(batch({ maxTime })(src.stream))) {
					emitted += 1;
					yield* each.next();
				}
			});
			src.push("A");
			yield* sleep(25);
			expect(emitted).toBe(0); // 25ms into a 100ms window: nothing emitted yet
			yield* sleep(125); // ~150ms total: the window has closed
			expect(emitted).toBe(1);
			yield* consumer.halt();
		});
	});

	test("waits for and coalesces items arriving within the window, splitting later ones", async () => {
		const src = make_source<string>();
		const maxTime = 80;
		const batches = await run(function* () {
			const consumer = yield* spawn(() => take_batches(batch({ maxTime })(src.stream), 2));
			src.push("A");
			yield* sleep(30); // still inside A's window
			src.push("B"); // must join A's batch, proving we waited past A
			yield* sleep(90); // window closes: batch [A, B] emitted
			src.push("C"); // opens a fresh window
			yield* sleep(120); // window closes: batch [C] emitted
			return yield* consumer;
		});
		expect(batches).toEqual([["A", "B"], ["C"]]);
	});

	test("maxTime of 0 emits each item as its own batch", async () => {
		const src = make_source<number>();
		const batches = await run(function* () {
			const consumer = yield* spawn(() => take_batches(batch({ maxTime: 0 })(src.stream), 3));
			for (const n of [1, 2, 3]) {
				src.push(n);
			}
			return yield* consumer;
		});
		expect(batches).toEqual([[1], [2], [3]]);
	});
});

// ----------------------------------------------------------------------------
// size batching
// ----------------------------------------------------------------------------

describe("maxSize batching", () => {
	test("with only maxSize, batches strictly by count", async () => {
		const src = make_source<number>();
		const batches = await run(function* () {
			const consumer = yield* spawn(() => take_batches(batch({ maxSize: 2 })(src.stream), 2));
			for (const n of [1, 2, 3, 4]) {
				src.push(n);
			}
			return yield* consumer;
		});
		expect(batches).toEqual([[1, 2], [3, 4]]);
	});

	test("emits as soon as maxSize is reached, before maxTime could fire", async () => {
		const src = make_source<number>();
		const batches = await run(function* () {
			// maxTime is enormous, so only the size bound can trigger.
			const consumer = yield* spawn(() =>
				take_batches(batch({ maxSize: 3, maxTime: 10_000 })(src.stream), 2),
			);
			for (const n of [1, 2, 3, 4, 5, 6]) {
				src.push(n);
			}
			return yield* consumer;
		});
		expect(batches).toEqual([[1, 2, 3], [4, 5, 6]]);
	});
});

// ----------------------------------------------------------------------------
// contract: an infinite source must not end
// ----------------------------------------------------------------------------

describe("Stream<T, never> contract", () => {
	test("throws if the source ends immediately", async () => {
		await expect(
			run(() => take_batches(batch({ maxTime: 50 })(ended_source()), 1)),
		).rejects.toThrow(/ended unexpectedly/);
	});

	test("throws if the source ends after emitting part of a batch", async () => {
		await expect(
			run(() => take_batches(batch({ maxTime: 50 })(one_then_ended_source(1)), 1)),
		).rejects.toThrow(/ended unexpectedly/);
	});
});

// ----------------------------------------------------------------------------
// options validation
// ----------------------------------------------------------------------------

describe("options", () => {
	test("throws when neither maxTime nor maxSize is given", () => {
		expect(() => batch({})).toThrow(/maxTime or maxSize/);
	});
});

// ----------------------------------------------------------------------------
// type-level guarantees
// ----------------------------------------------------------------------------

describe("types", () => {
	test("produces a stream of readonly batches typed by the source element", () => {
		const src = make_source<number>();
		// The annotation is the assertion: tsc rejects it unless batch maps a
		// Stream<number, never> to exactly a Stream<readonly number[], never> — a
		// void-closed result would not be assignable to the `never` close type.
		const batched: Stream<readonly number[], never> = batch({ maxTime: 10 })(src.stream);
		expect(batched).toBeDefined();
	});
});

// @ts-expect-error a terminating source (TClose = void) is rejected: batch requires Stream<T, never>
const _reject_terminating = () => batch({ maxTime: 10 })(null as unknown as Stream<number, void>);
