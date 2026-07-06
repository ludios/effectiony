// Model-output: Claude Opus 4.8

import { describe, expect, test } from "vitest";
import { createQueue, each, run, sleep, spawn } from "effection";
import type { Operation, Stream } from "effection";
import { timebox } from "@effectionx/timebox";
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
 * Like {@link make_source}, but its subscription outlives the consumer (the
 * queue is owned here, not torn down when the `each` loop ends) and it counts
 * how many items were actually *retrieved*. Lets a test observe whether `batch`
 * consumes a source item after the consumer has broken out of the loop.
 *
 * @returns the `stream`, a `push`, and a `consumed()` count of retrieved items.
 */
function counting_source<T>(): {
	stream: Stream<T, never>;
	push: (value: T) => void;
	consumed: () => number;
} {
	const queue = createQueue<T, never>();
	let consumed = 0;
	const stream: Stream<T, never> = {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			return {
				*next() {
					const result = yield* queue.next();
					if (!result.done) {
						consumed += 1;
					}
					return result;
				},
			};
		},
	};
	return { stream, push: (value) => queue.add(value), consumed: () => consumed };
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

/**
 * A source that emits one value immediately, then violates the contract by
 * ending only after `delay` ms — long enough for the ending pull to be carried
 * past a timed-out window, so the `done` arrives on a *carried* pull.
 *
 * @param value - the single value emitted before the source ends.
 * @param delay - how long the second pull waits before reporting `done`.
 * @returns a stream that yields `value` once, then ends after `delay` ms.
 */
function ends_later_source(value: number, delay: number): Stream<number, never> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			let pulls = 0;
			return {
				*next() {
					pulls += 1;
					if (pulls === 1) {
						return { done: false, value };
					}
					yield* sleep(delay);
					return { done: true, value: undefined as never };
				},
			};
		},
	};
}

/**
 * A queue-backed source that can be made to throw from a pull, emulating a
 * source whose `next()` fails (e.g. a lost NATS connection). Errors are
 * enqueued like values, so a `fail` after N pushes throws on pull N+1.
 *
 * @returns the `stream`, a `push` to feed values, and a `fail` to enqueue an error.
 */
function failable_source<T>(): {
	stream: Stream<T, never>;
	push: (value: T) => void;
	fail: (error: Error) => void;
} {
	const queue = createQueue<{ value: T } | { error: Error }, never>();
	const stream: Stream<T, never> = {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			return {
				*next() {
					const item = yield* queue.next();
					if (item.done) {
						throw new Error("unreachable: the queue is never closed");
					}
					if ("error" in item.value) {
						throw item.value.error;
					}
					return { done: false, value: item.value.value };
				},
			};
		},
	};
	return {
		stream,
		push: (value) => queue.add({ value }),
		fail: (error) => queue.add({ error }),
	};
}

/**
 * Like {@link make_source}, but counts pulls that were unwound by a halt while
 * still pending — the observable stand-in for "halting a pending pull closes a
 * NATS consumer".
 *
 * @returns the `stream`, a `push`, and `halted_pulls()`, the count of pulls
 * unwound before they produced a result.
 */
function halt_counting_source<T>(): {
	stream: Stream<T, never>;
	push: (value: T) => void;
	halted_pulls: () => number;
} {
	const queue = createQueue<T, never>();
	let halted = 0;
	const stream: Stream<T, never> = {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			return {
				*next() {
					let resolved = false;
					try {
						const result = yield* queue.next();
						resolved = true;
						return result;
					} finally {
						if (!resolved) {
							halted += 1;
						}
					}
				},
			};
		},
	};
	return { stream, push: (value) => queue.add(value), halted_pulls: () => halted };
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

	test("measures each window from its first item's arrival, even when a slow consumer lags", async () => {
		// Regression guard: a batch that times out keeps its pending pull in
		// `carried`. If that pull resolves while a slow consumer is busy, the next
		// window must start from the item's *arrival*, not from when the consumer
		// finally asks — otherwise items that arrived > maxTime apart get merged.
		const src = make_source<string>();
		const maxTime = 50;
		const batches = await run(function* () {
			const consumer = yield* spawn(function* () {
				const out: string[][] = [];
				for (const b of yield* each(batch({ maxTime })(src.stream))) {
					out.push([...b]);
					if (out.length >= 3) {
						break;
					}
					if (out.length === 1) {
						// Stay busy after [A] so B (which resolves the carried pull) and
						// C both land before we ask for the next batch.
						yield* sleep(200);
					}
					yield* each.next();
				}
				return out;
			});
			src.push("A"); // t≈0; [A] emitted once its window closes at ~50
			yield* sleep(80);
			src.push("B"); // t≈80; resolves the pull carried from [A]
			yield* sleep(90);
			src.push("C"); // t≈170; > maxTime after B, so it must be its own batch
			yield* sleep(250);
			return yield* consumer;
		});
		expect(batches).toEqual([["A"], ["B"], ["C"]]);
	});

	test("does not consume a source item after the consumer breaks out of the loop", async () => {
		// The pull carried past a timed-out batch must be tied to the subscription's
		// lifetime, not the caller task. Otherwise, breaking after a batch whose
		// carried pull was created in a later each.next() leaves that pull alive to
		// consume (and drop) one more source item while the caller keeps running.
		const src = counting_source<string>();
		const maxTime = 40;
		await run(function* () {
			const consumer = yield* spawn(function* () {
				let batches = 0;
				for (const _b of yield* each(batch({ maxTime })(src.stream))) {
					batches += 1;
					if (batches >= 2) {
						break; // break after the SECOND batch (its carried pull is in this task)
					}
					yield* each.next();
				}
				yield* sleep(200); // keep the caller task alive after breaking
			});
			src.push("A"); // batch 1: [A]
			yield* sleep(60);
			src.push("B"); // batch 2: [B], leaves a pull carried for the next item
			yield* sleep(60); // [B] emitted -> consumer breaks
			const before = src.consumed(); // A and B
			src.push("C"); // a leaked carried pull would consume this
			yield* sleep(80);
			expect(src.consumed()).toBe(before); // C must remain unconsumed
			yield* consumer;
		});
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

	test("with both options, a partial batch is emitted when maxTime fires first", async () => {
		const src = make_source<string>();
		const batches = await run(function* () {
			const consumer = yield* spawn(() =>
				take_batches(batch({ maxTime: 60, maxSize: 10 })(src.stream), 1),
			);
			src.push("A");
			yield* sleep(20);
			src.push("B"); // joins A's window; only 2 of 10 items when the window closes
			return yield* consumer;
		});
		expect(batches).toEqual([["A", "B"]]);
	});

	test("maxSize of 1 emits singleton batches immediately, without waiting on the window", async () => {
		// If the size check did not fire before the window logic, each batch
		// would stall for the enormous maxTime and time out the test.
		const src = make_source<number>();
		const batches = await run(function* () {
			const consumer = yield* spawn(() =>
				take_batches(batch({ maxSize: 1, maxTime: 60_000 })(src.stream), 3),
			);
			for (const n of [1, 2, 3]) {
				src.push(n);
			}
			return yield* consumer;
		});
		expect(batches).toEqual([[1], [2], [3]]);
	});

	test("a carried item leads the next batch and counts toward its maxSize", async () => {
		// After [A] times out, a pull is carried. Its item must open batch 2 AND
		// count toward maxSize: with one more item the batch is full and must be
		// emitted immediately, well before batch 2's 40ms window would close.
		const src = make_source<string>();
		const emitted: string[][] = [];
		await run(function* () {
			yield* spawn(function* () {
				for (const b of yield* each(batch({ maxTime: 40, maxSize: 2 })(src.stream))) {
					emitted.push([...b]);
					yield* each.next();
				}
			});
			src.push("A");
			yield* sleep(60); // [A] emitted at ~40ms; a pull is carried for batch 2
			expect(emitted).toEqual([["A"]]);
			src.push("B"); // resolves the carried pull: batch 2 is [B, ...]
			src.push("C"); // fills batch 2 to maxSize
			yield* sleep(15); // well inside B's 40ms window
			expect(emitted).toEqual([["A"], ["B", "C"]]); // emitted by size, not by the window
		});
	});

	test("yields a fresh array per batch and never mutates an already-yielded batch", async () => {
		// take_batches copies each batch, which would mask reuse of the internal
		// items array — so collect the yielded arrays themselves.
		const src = make_source<number>();
		const collected: Array<readonly number[]> = [];
		await run(function* () {
			const consumer = yield* spawn(function* () {
				for (const b of yield* each(batch({ maxSize: 2 })(src.stream))) {
					collected.push(b);
					if (collected.length >= 2) {
						break;
					}
					yield* each.next();
				}
			});
			for (const n of [1, 2, 3, 4]) {
				src.push(n);
			}
			yield* consumer;
		});
		expect(collected[0]).not.toBe(collected[1]);
		expect(collected[0]).toEqual([1, 2]);
		expect(collected[1]).toEqual([3, 4]);
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

	test("throws when the contract violation arrives on a carried pull", async () => {
		// The source ends 100ms into a pull that was carried past a timed-out 40ms
		// window; the `done` must be surfaced by the next batch's deadline-free pull.
		await expect(
			run(() => take_batches(batch({ maxTime: 40 })(ends_later_source(1, 100)), 2)),
		).rejects.toThrow(/ended unexpectedly/);
	});
});

// ----------------------------------------------------------------------------
// source errors: a failing pull must crash the consumer, never vanish
// ----------------------------------------------------------------------------

describe("source error propagation", () => {
	test("propagates an error thrown during the first, deadline-free pull", async () => {
		const src = failable_source<string>();
		const result = run(() => take_batches(batch({ maxTime: 50 })(src.stream), 1));
		src.fail(new Error("source exploded"));
		await expect(result).rejects.toThrow("source exploded");
	});

	test("propagates an error that fails a carried pull while the consumer is busy", async () => {
		// After [A] times out, a pull is carried with nobody awaiting it. When the
		// source then fails, the error must crash the consumer via the
		// subscription's scope — not vanish because no yield point observes it.
		// The sleeps below are far longer than the test timeout: only the
		// propagating error can end this run early.
		const src = failable_source<string>();
		await expect(
			run(function* () {
				yield* spawn(function* () {
					for (const _b of yield* each(batch({ maxTime: 40 })(src.stream))) {
						yield* sleep(60_000); // busy: the carried pull is unobserved
						yield* each.next();
					}
				});
				src.push("A");
				yield* sleep(60); // [A] emitted at ~40ms; a fresh pull is carried
				src.fail(new Error("source exploded"));
				yield* sleep(60_000);
			}),
		).rejects.toThrow("source exploded");
	});
});

// ----------------------------------------------------------------------------
// halt safety: halting one next() call must not disturb the source
// ----------------------------------------------------------------------------

describe("halt safety of next()", () => {
	test("halting next() mid-window carries the in-flight pull; its item leads the next batch", async () => {
		// Regression guard: before pulls were recorded in `carried` up front, a
		// halt here orphaned the in-flight pull, which silently ate the next
		// source item. The halted call's partial batch ([A]) is dropped by design.
		const src = make_source<string>();
		await run(function* () {
			const subscription = yield* batch({ maxTime: 50 })(src.stream);
			src.push("A");
			// Halt the first next() ~20ms into its 50ms extend window.
			const outcome = yield* timebox(20, () => subscription.next());
			expect(outcome.timeout).toBe(true);
			src.push("B"); // must resolve the carried pull, not feed an orphan
			yield* sleep(10);
			src.push("C");
			const result = yield* subscription.next();
			expect(result.done).toBe(false);
			expect([...result.value]).toEqual(["B", "C"]);
		});
	});

	test("halting next() during the first, deadline-free pull leaves the source pull pending", async () => {
		// Unwinding a pending pull is what closes a NATS consumer. A halt local
		// to one next() call must not do that: the pull survives in the
		// subscription's scope and its item opens the next batch.
		const src = halt_counting_source<string>();
		await run(function* () {
			const subscription = yield* batch({ maxTime: 50 })(src.stream);
			const outcome = yield* timebox(20, () => subscription.next());
			expect(outcome.timeout).toBe(true); // halted while the first pull was pending
			expect(src.halted_pulls()).toBe(0); // ...but the source pull was not unwound
			src.push("A"); // resolved by the surviving pull, carried into the next batch
			const result = yield* subscription.next();
			expect(result.done).toBe(false);
			expect([...result.value]).toEqual(["A"]);
		});
	});
});

// ----------------------------------------------------------------------------
// options validation
// ----------------------------------------------------------------------------

describe("options validation", () => {
	test("throws when neither maxTime nor maxSize is given", () => {
		expect(() => batch({})).toThrow(/maxTime or maxSize/);
	});

	test("rejects a negative maxTime", () => {
		expect(() => batch({ maxTime: -1 })).toThrow(/maxTime/);
	});

	test("rejects a non-finite maxTime", () => {
		expect(() => batch({ maxTime: Number.POSITIVE_INFINITY })).toThrow(/maxTime/);
		expect(() => batch({ maxTime: Number.NaN })).toThrow(/maxTime/);
	});

	test("rejects a maxSize below 1", () => {
		expect(() => batch({ maxSize: 0 })).toThrow(/maxSize/);
	});

	test("rejects a non-integer maxSize", () => {
		expect(() => batch({ maxSize: 1.5 })).toThrow(/maxSize/);
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
