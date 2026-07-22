// Model-output: Claude Opus 4.8

import { describe, expect, expectTypeOf, test } from "vitest";
import { createQueue, createSignal, each, resource, run, sleep, spawn } from "effection";
import type { Operation, Stream, Subscription } from "effection";
import { merge, type Tagged } from "./merge.ts";

// ----------------------------------------------------------------------------
// test sources
// ----------------------------------------------------------------------------

/**
 * A finite, fully-buffered stream: subscribing yields a fresh queue preloaded
 * with `values` and already closed. Because everything is buffered, delivery is
 * independent of scheduling, so multiset/order assertions never race. This also
 * exercises the greedy-drain path (a forwarder empties it without suspending).
 *
 * @param values - the values the stream emits, in order, before closing.
 * @returns a stream over `values` that closes with `void`.
 */
function from_values<T>(...values: T[]): Stream<T, void> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			let q = createQueue<T, void>();
			for (let v of values) {
				q.add(v);
			}
			q.close(undefined);
			return q;
		},
	};
}

/**
 * A live signal-backed stream plus a teardown probe. The stream is a resource,
 * so when the consuming forwarder is halted (normal close, early break, or a
 * sibling error) its `finally` runs and flips `torn`. Values pushed via `send`
 * before a subscriber exists are dropped, so callers must `sleep` first.
 *
 * @returns an object with the `stream`, a `send`/`close` pair driven from
 *          outside any operation, and a `torn()` probe reporting teardown.
 */
function make_tracked<T>() {
	let sig  = createSignal<T, void>();
	let torn = false;
	let stream = resource<Subscription<T, void>>(function* (provide) {
		let sub = yield* sig;
		try {
			yield* provide(sub);
		} finally {
			torn = true;
		}
	});
	return {
		stream,
		send:  (value: T) => sig.send(value),
		close: () => sig.close(),
		torn:  () => torn,
	};
}

/**
 * A stream whose subscription emits `values` then throws `err` on the next
 * `next()` call, optionally after `delay` ms so that sibling forwarders have a
 * chance to subscribe first.
 *
 * @param values - the values emitted before the throw.
 * @param err - the error thrown once `values` is exhausted.
 * @param delay - ms to suspend before throwing (default 0).
 * @returns a stream that fails mid-iteration.
 */
function throwing_stream<T>(values: T[], err: Error, delay = 0): Stream<T, void> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			let i = 0;
			let sub: Subscription<T, void> = {
				*next() {
					if (i < values.length) {
						return { done: false, value: values[i++]! };
					}
					if (delay > 0) {
						yield* sleep(delay);
					}
					throw err;
				},
			};
			return sub;
		},
	};
}

/**
 * A stream that throws when subscribed (before producing any value).
 *
 * @param err - the error thrown on subscribe.
 * @returns a stream that fails at subscription time.
 */
function boom_on_subscribe(err: Error): Stream<never, void> {
	return {
		// oxlint-disable-next-line eslint/require-yield
		*[Symbol.iterator]() {
			throw err;
		},
	};
}

// ----------------------------------------------------------------------------
// consumers
// ----------------------------------------------------------------------------

/**
 * Consume a stream with the `each` loop the public API is meant for.
 *
 * @param s - the stream to drain.
 * @returns every value emitted, in arrival order.
 */
function* drain_each<T>(s: Stream<T, void>): Operation<T[]> {
	let out: T[] = [];
	for (let v of yield* each(s)) {
		out.push(v);
		yield* each.next();
	}
	return out;
}

/**
 * Consume up to `n` values via `each`, then break — without calling
 * `each.next()` on the breaking iteration, per the `each` contract.
 *
 * @param s - the stream to drain.
 * @param n - the number of values to take before breaking.
 * @returns the first `n` (or fewer) values.
 */
function* drain_take<T>(s: Stream<T, void>, n: number): Operation<T[]> {
	let out: T[] = [];
	for (let v of yield* each(s)) {
		out.push(v);
		if (out.length >= n) {
			break;
		}
		yield* each.next();
	}
	return out;
}

/**
 * Consume a stream by hand so the terminal iterator result (the `void` close
 * value) is observable, which `each` hides.
 *
 * @param s - the stream to drain.
 * @returns the values and the final `done` result.
 */
function* drain_manual<T>(
	s: Stream<T, void>,
): Operation<{ values: T[]; final: IteratorResult<T, void> }> {
	let sub    = yield* s;
	let values: T[] = [];
	let next   = yield* sub.next();
	while (!next.done) {
		values.push(next.value);
		next = yield* sub.next();
	}
	return { values, final: next };
}

/**
 * Consume via `each`, sleeping `ms` between reads to model a slow consumer and
 * prove the unbounded buffer never drops a value.
 *
 * @param s - the stream to drain.
 * @param ms - delay inserted before each `each.next()`.
 * @returns every value emitted.
 */
function* drain_slow<T>(s: Stream<T, void>, ms: number): Operation<T[]> {
	let out: T[] = [];
	for (let v of yield* each(s)) {
		out.push(v);
		yield* sleep(ms);
		yield* each.next();
	}
	return out;
}

/** Stable sort key for order-independent multiset comparison. */
function canon(messages: { key: string; value: unknown }[]): string[] {
	// oxlint-disable-next-line unicorn/no-array-sort
	return messages.map((m) => `${m.key}=${JSON.stringify(m.value)}`).sort();
}

// ----------------------------------------------------------------------------
// tagging & delivery
// ----------------------------------------------------------------------------

describe("tagging & delivery", () => {
	test("delivers every value from every source, correctly tagged", async () => {
		let got = await run(() => drain_each(merge({
			a: from_values(1, 2, 3),
			b: from_values("x", "y"),
		})));
		expect(canon(got)).toEqual(canon([
			{ key: "a", value: 1 }, { key: "a", value: 2 }, { key: "a", value: 3 },
			{ key: "b", value: "x" }, { key: "b", value: "y" },
		]));
	});

	test("preserves per-source FIFO order", async () => {
		let got = await run(() => drain_each(merge({
			a: from_values(1, 2, 3, 4),
			b: from_values(10, 20, 30),
		})));
		expect(got.filter((m) => m.key === "a").map((m) => m.value)).toEqual([1, 2, 3, 4]);
		expect(got.filter((m) => m.key === "b").map((m) => m.value)).toEqual([10, 20, 30]);
	});

	test("single source passes through, tagged", async () => {
		let got = await run(() => drain_each(merge({ only: from_values("p", "q") })));
		expect(got).toEqual([{ key: "only", value: "p" }, { key: "only", value: "q" }]);
	});

	test("preserves value identity (no clobbering across sources)", async () => {
		let o1 = { id: 1 };
		let o2 = { id: 2 };
		let got = await run(() => drain_each(merge({
			a: from_values(o1),
			b: from_values(o2),
		})));
		expect(got.find((m) => m.key === "a")!.value).toBe(o1);
		expect(got.find((m) => m.key === "b")!.value).toBe(o2);
	});
});

// ----------------------------------------------------------------------------
// close semantics
// ----------------------------------------------------------------------------

describe("close semantics", () => {
	test("empty record yields an immediately-closed, empty stream", async () => {
		let { values, final } = await run(() => drain_manual(merge({})));
		expect(values).toEqual([]);
		expect(final.done).toBe(true);
		expect(final.value).toBe(undefined);
	});

	test("closes exactly once with a void value after all sources close", async () => {
		let { values, final } = await run(() => drain_manual(merge({
			a: from_values(1),
			b: from_values(2),
		})));
		expect(values).toHaveLength(2);
		expect(final.done).toBe(true);
		expect(final.value).toBe(undefined);
	});

	test("a source emitting nothing still counts toward close", async () => {
		let { values, final } = await run(() => drain_manual(merge({
			empty: from_values<number>(),
			full:  from_values(1, 2),
		})));
		expect(values).toEqual([{ key: "full", value: 1 }, { key: "full", value: 2 }]);
		expect(final.done).toBe(true);
	});

	test("does _not_ close early: values sent after one source closes still arrive", async () => {
		let a = make_tracked<string>();
		let b = make_tracked<string>();
		let { values, final } = await run(function* () {
			let task = yield* spawn(() => drain_manual(merge({ a: a.stream, b: b.stream })));
			yield* sleep(10);
			a.send("a1");
			yield* sleep(10);
			a.close();              // A done; B still open
			yield* sleep(10);
			b.send("b1");           // sent strictly after A closed
			yield* sleep(10);
			b.close();
			return yield* task;
		});
		expect(final.done).toBe(true);
		expect(final.value).toBe(undefined);
		expect(canon(values)).toEqual(canon([
			{ key: "a", value: "a1" },
			{ key: "b", value: "b1" },   // would be lost if the merge had closed at A
		]));
	});
});

// ----------------------------------------------------------------------------
// no data loss / integrity under load
// ----------------------------------------------------------------------------

describe("no data loss / integrity", () => {
	test("loses nothing across many sources and many values", async () => {
		let n_sources = 5;
		let n_values  = 200;
		let sources: Record<string, Stream<number, void>> = {};
		let expected: Record<string, number[]> = {};
		for (let s = 0; s < n_sources; s++) {
			let key  = `s${s}`;
			let vals = Array.from({ length: n_values }, (_, i) => s * 1000 + i);
			sources[key]  = from_values(...vals);
			expected[key] = vals;
		}
		let got = await run(() => drain_each(merge(sources)));
		expect(got).toHaveLength(n_sources * n_values);
		let seen = new Set(got.map((m) => `${m.key}:${m.value}`));
		expect(seen.size).toBe(got.length);                       // no duplicates
		for (let s = 0; s < n_sources; s++) {
			let key = `s${s}`;
			expect(got.filter((m) => m.key === key).map((m) => m.value)).toEqual(expected[key]);
		}
	});

	test("a slow consumer loses nothing from buffered sources", async () => {
		let got = await run(() => drain_slow(merge({
			a: from_values(1, 2, 3),
			b: from_values(4, 5, 6),
		}), 2));
		expect(got).toHaveLength(6);
		expect(got.filter((m) => m.key === "a").map((m) => m.value)).toEqual([1, 2, 3]);
		expect(got.filter((m) => m.key === "b").map((m) => m.value)).toEqual([4, 5, 6]);
	});

	test("interleaves live sources in readiness order", async () => {
		let a = make_tracked<number>();
		let b = make_tracked<number>();
		let got = await run(function* () {
			let task = yield* spawn(() => drain_each(merge({ a: a.stream, b: b.stream })));
			yield* sleep(10);
			a.send(1);
			yield* sleep(10);
			b.send(2);
			yield* sleep(10);
			a.send(3);
			yield* sleep(10);
			a.close();
			b.close();
			return yield* task;
		});
		expect(got).toEqual([
			{ key: "a", value: 1 },
			{ key: "b", value: 2 },
			{ key: "a", value: 3 },
		]);
	});
});

// ----------------------------------------------------------------------------
// errors: fail-fast, not swallowed
// ----------------------------------------------------------------------------

describe("errors (fail-fast)", () => {
	test("a throw at subscribe time rejects the run", async () => {
		await expect(run(() => drain_each(merge({
			ok:  from_values(1, 2),
			bad: boom_on_subscribe(new Error("boom-subscribe")),
		})))).rejects.toThrow("boom-subscribe");
	});

	test("a throw mid-stream rejects the run", async () => {
		await expect(run(() => drain_each(merge({
			bad: throwing_stream([10, 20], new Error("boom-mid")),
		})))).rejects.toThrow("boom-mid");
	});

	test("a source error tears down sibling sources", async () => {
		let live = make_tracked<number>();
		await expect(run(function* () {
			let task = yield* spawn(() => drain_each(merge({
				live: live.stream,                                  // subscribes, then suspends
				bad:  throwing_stream<number>([], new Error("boom-sibling"), 10),
			})));
			yield* task;
		})).rejects.toThrow("boom-sibling");
		expect(live.torn()).toBe(true);
	});
});

// ----------------------------------------------------------------------------
// structured-concurrency cleanup
// ----------------------------------------------------------------------------

describe("cleanup", () => {
	test("breaking the consumer early halts and tears down sources", async () => {
		let a = make_tracked<number>();
		let got = await run(function* () {
			let task = yield* spawn(() => drain_take(merge({ a: a.stream }), 2));
			yield* sleep(10);
			a.send(1);
			yield* sleep(5);
			a.send(2);
			yield* sleep(5);
			a.send(3);            // consumer already broke; must be dropped, not delivered
			return yield* task;
		});
		expect(got).toEqual([{ key: "a", value: 1 }, { key: "a", value: 2 }]);
		expect(a.torn()).toBe(true);
	});

	test("normal completion tears down sources", async () => {
		let a = make_tracked<number>();
		await run(function* () {
			let task = yield* spawn(() => drain_each(merge({ a: a.stream })));
			yield* sleep(10);
			a.send(1);
			yield* sleep(5);
			a.close();
			yield* task;
		});
		expect(a.torn()).toBe(true);
	});
});

// ----------------------------------------------------------------------------
// type-level guarantees (checked by `tsc` / `vitest --typecheck`)
// ----------------------------------------------------------------------------

describe("types", () => {
	test("Tagged is a per-key discriminated union that narrows value", () => {
		type R = { n: Stream<number, void>; s: Stream<string, void> };
		expectTypeOf<Tagged<R>>().toEqualTypeOf<
			{ key: "n"; value: number } | { key: "s"; value: string }
		>();
	});

	test("merge yields a Stream of the tagged union", () => {
		let m = merge({ n: from_values(1), s: from_values("x") });
		expectTypeOf(m).toEqualTypeOf<
			Stream<{ key: "n"; value: number } | { key: "s"; value: string }, void>
		>();
	});
});

// @ts-expect-error numeric keys are rejected at the type level (would desync tags)
const _reject_numeric = () => merge({ 0: from_values(1) });
// @ts-expect-error symbol keys are rejected at the type level
const _reject_symbol = () => merge({ [Symbol.iterator]: from_values(1) });
