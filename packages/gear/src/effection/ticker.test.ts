// Slop-provider: Claude Opus 4.8

import { describe, expect, test } from "vitest";
import { each, run, sleep, spawn } from "effection";
import type { Operation } from "effection";
import { createTicker, type Ticker } from "./ticker.ts";

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

/**
 * Drain a ticker in the background, appending the wall-clock time (ms since the
 * given `start`) at which each tick is observed. The returned array mutates live
 * as ticks arrive, so the driver can sleep and then assert on how many ticks
 * landed and roughly when.
 *
 * @param ticker - the ticker to consume.
 * @param start - `performance.now()` captured just before the ticker was made.
 * @returns an operation that spawns the consumer and yields the growing log of
 *          tick arrival times; it never returns on its own (the consumer is
 *          halted when its parent scope exits).
 */
function* record_ticks(ticker: Ticker, start: number): Operation<number[]> {
	const at: number[] = [];
	yield* spawn(function* () {
		for (const _ of yield* each(ticker)) {
			at.push(performance.now() - start);
			yield* each.next();
		}
	});
	return at;
}

// ----------------------------------------------------------------------------
// basic ticking
// ----------------------------------------------------------------------------

describe("createTicker — steady rate", () => {
	test("first tick fires after one interval, not immediately", async () => {
		await run(function* () {
			const start = performance.now();
			const at = yield* record_ticks(createTicker(50), start);
			yield* sleep(30); // less than one interval
			expect(at.length).toBe(0);
			yield* sleep(40); // ~70ms total: one interval has passed
			expect(at.length).toBe(1);
		});
	});

	test("emits one void tick per interval", async () => {
		const count = await run(function* () {
			const ticker = createTicker(20);
			let ticks = 0;
			const consumer = yield* spawn(function* () {
				for (const value of yield* each(ticker)) {
					expect(value).toBeUndefined(); // ticks carry no payload
					ticks += 1;
					if (ticks >= 3) {
						break;
					}
					yield* each.next();
				}
			});
			yield* consumer;
			return ticks;
		});
		expect(count).toBe(3);
	});

	test("a running ticker does not starve concurrent operations", async () => {
		await run(function* () {
			// The tightest consumer possible against the fastest interval allowed: if
			// next() ever returned without yielding to the reducer, the sleep below
			// would never resume and this test would hang.
			const ticker = createTicker(1);
			yield* spawn(function* () {
				for (const _ of yield* each(ticker)) {
					yield* each.next();
				}
			});
			const before = performance.now();
			yield* sleep(30);
			expect(performance.now() - before).toBeGreaterThanOrEqual(25);
		});
	});

	test("ticks are spaced by roughly the interval", async () => {
		await run(function* () {
			const start = performance.now();
			const at = yield* record_ticks(createTicker(40), start);
			yield* sleep(140); // room for ~3 ticks at 40ms
			expect(at.length).toBeGreaterThanOrEqual(2);
			for (let i = 1; i < at.length; i++) {
				// Allow generous slack for timer jitter under load.
				expect(at[i]! - at[i - 1]!).toBeGreaterThan(25);
			}
		});
	});
});

// ----------------------------------------------------------------------------
// changing the rate
// ----------------------------------------------------------------------------

describe("createTicker — set_interval", () => {
	test("speeding up an in-flight wait fires sooner than the old interval", async () => {
		await run(function* () {
			const start = performance.now();
			const ticker = createTicker(1000); // a long first wait...
			const at = yield* record_ticks(ticker, start);
			yield* sleep(30);
			expect(at.length).toBe(0); // nothing yet, still deep inside the 1000ms wait
			ticker.set_interval(60); // ...retimed to fire at last(0) + 60ms
			yield* sleep(80); // well past 60ms, nowhere near 1000ms
			expect(at.length).toBeGreaterThanOrEqual(1);
		});
	});

	test("dropping the interval below elapsed time fires immediately", async () => {
		await run(function* () {
			const start = performance.now();
			const ticker = createTicker(1000);
			const at = yield* record_ticks(ticker, start);
			yield* sleep(50); // 50ms into the wait
			ticker.set_interval(10); // due time (last + 10 = 10ms) is already in the past
			yield* sleep(20);
			expect(at.length).toBeGreaterThanOrEqual(1);
			expect(at[0]!).toBeLessThan(120); // fired promptly, not at ~1000ms
		});
	});

	test("slowing down stretches the spacing of later ticks", async () => {
		await run(function* () {
			const start = performance.now();
			const ticker = createTicker(30);
			const at = yield* record_ticks(ticker, start);
			yield* sleep(80); // a couple of fast ticks land
			const fast_count = at.length;
			expect(fast_count).toBeGreaterThanOrEqual(2);
			ticker.set_interval(500); // throttle way down
			yield* sleep(120); // less than one slow interval
			// At most one more tick (the one already in flight when we retimed).
			expect(at.length - fast_count).toBeLessThanOrEqual(1);
		});
	});

	test("interval_ms reflects the latest value", () => {
		const ticker = createTicker(100);
		expect(ticker.interval_ms).toBe(100);
		ticker.set_interval(250);
		expect(ticker.interval_ms).toBe(250);
	});
});

// ----------------------------------------------------------------------------
// validation
// ----------------------------------------------------------------------------

describe("createTicker — validation", () => {
	test("rejects a non-finite, zero, or negative initial interval", () => {
		expect(() => createTicker(NaN)).toThrow(/finite number > 0/);
		expect(() => createTicker(Infinity)).toThrow(/finite number > 0/);
		expect(() => createTicker(0)).toThrow(/finite number > 0/);
		expect(() => createTicker(-1)).toThrow(/finite number > 0/);
	});

	test("rejects a bad value passed to set_interval", () => {
		const ticker = createTicker(100);
		expect(() => ticker.set_interval(-5)).toThrow(/finite number > 0/);
		expect(() => ticker.set_interval(0)).toThrow(/finite number > 0/);
		expect(() => ticker.set_interval(NaN)).toThrow(/finite number > 0/);
		expect(ticker.interval_ms).toBe(100); // unchanged after a rejected update
	});
});
