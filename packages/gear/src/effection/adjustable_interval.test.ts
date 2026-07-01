// Slop-provider: Claude Opus 4.8

import { describe, expect, test } from "vitest";
import { each, run, sleep, spawn } from "effection";
import type { Operation } from "effection";
import { adjustable_interval, type AdjustableInterval } from "./adjustable_interval.ts";

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

/**
 * Drain a interval in the background, appending the wall-clock time (ms since the
 * given `start`) at which each tick is observed. The returned array mutates live
 * as ticks arrive, so the driver can sleep and then assert on how many ticks
 * landed and roughly when.
 *
 * @param interval - the interval to consume.
 * @param start - `performance.now()` captured just before the interval was made.
 * @returns an operation that spawns the consumer and yields the growing log of
 *          tick arrival times; it never returns on its own (the consumer is
 *          halted when its parent scope exits).
 */
function* record_ticks(interval: AdjustableInterval, start: number): Operation<number[]> {
	const at: number[] = [];
	yield* spawn(function* () {
		for (const _ of yield* each(interval)) {
			at.push(performance.now() - start);
			yield* each.next();
		}
	});
	return at;
}

// ----------------------------------------------------------------------------
// basic ticking
// ----------------------------------------------------------------------------

describe("adjustable_interval — steady rate", () => {
	test("first tick fires after one interval, not immediately", async () => {
		await run(function* () {
			const start = performance.now();
			const at = yield* record_ticks(adjustable_interval(50), start);
			yield* sleep(30); // less than one interval
			expect(at.length).toBe(0);
			yield* sleep(40); // ~70ms total: one interval has passed
			expect(at.length).toBe(1);
		});
	});

	test("emits one void tick per interval", async () => {
		const count = await run(function* () {
			const a_i = adjustable_interval(20);
			let ticks = 0;
			const consumer = yield* spawn(function* () {
				for (const value of yield* each(a_i)) {
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

	test("a running interval does not starve concurrent operations", async () => {
		await run(function* () {
			// The tightest consumer possible against the fastest interval allowed: if
			// next() ever returned without yielding to the reducer, the sleep below
			// would never resume and this test would hang.
			const a_i = adjustable_interval(1);
			yield* spawn(function* () {
				for (const _ of yield* each(a_i)) {
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
			const at = yield* record_ticks(adjustable_interval(40), start);
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

describe("adjustable_interval — set interval", () => {
	test("speeding up an in-flight wait fires sooner than the old interval", async () => {
		await run(function* () {
			const start = performance.now();
			const a_i = adjustable_interval(1000); // a long first wait...
			const at = yield* record_ticks(a_i, start);
			yield* sleep(30);
			expect(at.length).toBe(0); // nothing yet, still deep inside the 1000ms wait
			a_i.delay = 60; // ...retimed to fire at last(0) + 60ms
			yield* sleep(80); // well past 60ms, nowhere near 1000ms
			expect(at.length).toBeGreaterThanOrEqual(1);
		});
	});

	test("dropping the interval below elapsed time fires immediately", async () => {
		await run(function* () {
			const start = performance.now();
			const a_i = adjustable_interval(1000);
			const at = yield* record_ticks(a_i, start);
			yield* sleep(50); // 50ms into the wait
			a_i.delay = 10; // due time (last + 10 = 10ms) is already in the past
			yield* sleep(20);
			expect(at.length).toBeGreaterThanOrEqual(1);
			expect(at[0]!).toBeLessThan(120); // fired promptly, not at ~1000ms
		});
	});

	test("slowing down stretches the spacing of later ticks", async () => {
		await run(function* () {
			const start = performance.now();
			const a_i = adjustable_interval(30);
			const at = yield* record_ticks(a_i, start);
			yield* sleep(80); // a couple of fast ticks land
			const fast_count = at.length;
			expect(fast_count).toBeGreaterThanOrEqual(2);
			a_i.delay = 500; // throttle way down
			yield* sleep(120); // less than one slow interval
			// At most one more tick (the one already in flight when we retimed).
			expect(at.length - fast_count).toBeLessThanOrEqual(1);
		});
	});

	test("interval reflects the latest value", () => {
		const a_i = adjustable_interval(100);
		expect(a_i.delay).toBe(100);
		a_i.delay = 250;
		expect(a_i.delay).toBe(250);
	});
});

// ----------------------------------------------------------------------------
// validation
// ----------------------------------------------------------------------------

describe("adjustable_interval — validation", () => {
	test("rejects a non-finite, zero, or negative initial interval", () => {
		expect(() => adjustable_interval(NaN)).toThrow(/number of milliseconds/);
		expect(() => adjustable_interval(Infinity)).toThrow(/number of milliseconds/);
		expect(() => adjustable_interval(0)).toThrow(/number of milliseconds/);
		expect(() => adjustable_interval(-1)).toThrow(/number of milliseconds/);
	});

	test("rejects sub-1ms intervals that would starve the reducer", () => {
		// The dangerous case: `last + current` rounds back to `last`, so `next()`
		// would return synchronously forever. Below 1ms the timer can't honor the
		// rate anyway, so the whole sub-1ms range is rejected.
		expect(() => adjustable_interval(Number.MIN_VALUE)).toThrow(/number of milliseconds/);
		expect(() => adjustable_interval(0.5)).toThrow(/number of milliseconds/);
	});

	test("accepts a fractional interval >= 1ms (e.g. from division)", () => {
		expect(adjustable_interval(125 / 2).delay).toBe(62.5); // truncated by the timer, not rejected
		expect(adjustable_interval(1.5).delay).toBe(1.5);
	});

	test("rejects an interval larger than the max timer delay", () => {
		expect(() => adjustable_interval(2_147_483_648)).toThrow(/number of milliseconds/);
		expect(adjustable_interval(2_147_483_647).delay).toBe(2_147_483_647); // the boundary is allowed
	});

	test("rejects a bad value passed to interval", () => {
		const a_i = adjustable_interval(100);
		expect(() => { a_i.delay = -5; }).toThrow(/number of milliseconds/);
		expect(() => { a_i.delay = 0; }).toThrow(/number of milliseconds/);
		expect(() => { a_i.delay = 0.5; }).toThrow(/number of milliseconds/);
		expect(() => { a_i.delay = NaN; }).toThrow(/number of milliseconds/);
		expect(a_i.delay).toBe(100); // unchanged after a rejected update
		a_i.delay = 62.5; // fractional >= 1 is accepted
		expect(a_i.delay).toBe(62.5);
	});
});
