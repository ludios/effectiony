// Slop-provider: Claude Opus 4.8
//
// effection ships an `interval(ms)` stream, but its rate is fixed for the life
// of the stream. This is the same idea with a knob: a stream of evenly-spaced
// ticks whose spacing you can change on the fly, so you can speed it up or slow
// it down while it is running (e.g. poll fast while something is happening,
// back off to a trickle when it goes quiet).

import { createSignal, race, sleep } from "effection";
import type { Operation, Stream } from "effection";

/**
 * A {@link Stream} of evenly-spaced ticks whose spacing can be changed while it
 * is running. Consume it like any other stream — `for (const n of yield*
 * each(ticker))` — and call {@link Ticker.set_interval} from anywhere, including
 * outside any operation, to make the ticks faster or slower.
 *
 * Each subscription is independent: it emits `0, 1, 2, …` (the count of ticks it
 * has produced so far) and schedules its next tick relative to when it last
 * fired. But every subscription reads the one shared interval, so a single
 * `set_interval` call retimes them all.
 */
export interface Ticker extends Stream<number, never> {
	/**
	 * Change the spacing between ticks, in milliseconds. Takes effect at once: a
	 * subscription that is currently waiting reschedules its pending tick to
	 * `<time it last ticked> + ms`, which fires immediately if that moment has
	 * already passed. Must be a finite number `>= 0`; `0` means "tick as fast as
	 * the consumer pulls".
	 */
	set_interval(ms: number): void;
	/** The current spacing between ticks, in milliseconds. */
	readonly interval_ms: number;
}

/**
 * Reject intervals that would make the schedule meaningless (NaN, Infinity, or
 * negative), so a bad value fails loudly at the call site instead of quietly
 * producing a ticker that never fires or busy-loops.
 *
 * @param ms - the candidate interval in milliseconds.
 */
function assert_valid_interval(ms: number): void {
	if (typeof ms !== "number" || !Number.isFinite(ms) || ms < 0) {
		throw new Error(`ticker interval must be a finite number >= 0, got ${ms}`);
	}
}

/**
 * Create a {@link Ticker}: a rate-controllable stream of ticks.
 *
 * The first tick of a subscription fires one interval after it subscribes, and
 * subsequent ticks are spaced by the current interval — measured from when the
 * previous tick was *emitted*, not from an ideal grid. So a consumer that is
 * slow to pull never triggers a catch-up burst; the effective rate simply can't
 * exceed how fast the consumer pulls.
 *
 * @param interval_ms - the initial spacing between ticks; finite and `>= 0`.
 * @returns a ticker you can subscribe to and retime with `set_interval`.
 */
export function createTicker(interval_ms: number): Ticker {
	assert_valid_interval(interval_ms);
	let current = interval_ms;
	// Fires whenever the interval changes, purely to interrupt a subscription
	// that is mid-wait so it can reschedule against the new interval. A signal
	// (not a queue) is deliberate: with no subscribers, retiming is a harmless
	// no-op rather than a buffered value that would spuriously wake the first
	// subscriber. The value carried is meaningless; every waiter just re-reads
	// `current`.
	const changed = createSignal<void, never>();

	return {
		get interval_ms() {
			return current;
		},

		set_interval(ms: number): void {
			assert_valid_interval(ms);
			if (ms === current) {
				return;
			}
			current = ms;
			changed.send();
		},

		*[Symbol.iterator]() {
			// Subscribe once for the whole lifetime of this stream subscription, so
			// no interval change is missed between pulls. Mirrors how batch.ts holds
			// its source subscription across `next()` calls.
			const changes = yield* changed;
			let last = performance.now();
			let count = 0;
			return {
				*next(): Operation<IteratorResult<number, never>> {
					while (true) {
						const now = performance.now();
						const remaining = last + current - now;
						if (remaining <= 0) {
							last = now;
							return { done: false, value: count++ };
						}
						// Wake when the wait elapses OR when the interval changes, then loop
						// to recompute against the (possibly new) `current`. race halts the
						// loser, so at most one timer is ever pending.
						yield* race([sleep(remaining), changes.next()]);
					}
				},
			};
		},
	};
}
