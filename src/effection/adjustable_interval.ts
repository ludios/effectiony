// Model-output: Claude Opus 4.8
//
// effection ships an `interval(ms)` stream, but its rate is fixed for the life
// of the stream. This is the same idea with a knob: a stream of evenly-spaced
// ticks whose spacing you can change on the fly, so you can speed it up or slow
// it down while it is running (e.g. poll fast while something is happening,
// back off to a trickle when it goes quiet).

import { action, race, sleep } from "effection";
import type { Operation, Stream } from "effection";

/**
 * A {@link Stream} of evenly-spaced ticks whose spacing can be changed while it
 * is running. Consume it like any other stream — `for (const _ of yield*
 * each(ticker))` — and call `.delay = ...` from anywhere, including
 * outside any operation, to make the ticks faster or slower.
 *
 * Ticks carry no value (`void`): the stream exists purely for its timing. Each
 * subscription is independent and schedules its next tick relative to when it
 * last fired, but every subscription reads the one shared delay, so a single
 * `.delay = ...` call retimes them all.
 */
export interface AdjustableInterval extends Stream<void, never> {
	/**
	 * Get or set the spacing between ticks, in milliseconds. Takes effect at once:
	 * a subscription that is currently waiting reschedules its pending tick to
	 * `<time it last ticked> + ms`, which fires immediately if that moment has
	 * already passed. Must be a number of milliseconds from 1 to 2147483647.
	 */
	delay: number;
}

/** A one-shot callback that wakes a subscription blocked on a delay change. */
type Wake = () => void;

// Bounds of a sleep the Node timer backend honors faithfully. Below 1ms it
// clamps up to 1ms; above 2**31 - 1 (~24.8 days) it overflows and clamps down to
// 1ms (with a TimeoutOverflowWarning). We reject the whole out-of-range domain so
// a caller never gets a silently wrong rate.
const MIN_DELAY_MS = 1;
const MAX_DELAY_MS = 2_147_483_647;

/**
 * Reject delays the timer backend can't honor faithfully, so a bad value
 * fails loudly at the call site instead of quietly ticking at the wrong rate.
 * The floor is 1ms: below it the Node timer clamps up to 1ms anyway, and a value
 * tiny enough (e.g. `Number.MIN_VALUE`) rounds away entirely when added to
 * `performance.now()`, so `remaining` never goes positive and `next()` returns
 * synchronously forever, starving the reducer.
 *
 * @param ms - the candidate delay in milliseconds.
 */
function assert_valid_delay(ms: number): void {
	if (
		typeof ms !== "number" ||
		!Number.isFinite(ms) ||
		ms < MIN_DELAY_MS ||
		ms > MAX_DELAY_MS
	) {
		throw new Error(
			`ticker delay must be a number of milliseconds ` +
			`from ${MIN_DELAY_MS} to ${MAX_DELAY_MS}, got ${ms}`,
		);
	}
}

/**
 * Resolve and drop every waiter currently blocked on a delay change. The
 * set is snapshotted and cleared *before* the callbacks run, so if resolving one
 * synchronously registers a fresh waiter, that new waiter is kept for the next
 * change rather than being resolved by this one.
 *
 * @param waiters - the set of one-shot callbacks awaiting the next change.
 */
function notify_waiters(waiters: Set<Wake>): void {
	const pending = [...waiters];
	waiters.clear();
	for (const wake of pending) {
		wake();
	}
}

/**
 * Block until the ticker's delay changes. Compares against the revision that
 * was current when the caller computed its deadline, so a change that lands
 * between that read and this registration is not missed — it resolves at once.
 * The waiter is a one-shot entry in `waiters` that is always removed again,
 * whether it fires or is discarded when its `race` against `sleep` is lost.
 *
 * @param waiters - the shared set this waiter registers itself in.
 * @param get_revision - reads the ticker's current revision number.
 * @param observed_revision - the revision the caller saw before it started waiting.
 * @returns an operation that resolves once the revision has moved on.
 */
function wait_for_delay_change(
	waiters: Set<Wake>,
	get_revision: () => number,
	observed_revision: number,
): Operation<void> {
	return action<void>((resolve) => {
		// A change may have landed between the caller reading the revision and this
		// executor running (e.g. another task called `.delay = ...`); if so, the
		// notification already went out to a not-yet-registered waiter, so resolve
		// now instead of blocking forever on a change that already happened.
		if (get_revision() !== observed_revision) {
			resolve();
			return () => {};
		}
		const wake: Wake = () => {
			waiters.delete(wake);
			resolve();
		};
		waiters.add(wake);
		// Always runs (fired or discarded): never leak a waiter into the set.
		return () => {
			waiters.delete(wake);
		};
	}, "ticker interval change");
}

/**
 * Create a {@link AdjustableInterval}: a rate-controllable stream of ticks.
 *
 * The first tick of a subscription fires one interval after it subscribes, and
 * subsequent ticks are spaced by the current interval — measured from when the
 * previous tick was *emitted*, not from an ideal grid. So a consumer that is
 * slow to pull never triggers a catch-up burst; the effective rate simply can't
 * exceed how fast the consumer pulls.
 *
 * @param delay - the initial spacing between ticks; a number of
 *                milliseconds from 1 to 2147483647.
 * @returns a ticker you can subscribe to and retime with `.delay = ...`.
 */
export function adjustable_interval(delay: number): AdjustableInterval {
	assert_valid_delay(delay);
	let current = delay;
	// Bumped on every change. A waiter records the revision it saw before it slept
	// and wakes when the revision moves. This coalesces redundant changes — a
	// burst of `.delay = ...` calls between two pulls just leaves one higher number
	// to observe — and, together with the pre-check in wait_for_delay_change,
	// closes the gap where a change lands after the deadline was computed but
	// before the waiter registered.
	let revision = 0;
	const waiters = new Set<Wake>();

	return {
		get delay() {
			return current;
		},

		set delay(ms: number) {
			assert_valid_delay(ms);
			if (ms === current) {
				return;
			}
			current = ms;
			revision++;
			notify_waiters(waiters);
		},

		// oxlint-disable-next-line require-yield
		*[Symbol.iterator]() {
			let last = performance.now();
			return {
				*next(): Operation<IteratorResult<void, never>> {
					while (true) {
						const now = performance.now();
						const remaining = last + current - now;
						if (remaining <= 0) {
							// Overdue: the delay was just cut below the elapsed time, or the
							// consumer pulls slower than the delay. Fire now. Because the
							// delay is at least 1ms and firing resets `last`, this synchronous
							// path runs at most once per pull; a consumer that pulls promptly
							// always reaches the `sleep` below. Caveat: that sleep is this
							// loop's only yield to the event loop, so a consumer whose work
							// between pulls is purely synchronous CPU exceeding the delay
							// takes this path on every pull and starves the event loop. Any
							// real (async) work per tick avoids that.
							last = now;
							return { done: false, value: undefined };
						}
						const observed = revision;
						// Wake when the wait elapses OR when the delay changes, then loop
						// to recompute against the (possibly new) `current`. race halts the
						// loser, so at most one timer is ever pending.
						yield* race([
							sleep(remaining),
							wait_for_delay_change(waiters, () => revision, observed),
						]);
					}
				},
			};
		},
	};
}
