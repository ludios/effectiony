// Slop-provider: Claude Opus 4.8
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
 * each(ticker))` — and call {@link Ticker.set_interval} from anywhere, including
 * outside any operation, to make the ticks faster or slower.
 *
 * Ticks carry no value (`void`): the stream exists purely for its timing. Each
 * subscription is independent and schedules its next tick relative to when it
 * last fired, but every subscription reads the one shared interval, so a single
 * `set_interval` call retimes them all.
 */
export interface Ticker extends Stream<void, never> {
	/**
	 * Change the spacing between ticks, in milliseconds. Takes effect at once: a
	 * subscription that is currently waiting reschedules its pending tick to
	 * `<time it last ticked> + ms`, which fires immediately if that moment has
	 * already passed. Must be a finite number `> 0`.
	 */
	set_interval(ms: number): void;
	/** The current spacing between ticks, in milliseconds. */
	readonly interval_ms: number;
}

/** A one-shot callback that wakes a subscription blocked on an interval change. */
type Wake = () => void;

/**
 * Reject intervals that would make the schedule meaningless or starve the
 * scheduler (NaN, Infinity, zero, or negative). Zero is rejected because a
 * zero-length wait would let `next()` return without ever yielding to the
 * reducer, so a tight consumer loop could monopolize it; a bad value should fail
 * loudly at the call site rather than quietly spin or never fire.
 *
 * @param ms - the candidate interval in milliseconds.
 */
function assert_valid_interval(ms: number): void {
	if (typeof ms !== "number" || !Number.isFinite(ms) || ms <= 0) {
		throw new Error(`ticker interval must be a finite number > 0, got ${ms}`);
	}
}

/**
 * Resolve and drop every waiter currently blocked on an interval change. The
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
 * Block until the ticker's interval changes. Compares against the revision that
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
function wait_for_interval_change(
	waiters: Set<Wake>,
	get_revision: () => number,
	observed_revision: number,
): Operation<void> {
	return action<void>((resolve) => {
		// A change may have landed between the caller reading the revision and this
		// executor running (e.g. another task called set_interval); if so, the
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
 * Create a {@link Ticker}: a rate-controllable stream of ticks.
 *
 * The first tick of a subscription fires one interval after it subscribes, and
 * subsequent ticks are spaced by the current interval — measured from when the
 * previous tick was *emitted*, not from an ideal grid. So a consumer that is
 * slow to pull never triggers a catch-up burst; the effective rate simply can't
 * exceed how fast the consumer pulls.
 *
 * @param interval_ms - the initial spacing between ticks; finite and `> 0`.
 * @returns a ticker you can subscribe to and retime with `set_interval`.
 */
export function createTicker(interval_ms: number): Ticker {
	assert_valid_interval(interval_ms);
	let current = interval_ms;
	// Bumped on every change. A waiter records the revision it saw before it slept
	// and wakes when the revision moves. This coalesces redundant changes — a
	// burst of set_interval calls between two pulls just leaves one higher number
	// to observe — and, together with the pre-check in wait_for_interval_change,
	// closes the gap where a change lands after the deadline was computed but
	// before the waiter registered.
	let revision = 0;
	const waiters = new Set<Wake>();

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
							// Overdue (interval was just cut below the elapsed time, or the
							// consumer pulls slower than the interval): fire now. Since the
							// interval is always > 0, this returns synchronously at most once
							// between waits, so it can't starve the reducer.
							last = now;
							return { done: false, value: undefined };
						}
						const observed = revision;
						// Wake when the wait elapses OR when the interval changes, then loop
						// to recompute against the (possibly new) `current`. race halts the
						// loser, so at most one timer is ever pending.
						yield* race([
							sleep(remaining),
							wait_for_interval_change(waiters, () => revision, observed),
						]);
					}
				},
			};
		},
	};
}
