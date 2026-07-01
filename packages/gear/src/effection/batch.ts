// Slop-provider: Claude Opus 4.8
//
// A replacement for @effectionx/stream-helpers' `batch`, which is unsuitable for
// a source that is idle most of the time: it applies the maxTime deadline to the
// FIRST item of a batch, so once a source goes quiet for one window it halts its
// pending pull and returns `{ done: true }` — silently ending a stream typed
// `Stream<T, never>` (and, for a NATS consumer, closing the consumer, because
// halting a pending consumer pull triggers its close path).

import { useScope } from "effection";
import type { Operation, Stream, Task } from "effection";
import { timebox } from "@effectionx/timebox";

export interface BatchOptions {
	/** Emit the current batch once this many ms elapse since its first item arrived. Must be finite and >= 0. */
	readonly maxTime?: number;
	/** Emit the current batch once it holds this many items. Must be an integer >= 1. */
	readonly maxSize?: number;
}

/** A pulled result paired with the moment it actually arrived from the source. */
type Pulled<T> = {
	readonly result: IteratorResult<T, never>;
	readonly at: number;
};

/**
 * Extract a pulled value, treating a `done` result as a contract violation: the
 * source is typed `Stream<T, never>` and must never end.
 *
 * @param pulled - a result pulled from the source.
 * @returns the pulled value.
 */
function unwrap<T>(pulled: Pulled<T>): T {
	if (pulled.result.done) {
		throw new Error("batch: source stream typed Stream<T, never> ended unexpectedly");
	}
	return pulled.result.value;
}

/**
 * Group items from an infinite source stream into batches, emitting a batch when
 * either `maxTime` elapses (measured from the arrival of the batch's first item)
 * or the batch reaches `maxSize`. At least one of the two must be provided.
 *
 * "Arrival" means the moment `subscription.next()` resolves an item, not the
 * moment a producer enqueued it. For an already-buffering source, an item that
 * was waiting in the upstream buffer is timed from when this batcher pulls it.
 *
 * The first item of every batch is awaited with **no** deadline, so an idle
 * source never times a batch out into a spurious, empty result — `maxTime` only
 * bounds how long we keep waiting to *extend* a batch that has already started.
 *
 * When a window times out with the next pull still in flight, that pull is kept
 * (rather than halted, which would close a NATS consumer) and consumed by the
 * following batch, carrying its true arrival time so the next window is measured
 * from when the item arrived, not from when a slow consumer got around to asking.
 * A consequence is that batch reads one item ahead of the consumer after a
 * timeout; if the subscription is abandoned before that item is consumed it is
 * dropped, which is fine for a lossy-on-shutdown source like the NATS consumer.
 *
 * The source is typed `Stream<T, never>`, i.e. it is expected to never end; if it
 * ever yields `done` that violates the contract and this throws rather than
 * quietly ending the batched stream.
 *
 * @typeParam T - the element type of the source stream.
 * @param options - `maxTime` and/or `maxSize`; at least one is required.
 * @returns A function mapping a source stream to a stream of readonly batches.
 */
export function batch(
	options: BatchOptions,
): <T>(stream: Stream<T, never>) => Stream<Readonly<T[]>, never> {
	if (options.maxTime === undefined && options.maxSize === undefined) {
		throw new Error("batch: at least one of maxTime or maxSize is required");
	}
	if (options.maxTime !== undefined && (options.maxTime < 0 || !Number.isFinite(options.maxTime))) {
		throw new Error("batch: maxTime must be a finite number >= 0");
	}
	if (options.maxSize !== undefined && (options.maxSize < 1 || !Number.isInteger(options.maxSize))) {
		throw new Error("batch: maxSize must be an integer >= 1");
	}
	return <T>(stream: Stream<T, never>): Stream<Readonly<T[]>, never> => ({
		*[Symbol.iterator]() {
			// The scope in which this subscription was created. Carried pulls are
			// spawned here, not in the caller's current task, so they live and die
			// with the subscription: `each.next()` runs in the consumer's task, and
			// spawning there would let a carried pull outlive a broken `each` loop
			// and consume (drop) one more source item.
			const subscription_scope = yield* useScope();
			const subscription = yield* stream;
			// A pull started for a previous batch that timed out before it resolved.
			// The next batch consumes it — keeping its real arrival time — instead of
			// halting it (which would close a NATS consumer) or dropping the item.
			let carried: Task<Pulled<T>> | undefined;

			function* fresh_pull(): Operation<Pulled<T>> {
				const result = yield* subscription.next();
				return { result, at: performance.now() };
			}

			function* pull(): Operation<Pulled<T>> {
				if (carried) {
					const pulled = yield* carried;
					carried = undefined;
					return pulled;
				}
				return yield* fresh_pull();
			}

			return {
				*next(): Operation<IteratorResult<Readonly<T[]>, never>> {
					// Block with no deadline for the first item; the window is measured
					// from when that item *arrived*, not from when next() was called — a
					// carried item may have arrived while a slow consumer was still busy.
					const first = yield* pull();
					const items: T[] = [unwrap(first)];
					const start = first.at;
					while (true) {
						if (options.maxSize !== undefined && items.length >= options.maxSize) {
							return { done: false, value: items };
						}
						if (options.maxTime === undefined) {
							items.push(unwrap(yield* pull()));
							continue;
						}
						const remaining = start + options.maxTime - performance.now();
						if (remaining <= 0) {
							return { done: false, value: items };
						}
						// Race the next item against the remaining window. On timeout keep
						// the still-pending pull (with its eventual arrival time) for the
						// next batch rather than halting it.
						const task = yield* subscription_scope.spawn(fresh_pull);
						const outcome = yield* timebox(remaining, () => task);
						if (outcome.timeout) {
							carried = task;
							return { done: false, value: items };
						}
						items.push(unwrap(outcome.value));
					}
				},
			};
		},
	});
}
