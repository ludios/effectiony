// Slop-provider: Claude Opus 4.8
//
// A replacement for @effectionx/stream-helpers' `batch`, which is unsuitable for
// a source that is idle most of the time: it applies the maxTime deadline to the
// FIRST item of a batch, so once a source goes quiet for one window it halts its
// pending pull and returns `{ done: true }` — silently ending a stream typed
// `Stream<T, never>` (and, for a NATS consumer, closing the consumer, because
// halting a pending consumer pull triggers its close path).

import { spawn } from "effection";
import type { Operation, Stream, Task } from "effection";
import { timebox } from "@effectionx/timebox";

export interface BatchOptions {
	/** Emit the current batch once this many ms elapse since its first item arrived. */
	readonly maxTime?: number;
	/** Emit the current batch once it holds this many items. */
	readonly maxSize?: number;
}

/**
 * Group items from an infinite source stream into batches, emitting a batch when
 * either `maxTime` elapses (measured from the batch's first item) or the batch
 * reaches `maxSize`. At least one of the two must be provided.
 *
 * The first item of every batch is awaited with **no** deadline, so an idle
 * source never times a batch out into a spurious, empty result — `maxTime` only
 * bounds how long we keep waiting to *extend* a batch that has already started.
 * The source is typed `Stream<T, never>`, i.e. it is expected to never end; if
 * it ever yields `done` that violates the contract and this throws rather than
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
	return <T>(stream: Stream<T, never>): Stream<Readonly<T[]>, never> => ({
		*[Symbol.iterator]() {
			const subscription = yield* stream;
			// A pull that was started for the previous batch but timed out before it
			// resolved. The next batch consumes it instead of dropping its value.
			let carried: Task<IteratorResult<T, never>> | undefined;

			function* pull(): Operation<T> {
				const result = carried ? yield* carried : yield* subscription.next();
				carried = undefined;
				if (result.done) {
					throw new Error("batch: source stream typed Stream<T, never> ended unexpectedly");
				}
				return result.value;
			}

			return {
				*next(): Operation<IteratorResult<Readonly<T[]>, never>> {
					// Block with no deadline for the first item; the maxTime window only
					// bounds how long we wait to extend a batch that already has one.
					const items: T[] = [yield* pull()];
					const start = performance.now();
					while (true) {
						if (options.maxSize !== undefined && items.length >= options.maxSize) {
							return { done: false, value: items };
						}
						if (options.maxTime === undefined) {
							items.push(yield* pull());
							continue;
						}
						const remaining = start + options.maxTime - performance.now();
						if (remaining <= 0) {
							return { done: false, value: items };
						}
						// Race the next item against the remaining window. On timeout keep
						// the still-pending pull for the next batch rather than halting it:
						// halting a NATS consumer pull would close the consumer.
						const task = yield* spawn(() => subscription.next());
						const outcome = yield* timebox(remaining, () => task);
						if (outcome.timeout) {
							carried = task;
							return { done: false, value: items };
						}
						if (outcome.value.done) {
							throw new Error("batch: source stream typed Stream<T, never> ended unexpectedly");
						}
						items.push(outcome.value.value);
					}
				},
			};
		},
	});
}
