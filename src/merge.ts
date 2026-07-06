// Model-output: Claude Opus 4.8
// with with some code review from
// Model-output: ChatGPT 5.5 Thinking

import { createQueue, resource, spawn } from "effection";
import type { Queue, Stream, Subscription } from "effection";

type AnyStream         = Stream<unknown, unknown>;
type Sources           = Record<string, AnyStream>;
type StringKeyOf<R>    = Extract<keyof R, string>;
type OnlyStringKeys<R> = Exclude<keyof R, string> extends never ? R : never;
type StreamValue<S>    = S extends Stream<infer T, unknown> ? T : never;

/**
 * Discriminated union of one `{ key, value }` variant per source, where `value`
 * is that source's element type. `switch (msg.key)` narrows `msg.value`.
 */
export type Tagged<R extends Sources> = {
	[K in StringKeyOf<R>]: { key: K; value: StreamValue<R[K]> };
}[StringKeyOf<R>];

/**
 * Drain one source into the shared queue, tagging every value with its key,
 * then record the source as finished and close the queue once it is the last.
 * A throw from the source (subscribe or `next()`) is intentionally NOT caught:
 * it errors this child task, which fails the merge scope and halts the
 * siblings — fail-fast, not "close with void".
 *
 * @param queue - the shared output queue every source feeds.
 * @param key - the string label identifying this source.
 * @param source - the stream to subscribe to and drain.
 * @param remaining - mutable cell holding the count of sources not yet finished;
 *                    this forwarder decrements it exactly once, on completion.
 * @returns an operation that completes when `source` closes normally.
 */
function* forward_source<R extends Sources, K extends StringKeyOf<R>>(
	queue: Queue<Tagged<R>, void>,
	key: K,
	source: R[K],
	remaining: { count: number },
) {
	let sub  = yield* source;
	let next = yield* sub.next();
	while (!next.done) {
		queue.add({ key, value: next.value } as Tagged<R>);
		next = yield* sub.next();
	}
	if (remaining.count <= 0) {
		throw new Error("merge: a source finished after the count reached zero");
	}
	remaining.count -= 1;
	if (remaining.count === 0) {
		queue.close(undefined);
	}
}

/**
 * Merge a record of named streams into one stream of `{ key, value }` messages,
 * surfacing each value tagged with the source that produced it.
 *
 * Values are emitted in the order forwarders enqueue them: for live sources
 * whose `next()` suspends this tracks readiness, but an already-buffered source
 * is drained greedily, so buffered data arrives in enumeration-biased bursts
 * rather than round-robin. The queue is unbounded — a hot source outrunning the
 * consumer grows memory, there is no backpressure. Closes (with `void`) once
 * every source closes normally; per-source close values are discarded.
 * Forwarders are children of the consuming scope and are halted automatically
 * when the `each` loop ends or that scope exits.
 *
 * @typeParam R - record mapping a string label to each source stream.
 * @param sources - the named streams to fan in. An empty record yields an
 *                  immediately-closed stream.
 * @returns a single-consumer stream of tagged values, discriminated by key.
 */
export function merge<const R extends Sources>(
	sources: OnlyStringKeys<R>,
): Stream<Tagged<R>, void> {
	return resource<Subscription<Tagged<R>, void>>(function* (provide) {
		let queue     = createQueue<Tagged<R>, void>();
		let keys      = Object.keys(sources) as StringKeyOf<R>[];
		let remaining = { count: keys.length };
		if (remaining.count === 0) {
			queue.close(undefined);
		}
		for (let key of keys) {
			let source = (sources as R)[key];
			yield* spawn(function* () {
				yield* forward_source<R, typeof key>(queue, key, source, remaining);
			});
		}
		yield* provide(queue);
	});
}
