import type { Stream } from "effection";
type AnyStream = Stream<unknown, unknown>;
type Sources = Record<string, AnyStream>;
type StringKeyOf<R> = Extract<keyof R, string>;
type OnlyStringKeys<R> = Exclude<keyof R, string> extends never ? R : never;
type StreamValue<S> = S extends Stream<infer T, unknown> ? T : never;
/**
 * Discriminated union of one `{ key, value }` variant per source, where `value`
 * is that source's element type. `switch (msg.key)` narrows `msg.value`.
 */
export type Tagged<R extends Sources> = {
    [K in StringKeyOf<R>]: {
        key: K;
        value: StreamValue<R[K]>;
    };
}[StringKeyOf<R>];
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
export declare function merge<const R extends Sources>(sources: OnlyStringKeys<R>): Stream<Tagged<R>, void>;
export {};
