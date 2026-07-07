import type { Stream } from "effection";
export interface BatchOptions {
    /** Emit the current batch once this many ms elapse since its first item arrived. Must be finite and >= 0. */
    readonly maxTime?: number;
    /** Emit the current batch once it holds this many items. Must be an integer >= 1. */
    readonly maxSize?: number;
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
 * Pulls are never tied to a single `next()` call: every pull is spawned into
 * the subscription's scope and recorded before it is awaited, so halting one
 * `next()` (e.g. racing it against a shutdown signal) neither unwinds the
 * pending source pull (which would close a NATS consumer) nor orphans it and
 * loses its item — the pull is simply carried into the following `next()`.
 * Items already collected into the halted call's partial batch are dropped,
 * consistent with the lossy-on-shutdown stance above; source pulls are only
 * halted when the subscription itself is torn down.
 *
 * The source is typed `Stream<T, never>`, i.e. it is expected to never end; if it
 * ever yields `done` that violates the contract and this throws rather than
 * quietly ending the batched stream.
 *
 * @typeParam T - the element type of the source stream.
 * @param options - `maxTime` and/or `maxSize`; at least one is required.
 * @returns A function mapping a source stream to a stream of readonly batches.
 */
export declare function batch(options: BatchOptions): <T>(stream: Stream<T, never>) => Stream<Readonly<T[]>, never>;
