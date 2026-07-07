import type { Stream } from "effection";
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
export declare function adjustable_interval(delay: number): AdjustableInterval;
