import type { Operation, Stream } from "effection";
import type { NatsConnection, NodeConnectionOptions } from "@nats-io/transport-node";
import type { ConsumeOptions, Consumer, ConsumerConfig, ConsumerInfo, JetStreamClient, JetStreamManager, JetStreamManagerOptions, JetStreamOptions, JsMsg, OrderedConsumerOptions, StreamInfo, StreamUpdateConfig } from "@nats-io/jetstream";
export type ConnectFn = () => Promise<NatsConnection>;
export type NatsConnectionReleaseMode = "close" | "drain";
export type StreamConsumeOptions = ConsumeOptions & {
    callback?: never;
};
declare const scoped_kind: unique symbol;
/**
 * Opaque handle to a NATS connection owned by an Effection scope.
 *
 * It deliberately exposes nothing: pass it to `nats_jetstream()`,
 * `nats_jetstream_manager()`, or `get_nats_consumer()`. If a helper needs a
 * connection method that is not yet exposed, add an explicit wrapper here
 * rather than reaching for the raw connection.
 */
export interface ScopedNatsConnection {
    readonly [scoped_kind]: "nats_connection";
}
/**
 * Opaque handle to a JetStream consumer owned by an Effection scope.
 *
 * Message iteration must go through `use_nats_consumer_messages()` so that the
 * long-lived ConsumerMessages iterator cannot escape scoped cleanup.
 */
export interface ScopedNatsConsumer {
    readonly [scoped_kind]: "jetstream_consumer";
}
export interface NatsConnectionResourceOptions {
    /**
     * How the resource releases the connection on scope exit.
     *
     * `drain` is the default because it is NATS' graceful shutdown path: drain
     * subscriptions, flush outbound data, then close. `close` is an immediate
     * shutdown escape hatch for tests and deliberately lossy termination.
     */
    release?: NatsConnectionReleaseMode;
}
/**
 * Open a NATS connection with @nats-io/transport-node and scope its lifetime.
 *
 * @param options - Node transport connection options forwarded to connect().
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection handle. The underlying
 * connection is drained by default when the caller's scope exits.
 */
export declare function use_nats_connection(options?: NodeConnectionOptions, resource_options?: NatsConnectionResourceOptions): Operation<ScopedNatsConnection>;
/**
 * Open a NATS connection with a caller-supplied opener and scope its lifetime.
 *
 * @param open - A nullary function that returns an established NATS connection.
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection handle. The underlying
 * connection is drained by default when the caller's scope exits.
 */
export declare function use_nats_connection(open: ConnectFn, resource_options?: NatsConnectionResourceOptions): Operation<ScopedNatsConnection>;
/**
 * Create a JetStream client from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param options - JetStream options accepted by @nats-io/jetstream jetstream().
 * @returns A JetStream client whose lifetime is bounded by the underlying connection.
 */
export declare function nats_jetstream(connection: ScopedNatsConnection | NatsConnection, options?: JetStreamOptions): JetStreamClient;
/**
 * Create a JetStream manager from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param options - JetStream manager options passed to jetstreamManager().
 * @returns An operation yielding a JetStream manager whose lifetime is bounded
 * by the underlying connection.
 */
export declare function nats_jetstream_manager(connection: ScopedNatsConnection | NatsConnection, options?: JetStreamOptions | JetStreamManagerOptions): Operation<JetStreamManager>;
/**
 * Ensure that the JetStream stream exists and has a particular configuration.
 *
 * @param stream_manager - The JetStream manager used for stream administration.
 * @param stream_name - The name of the JetStream stream.
 * @param config - The configuration to set on the stream.
 *
 * For config, use e.g.
 * 	const config = {
 *		subjects: [
 *			"some_stream_name.topic1",
 *			"some_stream_name.topic2",
 *		] as Array<string>,
 *		retention: "limits",
 *		max_age: 24 * 3600 * 1_000_000_000, // in nanoseconds
 *		discard: "old",
 *	} as const;
 */
export declare function ensure_nats_stream(stream_manager: JetStreamManager, stream_name: string, config: Partial<StreamUpdateConfig>): Operation<StreamInfo>;
/**
 * Ensure that a durable JetStream consumer exists, creating it only when the
 * server reports it missing.
 *
 * @param stream_manager - The JetStream manager used for consumer administration.
 * @param stream_name - The stream containing the consumer.
 * @param consumer_name - The durable name of the consumer.
 * @param config - The consumer config, which cannot carry its own `durable_name`
 * or `name`: used in full at creation; when the consumer already exists, its
 * updatable subset is applied and its creation-only properties must match the
 * existing consumer.
 * @returns An operation that completes after the consumer exists.
 */
export declare function ensure_durable_nats_consumer(stream_manager: JetStreamManager, stream_name: string, consumer_name: string, config: Partial<Omit<ConsumerConfig, "durable_name" | "name">>): Operation<ConsumerInfo>;
/**
 * Get a JetStream consumer from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param stream_name - The stream containing the consumer.
 * @param consumer_name - The consumer name.
 * @param options - JetStream client options accepted by @nats-io/jetstream jetstream().
 * @returns An operation yielding a scoped JetStream consumer handle.
 */
export declare function get_nats_consumer(connection: ScopedNatsConnection | NatsConnection, stream_name: string, consumer_name: string, options?: JetStreamOptions): Operation<ScopedNatsConsumer>;
/**
 * Create an ordered (ephemeral) JetStream consumer and scope its lifetime.
 *
 * An ordered consumer is the ephemeral counterpart to the durable consumers
 * managed by ensure_durable_nats_consumer(): the server names it, forces
 * ack-none delivery, and the client transparently recreates it if delivery
 * becomes inconsistent. Because no cursor outlives the acquisition, every
 * acquisition starts at the position described by `deliver_policy` — use this
 * for snapshot-style subjects where only current data matters, and a durable
 * consumer would replay a backlog accumulated while the process was down.
 *
 * The server-side consumer is deleted when the acquiring scope exits; if that
 * fails, the server expires it after `inactive_threshold` (default five
 * minutes). Note that `ordered_options.inactive_threshold` is in
 * milliseconds, unlike the nanoseconds of ConsumerConfig.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param stream_name - The stream to consume.
 * @param ordered_options - Ordered consumer options, e.g. `deliver_policy`
 * and `filter_subjects`; an empty config delivers every message currently in
 * the stream and onward.
 * @param options - JetStream client options accepted by @nats-io/jetstream jetstream().
 * @returns An operation yielding a scoped JetStream consumer handle whose
 * server-side consumer lives until the acquiring scope exits.
 */
export declare function use_ordered_nats_consumer(connection: ScopedNatsConnection | NatsConnection, stream_name: string, ordered_options?: Partial<OrderedConsumerOptions>, options?: JetStreamOptions): Operation<ScopedNatsConsumer>;
/**
 * Consume JetStream messages as an Effection stream with scoped cleanup.
 *
 * @param consumer - The JetStream consumer returned by get_nats_consumer() or NATS directly.
 * @param options - Consumer consume options, excluding callback mode.
 * @returns A stream of JetStream messages. Leaving the consuming scope closes
 * the underlying ConsumerMessages iterator and returns its async iterator.
 */
export declare function use_nats_consumer_messages(consumer: Consumer | ScopedNatsConsumer, options?: StreamConsumeOptions): Stream<JsMsg, void>;
export {};
