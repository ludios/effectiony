// Model-output: ChatGPT 5.5 Thinking
// Model-output: ChatGPT 5.5 Pro
// Model-output: Claude Opus 4.8
// Model-output: Claude Fable 5

/**
 * Effection resources for NATS connections and JetStream helpers.
 *
 * `use_nats_connection()` yields an opaque handle whose underlying connection
 * is owned by the acquiring Effection scope: the connection is drained (or
 * closed) when the scope exits, and an error-caused close fails the scope. The
 * handle exposes no NATS methods; it can only be passed to the helpers in this
 * module, so connection lifetime cannot escape Effection. JetStream message
 * iteration goes through `use_nats_consumer_messages()`, which ties a
 * ConsumerMessages iterator to the consuming scope.
 */
import { A } from "ayy";
import { action, resource, spawn, until } from "effection";
import type { Operation, Stream, Subscription as EffectionSubscription } from "effection";
import {
	ClosedConnectionError,
	DrainingConnectionError,
	connect as node_connect,
} from "@nats-io/transport-node";
import {
	JetStreamApiCodes,
	JetStreamApiError,
	jetstream as create_jetstream,
	jetstreamManager as create_jetstream_manager,
} from "@nats-io/jetstream";
import type { NatsConnection, NodeConnectionOptions } from "@nats-io/transport-node";
import type {
	ConsumeOptions,
	Consumer,
	ConsumerConfig,
	ConsumerInfo,
	ConsumerMessages,
	ConsumerUpdateConfig,
	JetStreamApiCodes as JetStreamApiCode,
	JetStreamClient,
	JetStreamManager,
	JetStreamManagerOptions,
	JetStreamOptions,
	JsMsg,
	OrderedConsumerOptions,
	StreamInfo,
	StreamUpdateConfig,
} from "@nats-io/jetstream";
import { getLogger } from "@logtape/logtape";
const logger = getLogger(["sophon", "gear"]);

type SettleState = "pending" | "settled" | "discarded";
type ErrorClass = Function & { readonly prototype: Error; readonly name: string };

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

const expected_connection_drain_errors = [ClosedConnectionError, DrainingConnectionError] as const;

const scoped_connections = new WeakMap<ScopedNatsConnection, NatsConnection>();
const scoped_consumers = new WeakMap<ScopedNatsConsumer, Consumer>();

/**
 * Create an opaque scoped handle for a connection owned by an Effection resource.
 *
 * @param connection - The underlying NATS connection owned by the resource.
 * @returns A frozen, empty handle mapping back to the connection via a
 * module-private WeakMap.
 */
function create_scoped_connection(connection: NatsConnection): ScopedNatsConnection {
	const handle = Object.freeze(Object.create(null)) as ScopedNatsConnection;
	scoped_connections.set(handle, connection);
	return handle;
}

/**
 * Resolve a scoped handle back to the raw NATS connection it protects.
 *
 * @param connection - A scoped handle from use_nats_connection(), or a raw
 * connection whose lifetime the caller manages itself.
 * @returns The raw NATS connection.
 */
function unwrap_connection(connection: ScopedNatsConnection | NatsConnection): NatsConnection {
	return scoped_connections.get(connection as ScopedNatsConnection) ?? (connection as NatsConnection);
}

/**
 * Create an opaque scoped handle for a JetStream consumer.
 *
 * @param consumer - The underlying JetStream consumer returned by NATS.
 * @returns A frozen, empty handle mapping back to the consumer via a
 * module-private WeakMap.
 */
function create_scoped_consumer(consumer: Consumer): ScopedNatsConsumer {
	const handle = Object.freeze(Object.create(null)) as ScopedNatsConsumer;
	scoped_consumers.set(handle, consumer);
	return handle;
}

/**
 * Resolve a scoped handle back to the raw JetStream consumer it protects.
 *
 * @param consumer - A scoped handle from get_nats_consumer(), or a raw
 * consumer whose iterators the caller manages itself.
 * @returns The raw JetStream consumer.
 */
function unwrap_consumer(consumer: Consumer | ScopedNatsConsumer): Consumer {
	return scoped_consumers.get(consumer as ScopedNatsConsumer) ?? (consumer as Consumer);
}

/**
 * Normalize an arbitrary thrown value into an Error.
 *
 * @param thrown_value - A value thrown or used to reject a promise.
 * @returns An Error instance representing the thrown value, preserving the
 * original value as `cause`.
 */
function to_error(thrown_value: unknown): Error {
	if (thrown_value instanceof Error) {
		return thrown_value;
	}
	return new Error(String(thrown_value), { cause: thrown_value });
}

/**
 * Check whether an error is one of a known set of NATS errors.
 *
 * @param thrown_value - A value caught from a NATS promise or callback.
 * @param error_classes - Constructors for the accepted NATS error classes.
 * @returns True when the value is an instance of one of the classes, or when it
 * is an Error with the same name. The name fallback keeps cross-realm NATS
 * errors and deliberate test doubles working without accepting non-Error values.
 */
function is_error_of_kind(thrown_value: unknown, error_classes: readonly ErrorClass[]): boolean {
	if (!(thrown_value instanceof Error)) {
		return false;
	}
	return error_classes.some((error_class) => {
		return thrown_value instanceof error_class || thrown_value.name === error_class.name;
	});
}

/**
 * Check whether an error has a specific JetStream API code.
 *
 * @param thrown_value - A value caught from a JetStream API operation.
 * @param code - The JetStream API error code that should be treated specially.
 * @returns True when the value is a JetStreamApiError carrying the requested code.
 */
function is_jetstream_api_code(thrown_value: unknown, code: JetStreamApiCode): boolean {
	return thrown_value instanceof JetStreamApiError && thrown_value.code === code;
}

/**
 * Close a NATS connection that completed after its Effection acquisition was discarded.
 *
 * @param connection - A connection whose caller has already left the acquiring scope.
 * @returns Nothing; rejection is intentionally suppressed because no scope owns it.
 */
function close_discarded_connection(connection: NatsConnection): void {
	try {
		if (!connection.isClosed()) {
			void connection.close().catch(() => {});
		}
	} catch {
		// A discarded acquisition has no remaining scope to report cleanup failures to.
	}
}

/**
 * Convert a promise-returning resource opener into a discard-aware operation.
 *
 * Effection's until() has no hook for disposing a value that resolves after
 * the awaiting operation was halted; this fills that gap.
 *
 * @param start - Starts one underlying async acquisition attempt.
 * @param dispose_discarded_value - Disposes a value that resolved after the
 * acquiring scope was discarded; no scope remains to own it.
 * @param action_description - Effection action label used for diagnostics.
 * @returns An operation that yields the acquired value, or disposes a late value
 * if the operation is discarded before the promise settles.
 */
function acquire_with_discard<T>(
	start: () => Promise<T>,
	dispose_discarded_value: (value: T) => void,
	action_description: string,
): Operation<T> {
	return action<T>((resolve, reject) => {
		let settle_state: SettleState = "pending";
		try {
			start().then(
				(value) => {
					if (settle_state === "discarded") {
						dispose_discarded_value(value);
						return;
					}
					settle_state = "settled";
					resolve(value);
				},
				(thrown_value) => {
					if (settle_state !== "discarded") {
						settle_state = "settled";
						reject(to_error(thrown_value));
					}
				},
			);
		} catch (thrown_value) {
			settle_state = "settled";
			reject(to_error(thrown_value));
		}
		return () => {
			if (settle_state === "pending") {
				settle_state = "discarded";
			}
		};
	}, action_description);
}

/**
 * Create the connection-opening function for either overload shape.
 *
 * @param options_or_open - Node transport options or a caller-supplied opener.
 * @returns A nullary function that opens one NATS connection.
 */
function create_connect_fn(options_or_open: NodeConnectionOptions | ConnectFn): ConnectFn {
	if (typeof options_or_open === "function") {
		return options_or_open;
	}
	return () => node_connect(options_or_open);
}

/**
 * Assert that a connection can still start a new operation.
 *
 * @param connection - The raw NATS connection being used.
 * @param action_description - Human-readable operation used in assertion messages.
 * @returns Nothing when the connection is usable.
 */
function assert_connection_usable(connection: NatsConnection, action_description: string): void {
	A(!connection.isClosed(), `cannot ${action_description} on a closed NATS connection`);
	A(!connection.isDraining(), `cannot ${action_description} on a draining NATS connection`);
}

/**
 * Monitor a connection and fail the owning Effection scope on error-caused close.
 *
 * @param connection - The owned NATS connection to monitor.
 * @returns An operation that completes on clean close and throws on error close.
 */
function* monitor_connection_close(connection: NatsConnection): Operation<void> {
	const close_reason = yield* until(connection.closed());
	if (close_reason) {
		throw close_reason;
	}
}

/**
 * Release a connection according to the resource's configured release mode.
 *
 * @param connection - The owned NATS connection.
 * @param release - The release behavior to use on scope exit.
 * @returns An operation that completes when the connection is released.
 */
function* release_connection(connection: NatsConnection, release: NatsConnectionReleaseMode): Operation<void> {
	if (connection.isClosed()) {
		return;
	}
	if (release === "close") {
		yield* until(connection.close());
		return;
	}
	if (connection.isDraining()) {
		yield* until(connection.closed());
		return;
	}
	try {
		yield* until(connection.drain());
	} catch (thrown_value) {
		if (is_error_of_kind(thrown_value, expected_connection_drain_errors)) {
			if (connection.isDraining()) {
				yield* until(connection.closed());
			}
			return;
		}
		if (!connection.isClosed()) {
			yield* until(connection.close());
		}
		throw to_error(thrown_value);
	}
}

/**
 * Open a NATS connection with @nats-io/transport-node and scope its lifetime.
 *
 * @param options - Node transport connection options forwarded to connect().
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection handle. The underlying
 * connection is drained by default when the caller's scope exits.
 */
export function use_nats_connection(
	options?: NodeConnectionOptions,
	resource_options?: NatsConnectionResourceOptions,
): Operation<ScopedNatsConnection>;

/**
 * Open a NATS connection with a caller-supplied opener and scope its lifetime.
 *
 * @param open - A nullary function that returns an established NATS connection.
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection handle. The underlying
 * connection is drained by default when the caller's scope exits.
 */
export function use_nats_connection(
	open: ConnectFn,
	resource_options?: NatsConnectionResourceOptions,
): Operation<ScopedNatsConnection>;

/**
 * Open a NATS connection and scope its lifetime.
 *
 * @param options_or_open - Node transport options or a caller-supplied opener.
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection handle.
 */
export function use_nats_connection(
	options_or_open: NodeConnectionOptions | ConnectFn = {},
	resource_options: NatsConnectionResourceOptions = {},
): Operation<ScopedNatsConnection> {
	return resource(function* (provide) {
		const open = create_connect_fn(options_or_open);
		const release = resource_options.release ?? "drain";
		const connection = yield* acquire_with_discard(open, close_discarded_connection, "nats.connect");
		try {
			const scoped_connection = create_scoped_connection(connection);
			const monitor = yield* spawn(() => monitor_connection_close(connection));
			try {
				yield* provide(scoped_connection);
			} finally {
				// Halt the monitor before releasing: release_connection tolerates
				// the expected drain/close errors, and a still-live monitor would
				// rethrow the raw close reason and interrupt cleanup mid-drain.
				yield* monitor.halt();
			}
		} finally {
			yield* release_connection(connection, release);
		}
	});
}

/**
 * Create a JetStream client from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param options - JetStream options accepted by @nats-io/jetstream jetstream().
 * @returns A JetStream client whose lifetime is bounded by the underlying connection.
 */
export function nats_jetstream(
	connection: ScopedNatsConnection | NatsConnection,
	options: JetStreamOptions = {},
): JetStreamClient {
	const raw_connection = unwrap_connection(connection);
	assert_connection_usable(raw_connection, "create JetStream client");
	return create_jetstream(raw_connection, options);
}

/**
 * Create a JetStream manager from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param options - JetStream manager options passed to jetstreamManager().
 * @returns An operation yielding a JetStream manager whose lifetime is bounded
 * by the underlying connection.
 */
export function* nats_jetstream_manager(
	connection: ScopedNatsConnection | NatsConnection,
	options: JetStreamOptions | JetStreamManagerOptions = {},
): Operation<JetStreamManager> {
	const raw_connection = unwrap_connection(connection);
	assert_connection_usable(raw_connection, "create JetStream manager");
	return yield* acquire_with_discard(
		() => create_jetstream_manager(raw_connection, options),
		() => {},
		"nats.jetstreamManager",
	);
}

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
export function* ensure_nats_stream(stream_manager: JetStreamManager, stream_name: string, config: Partial<StreamUpdateConfig>): Operation<StreamInfo> {
	let exists = false;
	let info;
	try {
		info = yield* until(stream_manager.streams.info(stream_name));
		logger.info("found NATS JetStream for {stream_name}", { stream_name });
		exists = true;
	} catch (thrown_value) {
		if (!is_jetstream_api_code(thrown_value, JetStreamApiCodes.StreamNotFound)) {
			throw to_error(thrown_value);
		}
		info = yield* until(stream_manager.streams.add({ name: stream_name, ...config }));
		logger.info("created NATS JetStream for {stream_name}", { stream_name });
	}
	if (exists) {
		info = yield* until(stream_manager.streams.update(stream_name, config));
		logger.info("updated NATS JetStream for {stream_name}", { stream_name });
	}
	return info;
}

/**
 * Every key of ConsumerUpdateConfig as a runtime value. TypeScript types are
 * erased at runtime, so this is the only way to test key membership; the
 * `satisfies` clause makes compilation fail here if a @nats-io/jetstream
 * upgrade adds or removes updatable consumer properties.
 */
const consumer_update_config_keys = {
	// From PriorityGroups, which ConsumerUpdateConfig extends
	priority_groups: true,
	priority_policy: true,
	priority_timeout: true,
	description: true,
	ack_wait: true,
	max_deliver: true,
	sample_freq: true,
	max_ack_pending: true,
	max_waiting: true,
	headers_only: true,
	deliver_subject: true,
	max_batch: true,
	max_expires: true,
	inactive_threshold: true,
	backoff: true,
	max_bytes: true,
	num_replicas: true,
	mem_storage: true,
	filter_subject: true,
	filter_subjects: true,
	metadata: true,
} satisfies Record<keyof ConsumerUpdateConfig, true>;

/**
 * Narrow a consumer creation config to the properties accepted by consumers.update().
 *
 * ConsumerUpdateConfig has fewer properties than ConsumerConfig:
 * https://nats-io.github.io/nats.deno/interfaces/ConsumerUpdateConfig.html
 * https://nats-io.github.io/nats.deno/interfaces/ConsumerConfig.html
 *
 * Creation-only properties (e.g. ack_policy) whose values match the existing
 * consumer are dropped, keeping ensure_durable_nats_consumer() idempotent; one whose
 * value differs throws, because the server cannot change it on an existing
 * consumer and would reject the update with a less descriptive error.
 *
 * @param config - The consumer creation config passed to ensure_durable_nats_consumer().
 * @param existing_config - The config reported by the server for the existing consumer.
 * @returns The subset of config that consumers.update() can apply.
 */
function to_consumer_update_config(config: Partial<ConsumerConfig>, existing_config: ConsumerConfig): ConsumerUpdateConfig {
	const update_config: Record<string, unknown> = {};
	const conflicts: Array<string> = [];
	for (const [key, value] of Object.entries(config)) {
		if (value === undefined) {
			continue;
		}
		if (key in consumer_update_config_keys) {
			update_config[key] = value;
		} else {
			const existing_value = (existing_config as Record<string, unknown>)[key];
			if (value !== existing_value) {
				conflicts.push(`${key} (existing ${JSON.stringify(existing_value)}, config ${JSON.stringify(value)})`);
			}
		}
	}
	A(conflicts.length === 0, () => `consumer config sets creation-only properties that differ from the existing consumer; delete the consumer or match its config: ${conflicts.join(", ")}`);
	return update_config as ConsumerUpdateConfig;
}

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
export function* ensure_durable_nats_consumer(
	stream_manager: JetStreamManager,
	stream_name: string,
	consumer_name: string,
	config: Partial<Omit<ConsumerConfig, "durable_name" | "name">>,
): Operation<ConsumerInfo> {
	let exists = false;
	let info;
	try {
		info = yield* until(stream_manager.consumers.info(stream_name, consumer_name));
		logger.info("found NATS JetStream consumer {consumer_name} for stream {stream_name}", { consumer_name, stream_name });
		exists = true;
	} catch (thrown_value) {
		if (!is_jetstream_api_code(thrown_value, JetStreamApiCodes.ConsumerNotFound)) {
			throw to_error(thrown_value);
		}
		info = yield* until(stream_manager.consumers.add(stream_name, { ...config, durable_name: consumer_name }));
		logger.info("created NATS JetStream consumer {consumer_name} for stream {stream_name}", { consumer_name, stream_name });
	}
	if (exists) {
		const update_config = to_consumer_update_config(config, info.config);
		info = yield* until(stream_manager.consumers.update(stream_name, consumer_name, update_config));
		logger.info("updated NATS JetStream consumer {consumer_name} for stream {stream_name}", { consumer_name, stream_name });
	}
	return info;
}

/**
 * Get a JetStream consumer from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param stream_name - The stream containing the consumer.
 * @param consumer_name - The consumer name.
 * @param options - JetStream client options accepted by @nats-io/jetstream jetstream().
 * @returns An operation yielding a scoped JetStream consumer handle.
 */
export function* get_nats_consumer(
	connection: ScopedNatsConnection | NatsConnection,
	stream_name: string,
	consumer_name: string,
	options: JetStreamOptions = {},
): Operation<ScopedNatsConsumer> {
	const stream_client = nats_jetstream(connection, options);
	const consumer = yield* acquire_with_discard(
		() => stream_client.consumers.get(stream_name, consumer_name),
		() => {},
		"nats.consumer.get",
	);
	return create_scoped_consumer(consumer);
}

/**
 * Delete an ephemeral consumer whose creation completed after its Effection
 * acquisition was discarded.
 *
 * @param consumer - A consumer whose caller has already left the acquiring scope.
 * @returns Nothing; rejection is intentionally suppressed because no scope owns it.
 */
function delete_discarded_consumer(consumer: Consumer): void {
	try {
		void consumer.delete().catch(() => {});
	} catch {
		// A discarded acquisition has no remaining scope to report cleanup failures to.
	}
}

/**
 * Delete an ephemeral consumer on scope exit, best effort.
 *
 * Deletion is a courtesy to the server: the consumer's inactive_threshold
 * already guarantees cleanup, so a failure here (the server expired it first,
 * the connection is gone) must not fail an otherwise-clean scope or mask the
 * error already unwinding it.
 *
 * @param consumer - The raw ephemeral consumer to delete.
 * @param stream_name - The stream the consumer belongs to, for logging.
 * @returns An operation that completes once the deletion attempt settles.
 */
function* delete_ephemeral_consumer(consumer: Consumer, stream_name: string): Operation<void> {
	try {
		yield* until(consumer.delete());
		logger.info("deleted ephemeral NATS JetStream consumer for stream {stream_name}", { stream_name });
	} catch (thrown_value) {
		logger.debug("could not delete ephemeral NATS JetStream consumer for stream {stream_name}, leaving it to inactive_threshold: {thrown_value}", { stream_name, thrown_value });
	}
}

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
export function use_ordered_nats_consumer(
	connection: ScopedNatsConnection | NatsConnection,
	stream_name: string,
	ordered_options: Partial<OrderedConsumerOptions> = {},
	options: JetStreamOptions = {},
): Operation<ScopedNatsConsumer> {
	return resource(function* (provide) {
		const stream_client = nats_jetstream(connection, options);
		const consumer = yield* acquire_with_discard(
			() => stream_client.consumers.get(stream_name, ordered_options),
			delete_discarded_consumer,
			"nats.consumer.ordered",
		);
		logger.info("created ephemeral NATS JetStream consumer for stream {stream_name}", { stream_name });
		try {
			yield* provide(create_scoped_consumer(consumer));
		} finally {
			yield* delete_ephemeral_consumer(consumer, stream_name);
		}
	});
}

interface TrackedConsumerMessages {
	messages: ConsumerMessages;
	iterator: AsyncIterator<JsMsg, void>;
	close_promise: Promise<void> | null;
	return_promise: Promise<void> | null;
	next_started: boolean;
	iterator_error: Error | null;
	released: boolean;
}

/**
 * Close JetStream consumer messages that completed after their acquiring scope was discarded.
 *
 * @param messages - Consumer messages whose caller has already left scope.
 * @returns Nothing; rejection is intentionally suppressed because no scope owns it.
 */
function close_discarded_consumer_messages(messages: ConsumerMessages): void {
	try {
		const iterator = messages[Symbol.asyncIterator]();
		const close_promise = messages.close().catch(() => {});
		void Promise.resolve(iterator.next())
			.catch(() => {})
			.then(() => iterator.return?.())
			.catch(() => {})
			.then(() => close_promise);
	} catch {
		// A discarded acquisition has no remaining scope to report cleanup failures to.
	}
}

/**
 * Create a tracked JetStream consumer-messages record.
 *
 * @param messages - The live ConsumerMessages returned by consumer.consume().
 * @returns A record containing one async iterator and idempotent cleanup state.
 */
function create_tracked_consumer_messages(messages: ConsumerMessages): TrackedConsumerMessages {
	return {
		messages,
		iterator: messages[Symbol.asyncIterator](),
		close_promise: null,
		return_promise: null,
		next_started: false,
		iterator_error: null,
		released: false,
	};
}

/**
 * Start closing ConsumerMessages exactly once.
 *
 * @param tracked_messages - The tracked consumer messages to close.
 * @returns A promise that resolves when close() completes. NATS close reasons
 * are delivery state, not Effection cleanup failures, so resolved Error values are ignored.
 */
function start_consumer_messages_close(tracked_messages: TrackedConsumerMessages): Promise<void> {
	if (!tracked_messages.close_promise) {
		tracked_messages.close_promise = tracked_messages.messages.close().then(() => undefined);
	}
	return tracked_messages.close_promise;
}

/**
 * Start one iterator next() call so ConsumerMessages close callbacks can run.
 *
 * This is load-bearing, not paranoia: in @nats-io/jetstream (see stop() in
 * lib/consumer.js), close() pushes its completion callback onto the message
 * iterator's queue, so close() only settles once something drives the
 * iterator. If next() was never called, drive it once here; otherwise
 * teardown would hang forever.
 *
 * @param tracked_messages - The tracked consumer messages whose iterator may not have started.
 * @returns A promise for the priming next(), or null when the iterator was already started.
 */
function start_unstarted_consumer_messages_next(tracked_messages: TrackedConsumerMessages): Promise<void> | null {
	if (tracked_messages.next_started) {
		return null;
	}
	tracked_messages.next_started = true;
	return Promise.resolve(tracked_messages.iterator.next()).then(() => undefined, () => undefined);
}

/**
 * Start returning the consumer-messages async iterator exactly once.
 *
 * @param tracked_messages - The tracked consumer messages whose iterator should be returned.
 * @returns A promise for iterator return, or null when the iterator has no return method.
 */
function start_iterator_return(tracked_messages: TrackedConsumerMessages): Promise<void> | null {
	if (tracked_messages.return_promise) {
		return tracked_messages.return_promise;
	}
	const return_iterator = tracked_messages.iterator.return;
	if (!return_iterator) {
		return null;
	}
	tracked_messages.return_promise = Promise.resolve()
		.then(() => return_iterator.call(tracked_messages.iterator))
		.then(() => undefined);
	return tracked_messages.return_promise;
}

/**
 * Request non-reporting cleanup for a halted pending next() call.
 *
 * @param tracked_messages - The tracked consumer messages being consumed.
 * @returns Nothing; cleanup failures are reported by the enclosing resource cleanup.
 */
function interrupt_consumer_message_next(tracked_messages: TrackedConsumerMessages): void {
	const close_promise = start_consumer_messages_close(tracked_messages).catch(() => {});
	void close_promise
		.then(() => start_iterator_return(tracked_messages))
		.catch(() => {});
}

/**
 * Release tracked ConsumerMessages, including iterator return.
 *
 * @param tracked_messages - The tracked consumer messages to release.
 * @returns An operation that completes after close() and iterator return have settled.
 */
function* release_tracked_consumer_messages(tracked_messages: TrackedConsumerMessages): Operation<void> {
	if (tracked_messages.released) {
		return;
	}
	tracked_messages.released = true;
	let release_error: Error | null = null;
	const close_promise = start_consumer_messages_close(tracked_messages);
	const priming_next_promise = start_unstarted_consumer_messages_next(tracked_messages);
	try {
		if (priming_next_promise) {
			yield* until(priming_next_promise);
		}
	} catch (thrown_value) {
		release_error = to_error(thrown_value);
	}
	try {
		const return_promise = start_iterator_return(tracked_messages);
		if (return_promise) {
			yield* until(return_promise);
		}
	} catch (thrown_value) {
		if (!tracked_messages.iterator_error) {
			release_error ??= to_error(thrown_value);
		}
	}
	try {
		yield* until(close_promise);
	} catch (thrown_value) {
		release_error ??= to_error(thrown_value);
	}
	if (release_error) {
		throw release_error;
	}
}

/**
 * Build an Effection subscription around JetStream ConsumerMessages.
 *
 * Halting a pending next() (for example by halting the consuming task) closes
 * the underlying ConsumerMessages: an async iterator's next() cannot be
 * un-called, so abandoning one means abandoning the whole stream. Do not
 * race() a single next() against a timeout expecting the stream to survive.
 *
 * @param tracked_messages - The tracked ConsumerMessages and iterator.
 * @returns An Effection subscription whose halted next() closes the underlying messages.
 */
function create_consumer_messages_subscription(
	tracked_messages: TrackedConsumerMessages,
): EffectionSubscription<JsMsg, void> {
	return {
		next: () =>
			action<IteratorResult<JsMsg, void>>((resolve, reject) => {
				let active = true;
				let settled = false;
				if (tracked_messages.released) {
					settled = true;
					resolve({ done: true, value: undefined });
					return () => {};
				}
				tracked_messages.next_started = true;
				Promise.resolve(tracked_messages.iterator.next()).then(
					(result) => {
						settled = true;
						if (active) {
							resolve(result as IteratorResult<JsMsg, void>);
						}
					},
					(thrown_value) => {
						settled = true;
						const error = to_error(thrown_value);
						tracked_messages.iterator_error = error;
						if (active) {
							reject(error);
						}
					},
				);
				return () => {
					active = false;
					if (!settled) {
						interrupt_consumer_message_next(tracked_messages);
					}
				};
			}, "nats.consumer.next"),
	};
}

/**
 * Consume JetStream messages as an Effection stream with scoped cleanup.
 *
 * @param consumer - The JetStream consumer returned by get_nats_consumer() or NATS directly.
 * @param options - Consumer consume options, excluding callback mode.
 * @returns A stream of JetStream messages. Leaving the consuming scope closes
 * the underlying ConsumerMessages iterator and returns its async iterator.
 */
export function use_nats_consumer_messages(
	consumer: Consumer | ScopedNatsConsumer,
	options: StreamConsumeOptions = {},
): Stream<JsMsg, void> {
	return resource(function* (provide) {
		A.eq((options as ConsumeOptions).callback, undefined, "callback consumers cannot be adapted as streams");
		const raw_consumer = unwrap_consumer(consumer);
		const messages = yield* acquire_with_discard(
			() => raw_consumer.consume(options),
			close_discarded_consumer_messages,
			"nats.consumer.consume",
		);
		const tracked_messages = create_tracked_consumer_messages(messages);
		try {
			yield* provide(create_consumer_messages_subscription(tracked_messages));
		} finally {
			yield* release_tracked_consumer_messages(tracked_messages);
		}
	});
}
