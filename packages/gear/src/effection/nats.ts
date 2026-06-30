// Slop-provider: ChatGPT 5.5 Thinking
// Slop-provider: ChatGPT 5.5 Pro
// Slop-provider: Claude Opus 4.8

/**
 * Effection resources for NATS connections, subscriptions, and JetStream helpers.
 *
 * The scoped connection handle is a capability facade: user code can publish,
 * request, flush, and inspect status, but connection teardown and raw
 * subscriptions remain owned by Effection resources. Use `use_nats_subscription()`
 * to create subscription streams with lexical cleanup.
 */
import { A } from "ayy";
import { action, resource, spawn, until } from "effection";
import type { Operation, Stream, Subscription as EffectionSubscription } from "effection";
import {
	ClosedConnectionError,
	DrainingConnectionError,
	InvalidOperationError,
	connect as node_connect,
} from "@nats-io/transport-node";
import {
	JetStreamApiCodes,
	JetStreamApiError,
	jetstream as create_jetstream,
	jetstreamManager as create_jetstream_manager,
} from "@nats-io/jetstream";
import type {
	Msg,
	NatsConnection,
	NodeConnectionOptions,
	Subscription as NatsSubscription,
	SubscriptionOptions,
} from "@nats-io/transport-node";
import type {
	ConsumeOptions,
	Consumer,
	ConsumerConfig,
	FetchOptions,
	ConsumerMessages,
	JetStreamApiCodes as JetStreamApiCode,
	JetStreamClient,
	JetStreamManager,
	JetStreamManagerOptions,
	JetStreamOptions,
	JsMsg,
	StreamConfig,
} from "@nats-io/jetstream";

type SettleState = "pending" | "settled" | "discarded";
type AddStreamConfig = Partial<StreamConfig> & Pick<StreamConfig, "name">;
type ConnectionUnsafeKey = "close" | "drain" | "subscribe" | typeof Symbol.asyncDispose;
type ConsumerUnsafeKey = "consume" | "fetch";
type ErrorClass = Function & { readonly prototype: Error; readonly name: string };

export type ConnectFn = () => Promise<NatsConnection>;
export type NatsConnectionReleaseMode = "close" | "drain";
export type NatsSubscriptionReleaseMode = "unsubscribe" | "drain";

export type StreamSubscriptionOptions = Omit<SubscriptionOptions, "callback"> & {
	callback?: never;
};

export type StreamConsumeOptions = ConsumeOptions & {
	callback?: never;
};

export type StreamFetchOptions = FetchOptions;

export type ScopedNatsConsumer = Omit<Consumer, ConsumerUnsafeKey>;

export type ScopedNatsConnection = Omit<NatsConnection, ConnectionUnsafeKey | "info"> & {
	readonly info: NatsConnection["info"];
};

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

export interface NatsSubscriptionResourceOptions {
	/**
	 * How the resource releases the subscription on scope exit.
	 *
	 * `unsubscribe` is immediate. `drain` asks NATS to flush the unsubscribe path
	 * before the iterator is returned and the subscription closure is observed.
	 */
	release?: NatsSubscriptionReleaseMode;
}

interface ScopedConnectionState {
	raw_connection: NatsConnection;
	active_subscriptions: Set<TrackedNatsSubscription>;
	active_consumer_messages: Set<TrackedConsumerMessages>;
	method_cache: Map<PropertyKey, unknown>;
}

interface ScopedConsumerState {
	raw_consumer: Consumer;
	connection_state: ScopedConnectionState | undefined;
	method_cache: Map<PropertyKey, unknown>;
}

interface TrackedNatsSubscription {
	subscription: NatsSubscription;
	iterator: AsyncIterator<Msg, void>;
	release: NatsSubscriptionReleaseMode;
	return_promise: Promise<void> | null;
	iterator_started: boolean;
	released: boolean;
}

const hidden_connection_keys = new Set<PropertyKey>(["close", "drain", "subscribe", Symbol.asyncDispose]);
const hidden_consumer_keys = new Set<PropertyKey>(["consume", "fetch"]);

const expected_connection_drain_errors = [ClosedConnectionError, DrainingConnectionError] as const;

const expected_subscription_drain_errors = [ClosedConnectionError, InvalidOperationError] as const;

const scoped_connection_states = new WeakMap<object, ScopedConnectionState>();
const scoped_consumer_states = new WeakMap<object, ScopedConsumerState>();

/**
 * Normalize an arbitrary thrown value into an Error.
 *
 * @param thrown_value - A value thrown or used to reject a promise.
 * @returns An Error instance representing the thrown value, preserving the
 * original value as `cause` when the runtime supports or accepts it.
 */
function to_error(thrown_value: unknown): Error {
	if (thrown_value instanceof Error) {
		return thrown_value;
	}
	const error = new Error(String(thrown_value));
	(error as Error & { cause?: unknown }).cause = thrown_value;
	return error;
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
 * Assert that a stream name can be used in administrative helper operations.
 *
 * @param stream_name - The JetStream stream name supplied by the caller.
 * @returns Nothing when the stream name is non-empty.
 */
function assert_stream_name(stream_name: string): void {
	A.gt(stream_name.trim().length, 0, "stream_name must be non-empty");
}

/**
 * Assert that a consumer name can be used in administrative helper operations.
 *
 * @param consumer_name - The JetStream consumer or durable name supplied by the caller.
 * @returns Nothing when the consumer name is non-empty.
 */
function assert_consumer_name(consumer_name: string): void {
	A.gt(consumer_name.trim().length, 0, "consumer_name must be non-empty");
}

/**
 * Assert that a stream creation config corresponds to the stream being ensured.
 *
 * @param stream_name - The stream name being looked up.
 * @param config - The stream creation config passed to streams.add().
 * @returns Nothing when the config is internally consistent.
 */
function assert_stream_config_matches(stream_name: string, config: AddStreamConfig): void {
	assert_stream_name(stream_name);
	A.eq(config.name, stream_name, "stream config name must match stream_name");
}

/**
 * Assert that a consumer creation config corresponds to the consumer being ensured.
 *
 * @param consumer_name - The consumer or durable name being looked up.
 * @param config - The consumer creation config passed to consumers.add().
 * @returns Nothing when any explicit name in the config matches consumer_name.
 */
function assert_consumer_config_matches(consumer_name: string, config: Partial<ConsumerConfig>): void {
	assert_consumer_name(consumer_name);
	const configured_name = typeof config.durable_name === "string" ? config.durable_name : config.name;
	if (typeof configured_name === "string") {
		A(configured_name === consumer_name, "consumer config name must match consumer_name");
	}
}

/**
 * Check whether a property is owned by the Effection resource lifecycle.
 *
 * @param property - The property key being read or inspected.
 * @returns True when exposing the property would allow manual connection or
 * subscription lifetime escape from the scoped API.
 */
function is_hidden_connection_key(property: PropertyKey): boolean {
	return hidden_connection_keys.has(property);
}

/**
 * Check whether a consumer property is owned by Effection lifecycle helpers.
 *
 * @param property - The property key being read or inspected.
 * @returns True when exposing the property would allow a long-lived JetStream
 * message iterator to escape scoped cleanup.
 */
function is_hidden_consumer_key(property: PropertyKey): boolean {
	return hidden_consumer_keys.has(property);
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
 * Build a capability facade: a Proxy that forwards safe operations to `target`
 * but hides lifecycle-owning keys so callers cannot escape Effection's scope.
 *
 * @param target - The underlying NATS object the facade protects.
 * @param is_hidden - Predicate selecting keys the facade must hide and reject.
 * @param hidden_message - Error thrown when a hidden key is read or written.
 * @param method_cache - Per-facade cache of bound methods, so a destructured
 * method keeps `target` as its `this` and stays referentially stable.
 * @returns A Proxy over an empty target mirroring `target` minus hidden keys.
 */
function make_capability_facade(
	target: object,
	is_hidden: (property: PropertyKey) => boolean,
	hidden_message: string,
	method_cache: Map<PropertyKey, unknown>,
): object {
	const proxy_target = Object.create(null) as object;
	return new Proxy(proxy_target, {
		get(_target, property) {
			if (is_hidden(property)) {
				throw new Error(hidden_message);
			}
			const value = Reflect.get(target, property, target) as unknown;
			if (typeof value !== "function") {
				return value;
			}
			if (!method_cache.has(property)) {
				method_cache.set(property, value.bind(target));
			}
			return method_cache.get(property);
		},
		has(_target, property) {
			if (is_hidden(property)) {
				return false;
			}
			return property in target;
		},
		ownKeys() {
			return Reflect.ownKeys(target).filter((property) => {
				return !is_hidden(property);
			});
		},
		getOwnPropertyDescriptor(_target, property) {
			if (is_hidden(property)) {
				return undefined;
			}
			const descriptor = Reflect.getOwnPropertyDescriptor(target, property);
			if (!descriptor) {
				return undefined;
			}
			return { ...descriptor, configurable: true };
		},
		getPrototypeOf() {
			return Reflect.getPrototypeOf(target);
		},
		set(_target, property, value) {
			if (is_hidden(property)) {
				throw new Error(hidden_message);
			}
			return Reflect.set(target, property, value, target);
		},
	});
}

/**
 * Create the public scoped connection handle.
 *
 * @param connection - The underlying NATS connection owned by the resource.
 * @returns A connection facade that forwards safe NATS operations but hides
 * manual connection lifecycle methods and raw `subscribe()`.
 */
function create_scoped_connection(connection: NatsConnection): ScopedNatsConnection {
	assert_connection_usable(connection, "scope");
	const state: ScopedConnectionState = {
		raw_connection: connection,
		active_subscriptions: new Set(),
		active_consumer_messages: new Set(),
		method_cache: new Map(),
	};
	const scoped_connection = make_capability_facade(
		connection,
		is_hidden_connection_key,
		"NATS connection lifetime is owned by Effection; use use_nats_subscription() for subscriptions",
		state.method_cache,
	) as ScopedNatsConnection;
	scoped_connection_states.set(scoped_connection, state);
	return scoped_connection;
}

/**
 * Return the scoped state for a facade, if the connection is one of ours.
 *
 * @param connection - A raw NATS connection or a scoped facade.
 * @returns The internal scoped state, or undefined for raw connections.
 */
function get_scoped_connection_state(
	connection: NatsConnection | ScopedNatsConnection,
): ScopedConnectionState | undefined {
	return scoped_connection_states.get(connection as object);
}

/**
 * Resolve a scoped facade back to the raw NATS connection it protects.
 *
 * @param connection - A raw NATS connection or one created by create_scoped_connection().
 * @returns The raw NATS connection that can be passed to NATS extension packages.
 */
function unwrap_connection(connection: NatsConnection | ScopedNatsConnection): NatsConnection {
	return get_scoped_connection_state(connection)?.raw_connection ?? (connection as NatsConnection);
}

/**
 * Create the public scoped JetStream consumer handle.
 *
 * @param consumer - The underlying JetStream consumer returned by NATS.
 * @param connection_state - The scoped connection that owns long-lived iterators
 * started from this consumer, when available.
 * @returns A consumer facade that forwards safe operations but hides `consume()`
 * and `fetch()` so ConsumerMessages are acquired through Effection resources.
 */
function create_scoped_consumer(
	consumer: Consumer,
	connection_state: ScopedConnectionState | undefined,
): ScopedNatsConsumer {
	const state: ScopedConsumerState = {
		raw_consumer: consumer,
		connection_state,
		method_cache: new Map(),
	};
	const scoped_consumer = make_capability_facade(
		consumer as object,
		is_hidden_consumer_key,
		"JetStream consumer message iterators are owned by Effection; use use_nats_consumer_messages()",
		state.method_cache,
	) as ScopedNatsConsumer;
	scoped_consumer_states.set(scoped_consumer, state);
	return scoped_consumer;
}

/**
 * Return the scoped state for a JetStream consumer facade, if it is one of ours.
 *
 * @param consumer - A raw JetStream consumer or a scoped facade.
 * @returns The internal scoped consumer state, or undefined for raw consumers.
 */
function get_scoped_consumer_state(consumer: Consumer | ScopedNatsConsumer): ScopedConsumerState | undefined {
	return scoped_consumer_states.get(consumer as object);
}

/**
 * Resolve a scoped consumer facade back to its raw JetStream consumer.
 *
 * @param consumer - A raw JetStream consumer or one created by create_scoped_consumer().
 * @returns The raw JetStream consumer that can start upstream ConsumerMessages.
 */
function unwrap_consumer(consumer: Consumer | ScopedNatsConsumer): Consumer {
	return get_scoped_consumer_state(consumer)?.raw_consumer ?? (consumer as Consumer);
}

/**
 * Assert that a scoped consumer's originating connection can start new message iterators.
 *
 * @param scoped_consumer_state - The optional scoped consumer state.
 * @param action_description - Human-readable operation used in assertion messages.
 * @returns Nothing when the backing connection is usable or unknown.
 */
function assert_scoped_consumer_usable(
	scoped_consumer_state: ScopedConsumerState | undefined,
	action_description: string,
): void {
	if (scoped_consumer_state?.connection_state) {
		assert_connection_usable(scoped_consumer_state.connection_state.raw_connection, action_description);
	}
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
 * Release a subscription according to the stream resource's release mode.
 *
 * @param subscription - The NATS subscription owned by the stream resource.
 * @param release - The release behavior to use on scope exit.
 * @returns An operation that completes when release has been requested.
 */
function* release_subscription(subscription: NatsSubscription, release: NatsSubscriptionReleaseMode): Operation<void> {
	if (subscription.isClosed()) {
		return;
	}
	if (release === "unsubscribe") {
		subscription.unsubscribe();
		return;
	}
	if (subscription.isDraining()) {
		return;
	}
	try {
		yield* until(subscription.drain());
	} catch (thrown_value) {
		if (is_error_of_kind(thrown_value, expected_subscription_drain_errors)) {
			return;
		}
		if (!subscription.isClosed()) {
			subscription.unsubscribe();
		}
		throw to_error(thrown_value);
	}
}

/**
 * Start returning a NATS async iterator exactly once.
 *
 * @param tracked_subscription - The tracked subscription whose iterator should be returned.
 * @returns A promise for iterator return, or null when the iterator has no return method.
 */
function start_iterator_return<TYield>(
	record: { iterator: AsyncIterator<TYield, void>; return_promise: Promise<void> | null },
): Promise<void> | null {
	if (record.return_promise) {
		return record.return_promise;
	}
	const return_iterator = record.iterator.return;
	if (!return_iterator) {
		return null;
	}
	record.return_promise = Promise.resolve()
		.then(() => return_iterator.call(record.iterator))
		.then(() => undefined);
	return record.return_promise;
}

/**
 * Return a NATS async iterator, waiting for its finally block to run.
 *
 * @param tracked_subscription - The tracked subscription whose iterator should be returned.
 * @returns An operation that completes after iterator return has settled.
 */
function* return_subscription_iterator(tracked_subscription: TrackedNatsSubscription): Operation<void> {
	const return_promise = start_iterator_return(tracked_subscription);
	if (return_promise) {
		yield* until(return_promise);
	}
}

/**
 * Observe the subscription's own closed promise without treating its close reason as a teardown failure.
 *
 * @param subscription - The NATS subscription whose closure should be observed.
 * @returns An operation that completes when NATS says the subscription is closed.
 */
function* wait_for_subscription_closed(subscription: NatsSubscription): Operation<void> {
	yield* until(
		subscription.closed.then(
			() => undefined,
			() => undefined,
		),
	);
}

/**
 * Release a tracked subscription once, including the async iterator cleanup path.
 *
 * @param tracked_subscription - The subscription record to release.
 * @returns An operation that completes when the subscription has been released and closed.
 */
function* release_tracked_subscription(tracked_subscription: TrackedNatsSubscription): Operation<void> {
	if (tracked_subscription.released) {
		return;
	}
	tracked_subscription.released = true;
	let release_error: Error | null = null;
	try {
		yield* release_subscription(tracked_subscription.subscription, tracked_subscription.release);
	} catch (thrown_value) {
		release_error = to_error(thrown_value);
	}
	try {
		yield* return_subscription_iterator(tracked_subscription);
	} catch (thrown_value) {
		release_error ??= to_error(thrown_value);
	}
	if (tracked_subscription.iterator_started) {
		try {
			yield* wait_for_subscription_closed(tracked_subscription.subscription);
		} catch (thrown_value) {
			release_error ??= to_error(thrown_value);
		}
	}
	if (release_error) {
		throw release_error;
	}
}

/**
 * Release every tracked item, removing each from `items`, and rethrow the first failure.
 *
 * @param items - The live set of tracked records owned by a scoped connection.
 * @param release_one - Releases a single tracked record.
 * @returns An operation that attempts every release even when some of them fail.
 */
function* release_all<T>(items: Set<T>, release_one: (item: T) => Operation<void>): Operation<void> {
	let first_error: Error | null = null;
	for (const item of [...items]) {
		try {
			yield* release_one(item);
		} catch (thrown_value) {
			first_error ??= to_error(thrown_value);
		} finally {
			items.delete(item);
		}
	}
	if (first_error) {
		throw first_error;
	}
}

/**
 * Release a scoped connection and all resources created through it.
 *
 * @param state - The scoped connection state.
 * @param release - The connection release behavior.
 * @returns An operation that releases JetStream message iterators and plain
 * subscriptions first, then the connection.
 */
function* release_scoped_connection(state: ScopedConnectionState, release: NatsConnectionReleaseMode): Operation<void> {
	let first_error: Error | null = null;
	try {
		yield* release_all(state.active_consumer_messages, release_tracked_consumer_messages);
	} catch (thrown_value) {
		first_error = to_error(thrown_value);
	}
	try {
		yield* release_all(state.active_subscriptions, release_tracked_subscription);
	} catch (thrown_value) {
		first_error ??= to_error(thrown_value);
	}
	try {
		yield* release_connection(state.raw_connection, release);
	} catch (thrown_value) {
		first_error ??= to_error(thrown_value);
	}
	if (first_error) {
		throw first_error;
	}
}

/**
 * Register a subscription under a scoped connection, if the connection is scoped.
 *
 * @param state - The optional scoped connection state.
 * @param tracked_subscription - The subscription record to register.
 * @returns A function that removes the record from the scoped connection.
 */
function register_tracked_subscription(
	state: ScopedConnectionState | undefined,
	tracked_subscription: TrackedNatsSubscription,
): () => void {
	if (!state) {
		return () => {};
	}
	state.active_subscriptions.add(tracked_subscription);
	return () => state.active_subscriptions.delete(tracked_subscription);
}

/**
 * Register ConsumerMessages under a scoped connection, if the consumer came from one.
 *
 * @param state - The optional scoped connection state.
 * @param tracked_messages - The ConsumerMessages record to register.
 * @returns A function that removes the record from the scoped connection.
 */
function register_tracked_consumer_messages(
	state: ScopedConnectionState | undefined,
	tracked_messages: TrackedConsumerMessages,
): () => void {
	if (!state) {
		return () => {};
	}
	state.active_consumer_messages.add(tracked_messages);
	return () => state.active_consumer_messages.delete(tracked_messages);
}

/**
 * Build an Effection subscription around a NATS async iterator.
 *
 * @param tracked_subscription - The tracked NATS subscription and iterator.
 * @returns An Effection subscription whose pending `next()` returns the iterator when halted.
 */
function create_effection_subscription(
	tracked_subscription: TrackedNatsSubscription,
): EffectionSubscription<Msg, void> {
	return {
		next: () =>
			action<IteratorResult<Msg, void>>((resolve, reject) => {
				let active = true;
				let settled = false;
				if (tracked_subscription.released || tracked_subscription.subscription.isClosed()) {
					settled = true;
					resolve({ done: true, value: undefined });
					return () => {};
				}
				tracked_subscription.iterator_started = true;
				Promise.resolve(tracked_subscription.iterator.next()).then(
					(result) => {
						settled = true;
						if (active) {
							resolve(result as IteratorResult<Msg, void>);
						}
					},
					(thrown_value) => {
						settled = true;
						if (active) {
							reject(to_error(thrown_value));
						}
					},
				);
				return () => {
					active = false;
					if (!settled) {
						void start_iterator_return(tracked_subscription)?.catch(() => {});
					}
				};
			}, "nats.subscription.next"),
	};
}

/**
 * Create a tracked subscription record from a raw NATS subscription.
 *
 * @param subscription - The raw NATS subscription returned by `subscribe()`.
 * @param release - How this subscription should be released.
 * @returns A tracked subscription record with one async iterator.
 */
function create_tracked_subscription(
	subscription: NatsSubscription,
	release: NatsSubscriptionReleaseMode,
): TrackedNatsSubscription {
	return {
		subscription,
		iterator: subscription[Symbol.asyncIterator](),
		release,
		return_promise: null,
		iterator_started: false,
		released: false,
	};
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
 * Open a NATS connection with @nats-io/transport-node and scope its lifetime.
 *
 * @param options - Node transport connection options forwarded to connect().
 * @param resource_options - Options controlling resource teardown behavior.
 * @returns An operation yielding a scoped connection facade. The underlying
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
 * @returns An operation yielding a scoped connection facade. The underlying
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
 * @returns An operation yielding a scoped connection facade.
 */
export function use_nats_connection(
	options_or_open: NodeConnectionOptions | ConnectFn = {},
	resource_options: NatsConnectionResourceOptions = {},
): Operation<ScopedNatsConnection> {
	return resource(function* (provide) {
		const open = create_connect_fn(options_or_open);
		const release = resource_options.release ?? "drain";
		const connection = yield* acquire_with_discard(open, close_discarded_connection, "nats.connect");
		const scoped_connection = create_scoped_connection(connection);
		const scoped_state = get_scoped_connection_state(scoped_connection);
		A(scoped_state !== undefined, "scoped connection state was not registered");
		try {
			yield* spawn(() => monitor_connection_close(connection));
			yield* provide(scoped_connection);
		} finally {
			yield* release_scoped_connection(scoped_state, release);
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
 * Ensure that a JetStream stream exists, creating it only when the server reports it missing.
 *
 * @param stream_manager - The JetStream manager used for stream administration.
 * @param stream_name - The stream name to inspect.
 * @param config - The stream creation config used only when the stream is missing.
 * @returns An operation that completes after the stream exists.
 */
export function* ensure_nats_stream(
	stream_manager: JetStreamManager,
	stream_name: string,
	config: AddStreamConfig,
): Operation<void> {
	assert_stream_config_matches(stream_name, config);
	try {
		yield* until(stream_manager.streams.info(stream_name));
	} catch (thrown_value) {
		if (!is_jetstream_api_code(thrown_value, JetStreamApiCodes.StreamNotFound)) {
			throw to_error(thrown_value);
		}
		yield* until(stream_manager.streams.add(config));
	}
}

/**
 * Ensure that a JetStream consumer exists, creating it only when the server reports it missing.
 *
 * @param stream_manager - The JetStream manager used for consumer administration.
 * @param stream_name - The stream containing the consumer.
 * @param consumer_name - The consumer or durable name to inspect.
 * @param config - The consumer creation config used only when the consumer is missing.
 * @returns An operation that completes after the consumer exists.
 */
export function* ensure_nats_consumer(
	stream_manager: JetStreamManager,
	stream_name: string,
	consumer_name: string,
	config: Partial<ConsumerConfig>,
): Operation<void> {
	assert_stream_name(stream_name);
	assert_consumer_config_matches(consumer_name, config);
	try {
		yield* until(stream_manager.consumers.info(stream_name, consumer_name));
	} catch (thrown_value) {
		if (!is_jetstream_api_code(thrown_value, JetStreamApiCodes.ConsumerNotFound)) {
			throw to_error(thrown_value);
		}
		yield* until(stream_manager.consumers.add(stream_name, config));
	}
}

/**
 * Get a JetStream consumer from a scoped or raw NATS connection.
 *
 * @param connection - A connection returned by use_nats_connection(), or a raw connection.
 * @param stream_name - The stream containing the consumer.
 * @param consumer_name - The consumer or durable name to retrieve.
 * @param options - JetStream client options accepted by @nats-io/jetstream jetstream().
 * @returns An operation yielding a scoped JetStream consumer facade.
 */
export function* get_nats_consumer(
	connection: ScopedNatsConnection | NatsConnection,
	stream_name: string,
	consumer_name: string,
	options: JetStreamOptions = {},
): Operation<ScopedNatsConsumer> {
	assert_stream_name(stream_name);
	assert_consumer_name(consumer_name);
	const scoped_state = get_scoped_connection_state(connection);
	const stream_client = nats_jetstream(connection, options);
	const consumer = yield* acquire_with_discard(
		() => stream_client.consumers.get(stream_name, consumer_name),
		() => {},
		"nats.consumer.get",
	);
	return create_scoped_consumer(consumer, scoped_state);
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
		const scoped_consumer_state = get_scoped_consumer_state(consumer);
		assert_scoped_consumer_usable(scoped_consumer_state, "consume JetStream messages");
		const raw_consumer = unwrap_consumer(consumer);
		const messages = yield* acquire_with_discard(() => raw_consumer.consume(options), close_discarded_consumer_messages, "nats.consumer.consume");
		const tracked_messages = create_tracked_consumer_messages(messages);
		const unregister_consumer_messages = register_tracked_consumer_messages(
			scoped_consumer_state?.connection_state,
			tracked_messages,
		);
		try {
			yield* provide(create_consumer_messages_subscription(tracked_messages));
		} finally {
			try {
				yield* release_tracked_consumer_messages(tracked_messages);
			} finally {
				unregister_consumer_messages();
			}
		}
	});
}

/**
 * Fetch JetStream messages as an Effection stream with scoped cleanup.
 *
 * @param consumer - The JetStream consumer returned by get_nats_consumer() or NATS directly.
 * @param options - Consumer fetch options.
 * @returns A stream of fetched JetStream messages. Leaving the consuming scope
 * closes the underlying ConsumerMessages iterator and returns its async iterator.
 */
export function use_nats_consumer_fetch(
	consumer: Consumer | ScopedNatsConsumer,
	options: StreamFetchOptions = {},
): Stream<JsMsg, void> {
	return resource(function* (provide) {
		const scoped_consumer_state = get_scoped_consumer_state(consumer);
		assert_scoped_consumer_usable(scoped_consumer_state, "fetch JetStream messages");
		const raw_consumer = unwrap_consumer(consumer);
		const messages = yield* acquire_with_discard(() => raw_consumer.fetch(options), close_discarded_consumer_messages, "nats.consumer.fetch");
		const tracked_messages = create_tracked_consumer_messages(messages);
		const unregister_consumer_messages = register_tracked_consumer_messages(
			scoped_consumer_state?.connection_state,
			tracked_messages,
		);
		try {
			yield* provide(create_consumer_messages_subscription(tracked_messages));
		} finally {
			try {
				yield* release_tracked_consumer_messages(tracked_messages);
			} finally {
				unregister_consumer_messages();
			}
		}
	});
}

/**
 * Subscribe to a NATS subject as an Effection stream with scoped cleanup.
 *
 * @param connection - A NATS connection, usually from use_nats_connection().
 * @param subject - The NATS subject to subscribe to; may include wildcards.
 * @param options - NATS subscription options, excluding callback mode.
 * @param resource_options - Options controlling subscription teardown behavior.
 * @returns A stream of NATS messages. Leaving the consuming scope unsubscribes
 * or drains the underlying NATS subscription, returns the async iterator, and
 * waits for the subscription's close promise.
 */
export function use_nats_subscription(
	connection: ScopedNatsConnection | NatsConnection,
	subject: string,
	options: StreamSubscriptionOptions = {},
	resource_options: NatsSubscriptionResourceOptions = {},
): Stream<Msg, void> {
	return resource(function* (provide) {
		A.gt(subject.trim().length, 0, "subscription subject must be non-empty");
		A.eq((options as SubscriptionOptions).callback, undefined, "callback subscriptions cannot be adapted as streams");
		const scoped_state = get_scoped_connection_state(connection);
		const raw_connection = unwrap_connection(connection);
		const release = resource_options.release ?? "unsubscribe";
		assert_connection_usable(raw_connection, "subscribe");
		const subscription = raw_connection.subscribe(subject, options);
		const tracked_subscription = create_tracked_subscription(subscription, release);
		const unregister_subscription = register_tracked_subscription(scoped_state, tracked_subscription);
		try {
			yield* provide(create_effection_subscription(tracked_subscription));
		} finally {
			try {
				yield* release_tracked_subscription(tracked_subscription);
			} finally {
				unregister_subscription();
			}
		}
	});
}
