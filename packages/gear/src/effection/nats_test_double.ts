// Slop-provider: Claude Opus 4.8
// Slop-provider: GPT 5.5 Pro
// Additions: drain fault modes plus this-binding observation for facade tests.
// Slop-provider: GPT 5.5 Thinking

/**
 * In-memory NATS test doubles for unit-testing the Effection NATS resources
 * without a running `nats-server`. The double intentionally models the NATS
 * iterator-close path: unsubscribe/close request closure, but the iterator's
 * `finally`/`return()` path is what completes local subscription closure.
 */
import type { ConnectFn } from "./nats.ts";
import {
	ClosedConnectionError,
	DrainingConnectionError,
	InvalidOperationError,
	InvalidSubjectError,
} from "@nats-io/transport-node";
import type { Msg, NatsConnection, Payload, SubscriptionOptions } from "@nats-io/transport-node";
import type { ConsumeOptions, Consumer, ConsumerMessages, ConsumerNotification, FetchOptions, JsMsg } from "@nats-io/jetstream";

export { ClosedConnectionError, DrainingConnectionError, InvalidOperationError };

type QueuedSubscriptionItem = Msg | (() => void);
type QueuedConsumerMessagesItem = JsMsg | (() => void);
type FakeConnectionPredicate = (state: FakeConnectionState) => boolean;

type ResolveVoid = () => void;

interface FakeConnectionWaiter {
	predicate: FakeConnectionPredicate;
	resolve: ResolveVoid;
}

interface InternalFakeSubscription extends FakeSubscription {
	close_from_connection(error?: Error): void;
	set_connection_closed(): void;
}

export interface DrainFaultOptions {
	/** Whether a rejected drain also closes the fake resource. Defaults to true. */
	closes?: boolean;
}

export interface FakeSubscriptionState {
	sid: number;
	subject: string;
	closed: boolean;
	draining: boolean;
	unsub_calls: number;
	drain_calls: number;
	close_requests: number;
	iterator_finally_calls: number;
	drain_error: Error | null;
	drain_error_closes: boolean;
	pending_error: Error | null;
	received: number;
	processed: number;
}

export interface FakeSubscription {
	/** Promise resolving when the local iterator/subscription close path completes. */
	closed: Promise<void | Error>;
	/** Observable state for assertions. */
	state: FakeSubscriptionState;
	/** Deliver a message to an active consumer, mirroring a server publish. */
	push(subject: string, payload: Payload): void;
	/** Make the next pull throw `error`, mirroring a server-side delivery error. */
	fail(error: Error): void;
	/** Configure `drain()` to reject with `error` as a teardown/fault injection. */
	set_drain_error(error: Error, options?: DrainFaultOptions): void;
	isClosed(): boolean;
	isDraining(): boolean;
	unsubscribe(max?: number): void;
	drain(): Promise<void>;
	[Symbol.asyncDispose](): Promise<void>;
	[Symbol.asyncIterator](): AsyncIterator<Msg, void>;
	getSubject(): string;
	getPending(): number;
	getProcessed(): number;
	getReceived(): number;
	getID(): number;
	getMax(): number | undefined;
	callback(error: Error | null, msg: Msg): void;
}

export interface FakeConnectionState {
	close_calls: number;
	drain_calls: number;
	flush_calls: number;
	closed: boolean;
	draining: boolean;
	no_more_publishing: boolean;
	drain_error: Error | null;
	drain_error_closes: boolean;
	events: string[];
	publish_receivers: unknown[];
	subscriptions: FakeSubscription[];
}

export interface FakeConnection {
	/** Observable state for assertions. */
	state: FakeConnectionState;
	options: { timeout: number; inboxPrefix: string };
	/** Close synchronously without incrementing close calls, for test setup. */
	force_close(): void;
	/** Put the connection into a draining state without closing, for test setup. */
	force_draining(): void;
	/** Resolve `closed()` with an Error, mirroring an unrecoverable close. */
	fail(error: Error): void;
	/** Configure `drain()` to reject with `error` as a teardown/fault injection. */
	set_drain_error(error: Error, options?: DrainFaultOptions): void;
	/** Resolve once `predicate` is true for the fake connection state. */
	wait_for(predicate: FakeConnectionPredicate): Promise<void>;
	isClosed(): boolean;
	isDraining(): boolean;
	closed(): Promise<void | Error>;
	close(): Promise<void>;
	drain(): Promise<void>;
	flush(): Promise<void>;
	subscribe(subject: string, options?: SubscriptionOptions): FakeSubscription;
	publish(subject: string, payload?: Payload): void;
}

export type FakeConsumerMessagesPredicate = (state: FakeConsumerMessagesState) => boolean;
export type FakeConsumerPredicate = (state: FakeConsumerState) => boolean;

interface FakeConsumerMessagesWaiter {
	predicate: FakeConsumerMessagesPredicate;
	resolve: ResolveVoid;
}

interface FakeConsumerWaiter {
	predicate: FakeConsumerPredicate;
	resolve: ResolveVoid;
}

export interface FakeJsMsgState {
	ack_calls: number;
	nak_calls: number;
	term_calls: number;
	working_calls: number;
}

export interface FakeConsumerMessagesState {
	closed: boolean;
	close_calls: number;
	stop_calls: number;
	iterator_finally_calls: number;
	next_calls: number;
	received: number;
	processed: number;
	pending_error: Error | null;
	events: string[];
}

export interface FakeConsumerMessagesOptions {
	/** Optional shared ordered event log for cross-resource lifecycle assertions. */
	events?: string[];
}

export interface FakeConsumerMessages {
	/** Observable state for assertions. */
	state: FakeConsumerMessagesState;
	/** Push one message into the ConsumerMessages iterator. */
	push(subject: string, payload: Payload): JsMsg;
	/** Fail the iterator with a terminal error. */
	fail(error: Error): void;
	/** Resolve once `predicate` is true for the fake messages state. */
	wait_for(predicate: FakeConsumerMessagesPredicate): Promise<void>;
	close(): Promise<void | Error>;
	closed(): Promise<void | Error>;
	status(): AsyncIterable<ConsumerNotification>;
	stop(error?: Error): void;
	getProcessed(): number;
	getPending(): number;
	getReceived(): number;
	[Symbol.asyncIterator](): AsyncIterator<JsMsg, void>;
}

export interface FakeConsumerState {
	consume_calls: number;
	consume_options: ConsumeOptions[];
	next_consume_deferred: boolean;
	fetch_calls: number;
	fetch_options: FetchOptions[];
	next_fetch_deferred: boolean;
}

export interface FakeConsumer {
	/** Observable state for assertions. */
	state: FakeConsumerState;
	/** Default messages returned by consume() when not explicitly resolved with another instance. */
	messages: FakeConsumerMessages;
	/** Make the next consume() call wait for resolve_consume() or reject_consume(). */
	defer_consume(): void;
	/** Resolve a deferred consume() call. */
	resolve_consume(messages?: FakeConsumerMessages): void;
	/** Reject a deferred consume() call. */
	reject_consume(error: Error): void;
	/** Make the next fetch() call wait for resolve_fetch() or reject_fetch(). */
	defer_fetch(): void;
	/** Resolve a deferred fetch() call. */
	resolve_fetch(messages?: FakeConsumerMessages): void;
	/** Reject a deferred fetch() call. */
	reject_fetch(error: Error): void;
	/** Resolve once `predicate` is true for the fake consumer state. */
	wait_for(predicate: FakeConsumerPredicate): Promise<void>;
	consume(options?: ConsumeOptions): Promise<ConsumerMessages>;
	fetch(options?: FetchOptions): Promise<ConsumerMessages>;
}

const whitespace_regex = /\s/;

/**
 * Return a copy of a payload as bytes.
 *
 * @param payload - A NATS payload represented as a string or bytes.
 * @returns Bytes containing the payload data.
 */
function payload_to_bytes(payload: Payload): Uint8Array {
	if (typeof payload === "string") {
		return new TextEncoder().encode(payload);
	}
	return new Uint8Array(payload);
}

/**
 * Return a payload as a UTF-8 string.
 *
 * @param payload - A NATS payload represented as a string or bytes.
 * @returns The string representation used by `Msg.string()` and `Msg.json()`.
 */
function payload_to_string(payload: Payload): string {
	if (typeof payload === "string") {
		return payload;
	}
	return new TextDecoder().decode(payload);
}

/**
 * Assert that a subject is non-empty and has no whitespace.
 *
 * @param subject - A concrete or wildcard NATS subject.
 * @returns Nothing when the subject passes the client-side checks.
 */
function assert_subject_base(subject: string): void {
	if (subject.length === 0 || whitespace_regex.test(subject)) {
		throw new InvalidSubjectError(subject);
	}
}

/**
 * Assert that a subscription subject uses only legal wildcard tokens.
 *
 * @param subject - A subscription subject, possibly containing `*` or `>`.
 * @returns Nothing when the subscription subject can be matched by the fake.
 */
function assert_subscription_subject(subject: string): void {
	assert_subject_base(subject);
	const tokens = subject.split(".");
	for (let index = 0; index < tokens.length; index++) {
		const token = tokens[index]!;
		if (token.length === 0) {
			throw new InvalidSubjectError(subject);
		}
		if (token.includes("*") && token !== "*") {
			throw new InvalidSubjectError(subject);
		}
		if (token.includes(">") && (token !== ">" || index !== tokens.length - 1)) {
			throw new InvalidSubjectError(subject);
		}
	}
}

/**
 * Assert that a publish subject has no wildcard tokens.
 *
 * @param subject - A concrete published subject.
 * @returns Nothing when the publish subject can be sent.
 */
function assert_publish_subject(subject: string): void {
	assert_subject_base(subject);
	if (subject.split(".").some((token) => token === "*" || token === ">")) {
		throw new InvalidSubjectError(subject);
	}
}

/**
 * Match a published subject against a NATS subscription pattern.
 *
 * @param pattern - A subscription subject, possibly containing `*` or `>` wildcards.
 * @param subject - A concrete published subject.
 * @returns True when a message on `subject` is delivered to `pattern`.
 */
function subject_matches(pattern: string, subject: string): boolean {
	assert_subscription_subject(pattern);
	assert_publish_subject(subject);
	const pattern_tokens = pattern.split(".");
	const subject_tokens = subject.split(".");
	for (let index = 0; index < pattern_tokens.length; index++) {
		const token = pattern_tokens[index]!;
		if (token === ">") {
			return subject_tokens.length > index;
		}
		if (index >= subject_tokens.length) {
			return false;
		}
		if (token !== "*" && token !== subject_tokens[index]) {
			return false;
		}
	}
	return pattern_tokens.length === subject_tokens.length;
}

/**
 * Build a minimal NATS message double.
 *
 * @param subject - The subject the message was published to.
 * @param payload - The payload carried by the message.
 * @returns A `Msg`-shaped object exposing the fields and decoders the tests use.
 */
function make_msg(subject: string, payload: Payload): Msg {
	const payload_string = payload_to_string(payload);
	const msg = {
		subject,
		sid: 0,
		data: payload_to_bytes(payload),
		string: () => payload_string,
		json: () => JSON.parse(payload_string),
		respond: () => true,
	};
	return msg as unknown as Msg;
}

/**
 * Build a minimal JetStream message double.
 *
 * @param subject - The subject the message was published to.
 * @param payload - The payload carried by the message.
 * @returns A `JsMsg`-shaped object exposing fields and ack methods used by tests.
 */
function make_js_msg(subject: string, payload: Payload): JsMsg {
	const payload_string = payload_to_string(payload);
	const timestamp = new Date();
	const state: FakeJsMsgState = {
		ack_calls: 0,
		nak_calls: 0,
		term_calls: 0,
		working_calls: 0,
	};
	return {
		redelivered: false,
		info: {
			domain: "",
			account_hash: "",
			stream: "stream",
			consumer: "consumer",
			deliveryCount: 1,
			redelivered: false,
			streamSequence: 1,
			deliverySequence: 1,
			timestampNanos: Number(timestamp.getTime()) * 1_000_000,
			pending: 0,
		},
		seq: 1,
		headers: undefined,
		data: payload_to_bytes(payload),
		subject,
		sid: 0,
		time: timestamp,
		timestamp: timestamp.toISOString(),
		timestampNanos: BigInt(timestamp.getTime()) * 1_000_000n,
		ack: () => {
			state.ack_calls += 1;
		},
		nak: () => {
			state.nak_calls += 1;
		},
		working: () => {
			state.working_calls += 1;
		},
		next: () => {},
		term: () => {
			state.term_calls += 1;
		},
		ackAck: () => Promise.resolve(true),
		json: () => JSON.parse(payload_string),
		string: () => payload_string,
		state,
	} as JsMsg & { state: FakeJsMsgState };
}

/**
 * Resolve all state waiters whose predicates are now true.
 *
 * @param state - The fake connection state observed by waiters.
 * @param waiters - The mutable waiter set to drain.
 * @returns Nothing.
 */
function notify_waiters(state: FakeConnectionState, waiters: Set<FakeConnectionWaiter>): void {
	for (const waiter of [...waiters]) {
		if (waiter.predicate(state)) {
			waiters.delete(waiter);
			waiter.resolve();
		}
	}
}

/**
 * Resolve all ConsumerMessages waiters whose predicates are now true.
 *
 * @param state - The fake messages state observed by waiters.
 * @param waiters - The mutable waiter set to drain.
 * @returns Nothing.
 */
function notify_consumer_messages_waiters(
	state: FakeConsumerMessagesState,
	waiters: Set<FakeConsumerMessagesWaiter>,
): void {
	for (const waiter of [...waiters]) {
		if (waiter.predicate(state)) {
			waiters.delete(waiter);
			waiter.resolve();
		}
	}
}

/**
 * Resolve all fake consumer waiters whose predicates are now true.
 *
 * @param state - The fake consumer state observed by waiters.
 * @param waiters - The mutable waiter set to drain.
 * @returns Nothing.
 */
function notify_consumer_waiters(state: FakeConsumerState, waiters: Set<FakeConsumerWaiter>): void {
	for (const waiter of [...waiters]) {
		if (waiter.predicate(state)) {
			waiters.delete(waiter);
			waiter.resolve();
		}
	}
}

/**
 * Build an event emitter tied to fake ConsumerMessages waiters.
 *
 * @param state - The fake messages state to mutate.
 * @param waiters - Waiters to notify after every event.
 * @returns A function that records one event and resolves matching waiters.
 */
function create_consumer_messages_event_emitter(
	state: FakeConsumerMessagesState,
	waiters: Set<FakeConsumerMessagesWaiter>,
): (event: string) => void {
	return (event: string) => {
		state.events.push(event);
		notify_consumer_messages_waiters(state, waiters);
	};
}

/**
 * Build an event emitter tied to fake connection waiters.
 *
 * @param state - The fake connection state to mutate.
 * @param waiters - Waiters to notify after every event.
 * @returns A function that records one event and resolves matching waiters.
 */
function create_event_emitter(state: FakeConnectionState, waiters: Set<FakeConnectionWaiter>): (event: string) => void {
	return (event: string) => {
		state.events.push(event);
		notify_waiters(state, waiters);
	};
}

/**
 * Build a fake JetStream ConsumerMessages iterator backed by an in-memory queue.
 *
 * @returns ConsumerMessages-shaped test double with deterministic close and wait hooks.
 */
export function make_fake_consumer_messages(options: FakeConsumerMessagesOptions = {}): FakeConsumerMessages {
	const queue: QueuedConsumerMessagesItem[] = [];
	const waiters = new Set<FakeConsumerMessagesWaiter>();
	let waiting: (() => void) | null = null;
	let iterator_active = false;
	let iterator_broke = false;
	let resolve_closed!: (reason: void | Error) => void;
	const closed_promise = new Promise<void | Error>((resolve) => {
		resolve_closed = resolve;
	});
	const state: FakeConsumerMessagesState = {
		closed: false,
		close_calls: 0,
		stop_calls: 0,
		iterator_finally_calls: 0,
		next_calls: 0,
		received: 0,
		processed: 0,
		pending_error: null,
		events: options.events ?? [],
	};
	const emit = create_consumer_messages_event_emitter(state, waiters);
	const wake = () => {
		if (waiting) {
			const resume = waiting;
			waiting = null;
			resume();
		}
	};
	const stop = (error?: Error) => {
		state.stop_calls += 1;
		if (state.closed) {
			notify_consumer_messages_waiters(state, waiters);
			return;
		}
		state.closed = true;
		if (error) {
			state.pending_error = error;
		}
		emit("consumer_messages.closed");
		resolve_closed(error);
		wake();
	};
	const push_control = (fn: () => void) => {
		if (iterator_broke) {
			fn();
			return;
		}
		queue.push(fn);
		wake();
	};
	async function* iterate(): AsyncGenerator<JsMsg, void, unknown> {
		state.next_calls += 1;
		notify_consumer_messages_waiters(state, waiters);
		if (iterator_active) {
			throw new InvalidOperationError("iterator is already yielding");
		}
		iterator_active = true;
		let terminal_error: Error | undefined;
		try {
			while (true) {
				if (queue.length === 0) {
					if (state.pending_error) {
						terminal_error = state.pending_error;
						throw terminal_error;
					}
					if (state.closed) {
						return;
					}
					await new Promise<void>((resolve) => {
						waiting = resolve;
					});
				}
				if (state.pending_error) {
					terminal_error = state.pending_error;
					throw terminal_error;
				}
				const items = queue.splice(0, queue.length);
				for (const item of items) {
					if (typeof item === "function") {
						item();
						if (state.pending_error) {
							terminal_error = state.pending_error;
							throw terminal_error;
						}
						continue;
					}
					state.processed += 1;
					emit(`consumer_messages.next:${item.subject}`);
					yield item;
				}
				if (state.closed) {
					return;
				}
			}
		} finally {
			iterator_broke = true;
			state.iterator_finally_calls += 1;
			stop(terminal_error);
		}
	}
	// oxlint-disable-next-line eslint/require-yield unicorn/consistent-function-scoping
	async function* status(): AsyncGenerator<ConsumerNotification, void, unknown> {
		return;
	}
	const messages: FakeConsumerMessages = {
		state,
		push: (subject, payload) => {
			if (state.closed) {
				throw new InvalidOperationError("consumer messages are closed");
			}
			const message = make_js_msg(subject, payload);
			state.received += 1;
			queue.push(message);
			emit(`consumer_messages.push:${subject}`);
			wake();
			return message;
		},
		fail: (error) => {
			state.pending_error = error;
			emit("consumer_messages.fail");
			wake();
		},
		wait_for: (predicate) => {
			if (predicate(state)) {
				return Promise.resolve();
			}
			return new Promise<void>((resolve) => {
				waiters.add({ predicate, resolve });
			});
		},
		close: async () => {
			state.close_calls += 1;
			emit("consumer_messages.close");
			push_control(() => stop());
			return closed_promise;
		},
		closed: () => closed_promise,
		status,
		stop,
		getProcessed: () => state.processed,
		getPending: () => queue.filter((item) => typeof item !== "function").length,
		getReceived: () => state.received,
		[Symbol.asyncIterator]: iterate,
	};
	return messages;
}

interface DeferredConsumerMessages {
	resolve(messages: ConsumerMessages): void;
	reject(error: Error): void;
}

/**
 * Build a fake JetStream consumer whose consume() returns fake ConsumerMessages.
 *
 * @param messages - The default ConsumerMessages instance returned by consume().
 * @returns Consumer-shaped test double with controllable consume() resolution.
 */
export function make_fake_consumer(messages: FakeConsumerMessages = make_fake_consumer_messages()): FakeConsumer {
	let deferred_consume: DeferredConsumerMessages | null = null;
	let deferred_fetch: DeferredConsumerMessages | null = null;
	const waiters = new Set<FakeConsumerWaiter>();
	const state: FakeConsumerState = {
		consume_calls: 0,
		consume_options: [],
		next_consume_deferred: false,
		fetch_calls: 0,
		fetch_options: [],
		next_fetch_deferred: false,
	};
	const consumer: FakeConsumer = {
		state,
		messages,
		defer_consume: () => {
			state.next_consume_deferred = true;
			notify_consumer_waiters(state, waiters);
		},
		resolve_consume: (resolved_messages = messages) => {
			if (!deferred_consume) {
				throw new Error("no deferred consume() call is pending");
			}
			const deferred = deferred_consume;
			deferred_consume = null;
			deferred.resolve(resolved_messages as unknown as ConsumerMessages);
			notify_consumer_waiters(state, waiters);
		},
		reject_consume: (error) => {
			if (!deferred_consume) {
				throw new Error("no deferred consume() call is pending");
			}
			const deferred = deferred_consume;
			deferred_consume = null;
			deferred.reject(error);
			notify_consumer_waiters(state, waiters);
		},
		defer_fetch: () => {
			state.next_fetch_deferred = true;
			notify_consumer_waiters(state, waiters);
		},
		resolve_fetch: (resolved_messages = messages) => {
			if (!deferred_fetch) {
				throw new Error("no deferred fetch() call is pending");
			}
			const deferred = deferred_fetch;
			deferred_fetch = null;
			deferred.resolve(resolved_messages as unknown as ConsumerMessages);
			notify_consumer_waiters(state, waiters);
		},
		reject_fetch: (error) => {
			if (!deferred_fetch) {
				throw new Error("no deferred fetch() call is pending");
			}
			const deferred = deferred_fetch;
			deferred_fetch = null;
			deferred.reject(error);
			notify_consumer_waiters(state, waiters);
		},
		wait_for: (predicate) => {
			if (predicate(state)) {
				return Promise.resolve();
			}
			return new Promise<void>((resolve) => {
				waiters.add({ predicate, resolve });
			});
		},
		consume: (options = {}) => {
			state.consume_calls += 1;
			state.consume_options.push(options);
			notify_consumer_waiters(state, waiters);
			if (!state.next_consume_deferred) {
				return Promise.resolve(messages as unknown as ConsumerMessages);
			}
			state.next_consume_deferred = false;
			notify_consumer_waiters(state, waiters);
			return new Promise<ConsumerMessages>((resolve, reject) => {
				deferred_consume = { resolve, reject };
			});
		},
		fetch: (options = {}) => {
			state.fetch_calls += 1;
			state.fetch_options.push(options);
			notify_consumer_waiters(state, waiters);
			if (!state.next_fetch_deferred) {
				return Promise.resolve(messages as unknown as ConsumerMessages);
			}
			state.next_fetch_deferred = false;
			notify_consumer_waiters(state, waiters);
			return new Promise<ConsumerMessages>((resolve, reject) => {
				deferred_fetch = { resolve, reject };
			});
		},
	};
	return consumer;
}

/**
 * Build a single fake subscription backed by an in-memory queue.
 *
 * @param sid - The fake subscription id.
 * @param subject - The subject this subscription expressed interest in.
 * @param is_connection_closed - Function returning whether the parent connection is closed.
 * @param emit - Event emitter for ordered lifecycle assertions.
 * @returns A subscription double whose async iterator yields pushed messages.
 */
function make_fake_subscription(
	sid: number,
	subject: string,
	is_connection_closed: () => boolean,
	emit: (event: string) => void,
): InternalFakeSubscription {
	const queue: QueuedSubscriptionItem[] = [];
	let waiting: (() => void) | null = null;
	let iterator_active = false;
	let iterator_broke = false;
	let max_messages: number | undefined;
	let resolve_closed!: (reason: void | Error) => void;
	const closed_promise = new Promise<void | Error>((resolve) => {
		resolve_closed = resolve;
	});
	const state: FakeSubscriptionState = {
		sid,
		subject,
		closed: false,
		draining: false,
		unsub_calls: 0,
		drain_calls: 0,
		close_requests: 0,
		iterator_finally_calls: 0,
		drain_error: null,
		drain_error_closes: true,
		pending_error: null,
		received: 0,
		processed: 0,
	};
	const wake = () => {
		if (waiting) {
			const resume = waiting;
			waiting = null;
			resume();
		}
	};
	const stop = (error?: Error) => {
		if (state.closed) {
			return;
		}
		state.closed = true;
		if (error) {
			state.pending_error = error;
		}
		emit(`subscription.closed:${subject}`);
		resolve_closed(error);
		wake();
	};
	const push_control = (fn: () => void) => {
		if (iterator_broke) {
			fn();
			return;
		}
		queue.push(fn);
		wake();
	};
	const request_close = (error?: Error) => {
		state.close_requests += 1;
		push_control(() => stop(error));
	};
	async function* iterate(): AsyncGenerator<Msg, void, unknown> {
		if (iterator_active) {
			throw new InvalidOperationError("iterator is already yielding");
		}
		iterator_active = true;
		let terminal_error: Error | undefined;
		try {
			while (true) {
				if (queue.length === 0) {
					if (state.pending_error) {
						terminal_error = state.pending_error;
						throw terminal_error;
					}
					if (state.closed) {
						return;
					}
					await new Promise<void>((resolve) => {
						waiting = resolve;
					});
				}
				if (state.pending_error) {
					terminal_error = state.pending_error;
					throw terminal_error;
				}
				const items = queue.splice(0, queue.length);
				for (const item of items) {
					if (typeof item === "function") {
						item();
						if (state.pending_error) {
							terminal_error = state.pending_error;
							throw terminal_error;
						}
						continue;
					}
					state.processed += 1;
					emit(`subscription.next:${subject}:${item.subject}`);
					yield item;
				}
				if (state.closed) {
					return;
				}
			}
		} finally {
			iterator_broke = true;
			state.iterator_finally_calls += 1;
			stop(terminal_error);
		}
	}
	const subscription: InternalFakeSubscription = {
		closed: closed_promise,
		state,
		push: (subject_in, payload) => {
			if (state.closed || state.draining) {
				return;
			}
			state.received += 1;
			queue.push(make_msg(subject_in, payload));
			emit(`subscription.push:${subject}:${subject_in}`);
			wake();
		},
		fail: (error) => {
			state.pending_error = error;
			emit(`subscription.fail:${subject}`);
			wake();
		},
		set_drain_error: (error, options = {}) => {
			state.drain_error = error;
			state.drain_error_closes = options.closes ?? true;
		},
		isClosed: () => state.closed,
		isDraining: () => state.draining,
		unsubscribe: (max) => {
			state.unsub_calls += 1;
			max_messages = max;
			emit(`subscription.unsubscribe:${subject}`);
			if (max === undefined || state.received >= max) {
				request_close();
			}
		},
		drain: async () => {
			state.drain_calls += 1;
			emit(`subscription.drain:${subject}`);
			if (is_connection_closed()) {
				throw new ClosedConnectionError();
			}
			if (state.closed) {
				throw new InvalidOperationError("subscription is already closed");
			}
			if (state.drain_error) {
				if (state.drain_error_closes) {
					request_close(state.drain_error);
				}
				throw state.drain_error;
			}
			state.draining = true;
			request_close();
		},
		[Symbol.asyncDispose]: async () => {
			if (state.closed) {
				return;
			}
			if (state.draining) {
				await closed_promise;
				return;
			}
			await subscription.drain();
		},
		[Symbol.asyncIterator]: iterate,
		getSubject: () => subject,
		getPending: () => queue.filter((item) => typeof item !== "function").length,
		getProcessed: () => state.processed,
		getReceived: () => state.received,
		getID: () => sid,
		getMax: () => max_messages,
		callback: (error, msg) => {
			if (error) {
				subscription.fail(error);
				return;
			}
			subscription.push(msg.subject, msg.data);
		},
		close_from_connection: (error) => {
			emit(`subscription.connection_close:${subject}`);
			request_close(error);
		},
		set_connection_closed: () => {
			state.draining = false;
		},
	};
	return subscription;
}

/**
 * Build a fake NATS connection backed by in-memory subscriptions.
 *
 * @returns A connection double that routes `publish` to matching subscriptions,
 * records ordered lifecycle events, and exposes deterministic wait hooks.
 */
export function make_fake_connection(): FakeConnection {
	let resolve_closed!: (reason: void | Error) => void;
	const closed_promise = new Promise<void | Error>((resolve) => {
		resolve_closed = resolve;
	});
	const waiters = new Set<FakeConnectionWaiter>();
	const state: FakeConnectionState = {
		close_calls: 0,
		drain_calls: 0,
		flush_calls: 0,
		closed: false,
		draining: false,
		no_more_publishing: false,
		drain_error: null,
		drain_error_closes: true,
		events: [],
		publish_receivers: [],
		subscriptions: [],
	};
	const emit = create_event_emitter(state, waiters);
	const settle_closed = (reason: void | Error) => {
		if (state.closed) {
			return;
		}
		state.closed = true;
		state.draining = false;
		state.no_more_publishing = true;
		for (const subscription of state.subscriptions as InternalFakeSubscription[]) {
			subscription.close_from_connection(reason instanceof Error ? reason : undefined);
			subscription.set_connection_closed();
		}
		emit("connection.closed");
		resolve_closed(reason);
	};
	const assert_connection_open = (for_subscription: boolean, for_publish: boolean) => {
		if (state.closed) {
			throw new ClosedConnectionError();
		}
		if (for_subscription && state.draining) {
			throw new DrainingConnectionError();
		}
		if (for_publish && state.no_more_publishing) {
			throw new DrainingConnectionError();
		}
	};
	const connection: FakeConnection = {
		state,
		options: { timeout: 5_000, inboxPrefix: "_INBOX." },
		force_close: () => settle_closed(undefined),
		force_draining: () => {
			if (!state.closed) {
				state.draining = true;
				emit("connection.force_draining");
			}
		},
		fail: (error) => settle_closed(error),
		set_drain_error: (error, options = {}) => {
			state.drain_error = error;
			state.drain_error_closes = options.closes ?? true;
		},
		wait_for: (predicate) => {
			if (predicate(state)) {
				return Promise.resolve();
			}
			return new Promise<void>((resolve) => {
				waiters.add({ predicate, resolve });
			});
		},
		isClosed: () => state.closed,
		isDraining: () => state.draining,
		closed: () => closed_promise,
		close: async () => {
			state.close_calls += 1;
			emit("connection.close");
			settle_closed(undefined);
		},
		drain: async () => {
			state.drain_calls += 1;
			emit("connection.drain");
			if (state.closed) {
				throw new ClosedConnectionError();
			}
			if (state.draining) {
				throw new DrainingConnectionError();
			}
			if (state.drain_error) {
				if (state.drain_error_closes) {
					settle_closed(undefined);
				}
				throw state.drain_error;
			}
			state.draining = true;
			state.no_more_publishing = true;
			for (const subscription of state.subscriptions) {
				if (!subscription.isClosed() && !subscription.isDraining()) {
					await subscription.drain().catch(() => {});
				}
			}
			await connection.flush();
			settle_closed(undefined);
		},
		flush: async () => {
			state.flush_calls += 1;
			emit("connection.flush");
		},
		subscribe: (subject) => {
			assert_connection_open(true, false);
			assert_subscription_subject(subject);
			const subscription = make_fake_subscription(state.subscriptions.length + 1, subject, () => state.closed, emit);
			state.subscriptions.push(subscription);
			emit(`connection.subscribe:${subject}`);
			return subscription;
		},
		publish(subject, payload = "") {
			assert_connection_open(false, true);
			assert_publish_subject(subject);
			state.publish_receivers.push(this);
			emit(`connection.publish:${subject}`);
			for (const subscription of state.subscriptions) {
				if (
					!subscription.isClosed() &&
					!subscription.isDraining() &&
					subject_matches(subscription.state.subject, subject)
				) {
					subscription.push(subject, payload);
				}
			}
		},
	};
	return connection;
}

/**
 * Adapt a fake connection into a `ConnectFn` for the factory overload.
 *
 * @param connection - The fake connection one `use_nats_connection` call should yield.
 * @returns An opener that resolves immediately to `connection`.
 */
export function open_to(connection: FakeConnection): ConnectFn {
	return () => Promise.resolve(connection as unknown as NatsConnection);
}

/**
 * Build a `ConnectFn` from a fake connection factory.
 *
 * @param create_connection - A nullary factory that returns a fresh fake connection.
 * @returns An opener that resolves to a new fake connection for each evaluation.
 */
export function open_with(create_connection: () => FakeConnection): ConnectFn {
	return () => Promise.resolve(create_connection() as unknown as NatsConnection);
}

/**
 * View a fake connection as a `NatsConnection` for APIs taking one directly.
 *
 * @param connection - The fake connection to pass to e.g. `nats_jetstream`.
 * @returns The same object typed as `NatsConnection`.
 */
export function as_connection(connection: FakeConnection): NatsConnection {
	return connection as unknown as NatsConnection;
}

/**
 * View a fake JetStream consumer as a `Consumer` for APIs taking one directly.
 *
 * @param consumer - The fake consumer to pass to JetStream helpers.
 * @returns The same object typed as `Consumer`.
 */
export function as_consumer(consumer: FakeConsumer): Consumer {
	return consumer as unknown as Consumer;
}
