// Slop-provider: GPT 5.5 Pro
// Slop-provider: Claude Opus 4.8
// Slop-provider: GPT 5.5 Thinking

import { beforeEach, describe, expect, it, vi } from "vitest";
import { each, run, scoped, spawn, suspend, until } from "effection";
import type { Operation } from "effection";
import {
	AckPolicy,
	JetStreamApiCodes,
	JetStreamApiError,
	jetstream,
	jetstreamManager,
} from "@nats-io/jetstream";
import type { Consumer, JetStreamClient, JetStreamManager } from "@nats-io/jetstream";
import type { NatsConnection } from "@nats-io/transport-node";

import {
	ensure_nats_consumer,
	ensure_nats_stream,
	get_nats_consumer,
	nats_jetstream,
	nats_jetstream_manager,
	use_nats_connection,
	use_nats_consumer_fetch,
	use_nats_consumer_messages,
	use_nats_subscription,
} from "./nats.ts";
import {
	as_connection,
	as_consumer,
	ClosedConnectionError,
	DrainingConnectionError,
	InvalidOperationError,
	make_fake_connection,
	make_fake_consumer,
	make_fake_consumer_messages,
	open_to,
} from "./nats_test_double.ts";

const jetstream_mocks = vi.hoisted(() => ({
	client: undefined as JetStreamClient | undefined,
	manager: undefined as JetStreamManager | undefined,
}));

vi.mock("@nats-io/jetstream", async (import_actual) => {
	const actual = await import_actual<typeof import("@nats-io/jetstream")>();
	return {
		...actual,
		jetstream: vi.fn((nc: unknown, options: unknown) => {
			return jetstream_mocks.client ?? ({ kind: "client", nc, options } as unknown as JetStreamClient);
		}),
		jetstreamManager: vi.fn(async (nc: unknown, options: unknown) => {
			return jetstream_mocks.manager ?? ({ kind: "manager", nc, options } as unknown as JetStreamManager);
		}),
	};
});

beforeEach(() => {
	vi.clearAllMocks();
	jetstream_mocks.client = undefined;
	jetstream_mocks.manager = undefined;
});

interface TestSignal {
	operation: Operation<void>;
	resolve(): void;
}

interface JetStreamManagerDouble {
	value: JetStreamManager;
	streams_info: ReturnType<typeof vi.fn>;
	streams_add: ReturnType<typeof vi.fn>;
	consumers_info: ReturnType<typeof vi.fn>;
	consumers_add: ReturnType<typeof vi.fn>;
}

/**
 * Adapt an Effection test body into a vitest async test.
 *
 * @param body - A generator producing the operation the test should run.
 * @returns An async function that runs the operation to completion in a fresh
 * Effection scope; a thrown operation rejects and fails the test.
 */
function effection_test(body: () => Operation<void>): () => Promise<void> {
	return async () => {
		await run(body);
	};
}

/**
 * Create a one-shot Effection signal for deterministic test synchronization.
 *
 * @returns A signal whose operation completes after `resolve()` is called.
 */
function create_signal(): TestSignal {
	let resolve_signal!: () => void;
	const promise = new Promise<void>((resolve) => {
		resolve_signal = resolve;
	});
	return {
		operation: until(promise),
		resolve: resolve_signal,
	};
}

/**
 * Run an operation and return the error it throws.
 *
 * @param operation - The operation expected to fail.
 * @returns The thrown Error.
 * @throws If the operation completes without throwing.
 */
function* capture_error(operation: Operation<unknown>): Operation<Error> {
	try {
		yield* operation;
	} catch (thrown_value) {
		return thrown_value as Error;
	}
	throw new Error("expected the operation to throw, but it completed");
}

/**
 * Find the position of an event in the fake connection log.
 *
 * @param events - The fake connection event log.
 * @param event - The event to find.
 * @returns The zero-based event index.
 */
function event_index(events: readonly string[], event: string): number {
	const index = events.indexOf(event);
	expect(index).toBeGreaterThanOrEqual(0);
	return index;
}

/**
 * Build a JetStream API error with the supplied NATS API error code.
 *
 * @param api_code - The JetStream API `err_code` value.
 * @param description - The error description exposed as the Error message.
 * @returns A JetStreamApiError instance matching NATS' API error shape.
 */
function create_jetstream_api_error(api_code: number, description: string): JetStreamApiError {
	return new JetStreamApiError({
		code: 404,
		err_code: api_code,
		description,
	});
}

/**
 * Build a narrow JetStreamManager test double for admin helper tests.
 *
 * @returns A manager value plus direct handles to the mocked admin methods.
 */
function create_jetstream_manager_double(): JetStreamManagerDouble {
	const streams_info   = vi.fn();
	const streams_add    = vi.fn();
	const consumers_info = vi.fn();
	const consumers_add  = vi.fn();
	const value = {
		streams: {
			info: streams_info,
			add:  streams_add,
		},
		consumers: {
			info: consumers_info,
			add:  consumers_add,
		},
	} as unknown as JetStreamManager;
	return {
		value,
		streams_info,
		streams_add,
		consumers_info,
		consumers_add,
	};
}

// ---------------------------------------------------------------------------
// Basic usage
// ---------------------------------------------------------------------------

describe("use_nats_connection (usage)", () => {
	it(
		"connects, lets you publish, and routes matching messages to a subscription",
		effection_test(function* () {
			const server = make_fake_connection();
			const received: string[] = [];
			const message_received = create_signal();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server));
				yield* spawn(function* () {
					for (const msg of yield* each(use_nats_subscription(nc, "greet.*"))) {
						received.push(`${msg.subject}=${msg.string()}`);
						message_received.resolve();
						yield* each.next();
					}
				});
				yield* until(server.wait_for((state) => state.subscriptions.length === 1));
				nc.publish("greet.world", "hello");
				nc.publish("other.topic", "ignored");
				yield* message_received.operation;
			});
			expect(received).toEqual(["greet.world=hello"]);
			expect(server.state.drain_calls).toBe(1);
			expect(server.state.close_calls).toBe(0);
		}),
	);

	it(
		"releases tracked subscriptions before the connection leaves scope",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server), { release: "close" });
				yield* spawn(function* () {
					for (const _msg of yield* each(use_nats_subscription(nc, "x.*", {}, { release: "drain" }))) {
						yield* each.next();
					}
				});
				yield* until(server.wait_for((state) => state.subscriptions.length === 1));
			});
			const events = server.state.events;
			expect(event_index(events, "subscription.drain:x.*")).toBeLessThan(event_index(events, "connection.close"));
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(1);
			expect(server.state.close_calls).toBe(1);
		}),
	);
});

// ---------------------------------------------------------------------------
// Teardown & release modes
// ---------------------------------------------------------------------------

describe("use_nats_connection (release modes)", () => {
	it(
		"drains by default",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* use_nats_connection(open_to(server));
			});
			expect(server.state.drain_calls).toBe(1);
			expect(server.state.close_calls).toBe(0);
			expect(server.isClosed()).toBe(true);
		}),
	);

	it(
		"release: 'close' closes instead of draining",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* use_nats_connection(open_to(server), { release: "close" });
			});
			expect(server.state.close_calls).toBe(1);
			expect(server.state.drain_calls).toBe(0);
			expect(server.isClosed()).toBe(true);
		}),
	);

	it(
		"release: 'drain' swallows an expected ClosedConnectionError during teardown",
		effection_test(function* () {
			const server = make_fake_connection();
			server.set_drain_error(new ClosedConnectionError());
			yield* scoped(function* () {
				yield* use_nats_connection(open_to(server), { release: "drain" });
			});
			expect(server.state.drain_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' swallows an expected DrainingConnectionError during teardown",
		effection_test(function* () {
			const server = make_fake_connection();
			server.set_drain_error(new DrainingConnectionError());
			yield* scoped(function* () {
				yield* use_nats_connection(open_to(server), { release: "drain" });
			});
			expect(server.state.drain_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' rethrows an unexpected drain error after force-closing",
		effection_test(function* () {
			const server = make_fake_connection();
			server.set_drain_error(new Error("disk on fire"));
			const error = yield* capture_error(
				scoped(function* () {
					yield* use_nats_connection(open_to(server), { release: "drain" });
				}),
			);
			expect(error.message).toMatch(/disk on fire/);
			expect(server.state.drain_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' force-closes after an unexpected drain error leaves the connection open",
		effection_test(function* () {
			const server = make_fake_connection();
			server.set_drain_error(new Error("disk still on fire"), { closes: false });
			const error = yield* capture_error(
				scoped(function* () {
					yield* use_nats_connection(open_to(server), { release: "drain" });
				}),
			);
			expect(error.message).toMatch(/disk still on fire/);
			expect(server.state.drain_calls).toBe(1);
			expect(server.state.close_calls).toBe(1);
			expect(server.isClosed()).toBe(true);
		}),
	);
});

// ---------------------------------------------------------------------------
// Failure & cancellation
// ---------------------------------------------------------------------------

describe("use_nats_connection (failure & cancellation)", () => {
	it(
		"propagates a failure from the opener",
		effection_test(function* () {
			const error = yield* capture_error(use_nats_connection(() => Promise.reject(new Error("connect refused"))));
			expect(error).toBeInstanceOf(Error);
			expect(error.message).toMatch(/connect refused/);
		}),
	);

	it(
		"propagates a synchronous failure thrown by the opener",
		effection_test(function* () {
			const error = yield* capture_error(
				use_nats_connection(() => {
					throw new Error("bad config");
				}),
			);
			expect(error.message).toMatch(/bad config/);
		}),
	);

	it(
		"crashes the owning scope when the connection closes with an error",
		effection_test(function* () {
			const server = make_fake_connection();
			const error = yield* capture_error(
				scoped(function* () {
					yield* use_nats_connection(open_to(server));
					server.fail(new Error("server vanished"));
					yield* suspend();
				}),
			);
			expect(error.message).toMatch(/server vanished/);
		}),
	);

	it(
		"closes a connection that resolves after its acquiring scope was halted",
		effection_test(function* () {
			const server = make_fake_connection();
			const connect_started = create_signal();
			let release_open!: () => void;
			const open = () => {
				connect_started.resolve();
				return new Promise<NatsConnection>((resolve) => {
					release_open = () => resolve(as_connection(server));
				});
			};
			const task = yield* spawn(function* () {
				yield* use_nats_connection(open);
				yield* suspend();
			});
			yield* connect_started.operation;
			yield* task.halt();
			expect(server.state.close_calls).toBe(0);
			release_open();
			yield* until(server.wait_for((state) => state.close_calls === 1));
			expect(server.isClosed()).toBe(true);
		}),
	);

	it(
		"ignores a rejection that arrives after its acquiring scope was halted",
		effection_test(function* () {
			const connect_started = create_signal();
			let reject_open!: (reason: Error) => void;
			const open = () => {
				connect_started.resolve();
				return new Promise<never>((_resolve, reject) => {
					reject_open = reject;
				});
			};
			const task = yield* spawn(function* () {
				yield* use_nats_connection(open);
				throw new Error("unreachable");
			});
			yield* connect_started.operation;
			yield* task.halt();
			reject_open(new Error("connect eventually failed"));
			yield* until(Promise.resolve());
			expect(true).toBe(true);
		}),
	);
});

// ---------------------------------------------------------------------------
// Scoped facade
// ---------------------------------------------------------------------------

describe("use_nats_connection (scoped facade)", () => {
	it(
		"hides close/drain/subscribe/Symbol.asyncDispose so callers cannot escape resource lifetime",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server), { release: "close" });
				const escape = nc as unknown as {
					close: () => unknown;
					drain: () => unknown;
					subscribe: () => unknown;
					[Symbol.asyncDispose]: () => unknown;
				};
				expect(() => escape.close()).toThrow(/owned by Effection/);
				expect(() => escape.drain()).toThrow(/owned by Effection/);
				expect(() => escape.subscribe()).toThrow(/use_nats_subscription/);
				expect(() => escape[Symbol.asyncDispose]()).toThrow(/owned by Effection/);
				expect("close" in nc).toBe(false);
				expect("drain" in nc).toBe(false);
				expect("subscribe" in nc).toBe(false);
				expect(server.state.close_calls).toBe(0);
			});
			expect(server.state.close_calls).toBe(1);
		}),
	);

	it(
		"returns stable bound methods so destructured connection methods still use the raw connection as this",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server), { release: "close" });
				const publish = nc.publish;
				expect(nc.publish).toBe(publish);
				publish("greet.world", "hello");
				expect(server.state.publish_receivers).toEqual([server]);
				expect(server.state.subscriptions).toEqual([]);
			});
			expect(server.state.close_calls).toBe(1);
		}),
	);
});

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

describe("use_nats_subscription", () => {
	it(
		"delivers a subscription error to the consuming loop without force-crashing the scope",
		effection_test(function* () {
			const server = make_fake_connection();
			const delivered = create_signal();
			const received: string[] = [];
			let caught: Error | null = null;
			yield* scoped(function* () {
				yield* spawn(function* () {
					yield* until(server.wait_for((state) => state.subscriptions.length === 1));
					const subscription = server.state.subscriptions[0]!;
					subscription.push("x.a", "first");
					yield* delivered.operation;
					subscription.fail(new Error("permissions violation"));
				});
				try {
					for (const msg of yield* each(use_nats_subscription(as_connection(server), "x.*"))) {
						received.push(msg.subject);
						delivered.resolve();
						yield* each.next();
					}
				} catch (error) {
					caught = error as Error;
				}
			});
			expect(received).toEqual(["x.a"]);
			expect(caught).toBeInstanceOf(Error);
			expect((caught as unknown as Error).message).toMatch(/permissions violation/);
		}),
	);

	it(
		"unsubscribes by default when the consuming scope exits",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* spawn(function* () {
					for (const _msg of yield* each(use_nats_subscription(as_connection(server), "x.*"))) {
						yield* each.next();
					}
				});
				yield* until(server.wait_for((state) => state.subscriptions.length === 1));
			});
			expect(server.state.subscriptions[0]!.state.unsub_calls).toBe(1);
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(0);
			expect(server.state.subscriptions[0]!.state.iterator_finally_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' drains the subscription instead of unsubscribing",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* use_nats_subscription(as_connection(server), "x.*", {}, { release: "drain" });
			});
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(1);
			expect(server.state.subscriptions[0]!.state.unsub_calls).toBe(0);
		}),
	);

	it(
		"release: 'drain' swallows an expected InvalidOperationError during teardown",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* use_nats_subscription(as_connection(server), "x.*", {}, { release: "drain" });
				server.state.subscriptions[0]!.set_drain_error(new InvalidOperationError("already draining"));
			});
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' swallows an expected ClosedConnectionError during teardown",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				yield* use_nats_subscription(as_connection(server), "x.*", {}, { release: "drain" });
				server.state.subscriptions[0]!.set_drain_error(new ClosedConnectionError());
			});
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(1);
		}),
	);

	it(
		"release: 'drain' unsubscribes and rethrows when an unexpected drain error leaves the subscription open",
		effection_test(function* () {
			const server = make_fake_connection();
			const error = yield* capture_error(
				scoped(function* () {
					yield* use_nats_subscription(as_connection(server), "x.*", {}, { release: "drain" });
					server.state.subscriptions[0]!.set_drain_error(new Error("bad subscription drain"), { closes: false });
				}),
			);
			expect(error.message).toMatch(/bad subscription drain/);
			expect(server.state.subscriptions[0]!.state.drain_calls).toBe(1);
			expect(server.state.subscriptions[0]!.state.unsub_calls).toBe(1);
		}),
	);

	it(
		"rejects an empty subject",
		effection_test(function* () {
			const server = make_fake_connection();
			const error = yield* capture_error(use_nats_subscription(as_connection(server), "   "));
			expect(error.message).toMatch(/non-empty/);
			expect(server.state.subscriptions).toHaveLength(0);
		}),
	);

	it(
		"rejects callback-mode options it cannot adapt as a stream",
		effection_test(function* () {
			const server = make_fake_connection();
			const options = { callback: () => {} } as unknown as Record<string, never>;
			const error = yield* capture_error(use_nats_subscription(as_connection(server), "x.*", options));
			expect(error.message).toMatch(/callback subscriptions cannot be adapted/);
			expect(server.state.subscriptions).toHaveLength(0);
		}),
	);

	it(
		"rejects new subscriptions on a draining connection",
		effection_test(function* () {
			const server = make_fake_connection();
			server.force_draining();
			const error = yield* capture_error(use_nats_subscription(as_connection(server), "x.*"));
			expect(error.message).toMatch(/draining/);
			expect(server.state.subscriptions).toHaveLength(0);
		}),
	);
});

// ---------------------------------------------------------------------------
// JetStream helpers
// ---------------------------------------------------------------------------

describe("JetStream helpers", () => {
	it(
		"nats_jetstream forwards the unwrapped raw connection to jetstream()",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server));
				const client = nats_jetstream(nc, { domain: "hub" });
				expect(vi.mocked(jetstream)).toHaveBeenCalledTimes(1);
				const [passed_nc, passed_options] = vi.mocked(jetstream).mock.calls[0]!;
				expect(passed_nc).toBe(server);
				expect(passed_options).toEqual({ domain: "hub" });
				expect(client).toMatchObject({ kind: "client" });
			});
		}),
	);

	it(
		"nats_jetstream_manager is lazy: jetstreamManager() runs only when the operation is awaited",
		effection_test(function* () {
			const server = make_fake_connection();
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server));
				const operation = nats_jetstream_manager(nc, { timeout: 1_000 });
				expect(vi.mocked(jetstreamManager)).not.toHaveBeenCalled();
				const manager = yield* operation;
				expect(vi.mocked(jetstreamManager)).toHaveBeenCalledTimes(1);
				expect(vi.mocked(jetstreamManager).mock.calls[0]![0]).toBe(server);
				expect(manager).toMatchObject({ kind: "manager" });
			});
		}),
	);

	it("nats_jetstream_manager does not create a rejected promise when the operation is constructed but never awaited", function () {
		const server = make_fake_connection();
		vi.mocked(jetstreamManager).mockRejectedValueOnce(new Error("should not start eagerly"));
		const operation = nats_jetstream_manager(as_connection(server));
		void operation;
		expect(vi.mocked(jetstreamManager)).not.toHaveBeenCalled();
	});

	it("nats_jetstream throws on an already-closed connection", function () {
		const server = make_fake_connection();
		server.force_close();
		expect(() => nats_jetstream(as_connection(server))).toThrow(/closed/);
		expect(vi.mocked(jetstream)).not.toHaveBeenCalled();
	});

	it("nats_jetstream throws on a draining connection", function () {
		const server = make_fake_connection();
		server.force_draining();
		expect(() => nats_jetstream(as_connection(server))).toThrow(/draining/);
		expect(vi.mocked(jetstream)).not.toHaveBeenCalled();
	});

	it(
		"nats_jetstream_manager throws when awaited on an already-closed connection",
		effection_test(function* () {
			const server = make_fake_connection();
			server.force_close();
			const error = yield* capture_error(nats_jetstream_manager(as_connection(server)));
			expect(error.message).toMatch(/closed/);
			expect(vi.mocked(jetstreamManager)).not.toHaveBeenCalled();
		}),
	);

	it(
		"nats_jetstream_manager throws when awaited on a draining connection",
		effection_test(function* () {
			const server = make_fake_connection();
			server.force_draining();
			const error = yield* capture_error(nats_jetstream_manager(as_connection(server)));
			expect(error.message).toMatch(/draining/);
			expect(vi.mocked(jetstreamManager)).not.toHaveBeenCalled();
		}),
	);
});

// ---------------------------------------------------------------------------
// JetStream admin helpers
// ---------------------------------------------------------------------------

describe("JetStream admin helpers", () => {
	it(
		"ensure_nats_stream returns when the stream already exists",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			manager.streams_info.mockResolvedValueOnce({ config: { name: "events" } });
			yield* ensure_nats_stream(manager.value, "events", { name: "events", subjects: ["events.>"] });
			expect(manager.streams_info).toHaveBeenCalledWith("events");
			expect(manager.streams_add).not.toHaveBeenCalled();
		}),
	);

	it(
		"ensure_nats_stream creates only when the stream is missing",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			const config = { name: "events", subjects: ["events.>"] };
			manager.streams_info.mockRejectedValueOnce(
				create_jetstream_api_error(JetStreamApiCodes.StreamNotFound, "stream not found"),
			);
			manager.streams_add.mockResolvedValueOnce({ config });
			yield* ensure_nats_stream(manager.value, "events", config);
			expect(manager.streams_add).toHaveBeenCalledWith(config);
		}),
	);

	it(
		"ensure_nats_stream does not swallow unrelated stream errors",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			manager.streams_info.mockRejectedValueOnce(new Error("permission denied"));
			const error = yield* capture_error(
				ensure_nats_stream(manager.value, "events", { name: "events", subjects: ["events.>"] }),
			);
			expect(error.message).toMatch(/permission denied/);
			expect(manager.streams_add).not.toHaveBeenCalled();
		}),
	);

	it(
		"ensure_nats_consumer returns when the consumer already exists",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			manager.consumers_info.mockResolvedValueOnce({ name: "worker" });
			yield* ensure_nats_consumer(manager.value, "events", "worker", {
				durable_name: "worker",
				ack_policy:   AckPolicy.Explicit,
			});
			expect(manager.consumers_info).toHaveBeenCalledWith("events", "worker");
			expect(manager.consumers_add).not.toHaveBeenCalled();
		}),
	);

	it(
		"ensure_nats_consumer creates only when the consumer is missing",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			const config = {
				durable_name: "worker",
				ack_policy:   AckPolicy.Explicit,
			};
			manager.consumers_info.mockRejectedValueOnce(
				create_jetstream_api_error(JetStreamApiCodes.ConsumerNotFound, "consumer not found"),
			);
			manager.consumers_add.mockResolvedValueOnce({ name: "worker" });
			yield* ensure_nats_consumer(manager.value, "events", "worker", config);
			expect(manager.consumers_add).toHaveBeenCalledWith("events", config);
		}),
	);

	it(
		"ensure_nats_consumer does not swallow unrelated consumer errors",
		effection_test(function* () {
			const manager = create_jetstream_manager_double();
			manager.consumers_info.mockRejectedValueOnce(new Error("stream missing"));
			const error = yield* capture_error(
				ensure_nats_consumer(manager.value, "events", "worker", {
					durable_name: "worker",
					ack_policy:   AckPolicy.Explicit,
				}),
			);
			expect(error.message).toMatch(/stream missing/);
			expect(manager.consumers_add).not.toHaveBeenCalled();
		}),
	);
});

// ---------------------------------------------------------------------------
// JetStream consumers
// ---------------------------------------------------------------------------

describe("JetStream consumer helpers", () => {
	it(
		"get_nats_consumer retrieves a consumer through a scoped connection",
		effection_test(function* () {
			const server = make_fake_connection();
			const consumer = as_consumer(make_fake_consumer());
			const get = vi.fn(async () => consumer);
			jetstream_mocks.client = { consumers: { get } } as unknown as JetStreamClient;
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server));
				const result = yield* get_nats_consumer(nc, "events", "worker", { domain: "hub" });
				expect(result).not.toBe(consumer);
				expect(() => (result as unknown as Consumer).consume()).toThrow(/use_nats_consumer_messages/);
				expect(() => (result as unknown as Consumer).fetch()).toThrow(/use_nats_consumer_messages/);
				expect("consume" in result).toBe(false);
				expect("fetch" in result).toBe(false);
			});
			expect(vi.mocked(jetstream)).toHaveBeenCalledTimes(1);
			expect(vi.mocked(jetstream).mock.calls[0]![0]).toBe(server);
			expect(vi.mocked(jetstream).mock.calls[0]![1]).toEqual({ domain: "hub" });
			expect(get).toHaveBeenCalledWith("events", "worker");
		}),
	);

	it(
		"releases scoped ConsumerMessages before the originating connection drains",
		effection_test(function* () {
			const server = make_fake_connection();
			const consumer = make_fake_consumer();
			const original_close = consumer.messages.close.bind(consumer.messages);
			consumer.messages.close = () => {
				server.state.events.push("consumer_messages.close");
				return original_close();
			};
			const get = vi.fn(async () => as_consumer(consumer));
			jetstream_mocks.client = { consumers: { get } } as unknown as JetStreamClient;
			yield* scoped(function* () {
				const nc = yield* use_nats_connection(open_to(server));
				const scoped_consumer = yield* get_nats_consumer(nc, "events", "worker");
				yield* spawn(function* () {
					for (const _message of yield* each(use_nats_consumer_messages(scoped_consumer))) {
						yield* each.next();
					}
				});
				yield* until(consumer.wait_for((state) => state.consume_calls === 1));
				yield* until(consumer.messages.wait_for((state) => state.next_calls === 1));
			});
			const events = server.state.events;
			expect(event_index(events, "consumer_messages.close")).toBeLessThan(event_index(events, "connection.drain"));
			expect(consumer.messages.state.close_calls).toBe(1);
			expect(server.state.drain_calls).toBe(1);
		}),
	);

	it(
		"use_nats_consumer_messages consumes messages and closes ConsumerMessages on scope exit",
		effection_test(function* () {
			const consumer = make_fake_consumer();
			const received: string[] = [];
			const message_received = create_signal();
			yield* scoped(function* () {
				yield* spawn(function* () {
					for (const message of yield* each(use_nats_consumer_messages(as_consumer(consumer), { max_messages: 1 }))) {
						received.push(message.string());
						message_received.resolve();
						yield* each.next();
					}
				});
				yield* until(consumer.wait_for((state) => state.consume_calls === 1));
				consumer.messages.push("events.created", "hello");
				yield* message_received.operation;
			});
			expect(received).toEqual(["hello"]);
			expect(consumer.state.consume_options).toEqual([{ max_messages: 1 }]);
			expect(consumer.messages.state.close_calls).toBe(1);
			expect(consumer.messages.state.iterator_finally_calls).toBe(1);
			expect(consumer.messages.state.closed).toBe(true);
		}),
	);

	it(
		"use_nats_consumer_messages closes a pending next() when the consuming task is halted",
		effection_test(function* () {
			const consumer = make_fake_consumer();
			yield* scoped(function* () {
				const task = yield* spawn(function* () {
					for (const _message of yield* each(use_nats_consumer_messages(as_consumer(consumer)))) {
						yield* each.next();
					}
				});
				yield* until(consumer.wait_for((state) => state.consume_calls === 1));
				yield* until(consumer.messages.wait_for((state) => state.next_calls === 1));
				yield* task.halt();
			});
			expect(consumer.messages.state.close_calls).toBe(1);
			expect(consumer.messages.state.iterator_finally_calls).toBe(1);
			expect(consumer.messages.state.closed).toBe(true);
		}),
	);

	it(
		"use_nats_consumer_messages closes messages that resolve after acquisition was halted",
		effection_test(function* () {
			const consumer = make_fake_consumer(make_fake_consumer_messages());
			consumer.defer_consume();
			const task = yield* spawn(function* () {
				for (const _message of yield* each(use_nats_consumer_messages(as_consumer(consumer)))) {
					yield* each.next();
				}
			});
			yield* until(consumer.wait_for((state) => state.consume_calls === 1));
			yield* task.halt();
			expect(consumer.messages.state.close_calls).toBe(0);
			consumer.resolve_consume();
			yield* until(consumer.messages.wait_for((state) => state.close_calls === 1 && state.closed));
			expect(consumer.messages.state.iterator_finally_calls).toBe(1);
		}),
	);

	it(
		"use_nats_consumer_messages rejects callback-mode consumers it cannot adapt as a stream",
		effection_test(function* () {
			const consumer = make_fake_consumer();
			const options = { callback: () => {} } as unknown as Record<string, never>;
			const error = yield* capture_error(use_nats_consumer_messages(as_consumer(consumer), options));
			expect(error.message).toMatch(/callback consumers cannot be adapted/);
			expect(consumer.state.consume_calls).toBe(0);
		}),
	);

	it(
		"use_nats_consumer_messages propagates iterator errors",
		effection_test(function* () {
			const consumer = make_fake_consumer();
			const error = yield* capture_error(
				scoped(function* () {
					yield* spawn(function* () {
						yield* until(consumer.wait_for((state) => state.consume_calls === 1));
						consumer.messages.fail(new Error("consumer stream failed"));
					});
					for (const _message of yield* each(use_nats_consumer_messages(as_consumer(consumer)))) {
						yield* each.next();
					}
				}),
			);
			expect(error.message).toMatch(/consumer stream failed/);
		}),
	);

	it(
		"use_nats_consumer_fetch fetches messages and closes ConsumerMessages on scope exit",
		effection_test(function* () {
			const consumer = make_fake_consumer();
			const received: string[] = [];
			const message_received = create_signal();
			yield* scoped(function* () {
				yield* spawn(function* () {
					for (const message of yield* each(use_nats_consumer_fetch(as_consumer(consumer), { max_messages: 1 }))) {
						received.push(message.string());
						message_received.resolve();
						yield* each.next();
					}
				});
				yield* until(consumer.wait_for((state) => state.fetch_calls === 1));
				consumer.messages.push("events.created", "hello from fetch");
				yield* message_received.operation;
			});
			expect(received).toEqual(["hello from fetch"]);
			expect(consumer.state.fetch_options).toEqual([{ max_messages: 1 }]);
			expect(consumer.messages.state.close_calls).toBe(1);
			expect(consumer.messages.state.iterator_finally_calls).toBe(1);
			expect(consumer.messages.state.closed).toBe(true);
		}),
	);

	it(
		"use_nats_consumer_fetch closes messages that resolve after acquisition was halted",
		effection_test(function* () {
			const consumer = make_fake_consumer(make_fake_consumer_messages());
			consumer.defer_fetch();
			const task = yield* spawn(function* () {
				for (const _message of yield* each(use_nats_consumer_fetch(as_consumer(consumer)))) {
					yield* each.next();
				}
			});
			yield* until(consumer.wait_for((state) => state.fetch_calls === 1));
			yield* task.halt();
			expect(consumer.messages.state.close_calls).toBe(0);
			consumer.resolve_fetch();
			yield* until(consumer.messages.wait_for((state) => state.close_calls === 1 && state.closed));
			expect(consumer.messages.state.iterator_finally_calls).toBe(1);
		}),
	);
});
