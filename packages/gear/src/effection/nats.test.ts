// Model-output: Claude Fable 5

/**
 * Integration tests for the Effection NATS resources, run against a real
 * `nats-server` with JetStream enabled, started on a random port with a
 * temp-directory store. A real server is deliberate: the teardown logic in
 * nats.ts depends on undocumented @nats-io/* iterator internals (close
 * callbacks queued onto the message iterator), and a test double that imitated
 * those internals would keep passing if the library changed them.
 */

import { spawn as spawn_child } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { run, sleep, spawn, suspend, until } from "effection";
import { AckPolicy, DeliverPolicy } from "@nats-io/jetstream";
import { connect as node_connect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/transport-node";

import {
	ensure_durable_nats_consumer,
	ensure_nats_stream,
	get_nats_consumer,
	nats_jetstream,
	nats_jetstream_manager,
	use_nats_connection,
	use_nats_consumer_messages,
	use_ordered_nats_consumer,
} from "./nats.ts";

interface TestNatsServer {
	url: string;
	stop(): Promise<void>;
}

/**
 * Start a nats-server with JetStream on a random 127.0.0.1 port, storing all
 * state under a fresh temp directory. The port is discovered through the
 * server's --ports_file_dir mechanism rather than picked by us, so it can
 * never collide with the production NATS on this machine.
 *
 * @returns The client URL and an idempotent stop() that kills the server and
 * removes its temp directory.
 */
async function start_nats_server(): Promise<TestNatsServer> {
	const directory = await mkdtemp(join(tmpdir(), "sophon-nats-test-"));
	const child: ChildProcess = spawn_child(
		"nats-server",
		["-a", "127.0.0.1", "-p", "-1", "-js", "-sd", join(directory, "jetstream"), "--ports_file_dir", directory],
		{ stdio: ["ignore", "ignore", "pipe"] },
	);
	let stderr = "";
	child.stderr?.on("data", (chunk: Buffer) => {
		stderr += chunk.toString();
	});
	// Created once at spawn time: after a kill by signal, exitCode stays null
	// (only signalCode is set), so an exitCode check cannot tell "already
	// exited" from "still running".
	const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
	const ports_path = join(directory, `nats-server_${child.pid}.ports`);
	let url: string | undefined;
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline && child.exitCode === null && child.signalCode === null) {
		try {
			const ports = JSON.parse(await readFile(ports_path, "utf8")) as { nats?: string[] };
			url = ports.nats?.[0];
			if (url) {
				break;
			}
		} catch {
			// The ports file has not been written yet.
		}
		await delay(25);
	}
	const stop = async (): Promise<void> => {
		child.kill("SIGKILL");
		await exited;
		await rm(directory, { recursive: true, force: true });
	};
	if (!url) {
		await stop();
		throw new Error(`nats-server did not write a ports file; stderr:\n${stderr}`);
	}
	return { url, stop };
}

let server: TestNatsServer;
let name_counter = 0;

/**
 * Produce a stream/consumer name that is unique across the suite, so tests
 * sharing one server never see each other's JetStream state.
 *
 * @param prefix - Human-readable hint for the entity being named.
 * @returns A unique NATS-safe name.
 */
function unique_name(prefix: string): string {
	name_counter += 1;
	return `${prefix}_${name_counter}`;
}

beforeAll(async () => {
	server = await start_nats_server();
});

afterAll(async () => {
	await server.stop();
});

describe("use_nats_connection", () => {
	it("provides an opaque handle and drains the connection on scope exit", async () => {
		const raw = await node_connect({ servers: server.url });
		await run(function* () {
			const connection = yield* use_nats_connection(() => Promise.resolve(raw));
			expect(raw.isClosed()).toBe(false);
			expect(Object.getOwnPropertyNames(connection)).toEqual([]);
		});
		expect(raw.isClosed()).toBe(true);
	});

	it("closes immediately in release: 'close' mode", async () => {
		const raw = await node_connect({ servers: server.url });
		await run(function* () {
			yield* use_nats_connection(() => Promise.resolve(raw), { release: "close" });
		});
		expect(raw.isClosed()).toBe(true);
	});

	it("fails the owning scope when the connection closes with an error", async () => {
		const dedicated = await start_nats_server();
		try {
			await expect(
				run(function* () {
					yield* use_nats_connection({
						servers: dedicated.url,
						reconnect: true,
						maxReconnectAttempts: 2,
						reconnectTimeWait: 100,
					});
					yield* until(dedicated.stop());
					// The connection monitor must fail this scope once reconnect
					// attempts are exhausted; otherwise this suspends forever and
					// the test times out.
					yield* suspend();
				}),
			).rejects.toThrow(/connection refused/);
		} finally {
			await dedicated.stop();
		}
	}, 15_000);
});

describe("JetStream helpers", () => {
	it("publishes and consumes messages with scoped cleanup", async () => {
		const stream_name = unique_name("round_trip");
		const subject = `${stream_name}.events`;
		const consumer_name = "worker";
		const received: string[] = [];
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			yield* ensure_nats_stream(manager, stream_name, { subjects: [subject] });
			yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, {
				ack_policy: AckPolicy.Explicit,
			});
			const client = nats_jetstream(connection);
			for (let i = 0; i < 5; i++) {
				yield* until(client.publish(subject, `message ${i}`));
			}
			const consumer = yield* get_nats_consumer(connection, stream_name, consumer_name);
			const messages = yield* use_nats_consumer_messages(consumer, { max_messages: 2 });
			while (received.length < 5) {
				const result = yield* messages.next();
				if (result.done) {
					throw new Error("consumer stream ended before delivering all messages");
				}
				received.push(result.value.string());
				result.value.ack();
			}
		});
		expect(received).toEqual(["message 0", "message 1", "message 2", "message 3", "message 4"]);
	});

	it("ensure_nats_stream and ensure_durable_nats_consumer are idempotent", async () => {
		const stream_name = unique_name("idempotent");
		const consumer_name = "worker";
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			const stream_config = { subjects: [`${stream_name}.>`] };
			yield* ensure_nats_stream(manager, stream_name, stream_config);
			yield* ensure_nats_stream(manager, stream_name, stream_config);
			const consumer_config = { ack_policy: AckPolicy.Explicit };
			yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, consumer_config);
			yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, consumer_config);
			const info = yield* until(manager.consumers.info(stream_name, consumer_name));
			expect(info.name).toBe(consumer_name);
		});
	});

	it("ensure_durable_nats_consumer applies updatable config and rejects creation-only changes", async () => {
		const stream_name = unique_name("consumer_update");
		const consumer_name = "worker";
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			yield* ensure_nats_stream(manager, stream_name, { subjects: [`${stream_name}.>`] });
			yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, {
				ack_policy: AckPolicy.Explicit,
				max_deliver: 5,
			});
			// Creation-only properties matching the existing consumer are
			// tolerated while the updatable max_deliver change is applied.
			const updated = yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, {
				ack_policy: AckPolicy.Explicit,
				max_deliver: 10,
			});
			expect(updated.config.max_deliver).toBe(10);
			const info = yield* until(manager.consumers.info(stream_name, consumer_name));
			expect(info.config.max_deliver).toBe(10);
		});
		await expect(
			run(function* () {
				const connection = yield* use_nats_connection({ servers: server.url });
				const manager = yield* nats_jetstream_manager(connection);
				yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, {
					ack_policy: AckPolicy.None,
				});
			}),
		).rejects.toThrow(/creation-only properties.*ack_policy \(existing "explicit", config "none"\)/);
	});

	it("updates the configuration on an existing stream", async () => {
		const stream_name = unique_name("update_config");
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			const created = yield* ensure_nats_stream(manager, stream_name, {
				subjects: [`${stream_name}.before`],
				max_msgs: 100,
			});
			expect(created.config.name).toBe(stream_name);
			expect(created.config.subjects).toEqual([`${stream_name}.before`]);
			expect(created.config.max_msgs).toBe(100);
			const updated = yield* ensure_nats_stream(manager, stream_name, {
				subjects: [`${stream_name}.after`],
				max_msgs: 50,
			});
			expect(updated.config.subjects).toEqual([`${stream_name}.after`]);
			expect(updated.config.max_msgs).toBe(50);
			// Confirm the new configuration is what the server reports, not just
			// what the update call echoed back.
			const info = yield* until(manager.streams.info(stream_name));
			expect(info.config.subjects).toEqual([`${stream_name}.after`]);
			expect(info.config.max_msgs).toBe(50);
		});
	});

	it("propagates JetStream API errors for a missing consumer", async () => {
		const stream_name = unique_name("missing_consumer");
		await expect(
			run(function* () {
				const connection = yield* use_nats_connection({ servers: server.url });
				const manager = yield* nats_jetstream_manager(connection);
				yield* ensure_nats_stream(manager, stream_name, { subjects: [`${stream_name}.>`] });
				yield* get_nats_consumer(connection, stream_name, "does_not_exist");
			}),
		).rejects.toThrow(/consumer not found/);
	});

	it("refuses to create a JetStream client on a closed connection", async () => {
		let raw!: NatsConnection;
		await run(function* () {
			const connection = yield* use_nats_connection(async () => {
				raw = await node_connect({ servers: server.url });
				return raw;
			});
			void connection;
		});
		expect(raw.isClosed()).toBe(true);
		expect(() => nats_jetstream(raw)).toThrow(/closed NATS connection/);
	});
});

describe("use_ordered_nats_consumer", () => {
	it("skips the backlog with DeliverPolicy.New and deletes the consumer on scope exit", async () => {
		const stream_name = unique_name("ordered_new");
		const subject = `${stream_name}.events`;
		const received = await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			yield* ensure_nats_stream(manager, stream_name, { subjects: [subject] });
			const client = nats_jetstream(connection);
			// A durable consumer resuming its cursor would deliver this backlog.
			yield* until(client.publish(subject, "stale 0"));
			yield* until(client.publish(subject, "stale 1"));
			const consumer = yield* use_ordered_nats_consumer(connection, stream_name, {
				deliver_policy: DeliverPolicy.New,
			});
			const consumers = yield* until(manager.consumers.list(stream_name).next());
			expect(consumers.length).toBe(1);
			yield* until(client.publish(subject, "fresh"));
			const messages = yield* use_nats_consumer_messages(consumer);
			const result = yield* messages.next();
			if (result.done) {
				throw new Error("consumer stream ended before delivering a message");
			}
			return result.value.string();
		});
		expect(received).toBe("fresh");
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			const consumers = yield* until(manager.consumers.list(stream_name).next());
			expect(consumers.length).toBe(0);
		});
	});

	it("delivers a snapshot with DeliverPolicy.LastPerSubject", async () => {
		const stream_name = unique_name("ordered_last_per_subject");
		const received: string[] = [];
		await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const manager = yield* nats_jetstream_manager(connection);
			yield* ensure_nats_stream(manager, stream_name, { subjects: [`${stream_name}.>`] });
			const client = nats_jetstream(connection);
			yield* until(client.publish(`${stream_name}.alpha`, "alpha stale"));
			yield* until(client.publish(`${stream_name}.alpha`, "alpha latest"));
			yield* until(client.publish(`${stream_name}.beta`, "beta stale"));
			yield* until(client.publish(`${stream_name}.beta`, "beta latest"));
			const consumer = yield* use_ordered_nats_consumer(connection, stream_name, {
				deliver_policy: DeliverPolicy.LastPerSubject,
			});
			const messages = yield* use_nats_consumer_messages(consumer);
			while (received.length < 2) {
				const result = yield* messages.next();
				if (result.done) {
					throw new Error("consumer stream ended before delivering the snapshot");
				}
				received.push(result.value.string());
			}
		});
		expect(received).toEqual(["alpha latest", "beta latest"]);
	});
});

/**
 * Create a stream and a durable consumer for teardown tests, publishing no
 * messages so consumption stays pending.
 *
 * @param connection - The scoped connection to administer through.
 * @param stream_name - The unique stream name for this test.
 * @returns The scoped consumer handle.
 */
function* create_empty_consumer(connection: Parameters<typeof nats_jetstream>[0], stream_name: string) {
	const consumer_name = "worker";
	const manager = yield* nats_jetstream_manager(connection);
	yield* ensure_nats_stream(manager, stream_name, { subjects: [`${stream_name}.>`] });
	yield* ensure_durable_nats_consumer(manager, stream_name, consumer_name, {
		ack_policy: AckPolicy.Explicit,
	});
	return yield* get_nats_consumer(connection, stream_name, consumer_name);
}

describe("use_nats_consumer_messages teardown", () => {
	it("tears down a stream whose iterator was never consumed", async () => {
		// Exercises the priming-next() path: ConsumerMessages.close() only
		// settles once something drives the iterator, so without the priming
		// call this run() would hang and the test would time out.
		const outcome = await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const consumer = yield* create_empty_consumer(connection, unique_name("never_consumed"));
			yield* use_nats_consumer_messages(consumer);
			return "reached scope exit";
		});
		expect(outcome).toBe("reached scope exit");
	});

	it("tears down cleanly after a task with a pending next() is halted", async () => {
		const outcome = await run(function* () {
			const connection = yield* use_nats_connection({ servers: server.url });
			const consumer = yield* create_empty_consumer(connection, unique_name("halted_next"));
			const messages = yield* use_nats_consumer_messages(consumer);
			const reader = yield* spawn(function* () {
				// No messages are ever published, so this next() stays pending
				// until the halt below interrupts it.
				yield* messages.next();
			});
			yield* sleep(50);
			yield* reader.halt();
			return "reached scope exit";
		});
		expect(outcome).toBe("reached scope exit");
	});
});
