/**
 * End-to-end exercise of ws-effection against real sockets on 127.0.0.1.
 * Run: npx tsx test.ts
 */

import { call, createChannel, createSignal, each, main, scoped, sleep, spawn, withResolvers } from "effection";
import type { Operation, Stream, Task } from "effection";
import { WebSocket } from "ws";
import type { AddressInfo } from "node:net";
import { forward, heartbeat, serve, use_connection, use_web_socket_server } from "./ws-effection.ts";
import type { WsClose, WsConnection, WsServer } from "./ws-effection.ts";

/**
 * Assert a test expectation.
 * @param condition - must hold
 * @param label - which expectation
 */
function expect(condition: boolean, label: string): asserts condition {
	if (!condition) {
		throw new Error(`FAILED: ${label}`);
	}
	console.log(`ok: ${label}`);
}

/**
 * Extract the bound TCP port of a server.
 * @param server - a listening WsServer
 * @returns the port number
 */
function port_of(server: WsServer): number {
	const address = server.address();
	if (address === null || typeof address === "string") {
		throw new Error(`expected an AddressInfo, got ${String(address)}`);
	}
	return (address as AddressInfo).port;
}

/**
 * Connect a client to a local port and provide it as a connection resource.
 * @param port - server port
 * @param auto_pong - whether the client answers pings (disable to test heartbeat)
 * @returns an operation yielding an open client connection
 */
function use_client(port: number, auto_pong = true): Operation<WsConnection> {
	return use_connection(new WebSocket(`ws://127.0.0.1:${port}`, { autoPong: auto_pong }));
}

/**
 * Read messages from a connection as strings until its stream closes.
 * @param connection - the connection to drain
 * @param into - collector receiving each decoded message
 * @returns an operation yielding the close info
 */
function drain_text(connection: WsConnection, into: string[]): Operation<WsClose> {
	return call(function* () {
		const subscription = yield* connection;
		let   next         = yield* subscription.next();
		while (!next.done) {
			into.push(String(next.value.data));
			next = yield* subscription.next();
		}
		return next.value;
	});
}

/** Test 1: echo round-trip through an accept loop with one task per client. */
export function* test_echo(): Operation<void> {
	yield* scoped(function* () {
		const server = yield* use_web_socket_server({ port: 0 });
		yield* spawn(function* () {
			for (const { socket } of yield* each(server)) {
				yield* spawn(function* () {
					const connection = yield* use_connection(socket);
					for (const message of yield* each(connection)) {
						yield* connection.send(message.data, { binary: message.is_binary });
						yield* each.next();
					}
				});
				yield* each.next();
			}
		});
		const client       = yield* use_client(port_of(server));
		const subscription = yield* client;
		yield* client.send("hello");
		const reply = yield* subscription.next();
		expect(!reply.done && String(reply.value.data) === "hello", "echo round-trip");
	});
}

/** Test 2 + 3: one shared stream fanned out to three clients; slow client only slows itself. */
export function* test_broadcast(): Operation<void> {
	yield* scoped(function* () {
		const hub    = createSignal<string, never>(); // the "same effection Stream", multicast
		const joins  = createChannel<void>();
		const server = yield* use_web_socket_server({ port: 0 });
		yield* spawn(function* () {
			for (const { socket } of yield* each(server)) {
				yield* spawn(function* () {
					const connection = yield* use_connection(socket);
					// Announce readiness only once the hub subscription exists;
					// signals drop values sent while nobody is subscribed.
					const announced: Stream<string, never> = {
						*[Symbol.iterator]() {
							const subscription = yield* hub;
							yield* joins.send();
							return subscription;
						},
					};
					yield* forward(announced, connection); // this client now rides the shared stream
				});
				yield* each.next();
			}
		});
		const port          = port_of(server);
		const join_watch    = yield* joins;
		const fast_a: string[] = [];
		const fast_b: string[] = [];
		const slow_c: string[] = [];
		const finish_order: string[] = [];
		const total = 10;
		const collect = (name: string, into: string[], delay_ms: number) =>
			function* (): Operation<void> {
				const client       = yield* use_client(port);
				const subscription = yield* client;
				while (into.length < total) {
					const next = yield* subscription.next();
					if (next.done) {
						break;
					}
					into.push(String(next.value.data));
					if (delay_ms > 0) {
						yield* sleep(delay_ms);
					}
				}
				finish_order.push(name);
			};
		const task_a = yield* spawn(collect("a", fast_a, 0));
		const task_b = yield* spawn(collect("b", fast_b, 0));
		const task_c = yield* spawn(collect("c", slow_c, 25));
		for (let joined = 0; joined < 3; joined += 1) {
			yield* join_watch.next();
		}
		for (let index = 0; index < total; index += 1) {
			hub.send(`message-${index}`);
			yield* sleep(1); // let per-client forwarders interleave
		}
		yield* task_a;
		yield* task_b;
		yield* task_c;
		expect(fast_a.length === total && fast_b.length === total && slow_c.length === total,
			`all three clients received all ${total} broadcast messages`);
		expect(fast_a.join() === fast_b.join() && fast_b.join() === slow_c.join(),
			"broadcast order identical across clients");
		expect(finish_order[2] === "c",
			`slow client finished last without delaying fast ones (order: ${finish_order.join(",")})`);
	});
}

/** Test 4: a returning handler closes with the connection's code; whole-server shutdown closes remaining clients with 1001. */
export function* test_shutdown_codes(): Operation<void> {
	const port_box     = withResolvers<number>("server port");
	const first_close  = withResolvers<WsClose>("first client closed");
	const second_close = withResolvers<WsClose>("second client closed");
	const server_task  = yield* spawn(() =>
		scoped(function* () {
			const server = yield* use_web_socket_server({ port: 0 });
			port_box.resolve(port_of(server));
			yield* serve(server, function* (connection) {
				const subscription = yield* connection;
				let next = yield* subscription.next();
				while (!next.done && String(next.value.data) !== "bye") {
					next = yield* subscription.next();
				}
				// handler returns; the connection resource closes this client
			});
		}),
	);
	const port = yield* port_box.operation;
	yield* spawn(function* () {
		const client = yield* use_client(port);
		yield* client.send("bye");
		first_close.resolve(yield* drain_text(client, []));
	});
	const first = yield* first_close.operation;
	expect(first.code === 1000 && String(first.reason) === "resource released",
		`handler return closed its client with the connection's code (got ${first.code} ${first.reason})`);
	yield* spawn(function* () {
		const client = yield* use_client(port);
		second_close.resolve(yield* drain_text(client, []));
	});
	yield* sleep(50); // second client is connected and being served, but never says bye
	yield* server_task.halt();
	const second = yield* second_close.operation;
	expect(second.code === 1001 && String(second.reason) === "server shutting down",
		`whole-server shutdown closed a live client with 1001 (got ${second.code} ${second.reason})`);
}

/** Test 5: heartbeat terminates a peer that never pongs; a responsive peer survives. */
export function* test_heartbeat(): Operation<void> {
	yield* scoped(function* () {
		const server = yield* use_web_socket_server({ port: 0 });
		yield* spawn(function* () {
			for (const { socket } of yield* each(server)) {
				yield* spawn(function* () {
					const connection = yield* use_connection(socket);
					yield* spawn(() => heartbeat(connection, { period: 60 }));
					yield* drain_text(connection, []);
				});
				yield* each.next();
			}
		});
		const port = port_of(server);
		const dead_close = withResolvers<WsClose>("mute client closed");
		yield* spawn(function* () {
			const mute = yield* use_client(port, false); // autoPong: false — never answers pings
			dead_close.resolve(yield* drain_text(mute, []));
		});
		const live       = yield* use_client(port, true);
		const observed   = yield* dead_close.operation;
		expect(observed.code === 1006, `unresponsive client terminated by heartbeat (got ${observed.code})`);
		yield* sleep(150); // more than two heartbeat periods
		expect(live.ready_state === WebSocket.OPEN, "responsive client survives the heartbeat");
	});
}

/** Test 6: a crashing handler is isolated by serve(); its client closes cleanly, the server lives on. */
export function* test_error_isolation(): Operation<void> {
	yield* scoped(function* () {
		const crashes: unknown[] = [];
		const server  = yield* use_web_socket_server({ port: 0 });
		yield* spawn(() =>
			serve(server, function* (connection) {
				for (const message of yield* each(connection)) {
					if (String(message.data) === "boom") {
						throw new Error("handler crash");
					}
					yield* connection.send(message.data);
					yield* each.next();
				}
			}, { on_error: (error) => { crashes.push(error); } }),
		);
		const port         = port_of(server);
		const victim       = yield* use_client(port);
		const victim_close = withResolvers<WsClose>("victim closed");
		yield* spawn(function* () {
			victim_close.resolve(yield* drain_text(victim, []));
		});
		yield* victim.send("boom");
		const closed = yield* victim_close.operation;
		expect(closed.code === 1000, `crashed handler still closed its socket cleanly (got ${closed.code})`);
		expect(crashes.length === 1 && String(crashes[0]).includes("handler crash"),
			"on_error observed the handler crash");
		const survivor     = yield* use_client(port);
		const subscription = yield* survivor;
		yield* survivor.send("still-alive");
		const reply = yield* subscription.next();
		expect(!reply.done && String(reply.value.data) === "still-alive",
			"server keeps accepting and serving after a handler crash");
	});
}

/** Run every test in sequence inside one effection main. */
function* run_all(): Operation<void> {
	yield* test_echo();
	yield* test_broadcast();
	yield* test_shutdown_codes();
	yield* test_heartbeat();
	yield* test_error_isolation();
	console.log("all tests passed");
}

import { pathToFileURL } from "node:url";
if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
	await main(run_all);
}
