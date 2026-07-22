// Model-output: Claude Fable 5
/**
 * End-to-end tests of ws-effection against real sockets on 127.0.0.1.
 */
import { describe, expect, test, vi } from "vitest";
import { A } from "ayy";
import { call, createChannel, createSignal, each, run, scoped, sleep, spawn, suspend, withResolvers } from "effection";
import { WebSocket } from "ws";
import { forward, heartbeat, serve, use_connection, use_web_socket_server } from "./ws.js";
/**
 * Extract the bound TCP port of a server.
 * @param server - a listening WsServer
 * @returns the port number
 */
function port_of(server) {
    const address = server.address();
    if (address === null || typeof address === "string") {
        throw new Error(`expected an AddressInfo, got ${String(address)}`);
    }
    return address.port;
}
/**
 * Decode a message payload these tests know arrives as a single text frame.
 * @param data - raw payload of a received message
 * @returns the payload as UTF-8 text
 */
function text(data) {
    A(Buffer.isBuffer(data), "expected a single-Buffer frame");
    return data.toString("utf8");
}
/**
 * Connect a client to a local port and provide it as a connection resource.
 * @param port - server port
 * @param auto_pong - whether the client answers pings (disable to test heartbeat)
 * @returns an operation yielding an open client connection
 */
function use_client(port, auto_pong = true) {
    return use_connection(new WebSocket(`ws://127.0.0.1:${port}`, { autoPong: auto_pong }));
}
/**
 * Read messages from a connection as strings until its stream closes.
 * @param connection - the connection to drain
 * @param into - collector receiving each decoded message
 * @returns an operation yielding the close info
 */
function drain_text(connection, into) {
    return call(function* () {
        const subscription = yield* connection;
        let next = yield* subscription.next();
        while (!next.done) {
            into.push(text(next.value.data));
            next = yield* subscription.next();
        }
        return next.value;
    });
}
describe("ws-effection", () => {
    test("echo round-trip through an accept loop with one task per client", async () => {
        await run(function* () {
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
            const client = yield* use_client(port_of(server));
            const subscription = yield* client;
            yield* client.send("hello");
            const reply = yield* subscription.next();
            A(!reply.done, "connection closed before the echo reply");
            expect(text(reply.value.data)).toBe("hello");
        });
    });
    test("one shared stream fans out to all clients; a slow client only slows itself", async () => {
        await run(function* () {
            const hub = createSignal(); // the "same effection Stream", multicast
            const joins = createChannel();
            const server = yield* use_web_socket_server({ port: 0 });
            yield* spawn(function* () {
                for (const { socket } of yield* each(server)) {
                    yield* spawn(function* () {
                        const connection = yield* use_connection(socket);
                        // Announce readiness only once the hub subscription exists;
                        // signals drop values sent while nobody is subscribed.
                        const announced = {
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
            const port = port_of(server);
            const join_watch = yield* joins;
            const fast_a = [];
            const fast_b = [];
            const slow_c = [];
            const finish_order = [];
            const total = 10;
            const collect = (name, into, delay_ms) => function* () {
                const client = yield* use_client(port);
                const subscription = yield* client;
                while (into.length < total) {
                    const next = yield* subscription.next();
                    if (next.done) {
                        break;
                    }
                    into.push(text(next.value.data));
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
            expect(fast_a).toHaveLength(total);
            expect(fast_b).toHaveLength(total);
            expect(slow_c).toHaveLength(total);
            expect(fast_b).toEqual(fast_a); // broadcast order identical across clients
            expect(slow_c).toEqual(fast_a);
            // The slow client finished last without delaying the fast ones.
            expect(finish_order[2]).toBe("c");
        });
    });
    test("a returning handler closes with the connection's code; server shutdown closes live clients with 1001", async () => {
        await run(function* () {
            const port_box = withResolvers("server port");
            const first_close = withResolvers("first client closed");
            const second_close = withResolvers("second client closed");
            // The scoped block is load-bearing: it holds the server resource and
            // serve()'s per-connection tasks in one scope that halt() destroys in
            // creation order (server first, so clients see 1001). Halting a bare
            // task body unwinds serve() first, closing the still-served client
            // with the connection's own code 1000 instead.
            const server_task = yield* spawn(() => scoped(function* () {
                const server = yield* use_web_socket_server({ port: 0 });
                port_box.resolve(port_of(server));
                yield* serve(server, function* (connection) {
                    const subscription = yield* connection;
                    let next = yield* subscription.next();
                    while (!next.done && text(next.value.data) !== "bye") {
                        next = yield* subscription.next();
                    }
                    // handler returns; the connection resource closes this client
                });
            }));
            const port = yield* port_box.operation;
            yield* spawn(function* () {
                const client = yield* use_client(port);
                yield* client.send("bye");
                first_close.resolve(yield* drain_text(client, []));
            });
            const first = yield* first_close.operation;
            expect(first.code).toBe(1000);
            expect(String(first.reason)).toBe("resource released");
            yield* spawn(function* () {
                const client = yield* use_client(port);
                second_close.resolve(yield* drain_text(client, []));
            });
            yield* sleep(50); // second client is connected and being served, but never says bye
            yield* server_task.halt();
            const second = yield* second_close.operation;
            expect(second.code).toBe(1001);
            expect(String(second.reason)).toBe("server shutting down");
        });
    });
    test("heartbeat terminates a peer that never pongs; a responsive peer survives", async () => {
        await run(function* () {
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
            const dead_close = withResolvers("mute client closed");
            yield* spawn(function* () {
                const mute = yield* use_client(port, false); // autoPong: false — never answers pings
                dead_close.resolve(yield* drain_text(mute, []));
            });
            const live = yield* use_client(port, true);
            const observed = yield* dead_close.operation;
            expect(observed.code).toBe(1006); // unresponsive client terminated by heartbeat
            yield* sleep(150); // more than two heartbeat periods
            expect(live.ready_state).toBe(WebSocket.OPEN);
        });
    });
    test("a crashing handler is isolated by serve(); its client closes cleanly, the server lives on", async () => {
        await run(function* () {
            const crashes = [];
            const server = yield* use_web_socket_server({ port: 0 });
            yield* spawn(() => serve(server, function* (connection) {
                for (const message of yield* each(connection)) {
                    if (text(message.data) === "boom") {
                        throw new Error("handler crash");
                    }
                    yield* connection.send(message.data);
                    yield* each.next();
                }
            }, { on_error: (error) => { crashes.push(error); } }));
            const port = port_of(server);
            const victim = yield* use_client(port);
            const victim_close = withResolvers("victim closed");
            yield* spawn(function* () {
                victim_close.resolve(yield* drain_text(victim, []));
            });
            yield* victim.send("boom");
            const closed = yield* victim_close.operation;
            expect(closed.code).toBe(1000); // crashed handler still closed its socket cleanly
            expect(crashes).toHaveLength(1);
            expect(String(crashes[0])).toContain("handler crash");
            const survivor = yield* use_client(port);
            const subscription = yield* survivor;
            yield* survivor.send("still-alive");
            const reply = yield* subscription.next();
            A(!reply.done, "server closed the survivor's connection");
            expect(text(reply.value.data)).toBe("still-alive");
        });
    });
    test("shutdown closes a client that was never vended to an accept loop", async () => {
        // No accept loop anywhere: the client sits accepted by ws but never
        // handed to anyone. Shutdown must still close it with 1001.
        const observed = await run(function* () {
            const port_box = withResolvers("server port");
            const closed = withResolvers("unvended client closed");
            const server_task = yield* spawn(() => scoped(function* () {
                const server = yield* use_web_socket_server({ port: 0 });
                port_box.resolve(port_of(server));
                yield* suspend();
            }));
            const port = yield* port_box.operation;
            yield* spawn(function* () {
                const client = yield* use_client(port);
                closed.resolve(yield* drain_text(client, []));
            });
            yield* sleep(50); // connected, tracked by ws, never vended
            yield* server_task.halt();
            return yield* closed.operation;
        });
        expect(observed.code).toBe(1001);
        expect(String(observed.reason)).toBe("server shutting down");
    });
    test("a throwing on_error is reported on stderr and the server keeps serving", async () => {
        const console_error = vi.spyOn(console, "error").mockImplementation(() => { });
        try {
            await run(function* () {
                const server = yield* use_web_socket_server({ port: 0 });
                yield* spawn(() => serve(server, function* (connection) {
                    for (const message of yield* each(connection)) {
                        if (text(message.data) === "boom") {
                            throw new Error("handler crash");
                        }
                        yield* connection.send(message.data);
                        yield* each.next();
                    }
                }, { on_error: () => {
                        throw new Error("observer crash");
                    } }));
                const port = port_of(server);
                const victim = yield* use_client(port);
                yield* victim.send("boom");
                yield* drain_text(victim, []);
                const survivor = yield* use_client(port);
                const subscription = yield* survivor;
                yield* survivor.send("still-alive");
                const reply = yield* subscription.next();
                A(!reply.done, "server closed the survivor's connection");
                expect(text(reply.value.data)).toBe("still-alive");
            });
            expect(console_error).toHaveBeenCalledWith("ws-effection: serve() on_error callback threw", expect.objectContaining({ message: "observer crash" }));
        }
        finally {
            console_error.mockRestore();
        }
    });
    test("clientTracking: false is rejected — shutdown depends on ws client tracking", () => {
        const options = { port: 0, clientTracking: false };
        expect(() => use_web_socket_server(options)).toThrow(/client tracking/);
    });
    test("nonsense timing options are rejected", () => {
        expect(() => heartbeat({}, { period: 0 })).toThrow(/positive number of milliseconds/);
        expect(() => heartbeat({}, { period: NaN })).toThrow(/positive number of milliseconds/);
        expect(() => use_connection({}, { close_timeout: -1 })).toThrow(/non-negative number of milliseconds/);
        expect(() => use_web_socket_server({ port: 0, shutdown_timeout: NaN })).toThrow(/non-negative number of milliseconds/);
    });
});
