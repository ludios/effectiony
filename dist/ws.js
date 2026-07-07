// Model-output: Claude Fable 5
/**
 * ws-effection — structured-concurrency bindings for the `ws` library
 * (https://github.com/websockets/ws) on top of Effection v4.
 *
 * See dev/ws-demo.ts for a basic example.
 *
 * Model:
 *   - `use_web_socket_server()` is a resource yielding a Stream of raw
 *     `{ socket, request }` pairs. Consume it with a single accept loop.
 *   - Each accepted socket is wrapped per-task with `use_connection()`,
 *     a resource that is itself a Stream of inbound messages and closes
 *     the socket when its owning scope exits.
 *   - `serve()` wraps the accept loop: one spawned task per client, socket
 *     ownership established before the next accept, per-connection errors
 *     isolated from the server.
 *   - Fan-out ("pipe one stream to many clients") needs no new machinery,
 *     but the shared source must itself be multicast: Effection Signals and
 *     Channels are (every subscriber gets its own queue), so each connection
 *     task subscribes to the same Signal/Channel and `forward()`s it into its
 *     socket. An arbitrary Stream is NOT multicast — a `forward()` per client
 *     would re-run its upstream work once per subscription — so fan a plain
 *     stream out by pumping it into a Signal/Channel hub first.
 */
import { action, call, createQueue, createSignal, each, race, resource, scoped, sleep, spawn, withResolvers, } from "effection";
import { WebSocket, WebSocketServer } from "ws";
/**
 * Throw when an invariant is violated.
 * @param condition - must hold
 * @param message - what was violated
 */
function assert(condition, message) {
    if (!condition) {
        throw new Error(`ws-effection invariant violated: ${message}`);
    }
}
/**
 * Listener/callback that deliberately ignores an error. A bare `error`
 * emission on an EventEmitter with no listener kills the whole process, so
 * the resources here keep one of these attached for their entire lifetime
 * (deliberate crash semantics come from watcher tasks instead); it also
 * discards send-callback errors in fire-and-forget send modes.
 */
function ignore_error() { }
/**
 * Stream the arguments of every emission of `event` on a Node EventEmitter.
 * The listener is attached for exactly the lifetime of the surrounding scope.
 * (Counterpart of effection's EventTarget-only `on()`; same shape as
 * `@effectionx/node`.)
 * @param emitter - the event source
 * @param event - the event name
 * @returns a Stream of listener-argument tuples that never completes
 */
export function on_emitter(emitter, event) {
    return resource(function* (provide) {
        const signal = createSignal();
        const listener = (...args) => {
            signal.send(args);
        };
        emitter.on(event, listener);
        try {
            yield* provide(yield* signal);
        }
        finally {
            emitter.off(event, listener);
        }
    });
}
/**
 * Wait for the next emission of `event` on a Node EventEmitter. Runs in its
 * own scope so the listener detaches as soon as the event arrives.
 * @param emitter - the event source
 * @param event - the event name
 * @returns an operation yielding the listener-argument tuple
 */
export function once_emitter(emitter, event) {
    return scoped(function* () {
        const subscription = yield* on_emitter(emitter, event);
        const next = yield* subscription.next();
        return next.value;
    });
}
/**
 * Adapt a completion-callback API (`ws`'s send/ping/pong callbacks) into an
 * operation that resolves when the callback fires without an error.
 * @param start - invoked immediately; must arrange for `done` to be called exactly once
 * @returns an operation resolving on `done()`, failing on `done(error)` or a synchronous throw
 */
function flushed(start) {
    return action((resolve, reject) => {
        try {
            start((error) => {
                if (error) {
                    reject(error);
                }
                else {
                    resolve();
                }
            });
        }
        catch (error) {
            reject(error);
        }
        return () => { };
    });
}
/**
 * Wrap a `ws` WebSocket in a scope-bound {@link WsConnection} resource.
 *
 * Works for both server-side sockets (already OPEN when accepted) and
 * client-side sockets (waits for the `open` event first). Inbound messages
 * are buffered from the moment this resource initializes, so nothing sent
 * between accept and your first read is lost. A socket `error` event crashes
 * the owning scope (your error boundary sees it). When the owning scope
 * exits, the socket is closed gracefully, escalating to `terminate()` after
 * `close_timeout`.
 *
 * @param socket - a raw `ws` socket in the CONNECTING or OPEN state
 * @param options - close code/reason/timeout used at teardown
 * @returns an operation yielding the live connection
 */
export function use_connection(socket, options = {}) {
    const { close_code = 1000, close_reason = "resource released", close_timeout = 3_000, } = options;
    assert(Number.isFinite(close_timeout) && close_timeout >= 0, `close_timeout must be a non-negative number of milliseconds (got ${close_timeout})`);
    return resource(function* (provide) {
        const messages = createQueue();
        const closed = withResolvers("socket closed");
        let watcher;
        const on_message = (data, is_binary) => {
            messages.add({ data, is_binary });
        };
        const on_close = (code, reason) => {
            const info = { code, reason };
            messages.close(info);
            closed.resolve(info);
        };
        socket.on("error", ignore_error);
        socket.on("message", on_message);
        socket.on("close", on_close);
        try {
            if (socket.readyState === WebSocket.CONNECTING) {
                yield* race([
                    once_emitter(socket, "open"),
                    call(function* () {
                        const [error] = yield* once_emitter(socket, "error");
                        throw error;
                    }),
                ]);
            }
            assert(socket.readyState === WebSocket.OPEN, `socket must be OPEN to provide a connection (readyState=${socket.readyState})`);
            watcher = yield* spawn(function* () {
                const [error] = yield* once_emitter(socket, "error");
                throw error;
            });
            let vended = false;
            const subscription = { next: messages.next };
            yield* provide({
                socket,
                get ready_state() { return socket.readyState; },
                get buffered_amount() { return socket.bufferedAmount; },
                get protocol() { return socket.protocol; },
                send: (data, send_options) => flushed((done) => {
                    socket.send(data, send_options ?? {}, done);
                }),
                ping: (data) => flushed((done) => {
                    socket.ping(data, undefined, done);
                }),
                // oxlint-disable-next-line require-yield -- effection Stream contract: a generator Operation that completes immediately with the subscription
                *[Symbol.iterator]() {
                    assert(!vended, "a WsConnection carries one buffered message queue and supports a single subscription");
                    vended = true;
                    return subscription;
                },
            });
        }
        finally {
            try {
                // Stop treating socket errors as crashes before initiating the
                // close handshake (a peer ECONNRESET mid-close is not an
                // application error). This must live in `finally` because a halt
                // of the owning scope unwinds `provide` by throwing.
                if (watcher !== undefined) {
                    yield* watcher.halt();
                }
                if (socket.readyState !== WebSocket.CLOSED) {
                    if (socket.readyState === WebSocket.OPEN) {
                        socket.close(close_code, close_reason);
                    }
                    else if (socket.readyState === WebSocket.CONNECTING) {
                        socket.terminate();
                    }
                    yield* race([
                        closed.operation,
                        call(function* () {
                            yield* sleep(close_timeout);
                            socket.terminate();
                            return yield* closed.operation;
                        }),
                    ]);
                }
            }
            finally {
                socket.off("error", ignore_error);
                socket.off("message", on_message);
                socket.off("close", on_close);
            }
        }
    });
}
/**
 * Ping the peer every `period` ms and `terminate()` it if a full period
 * passes without any pong — the flag algorithm from the `ws` README, in
 * scope-bound form. Run it concurrently with your read loop:
 *
 *   yield* spawn(() => heartbeat(connection, { period: 30_000 }));
 *
 * Termination surfaces to the read loop as a stream close with code 1006,
 * so no extra signaling is needed.
 *
 * @param connection - the connection to monitor
 * @param options - ping period
 * @returns an operation that runs until the socket closes or fails the check
 */
export function heartbeat(connection, options = {}) {
    const { period = 30_000 } = options;
    assert(Number.isFinite(period) && period > 0, `heartbeat period must be a positive number of milliseconds (got ${period})`);
    return call(function* () {
        const socket = connection.socket;
        let alive = true;
        const on_pong = () => {
            alive = true;
        };
        socket.on("pong", on_pong);
        try {
            while (socket.readyState === WebSocket.OPEN) {
                alive = false;
                try {
                    yield* connection.ping();
                }
                catch {
                    return; // socket died mid-ping; the connection resource reports it
                }
                yield* sleep(period);
                if (!alive) {
                    socket.terminate();
                    return;
                }
            }
        }
        finally {
            socket.off("pong", on_pong);
        }
    });
}
/**
 * Pump every item of `source` into `connection` until the source completes,
 * the peer disconnects, or (in "close" mode) the peer falls too far behind.
 *
 * Because Effection Signals/Channels are multicast, broadcasting is just
 * running one `forward(shared, connection)` per client task over the same
 * shared Signal/Channel — each subscriber has an independent queue, so a
 * slow client back-pressures only itself (see {@link ForwardOptions.mode}
 * for what that costs). This shape needs the source to be multicast; passing
 * one plain Stream to several `forward()` calls instead re-runs its upstream
 * work once per subscription.
 *
 * @param source - the stream to transmit
 * @param connection - the destination peer
 * @param options - encoding and slow-peer policy
 * @returns an operation completing when the source ends or the peer is gone
 */
export function forward(source, connection, options = {}) {
    const { encode = (item) => item, mode = "await", high_water_mark = 1 << 20, } = options;
    assert(high_water_mark > 0, "high_water_mark must be positive");
    const pump = call(function* () {
        for (const item of yield* each(source)) {
            if (connection.ready_state !== WebSocket.OPEN) {
                return;
            }
            if (mode === "await") {
                yield* connection.send(encode(item));
            }
            else if (connection.buffered_amount <= high_water_mark) {
                connection.socket.send(encode(item), ignore_error);
            }
            else if (mode === "close") {
                connection.socket.close(1013, "backpressure limit exceeded");
                return;
            } // mode === "drop": skip this item
            yield* each.next();
        }
    });
    return call(function* () {
        if (connection.ready_state !== WebSocket.OPEN) {
            return;
        }
        // The close race (not the message queue) notices a departed peer, so a
        // forwarder never sits on a quiet source stream after disconnection.
        yield* race([once_emitter(connection.socket, "close"), pump]);
    });
}
/**
 * Accept connections forever, running `handler` in its own task per client.
 *
 * Ownership is airtight: the accept loop does not proceed to the next client
 * until the freshly spawned task has initialized its {@link use_connection}
 * resource, so no socket is ever left without an owner (a task halted before
 * its first step never runs, so a naive `spawn` + `use_connection` inside it
 * leaves a window where the accepted socket belongs to nobody).
 *
 * A handler error closes that client and invokes `on_error`; it does not
 * bring the server down. The handler returning closes its client with the
 * connection's `close_code`.
 *
 * @param server - the accept stream from {@link use_web_socket_server}
 * @param handler - per-client operation; when it returns, the client is closed
 * @param options - connection options and error observation
 * @returns an operation that runs until the surrounding scope exits
 */
export function serve(server, handler, options = {}) {
    const { connection: connection_options, on_error = () => { } } = options;
    return call(function* () {
        const accept = yield* server;
        for (;;) {
            const { value: { socket, request } } = yield* accept.next();
            const claimed = withResolvers("socket claimed");
            yield* spawn(function* () {
                try {
                    const connection = yield* use_connection(socket, connection_options);
                    claimed.resolve();
                    yield* handler(connection, request);
                }
                catch (error) {
                    try {
                        on_error(error, request);
                    }
                    catch (observer_error) {
                        console.error("ws-effection: serve() on_error callback threw", observer_error);
                    }
                }
                finally {
                    claimed.resolve(); // idempotent; unblocks the accept loop on instant failure
                }
            });
            yield* claimed.operation;
        }
    });
}
/**
 * Run a `ws` WebSocketServer as a scope-bound resource.
 *
 * With `port`, initialization completes only once the internal HTTP server is
 * listening (or fails with e.g. EADDRINUSE). Accepted connections are
 * buffered, so none are dropped before your accept loop subscribes. On scope
 * exit: stops accepting, closes every still-open client (handled or not)
 * with `shutdown_code`/`shutdown_reason`, waits up to `shutdown_timeout` for
 * the close handshakes, terminates stragglers, and waits for the server's own
 * `close` event. (Effection v4 destroys children in creation order, so on
 * whole-server shutdown this resource unwinds before connection-handler
 * tasks; clients therefore see `shutdown_code`, while a handler that exits
 * individually closes its own client with the connection's `close_code`.)
 *
 * Note: when you pass an external `server`/`noServer`, `ws` never closes your
 * HTTP server — own it as its own resource ordered before this one.
 *
 * The intended shape is {@link serve} (or an equivalent manual accept loop):
 *
 *   const server = yield* use_web_socket_server({ port: 8080 });
 *   yield* serve(server, function* (connection, request) {
 *     // read loop / forward / heartbeat here
 *   });
 *
 * @param options - `ws` ServerOptions (client tracking required) plus shutdown policy
 * @returns an operation yielding the accept stream
 */
export function use_web_socket_server(options) {
    const { shutdown_timeout = 3_000, shutdown_code = 1001, shutdown_reason = "server shutting down", ...server_options } = options;
    assert(server_options.clientTracking !== false, "client tracking must stay on: shutdown enumerates wss.clients to close every live client");
    assert(Number.isFinite(shutdown_timeout) && shutdown_timeout >= 0, `shutdown_timeout must be a non-negative number of milliseconds (got ${shutdown_timeout})`);
    return resource(function* (provide) {
        const wss = new WebSocketServer(server_options);
        const pending = createQueue();
        let watcher;
        const wss_closed = withResolvers("server closed");
        const on_connection = (socket, request) => {
            pending.add({ socket, request });
        };
        const on_wss_close = () => {
            wss_closed.resolve();
        };
        wss.on("error", ignore_error);
        wss.on("connection", on_connection);
        wss.on("close", on_wss_close);
        try {
            if (server_options.port !== undefined) {
                yield* race([
                    once_emitter(wss, "listening"),
                    call(function* () {
                        const [error] = yield* once_emitter(wss, "error");
                        throw error;
                    }),
                ]);
            }
            watcher = yield* spawn(function* () {
                const [error] = yield* once_emitter(wss, "error");
                throw error;
            });
            let vended = false;
            const subscription = { next: pending.next };
            yield* provide({
                socket_server: wss,
                address: () => wss.address(),
                // oxlint-disable-next-line require-yield -- effection Stream contract: a generator Operation that completes immediately with the subscription
                *[Symbol.iterator]() {
                    assert(!vended, "a WsServer carries one buffered accept queue and supports a single accept loop");
                    vended = true;
                    return subscription;
                },
            });
        }
        finally {
            try {
                if (watcher !== undefined) {
                    yield* watcher.halt();
                }
                // No accept race here: this block runs synchronously through
                // `wss.close()`, so no `connection` event can slip in after the
                // listener detaches, and `ws` aborts (503) any upgrade that
                // completes after close() flips its state to CLOSING.
                wss.off("connection", on_connection);
                // Effection v4 destroys children in creation order, so this
                // resource usually unwinds BEFORE sibling tasks that own the
                // accepted connections. Whole-server shutdown therefore closes
                // every still-open client here with `shutdown_code`; the
                // per-connection close code applies when an individual handler
                // exits while the server lives on. `wss.clients` covers vended
                // and not-yet-vended sockets alike (hence the constructor-time
                // requirement that client tracking stays on).
                for (const socket of wss.clients) {
                    if (socket.readyState === WebSocket.OPEN) {
                        socket.close(shutdown_code, shutdown_reason);
                    }
                }
                wss.close();
                yield* race([
                    wss_closed.operation,
                    call(function* () {
                        yield* sleep(shutdown_timeout);
                        for (const socket of wss.clients) {
                            socket.terminate();
                        }
                        return yield* wss_closed.operation;
                    }),
                ]);
            }
            finally {
                wss.off("error", ignore_error);
                wss.off("close", on_wss_close);
            }
        }
    });
}
