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
import type { Operation, Stream } from "effection";
import { WebSocket, WebSocketServer } from "ws";
import type { RawData, ServerOptions } from "ws";
import type { IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
/** Any payload acceptable to `WebSocket#send`. */
export type SendData = Parameters<WebSocket["send"]>[0];
/** The subset of EventEmitter these helpers rely on. */
export interface EmitterLike {
    on(event: string, listener: (...args: never[]) => void): unknown;
    off(event: string, listener: (...args: never[]) => void): unknown;
}
/** One inbound WebSocket message. */
export interface WsMessage {
    /** Raw payload. With default `binaryType`, a Buffer (or Buffer[]) for both text and binary frames. */
    data: RawData;
    /** True for binary frames; text frames arrive with false (call `data.toString()` yourself). */
    is_binary: boolean;
}
/** How a connection ended; this is the stream's close value. */
export interface WsClose {
    /** RFC 6455 status code (1000 normal, 1001 going away, 1006 abnormal, ...). */
    code: number;
    /** Peer-supplied close reason; empty Buffer when none was given. */
    reason: Buffer;
}
/**
 * A live WebSocket bound to the current scope. Subscribing to it (it is a
 * Stream) yields inbound messages until the socket closes, at which point
 * the stream completes with a {@link WsClose}. There is no close method:
 * the socket is closed when the owning scope exits.
 */
export interface WsConnection extends Stream<WsMessage, WsClose> {
    /** Escape hatch to the underlying `ws` socket. */
    socket: WebSocket;
    /** Live `WebSocket.readyState` (CONNECTING=0, OPEN=1, CLOSING=2, CLOSED=3). */
    readonly ready_state: number;
    /** Bytes queued by `send()` but not yet handed to the OS; a lag gauge for this peer. */
    readonly buffered_amount: number;
    /** Negotiated subprotocol; empty string when none. */
    readonly protocol: string;
    /**
     * Send a frame; the operation completes when the bytes are written out
     * (i.e. it carries the socket's write backpressure) and fails if the
     * write fails or the socket is no longer open.
     * @param data - payload to send
     * @param options - `ws` send options (binary, compress, fin, mask)
     * @returns an operation resolving once the frame is flushed
     */
    send(data: SendData, options?: Parameters<WebSocket["send"]>[1]): Operation<void>;
    /**
     * Send a ping frame.
     * @param data - optional ping payload
     * @returns an operation resolving once the ping is written out
     */
    ping(data?: Buffer): Operation<void>;
}
/** Options for {@link use_connection}. */
export interface ConnectionOptions {
    /** Close code sent when the owning scope exits; default 1000 (normal closure). */
    close_code?: number;
    /** Close reason sent when the owning scope exits; default "resource released". */
    close_reason?: string;
    /** Milliseconds to wait for the close handshake before `terminate()`; default 3000. */
    close_timeout?: number;
}
/**
 * A newly accepted, already-OPEN server-side socket plus its upgrade request.
 * Until {@link use_connection} initializes, the socket has no owner other
 * than the server resource itself (which will close it at shutdown), so wrap
 * it promptly — or use {@link serve}, which makes ownership transfer atomic
 * with accepting.
 */
export interface WsConnectionRequest {
    /** The raw socket; wrap it with {@link use_connection} inside the task that owns it. */
    socket: WebSocket;
    /** The HTTP GET upgrade request — headers, URL, cookies, auth material. */
    request: IncomingMessage;
}
/**
 * A listening WebSocket server bound to the current scope. Subscribing to it
 * (it is a Stream) yields one {@link WsConnectionRequest} per client; the
 * stream never completes on its own — it runs until the scope exits.
 */
export interface WsServer extends Stream<WsConnectionRequest, never> {
    /** Escape hatch to the underlying `WebSocketServer`. */
    socket_server: WebSocketServer;
    /**
     * Bound address of the underlying HTTP server (null before listening or
     * after close, string for pipes). Passes through to `ws`, which throws in
     * `noServer` mode — there is no address to report there.
     */
    address(): AddressInfo | string | null;
}
/**
 * Options for {@link use_web_socket_server}: everything `ws` accepts (except
 * `clientTracking`, which must stay on: shutdown enumerates `wss.clients` to
 * close every live client, and without it a server with connected clients
 * never finishes tearing down), plus shutdown policy.
 */
export interface WsServerOptions extends Omit<ServerOptions, "clientTracking"> {
    /** Milliseconds to wait for graceful client closes on teardown before `terminate()`; default 3000. */
    shutdown_timeout?: number;
    /** Close code sent to still-open clients on teardown; default 1001 (going away). */
    shutdown_code?: number;
    /** Close reason sent to still-open clients on teardown; default "server shutting down". */
    shutdown_reason?: string;
}
/** Options for {@link forward}. */
export interface ForwardOptions<T> {
    /**
     * Convert a stream item into wire data; default is the identity function,
     * so pre-encoded streams need no option.
     */
    encode?: (item: T) => SendData;
    /**
     * Slow-peer policy. "await" (default) waits for each frame to flush, so a
     * slow peer only slows its own forwarder — but note that while it waits,
     * a multicast source (Signal/Channel) keeps growing that subscriber's
     * queue without bound, so a peer that lags forever costs memory. "drop"
     * and "close" never wait, and thereby bound the queue: once
     * `buffered_amount` exceeds `high_water_mark`, "drop" discards items and
     * "close" disconnects the peer with 1013 (try again later).
     */
    mode?: "await" | "drop" | "close";
    /** Byte threshold for the "drop"/"close" modes; default 1 MiB. */
    high_water_mark?: number;
}
/** Options for {@link serve}. */
export interface ServeOptions {
    /** Passed through to each {@link use_connection}. */
    connection?: ConnectionOptions;
    /**
     * Called when a handler task fails; the failed client's socket is closed
     * by its resource regardless, and the server keeps serving. If the
     * callback itself throws, that error is reported on stderr and otherwise
     * ignored — an error observer must not become a server error. Default:
     * the error is discarded — supply a callback if you want to observe
     * crashes.
     */
    on_error?: (error: unknown, request: IncomingMessage) => void;
}
/** Options for {@link heartbeat}. */
export interface HeartbeatOptions {
    /**
     * Milliseconds between pings; a peer that produces no pong for a full
     * period is terminated (same flag algorithm as the `ws` README).
     * Default 30000.
     */
    period?: number;
}
/**
 * Stream the arguments of every emission of `event` on a Node EventEmitter.
 * The listener is attached for exactly the lifetime of the surrounding scope.
 * (Counterpart of effection's EventTarget-only `on()`; same shape as
 * `@effectionx/node`.)
 * @param emitter - the event source
 * @param event - the event name
 * @returns a Stream of listener-argument tuples that never completes
 */
export declare function on_emitter<T extends unknown[] = unknown[]>(emitter: EmitterLike, event: string): Stream<T, never>;
/**
 * Wait for the next emission of `event` on a Node EventEmitter. Runs in its
 * own scope so the listener detaches as soon as the event arrives.
 * @param emitter - the event source
 * @param event - the event name
 * @returns an operation yielding the listener-argument tuple
 */
export declare function once_emitter<T extends unknown[] = unknown[]>(emitter: EmitterLike, event: string): Operation<T>;
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
export declare function use_connection(socket: WebSocket, options?: ConnectionOptions): Operation<WsConnection>;
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
export declare function heartbeat(connection: WsConnection, options?: HeartbeatOptions): Operation<void>;
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
export declare function forward<T>(source: Stream<T, unknown>, connection: WsConnection, options?: ForwardOptions<T>): Operation<void>;
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
export declare function serve(server: WsServer, handler: (connection: WsConnection, request: IncomingMessage) => Operation<void>, options?: ServeOptions): Operation<never>;
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
export declare function use_web_socket_server(options: WsServerOptions): Operation<WsServer>;
