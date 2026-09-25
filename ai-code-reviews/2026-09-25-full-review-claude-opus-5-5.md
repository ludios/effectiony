<!-- Model-output: Claude Opus 5.5 -->

# Full review of effectiony (Claude Opus 5.5, 2026-09-25)

Scope: the whole repository at `021bdd4`, not a diff. I checked behaviour against the installed
effection 4.1.0, ws 8.21.3, @nats-io/jetstream 3.4.0 and @effectionx/timebox 0.4.3. The Codex review
ran in parallel. Its findings are in `2026-09-25-full-review-codex-gpt-6-astra.md`, and my assessment
of them is folded in below and marked **(Codex)**.

Baseline: `pnpm lint`, `pnpm check` and `pnpm test` pass (168 tests). A fresh `tsc` build matches the
committed `dist/` exactly.

Labels: **confirmed** means reproduced with a scratch script against real sockets or a real
nats-server (scripts were in `/tmp/effy-exp`, not committed). **reasoned** means established by
reading the code only.

## High

### H1. One client can take down the whole `serve()` server — confirmed (me and Codex)

[src/ws.ts:551](../src/ws.ts#L551)

`serve()` guards each client with a plain `try/catch`. In effection 4, a failure in a *child*
(the `use_connection` resource's socket-error watcher at [ws.ts:356](../src/ws.ts#L356), or anything
the handler `spawn`s) is not thrown into the parent. Instead the parent's trap unwinds it with
`return()`, so the `catch` never runs. The client task then fails, and that failure propagates to the
task running `serve()`.

- A client sends one frame over `maxPayload` (or invalid UTF-8, an unmasked frame, etc.). `run()`
  failed with `Max payload size exceeded`, the server went down, and `on_error` was never called.
  Any client can do this.
- A handler that runs `yield* spawn(() => forward(hub, connection))` beside its read loop (the
  documented fan-out shape) takes the whole server down on an **ordinary** client disconnect: see M1.

Fix (verified: both reproductions become `on_error` calls and the server keeps accepting): wrap the
per-client body in `scoped()`.

```ts
try {
	yield* scoped(function* () {
		const connection = yield* use_connection(socket, connection_options);
		claimed.resolve();
		yield* handler(connection, request);
	});
} catch (error) {
```

Side effect: `on_error` now runs *after* the connection's close handshake rather than before it. The
test "a crashing handler is isolated by serve()" asserts `crashes.length === 1` right after the victim
sees its close, so it needs to wait for `on_error`. Add regression tests for an invalid frame and for a
spawned child that throws.

### H2. An accepted socket is unowned until `use_connection` runs — confirmed (Codex found the crash; I found the message loss)

[src/ws.ts:635](../src/ws.ts#L635)

`on_connection` only queues the socket. Until an accept loop wraps it, the socket has no `message`
listener and no `error` listener.

- **Process crash (Codex):** a malformed frame while the socket waits in the queue emits `error` with
  no listener, and Node exits with code 1. Confirmed.
- **Silent message loss (me):** a client that speaks first (auth or hello on open) loses those
  messages if `serve()` starts even slightly after `use_web_socket_server()`. With `serve()` started
  100 ms late, the first message never arrived; with `serve()` already waiting, it did. The docs
  ("Accepted connections are buffered, so none are dropped before your accept loop subscribes")
  invite exactly this shape.
- A peer that disconnects in that window makes `use_connection` throw
  `invariant violated: socket must be OPEN`, which is noise for a normal event.

Fix (verified; combined with H1 all 9 ws tests pass after the one test adjustment above). Attach an
error listener and pause the socket at accept time, resume it once `use_connection` owns it, and
resume before the shutdown close so the close handshake can be read:

```diff
@@ use_connection
 		socket.on("close",   on_close);
+		socket.resume();
@@ use_web_socket_server
 		const on_connection = (socket: WebSocket, request: IncomingMessage) => {
+			socket.on("error", ignore_error);
+			socket.pause();
 			pending.add({ socket, request });
 		};
@@ close_clients
 				if (socket.readyState === WebSocket.OPEN) {
+					socket.resume();
 					socket.close(shutdown_code, shutdown_reason);
```

Pausing alone is not enough: the bad frame is then read at shutdown, when `close_clients` resumes the
socket, and it crashes there. The accept-time error listener is required.

### H3. `effectiony/nats.js` cannot be imported by a real consumer — confirmed (me and Codex)

[package.json:30](../package.json#L30), [src/nats.ts:49](../src/nats.ts#L49)

`nats.ts` imports `@logtape/logtape` at runtime, but it is only a devDependency. I ran `pnpm pack`,
then a `--prod` install in a scratch project: `import("effectiony/nats.js")` fails with
`ERR_MODULE_NOT_FOUND: @logtape/logtape`, while `merge.js` imports fine. This goes unnoticed locally
because a workspace link installs devDependencies.

Fix: move `@logtape/logtape` to `dependencies`.

## Medium

### M1. `forward()` in `"await"` mode throws on a routine disconnect — confirmed

[src/ws.ts:496](../src/ws.ts#L496)

The docstring says it completes "when the source ends or the peer is gone". With a write in flight
when the peer leaves, it throws `write ECANCELED` instead (5 of 5 disconnects). Before H1 is fixed,
this crashes the server when `forward` is spawned. After H1, every ordinary disconnect is still
reported through `on_error`.

Fix: in `pump`, catch a send failure and `return` if `connection.ready_state !== WebSocket.OPEN`;
rethrow otherwise.

### M2. Shutdown time is not bounded by `shutdown_timeout` — confirmed (me), plus a second case (Codex)

[src/ws.ts:577](../src/ws.ts#L577), [src/ws.ts:696](../src/ws.ts#L696)

- effection halts child tasks **one at a time**, and each connection teardown waits up to its own
  `close_timeout`. Four peers that stopped reading took **4008 ms** to shut down with
  `close_timeout: 1000` and `shutdown_timeout: 1000`. At the 3 s default, 100 dead peers would take
  about 5 minutes, well past a systemd stop timeout.
- (Codex, not re-verified) An HTTP request that never finishes upgrading is not in `wss.clients`, so
  the terminate sweep misses it and `wss_closed` never resolves.

Fix: in `serve()`'s `finally`, after `close_clients()`, wait once for all clients (bounded by
`shutdown_timeout`) and terminate stragglers *before* the connection tasks are halted. Codex's
suggestion for the HTTP case: track and destroy remaining raw sockets at the deadline when the HTTP
server is internal.

### M3. Under a slow consumer, `batch` alternates between singleton and full batches — confirmed; needs a decision

[src/batch.ts:145](../src/batch.ts#L145)

Setup: a buffering source at 1 item/10 ms, `maxTime: 50`, and a consumer taking 200 ms per batch.
Batch sizes were `6, 24, 1, 44, 1, 43, 1, 43, 1, 43, 1, 43`. The item carried over from a timed-out
pull already has an expired window, so it goes out alone, even though a backlog is ready behind it. For
the motivating use (a fixed cost per batch, e.g. one DB round-trip), that halves efficiency exactly when
batching matters most.

Options:

- (a) When the window has already expired, still take whatever is *immediately* available up to
  `maxSize` (e.g. `timebox(0, …)`), costing at most about 1 ms extra latency.
- (b) Restart the window when `next()` is called if the carried item's window has expired, which adds
  up to `maxTime` latency for that item.

I'd pick (a).

### M4. `ensure_durable_nats_consumer` is not idempotent for some configs, and can accept a non-durable consumer — confirmed (me and Codex)

[src/nats.ts:523](../src/nats.ts#L523), [src/nats.ts:565](../src/nats.ts#L565)

- Creation-only values the server omits when they are default (`flow_control: false`,
  `opt_start_seq: 0`, `idle_heartbeat: 0`, `rate_limit_bps: 0`) compare as `undefined !== false`. The
  first boot works; **every restart throws**. Confirmed on nats-server for the first two.
- (Codex) `opt_start_time` comes back normalized (`…00.000Z` becomes `…00Z`), which is also rejected
  as a change.
- (Codex) An existing *ephemeral* consumer with the same name is accepted and updated; it stays
  ephemeral and can expire.

Fix: compare with server-equivalent normalization (treat an omitted value as the zero value, and
compare timestamps as instants). Assert that `info.config.durable_name === consumer_name`. The conflict
is caller misconfiguration, not a broken invariant, so a domain error fits better than `ayy`'s
`AssertionError`.

### M5. `heartbeat()` cannot detect a peer that has stopped reading — Codex-confirmed, reasoning checks out

[src/ws.ts:446](../src/ws.ts#L446)

The `ping()` callback fires only once the frame is written. If the peer stops reading and the send
buffer is full, `heartbeat` waits in `ping()` indefinitely and its deadline never starts. That is the
case heartbeat exists for. Fix: start the deadline independently of the ping flush.

### M6. An invalid `shutdown_code`, `close_code` or close reason breaks cleanup — Codex-confirmed

[src/ws.ts:644](../src/ws.ts#L644)

For example, with `shutdown_code: 1006`, `socket.close()` throws inside `finally` before `wss.close()`
runs, which leaves the server listening. Fix: validate codes and the reason's byte length up front,
next to the existing timeout checks.

### M7. `dist/ws.d.ts` needs `@types/ws`, which consumers don't get — Codex-confirmed

[package.json:32](../package.json#L32)

A strict TypeScript consumer gets TS7016. Fix: make `@types/ws` a dependency, or a documented peer
dependency.

## Low

- **Concurrent `next()` on a `batch` subscription duplicates items** (confirmed by me and Codex): both
  callers await the same `carried` task and both get `["X"]`. Effection subscriptions are
  single-reader in practice, so add an assert (a busy flag reset in `finally`) rather than supporting it.
- **Packaging hygiene** (confirmed):
  - vitest also runs `dist/*.test.js` (168 = 84 × 2). A stale `dist/` would produce confusing failures.
  - `tsconfig` compiles tests into `dist/`, and `"./*": "./dist/*"` exports them.
  - With no `"files"` field, the tarball ships `AGENTS.md`, `src/`, `dev/` and the configs.
  - `pnpm check` runs `tsc` without `--noEmit`, so it rewrites the committed `dist/`.
- **`effection` is a `dependency`, not a `peerDependency`** (reasoned): a consumer on a different
  effection 4.x could get two copies in one process. `@effectionx/timebox` declares it as a peer.
  The same argument applies more weakly to `@nats-io/*`, whose types and `instanceof JetStreamApiError`
  cross this API.
- **`batch` cannot be applied to `use_nats_consumer_messages()` without a cast** (Codex): the stream
  closes with `void`, but `batch` requires `never`. That's odd, given that `batch.ts` exists
  because of NATS consumers.
- **Timer upper bounds** (Codex): `maxTime` over 2³¹−1 is clamped to 1 ms by Node; `adjustable_interval`
  already guards this range, but `batch` and the ws timeouts don't.
- **`ensure_nats_stream` / `ensure_durable_nats_consumer` merge rather than replace** (reasoned from the
  jetstream source, where both `update`s `Object.assign` onto the existing config). Removing a key from
  your config does not reset it on the server. The docstring says "has a particular configuration".
- **Tests** (Codex, reasoned): the "slow client" fan-out test never creates real socket backpressure,
  so the `"drop"`/`"close"` modes are untested. A few tests rely on short sleeps as readiness or
  latency bounds.
- **Style:** `ws.ts` has its own `assert()` (vs `ayy`), which it also uses for argument validation
  under an "invariant violated" message. `dev/ws-demo.ts` is indented with spaces.

## Simplification opportunities

- The `"imports": { "#core/*": … }` map in `package.json` is unused.
- Decide whether `dist/` needs to be committed at all. If consumers install from git, a `prepare`
  script can build on install, and `dist/` plus its churn goes away. If it stays committed, add a CI
  or pre-commit check that it matches a fresh build.

## Looked at and fine

- `merge()` and `adjustable_interval()` hold up within their documented assumptions. Codex agrees.
- The claims in `batch`'s comments about effection internals are right. `each()` subscribes inside its
  own spawned task, so `useScope()` in the subscription returns that task's scope, and awaiting a Task
  does not halt it when the waiter is halted.
- The ConsumerMessages priming and return machinery in `nats.ts` is justified by `stop()`/`close()` in
  the installed jetstream, and the ordered-consumer create and delete lifecycle is correct for 3.4.0.
- `EADDRINUSE` fails fast (6 ms) without hanging teardown.
- The reverse-order child teardown and "body `finally` runs before children are halted" that
  `serve()`'s `close_clients()` relies on hold in effection 4.1.0.
