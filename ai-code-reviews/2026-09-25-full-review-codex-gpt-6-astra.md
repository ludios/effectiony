# Codex full review

The most serious problems are WebSocket error isolation and the missing production dependency for NATS. Several teardown and invariant failures also reproduced.

I reviewed the source, tests, demo, packaging, and installed Effection 4.1.0, NATS 3.4.0, and ws 8.21.3 implementations. In a scratch copy, lint and type-checking passed, and all 168 tests passed—84 source tests plus their compiled copies. Fresh compilation matched checked-in `dist/` exactly. **Repository files remain unchanged.**

1. **High — A single client can bring down `serve()`**
   
   **Location:** [src/ws.ts:552](/home/at/code/js/effectiony/src/ws.ts:552), [src/ws.ts:356](/home/at/code/js/effectiony/src/ws.ts:356)  
   **Evidence:** Confirmed by real-socket experiments.

   A client sends an invalid, unmasked frame → the connection’s error watcher throws → the entire server scope fails. `on_error` is never called. A task spawned by a handler throwing produces the same outcome.

   The surrounding plain `try/catch` catches direct handler exceptions, but cannot catch background child/resource failures under Effection 4.1. This also matches [Effection’s error-boundary documentation](https://frontside.com/effection/guides/v4/errors/).

   **Fix:** Inside each client task’s `try`, wrap both `use_connection()` and the handler in `yield* scoped(...)`. This exact change, tested only in scratch, isolated both failures and invoked `on_error`.

2. **High — An accepted but unclaimed socket can crash the Node process**
   
   **Location:** [src/ws.ts:635](/home/at/code/js/effectiony/src/ws.ts:635)  
   **Evidence:** Confirmed by a real-socket subprocess experiment.

   Start the server without immediately running an accept loop, then send a malformed WebSocket frame. The accepted socket has no `"error"` listener, so Node terminates with an unhandled error event. The listener on `wss` does not handle errors emitted by individual sockets.

   **Fix:** Install error handling synchronously when accepting each socket and retain it until ownership transfers to `use_connection()`. Handle failed/closed queued sockets explicitly.

3. **High — Production consumers cannot import the NATS module**
   
   **Location:** [package.json:30](/home/at/code/js/effectiony/package.json:30), [src/nats.ts:49](/home/at/code/js/effectiony/src/nats.ts:49)  
   **Evidence:** Confirmed in an isolated copy containing only the declared production dependency closure.

   `import("effectiony/nats.js")` fails with `ERR_MODULE_NOT_FOUND` for `@logtape/logtape`. The compiled module imports it unconditionally, but it is only a dev dependency.

   **Fix:** Move `@logtape/logtape` to `dependencies`.

4. **Medium — Overlapping batch reads duplicate items**
   
   **Location:** [src/batch.ts:115](/home/at/code/js/effectiony/src/batch.ts:115), [src/batch.ts:122](/home/at/code/js/effectiony/src/batch.ts:122)  
   **Evidence:** Confirmed by experiment.

   With `maxSize: 1`, start two concurrent `subscription.next()` calls, then enqueue `"A"` and `"B"`. Both calls return `["A"]`; the following call returns `["B"]`. Both readers await the same `carried` task.

   **Fix:** Enforce one active `next()` per subscription with an assertion and a `finally` reset, or serialize reads. An explicit invariant is sufficient if concurrent reads are intentionally unsupported.

5. **Medium — `ensure_durable_nats_consumer()` can return an ephemeral consumer**
   
   **Location:** [src/nats.ts:554](/home/at/code/js/effectiony/src/nats.ts:554), [src/nats.ts:564](/home/at/code/js/effectiony/src/nats.ts:564)  
   **Evidence:** Confirmed against a real JetStream server.

   Create a named ephemeral consumer, then call `ensure_durable_nats_consumer()` with that name. The helper reports success and updates it, but `config.durable_name` remains absent. The consumer can subsequently expire, contrary to the caller’s durable-consumer assumption.

   **Fix:** Assert that an existing consumer’s `durable_name` matches `consumer_name` before updating. Report the conflict without automatically deleting existing state.

6. **Medium — Heartbeat detection stalls behind a blocked write**
   
   **Location:** [src/ws.ts:446](/home/at/code/js/effectiony/src/ws.ts:446)  
   **Evidence:** Confirmed with a paused real client socket.

   Pause the peer’s reads and fill the server’s outgoing buffer. `heartbeat()` waits indefinitely in `connection.ping()` before starting its timeout. With a 25 ms period, my connection remained OPEN after approximately 300 ms with about 22 MB buffered.

   The ws ping callback runs when the frame is written, so waiting for it can itself block. See the [ws API documentation](https://github.com/websockets/ws/blob/master/doc/ws.md).

   **Fix:** Make the heartbeat deadline independent of ping flushing—either time-bound the ping operation or run a watchdog that can terminate the socket while the write remains pending.

7. **Medium — `shutdown_timeout` does not bound server shutdown**
   
   **Location:** [src/ws.ts:577](/home/at/code/js/effectiony/src/ws.ts:577), [src/ws.ts:698](/home/at/code/js/effectiony/src/ws.ts:698)  
   **Evidence:** Two failure scenarios confirmed experimentally.

   An incomplete HTTP request never enters `wss.clients`. At timeout, terminating that set does nothing, and teardown continues awaiting `wss_closed`. With a 20 ms timeout, shutdown remained blocked after 250 ms and completed only when I destroyed the HTTP socket.

   Separately, with `serve()`, connection resources unwind before the server resource starts its timer. Three nonresponsive clients with `close_timeout: 150` and `shutdown_timeout: 10` took approximately 454 ms to shut down.

   **Fix:** Begin shutdown and its shared deadline before awaiting handler teardown. For an internally owned HTTP server, also track and destroy remaining non-upgraded sockets at the deadline. Preserve ownership of externally supplied HTTP servers.

8. **Medium — Invalid close parameters break cleanup and leak resources**
   
   **Location:** [src/ws.ts:393](/home/at/code/js/effectiony/src/ws.ts:393), [src/ws.ts:644](/home/at/code/js/effectiony/src/ws.ts:644)  
   **Evidence:** Confirmed experimentally.

   Set `shutdown_code: 1006`, connect a client, then exit scope. `socket.close()` throws before `wss.close()` and the timeout are reached. The resource exits with an error while the server remains listening and the socket remains CLOSING. An oversized close reason has the same underlying problem.

   **Fix:** Validate close codes and the reason’s UTF-8 byte length before acquiring resources. Ensure cleanup still stops the server and terminates sockets if initiating a graceful close fails.

9. **Medium — Consumer configuration comparison breaks idempotent startup**
   
   **Location:** [src/nats.ts:523](/home/at/code/js/effectiony/src/nats.ts:523)  
   **Evidence:** Confirmed against a real JetStream server.

   The first ensure call succeeds, but an identical second call fails for configurations containing `flow_control: false`, `idle_heartbeat: 0`, or `rate_limit_bps: 0`: the server omits those default-valued fields.

   Likewise, `opt_start_time: "2026-01-01T00:00:00.000Z"` comes back as `"2026-01-01T00:00:00Z"` and is incorrectly rejected as a creation-only change.

   **Fix:** Compare creation-only settings using server-equivalent normalization: account for omitted defaults and equivalent timestamps while continuing to reject actual configuration changes.

10. **Medium — Public WebSocket declarations depend on unpublished type dependencies**
    
    **Location:** [package.json:32](/home/at/code/js/effectiony/package.json:32), [dist/ws.d.ts:25](/home/at/code/js/effectiony/dist/ws.d.ts:25)  
    **Evidence:** Confirmed with a separate strict TypeScript consumer.

    A consumer with Node types installed imports `effectiony/ws.js` → compilation fails with TS7016 because the public declarations import `ws`, whose declarations come from `@types/ws`. That package is only a dev dependency.

    **Fix:** Include `@types/ws` in dependencies, or explicitly declare and document it as a required peer dependency. Check declarations from a consumer fixture without the repository’s dev dependencies.

11. **Low — The advertised NATS batching use case does not type-check**
    
    **Location:** [src/batch.ts:81](/home/at/code/js/effectiony/src/batch.ts:81), [src/nats.ts:896](/home/at/code/js/effectiony/src/nats.ts:896)  
    **Evidence:** Confirmed by compilation.

    `batch({ maxTime: 100 })(use_nats_consumer_messages(consumer))` produces TS2345: the NATS stream closes with `void`, while `batch()` requires `never`. The same restriction prevents directly batching `merge()` output.

    This is an API composition limitation, rather than incorrectly generated declarations.

    **Fix:** Consider generalizing batching to preserve a source’s completion type and flush its final partial batch. Alternatively, provide an explicit adapter enforcing the intended never-ending contract instead of requiring unchecked casts.

12. **Low — Accepted timer values can produce radically shorter waits**
    
    **Location:** [src/batch.ts:85](/home/at/code/js/effectiony/src/batch.ts:85), [src/ws.ts:434](/home/at/code/js/effectiony/src/ws.ts:434)  
    **Evidence:** Batch behavior confirmed experimentally; the same ws validation gap is established by inspection.

    `maxTime: 2 ** 31 + 1000` passes validation, but Node clamps the timer to 1 ms. A batch intended to wait roughly 25 days emitted after approximately 6 ms. WebSocket heartbeat and teardown timing options also lack an upper bound.

    **Fix:** Reuse the timer-range policy already implemented by `adjustable_interval()`, or implement long waits in bounded chunks.

13. **Low — Build and package configuration ship and run compiled tests**
    
    **Location:** [tsconfig.json:3](/home/at/code/js/effectiony/tsconfig.json:3), [package.json:6](/home/at/code/js/effectiony/package.json:6), [package.json:13](/home/at/code/js/effectiony/package.json:13)  
    **Evidence:** Confirmed by test discovery and packing a clean scratch copy.

    Compilation includes `src/*.test.ts`, publishing includes their JavaScript and declarations, and wildcard exports expose them. Vitest runs both source and compiled suites. The tarball also includes source tests, the demo, and development configuration.

    Additionally, `pnpm check` emits into `dist/`; it is not a read-only check.

    **Fix:** Separate build and checking configurations, make checking use `--noEmit`, exclude tests from production compilation, clean old generated test artifacts, restrict Vitest discovery, and define package contents explicitly.

14. **Low — The slow-client test does not exercise network backpressure**
    
    **Location:** [src/ws.test.ts:122](/home/at/code/js/effectiony/src/ws.test.ts:122)  
    **Evidence:** Reasoned from the test and connection implementation.

    The “slow” client sleeps between subscription reads, while `use_connection()` continues receiving frames into its unbounded queue. Ten small messages do not establish a blocked server write. The test therefore demonstrates independent application queues, but cannot establish the claimed behavior under socket backpressure or validate `"drop"`/`"close"` modes.

    **Fix:** Pause actual socket reads, send enough data to establish a blocked write, assert that backpressure occurred, then verify each policy and that other clients still progress.

15. **Low — Several tests assume bounded scheduler latency**
    
    **Location:** [src/adjustable_interval.test.ts:188](/home/at/code/js/effectiony/src/adjustable_interval.test.ts:188), [src/batch.test.ts:268](/home/at/code/js/effectiony/src/batch.test.ts:268), [src/ws.test.ts:196](/home/at/code/js/effectiony/src/ws.test.ts:196)  
    **Evidence:** Reasoned; no flaky failure occurred in the baseline run.

    A scheduler pause can push the correctly anchored interval tick past the test’s 590 ms upper bound. Similarly, `sleep(30)` does not guarantee that a message arrives inside an 80 ms batch window, and `sleep(50)` does not establish that a client has connected before shutdown.

    **Fix:** Use controlled timers for interval/batch semantics and explicit readiness signals for socket tests. Retain real-time integration checks where needed, without assuming a short sleep proves readiness.

I found no additional substantive defect in `merge()` or `adjustable_interval()` within their documented operating assumptions. I also checked the JetStream iterator cleanup against its installed queue implementation; the priming and return machinery is justified.

Style only: the demo’s space indentation and ws’s local assertion helper depart from the repository conventions. These are secondary to the correctness fixes above.