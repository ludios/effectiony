import { main, sleep } from "effection";
import { serve, use_web_socket_server } from "../src/ws.ts";

await main(function* () {
  const server = yield* use_web_socket_server({ port: 8080 });
  console.log("listening");
  yield* serve(server, function* (connection, request) {
    // Runs as its own task, once per client. When the peer disconnects,
    // the pending send fails and serve() ends just this task; the
    // server keeps accepting others.
    const ip = request.socket.remoteAddress;
    console.log(`${ip} connected`);
    try {
      for (;;) {
        yield* connection.send(`you're still there, ${ip}`);
        yield* sleep(1_000);
      }
    } finally {
      console.log(`${ip} disconnected`);
    }
  });
});
