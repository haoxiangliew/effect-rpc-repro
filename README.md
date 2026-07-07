# effect rpc: stream behavior across server restarts

Minimal reproductions against `effect@4.0.0-beta.94` /
`@effect/platform-node@4.0.0-beta.94` (websocket protocol,
`RpcClient.layerProtocolSocket({ retryTransientErrors: true })`,
`RpcSerialization.layerJson`). Models a dev server run with
`node --watch src/index.ts`.

The scripts run under Node (v24, native type stripping); bun is only the
package manager / script runner.

```sh
bun install
bun run control        # scenario A: hard kill (SIGKILL)
bun run graceful-hang  # scenario B: graceful shutdown (SIGTERM)
```

## Scenario A — hard kill: works as designed (baseline)

`control.ts` subscribes two streams (a 500ms ticker and a quiet
SubscriptionRef-changes watch), SIGKILLs the server, restarts it, and probes.

Observed — arguably the correct design:

```
time.watch  FAILED
~effect/rpc/RpcClientError: SocketReadError: An error occurred during Read
    ...
value.watch FAILED
~effect/rpc/RpcClientError: SocketReadError: An error occurred during Read
    ...
-- server answers unary calls again (same client) --
```

In-flight streams fail loudly, the transport reconnects for subsequent
requests, and streams are not auto-resubscribed (the application decides, e.g.
`Stream.retry`). No bug here — this scenario exists as the baseline that makes
scenario B surprising.

## Scenario B — graceful shutdown never completes while a stream is connected

`graceful-hang.ts` subscribes the ticker, then sends SIGTERM (what
`node --watch` and most supervisors send first):

```
ticks in the 8s AFTER SIGTERM: expected 0, got 16
server process exited: false
```

The server keeps serving the stream indefinitely and never exits: the
`Layer.launch`/`NodeRuntime.runMain` teardown appears to wait for open RPC
connections instead of interrupting active subscriptions. Once the client
disconnects, the server does finish shutting down (exit code 130).

Why this matters: under `node --watch` (or any supervisor that escalates
slowly), the old process lingers in a half-alive drain state — quiet streams
look frozen without ever erroring, and the replacement process crashes with
EADDRINUSE. A client connected during this window observes a stream that is
neither failed nor progressing, which is undebuggable from the client side.

Expected: server shutdown interrupts active RPC streams, so connected clients
promptly receive the close/failure from scenario A.

## Files

- `rpcs.ts` — shared RPC group (unary probe, ticker stream, value watch + set)
- `server.ts` — minimal websocket RPC server (port 3210)
- `control.ts` — scenario A orchestrator (spawns/kills the server itself)
- `graceful-hang.ts` — scenario B orchestrator
