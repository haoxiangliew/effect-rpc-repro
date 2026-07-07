import {
  NodeChildProcessSpawner,
  NodeFileSystem,
  NodePath,
  NodeRuntime,
  NodeSocket,
} from "@effect/platform-node";
import { Cause, Effect, Layer, Logger, Ref, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";

import { TimeRpcs } from "./rpcs.ts";

// killSignal is SIGKILL so scope cleanup actually kills the server — SIGTERM
// (the default) is exactly the signal it hangs on.
const serverCommand = ChildProcess.make(process.execPath, ["server.ts"], {
  cwd: import.meta.dirname,
  killSignal: "SIGKILL",
  stderr: "ignore",
  stdin: "ignore",
  stdout: "ignore",
});

const program = Effect.gen(function* () {
  const server = yield* serverCommand;
  const client = yield* RpcClient.make(TimeRpcs);

  const ticks = yield* Ref.make(0);
  yield* Effect.forkChild(
    client["time.watch"]().pipe(
      Stream.tap(() => Ref.update(ticks, (count) => count + 1)),
      Stream.runDrain,
      Effect.tapError((error) => Effect.logWarning("stream failed", Cause.fail(error))),
      Effect.ignore,
    ),
  );

  yield* Effect.sleep("2 seconds");
  const ticksBefore = yield* Ref.get(ticks);
  yield* Effect.log(`ticks before SIGTERM: ${ticksBefore}`);

  yield* Effect.log("-- SIGTERM (graceful shutdown) --");
  // kill only resolves once the process exits — which is the bug under test —
  // so fork it to send the signal without blocking on exit.
  yield* Effect.forkChild(server.kill({ killSignal: "SIGTERM" }));

  yield* Effect.sleep("8 seconds");
  const ticksAfter = yield* Ref.get(ticks);
  const running = yield* server.isRunning;
  yield* Effect.log(`ticks in the 8s AFTER SIGTERM: expected 0, got ${ticksAfter - ticksBefore}`);
  yield* Effect.log(`server process exited: ${!running}`);
  yield* running
    ? Effect.logError(
        "BUG: graceful shutdown never completes while an RPC stream is connected;\n" +
          "the server keeps serving indefinitely. Under a supervisor like node --watch\n" +
          "this leaves a half-dead server running while its replacement fails EADDRINUSE.",
      )
    : Effect.log("Server shut down; no hang observed.");
});

const ProtocolLive = RpcClient.layerProtocolSocket({ retryTransientErrors: true }).pipe(
  Layer.provide(NodeSocket.layerWebSocket("ws://127.0.0.1:3210/rpc")),
  Layer.provide(RpcSerialization.layerJson),
);

const SpawnerLive = NodeChildProcessSpawner.layer.pipe(
  Layer.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer)),
);

const PrettyLoggerLive = Logger.layer([Logger.consolePretty()]);

program.pipe(
  Effect.provide(Layer.mergeAll(ProtocolLive, SpawnerLive, PrettyLoggerLive)),
  Effect.scoped,
  NodeRuntime.runMain,
);
