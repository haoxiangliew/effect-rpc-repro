import {
  NodeChildProcessSpawner,
  NodeFileSystem,
  NodePath,
  NodeRuntime,
  NodeSocket,
} from "@effect/platform-node";
import { Effect, Layer, Logger, Schedule, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";

import { TimeRpcs } from "./rpcs.ts";

// killSignal is SIGKILL because graceful SIGTERM shutdown blocks on the open
// stream connections (scenario B), so scope cleanup must hard-kill instead.
const serverCommand = ChildProcess.make(process.execPath, ["server.ts"], {
  cwd: import.meta.dirname,
  killSignal: "SIGKILL",
  stderr: "inherit",
  stdin: "ignore",
  stdout: "inherit",
});

const observe = <E>(label: string, stream: Stream.Stream<number, E>) =>
  stream.pipe(
    Stream.tap((element) => Effect.log(`${label} <- ${element}`)),
    Stream.runDrain,
    Effect.tapCause((cause) => Effect.logWarning(`${label} FAILED`, cause)),
    Effect.ensuring(Effect.log(`${label} ended`)),
    Effect.ignore,
  );

const program = Effect.gen(function* () {
  const server = yield* serverCommand;
  const client = yield* RpcClient.make(TimeRpcs);

  // Polls the unary rpc until the server answers.
  const untilServerAnswers = client["time.now"]().pipe(
    Effect.retry(Schedule.spaced("250 millis")),
    Effect.timeout("10 seconds"),
  );

  yield* Effect.forkChild(observe("time.watch ", client["time.watch"]()));
  yield* Effect.forkChild(observe("value.watch", client["value.watch"]()));

  yield* Effect.sleep("2 seconds");

  yield* Effect.log("-- killing server (SIGKILL, awaiting exit) --");
  yield* server.kill({ killSignal: "SIGKILL" });
  yield* Effect.sleep("1 second");
  yield* Effect.log("-- starting server --");
  yield* serverCommand;
  // Scope finalizers run LIFO, so this delay runs after the client socket has
  // closed and before the server is SIGKILLed — keeping the server alive long
  // enough for its side of the disconnect to show up in the shared console.
  yield* Effect.addFinalizer(() => Effect.sleep("1 second"));
  yield* untilServerAnswers;
  yield* Effect.log("-- server answers unary calls again (same client) --");

  yield* Effect.sleep("3 seconds");
  yield* Effect.log("-- value.set(42): does the quiet watch still deliver changes? --");
  yield* client["value.set"]({ value: 42 });
  yield* Effect.sleep("3 seconds");
  yield* Effect.log("-- done --");
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
