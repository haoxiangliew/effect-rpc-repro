import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { Clock, Effect, Layer, Logger, Schedule, Stream, SubscriptionRef } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { createServer } from "node:http";

import { TimeRpcs } from "./rpcs.ts";

const TimeHandlersLive = TimeRpcs.toLayer(
  Effect.gen(function* () {
    const value = yield* SubscriptionRef.make(0);

    return TimeRpcs.of({
      "time.now": () => Clock.currentTimeMillis,
      // Chatty: ticks every 500ms forever.
      "time.watch": () =>
        Stream.fromEffectSchedule(Clock.currentTimeMillis, Schedule.spaced("500 millis")),
      // Quiet: replays the current value, then emits on every change —
      // the shape of a settings/config subscription.
      "value.set": ({ value: next }) => SubscriptionRef.set(value, next),
      "value.watch": () => SubscriptionRef.changes(value),
    });
  }),
);

const RpcRoutesLive = RpcServer.layerHttp({
  group: TimeRpcs,
  path: "/rpc",
  protocol: "websocket",
}).pipe(Layer.provide(TimeHandlersLive));

const ServerLive = HttpRouter.serve(RpcRoutesLive).pipe(
  Layer.provide(RpcSerialization.layerJson),
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3210 })),
);

const PrettyLoggerLive = Logger.layer([Logger.consolePretty()]);

Layer.launch(ServerLive).pipe(Effect.provide(PrettyLoggerLive), NodeRuntime.runMain);
