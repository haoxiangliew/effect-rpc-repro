import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";

/**
 * Minimal RPC surface:
 * - a unary call (server liveness probe)
 * - a chatty infinite stream (ticks every 500ms)
 * - a quiet watch stream (replays the current value, then emits on change) —
 *   the shape of a settings/config subscription
 * - a mutation for triggering the quiet stream
 */
export const TimeRpcs = RpcGroup.make(
  Rpc.make("time.now", { success: Schema.Finite }),
  Rpc.make("time.watch", { success: Schema.Finite, stream: true }),
  Rpc.make("value.set", { payload: { value: Schema.Finite } }),
  Rpc.make("value.watch", { success: Schema.Finite, stream: true }),
);
