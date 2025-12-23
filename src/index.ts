/**
 * Effect Distributed Lock
 *
 * A distributed mutex library for Effect with pluggable backends.
 *
 * @example
 * ```ts
 * import { DistributedMutex, makeRedisBackingLayer } from "effect-distributed-lock";
 * import { Effect } from "effect";
 * import Redis from "ioredis";
 *
 * const redis = new Redis(process.env.REDIS_URL);
 *
 * const program = Effect.gen(function* () {
 *   // Create a mutex for a specific resource
 *   const mutex = yield* DistributedMutex.make("my-resource-lock", {
 *     ttl: "30 seconds",
 *     acquireTimeout: "10 seconds",
 *   });
 *
 *   // Use the lock
 *   yield* mutex.withLock(
 *     Effect.gen(function* () {
 *       // Critical section - only one process can be here at a time
 *       yield* doSomethingExclusive();
 *     })
 *   );
 * });
 *
 * program.pipe(
 *   Effect.provide(makeRedisBackingLayer(redis)),
 *   Effect.runPromise
 * );
 * ```
 *
 * @module
 */

// Core module (namespace with types and functions)
export * as DistributedMutex from "./DistributedMutex.js";
// Errors
export {
  AcquireTimeoutError,
  BackingError,
  DistributedMutexError,
  LockLostError,
} from "./Errors.js";

// Redis backing
export * as RedisBacking from "./RedisBacking.js";
