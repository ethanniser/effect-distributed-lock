/**
 * Redis backing for Effect Distributed Semaphore
 *
 * This module provides Redis-based backing implementation for distributed semaphores.
 * Import from "effect-distributed-lock/redis" to use Redis functionality.
 *
 * **Important:** This implementation is for single-instance Redis only.
 * It does not implement the Redlock algorithm and should not be used with
 * Redis Cluster or Redis Sentinel for distributed locking guarantees.
 *
 * @example
 * ```ts
 * import { DistributedSemaphore } from "effect-distributed-lock";
 * import { RedisBacking } from "effect-distributed-lock/redis";
 * import { Effect } from "effect";
 * import Redis from "ioredis";
 *
 * const redis = new Redis(process.env.REDIS_URL);
 *
 * const program = Effect.gen(function* () {
 *   const sem = yield* DistributedSemaphore.make("my-resource", {
 *     limit: 5,
 *     ttl: "30 seconds",
 *   });
 *
 *   yield* sem.withPermits(2)(
 *     Effect.gen(function* () {
 *       yield* doSomethingLimited();
 *     })
 *   );
 * });
 *
 * program.pipe(
 *   Effect.provide(RedisBacking.layer(redis)),
 *   Effect.runPromise
 * );
 * ```
 *
 * @module
 */

export * as RedisBacking from "./RedisBacking.js";
