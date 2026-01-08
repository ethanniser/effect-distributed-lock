/**
 * Effect Distributed Semaphore
 *
 * A distributed semaphore library for Effect with pluggable backends.
 * Implements a multi-semaphore that can be used to implement:
 * - Mutex (limit=1, permits=1)
 * - Semaphore (limit=N, permits=1)
 * - Multi-semaphore (limit=N, permits=M)
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
 *   // Create a semaphore that allows 5 concurrent operations
 *   const sem = yield* DistributedSemaphore.make("my-resource", {
 *     limit: 5,
 *     ttl: "30 seconds",
 *   });
 *
 *   // Acquire 2 permits, run effect, release when done
 *   yield* sem.withPermits(2)(
 *     Effect.gen(function* () {
 *       // Only 2 of the 5 slots are used
 *       yield* doSomethingLimited();
 *     })
 *   );
 *
 *   // For mutex behavior, use limit=1 and withPermits(1)
 *   const mutex = yield* DistributedSemaphore.make("my-lock", { limit: 1 });
 *   yield* mutex.withPermits(1)(criticalSection);
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

// Backing interface
export * as Backing from "./Backing.js";
export {
  DistributedSemaphoreBacking,
  SemaphoreBackingError,
} from "./Backing.js";

// Core module (namespace with types and functions)
export * as DistributedSemaphore from "./DistributedSemaphore.js";

// Errors
export { LockLostError } from "./Errors.js";
