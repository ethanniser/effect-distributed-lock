/**
 * Example usage of the distributed mutex library.
 *
 * Run with: bun run example.ts
 * Requires REDIS_URL environment variable.
 */
import { Effect, Console, Duration } from "effect";
import Redis from "ioredis";
import {
  DistributedMutex,
  makeRedisBackingLayer,
} from "./src/index.ts";

// Create Redis client
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");

// Create the Redis backing layer
const RedisLayer = makeRedisBackingLayer(redis, "example:");

// Example 1: Using withLock for a critical section
const example1 = Effect.gen(function* () {
  yield* Console.log("=== Example 1: withLock ===");

  const mutex = yield* DistributedMutex.make("my-resource", {
    ttl: Duration.seconds(10),
    acquireTimeout: Duration.seconds(5),
  });

  yield* mutex.withLock(
    Effect.gen(function* () {
      yield* Console.log("Lock acquired! Doing critical work...");
      yield* Effect.sleep(2000);
      yield* Console.log("Critical work done!");
    })
  );

  yield* Console.log("Lock released automatically");
});

// Example 2: Using tryAcquire (non-blocking)
const example2 = Effect.gen(function* () {
  yield* Console.log("\n=== Example 2: withLockIfAvailable ===");

  const mutex = yield* DistributedMutex.make("another-resource", {
    ttl: Duration.seconds(10),
  });

  const result = yield* mutex.withLockIfAvailable(
    Effect.gen(function* () {
      yield* Console.log("Got the lock without waiting!");
      return "success";
    })
  );

  yield* Console.log(`Result: ${JSON.stringify(result)}`);
});

// Example 3: Manual scope management with acquire()
const example3 = Effect.gen(function* () {
  yield* Console.log("\n=== Example 3: Manual acquire with Scope ===");

  const mutex = yield* DistributedMutex.make("manual-resource", {
    ttl: Duration.seconds(10),
    acquireTimeout: Duration.seconds(5),
  });

  // Using Effect.scoped to manage the lock lifecycle
  yield* Effect.scoped(
    Effect.gen(function* () {
      yield* mutex.acquire;
      yield* Console.log("Lock acquired via acquire()");
      yield* Effect.sleep(1000);
      yield* Console.log("About to exit scope...");
      // Lock is automatically released when scope closes
    })
  );

  yield* Console.log("Scope closed, lock released");
});

// Run all examples
const main = Effect.gen(function* () {
  yield* example1;
  yield* example2;
  yield* example3;
  yield* Console.log("\n✓ All examples completed!");
}).pipe(
  Effect.ensuring(Effect.promise(() => redis.quit())),
  Effect.provide(RedisLayer)
);

Effect.runPromise(main).catch(console.error);

