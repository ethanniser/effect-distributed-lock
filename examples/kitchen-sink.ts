/**
 * Example usage of the distributed semaphore library.
 *
 * Run with: bun run example.ts
 * Requires REDIS_URL environment variable.
 */
import { Effect, Console, Duration, Option } from "effect";
import Redis from "ioredis";
import { DistributedSemaphore } from "../src/index.ts";
import { RedisBacking } from "../src/redis.ts";

// Create Redis client
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");

// Create the Redis backing layer
const RedisLayer = RedisBacking.layer(redis, {
  keyPrefix: "example:",
});

// Example 1: Using withPermits for a critical section (mutex behavior)
const example1 = Effect.gen(function* () {
  yield* Console.log("=== Example 1: Mutex with withPermits(1) ===");

  const mutex = yield* DistributedSemaphore.make("my-resource", {
    limit: 1, // Acts as a mutex
    ttl: Duration.seconds(10),
  });

  yield* mutex.withPermits(1)(
    Effect.gen(function* () {
      yield* Console.log("Lock acquired! Doing critical work...");
      yield* Effect.sleep(2000);
      yield* Console.log("Critical work done!");
    })
  );

  yield* Console.log("Lock released automatically");
});

// Example 2: Using withPermitsIfAvailable (non-blocking)
const example2 = Effect.gen(function* () {
  yield* Console.log("\n=== Example 2: withPermitsIfAvailable ===");

  const mutex = yield* DistributedSemaphore.make("another-resource", {
    limit: 1,
    ttl: Duration.seconds(10),
  });

  const result = yield* mutex.withPermitsIfAvailable(1)(
    Effect.gen(function* () {
      yield* Console.log("Got the lock without waiting!");
      return "success";
    })
  );

  if (Option.isSome(result)) {
    yield* Console.log(`Result: ${result.value}`);
  } else {
    yield* Console.log("Could not acquire lock immediately");
  }
});

// Example 3: Semaphore with multiple permits
const example3 = Effect.gen(function* () {
  yield* Console.log("\n=== Example 3: Semaphore with limit=5 ===");

  const sem = yield* DistributedSemaphore.make("pool-resource", {
    limit: 5, // Allow 5 concurrent operations
    ttl: Duration.seconds(10),
  });

  // Acquire 2 permits out of 5
  yield* sem.withPermits(2)(
    Effect.gen(function* () {
      yield* Console.log("Acquired 2 permits (3 still available)");
      const count = yield* sem.currentCount;
      yield* Console.log(`Current active permits: ${count}`);
      yield* Effect.sleep(1000);
    })
  );

  yield* Console.log("Released 2 permits");
});

// Example 4: Manual scope management with take()
const example4 = Effect.gen(function* () {
  yield* Console.log("\n=== Example 4: Manual acquire with Scope ===");

  const mutex = yield* DistributedSemaphore.make("manual-resource", {
    limit: 1,
    ttl: Duration.seconds(10),
  });

  // Using Effect.scoped to manage the lock lifecycle
  yield* Effect.scoped(
    Effect.gen(function* () {
      yield* mutex.take(1);
      yield* Console.log("Permit acquired via take(1)");
      yield* Effect.sleep(1000);
      yield* Console.log("About to exit scope...");
      // Permits are automatically released when scope closes
    })
  );

  yield* Console.log("Scope closed, permit released");
});

// Run all examples
const main = Effect.gen(function* () {
  yield* example1;
  yield* example2;
  yield* example3;
  yield* example4;
  yield* Console.log("\n✓ All examples completed!");
}).pipe(
  Effect.ensuring(Effect.promise(() => redis.quit())),
  Effect.provide(RedisLayer)
);

Effect.runPromise(main).catch(console.error);
