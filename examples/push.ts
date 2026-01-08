/**
 * Demonstrates concurrent effects competing for a distributed lock.
 *
 * This example runs two scenarios:
 * 1. With push-based acquisition DISABLED (polling only)
 * 2. With push-based acquisition ENABLED (pub/sub notifications)
 *
 * You'll see how push-based acquisition is faster because waiters are
 * notified immediately when permits are released, rather than polling.
 *
 * Run with: bun run examples/push.ts
 * Requires REDIS_URL environment variable or local Redis at localhost:6379.
 */
import { Console, Duration, Effect, Schedule } from "effect";
import Redis from "ioredis";
import { DistributedSemaphore } from "../src/index.ts";
import { RedisBacking } from "../src/redis.ts";

const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");

// Helper to create a task that competes for the lock
const makeTask = (
  id: number,
  mutex: DistributedSemaphore.DistributedSemaphore
) =>
  Effect.gen(function* () {
    yield* Console.log(`[Task ${id}] Starting, waiting for lock...`);
    const startWait = Date.now();

    yield* mutex.withPermits(1)(
      Effect.gen(function* () {
        const waitTime = Date.now() - startWait;
        yield* Console.log(
          `[Task ${id}] 🔒 Lock acquired! (waited ${waitTime}ms)`
        );

        // Simulate some work
        yield* Effect.sleep(Duration.millis(200));

        yield* Console.log(`[Task ${id}] 🔓 Releasing lock...`);
      })
    );

    yield* Console.log(`[Task ${id}] Done`);
  });

// Run a scenario with the given configuration
const runScenario = (name: string, pushEnabled: boolean) =>
  Effect.gen(function* () {
    yield* Console.log(`\n${"=".repeat(60)}`);
    yield* Console.log(`${name}`);
    yield* Console.log(`Push-based acquisition: ${pushEnabled ? "ON" : "OFF"}`);
    yield* Console.log(`${"=".repeat(60)}\n`);

    const startTime = Date.now();

    // Create mutex with a unique key per scenario to avoid interference
    const mutex = yield* DistributedSemaphore.make(
      `concurrent-example-${pushEnabled ? "push" : "poll"}`,
      {
        acquireRetryPolicy: Schedule.spaced(Duration.millis(500)),
        limit: 1, // Mutex - only one holder at a time
      }
    );

    // Run 3 tasks concurrently, all competing for the same lock
    yield* Effect.all(
      [makeTask(1, mutex), makeTask(2, mutex), makeTask(3, mutex)],
      { concurrency: 3 }
    );

    const totalTime = Date.now() - startTime;
    yield* Console.log(`\n⏱️  Total time: ${totalTime}ms\n`);
  });

// Run both scenarios
const main = Effect.gen(function* () {
  yield* Console.log("🚀 Distributed Lock Concurrency Demo");
  yield* Console.log(
    "Showing 3 concurrent tasks competing for a mutex (limit=1)"
  );

  // Run WITHOUT push (polling only)
  const RedisLayerNoPush = RedisBacking.layer(redis, {
    keyPrefix: "concurrent-demo:",
    pushBasedAcquireEnabled: false,
  });
  yield* runScenario("Scenario 1: Polling Only", false).pipe(
    Effect.provide(RedisLayerNoPush)
  );

  // Run WITH push (pub/sub notifications)
  const RedisLayerWithPush = RedisBacking.layer(redis, {
    keyPrefix: "concurrent-demo:",
    pushBasedAcquireEnabled: true,
  });
  yield* runScenario("Scenario 2: Push-Based (Pub/Sub)", true).pipe(
    Effect.provide(RedisLayerWithPush)
  );

  yield* Console.log("✅ Demo complete!");
  yield* Console.log(
    "Notice how push-based acquisition completes faster because"
  );
  yield* Console.log(
    "waiters are notified immediately instead of waiting for the next poll.\n"
  );
}).pipe(Effect.ensuring(Effect.promise(() => redis.quit())));

Effect.runPromise(main).catch(console.error);
