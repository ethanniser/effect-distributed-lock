# effect-distributed-lock

*WARNING: This is still in active development, possibly has bugs (distributed systems are hard!), and is subject to change.*

A distributed semaphore library for [Effect](https://effect.website/) with pluggable backends.

It's like the built in `Effect.Semaphore`, but asynchronously distributed across multiple processes/services!

## Features

- **Distributed semaphore** — control concurrent access across multiple processes/services
- **Scope-based resource management** — permits are automatically released when the scope closes
- **Automatic TTL refresh** — keeps permits alive while held, prevents deadlocks if holder crashes
- **Pluggable backends** — ships with Redis (single-instance), easy to implement others
- **Push-based waiting** — uses pub/sub for efficient notification when permits become available (optional, with polling fallback)
- **Configurable retry policies** — control polling interval, TTL, and backing failure retry behavior
- **Type-safe errors** — tagged errors for precise error handling

## Installation

```bash
npm install effect-distributed-lock effect
# or
bun add effect-distributed-lock effect

# For Redis backing (optional)
npm install ioredis
```

## Quick Start

```typescript
import { Effect, Schedule } from "effect";
import Redis from "ioredis";
import { DistributedSemaphore } from "effect-distributed-lock";
import { RedisBacking } from "effect-distributed-lock/redis";

const redis = new Redis(process.env.REDIS_URL);
const RedisLayer = RedisBacking.layer(redis, { keyPrefix: "my-app:" });

const program = Effect.gen(function* () {
  // Create a semaphore that allows 5 concurrent operations
  const sem = yield* DistributedSemaphore.make("my-resource", {
    limit: 5,
    ttl: "10 seconds",
    refreshInterval: "3 seconds",
    acquireRetryInterval: "500 millis",
    backingFailureRetryPolicy: Schedule.exponential("100 millis"),
  });

  // Acquire 2 permits, run effect, release automatically after
  yield* doWork.pipe(sem.withPermits(2));
});

program.pipe(Effect.provide(RedisLayer), Effect.runPromise);
```

## API

### Creating a Semaphore

```typescript
const sem = yield* DistributedSemaphore.make(key, config);
```

| Config Option                | Type             | Default      | Description                                |
| ---------------------------- | ---------------- | ------------ | ------------------------------------------ |
| `limit`                      | `number`         | `1`          | Max permits (1 = mutex behavior)           |
| `ttl`                        | `DurationInput`  | `30 seconds` | Permit TTL (auto-releases if holder crashes) |
| `refreshInterval`            | `DurationInput`  | `ttl / 3`    | How often to refresh TTL while holding     |
| `acquireRetryInterval`       | `DurationInput`  | `100ms`      | Polling interval when waiting to acquire   |
| `backingFailureRetryPolicy`  | `Schedule<void>` | `100ms`      | Retry schedule for backing store failures  |

### Using the Semaphore

#### `withPermits` — Acquire, run, release

The simplest and recommended way. Acquires permits (waiting if needed), runs your effect, and releases when done:

```typescript
// Acquire 2 permits out of limit
yield* myEffect.pipe(sem.withPermits(2));

// For mutex behavior (limit=1), use withPermits(1)
yield* criticalSection.pipe(mutex.withPermits(1));
```

#### `withPermitsIfAvailable` — Non-blocking acquire

Tries to acquire immediately without waiting. Returns `Option.some(result)` if successful, `Option.none()` if not enough permits:

```typescript
const result = yield* myEffect.pipe(sem.withPermitsIfAvailable(1));
if (Option.isSome(result)) {
  console.log("Got the permit!", result.value);
} else {
  console.log("No permits available, skipping");
}
```

#### `take` / `tryTake` — Manual scope control

For advanced use cases where you need explicit control over the permit lifecycle:

```typescript
yield* Effect.scoped(
  Effect.gen(function* () {
    const keepAliveFiber = yield* sem.take(2); // 2 permits held until scope closes
    yield* doWork;
    // Permits automatically released + keepalive fiber interrupted here
  })
);
```

Both `take` and `tryTake` return the keepalive fiber that refreshes the permit TTL.

⚠️ **CRITICAL**: Errors from the keepalive fiber (losing permits or backing store failure) mean **the lock is effectively lost**. You **must** join this fiber at some point in your program to detect these failures. If the keepalive fiber errors and you don't join it, your program will continue running without holding the lock, potentially leading to race conditions or data corruption.

**It is highly recommended to use `withPermits` or `withPermitsIfAvailable` instead**, which automatically manage the keepalive fiber lifecycle and propagate errors for you. Only use `take`/`tryTake` if you need explicit scope control and understand the responsibility of managing the keepalive fiber.

### Acquire Options

All acquire methods (`withPermits`, `withPermitsIfAvailable`, `take`, `tryTake`) accept an optional second parameter for advanced use cases:

```typescript
yield* myEffect.pipe(sem.withPermits(1, { identifier: "my-custom-id" }));
```

| Option              | Type      | Default              | Description                                      |
| ------------------- | --------- | -------------------- | ------------------------------------------------ |
| `identifier`        | `string`  | `crypto.randomUUID()` | Unique ID for this permit holder                 |
| `acquiredExternally`| `boolean` | `false`              | Assume permits already held, use refresh to verify |

#### Custom Identifiers

By default, a random UUID is generated for each acquire. Override this for:
- **Debugging/observability**: Use meaningful identifiers to trace lock holders
- **Cross-process handoff**: Share identifiers between processes

```typescript
// Custom identifier for debugging
yield* myEffect.pipe(sem.withPermits(1, { identifier: "worker-1-job-123" }));
```

⚠️ **Warning**: Identifiers must be unique across concurrent holders. Using the same identifier from different processes will cause them to be treated as the same holder.

#### Resuming After Crash (`acquiredExternally`)

Use `acquiredExternally: true` to resume ownership of permits that were acquired previously but not properly released (e.g., after a process crash). This uses `refresh` instead of `acquire` to verify ownership.

```typescript
// Store identifier persistently before doing work
const identifier = crypto.randomUUID();
yield* saveToDatabase({ jobId, lockIdentifier: identifier });

yield* Effect.gen(function* () {
  yield* doWork();
  yield* deleteFromDatabase(jobId);
}).pipe(sem.withPermits(1, { identifier }));

// === Later, after crash recovery ===
const { lockIdentifier } = yield* loadFromDatabase(jobId);

// Check if we still hold the lock (TTL hasn't expired)
const result = yield* Effect.gen(function* () {
  yield* resumeWork();
  yield* deleteFromDatabase(jobId);
}).pipe(
  sem.withPermitsIfAvailable(1, { 
    identifier: lockIdentifier, 
    acquiredExternally: true 
  })
);

if (Option.isNone(result)) {
  // Lock expired, need to re-acquire normally
  yield* restartWork().pipe(sem.withPermits(1));
}
```

This is useful for:
- **Crash recovery**: Resume work if you crashed while holding permits
- **Process restart**: Check if your previous lock is still valid

⚠️ **Unsafe**: If the identifier is wrong or the lock expired, `tryTake`/`withPermitsIfAvailable` return `None`, while `take`/`withPermits` keep retrying forever (waiting for permits that will never come).

#### `currentCount` — Check held permits

```typescript
const held = yield* sem.currentCount; // Number of permits currently held
const available = sem.limit - held;   // Number of permits available
```

## Error Handling

All errors are tagged for precise handling with `Effect.catchTag`:

```typescript
yield* myEffect.pipe(
  sem.withPermits(1),
  Effect.catchTag("LockLostError", (e) =>
    Effect.log(`Permits were lost: ${e.key}`)
  ),
  Effect.catchTag("SemaphoreBackingError", (e) =>
    Effect.log(`Redis error: ${e.message}`)
  )
);
```

| Error                   | Description                                          |
| ----------------------- | ---------------------------------------------------- |
| `LockLostError`         | Permit TTL expired while we thought we held it       |
| `SemaphoreBackingError` | Error from the backing store (Redis connection, etc) |

## Redis Backing (Single-Instance Only)

The provided Redis backing is designed for **single-instance Redis only**. It does not implement the [Redlock algorithm](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) and should not be used with Redis Cluster or Redis Sentinel when you need strong distributed locking guarantees.

```typescript
import Redis from "ioredis";
import { RedisBacking } from "effect-distributed-lock/redis";

// Single Redis instance
const redis = new Redis("redis://localhost:6379");
const RedisLayer = RedisBacking.layer(redis, {
  keyPrefix: "my-prefix:",
  pushBasedAcquireEnabled: true, // default: true
});
```

### Configuration Options

| Option                     | Type             | Default            | Description                                          |
| -------------------------- | ---------------- | ------------------ | ---------------------------------------------------- |
| `keyPrefix`                | `string`         | `"semaphore:"`     | Prefix for all Redis keys                            |
| `pushBasedAcquireEnabled`  | `boolean`        | `true`             | Use pub/sub for efficient waiting (see below)        |
| `pushStreamRetrySchedule`  | `Schedule<void>` | `Schedule.forever` | Retry schedule for pub/sub stream errors             |

### Push-Based Acquisition

By default, the Redis backing uses pub/sub to notify waiters when permits become available. This reduces latency and load on Redis compared to pure polling.

When permits are released, a message is published to a channel. Waiters subscribe to this channel and immediately attempt to acquire when notified. The semaphore still falls back to polling as a safety net.

**Trade-offs:**
- ✅ Lower latency — waiters are notified immediately
- ✅ Reduced Redis load — fewer polling requests
- ⚠️ Extra connection — each waiting semaphore uses a subscriber connection

To disable and use polling only:

```typescript
const RedisLayer = RedisBacking.layer(redis, {
  pushBasedAcquireEnabled: false,
});
```

For multi-instance Redis deployments requiring Redlock, you'll need to implement a custom backing.

## Custom Backends

Implement the `DistributedSemaphoreBacking` interface to use a different store:

```typescript
import { Duration, Effect, Layer, Stream } from "effect";
import { Backing, DistributedSemaphoreBacking } from "effect-distributed-lock";

const MyCustomBacking = Layer.succeed(DistributedSemaphoreBacking, {
  tryAcquire: (key, holderId, ttl, limit, permits) => 
    Effect.succeed(true), // Try to acquire permits
  
  release: (key, holderId, permits) => 
    Effect.succeed(permits), // Release permits, return count released
  
  refresh: (key, holderId, ttl, limit, permits) => 
    Effect.succeed(true), // Refresh TTL, return false if lost
  
  getCount: (key, ttl) => 
    Effect.succeed(0), // Return number of permits currently held

  // Optional: Enable push-based waiting
  onPermitsReleased: (key) => 
    Stream.never, // Stream that emits when permits MAY be available
});
```

The `onPermitsReleased` method is optional. If provided, the semaphore will use it for efficient push-based waiting instead of pure polling. The stream should emit whenever permits are released on the given key. Multiple waiters may race for permits after a notification, so `tryAcquire` is still called after each notification.

## How It Works

1. **Acquire**: Atomically adds permits to a sorted set if there's room (Redis: Lua script with `ZADD`)
2. **Keepalive**: A background fiber refreshes the TTL periodically by updating timestamps
3. **Release**: Atomically removes permits and publishes notification to waiters (Lua script with `ZREM` + `PUBLISH`)
4. **Waiting**: Combines polling with pub/sub notifications — waiters are notified immediately when permits are released
5. **Expiration**: Expired entries (based on TTL) are cleaned up on each operation
6. **Crash safety**: If the holder crashes, permits expire and become available

## License

MIT
