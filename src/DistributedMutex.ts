import { Context, Duration, Effect, Option, Schedule, Scope } from "effect";
import { AcquireTimeoutError, BackingError, LockLostError } from "./Errors.ts";

// =============================================================================
// Backing Service
// =============================================================================

/**
 * Low-level backing store interface for distributed mutex operations.
 * Implementations handle the actual storage (Redis, etcd, DynamoDB, etc.)
 */
export interface DistributedMutexBacking {
  /**
   * Try to acquire the lock. Returns true if acquired, false if already held.
   * Must set TTL on the lock.
   */
  readonly tryAcquire: (
    key: string,
    holderId: string,
    ttlMs: number
  ) => Effect.Effect<boolean, BackingError>;

  /**
   * Release the lock. Only succeeds if we are the current holder.
   * Returns true if released, false if we weren't the holder.
   */
  readonly release: (
    key: string,
    holderId: string
  ) => Effect.Effect<boolean, BackingError>;

  /**
   * Refresh the TTL on a lock we hold.
   * Returns true if refreshed, false if lock was lost.
   */
  readonly refresh: (
    key: string,
    holderId: string,
    ttlMs: number
  ) => Effect.Effect<boolean, BackingError>;

  /**
   * Check if the lock is currently held (by anyone).
   */
  readonly isLocked: (key: string) => Effect.Effect<boolean, BackingError>;

  /**
   * Get the current holder ID, if any.
   */
  readonly getHolder: (
    key: string
  ) => Effect.Effect<Option.Option<string>, BackingError>;
}

export const DistributedMutexBacking =
  Context.GenericTag<DistributedMutexBacking>(
    "@effect-distributed-lock/DistributedMutexBacking"
  );

// =============================================================================
// Mutex Configuration
// =============================================================================

export interface DistributedMutexConfig {
  /**
   * TTL for the lock. If the holder crashes, the lock auto-releases after this.
   * @default 30 seconds
   */
  readonly ttl?: Duration.DurationInput;

  /**
   * How often to refresh the TTL while holding the lock.
   * Should be less than TTL to avoid losing the lock.
   * @default 1/3 of TTL
   */
  readonly refreshInterval?: Duration.DurationInput;

  /**
   * How often to poll when waiting to acquire the lock.
   * @default 100ms
   */
  readonly retryInterval?: Duration.DurationInput;

  /**
   * Maximum time to wait when acquiring the lock.
   * If not set, will wait forever.
   */
  readonly acquireTimeout?: Duration.DurationInput;
}

const DEFAULT_TTL = Duration.seconds(30);
const DEFAULT_RETRY_INTERVAL = Duration.millis(100);

// =============================================================================
// Distributed Mutex Interface
// =============================================================================

/**
 * A distributed mutex that can be used across multiple processes/services.
 */
export interface DistributedMutex {
  /**
   * The key identifying this mutex in the backing store.
   */
  readonly key: string;

  /**
   * Acquire the lock, run the effect, then release.
   * If acquisition fails within timeout, returns AcquireTimeoutError.
   * The lock TTL is refreshed automatically while the effect runs.
   */
  readonly withLock: <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<
    A,
    E | AcquireTimeoutError | LockLostError | BackingError,
    R
  >;

  /**
   * Try to acquire the lock immediately without waiting.
   * Returns Some(result) if lock was acquired and effect ran,
   * None if lock was not available.
   */
  readonly withLockIfAvailable: <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<Option.Option<A>, E | LockLostError | BackingError, R>;

  /**
   * Acquire the lock, waiting if necessary.
   * The lock is held until the scope is closed.
   * The lock TTL is refreshed automatically while held.
   */
  readonly acquire: Effect.Effect<
    void,
    AcquireTimeoutError | LockLostError | BackingError,
    Scope.Scope
  >;

  /**
   * Try to acquire immediately without waiting.
   * Returns Some(void) if acquired (lock held until scope closes),
   * None if lock was not available.
   */
  readonly tryAcquire: Effect.Effect<
    Option.Option<void>,
    LockLostError | BackingError,
    Scope.Scope
  >;

  /**
   * Check if the lock is currently held.
   */
  readonly isLocked: Effect.Effect<boolean, BackingError>;
}

// =============================================================================
// Factory
// =============================================================================

/**
 * Create a distributed mutex for the given key.
 */
export const make = (
  key: string,
  config: DistributedMutexConfig = {}
): Effect.Effect<DistributedMutex, never, DistributedMutexBacking> =>
  Effect.gen(function* () {
    const backing = yield* DistributedMutexBacking;

    // Generate unique holder ID for this instance
    const holderId = crypto.randomUUID();

    // Resolve config with defaults
    const ttl = config.ttl ? Duration.decode(config.ttl) : DEFAULT_TTL;
    const ttlMs = Duration.toMillis(ttl);
    const refreshInterval = config.refreshInterval
      ? Duration.decode(config.refreshInterval)
      : Duration.millis(ttlMs / 3);
    const retryInterval = config.retryInterval
      ? Duration.decode(config.retryInterval)
      : DEFAULT_RETRY_INTERVAL;
    const acquireTimeout = config.acquireTimeout
      ? Option.some(Duration.decode(config.acquireTimeout))
      : Option.none<Duration.Duration>();

    // Keep the lock alive by refreshing TTL periodically.
    // This effect runs forever until interrupted (when scope closes).
    const keepAlive = Effect.repeat(
      Effect.gen(function* () {
        const refreshed = yield* backing.refresh(key, holderId, ttlMs);
        if (!refreshed) {
          return yield* new LockLostError({ key });
        }
      }),
      Schedule.spaced(refreshInterval)
    );

    // Try to acquire immediately, returns Option
    const tryAcquire: Effect.Effect<
      Option.Option<void>,
      LockLostError | BackingError,
      Scope.Scope
    > = Effect.gen(function* () {
      const acquired = yield* backing.tryAcquire(key, holderId, ttlMs);
      if (!acquired) {
        return Option.none();
      }

      // Start keepalive fiber, tied to this scope
      yield* Effect.forkScoped(keepAlive);

      // Add finalizer to release lock when scope closes
      yield* Effect.addFinalizer(() =>
        backing.release(key, holderId).pipe(Effect.ignore)
      );

      return Option.some(undefined as void);
    });

    // Acquire with retry/timeout, returns void when acquired
    const acquire: Effect.Effect<
      void,
      AcquireTimeoutError | LockLostError | BackingError,
      Scope.Scope
    > = Effect.gen(function* () {
      // Build retry schedule with optional timeout
      const schedule = Option.match(acquireTimeout, {
        onNone: () => Schedule.spaced(retryInterval),
        onSome: (timeout) =>
          Schedule.spaced(retryInterval).pipe(Schedule.upTo(timeout)),
      });

      // Retry until we acquire the lock
      yield* Effect.retry(
        Effect.gen(function* () {
          const maybeAcquired = yield* tryAcquire;
          if (Option.isNone(maybeAcquired)) {
            return yield* Effect.fail({ _tag: "NotYetAcquired" as const });
          }
        }),
        schedule
      ).pipe(
        Effect.catchIf(
          (e): e is { _tag: "NotYetAcquired" } =>
            typeof e === "object" &&
            e !== null &&
            "_tag" in e &&
            e._tag === "NotYetAcquired",
          () =>
            new AcquireTimeoutError({
              key,
              timeoutMs: Option.match(acquireTimeout, {
                onNone: () => -1,
                onSome: Duration.toMillis,
              }),
            })
        )
      );
    });

    // Convenience: acquire lock, run effect, release when done
    const withLock = <A, E, R>(
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<
      A,
      E | AcquireTimeoutError | LockLostError | BackingError,
      R
    > =>
      Effect.scoped(
        Effect.gen(function* () {
          yield* acquire;
          return yield* effect;
        })
      );

    // Convenience: try to acquire, run effect if successful
    const withLockIfAvailable = <A, E, R>(
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<Option.Option<A>, E | LockLostError | BackingError, R> =>
      Effect.scoped(
        Effect.gen(function* () {
          const maybeAcquired = yield* tryAcquire;
          if (Option.isNone(maybeAcquired)) {
            return Option.none<A>();
          }
          const result = yield* effect;
          return Option.some(result);
        })
      );

    const isLocked: Effect.Effect<boolean, BackingError> =
      backing.isLocked(key);

    return {
      key,
      withLock,
      withLockIfAvailable,
      acquire,
      tryAcquire,
      isLocked,
    } satisfies DistributedMutex;
  });
