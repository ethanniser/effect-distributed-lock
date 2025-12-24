import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  Function,
  Option,
  Schedule,
  Scope,
  Stream,
} from "effect";
import {
  DistributedSemaphoreBacking,
  SemaphoreBackingError,
} from "./Backing.js";
import { LockLostError, LockNotAcquiredError } from "./Errors.js";

// =============================================================================
// Semaphore Configuration
// =============================================================================

export interface DistributedSemaphoreConfig {
  /**
   * Maximum number of permits available.
   * This is the total capacity of the semaphore.
   * @default 1 (mutex behavior)
   */
  readonly limit?: number;

  /**
   * TTL for held permits. If the holder crashes, permits auto-release after this.
   * @default 30 seconds
   */
  readonly ttl?: Duration.DurationInput;

  /**
   * How often to refresh the TTL while holding permits.
   * Should be less than TTL to avoid losing the permits.
   * @default 1/3 of TTL
   */
  readonly refreshInterval?: Duration.DurationInput;

  /**
   * How often to poll when waiting to acquire permits.
   * @default Schedule.spaced(Duration.millis(100))
   */
  readonly acquireRetryPolicy?: Schedule.Schedule<unknown>;

  /**
   * Retry policy when a backing failure occurs.
   * This could happen when:
   * - Trying to acquire permits
   * - Refreshing the TTL
   * - Releasing permits
   * @default Schedule.recurs(3)
   */
  readonly backingFailureRetryPolicy?: Schedule.Schedule<unknown>;
}

const DEFAULT_LIMIT = 1;
const DEFAULT_TTL = Duration.seconds(30);
const DEFAULT_ACQUIRE_RETRY_POLICY = Schedule.spaced(Duration.millis(100));
const DEFAULT_FAILURE_RETRY_POLICY = Schedule.recurs(3);

// =============================================================================
// Acquire Options
// =============================================================================

/**
 * Options for acquire operations (take, tryTake, withPermits, etc.)
 */
export interface AcquireOptions {
  /**
   * Unique identifier for this permit holder.
   *
   * By default, a random UUID is generated per-acquire. Override this if you need:
   * - Predictable identifiers for debugging/observability
   * - Cross-process lock handoff (acquire in one process, release in another)
   *
   * ⚠️ **Warning**: Must be unique across concurrent holders, otherwise locks with the same
   * identifier may be treated as the same holder.
   *
   * @default crypto.randomUUID()
   */
  readonly identifier?: string;

  /**
   * If true, assumes the permits were already acquired externally with the given identifier.
   * Instead of acquiring, uses refresh to verify ownership.
   *
   * **Requires `identifier` to be provided.**
   *
   * This is useful for cross-process lock handoff:
   * 1. Process A acquires permits with a known identifier
   * 2. Process A passes the identifier to Process B (via message queue, etc.)
   * 3. Process B calls take/withPermits with `{ identifier, acquiredExternally: true }`
   * 4. Process B now owns the permits (refreshing and releasing)
   *
   * ⚠️ **Unsafe**: If the identifier is wrong or the lock expired, this will fail immediately.
   *
   * @default false
   */
  readonly acquiredExternally?: boolean;
}

// =============================================================================
// Distributed Semaphore Interface
// =============================================================================

/**
 * A distributed semaphore that can be used across multiple processes/services.
 *
 * Similar to Effect's built-in Semaphore, but distributed across processes
 * using a backing store like Redis.
 *
 * A semaphore manages a pool of permits. Tasks can acquire one or more permits,
 * and must release them when done. If not enough permits are available, tasks
 * wait until permits are released.
 *
 * @example
 * ```ts
 * // Create a semaphore that allows 5 concurrent operations
 * const sem = yield* DistributedSemaphore.make("my-resource", { limit: 5 });
 *
 * // Acquire 2 permits, run effect, then release
 * yield* sem.withPermits(2)(myEffect);
 *
 * // Create a mutex (limit=1) for exclusive access
 * const mutex = yield* DistributedSemaphore.make("my-lock", { limit: 1 });
 * yield* mutex.withPermits(1)(criticalSection);
 * ```
 */
export interface DistributedSemaphore {
  /**
   * The key identifying this semaphore in the backing store.
   */
  readonly key: string;

  /**
   * The maximum number of permits available.
   */
  readonly limit: number;

  /**
   * Run an effect with the specified number of permits.
   * Acquires the permits, runs the effect, then releases.
   * If not enough permits are available, waits until they are.
   *
   * The permit TTL is refreshed automatically while the effect runs.
   */
  readonly withPermits: (
    permits: number,
    options?: AcquireOptions
  ) => <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<
    A,
    E | LockLostError | SemaphoreBackingError | LockNotAcquiredError,
    R
  >;

  /**
   * Run an effect only if the specified permits are immediately available.
   * Returns Some(result) if permits were acquired and effect ran,
   * None if permits were not available.
   */
  readonly withPermitsIfAvailable: (
    permits: number,
    options?: AcquireOptions
  ) => <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<
    Option.Option<A>,
    E | LockLostError | SemaphoreBackingError,
    R
  >;

  /**
   * Acquire the specified number of permits, waiting if necessary.
   * The permits are held until the scope is closed.
   * The permit TTL is refreshed automatically while held.
   *
   * @returns The fiber that refreshes the permit hold. Its lifetime is tied to the scope.
   * When the scope closes, the fiber is interrupted and permits are released.
   */
  readonly take: (
    permits: number,
    options?: AcquireOptions
  ) => Effect.Effect<
    Fiber.Fiber<never, LockLostError | SemaphoreBackingError>,
    LockLostError | SemaphoreBackingError | LockNotAcquiredError,
    Scope.Scope
  >;

  /**
   * Try to acquire permits immediately without waiting.
   * Returns Some(fiber) if acquired (permits held until scope closes),
   * None if permits were not available.
   *
   * @returns The fiber that refreshes the permit hold. Its lifetime is tied to the scope.
   * When the scope closes, the fiber is interrupted and permits are released.
   */
  readonly tryTake: (
    permits: number,
    options?: AcquireOptions
  ) => Effect.Effect<
    Option.Option<Fiber.Fiber<never, LockLostError | SemaphoreBackingError>>,
    LockLostError | SemaphoreBackingError,
    Scope.Scope
  >;

  /**
   * Get the current number of active permits (for debugging/introspection).
   * Note: This is eventually consistent due to TTL-based expiration.
   */
  readonly currentCount: Effect.Effect<number, SemaphoreBackingError>;
}

// =============================================================================
// Factory
// =============================================================================

type FullyResolvedConfig = {
  limit: number;
  ttl: Duration.Duration;
  refreshInterval: Duration.Duration;
  acquireRetryPolicy: Schedule.Schedule<unknown>;
  backingFailureRetryPolicy: Schedule.Schedule<unknown>;
};

function fullyResolveConfig(
  config: DistributedSemaphoreConfig
): FullyResolvedConfig {
  const limit = config.limit ?? DEFAULT_LIMIT;
  const ttl = config.ttl ? Duration.decode(config.ttl) : DEFAULT_TTL;
  const refreshInterval = config.refreshInterval
    ? Duration.decode(config.refreshInterval)
    : Duration.millis(Duration.toMillis(ttl) / 3);
  const acquireRetryPolicy = config.acquireRetryPolicy
    ? config.acquireRetryPolicy
    : DEFAULT_ACQUIRE_RETRY_POLICY;
  const backingFailureRetryPolicy = config.backingFailureRetryPolicy
    ? config.backingFailureRetryPolicy
    : DEFAULT_FAILURE_RETRY_POLICY;

  return {
    limit,
    ttl,
    refreshInterval,
    acquireRetryPolicy,
    backingFailureRetryPolicy,
  };
}

/**
 * Create a distributed semaphore for the given key.
 *
 * @param key - Unique identifier for this semaphore in the backing store
 * @param config - Configuration options
 *
 * @example
 * ```ts
 * // Create a semaphore that allows 5 concurrent operations
 * const sem = yield* DistributedSemaphore.make("my-resource", { limit: 5 });
 *
 * // Use withPermits to run an effect with acquired permits
 * yield* sem.withPermits(2)(Effect.log("I have 2 permits"));
 *
 * // Use withPermitsIfAvailable to try without waiting
 * const result = yield* sem.withPermitsIfAvailable(1)(Effect.succeed(42));
 * // result: Option<number>
 * ```
 */
export const make = (
  key: string,
  config: DistributedSemaphoreConfig = {}
): Effect.Effect<DistributedSemaphore, never, DistributedSemaphoreBacking> =>
  Effect.gen(function* () {
    const backing = yield* DistributedSemaphoreBacking;

    // Resolve config with defaults
    const {
      limit,
      ttl,
      refreshInterval,
      acquireRetryPolicy,
      backingFailureRetryPolicy,
    } = fullyResolveConfig(config);

    const withBackingErrorRetry = <A, E extends { _tag: string }, R>(
      effect: Effect.Effect<A, E | SemaphoreBackingError, R>
    ) =>
      effect.pipe(
        Effect.retry({
          while: (e) => e._tag === "SemaphoreBackingError",
          schedule: backingFailureRetryPolicy,
        })
      );

    // Keep the permits alive by refreshing TTL periodically.
    // This effect runs forever until interrupted (when scope closes).
    const keepAlive = (
      identifier: string,
      permits: number
    ): Effect.Effect<never, SemaphoreBackingError | LockLostError, never> =>
      Effect.repeat(
        Effect.gen(function* () {
          const refreshed = yield* backing
            .refresh(key, identifier, ttl, limit, permits)
            .pipe(withBackingErrorRetry);

          if (!refreshed) {
            return yield* new LockLostError({ key });
          }
        }),
        Schedule.spaced(refreshInterval)
      ).pipe(
        Effect.andThen(
          Effect.dieMessage(
            "Invariant violated: `keepAlive` should never return a value"
          )
        )
      );

    // Try to acquire permits immediately, returns Option
    const tryTake = (
      permits: number,
      options?: AcquireOptions
    ): Effect.Effect<
      Option.Option<Fiber.Fiber<never, LockLostError | SemaphoreBackingError>>,
      SemaphoreBackingError,
      Scope.Scope
    > =>
      Effect.gen(function* () {
        // Generate identifier per-acquire if not provided
        const identifier = options?.identifier ?? crypto.randomUUID();
        const acquiredExternally = options?.acquiredExternally ?? false;

        // If acquiredExternally, use refresh to verify ownership instead of acquire
        const acquired = acquiredExternally
          ? yield* backing
              .refresh(key, identifier, ttl, limit, permits)
              .pipe(withBackingErrorRetry)
          : yield* backing
              .tryAcquire(key, identifier, ttl, limit, permits)
              .pipe(withBackingErrorRetry);

        if (!acquired) {
          return Option.none();
        }

        // Start keepalive fiber, tied to this scope
        const keepAliveFiber = yield* Effect.forkScoped(
          keepAlive(identifier, permits)
        );

        // Add finalizer to release permits when scope closes
        yield* Effect.addFinalizer(() =>
          backing
            .release(key, identifier, permits)
            .pipe(withBackingErrorRetry, Effect.ignore)
        );

        return Option.some(keepAliveFiber);
      });

    // Acquire permits with retry, returns fiber when acquired
    const take = (
      permits: number,
      options?: AcquireOptions
    ): Effect.Effect<
      Fiber.Fiber<never, LockLostError | SemaphoreBackingError>,
      SemaphoreBackingError | LockNotAcquiredError,
      Scope.Scope
    > =>
      Effect.gen(function* () {
        // Generate identifier once for all retry attempts (outside the retry loop)
        const identifier = options?.identifier ?? crypto.randomUUID();
        const resolvedOptions: AcquireOptions = {
          identifier,
          acquiredExternally: options?.acquiredExternally,
        };

        // We use a semaphore to ensure that only one acquire attempt is made at a time.
        // With `withPermitsIfAvailable`, if both the poll-based and push-based attempts "trigger" at the same time,
        // one will succeed and the other will simple be a no-op.
        const acquireSemaphore = yield* Effect.makeSemaphore(1);

        const pushBasedAcquireEnabled = backing.onPermitsReleased
          ? true
          : false;

        const pollBasedAcquire = Effect.gen(function* () {
          const maybeAcquired = yield* tryTake(permits, resolvedOptions).pipe(
            // only apply the semaphore if push-based acquire is supported
            pushBasedAcquireEnabled
              ? Function.compose(
                  acquireSemaphore.withPermitsIfAvailable(1),
                  Effect.map(Option.flatten)
                )
              : Function.identity
          );
          if (Option.isNone(maybeAcquired)) {
            return yield* new LockNotAcquiredError({ key });
          }
          return maybeAcquired.value;
        }).pipe(
          Effect.retry({
            while: (e) => e._tag === "LockNotAcquiredError",
            schedule: acquireRetryPolicy,
          })
        );

        if (!pushBasedAcquireEnabled) {
          return yield* pollBasedAcquire;
        }

        // Push-based acquire: run both poll-based and push-based acquire in parallel, and return the first one to complete
        const pushBasedAcquire = Effect.gen(function* () {
          if (!backing.onPermitsReleased) {
            // SAFETY: We know that onPermitsReleased is provided because we checked it above
            return yield* Effect.dieMessage(
              "Invariant violated: `onPermitsReleased` is not provided when it was expected"
            );
          }
          return yield* backing.onPermitsReleased(key).pipe(
            Stream.runFoldWhileEffect(
              Option.none<
                Fiber.Fiber<never, LockLostError | SemaphoreBackingError>
              >(),
              Option.isNone, // keep folding while we haven't acquired
              () =>
                tryTake(permits, resolvedOptions).pipe(
                  acquireSemaphore.withPermitsIfAvailable(1),
                  Effect.map(Option.flatten)
                )
            ),
            Effect.flatMap(Option.match({
              onSome: Effect.succeed,
              onNone: () => Effect.dieMessage("Invariant violated: the stream should never return `None`")
            }))
          );
        });

        // first to succeed (acquire permits) wins
        return yield* Effect.race(pollBasedAcquire, pushBasedAcquire);
      });

    // Convenience: acquire permits, run effect, release when done
    const withPermits =
      (permits: number, options?: AcquireOptions) =>
      <A, E, R>(
        effect: Effect.Effect<A, E, R>
      ): Effect.Effect<
        A,
        E | LockLostError | SemaphoreBackingError | LockNotAcquiredError,
        R
      > =>
        Effect.scoped(
          Effect.gen(function* () {
            const keepAliveFiber = yield* take(permits, options);

            return yield* Effect.raceFirst(effect, Fiber.join(keepAliveFiber));
          })
        );

    // Convenience: try to acquire permits, run effect if successful
    const withPermitsIfAvailable =
      (permits: number, options?: AcquireOptions) =>
      <A, E, R>(
        effect: Effect.Effect<A, E, R>
      ): Effect.Effect<
        Option.Option<A>,
        E | LockLostError | SemaphoreBackingError,
        R
      > =>
        Effect.scoped(
          Effect.gen(function* () {
            const maybeAcquired = yield* tryTake(permits, options);
            if (Option.isNone(maybeAcquired)) {
              return Option.none();
            }
            const keepAliveFiber = maybeAcquired.value;
            return yield* Effect.raceFirst(
              effect.pipe(Effect.asSome),
              Fiber.join(keepAliveFiber)
            );
          })
        );

    const currentCount: Effect.Effect<number, SemaphoreBackingError> = backing
      .getCount(key, ttl)
      .pipe(withBackingErrorRetry);

    return {
      key,
      limit,
      withPermits,
      withPermitsIfAvailable,
      take,
      tryTake,
      currentCount,
    } satisfies DistributedSemaphore;
  });
