import { Context, Data, Duration, Effect, Stream } from "effect";

// =============================================================================
// Errors
// =============================================================================

/**
 * Error from the backing store (Redis, etc.)
 */
export class SemaphoreBackingError extends Data.TaggedError(
  "SemaphoreBackingError"
)<{
  readonly operation: string;
  readonly cause: unknown;
}> {
  get message() {
    return `Backing store error during "${this.operation}": ${this.cause}`;
  }
}

// =============================================================================
// Backing Service
// =============================================================================

/**
 * Low-level backing store interface for distributed semaphore operations.
 */
export interface DistributedSemaphoreBacking {
  /**
   * Try to acquire `permits` from a semaphore with the given `limit`.
   *
   * @returns `true` if acquired, `false` if not enough permits available.
   */
  readonly tryAcquire: (
    key: string,
    holderId: string,
    ttl: Duration.Duration,
    limit: number,
    permits: number
  ) => Effect.Effect<boolean, SemaphoreBackingError>;

  /**
   * Release `permits` held by the given holder.
   *
   * @returns The number of permits actually released.
   */
  readonly release: (
    key: string,
    holderId: string,
    permits: number
  ) => Effect.Effect<number, SemaphoreBackingError>;

  /**
   * Refresh the TTL on permits held by this holder.
   *
   * @returns `true` if refreshed, `false` if permits were lost (e.g., expired).
   */
  readonly refresh: (
    key: string,
    holderId: string,
    ttl: Duration.Duration,
    limit: number,
    permits: number
  ) => Effect.Effect<boolean, SemaphoreBackingError>;

  /**
   * Get the number of permits currently held (in use).
   */
  readonly getCount: (
    key: string,
    ttl: Duration.Duration
  ) => Effect.Effect<number, SemaphoreBackingError>;

  /**
   * Optional: Stream of notifications when permits MAY be available.
   *
   * If provided, the semaphore layer uses this in addition to polling. The stream emits a signal whenever permits are released.
   *
   * Notes:
   * - Multiple waiters may race for permits after a notification
   * - The semaphore still calls `tryAcquire` after each notification
   * - Implementations should handle reconnection internally (hence why the stream does not have an error type)
   */
  readonly onPermitsReleased?: (key: string) => Stream.Stream<void>;
}

export const DistributedSemaphoreBacking =
  Context.GenericTag<DistributedSemaphoreBacking>(
    "@effect-distributed-lock/DistributedSemaphoreBacking"
  );
