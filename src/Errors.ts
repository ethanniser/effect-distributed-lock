import { Data } from "effect";

/**
 * Base error for all distributed mutex errors
 */
export class DistributedMutexError extends Data.TaggedError(
  "DistributedMutexError"
)<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

/**
 * Failed to acquire the lock within the timeout period
 */
export class AcquireTimeoutError extends Data.TaggedError(
  "AcquireTimeoutError"
)<{
  readonly key: string;
  readonly timeoutMs: number;
}> {
  get message() {
    return `Failed to acquire lock "${this.key}" within ${this.timeoutMs}ms`;
  }
}

/**
 * The lock was lost (TTL expired while we thought we held it)
 */
export class LockLostError extends Data.TaggedError("LockLostError")<{
  readonly key: string;
}> {
  get message() {
    return `Lock "${this.key}" was lost (TTL expired or taken by another holder)`;
  }
}

/**
 * Error from the backing store (Redis, etc.)
 */
export class BackingError extends Data.TaggedError("BackingError")<{
  readonly operation: string;
  readonly cause: unknown;
}> {
  get message() {
    return `Backing store error during "${this.operation}": ${this.cause}`;
  }
}
