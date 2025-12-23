import { Effect, Layer, Option } from "effect";
import type { Redis } from "ioredis";
import { DistributedMutexBacking } from "./DistributedMutex.ts";
import { BackingError } from "./Errors.ts";

/**
 * Lua script for atomic lock acquisition.
 * SET key value NX PX ttl - set only if not exists, with TTL
 */
const ACQUIRE_SCRIPT = `
local key = KEYS[1]
local holderId = ARGV[1]
local ttlMs = tonumber(ARGV[2])

local result = redis.call('SET', key, holderId, 'NX', 'PX', ttlMs)
if result then
  return 1
else
  return 0
end
`;

/**
 * Lua script for atomic lock release.
 * Only deletes if we are the current holder.
 */
const RELEASE_SCRIPT = `
local key = KEYS[1]
local holderId = ARGV[1]

local currentHolder = redis.call('GET', key)
if currentHolder == holderId then
  redis.call('DEL', key)
  return 1
else
  return 0
end
`;

/**
 * Lua script for atomic TTL refresh.
 * Only refreshes if we are the current holder.
 */
const REFRESH_SCRIPT = `
local key = KEYS[1]
local holderId = ARGV[1]
local ttlMs = tonumber(ARGV[2])

local currentHolder = redis.call('GET', key)
if currentHolder == holderId then
  redis.call('PEXPIRE', key, ttlMs)
  return 1
else
  return 0
end
`;

/**
 * Create a Redis-backed distributed mutex backing layer.
 *
 * @param redis - An ioredis client instance
 * @param keyPrefix - Optional prefix for all keys (default: "dmutex:")
 */
export const makeRedisBackingLayer = (
  redis: Redis,
  keyPrefix = "dmutex:"
): Layer.Layer<DistributedMutexBacking> => {
  const prefixKey = (key: string) => `${keyPrefix}${key}`;

  const tryAcquire = (
    key: string,
    holderId: string,
    ttlMs: number
  ): Effect.Effect<boolean, BackingError> =>
    Effect.tryPromise({
      try: async () => {
        const result = await redis.eval(
          ACQUIRE_SCRIPT,
          1,
          prefixKey(key),
          holderId,
          ttlMs.toString()
        );
        return result === 1;
      },
      catch: (cause) => new BackingError({ operation: "tryAcquire", cause }),
    });

  const release = (
    key: string,
    holderId: string
  ): Effect.Effect<boolean, BackingError> =>
    Effect.tryPromise({
      try: async () => {
        const result = await redis.eval(
          RELEASE_SCRIPT,
          1,
          prefixKey(key),
          holderId
        );
        return result === 1;
      },
      catch: (cause) => new BackingError({ operation: "release", cause }),
    });

  const refresh = (
    key: string,
    holderId: string,
    ttlMs: number
  ): Effect.Effect<boolean, BackingError> =>
    Effect.tryPromise({
      try: async () => {
        const result = await redis.eval(
          REFRESH_SCRIPT,
          1,
          prefixKey(key),
          holderId,
          ttlMs.toString()
        );
        return result === 1;
      },
      catch: (cause) => new BackingError({ operation: "refresh", cause }),
    });

  const isLocked = (key: string): Effect.Effect<boolean, BackingError> =>
    Effect.tryPromise({
      try: async () => {
        const exists = await redis.exists(prefixKey(key));
        return exists === 1;
      },
      catch: (cause) => new BackingError({ operation: "isLocked", cause }),
    });

  const getHolder = (
    key: string
  ): Effect.Effect<Option.Option<string>, BackingError> =>
    Effect.tryPromise({
      try: async () => {
        const holder = await redis.get(prefixKey(key));
        return holder ? Option.some(holder) : Option.none();
      },
      catch: (cause) => new BackingError({ operation: "getHolder", cause }),
    });

  return Layer.succeed(DistributedMutexBacking, {
    tryAcquire,
    release,
    refresh,
    isLocked,
    getHolder,
  });
};

/**
 * Create a Redis backing from a connection URL.
 * This creates and manages the Redis connection lifecycle.
 */
export const makeRedisBackingLayerFromUrl = (
  url: string,
  keyPrefix = "dmutex:"
): Layer.Layer<DistributedMutexBacking, BackingError> =>
  Layer.scoped(
    DistributedMutexBacking,
    Effect.gen(function* () {
      // Dynamic import to avoid requiring ioredis at module load time
      const { default: Redis } = yield* Effect.tryPromise({
        try: () => import("ioredis"),
        catch: (cause) =>
          new BackingError({
            operation: "import",
            cause: `Failed to import ioredis: ${cause}`,
          }),
      });

      const redis = new Redis(url);

      // Ensure cleanup on scope close
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => redis.quit().catch(() => {}))
      );

      const prefixKey = (key: string) => `${keyPrefix}${key}`;

      const tryAcquire = (
        key: string,
        holderId: string,
        ttlMs: number
      ): Effect.Effect<boolean, BackingError> =>
        Effect.tryPromise({
          try: async () => {
            const result = await redis.eval(
              ACQUIRE_SCRIPT,
              1,
              prefixKey(key),
              holderId,
              ttlMs.toString()
            );
            return result === 1;
          },
          catch: (cause) =>
            new BackingError({ operation: "tryAcquire", cause }),
        });

      const release = (
        key: string,
        holderId: string
      ): Effect.Effect<boolean, BackingError> =>
        Effect.tryPromise({
          try: async () => {
            const result = await redis.eval(
              RELEASE_SCRIPT,
              1,
              prefixKey(key),
              holderId
            );
            return result === 1;
          },
          catch: (cause) => new BackingError({ operation: "release", cause }),
        });

      const refresh = (
        key: string,
        holderId: string,
        ttlMs: number
      ): Effect.Effect<boolean, BackingError> =>
        Effect.tryPromise({
          try: async () => {
            const result = await redis.eval(
              REFRESH_SCRIPT,
              1,
              prefixKey(key),
              holderId,
              ttlMs.toString()
            );
            return result === 1;
          },
          catch: (cause) => new BackingError({ operation: "refresh", cause }),
        });

      const isLocked = (key: string): Effect.Effect<boolean, BackingError> =>
        Effect.tryPromise({
          try: async () => {
            const exists = await redis.exists(prefixKey(key));
            return exists === 1;
          },
          catch: (cause) => new BackingError({ operation: "isLocked", cause }),
        });

      const getHolder = (
        key: string
      ): Effect.Effect<Option.Option<string>, BackingError> =>
        Effect.tryPromise({
          try: async () => {
            const holder = await redis.get(prefixKey(key));
            return holder ? Option.some(holder) : Option.none();
          },
          catch: (cause) => new BackingError({ operation: "getHolder", cause }),
        });

      return {
        tryAcquire,
        release,
        refresh,
        isLocked,
        getHolder,
      } satisfies DistributedMutexBacking;
    })
  );
