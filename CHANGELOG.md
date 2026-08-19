# effect-distributed-lock

## 0.0.12

### Patch Changes

- 4a8421e: Stop semaphore keepAlive refreshes before releasing permits so scoped release cannot race an in-flight refresh and resurrect a released holder.
