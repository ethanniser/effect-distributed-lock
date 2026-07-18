---
"effect-distributed-lock": patch
---

Stop semaphore keepAlive refreshes before releasing permits so scoped release cannot race an in-flight refresh and resurrect a released holder.
