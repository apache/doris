# BE Runtime Module — Review Guide

## MemTracker Hierarchy

Two levels: `MemTrackerLimiter` (heavyweight, with limits) and `MemTracker` (lightweight, nested accounting). Thread-local state via `ThreadMemTrackerMgr`.

### Checkpoints

- [ ] Task-bound threads/bthreads enter with `SCOPED_ATTACH_TASK`?
- [ ] Non-task background threads use `SCOPED_INIT_THREAD_CONTEXT`?
- [ ] Temporary limiter switching uses `SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER`, not manual save/restore?
- [ ] Thread-pool callbacks attach at callback entry, not deeper in the call chain?
- [ ] Large allocations use `try_reserve()` with `DEFER_RELEASE_RESERVED()` for balanced accounting on every exit?
- [ ] For BE-owned buffers or containers whose memory should be tracked by Doris accounting, prefer allocator-aware types from `be/src/core/custom_allocator.h` such as `DorisVector`, `DorisMap`, and `DorisUniqueBufferPtr` instead of the corresponding standard library ownership types
- [ ] Code reachable from `ThreadMemTrackerMgr::consume` avoids operations that may allocate recursively?

## Object Lifecycle

- [ ] `ENABLE_FACTORY_CREATOR` classes use `create_shared()` / `create_unique()`, not raw `new`?
- [ ] Ownership cycles broken with `weak_ptr` or raw observer pointers?
- [ ] Single-owner paths use `unique_ptr` plus observer raw pointers?

## Cache Lifecycle

- [ ] Every `Cache::Handle*` released after use?
- [ ] Cache values inherit `LRUCacheValueBase` for tracked-byte release?
- [ ] New caches registered with `CacheManager` for global GC?

## Static Initialization

- [ ] New namespace-scope static/global depends on another TU's object? That is a SIOF hazard
- [ ] Fix: `constexpr`, same-header `inline`, or function-local static?

## Workload Group Memory

- [ ] When precise limit enforcement matters, code uses `check_mem_used()` not just `exceed_limit()`?
