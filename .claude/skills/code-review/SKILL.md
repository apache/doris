---
name: code-review
description: Create consistent releases and changelogs
compatibility: opencode
---

## What I do

Complete code review work to ensure code quality. Due to the length of content, you can read sections as needed based on the actual modifications being reviewed.

## When to use me

Use this when you need to review code, whether it's code you just completed or directly reviewing target code.

---

## Part 1: General Principles

### 1.1 Core Invariants

Always focus on the following core invariants during review:
1. **Data Correctness**: Data from any committed transaction must be visible to subsequent queries and not lost; data from uncommitted transactions must not be visible
2. **Version Consistency**: Partition visible version is the sole standard for data visibility; BE must strictly read data not exceeding this version
3. **Delete Bitmap Consistency** (MoW tables): delete bitmap must be strictly aligned with rowset versions; sentinel mark / TEMP_VERSION must be replaced with actual versions before use
4. **Memory Safety**: All significant memory allocations must be tracked by MemTracker; orphan memory is not allowed
5. **Error Means Failure**: Invariant violations should trigger exceptions or non-OK Status; silent continuation is never allowed

### 1.2 Review Principles

- **Defensive Programming Prohibited**: Do not use `if(valid)` style defensive checks to mask logic errors. If logically A=true should imply B=true, write `if(A) { DORIS_CHECK(B); ... }`, never `if(A && B)`
- **Follow Context Conventions**: When adding code, strictly follow patterns in adjacent code—same error handling, same interface call order, same lock acquisition patterns, unless a clearly correct and more reasonable approach is identified.
- **Prioritize Reuse**: Before adding new functionality, search for similar implementations that can be reused or extended; after adding, ensure good abstraction for shared code.
- **Code First**: When this SKILL conflicts with actual code behavior, defer to your understanding of actual code behavior and explain
- **Performance First**: All obviously redundant operations should be optimized away, all obvious performance optimizations must be applied, and obvious anti-patterns must be eliminated.

### 1.3 Critical Checkpoints (Self-Review and Review Priority)

The following checkpoints must be **individually confirmed with conclusions** during self-review and review:

- What is the goal of the current task? Does the current code accomplish this goal? Is there a test that proves it?
- Is this modification as small, clear, and focused as possible?
- Does the code segment involve concurrency? If yes:
  - Which threads introduce concurrency, what are the critical variables? What locks protect them?
  - Are operations within locks as lightweight as possible? Are all heavy operations outside locks while maintaining concurrency safety?
  - If multiple locks exist, is the locking order consistent? Is there deadlock risk?
- Is there special or non-intuitive lifecycle management? If yes:
  - Understand the complete lifecycle and relationships of related variables. Are there circular references? When is each released? Can all lifecycles end normally?
- Are configuration items added?
  - Should the configuration allow dynamic changes, and does it actually allow them? If yes:
    - Can affected processes detect the change promptly without restart?
- Does it involve incompatible changes like function symbols or storage formats? If yes:
  - Is compatibility code added? Can it correctly handle requests during rolling upgrades?
- Are there functionally parallel code paths to the modified one? If yes:
  - Should this modification be applied to other paths, and has it been?
- Are there special conditional checks? If yes:
  - Does the condition have clear comments explaining why this check is necessary, simplest, and most viable?
  - Are there similar paths with similar issues?
- What is the test coverage?
  - Are end-to-end functional tests comprehensive?
  - Are there comprehensive negative test cases from various angles?
  - For complex features, are key functions sufficiently modular with unit tests?
- Does the feature need increased observability? If yes:
  - When bugs occur, are existing logs and VLOGs sufficient to investigate key issues?
  - Are INFO logs lightweight enough?
  - Are Metrics added for critical paths? Referencing similar features, are all necessary observability values covered?
- Does it involve transaction and persistence-related modifications? If yes:
  - Are all EditLog reads and writes correctly covered? Is all information requiring persistent storage persisted? Does it follow our standard approach?
  - In transaction processing logic, can master failover at any time point be handled correctly?
- Does it involve data writes and modifications? If yes:
  - Are transactionality and atomicity guaranteed? Are there issues with concurrency?
  - Would FE or BE crashes cause leaks, deadlocks, or incorrect data?
- Are new variables added that need to be passed between FE and BE? If yes:
  - Is variable passing modified in all paths? For example, various scattered thrift sending points like constant folding, point queries.
- Analyze deeply from all angles (including but not limited to time complexity, anti-patterns, CPU and memory friendliness, redundant calculations, etc.) based on the code context and available information. Are there any performance issues or optimization opportunities?
- Based on your understanding, are there any other issues with the current modification?

After checking all the above items with code. Use the remaining parts of this skill as needed. The following content does not need individual responses; just check during the review process.

#### 1.3.1 Concurrency and Thread Safety (Highest Priority)

- [ ] **New thread/bthread entry**: Is `SCOPED_ATTACH_TASK` used? Omission causes memory to be counted in orphan tracker, debug builds crash directly
- [ ] **Lock protection scope**: Is shared data (version map, delete bitmap, transaction state) accessed within correct locks? Is lock type correct (shared vs exclusive)?
- [ ] **Lock order**: Are there nested acquisitions of multiple locks? Must follow known lock order (e.g., `_txn_lock` -> `_txn_map_lock`), otherwise deadlock
- [ ] **Atomic operation memory order**: Use `relaxed` for statistics, at least `acquire/release` for flag/signal
- [ ] **COW column modification**: Is exclusive ownership ensured before `mutate()`? Is `assume_mutable_ref()` only used when exclusive ownership is confirmed?

#### 1.3.2 Error Handling (Highest Priority)

- [ ] **Status checking**: `Status` return values **must** be checked, `static_cast<void>(...)` silent discard is prohibited
- [ ] **Exception propagation chain**: Is there `RETURN_IF_CATCH_EXCEPTION` above `THROW_IF_ERROR` to catch?
- [ ] **Error messages**: Are they clear, include context (table/partition/TabletID/transaction ID), provide solution direction?
- [ ] **DORIS_CHECK usage**: Only for invariant assertions (e.g., `DORIS_CHECK(hash_table != nullptr)`), not for defensive programming

#### 1.3.3 Memory Safety (High Priority)

- [ ] **Large memory reservation**: Is `try_reserve()` called before allocation? Is there `DEFER_RELEASE_RESERVED()` to ensure release?
- [ ] **Cache Handle**: Is `cache->release(handle)` called after using `Cache::Handle*`?
- [ ] **shared_ptr circular references**: Do scenarios like `Dependency` use raw pointers or `weak_ptr` to break cycles?
- [ ] **Object creation**: Do classes with `ENABLE_FACTORY_CREATOR` use `create_shared()` / `create_unique()`? Raw `new` is prohibited

#### 1.3.4 Data Correctness (High Priority)

- [ ] **Delete Bitmap version**: In Cloud mode, is `TEMP_VERSION_COMMON` manually replaced before use?
- [ ] **Version consistency**: Does data reading strictly not exceed Partition visible version?
- [ ] **MoW setting**: Is `SegmentWriterOptions::enable_unique_key_merge_on_write` correctly set to `true`?
- [ ] **EditLog persistence**: Do metadata modifications call `editLog.logXxx()`? Is replay logic strictly equivalent to master?

#### 1.3.5 Observability (Medium Priority)

- [ ] **Metrics addition**: Are `ADD_TIMER/ADD_COUNTER` added to critical paths? Does naming follow `module_xxx` convention?
- [ ] **Log levels**: Use `LOG(ERROR)` for error logs, `LOG(INFO)` for critical paths, `VLOG` or `DVLOG` for debug info
- [ ] **Trace information**: Do distributed operations (RPC, transactions) include trace ID for tracking?

### 1.4 Test Coverage Principles

- All kernel features **must** have corresponding tests
- End-to-end SQL functionality should prioritize adding regression tests under `regression-test/` (Groovy DSL)
- Also add BE unit tests (`be/test/`, Google Test) and FE unit tests (`fe/fe-core/src/test/`, JUnit 5) where possible. For complex features, must be modular with unit tests.
- Regression test result files (`.out`) **must not be handwritten**, must be auto-generated via `-genOut`
- Regression tests must use `order_qt_` prefix or manual `ORDER BY` to ensure ordered results
- Expected error cases use `test { sql "..."; exception "..." }` pattern
- Tables should be **DROPped before use** (not after tests end), to preserve environment for debugging
- Simple tests should hardcode table names directly, not use `def tableName`

---

## Part 2: BE Module Review Guide

### 2.1 Error Handling (Highest Priority)

#### 2.1.1 Status / Exception / Result\<T\> Relationship

| Type | Usage | Propagation Macro |
|------|-------|------------------|
| `Status` | Default error return type, `[[nodiscard]]` | `RETURN_IF_ERROR` |
| `Exception` | Vectorized execution engine hot path, expression evaluation | `RETURN_IF_CATCH_EXCEPTION` |
| `Result<T>` | Return value or error (replaces old output parameter pattern) | `DORIS_TRY` |

**Review Checkpoints:**

- [ ] Are `Status` return values checked? Search for `static_cast<void>(...)` pattern—this is the **most dangerous anti-pattern**, silently swallowing errors without logging. Currently 171+ occurrences in codebase. Never allow this pattern in new code
- [ ] Where `THROW_IF_ERROR` is used, is there `RETURN_IF_CATCH_EXCEPTION` or `RETURN_IF_ERROR_OR_CATCH_EXCEPTION` above in the call chain to catch? If not, exceptions propagate uncaught causing crashes. Current version has related catch in upper layers when Pipeline executes specific operators.
- [ ] **`THROW_IF_ERROR` prohibited in Defer/RAII destructor lambdas**: Throwing exceptions during stack unwinding triggers `std::terminate`. Known issues: `pipeline_task.cpp:412-413, 638-639`. Use `WARN_IF_ERROR` or `static_cast<void>` + logging in Defer
- [ ] **`THROW_IF_ERROR` prohibited in Thrift RPC handlers without try/catch protection**: Thrift framework doesn't catch Doris exceptions, causing connection drops. Known issues: `backend_service.cpp:1323,1326`. RPC handlers should use `RETURN_IF_ERROR` pattern or outermost try/catch
- [ ] Is `WARN_IF_ERROR` usage reasonable? Only allowed in destructors, cleanup paths, best-effort operations. Warning message must not be empty string
- [ ] Is `DORIS_CHECK` used for **invariant assertions** (not defensive programming)? Correct usage example: `DORIS_CHECK(hash_table)` asserts hash table is initialized
- [ ] Do new catch blocks correctly convert exceptions? Should follow standard pattern from `internal_service.cpp:348-357`: catch `doris::Exception` first, then `std::exception`, finally `...`

#### 2.1.2 Cloud RPC Error Handling

The `retry_rpc` template in `cloud_meta_mgr.cpp` is the standard pattern for RPC error handling in Cloud mode:
- [ ] Do new Cloud RPC calls use `retry_rpc`?
- [ ] Does `INVALID_ARGUMENT` return directly without retry? (Correct behavior)
- [ ] Does `KV_TXN_CONFLICT` use an independent conflict retry counter?
- [ ] Do timeout and retry counts use config variables instead of hardcoding?

#### 2.1.3 compile_check Mechanism

`compile_check_begin.h` / `compile_check_end.h` elevate `-Wconversion` to **compile error** (excluding sign-conversion and float-conversion).

- [ ] Do new `.h` files include paired `compile_check_begin.h` / `compile_check_end.h` in declaration regions?
- [ ] **Coverage status**: Currently only 31.1% (1,049/3,372) of source files use compile_check. Additionally, 218 files contain `compile_check_begin.h` but lack corresponding `compile_check_end.h` (unpaired). New files should always include complete pairing
- [ ] Are there implicit narrowing conversions? Pay special attention to `int64_t -> int32_t`, `size_t -> int`, `uint64_t -> int64_t`
- [ ] If bypassing due to third-party code is necessary, are `compile_check_avoid_begin.h` / `compile_check_avoid_end.h` used?

### 2.2 Memory Management

#### 2.2.1 MemTracker Hierarchy

Doris uses two-level memory tracking:
- **`MemTrackerLimiter`**: Heavy-weight tracker with limits (QUERY / LOAD / COMPACTION / SCHEMA_CHANGE / METADATA / CACHE types)
- **`MemTracker`**: Lightweight tracker without limits, for fine-grained sub-component tracking

Thread-level tracking is implemented via `ThreadMemTrackerMgr`, each thread has one active `MemTrackerLimiter` and a `MemTracker` stack.

**Review Checkpoints:**

- [ ] Do newly started threads/bthreads use `SCOPED_ATTACH_TASK` at entry? Without it, memory is counted in orphan tracker, debug builds trigger assertions
- [ ] Does temporary tracker switching use `SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER` (supports nesting)?
- [ ] **`ThreadPool::submit_func` callbacks**: Do lambdas submitted to thread pools use `SCOPED_ATTACH_TASK` internally? Currently 87+ `submit_func` calls in codebase, many rely on "indirect attach" (attach happens deep in call chain), which is fragile. Correct approach references gold-standard wrapper pattern from `calc_delete_bitmap_executor.h:67-83`: explicit attach at callback entry. Known problematic files: `s3_file_bufferpool.cpp:114`, `wal_manager.cpp:414`, `download_action.cpp:166`
- [ ] Is `try_reserve()` called before large memory allocation? Is there `DEFER_RELEASE_RESERVED()` to ensure release after allocation?
- [ ] Does memory hook path (`ThreadMemTrackerMgr::consume`) avoid operations that may trigger memory allocation (e.g., LOG, string formatting)? `_stop_consume` flag exists to prevent recursion

#### 2.2.2 Object Lifecycle

- [ ] Are classes using `ENABLE_FACTORY_CREATOR` created via `create_shared()` / `create_unique()`? Raw `new` is prohibited
- [ ] Are there `shared_ptr` circular references? Standard ways to break cycles:
  - `Dependency::_shared_state` uses **raw pointer** (non-owning)
  - `Dependency::_blocked_task` uses `vector<weak_ptr<PipelineTask>>`
  - `TrackerLimiterGroup::trackers` uses `list<weak_ptr<MemTrackerLimiter>>`
- [ ] Is Rowset's dual reference counting used correctly? `acquire()` / `release()` is manual reference counting (for performance), `shared_ptr` manages ownership. When `release()` decrements to 0 and in UNLOADING state, triggers `do_close()`
- [ ] Are rowsets in `_unused_rowsets` judged deletable via `use_count() == 1`?
- [ ] Are there suspicious `shared_ptr` circular reference issues? For clear lifecycles, prefer `unique_ptr` + raw pointer observer pattern.

#### 2.2.3 COW Column Semantics

Vectorized columns (`IColumn`) use custom intrusive reference-counted Copy-on-Write mechanism:

- [ ] Is exclusive ownership ensured when calling `mutate()`? When `use_count() > 1`, triggers deep copy, which is a performance hotspot
- [ ] Is `assume_mutable_ref()` only used when exclusive ownership is confirmed? This method **does not check reference count**, using it in shared state causes data corruption
- [ ] Is `set_columns()` called to restore after `Block::mutate_columns()`? After calling, original Block's column pointers are null
- [ ] Can the pointer returned by `convert_to_full_column_if_const()` share with original column? For `ColumnConst` it materializes a new column, but for normal columns returns shared pointer

#### 2.2.4 Cache Lifecycle

- [ ] Is `cache->release(handle)` called after using `Cache::Handle*`? Omission causes memory leaks
- [ ] Do cache values inherit from `LRUCacheValueBase`? This base class automatically releases tracking bytes on destruction
- [ ] Are new cache types registered with `CacheManager`? Unregistered caches don't participate in global GC

### 2.3 Concurrency and Locks

#### 2.3.1 Tablet Lock Hierarchy

The `Tablet` class has 8+ independent locks, with confirmed usage rules:

| Lock | Type | Rule |
|----|------|------|
| `_meta_lock` | `shared_mutex` | Modifying version map requires EXCLUSIVE; reading requires SHARED |
| `_rowset_update_lock` | `mutex` | Serializes delete bitmap updates during MoW table publish_txn, **independent of _meta_lock** (avoids blocking read path) |
| `_base_compaction_lock` | `mutex` | Serializes base compaction |
| `_cumulative_compaction_lock` | `mutex` | Serializes cumulative compaction |
| `_migration_lock` | `shared_timed_mutex` | Protects tablet migration |
| `_cooldown_conf_lock` | `shared_mutex` | Protects cooldown configuration |

**Review Checkpoints:**

- [ ] Do operations on `_rs_version_map` or `_stale_rs_version_map` hold `_meta_lock` (shared or exclusive)?
- [ ] Do `TabletMeta` modification operations hold the owning Tablet's EXCLUSIVE `_meta_lock`?
- [ ] Is there acquisition of other tablet locks while holding `_meta_lock`? This may cause deadlock

#### 2.3.2 TxnManager Lock Order

Confirmed consistent lock order: `_txn_lock` -> `_txn_map_lock` (applies to commit, publish, delete, rollback).
- [ ] Do new TxnManager operations follow this lock order?

#### 2.3.3 Pipeline Dependency Concurrency

`Dependency` uses double-checked locking + atomic `_ready` flag + mutex `_task_lock`:
- `set_ready()`: First check `_ready` (fast path), then lock to set `_ready = true` and wake up
- `is_blocked_by()`: Lock, check `_ready`, add to wait list if not ready

- [ ] Do new `Dependency` subclasses correctly call `block()` / `set_ready()`?
- [ ] Do `CountedFinishDependency`'s `add()` / `sub()` operate under `_mtx` protection?

#### 2.3.4 Atomic Operation Memory Order

Memory order usage patterns in codebase:
- `memory_order_relaxed`: Statistics counters (correct)
- `memory_order_acquire/release`: Lifecycle flags like `ExecEnv::_s_ready` (correct)
- `memory_order_acq_rel`: CAS close operations (correct)

- [ ] Do new atomic operations use appropriate memory order? Use relaxed for statistics counters, at least acquire/release for flag/signal
- [ ] Does `std::atomic<bool>` as stop signal use at least acquire/release? Relaxed may theoretically delay propagation

### 2.4 Type System and Serialization

#### 2.4.1 Upgrade/Downgrade Compatibility

Upgrade/downgrade compatibility no longer uses be_exec_version for control. Instead, each feature (e.g., functions, compression algorithms, etc.) adds its own option for confirmation. BE uses is_set&&is_true to branch or replace functionality at appropriate nodes.

#### 2.4.2 Decimal Precision Overflow

- [ ] Do Decimal operation results check precision upper limit? `decimal_result_type()` may promote result to DECIMAL256, causing unexpected memory layout changes
- [ ] Are DecimalV2's `original_precision` / `original_scale` correctly set? Default value `UINT32_MAX` means "unknown", `get_format_scale()` returns different value from `scale` in this case

#### 2.4.3 Thrift Type Mapping

- [ ] Are `TScalarType` optional fields (`precision`, `scale`, `len`) set when needed? DECIMAL needs precision/scale, CHAR/VARCHAR needs len, DATETIMEV2 needs scale
- [ ] Is depth-first flattened representation of `TTypeDesc.types` correctly traversed? Off-by-one errors are common with nested complex types (ARRAY<MAP<K,V>>)

#### 2.4.4 Block Merge Nullable Handling

`MutableBlock::merge_impl()` assumes target is Nullable and source is non-Nullable when types mismatch, using C-style cast `(DataTypeNullable*)` instead of `dynamic_cast`.

- [ ] Does new Block merge logic correctly handle Nullable promotion? DCHECK only protects in debug builds

### 2.5 Pipeline Execution Engine

#### 2.5.1 Operator Lifecycle

Pipeline Task state machine: `INITED -> RUNNABLE -> BLOCKED -> FINISHED -> FINALIZED`

- [ ] Are `SharedState`'s source_deps and sink_deps correctly connected? `inject_shared_state()` is responsible for connection

#### 2.5.2 Memory Reservation and Spill

- [ ] Do memory-intensive operations (Hash Join build, aggregation) use `_try_to_reserve_memory()`?
- [ ] Is there `_memory_sufficient_dependency` for backpressure when memory is insufficient?
- [ ] Does `revoke_memory()` correctly implement spill logic?

### 2.6 Storage Engine

#### 2.6.1 Rowset Version Management

- [ ] Are `add_rowset()` and `modify_rowsets()` executed under exclusive `_meta_lock`?
- [ ] Are version ranges continuous? There should be no version gaps in `_rs_version_map`
- [ ] Does compaction output rowset's version range correctly cover input rowsets?

#### 2.6.2 Delete Bitmap (MoW Tables, Extremely High Priority)

- [ ] In Cloud mode, is **TEMP_VERSION_COMMON manually replaced** for bitmaps obtained from `CloudTxnDeleteBitmapCache`? Code comments explicitly warn about this issue (`cloud_txn_delete_bitmap_cache.h:72-75`). Omission causes bitmap to be applied to wrong version
- [ ] Is `_rowset_update_lock` held during delete bitmap calculation? Prevents concurrent calculation on same (txn_id, tablet_id)
- [ ] Does compaction's delete bitmap calculation use **latest** compaction stats (`ms_base_compaction_cnt`, etc.)? Stale stats will miss recently compacted rowsets

#### 2.6.3 Segment Writing

- [ ] Is `SegmentWriterOptions::enable_unique_key_merge_on_write` correctly set for MoW tables? Default value is `false`, omission causes deduplication to be silently skipped
- [ ] Is performance impact of row-stored columns (`_serialize_block_to_row_column`) on wide tables within expectations?

### 2.7 IO Subsystem

- [ ] Do `FileSystem` operations use `FILESYSTEM_M` macro to handle bthread async IO?
- [ ] Do remote storage (S3/HDFS) reads use `create_cached_file_reader()` for file caching?

### 2.8 Inverted Index Subsystem

#### 2.8.1 Lifecycle and Destruction Order

- [ ] **Field declaration order is destruction order**: In `inverted_index_writer.h:92-95`, `_dir` must be declared **before** `_index_writer`, because `_index_writer`'s `write.lock` is created by `_dir.lockFactory`, C++ destructs in reverse declaration order. New fields must follow this constraint
- [ ] **Cache handle destruction extends LRU retention**: `inverted_index_cache.h:142-152`, `InvertedIndexCacheHandle` destructor sets `last_visit_time` to future timestamp (`UnixMillis() + config::index_cache_entry_stay_time_after_lookup_s * 1000`), making actually queried searchers survive longer in LRU. Be aware of this extension mechanism when modifying cache logic

#### 2.8.2 CLucene Exception Handling

CLucene doesn't use C++ exceptions, `inverted_index_common.h:65-111` implements `ErrorContext` + FINALLY macro system:
- `FINALLY`: Catches exceptions and returns `Status` error
- `FINALLY_EXCEPTION`: Catches and rethrows
- All three macros use `static_assert` to enforce existence of `error_context` local variable in scope

- [ ] Does new CLucene-related code use `ErrorContext` + `FINALLY` pattern instead of raw try/catch?

#### 2.8.3 Three-Valued Logic Bitmap

`InvertedIndexResultBitmap` (`inverted_index_reader.h:80-214`) implements SQL three-valued logic (TRUE/FALSE/NULL), storing separately via `_data_bitmap` + `_null_bitmap`:
- [ ] **`op_not` is const but actually modifies data**: Modifies pointed object via `shared_ptr` to bypass const qualifier. Callers should not assume const reference returned results are immutable
- [ ] `operator|=` implements complete SQL three-valued OR semantics (lines 148-157), same for `operator&=` and `operator-=`. New bitmap operations must follow three-valued logic

### 2.9 Schema Change Subsystem

#### 2.9.1 Three-Strategy Selection and Lock Order

`_get_sc_procedure()` (`schema_change.h:303-315`) selects among three strategies based on change type:
- `LinkedSchemaChange`: Column name/type alias changes only, zero-copy
- `VSchemaChangeDirectly`: Direct conversion without sorting
- `VLocalSchemaChangeWithSorting`: Full rewrite with sorting

**Lock acquisition order** (`schema_change.cpp:972-976`): `base_tablet push_lock → new_tablet push_lock → base_tablet header_lock → new_tablet header_lock`

- [ ] Do schema change lock acquisitions follow this four-level order? Out-of-order causes deadlock

#### 2.9.2 MOW Delete Bitmap Four-Step Flow (Extremely High Priority)

`schema_change.cpp:1595-1657` describes critical correctness flow for MOW table schema change:
1. Newly imported rowsets during dual-write **skip** delete bitmap calculation
2. After conversion completes, calculate delete bitmap for rowsets during dual-write period (lines 1618-1634)
3. **Block new publish**, calculate delete bitmap for incremental rowsets (lines 1636-1652)
4. Switch tablet state to `TABLET_RUNNING` (lines 1653-1656)

- [ ] When modifying schema change delete bitmap path, are the four-step order and blocking logic complete? Step 3's blocking is key to ensuring consistency

#### 2.9.3 Column Deletion Order Trap

`return_columns` is calculated **before** `merge_dropped_columns()` (`schema_change.cpp:959-964` vs line 1062), this is intentional: `return_columns` only covers non-dropped columns, dropped columns are handled internally by `SegmentIterator` for delete predicate evaluation.

- [ ] When modifying schema change column processing logic, is the timing of `return_columns` and dropped columns calculation understood?
- [ ] `_check_row_nums` can be bypassed by `config::ignore_schema_change_check` (default false) (`schema_change.h:155-171`), this config should remain off in production

### 2.10 Workload Group Memory Tracking

`WorkloadGroup` (`workload_group.h`) uses dual-lock pattern: `_mutex` protects name/version/memory limit/resource_ctxs/shutdown state, `_task_sched_lock` protects scheduler and cgroup pointers.

- [ ] **Memory tracking inconsistency**: `check_mem_used()` (lines 115-123) calculates `_total_mem_used + _wg_refresh_interval_memory_growth` for watermark check, but `exceed_limit()` (lines 132-135) **only compares `_total_mem_used > _memory_limit`**, ignoring `_wg_refresh_interval_memory_growth`. This means during refresh interval, workload group may exceed hard limit without being detected by `exceed_limit()`
- [ ] Does new memory check logic use the correct method? If precise limit checking is needed, should reference `check_mem_used()` approach

---

## Part 3: FE Module Review Guide

### 3.1 Metadata Persistence (EditLog)

#### 3.1.1 Write Path

All metadata modifications must go through EditLog persistence. Flow:
1. Modify in-memory state
2. Call `editLog.logXxx()` to write log
3. Log is replicated to Follower via BDBJE
4. Caller blocks until persistence confirmation

**Review Checkpoints:**

- [ ] Do new DDL/DML operations call corresponding `editLog.logXxx()` methods? Omission causes Follower state inconsistency
- [ ] Do new OperationTypes have corresponding replay handling in `EditLog.loadJournal()` switch?
- [ ] Is replay logic **strictly equivalent** to master execution logic? Any difference causes Follower state drift
- [ ] Do new serializable objects correctly implement Gson serialization? Are `@SerializedName` annotations complete?
- [ ] Can Image checkpoint correctly include new metadata?
- [ ] Can master failover at any moment guarantee no duplicate or lost data? Short-term task failures are tolerable, but memory/transaction leaks and data inconsistencies are not allowed.

#### 3.1.2 EditLog Batch and Outside-Lock Commit Mode

`DatabaseTransactionMgr` implements two EditLog commit modes, controlled by `Config.enable_txn_log_outside_lock`:

- **Inside-lock mode** (default): Directly calls `editLog.logInsertTransactionState()` within transaction lock
- **Outside-lock mode**: Only modifies in-memory state and calls `submitEdit()` to submit to queue within lock, waits for persistence completion via `awaitTransactionState()` outside lock (`DatabaseTransactionMgr.java:383-395`)

Fuzzy testing (`DorisFE.java:612-615`) randomly switches between these two modes, so both paths must be correct.

**Review Checkpoints:**

- [ ] Do modifications involving transaction EditLog consider both inside-lock and outside-lock commit paths?
- [ ] In outside-lock mode, are `awaitTransactionState()` timeout and exception handling correct? Timeout doesn't mean failure—transaction may still be committing
- [ ] Do new transaction state modifications complete all in-memory state changes before `submitEdit()` submission? After submission, other threads may immediately see new state

#### 3.1.3 EditLog Fatal Error Handling

When EditLog flush fails, FE calls `System.exit(-1)` to terminate process—this is **intentional**, preventing split-brain.
- [ ] Does new code catch and swallow exceptions in EditLog-related paths? This may cause metadata loss

### 3.2 Lock Hierarchy and Concurrency

#### 3.2.1 Confirmed Lock Orders

| Scenario | Lock Order |
|------|------|
| ConsistencyChecker | `jobs_lock` -> `synchronized(job)` -> `db_lock` |
| AlterJobV2 | `synchronized(job)` -> `db_lock` |
| TabletScheduler | `synchronized(this)` **internal** operations, then **release**, then acquire `db_lock` (two-step method avoids deadlock) |
| DatabaseTransactionMgr | Its RWLock is **leaf lock**, should not acquire other locks internally |
| MTMVTask | Acquire table locks after sorting by table ID (avoids deadlock) |

**Review Checkpoints:**

- [ ] Do new lock acquisitions follow the above lock orders?
- [ ] Is `tryLock(timeout)` used instead of infinite-wait `lock()`? FE's standard pattern is `tryLock` + timeout logging
- [ ] **Lock timeout standard values**: Env lock = 5s, DB write lock = 100s, Table read lock = 1min. Operations involving multiple tables must lock in ascending table ID order (implemented via `PriorityQueue`) to prevent deadlock
- [ ] Does `Database.tryWriteLock()` log current owner thread on timeout?
- [ ] Are there paths acquiring database lock within `synchronized` block? This is a known deadlock risk, should use two-step method

#### 3.2.2 ConcurrentModificationException

- [ ] Is traversal of shared collections under lock protection? `CatalogRecycleBin.write()` added `synchronized` because dump image API concurrent access caused CME
- [ ] Can there be concurrent modifications during traversal of `Map` / `List`? Prefer `ConcurrentHashMap` or snapshot before traversal

#### 3.2.3 Known Deadlock Cases (New Code Must Avoid Similar Patterns)

The following are confirmed deadlock risk points in codebase, be extra vigilant when reviewing similar patterns:

| Location | Pattern | Description |
|------|------|------|
| `MasterCatalogExecutor.java:70-78` | Initiating RPC while holding catalog lock | RPC timeout blocks catalog operations for long time |
| `ExternalDatabase.java:137-158` | Nested acquisition of db lock + makeSureInitialized() | Comments explicitly mark deadlock avoidance strategy |
| `HiveMetaStoreCache.java:126-130` | Acquiring cache lock in thread pool callback | Comment marks "this may cause a deadlock" |
| `CacheFactory.java:85-92` | Circular dependency in cache initialization | Avoided via lazy initialization |

- [ ] Does new code initiate RPC or IO operations while holding locks? This is the most common source of deadlock and performance issues
- [ ] Does new code acquire locks in callbacks (e.g., thread pool, cache loader) that callers may hold?

### 3.3 Nereids Optimizer

#### 3.3.1 Rule Implementation Specifications

**Review Checkpoints:**

- [ ] Is new Rule's `RuleType` added to `RuleType.java` enum?
- [ ] Does Rule correctly handle **all Join types**? Pay special attention to ASOF variants (`ASOF_LEFT_INNER_JOIN`, etc.). `COULD_PUSH_THROUGH_LEFT/RIGHT` lists in `PushDownFilterThroughJoin` explicitly include ASOF types
- [ ] Are constant predicates (`slots.isEmpty()`) correctly handled? Inner Join can push to both sides, Outer Join cannot
- [ ] Is there **infinite loop risk**? Can created nodes be matched again by another rule in same batch? Code comments have multiple "separate topDown passes to avoid dead loops" warnings
- [ ] Does `OneRewriteRuleFactory` pattern match correct wrapper nodes? For example, `EliminateJoinByFK` only matches `project(join)` not bare `join`
- [ ] Are predicates checked for `containsUniqueFunction()`? Predicates containing unique functions cannot be pushed down

#### 3.3.2 Rewriter Stage Order

Rewriter organizes rules into multi-stage pipeline, key dependencies:
- `PUSH_DOWN_FILTERS` is called 5-6 times throughout pipeline, because it interacts with many other transformations
- `InferPredicates` is called twice (with `PUSH_DOWN_FILTERS` in between), because eliminating outer join exposes new inference opportunities
- `costBased(...)` rules are controlled by `runCboRules` flag

- [ ] Is new rewrite rule placed in correct stage? For example, rules needing predicate pushdown should be after `PUSH_DOWN_FILTERS`
- [ ] Is new exploration rule registered in `RuleSet`?

#### 3.3.3 Property Derivation

`RequestPropertyDeriver` implements top-down property requests:

- [ ] Does new Physical Operator have visit method in `RequestPropertyDeriver`?
- [ ] Are multiple alternative property requirements provided (e.g., HashJoin provides both shuffle and broadcast choices)? Single requirement narrows search space
- [ ] Is `PhysicalProject` alias penetration logic correct? Only `Alias(SlotRef)` and bare `SlotRef` can penetrate

### 3.4 Transaction Management

#### 3.4.1 Transaction Lifecycle

State machine: `PREPARE -> COMMITTED -> VISIBLE` (or `ABORTED`)

- [ ] After transaction commit, can `PublishVersionDaemon` correctly send `PublishVersionTask` to all BEs?
- [ ] Does publish wait for quorum confirmation?
- [ ] Does transaction correctly abort after timeout?

**Known race condition—`TransactionState.tableIdList`**: At `TransactionState.java:632`, TODO comment marks that `addTableId()` / `removeTableId()` lack synchronization protection. `tableIdList` is plain `ArrayList`, has race condition (CME or data loss) when multiple threads concurrently add/remove table IDs.

- [ ] Are operations on `TransactionState.tableIdList` under external lock protection? Or should it be replaced with thread-safe collection?

#### 3.4.2 Cloud Mode Differences

In Cloud mode, transactions are centrally managed by Meta-Service, not FE:
- [ ] Do Cloud mode transaction operations use `CloudGlobalTransactionMgr` instead of local `GlobalTransactionMgr`?
- [ ] **Unsafe downcasting prohibited**: Current code has 4 places forcibly casting `GlobalTransactionMgrIface` to `CloudGlobalTransactionMgr` (e.g., `InternalCatalog.java:994`). These casts cause ClassCastException in non-Cloud mode. New code should add required methods by extending `GlobalTransactionMgrIface` interface, not rely on downcasting
- [ ] Does version retrieval use `getVisibleVersion()` (RPC + cache in Cloud mode)?
- [ ] Is `getVisibleVersion()`'s `RpcException` correctly handled? Multiple callers catch and return 0, may cause MV refresh misjudgment

### 3.5 Exception System

```
UserException (checked)
  ├── AnalysisException      -- SQL analysis error
  ├── DdlException           -- DDL execution failure
  ├── MetaNotFoundException  -- Metadata lookup failure
  ├── LoadException          -- Load failure
  └── ...

NereidsException (unchecked, RuntimeException)
  ├── nereids/AnalysisException  -- Nereids-specific, different from common/
  └── nereids/ParseException
```

- [ ] **Note Nereids has its own `AnalysisException`** (`nereids/exceptions/AnalysisException.java`, extends RuntimeException), which is a **different class** from `common/AnalysisException` (checked)
- [ ] Does `ErrorReport.reportAnalysisException()` use correct `ErrorCode`? Custom Doris error codes start from 5000
- [ ] Does new RPC error handling use `Status` pattern (`TStatusCode` / `PStatus`)?

### 3.6 OlapTable Version Visibility

- [ ] `getVisibleVersion()` behaves differently in Cloud vs non-Cloud mode:
  - Non-Cloud: Returns local `tableAttributes.getVisibleVersion()`
  - Cloud: RPC to meta-service, has cache TTL
- [ ] Can Cloud mode's version cache TTL (`cloudTableVersionCacheTtlMs`) cause reading stale versions?
- [ ] `VERSION_NOT_FOUND` is forcibly converted to version=1 (`PARTITION_INIT_VERSION`)—cannot distinguish "never loaded" from "loaded to version 1"

### 3.7 External Catalog Concurrency Traps

#### 3.7.1 Initialization Race Condition

`ExternalCatalog.makeSureInitialized()` (`ExternalCatalog.java:354-379`) is synchronized, but has the following traps:

- [ ] **`isInitializing` guard is dead code in `ExternalCatalog`**: Although code checks `isInitializing` (line 358), it's never set to `true` (only set to `false` in finally). Compare with `ExternalDatabase.makeSureInitialized()` (`ExternalDatabase.java:137-158`) which **correctly** sets `isInitializing = true`. This means ExternalCatalog has no reentrancy protection
- [ ] **`lowerCaseToDatabaseName.clear()` race window**: `getFilteredDatabaseNames()` (line 504) first `clear()` then refills `ConcurrentMap`, this method is **not synchronized**. Between clear and refill completion, concurrent `get()` calls return null

#### 3.7.2 resetToUninitialized() NPE Risk

`resetToUninitialized()` (`ExternalCatalog.java:581-590`) sets `objectCreated` and `initialized` to false within synchronized block, then calls `onClose()` (lines 773-784) to set `executionAuthenticator` and `transactionManager` to null.

- [ ] Concurrent queries that have passed `makeSureInitialized()` may be using these fields being set to null. synchronized only protects callers of other synchronized methods, not non-synchronized read paths already in execution

### 3.8 Backup/Restore State Machine

#### 3.8.1 BackupJob State Machine and EditLog Timing

EditLog write timing in `BackupJob` state machine is **uneven**, this is the most important non-obvious convention:

- [ ] **PENDING→SNAPSHOTING doesn't write EditLog** (`BackupJob.java:620-624`): After FE restart, state reverts to PENDING, all snapshot tasks are regenerated and resent. Don't add EditLog when modifying this transition—restart recovery relies on this behavior
- [ ] **BarrierLog mechanism**: `prepareSnapshotTaskForOlapTableWithoutLock()` creates `BarrierLog` for each table and writes via `editLog.logBarrier()` (lines 648-657), returned `commitSeq` is stored in properties map, used to ensure snapshot consistency
- [ ] **UPLOAD_INFO stage errors only retry, not cancel** (`BackupJob.java:509-514`): This is the final stage, code comments explicitly state continuous retry until timeout even when encountering unrecoverable errors. Don't call `cancelInternal()` in this stage when modifying error handling

#### 3.8.2 RestoreJob state vs showState

`RestoreJob` has two independent state fields (`RestoreJob.java:157,162`):
- `state`: Serialized persistent actual state, master sets to FINISHED **before** writing EditLog
- `showState`: Non-serialized, only for display, master sets to FINISHED **after** writing EditLog (line 2191)

- [ ] This prevents follower from seeing FINISHED state before EditLog replication completes. After deserialization, `gsonPostProcess()` syncs `showState = state`. Must update both fields when modifying state transitions

---

## Part 4: Cloud Module Review Guide

### 4.1 FDB Transaction Mode

Cloud module uses FoundationDB (FDB) as the sole metadata storage. All metadata operations must be completed within FDB transactions.

#### 4.1.1 TxnKv Interface Specifications

- [ ] Is `TxnErrorCode` return value from each `Transaction::get()` / `commit()` checked? Marked as `[[nodiscard]]`
- [ ] Is `TXN_CONFLICT` correctly handled? `commit_txn_immediately` returns error without retry, `commit_txn_eventually` has lazy committer path
- [ ] Can transaction exceed FDB's 10MB limit? Large transactions need to use `TxnLazyCommitter` for batched commits
- [ ] Is `TXN_MAYBE_COMMITTED` correctly handled? This indicates commit may have succeeded or failed, requires idempotent retry

#### 4.1.2 Key Encoding Specifications

Three key spaces:
- `0x01`: User space (mutable metadata)
- `0x02`: System space (service registration, encryption keys)
- `0x03`: Version space (multi-version metadata with versionstamp, for snapshots)

- [ ] Do new keys use correct key space prefix?
- [ ] Does key encoding use `encode_bytes()` (order-preserving byte encoding) and `encode_int64()` (big-endian + sign marker)?
- [ ] Does compound key field order match query pattern? Range scan relies on key prefix

#### 4.1.3 Commit Path

Two commit paths:
- **`commit_txn_immediately`**: Single FDB transaction completes all operations (read txn info, move tmp_rowset, bump version, update stats)
- **`commit_txn_eventually`**: Uses lazy committer for batched commits when transaction is too large

- [ ] Are the three config switches for lazy commit (`is_2pc`, `enable_txn_lazy_commit`, `config::enable_cloud_txn_lazy_commit`) correctly combined?
- [ ] Is fallback path from `commit_txn_immediately` to `commit_txn_eventually` (triggered by `TXN_BYTES_TOO_LARGE`) correct?

**Known risk—`commit_txn` infinite loop**: The `commit_txn` entry in `meta_service_txn.cpp:3257-3341` uses `do { ... } while(true)` loop to dispatch `commit_txn_immediately` and `commit_txn_eventually`. When pending lazy-committed transactions are found, it retries, but this loop **has no independent retry count limit**. If lazy committer continuously fails to complete (e.g., FDB continuous conflicts), theoretically infinite loop.

- [ ] Do modifications involving `commit_txn` dispatch loop consider maximum retry count or total timeout?

#### 4.1.4 Versionstamp Usage

Transaction ID is derived from FDB versionstamp (`get_txn_id_from_fdb_ts`).

- [ ] Does code depending on FDB versionstamp correctly use `atomic_set_ver_key()` / `atomic_set_ver_value()`?
- [ ] Is `get_versionstamp()` only called after `commit()` succeeds?

### 4.2 Meta-Service RPC Specifications

#### 4.2.1 RPC Preprocessing

Each RPC uses `RPC_PREPROCESS` macro for preprocessing:
- StopWatch timing
- DORIS_CLOUD_DEFER sets response status and records metrics on return
- `RPC_RATE_LIMIT` performs per-instance QPS throttling

- [ ] Do new RPCs use `RPC_PREPROCESS` and `RPC_RATE_LIMIT`?
- [ ] Is response `MetaServiceCode` correctly set?

#### 4.2.2 MetaServiceProxy Automatic Retry

`MetaServiceProxy` decorator automatically retries (exponential backoff) on following error codes:
- `KV_TXN_STORE_*_RETRYABLE`, `KV_TXN_TOO_OLD`, `KV_TXN_CONFLICT`

- [ ] Do new RPCs need to be idempotent? Retry means same request may execute multiple times
- [ ] Do non-idempotent operations correctly handle `TXN_MAYBE_COMMITTED`?

### 4.3 Recycler Safety Specifications

Recycler is responsible for GC of expired/deleted data:

- [ ] Before deleting rowset data, are associated transactions aborted first (`abort_txn_for_related_rowset`)? Prevents commit-after-delete data loss
- [ ] Are recycle operations best-effort + next-round retry mode? Should not stop entire recycle flow due to single failure
- [ ] Is packed file reference count recycling correct? (`recycle_packed_files`)

### 4.4 Delete Bitmap (Cloud MoW, Extremely High Priority)

Cloud mode delete bitmap management fundamentally differs from shared-nothing mode:

- [ ] Bitmaps in `CloudTxnDeleteBitmapCache` contain sentinel marks and `TEMP_VERSION_COMMON`, **must manually replace version** before use. This is a critical correctness constraint explicitly marked in code comments
- [ ] Empty rowset marks returned by `is_empty_rowset()` are not actively cleared (rely on expiration cleanup), does this affect correctness?
- [ ] **`CloudTxnDeleteBitmapCache` LRU eviction side effect**: When cache entry is LRU evicted, `publish_status` is reset to `INIT`, causing next access to **completely recalculate** delete bitmap (not incremental update). This is correct but expensive. Additionally, marks written by `mark_empty_rowset()` are never actively cleared—they only disappear when entire cache entry is evicted. On high-frequency MoW tables with insufficient cache capacity, causes repeated recalculation
- [ ] Do modifications involving `CloudTxnDeleteBitmapCache` consider impact of state reset after eviction?
- [ ] Are `CalcDeleteBitmapTask`'s compaction stats consistent with latest state in meta-service?

---

## Part 5: Cross-Module Concerns

### 5.1 FE-BE Protocol Compatibility

- [ ] Do new `TPlanNodeType`s have corresponding branches in BE's processing switch?
- [ ] Can deprecated node types (e.g., `MERGE_JOIN_NODE`, `KUDU_SCAN_NODE`) still be gracefully handled?
- [ ] If new variables are added, are they correctly set in all paths where FE sends execution plans to BE?

### 5.2 Cloud Mode vs Shared-Nothing Mode

Code extensively uses `Config.isNotCloudMode()` / `Config.isCloudMode()` branches.

- [ ] Do new features consider both modes?
- [ ] In Cloud mode, tablets are not bound to specific BE—any compute node can load any tablet's metadata
- [ ] In Cloud mode, rowsets go through two-phase protocol (`prepare_rowset` -> `commit_rowset`), with recycle marker protection in between
- [ ] In Cloud mode, partition version is centrally managed by Meta-Service, not FE

### 5.3 Merge-on-Write Unique Key Tables

MoW tables are one of the most complex data paths, require special attention during review:

- [ ] Write path: Does `SegmentWriter::probe_key_for_mow()` correctly probe existing rowsets?
- [ ] Publish path: Is delete bitmap calculation under `_rowset_update_lock` protection?
- [ ] Query path: Is delete bitmap version-aligned before reading?
- [ ] Compaction path: Does `calc_delete_bitmap_for_compaction()` correctly handle `allow_delete_in_cumu_compaction`?
- [ ] Cloud mode: Does delete bitmap cache have expiration mechanism? Is `_unused_delete_bitmap` cleaned up promptly?

### 5.4 Performance Hotspot Concerns

- [ ] Does `SendBatchThreadPool` (64 threads, 102K queue) have saturation risk in heavy shuffle scenarios?
- [ ] Does `SegmentPrefetchThreadPool` (max 2000 threads) create too many OS threads in intensive scan scenarios?
- [ ] In Cloud mode, does label deduplication check perform linear scan on labels with many aborted transactions, causing performance cliff?
- [ ] Is serialization overhead of `_serialize_block_to_row_column()` for wide tables within expectations?
- [ ] COW columns trigger deep copy on `mutate()` when `use_count > 1`—are there unnecessary copies in high-concurrency scenarios?

---

## Part 6: Testing Review Guide

### 6.1 Regression Test Specifications

- [ ] Is `order_qt_` prefix or manual `ORDER BY` used to ensure ordered results?
- [ ] Do expected error cases use `test { sql "..."; exception "..." }` pattern?
- [ ] Are tables **DROPped before use** (`DROP TABLE IF EXISTS xxx`), not after test ends?
- [ ] Do simple tests directly hardcode table names instead of using `def tableName`?
- [ ] Do tests cover these seven scenarios: empty table, all null, mixed null, pure non-null, constant, constant null, nullable wrapper (`nullable()`)?
- [ ] Are result files (`.out`) auto-generated via `-genOut`? **Handwriting prohibited**

### 6.2 BE Unit Test Specifications

- [ ] Are Google Test's `TEST` / `TEST_F` macros used?
- [ ] Are tests independent? Not dependent on other tests' execution order
- [ ] Are `ASSERT_*` (failure terminates test) and `EXPECT_*` (failure continues execution) used correctly?
- [ ] Do memory-intensive tests use `MemTracker` to check for leaks?

### 6.3 FE Unit Test Specifications

- [ ] Is JUnit 5's `@Test` annotation used?
- [ ] Do test method names clearly describe test scenarios?
- [ ] Is `Assertions.assertXxx()` used instead of JUnit 4's static imports?
- [ ] Do tests involving concurrency use `CountDownLatch` / `CyclicBarrier` for synchronization?

### 6.4 FE-BE Integration Test Specifications
---

## Part 7: Known High-Risk Code Patterns Quick Reference

| Pattern | Risk Level | Location | Description |
|------|---------|------|------|
| `static_cast<void>(status_returning_call)` | **CRITICAL** | BE global (195+ places) | Silently swallows errors, no logging |
| Delete bitmap TEMP_VERSION not replaced | **CRITICAL** | `cloud_txn_delete_bitmap_cache.h:72-75` | Data visibility error |
| `be_exec_version` mismatch | **CRITICAL** | FE-BE communication all paths | Serialization format incompatibility causes data corruption |
| `(DataTypeNullable*)` C-style cast | **HIGH** | `block.h merge_impl` | No protection in release builds |
| `sizeof(size_t*)` vs `sizeof(size_t)` | **HIGH** | `data_type.cpp:195` | Happens to work on 64-bit, crashes on 32-bit |
| Lazy commit three-switch combination | **HIGH** | `meta_service_txn.cpp` | Missing config causes large transaction failure |
| DecimalV2 original_precision default UINT32_MAX | **HIGH** | `data_type_decimal.h` | Affects string representation correctness |
| `MOW` default false | **MEDIUM** | `segment_writer.h:70` | Omission causes deduplication to be silently skipped |
| Cloud version cache TTL | **MEDIUM** | `OlapTable.getVisibleVersion()` | May read stale version |
| Label scan linear complexity | **MEDIUM** | `meta_service_txn.cpp` | Many aborted txns cause performance cliff |
| `THROW_IF_ERROR` no outer catch | **MEDIUM** | vec/ execution engine | Uncaught exception propagation causes crash |
| `THROW_IF_ERROR` in Defer lambda | **CRITICAL** | `pipeline_task.cpp:412-413, 638-639` | Throwing exception during stack unwinding causes `std::terminate` |
| `THROW_IF_ERROR` in Thrift RPC handler | **HIGH** | `backend_service.cpp:1323,1326` | No try/catch protection, connection drops |
| `DCHECK` disappears in RELEASE builds | **HIGH** | BE global (2,872 places) | No invariant protection in production, should migrate to `DORIS_CHECK` |
| `ThreadPool::submit_func` no SCOPED_ATTACH_TASK | **HIGH** | BE global (87+ places) | Memory counted in orphan tracker, debug crashes |
| `TransactionState.tableIdList` no sync | **MEDIUM** | `TransactionState.java:632` | ArrayList concurrent operations cause CME or data loss |
| `GlobalTransactionMgrIface` unsafe downcast | **MEDIUM** | `InternalCatalog.java:994` etc 4 places | ClassCastException in non-Cloud mode |
| `commit_txn` dispatch loop no retry limit | **MEDIUM** | `meta_service_txn.cpp:3257-3341` | Theoretically infinite loop when lazy commit continuously fails |
| `CloudTxnDeleteBitmapCache` LRU eviction reset | **MEDIUM** | `cloud_txn_delete_bitmap_cache.h` | After eviction, publish_status resets to INIT, triggers full recalculation |
| TabletScheduler synchronized + db lock | **MEDIUM** | `TabletScheduler.java` | Mitigated with two-step method, new code must follow |
| `getVisibleVersion()` `RpcException` | **MEDIUM** | FE Cloud mode multiple places | Catch and return 0 causes MV misjudgment |
| FE/BE function return type mismatch | **HIGH** | FE SIGNATURES vs BE get_return_type_impl | build() throws INTERNAL_ERROR, query fails directly |
| Null framework disabled without custom null map | **HIGH** | `use_default_implementation_for_nulls()=false` | Null row data treated as valid data |
| IFunction subclass has member data | **HIGH** | `SimpleFunctionFactory` registration | `sizeof` static_assert compilation failure or UB |
| Aggregate function Nullable/NotNullable tag error | **HIGH** | `helpers.h creator_without_type` | count returns null or sum doesn't return null |
| Variadic function family name mismatch | **MEDIUM** | `SimpleFunctionFactory::get_function` | BE function lookup fails, returns nullptr |
| `need_replace_null_data_to_default` omitted | **MEDIUM** | Arithmetic/string functions | Garbage data at null positions causes overflow or huge memory allocation |
| ColumnString offsets update error | **MEDIUM** | String function execute_impl | Silently corrupts subsequent row data |
| Inverted index field declaration order | **HIGH** | `inverted_index_writer.h:92-95` | `_dir` must be declared before `_index_writer`, otherwise use-after-free on destruction |
| `op_not` const but modifies data | **MEDIUM** | `inverted_index_reader.h:170` | Modifies via shared_ptr bypassing const, callers assume immutable |
| Schema change four-level lock order | **HIGH** | `schema_change.cpp:972-976` | base push→new push→base header→new header, out-of-order deadlock |
| MOW schema change delete bitmap four steps | **CRITICAL** | `schema_change.cpp:1595-1657` | Step 3 must block publish, omission causes bitmap inconsistency |
| `ExternalCatalog.isInitializing` dead code | **MEDIUM** | `ExternalCatalog.java:358` | Never set to true, reentrancy protection ineffective |
| `lowerCaseToDatabaseName.clear()` race | **HIGH** | `ExternalCatalog.java:504` | Concurrent get returns null between clear and refill |
| `resetToUninitialized()` sets fields to null | **HIGH** | `ExternalCatalog.java:581-590` | Concurrent queries encounter null fields midway causing NPE |
| BackupJob PENDING→SNAPSHOTING no EditLog | **MEDIUM** | `BackupJob.java:620-624` | Intentionally no log, restart reverts to PENDING and resends |
| RestoreJob dual state fields | **MEDIUM** | `RestoreJob.java:157,162` | state set to FINISHED first, showState after EditLog |
| WorkloadGroup `exceed_limit` omission | **MEDIUM** | `workload_group.h:132-135` | Only checks `_total_mem_used`, ignores refresh interval increment |

---

## Part 8: Function System Review Guide (Non-Obvious Conventions Only)

This section only records **implicit conventions and traps in function development that are not easily apparent from the code itself**. Interface definitions, registration steps, and other content directly readable from code are not repeated.

### 8.1 FE-BE Consistency Traps (Most Common Issues)

- [ ] **FE and BE independently calculate return types**: `FunctionBuilderImpl::build()` compares both sides' results, throws `INTERNAL_ERROR` on mismatch. Date/DateV2/DateTime/DateTimeV2 and Decimal versions allow interoperability, other types must strictly match
- [ ] **Function names must be all lowercase**: FE automatically `toLowerCase()` in `FunctionName` constructor, but BE `SimpleFunctionFactory` matches exact strings. Spelling or case mismatch = BE can't find function
- [ ] **Variadic function lookup key = function name + each parameter type's `get_family_name()`**: For example, `hex(String)` -> `"hexString"`, `hex(Int64)` -> `"hexInt64"`. FE-passed parameter type family names must correspond to BE-registered `get_variadic_argument_types_impl()`, otherwise lookup fails
- [ ] **FE/BE aliases must be registered on both sides**: If `register_alias()` alias doesn't have corresponding registration on FE side (alias list in `BuiltinScalarFunctions.java`), query fails
- [ ] **Aggregate function intermediate type mismatch causes shuffle serialization failure**—FE and BE must agree on `intermediate_type`

### 8.2 Null Handling Framework Traps

- [ ] **Must build custom null map after disabling `use_default_implementation_for_nulls()`**: This is the most common null-related bug. Framework defaults to automatically stripping `ColumnNullable` wrapper and OR-merging null map after execution; after disabling, function must handle completely
- [ ] **FE nullable semantics must match BE**: FE's `PropagateNullable` corresponds to BE default null framework enabled. If BE disables default null handling, FE-side nullable interface choice must match
- [ ] **`need_replace_null_data_to_default()` omission causes crashes**: Underlying data at null positions is garbage. String repeat count as garbage → huge memory allocation; arithmetic operation garbage → overflow. Operations that may produce such issues must be set to `true`
- [ ] **`ColumnConst(ColumnNullable(...))` nesting**: After disabling default null handling, const null columns are this nested type, need special checking. Framework **won't** auto-unpack when some parameters are const

### 8.3 BE Registration Implicit Constraints

- [ ] **`IFunction` subclasses prohibited from having member data**: `sizeof(Function) == sizeof(IFunction)` static_assert enforces at compile time. State should be placed in `FunctionContext`
- [ ] **`_foreach` combinator must be registered last**: It traverses all registered functions to auto-generate wrappers, early registration misses subsequent functions
- [ ] **Aggregate function Nullable/NotNullable tag directly affects result**: `NullableAggregateFunction` tag makes result nullable (e.g., sum), `NotNullableAggregateFunction` makes result non-nullable (e.g., count). Wrong tag causes count to return null or sum to not return null on all-null input
- [ ] **Aggregate function parameter tag affects nullable wrapper choice**: `UnaryExpression`/`MultiExpression`/`VarargsExpression` determines whether to use single-parameter or multi-parameter nullable wrapper
- [ ] **`ColumnString` doesn't include terminating zero**: `insert_data(ptr, len)`'s `len` doesn't include `\0`, `offsets` must be strictly correctly updated, otherwise silently corrupts subsequent rows

---

## Part 9: Development Standards Supplement

### 9.1 Configuration Parameter Addition Specifications

When adding new `config::xxx` parameters:

- [ ] Define in `be/src/common/config.h`, **must** provide default value and detailed comments
- [ ] Configuration name uses `snake_case`, add `// NOLINT` to avoid lint warnings
- [ ] Does it need to support dynamic modification? If yes, use `DEFINE_mBool/Int32/...` (m = mutable)
- [ ] In Cloud mode, does configuration need to be synchronously added in Meta-Service? Check configuration definitions under `cloud/src/meta-service/`

**Example:**
```cpp
DEFINE_mInt32(my_config, "100"); // NOLINT
// Describe configuration purpose, unit, scope of impact, rationale for default value choice
```

### 9.2 Metrics and Observability

New features should add monitoring metrics:

**BE Metrics:**
- [ ] Use `ADD_TIMER` / `ADD_COUNTER` / `ADD_GAUGE` macros to add metrics in critical paths
- [ ] Metric names use `snake_case`, prefix with module name (e.g., `compaction_xxx`, `query_xxx`)
- [ ] Expose to HTTP interface: `be_ip:webserver_port/metrics`

**Critical paths must monitor:**
- Memory allocation frequency and size statistics
- RPC call latency and error rate
- IO operation latency (S3/HDFS/OSS)
- Lock wait time (lock operations exceeding 100ms)

### 9.3 bthread/Coroutine Safety

BE extensively uses bthread, following are easily overlooked bthread-specific constraints:

- [ ] Avoid holding `std::mutex` for long time in bthread—this blocks entire bthread worker thread (not just current coroutine). Prefer `bthread_mutex_t`
- [ ] Use `bthread_usleep` instead of `sleep`/`usleep`, yields coroutine not thread

### 9.4 Error Message Specifications

- [ ] Error messages include context information: table name, partition ID, TabletID, transaction ID, operation type
- [ ] User-visible errors use `ErrorCode` definition (custom error codes start from 5000)

### 9.5 Code Comment Specifications

- [ ] **Lock-protected data**: Comment next to member variables `// protected by xxx_lock`
- [ ] **TODO/FIXME**: Include author, date, brief description, e.g., `// TODO(zhaochangle, 2026-03-01): optimize this path`
