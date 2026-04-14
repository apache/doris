---
name: code-review
description: Review code with Doris-specific checklists
compatibility: opencode
---

## What I do

Complete code review work to ensure code quality. Due to the length of content, you can read sections as needed based on the actual modifications being reviewed.

## When to use me

Use this when you need to review code, whether it is code you just completed or directly reviewing target code.

## How to use me

1. **Always read and respond to Part 1** (General Principles) — it applies to all code.
2. For module-specific review, **read the `AGENTS.md` in the corresponding source directory** listed in Part 2. Those files contain non-obvious conventions and traps specific to each subsystem.
3. Parts 3–7 cover cross-module concerns, testing, high-risk patterns, functions, and standards — refer as needed.

---

## Part 1: General Principles

### 1.1 Core Invariants

Always focus on the following core invariants during review:
1. **Data Correctness**: Data from any committed transaction must be visible to subsequent queries and not lost; data from uncommitted transactions must not be visible
2. **Version Consistency**: Partition visible version is the sole standard for data visibility; BE must strictly read data not exceeding this version
3. **Delete Bitmap Consistency** (MoW tables): delete bitmap must be strictly aligned with rowset versions; sentinel marks and `TEMP_VERSION`-style placeholders must be replaced with actual versions before use
4. **Memory Safety**: All significant memory allocations must be tracked by `MemTracker`; orphan memory is not allowed
5. **Error Means Failure**: Invariant violations should trigger exceptions or non-OK `Status`; silent continuation is never allowed

### 1.2 Review Principles

- **Follow Context**: Match adjacent code's error handling, interface usage, and lock patterns unless a clearly better approach exists
- **Reuse First**: Search for existing implementations before adding new ones; ensure good abstraction afterward. For example, in the implementation of SQL functions in BE, we prefer to use an existing base template rather than implementing everything from scratch. The same principle applies to the implementation of other features. Common parts should be abstracted as much as possible.
- **Code Over Docs**: When this skill conflicts with actual code, defer to code and note the mismatch
- **Performance First**: All obviously redundant operations should be optimized away, all obvious performance optimizations must be applied, and obvious anti-patterns must be eliminated.
- **Evidence Speaks**: All issues with code itself (not memory or environment) must be clearly identified as either having problems or not. For any erroneous situation, if it cannot be confirmed locally, you must provide the specific path or logic where the error occurs. That is, if you believe that if A then B, you must specify a clear scenario where A occurs.
- **Review Holistically**: For any new feature or modification, you must analyze its upstream and downstream code to understand the real invocation chain. Identify all implicit assumptions and constraints throughout the flow, then verify carefully that the current change works correctly within the entire end-to-end process. Also determine whether a seemingly problematic local pattern is actually safe due to strong guarantees from upstream or downstream, or whether a conventional local implementation fails to achieve optimal performance because it does not leverage additional information available from the surrounding context.

### 1.3 Critical Checkpoints (Review Priority)

For PRs with not only local minor modifications, before answering specific questions, you must read and deeply understand the complete process involved in the code modification, thoroughly comprehend the role, function, and expected functionality of the reviewed functions and modules, the actual triggering methods, potential concurrency, and lifecycle. It is necessary to understand the specific triggering methods and runtime interaction relationships, as well as dependencies, of the specific code in a concrete visual manner.

The following checkpoints must be **individually confirmed with conclusions** during review. If the code is too long or too complex, you should delve into the code again as needed when analyzing specific issues, especially the entire logic chain where there are doubts:

- What is the goal of the current task? Does the current code accomplish this goal? Is there a test that proves it?
- Is this modification as small, clear, and focused as possible?
- Does the code segment involve concurrency? If yes:
  - Which threads introduce concurrency, what are the critical variables? What locks protect them?
  - Are operations within locks as lightweight as possible? Are all heavy operations outside locks while maintaining concurrency safety?
  - If multiple locks exist, is the locking order consistent? Is there deadlock risk?
- Is there special or non-intuitive lifecycle management including static initialization order? If yes:
  - Understand the complete lifecycle and relationships of related variables. Are there circular references? When is each released? Can all lifecycles end normally?
  - For cross-TU static/global variables: does the initializer of one static variable depend on another static variable defined in a different translation unit? If yes, the initialization order is undefined by the C++ standard (static initialization order fiasco). Use constexpr, inline variables in the same header, or lazy initialization to fix.
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
- Has the test result been added/modified?
  - Are ALL the new test results correct?
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

If it involves the judgment of concurrent scenarios, it is necessary to find the starting point of concurrency and actually understand all actually possible concurrent situations (which thread initiated what at what stage, and what concurrent operations there will be). Due to the clear program semantics, some functions of the same module are executed in stages, so concurrency is definitely not present, there should be no misjudgment.

- [ ] **Thread context**: Does every new thread or bthread entry attach the right memory-tracking context? (See `be/src/runtime/AGENTS.md`)
- [ ] **Lock protection**: Is shared data accessed under the correct lock and mode?
- [ ] **Lock order**: Do nested acquisitions follow the module's documented lock order? (See storage, schema-change, cloud AGENTS.md)
- [ ] **Atomic memory order**: `relaxed` for counters only; at least `acquire/release` for signals and lifecycle flags

#### 1.3.2 Error Handling (Highest Priority)

- [ ] **Status checking**: Every `Status` must be checked; silent discard is prohibited
- [ ] **Exception propagation**: Is there a catch-and-convert boundary above every `THROW_IF_ERROR`? (See `be/src/common/AGENTS.md`)
- [ ] **Error context**: Do error messages include table, partition, tablet, or transaction identifiers?
- [ ] **Assertions**: `DORIS_CHECK` for invariants only, never speculative defensive checks

#### 1.3.3 Memory Safety (High Priority)

- [ ] **Reservation**: `try_reserve()` before large allocations, with guaranteed release on every exit path
- [ ] **Allocator-aware ownership**: In BE code, for memory-owning containers or buffers that should be tracked by Doris memory accounting, prefer allocator-aware types from `be/src/core/custom_allocator.h` such as `DorisVector`, `DorisMap`, and `DorisUniqueBufferPtr` instead of `std::vector`, `std::map`, and `std::unique_ptr`
- [ ] **Ownership**: See `be/src/runtime/AGENTS.md` for cache-handle, shared_ptr-cycle, and factory-creator rules

#### 1.3.4 Data Correctness (High Priority)

- [ ] **Version consistency**: Does data reading stay at or below the visible version?
- [ ] **MoW correctness**: See `be/src/storage/AGENTS.md` for delete-bitmap sentinel replacement, MoW enablement, and serialization rules
- [ ] **EditLog**: Are metadata changes logged and replayed equivalently? (See `fe/.../persist/AGENTS.md`)

#### 1.3.5 Observability (Medium Priority)

- [ ] Are critical new paths observable with appropriate log levels and metrics?
- [ ] Do distributed operations include enough identifiers for tracing?

### 1.4 Test Coverage Principles

- All kernel features **must** have tests; prioritize regression tests, add unit tests where practical
- `.out` files must be auto-generated, never handwritten
- Use `order_qt_` or explicit `ORDER BY` for deterministic output
- Expected errors: `test { sql "..."; exception "..." }`
- Drop tables before use, not after, to preserve debug state
- Hardcode table names in simple tests instead of `def tableName`

---

## Part 2: Module-Specific Review Guides (AGENTS.md)

Detailed module review guides live in `AGENTS.md` files in each source directory. **Read the relevant file** when reviewing module-specific code.

### BE Module

| Directory | Coverage |
|-----------|----------|
| `be/src/common/AGENTS.md` | Error handling and `compile_check` |
| `be/src/cloud/AGENTS.md` | Cloud RPC and CloudTablet sync rules |
| `be/src/runtime/AGENTS.md` | MemTracker, cache lifecycle, SIOF, workload-group memory tracking |
| `be/src/core/AGENTS.md` | COW columns, type metadata, serialization compatibility |
| `be/src/exec/AGENTS.md` | Pipeline execution, dependency concurrency, spill and memory pressure |
| `be/src/storage/AGENTS.md` | Tablet locks, rowset lifecycle, MoW delete bitmap, segment writing |
| `be/src/storage/index/inverted/AGENTS.md` | Inverted-index lifetime, CLucene boundary, three-valued bitmap |
| `be/src/storage/schema_change/AGENTS.md` | Schema-change strategy, lock order, MoW catch-up flow |
| `be/src/io/AGENTS.md` | Filesystem async path and remote reader caching |

### FE Module

| Directory | Coverage |
|-----------|----------|
| `fe/fe-core/AGENTS.md` | FE locking, exception model, visible-version semantics |
| `fe/fe-core/src/main/java/org/apache/doris/persist/AGENTS.md` | EditLog write/replay path and transaction persistence modes |
| `fe/fe-core/src/main/java/org/apache/doris/nereids/AGENTS.md` | Rule placement, mark-join constraints, property derivation |
| `fe/fe-core/src/main/java/org/apache/doris/transaction/AGENTS.md` | Transaction lifecycle, publish semantics, cloud manager usage |
| `fe/fe-core/src/main/java/org/apache/doris/datasource/AGENTS.md` | External catalog initialization and reset hazards |
| `fe/fe-core/src/main/java/org/apache/doris/backup/AGENTS.md` | Backup/restore log timing and replay ordering |

### Cloud Module

| Directory | Coverage |
|-----------|----------|
| `cloud/src/meta-store/AGENTS.md` | TxnKv, key encoding, versioned values, versionstamp helpers |
| `cloud/src/meta-service/AGENTS.md` | Cloud transaction commit paths, RPC retry contract, Cloud MoW contract |
| `cloud/src/recycler/AGENTS.md` | Recycler safety, two-phase delete, packed-file ordering |

---

## Part 3: Cross-Module Concerns

### 3.1 FE-BE Protocol Compatibility

- [ ] Do new `TPlanNodeType` values have matching BE handling?
- [ ] Are all FE-to-BE send paths updated when new transmitted variables are added?
- [ ] Is mixed-version compatibility preserved for serialization or function metadata changes?

### 3.2 Cloud Mode vs Shared-Nothing Mode

- [ ] Does the change handle both cloud and non-cloud paths where both exist?
- [ ] In Cloud mode, is centrally managed partition version still the visibility source of truth?
- [ ] Are prepare/commit rowset protocols respected for Cloud writes?

### 3.3 Merge-on-Write Unique Key Tables

- [ ] Does the write path probe or deduplicate against the correct rowset set?
- [ ] Is publish-side delete bitmap work correctly serialized?
- [ ] Is query-side delete bitmap usage version-aligned?
- [ ] Do compaction and schema-change paths preserve MoW-specific correctness steps?

### 3.4 Performance

- [ ] Are hot paths free of redundant copies, repeated scans, or avoidable allocations?
- [ ] Do cloud metadata loops avoid pathological retry or scan behavior?

---

## Part 4: Testing Review Guide

### 4.1 Regression Tests

- [ ] `order_qt_` or explicit `ORDER BY` used?
- [ ] Expected errors use `test { sql ...; exception ... }`?
- [ ] Tables dropped before use, not after?
- [ ] Table names hardcoded, not `def tableName`?
- [ ] `.out` files generated, not handwritten?

### 4.2 BE Unit Tests

- [ ] `TEST` / `TEST_F` used appropriately, independent of execution order?
- [ ] Memory or resource leaks covered where relevant?

### 4.3 FE Unit Tests

- [ ] JUnit 5 used consistently with descriptive test names?
- [ ] Concurrency tests use latches or barriers, not timing guesses?

---

## Part 5: Known High-Risk Patterns Quick Reference

These are cross-module patterns that cause silent or hard-to-diagnose failures. Module-specific traps are in the corresponding `AGENTS.md` files.

| Pattern | Risk | Why it's dangerous |
|--------|------|-------------|
| Ignored `Status` | Critical | Silent failure propagation; invariant violations go undetected |
| `THROW_IF_ERROR` in `Defer` or destructors | Critical | Terminates during stack unwinding |
| `THROW_IF_ERROR` without catch-and-convert boundary | High | Doris exception escapes its intended scope |
| Cross-TU static initializer dependency | High | Initialization order undefined; crashes or wrong values at startup |

---

## Part 6: Function System Review Guide

### 6.1 FE-BE Consistency

- [ ] FE and BE compute return types independently — do they agree?
- [ ] Function names lowercase and consistent on both sides?
- [ ] Variadic lookup depends on argument family names — do FE families match BE registration?
- [ ] Aliases registered on both FE and BE?
- [ ] Aggregate intermediate types match (critical for shuffle serialization)?

### 6.2 Null Handling

- [ ] If `use_default_implementation_for_nulls()` is disabled, is the null map fully handled manually?
- [ ] Do FE nullable semantics match BE implementation?
- [ ] Is `need_replace_null_data_to_default()` set correctly for garbage-payload null positions?
- [ ] Is nested `ColumnConst(ColumnNullable(...))` handled when default null handling is disabled?

### 6.3 BE Registration

- [ ] `IFunction` subclasses carry no member state?
- [ ] Order-sensitive combinators like `_foreach` placed correctly in registration?
- [ ] Aggregate Nullable/NotNullable tags match semantic result type?
- [ ] `ColumnString` offset maintenance exact (off-by-one corrupts later rows silently)?

---

## Part 7: Development Standards Supplement

### 7.1 Configuration

- [ ] New `config::xxx` has comments, defaults, and standard naming?
- [ ] Dynamic configs use the mutable form and are observed without restart?
- [ ] Cloud-visible behavior changes: does the meta-service config need updating too?

### 7.2 Metrics

- [ ] New metric names follow existing module naming patterns?

### 7.3 bthread Safety

- [ ] No long blocking under `std::mutex` on bthread paths?
- [ ] `bthread_usleep` used instead of thread-level sleep in coroutine code?

### 7.4 Error Messages and Comments

- [ ] User-visible errors include identifiers needed for debugging?
- [ ] Lock-protected members and non-obvious TODOs have precise local comments?

## Finally

Item-by-item responses are required only for Part 1.3. Parts 2–7 are supporting material for finding bugs, stale assumptions, and missing coverage.
