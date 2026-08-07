# SNII Threadsafe Death Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every current SNII GoogleTest death assertion use threadsafe execution without changing the global BE unit-test runner or tests owned by other modules.

**Architecture:** Keep GoogleTest's default behavior unchanged globally. Set `death_test_style` to `threadsafe` inside each SNII test that owns a death assertion; GoogleTest's per-test flag saver restores the prior value after that test. Audit active and disabled SNII tests together so the current failure and all known equivalents are fixed in one change.

**Tech Stack:** C++20, GoogleTest 1.12 death tests, Doris `run-be-ut.sh`, clang-format 16, clang-tidy.

## Global Constraints

- Modify only SNII unit-test source files.
- Do not change `be/test/testutil/run_all_tests.cpp`, `run-be-ut.sh`, shared GoogleTest configuration, or non-SNII tests.
- Do not add a helper, fixture, listener, macro, or separate test executable.
- Preserve every existing death expression, matcher, suite name, and test name.
- Put `GTEST_FLAG_SET(death_test_style, "threadsafe");` immediately before every SNII death assertion.
- Build and test with 192-way parallelism.

---

### Task 1: Stabilize every SNII death-test owner

**Files:**
- Modify: `be/test/storage/index/snii_spimi_intern_test.cpp:439`
- Modify: `be/test/storage/index/snii/format/norms_pod_test.cpp:197`
- Modify: `be/test/storage/index/snii/bench/snii_vs_v3_benchmark_test.cpp:237`
- Verify unchanged: `be/test/storage/index/snii/query/exact_phrase_stream_matcher_test.cpp:357`

**Interfaces:**
- Consumes: GoogleTest's `GTEST_FLAG_SET` and existing `EXPECT_DEATH` assertions.
- Produces: Nine SNII death assertions that select threadsafe execution locally; the exact-phrase assertion already satisfies the contract.

- [ ] **Step 1: Record the failing baseline**

Use TeamCity BE UT build `1017755` as the observed RED result. It must show:

```text
[ RUN      ] SniiSpimiTermBufferTest.PairKeyModeRejectsGenericStringTokenEntryPoint
Death tests use fork(), ... detected 156 threads
Aborted (core dumped)
```

Confirm the source audit finds nine SNII death assertions but only the exact-phrase owner already sets threadsafe mode:

```bash
rg -n -C 5 'EXPECT_DEATH|ASSERT_DEATH|GTEST_FLAG_SET\(death_test_style' \
  be/test/storage/index/snii be/test/storage/index/snii_*.cpp --glob '*.cpp'
```

Expected: missing settings in `PairKeyModeRejectsGenericStringTokenEntryPoint`,
`OutOfRangeDocidAsserts`, `RejectsInvalidPercentiles`, and
`RejectsInvalidQueryIterations`.

- [ ] **Step 2: Add the minimal scoped settings**

In `PairKeyModeRejectsGenericStringTokenEntryPoint`, insert immediately before `EXPECT_DEATH`:

```cpp
    GTEST_FLAG_SET(death_test_style, "threadsafe");
```

In `OutOfRangeDocidAsserts`, insert the same line after the reader is opened and before
`EXPECT_DEATH`.

In the disabled benchmark tests, insert the same line immediately before every death assertion:

```cpp
TEST(DISABLED_SniiBenchmarkPercentile, RejectsInvalidPercentiles) {
    const std::vector<double> samples {1.0};
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(nearest_rank_percentile(samples, 0)); }, "");
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(nearest_rank_percentile(samples, 101)); }, "");
}

TEST(DISABLED_SniiBenchmarkConfig, RejectsInvalidQueryIterations) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("0")); }, "");
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("-1")); }, "");
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("30junk")); }, "");
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations("abc")); }, "");

    const std::string overflow =
            std::to_string(static_cast<int64_t>(std::numeric_limits<int>::max()) + 1);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH({ static_cast<void>(parse_query_iterations(overflow)); }, "");
}
```

Do not alter the exact-phrase test; it already contains the required setting.

- [ ] **Step 3: Run active SNII death tests (GREEN)**

```bash
env GLIBC_COMPATIBILITY=OFF ./run-be-ut.sh --run \
  --filter='SniiSpimiTermBufferTest.PairKeyModeRejectsGenericStringTokenEntryPoint:SniiNormsPodDeathTest.OutOfRangeDocidAsserts:ExactPhraseStreamMatcherTest.RejectsRepeatedCursorIndices' \
  -j 192
```

Expected: all three tests pass; no parent-process abort and no fast-style fork warning.

- [ ] **Step 4: Run disabled SNII benchmark death tests (GREEN)**

```bash
env GLIBC_COMPATIBILITY=OFF GTEST_ALSO_RUN_DISABLED_TESTS=1 ./run-be-ut.sh --run \
  --filter='DISABLED_SniiBenchmarkPercentile.RejectsInvalidPercentiles:DISABLED_SniiBenchmarkConfig.RejectsInvalidQueryIterations' \
  -j 192
```

Expected: both disabled tests run and pass, covering all seven benchmark death assertions.

- [ ] **Step 5: Audit scope and formatting**

Temporarily protect the pre-existing tracked and untracked C++ worktree changes before invoking the
repository-wide formatter. Run:

```bash
build-support/clang-format.sh
build-support/check-format.sh
git diff --check
```

Restore the protected user changes, then confirm only these three SNII test files are part of the
implementation diff and no SNII death assertion lacks an immediately preceding threadsafe setting. Run clang-tidy against
the UT compilation database:

```bash
build-support/run-clang-tidy.sh --build-dir be/ut_build_ASAN --files \
  be/test/storage/index/snii_spimi_intern_test.cpp \
  be/test/storage/index/snii/format/norms_pod_test.cpp \
  be/test/storage/index/snii/bench/snii_vs_v3_benchmark_test.cpp
```

Expected: formatting, whitespace, and static-analysis checks pass with no warnings on the modified
files. The source audit still reports nine death assertions, each with an immediately preceding
threadsafe setting (including the already-fixed exact-phrase test).

- [ ] **Step 6: Commit the implementation**

Stage only the three modified SNII test files. Use a Doris PR-template commit message with:

```text
[fix](be) Run all SNII death tests in threadsafe mode
```

The problem summary must reference PR #66052, the two BE UT coredump sites, the multi-threaded
fast-death-test root cause, and the targeted test results actually run. Do not stage configuration,
unrelated tests, build output, or other user-owned files.
