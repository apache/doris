# SNII Threadsafe Death Tests

## Problem

The monolithic BE unit-test process owns many threads by the time late SNII death tests run.
GoogleTest's default fast death-test mode uses `fork()`, which has twice aborted the parent test
process instead of reporting the expected child-process death. The latest PR #66052 failure occurs
in `SniiSpimiTermBufferTest.PairKeyModeRejectsGenericStringTokenEntryPoint`; the preceding failure
occurred in `ExactPhraseStreamMatcherTest.RejectsRepeatedCursorIndices` for the same reason.

## Scope

Change only SNII unit tests. Do not change `doris_be_test` startup, the BE unit-test runner, or any
death tests owned by other modules.

## Design

Audit every `EXPECT_DEATH` and `ASSERT_DEATH` under the SNII test paths. Immediately before each
SNII death assertion, set GoogleTest's per-test flag:

```cpp
GTEST_FLAG_SET(death_test_style, "threadsafe");
```

Keep the existing assertion, test suite, test name, and expected failure expression unchanged.
Do not add a shared helper, fixture, listener, macro, or separate test executable. GoogleTest saves
and restores its flags around each test, so the setting remains scoped to that SNII test and does
not alter other modules.

The audit currently covers nine death assertions in four files, including disabled SNII benchmark
tests so they are safe when explicitly enabled. The exact-phrase test already has the setting and
requires no semantic change.

## Verification

Use the existing TeamCity failure as the red phase. After the change:

1. Run the affected active SNII death-test suites through `run-be-ut.sh`.
2. Explicitly enable and run the disabled SNII benchmark death tests.
3. Run BE formatting checks for the modified C++ test files.
4. Confirm a source audit finds no SNII death assertion without a preceding threadsafe setting.

Success means all scoped death tests pass without aborting the parent process, while no non-SNII
test or global test-runner code changes.
