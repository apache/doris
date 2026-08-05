# Analyzer Selection and Execution Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the explicit analyzer name as the physical index-selection key while independently resolving the analyzer/provider used to execute a match predicate.

**Architecture:** Correct and reuse the existing BE-only `AnalyzerConfigParser` as the single resolver for the Thrift analyzer fields. Store its selection key separately from its execution provider/parser in `InvertedIndexAnalyzerCtx`, and make `VMatchPredicate` construct an analyzer only when execution requires one. No FE, Thrift, cache-key, fingerprint, or index-id changes are needed.

**Tech Stack:** C++20, Apache Doris BE, GoogleTest, CLucene analyzers, Doris `run-be-ut.sh` and build scripts.

## Global Constraints

- Follow strict RED-GREEN-REFACTOR: observe each focused test fail for the intended reason before editing production code.
- Analyzer-name precedence is absolute: a non-empty builtin name selects that parser; a non-empty custom name selects that provider with `PARSER_NONE`; only an empty name falls back to `parser_type_str`.
- The normalized explicit analyzer name is the only selection key. An empty analyzer name keeps an empty selection key even when a fallback parser exists.
- `none` resolves to an empty provider and `PARSER_NONE`, so it neither tokenizes nor constructs an analyzer.
- Do not change FE, Thrift, wire compatibility, index identifiers, cache keys, or fingerprints.
- Conf and worktree-local environment changes are never staged.

---

### Task 1: Make the Existing Resolver Authoritative

**Files:**
- Modify: `be/src/storage/index/inverted/inverted_index_parser.h`
- Modify: `be/src/storage/index/inverted/inverted_index_parser.cpp`
- Test: `be/test/storage/index/inverted_index_parser_test.cpp`

**Interfaces:**
- Consumes: raw `analyzer_name` and `parser_type_str` from `TMatchPredicate`.
- Produces: `AnalyzerConfigParser::parse(const std::string&, const std::string&) -> AnalyzerConfig`, where `AnalyzerConfig` contains `provider_name`, `parser_type`, and `analyzer_key`.

- [x] **Step 1: Add the failing analyzer-name precedence test**

```cpp
TEST_F(InvertedIndexParserTest, AnalyzerConfigParser_AnalyzerNameOverridesParserFallback) {
    auto config = AnalyzerConfigParser::parse("none", "english");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_NONE);
    EXPECT_EQ(config.analyzer_key, "none");

    config = AnalyzerConfigParser::parse("ik", "chinese");
    EXPECT_TRUE(config.custom_analyzer.empty());
    EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_IK);
    EXPECT_EQ(config.analyzer_key, "ik");
}
```

- [x] **Step 2: Run the focused test and verify RED**

Run: `./run-be-ut.sh --run --filter=InvertedIndexParserTest.AnalyzerConfigParser_AnalyzerNameOverridesParserFallback -j$(nproc)`

Expected: FAIL because the current resolver lets `parser_type_str` override the explicit analyzer name.

- [x] **Step 3: Implement explicit-name precedence with the existing field names**

Implement `parse` with exactly three branches: builtin non-empty name, custom non-empty name, and empty name. Normalize only `analyzer_key`; preserve a custom provider's original spelling.

- [x] **Step 4: Run the focused test and verify GREEN**

Run: `./run-be-ut.sh --run --filter=InvertedIndexParserTest.AnalyzerConfigParser_AnalyzerNameOverridesParserFallback -j$(nproc)`

Expected: PASS.

- [x] **Step 5: Refactor the provider field after GREEN**

```cpp
struct AnalyzerConfig {
    std::string provider_name;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_NONE;
    std::string analyzer_key;

    bool uses_provider() const { return !provider_name.empty(); }
    bool is_user_specified() const { return !analyzer_key.empty(); }
};
```

Rename every `AnalyzerConfig` assertion and implementation reference from `custom_analyzer`/`is_custom()` to `provider_name`/`uses_provider()`, then rerun the focused test to keep it GREEN.

- [x] **Step 6: Add the failing parser-only selection-key test**

Change `AnalyzerConfigParser_OnlyParserTypeStr` to expect:

```cpp
EXPECT_TRUE(config.analyzer_key.empty());
EXPECT_EQ(config.parser_type, InvertedIndexParserType::PARSER_STANDARD);
```

- [x] **Step 7: Run the focused test and verify RED**

Run: `./run-be-ut.sh --run --filter=InvertedIndexParserTest.AnalyzerConfigParser_OnlyParserTypeStr -j$(nproc)`

Expected: FAIL because the current resolver derives the physical selection key from the fallback parser.

- [x] **Step 8: Keep the key empty in the empty-name branch**

```cpp
config.analyzer_key = normalized_analyzer;
config.parser_type = parser_type == InvertedIndexParserType::PARSER_UNKNOWN
                             ? InvertedIndexParserType::PARSER_NONE
                             : parser_type;
```

- [x] **Step 9: Run all resolver tests and verify GREEN**

Run: `./run-be-ut.sh --run --filter=InvertedIndexParserTest.AnalyzerConfigParser* -j$(nproc)`

Expected: PASS after updating old assertions and terminology to the approved semantics.

- [ ] **Step 10: Commit the resolver slice**

Stage only the parser header, parser implementation, parser unit test, and this plan document. Commit with the project template and report exactly the tests run.

---

### Task 2: Separate Selection from Match Execution

**Files:**
- Modify: `be/src/storage/index/inverted/inverted_index_parser.h`
- Modify: `be/src/exprs/vmatch_predicate.cpp`
- Modify: `be/src/exprs/vmatch_predicate.h` only if a test seam is required after attempting public APIs first.
- Modify: `be/src/storage/index/inverted/inverted_index_iterator.cpp`
- Create: `be/test/exprs/vmatch_predicate_test.cpp`
- Test: `be/test/storage/index/inverted_index_parser_test.cpp`
- Test: `be/test/storage/index/snii/snii_index_reader_count_fallback_test.cpp`

**Interfaces:**
- Consumes: Task 1's `AnalyzerConfig {provider_name, parser_type, analyzer_key}`.
- Produces: `InvertedIndexAnalyzerCtx::analyzer_key`, `InvertedIndexAnalyzerCtx::provider_name`, and `requires_analysis() const`; `VMatchPredicate::get_analyzer_key()` returns only the selection key.

- [ ] **Step 1: Add a minimal match-predicate fixture and failing explicit-none test**

```cpp
TExprNode make_match_node(std::string analyzer_name, std::string parser_type) {
    TExprNode node;
    node.node_type = TExprNodeType::MATCH_PRED;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.num_children = 2;
    TMatchPredicate predicate;
    predicate.parser_type = std::move(parser_type);
    predicate.parser_mode = "";
    predicate.__set_analyzer_name(std::move(analyzer_name));
    node.__set_match_predicate(std::move(predicate));
    return node;
}

TEST(VMatchPredicateTest, ExplicitNoneKeepsSelectionKeyWithoutAnalyzer) {
    auto predicate = VMatchPredicate::create_shared(make_match_node("none", "english"));
    const auto* context = predicate->query_analyzer_ctx();
    EXPECT_EQ(predicate->get_analyzer_key(), "none");
    EXPECT_FALSE(context->requires_analysis());
    EXPECT_EQ(context->analyzer, nullptr);
    EXPECT_EQ(context->analyzer_provider, nullptr);
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `./run-be-ut.sh --run --filter=VMatchPredicateTest.ExplicitNoneKeepsSelectionKeyWithoutAnalyzer -j$(nproc)`

Expected: FAIL because the current constructor resolves the fallback `english` parser and always constructs an analyzer.

- [ ] **Step 3: Add distinct runtime fields and consume the resolver in `VMatchPredicate`**

```cpp
struct InvertedIndexAnalyzerCtx {
    std::string analyzer_key;
    std::string provider_name;
    InvertedIndexParserType parser_type = InvertedIndexParserType::PARSER_NONE;
    // existing analysis objects and configuration

    bool requires_analysis() const {
        return !provider_name.empty() || parser_type != InvertedIndexParserType::PARSER_NONE;
    }
};
```

Resolve before building `InvertedIndexAnalyzerConfig`. Populate its `analyzer_name` from `provider_name`, store all three resolved values in the context, and call `create_analyzer_provider` only when `requires_analysis()` is true. Change `get_analyzer_key()` to return `context->analyzer_key`.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `./run-be-ut.sh --run --filter=VMatchPredicateTest.ExplicitNoneKeepsSelectionKeyWithoutAnalyzer -j$(nproc)`

Expected: PASS.

- [ ] **Step 5: Add table-driven execution-mode coverage**

```cpp
struct Case {
    std::string name;
    std::string fallback;
    std::string key;
    std::string provider;
    InvertedIndexParserType parser;
    bool requires_analysis;
};
```

Cover builtin `english`, custom `customer_analyzer`, empty name with `standard`, and empty input. Assert selection key, provider, parser, `requires_analysis`, and analyzer/provider nullness.

- [ ] **Step 6: Run all match-predicate tests and verify GREEN**

Run: `./run-be-ut.sh --run --filter=VMatchPredicateTest.* -j$(nproc)`

Expected: PASS.

- [ ] **Step 7: Make physical selection consume only `analyzer_key`**

Replace iterator comparisons and diagnostics that currently read `analyzer_name` as a selection key with `analyzer_key`. Rename all execution-side context reads from `analyzer_name` to `provider_name`; replace `should_tokenize()` calls with `requires_analysis()`.

- [ ] **Step 8: Reproduce explicit `none` through the SNII reader before updating its context**

In `NoneParserWithoutAnalyzerUsesRawString`, set the current ambiguous context field to the explicit selection value:

```cpp
analyzer_ctx.analyzer_name = "none";
```

Run: `./run-be-ut.sh --run --filter=SniiIndexReaderCountFallback.NoneParserWithoutAnalyzerUsesRawString -j$(nproc)`

Expected: FAIL because the current `should_tokenize()` treats the non-empty selection key as an execution provider and sends the uppercase raw query through the index's English analyzer.

- [ ] **Step 9: Move the explicit value to `analyzer_key` and verify GREEN**

```cpp
analyzer_ctx.analyzer_key = "none";
```

Run: `./run-be-ut.sh --run --filter=SniiIndexReaderCountFallback.NoneParserWithoutAnalyzerUsesRawString -j$(nproc)`

Expected: PASS because selection metadata no longer changes execution semantics.

- [ ] **Step 10: Run parser, match, inverted-index, and SNII unit tests**

Run: `./run-be-ut.sh --run --filter='InvertedIndexParserTest.*:VMatchPredicateTest.*:*InvertedIndexIterator*:*Snii*' -j$(nproc)`

Expected: PASS.

- [ ] **Step 11: Commit the runtime separation slice**

Stage only the runtime/parser files and their tests. Commit with the project template and report exactly the tests run.

---

### Task 3: Add the Explicit-None SNII Regression

**Files:**
- Modify: `regression-test/suites/inverted_index_p0/storage_format/test_storage_format_snii_custom_analyzer.groovy`
- Generate: `regression-test/data/inverted_index_p0/storage_format/test_storage_format_snii_custom_analyzer.out`

**Interfaces:**
- Consumes: Task 2's separated selection/execution context and the existing `v_raw` SNII index.
- Produces: a SQL-level regression proving `USING ANALYZER none` keeps a multi-word query raw on a built SNII index.

- [ ] **Step 1: Add the deterministic query to the existing custom-analyzer suite**

```groovy
qt_snii_raw_explicit_none \
    "SELECT count(*) FROM ${sniiTable} WHERE v_raw MATCH_ALL 'FAILED ORDER' USING ANALYZER none"
```

- [ ] **Step 2: Generate the result with the preset regression runner**

Run: `./run-regression-test.sh -d inverted_index_p0/storage_format -s test_storage_format_snii_custom_analyzer -gen_out`

Expected: the generated `qt_snii_raw_explicit_none` result is `1`.

- [ ] **Step 3: Verify the generated regression normally**

Run: `./run-regression-test.sh -d inverted_index_p0/storage_format -s test_storage_format_snii_custom_analyzer`

Expected: PASS.

- [ ] **Step 4: Commit the regression slice**

Stage only the Groovy suite and its auto-generated output. Commit with the project template and report both regression commands.

---

### Task 4: Regression, Style, and Full Build Verification

**Files:**
- Modify only files already listed if verification exposes an issue in the approved scope.
- Do not stage: `conf/be.conf`, `conf/fe.conf`, or `regression-test/conf/regression-conf.groovy`.

**Interfaces:**
- Consumes: Task 2's separated selection/execution context.
- Produces: verified code ready for review and integration.

- [ ] **Step 1: Auto-format the changed BE files**

Run: `build-support/clang-format.sh` with the changed C++ headers/sources/tests.

- [ ] **Step 2: Check formatting without modifying files**

Run: `build-support/check-format.sh` with the changed C++ headers/sources/tests.

Expected: PASS.

- [ ] **Step 3: Build BE at full available parallelism**

Run: `./build.sh --be -j$(nproc)`

Expected: PASS and an updated `compile_commands.json`.

- [ ] **Step 4: Run clang-tidy for changed C++ files**

Run: `build-support/run-clang-tidy.sh` using the generated BE compilation database and the changed production C++ files.

Expected: no new warnings on changed lines.

- [ ] **Step 5: Run the focused BE test suites again**

Run: `./run-be-ut.sh --run --filter='InvertedIndexParserTest.*:VMatchPredicateTest.*:*InvertedIndexIterator*:*Snii*' -j$(nproc)`

Expected: PASS.

- [ ] **Step 6: Re-run the explicit-none generic slow-path regression**

Run: `./run-regression-test.sh -d inverted_index_p0 -s test_multi_tokenize_index_not_built`

Expected: the explicit `USING ANALYZER none` query selects the `none` index and no longer fails through customer-analyzer lookup.

- [ ] **Step 7: Re-run the V3/SNII/V2 customer-analyzer regression**

Run: `./run-regression-test.sh -d inverted_index_p0/storage_format -s test_storage_format_snii_custom_analyzer`

Expected: all storage formats retain custom-analyzer behavior, and SNII retains raw explicit-none behavior.

- [ ] **Step 8: Review the final diff and commit any verification-only corrections**

Confirm `git diff --check`, inspect all staged paths, keep worktree configuration out of commits, and use the required Doris commit-message template. Do not claim a test that was not run successfully.
