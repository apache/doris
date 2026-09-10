// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/like.h"
#include "exprs/function_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"

namespace doris::segment_v2::gram {
namespace {

enum class ScalarPath {
    ANY,
    HYPERSCAN,
    RE2,
    DYNAMIC_RE2,
    BOOST,
    ALLPASS,
    EQUALS,
    STARTS_WITH,
    ENDS_WITH,
    SUBSTRING
};

// BE UT permits private/protected access. Inspect the state selected by open(), without
// replacing the production engine or using a second regex implementation as the oracle.
void check_scalar_path(const LikeState& state, ScalarPath path) {
    if (path == ScalarPath::ANY) {
        return;
    }
    const auto& search = state.search_state;
    EXPECT_EQ(search.hs_database != nullptr, path == ScalarPath::HYPERSCAN);
    EXPECT_EQ(search.hs_scratch != nullptr, path == ScalarPath::HYPERSCAN);
    EXPECT_EQ(search.regex != nullptr, path == ScalarPath::RE2);
    EXPECT_EQ(search.boost_regex != nullptr, path == ScalarPath::BOOST);

    using FunctionPointer = Status (*)(const LikeSearchState*, const ColumnString&,
                                       const StringRef&, ColumnUInt8::Container&);
    const auto* selected = state.function.target<FunctionPointer>();
    ASSERT_NE(selected, nullptr);
    switch (path) {
    case ScalarPath::HYPERSCAN:
    case ScalarPath::RE2:
    case ScalarPath::BOOST:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_regex_fn);
        break;
    case ScalarPath::DYNAMIC_RE2: {
        using ScalarFunctionPointer = Status (*)(const LikeSearchState*, const StringRef&,
                                                 const StringRef&, unsigned char*);
        const auto* scalar = state.scalar_function.target<ScalarFunctionPointer>();
        ASSERT_NE(scalar, nullptr);
        EXPECT_EQ(*selected,
                  state.is_like_pattern ? &FunctionLike::like_fn : &FunctionLikeBase::regexp_fn);
        EXPECT_EQ(*scalar, state.is_like_pattern ? &FunctionLike::like_fn_scalar
                                                 : &FunctionLikeBase::regexp_fn_scalar);
        break;
    }
    case ScalarPath::ALLPASS:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_allpass_fn);
        break;
    case ScalarPath::EQUALS:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_equals_fn);
        break;
    case ScalarPath::STARTS_WITH:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_starts_with_fn);
        break;
    case ScalarPath::ENDS_WITH:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_ends_with_fn);
        break;
    case ScalarPath::SUBSTRING:
        EXPECT_EQ(*selected, &FunctionLikeBase::constant_substring_fn);
        break;
    case ScalarPath::ANY:
        break;
    }
}

// The expression chooses the real scalar engine, including Hyperscan and its RE2 fallback.
// Calling RE2 directly would miss differences between the compiler and the production path.
template <typename Function = FunctionRegexpLike>
Status scalar_matches(const std::string& pattern, const std::vector<std::string>& rows,
                      std::vector<bool>* matches, bool enable_hyperscan_fallback = true,
                      bool enable_extended_regex = false, ScalarPath path = ScalarPath::ANY) {
    TQueryOptions query_options;
    query_options.__set_enable_hyperscan_fallback(enable_hyperscan_fallback);
    query_options.__set_enable_extended_regex(enable_extended_regex);
    RuntimeState runtime_state(query_options, TQueryGlobals {});
    auto string_type = std::make_shared<DataTypeString>();
    auto context = FunctionContext::create_context(
            &runtime_state, std::make_shared<DataTypeUInt8>(), {string_type, string_type});

    auto values = ColumnString::create();
    for (const auto& row : rows) {
        values->insert_data(row.data(), row.size());
    }
    auto patterns = ColumnString::create();
    const bool dynamic_pattern = path == ScalarPath::DYNAMIC_RE2;
    const size_t pattern_rows = dynamic_pattern ? rows.size() : 1;
    for (size_t i = 0; i < pattern_rows; ++i) {
        patterns->insert_data(pattern.data(), pattern.size());
    }
    ColumnPtr pattern_column;
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_columns(2);
    if (dynamic_pattern) {
        // Prove vector_non_const will reach the scalar regex function, where RE2 is
        // constructed locally. No scalar or vector string fast path accepts these patterns.
        constexpr bool is_like = std::is_same_v<Function, FunctionLike>;
        EXPECT_EQ(FunctionLikeBase::pattern_type_recognition<is_like>(*patterns), nullptr);
        if constexpr (is_like) {
            std::string literal;
            EXPECT_EQ(extract_like_fast_path(pattern.data(), pattern.size(), literal),
                      LikeFastPath::REGEX);
        }
        pattern_column = std::move(patterns);
    } else {
        pattern_column = ColumnConst::create(std::move(patterns), rows.size());
        constant_columns[1] = std::make_shared<ColumnPtrWrapper>(pattern_column);
    }
    context->set_constant_cols(constant_columns);

    Function function;
    if (dynamic_pattern) {
        // Dynamic-pattern execution is a scalar compatibility boundary, not an index
        // acceleration claim: the eligibility hook rejects a pattern column.
        auto value = VSlotRef::create_shared(0, 0, 0, string_type, "value");
        auto pattern_slot = VSlotRef::create_shared(1, 1, 1, string_type, "pattern");
        EXPECT_FALSE(function.can_evaluate_inverted_index({value, pattern_slot}));
    }
    RETURN_IF_ERROR(function.open(context.get(), FunctionContext::THREAD_LOCAL));
    const auto* state =
            static_cast<LikeState*>(context->get_function_state(FunctionContext::THREAD_LOCAL));
    check_scalar_path(*state, path);
    Block block;
    block.insert({std::move(values), string_type, "value"});
    block.insert({std::move(pattern_column), string_type, "pattern"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});
    RETURN_IF_ERROR(function.execute_impl(context.get(), block, {0, 1}, 2, rows.size()));
    matches->clear();
    for (size_t i = 0; i < rows.size(); ++i) {
        matches->push_back(block.get_by_position(2).column->get_bool(i));
        unsigned char scalar_result = 0;
        RETURN_IF_ERROR(state->scalar_function(&state->search_state, StringRef(rows[i]),
                                               StringRef(pattern), &scalar_result));
        EXPECT_EQ(scalar_result != 0, matches->back()) << "row " << i;
    }
    return function.close(context.get(), FunctionContext::THREAD_LOCAL);
}

// Interpret the public boolean query without reusing its compiler or simplifier.
bool is_candidate(const GramQuery& query, const std::set<std::string>& grams) {
    switch (query.op) {
    case GramQuery::Op::ALL:
        return true;
    case GramQuery::Op::NONE:
        return false;
    case GramQuery::Op::AND:
        return std::ranges::all_of(query.grams,
                                   [&](const auto& gram) { return grams.contains(gram); }) &&
               std::ranges::all_of(query.subs,
                                   [&](const auto& sub) { return is_candidate(sub, grams); });
    case GramQuery::Op::OR:
        return std::ranges::any_of(query.grams,
                                   [&](const auto& gram) { return grams.contains(gram); }) ||
               std::ranges::any_of(query.subs,
                                   [&](const auto& sub) { return is_candidate(sub, grams); });
    }
    ADD_FAILURE() << "Unknown gram query operator";
    return false;
}

template <typename Function>
void check_scheme_recall(const GramScheme& scheme, const std::string& pattern,
                         const std::vector<std::string>& rows, const std::vector<bool>& truth,
                         bool require_pruning, bool require_all = false) {
    SCOPED_TRACE(scheme.cache_key());
    RegexGramCompiler compiler(scheme);
    GramExtractor extractor(scheme);
    GramQuery query;
    if constexpr (std::is_same_v<Function, FunctionLike>) {
        ASSERT_TRUE(compiler.compile_like(pattern, &query).ok());
    } else {
        ASSERT_TRUE(compiler.compile_regexp(pattern, &query).ok());
    }
    if (require_all) {
        EXPECT_EQ(query.op, GramQuery::Op::ALL) << query.to_debug_string();
    }
    bool pruned = false;
    for (size_t i = 0; i < rows.size(); ++i) {
        SCOPED_TRACE(rows[i]);
        std::vector<std::string_view> extracted;
        extractor.extract(rows[i], &extracted);
        std::set<std::string> grams(extracted.begin(), extracted.end());
        const bool candidate = is_candidate(query, grams);
        EXPECT_TRUE(!truth[i] || candidate) << query.to_debug_string();
        pruned |= !truth[i] && !candidate;
    }
    if (require_pruning) {
        EXPECT_TRUE(pruned) << "A determined literal must still filter unrelated rows";
    }
}

template <typename Function = FunctionRegexpLike>
void check_recall(const std::string& pattern, const std::vector<std::string>& rows,
                  const std::vector<bool>& expected, bool require_pruning,
                  bool enable_hyperscan_fallback = true, bool enable_extended_regex = false,
                  ScalarPath path = ScalarPath::ANY, bool require_all = false) {
    SCOPED_TRACE(pattern);
    std::vector<bool> truth;
    auto status = scalar_matches<Function>(pattern, rows, &truth, enable_hyperscan_fallback,
                                           enable_extended_regex, path);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(truth, expected);
    for (auto mode : {GramMode::DENSE, GramMode::SPARSE}) {
        for (bool lower_case : {false, true}) {
            GramScheme scheme;
            scheme.mode = mode;
            scheme.lower_case = lower_case;
            check_scheme_recall<Function>(scheme, pattern, rows, truth, require_pruning,
                                          require_all);
        }
    }
}

struct RecallCase {
    std::string pattern;
    std::vector<std::string> rows;
    std::vector<bool> expected;
    ScalarPath path;
    bool require_pruning = true;
    bool require_all = false;
};

template <typename Function = FunctionRegexpLike>
void check_cases(const std::vector<RecallCase>& cases, bool fallback, bool extended) {
    SCOPED_TRACE(fallback);
    SCOPED_TRACE(extended);
    for (const auto& test_case : cases) {
        check_recall<Function>(test_case.pattern, test_case.rows, test_case.expected,
                               test_case.require_pruning, fallback, extended, test_case.path,
                               test_case.require_all);
    }
}

} // namespace

TEST(RegexGramRecallTest, HyperscanEngineOptionsMatrix) {
    const std::vector<RecallCase> cases = {
            {"^prefix[0-9]+timeout$",
             {"prefix123timeout", "prefix9timeout", "prefixxtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {"prefix.timeout",
             {"prefix\ntimeout", "prefix😀timeout", "prefixtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {"(?-s)prefix.timeout",
             {"prefix\ntimeout", "prefix😀timeout", "prefixtimeout", "unrelated"},
             {false, true, false, false},
             ScalarPath::HYPERSCAN},
            {"(?m)^timeout$",
             {"before\ntimeout\nafter", "timeout", "xtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {"(?i:ask)(?-i:timeout)",
             {"ASKtimeout", "aſktimeout", "asKtimeout", "ASKTIMEOUT", "unrelated"},
             {true, true, true, false, false},
             ScalarPath::HYPERSCAN},
            {"(?i)(?:(?-i)abc)DEF(?-i:timeout)",
             {"abcdeftimeout", "abcDEFtimeout", "ABCdeftimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {R"(ab\vcdtimeout)",
             {"ab\vcdtimeout", "ab\ncdtimeout", "ab\rcdtimeout", "ab\fcdtimeout", "unrelated"},
             {true, true, true, true, false},
             ScalarPath::HYPERSCAN},
            {R"(\Qab😀\E{2}timeout)",
             {"ab😀😀timeout", "ab😀timeout", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            {R"(foo\x00timeout)",
             {std::string("foo") + '\0' + "timeout", "foo", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            // Hyperscan permits zero hex digits after \x and emits a NUL byte.
            // The gram parser requires two digits and conservatively returns ALL.
            {R"(timeout\xZZ)",
             {std::string("timeout") + '\0' + "ZZ", "timeoutxZZ", R"(timeout\xZZ)", "unrelated"},
             {true, false, false, false},
             ScalarPath::HYPERSCAN,
             false,
             true},
            {std::string("foo[0-9]") + '\0' + "timeout",
             {"foo1", "foo1timeout", "foo", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN,
             false,
             true},
            {R"(timeout\Z)",
             {"timeout", "timeoutx", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            {R"(ab\hcdtimeout)",
             {"ab cdtimeout", "ab\tcdtimeout", "ab\ncdtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN,
             false,
             true}};
    for (bool fallback : {false, true}) {
        for (bool extended : {false, true}) {
            check_cases(cases, fallback, extended);
        }
    }
}

TEST(RegexGramRecallTest, Re2FallbackEngineMatrix) {
    // Bounded repetition above 50 is rejected before hs_compile. The optional suffix
    // deliberately selects RE2 without changing the corner case's expected matches.
    const std::vector<RecallCase> cases = {
            {"^prefix.{0,51}timeout$",
             {"prefixtimeout", "prefix" + std::string(51, 'x') + "timeout",
              "prefix" + std::string(52, 'x') + "timeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::RE2},
            {"(?i:é)timeout.{0,51}$",
             {"étimeout", "Étimeout", "ÉTIMEOUT", "unrelated"},
             {true, true, false, false},
             ScalarPath::RE2},
            {"(?i:ask)(?-i:timeout).{0,51}$",
             {"ASKtimeout", "aſktimeout", "asKtimeout", "ASKTIMEOUT", "unrelated"},
             {true, true, true, false, false},
             ScalarPath::RE2},
            {"(?i)(?:(?-i)abc)DEF(?-i:timeout).{0,51}$",
             {"abcdeftimeout", "abcDEFtimeout", "ABCdeftimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::RE2},
            {"(?-s)prefix.timeout.{0,51}$",
             {"prefix\ntimeout", "prefix😀timeout", "prefixtimeout", "unrelated"},
             {false, true, false, false},
             ScalarPath::RE2},
            {"(?m)^timeout$.{0,51}",
             {"before\ntimeout\nafter", "timeout", "xtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::RE2},
            {"(?U)prefix.*timeout.{0,51}$",
             {"prefixtimeout", "prefix\ntimeout", "prefixTIMEOUT", "unrelated"},
             {true, true, false, false},
             ScalarPath::RE2},
            // RE2's \v matches only VT; Hyperscan's \v also matches LF, CR and FF.
            {R"(ab\vcdtimeout.{0,51}$)",
             {"ab\vcdtimeout", "ab\ncdtimeout", "ab\rcdtimeout", "ab\fcdtimeout", "unrelated"},
             {true, false, false, false, false},
             ScalarPath::RE2},
            {R"(ab[\141]cdtimeout.{0,51}$)",
             {"abacdtimeout", "ab1cdtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::RE2},
            {R"(\Qab😀\E{2}timeout.{0,51}$)",
             {"ab😀😀timeout", "ab😀timeout", "unrelated"},
             {true, false, false},
             ScalarPath::RE2},
            // This fallback is caused by rejected syntax rather than repetition cost.
            {R"(ab+\Q\E?timeout)",
             {"atimeout", "abtimeout", "abbbtimeout", "unrelated"},
             {true, true, true, false},
             ScalarPath::RE2},
            {R"(foo\x00timeout.{0,51}$)",
             {std::string("foo") + '\0' + "timeout", "foo", "unrelated"},
             {true, false, false},
             ScalarPath::RE2},
            {std::string("foo[0-9].{0,51}") + '\0' + "timeout",
             {std::string("foo1") + '\0' + "timeout", "foo1", "foo1timeout", "unrelated"},
             {true, false, false, false},
             ScalarPath::RE2,
             false,
             true}};
    for (bool extended : {false, true}) {
        check_cases(cases, true, extended);
        for (const auto& test_case : cases) {
            SCOPED_TRACE(test_case.pattern);
            std::vector<bool> matches;
            auto status =
                    scalar_matches(test_case.pattern, test_case.rows, &matches, false, extended);
            EXPECT_FALSE(status.ok()) << "Disabling fallback must surface Hyperscan rejection";
        }
    }
}

TEST(RegexGramRecallTest, BoostExtendedEngineAndRejectionMatrix) {
    // Each expression is unsupported by both Hyperscan and RE2. Boost is available only
    // when both fallback and extended regex are enabled. The gram parser must return ALL.
    const std::vector<RecallCase> cases = {
            {"timeout(?=END)",
             {"timeoutEND", "timeoutend", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {"timeout(?!END)",
             {"timeoutend", "timeoutEND", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {"(?<=prefix)timeout",
             {"prefixtimeout", "xtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {"(?<!prefix)timeout",
             {"xtimeout", "prefixtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {R"((ab)\1timeout)",
             {"ababtimeout", "abtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {"(?i)timeout(?=END)",
             {"timeoutEND", "TIMEOUTend", "unrelated"},
             {true, true, false},
             ScalarPath::BOOST,
             false,
             true},
            {"(?x)prefix timeout(?=END)",
             {"prefixtimeoutEND", "prefix timeoutEND", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {R"((?=ab)\Qab\E{2}timeout)",
             {"abbtimeout", "ababtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            // Boost octal escapes start with 0 followed by up to three octal digits.
            {R"((?=ab)ab\0141cdtimeout)",
             {"abacdtimeout", "ab141cdtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {"(?=é)étimeout",
             {"étimeout", "Étimeout", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true},
            {std::string("(?=foo)foo") + '\0' + "timeout",
             {std::string("foo") + '\0' + "timeout", "foo", "unrelated"},
             {true, false, false},
             ScalarPath::BOOST,
             false,
             true}};
    check_cases(cases, true, true);
    for (bool fallback : {false, true}) {
        for (bool extended : {false, true}) {
            if (fallback && extended) {
                continue;
            }
            SCOPED_TRACE(fallback);
            SCOPED_TRACE(extended);
            for (const auto& test_case : cases) {
                SCOPED_TRACE(test_case.pattern);
                std::vector<bool> matches;
                auto status = scalar_matches(test_case.pattern, test_case.rows, &matches, fallback,
                                             extended);
                EXPECT_FALSE(status.ok());
                if (fallback) {
                    EXPECT_NE(status.to_string().find("enable_extended_regex=true"),
                              std::string::npos);
                }
            }
        }
    }
}

TEST(RegexGramRecallTest, RegexpAndLikeFastPathMatrix) {
    const std::vector<std::string> rows = {"timeout",  "Timeout",   "xtimeout",
                                           "timeoutx", "xtimeoutx", "unrelated"};
    const std::string nul_literal = std::string("foo") + '\0' + "timeout";
    const std::vector<RecallCase> regexp_cases = {
            {".*",
             {"", "\n", nul_literal, "unrelated"},
             {true, true, true, true},
             ScalarPath::ALLPASS,
             false,
             true},
            {"^timeout$", rows, {true, false, false, false, false, false}, ScalarPath::EQUALS},
            {"^timeout.*", rows, {true, false, false, true, false, false}, ScalarPath::STARTS_WITH},
            {".*timeout$", rows, {true, false, true, false, false, false}, ScalarPath::ENDS_WITH},
            {".*timeout.*", rows, {true, false, true, true, true, false}, ScalarPath::SUBSTRING},
            {nul_literal,
             {nul_literal, "foo", "unrelated"},
             {true, false, false},
             ScalarPath::SUBSTRING,
             false,
             true}};
    const std::vector<RecallCase> like_cases = {
            {"%%",
             {"", "\n", nul_literal, "unrelated"},
             {true, true, true, true},
             ScalarPath::ALLPASS,
             false,
             true},
            {"timeout", rows, {true, false, false, false, false, false}, ScalarPath::EQUALS},
            {"timeout%", rows, {true, false, false, true, false, false}, ScalarPath::STARTS_WITH},
            {"%timeout", rows, {true, false, true, false, false, false}, ScalarPath::ENDS_WITH},
            {"%timeout%", rows, {true, false, true, true, true, false}, ScalarPath::SUBSTRING},
            {R"(prefix\_timeout)",
             {"prefix_timeout", "prefixXtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::EQUALS},
            {R"(%prefix\_timeout%)",
             {"xprefix_timeoutx", "prefixXtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::SUBSTRING},
            {R"(prefix\\timeout)",
             {R"(prefix\timeout)", "prefixtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::EQUALS},
            {nul_literal,
             {nul_literal, "foo", "unrelated"},
             {true, false, false},
             ScalarPath::EQUALS,
             false,
             true}};
    for (bool fallback : {false, true}) {
        for (bool extended : {false, true}) {
            check_cases(regexp_cases, fallback, extended);
            check_cases<FunctionLike>(like_cases, fallback, extended);
        }
    }
}

TEST(RegexGramRecallTest, LikeHyperscanEngineMatrix) {
    const std::vector<RecallCase> cases = {
            {"prefix_timeout",
             {"prefix😀timeout", "prefix\ntimeout", "prefixtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {"%prefix%timeout%",
             {"prefix\ntimeout", "prefix😀timeout", "timeoutprefix", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN},
            {R"(prefix\%timeout_)",
             {"prefix%timeoutX", "prefixxtimeoutX", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            {R"(%prefix\qtimeout%)",
             {R"(xprefix\qtimeoutx)", "prefixqtimeout", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            {"étimeout_",
             {"étimeout😀", "Étimeout😀", "unrelated"},
             {true, false, false},
             ScalarPath::HYPERSCAN},
            {std::string("prefix_") + '\0' + "%timeout%",
             {"prefix1", "prefix12", "xprefix1", "unrelated"},
             {true, true, false, false},
             ScalarPath::HYPERSCAN,
             false,
             true}};
    for (bool fallback : {false, true}) {
        for (bool extended : {false, true}) {
            check_cases<FunctionLike>(cases, fallback, extended);
        }
    }
}

TEST(RegexGramRecallTest, InvalidPatternsRejectAcrossEngineOptions) {
    // Unlike Hyperscan/RE2, Boost interprets \141 as backreference 1 followed by
    // literal 41. This expression has no capture, so even extended regex rejects it.
    for (const auto* pattern : {"[", "(abc", "(?i", "(?q)timeout", R"((?=ab)ab\141cdtimeout)"}) {
        SCOPED_TRACE(pattern);
        for (bool fallback : {false, true}) {
            for (bool extended : {false, true}) {
                SCOPED_TRACE(fallback);
                SCOPED_TRACE(extended);
                std::vector<bool> matches;
                auto status = scalar_matches(pattern, {"timeout", "unrelated"}, &matches, fallback,
                                             extended);
                EXPECT_FALSE(status.ok()) << "Invalid syntax must not become a successful match";
            }
        }
        // Scalar rejection and conservative index compilation are separate contracts.
        for (auto mode : {GramMode::DENSE, GramMode::SPARSE}) {
            for (bool lower_case : {false, true}) {
                GramScheme scheme;
                scheme.mode = mode;
                scheme.lower_case = lower_case;
                check_scheme_recall<FunctionRegexpLike>(scheme, pattern, {"timeout", "unrelated"},
                                                        {false, false}, false, true);
            }
        }
    }
}

TEST(RegexGramRecallTest, DynamicPatternRe2ExecutionAndIndexEligibility) {
    // A ColumnString pattern reaches vector_non_const -> scalar_function -> RE2 even
    // with Hyperscan fallback disabled. Extended regex does not enable Boost on this path.
    // Each execution also checks that can_evaluate_inverted_index rejects the pattern slot.
    const std::vector<RecallCase> regexp_cases = {
            {R"(ab\vcdtimeout)",
             {"ab\vcdtimeout", "ab\ncdtimeout", "ab\rcdtimeout", "unrelated"},
             {true, false, false, false},
             ScalarPath::DYNAMIC_RE2},
            {"(?i:ask)(?-i:timeout)",
             {"ASKtimeout", "aſktimeout", "asKtimeout", "ASKTIMEOUT", "unrelated"},
             {true, true, true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {R"(\Qab😀\E{2}timeout)",
             {"ab😀😀timeout", "ab😀timeout", "unrelated"},
             {true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {std::string("foo[0-9]") + '\0' + "timeout",
             {std::string("foo1") + '\0' + "timeout", "foo1", "foo1timeout", "unrelated"},
             {true, false, false, false},
             ScalarPath::DYNAMIC_RE2,
             false,
             true}};
    const std::vector<RecallCase> like_cases = {
            {"prefix_timeout",
             {"prefix😀timeout", "prefix\ntimeout", "prefixtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {"%prefix%\ntimeout%",
             {"prefix\ntimeout", "prefixx\ntimeout", "prefixxtimeout", "unrelated"},
             {true, true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {R"(prefix\%timeout_)",
             {"prefix%timeoutX", "prefixxtimeoutX", "unrelated"},
             {true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {"étimeout_",
             {"étimeout😀", "Étimeout😀", "unrelated"},
             {true, false, false},
             ScalarPath::DYNAMIC_RE2},
            {std::string("prefix_") + '\0' + "%timeout%",
             {std::string("prefix1") + '\0' + "timeout", "prefix1", "prefix1timeout", "unrelated"},
             {true, false, false, false},
             ScalarPath::DYNAMIC_RE2,
             false,
             true}};
    for (bool fallback : {false, true}) {
        for (bool extended : {false, true}) {
            check_cases(regexp_cases, fallback, extended);
            check_cases<FunctionLike>(like_cases, fallback, extended);
            for (const auto* pattern :
                 {R"(timeout\Z)", R"(ab\hcdtimeout)", R"(timeout\xZZ)", "timeout(?=END)"}) {
                SCOPED_TRACE(pattern);
                std::vector<bool> matches;
                auto status = scalar_matches(pattern, {"timeoutEND", "unrelated"}, &matches,
                                             fallback, extended, ScalarPath::DYNAMIC_RE2);
                EXPECT_FALSE(status.ok()) << "Dynamic patterns do not use Hyperscan or Boost";
            }
        }
    }
}

TEST(RegexGramRecallTest, UnicodeCaseInsensitiveRecall) {
    check_recall("(?i)é", {"é", "É", "unrelated"}, {true, true, false}, false);
    check_recall("(?i)[é]", {"é", "É", "unrelated"}, {true, true, false}, false);
    check_recall("(?i)ask", {"ask", "ASK", "aſk", "asK", "unrelated"},
                 {true, true, true, true, false}, false);
    check_recall("(?i)a[s]k", {"aſk", "asK", "unrelated"}, {true, true, false}, false);
    check_recall("(?i)as[k]", {"aſk", "asK", "unrelated"}, {true, true, false}, false);

    // Unknown folded characters may weaken their own constraints, but must not disable the
    // determined ASCII suffix. The short prefix also checks that grams do not cross that leaf.
    check_recall("ab(?i)é(?-i:timeout)", {"abÉtimeout", "abétimeout", "unrelated"},
                 {true, true, false}, true);
    check_recall("ab(?i)[é](?-i:timeout)", {"abÉtimeout", "abétimeout", "unrelated"},
                 {true, true, false}, true);
    check_recall("timeout", {"timeout", "unrelated"}, {true, false}, true);
}

TEST(RegexGramRecallTest, EscapesPreserveScalarRecall) {
    check_recall(R"(ab\acdtimeout)", {"ab\acdtimeout", "abacdtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab\fcdtimeout)", {"ab\fcdtimeout", "abfcdtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab\vcdtimeout)",
                 {"ab\vcdtimeout", "ab\ncdtimeout", "ab\rcdtimeout", "ab\fcdtimeout",
                  "ab cdtimeout", "ab\tcdtimeout", "abvcdtimeout", "unrelated"},
                 {true, true, true, true, false, false, false, false}, true);
    check_recall(R"(ab[\a]cdtimeout)", {"ab\acdtimeout", "abacdtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab[\f]cdtimeout)", {"ab\fcdtimeout", "abfcdtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab[\v]cdtimeout)",
                 {"ab\vcdtimeout", "ab\ncdtimeout", "ab\rcdtimeout", "ab\fcdtimeout",
                  "ab cdtimeout", "ab\tcdtimeout", "abvcdtimeout", "unrelated"},
                 {true, true, true, true, false, false, false, false}, true);

    check_recall(R"(ab\141cdtimeout)", {"abacdtimeout", "ab141cdtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab[\141]cdtimeout)", {"abacdtimeout", "ab1cdtimeout", "unrelated"},
                 {true, false, false}, true);

    // Hyperscan accepts \Z even though RE2 rejects it. It must not become a literal Z.
    check_recall(R"(timeout\Z)", {"timeout", "timeoutx", "unrelated"}, {true, false, false}, false);

    // An escape outside the gram parser's subset may disable the entire query. Hyperscan's
    // horizontal whitespace class must not be mistaken for the literal h.
    check_recall(R"(ab\hcdtimeout)",
                 {"ab cdtimeout", "ab\tcdtimeout", "ab\ncdtimeout", "abhcdtimeout", "unrelated"},
                 {true, true, false, false, false}, false);

    // Known zero-width escapes and escaped punctuation retain their literal constraints.
    check_recall(R"(\Atimeout\z)", {"timeout", "xtimeout", "unrelated"}, {true, false, false},
                 true);
    check_recall(R"(\btimeout\b)", {"timeout", "xtimeoutx", "unrelated"}, {true, false, false},
                 true);
    check_recall(R"(\Btimeout\B)", {"xtimeoutx", "timeout", "unrelated"}, {true, false, false},
                 true);
    check_recall(R"(ab\.cdtimeout)", {"ab.cdtimeout", "abxcdtimeout", "unrelated"},
                 {true, false, false}, true);
}

TEST(RegexGramRecallTest, GroupFlagsPreserveScalarRecall) {
    // Disabling case folding inside any group must restore the enclosing (?i) after ')'.
    // The final case-sensitive suffix keeps every scheme selective even if the folded part
    // has too many alternatives to enumerate.
    for (const auto* pattern :
         {"(?i)(?-i:abc)DEF(?-i:timeout)", "(?i)((?-i)abc)DEF(?-i:timeout)",
          "(?i)(?:(?-i)abc)DEF(?-i:timeout)", "(?i)(?P<word>(?-i)abc)DEF(?-i:timeout)",
          "(?i)(?:(?-i)(abc))DEF(?-i:timeout)"}) {
        check_recall(pattern, {"abcdeftimeout", "abcDEFtimeout", "ABCdeftimeout", "unrelated"},
                     {true, true, false, false}, true);
    }

    // A flags-only group changes the remainder of its enclosing scope; it is not a nested
    // scope whose flags should be restored immediately after its own closing parenthesis.
    check_recall("(?i)ABC(?-i)timeout", {"abctimeout", "ABCtimeout", "ABCTIMEOUT", "unrelated"},
                 {true, true, false, false}, true);
    check_recall("(?:(?i)ABC(?-i)timeout)END",
                 {"abctimeoutEND", "ABCtimeoutEND", "ABCTIMEOUTEND", "abctimeoutend", "unrelated"},
                 {true, true, false, false, false}, true);
}

TEST(RegexGramRecallTest, QuotedLiteralQuantifiersPreserveScalarRecall) {
    // Quoting changes how characters are tokenized; it does not group them into one atom.
    check_recall(R"(\Qabc\E+deftimeout)",
                 {"abcccdeftimeout", "abcdeftimeout", "abdeftimeout", "unrelated"},
                 {true, true, false, false}, true);
    check_recall(R"(\Qab\E{2}timeout)", {"abbtimeout", "ababtimeout", "abtimeout", "unrelated"},
                 {true, false, false, false}, true);
    check_recall(R"(\Qabc\E*timeout)", {"abtimeout", "abccctimeout", "atimeout", "unrelated"},
                 {true, true, false, false}, true);
    check_recall(R"(\Qa.b+\E{2}timeout)",
                 {"a.b++timeout", "a.b+timeout", "abbbtimeout", "unrelated"},
                 {true, false, false, false}, true);

    // Explicit parentheses still make the whole quoted sequence the repeated atom.
    check_recall(R"((\Qabc\E)+timeout)",
                 {"abcabctimeout", "abctimeout", "abccctimeout", "unrelated"},
                 {true, true, false, false}, true);

    // Empty quotes are transparent, including between an existing atom and its quantifier.
    check_recall(R"(ab\Q\E+timeout)", {"abbbtimeout", "abtimeout", "atimeout", "unrelated"},
                 {true, true, false, false}, true);
    check_recall(R"((ab)\Q\E{2}timeout)", {"ababtimeout", "abbtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(ab\Q\E\Q\E{2}timeout)", {"abbtimeout", "abtimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(\Q\Etimeout\Q\E)", {"timeout", "unrelated"}, {true, false}, true);
    // Hyperscan rejects the two separated repeats. Its RE2 fallback treats the '?' after
    // the empty quote as a second repeat over b+, allowing zero occurrences of b. Removing
    // the quote would instead join '+?' into one lazy repeat, which still requires a b.
    std::vector<bool> matches;
    const auto hs_only = scalar_matches(R"(ab+\Q\E?timeout)", {"atimeout"}, &matches, false);
    ASSERT_FALSE(hs_only.ok());
    EXPECT_NE(hs_only.to_string().find("Invalid repeat"), std::string::npos);
    check_recall(R"(ab+\Q\E?timeout)", {"abbbtimeout", "abtimeout", "atimeout", "unrelated"},
                 {true, true, true, false}, true);
    check_recall(R"(ab+?timeout)", {"abbbtimeout", "abtimeout", "atimeout", "unrelated"},
                 {true, true, false, false}, true);

    // The final atom is an entire Unicode code point, including a four-byte character.
    check_recall(R"(\Qabé\E{2}timeout)", {"abéétimeout", "abétimeout", "unrelated"},
                 {true, false, false}, true);
    check_recall(R"(\Qab😀\E{2}timeout)", {"ab😀😀timeout", "ab😀timeout", "unrelated"},
                 {true, false, false}, true);
}

TEST(RegexGramRecallTest, EmbeddedNulPreservesScalarRecall) {
    // These patterns bypass the scalar string fast paths. Hyperscan receives a C string,
    // so the literal constraints after the raw NUL are not required by the scalar match.
    check_recall(std::string("foo[0-9]") + '\0' + "timeout", {"foo1", "xfoo2x", "foo", "unrelated"},
                 {true, true, false, false}, false);
    check_recall<FunctionLike>(std::string("%prefix") + '\0' + "%timeout%",
                               {"prefix", "xprefixx", "timeout", "unrelated"},
                               {true, true, false, false}, false);
    check_recall<FunctionLike>(std::string("prefix_") + '\0' + "%timeout%",
                               {"prefix1", "prefix12", "xprefix1", "prefix", "unrelated"},
                               {true, true, false, false, false}, false);

    // The scalar string fast paths are length-aware. Raw NUL must not be universally
    // reinterpreted as a terminator: these patterns still require their entire literal.
    const std::string literal = std::string("foo") + '\0' + "timeout";
    check_recall(literal, {literal, "foo", "unrelated"}, {true, false, false}, false);
    check_recall<FunctionLike>(literal, {literal, "foo", "unrelated"}, {true, false, false}, false);

    // An escaped NUL is regex syntax, not a raw pattern terminator. Its determined suffix
    // and ordinary LIKE segments must continue to filter unrelated rows.
    check_recall(R"(foo\x00timeout)", {literal, "foo", "unrelated"}, {true, false, false}, true);
    check_recall<FunctionLike>("%prefix%timeout%", {"prefixtimeout", "prefix", "unrelated"},
                               {true, false, false}, true);
}

} // namespace doris::segment_v2::gram
