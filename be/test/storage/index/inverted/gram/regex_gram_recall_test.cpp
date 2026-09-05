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
#include "runtime/runtime_state.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"

namespace doris::segment_v2::gram {
namespace {

// The expression chooses the real scalar engine, including Hyperscan and its RE2 fallback.
// Calling RE2 directly would miss differences between the compiler and the production path.
template <typename Function = FunctionRegexpLike>
Status scalar_matches(const std::string& pattern, const std::vector<std::string>& rows,
                      std::vector<bool>* matches, bool enable_hyperscan_fallback = true) {
    TQueryOptions query_options;
    query_options.__set_enable_hyperscan_fallback(enable_hyperscan_fallback);
    RuntimeState runtime_state(query_options, TQueryGlobals {});
    auto string_type = std::make_shared<DataTypeString>();
    auto context = FunctionContext::create_context(
            &runtime_state, std::make_shared<DataTypeUInt8>(), {string_type, string_type});

    auto values = ColumnString::create();
    for (const auto& row : rows) {
        values->insert_data(row.data(), row.size());
    }
    auto patterns = ColumnString::create();
    patterns->insert_data(pattern.data(), pattern.size());
    ColumnPtr pattern_column = ColumnConst::create(std::move(patterns), rows.size());
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_columns(2);
    constant_columns[1] = std::make_shared<ColumnPtrWrapper>(pattern_column);
    context->set_constant_cols(constant_columns);

    Function function;
    RETURN_IF_ERROR(function.open(context.get(), FunctionContext::THREAD_LOCAL));
    Block block;
    block.insert({std::move(values), string_type, "value"});
    block.insert({std::move(pattern_column), string_type, "pattern"});
    block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "result"});
    RETURN_IF_ERROR(function.execute_impl(context.get(), block, {0, 1}, 2, rows.size()));
    matches->clear();
    for (size_t i = 0; i < rows.size(); ++i) {
        matches->push_back(block.get_by_position(2).column->get_bool(i));
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
                         bool require_pruning) {
    SCOPED_TRACE(scheme.cache_key());
    RegexGramCompiler compiler(scheme);
    GramExtractor extractor(scheme);
    GramQuery query;
    if constexpr (std::is_same_v<Function, FunctionLike>) {
        ASSERT_TRUE(compiler.compile_like(pattern, &query).ok());
    } else {
        ASSERT_TRUE(compiler.compile_regexp(pattern, &query).ok());
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
                  const std::vector<bool>& expected, bool require_pruning) {
    SCOPED_TRACE(pattern);
    std::vector<bool> truth;
    ASSERT_TRUE(scalar_matches<Function>(pattern, rows, &truth).ok());
    ASSERT_EQ(truth, expected);
    for (auto mode : {GramMode::DENSE, GramMode::SPARSE}) {
        for (bool lower_case : {false, true}) {
            GramScheme scheme;
            scheme.mode = mode;
            scheme.lower_case = lower_case;
            check_scheme_recall<Function>(scheme, pattern, rows, truth, require_pruning);
        }
    }
}

} // namespace

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
