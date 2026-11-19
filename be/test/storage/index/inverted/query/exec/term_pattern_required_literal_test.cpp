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

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/query/exec/term_pattern.h"

namespace doris::index_query {
namespace {

std::string required_literal(std::string_view pattern) {
    TermPattern term_pattern;
    const Status status = TermPattern::create(TermPatternKind::kRegexp, pattern, &term_pattern);
    EXPECT_TRUE(status.ok()) << pattern << ": " << status.to_string();
    return term_pattern.required_text();
}

} // namespace

// The literal of a regular expression is the longest run of literal code points every match
// holds; anything optional, alternative, case-folded or a class ends the run. An expression with
// an enumeration prefix names none: the prefix already bounds its walk, and a format that tests
// the literal before the prefix would walk on past the prefix range.
TEST(TermPatternRegexpLiteral, NamesTheLongestLiteralEveryMatchHolds) {
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"cache", "cache"},        {"^cache$", ""},      {"^cach.", ""},
            {"^.*cache", "cache"},     {"cach.", "cach"},    {"cache\\.go", "cache.go"},
            {"ab(cd)ef", "abcdef"},    {"abc(de)+f", "abc"}, {"x(abcd)+y", "abcd"},
            {"x{0,3}yz", "yz"},        {"(abc){2}", "abc"},  {"a*bcd", "bcd"},
            {"cach?e", "cac"},         {"c[ab]che", "che"},  {"\\d+timeout", "timeout"},
            {"timeout\\b", "timeout"}, {"\\Qa*b\\E", "a*b"}, {"猫.狗", "猫"},
            {"cache|error", ""},       {"(?i)cache", ""},    {"[a-z]+", ""},
    };
    for (const auto& [pattern, literal] : cases) {
        EXPECT_EQ(required_literal(pattern), literal) << pattern;
    }
}

// The literal only narrows the terms tested; the expression still decides each match.
TEST(TermPatternRegexpLiteral, LiteralOnlyNarrowsTheTerms) {
    TermPattern term_pattern;
    ASSERT_TRUE(TermPattern::create(TermPatternKind::kRegexp, "cach.", &term_pattern).ok());
    EXPECT_EQ(term_pattern.required_text(), "cach");
    EXPECT_TRUE(term_pattern.matches("cache"));
    EXPECT_TRUE(term_pattern.matches("mcached"));
    EXPECT_FALSE(term_pattern.matches("cach"));
    EXPECT_FALSE(term_pattern.matches("error"));
}

// A pattern the regular-expression engine rejects matches nothing and names no literal.
TEST(TermPatternRegexpLiteral, InvalidPatternNamesNoLiteral) {
    TermPattern term_pattern;
    ASSERT_TRUE(TermPattern::create(TermPatternKind::kRegexp, "cache(", &term_pattern).ok());
    EXPECT_FALSE(term_pattern.can_match());
    EXPECT_EQ(term_pattern.required_text(), "");
}

// Hyperscan reads the expression as a C string, so a raw NUL ends it. The literal and the prefix
// must come from that same text, or they would skip terms the expression matches.
TEST(TermPatternRegexpLiteral, RawNulEndsTheExpression) {
    TermPattern unanchored;
    ASSERT_TRUE(TermPattern::create(TermPatternKind::kRegexp, std::string("cache\0missing", 13),
                                    &unanchored)
                        .ok());
    EXPECT_TRUE(unanchored.matches("cache"));
    EXPECT_EQ(unanchored.required_text(), "cache");

    TermPattern anchored;
    ASSERT_TRUE(TermPattern::create(TermPatternKind::kRegexp, std::string("^cache\0missing", 14),
                                    &anchored)
                        .ok());
    EXPECT_TRUE(anchored.matches("cache"));
    EXPECT_TRUE(std::string_view("cache").starts_with(anchored.enumeration_prefix()))
            << anchored.enumeration_prefix();
}

} // namespace doris::index_query
