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

#include "storage/index/inverted/gram/regex_gram_compiler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace doris::segment_v2::gram {

// Dense golden: byte-for-byte identical to the output of the prototype's
// `ngram_model_check --n 3 --explain <re>` (for 3-byte grams the numeric order matches the
// lexicographic one, so the prototype's hash-order printout is already lexicographic).
static std::string dense(const std::string& re) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    RegexGramCompiler c(s);
    GramQuery q;
    EXPECT_TRUE(c.compile_regexp(re, &q).ok());
    return q.to_debug_string();
}

// Sparse golden: corresponds to the prototype's
// `--n 3 --cdc --p 0.25 --maxlen 24 --explain <re>`; the gram sets are the same, but GramQuery
// emits them in lexicographic order (the prototype prints them in hash order).
static std::string sparse(const std::string& re) {
    GramScheme s;
    s.max_len = 24;
    RegexGramCompiler c(s);
    GramQuery q;
    EXPECT_TRUE(c.compile_regexp(re, &q).ok());
    return q.to_debug_string();
}

TEST(RegexGramCompilerTest, DenseGolden) {
    EXPECT_EQ(dense("abc"), "(\"abc\")");
    EXPECT_EQ(dense("a.*b"), "ALL");
    EXPECT_EQ(dense("\\d{3}-\\d{4}"), "ALL");
    EXPECT_EQ(dense("hello|world"),
              "((\"ell\" & \"hel\" & \"llo\") | (\"orl\" & \"rld\" & \"wor\"))");
    EXPECT_EQ(
            dense("(foo|bar)baz"),
            "((\"arb\" & \"bar\" & \"baz\" & \"rba\") | (\"baz\" & \"foo\" & \"oba\" & \"oob\"))");
    EXPECT_EQ(dense("conn(ection)? re(set|fused)"),
              "(\" re\" & \"con\" & \"onn\" & ((\"efu\" & \"fus\" & \"ref\" & \"sed\" & \"use\") | "
              "(\"ese\" & \"res\" & \"set\")))");
    EXPECT_EQ(dense("GET|POST"), "(\"GET\" | (\"OST\" & \"POS\"))");
    EXPECT_EQ(dense("a(b|cd)e"), "(\"abe\" | (\"acd\" & \"cde\"))");
    EXPECT_EQ(dense("[ab]cd"), "(\"acd\" | \"bcd\")");
    EXPECT_EQ(dense("(abc){2}"), "(\"abc\" & \"bca\" & \"cab\")");
    EXPECT_EQ(dense("error.*timeout"),
              "(\"eou\" & \"err\" & \"ime\" & \"meo\" & \"out\" & \"ror\" & \"rro\" & \"tim\")");
}

TEST(RegexGramCompilerTest, SparseGolden) {
    EXPECT_EQ(sparse("rpc error: code = Unavailable"),
              "(\" Unavai\" & \"ailable\" & \"cod\" & \"ode = U\" & \"or: co\")");
    EXPECT_EQ(sparse("error.*timeout"), "(\"timeo\")");
    EXPECT_EQ(sparse("GET|POST"), "ALL");
}

TEST(RegexGramCompilerTest, ParseErrorIsAll) {
    GramScheme s;
    RegexGramCompiler c(s);
    GramQuery q;
    ASSERT_TRUE(c.compile_regexp("(ab", &q).ok());
    EXPECT_TRUE(q.is_all());
}

TEST(RegexGramCompilerTest, CaseInsensitiveWithAndWithoutFolding) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    RegexGramCompiler c(s);
    GramQuery q;
    ASSERT_TRUE(c.compile_regexp("(?i)abcd", &q).ok());
    EXPECT_EQ(q.to_debug_string(),
              "((\"ABC\" | \"ABc\" | \"AbC\" | \"Abc\" | \"aBC\" | \"aBc\" | \"abC\" | \"abc\") & "
              "(\"BCD\" | \"BCd\" | \"BcD\" | \"Bcd\" | \"bCD\" | \"bCd\" | \"bcD\" | \"bcd\"))");
    s.lower_case = true;
    RegexGramCompiler c2(s);
    ASSERT_TRUE(c2.compile_regexp("(?i)ABCD", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"bcd\")");
}

TEST(RegexGramCompilerTest, Like) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    RegexGramCompiler c(s);
    GramQuery q;
    ASSERT_TRUE(c.compile_like("%abcd%ef_gh%", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"bcd\")"); // "ef" and "gh" are shorter than 3
    ASSERT_TRUE(c.compile_like("abc\\%def", &q).ok());     // an escaped % is a literal
    EXPECT_EQ(q.to_debug_string(), "(\"%de\" & \"abc\" & \"bc%\" & \"c%d\" & \"def\")");
    ASSERT_TRUE(c.compile_like("%", &q).ok());
    EXPECT_TRUE(q.is_all());
}

// Ruling R10: the LIKE escape character is fixed to `\`, and only `\%`, `\_` and `\\` are real
// escapes; for `\x` (x none of those three) it is unclear whether the engine keeps the backslash
// itself (two bytes "\x" in the row) or drops it (just "x" in the row, the old implementation's
// wrong assumption). All that is certainly adjacent under both readings is "x and whatever
// follows it", so the segment must be cut at the backslash: the literal before it (such as "abc")
// may not be merged with x, while x may still form a new segment with what comes after it (such
// as "def").
TEST(RegexGramCompilerTest, LikeEscapeConservative) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    RegexGramCompiler c(s);
    GramQuery q;

    // `\d` is not a known escape: cut between "abc" and "d"; "d" is still adjacent to the "ef"
    // after it, forming the new segment "def". Each segment yields exactly one 3-gram.
    ASSERT_TRUE(c.compile_like("abc\\def", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"def\")");

    // An existing golden that must not change: `\%` is a known escape, so "%" joins the previous
    // segment.
    ASSERT_TRUE(c.compile_like("abc\\%def", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"%de\" & \"abc\" & \"bc%\" & \"c%d\" & \"def\")");

    // `\\` escapes to one literal backslash, forming the contiguous literal segment "ab\cd"
    // (5 bytes: a b \ c d) together with its neighbours. The resulting grams contain 0x5C, which
    // sorts before the letters; comparing to_debug_string directly would need two layers of
    // manual escaping, so we compare the sorted q.grams instead to avoid escaping mistakes.
    ASSERT_TRUE(c.compile_like("ab\\\\cd", &q).ok());
    ASSERT_EQ(q.op, GramQuery::Op::AND);
    EXPECT_TRUE(q.subs.empty());
    std::vector<std::string> grams = q.grams;
    std::sort(grams.begin(), grams.end());
    EXPECT_EQ(grams, std::vector<std::string>({"\\cd", "ab\\", "b\\c"}));

    // A lone trailing backslash: nothing to escape, so the segment is cut and it is ignored.
    ASSERT_TRUE(c.compile_like("abc\\", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\")");

    // The escaped % yields a 1-byte literal segment, too short for one gram (n=3) -> ALL.
    ASSERT_TRUE(c.compile_like("%\\%", &q).ok());
    EXPECT_TRUE(q.is_all());
}

} // namespace doris::segment_v2::gram
