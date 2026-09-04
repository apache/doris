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

// 稠密 golden：与原型 `ngram_model_check --n 3 --explain <re>` 的输出逐字相同
//（3 字节 gram 的数值序与字典序一致，故原型的哈希序打印即字典序）。
static std::string dense(const std::string& re) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    RegexGramCompiler c(s);
    GramQuery q;
    EXPECT_TRUE(c.compile_regexp(re, &q).ok());
    return q.to_debug_string();
}

// 稀疏 golden：对应原型 `--n 3 --cdc --p 0.25 --maxlen 24 --explain <re>`，
// gram 集合相同，但 GramQuery 按字典序排序输出（原型按哈希序打印）。
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
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"bcd\")"); // "ef" 与 "gh" 短于 3
    ASSERT_TRUE(c.compile_like("abc\\%def", &q).ok());     // 转义的 % 是字面量
    EXPECT_EQ(q.to_debug_string(), "(\"%de\" & \"abc\" & \"bc%\" & \"c%d\" & \"def\")");
    ASSERT_TRUE(c.compile_like("%", &q).ok());
    EXPECT_TRUE(q.is_all());
}

// Ruling R10：LIKE 的转义字符固定假定为 `\`，只有 `\%`、`\_`、`\\` 是真正的
// 转义；`\x`（x 不是这三者之一）时不确定引擎是保留反斜杠本身（行内 "\x" 两
// 字节）还是丢弃反斜杠（行内只有 "x"，旧实现的错误假设）。两种语义下都能确定
// 连续出现的只有「x 与它之后的字符」，因此必须在反斜杠处切段：段前的字面量
// （如 "abc"）不能与 x 合并，x 仍可与其后的字符合成新段（如 "def"）。
TEST(RegexGramCompilerTest, LikeEscapeConservative) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    RegexGramCompiler c(s);
    GramQuery q;

    // `\d` 不是已知转义：在 "abc" 与 "d" 之间切段；"d" 与其后的 "ef" 仍连续，
    // 合成新段 "def"。两段各恰好凑成一个 3-gram。
    ASSERT_TRUE(c.compile_like("abc\\def", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"def\")");

    // 既有 golden，必须保持不变：`\%` 是已知转义，"%" 并入前一段。
    ASSERT_TRUE(c.compile_like("abc\\%def", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"%de\" & \"abc\" & \"bc%\" & \"c%d\" & \"def\")");

    // `\\` 转义出一个字面反斜杠，与前后字符组成连续字面量段
    // "ab\cd"（5 字节：a b \ c d）。产出的 gram 本身含 0x5C，字典序排在字母
    // 之前；直接比较 to_debug_string 需要手工转义两层，改为比较排序后的
    // q.grams，避免转义出错。
    ASSERT_TRUE(c.compile_like("ab\\\\cd", &q).ok());
    ASSERT_EQ(q.op, GramQuery::Op::AND);
    EXPECT_TRUE(q.subs.empty());
    std::vector<std::string> grams = q.grams;
    std::sort(grams.begin(), grams.end());
    EXPECT_EQ(grams, std::vector<std::string>({"\\cd", "ab\\", "b\\c"}));

    // 模式尾部单独一个反斜杠：没有可转义的对象，切段后忽略。
    ASSERT_TRUE(c.compile_like("abc\\", &q).ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\")");

    // 转义的 % 只产出 1 字节字面量段，不够一个 gram（n=3）→ ALL。
    ASSERT_TRUE(c.compile_like("%\\%", &q).ok());
    EXPECT_TRUE(q.is_all());
}

} // namespace doris::segment_v2::gram
