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
#include <re2/re2.h>

#include <algorithm>
#include <random>
#include <set>

#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"

namespace doris::segment_v2::gram {

namespace {

// 在一行的 gram 集合上求值查询树。刻意不复用 GramQuery::and_/or_ 的化简逻辑，
// 而是照 Op 语义从零手写一遍求值，这样本函数才是一个独立于被测编译器的「真值
// 解释器」：如果这里也调用被测代码，差分测试就退化成了用被测代码验证被测代码。
bool eval(const GramQuery& q, const std::set<std::string>& grams) {
    switch (q.op) {
    case GramQuery::Op::ALL:
        return true;
    case GramQuery::Op::NONE:
        return false;
    case GramQuery::Op::AND:
        return std::ranges::all_of(q.grams, [&](const auto& g) { return grams.contains(g); }) &&
               std::ranges::all_of(q.subs, [&](const auto& s) { return eval(s, grams); });
    case GramQuery::Op::OR:
        return std::ranges::any_of(q.grams, [&](const auto& g) { return grams.contains(g); }) ||
               std::ranges::any_of(q.subs, [&](const auto& s) { return eval(s, grams); });
    }
    return true;
}

// 随机正则 / 随机行共用的原子词表：前 10 项是「自然语言」词/token（random_row
// 拼行时也从这一段取），后 10 项是正则元字符/字符类，只用于 random_regex 拼正则。
const char* kAtoms[] = {"error", "code", "Unavailable", "timeout", "user_id=", "10.68.", "GET",
                        "POST",  "手机", "微博",        "[0-9]",   "[a-z]",    "\\d",    ".",
                        "a",     "b",    "c",           " ",       "=",        ":"};

// 生成一个 RE2 语法子集内的随机正则：1~3 个原子拼接，每个原子有约一半概率被
// 交替分支 (a|b)、?、+、*、{1,3} 之一包裹；depth 控制交替分支递归展开的层数
// 上限，避免正则规模随机爆炸（配合 RegexGramCompiler 自身的深度保护双保险）。
std::string random_regex(std::mt19937& rng, int depth) {
    std::uniform_int_distribution<int> pick(0, 19), shape(0, 9);
    std::string s;
    int parts = 1 + rng() % 3;
    for (int i = 0; i < parts; i++) {
        int sh = shape(rng);
        std::string atom = kAtoms[pick(rng)];
        if (depth > 0 && sh == 0) {
            atom = "(" + random_regex(rng, depth - 1) + "|" + random_regex(rng, depth - 1) + ")";
        }
        if (sh == 1) {
            atom = "(" + atom + ")?";
        }
        if (sh == 2) {
            atom = "(" + atom + ")+";
        }
        if (sh == 3) {
            atom = "(" + atom + ")*";
        }
        if (sh == 4) {
            atom = "(" + atom + "){1,3}";
        }
        s += atom;
    }
    return s;
}

// 生成一行随机文本：从 8 个预置模板中选一个，1/3 概率再追加一个「自然语言」
// 原子。模板与原子（含两个中文模板）全部是合法 UTF-8 字面量，因此本函数恒
// 产出合法 UTF-8 行——RE2 对非法 UTF-8 行拒绝匹配的行为不会在本测试里触发。
std::string random_row(std::mt19937& rng) {
    static const char* kRows[] = {"rpc error: code = Unavailable desc = timeout",
                                  "user_id=abc GET /images/x.gif",
                                  "手机微博 POST 10.68.3.18:8080 error",
                                  "Convert conversion successful",
                                  "",
                                  "aaa bbb ccc",
                                  "code=Unavailable",
                                  "timeout after error error error"};
    std::string r = kRows[rng() % 8];
    if (rng() % 3 == 0) {
        r += kAtoms[rng() % 10];
    }
    return r;
}

// 对一条正则跑「编译 + 20 行差分校验」；从 TestBody 里抽出来是为了压低其认知
// 复杂度（clang-tidy readability-function-cognitive-complexity，阈值 50）。
// gtest 允许在返回 void 的普通函数里使用 ASSERT_*：失败时宏内部的 return 只会
// 退出这个函数本身、不会自动终止调用方的循环，因此调用方每次调用后还需自行
// 检查 HasFatalFailure() 才能保留「一发现假阴性就整体停止」的原始行为。
void check_regex(GramMode mode, bool lc, GramExtractor& ex, RegexGramCompiler& comp,
                 std::mt19937& rng, const std::string& re, int* compiled, int* indexable) {
    RE2 rx(re, RE2::Quiet);
    if (!rx.ok()) {
        return;
    }
    GramQuery q;
    ASSERT_TRUE(comp.compile_regexp(re, &q).ok());
    (*compiled)++;
    if (!q.is_all()) {
        (*indexable)++;
    }
    for (int r = 0; r < 20; r++) {
        std::string row = random_row(rng);
        bool truth = RE2::PartialMatch(row, rx);
        std::vector<std::string_view> g;
        ex.extract(row, &g);
        std::set<std::string> grams(g.begin(), g.end());
        bool cand = eval(q, grams);
        ASSERT_TRUE(!truth || cand)
                << "FALSE NEGATIVE mode=" << (int)mode << " lc=" << lc << " re=" << re
                << " row=" << row << " q=" << q.to_debug_string();
    }
}

} // namespace

// 差分模糊测试：对随机正则与随机行，比较 RE2 的真值判定与「编译出的 GramQuery
// 在该行 gram 集合上求值」的结果。唯一禁止出现的组合是 truth=true 且
// cand=false（编译器只能漏杀、不能误杀：truth ⇒ cand）；反过来 cand=true 但
// truth=false 完全允许，那只是候选行需要交给上层 regexp 表达式复验。
// DENSE/SPARSE × lower_case=false/true 四种方案共用同一个 rng 序列，保证四条
// 曲线看到完全相同的《正则, 行》分布、唯一变量是 scheme；3000 条正则 × 20 行
// = 每种方案 6 万次、四种方案合计 24 万次 RE2 匹配。
TEST(RegexGramFuzzTest, CompiledQueryIsSuperset) {
    std::mt19937 rng(20260903);
    for (GramMode mode : {GramMode::DENSE, GramMode::SPARSE}) {
        for (bool lc : {false, true}) {
            GramScheme s;
            s.mode = mode;
            s.lower_case = lc;
            GramExtractor ex(s);
            RegexGramCompiler comp(s);
            int compiled = 0, indexable = 0;
            for (int it = 0; it < 3000; it++) {
                std::string re = random_regex(rng, 2);
                check_regex(mode, lc, ex, comp, rng, re, &compiled, &indexable);
                if (::testing::Test::HasFatalFailure()) {
                    return;
                }
            }
            // 非致命覆盖率断言，用于防止生成器/编译器退化：至少 1000/3000 条随机
            // 正则能通过 RE2 语法校验（生成器没有大面积产出非法正则）；且至少
            // 1/4 的编译结果不是 ALL（编译器没有对几乎所有正则都放弃过滤）。
            EXPECT_GT(compiled, 1000);
            EXPECT_GT(indexable, compiled / 4) << "编译器过于保守";
        }
    }
}

} // namespace doris::segment_v2::gram
