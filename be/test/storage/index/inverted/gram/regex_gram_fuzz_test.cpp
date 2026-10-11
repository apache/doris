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

// Evaluate a query tree over the gram set of one row. It deliberately reuses none of the
// simplification logic of GramQuery::and_/or_ and re-implements the evaluation from scratch by Op
// semantics, which is what makes this function a "truth interpreter" independent of the compiler
// under test: calling the code under test here would reduce the differential test to verifying
// the code under test with itself.
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

// The atom vocabulary shared by random regexes and random rows: the first 10 entries are "natural
// language" words/tokens (random_row draws from that range too when assembling a row), the last
// 10 are regex metacharacters/classes used only by random_regex.
const char* kAtoms[] = {"error", "code", "Unavailable", "timeout", "user_id=", "10.68.", "GET",
                        "POST",  "手机", "微博",        "[0-9]",   "[a-z]",    "\\d",    ".",
                        "a",     "b",    "c",           " ",       "=",        ":"};

// Generate a random regex within the RE2 syntax subset: 1 to 3 concatenated atoms, each with
// roughly even odds of being wrapped in one of an alternation (a|b), ?, +, *, {1,3}; depth caps
// how deep alternation branches recurse, so the regex cannot explode in size at random (a second
// safety net alongside RegexGramCompiler's own depth protection).
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

// Generate one random row of text: pick one of 8 preset templates and, with probability 1/3,
// append a "natural language" atom. The templates and atoms (two CJK templates included) are all
// valid UTF-8 literals, so this function always produces a valid UTF-8 row -- RE2's behaviour of
// refusing to match an invalid UTF-8 row is never triggered by this test.
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

// Run "compile + a differential check over 20 rows" for one regex; extracted from TestBody to
// keep its cognitive complexity down (clang-tidy readability-function-cognitive-complexity,
// threshold 50). gtest allows ASSERT_* inside an ordinary void function: on failure the return
// inside the macro only leaves this function and does not terminate the caller's loop, so the
// caller must still check HasFatalFailure() after every call to preserve the original "stop
// everything as soon as a false negative appears" behaviour.
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

// Differential fuzz test: for random regexes and random rows, compare RE2's ground truth with the
// result of "evaluating the compiled GramQuery over that row's gram set". The one forbidden
// combination is truth=true with cand=false (the compiler may only give false positives, never
// false negatives: truth => cand); the reverse, cand=true with truth=false, is entirely allowed
// and simply means the candidate row must be re-verified by the regexp expression above it.
// The four schemes DENSE/SPARSE x lower_case=false/true share one rng sequence, so all four see
// exactly the same distribution of (regex, row) pairs and the only variable is the scheme; 3000
// regexes x 20 rows = 60k RE2 matches per scheme, 240k across the four.
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
            // Non-fatal coverage assertions guarding against a degenerate generator/compiler: at
            // least 1000 of the 3000 random regexes must pass RE2's syntax check (the generator
            // is not producing mostly invalid regexes), and at least 1/4 of the compilations must
            // not be ALL (the compiler has not given up filtering on nearly every regex).
            EXPECT_GT(compiled, 1000);
            EXPECT_GT(indexable, compiled / 4) << "compiler is too conservative";
        }
    }
}

} // namespace doris::segment_v2::gram
