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

#include "storage/index/inverted/gram/gram_extractor.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

namespace doris::segment_v2::gram {

static std::vector<std::string> run(const GramScheme& s, std::string_view v) {
    GramExtractor ex(s);
    std::vector<std::string_view> out;
    ex.extract(v, &out);
    return {out.begin(), out.end()};
}

TEST(GramExtractorTest, DenseAsciiTrigrams) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    EXPECT_EQ(run(s, "abcde"), (std::vector<std::string> {"abc", "bcd", "cde"}));
    EXPECT_EQ(run(s, "ab"), (std::vector<std::string> {}));        // 短于 n 不产出
    EXPECT_EQ(run(s, "aaaa"), (std::vector<std::string> {"aaa"})); // 行内去重
}

TEST(GramExtractorTest, NonAsciiCodepointsAreUnigrams) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    // "手机ab微博" → 手, 机, 微, 博；ASCII 段 "ab" 短于 3 不产出
    EXPECT_EQ(run(s, "手机ab微博"), (std::vector<std::string> {"手", "机", "微", "博"}));
    // 非法 UTF-8 字节 0xFF 作为单字节 1-gram
    EXPECT_EQ(run(s, std::string("\xFF", 1)), (std::vector<std::string> {std::string("\xFF", 1)}));
}

TEST(GramExtractorTest, SparseIsLocalAndDeterministic) {
    GramScheme s; // sparse p=0.25 n=3 L=16
    GramExtractor ex(s);
    // 局部性：任意子串的 gram 集合 ⊆ 全串的 gram 集合
    const std::string doc = "rpc error: code = Unavailable desc = error reading from server";
    std::vector<std::string_view> all;
    ex.extract(doc, &all);
    std::set<std::string> whole(all.begin(), all.end());
    for (size_t i = 0; i < doc.size(); i++) {
        for (size_t len = 3; i + len <= doc.size(); len++) {
            std::vector<std::string_view> sub;
            ex.extract(std::string_view(doc).substr(i, len), &sub);
            for (auto g : sub) {
                EXPECT_TRUE(whole.count(std::string(g))) << "gram '" << g << "' of substring [" << i
                                                         << "," << len << ") missing from whole";
            }
        }
    }
    // 密度：0.25 时 gram 数明显少于稠密 (len-2)
    EXPECT_LT(all.size(), (doc.size() - 2) / 2);
}

TEST(GramExtractorTest, SparseGoldenFromPrototype) {
    // golden 由 tools/regex-ngram-model/ngram_model.cpp --cdc --p 0.25 --maxlen 16 --n 3 生成
    GramScheme s;
    EXPECT_EQ(run(s, "rpc error: code = Unavailable"),
              (std::vector<std::string> {"or: co", "cod", "ode = U", " Unavai", "ailable"}));
}

TEST(GramExtractorTest, LowerCaseFoldsBeforeBoundaryHash) {
    GramScheme s;
    s.lower_case = true;
    EXPECT_EQ(run(s, "Code = Unavailable"), run(s, "code = unavailable"));
}

// 覆盖 Ruling R9：提取器绝不产出含 NUL 字节（0x00）的 gram。
TEST(GramExtractorTest, NoGramContainsNulByte) {
    // DENSE n=3："ab\0cd"（5 字节）的三个窗口 "ab\0" "b\0c" "\0cd" 全部含 NUL，
    // 应全部被跳过，不改变窗口边界的计算方式，只是不产出。
    GramScheme dense;
    dense.mode = GramMode::DENSE;
    dense.min_len = 3;
    EXPECT_EQ(run(dense, std::string("ab\0cd", 5)), (std::vector<std::string> {}));

    // SPARSE：更长的字符串中间嵌入一个 NUL，跨过 NUL 的候选 gram 被跳过，但
    // 远离 NUL 的部分仍应正常产出 gram（局部性不受影响）。
    GramScheme sparse; // 默认 SPARSE，p=0.25 n=3 max_len=16
    GramExtractor ex(sparse);
    std::string doc = "rpc error: code = Unavailable desc = error reading from server";
    doc.insert(doc.begin() + 10, '\0');
    std::vector<std::string_view> out;
    ex.extract(doc, &out);
    ASSERT_FALSE(out.empty());
    for (const auto& g : out) {
        EXPECT_EQ(g.find('\0'), std::string_view::npos) << "gram '" << g << "' contains NUL";
    }
}

TEST(GramExtractorTest, BoundaryTableMatchesFormula) {
    GramScheme s;
    GramExtractor ex(s);
    size_t cnt = 0;
    for (int a = 0; a < 256; a++) {
        for (int b = 0; b < 256; b++) {
            cnt += ex.is_boundary(a, b);
        }
    }
    // p=0.25 ± 2%
    EXPECT_NEAR(cnt / 65536.0, 0.25, 0.02);
}

} // namespace doris::segment_v2::gram
