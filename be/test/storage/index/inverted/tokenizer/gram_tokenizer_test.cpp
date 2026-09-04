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

#include "storage/index/inverted/tokenizer/ngram/gram_tokenizer.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/setting.h"
#include "storage/index/inverted/tokenizer/ngram/ngram_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

// Ruling R8：BE UT 二进制启用 Unity Build，ngram_tokenizer_test.cpp 已经在本命名空间
// 定义了同名同参的 tokenize(NGramTokenizerFactory&, const std::string&)，直接重名会在
// 合批后的翻译单元里重定义报错。这里用文件专属的具名命名空间隔离辅助函数，并改名为
// gram_tokenize 双重避免冲突。
namespace gram_tokenizer_test_detail {

std::vector<std::string> gram_tokenize(NGramTokenizerFactory& factory, const std::string& data) {
    auto tokenizer = factory.create();
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(data.data(), data.size(), false);
    tokenizer->set_reader(reader);
    tokenizer->reset();
    std::vector<std::string> out;
    Token t;
    while (tokenizer->next(&t)) {
        out.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    return out;
}

} // namespace gram_tokenizer_test_detail

using gram_tokenizer_test_detail::gram_tokenize;

TEST(GramTokenizerTest, ModeAbsentKeepsLegacyBehaviour) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"min_gram", "2"}, {"max_gram", "3"}};
    Settings settings(args);
    factory.initialize(settings);
    EXPECT_FALSE(factory.gram_scheme().has_value());
    EXPECT_EQ(gram_tokenize(factory, "abc"), (std::vector<std::string> {"ab", "abc", "bc"}));
}

TEST(GramTokenizerTest, SparseModeProducesCdcGrams) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {
            {"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "16"}, {"density", "0.25"}};
    Settings settings(args);
    factory.initialize(settings);
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_EQ(factory.gram_scheme()->mode, gram::GramMode::SPARSE);
    // golden 同 GramExtractorTest.SparseGoldenFromPrototype（按起点顺序）
    EXPECT_EQ(gram_tokenize(factory, "rpc error: code = Unavailable"),
              (std::vector<std::string> {"or: co", "cod", "ode = U", " Unavai", "ailable"}));
    EXPECT_EQ(gram_tokenize(factory, "手机ab微博"),
              (std::vector<std::string> {"手", "机", "微", "博"}));
    EXPECT_EQ(gram_tokenize(factory, ""), (std::vector<std::string> {}));
}

TEST(GramTokenizerTest, DenseModeAndAutoAlias) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "dense"}, {"min_gram", "3"}};
    factory.initialize(Settings(args));
    EXPECT_EQ(gram_tokenize(factory, "abcd"), (std::vector<std::string> {"abc", "bcd"}));
    NGramTokenizerFactory f2;
    std::unordered_map<std::string, std::string> a2 {{"mode", "auto"}};
    f2.initialize(Settings(a2));
    EXPECT_EQ(f2.gram_scheme()->mode, gram::GramMode::SPARSE); // P0：auto = sparse
}

// min/max_gram 缺省时的默认值来自 GramScheme 的成员初值（3/16），工厂不再注入副本。
TEST(GramTokenizerTest, GramDefaultsComeFromGramScheme) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "sparse"}};
    factory.initialize(Settings(args));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_EQ(factory.gram_scheme()->min_len, 3U);
    EXPECT_EQ(factory.gram_scheme()->max_len, 16U);
}

// lower_case=true 时折叠发生在切分之前（边界哈希也算在折叠后的字节上），因此大小写不同
// 的同一段文本必须切出完全相同的 gram 序列。
TEST(GramTokenizerTest, LowerCaseFoldsBeforeExtraction) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "sparse"}, {"lower_case", "true"}};
    factory.initialize(Settings(args));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_TRUE(factory.gram_scheme()->lower_case);
    EXPECT_EQ(gram_tokenize(factory, "Code = Unavailable"),
              gram_tokenize(factory, "code = unavailable"));
    // 换一段够长、确定能切出 gram 的输入，避免上面那条在两边都为空时空转。
    const auto mixed = gram_tokenize(factory, "RPC error: Code = Unavailable");
    EXPECT_FALSE(mixed.empty());
    EXPECT_EQ(mixed, gram_tokenize(factory, "rpc error: code = unavailable"));
}

TEST(GramTokenizerTest, GramModeSkipsLegacyMinMaxGapCheck) {
    // 现状 initialize 对 max_gram-min_gram>1 抛 INVALID_ARGUMENT（ngram_tokenizer_factory.cpp:29-36）；
    // gram 族在 mode 分支内直接 return，不受此限。
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {
            {"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "24"}};
    EXPECT_NO_THROW(factory.initialize(Settings(args)));
    std::unordered_map<std::string, std::string> bad {{"mode", "sparse"}, {"density", "2"}};
    EXPECT_THROW(NGramTokenizerFactory().initialize(Settings(bad)), Exception);
}

} // namespace doris::segment_v2
