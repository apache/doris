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

// Ruling R8: the BE UT binary uses unity builds, and ngram_tokenizer_test.cpp already defines a
// tokenize(NGramTokenizerFactory&, const std::string&) with the same name and signature in this
// namespace, so reusing the name would be a redefinition error in the merged translation unit.
// The helper here lives in a file-private named namespace and is renamed to gram_tokenize, which
// avoids the clash twice over.
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
    // Same golden as GramExtractorTest.SparseGoldenFromPrototype (in start-position order)
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
    EXPECT_EQ(f2.gram_scheme()->mode, gram::GramMode::SPARSE); // P0: auto = sparse
}

// When min/max_gram are absent the defaults come from GramScheme's member initializers (3/16);
// the factory no longer injects a copy of its own.
TEST(GramTokenizerTest, GramDefaultsComeFromGramScheme) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "sparse"}};
    factory.initialize(Settings(args));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_EQ(factory.gram_scheme()->min_len, 3U);
    EXPECT_EQ(factory.gram_scheme()->max_len, 16U);
}

// With lower_case=true the folding happens before the split (the boundary hash is computed over
// the folded bytes as well), so the same text in different cases must yield exactly the same gram
// sequence.
TEST(GramTokenizerTest, LowerCaseFoldsBeforeExtraction) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "sparse"}, {"lower_case", "true"}};
    factory.initialize(Settings(args));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_TRUE(factory.gram_scheme()->lower_case);
    EXPECT_EQ(gram_tokenize(factory, "Code = Unavailable"),
              gram_tokenize(factory, "code = unavailable"));
    // Use a longer input that is guaranteed to yield grams, so the check above cannot pass
    // vacuously with both sides empty.
    const auto mixed = gram_tokenize(factory, "RPC error: Code = Unavailable");
    EXPECT_FALSE(mixed.empty());
    EXPECT_EQ(mixed, gram_tokenize(factory, "rpc error: code = unavailable"));
}

TEST(GramTokenizerTest, GramModeSkipsLegacyMinMaxGapCheck) {
    // Today initialize throws INVALID_ARGUMENT for max_gram-min_gram>1
    // (ngram_tokenizer_factory.cpp:29-36); the gram family returns from inside the mode branch and
    // is not subject to that limit.
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {
            {"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "24"}};
    EXPECT_NO_THROW(factory.initialize(Settings(args)));
    std::unordered_map<std::string, std::string> bad {{"mode", "sparse"}, {"density", "2"}};
    EXPECT_THROW(NGramTokenizerFactory().initialize(Settings(bad)), Exception);
}

} // namespace doris::segment_v2
