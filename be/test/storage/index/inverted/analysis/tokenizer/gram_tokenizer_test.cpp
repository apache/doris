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

#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/setting.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer_factory.h"

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
    // Non-ASCII text is not indexed: it yields no terms and separates the ASCII around it,
    // and "ab" is shorter than min_gram, so this row contributes nothing at all.
    EXPECT_EQ(gram_tokenize(factory, "手机ab微博"), (std::vector<std::string> {}));
    // The CJK is adjacent to the ASCII on purpose: the ASCII segment is then byte-for-byte
    // the string above, so this pins that skipping the non-ASCII leaves the segment's own
    // gram boundaries untouched rather than shifting them.
    EXPECT_EQ(gram_tokenize(factory, "手机rpc error: code = Unavailable"),
              (std::vector<std::string> {"or: co", "cod", "ode = U", " Unavai", "ailable"}))
            << "the ASCII part of a mixed row indexes exactly as it would alone";
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

// When min/max_gram are absent the defaults come from GramScheme's member initializers (3/4);
// the factory no longer injects a copy of its own.
TEST(GramTokenizerTest, GramDefaultsComeFromGramScheme) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {{"mode", "sparse"}};
    factory.initialize(Settings(args));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    EXPECT_EQ(factory.gram_scheme()->min_len, 3U);
    EXPECT_EQ(factory.gram_scheme()->max_len, 4U);
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
    // The legacy path limits max_gram - min_gram, while the gram family returns before that check.
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args {
            {"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "24"}};
    EXPECT_NO_THROW(factory.initialize(Settings(args)));
    std::unordered_map<std::string, std::string> bad {{"mode", "sparse"}, {"density", "2"}};
    EXPECT_THROW(NGramTokenizerFactory().initialize(Settings(bad)), Exception);
}

// The gram-family analyzer streams with the native GramTokenizer: it gives the grams the tokenizer
// of a CLucene chain gives, each one position after the last, and keeps the stream contract.
TEST(GramTokenizerTest, NativeStreamMatchesTheCLuceneTokenizer) {
    std::string log_line;
    while (log_line.size() < 40000) {
        log_line += "2026-09-06 INFO rpc request completed service=Frontend latency_ms=17 ";
    }
    const std::vector<std::string> rows = {"rpc error: code = Unavailable", "手机ab微博", "",
                                           "ABC def GHI", log_line};
    for (const std::unordered_map<std::string, std::string>& args :
         {std::unordered_map<std::string, std::string> {
                  {"mode", "sparse"}, {"min_gram", "3"}, {"max_gram", "16"}, {"density", "0.25"}},
          std::unordered_map<std::string, std::string> {{"mode", "dense"},
                                                        {"min_gram", "2"},
                                                        {"max_gram", "4"},
                                                        {"lower_case", "true"}}}) {
        NGramTokenizerFactory factory;
        factory.initialize(Settings(args));
        ASSERT_TRUE(factory.gram_scheme().has_value());
        const GramTextAnalyzer analyzer(*factory.gram_scheme());
        for (const std::string& row : rows) {
            const auto stream = analyzer.create_stream();
            std::vector<std::string> native;
            for (const CollectedToken& token : collect_checked(*stream, row)) {
                EXPECT_EQ(token.position_increment, 1);
                native.push_back(token.term);
            }
            EXPECT_EQ(native, gram_tokenize(factory, row)) << row.substr(0, 40);
        }
        expect_text_stream_contract([&analyzer] { return analyzer.create_stream(); }, rows);
    }
}

// A long row's scratch stays until the next row, which releases each buffer above the high water
// mark.
TEST(GramTokenizerTest, NativeStreamReleasesALongRowAtTheNextRow) {
    NGramTokenizerFactory factory;
    factory.initialize(Settings(std::unordered_map<std::string, std::string> {
            {"mode", "dense"}, {"min_gram", "3"}, {"max_gram", "3"}, {"lower_case", "true"}}));
    ASSERT_TRUE(factory.gram_scheme().has_value());
    GramTokenizer tokenizer(*factory.gram_scheme());
    std::string long_row;
    while (long_row.size() < (1U << 20)) {
        long_row += "Abcdefghij ";
    }
    TokenBatch batch;
    tokenizer.reset(long_row);
    while (tokenizer.next_batch(batch)) {
    }
    EXPECT_GT(tokenizer.reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    tokenizer.reset("short");
    EXPECT_LE(tokenizer.reserved_bytes(), 4 * ANALYZER_SCRATCH_HIGH_WATER_BYTES);
}

} // namespace doris::segment_v2
