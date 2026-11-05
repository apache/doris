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

#include "storage/index/inverted/analysis/tokenizer/english/english_tokenizer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/golden/golden_driver.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The english parser as production builds it.
TextAnalyzerPtr english_analyzer(bool lowercase) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_ENGLISH,
                                                          "", lowercase ? "true" : "false", "");
}

// CLucene's SimpleAnalyzer<char>, which made the english tokens before.
std::unique_ptr<golden::GoldenDriver> clucene_english(bool lowercase) {
    return golden::make_lucene_driver([lowercase] {
        auto analyzer = std::make_shared<lucene::analysis::SimpleAnalyzer<char>>();
        analyzer->set_lowercase(lowercase);
        return analyzer;
    });
}

// Values that stress the cuts: any byte, letters and digits in long runs, UTF-8 between them.
std::string random_value(std::mt19937_64& random) {
    static const std::vector<std::string> kPieces = {"a", "B",  "z9", "Q", " ",
                                                     "-", "\n", "中", "é", "\xF0\x9F\x98\x80"};
    const size_t size = random() % 700;
    std::string value;
    switch (random() % 3) {
    case 0:
        for (size_t i = 0; i < size; ++i) {
            value.push_back(static_cast<char>(random()));
        }
        break;
    case 1:
        while (value.size() < size) {
            value += kPieces[random() % kPieces.size()];
        }
        break;
    default:
        // Runs long enough to be cut at MAX_WORD_LEN, with a separator now and then.
        while (value.size() < size) {
            value.append(random() % 400, static_cast<char>('a' + random() % 26));
            value.push_back(random() % 2 == 0 ? ' ' : static_cast<char>(0x80 + random() % 128));
        }
        break;
    }
    return value;
}

void expect_same_as_clucene(const std::string& value, golden::GoldenDriver& candidate,
                            golden::GoldenDriver& oracle) {
    ASSERT_EQ(candidate.fresh(value), oracle.fresh(value))
            << golden::format_line(oracle.fresh(value));
    ASSERT_EQ(candidate.reused(value), oracle.reused(value));
}

TEST(EnglishTokenizerTest, MatchesSimpleTokenizerOnRandomBytes) {
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        auto oracle = clucene_english(lowercase);
        auto candidate = golden::make_text_driver(english_analyzer(lowercase));
        std::mt19937_64 random(lowercase ? 20261103 : 20261104);
        for (int round = 0; round < 20000; ++round) {
            expect_same_as_clucene(random_value(random), *candidate, *oracle);
        }
    }
}

TEST(EnglishTokenizerTest, CutsAtBlockBoundaries) {
    auto oracle = clucene_english(true);
    auto candidate = golden::make_text_driver(english_analyzer(true));
    const std::string spaces63(63, ' ');
    for (const std::string& value :
         {spaces63 + "中", spaces63 + "Abc", spaces63 + "abc中", std::string("abc中"),
          std::string(64, 'x'), std::string(64, 'x') + "Y", std::string(60, ' ') + "CrossThe64",
          std::string(128, 'q') + " " + std::string(127, 'r'), std::string(63, 'a') + " b",
          std::string(64, ' ') + "start", std::string(255, 'a'), std::string(256, 'a'),
          std::string(510, 'a'), std::string(511, 'a')}) {
        expect_same_as_clucene(value, *candidate, *oracle);
    }
    EXPECT_EQ(collect_terms(*english_analyzer(true), spaces63 + "Abc"),
              std::vector<std::string> {"abc"});
}

TEST(EnglishTokenizerTest, ALongRunIsCutEveryMaxWordLenBytes) {
    const std::vector<std::string> terms =
            collect_terms(*english_analyzer(true), std::string(300, 'a'));
    EXPECT_EQ(terms, (std::vector<std::string> {std::string(255, 'a'), std::string(45, 'a')}));
}

TEST(EnglishTokenizerTest, BatchesLoseNoTokenAroundTheBatchSize) {
    auto oracle = clucene_english(true);
    auto candidate = golden::make_text_driver(english_analyzer(true));
    for (size_t count : {size_t {255}, size_t {256}, size_t {257}, size_t {512}}) {
        std::string value;
        for (size_t i = 0; i < count; ++i) {
            value += "w" + std::to_string(i) + " ";
        }
        const std::vector<CollectedToken> tokens = collect_checked(*english_analyzer(true), value);
        ASSERT_EQ(tokens.size(), count);
        for (size_t i = 0; i < count; ++i) {
            EXPECT_EQ(tokens[i].term, "w" + std::to_string(i));
            EXPECT_EQ(tokens[i].position_increment, 1);
        }
        expect_same_as_clucene(value, *candidate, *oracle);
    }
}

TEST(EnglishTokenizerTest, WithoutLowercaseTheTermsAreViewsOfTheValue) {
    const std::string value = "Mixed CASE words 123 and Ünïcode-ish ASCII";
    EnglishTokenizer tokenizer(false);
    tokenizer.reset(value);
    TokenBatch batch;
    size_t count = 0;
    while (tokenizer.next_batch(batch)) {
        for (const TokenRef& token : batch.tokens()) {
            EXPECT_GE(token.data, value.data());
            EXPECT_LE(token.data + token.size, value.data() + value.size());
            ++count;
        }
    }
    EXPECT_EQ(count, 9); // Mixed CASE words 123 and n code ish ASCII
    EXPECT_EQ(tokenizer.reserved_bytes(), 0);
}

TEST(EnglishTokenizerTest, ReadsNothingPastTheValue) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    void* mapping = mmap(nullptr, page_size * 3, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    auto* page = static_cast<char*>(mapping) + page_size;
    ASSERT_EQ(mprotect(page, page_size, PROT_READ | PROT_WRITE), 0);
    for (bool lowercase : {true, false}) {
        EnglishTokenizer tokenizer(lowercase);
        for (size_t size = 1; size <= 200; ++size) {
            char* const value = page + page_size - size;
            std::memset(value, 'K', size);
            const std::vector<std::string> terms =
                    collect_terms(tokenizer, std::string_view(value, size));
            ASSERT_EQ(terms.size(), (size + MAX_WORD_LEN - 1) / MAX_WORD_LEN);
            EXPECT_EQ(terms[0][0], lowercase ? 'k' : 'K');
        }
    }
    EXPECT_EQ(munmap(mapping, page_size * 3), 0);
}

TEST(EnglishTokenizerTest, KeepsTheStreamContract) {
    for (bool lowercase : {true, false}) {
        const TextAnalyzerPtr analyzer = english_analyzer(lowercase);
        std::string many_batches;
        for (int i = 0; i < 2000; ++i) {
            many_batches += "Word" + std::to_string(i % 97) + " ";
        }
        expect_text_stream_contract([&] { return analyzer->create_stream(); },
                                    {"", "Hello World", "a", std::string(300, 'Z'),
                                     "中文 mixed ÄÖÜ 123", many_batches});
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
