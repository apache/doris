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

#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unicode/uchar.h>
#include <unicode/utf8.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The basic parser as production builds it.
TextAnalyzerPtr basic_analyzer(bool lowercase) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_BASIC, "",
                                                          lowercase ? "true" : "false", "");
}

bool reference_chinese(UChar32 c) {
    const auto in = [c](UChar32 lo, UChar32 hi) { return c >= lo && c <= hi; };
    return in(0x4E00, 0x9FFF) || in(0x3400, 0x4DBF) || in(0x20000, 0x2A6DF) ||
           in(0x2A700, 0x2EBEF) || in(0x30000, 0x3134A);
}

bool reference_alnum(uint8_t c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

// The run of letters and digits at `i`, which moves past it, cut to 255 bytes and lowered the
// way the lowercase filter lowered it.
std::string reference_run(const uint8_t* s, int32_t& i, int32_t length, bool lowercase) {
    const int32_t start = i;
    while (i < length && reference_alnum(s[i])) {
        ++i;
    }
    std::string term(reinterpret_cast<const char*>(s + start), std::min(i - start, 255));
    if (lowercase) {
        for (char& c : term) {
            c = static_cast<char>(c >= 'A' && c <= 'Z' ? c - 'A' + 'a' : c);
        }
    }
    return term;
}

// The tokens of the scalar loop the basic tokenizer had, lowercased the way the lowercase filter
// that followed it lowercased them.
std::vector<CollectedToken> reference_basic(std::string_view text, bool lowercase) {
    std::vector<CollectedToken> tokens;
    const auto* s = reinterpret_cast<const uint8_t*>(text.data());
    const auto length = static_cast<int32_t>(text.size());
    for (int32_t i = 0; i < length;) {
        if (reference_alnum(s[i])) {
            tokens.push_back(
                    {.term = reference_run(s, i, length, lowercase), .position_increment = 1});
        } else {
            const int32_t start = i;
            UChar32 c = U_UNASSIGNED;
            U8_NEXT(s, i, length, c);
            if (c >= 0 && reference_chinese(c)) {
                tokens.push_back(
                        {.term = std::string(reinterpret_cast<const char*>(s + start), i - start),
                         .position_increment = 1});
            }
        }
    }
    return tokens;
}

std::string random_value(std::mt19937_64& random) {
    static const std::vector<std::string> kPieces = {
            "a", "Bc",       "9",    " ",    "中", "文", "\xF0\xA0\x80\x80",
            "é", "\xE4\xB8", "\x80", "\xFF", "-",  "\n"};
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
        while (value.size() < size) {
            value.append(random() % 400, static_cast<char>('A' + random() % 26));
            value += random() % 2 == 0 ? "中" : " ";
        }
        break;
    }
    return value;
}

TEST(BasicTokenizerTest, MatchesTheScalarLoopOnRandomBytes) {
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        const TextAnalyzerPtr analyzer = basic_analyzer(lowercase);
        const std::unique_ptr<TextTokenStream> reused = analyzer->create_stream();
        std::mt19937_64 random(lowercase ? 31 : 32);
        for (int round = 0; round < 20000; ++round) {
            const std::string value = random_value(random);
            const std::vector<CollectedToken> expected = reference_basic(value, lowercase);
            ASSERT_EQ(collect_checked(*analyzer, value), expected);
            ASSERT_EQ(collect_checked(*reused, value), expected);
        }
    }
}

TEST(BasicTokenizerTest, CutsAtBlockBoundaries) {
    const TextAnalyzerPtr analyzer = basic_analyzer(true);
    const std::string spaces63(63, ' ');
    for (const std::string& value :
         {spaces63 + "中", spaces63 + "\xF0\xA0\x80\x80", spaces63 + "Abc", spaces63 + "abc中",
          std::string("abc中"), std::string(64, 'x'), std::string(62, ' ') + "中x",
          std::string(60, ' ') + "CrossThe64", std::string(128, 'Q') + "中" + std::string(127, 'R'),
          std::string(64, ' ') + "start"}) {
        EXPECT_EQ(collect_checked(*analyzer, value), reference_basic(value, true));
    }
    EXPECT_EQ(collect_terms(*analyzer, spaces63 + "中"), std::vector<std::string> {"中"});
    EXPECT_EQ(collect_terms(*analyzer, "abc中"), (std::vector<std::string> {"abc", "中"}));
}

TEST(BasicTokenizerTest, ALongRunIsCutToMaxWordLenAndTheRestDropped) {
    EXPECT_EQ(collect_terms(*basic_analyzer(true), std::string(300, 'a')),
              std::vector<std::string> {std::string(255, 'a')});
}

TEST(BasicTokenizerTest, BatchesLoseNoTokenAroundTheBatchSize) {
    const TextAnalyzerPtr analyzer = basic_analyzer(true);
    for (size_t count : {size_t {255}, size_t {256}, size_t {257}, size_t {512}}) {
        std::string value;
        for (size_t i = 0; i < count; ++i) {
            value += i % 2 == 0 ? "文" : "w" + std::to_string(i) + " ";
        }
        const std::vector<CollectedToken> tokens = collect_checked(*analyzer, value);
        ASSERT_EQ(tokens.size(), count);
        EXPECT_EQ(tokens, reference_basic(value, true));
    }
}

TEST(BasicTokenizerTest, WithoutLowercaseTheTermsAreViewsOfTheValue) {
    const std::string value = "Mixed CASE 中文 words 123";
    BasicTokenizer tokenizer(false);
    tokenizer.initialize();
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
    EXPECT_EQ(count, 6); // Mixed CASE 中 文 words 123
    EXPECT_EQ(tokenizer.reserved_bytes(), 0);
}

TEST(BasicTokenizerTest, ReadsNothingPastTheValue) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    void* mapping = mmap(nullptr, page_size * 3, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    auto* page = static_cast<char*>(mapping) + page_size;
    ASSERT_EQ(mprotect(page, page_size, PROT_READ | PROT_WRITE), 0);
    for (bool lowercase : {true, false}) {
        BasicTokenizer tokenizer(lowercase);
        tokenizer.initialize();
        for (size_t size = 1; size <= 200; ++size) {
            char* const value = page + page_size - size;
            // A cut-short 中 at the very end: decoding it must stop at the end of the value.
            std::memset(value, 'K', size);
            value[size - 1] = '\xE4';
            const std::string_view text(value, size);
            EXPECT_EQ(collect_checked(tokenizer, text), reference_basic(text, lowercase));
        }
    }
    EXPECT_EQ(munmap(mapping, page_size * 3), 0);
}

TEST(BasicTokenizerTest, ExtraCharsKeepTheScalarLoop) {
    BasicTokenizer tokenizer;
    tokenizer.initialize("-_");
    EXPECT_EQ(collect_terms(tokenizer, "a-b_c 中"),
              (std::vector<std::string> {"a", "-", "b", "_", "c", "中"}));
    expect_text_stream_contract(
            [] {
                auto stream = std::make_unique<BasicTokenizer>();
                stream->initialize("-");
                return stream;
            },
            {"", "x-y", std::string(300, 'b') + "-", std::string(600, '-')});
}

TEST(BasicTokenizerTest, KeepsTheStreamContract) {
    for (bool lowercase : {true, false}) {
        const TextAnalyzerPtr analyzer = basic_analyzer(lowercase);
        std::string many_batches;
        for (int i = 0; i < 2000; ++i) {
            many_batches += "Word" + std::to_string(i % 97) + "中";
        }
        expect_text_stream_contract([&] { return analyzer->create_stream(); },
                                    {"", "Hello World", "a", std::string(300, 'Z'),
                                     "中文 mixed ÄÖÜ 123", many_batches});
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
