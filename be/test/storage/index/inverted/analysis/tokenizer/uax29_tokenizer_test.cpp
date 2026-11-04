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

#include "storage/index/inverted/analysis/tokenizer/standard/uax29_tokenizer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/golden/golden_driver.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The standard parser as production builds it; unicode is the same analyzer.
TextAnalyzerPtr standard_analyzer(bool lowercase, bool stop_words) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_STANDARD,
                                                          "", lowercase ? "true" : "false",
                                                          stop_words ? "" : "none");
}

// CLucene's standard95 analyzer, which made the standard tokens before.
std::unique_ptr<golden::GoldenDriver> clucene_standard(bool lowercase, bool stop_words) {
    return golden::make_lucene_driver([lowercase, stop_words] {
        auto analyzer = std::make_shared<lucene::analysis::standard95::StandardAnalyzer>();
        analyzer->set_lowercase(lowercase);
        analyzer->set_stopwords(stop_words ? &lucene::analysis::standard95::stop_words : nullptr);
        return analyzer;
    });
}

std::unique_ptr<StandardTokenizer> custom_standard(int32_t max_token_length) {
    auto tokenizer = std::make_unique<StandardTokenizer>();
    tokenizer->set_max_token_length(max_token_length);
    return tokenizer;
}

// What the custom standard tokenizer made before: the tokens of StandardTokenizerImpl, a token
// longer than max_token_length dropped with its position left behind.
std::vector<CollectedToken> scanner_tokens(std::string_view text, int32_t max_token_length) {
    StandardTokenizerImpl scanner;
    scanner.set_buffer_size(max_token_length);
    scanner.yyreset(text);
    std::vector<CollectedToken> tokens;
    int32_t skipped = 0;
    while (scanner.get_next_token() != StandardTokenizerImpl::TokenType::EOF_TOKEN) {
        if (scanner.yylength() <= max_token_length) {
            tokens.push_back(
                    {.term = std::string(scanner.get_text()), .position_increment = skipped + 1});
            skipped = 0;
        } else {
            ++skipped;
        }
    }
    return tokens;
}

std::string bytes(std::initializer_list<int> values) {
    std::string text;
    for (int value : values) {
        text.push_back(static_cast<char>(value));
    }
    return text;
}

// Values that stress the DFA and the windows: any byte, the separators UAX#29 treats specially,
// the scripts with token types of their own, overlong forms, and long runs.
std::string random_value(std::mt19937_64& random) {
    static const std::vector<std::string> kPieces = {"a",
                                                     "B",
                                                     "z9",
                                                     "Q",
                                                     " ",
                                                     "'",
                                                     ".",
                                                     ",",
                                                     ":",
                                                     "_",
                                                     "-",
                                                     "\n",
                                                     "3",
                                                     "1.5",
                                                     "中",
                                                     "文",
                                                     "é",
                                                     "Ä",
                                                     "ひ",
                                                     "カ",
                                                     "한",
                                                     "ก",
                                                     "ข",
                                                     "́",
                                                     "‍",
                                                     "\U0001F600",
                                                     "️",
                                                     "\U0001F1EF",
                                                     "\xC0\x80",
                                                     "\xC1\x81",
                                                     "\xE0\x80\x80",
                                                     "\xF0\x80\x80\x80",
                                                     "\xED\xA0\x80"};
    const size_t size = random() % 600;
    std::string value;
    switch (random() % 4) {
    case 0:
        for (size_t i = 0; i < size; ++i) {
            value.push_back(static_cast<char>(random()));
        }
        break;
    case 1:
    case 2:
        while (value.size() < size) {
            value += kPieces[random() % (kPieces.size() - (random() % 8 == 0 ? 0 : 5))];
        }
        break;
    default:
        // Words long enough to meet the windows, with a multi-byte character now and then.
        while (value.size() < size) {
            value.append(random() % 300, static_cast<char>('a' + random() % 26));
            value += kPieces[random() % kPieces.size()];
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

TEST(Uax29TokenizerTest, MatchesCLuceneOnRandomValues) {
    for (bool lowercase : {true, false}) {
        for (bool stop_words : {true, false}) {
            SCOPED_TRACE(testing::Message() << lowercase << stop_words);
            auto oracle = clucene_standard(lowercase, stop_words);
            auto candidate = golden::make_text_driver(standard_analyzer(lowercase, stop_words));
            std::mt19937_64 random(20261104 + lowercase * 2 + stop_words);
            for (int round = 0; round < 8000; ++round) {
                expect_same_as_clucene(random_value(random), *candidate, *oracle);
            }
        }
    }
}

// A token is scanned while it is shorter than 255 bytes, so a character that starts inside the
// window may end past it, and a token longer than 255 bytes is dropped without a gap.
TEST(Uax29TokenizerTest, ScansAtMost255BytesFromATokenStart) {
    auto oracle = clucene_standard(true, true);
    auto candidate = golden::make_text_driver(standard_analyzer(true, true));
    const std::string a254(254, 'a');
    for (const std::string& value :
         {std::string(255, 'a'), std::string(256, 'a'), std::string(300, 'a'),
          std::string(510, 'a'), std::string(511, 'a'), a254 + "é", a254 + "é bc", a254 + "中",
          std::string(253, 'a') + "é", a254 + "\U0001F600", a254 + "'s",
          std::string(250, '1') + ".5.6.7", "x " + a254 + "é y"}) {
        expect_same_as_clucene(value, *candidate, *oracle);
    }
    EXPECT_EQ(collect_terms(*standard_analyzer(true, true), std::string(300, 'a')),
              (std::vector<std::string> {std::string(255, 'a'), std::string(45, 'a')}));
    // 254 letters and é are 256 bytes: dropped, and "bc" is one position after "x".
    const std::vector<CollectedToken> tokens =
            collect_checked(*standard_analyzer(true, true), "x " + a254 + "é bc");
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[1].term, "bc");
    EXPECT_EQ(tokens[1].position_increment, 1);
}

TEST(Uax29TokenizerTest, PassesOverlongFormsAndMakesNothingOfInvalidValues) {
    auto oracle = clucene_standard(true, true);
    auto candidate = golden::make_text_driver(standard_analyzer(true, true));
    for (const std::string& value :
         {"abc" + bytes({0xC0, 0x80}) + "def", "x" + bytes({0xC1, 0x81}) + "y",
          "Wo" + bytes({0xE0, 0x81, 0x81}) + "rd", "abc" + bytes({0xFF}),
          "abc" + bytes({0xED, 0xA0, 0x80}), "abc " + bytes({0xE4, 0xB8}),
          bytes({0xF0, 0x80, 0x80, 0x80}) + " tail"}) {
        expect_same_as_clucene(value, *candidate, *oracle);
    }
    EXPECT_TRUE(collect_terms(*standard_analyzer(true, true), "abc" + bytes({0xFF})).empty());
}

TEST(Uax29TokenizerTest, StopWordsAreTheLuceneSet) {
    const std::set<std::string_view> ours(standard_stop_words().begin(),
                                          standard_stop_words().end());
    const std::set<std::string_view> lucene(lucene::analysis::standard95::stop_words.begin(),
                                            lucene::analysis::standard95::stop_words.end());
    EXPECT_EQ(ours.size(), 33);
    EXPECT_EQ(ours, lucene);
}

TEST(Uax29TokenizerTest, OnlyTheStopWordsThemselvesAreStopWords) {
    const std::set<std::string> words(standard_stop_words().begin(), standard_stop_words().end());
    for (const std::string& word : words) {
        EXPECT_TRUE(is_standard_stop_word(word)) << word;
        std::vector<std::string> variants = {
                word + "s", "x" + word, word + word,
                std::string(1, static_cast<char>(word[0] - 32)) + word.substr(1),
                word + std::string(1, '\0')};
        if (word.size() > 1) {
            variants.push_back(word.substr(0, word.size() - 1));
            variants.push_back(word.substr(1));
        }
        for (const std::string& variant : variants) {
            EXPECT_EQ(is_standard_stop_word(variant), words.contains(variant))
                    << word << " -> " << variant;
        }
    }
    EXPECT_FALSE(is_standard_stop_word(""));
    EXPECT_FALSE(is_standard_stop_word("thereby"));
}

TEST(Uax29TokenizerTest, DropsStopWordsWithoutAGap) {
    const std::string value = "The quick fox and THE lazy dog is not here";
    const std::vector<CollectedToken> lowered =
            collect_checked(*standard_analyzer(true, true), value);
    std::vector<std::string> terms;
    for (const CollectedToken& token : lowered) {
        terms.push_back(token.term);
        EXPECT_EQ(token.position_increment, 1) << token.term;
    }
    EXPECT_EQ(terms, (std::vector<std::string> {"quick", "fox", "lazy", "dog", "here"}));
    // Without lowercase only the lowercase spellings are stop words.
    EXPECT_EQ(collect_terms(*standard_analyzer(false, true), value),
              (std::vector<std::string> {"The", "quick", "fox", "THE", "lazy", "dog", "here"}));
    EXPECT_EQ(collect_terms(*standard_analyzer(true, false), value).size(), 10);
}

TEST(Uax29TokenizerTest, LowercasesWordsAndPointsIntoTheValueOrItsCopy) {
    const std::string value = "MiXeD 42 ÄÖ 中文 WORD_x";
    EXPECT_EQ(collect_terms(*standard_analyzer(true, false), value),
              (std::vector<std::string> {"mixed", "42", "ÄÖ", "中", "文", "word_x"}));
    for (bool lowercase : {true, false}) {
        Uax29Tokenizer tokenizer(lowercase, false);
        tokenizer.reset(value);
        TokenBatch batch;
        ASSERT_TRUE(tokenizer.next_batch(batch));
        for (const TokenRef& token : batch.tokens()) {
            const bool inside = token.data >= value.data() &&
                                token.data + token.size <= value.data() + value.size();
            EXPECT_EQ(inside, !lowercase) << token.term();
        }
        EXPECT_EQ(tokenizer.reserved_bytes() > 0, lowercase);
    }
}

TEST(Uax29TokenizerTest, BatchesLoseNoTokenAroundTheBatchSize) {
    auto oracle = clucene_standard(true, true);
    auto candidate = golden::make_text_driver(standard_analyzer(true, true));
    for (size_t count : {size_t {255}, size_t {256}, size_t {257}, size_t {512}}) {
        std::string value;
        for (size_t i = 0; i < count; ++i) {
            value += "w" + std::to_string(i) + " the ";
        }
        const std::vector<CollectedToken> tokens =
                collect_checked(*standard_analyzer(true, true), value);
        ASSERT_EQ(tokens.size(), count);
        for (size_t i = 0; i < count; ++i) {
            EXPECT_EQ(tokens[i].term, "w" + std::to_string(i));
            EXPECT_EQ(tokens[i].position_increment, 1);
        }
        expect_same_as_clucene(value, *candidate, *oracle);
        EXPECT_EQ(collect_checked(*custom_standard(255), value), scanner_tokens(value, 255));
    }
}

// A valid value ending at a guard page: the decoder reads only the bytes of the last character.
TEST(Uax29TokenizerTest, ReadsNothingPastTheValue) {
    const size_t page_size = sysconf(_SC_PAGESIZE);
    void* mapping = mmap(nullptr, page_size * 3, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    auto* page = static_cast<char*>(mapping) + page_size;
    ASSERT_EQ(mprotect(page, page_size, PROT_READ | PROT_WRITE), 0);
    for (const std::string& tail : {std::string("K"), std::string("é"), std::string("中"),
                                    std::string("\U0001F600"), std::string("\xC1\x81")}) {
        for (size_t words = 0; words < 3; ++words) {
            std::string text;
            for (size_t i = 0; i < words; ++i) {
                text += "ab ";
            }
            text += tail;
            char* const value = page + page_size - text.size();
            std::memcpy(value, text.data(), text.size());
            const std::string_view view(value, text.size());
            Uax29Tokenizer builtin(true, true);
            EXPECT_EQ(collect_terms(builtin, view).size(), collect_terms(builtin, text).size());
            auto custom = custom_standard(255);
            EXPECT_EQ(collect_checked(*custom, view), scanner_tokens(text, 255));
        }
    }
    EXPECT_EQ(munmap(mapping, page_size * 3), 0);
}

TEST(Uax29TokenizerTest, KeepsTheStreamContract) {
    std::string many_batches;
    for (int i = 0; i < 2000; ++i) {
        many_batches += "Word" + std::to_string(i % 97) + " the ";
    }
    const std::vector<std::string> texts = {"",
                                            "Hello World",
                                            "a",
                                            std::string(300, 'Z'),
                                            "中文 mixed ÄÖÜ 123 \U0001F600",
                                            "bad" + bytes({0xFF}) + " tail",
                                            many_batches};
    for (bool lowercase : {true, false}) {
        const TextAnalyzerPtr analyzer = standard_analyzer(lowercase, true);
        expect_text_stream_contract([&] { return analyzer->create_stream(); }, texts);
    }
    for (int32_t length : {1, 4, 255}) {
        expect_text_stream_contract([length] { return custom_standard(length); }, texts);
    }
}

TEST(CustomStandardTokenizerTest, MatchesTheScannerOnRandomValues) {
    for (int32_t length : {1, 2, 3, 4, 5, 16, 255, 256, 1000, 16383}) {
        SCOPED_TRACE(length);
        auto tokenizer = custom_standard(length);
        std::mt19937_64 random(20261105 + length);
        for (int round = 0; round < 2000; ++round) {
            const std::string value = random_value(random);
            ASSERT_EQ(collect_checked(*tokenizer, value), scanner_tokens(value, length))
                    << round << ": " << value;
        }
    }
}

TEST(CustomStandardTokenizerTest, CutsWordsAtTheWindow) {
    EXPECT_EQ(collect_terms(*custom_standard(4), "abcdefghij"),
              (std::vector<std::string> {"abcd", "efgh", "ij"}));
    EXPECT_EQ(collect_terms(*custom_standard(4), "ab中cd"),
              (std::vector<std::string> {"ab", "中", "cd"}));
    for (const std::string& value : {std::string("abcdefghij"), std::string("ab中cd"),
                                     std::string("1.5.6.7.8 can't"), std::string(300, 'a')}) {
        for (int32_t length : {2, 3, 4, 5, 8}) {
            EXPECT_EQ(collect_checked(*custom_standard(length), value),
                      scanner_tokens(value, length))
                    << length << ": " << value;
        }
    }
}

// The scanner's buffer cannot take a character longer than max_token_length, and the value ends
// there.
TEST(CustomStandardTokenizerTest, EndsTheValueAtACharacterLongerThanTheWindow) {
    EXPECT_EQ(collect_terms(*custom_standard(2), "ab中cd"), (std::vector<std::string> {"ab"}));
    EXPECT_EQ(collect_terms(*custom_standard(1), "a中b"), (std::vector<std::string> {"a"}));
    EXPECT_EQ(collect_terms(*custom_standard(3), "a中b"),
              (std::vector<std::string> {"a", "中", "b"}));
    EXPECT_EQ(collect_terms(*custom_standard(3), "x \U0001F600 y"),
              (std::vector<std::string> {"x"}));
    for (int32_t length : {1, 2, 3, 4}) {
        for (const std::string& value : {std::string("ab中cd"), std::string("a中b"),
                                         std::string("x \U0001F600 y"), std::string("é")}) {
            EXPECT_EQ(collect_checked(*custom_standard(length), value),
                      scanner_tokens(value, length))
                    << length << ": " << value;
        }
    }
}

// An invalid value goes through the scanner, whose buffer moves at every refill: with a window of
// 8 bytes every word is read by a refill of its own, and views of the buffer would all show the
// last one.
TEST(CustomStandardTokenizerTest, CopiesTheTermsOfAnInvalidValue) {
    // The word before an invalid byte is lost, as it always was: the scanner meets the byte while
    // it reads the word and ends the value there.
    for (const auto& [value, expected] :
         {std::pair {"aaaa bbbb cccc" + bytes({0xFF}), std::vector<std::string> {"aaaa", "bbbb"}},
          std::pair {"aaaa bbbb cccc dd" + bytes({0xFF}),
                     std::vector<std::string> {"aaaa", "bbbb", "cccc"}}}) {
        auto tokenizer = custom_standard(8);
        tokenizer->reset(value);
        TokenBatch batch;
        ASSERT_TRUE(tokenizer->next_batch(batch));
        std::vector<std::string> terms;
        for (const TokenRef& token : batch.tokens()) {
            terms.emplace_back(token.term());
        }
        EXPECT_EQ(terms, expected);
        EXPECT_FALSE(tokenizer->next_batch(batch));
        EXPECT_EQ(collect_checked(*custom_standard(8), value), scanner_tokens(value, 8));
    }
}

TEST(CustomStandardTokenizerTest, TermsOfAValidValueAreViewsOfIt) {
    const std::string value = "Views of the value, 中文 too";
    auto tokenizer = custom_standard(255);
    tokenizer->reset(value);
    TokenBatch batch;
    ASSERT_TRUE(tokenizer->next_batch(batch));
    EXPECT_EQ(batch.size(), 7);
    for (const TokenRef& token : batch.tokens()) {
        EXPECT_GE(token.data, value.data());
        EXPECT_LE(token.data + token.size, value.data() + value.size());
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
