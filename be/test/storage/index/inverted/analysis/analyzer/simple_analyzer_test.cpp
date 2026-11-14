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

#include <string>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"

namespace doris::segment_v2 {

// The builtin basic parser, lowercasing or not.
std::vector<std::string> tokenize(const std::string& s, bool lowercase = false) {
    auto analyzer = inverted_index::InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_BASIC, "",
            lowercase ? INVERTED_INDEX_PARSER_TRUE : INVERTED_INDEX_PARSER_FALSE, "");
    return inverted_index::collect_terms(*analyzer, s);
}

class BasicTokenizerTest : public ::testing::Test {};

TEST(BasicTokenizerTest, EnglishBasic1) {
    std::string text = "Hello World! This is a test.";
    auto tokens = tokenize(text, false);

    std::vector<std::string> expected = {"Hello", "World", "This", "is", "a", "test"};
    ASSERT_EQ(tokens, expected);
}

TEST(BasicTokenizerTest, EnglishBasic2) {
    std::string text = "Hello World! This is a test.";
    auto tokens = tokenize(text, true);

    std::vector<std::string> expected = {"hello", "world", "this", "is", "a", "test"};
    ASSERT_EQ(tokens, expected);
}

TEST(BasicTokenizerTest, EnglishLowercase) {
    std::string text = "Hello World";
    auto tokens = tokenize(text, true);

    std::vector<std::string> expected = {"hello", "world"};
    ASSERT_EQ(tokens, expected);
}

TEST(BasicTokenizerTest, ChineseBasic) {
    std::string text = "你好世界";
    auto tokens = tokenize(text);

    std::vector<std::string> expected = {"你", "好", "世", "界"};
    ASSERT_EQ(tokens, expected);
}

TEST(BasicTokenizerTest, MixedLanguage) {
    std::string text = "Hello你好World世界";
    auto tokens = tokenize(text, true);

    std::vector<std::string> expected = {"hello", "你", "好", "world", "世", "界"};
    ASSERT_EQ(tokens, expected);
}

TEST(BasicTokenizerTest, LongWordTruncation) {
    const int32_t MAX_LEN = 255;
    std::string longWord(MAX_LEN + 100, 'A');

    auto tokens = tokenize(longWord);
    ASSERT_EQ(tokens.size(), 1);
    ASSERT_EQ(tokens[0].size(), MAX_LEN);
}

TEST(BasicTokenizerTest, LargeDataset) {
    const std::string english = "The quick brown fox jumps over the lazy dog. ";
    const std::string chinese = "这是一个用于测试的分词样例。";
    const int32_t REPEAT = 5000;

    std::string largeText;
    for (int32_t i = 0; i < REPEAT; ++i) {
        largeText += english;
        largeText += chinese;
    }

    auto tokens = tokenize(largeText);

    const size_t englishPerIteration = 9;
    const size_t chinesePerIteration = 13;
    const size_t expectedTotal = REPEAT * (englishPerIteration + chinesePerIteration);

    ASSERT_EQ(tokens.size(), expectedTotal);

    ASSERT_EQ(tokens[0], "The");
    ASSERT_EQ(tokens[englishPerIteration], "这");
    ASSERT_EQ(tokens[englishPerIteration + 1], "是");
}

TEST(BasicTokenizerTest, InvalidUTF8) {
    std::string invalidText = "\x80\x81\xff";
    auto tokens = tokenize(invalidText);
    ASSERT_EQ(tokens.size(), 0);
}

TEST(BasicTokenizerTest, ConsecutiveNumbers) {
    const std::string input(300, '1');
    auto tokens = tokenize(input);
    EXPECT_EQ(tokens.size(), 1);

    EXPECT_EQ(tokens[0].size(), 255);
}

TEST(BasicTokenizerTest, EmojiHandling) {
    const std::string input = "😊😋";
    auto tokens = tokenize(input);
    EXPECT_EQ(tokens.size(), 0);
}

} // namespace doris::segment_v2
