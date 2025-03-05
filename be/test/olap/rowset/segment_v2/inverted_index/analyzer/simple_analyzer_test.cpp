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

#include "olap/rowset/segment_v2/inverted_index/analyzer/simple/simple_tokenizer.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

std::vector<std::string> tokenize(const std::string& text, bool lowercase = false) {
    try {
        lucene::util::SStringReader<char> reader;
        reader.init(s.data(), s.size(), false);

        std::unique_ptr<SimpleTokenizer> tokenizer;
        tokenizer.reset((SimpleTokenizer*)analyzer.tokenStream(L"", &reader));

        Token t;
        while (tokenizer->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            datas.emplace_back(term);
        }
    } catch (CLuceneError& e) {
        std::cout << e.what() << std::endl;
        throw;
    }
}

TEST(SimpleTokenizerTest, EnglishBasic) {
    std::string text = "Hello World! This is a test.";
    auto tokens = tokenize(text);

    std::vector<std::string> expected = {"Hello", "World", "This", "is", "a", "test"};
    ASSERT_EQ(tokens, expected);
}

TEST(SimpleTokenizerTest, EnglishLowercase) {
    std::string text = "Hello World";
    auto tokens = tokenize(text, true);

    std::vector<std::string> expected = {"hello", "world"};
    ASSERT_EQ(tokens, expected);
}

TEST(SimpleTokenizerTest, ChineseBasic) {
    std::string text = "你好世界";
    auto tokens = tokenize(text);

    std::vector<std::string> expected = {"你", "好", "世", "界"};
    ASSERT_EQ(tokens, expected);
}

TEST(SimpleTokenizerTest, MixedLanguage) {
    std::string text = "Hello你好World世界";
    auto tokens = tokenize(text, true);

    std::vector<std::string> expected = {"hello", "你", "好", "world", "世", "界"};
    ASSERT_EQ(tokens, expected);
}

TEST(SimpleTokenizerTest, LongWordTruncation) {
    const int32_t MAX_LEN = 255;
    std::string longWord(MAX_LEN + 100, 'A');

    auto tokens = tokenize(longWord);
    ASSERT_EQ(tokens.size(), 1);
    ASSERT_EQ(tokens[0].size(), MAX_LEN);
}

TEST(SimpleTokenizerTest, LargeDataset) {
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
    const size_t chinesePerIteration = 10;
    const size_t expectedTotal = REPEAT * (englishPerIteration + chinesePerIteration);

    ASSERT_EQ(tokens.size(), expectedTotal);

    ASSERT_EQ(tokens[0], "The");
    ASSERT_EQ(tokens[englishPerIteration], "这");
    ASSERT_EQ(tokens[englishPerIteration + 1], "是");
}

TEST(SimpleTokenizerTest, InvalidUTF8) {
    std::string invalidText = "\x80\x81\xff";

    try {
        auto tokens = tokenize(invalidText);
        FAIL() << "Expected CL_ERR_Runtime exception";
    } catch (CLuceneError& e) {
        ASSERT_STREQ(e.what(), "invalid UTF-8 sequence");
    }
}

TEST(SimpleTokenizerTest, ConsecutiveNumbers) {
    const std::string input(300, '1');
    StringReader* reader = new StringReader(input);
    SimpleTokenizer tokenizer;

    tokenizer.reset(reader);
    Token* token = tokenizer.next(nullptr);

    ASSERT_NE(token, nullptr);
    EXPECT_EQ(std::string(token->text(), token->length()), input);
    delete token;

    token = tokenizer.next(nullptr);
    EXPECT_EQ(token, nullptr);
}

TEST(SimpleTokenizerTest, EmojiHandling) {
    const std::string input = "\xE2\x98\x8A\xE2\x98\x8B"; // 😊😋
    StringReader* reader = new StringReader(input);
    SimpleTokenizer tokenizer;

    tokenizer.reset(reader);
    Token* token = tokenizer.next(nullptr);

    ASSERT_NE(token, nullptr);
    // 😊 (U+1F60A)
    EXPECT_EQ(std::string(token->text(), token->length()), "\xE2\x98\x8A");
    delete token;

    token = tokenizer.next(nullptr);
    ASSERT_NE(token, nullptr);
    // 😋 (U+1F60B)
    EXPECT_EQ(std::string(token->text(), token->length()), "\xE2\x98\x8B");
    delete token;

    token = tokenizer.next(nullptr);
    EXPECT_EQ(token, nullptr);
}

} // namespace doris::segment_v2
