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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/char/char_group_tokenizer_factory.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/setting.h"

namespace doris::segment_v2::inverted_index {

class CharGroupTokenizerTest : public ::testing::Test {
protected:
    std::vector<std::string> tokenize(CharGroupTokenizerFactory& factory, const std::string& text) {
        std::vector<std::string> tokens;
        auto tokenizer = factory.create();
        {
            lucene::util::SStringReader<char> reader;
            reader.init(text.data(), text.size(), false);
            tokenizer->set_reader(&reader);
            tokenizer->reset();

            Token t;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                tokens.emplace_back(term);
            }
        }
        return tokens;
    }
};

TEST_F(CharGroupTokenizerTest, DefaultConfiguration) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "Hello World 123!");
    std::vector<std::string> expected = {"Hello World 123!"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnSpace) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "Hello World\tTab\nNewline");
    std::vector<std::string> expected = {"Hello", "World", "Tab", "Newline"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnLetter) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[letter]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "Hello123World");
    std::vector<std::string> expected = {"123"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnDigit) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[digit]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "abc123def456");
    std::vector<std::string> expected = {"abc", "def"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnPunctuation) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[punctuation]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "Hello,World!Test?End");
    std::vector<std::string> expected = {"Hello", "World", "Test", "End"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnSymbol) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[symbol]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "Hello$World+Test@End");
    std::vector<std::string> expected = {"Hello", "World", "Test@End"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, TokenizeOnCustomChars) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[-], [_]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello-world_test");
    std::vector<std::string> expected = {"hello", "world", "test"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, EscapedChars) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[\\n], [\\t], [\\r]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello\nworld\ttest\rend");
    std::vector<std::string> expected = {"hello", "world", "test", "end"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, MaxTokenLength) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("max_token_length", "5");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello verylongword world");
    std::vector<std::string> expected = {"hello", " very", "longw", "ord w", "orld"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, CombinedConfiguration) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[-], [_]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello-world test_case, end!");
    std::vector<std::string> expected = {"hello", "world test", "case, end!"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, UnicodeCharacters) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "你好 世界 测试");
    std::vector<std::string> expected = {"你好", "世界", "测试"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, ChinesePunctuation) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[，], [。], [！], [？]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "你好，世界！测试？结束。");
    std::vector<std::string> expected = {"你好", "世界", "测试", "结束"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, EmptyString) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "");
    ASSERT_TRUE(tokens.empty());
}

TEST_F(CharGroupTokenizerTest, OnlyDelimiters) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace], [punctuation]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "   !!!   ???   ");
    ASSERT_TRUE(tokens.empty());
}

TEST_F(CharGroupTokenizerTest, SingleCharacter) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "a");
    std::vector<std::string> expected = {"a"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, LongText) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace]");
    factory.initialize(settings);

    std::string long_text;
    for (int i = 0; i < 1000; ++i) {
        long_text += "word" + std::to_string(i) + " ";
    }

    auto tokens = tokenize(factory, long_text);
    ASSERT_EQ(tokens.size(), 1000);
    ASSERT_EQ(tokens[0], "word0");
    ASSERT_EQ(tokens[999], "word999");
}

TEST_F(CharGroupTokenizerTest, ConsecutiveDelimiters) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[whitespace], [-]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello---world   test");
    std::vector<std::string> expected = {"hello", "world", "test"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, VeryLongToken) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("max_token_length", "10");
    factory.initialize(settings);

    std::string very_long_word(50, 'a');
    auto tokens = tokenize(factory, very_long_word);
    ASSERT_EQ(tokens.size(), 5);
    ASSERT_EQ(tokens[0].length(), 10);
    ASSERT_EQ(tokens[0], std::string(10, 'a'));
}

TEST_F(CharGroupTokenizerTest, SpecialUnicodeSymbols) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[symbol]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "hello©world®test™end");
    std::vector<std::string> expected = {"hello", "world", "test", "end"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, MathSymbols) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[symbol]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "a+b=c×d÷e∑f");
    std::vector<std::string> expected = {"a", "b", "c", "d", "e", "f"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, CurrencySymbols) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[symbol]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "100$200€300¥400");
    std::vector<std::string> expected = {"100", "200", "300", "400"};
    ASSERT_EQ(tokens, expected);
}

TEST_F(CharGroupTokenizerTest, CJKWithEnglishAndDigits) {
    CharGroupTokenizerFactory factory;
    Settings settings;
    settings.set("tokenize_on_chars", "[cjk]");
    factory.initialize(settings);

    auto tokens = tokenize(factory, "abc中文123");
    std::vector<std::string> expected = {"abc", "中", "文", "123"};
    ASSERT_EQ(tokens, expected);
}

} // namespace doris::segment_v2::inverted_index