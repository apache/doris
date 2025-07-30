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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer.h"

#include <gtest/gtest.h>

#include <unordered_map>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/edge_ngram_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class EdgeNGramTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(EdgeNGramTokenizerFactory& factory, const std::string& text) {
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

TEST(EdgeNGramTokenizerTest, DefaultMinMaxValues) {
    EdgeNGramTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "hello");

    std::vector<std::string> expected {"h", "he"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, CustomMinMaxValues) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "3";
    args["max_gram"] = "5";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "photography");

    std::vector<std::string> expected {"pho", "phot", "photo"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, MinGramLargerThanInput) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "5";
    args["max_gram"] = "10";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "test");

    ASSERT_TRUE(tokens.empty());
}

TEST(EdgeNGramTokenizerTest, MixedTokenChars) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "2";
    args["max_gram"] = "3";
    args["token_chars"] = "letter, digit";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "a1b2c3");

    std::vector<std::string> expected {"a1", "a1b"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, CustomTokenChars) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "1";
    args["max_gram"] = "2";
    args["token_chars"] = "custom";
    args["custom_token_chars"] = "*";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "a*b-c+d");

    std::vector<std::string> expected {"*"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, InvalidTokenCharsType) {
    bool exception_thrown = false;
    try {
        EdgeNGramTokenizerFactory factory;
        std::unordered_map<std::string, std::string> args;
        args["token_chars"] = "invalid";
        Settings settings(args);
        factory.initialize(settings);
    } catch (...) {
        exception_thrown = true;
    }
    ASSERT_TRUE(exception_thrown);
}

TEST(EdgeNGramTokenizerTest, EmptyInputString) {
    EdgeNGramTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "");

    ASSERT_TRUE(tokens.empty());
}

TEST(EdgeNGramTokenizerTest, WhitespaceDelimiters) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["token_chars"] = "letter";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "foo bar");

    std::vector<std::string> expected {"f", "fo", "b", "ba"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, MaxGramCutoff) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["max_gram"] = "4";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "abcdef");

    std::vector<std::string> expected {"a", "ab", "abc", "abcd"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, CJKCharactersHandling) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "1";
    args["max_gram"] = "2";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "你好世界");

    std::vector<std::string> expected {"你", "你好"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, CombinedTokenizationFlow) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "2";
    args["max_gram"] = "6";
    args["token_chars"] = "letter, punctuation";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "hello,world!");

    std::vector<std::string> expected {"he", "hel", "hell", "hello", "hello,"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, NumbersOnlyTokenization) {
    EdgeNGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["max_gram"] = "5";
    args["token_chars"] = "digit";
    Settings settings(args);
    factory.initialize(settings);
    auto tokenizer = factory.create();
    auto tokens = tokenize(factory, "2024year");

    std::vector<std::string> expected {"2", "20", "202", "2024"};
    ASSERT_EQ(tokens, expected);
}

TEST(EdgeNGramTokenizerTest, CustomSymbolTokenization) {
    EdgeNGramTokenizerFactory factory;
    {
        std::unordered_map<std::string, std::string> args;
        args["max_gram"] = "4";
        args["token_chars"] = "custom, symbol";
        args["custom_token_chars"] = "+-_";
        Settings settings(args);
        factory.initialize(settings);
        auto tokenizer = factory.create();
        auto tokens = tokenize(factory, "C++_123");
        std::vector<std::string> expected {"+", "++", "++_"};
        ASSERT_EQ(tokens, expected);
    }

    {
        std::unordered_map<std::string, std::string> args;
        args["max_gram"] = "4";
        args["token_chars"] = "custom, symbol";
        args["custom_token_chars"] = "+-_";
        Settings settings(args);
        factory.initialize(settings);
        auto tokenizer = factory.create();
        auto tokens = tokenize(factory, "Biplab Mitra");
        std::vector<std::string> expected;
        ASSERT_EQ(tokens, expected);
    }
}

} // namespace doris::segment_v2