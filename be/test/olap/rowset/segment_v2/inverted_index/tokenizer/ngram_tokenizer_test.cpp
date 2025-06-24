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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/ngram_tokenizer.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <unordered_map>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/ngram/ngram_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class NGramTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(NGramTokenizerFactory& factory, const std::string& text) {
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

TEST(NGramTokenizerTest, DefaultMinMaxValues) {
    NGramTokenizerFactory factory;
    Settings settings;
    factory.initialize(settings);
    auto tokens = tokenize(factory, "hello");

    std::vector<std::string> expected {"h", "he", "e", "el", "l", "ll", "l", "lo", "o"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, ValidMinMaxDifference) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "2";
    args["max_gram"] = "3";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "apple");

    std::vector<std::string> expected {"ap", "app", "pp", "ppl", "pl", "ple", "le"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, InvalidMinMaxDifference) {
    bool exception_thrown = false;
    try {
        NGramTokenizerFactory factory;
        std::unordered_map<std::string, std::string> args;
        args["min_gram"] = "1";
        args["max_gram"] = "3";
        Settings settings(args);
        factory.initialize(settings);
    } catch (...) {
        exception_thrown = true;
    }
    ASSERT_TRUE(exception_thrown);
}

TEST(NGramTokenizerTest, SymbolCharactersHandling) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["token_chars"] = "symbol";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "a$c#d");

    std::vector<std::string> expected {"$"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, MixedCharacterTypes) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "2";
    args["max_gram"] = "2";
    args["token_chars"] = "letter, digit";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "vm01");

    std::vector<std::string> expected {"vm", "m0", "01"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, FullTokenizationFlow) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "3";
    args["max_gram"] = "3";
    args["token_chars"] = "letter";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "big_data");

    std::vector<std::string> expected {"big", "dat", "ata"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, EmptyTokenCharsSettings) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["token_chars"] = "";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "a1b2c3");

    ASSERT_EQ(tokens.size(), 11);
}

TEST(NGramTokenizerTest, CJKMultiCharacterHandling) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "2";
    args["max_gram"] = "2";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "日本語");

    std::vector<std::string> expected {"日本", "本語"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, MaxGramExceedsInputLength) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "3";
    args["max_gram"] = "4";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "hi");

    ASSERT_TRUE(tokens.empty());
}

TEST(NGramTokenizerTest, CustomTokenCharsWithSpecialSymbols) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["token_chars"] = "custom";
    args["custom_token_chars"] = "éè";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "caféè");

    std::vector<std::string> expected {"é", "éè", "è"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, PunctuationHandling) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["token_chars"] = "punctuation";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "Hello, world!");

    std::vector<std::string> expected {",", "!"};
    ASSERT_EQ(tokens, expected);
}

TEST(NGramTokenizerTest, WhitespaceTokenization) {
    NGramTokenizerFactory factory;
    std::unordered_map<std::string, std::string> args;
    args["min_gram"] = "1";
    args["max_gram"] = "1";
    args["token_chars"] = "whitespace";
    Settings settings(args);
    factory.initialize(settings);
    auto tokens = tokenize(factory, "a b  c   d");

    std::vector<std::string> expected {" ", " ", " ", " ", " ", " "};
    ASSERT_EQ(tokens, expected);
}

} // namespace doris::segment_v2