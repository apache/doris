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

#include <unordered_map>

#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class KeywordTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(KeywordTokenizerFactory& factory, const std::string& text) {
    std::vector<std::string> tokens;
    auto tokenizer = factory.create();
    {
        ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
        reader->init(text.data(), text.size(), false);
        tokenizer->set_reader(reader);
        tokenizer->reset();

        Token t;
        while (tokenizer->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            tokens.emplace_back(term);
        }
    }
    return tokens;
}

TEST(KeywordTokenizerTest, BasicTokenization) {
    std::unordered_map<std::string, std::string> args;
    args["buffer_size"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0], "ApacheDoris");
}

TEST(KeywordTokenizerTest, BufferSizeLimit) {
    std::unordered_map<std::string, std::string> args;
    args["buffer_size"] = "5";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0], "ApacheDoris");
}

TEST(KeywordTokenizerTest, InvalidBufferSize) {
    bool exception_thrown = false;
    try {
        std::unordered_map<std::string, std::string> args;
        args["buffer_size"] = "-1";
        Settings settings(args);
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
        auto tokenizer = factory.create();
    } catch (...) {
        exception_thrown = true;
    }
    EXPECT_TRUE(exception_thrown);

    exception_thrown = false;
    try {
        std::unordered_map<std::string, std::string> args;
        args["buffer_size"] = "100000";
        Settings settings(args);
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
        auto tokenizer = factory.create();
    } catch (...) {
        exception_thrown = true;
    }
    EXPECT_TRUE(exception_thrown);
}

TEST(KeywordTokenizerTest, FactoryCreatesValidTokenizer) {
    std::unordered_map<std::string, std::string> args;
    args["buffer_size"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, "ApacheDoris");

    EXPECT_EQ(tokens[0].size(), 11);
}

TEST(KeywordTokenizerTest, EmptyInput) {
    std::unordered_map<std::string, std::string> args;
    args["buffer_size"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    auto tokens = tokenize(factory, " ");

    EXPECT_EQ(tokens.size(), 1);
}

TEST(KeywordTokenizerTest, LongInput) {
    std::unordered_map<std::string, std::string> args;
    args["buffer_size"] = "256";
    Settings settings(args);
    KeywordTokenizerFactory factory;
    factory.initialize(settings);

    std::string s;
    for (int32_t i = 0; i < 8192; i++) {
        s += "a";
    }
    auto tokens = tokenize(factory, s);
    EXPECT_EQ(tokens[0].size(), 8192);

    std::string s1;
    for (int32_t i = 0; i < 8193; i++) {
        s1 += "a";
    }
    auto tokens1 = tokenize(factory, s1);
    EXPECT_EQ(tokens1[0].size(), 8192);
}

TEST(KeywordTokenizerTest, LongInputStopsBeforeMultibyteRune) {
    KeywordTokenizerFactory factory;
    factory.initialize({});
    auto tokenizer = factory.create();
    tokenizer->set_source_byte_offsets_enabled(true);

    const std::string text = std::string(8191, 'a') + "\xE5\x88\x98";
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    tokenizer->set_reader(reader);
    tokenizer->reset();

    Token token;
    ASSERT_NE(tokenizer->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
              std::string(8191, 'a'));
    EXPECT_EQ(token.startOffset(), 0);
    EXPECT_EQ(token.endOffset(), 8191);
    ASSERT_FALSE(tokenizer->get_source_byte_offsets().empty());
    EXPECT_EQ(tokenizer->get_source_byte_offsets().back(), 8191);

    const std::string reset_text = "\xE5\x88\x98";
    reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reader);
    tokenizer->reset();
    ASSERT_NE(tokenizer->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), reset_text);
    EXPECT_EQ(token.startOffset(), 0);
    EXPECT_EQ(token.endOffset(), 3);
}

} // namespace doris::segment_v2
