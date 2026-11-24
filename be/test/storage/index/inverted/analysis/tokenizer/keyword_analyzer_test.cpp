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

#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class KeywordTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(KeywordTokenizerFactory& factory, const std::string& text) {
    auto tokenizer = factory.create();
    return collect_terms(*tokenizer, text);
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

    const std::string text = std::string(8191, 'a') + "\xE5\x88\x98";
    const std::vector<std::string> tokens = collect_terms(*tokenizer, text);
    ASSERT_FALSE(tokens.empty());
    EXPECT_EQ(tokens[0], std::string(8191, 'a'));

    const std::string reset_text = "\xE5\x88\x98";
    const std::vector<std::string> reset_tokens = collect_terms(*tokenizer, reset_text);
    ASSERT_FALSE(reset_tokens.empty());
    EXPECT_EQ(reset_tokens[0], reset_text);
}

} // namespace doris::segment_v2
