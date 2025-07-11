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

#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2 {

using namespace inverted_index;

class KeywordTokenizerTest : public ::testing::Test {};

std::vector<std::string> tokenize(KeywordTokenizerFactory& factory, const std::string& text) {
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

} // namespace doris::segment_v2