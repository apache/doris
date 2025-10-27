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

#include "olap/rowset/segment_v2/inverted_index/analysis_factory_mgr.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/empty_char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/empty_token_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/token_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/empty/empty_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class AnalysisFactoryMgrTest : public ::testing::Test {
protected:
    void SetUp() override { AnalysisFactoryMgr::instance().initialise(); }

    void TearDown() override {}
};

TEST_F(AnalysisFactoryMgrTest, CreateBuiltInFactories) {
    Settings empty_settings;

    auto tokenizer_factory =
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("standard", empty_settings);
    ASSERT_NE(tokenizer_factory, nullptr);
    auto tokenizer = tokenizer_factory->create();
    ASSERT_NE(tokenizer, nullptr);

    auto char_filter_factory = AnalysisFactoryMgr::instance().create<CharFilterFactory>(
            "char_replace", empty_settings);
    ASSERT_NE(char_filter_factory, nullptr);

    auto token_filter_factory =
            AnalysisFactoryMgr::instance().create<TokenFilterFactory>("lowercase", empty_settings);
    ASSERT_NE(token_filter_factory, nullptr);
}

TEST_F(AnalysisFactoryMgrTest, SameNameDifferentTypes) {
    Settings empty_settings;

    auto char_filter_factory =
            AnalysisFactoryMgr::instance().create<CharFilterFactory>("empty", empty_settings);
    ASSERT_NE(char_filter_factory, nullptr);

    auto tokenizer_factory =
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("empty", empty_settings);
    ASSERT_NE(tokenizer_factory, nullptr);

    auto token_filter_factory =
            AnalysisFactoryMgr::instance().create<TokenFilterFactory>("empty", empty_settings);
    ASSERT_NE(token_filter_factory, nullptr);

    EXPECT_NE(dynamic_cast<EmptyCharFilterFactory*>(char_filter_factory.get()), nullptr);
    EXPECT_NE(dynamic_cast<EmptyTokenizerFactory*>(tokenizer_factory.get()), nullptr);
    EXPECT_NE(dynamic_cast<EmptyTokenFilterFactory*>(token_filter_factory.get()), nullptr);
}

TEST_F(AnalysisFactoryMgrTest, EmptyCharFilterPassesThrough) {
    Settings empty_settings;

    auto factory =
            AnalysisFactoryMgr::instance().create<CharFilterFactory>("empty", empty_settings);
    ASSERT_NE(factory, nullptr);

    std::string test_data = "Hello World!";
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(test_data.data(), test_data.size(), false);

    ReaderPtr filtered = factory->create(reader);
    ASSERT_NE(filtered, nullptr);

    const char* buffer = nullptr;
    int32_t len = filtered->read((const void**)&buffer, 0, test_data.size());
    EXPECT_EQ(len, test_data.size());
    std::string result(buffer, len);
    EXPECT_EQ(result, test_data);
}

TEST_F(AnalysisFactoryMgrTest, EmptyTokenizerReturnsWholeInput) {
    Settings empty_settings;

    auto factory = AnalysisFactoryMgr::instance().create<TokenizerFactory>("empty", empty_settings);
    ASSERT_NE(factory, nullptr);

    auto tokenizer = factory->create();
    ASSERT_NE(tokenizer, nullptr);

    std::string test_data = "Hello World Test";
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(test_data.data(), test_data.size(), false);

    tokenizer->set_reader(reader);
    tokenizer->reset();

    Token token;
    EXPECT_TRUE(tokenizer->next(&token));
    std::string result(token.termBuffer<char>(), token.termLength<char>());
    EXPECT_EQ(result, test_data);

    EXPECT_FALSE(tokenizer->next(&token));
}

TEST_F(AnalysisFactoryMgrTest, EmptyTokenFilterPassesThrough) {
    Settings empty_settings;

    auto tokenizer_factory =
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("keyword", empty_settings);
    auto tokenizer = tokenizer_factory->create();

    std::string test_data = "TestToken";
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(test_data.data(), test_data.size(), false);
    tokenizer->set_reader(reader);
    tokenizer->reset();

    auto filter_factory =
            AnalysisFactoryMgr::instance().create<TokenFilterFactory>("empty", empty_settings);
    auto filter = filter_factory->create(tokenizer);
    ASSERT_NE(filter, nullptr);

    Token token;
    EXPECT_TRUE(filter->next(&token));
    std::string result(token.termBuffer<char>(), token.termLength<char>());
    EXPECT_EQ(result, test_data);

    EXPECT_FALSE(filter->next(&token));
}

TEST_F(AnalysisFactoryMgrTest, UnknownFactoryNameThrowsException) {
    Settings empty_settings;

    EXPECT_THROW(
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("nonexistent", empty_settings),
            Exception);

    EXPECT_THROW(AnalysisFactoryMgr::instance().create<CharFilterFactory>("unknown_char_filter",
                                                                          empty_settings),
                 Exception);

    EXPECT_THROW(AnalysisFactoryMgr::instance().create<TokenFilterFactory>("unknown_filter",
                                                                           empty_settings),
                 Exception);
}

TEST_F(AnalysisFactoryMgrTest, FactoryWithSettings) {
    Settings settings;
    settings.set("buffer_size", "512");

    auto factory = AnalysisFactoryMgr::instance().create<TokenizerFactory>("keyword", settings);
    ASSERT_NE(factory, nullptr);

    auto tokenizer = factory->create();
    ASSERT_NE(tokenizer, nullptr);
}

TEST_F(AnalysisFactoryMgrTest, SingletonBehavior) {
    auto& instance1 = AnalysisFactoryMgr::instance();
    auto& instance2 = AnalysisFactoryMgr::instance();

    EXPECT_EQ(&instance1, &instance2);
}

TEST_F(AnalysisFactoryMgrTest, MultipleCreationsFromSameName) {
    Settings empty_settings;

    auto factory1 =
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("standard", empty_settings);
    auto factory2 =
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("standard", empty_settings);

    ASSERT_NE(factory1, nullptr);
    ASSERT_NE(factory2, nullptr);

    EXPECT_NE(factory1.get(), factory2.get());
}

TEST_F(AnalysisFactoryMgrTest, AllBuiltInTokenizersRegistered) {
    Settings empty_settings;

    std::vector<std::string> tokenizer_names = {"standard",   "keyword", "ngram", "edge_ngram",
                                                "char_group", "basic",   "icu",   "empty"};

    for (const auto& name : tokenizer_names) {
        auto factory =
                AnalysisFactoryMgr::instance().create<TokenizerFactory>(name, empty_settings);
        EXPECT_NE(factory, nullptr) << "Tokenizer '" << name << "' should be registered";
    }
}

TEST_F(AnalysisFactoryMgrTest, AllBuiltInCharFiltersRegistered) {
    Settings empty_settings;

    std::vector<std::string> char_filter_names = {"char_replace", "empty"};

    for (const auto& name : char_filter_names) {
        auto factory =
                AnalysisFactoryMgr::instance().create<CharFilterFactory>(name, empty_settings);
        EXPECT_NE(factory, nullptr) << "Char filter '" << name << "' should be registered";
    }
}

TEST_F(AnalysisFactoryMgrTest, AllBuiltInTokenFiltersRegistered) {
    Settings empty_settings;

    std::vector<std::string> token_filter_names = {"lowercase", "asciifolding", "word_delimiter",
                                                   "empty"};

    for (const auto& name : token_filter_names) {
        auto factory =
                AnalysisFactoryMgr::instance().create<TokenFilterFactory>(name, empty_settings);
        EXPECT_NE(factory, nullptr) << "Token filter '" << name << "' should be registered";
    }
}

TEST_F(AnalysisFactoryMgrTest, TypeSafetyForDifferentFactoryTypes) {
    Settings empty_settings;

    EXPECT_THROW(
            AnalysisFactoryMgr::instance().create<TokenizerFactory>("lowercase", empty_settings),
            Exception);

    EXPECT_THROW(
            AnalysisFactoryMgr::instance().create<CharFilterFactory>("standard", empty_settings),
            Exception);

    EXPECT_THROW(AnalysisFactoryMgr::instance().create<TokenFilterFactory>("char_replace",
                                                                           empty_settings),
                 Exception);
}

} // namespace doris::segment_v2::inverted_index