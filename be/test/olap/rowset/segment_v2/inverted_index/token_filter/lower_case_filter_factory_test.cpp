
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

#include "olap/rowset/segment_v2/inverted_index/token_filter/lower_case_filter_factory.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_lowercase_filter(const std::string& text, Settings settings = Settings()) {
    static lucene::util::SStringReader<char> reader;
    reader.init(text.data(), text.size(), false);

    KeywordTokenizerFactory tokenizer_factory;
    tokenizer_factory.initialize(Settings());
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(&reader);

    LowerCaseFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);
    filter->reset();
    return filter;
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

class LowerCaseFilterTest : public ::testing::Test {
protected:
    void assert_filter_output(const std::string& text, const std::vector<ExpectedToken>& expected) {
        auto filter = create_lowercase_filter(text);

        Token t;
        size_t i = 0;
        while (filter->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            EXPECT_EQ(term, expected[i].term) << "Term mismatch at index " << i;
            EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc)
                    << "Pos increment mismatch at index " << i;
            ++i;
        }
        EXPECT_EQ(i, expected.size()) << "Number of tokens mismatch";
    }
};

TEST_F(LowerCaseFilterTest, LeavesASCIIUntouched) {
    assert_filter_output("hello world", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, ConvertsUpperCaseASCII) {
    assert_filter_output("HELLO WORLD", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, HandlesMixedCase) {
    assert_filter_output("HeLLo WoRLd", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, ConvertsUnicodeCharacters) {
    assert_filter_output("ÜBER ΜΈΓΑ", {{"über μέγα", 1}});
}

TEST_F(LowerCaseFilterTest, HandlesNumbersAndSymbols) {
    assert_filter_output("123!@# ABC", {{"123!@# abc", 1}});
}

TEST_F(LowerCaseFilterTest, HandlesEmptyString) {
    assert_filter_output("", {});
}

TEST_F(LowerCaseFilterTest, PreservesPositionIncrements) {
    // Test with multiple tokens to verify position increments
    assert_filter_output("HELLO WORLD", {{"hello world", 1}});
}

TEST_F(LowerCaseFilterTest, FactoryInitialization) {
    Settings settings;
    LowerCaseFilterFactory factory;

    // Should not throw or crash
    factory.initialize(settings);

    // Verify factory creates correct filter type
    auto mockStream = std::make_shared<KeywordTokenizer>();
    auto filter = factory.create(mockStream);
    EXPECT_NE(filter, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<LowerCaseFilter>(filter), nullptr);
}

} // namespace doris::segment_v2::inverted_index
