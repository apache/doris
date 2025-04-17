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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_standard_tokenizer(const std::string& text, Settings settings = Settings()) {
    static lucene::util::SStringReader<char> reader;
    reader.init(text.data(), text.size(), false);

    StandardTokenizerFactory factory;
    factory.initialize(settings);
    auto tokenizer = factory.create();
    tokenizer->set_reader(&reader);
    tokenizer->reset();
    return tokenizer;
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

class StandardTokenizerTest : public ::testing::Test {
protected:
    void assert_tokenizer_output(
            const std::string& text, const std::vector<ExpectedToken>& expected,
            int32_t max_token_length = StandardTokenizer::DEFAULT_MAX_TOKEN_LENGTH) {
        Settings settings;
        if (max_token_length != StandardTokenizer::DEFAULT_MAX_TOKEN_LENGTH) {
            settings.set("max_token_length", std::to_string(max_token_length));
        }
        auto tokenizer = create_standard_tokenizer(text, settings);

        Token t;
        size_t i = 0;
        while (tokenizer->next(&t)) {
            ASSERT_LT(i, expected.size()) << "More tokens produced than expected";
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            EXPECT_EQ(term, expected[i].term) << "Term mismatch at index " << i;
            EXPECT_EQ(t.getPositionIncrement(), expected[i].pos_inc)
                    << "Pos increment mismatch at index " << i;
            ++i;
        }
        EXPECT_EQ(i, expected.size()) << "Number of tokens mismatch";
    }
};

TEST_F(StandardTokenizerTest, BasicTokenization) {
    assert_tokenizer_output("Hello world!", {{"Hello", 1}, {"world", 1}});
}

TEST_F(StandardTokenizerTest, HandlesPunctuation) {
    assert_tokenizer_output(
            "This is a test, with punctuation!",
            {{"This", 1}, {"is", 1}, {"a", 1}, {"test", 1}, {"with", 1}, {"punctuation", 1}});
}

TEST_F(StandardTokenizerTest, HandlesNumbers) {
    assert_tokenizer_output(
            "Version 2.0 was released in 2023",
            {{"Version", 1}, {"2.0", 1}, {"was", 1}, {"released", 1}, {"in", 1}, {"2023", 1}});
}

TEST_F(StandardTokenizerTest, OneLength) {
    assert_tokenizer_output("Draft:Illinois",
                            {{"D", 1},
                             {"r", 1},
                             {"a", 1},
                             {"f", 1},
                             {"t", 1},
                             {"I", 1},
                             {"l", 1},
                             {"l", 1},
                             {"i", 1},
                             {"n", 1},
                             {"o", 1},
                             {"i", 1},
                             {"s", 1}},
                            1);
}

TEST_F(StandardTokenizerTest, HandlesMaxTokenLength) {
    std::string long_word(300, 'a');
    assert_tokenizer_output(
            long_word,
            {{std::string(StandardTokenizer::DEFAULT_MAX_TOKEN_LENGTH, 'a'), 1},
             {std::string(300 - StandardTokenizer::DEFAULT_MAX_TOKEN_LENGTH, 'a'), 1}},
            StandardTokenizer::DEFAULT_MAX_TOKEN_LENGTH);
}

TEST_F(StandardTokenizerTest, CustomMaxTokenLength) {
    std::string long_word(50, 'b'); // 50 'b's
    int custom_length = 20;
    assert_tokenizer_output(long_word,
                            {{std::string(custom_length, 'b'), 1},
                             {std::string(custom_length, 'b'), 1},
                             {std::string(10, 'b'), 1}},
                            custom_length);
}

TEST_F(StandardTokenizerTest, FactoryInitialization) {
    Settings settings;
    settings.set("max_token_length", "100");

    StandardTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto standard_tokenizer = std::dynamic_pointer_cast<StandardTokenizer>(tokenizer);
    ASSERT_NE(standard_tokenizer, nullptr);
}

TEST_F(StandardTokenizerTest, EdgeCases) {
    assert_tokenizer_output("", {});
    assert_tokenizer_output("   ", {});
    assert_tokenizer_output("...", {});
}

TEST_F(StandardTokenizerTest, InvalidMaxTokenLength) {
    StandardTokenizer tokenizer;

    EXPECT_THROW(tokenizer.set_max_token_length(0), Exception);
    EXPECT_THROW(tokenizer.set_max_token_length(StandardTokenizer::MAX_TOKEN_LENGTH_LIMIT + 1),
                 Exception);

    // Test valid values don't throw
    EXPECT_NO_THROW(tokenizer.set_max_token_length(1));
    EXPECT_NO_THROW(tokenizer.set_max_token_length(StandardTokenizer::MAX_TOKEN_LENGTH_LIMIT));
}

} // namespace doris::segment_v2::inverted_index
