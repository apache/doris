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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/basic/basic_tokenizer_factory.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_basic_tokenizer(const std::string& text, Settings settings = Settings()) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), text.size(), false);

    BasicTokenizerFactory factory;
    factory.initialize(settings);
    auto tokenizer = factory.create();
    tokenizer->set_reader(reader);
    tokenizer->reset();
    return tokenizer;
}

struct ExpectedToken {
    std::string term;
    int pos_inc;
};

class BasicTokenizerFactoryTest : public ::testing::Test {
protected:
    void assert_tokenizer_output(const std::string& text,
                                 const std::vector<ExpectedToken>& expected,
                                 BasicTokenizerMode mode = BasicTokenizerMode::L1) {
        Settings settings;
        settings.set("mode", std::to_string(static_cast<int32_t>(mode)));
        auto tokenizer = create_basic_tokenizer(text, settings);

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

TEST_F(BasicTokenizerFactoryTest, BasicTokenizationL1) {
    // Test L1 mode: English + numbers + Chinese tokenization
    assert_tokenizer_output("Hello world!", {{"Hello", 1}, {"world", 1}}, BasicTokenizerMode::L1);
}

TEST_F(BasicTokenizerFactoryTest, ChineseTokenizationL1) {
    // Test L1 mode with Chinese characters
    assert_tokenizer_output("你好世界", {{"你", 1}, {"好", 1}, {"世", 1}, {"界", 1}},
                            BasicTokenizerMode::L1);
}

TEST_F(BasicTokenizerFactoryTest, MixedLanguageL1) {
    // Test L1 mode with mixed English and Chinese
    assert_tokenizer_output(
            "Hello你好World世界",
            {{"Hello", 1}, {"你", 1}, {"好", 1}, {"World", 1}, {"世", 1}, {"界", 1}},
            BasicTokenizerMode::L1);
}

TEST_F(BasicTokenizerFactoryTest, NumbersAndPunctuationL1) {
    // Test L1 mode with numbers and punctuation
    assert_tokenizer_output("Version 2.0 版本",
                            {{"Version", 1}, {"2", 1}, {"0", 1}, {"版", 1}, {"本", 1}},
                            BasicTokenizerMode::L1);
}

TEST_F(BasicTokenizerFactoryTest, BasicTokenizationL2) {
    // Test L2 mode: L1 + all Unicode characters tokenized
    assert_tokenizer_output("Hello world!", {{"Hello", 1}, {"world", 1}, {"!", 1}},
                            BasicTokenizerMode::L2);
}

TEST_F(BasicTokenizerFactoryTest, UnicodeTokenizationL2) {
    // Test L2 mode with various Unicode characters
    assert_tokenizer_output("Hello��世界", {{"Hello", 1}, {"�", 1}, {"�", 1}, {"世", 1}, {"界", 1}},
                            BasicTokenizerMode::L2);
}

TEST_F(BasicTokenizerFactoryTest, WhitespaceHandlingL2) {
    // Test L2 mode skips whitespace
    assert_tokenizer_output("Hello   world", {{"Hello", 1}, {"world", 1}}, BasicTokenizerMode::L2);
}

TEST_F(BasicTokenizerFactoryTest, FactoryInitialization) {
    Settings settings;
    settings.set("mode", "1");

    BasicTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto basic_tokenizer = std::dynamic_pointer_cast<BasicTokenizer>(tokenizer);
    ASSERT_NE(basic_tokenizer, nullptr);
}

TEST_F(BasicTokenizerFactoryTest, FactoryInitializationL2) {
    Settings settings;
    settings.set("mode", "2");

    BasicTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto basic_tokenizer = std::dynamic_pointer_cast<BasicTokenizer>(tokenizer);
    ASSERT_NE(basic_tokenizer, nullptr);
}

TEST_F(BasicTokenizerFactoryTest, DefaultMode) {
    // Test default mode (L1) when no mode is specified
    Settings settings;
    BasicTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto basic_tokenizer = std::dynamic_pointer_cast<BasicTokenizer>(tokenizer);
    ASSERT_NE(basic_tokenizer, nullptr);
}

TEST_F(BasicTokenizerFactoryTest, InvalidMode) {
    Settings settings;
    settings.set("mode", "3"); // Invalid mode

    BasicTokenizerFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(BasicTokenizerFactoryTest, InvalidModeZero) {
    Settings settings;
    settings.set("mode", "0"); // Invalid mode

    BasicTokenizerFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(BasicTokenizerFactoryTest, InvalidModeNegative) {
    Settings settings;
    settings.set("mode", "-1"); // Invalid mode

    BasicTokenizerFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(BasicTokenizerFactoryTest, EdgeCases) {
    // Test empty string
    assert_tokenizer_output("", {}, BasicTokenizerMode::L1);

    // Test whitespace only
    assert_tokenizer_output("   ", {}, BasicTokenizerMode::L1);

    // Test punctuation only (L1 mode should skip non-Chinese punctuation)
    assert_tokenizer_output("...", {}, BasicTokenizerMode::L1);

    // Test punctuation only (L2 mode should tokenize punctuation)
    assert_tokenizer_output("...", {{".", 1}, {".", 1}, {".", 1}}, BasicTokenizerMode::L2);
}

TEST_F(BasicTokenizerFactoryTest, LongText) {
    // Test with longer text
    std::string long_text = "This is a long text with multiple words and 中文 characters";
    std::vector<ExpectedToken> expected = {
            {"This", 1},     {"is", 1},    {"a", 1},   {"long", 1}, {"text", 1}, {"with", 1},
            {"multiple", 1}, {"words", 1}, {"and", 1}, {"中", 1},   {"文", 1},   {"characters", 1}};
    assert_tokenizer_output(long_text, expected, BasicTokenizerMode::L1);
}

} // namespace doris::segment_v2::inverted_index