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
                                 const std::string& extra_chars = "") {
        Settings settings;
        if (!extra_chars.empty()) {
            settings.set("extra_chars", extra_chars);
        }
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

TEST_F(BasicTokenizerFactoryTest, BasicTokenization) {
    // Test basic tokenization: English + numbers + Chinese
    assert_tokenizer_output("Hello world!", {{"Hello", 1}, {"world", 1}});
}

TEST_F(BasicTokenizerFactoryTest, ChineseTokenization) {
    // Test Chinese characters tokenization
    assert_tokenizer_output("你好世界", {{"你", 1}, {"好", 1}, {"世", 1}, {"界", 1}});
}

TEST_F(BasicTokenizerFactoryTest, MixedLanguage) {
    // Test mixed English and Chinese
    assert_tokenizer_output(
            "Hello你好World世界",
            {{"Hello", 1}, {"你", 1}, {"好", 1}, {"World", 1}, {"世", 1}, {"界", 1}});
}

TEST_F(BasicTokenizerFactoryTest, NumbersAndPunctuation) {
    // Test with numbers, punctuation is ignored by default
    assert_tokenizer_output("Version 2.0 版本",
                            {{"Version", 1}, {"2", 1}, {"0", 1}, {"版", 1}, {"本", 1}});
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsBasic) {
    // Test extra_chars: tokenize '.' as separate token
    assert_tokenizer_output("Version 2.0", {{"Version", 1}, {"2", 1}, {".", 1}, {"0", 1}}, ".");
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsMultiple) {
    // Test multiple extra_chars: '[', ']', '.', '_'
    assert_tokenizer_output(
            "test[0].value_1",
            {{"test", 1}, {"[", 1}, {"0", 1}, {"]", 1}, {".", 1}, {"value", 1}, {"_", 1}, {"1", 1}},
            "[]._");
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsWithChinese) {
    // Test extra_chars with Chinese text
    assert_tokenizer_output("测试.数据", {{"测", 1}, {"试", 1}, {".", 1}, {"数", 1}, {"据", 1}},
                            ".");
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsSpecialSymbols) {
    // Test various special symbols as extra_chars
    assert_tokenizer_output("hello@world#2024",
                            {{"hello", 1}, {"@", 1}, {"world", 1}, {"#", 1}, {"2024", 1}}, "@#");
}

TEST_F(BasicTokenizerFactoryTest, FactoryInitializationDefault) {
    // Test default initialization without extra_chars
    Settings settings;

    BasicTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto basic_tokenizer = std::dynamic_pointer_cast<BasicTokenizer>(tokenizer);
    ASSERT_NE(basic_tokenizer, nullptr);
}

TEST_F(BasicTokenizerFactoryTest, FactoryInitializationWithExtraChars) {
    // Test initialization with extra_chars
    Settings settings;
    settings.set("extra_chars", "[]._");

    BasicTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto basic_tokenizer = std::dynamic_pointer_cast<BasicTokenizer>(tokenizer);
    ASSERT_NE(basic_tokenizer, nullptr);
}

TEST_F(BasicTokenizerFactoryTest, EmptyExtraChars) {
    // Test with empty extra_chars
    Settings settings;
    settings.set("extra_chars", "");

    BasicTokenizerFactory factory;
    EXPECT_NO_THROW(factory.initialize(settings));
}

TEST_F(BasicTokenizerFactoryTest, InvalidExtraCharsNonASCII) {
    // Test with non-ASCII characters in extra_chars (should throw)
    Settings settings;
    settings.set("extra_chars", "你好"); // Chinese characters

    BasicTokenizerFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(BasicTokenizerFactoryTest, InvalidExtraCharsMixedASCIIAndNonASCII) {
    // Test with mixed ASCII and non-ASCII (should throw)
    Settings settings;
    settings.set("extra_chars", ".中"); // '.' is ASCII, '中' is not

    BasicTokenizerFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(BasicTokenizerFactoryTest, ValidASCIIExtraChars) {
    // Test with various valid ASCII characters
    Settings settings;
    settings.set("extra_chars", "!@#$%^&*()_+-=[]{}|;:',.<>?/~`");

    BasicTokenizerFactory factory;
    EXPECT_NO_THROW(factory.initialize(settings));
}

TEST_F(BasicTokenizerFactoryTest, EdgeCases) {
    // Test empty string
    assert_tokenizer_output("", {});

    // Test whitespace only
    assert_tokenizer_output("   ", {});

    // Test punctuation only without extra_chars (should be ignored)
    assert_tokenizer_output("...", {});

    // Test punctuation only with extra_chars (should be tokenized)
    assert_tokenizer_output("...", {{".", 1}, {".", 1}, {".", 1}}, ".");
}

TEST_F(BasicTokenizerFactoryTest, LongText) {
    // Test with longer text
    std::string long_text = "This is a long text with multiple words and 中文 characters";
    std::vector<ExpectedToken> expected = {
            {"This", 1},     {"is", 1},    {"a", 1},   {"long", 1}, {"text", 1}, {"with", 1},
            {"multiple", 1}, {"words", 1}, {"and", 1}, {"中", 1},   {"文", 1},   {"characters", 1}};
    assert_tokenizer_output(long_text, expected);
}

TEST_F(BasicTokenizerFactoryTest, LongTextWithExtraChars) {
    // Test longer text with extra_chars
    std::string long_text = "user.name=test_123";
    std::vector<ExpectedToken> expected = {{"user", 1}, {".", 1}, {"name", 1}, {"=", 1},
                                           {"test", 1}, {"_", 1}, {"123", 1}};
    assert_tokenizer_output(long_text, expected, "._=");
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsWithAlphanumeric) {
    assert_tokenizer_output("[123.456_7890abcd]",
                            {{.term = "[", .pos_inc = 1},
                             {.term = "123", .pos_inc = 1},
                             {.term = ".", .pos_inc = 1},
                             {.term = "456", .pos_inc = 1},
                             {.term = "_", .pos_inc = 1},
                             {.term = "7890abcd", .pos_inc = 1},
                             {.term = "]", .pos_inc = 1}},
                            "[]._");
}

TEST_F(BasicTokenizerFactoryTest, ExtraCharsShouldNotIncludeAlphanumeric) {
    assert_tokenizer_output("Found OpenMP: TRUE (found version \"5.1\")",
                            {{.term = "Found", .pos_inc = 1},
                             {.term = "OpenMP", .pos_inc = 1},
                             {.term = "TRUE", .pos_inc = 1},
                             {.term = "(", .pos_inc = 1},
                             {.term = "found", .pos_inc = 1},
                             {.term = "version", .pos_inc = 1},
                             {.term = "\"", .pos_inc = 1},
                             {.term = "5", .pos_inc = 1},
                             {.term = ".", .pos_inc = 1},
                             {.term = "1", .pos_inc = 1},
                             {.term = "\"", .pos_inc = 1},
                             {.term = ")", .pos_inc = 1}},
                            "()\".");
}

} // namespace doris::segment_v2::inverted_index