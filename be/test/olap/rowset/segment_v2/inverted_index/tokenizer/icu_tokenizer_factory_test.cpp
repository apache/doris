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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/icu/icu_tokenizer_factory.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

TokenStreamPtr create_icu_tokenizer(const std::string& text, Settings settings = Settings()) {
    ReaderPtr reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), text.size(), false);

    ICUTokenizerFactory factory;
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

class ICUTokenizerFactoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        original_dict_path_ = config::inverted_index_dict_path;

        constexpr static uint32_t MAX_PATH_LEN = 1024;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        std::string current_dir = std::string(buffer);

        config::inverted_index_dict_path = current_dir + "/be/dict";
    }

    void TearDown() override { config::inverted_index_dict_path = original_dict_path_; }

    void assert_tokenizer_output(const std::string& text,
                                 const std::vector<ExpectedToken>& expected) {
        auto tokenizer = create_icu_tokenizer(text);

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

private:
    std::string original_dict_path_;
};

TEST_F(ICUTokenizerFactoryTest, BasicEnglishTokenization) {
    // Test basic English tokenization
    assert_tokenizer_output("Hello world!", {{"Hello", 1}, {"world", 1}});
}

TEST_F(ICUTokenizerFactoryTest, EnglishWithPunctuation) {
    // Test English with punctuation
    assert_tokenizer_output(
            "This is a test, with punctuation!",
            {{"This", 1}, {"is", 1}, {"a", 1}, {"test", 1}, {"with", 1}, {"punctuation", 1}});
}

TEST_F(ICUTokenizerFactoryTest, EnglishWithNumbers) {
    // Test English with numbers
    assert_tokenizer_output(
            "Version 2.0 was released in 2023",
            {{"Version", 1}, {"2.0", 1}, {"was", 1}, {"released", 1}, {"in", 1}, {"2023", 1}});
}

TEST_F(ICUTokenizerFactoryTest, ChineseTokenization) {
    // Test Chinese character tokenization
    assert_tokenizer_output("你好世界", {{"你好", 1}, {"世界", 1}});
}

TEST_F(ICUTokenizerFactoryTest, MixedLanguage) {
    // Test mixed English and Chinese
    assert_tokenizer_output("Hello你好World世界",
                            {{"Hello", 1}, {"你好", 1}, {"World", 1}, {"世界", 1}});
}

TEST_F(ICUTokenizerFactoryTest, JapaneseTokenization) {
    // Test Japanese tokenization (Hiragana, Katakana, Kanji)
    assert_tokenizer_output("こんにちは世界", {{"こんにちは", 1}, {"世界", 1}});
}

TEST_F(ICUTokenizerFactoryTest, KoreanTokenization) {
    // Test Korean tokenization
    assert_tokenizer_output("안녕하세요", {{"안녕하세요", 1}});
}

TEST_F(ICUTokenizerFactoryTest, ArabicTokenization) {
    // Test Arabic tokenization
    assert_tokenizer_output("مرحبا بالعالم", {{"مرحبا", 1}, {"بالعالم", 1}});
}

TEST_F(ICUTokenizerFactoryTest, CyrillicTokenization) {
    // Test Cyrillic (Russian) tokenization
    assert_tokenizer_output("Привет мир", {{"Привет", 1}, {"мир", 1}});
}

TEST_F(ICUTokenizerFactoryTest, EmojiAndSymbols) {
    // Test emoji and special symbols
    assert_tokenizer_output("Hello 😀 world 🌍",
                            {{"Hello", 1}, {"😀", 1}, {"world", 1}, {"🌍", 1}});
}

TEST_F(ICUTokenizerFactoryTest, WhitespaceHandling) {
    // Test whitespace handling
    assert_tokenizer_output("Hello   world\t\n", {{"Hello", 1}, {"world", 1}});
}

TEST_F(ICUTokenizerFactoryTest, FactoryInitialization) {
    Settings settings;
    ICUTokenizerFactory factory;
    factory.initialize(settings);

    auto tokenizer = factory.create();
    auto icu_tokenizer = std::dynamic_pointer_cast<ICUTokenizer>(tokenizer);
    ASSERT_NE(icu_tokenizer, nullptr);
}

TEST_F(ICUTokenizerFactoryTest, FactoryCreateMultipleInstances) {
    ICUTokenizerFactory factory;
    factory.initialize(Settings {});

    auto tokenizer1 = factory.create();
    auto tokenizer2 = factory.create();

    ASSERT_NE(tokenizer1, tokenizer2);
    ASSERT_NE(tokenizer1, nullptr);
    ASSERT_NE(tokenizer2, nullptr);
}

TEST_F(ICUTokenizerFactoryTest, EdgeCases) {
    // Test empty string
    assert_tokenizer_output("", {});

    // Test whitespace only
    assert_tokenizer_output("   \t\n", {});

    // Test single character
    assert_tokenizer_output("a", {{"a", 1}});

    // Test single Chinese character
    assert_tokenizer_output("中", {{"中", 1}});
}

TEST_F(ICUTokenizerFactoryTest, LongText) {
    // Test with longer text
    std::string long_text =
            "This is a long text with multiple words and 中文 characters and 日本語 text";
    std::vector<ExpectedToken> expected = {
            {"This", 1},       {"is", 1},       {"a", 1},      {"long", 1}, {"text", 1},
            {"with", 1},       {"multiple", 1}, {"words", 1},  {"and", 1},  {"中文", 1},
            {"characters", 1}, {"and", 1},      {"日本語", 1}, {"text", 1}};
    assert_tokenizer_output(long_text, expected);
}

TEST_F(ICUTokenizerFactoryTest, SpecialCharacters) {
    // Test special characters and symbols
    assert_tokenizer_output("Price: $100.50 (USD)", {{"Price", 1}, {"100.50", 1}, {"USD", 1}});
}

TEST_F(ICUTokenizerFactoryTest, URLAndEmail) {
    // Test URL and email handling
    assert_tokenizer_output("Visit https://example.com or email test@example.com",
                            {{"Visit", 1},
                             {"https", 1},
                             {"example.com", 1},
                             {"or", 1},
                             {"email", 1},
                             {"test", 1},
                             {"example.com", 1}});
}

TEST_F(ICUTokenizerFactoryTest, CaseSensitivity) {
    // Test case sensitivity (ICU tokenizer should preserve case by default)
    assert_tokenizer_output("Hello WORLD Test", {{"Hello", 1}, {"WORLD", 1}, {"Test", 1}});
}

TEST_F(ICUTokenizerFactoryTest, UnicodeNormalization) {
    // Test Unicode normalization
    assert_tokenizer_output("café naïve résumé", {{"café", 1}, {"naïve", 1}, {"résumé", 1}});
}

} // namespace doris::segment_v2::inverted_index