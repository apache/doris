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

#include "storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

namespace doris::segment_v2::inverted_index {

std::unique_ptr<TextCharFilter> create_char_replace_filter(const std::string& pattern,
                                                           const std::string& replacement = " ") {
    Settings settings;
    settings.set(CHAR_REPLACE_PATTERN, pattern);
    settings.set(CHAR_REPLACE_REPLACEMENT, replacement);

    CharReplaceCharFilterFactory factory;
    factory.initialize(settings);
    return factory.create();
}

class CharReplaceCharFilterFactoryTest : public ::testing::Test {
protected:
    void assert_char_filter_output(const std::string& input_text, const std::string& pattern,
                                   const std::string& expected_output,
                                   const std::string& replacement = " ") {
        auto char_filter = create_char_replace_filter(pattern, replacement);

        std::string scratch;
        const std::string_view result = char_filter->apply(input_text, scratch);
        ASSERT_GT(result.size(), 0) << "Failed to read from char filter";

        EXPECT_EQ(result, expected_output) << "Char filter output mismatch";
    }
};

TEST_F(CharReplaceCharFilterFactoryTest, BasicReplacement) {
    assert_char_filter_output("hello,world", ",", "hello world");
}

TEST_F(CharReplaceCharFilterFactoryTest, MultipleReplacements) {
    assert_char_filter_output("a,b,c,d", ",", "a b c d");
}

TEST_F(CharReplaceCharFilterFactoryTest, CustomReplacement) {
    assert_char_filter_output("hello,world", ",", "hello_world", "_");
}

TEST_F(CharReplaceCharFilterFactoryTest, MultiplePatternChars) {
    assert_char_filter_output("a,b;c:d", ",;:", "a b c d");
}

TEST_F(CharReplaceCharFilterFactoryTest, NoMatch) {
    assert_char_filter_output("hello world", "x", "hello world");
}

TEST_F(CharReplaceCharFilterFactoryTest, EmptyInput) {
    auto char_filter = create_char_replace_filter(",");

    std::string scratch;
    const std::string_view result = char_filter->apply("", scratch);

    // An empty value stays empty
    EXPECT_TRUE(result.empty()) << "Empty input should give empty text";
    EXPECT_TRUE(scratch.empty()) << "No data should be written for empty input";
}

TEST_F(CharReplaceCharFilterFactoryTest, EmptyPattern) {
    try {
        assert_char_filter_output("hello,world", "", "hello,world");
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
    }
}

TEST_F(CharReplaceCharFilterFactoryTest, AllCharsMatch) {
    assert_char_filter_output("abc", "abc", "   ");
}

TEST_F(CharReplaceCharFilterFactoryTest, ChineseCharacters) {
    assert_char_filter_output("你好,世界", ",", "你好 世界");
}

TEST_F(CharReplaceCharFilterFactoryTest, SpecialCharacters) {
    assert_char_filter_output("test@example.com", "@.", "test example com");
}

TEST_F(CharReplaceCharFilterFactoryTest, FactoryInitialization) {
    Settings settings;
    settings.set(CHAR_REPLACE_PATTERN, ",");
    settings.set(CHAR_REPLACE_REPLACEMENT, " ");

    CharReplaceCharFilterFactory factory;
    EXPECT_NO_THROW(factory.initialize(settings));
}

TEST_F(CharReplaceCharFilterFactoryTest, FactoryInitializationMissingPattern) {
    Settings settings;
    // Missing pattern - should throw exception
    settings.set(CHAR_REPLACE_REPLACEMENT, " ");

    CharReplaceCharFilterFactory factory;
    EXPECT_NO_THROW(factory.initialize(settings));
}

TEST_F(CharReplaceCharFilterFactoryTest, FactoryInitializationEmptyPattern) {
    Settings settings;
    settings.set(CHAR_REPLACE_PATTERN, "");
    settings.set(CHAR_REPLACE_REPLACEMENT, " ");

    CharReplaceCharFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(CharReplaceCharFilterFactoryTest, FactoryCreateFilter) {
    Settings settings;
    settings.set(CHAR_REPLACE_PATTERN, ",");
    settings.set(CHAR_REPLACE_REPLACEMENT, " ");

    CharReplaceCharFilterFactory factory;
    factory.initialize(settings);

    auto char_filter = factory.create();
    ASSERT_NE(char_filter, nullptr);

    std::string scratch;
    const std::string_view result = char_filter->apply("test,data", scratch);
    ASSERT_GT(result.size(), 0);

    EXPECT_EQ(result, "test data");
}

TEST_F(CharReplaceCharFilterFactoryTest, DefaultReplacement) {
    Settings settings;
    settings.set(CHAR_REPLACE_PATTERN, ",");
    // No replacement specified - should use default " "

    CharReplaceCharFilterFactory factory;
    factory.initialize(settings);

    auto char_filter = factory.create();

    std::string scratch;
    const std::string_view result = char_filter->apply("a,b,c", scratch);
    ASSERT_GT(result.size(), 0);

    EXPECT_EQ(result, "a b c");
}

TEST_F(CharReplaceCharFilterFactoryTest, EdgeCases) {
    // Test with whitespace only
    assert_char_filter_output("   ", ",", "   ");

    // Test with only pattern characters
    assert_char_filter_output(",,,", ",", "   ");

    // Test with mixed content
    assert_char_filter_output("a,,b,", ",", "a  b ");
}

} // namespace doris::segment_v2::inverted_index
