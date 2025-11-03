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

#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter_factory.h"

#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <vector>

namespace doris::segment_v2::inverted_index {

class WordDelimiterFactoryTest : public ::testing::Test {
protected:
    WordDelimiterFilterFactory _factory;
};

TEST_F(WordDelimiterFactoryTest, DefaultSettings) {
    Settings settings;
    _factory.initialize(settings);

    int32_t expected_flags =
            WordDelimiterFilter::GENERATE_WORD_PARTS | WordDelimiterFilter::GENERATE_NUMBER_PARTS |
            WordDelimiterFilter::SPLIT_ON_CASE_CHANGE | WordDelimiterFilter::SPLIT_ON_NUMERICS |
            WordDelimiterFilter::STEM_ENGLISH_POSSESSIVE;

    EXPECT_EQ(_factory._flags, expected_flags);
    EXPECT_TRUE(_factory._protected_words.empty());
}

TEST_F(WordDelimiterFactoryTest, CustomFlags) {
    Settings settings;
    settings.set("catenate_words", "true");
    settings.set("split_on_numerics", "false");

    _factory.initialize(settings);

    int32_t expected_flags =
            WordDelimiterFilter::GENERATE_WORD_PARTS | WordDelimiterFilter::GENERATE_NUMBER_PARTS |
            WordDelimiterFilter::CATENATE_WORDS | WordDelimiterFilter::SPLIT_ON_CASE_CHANGE |
            WordDelimiterFilter::STEM_ENGLISH_POSSESSIVE;

    EXPECT_EQ(_factory._flags, expected_flags);
    EXPECT_EQ(_factory._char_type_table, WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE);
    EXPECT_TRUE(_factory._protected_words.empty());
}

TEST_F(WordDelimiterFactoryTest, ProtectedWords) {
    Settings settings;
    settings.set("protected_words", "foo, bar");

    _factory.initialize(settings);

    EXPECT_EQ(_factory._protected_words.size(), 2);
    EXPECT_TRUE(_factory._protected_words.contains("foo"));
    EXPECT_TRUE(_factory._protected_words.contains("bar"));

    int32_t expected_flags =
            WordDelimiterFilter::GENERATE_WORD_PARTS | WordDelimiterFilter::GENERATE_NUMBER_PARTS |
            WordDelimiterFilter::SPLIT_ON_CASE_CHANGE | WordDelimiterFilter::SPLIT_ON_NUMERICS |
            WordDelimiterFilter::STEM_ENGLISH_POSSESSIVE;

    EXPECT_EQ(_factory._flags, expected_flags);
    EXPECT_EQ(_factory._char_type_table, WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE);
}

TEST_F(WordDelimiterFactoryTest, CustomTypeTable) {
    Settings settings;
    settings.set("type_table", "[a => ALPHA],[1 => DIGIT],[\\u002C => DIGIT]");

    _factory.initialize(settings);

    EXPECT_EQ(_factory._char_type_table['a'], WordDelimiterFilter::ALPHA);
    EXPECT_EQ(_factory._char_type_table['1'], WordDelimiterFilter::DIGIT);
    EXPECT_EQ(_factory._char_type_table['b'], WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE['b']);
    EXPECT_GE(_factory._char_type_table.size(),
              WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE.size());
}

TEST_F(WordDelimiterFactoryTest, InvalidTypeRules) {
    bool exception_flag = false;
    try {
        Settings settings;
        settings.set("type_table", "[ab => ALPHA]");
        _factory.initialize(settings);
    } catch (...) {
        exception_flag = true;
    }
    EXPECT_TRUE(exception_flag);

    exception_flag = false;
    try {
        Settings settings;
        settings.set("type_table", "[a => INVALID_TYPE]");
        _factory.initialize(settings);
    } catch (...) {
        exception_flag = true;
    }
    EXPECT_TRUE(exception_flag);

    exception_flag = false;
    try {
        Settings settings;
        settings.set("type_table", "[ => ALPHA]");
        _factory.initialize(settings);
    } catch (...) {
        exception_flag = true;
    }
    EXPECT_TRUE(exception_flag);
}

TEST_F(WordDelimiterFactoryTest, EscapeSequences) {
    {
        Settings settings;
        settings.set("type_table", "[\\u002C => DIGIT]");

        _factory.initialize(settings);
        EXPECT_EQ(_factory._char_type_table[','], WordDelimiterFilter::DIGIT);
    }

    bool exception_flag = false;
    try {
        Settings settings;
        settings.set("type_table", "[\\u00 => DIGIT]");
        _factory.initialize(settings);
    } catch (...) {
        exception_flag = true;
    }
    EXPECT_TRUE(exception_flag);
}

} // namespace doris::segment_v2::inverted_index