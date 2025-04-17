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

#include <cstring>

#include "olap/rowset/segment_v2/inverted_index/token_filter/word_delimiter_filter.h"

namespace doris::segment_v2::inverted_index {

class WordDelimiterIteratorTest : public ::testing::Test {
protected:
    void SetUp() override { default_table = WordDelimiterIterator::DEFAULT_WORD_DELIM_TABLE; }

    std::string get_current_subword(WordDelimiterIterator& iter) {
        if (iter._end == WordDelimiterIterator::DONE) {
            return "";
        }
        return {iter._text + iter._current, (size_t)iter._end - iter._current};
    }

    std::vector<char> default_table;
};

TEST_F(WordDelimiterIteratorTest, BasicSplit) {
    char text[] = "HelloWorld";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(text, 10);

    EXPECT_EQ(iter.next(), 5);
    EXPECT_EQ(get_current_subword(iter), "Hello");
    EXPECT_EQ(iter.next(), 10);
    EXPECT_EQ(get_current_subword(iter), "World");
    EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
}

TEST_F(WordDelimiterIteratorTest, ChineseHandling) {
    const char* text = " 你好世界 ";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(const_cast<char*>(text), strlen(text));

    std::vector<std::string> result;
    while (iter.next() != WordDelimiterIterator::DONE) {
        result.emplace_back(iter._text + iter._current, iter._end - iter._current);
    }

    EXPECT_EQ(result, std::vector<std::string> {"你好世界"});

    EXPECT_EQ(iter.get_type(u'你'), WordDelimiterIterator::ALPHA);
}

TEST_F(WordDelimiterIteratorTest, ChineseEnglishMix) {
    const char* text = "Hello世界123";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(const_cast<char*>(text), strlen(text));

    std::vector<std::string> expected {"Hello世界", "123"};

    std::vector<std::string> actual;
    while (iter.next() != WordDelimiterIterator::DONE) {
        actual.emplace_back(iter._text + iter._current, iter._end - iter._current);
    }

    EXPECT_EQ(actual, expected);
}

TEST_F(WordDelimiterIteratorTest, CaseSplitControl) {
    {
        char text[] = "PowerShot";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 9);

        EXPECT_EQ(iter.next(), 5);
        EXPECT_EQ(get_current_subword(iter), "Power");
        EXPECT_EQ(iter.next(), 9);
        EXPECT_EQ(get_current_subword(iter), "Shot");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }

    {
        char text[] = "PowerShot";
        WordDelimiterIterator iter(default_table, false, true, true);
        iter.set_text(text, 9);

        EXPECT_EQ(iter.next(), 9);
        EXPECT_EQ(get_current_subword(iter), "PowerShot");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }
}

TEST_F(WordDelimiterIteratorTest, NumericSplit) {
    {
        char text[] = "j2se";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 4);

        EXPECT_EQ(iter.next(), 1);
        EXPECT_EQ(get_current_subword(iter), "j");
        EXPECT_EQ(iter.next(), 2);
        EXPECT_EQ(get_current_subword(iter), "2");
        EXPECT_EQ(iter.next(), 4);
        EXPECT_EQ(get_current_subword(iter), "se");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }

    {
        char text[] = "j2se";
        WordDelimiterIterator iter(default_table, true, false, true);
        iter.set_text(text, 4);

        EXPECT_EQ(iter.next(), 4);
        EXPECT_EQ(get_current_subword(iter), "j2se");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }
}

TEST_F(WordDelimiterIteratorTest, PossessiveHandling) {
    {
        char text[] = "O'Neil's";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 7);

        EXPECT_EQ(iter.next(), 1);
        EXPECT_EQ(get_current_subword(iter), "O");
        EXPECT_EQ(iter.next(), 6);
        EXPECT_EQ(get_current_subword(iter), "Neil");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }

    {
        char text[] = "John's";
        WordDelimiterIterator iter(default_table, true, true, false);
        iter.set_text(text, 5);

        EXPECT_EQ(iter.next(), 4);
        EXPECT_EQ(get_current_subword(iter), "John");
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }
}

TEST_F(WordDelimiterIteratorTest, EdgeCases) {
    {
        char text[] = "";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 0);

        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }

    {
        char text[] = "!!!";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 3);

        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }

    {
        char text[] = "A";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 1);

        EXPECT_EQ(iter.next(), 1);
        EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
    }
}

TEST_F(WordDelimiterIteratorTest, CharTypeDetection) {
    using WDI = WordDelimiterIterator;

    EXPECT_EQ(WDI::get_type(u'a'), WDI::LOWER);
    EXPECT_EQ(WDI::get_type(u'Ä'), WDI::UPPER);
    EXPECT_EQ(WDI::get_type(u'5'), WDI::DIGIT);
    EXPECT_EQ(WDI::get_type(u' '), WDI::SUBWORD_DELIM);
    EXPECT_EQ(WDI::get_type(128007), WDI::ALPHA | WDI::DIGIT);
}

TEST_F(WordDelimiterIteratorTest, CustomCharTypeTable) {
    std::vector<char> custom_table(256, WordDelimiterIterator::SUBWORD_DELIM);
    custom_table['_'] = WordDelimiterIterator::LOWER;

    char text[] = "hello_world";
    WordDelimiterIterator iter(custom_table, true, true, true);
    iter.set_text(text, 11);

    EXPECT_EQ(iter.next(), 6);
    EXPECT_EQ(get_current_subword(iter), "_");
    EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
}

TEST_F(WordDelimiterIteratorTest, SingleWordCheck) {
    {
        char text[] = "Hello";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 5);
        iter.next();
        EXPECT_TRUE(iter.is_single_word());
    }

    {
        char text[] = "Test's";
        WordDelimiterIterator iter(default_table, true, true, true);
        iter.set_text(text, 5);
        iter.next();
        EXPECT_TRUE(iter.is_single_word());
    }
}

TEST_F(WordDelimiterIteratorTest, TypeReturn) {
    char text[] = "aB3_";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(text, 4);

    iter.next(); // "a"
    EXPECT_EQ(iter.type(), WordDelimiterIterator::ALPHA);

    iter.next(); // "B"
    EXPECT_EQ(iter.type(), WordDelimiterIterator::ALPHA);

    iter.next(); // "3"
    EXPECT_EQ(iter.type(), WordDelimiterIterator::DIGIT);

    // "_"
    EXPECT_EQ(iter.get_type(u'_'), WordDelimiterIterator::SUBWORD_DELIM);
}

TEST_F(WordDelimiterIteratorTest, SkipPossessive) {
    char text[] = "Test's";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(text, 5);

    iter.next();
    EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
}

TEST_F(WordDelimiterIteratorTest, ComplexScenario) {
    char text[] = "iPhone12Pro's";
    WordDelimiterIterator iter(default_table, true, true, true);
    iter.set_text(text, 12);

    EXPECT_EQ(iter.next(), 1);
    EXPECT_EQ(get_current_subword(iter), "i");
    EXPECT_EQ(iter.next(), 6);
    EXPECT_EQ(get_current_subword(iter), "Phone");
    EXPECT_EQ(iter.next(), 8);
    EXPECT_EQ(get_current_subword(iter), "12");
    EXPECT_EQ(iter.next(), 11);
    EXPECT_EQ(get_current_subword(iter), "Pro");
    EXPECT_EQ(iter.next(), WordDelimiterIterator::DONE);
}

} // namespace doris::segment_v2::inverted_index