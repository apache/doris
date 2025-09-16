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

#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>

namespace doris::segment_v2::inverted_index {

class StringHelperTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StringHelperTest, EmptyStringConversion) {
    std::string empty_str = "";
    std::wstring wstr = StringHelper::to_wstring(empty_str);
    EXPECT_EQ(wstr.length(), 0);

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, empty_str);
}

TEST_F(StringHelperTest, SingleChineseCharacterConversion) {
    std::string chinese_str = "中";
    std::wstring wstr = StringHelper::to_wstring(chinese_str);

    EXPECT_EQ(chinese_str.length(), 3);
    EXPECT_EQ(wstr.length(), 1);

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, chinese_str);
}

TEST_F(StringHelperTest, SingleChineseCharacterConversion2) {
    std::wstring wstr = L"中";
    std::string chinese_str = StringHelper::to_string(wstr);

    EXPECT_EQ(chinese_str.length(), 3);
    EXPECT_EQ(wstr.length(), 1);

    std::wstring wstr2 = StringHelper::to_wstring(chinese_str);
    EXPECT_EQ(wstr2, wstr);
}

TEST_F(StringHelperTest, MultipleChineseCharactersConversion) {
    std::string chinese_str = "中文测试";
    std::wstring wstr = StringHelper::to_wstring(chinese_str);

    EXPECT_EQ(chinese_str.length(), 12);
    EXPECT_EQ(wstr.length(), 4);

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, chinese_str);
}

TEST_F(StringHelperTest, MixedChineseEnglishConversion) {
    std::string mixed_str = "Hello世界123";
    std::wstring wstr = StringHelper::to_wstring(mixed_str);

    EXPECT_EQ(mixed_str.length(), 14);
    EXPECT_EQ(wstr.length(), 10);

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, mixed_str);
}

TEST_F(StringHelperTest, SpecialChineseCharactersConversion) {
    std::string special_str = "你好，世界！";
    std::wstring wstr = StringHelper::to_wstring(special_str);

    EXPECT_EQ(special_str.length(), 18);
    EXPECT_EQ(wstr.length(), 6);

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, special_str);
}

TEST_F(StringHelperTest, CommonChineseWordsConversion) {
    std::vector<std::string> test_words = {"数据库",   "搜索引擎", "倒排索引",
                                           "全文检索", "字符编码", "Unicode转换"};

    for (const auto& word : test_words) {
        std::wstring wstr = StringHelper::to_wstring(word);
        std::string back_str = StringHelper::to_string(wstr);

        EXPECT_EQ(back_str, word) << "Failed for word: " << word;

        EXPECT_LT(wstr.length(), word.length()) << "Wide string length check failed for: " << word;
    }
}

TEST_F(StringHelperTest, LongChineseStringConversion) {
    std::string long_str =
            "这是一个很长的中文字符串，用来测试字符串转换功能的正确性和性能表现。"
            "包含了各种常见的中文字符，以及标点符号。希望转换后的结果是正确的。";

    std::wstring wstr = StringHelper::to_wstring(long_str);
    std::string back_str = StringHelper::to_string(wstr);

    EXPECT_EQ(back_str, long_str);

    EXPECT_LT(wstr.length(), long_str.length());
}

TEST_F(StringHelperTest, ChineseWithNumbersAndSymbolsConversion) {
    std::string complex_str = "版本号：2.1.3，发布时间：2024年1月15日@北京";
    std::wstring wstr = StringHelper::to_wstring(complex_str);
    std::string back_str = StringHelper::to_string(wstr);

    EXPECT_EQ(back_str, complex_str);

    EXPECT_LT(wstr.length(), complex_str.length());
}

TEST_F(StringHelperTest, ASCIICharactersConversion) {
    std::string ascii_str = "Hello World 123!@#";
    std::wstring wstr = StringHelper::to_wstring(ascii_str);

    EXPECT_EQ(wstr.length(), ascii_str.length());

    std::string back_str = StringHelper::to_string(wstr);
    EXPECT_EQ(back_str, ascii_str);
}

TEST_F(StringHelperTest, PerformanceTest) {
    std::string large_str;
    for (int i = 0; i < 1000; ++i) {
        large_str += "测试";
    }

    std::wstring wstr = StringHelper::to_wstring(large_str);
    std::string back_str = StringHelper::to_string(wstr);

    EXPECT_EQ(back_str, large_str);
    EXPECT_EQ(wstr.length(), 2000);
}

} // namespace doris::segment_v2::inverted_index