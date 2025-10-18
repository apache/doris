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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_util.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_format.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

class PinyinUtilTest : public ::testing::Test {
protected:
    std::string original_dict_path_;

    std::string test_str =
            "正品行货 正品行货 "
            "码完代码，他起身关上电脑，用滚烫的开水为自己泡制一碗腾着热气的老坛酸菜面。中国的程序员"
            "更偏爱拉上窗帘，在黑暗中享受这独特的美食。这是现代工业给一天辛苦劳作的人最好的馈赠。南"
            "方一带生长的程序员虽然在京城多年，但仍口味清淡，他们往往不加料包，由脸颊自然淌下的热泪"
            "补充恰当的盐分。他们相信，用这种方式，能够抹平思考着现在是不是过去想要的未来而带来的大"
            "部分忧伤…小李的父亲在年轻的时候也是从爷爷手里接收了祖传的代码，不过令人惊讶的是，到了"
            "小李这一代，很多东西都遗失了，但是程序员苦逼的味道保存的是如此的完整。 "
            "就在24小时之前，最新的需求从PM处传来，为了得到这份自然的馈赠，码农们开机、写码、调试、"
            "重构，四季轮回的等待换来这难得的丰收时刻。码农知道，需求的保鲜期只有短短的两天，码农们"
            "要以最快的速度对代码进行精致的加工，任何一个需求都可能在24小时之后失去原本的活力，变成"
            "一文不值的垃圾创意。";

    void SetUp() override {
        FLAGS_v = 5;

        original_dict_path_ = config::inverted_index_dict_path;
        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    }

    void TearDown() override { config::inverted_index_dict_path = original_dict_path_; }

    size_t getUtf8CharCount(const std::string& text) {
        size_t char_count = 0;
        int32_t i = 0;
        const char* str = text.c_str();
        int32_t length = static_cast<int32_t>(text.length());
        while (i < length) {
            UChar32 cp;
            U8_NEXT(str, i, length, cp);
            char_count++;
        }
        return char_count;
    }

    std::string list2StringSkipNull(const std::vector<std::string>& list) {
        std::string result;
        for (const auto& item : list) {
            if (!item.empty()) {
                if (!result.empty()) result += " ";
                result += item;
            }
        }
        return result;
    }

    std::string list2String(const std::vector<std::string>& list) {
        std::string result;
        for (size_t i = 0; i < list.size(); ++i) {
            if (i > 0) result += " ";
            result += "\"" + list[i] + "\"";
        }
        return result;
    }

    // Helper function to convert UTF-8 string to Unicode code points vector
    std::vector<UChar32> stringToCodepoints(const std::string& text) {
        std::vector<UChar32> codepoints;
        const char* text_ptr = text.c_str();
        int text_len = static_cast<int>(text.length());
        int byte_pos = 0;

        while (byte_pos < text_len) {
            UChar32 cp;
            U8_NEXT(text_ptr, byte_pos, text_len, cp);
            if (cp < 0) cp = 0xFFFD; // Replace invalid characters
            codepoints.push_back(cp);
        }

        return codepoints;
    }
};

TEST_F(PinyinUtilTest, TestStr2Pinyin) {
    auto& pinyin_util = PinyinUtil::instance();

    std::vector<std::string> parse_result =
            pinyin_util.convert(stringToCodepoints(test_str), PinyinFormat::DEFAULT_PINYIN_FORMAT);

    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(parse_result.size(), expected_length);
}

TEST_F(PinyinUtilTest, TestPinyinStr) {
    auto& pinyin_util = PinyinUtil::instance();

    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(test_str), PinyinFormat::DEFAULT_PINYIN_FORMAT);

    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length);
}

TEST_F(PinyinUtilTest, TestPinyinWithoutTone) {
    auto& pinyin_util = PinyinUtil::instance();

    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(test_str), PinyinFormat::TONELESS_PINYIN_FORMAT);

    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length);
}

TEST_F(PinyinUtilTest, TestStr2FirstCharArr) {
    auto& pinyin_util = PinyinUtil::instance();

    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(test_str), PinyinFormat::ABBR_PINYIN_FORMAT);

    size_t expected_length = getUtf8CharCount(test_str);

    EXPECT_EQ(result.size(), expected_length);
}

TEST_F(PinyinUtilTest, TestInsertPinyin) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string test_phrase = test_str;

    std::vector<std::string> result1 = pinyin_util.convert(stringToCodepoints(test_phrase),
                                                           PinyinFormat::DEFAULT_PINYIN_FORMAT);

    pinyin_util.insertPinyin("行货", {"hang2", "huo4"});

    std::vector<std::string> result2 = pinyin_util.convert(stringToCodepoints(test_phrase),
                                                           PinyinFormat::DEFAULT_PINYIN_FORMAT);

    EXPECT_EQ(result1.size(), result2.size());

    bool found_difference = false;
    for (size_t i = 0; i < std::min(result1.size(), result2.size()); ++i) {
        if (result1[i] != result2[i]) {
            found_difference = true;
        }
    }

    EXPECT_TRUE(found_difference);
}

TEST_F(PinyinUtilTest, TestList2String) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string short_str = "中国程序员";
    std::vector<std::string> pinyin_list = pinyin_util.convert(
            stringToCodepoints(short_str), PinyinFormat::TONELESS_PINYIN_FORMAT);

    std::string result_with_null = list2String(pinyin_list);

    std::string result_skip_null = list2StringSkipNull(pinyin_list);

    EXPECT_FALSE(result_with_null.empty());
    EXPECT_FALSE(result_skip_null.empty());
}

TEST_F(PinyinUtilTest, TestSingleCharPinyin) {
    auto& pinyin_util = PinyinUtil::instance();

    struct TestCase {
        std::string character;
        std::string expected_pinyin_start;
    };

    std::vector<TestCase> test_cases = {{"中", "zhong"}, {"国", "guo"},   {"你", "ni"},
                                        {"好", "hao"},   {"程", "cheng"}, {"序", "xu"},
                                        {"员", "yuan"}};

    for (const auto& test_case : test_cases) {
        std::vector<std::string> result = pinyin_util.convert(
                stringToCodepoints(test_case.character), PinyinFormat::TONELESS_PINYIN_FORMAT);

        EXPECT_EQ(result.size(), 1);

        if (!result.empty()) {
            EXPECT_TRUE(result[0].find(test_case.expected_pinyin_start) == 0);
        }
    }
}

} // namespace doris::segment_v2::inverted_index
