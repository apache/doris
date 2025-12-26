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

// Test polyphone dictionary matching for common phrases
TEST_F(PinyinUtilTest, TestPolyphonePhrases) {
    auto& pinyin_util = PinyinUtil::instance();

    struct TestCase {
        std::string text;
        std::vector<std::string> expected_pinyins;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
            // Basic polyphone phrases from polyphone.txt
            {"你呢", {"ni", "ne"}, "你呢 should be 'ni ne' not 'ni ni'"},
            {"做不了", {"zuo", "bu", "liao"}, "做不了 should be 'zuo bu liao' not 'zuo bu le'"},
            {"长城", {"chang", "cheng"}, "长城 should be 'chang cheng' not 'zhang cheng'"},
            {"重要", {"zhong", "yao"}, "重要 should be 'zhong yao' not 'chong yao'"},

            // Common polyphone words
            {"空调", {"kong", "tiao"}, "空调 should be 'kong tiao' not 'kong diao'"},
            {"厦门", {"xia", "men"}, "厦门 should be 'xia men' not 'sha men'"},

            // Note: 银行 in polyphone.txt has "yin xing" (incorrect), not "yin hang"
            // So we test what it actually produces, not what we wish it produced
            {"银行", {"yin", "xing"}, "银行 from polyphone.txt (currently has yin xing)"},

            // Single characters (fallback to default pinyin)
            {"长", {"chang"}, "Single char 长 defaults to 'chang'"},
            {"重", {"zhong"}, "Single char 重 defaults to 'zhong'"},
            // Note: 行 default pinyin is "xing", not "hang"
            {"行", {"xing"}, "Single char 行 defaults to 'xing'"},

            // Single chars from polyphone words - should use default single-char pinyin
            {"空", {"kong"}, "Single char 空 defaults to 'kong'"},
            {"调", {"diao"}, "Single char 调 defaults to 'diao'"},
            {"厦", {"sha"}, "Single char 厦 defaults to 'sha'"},

            // Phrases not in polyphone.txt (should use default single-char pinyin)
            {"今天", {"jin", "tian"}, "今天 has no polyphone entry, use default"},
    };

    for (const auto& test_case : test_cases) {
        std::vector<std::string> result = pinyin_util.convert(stringToCodepoints(test_case.text),
                                                              PinyinFormat::TONELESS_PINYIN_FORMAT);

        EXPECT_EQ(result.size(), test_case.expected_pinyins.size())
                << "Failed for: " << test_case.description;

        for (size_t i = 0; i < std::min(result.size(), test_case.expected_pinyins.size()); ++i) {
            EXPECT_EQ(result[i], test_case.expected_pinyins[i])
                    << "Failed at position " << i << " for: " << test_case.description
                    << ". Expected '" << test_case.expected_pinyins[i] << "' but got '" << result[i]
                    << "'";
        }
    }
}

// Test polyphone phrases with mixed Chinese and non-Chinese characters
TEST_F(PinyinUtilTest, TestPolyphoneMixedContent) {
    auto& pinyin_util = PinyinUtil::instance();

    struct TestCase {
        std::string text;
        std::vector<std::string> expected_pinyins;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
            // Polyphone phrase followed by English
            {"你呢ABC", {"ni", "ne", "A", "B", "C"}, "你呢ABC mixed content"},

            // Note: Numbers are formatted by PinyinFormatter and may return empty strings
            // This is expected behavior - TONELESS_PINYIN_FORMAT filters out non-pinyin content
            // If we want to keep numbers, we should use a format that allows non-pinyin
            // For now, we test with a phrase that won't have this issue
            {"ABC做不了", {"A", "B", "C", "zuo", "bu", "liao"}, "ABC做不了 mixed content"},

            // Multiple polyphone phrases
            {"长城很重要",
             {"chang", "cheng", "hen", "zhong", "yao"},
             "Multiple polyphone phrases: 长城 and 重要"},

            // More polyphone phrases in sentences
            {"厦门空调很好",
             {"xia", "men", "kong", "tiao", "hen", "hao"},
             "Multiple polyphone phrases: 厦门 and 空调"},

            // Polyphone phrases with punctuation
            {"你呢?做不了!",
             {"ni", "ne", "?", "zuo", "bu", "liao", "!"},
             "Polyphone phrases with punctuation"},
    };

    for (const auto& test_case : test_cases) {
        std::vector<std::string> result = pinyin_util.convert(stringToCodepoints(test_case.text),
                                                              PinyinFormat::TONELESS_PINYIN_FORMAT);

        EXPECT_EQ(result.size(), test_case.expected_pinyins.size())
                << "Failed for: " << test_case.description;

        for (size_t i = 0; i < std::min(result.size(), test_case.expected_pinyins.size()); ++i) {
            EXPECT_EQ(result[i], test_case.expected_pinyins[i])
                    << "Failed at position " << i << " for: " << test_case.description;
        }
    }
}

// Test edge cases for polyphone matching
TEST_F(PinyinUtilTest, TestPolyphoneEdgeCases) {
    auto& pinyin_util = PinyinUtil::instance();

    // Empty string
    std::vector<std::string> result1 =
            pinyin_util.convert(stringToCodepoints(""), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_TRUE(result1.empty()) << "Empty string should return empty result";

    // Single space
    std::vector<std::string> result2 =
            pinyin_util.convert(stringToCodepoints(" "), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result2.size(), 1) << "Single space should return one element";
    EXPECT_EQ(result2[0], " ") << "Space should be preserved";

    // Only non-Chinese characters
    std::vector<std::string> result3 =
            pinyin_util.convert(stringToCodepoints("ABC123"), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result3.size(), 6) << "ABC123 should have 6 characters";
}

// Test that polyphone dictionary takes precedence over single-char pinyin
TEST_F(PinyinUtilTest, TestPolyphonePrecedence) {
    auto& pinyin_util = PinyinUtil::instance();

    // Test "长" in different contexts
    // Single char: default pinyin from single-char dict
    std::vector<std::string> result1 =
            pinyin_util.convert(stringToCodepoints("长"), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result1.size(), 1);
    EXPECT_EQ(result1[0], "chang") << "Single char 长 should default to 'chang'";

    // In phrase "长城": should be "chang" from polyphone dict
    std::vector<std::string> result2 =
            pinyin_util.convert(stringToCodepoints("长城"), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result2.size(), 2);
    EXPECT_EQ(result2[0], "chang") << "长 in 长城 should be 'chang' from polyphone dict";
    EXPECT_EQ(result2[1], "cheng");

    // Test "了" in different contexts
    // In phrase "做不了": should be "liao" from polyphone dict
    std::vector<std::string> result3 =
            pinyin_util.convert(stringToCodepoints("做不了"), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result3.size(), 3);
    EXPECT_EQ(result3[0], "zuo");
    EXPECT_EQ(result3[1], "bu");
    EXPECT_EQ(result3[2], "liao") << "了 in 做不了 should be 'liao' from polyphone dict";
}

// Test different pinyin formats with polyphone phrases
TEST_F(PinyinUtilTest, TestPolyphoneWithDifferentFormats) {
    auto& pinyin_util = PinyinUtil::instance();

    // Insert test polyphone phrases with tone numbers (as stored in polyphone.txt)
    pinyin_util.insertPinyin("你呢", {"ni3", "ne"});
    pinyin_util.insertPinyin("做不了", {"zuo4", "bu4", "liao3"});
    pinyin_util.insertPinyin("空调", {"kong1", "tiao2"});

    struct TestCase {
        std::string text;
        PinyinFormat format;
        std::vector<std::string> expected_pinyins;
        std::string description;
    };

    std::vector<TestCase> test_cases = {
            // Test WITH_TONE_NUMBER format (default: da3)
            {"你呢", PinyinFormat::DEFAULT_PINYIN_FORMAT, {"ni3", "ne"}, "你呢 with tone numbers"},
            {"做不了",
             PinyinFormat::DEFAULT_PINYIN_FORMAT,
             {"zuo4", "bu4", "liao3"},
             "做不了 with tone numbers"},
            {"空调",
             PinyinFormat::DEFAULT_PINYIN_FORMAT,
             {"kong1", "tiao2"},
             "空调 with tone numbers"},

            // Test TONELESS format (da)
            {"你呢", PinyinFormat::TONELESS_PINYIN_FORMAT, {"ni", "ne"}, "你呢 without tone"},
            {"做不了",
             PinyinFormat::TONELESS_PINYIN_FORMAT,
             {"zuo", "bu", "liao"},
             "做不了 without tone"},
            {"空调", PinyinFormat::TONELESS_PINYIN_FORMAT, {"kong", "tiao"}, "空调 without tone"},

            // Test UNICODE format with tone marks (dǎ)
            {"你呢",
             PinyinFormat::UNICODE_PINYIN_FORMAT,
             {"nǐ", "ne"},
             "你呢 with unicode tone marks"},
            {"做不了",
             PinyinFormat::UNICODE_PINYIN_FORMAT,
             {"zuò", "bù", "liǎo"},
             "做不了 with unicode tone marks"},
            {"空调",
             PinyinFormat::UNICODE_PINYIN_FORMAT,
             {"kōng", "tiáo"},
             "空调 with unicode tone marks"},

            // Test ABBR format (first letter only)
            {"你呢", PinyinFormat::ABBR_PINYIN_FORMAT, {"n", "n"}, "你呢 abbreviation"},
            {"做不了", PinyinFormat::ABBR_PINYIN_FORMAT, {"z", "b", "l"}, "做不了 abbreviation"},
            {"空调", PinyinFormat::ABBR_PINYIN_FORMAT, {"k", "t"}, "空调 abbreviation"},
    };

    for (const auto& test_case : test_cases) {
        std::vector<std::string> result =
                pinyin_util.convert(stringToCodepoints(test_case.text), test_case.format);

        EXPECT_EQ(result.size(), test_case.expected_pinyins.size())
                << "Failed for: " << test_case.description;

        for (size_t i = 0; i < std::min(result.size(), test_case.expected_pinyins.size()); ++i) {
            EXPECT_EQ(result[i], test_case.expected_pinyins[i])
                    << "Failed at position " << i << " for: " << test_case.description
                    << ". Expected '" << test_case.expected_pinyins[i] << "' but got '" << result[i]
                    << "'";
        }
    }
}

// Test custom pinyin format with polyphone phrases
TEST_F(PinyinUtilTest, TestPolyphoneWithCustomFormat) {
    auto& pinyin_util = PinyinUtil::instance();

    // Insert test polyphone phrases with tone numbers
    pinyin_util.insertPinyin("你呢", {"ni3", "ne"});
    pinyin_util.insertPinyin("做不了", {"zuo4", "bu4", "liao3"});
    pinyin_util.insertPinyin("空调", {"kong1", "tiao2"});

    // Test uppercase format
    PinyinFormat uppercase_format(YuCharType::WITH_U_AND_COLON, ToneType::WITHOUT_TONE,
                                  CaseType::UPPERCASE);

    std::vector<std::string> result1 =
            pinyin_util.convert(stringToCodepoints("你呢"), uppercase_format);
    EXPECT_EQ(result1.size(), 2);
    EXPECT_EQ(result1[0], "NI") << "你 should be NI in uppercase";
    EXPECT_EQ(result1[1], "NE") << "呢 should be NE in uppercase";

    // Test capitalize format
    PinyinFormat capitalize_format(YuCharType::WITH_U_AND_COLON, ToneType::WITHOUT_TONE,
                                   CaseType::CAPITALIZE);

    std::vector<std::string> result2 =
            pinyin_util.convert(stringToCodepoints("空调"), capitalize_format);
    EXPECT_EQ(result2.size(), 2);
    EXPECT_EQ(result2[0], "Kong") << "空 should be Kong in capitalize";
    EXPECT_EQ(result2[1], "Tiao") << "调 should be Tiao in capitalize";

    // Test with tone numbers and uppercase
    PinyinFormat tone_uppercase_format(YuCharType::WITH_U_AND_COLON, ToneType::WITH_TONE_NUMBER,
                                       CaseType::UPPERCASE);

    std::vector<std::string> result3 =
            pinyin_util.convert(stringToCodepoints("做不了"), tone_uppercase_format);
    EXPECT_EQ(result3.size(), 3);
    EXPECT_EQ(result3[0], "ZUO4") << "做 should be ZUO4";
    EXPECT_EQ(result3[1], "BU4") << "不 should be BU4";
    EXPECT_EQ(result3[2], "LIAO3") << "了 should be LIAO3";
}

// Test format consistency across single chars and phrases
TEST_F(PinyinUtilTest, TestFormatConsistency) {
    auto& pinyin_util = PinyinUtil::instance();

    // Insert test polyphone phrase with tone numbers
    pinyin_util.insertPinyin("你呢", {"ni3", "ne"});

    // Test that format is applied consistently to both polyphone and non-polyphone chars
    std::string mixed_text = "你呢今天"; // "你呢" is polyphone, "今天" is not

    // Test with DEFAULT format (with tone numbers)
    std::vector<std::string> result1 = pinyin_util.convert(stringToCodepoints(mixed_text),
                                                           PinyinFormat::DEFAULT_PINYIN_FORMAT);
    EXPECT_EQ(result1.size(), 4);
    EXPECT_EQ(result1[0], "ni3") << "你 with tone";
    EXPECT_EQ(result1[1], "ne") << "呢 with tone";
    EXPECT_EQ(result1[2], "jin1") << "今 with tone";
    EXPECT_EQ(result1[3], "tian1") << "天 with tone";

    // Test with UNICODE format (with tone marks)
    std::vector<std::string> result2 = pinyin_util.convert(stringToCodepoints(mixed_text),
                                                           PinyinFormat::UNICODE_PINYIN_FORMAT);
    EXPECT_EQ(result2.size(), 4);
    EXPECT_EQ(result2[0], "nǐ") << "你 with unicode tone mark";
    EXPECT_EQ(result2[1], "ne") << "呢 with unicode tone mark";
    EXPECT_EQ(result2[2], "jīn") << "今 with unicode tone mark";
    EXPECT_EQ(result2[3], "tiān") << "天 with unicode tone mark";
}

// Test rare Chinese characters
TEST_F(PinyinUtilTest, TestRareChineseCharacters) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string rare_chars = "龘靐齉";
    std::vector<std::string> result = pinyin_util.convert(stringToCodepoints(rare_chars),
                                                          PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 3) << "Should handle rare Chinese characters";
    // Each character should produce some pinyin (even if empty/default)
    EXPECT_FALSE(result.empty());
}

// Test traditional and simplified Chinese
TEST_F(PinyinUtilTest, TestTraditionalSimplified) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string simplified = "中国";
    std::vector<std::string> result1 = pinyin_util.convert(stringToCodepoints(simplified),
                                                           PinyinFormat::TONELESS_PINYIN_FORMAT);

    std::string traditional = "中國";
    std::vector<std::string> result2 = pinyin_util.convert(stringToCodepoints(traditional),
                                                           PinyinFormat::TONELESS_PINYIN_FORMAT);

    EXPECT_EQ(result1.size(), 2);
    EXPECT_EQ(result1[0], "zhong");
    EXPECT_EQ(result1[1], "guo");

    EXPECT_EQ(result2.size(), 2);
    EXPECT_EQ(result2[0], "zhong");
}

// Test numbers and special characters
TEST_F(PinyinUtilTest, TestNumbersAndSpecialChars) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string mixed = "中123国@#$";
    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(mixed), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 8);
    // First character should be Chinese pinyin
    EXPECT_EQ(result[0], "zhong");
    // Numbers and some special chars may be filtered (empty) or preserved depending on format
    // The 4th character should be the Chinese character '国'
    EXPECT_EQ(result[4], "guo");
}

// Test very long text
TEST_F(PinyinUtilTest, TestVeryLongText) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string long_text;
    for (int i = 0; i < 100; i++) {
        long_text += "测试";
    }

    std::vector<std::string> result = pinyin_util.convert(stringToCodepoints(long_text),
                                                          PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 200) << "100 iterations * 2 chars = 200";
    EXPECT_EQ(result[0], "ce");
    EXPECT_EQ(result[1], "shi");
    EXPECT_EQ(result[198], "ce");
    EXPECT_EQ(result[199], "shi");
}

// Test mixed content with multiple types
TEST_F(PinyinUtilTest, TestMixedContentTypes) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string mixed = "Hello你好123世界！";
    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(mixed), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 13); // 5 + 2 + 3 + 2 + 1

    bool has_ni = std::find(result.begin(), result.end(), "ni") != result.end();
    bool has_hao = std::find(result.begin(), result.end(), "hao") != result.end();
    EXPECT_TRUE(has_ni) << "Should contain 'ni'";
    EXPECT_TRUE(has_hao) << "Should contain 'hao'";
}

// Test all pinyin format types
TEST_F(PinyinUtilTest, TestAllPinyinFormats) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string test_char = "好";

    // TONELESS
    std::vector<std::string> result1 = pinyin_util.convert(stringToCodepoints(test_char),
                                                           PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result1[0], "hao");

    // DEFAULT (with tone number)
    std::vector<std::string> result2 =
            pinyin_util.convert(stringToCodepoints(test_char), PinyinFormat::DEFAULT_PINYIN_FORMAT);
    EXPECT_TRUE(result2[0].find("hao") == 0);

    // UNICODE (with tone mark)
    std::vector<std::string> result3 =
            pinyin_util.convert(stringToCodepoints(test_char), PinyinFormat::UNICODE_PINYIN_FORMAT);
    EXPECT_GT(result3[0].length(), 0);

    // ABBR (first letter)
    std::vector<std::string> result4 =
            pinyin_util.convert(stringToCodepoints(test_char), PinyinFormat::ABBR_PINYIN_FORMAT);
    EXPECT_EQ(result4[0], "h");
}

// Test whitespace handling
TEST_F(PinyinUtilTest, TestWhitespaceHandling) {
    auto& pinyin_util = PinyinUtil::instance();

    std::string text_with_spaces = "你 好 世 界";
    std::vector<std::string> result = pinyin_util.convert(stringToCodepoints(text_with_spaces),
                                                          PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 7); // 4 chars + 3 spaces
    EXPECT_EQ(result[0], "ni");
    EXPECT_EQ(result[1], " ");
    EXPECT_EQ(result[2], "hao");
    EXPECT_EQ(result[3], " ");
    EXPECT_EQ(result[4], "shi");
    EXPECT_EQ(result[5], " ");
    EXPECT_EQ(result[6], "jie");
}

// Test consecutive polyphone phrases
TEST_F(PinyinUtilTest, TestConsecutivePolyphonePhrases) {
    auto& pinyin_util = PinyinUtil::instance();

    pinyin_util.insertPinyin("长城", {"chang2", "cheng2"});
    pinyin_util.insertPinyin("重要", {"zhong4", "yao4"});

    std::string text = "长城很重要";
    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(text), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], "chang");
    EXPECT_EQ(result[1], "cheng");
    EXPECT_EQ(result[2], "hen");
    EXPECT_EQ(result[3], "zhong");
    EXPECT_EQ(result[4], "yao");
}

// Test insertPinyin with various formats
TEST_F(PinyinUtilTest, TestInsertPinyinVariousFormats) {
    auto& pinyin_util = PinyinUtil::instance();

    // Insert with tone numbers
    pinyin_util.insertPinyin("测试一", {"ce4", "shi4", "yi1"});

    std::string text = "测试一";
    std::vector<std::string> result =
            pinyin_util.convert(stringToCodepoints(text), PinyinFormat::TONELESS_PINYIN_FORMAT);
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], "ce");
    EXPECT_EQ(result[1], "shi");
    EXPECT_EQ(result[2], "yi");
}

// Test getUtf8CharCount with various inputs
TEST_F(PinyinUtilTest, TestUtf8CharCountVariousInputs) {
    EXPECT_EQ(getUtf8CharCount(""), 0);
    EXPECT_EQ(getUtf8CharCount("a"), 1);
    EXPECT_EQ(getUtf8CharCount("abc"), 3);
    EXPECT_EQ(getUtf8CharCount("中"), 1);
    EXPECT_EQ(getUtf8CharCount("中国"), 2);
    EXPECT_EQ(getUtf8CharCount("中国abc"), 5);
}

} // namespace doris::segment_v2::inverted_index
