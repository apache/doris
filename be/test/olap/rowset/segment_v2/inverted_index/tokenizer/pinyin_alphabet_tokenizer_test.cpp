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

#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"

namespace doris::segment_v2::inverted_index {

class PinyinAlphabetTokenizerTest : public ::testing::Test {
protected:
    std::string original_dict_path_;
    void SetUp() override {
        original_dict_path_ = config::inverted_index_dict_path;
        const char* doris_home = std::getenv("DORIS_HOME");
        config::inverted_index_dict_path = std::string(doris_home) + "../../dict";

        PinyinAlphabetDict::instance();
    }

    void TearDown() override { config::inverted_index_dict_path = original_dict_path_; }

    std::string vectorToString(const std::vector<std::string>& vec) {
        if (vec.empty()) {
            return "[]";
        }

        std::string result = "[";
        for (size_t i = 0; i < vec.size(); ++i) {
            if (i > 0) {
                result += ", ";
            }
            result += vec[i];
        }
        result += "]";
        return result;
    }

    void assertTokensEqual(const std::vector<std::string>& expected,
                           const std::vector<std::string>& actual, const std::string& input) {
        EXPECT_EQ(expected.size(), actual.size())
                << "Token count mismatch for input: '" << input << "'\n"
                << "Expected: " << vectorToString(expected) << "\n"
                << "Actual: " << vectorToString(actual);

        for (size_t i = 0; i < std::min(expected.size(), actual.size()); ++i) {
            EXPECT_EQ(expected[i], actual[i])
                    << "Token mismatch at position " << i << " for input: '" << input << "'\n"
                    << "Expected: " << vectorToString(expected) << "\n"
                    << "Actual: " << vectorToString(actual);
        }
    }
};

TEST_F(PinyinAlphabetTokenizerTest, TestSinglePinyin) {
    std::string input = "xian";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"xian"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestContinuousPinyin) {
    std::string input = "woshiliang";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"wo", "shi", "liang"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestLongPinyinString) {
    std::string input = "zhonghuarenmingongheguo";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"zhong", "hua", "ren", "min", "gong", "he", "guo"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestMixedWithNumbers) {
    std::string input = "5zhonghuaren89mingongheguo234";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"5",   "zhong", "hua", "ren", "89",
                                         "min", "gong",  "he",  "guo", "234"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestEmptyString) {
    std::string input = "";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestOnlyNumbers) {
    std::string input = "12345";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"12345"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestCaseHandling) {
    std::string input = "WoShiLiang";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"wo", "shi", "liang"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestWithSpecialCharacters) {
    std::string input = "wo-shi_liang.txt";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"wo", "-", "shi", "_", "liang", ".", "t", "x", "t"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestSingleCharacter) {
    std::string input = "a";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"a"};

    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestComplexMixed) {
    std::string input = "hello123world-ni456hao";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"he", "l", "lo", "123", "wo",  "r",
                                         "l",  "d", "-",  "ni",  "456", "hao"};
    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestVeryLongPinyinSequence) {
    std::string input = "zhonghuarenmingongheguoguogehenhaoting";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"zhong", "hua", "ren", "min", "gong", "he",
                                         "guo",   "guo", "ge",  "hen", "hao",  "ting"};
    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestMultipleNumbers) {
    std::string input = "123abc456def789";
    auto result = PinyinAlphabetTokenizer::walk(input);

    EXPECT_GE(result.size(), 5) << "Should have at least 5 segments";
    EXPECT_EQ(result[0], "123");
    bool has_456 = std::find(result.begin(), result.end(), "456") != result.end();
    bool has_789 = std::find(result.begin(), result.end(), "789") != result.end();
    EXPECT_TRUE(has_456) << "Should contain '456'";
    EXPECT_TRUE(has_789) << "Should contain '789'";
}

TEST_F(PinyinAlphabetTokenizerTest, TestSpecialCharactersOnly) {
    std::string input = "!@#$%^&*()";
    auto result = PinyinAlphabetTokenizer::walk(input);
    // Consecutive special characters are kept together as one token
    EXPECT_EQ(result.size(), 1) << "Should return special characters as one token";
    EXPECT_EQ(result[0], "!@#$%^&*()");
}

TEST_F(PinyinAlphabetTokenizerTest, TestSingleLetter) {
    std::string input = "x";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"x"};
    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestUncommonPinyinCombinations) {
    std::string input = "chuangzhuangshuangnuan";
    auto result = PinyinAlphabetTokenizer::walk(input);
    std::vector<std::string> expected = {"chuang", "zhuang", "shuang", "nuan"};
    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestRepeatedCharacters) {
    std::string input = "aaaaabbbbbccccc";
    auto result = PinyinAlphabetTokenizer::walk(input);

    // Should be tokenized as individual 'a's or combined, depends on implementation
    EXPECT_GE(result.size(), 1) << "Should handle repeated characters";
    // Check that result contains 'a' in some form
    bool has_a = false;
    for (const auto& token : result) {
        if (token.find("a") != std::string::npos) {
            has_a = true;
            break;
        }
    }
    EXPECT_TRUE(has_a) << "Should contain 'a' characters";
}

TEST_F(PinyinAlphabetTokenizerTest, TestMixedCase) {
    std::string input = "NiHaoShiJie";
    auto result = PinyinAlphabetTokenizer::walk(input);
    // Case is converted to lowercase
    std::vector<std::string> expected = {"ni", "hao", "shi", "jie"};
    assertTokensEqual(expected, result, input);
}

TEST_F(PinyinAlphabetTokenizerTest, TestLeadingTrailingNumbers) {
    std::string input = "123nihao456";
    auto result = PinyinAlphabetTokenizer::walk(input);

    EXPECT_GE(result.size(), 3);
    EXPECT_EQ(result[0], "123") << "Should start with '123'";

    bool has_ni = std::find(result.begin(), result.end(), "ni") != result.end();
    bool has_hao = std::find(result.begin(), result.end(), "hao") != result.end();
    EXPECT_TRUE(has_ni) << "Should contain 'ni'";
    EXPECT_TRUE(has_hao) << "Should contain 'hao'";

    EXPECT_EQ(result.back(), "456") << "Should end with '456'";
}

TEST_F(PinyinAlphabetTokenizerTest, TestOnlySpecialChars) {
    std::string input = "___---...";
    auto result = PinyinAlphabetTokenizer::walk(input);
    // Consecutive special characters are kept together as one token
    EXPECT_EQ(result.size(), 1) << "Should return special characters as one token";
    EXPECT_EQ(result[0], "___---...");
}

} // namespace doris::segment_v2::inverted_index
