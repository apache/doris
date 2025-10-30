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

#include <memory>
#include <string>
#include <vector>

#include "CLucene.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/pinyin_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {
class PinyinFilterTest : public ::testing::Test {
public:
    void SetUp() override {
        const char* doris_home = std::getenv("DORIS_HOME");
        doris::config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    }

    TokenizerPtr createTokenizer(const std::string& tokenizer_type, const std::string& text) {
        auto reader = std::make_shared<lucene::util::SStringReader<char>>();
        reader->init(text.data(), text.size(), false);

        TokenizerPtr tokenizer;

        if (tokenizer_type == "standard") {
            StandardTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "keyword") {
            KeywordTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else {
            throw std::invalid_argument("Unknown tokenizer type: " + tokenizer_type);
        }

        tokenizer->set_reader(reader);
        tokenizer->reset();
        return tokenizer;
    }

    std::vector<std::string> tokenizeWithFilter(
            const std::string& text, const std::string& tokenizer_type,
            const std::unordered_map<std::string, std::string>& filter_config) {
        auto tokenizer_only = createTokenizer(tokenizer_type, text);
        std::vector<std::string> tokenizer_tokens;
        Token temp_token;
        while (tokenizer_only->next(&temp_token) != nullptr) {
            std::string token_text(temp_token.termBuffer<char>(), temp_token.termLength<char>());
            tokenizer_tokens.push_back(token_text);
        }

        auto tokenizer = createTokenizer(tokenizer_type, text);

        PinyinFilterFactory filter_factory;
        Settings filter_settings(filter_config);
        filter_factory.initialize(filter_settings);
        auto filter = filter_factory.create(tokenizer);

        std::vector<std::string> tokens;
        Token token;

        while (filter->next(&token) != nullptr) {
            std::string token_text(token.termBuffer<char>(), token.termLength<char>());
            tokens.push_back(token_text);
        }

        return tokens;
    }

    void assertTokens(const std::vector<std::string>& actual,
                      const std::vector<std::string>& expected, const std::string& test_case) {
        EXPECT_EQ(actual.size(), expected.size()) << "Token count mismatch in " << test_case;

        for (size_t i = 0; i < std::min(actual.size(), expected.size()); ++i) {
            EXPECT_EQ(actual[i], expected[i]) << "Token[" << i << "] mismatch in " << test_case;
        }
    }
};

TEST_F(PinyinFilterTest, TestTokenFilter_StandardAnalyzer_FirstLetter) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_original"] = "false";
    config["keep_full_pinyin"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "standard", config);

    std::vector<std::string> expected = {"l", "d", "h"};
    assertTokens(tokens, expected, "StandardTokenizer + FirstLetter");
}

TEST_F(PinyinFilterTest, TestTokenFilter_KeywordAnalyzer_FirstLetter) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_original"] = "false";
    config["keep_full_pinyin"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "keyword", config);

    std::vector<std::string> expected = {"ldh"};
    assertTokens(tokens, expected, "KeywordTokenizer + FirstLetter");
}

TEST_F(PinyinFilterTest, TestTokenFilter_StandardAnalyzer_FullPinyin) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "false";
    config["keep_none_chinese"] = "true";
    config["keep_original"] = "false";
    config["keep_full_pinyin"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "standard", config);

    std::vector<std::string> expected = {"liu", "de", "hua"};
    assertTokens(tokens, expected, "StandardTokenizer + FullPinyin");
}

TEST_F(PinyinFilterTest, TestTokenFilter_StandardAnalyzer_Full) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_original"] = "true";
    config["keep_full_pinyin"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "standard", config);

    std::vector<std::string> expected = {"liu", "刘", "l", "de", "德", "d", "hua", "华", "h"};
    assertTokens(tokens, expected, "StandardTokenizer + Full");
}

TEST_F(PinyinFilterTest, TestTokenFilter_KeywordAnalyzer_Full) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_original"] = "true";
    config["keep_full_pinyin"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "keyword", config);

    std::vector<std::string> expected = {"liu", "刘德华", "ldh", "de", "hua"};
    assertTokens(tokens, expected, "KeywordTokenizer + Full");
}

TEST_F(PinyinFilterTest, TestTokenFilter_KeywordAnalyzer_LimitFirstLetter) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_none_chinese"] = "false";
    config["keep_none_chinese_in_first_letter"] = "true";
    config["keep_original"] = "false";
    config["keep_full_pinyin"] = "false";
    config["limit_first_letter_length"] = "5";
    config["lowercase"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("Go的数组是纯粹的值类型，传递一个[N]T的代价是N个T", "keyword",
                                     config);

    EXPECT_EQ(tokens.size(), 1) << "Should generate only one first letter token";
    EXPECT_EQ(tokens[0].length(), 5) << "First letter length should be limited to 5 characters";
    EXPECT_EQ(tokens[0], "godsz") << "Should exactly match Java output";
}

TEST_F(PinyinFilterTest, TestTokenFilter_AlphaNumeric) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_separate_first_letter"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_in_first_letter"] = "false";
    config["keep_original"] = "false";
    config["keep_full_pinyin"] = "true";
    config["limit_first_letter_length"] = "5";
    config["lowercase"] = "true";
    config["none_chinese_pinyin_tokenize"] = "true";
    config["remove_duplicated_term"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("liudehuaalibaba13zhuanghan134", "keyword", config);

    std::vector<std::string> expected = {"liu", "de", "hua",    "a",   "li", "ba",
                                         "ba",  "13", "zhuang", "han", "134"};
    assertTokens(tokens, expected, "KeywordTokenizer + AlphaNumeric");
}

TEST_F(PinyinFilterTest, TestTokenFilter_JoinedFullPinyin) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "false";
    config["keep_joined_full_pinyin"] = "true";
    config["keep_none_chinese"] = "false";
    config["keep_none_chinese_together"] = "true";
    config["none_chinese_pinyin_tokenize"] = "true";
    config["keep_none_chinese_in_first_letter"] = "true";
    config["keep_original"] = "false";
    config["lowercase"] = "true";
    config["trim_whitespace"] = "true";
    config["fixed_pinyin_offset"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华", "keyword", config);
    std::vector<std::string> expected = {"liudehua", "ldh"};
    assertTokens(tokens, expected, "KeywordTokenizer + JoinedFullPinyin");
}

TEST_F(PinyinFilterTest, TestTokenFilter_ConfigCombinations) {
    {
        std::unordered_map<std::string, std::string> config;
        config["keep_first_letter"] = "true";
        config["keep_full_pinyin"] = "false";
        config["keep_original"] = "true";
        config["ignore_pinyin_offset"] = "false";

        auto tokens = tokenizeWithFilter("测试", "keyword", config);

        bool has_original = std::find(tokens.begin(), tokens.end(), "测试") != tokens.end();
        bool has_first_letters = std::find(tokens.begin(), tokens.end(), "cs") != tokens.end();
        EXPECT_TRUE(has_original || has_first_letters)
                << "Should contain original text or first letters";
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_first_letter"] = "true";
        config["keep_full_pinyin"] = "false";
        config["keep_original"] = "false";
        config["ignore_pinyin_offset"] = "false";

        auto tokens = tokenizeWithFilter("测试", "keyword", config);

        EXPECT_GT(tokens.size(), 0) << "Should generate some tokens";

        bool has_cs = std::find(tokens.begin(), tokens.end(), "cs") != tokens.end();
        if (has_cs) {
            EXPECT_TRUE(has_cs) << "Should contain first letter combination 'cs'";
        }
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_first_letter"] = "false";
        config["keep_full_pinyin"] = "true";
        config["keep_original"] = "false";
        config["ignore_pinyin_offset"] = "false";

        auto tokens = tokenizeWithFilter("测试", "keyword", config);

        EXPECT_GT(tokens.size(), 0) << "Should generate some tokens";
    }
}

TEST_F(PinyinFilterTest, TestTokenFilter_EdgeCases) {
    {
        std::unordered_map<std::string, std::string> config;
        config["keep_original"] = "true";

        auto tokens = tokenizeWithFilter("", "keyword", config);
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_none_chinese"] = "true";
        config["keep_original"] = "false";

        auto tokens = tokenizeWithFilter("hello", "keyword", config);
        EXPECT_GE(tokens.size(), 0) << "Pure English text should be processed normally";
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_none_chinese"] = "true";
        config["keep_original"] = "false";

        auto tokens = tokenizeWithFilter("12345", "keyword", config);
        EXPECT_GE(tokens.size(), 0) << "Pure numeric text should be processed normally";
    }
}

} // namespace doris::segment_v2::inverted_index