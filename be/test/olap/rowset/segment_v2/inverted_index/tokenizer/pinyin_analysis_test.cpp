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

#include <algorithm>
#include <chrono>
#include <map>
#include <set>
#include <unordered_map>

#include "common/config.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_format.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_util.h"
#include "unicode/utf8.h"

using namespace doris::segment_v2;
using namespace doris::segment_v2::inverted_index;

class PinyinAnalysisTest : public ::testing::Test {
public:
    void SetUp() override {
        original_dict_path_ = doris::config::inverted_index_dict_path;
        const char* doris_home = std::getenv("DORIS_HOME");
        doris::config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    }

    void TearDown() override { doris::config::inverted_index_dict_path = original_dict_path_; }

private:
    std::string original_dict_path_;

protected:
    struct TokenWithPosition {
        std::string term;
        int startOffset;
        int endOffset;
        int position;
    };

    std::vector<TokenWithPosition> tokenizeWithDetails(PinyinTokenizerFactory& factory,
                                                       const std::string& text) {
        std::vector<TokenWithPosition> tokens;
        auto tokenizer = factory.create();
        {
            auto reader = std::make_shared<lucene::util::SStringReader<char>>();
            reader->init(text.data(), text.size(), false);
            tokenizer->set_reader(reader);
            tokenizer->reset();

            Token t;
            int cumulative_position = 0;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                cumulative_position += t.getPositionIncrement();
                TokenWithPosition token_detail;
                token_detail.term = term;
                token_detail.startOffset = t.startOffset();
                token_detail.endOffset = t.endOffset();
                token_detail.position = cumulative_position;
                tokens.emplace_back(token_detail);
            }
        }
        return tokens;
    }

    std::vector<std::string> tokenize(PinyinTokenizerFactory& factory, const std::string& text) {
        std::vector<std::string> tokens;
        auto tokenizer = factory.create();
        {
            auto reader = std::make_shared<lucene::util::SStringReader<char>>();
            reader->init(text.data(), text.size(), false);
            tokenizer->set_reader(reader);
            tokenizer->reset();

            Token t;
            while (tokenizer->next(&t)) {
                std::string term(t.termBuffer<char>(), t.termLength<char>());
                tokens.emplace_back(term);
            }
        }
        return tokens;
    }

    struct PinyinConfig {
        bool lowercase = true;
        bool trimWhitespace = true;
        bool keepNoneChinese = true;
        bool keepNoneChineseInFirstLetter = true;
        bool keepNoneChineseInJoinedFullPinyin = false;
        bool keepOriginal = false;
        bool keepFirstLetter = true;
        bool keepSeparateFirstLetter = false;
        bool keepNoneChineseTogether = true;
        bool noneChinesePinyinTokenize = true;
        bool keepFullPinyin = true;
        bool keepJoinedFullPinyin = false;
        bool removeDuplicatedTerm = false;
        bool fixedPinyinOffset = false;
        bool ignorePinyinOffset = true;
        bool keepSeparateChinese = false;
        int limitFirstLetterLength = 16;
    };

    struct TermItem {
        std::string term;
        int startOffset;
        int endOffset;
        int position;

        TermItem() = default;
        TermItem(const std::string& t, int start, int end, int pos)
                : term(t), startOffset(start), endOffset(end), position(pos) {}
    };

    std::unordered_map<std::string, std::vector<TermItem>> getStringArrayListHashMap(
            const std::vector<std::string>& texts, const PinyinConfig& config) {
        std::unordered_map<std::string, std::vector<TermItem>> result;

        try {
            PinyinTokenizerFactory factory;
            std::unordered_map<std::string, std::string> args;

            args["keep_first_letter"] = config.keepFirstLetter ? "true" : "false";
            args["keep_separate_first_letter"] = config.keepSeparateFirstLetter ? "true" : "false";
            args["keep_full_pinyin"] = config.keepFullPinyin ? "true" : "false";
            args["keep_joined_full_pinyin"] = config.keepJoinedFullPinyin ? "true" : "false";
            args["keep_none_chinese"] = config.keepNoneChinese ? "true" : "false";
            args["keep_none_chinese_together"] = config.keepNoneChineseTogether ? "true" : "false";
            args["keep_none_chinese_in_first_letter"] =
                    config.keepNoneChineseInFirstLetter ? "true" : "false";
            args["keep_none_chinese_in_joined_full_pinyin"] =
                    config.keepNoneChineseInJoinedFullPinyin ? "true" : "false";
            args["keep_original"] = config.keepOriginal ? "true" : "false";
            args["keep_separate_chinese"] = config.keepSeparateChinese ? "true" : "false";
            args["none_chinese_pinyin_tokenize"] =
                    config.noneChinesePinyinTokenize ? "true" : "false";
            args["remove_duplicated_term"] = config.removeDuplicatedTerm ? "true" : "false";
            args["fixed_pinyin_offset"] = config.fixedPinyinOffset ? "true" : "false";
            args["ignore_pinyin_offset"] = config.ignorePinyinOffset ? "true" : "false";
            args["lowercase"] = config.lowercase ? "true" : "false";
            args["trim_whitespace"] = config.trimWhitespace ? "true" : "false";
            args["limit_first_letter_length"] = std::to_string(config.limitFirstLetterLength);

            Settings settings(args);
            factory.initialize(settings);

            for (const auto& text : texts) {
                auto tokens_detail = tokenizeWithDetails(factory, text);
                std::vector<TermItem> termItems;

                for (const auto& token : tokens_detail) {
                    TermItem item(token.term, token.startOffset, token.endOffset, token.position);
                    termItems.push_back(item);
                }

                result[text] = termItems;
            }

            return result;
        } catch (const std::exception& e) {
            std::cout << "Error: Tokenizer configuration failed: " << e.what() << std::endl;
            return {};
        }
    }

    void verifyTokens(const std::string& text, const PinyinConfig& config,
                      const std::vector<std::string>& expected) {
        auto result = getStringArrayListHashMap({text}, config);
        auto& tokens = result[text];

        EXPECT_EQ(tokens.size(), expected.size()) << "Token count mismatch";
        for (size_t i = 0; i < std::min(tokens.size(), expected.size()); ++i) {
            EXPECT_EQ(tokens[i].term, expected[i])
                    << "Token[" << i << "] mismatch, expected: " << expected[i]
                    << ", actual: " << tokens[i].term;
        }
    }

    std::vector<std::string> getTokensWithConfig(const std::string& text, bool keepFirstLetter,
                                                 bool keepNoneChinese, bool keepOriginal,
                                                 bool keepFullPinyin,
                                                 bool ignorePinyinOffset = false) {
        PinyinConfig config;
        config.keepFirstLetter = keepFirstLetter;
        config.keepNoneChinese = keepNoneChinese;
        config.keepOriginal = keepOriginal;
        config.keepFullPinyin = keepFullPinyin;
        config.ignorePinyinOffset = ignorePinyinOffset;

        auto result_map = getStringArrayListHashMap({text}, config);
        std::vector<std::string> tokens;

        if (result_map.count(text)) {
            for (const auto& item : result_map[text]) {
                tokens.push_back(item.term);
            }
        }

        return tokens;
    }

    std::vector<UChar32> stringToCodepoints(const std::string& text) {
        std::vector<UChar32> codepoints;
        const char* text_ptr = text.c_str();
        int text_len = static_cast<int>(text.length());
        int byte_pos = 0;

        while (byte_pos < text_len) {
            UChar32 cp;
            U8_NEXT(text_ptr, byte_pos, text_len, cp);
            if (cp < 0) cp = 0xFFFD;
            codepoints.push_back(cp);
        }

        return codepoints;
    }
};

TEST_F(PinyinAnalysisTest, TestTokenizer) {
    PinyinConfig config;
    config.noneChinesePinyinTokenize = false;
    config.keepOriginal = true;
    config.ignorePinyinOffset = false;

    verifyTokens("刘德华", config, {"liu", "刘德华", "ldh", "de", "hua"});

    verifyTokens("劉德華", config, {"liu", "劉德華", "ldh", "de", "hua"});

    verifyTokens("刘德华A1", config, {"liu", "刘德华a1", "ldha1", "de", "hua", "a1"});

    verifyTokens("DJ音乐家", config, {"dj", "dj音乐家", "djyyj", "yin", "yue", "jia"});

    verifyTokens("β-氨基酸尿", config, {"an", "β-氨基酸尿", "ajsn", "ji", "suan", "niao"});

    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepSeparateFirstLetter = true;
    config1.keepNoneChinese = false;
    config1.keepNoneChineseInFirstLetter = false;
    config1.keepOriginal = false;
    config1.keepFullPinyin = true;
    config1.limitFirstLetterLength = 5;
    config1.lowercase = false;
    config1.ignorePinyinOffset = false;

    verifyTokens("刘德华", config1, {"l", "liu", "ldh", "d", "de", "h", "hua"});

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepSeparateFirstLetter = true;
    config2.keepNoneChinese = false;
    config2.keepNoneChineseInFirstLetter = false;
    config2.keepOriginal = false;
    config2.keepFullPinyin = true;
    config2.limitFirstLetterLength = 5;
    config2.removeDuplicatedTerm = true;
    config2.lowercase = false;
    config2.ignorePinyinOffset = false;

    verifyTokens("我的的", config2, {"w", "wo", "wdd", "d", "de"});

    PinyinConfig config3;
    config3.keepFirstLetter = true;
    config3.keepFullPinyin = false;
    config3.keepNoneChinese = false;
    config3.keepNoneChineseTogether = true;
    config3.noneChinesePinyinTokenize = true;
    config3.keepNoneChineseInFirstLetter = true;
    config3.keepOriginal = false;
    config3.lowercase = true;
    config3.trimWhitespace = true;
    config3.ignorePinyinOffset = false;

    verifyTokens("lu金 s刘德华 张学友 郭富城 黎明 四大lao天王liudehua", config3,
                 {"lujsldhzxygfclms"});

    PinyinConfig config4;
    config4.keepFirstLetter = true;
    config4.keepFullPinyin = false;
    config4.keepJoinedFullPinyin = true;
    config4.keepNoneChinese = false;
    config4.keepNoneChineseTogether = true;
    config4.noneChinesePinyinTokenize = true;
    config4.keepNoneChineseInFirstLetter = true;
    config4.keepOriginal = false;
    config4.lowercase = true;
    config4.trimWhitespace = true;
    config4.ignorePinyinOffset = false;

    verifyTokens("刘德华", config4, {"liudehua", "ldh"});

    PinyinConfig config5;
    config5.keepFirstLetter = false;
    config5.keepFullPinyin = false;
    config5.keepJoinedFullPinyin = true;
    config5.keepNoneChinese = false;
    config5.keepNoneChineseTogether = true;
    config5.noneChinesePinyinTokenize = true;
    config5.keepNoneChineseInFirstLetter = true;
    config5.keepOriginal = false;
    config5.lowercase = true;
    config5.trimWhitespace = true;
    config5.ignorePinyinOffset = false;

    verifyTokens("刘德华", config5, {"liudehua"});

    PinyinConfig config6;
    config6.keepFirstLetter = false;
    config6.keepSeparateFirstLetter = false;
    config6.keepFullPinyin = false;
    config6.keepJoinedFullPinyin = true;
    config6.keepNoneChinese = true;
    config6.keepNoneChineseTogether = true;
    config6.keepOriginal = true;
    config6.limitFirstLetterLength = 16;
    config6.noneChinesePinyinTokenize = true;
    config6.lowercase = true;
    config6.ignorePinyinOffset = false;

    verifyTokens("ceshi", config6, {"ce", "ceshi", "shi"});
}

TEST_F(PinyinAnalysisTest, TestFirstLetters) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = true;
    config.keepFullPinyin = false;
    config.keepJoinedFullPinyin = false;
    config.keepNoneChinese = true;
    config.keepNoneChineseTogether = true;
    config.keepOriginal = false;
    config.limitFirstLetterLength = 16;
    config.noneChinesePinyinTokenize = true;
    config.lowercase = true;
    config.ignorePinyinOffset = false;

    verifyTokens("刘德华", config, {"l", "d", "h"});
}

TEST_F(PinyinAnalysisTest, TestOnlyLetters) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = false;
    config.keepFullPinyin = true;
    config.keepJoinedFullPinyin = false;
    config.keepNoneChinese = true;
    config.keepNoneChineseTogether = true;
    config.keepOriginal = false;
    config.limitFirstLetterLength = 16;
    config.noneChinesePinyinTokenize = true;
    config.lowercase = true;
    config.ignorePinyinOffset = false;

    verifyTokens("ldh", config, {"l", "d", "h"});
    verifyTokens("liuldhdehua", config, {"liu", "l", "d", "h", "de", "hua"});
    verifyTokens("liuldh", config, {"liu", "l", "d", "h"});
    verifyTokens("ldhdehua", config, {"l", "d", "h", "de", "hua"});
    verifyTokens("ldh123dehua", config, {"l", "d", "h", "123", "de", "hua"});
}

TEST_F(PinyinAnalysisTest, TestOnlyFirstLetterTokenizer) {
    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepNoneChinese = true;
    config1.keepOriginal = false;
    config1.keepFullPinyin = false;
    config1.keepNoneChineseTogether = false;
    config1.ignorePinyinOffset = false;

    verifyTokens("刘德华", config1, {"ldh"});
    verifyTokens("β-氨基酸尿", config1, {"ajsn"});
    verifyTokens("DJ音乐家", config1, {"d", "djyyj", "j"});

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepNoneChinese = false;
    config2.keepNoneChineseInFirstLetter = false;
    config2.keepOriginal = false;
    config2.keepFullPinyin = false;
    config2.keepNoneChineseTogether = false;
    config2.ignorePinyinOffset = false;

    verifyTokens("DJ音乐家", config2, {"yyj"});

    PinyinConfig config3;
    config3.keepFirstLetter = true;
    config3.keepNoneChinese = true;
    config3.keepNoneChineseInFirstLetter = true;
    config3.keepNoneChineseTogether = true;
    config3.keepOriginal = false;
    config3.keepFullPinyin = false;
    config3.noneChinesePinyinTokenize = false;
    config3.ignorePinyinOffset = false;

    verifyTokens("DJ音乐家", config3, {"dj", "djyyj"});

    auto result_map = getStringArrayListHashMap({"DJ音乐家"}, config3);
    auto& tokens = result_map["DJ音乐家"];
    ASSERT_EQ(tokens.size(), 2);
    EXPECT_EQ(tokens[0].position, 1);
    EXPECT_EQ(tokens[1].position, 1);
}

TEST_F(PinyinAnalysisTest, TestPinyin) {
    try {
        auto result = PinyinUtil::instance().convert(stringToCodepoints("德"),
                                                     PinyinFormat::TONELESS_PINYIN_FORMAT);
        ASSERT_GE(result.size(), 1);
        EXPECT_EQ(result[0], "de");
    } catch (const std::exception& e) {
        std::cout << "⚠️ PinyinUtil Err: " << e.what() << std::endl;
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinFunction) {
    try {
        auto result = PinyinUtil::instance().convert(stringToCodepoints("貌美如誮"),
                                                     PinyinFormat::TONELESS_PINYIN_FORMAT);

        EXPECT_GE(result.size(), 4);
        if (result.size() >= 4) {
            EXPECT_EQ(result[0], "mao");
            EXPECT_EQ(result[1], "mei");
            EXPECT_EQ(result[2], "ru");
            EXPECT_EQ(result[3], "hua");
        }
    } catch (const std::exception& e) {
        std::cout << "⚠️ PinyinUtil Err: " << e.what() << std::endl;
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinTokenize) {
    std::string str = "liudehuaalibaba13zhuanghan134";
    auto result = PinyinAlphabetTokenizer::walk(str);

    ASSERT_EQ(result.size(), 11);
    EXPECT_EQ(result[0], "liu");
    EXPECT_EQ(result[1], "de");
    EXPECT_EQ(result[2], "hua");
    EXPECT_EQ(result[3], "a");
    EXPECT_EQ(result[4], "li");
    EXPECT_EQ(result[5], "ba");
    EXPECT_EQ(result[6], "ba");
    EXPECT_EQ(result[7], "13");
    EXPECT_EQ(result[8], "zhuang");
    EXPECT_EQ(result[9], "han");
    EXPECT_EQ(result[10], "134");

    str = "a123";
    result = PinyinAlphabetTokenizer::walk(str);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "a");
    EXPECT_EQ(result[1], "123");

    str = "liudehua";
    result = PinyinAlphabetTokenizer::walk(str);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], "liu");
    EXPECT_EQ(result[1], "de");
    EXPECT_EQ(result[2], "hua");

    str = "ceshi";
    result = PinyinAlphabetTokenizer::walk(str);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], "ce");
    EXPECT_EQ(result[1], "shi");
}

TEST_F(PinyinAnalysisTest, TestMixedPinyinTokenizer) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepSeparateFirstLetter = true;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"刘德华", "刘de华", "liude华", " liude 华"};
    auto result = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result[test_case];

        EXPECT_GT(tokens_detail.size(), 0);

        if (test_case == "刘德华") {
            EXPECT_EQ(tokens_detail.size(), 8);

            std::map<std::string, TermItem> token_map;
            for (const auto& token : tokens_detail) {
                token_map[token.term] = token;
            }

            EXPECT_TRUE(token_map.count("l"));
            EXPECT_TRUE(token_map.count("liu"));
            EXPECT_TRUE(token_map.count("刘德华"));
            EXPECT_TRUE(token_map.count("ldh"));
            EXPECT_TRUE(token_map.count("d"));
            EXPECT_TRUE(token_map.count("de"));
            EXPECT_TRUE(token_map.count("h"));
            EXPECT_TRUE(token_map.count("hua"));

            if (token_map.count("l")) EXPECT_EQ(token_map["l"].position, 1);
            if (token_map.count("liu")) EXPECT_EQ(token_map["liu"].position, 1);
            if (token_map.count("刘德华")) EXPECT_EQ(token_map["刘德华"].position, 1);
            if (token_map.count("ldh")) EXPECT_EQ(token_map["ldh"].position, 1);
            if (token_map.count("d")) EXPECT_EQ(token_map["d"].position, 2);
            if (token_map.count("de")) EXPECT_EQ(token_map["de"].position, 2);
            if (token_map.count("h")) EXPECT_EQ(token_map["h"].position, 3);
            if (token_map.count("hua")) EXPECT_EQ(token_map["hua"].position, 3);
        } else if (test_case == "刘de华") {
            EXPECT_EQ(tokens_detail.size(), 7);

            std::map<std::string, TermItem> token_map;
            for (const auto& token : tokens_detail) {
                token_map[token.term] = token;
            }

            EXPECT_TRUE(token_map.count("l"));
            EXPECT_TRUE(token_map.count("liu"));
            EXPECT_TRUE(token_map.count("刘de华"));
            EXPECT_TRUE(token_map.count("ldeh"));
            EXPECT_TRUE(token_map.count("de"));
            EXPECT_TRUE(token_map.count("h"));
            EXPECT_TRUE(token_map.count("hua"));
        } else if (test_case == "liude华") {
            EXPECT_EQ(tokens_detail.size(), 6);

            std::map<std::string, TermItem> token_map;
            for (const auto& token : tokens_detail) {
                token_map[token.term] = token;
            }

            EXPECT_TRUE(token_map.count("liu"));
            EXPECT_TRUE(token_map.count("liude华"));
            EXPECT_TRUE(token_map.count("liudeh"));
            EXPECT_TRUE(token_map.count("de"));
            EXPECT_TRUE(token_map.count("h"));
            EXPECT_TRUE(token_map.count("hua"));
        } else if (test_case == " liude 华") {
            EXPECT_EQ(tokens_detail.size(), 6);

            std::map<std::string, TermItem> token_map;
            for (const auto& token : tokens_detail) {
                token_map[token.term] = token;
            }

            EXPECT_TRUE(token_map.count("liu"));
            EXPECT_TRUE(token_map.count("liude 华"));
            EXPECT_TRUE(token_map.count("liudeh"));
            EXPECT_TRUE(token_map.count("de"));
            EXPECT_TRUE(token_map.count("h"));
            EXPECT_TRUE(token_map.count("hua"));
        }
    }
}

TEST_F(PinyinAnalysisTest, TestFullJoinedPinyin) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepNoneChineseInFirstLetter = false;
    config.keepOriginal = false;
    config.keepFullPinyin = false;
    config.noneChinesePinyinTokenize = false;
    config.keepNoneChinese = true;
    config.keepJoinedFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.keepNoneChineseInJoinedFullPinyin = true;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"DJ音乐家"};
    auto result = getStringArrayListHashMap(test_cases, config);
    auto& tokens_detail = result["DJ音乐家"];

    EXPECT_EQ(tokens_detail.size(), 1);
    if (tokens_detail.size() >= 1) {
        EXPECT_EQ(tokens_detail[0].term, "djyinyuejia");
    }
}

TEST_F(PinyinAnalysisTest, TestMixedPinyinTokenizer2) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = false;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.keepFullPinyin = false;
    config.keepNoneChineseTogether = true;
    config.ignorePinyinOffset = false;
    config.keepSeparateChinese = true;

    std::vector<std::string> test_cases = {"ldh",    "ldhua",   "ld华",     "刘德华",
                                           "刘de华", "liude华", " liude 华"};

    auto result_map = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& token_items = result_map[test_case];
        std::vector<std::string> tokens;
        for (const auto& item : token_items) {
            tokens.push_back(item.term);
        }

        EXPECT_GT(tokens.size(), 0);

        if (test_case == "ldh") {
            EXPECT_EQ(tokens.size(), 4);
            EXPECT_EQ(tokens[0], "l");
            EXPECT_EQ(tokens[1], "ldh");
            EXPECT_EQ(tokens[2], "d");
            EXPECT_EQ(tokens[3], "h");
        } else if (test_case == "ldhua") {
            EXPECT_EQ(tokens.size(), 4);
            EXPECT_EQ(tokens[0], "l");
            EXPECT_EQ(tokens[1], "ldhua");
            EXPECT_EQ(tokens[2], "d");
            EXPECT_EQ(tokens[3], "hua");
        } else if (test_case == "ld华") {
            EXPECT_EQ(tokens.size(), 4);
            EXPECT_EQ(tokens[0], "l");
            EXPECT_EQ(tokens[1], "ld华");
            EXPECT_EQ(tokens[2], "d");
            EXPECT_EQ(tokens[3], "华");
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinTokenizerOffset) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = false;
    config.keepNoneChinese = true;
    config.keepOriginal = false;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.removeDuplicatedTerm = true;
    config.fixedPinyinOffset = false;
    config.keepJoinedFullPinyin = false;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"ceshi", "测shi", "ce试", "测试", "1测shi"};

    auto result_map = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map[test_case];
        EXPECT_GT(tokens_detail.size(), 0);

        if (test_case == "ceshi") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
        } else if (test_case == "测shi") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "ce试") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "测试") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "1测shi") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "1");
            EXPECT_EQ(tokens_detail[1].term, "ce");
            EXPECT_EQ(tokens_detail[2].term, "shi");
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinTokenizerOffsetWithExtraTerms) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = false;
    config.keepNoneChinese = true;
    config.keepOriginal = false;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.removeDuplicatedTerm = true;
    config.fixedPinyinOffset = false;
    config.keepJoinedFullPinyin = false;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"ceshi", "测shi", "ce试", "测试", "1测shi"};

    auto result_map = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map[test_case];

        EXPECT_GT(tokens_detail.size(), 0);

        if (test_case == "ceshi") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "测shi") {
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
            EXPECT_EQ(tokens_detail.size(), 2);
        } else if (test_case == "ce试") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "测试") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "1测shi") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "1");
            EXPECT_EQ(tokens_detail[1].term, "ce");
            EXPECT_EQ(tokens_detail[2].term, "shi");
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinTokenizerFixedOffset) {
    PinyinConfig config;
    config.keepFirstLetter = false;
    config.keepSeparateFirstLetter = false;
    config.keepNoneChinese = true;
    config.keepOriginal = false;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.fixedPinyinOffset = true;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"ceshi", "测shi", "测试", "1测shi"};

    auto result_map = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map[test_case];
        EXPECT_GT(tokens_detail.size(), 0);

        if (test_case == "ceshi") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "测shi") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "测试") {
            EXPECT_EQ(tokens_detail.size(), 2);
            EXPECT_EQ(tokens_detail[0].term, "ce");
            EXPECT_EQ(tokens_detail[1].term, "shi");
        } else if (test_case == "1测shi") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "1");
            EXPECT_EQ(tokens_detail[1].term, "ce");
            EXPECT_EQ(tokens_detail[2].term, "shi");
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinPosition1) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepSeparateFirstLetter = true;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"刘德华"};
    auto result = getStringArrayListHashMap(test_cases, config);
    auto& tokens_detail = result["刘德华"];

    EXPECT_EQ(tokens_detail.size(), 8);

    std::map<std::string, TermItem> token_map;
    for (const auto& token : tokens_detail) {
        token_map[token.term] = token;
    }

    if (token_map.count("l")) EXPECT_EQ(token_map["l"].position, 1);
    if (token_map.count("liu")) EXPECT_EQ(token_map["liu"].position, 1);
    if (token_map.count("刘德华")) EXPECT_EQ(token_map["刘德华"].position, 1);
    if (token_map.count("ldh")) EXPECT_EQ(token_map["ldh"].position, 1);
    if (token_map.count("d")) EXPECT_EQ(token_map["d"].position, 2);
    if (token_map.count("de")) EXPECT_EQ(token_map["de"].position, 2);
    if (token_map.count("h")) EXPECT_EQ(token_map["h"].position, 3);
    if (token_map.count("hua")) EXPECT_EQ(token_map["hua"].position, 3);
}

TEST_F(PinyinAnalysisTest, TestPinyinPosition2) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepSeparateFirstLetter = true;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.ignorePinyinOffset = false;

    const std::string test_text = "l德华";
    auto result_map = getStringArrayListHashMap({test_text}, config);
    auto& tokens_detail = result_map[test_text];

    EXPECT_EQ(tokens_detail.size(), 7);

    std::map<std::string, TermItem> token_map;
    for (const auto& token : tokens_detail) {
        token_map[token.term] = token;
    }

    if (token_map.count("l")) EXPECT_EQ(token_map["l"].position, 1);
    if (token_map.count("l德华")) EXPECT_EQ(token_map["l德华"].position, 1);
    if (token_map.count("ldh")) EXPECT_EQ(token_map["ldh"].position, 1);
    if (token_map.count("d")) EXPECT_EQ(token_map["d"].position, 2);
    if (token_map.count("de")) EXPECT_EQ(token_map["de"].position, 2);
    if (token_map.count("h")) EXPECT_EQ(token_map["h"].position, 3);
    if (token_map.count("hua")) EXPECT_EQ(token_map["hua"].position, 3);
}

TEST_F(PinyinAnalysisTest, TestPinyinPositionWithNonChinese) {
    PinyinConfig config1;
    config1.keepFirstLetter = false;
    config1.keepSeparateFirstLetter = true;
    config1.keepNoneChinese = true;
    config1.keepNoneChineseTogether = false;
    config1.keepNoneChineseInFirstLetter = true;
    config1.keepOriginal = false;
    config1.keepFullPinyin = false;
    config1.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"l德华", "liu德华"};

    auto result_map1 = getStringArrayListHashMap(test_cases, config1);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map1[test_case];

        if (test_case == "l德华") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "l");
            EXPECT_EQ(tokens_detail[1].term, "d");
            EXPECT_EQ(tokens_detail[2].term, "h");
            EXPECT_EQ(tokens_detail[0].position, 1);
            EXPECT_EQ(tokens_detail[1].position, 2);
            EXPECT_EQ(tokens_detail[2].position, 3);
        }
    }

    PinyinConfig config2 = config1;
    config2.keepNoneChineseTogether = true;

    auto result_map2 = getStringArrayListHashMap(test_cases, config2);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map2[test_case];

        EXPECT_GT(tokens_detail.size(), 0);
        if (test_case == "l德华") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "l");
            EXPECT_EQ(tokens_detail[1].term, "d");
            EXPECT_EQ(tokens_detail[2].term, "h");
            EXPECT_EQ(tokens_detail[0].position, 1);
            EXPECT_EQ(tokens_detail[1].position, 2);
            EXPECT_EQ(tokens_detail[2].position, 3);
        }
        if (test_case == "liu德华") {
            EXPECT_EQ(tokens_detail.size(), 3);
            EXPECT_EQ(tokens_detail[0].term, "liu");
            EXPECT_EQ(tokens_detail[1].term, "d");
            EXPECT_EQ(tokens_detail[2].term, "h");
            EXPECT_EQ(tokens_detail[0].position, 1);
            EXPECT_EQ(tokens_detail[1].position, 2);
            EXPECT_EQ(tokens_detail[2].position, 3);
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinPosition3) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepSeparateFirstLetter = true;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.keepFullPinyin = true;
    config.keepNoneChineseTogether = true;
    config.ignorePinyinOffset = false;

    std::vector<std::string> test_cases = {"liude华", "liudehua", "ldhua",
                                           "刘de华",  "刘dehua",  "DJ音乐家"};

    auto result_map = getStringArrayListHashMap(test_cases, config);

    for (const auto& test_case : test_cases) {
        auto& tokens_detail = result_map[test_case];

        if (test_case == "liude华") {
            EXPECT_EQ(tokens_detail.size(), 6);

            std::map<std::string, TermItem> token_map;
            for (const auto& token : tokens_detail) {
                token_map[token.term] = token;
            }

            if (token_map.count("liu")) EXPECT_EQ(token_map["liu"].position, 1);
            if (token_map.count("liude华")) EXPECT_EQ(token_map["liude华"].position, 1);
            if (token_map.count("liudeh")) EXPECT_EQ(token_map["liudeh"].position, 1);
            if (token_map.count("de")) EXPECT_EQ(token_map["de"].position, 2);
            if (token_map.count("h")) EXPECT_EQ(token_map["h"].position, 3);
            if (token_map.count("hua")) EXPECT_EQ(token_map["hua"].position, 3);
        }
    }
}

TEST_F(PinyinAnalysisTest, TestPinyinPosition4) {
    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepSeparateFirstLetter = true;
    config1.keepNoneChinese = true;
    config1.keepOriginal = true;
    config1.keepFullPinyin = true;
    config1.keepNoneChineseTogether = true;
    config1.ignorePinyinOffset = false;

    const std::string test_text = "medcl";
    auto result_map1 = getStringArrayListHashMap({test_text}, config1);
    auto& tokens_detail1 = result_map1[test_text];

    EXPECT_EQ(tokens_detail1[0].term, "me");
    EXPECT_EQ(tokens_detail1[0].position, 1);
    EXPECT_EQ(tokens_detail1[1].term, "medcl");
    EXPECT_EQ(tokens_detail1[1].position, 1);

    PinyinConfig config2 = config1;
    config2.keepNoneChineseTogether = false;
    config2.keepJoinedFullPinyin = true;

    auto result_map2 = getStringArrayListHashMap({test_text}, config2);
    auto& tokens_detail2 = result_map2[test_text];

    EXPECT_EQ(tokens_detail2[0].term, "m");
    EXPECT_EQ(tokens_detail2[0].position, 1);
    EXPECT_EQ(tokens_detail2[1].term, "medcl");
    EXPECT_EQ(tokens_detail2[1].position, 1);
    EXPECT_EQ(tokens_detail2[2].term, "e");
    EXPECT_EQ(tokens_detail2[2].position, 2);
    EXPECT_EQ(tokens_detail2[3].term, "d");
    EXPECT_EQ(tokens_detail2[3].position, 3);
}

TEST_F(PinyinAnalysisTest, TestTrimWhitespace) {
    PinyinConfig config1;
    config1.keepFullPinyin = true;
    config1.keepFirstLetter = true;
    config1.keepNoneChinese = false;
    config1.keepOriginal = true;
    config1.trimWhitespace = true;
    config1.ignorePinyinOffset = false;

    verifyTokens("  刘德华  ", config1, {"liu", "刘德华", "ldh", "de", "hua"});

    PinyinConfig config2;
    config2.keepFullPinyin = true;
    config2.keepFirstLetter = true;
    config2.keepNoneChinese = false;
    config2.keepOriginal = true;
    config2.trimWhitespace = false;
    config2.ignorePinyinOffset = false;

    verifyTokens("  刘德华  ", config2, {"liu", "  刘德华  ", "ldh", "de", "hua"});
}

TEST_F(PinyinAnalysisTest, TestLowercase) {
    PinyinConfig config1;
    config1.keepFullPinyin = true;
    config1.keepFirstLetter = true;
    config1.keepNoneChinese = true;
    config1.keepOriginal = true;
    config1.lowercase = true;
    config1.ignorePinyinOffset = false;

    verifyTokens("刘De华ABC", config1, {"liu", "刘de华abc", "ldehabc", "de", "hua", "a", "b", "c"});

    PinyinConfig config2;
    config2.keepFullPinyin = true;
    config2.keepFirstLetter = true;
    config2.keepNoneChinese = true;
    config2.keepOriginal = true;
    config2.lowercase = false;
    config2.ignorePinyinOffset = false;

    verifyTokens("刘De华ABC", config2, {"liu", "刘De华ABC", "lDehABC", "de", "hua", "a", "b", "c"});
}

TEST_F(PinyinAnalysisTest, TestKeepNoneChineseInJoinedFullPinyin) {
    PinyinConfig config1;
    config1.keepFirstLetter = false;
    config1.keepFullPinyin = false;
    config1.keepJoinedFullPinyin = true;
    config1.keepNoneChinese = true;
    config1.keepOriginal = false;
    config1.keepNoneChineseInJoinedFullPinyin = false;
    config1.ignorePinyinOffset = false;
    verifyTokens("刘a德华", config1, {"a", "liudehua"});

    PinyinConfig config2;
    config2.keepFirstLetter = false;
    config2.keepFullPinyin = false;
    config2.keepJoinedFullPinyin = true;
    config2.keepNoneChinese = true;
    config2.keepOriginal = false;
    config2.keepNoneChineseInJoinedFullPinyin = true;
    config2.ignorePinyinOffset = false;

    verifyTokens("刘a德华", config2, {"a", "liuadehua"});
}

TEST_F(PinyinAnalysisTest, TestLimitFirstLetterLength) {
    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepFullPinyin = false;
    config1.keepNoneChinese = false;
    config1.keepOriginal = false;
    config1.limitFirstLetterLength = 2;
    config1.ignorePinyinOffset = false;

    verifyTokens("刘德华张学友", config1, {"ld"});

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepFullPinyin = false;
    config2.keepNoneChinese = false;
    config2.keepOriginal = false;
    config2.limitFirstLetterLength = 10;
    config2.ignorePinyinOffset = false;

    verifyTokens("刘德华张学友", config2, {"ldhzxy"});

    PinyinConfig config3;
    config3.keepFirstLetter = true;
    config3.keepFullPinyin = false;
    config3.keepNoneChinese = false;
    config3.keepOriginal = false;
    config3.limitFirstLetterLength = 10;
    config3.ignorePinyinOffset = false;

    verifyTokens("刘德华", config3, {"ldh"});
}

TEST_F(PinyinAnalysisTest, TestRemoveDuplicatedTerm) {
    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepSeparateFirstLetter = true;
    config1.keepFullPinyin = true;
    config1.keepNoneChinese = false;
    config1.keepOriginal = false;
    config1.removeDuplicatedTerm = false;
    config1.ignorePinyinOffset = false;

    verifyTokens("我的的", config1, {"w", "wo", "wdd", "d", "de", "d", "de"});

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepSeparateFirstLetter = true;
    config2.keepFullPinyin = true;
    config2.keepNoneChinese = false;
    config2.keepOriginal = false;
    config2.removeDuplicatedTerm = true;
    config2.ignorePinyinOffset = false;

    verifyTokens("我的的", config2, {"w", "wo", "wdd", "d", "de"});
}

TEST_F(PinyinAnalysisTest, TestSingleConfigIsolation) {
    const std::string test_text = "刘德华";

    PinyinConfig config1;
    config1.keepFirstLetter = false;
    config1.keepFullPinyin = true; // Keep at least one to avoid error
    config1.keepNoneChinese = false;
    config1.keepOriginal = true;
    config1.ignorePinyinOffset = false;

    verifyTokens(test_text, config1, {"liu", "刘德华", "de", "hua"});

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepFullPinyin = false;
    config2.keepNoneChinese = false;
    config2.keepOriginal = false;
    config2.ignorePinyinOffset = false;

    verifyTokens(test_text, config2, {"ldh"});

    PinyinConfig config3;
    config3.keepFirstLetter = false;
    config3.keepFullPinyin = true;
    config3.keepNoneChinese = false;
    config3.keepOriginal = false;
    config3.ignorePinyinOffset = false;

    verifyTokens(test_text, config3, {"liu", "de", "hua"});

    PinyinConfig config4;
    config4.keepFirstLetter = false;
    config4.keepSeparateFirstLetter = true;
    config4.keepFullPinyin = false;
    config4.keepNoneChinese = false;
    config4.keepOriginal = false;
    config4.ignorePinyinOffset = false;

    verifyTokens(test_text, config4, {"l", "d", "h"});

    PinyinConfig config5;
    config5.keepFirstLetter = false;
    config5.keepFullPinyin = false;
    config5.keepJoinedFullPinyin = true;
    config5.keepNoneChinese = false;
    config5.keepOriginal = false;
    config5.ignorePinyinOffset = false;

    verifyTokens(test_text, config5, {"liudehua"});
}

TEST_F(PinyinAnalysisTest, TestEmptyInput) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.ignorePinyinOffset = false;

    verifyTokens("", config, {});
}

TEST_F(PinyinAnalysisTest, TestSingleSpace) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.keepNoneChinese = true;
    config.ignorePinyinOffset = false;

    auto result = getStringArrayListHashMap({" "}, config);
    EXPECT_TRUE(result.count(" "));
}

TEST_F(PinyinAnalysisTest, TestPureNumbers) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.keepNoneChinese = true;
    config.ignorePinyinOffset = false;

    auto tokens = getTokensWithConfig("123456", true, true, true, true);
    EXPECT_EQ(tokens.size(), 1);
    EXPECT_EQ(tokens[0], "123456");
}

TEST_F(PinyinAnalysisTest, TestPureEnglish) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.keepNoneChinese = true;
    config.noneChinesePinyinTokenize = true;
    config.ignorePinyinOffset = false;

    auto tokens = getTokensWithConfig("hello", true, true, true, true);
    EXPECT_GE(tokens.size(), 1);

    bool has_hello = std::find(tokens.begin(), tokens.end(), "hello") != tokens.end();
    bool has_he = std::find(tokens.begin(), tokens.end(), "he") != tokens.end();
    EXPECT_TRUE(has_hello || has_he) << "Should contain 'hello' or 'he'";
}

TEST_F(PinyinAnalysisTest, TestSpecialCharactersOnly) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.keepNoneChinese = true;
    config.ignorePinyinOffset = false;

    auto tokens = getTokensWithConfig("@#$%^&*()", true, true, true, true);
    EXPECT_GE(tokens.size(), 1);
    // Should keep special characters
    bool has_special = false;
    for (const auto& token : tokens) {
        if (token.find_first_of("@#$%^&*()") != std::string::npos) {
            has_special = true;
            break;
        }
    }
    EXPECT_TRUE(has_special) << "Should contain special characters";
}

TEST_F(PinyinAnalysisTest, TestVeryLongChineseText) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = false;
    config.keepNoneChinese = false;
    config.limitFirstLetterLength = 50;
    config.ignorePinyinOffset = false;

    std::string long_text = "刘德华张学友郭富城黎明是香港四大天王他们的歌曲深受大家喜爱";
    auto tokens = getTokensWithConfig(long_text, true, false, false, true);
    EXPECT_GE(tokens.size(), 2); // At least first letter and full pinyin

    // Verify that there are tokens with reasonable lengths
    bool has_valid_tokens = false;
    for (const auto& token : tokens) {
        if (token.length() >= 1) {
            has_valid_tokens = true;
            break;
        }
    }
    EXPECT_TRUE(has_valid_tokens) << "Should have valid tokens";
}

TEST_F(PinyinAnalysisTest, TestKeepNoneChineseTogether) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = false;
    config.keepNoneChinese = true;
    config.keepNoneChineseTogether = true;
    config.noneChinesePinyinTokenize = false;
    config.keepOriginal = false;
    config.ignorePinyinOffset = false;

    verifyTokens("abc刘德华def", config, {"abc", "abcldhdef", "def"});
}

TEST_F(PinyinAnalysisTest, TestIgnorePinyinOffset) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = false;
    config.ignorePinyinOffset = true;

    auto tokens = getTokensWithConfig("刘德华", true, false, false, true, true);
    EXPECT_GE(tokens.size(), 2); // Should have first letter and full pinyin

    bool has_ldh = std::find(tokens.begin(), tokens.end(), "ldh") != tokens.end();
    bool has_liu = std::find(tokens.begin(), tokens.end(), "liu") != tokens.end();
    EXPECT_TRUE(has_ldh) << "Should contain first letter 'ldh'";
    EXPECT_TRUE(has_liu) << "Should contain pinyin 'liu'";
}

TEST_F(PinyinAnalysisTest, TestMixedTraditionalSimplified) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = true;
    config.ignorePinyinOffset = false;

    verifyTokens("劉德華", config, {"liu", "劉德華", "ldh", "de", "hua"});
}

TEST_F(PinyinAnalysisTest, TestLimitFirstLetterLength_EdgeCases) {
    PinyinConfig config1;
    config1.keepFirstLetter = true;
    config1.keepFullPinyin = false;
    config1.keepNoneChinese = false;
    config1.keepOriginal = false;
    config1.limitFirstLetterLength = 0;
    config1.ignorePinyinOffset = false;

    auto result1 = getStringArrayListHashMap({"刘德华"}, config1);
    EXPECT_TRUE(result1.count("刘德华"));

    PinyinConfig config2;
    config2.keepFirstLetter = true;
    config2.keepFullPinyin = false;
    config2.keepNoneChinese = false;
    config2.keepOriginal = false;
    config2.limitFirstLetterLength = 1;
    config2.ignorePinyinOffset = false;

    verifyTokens("刘德华", config2, {"l"});
}

TEST_F(PinyinAnalysisTest, TestCombinedConfigOptions) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepSeparateFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepJoinedFullPinyin = true;
    config.keepNoneChinese = true;
    config.keepOriginal = true;
    config.ignorePinyinOffset = false;

    auto tokens = getTokensWithConfig("测试", true, true, true, true);
    EXPECT_GE(tokens.size(), 3); // Should have multiple tokens with all options enabled

    bool has_ce = std::find(tokens.begin(), tokens.end(), "ce") != tokens.end();
    bool has_shi = std::find(tokens.begin(), tokens.end(), "shi") != tokens.end();

    EXPECT_TRUE(has_ce) << "Should contain 'ce'";
    EXPECT_TRUE(has_shi) << "Should contain 'shi'";
}

TEST_F(PinyinAnalysisTest, TestNoneChinesePinyinTokenize_Disabled) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = false;
    config.keepNoneChinese = true;
    config.noneChinesePinyinTokenize = false;
    config.ignorePinyinOffset = false;

    auto tokens = getTokensWithConfig("abc", true, true, false, true);
    EXPECT_GE(tokens.size(), 1);

    // When noneChinesePinyinTokenize is false, "abc" should be kept as whole
    bool has_abc = std::find(tokens.begin(), tokens.end(), "abc") != tokens.end();
    EXPECT_TRUE(has_abc) << "Should contain 'abc' as a whole token";
}

TEST_F(PinyinAnalysisTest, TestPinyinAlphabetTokenizerIntegration) {
    std::string str = "abc123def";
    auto result = PinyinAlphabetTokenizer::walk(str);

    EXPECT_GE(result.size(), 3);
    bool has_123 = std::find(result.begin(), result.end(), "123") != result.end();
    EXPECT_TRUE(has_123) << "Should contain '123'";
}

TEST_F(PinyinAnalysisTest, TestRepeatedCharacters) {
    PinyinConfig config;
    config.keepFirstLetter = true;
    config.keepFullPinyin = true;
    config.keepOriginal = false;
    config.removeDuplicatedTerm = true;
    config.ignorePinyinOffset = false;

    auto result = getStringArrayListHashMap({"的的的"}, config);
    auto& tokens_detail = result["的的的"];

    // With remove duplicated, should not have repeated "de"
    int de_count = 0;
    for (const auto& token : tokens_detail) {
        if (token.term == "de") {
            de_count++;
        }
    }
    EXPECT_LE(de_count, 1) << "Should have at most one 'de' with removeDuplicatedTerm";
}
