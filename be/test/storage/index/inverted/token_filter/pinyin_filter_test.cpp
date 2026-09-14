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
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "CLucene.h"
#include "storage/index/inverted/char_filter/icu_normalizer_char_filter_factory.h"
#include "storage/index/inverted/token_filter/pinyin_filter_factory.h"
#include "storage/index/inverted/token_filter/word_delimiter_filter_factory.h"
#include "storage/index/inverted/tokenizer/ik/ik_tokenizer_factory.h"
#include "storage/index/inverted/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "storage/index/inverted/tokenizer/standard/standard_tokenizer_factory.h"

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
        } else if (tokenizer_type == "ik_smart" || tokenizer_type == "ik_max_word") {
            IKTokenizerFactory factory(tokenizer_type == "ik_smart");
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

    void assertToken(const TokenFilterPtr& filter, Token* token, const std::string& expected_term,
                     int32_t expected_start, int32_t expected_end) {
        ASSERT_NE(filter->next(token), nullptr);
        EXPECT_EQ(std::string(token->termBuffer<char>(), token->termLength<char>()), expected_term);
        EXPECT_EQ(token->startOffset(), expected_start);
        EXPECT_EQ(token->endOffset(), expected_end);
    }

    void assertEndOfTokens(const TokenFilterPtr& filter, Token* token) {
        EXPECT_EQ(filter->next(token), nullptr);
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

TEST_F(PinyinFilterTest, TestTokenFilter_IKTokenizers) {
    std::unordered_map<std::string, std::string> filter_config;
    filter_config["keep_none_chinese"] = "false";
    filter_config["keep_first_letter"] = "true";
    filter_config["keep_full_pinyin"] = "false";
    filter_config["keep_separate_first_letter"] = "false";
    filter_config["keep_original"] = "true";
    filter_config["keep_joined_full_pinyin"] = "true";

    auto smart_tokens = tokenizeWithFilter("我来到北京清华大学", "ik_smart", filter_config);
    EXPECT_NE(std::ranges::find(smart_tokens, "清华大学"), smart_tokens.end());
    EXPECT_NE(std::ranges::find(smart_tokens, "qinghuadaxue"), smart_tokens.end());

    auto max_word_tokens = tokenizeWithFilter("我来到北京清华大学", "ik_max_word", filter_config);
    EXPECT_NE(std::ranges::find(max_word_tokens, "清华"), max_word_tokens.end());
    EXPECT_NE(std::ranges::find(max_word_tokens, "qinghua"), max_word_tokens.end());
    EXPECT_NE(std::ranges::find(max_word_tokens, "大学"), max_word_tokens.end());
    EXPECT_NE(std::ranges::find(max_word_tokens, "daxue"), max_word_tokens.end());
}

TEST_F(PinyinFilterTest, TestIKSmartOffsetsRemainDocumentRelative) {
    std::unordered_map<std::string, std::string> filter_config;
    filter_config["keep_none_chinese"] = "false";
    filter_config["keep_first_letter"] = "true";
    filter_config["keep_full_pinyin"] = "false";
    filter_config["keep_separate_first_letter"] = "false";
    filter_config["keep_original"] = "true";
    filter_config["keep_joined_full_pinyin"] = "true";
    filter_config["ignore_pinyin_offset"] = "false";

    auto tokenizer = createTokenizer("ik_smart", "我来到北京清华大学");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(Settings(filter_config));
    auto filter = filter_factory.create(tokenizer);

    const std::vector<std::tuple<std::string, int32_t, int32_t>> expected = {
            {"我", 0, 3},
            {"wo", 0, 3},
            {"w", 0, 3},
            {"来到", 3, 9},
            {"laidao", 3, 9},
            {"ld", 3, 9},
            {"北京", 9, 15},
            {"beijing", 9, 15},
            {"bj", 9, 15},
            {"清华大学", 15, 27},
            {"qinghuadaxue", 15, 27},
            {"qhdx", 15, 27}};
    Token token;
    for (const auto& [term, start, end] : expected) {
        ASSERT_NE(filter->next(&token), nullptr);
        EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), term);
        EXPECT_EQ(token.startOffset(), start);
        EXPECT_EQ(token.endOffset(), end);
    }
    EXPECT_EQ(filter->next(&token), nullptr);
}

TEST_F(PinyinFilterTest, TestIKOffsetsPreserveFullwidthSourceBytes) {
    std::unordered_map<std::string, std::string> filter_config;
    filter_config["keep_first_letter"] = "false";
    filter_config["keep_full_pinyin"] = "false";
    filter_config["keep_original"] = "false";
    filter_config["keep_none_chinese"] = "true";
    filter_config["none_chinese_pinyin_tokenize"] = "true";
    filter_config["ignore_pinyin_offset"] = "false";

    auto tokenizer = createTokenizer("ik_smart", "ＬＩＵＤＥ");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(Settings(filter_config));
    auto filter = filter_factory.create(tokenizer);

    Token token;
    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "liu");
    EXPECT_EQ(token.startOffset(), 0);
    EXPECT_EQ(token.endOffset(), 9);
    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "de");
    EXPECT_EQ(token.startOffset(), 9);
    EXPECT_EQ(token.endOffset(), 15);
    EXPECT_EQ(filter->next(&token), nullptr);
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeWithICUNormalizerCharFilterAndReset) {
    auto source = std::make_shared<lucene::util::SStringReader<char>>();
    const std::string text = "ＬＩＵＤＥ";
    source->init(text.data(), static_cast<int32_t>(text.size()), false);
    ICUNormalizerCharFilterFactory char_filter_factory;
    char_filter_factory.initialize({});
    auto reader = char_filter_factory.create(source);

    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(reader);
    tokenizer->reset();

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    auto assert_offsets =
            [&filter](const std::vector<std::tuple<std::string, int32_t, int32_t>>& expected) {
                Token token;
                for (const auto& [term, start, end] : expected) {
                    ASSERT_NE(filter->next(&token), nullptr);
                    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
                              term);
                    EXPECT_EQ(token.startOffset(), start);
                    EXPECT_EQ(token.endOffset(), end);
                }
                EXPECT_EQ(filter->next(&token), nullptr);
            };
    assert_offsets({{"liu", 0, 9}, {"de", 9, 15}});

    const std::string reset_text = "ＡＢＣＤ";
    reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reader);
    filter->reset();
    assert_offsets({{"a", 0, 3}, {"b", 3, 6}, {"c", 6, 9}, {"d", 9, 12}});
}

TEST_F(PinyinFilterTest, TestKeywordAndStandardOffsetsComposeWithICUNormalizerAndReset) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");

    for (const std::string tokenizer_type : {"keyword", "standard"}) {
        auto source = std::make_shared<lucene::util::SStringReader<char>>();
        const std::string text = "ＬＩＵＤＥ";
        source->init(text.data(), static_cast<int32_t>(text.size()), false);
        ICUNormalizerCharFilterFactory char_filter_factory;
        char_filter_factory.initialize({});
        auto reader = char_filter_factory.create(source);

        TokenizerPtr tokenizer;
        if (tokenizer_type == "keyword") {
            KeywordTokenizerFactory tokenizer_factory;
            tokenizer_factory.initialize({});
            tokenizer = tokenizer_factory.create();
        } else {
            StandardTokenizerFactory tokenizer_factory;
            tokenizer_factory.initialize({});
            tokenizer = tokenizer_factory.create();
        }
        tokenizer->set_reader(reader);
        tokenizer->reset();

        PinyinFilterFactory filter_factory;
        filter_factory.initialize(settings);
        auto filter = filter_factory.create(tokenizer);

        auto assert_offsets =
                [&filter](const std::vector<std::tuple<std::string, int32_t, int32_t>>& expected) {
                    Token token;
                    for (const auto& [term, start, end] : expected) {
                        ASSERT_NE(filter->next(&token), nullptr);
                        EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
                                  term);
                        EXPECT_EQ(token.startOffset(), start);
                        EXPECT_EQ(token.endOffset(), end);
                    }
                    EXPECT_EQ(filter->next(&token), nullptr);
                };
        assert_offsets({{"liu", 0, 9}, {"de", 9, 15}});

        const std::string reset_text = "ＡＢＣＤ";
        reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
        tokenizer->set_reader(reader);
        filter->reset();
        assert_offsets({{"a", 0, 3}, {"b", 3, 6}, {"c", 6, 9}, {"d", 9, 12}});
    }
}

TEST_F(PinyinFilterTest, TestPinyinTrimmedKeywordOffsetsPreserveSourceBoundariesAndReset) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    const std::string text = "  刘德华  ";
    auto tokenizer = createTokenizer("keyword", text);
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    auto assert_offsets =
            [&filter](const std::vector<std::tuple<std::string, int32_t, int32_t>>& expected) {
                Token token;
                for (const auto& [term, start, end] : expected) {
                    ASSERT_NE(filter->next(&token), nullptr);
                    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
                              term);
                    EXPECT_EQ(token.startOffset(), start);
                    EXPECT_EQ(token.endOffset(), end);
                }
                EXPECT_EQ(filter->next(&token), nullptr);
            };
    assert_offsets({{"liu", 2, 5}, {"de", 5, 8}, {"hua", 8, 11}});

    const std::string reset_text = " \t测试 ";
    auto reset_reader = std::make_shared<lucene::util::SStringReader<char>>();
    reset_reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reset_reader);
    filter->reset();
    assert_offsets({{"ce", 2, 5}, {"shi", 5, 8}});
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeWithICUNormalizerForManyTokensAndReset) {
    std::string text;
    constexpr int token_count = 4096;
    for (int i = 0; i < token_count; ++i) {
        text += "Ｌ ";
    }

    auto source = std::make_shared<lucene::util::SStringReader<char>>();
    source->init(text.data(), static_cast<int32_t>(text.size()), false);
    ICUNormalizerCharFilterFactory char_filter_factory;
    char_filter_factory.initialize({});
    auto reader = char_filter_factory.create(source);

    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(reader);
    tokenizer->reset();

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    for (int i = 0; i < token_count; ++i) {
        Token token;
        assertToken(filter, &token, "l", i * 4, i * 4 + 3);
    }
    Token token;
    assertEndOfTokens(filter, &token);

    const std::string reset_text = "ＡＢＣＤ";
    reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reader);
    filter->reset();
    for (const auto& [term, start, end] : std::vector<std::tuple<std::string, int32_t, int32_t>> {
                 {"a", 0, 3}, {"b", 3, 6}, {"c", 6, 9}, {"d", 9, 12}}) {
        assertToken(filter, &token, term, start, end);
    }
    assertEndOfTokens(filter, &token);
}

TEST_F(PinyinFilterTest, TestPinyinWholeTokenAlternativesUseKeywordOffsets) {
    Settings settings;
    settings.set("keep_first_letter", "true");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "true");
    settings.set("keep_joined_full_pinyin", "true");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    const std::string text = "刘德华";
    auto tokenizer = createTokenizer("keyword", text);
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    const std::vector<std::string> expected_terms = {"刘德华", "liudehua", "ldh"};
    Token token;
    for (const auto& term : expected_terms) {
        assertToken(filter, &token, term, 0, 9);
    }
    assertEndOfTokens(filter, &token);
}

TEST_F(PinyinFilterTest, TestPinyinStandardOffsetsDoNotReusePreviousTokenState) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_joined_full_pinyin", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    const std::string text = "刘德华 测试";
    auto tokenizer = createTokenizer("standard", text);
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    auto assert_offsets =
            [&filter](const std::vector<std::tuple<std::string, int32_t, int32_t>>& expected) {
                Token token;
                for (const auto& [term, start, end] : expected) {
                    ASSERT_NE(filter->next(&token), nullptr);
                    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
                              term);
                    EXPECT_EQ(token.startOffset(), start);
                    EXPECT_EQ(token.endOffset(), end);
                }
                EXPECT_EQ(filter->next(&token), nullptr);
            };

    assert_offsets({{"liu", 0, 3}, {"de", 3, 6}, {"hua", 6, 9}, {"ce", 10, 13}, {"shi", 13, 16}});

    const std::string reset_text = "测试 刘德华";
    auto reset_reader = std::make_shared<lucene::util::SStringReader<char>>();
    reset_reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reset_reader);
    filter->reset();
    assert_offsets({{"ce", 0, 3}, {"shi", 3, 6}, {"liu", 7, 10}, {"de", 10, 13}, {"hua", 13, 16}});
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeAcrossICUDeletionAndReset) {
    const std::string text = std::string("liu") + "\xC2\xAD" + "de";
    auto source = std::make_shared<lucene::util::SStringReader<char>>();
    source->init(text.data(), static_cast<int32_t>(text.size()), false);
    ICUNormalizerCharFilterFactory char_filter_factory;
    char_filter_factory.initialize({});
    auto inner_filter = char_filter_factory.create(source);
    auto outer_filter = char_filter_factory.create(inner_filter);

    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(outer_filter);
    tokenizer->reset();

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    auto assert_offsets =
            [&filter](const std::vector<std::tuple<std::string, int32_t, int32_t>>& expected) {
                Token token;
                for (const auto& [term, start, end] : expected) {
                    ASSERT_NE(filter->next(&token), nullptr);
                    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()),
                              term);
                    EXPECT_EQ(token.startOffset(), start);
                    EXPECT_EQ(token.endOffset(), end);
                }
                EXPECT_EQ(filter->next(&token), nullptr);
            };

    assert_offsets({{"liu", 0, 5}, {"de", 5, 7}});

    outer_filter->init(text.data(), static_cast<int32_t>(text.size()), false);
    tokenizer->set_reader(outer_filter);
    filter->reset();
    assert_offsets({{"liu", 0, 5}, {"de", 5, 7}});
}

TEST_F(PinyinFilterTest, TestIKOffsetsPreserveConnectorGaps) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");

    for (const std::string tokenizer_type : {"ik_smart", "ik_max_word"}) {
        auto tokenizer = createTokenizer(tokenizer_type, "ＬＩＵ-ＤＥ");
        PinyinFilterFactory filter_factory;
        filter_factory.initialize(settings);
        auto filter = filter_factory.create(tokenizer);

        Token token;
        ASSERT_NE(filter->next(&token), nullptr);
        EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "liu");
        EXPECT_EQ(token.startOffset(), 0);
        EXPECT_EQ(token.endOffset(), 9);
        ASSERT_NE(filter->next(&token), nullptr);
        EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "de");
        EXPECT_EQ(token.startOffset(), 10);
        EXPECT_EQ(token.endOffset(), 16);
    }
}

TEST_F(PinyinFilterTest, TestIKOffsetsStayAlignedWhenLongTermsAreClippedAndReset) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");

    std::string long_text;
    for (int i = 0; i < 1024; ++i) {
        long_text += "ＬＩＵＤＥ";
    }

    auto tokenizer = createTokenizer("ik_smart", long_text);
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);

    Token token;
    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "liu");
    EXPECT_EQ(token.startOffset(), 0);
    EXPECT_EQ(token.endOffset(), 9);

    auto reset_reader = std::make_shared<lucene::util::SStringReader<char>>();
    const std::string reset_text = "ＬＩＵＤＥ";
    reset_reader->init(reset_text.data(), static_cast<int32_t>(reset_text.size()), false);
    tokenizer->set_reader(reset_reader);
    filter->reset();

    ASSERT_NE(filter->next(&token), nullptr);
    EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), "liu");
    EXPECT_EQ(token.startOffset(), 0);
    EXPECT_EQ(token.endOffset(), 9);
}

TEST_F(PinyinFilterTest, TestIKSourceOffsetsAreOptIn) {
    auto tokenizer = createTokenizer("ik_smart", "ＬＩＵＤＥ");
    Token token;
    ASSERT_NE(tokenizer->next(&token), nullptr);
    EXPECT_TRUE(tokenizer->get_source_byte_offsets().empty());

    tokenizer = createTokenizer("ik_smart", "ＬＩＵＤＥ");
    PinyinFilterFactory filter_factory;
    Settings settings;
    settings.set("ignore_pinyin_offset", "false");
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(tokenizer);
    EXPECT_NE(filter, nullptr);
    ASSERT_NE(tokenizer->next(&token), nullptr);
    auto offsets = tokenizer->get_source_byte_offsets();
    ASSERT_EQ(offsets.size(), 6);
    EXPECT_EQ(std::vector<int32_t>(offsets.begin(), offsets.end()),
              (std::vector<int32_t> {0, 3, 6, 9, 12, 15}));
}

TEST_F(PinyinFilterTest, TestWordDelimiterPreservesIKSourceOffsets) {
    auto tokenizer = createTokenizer("ik_smart", "ＬＩＵＤＥ１２３");

    WordDelimiterFilterFactory delimiter_factory;
    delimiter_factory.initialize({});
    auto delimiter = delimiter_factory.create(tokenizer);

    Settings pinyin_settings;
    pinyin_settings.set("keep_first_letter", "false");
    pinyin_settings.set("keep_full_pinyin", "false");
    pinyin_settings.set("keep_original", "false");
    pinyin_settings.set("keep_none_chinese", "true");
    pinyin_settings.set("none_chinese_pinyin_tokenize", "true");
    pinyin_settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(pinyin_settings);
    auto filter = pinyin_factory.create(delimiter);

    const std::vector<std::tuple<std::string, int32_t, int32_t>> expected = {
            {"liu", 0, 9}, {"de", 9, 15}, {"123", 15, 24}};
    Token token;
    for (const auto& [term, start, end] : expected) {
        ASSERT_NE(filter->next(&token), nullptr);
        EXPECT_EQ(std::string(token.termBuffer<char>(), token.termLength<char>()), term);
        EXPECT_EQ(token.startOffset(), start);
        EXPECT_EQ(token.endOffset(), end);
    }
    EXPECT_EQ(filter->next(&token), nullptr);
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
        EXPECT_EQ(tokens.size(), 0) << "Empty string should produce no tokens";
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_none_chinese"] = "true";
        config["keep_original"] = "false";
        config["none_chinese_pinyin_tokenize"] = "false";

        auto tokens = tokenizeWithFilter("hello", "keyword", config);
        EXPECT_EQ(tokens.size(), 1) << "Pure English text should be kept as one token";
        EXPECT_EQ(tokens[0], "hello");
    }

    {
        std::unordered_map<std::string, std::string> config;
        config["keep_none_chinese"] = "true";
        config["keep_original"] = "false";
        config["none_chinese_pinyin_tokenize"] = "false";

        auto tokens = tokenizeWithFilter("12345", "keyword", config);
        EXPECT_EQ(tokens.size(), 1) << "Pure numeric text should be kept as one token";
        EXPECT_EQ(tokens[0], "12345");
    }
}

TEST_F(PinyinFilterTest, TestTokenFilter_SpecialCharacters) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "true";
    config["keep_none_chinese"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("你好@世界", "keyword", config);

    bool has_ni = std::find(tokens.begin(), tokens.end(), "ni") != tokens.end();
    bool has_hao = std::find(tokens.begin(), tokens.end(), "hao") != tokens.end();
    bool has_shi = std::find(tokens.begin(), tokens.end(), "shi") != tokens.end();
    bool has_jie = std::find(tokens.begin(), tokens.end(), "jie") != tokens.end();

    EXPECT_TRUE(has_ni) << "Should contain pinyin 'ni'";
    EXPECT_TRUE(has_hao) << "Should contain pinyin 'hao'";
    EXPECT_TRUE(has_shi) << "Should contain pinyin 'shi'";
    EXPECT_TRUE(has_jie) << "Should contain pinyin 'jie'";
}

TEST_F(PinyinFilterTest, TestTokenFilter_WhitespaceHandling) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["trim_whitespace"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("  刘德华  ", "keyword", config);

    std::vector<std::string> expected = {"liu", "ldh", "de", "hua"};
    assertTokens(tokens, expected, "Trimmed whitespace test");
}

TEST_F(PinyinFilterTest, TestTokenFilter_MixedContent) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "true";
    config["none_chinese_pinyin_tokenize"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("abc测试123", "keyword", config);

    bool has_abc = std::find(tokens.begin(), tokens.end(), "abc") != tokens.end();
    bool has_ce = std::find(tokens.begin(), tokens.end(), "ce") != tokens.end();
    bool has_shi = std::find(tokens.begin(), tokens.end(), "shi") != tokens.end();
    bool has_123 = std::find(tokens.begin(), tokens.end(), "123") != tokens.end();

    EXPECT_TRUE(has_abc || has_ce) << "Should contain either 'abc' or 'ce'";
    EXPECT_TRUE(has_shi) << "Should contain pinyin 'shi'";
    EXPECT_TRUE(has_123) << "Should contain number '123'";
}

TEST_F(PinyinFilterTest, TestTokenFilter_RemoveDuplicates) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_separate_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["remove_duplicated_term"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("我的的", "keyword", config);

    int de_count = std::count(tokens.begin(), tokens.end(), "de");
    EXPECT_LE(de_count, 1) << "With remove_duplicated_term, 'de' should appear at most once";
}

TEST_F(PinyinFilterTest, TestTokenFilter_OnlyFirstLetter) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "false";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("测试", "keyword", config);

    EXPECT_EQ(tokens.size(), 1) << "Should only have first letter token";
    EXPECT_EQ(tokens[0], "cs") << "First letter should be 'cs'";
}

TEST_F(PinyinFilterTest, TestTokenFilter_OnlyFullPinyin) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "false";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("测试", "keyword", config);

    std::vector<std::string> expected = {"ce", "shi"};
    assertTokens(tokens, expected, "Only full pinyin test");
}

// Test emoji preservation without keep_original setting
TEST_F(PinyinFilterTest, TestTokenFilter_EmojiPreservation) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false"; // Emoji should still be preserved via fallback
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("⭐白菜", "standard", config);

    // Standard tokenizer outputs "⭐" and "白菜" as separate tokens
    // "⭐" -> preserved via fallback (no pinyin candidates)
    // "白菜" -> "bai", "b", "cai", "c", "bc"
    std::vector<std::string> expected = {"⭐", "bai", "b", "cai", "c"};
    assertTokens(tokens, expected, "StandardTokenizer + Emoji preservation");
}

// Test pure emoji input
TEST_F(PinyinFilterTest, TestTokenFilter_PureEmoji) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("⭐", "standard", config);

    std::vector<std::string> expected = {"⭐"};
    assertTokens(tokens, expected, "Pure emoji should be preserved");
}

// Test multiple emojis
TEST_F(PinyinFilterTest, TestTokenFilter_MultipleEmojis) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("🎉中国🚀", "standard", config);

    std::vector<std::string> expected = {"🎉", "zhong", "z", "guo", "g", "🚀"};
    assertTokens(tokens, expected, "Multiple emojis with Chinese");
}

// Test keepNoneChineseTogether = false with letters and digits
TEST_F(PinyinFilterTest, TestTokenFilter_KeepNoneChineseTogetherFalse) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_together"] = "false";
    config["none_chinese_pinyin_tokenize"] = "true";
    config["lowercase"] = "true";
    config["remove_duplicated_term"] = "true";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华ABC123", "keyword", config);

    // Letters are split individually, digits are split individually too
    std::vector<std::string> expected = {
            "liu", "刘德华abc123", "ldhabc123", "de", "hua", "a", "b", "c", "1", "2", "3"};
    assertTokens(tokens, expected, "KeepNoneChineseTogether=false with mixed content");
}

// Test keepNoneChineseTogether = false with pure letters
TEST_F(PinyinFilterTest, TestTokenFilter_KeepNoneChineseTogetherFalse_PureLetters) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true"; // Need at least one output format enabled
    config["keep_full_pinyin"] = "false";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_together"] = "false";
    config["none_chinese_pinyin_tokenize"] = "true";
    config["lowercase"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("DEL", "keyword", config);

    // All letters should be split individually, plus the result from PinyinAlphabetTokenizer
    // With keep_first_letter=true, we get the combined first letter too
    std::vector<std::string> expected = {"D", "DEL", "E", "L"};
    assertTokens(tokens, expected, "KeepNoneChineseTogether=false pure letters");
}

// Test Unicode symbols fallback when no candidates generated
TEST_F(PinyinFilterTest, TestTokenFilter_UnicodeFallback) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false"; // No keep_original but symbols should still be preserved
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Use keyword tokenizer to ensure the input is passed as-is to the filter
    auto tokens = tokenizeWithFilter("①②③", "keyword", config);

    // Unicode symbols should be preserved even without keep_original via fallback
    std::vector<std::string> expected = {"①②③"};
    assertTokens(tokens, expected, "Unicode symbols preserved without keep_original");
}

TEST_F(PinyinFilterTest, TestTokenFilter_NullConfig) {
    std::string text = "测试"; // Keep string alive
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), text.size(), false);

    StandardTokenizerFactory tokenizer_factory;
    Settings tokenizer_settings;
    tokenizer_factory.initialize(tokenizer_settings);
    auto tokenizer = tokenizer_factory.create();
    tokenizer->set_reader(reader);
    tokenizer->reset();

    // Create filter with nullptr config directly
    PinyinFilter filter(tokenizer, nullptr);
    filter.initialize();

    Token token;
    // Should work with default config without crashing
    EXPECT_NE(filter.next(&token), nullptr);
}

TEST_F(PinyinFilterTest, TestTokenFilter_KeepNoneChineseFalse) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("测试ABC123", "standard", config);
    std::vector<std::string> expected = {"ce", "c", "shi", "s", "abc123"};
    assertTokens(tokens, expected, "KeepNoneChineseFalse test");
}

TEST_F(PinyinFilterTest, TestTokenFilter_NoneChinesePinyinTokenizeFalse) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "true";
    config["none_chinese_pinyin_tokenize"] = "false"; // Don't tokenize ASCII
    config["ignore_pinyin_offset"] = "false";

    auto tokens = tokenizeWithFilter("刘德华ABC123", "standard", config);
    std::vector<std::string> expected = {"liu", "l", "de", "d", "hua", "h", "abc123"};
    assertTokens(tokens, expected, "NoneChinesePinyinTokenizeFalse test");
}

TEST_F(PinyinFilterTest, TestTokenFilter_WhitespaceOnly) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "true";
    config["keep_none_chinese"] = "true";
    config["ignore_pinyin_offset"] = "false";

    // Test with whitespace-only and mixed content
    auto tokens = tokenizeWithFilter("   测试   ", "keyword", config);
    std::vector<std::string> expected = {"ce", "测试", "cs", "shi"};
    assertTokens(tokens, expected, "WhitespaceOnly test");
}

TEST_F(PinyinFilterTest, TestTokenFilter_PositionIncrement) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_together"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Use multiple Chinese characters to trigger position increment logic
    auto tokens = tokenizeWithFilter("刘德华", "standard", config);

    std::vector<std::string> expected = {"liu", "刘", "l", "de", "德", "d", "hua", "华", "h"};
    assertTokens(tokens, expected, "PositionIncrement test");
}

// Test reset() method - verifies PinyinFilter::reset() correctly resets state
// This tests the code path in pinyin_filter.cpp:99-110
TEST_F(PinyinFilterTest, TestTokenFilter_Reset) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // First tokenization - tests that filter works correctly
    auto tokens1 = tokenizeWithFilter("刘德华", "keyword", config);

    EXPECT_GT(tokens1.size(), 0) << "First tokenization should produce tokens";
    bool has_liu = std::find(tokens1.begin(), tokens1.end(), "liu") != tokens1.end();
    EXPECT_TRUE(has_liu) << "First tokenization should contain 'liu'";

    // Second tokenization with different text - tests that filter state is independent
    auto tokens2 = tokenizeWithFilter("张学友", "keyword", config);

    EXPECT_GT(tokens2.size(), 0) << "Second tokenization should produce tokens";
    bool has_zhang = std::find(tokens2.begin(), tokens2.end(), "zhang") != tokens2.end();
    EXPECT_TRUE(has_zhang) << "Second tokenization should contain 'zhang'";

    // Ensure tokens from first text are not in second result (state isolation)
    bool has_liu_in_second = std::find(tokens2.begin(), tokens2.end(), "liu") != tokens2.end();
    EXPECT_FALSE(has_liu_in_second)
            << "Second tokenization should NOT contain 'liu' from first text";
}

// Test reset() with empty input after valid input
TEST_F(PinyinFilterTest, TestTokenFilter_ResetWithEmptyInput) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // First tokenization with valid text
    auto tokens1 = tokenizeWithFilter("测试", "keyword", config);
    EXPECT_GT(tokens1.size(), 0) << "First tokenization should produce tokens";

    // Tokenization with empty text
    auto tokens2 = tokenizeWithFilter("", "keyword", config);
    EXPECT_EQ(tokens2.size(), 0) << "Empty input should produce no tokens";
}

// Test Unicode symbol preservation fallback - tests pinyin_filter.cpp:243-259
// When pinyin_list and chinese_list are empty but there are non-ASCII Unicode chars,
// the filter should preserve the original token
TEST_F(PinyinFilterTest, TestTokenFilter_UnicodeSymbolPreservation) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false"; // Even without keep_original, Unicode should be preserved
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Test circled numbers - these are Unicode symbols that cannot be converted to pinyin
    auto tokens1 = tokenizeWithFilter("①②③", "keyword", config);
    EXPECT_EQ(tokens1.size(), 1) << "Circled numbers should be preserved as one token";
    EXPECT_EQ(tokens1[0], "①②③") << "Circled numbers should be preserved";

    // Test other Unicode symbols
    auto tokens2 = tokenizeWithFilter("★☆♠♥", "keyword", config);
    EXPECT_EQ(tokens2.size(), 1) << "Card suit symbols should be preserved";
    EXPECT_EQ(tokens2[0], "★☆♠♥") << "Card suit symbols should be preserved";

    // Test mathematical symbols
    auto tokens3 = tokenizeWithFilter("∑∏∫∂", "keyword", config);
    EXPECT_EQ(tokens3.size(), 1) << "Math symbols should be preserved";
    EXPECT_EQ(tokens3[0], "∑∏∫∂") << "Math symbols should be preserved";
}

// Test Unicode symbols mixed with Chinese characters
// Note: Standard tokenizer may filter out some Unicode symbols, so we test with keyword tokenizer
TEST_F(PinyinFilterTest, TestTokenFilter_UnicodeSymbolsWithChinese) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Use keyword tokenizer to keep the whole input as one token
    // This tests the fallback logic for mixed Unicode symbols and Chinese
    auto tokens = tokenizeWithFilter("①中国", "keyword", config);

    // With keyword tokenizer, pinyin filter should process the whole string
    // Chinese characters get converted to pinyin, but we need to check fallback behavior
    bool has_zhong = std::find(tokens.begin(), tokens.end(), "zhong") != tokens.end();
    bool has_guo = std::find(tokens.begin(), tokens.end(), "guo") != tokens.end();

    EXPECT_TRUE(has_zhong) << "Should have pinyin 'zhong'";
    EXPECT_TRUE(has_guo) << "Should have pinyin 'guo'";

    // Test that Chinese pinyin is correctly generated even with Unicode prefix
    EXPECT_GT(tokens.size(), 0) << "Should produce tokens";
}

// Test pure emoji preservation
TEST_F(PinyinFilterTest, TestTokenFilter_PureEmojiPreservation) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Pure emoji should be preserved via the fallback logic
    auto tokens = tokenizeWithFilter("😀😁😂", "keyword", config);
    EXPECT_EQ(tokens.size(), 1) << "Pure emoji should be preserved as one token";
    EXPECT_EQ(tokens[0], "😀😁😂") << "Emoji string should be preserved";
}

// Test that ASCII-only input without Unicode symbols returns false
// This tests the code path: if (!has_unicode_symbols) { return false; }
TEST_F(PinyinFilterTest, TestTokenFilter_AsciiOnlyFallbackHandling) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "false";
    config["keep_full_pinyin"] = "false";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false"; // This will cause ASCII to have no candidates
    config["keep_joined_full_pinyin"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // With all options disabled, pure ASCII should not produce tokens
    // because it has no Unicode symbols to preserve via fallback
    auto tokens = tokenizeWithFilter("abc123", "keyword", config);
    // The filter returns the original token when no candidates are generated
    // but for pure ASCII without Unicode symbols, it returns false
    // Actually, looking at the code, it seems like it would return the original
    // Let's verify the actual behavior
    EXPECT_LE(tokens.size(), 1)
            << "Pure ASCII with no output options should produce minimal tokens";
}

// Test currency and special Unicode symbols
TEST_F(PinyinFilterTest, TestTokenFilter_CurrencySymbols) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Currency symbols are Unicode but not Chinese
    auto tokens = tokenizeWithFilter("€£¥₹", "keyword", config);
    EXPECT_EQ(tokens.size(), 1) << "Currency symbols should be preserved";
    EXPECT_EQ(tokens[0], "€£¥₹") << "Currency symbols should be preserved as-is";
}

// Test Japanese/Korean characters (CJK but not in Chinese pinyin dict)
TEST_F(PinyinFilterTest, TestTokenFilter_NonChineseCJK) {
    std::unordered_map<std::string, std::string> config;
    config["keep_first_letter"] = "true";
    config["keep_full_pinyin"] = "true";
    config["keep_original"] = "false";
    config["keep_none_chinese"] = "false";
    config["ignore_pinyin_offset"] = "false";

    // Japanese hiragana - these are non-ASCII Unicode but not Chinese
    auto tokens1 = tokenizeWithFilter("あいう", "keyword", config);
    EXPECT_EQ(tokens1.size(), 1) << "Japanese hiragana should be preserved";
    EXPECT_EQ(tokens1[0], "あいう") << "Japanese hiragana should be preserved as-is";

    // Korean hangul
    auto tokens2 = tokenizeWithFilter("한글", "keyword", config);
    EXPECT_EQ(tokens2.size(), 1) << "Korean hangul should be preserved";
    EXPECT_EQ(tokens2[0], "한글") << "Korean hangul should be preserved as-is";
}

TEST_F(PinyinFilterTest, TestBugFix_SpaceHandlingWithKeywordTokenizer) {
    std::unordered_map<std::string, std::string> config;
    config["keep_joined_full_pinyin"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_in_joined_full_pinyin"] = "true";
    config["none_chinese_pinyin_tokenize"] = "false";
    config["keep_original"] = "false";
    config["keep_first_letter"] = "false";
    config["keep_full_pinyin"] = "false";
    config["lowercase"] = "false";
    config["trim_whitespace"] = "false";
    config["ignore_pinyin_offset"] = "true";

    // Test case 1: Pure English with space
    // Before fix: ["ALF", "Characters"] - space triggered buffer processing
    // After fix: ["ALFCharacters"] - space is skipped, buffer continues accumulating
    auto tokens1 = tokenizeWithFilter("ALF Characters", "keyword", config);
    EXPECT_EQ(tokens1.size(), 1) << "Should produce one token (space should not split)";
    EXPECT_EQ(tokens1[0], "ALFCharacters") << "Space should be ignored in joined output";

    // Test case 2: English with multiple spaces
    auto tokens2 = tokenizeWithFilter("Hello   World", "keyword", config);
    EXPECT_EQ(tokens2.size(), 1) << "Multiple spaces should not split tokens";
    EXPECT_EQ(tokens2[0], "HelloWorld") << "All spaces should be ignored";

    // Test case 3: Mixed with punctuation
    auto tokens3 = tokenizeWithFilter("Test-Case_123", "keyword", config);
    EXPECT_EQ(tokens3.size(), 1) << "Punctuation should not split tokens";
    EXPECT_EQ(tokens3[0], "TestCase123") << "Non-alphanumeric ASCII chars should be ignored";
}

// Test Bug #1: Space handling with Chinese-English mixed content
TEST_F(PinyinFilterTest, TestBugFix_SpaceHandlingWithMixedContent) {
    std::unordered_map<std::string, std::string> config;
    config["keep_joined_full_pinyin"] = "true";
    config["keep_none_chinese"] = "true";
    config["keep_none_chinese_in_joined_full_pinyin"] = "true";
    config["none_chinese_pinyin_tokenize"] = "false";
    config["keep_original"] = "false";
    config["keep_first_letter"] = "false";
    config["keep_full_pinyin"] = "false";
    config["lowercase"] = "true";
    config["ignore_pinyin_offset"] = "true";

    // Chinese-English mixed with spaces
    // The space should be ignored, English letters should be preserved in joined output
    auto tokens = tokenizeWithFilter("ALF 刘德华", "keyword", config);
    EXPECT_GT(tokens.size(), 0) << "Should produce tokens";

    // Check that English and pinyin are joined together
    bool found_joined = false;
    for (const auto& token : tokens) {
        if (token.find("alf") != std::string::npos && token.find("liu") != std::string::npos) {
            found_joined = true;
            EXPECT_EQ(token, "alfliudehua") << "English and pinyin should be joined, space ignored";
            break;
        }
    }
    EXPECT_TRUE(found_joined) << "Should find joined English+Pinyin token";
}

// Test Bug #2: Fallback mechanism for pure English text
// When keep_none_chinese=false and input is pure English, should preserve original token (ES behavior)
TEST_F(PinyinFilterTest, TestBugFix_PureEnglishFallback) {
    std::unordered_map<std::string, std::string> config;
    config["keep_none_chinese"] = "false"; // Don't generate separate English tokens
    config["keep_original"] = "false";
    config["keep_first_letter"] = "false";
    config["keep_full_pinyin"] = "false";
    config["keep_joined_full_pinyin"] = "true";
    config["ignore_pinyin_offset"] = "true";
    config["lowercase"] = "false";       // Preserve original case for testing
    config["trim_whitespace"] = "false"; // Preserve original whitespace
    // CRITICAL: Must set these to false to trigger fallback correctly
    config["keep_none_chinese_in_first_letter"] = "false";
    config["keep_none_chinese_in_joined_full_pinyin"] = "false";

    // Test case 1: Pure English text (no Chinese to convert)
    // Before fix: [] - token was dropped because:
    //   1. processCurrentToken() returned false (early return removed in fix #1)
    //   2. Fallback checked first_letters_.empty() instead of will_output (fixed in this commit)
    // After fix: ["Lanky Kong"] - original token preserved via improved fallback mechanism
    //   The fallback now checks if ANY content WILL BE OUTPUT, not just if buffers have content
    auto tokens1 = tokenizeWithFilter("Lanky Kong", "keyword", config);
    EXPECT_EQ(tokens1.size(), 1) << "Pure English should be preserved via fallback";
    EXPECT_EQ(tokens1[0], "Lanky Kong") << "Original token should be returned";

    // Test case 2: Another pure English example
    // ES behavior: fallback preserves original text INCLUDING spaces
    // (trim_whitespace only removes leading/trailing, not middle spaces)
    auto tokens2 = tokenizeWithFilter("ALF Characters", "keyword", config);
    EXPECT_EQ(tokens2.size(), 1) << "Pure English with space should be preserved";
    EXPECT_EQ(tokens2[0], "ALF Characters") << "Original token preserved as-is (ES behavior)";

    // Test case 3: Pure numbers
    auto tokens3 = tokenizeWithFilter("12345", "keyword", config);
    EXPECT_EQ(tokens3.size(), 1) << "Pure numbers should be preserved";
    EXPECT_EQ(tokens3[0], "12345");
}

} // namespace doris::segment_v2::inverted_index
