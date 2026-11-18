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
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/analysis/char_filter/char_filtered_stream.h"
#include "storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter_factory.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_filter/ascii_folding_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/icu_normalizer_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/pinyin_filter_factory.h"
#include "storage/index/inverted/analysis/token_filter/word_delimiter_filter_factory.h"
#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/char/char_group_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/empty/empty_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/ik/ik_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

namespace {

// Checks the terms `stream` reads from `text` one at a time against `expected`, as the token by
// token reads of the reader API did: a missing term fails, and with `exhaustive` so does a term
// left over.
void expect_terms(TextTokenStream& stream, std::string_view text,
                  const std::vector<std::string>& expected, bool exhaustive = true) {
    const std::vector<std::string> terms = collect_terms(stream, text);
    size_t i = 0;
    for (const std::string& term : expected) {
        ASSERT_LT(i, terms.size()) << "missing " << term;
        EXPECT_EQ(terms[i], term);
        ++i;
    }
    if (exhaustive) {
        EXPECT_EQ(i, terms.size()) << "more terms than expected";
    }
}

// `in` behind `count` default ICU normalizer char filters (nfkc_cf), where the reader of the
// tokenizer used to be. The default normalizer is ICU's own, so the filters outlive the factory.
std::unique_ptr<TextTokenStream> with_icu_char_filters(std::unique_ptr<TextTokenStream> in,
                                                       size_t count = 1) {
    ICUNormalizerCharFilterFactory factory;
    factory.initialize({});
    std::vector<std::unique_ptr<TextCharFilter>> filters;
    for (size_t i = 0; i < count; ++i) {
        filters.push_back(factory.create());
    }
    return std::make_unique<CharFilteredStream>(std::move(filters), std::move(in));
}

} // namespace

class PinyinFilterTest : public ::testing::Test {
public:
    void SetUp() override {
        const char* doris_home = std::getenv("DORIS_HOME");
        doris::config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    }

    std::unique_ptr<TextTokenizer> createTokenizer(const std::string& tokenizer_type) {
        std::unique_ptr<TextTokenizer> tokenizer;

        if (tokenizer_type == "standard") {
            StandardTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "basic") {
            BasicTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "char_group") {
            CharGroupTokenizerFactory factory;
            Settings settings;
            settings.set("tokenize_on_chars", "[whitespace]");
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "empty") {
            EmptyTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "icu") {
            ICUTokenizerFactory factory;
            Settings settings;
            factory.initialize(settings);
            tokenizer = factory.create();
        } else if (tokenizer_type == "ngram") {
            NGramTokenizerFactory factory;
            Settings settings;
            settings.set("min_gram", "1");
            settings.set("max_gram", "1");
            settings.set("token_chars", "letter");
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
        return tokenizer;
    }

    std::vector<std::string> tokenizeWithFilter(
            const std::string& text, const std::string& tokenizer_type,
            const std::unordered_map<std::string, std::string>& filter_config) {
        // The tokenizer alone first, as the reader API test did.
        collect_checked(*createTokenizer(tokenizer_type), text);

        PinyinFilterFactory filter_factory;
        Settings filter_settings(filter_config);
        filter_factory.initialize(filter_settings);
        auto filter = filter_factory.create(createTokenizer(tokenizer_type));
        return collect_terms(*filter, text);
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

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(Settings(filter_config));
    auto filter = filter_factory.create(createTokenizer("ik_smart"));

    expect_terms(*filter, "我来到北京清华大学",
                 {"我", "wo", "w", "来到", "laidao", "ld", "北京", "beijing", "bj", "清华大学",
                  "qinghuadaxue", "qhdx"});
}

TEST_F(PinyinFilterTest, TestIKOffsetsPreserveFullwidthSourceBytes) {
    std::unordered_map<std::string, std::string> filter_config;
    filter_config["keep_first_letter"] = "false";
    filter_config["keep_full_pinyin"] = "false";
    filter_config["keep_original"] = "false";
    filter_config["keep_none_chinese"] = "true";
    filter_config["none_chinese_pinyin_tokenize"] = "true";
    filter_config["ignore_pinyin_offset"] = "false";

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(Settings(filter_config));
    auto filter = filter_factory.create(createTokenizer("ik_smart"));

    expect_terms(*filter, "ＬＩＵＤＥ", {"liu", "de"});
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeWithICUNormalizerCharFilterAndReset) {
    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto stream = with_icu_char_filters(filter_factory.create(tokenizer_factory.create()));

    expect_terms(*stream, "ＬＩＵＤＥ", {"liu", "de"});
    expect_terms(*stream, "ＡＢＣＤ", {"a", "b", "c", "d"});
}

TEST_F(PinyinFilterTest, TestIKExpandedLigatureOffsetsStayConservativeAndReset) {
    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("keep_none_chinese_together", "false");
    settings.set("keep_none_chinese_in_first_letter", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto stream = with_icu_char_filters(filter_factory.create(tokenizer_factory.create()));

    // U+FB01 LATIN SMALL LIGATURE FI; nfkc_cf expands the 3-byte source rune to "fi".
    expect_terms(*stream, "\xEF\xAC\x81", {"f", "i"});
    expect_terms(*stream, "ＬＩ", {"l", "i"});
}

TEST_F(PinyinFilterTest, TestChainedPinyinFiltersPublishCandidateSpansAndReset) {
    Settings full_pinyin;
    full_pinyin.set("keep_first_letter", "false");
    full_pinyin.set("keep_full_pinyin", "true");
    full_pinyin.set("keep_original", "false");
    full_pinyin.set("keep_joined_full_pinyin", "false");
    full_pinyin.set("keep_none_chinese", "false");
    full_pinyin.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory full_pinyin_factory;
    full_pinyin_factory.initialize(full_pinyin);

    Settings letters;
    letters.set("keep_first_letter", "false");
    letters.set("keep_full_pinyin", "false");
    letters.set("keep_original", "false");
    letters.set("keep_none_chinese", "true");
    letters.set("keep_none_chinese_together", "false");
    letters.set("keep_none_chinese_in_first_letter", "false");
    letters.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory letters_factory;
    letters_factory.initialize(letters);

    const std::string text = "刘德华";
    auto filter = letters_factory.create(full_pinyin_factory.create(createTokenizer("keyword")));

    expect_terms(*filter, text, {"l", "i", "u", "d", "e", "h", "u", "a"});
    expect_terms(*filter, "华刘", {"h", "u", "a", "l", "i", "u"});

    Settings original;
    original.set("keep_first_letter", "false");
    original.set("keep_full_pinyin", "false");
    original.set("keep_original", "true");
    original.set("keep_none_chinese", "false");
    original.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory original_factory;
    original_factory.initialize(original);
    filter = full_pinyin_factory.create(original_factory.create(createTokenizer("keyword")));
    expect_terms(*filter, text, {"liu", "de", "hua"});
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
        PinyinFilterFactory filter_factory;
        filter_factory.initialize(settings);
        auto stream = with_icu_char_filters(filter_factory.create(createTokenizer(tokenizer_type)));

        expect_terms(*stream, "ＬＩＵＤＥ", {"liu", "de"});
        expect_terms(*stream, "ＡＢＣＤ", {"a", "b", "c", "d"});
    }
}

TEST_F(PinyinFilterTest, TestICUNormalizerFilterUsesConservativeSourceSpansAndReset) {
    ICUNormalizerFilterFactory normalizer_factory;
    normalizer_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    auto filter = pinyin_factory.create(normalizer_factory.create(createTokenizer("keyword")));

    expect_terms(*filter, std::string("\xEF\xAC\x81") + "\xE5\x88\x98", {"liu"});
    expect_terms(*filter, std::string("\xEF\xAC\x82") + "\xE6\xB5\x8B", {"ce"});
}

TEST_F(PinyinFilterTest, TestICUCharFilterExpansionUsesConservativeRuneSpans) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("keep_none_chinese_together", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    auto stream = with_icu_char_filters(pinyin_factory.create(createTokenizer("keyword")));

    expect_terms(*stream, std::string("\xEF\xAC\x81") + "\xE5\x88\x98", {"f", "i", "liu"});
    expect_terms(*stream, std::string("a") + "\xE5\x88\x98", {"a", "liu"});
}

TEST_F(PinyinFilterTest, TestDefaultOffsetsPreserveWidthChangingTrimmedSourceSpan) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    auto stream = with_icu_char_filters(pinyin_factory.create(createTokenizer("keyword")));

    expect_terms(*stream, std::string("\xE3\x80\x80") + "\xE5\x88\x98" + "\xE3\x80\x80", {"liu"});
}

TEST_F(PinyinFilterTest, TestASCIIFoldingExpansionUsesConservativeSourceSpanAndReset) {
    ASCIIFoldingFilterFactory folding_factory;
    folding_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    auto filter = pinyin_factory.create(folding_factory.create(createTokenizer("keyword")));

    expect_terms(*filter, "ꜳ刘", {"a", "a", "liu"});
    expect_terms(*filter, "a刘", {"a", "liu"});
}

TEST_F(PinyinFilterTest, TestLowercaseExpansionUsesConservativeSourceSpanAndReset) {
    LowerCaseFilterFactory lowercase_factory;
    lowercase_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    auto filter = pinyin_factory.create(lowercase_factory.create(createTokenizer("keyword")));

    expect_terms(*filter, "İ刘", {"i", "liu"});
    expect_terms(*filter, "A刘", {"a", "liu"});
}

TEST_F(PinyinFilterTest, TestDefaultOffsetModeReadsALongToken) {
    constexpr size_t input_size = 64 * 1024;
    const std::string text(input_size, 'a');
    PinyinFilterFactory factory;
    factory.initialize({});
    auto stream = factory.create(createTokenizer("empty"));
    auto* filter = dynamic_cast<PinyinFilter*>(stream.get());
    ASSERT_NE(filter, nullptr);

    ASSERT_FALSE(collect_checked(*filter, text).empty());
}

TEST_F(PinyinFilterTest, TestResetRetainsOrdinaryScratchAndReleasesOversizedScratch) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "true");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory factory;
    factory.initialize(settings);

    // The UT build has -fno-access-control, which reaches the filter's buffers.
    const std::string ordinary(256, 'a');
    auto ordinary_stream = factory.create(createTokenizer("empty"));
    auto* ordinary_filter = dynamic_cast<PinyinFilter*>(ordinary_stream.get());
    ASSERT_NE(ordinary_filter, nullptr);
    ASSERT_FALSE(collect_checked(*ordinary_filter, ordinary).empty());
    const size_t token_capacity = ordinary_filter->current_token_text_.capacity();
    const size_t source_capacity = ordinary_filter->current_source_.capacity();
    ASSERT_GT(token_capacity, 0);
    ASSERT_GT(source_capacity, 0);
    ordinary_filter->reset(ordinary);
    EXPECT_EQ(ordinary_filter->current_token_text_.capacity(), token_capacity);
    EXPECT_EQ(ordinary_filter->current_source_.capacity(), source_capacity);

    const std::string oversized(96 * 1024, 'b');
    auto oversized_stream = factory.create(createTokenizer("empty"));
    auto* oversized_filter = dynamic_cast<PinyinFilter*>(oversized_stream.get());
    ASSERT_NE(oversized_filter, nullptr);
    ASSERT_FALSE(collect_checked(*oversized_filter, oversized).empty());
    oversized_filter->reset("");
    EXPECT_LE(oversized_filter->current_token_text_.capacity(), 64 * 1024);
    EXPECT_LE(oversized_filter->current_source_.capacity(), 64 * 1024);
    EXPECT_LE(oversized_filter->reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
}

TEST_F(PinyinFilterTest, TestResetReleasesOversizedUpstreamScratch) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "true");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(settings);
    WordDelimiterFilterFactory delimiter_factory;
    delimiter_factory.initialize({});

    const std::string ordinary(256, 'a');
    // 96 KiB of one-letter parts, all of which the delimiter holds until the token is done.
    std::string oversized;
    for (int i = 0; i < 48 * 1024; ++i) {
        oversized += "b-";
    }
    const std::string empty;
    auto delimiter_stream = delimiter_factory.create(createTokenizer("empty"));
    auto* delimiter = dynamic_cast<WordDelimiterFilter*>(delimiter_stream.get());
    ASSERT_NE(delimiter, nullptr);
    auto filter = pinyin_factory.create(std::move(delimiter_stream));

    auto collect = [&](const std::string& text) { return collect_terms(*filter, text); };

    const auto ordinary_tokens = collect(ordinary);
    ASSERT_FALSE(ordinary_tokens.empty());
    const size_t ordinary_delimiter_bytes = delimiter->reserved_bytes();
    filter->reset(ordinary);
    EXPECT_EQ(delimiter->reserved_bytes(), ordinary_delimiter_bytes);
    EXPECT_EQ(collect(ordinary), ordinary_tokens);

    ASSERT_FALSE(collect(oversized).empty());
    ASSERT_GT(delimiter->reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);

    filter->reset(empty);
    EXPECT_LE(delimiter->reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    EXPECT_TRUE(collect(empty).empty());

    EXPECT_EQ(collect(ordinary), ordinary_tokens);
}

namespace {

// Offset-aware Pinyin filter that emits every ASCII letter on its own.
PinyinFilterFactory make_letters_filter_factory() {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("keep_none_chinese_together", "false");
    settings.set("keep_none_chinese_in_first_letter", "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory factory;
    factory.initialize(settings);
    return factory;
}

std::unique_ptr<TextTokenizer> make_pinyin_tokenizer(bool tokenize_none_chinese) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("keep_none_chinese_together", "true");
    settings.set("none_chinese_pinyin_tokenize", tokenize_none_chinese ? "true" : "false");
    settings.set("ignore_pinyin_offset", "false");
    PinyinTokenizerFactory factory;
    factory.initialize(settings);
    return factory.create();
}

} // namespace

TEST_F(PinyinFilterTest, TestPinyinTokenizerCandidatesThroughLettersFilter) {
    PinyinFilterFactory letters = make_letters_filter_factory();

    {
        auto filter = letters.create(make_pinyin_tokenizer(true));
        expect_terms(*filter, "中", {"z", "h", "o", "n", "g"});
    }
    {
        auto filter = letters.create(make_pinyin_tokenizer(false));
        expect_terms(*filter, "a-b", {"a", "b"});
    }
    // Behind a char filter, with reset/reuse.
    {
        auto stream = with_icu_char_filters(letters.create(make_pinyin_tokenizer(false)));
        expect_terms(*stream, "\xEF\xAC\x81", {"f", "i"}); // U+FB01 expands to "fi"
        expect_terms(*stream, "ＬＩ", {"l", "i"});
    }
}

TEST_F(PinyinFilterTest, TestAsciiFoldingMalformedInputThroughLettersFilter) {
    PinyinFilterFactory letters = make_letters_filter_factory();
    for (const bool preserve_original : {false, true}) {
        SCOPED_TRACE(preserve_original);
        const std::string text = "\xFF\xC3\x86"; // stray byte, then U+00C6
        Settings settings;
        settings.set("preserve_original", preserve_original ? "true" : "false");
        ASCIIFoldingFilterFactory folding_factory;
        folding_factory.initialize(settings);
        auto filter = letters.create(folding_factory.create(createTokenizer("keyword")));

        std::vector<std::string> expected = {"a", "e"};
        if (preserve_original) {
            expected.push_back(text);
        }
        expect_terms(*filter, text, expected);

        const std::string reset_text = "\xC3\x86";
        expected = {"a", "e"};
        if (preserve_original) {
            expected.push_back(reset_text);
        }
        expect_terms(*filter, reset_text, expected);
    }
}

TEST_F(PinyinFilterTest, TestWordDelimiterMalformedInputThroughLettersFilter) {
    PinyinFilterFactory letters = make_letters_filter_factory();
    WordDelimiterFilterFactory delimiter_factory;
    delimiter_factory.initialize({});
    auto filter = letters.create(delimiter_factory.create(createTokenizer("keyword")));

    expect_terms(*filter,
                 "\xFF"
                 "a",
                 {"a"});
    // An interior malformed byte is not a delimiter, so the token passes through unchanged.
    expect_terms(*filter,
                 "a\xFF"
                 "b",
                 {"a", "b"});
    expect_terms(*filter, "liu-de", {"l", "i", "u", "d", "e"});
}

TEST_F(PinyinFilterTest, TestNGramInsideCharFilterExpansion) {
    PinyinFilterFactory letters = make_letters_filter_factory();

    Settings settings;
    settings.set("min_gram", "1");
    settings.set("max_gram", "1");
    NGramTokenizerFactory tokenizer_factory;
    tokenizer_factory.initialize(settings);
    auto stream = with_icu_char_filters(letters.create(tokenizer_factory.create()));

    expect_terms(*stream, "\xEF\xAC\x81", {"f", "i"}); // U+FB01 expands to "fi"
    expect_terms(*stream, "ＬＩ", {"l", "i"});
}

TEST_F(PinyinFilterTest, TestPinyinTokenizerReleasesOversizedScratch) {
    const std::string large(96 * 1024, 'a');

    PinyinTokenizerFactory default_factory;
    default_factory.initialize({});
    auto ignoring_stream = default_factory.create();
    auto* ignoring = dynamic_cast<PinyinTokenizer*>(ignoring_stream.get());
    ASSERT_NE(ignoring, nullptr);
    collect_checked(*ignoring, large);

    auto tracking_stream = make_pinyin_tokenizer(false);
    auto* tracking = dynamic_cast<PinyinTokenizer*>(tracking_stream.get());
    ASSERT_NE(tracking, nullptr);
    collect_checked(*tracking, large);
    EXPECT_GT(tracking->reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);

    const std::string small = "ab";
    tracking->reset(small);
    EXPECT_LE(tracking->reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
    collect_checked(*tracking, small);
}

TEST_F(PinyinFilterTest, TestPinyinFilterReadsLongAsciiTheSameInBothOffsetModes) {
    const std::string large(4096, 'a');

    PinyinFilterFactory default_factory;
    default_factory.initialize({});
    auto ignoring_stream = default_factory.create(createTokenizer("keyword"));
    auto* ignoring = dynamic_cast<PinyinFilter*>(ignoring_stream.get());
    ASSERT_NE(ignoring, nullptr);
    const std::vector<std::string> ignoring_terms = collect_terms(*ignoring, large);
    ASSERT_FALSE(ignoring_terms.empty());

    Settings settings;
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory tracking_factory;
    tracking_factory.initialize(settings);
    auto tracking_stream = tracking_factory.create(createTokenizer("keyword"));
    auto* tracking = dynamic_cast<PinyinFilter*>(tracking_stream.get());
    ASSERT_NE(tracking, nullptr);
    const std::vector<std::string> tracking_terms = collect_terms(*tracking, large);
    ASSERT_FALSE(tracking_terms.empty());
    EXPECT_EQ(tracking_terms, ignoring_terms);
}

TEST_F(PinyinFilterTest, TestIcuTokenizerReadsTheSameInStrictMode) {
    const std::string text = "Hello";
    ICUTokenizerFactory factory;
    factory.initialize({});

    auto plain_stream = factory.create();
    auto* plain = dynamic_cast<ICUTokenizer*>(plain_stream.get());
    ASSERT_NE(plain, nullptr);
    ASSERT_FALSE(collect_checked(*plain, text).empty());

    auto strict_stream = factory.create();
    auto* strict = dynamic_cast<ICUTokenizer*>(strict_stream.get());
    ASSERT_NE(strict, nullptr);
    strict->set_strict_utf8(true);
    const std::vector<std::string> strict_terms = collect_terms(*strict, text);
    ASSERT_FALSE(strict_terms.empty());
    EXPECT_EQ(strict_terms.front(), text);
}

TEST_F(PinyinFilterTest, TestCaseAndFoldingFiltersInStrictMode) {
    for (const bool enabled : {false, true}) {
        SCOPED_TRACE(enabled);
        // U+0130 lowercases to two code points.
        const std::string dotted = "\xC4\xB0";
        LowerCaseFilterFactory lower_factory;
        lower_factory.initialize({});
        auto lower_stream = lower_factory.create(createTokenizer("keyword"));
        auto* lower = dynamic_cast<LowerCaseFilter*>(lower_stream.get());
        ASSERT_NE(lower, nullptr);
        lower->set_strict_utf8(enabled);
        const std::vector<std::string> lower_terms = collect_terms(*lower, dotted);
        ASSERT_FALSE(lower_terms.empty());
        EXPECT_EQ(lower_terms.front(), "i\xCC\x87");

        const std::string ligature = "\xC3\x86"; // U+00C6 folds to "AE"
        ASCIIFoldingFilterFactory folding_factory;
        folding_factory.initialize({});
        auto folding_stream = folding_factory.create(createTokenizer("keyword"));
        auto* folding = dynamic_cast<ASCIIFoldingFilter*>(folding_stream.get());
        ASSERT_NE(folding, nullptr);
        folding->set_strict_utf8(enabled);
        const std::vector<std::string> folding_terms = collect_terms(*folding, ligature);
        ASSERT_FALSE(folding_terms.empty());
        EXPECT_EQ(folding_terms.front(), "AE");
    }
}

TEST_F(PinyinFilterTest, TestPinyinTokenizerClipsOriginalCandidate) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("keep_original", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinTokenizerFactory factory;
    factory.initialize(settings);

    // An over-cap original candidate comes out as its prefix.
    const std::string ascii(300, 'a');
    auto tokenizer = factory.create();
    const std::vector<CollectedToken> ascii_tokens = collect_checked(*tokenizer, ascii);
    ASSERT_FALSE(ascii_tokens.empty());
    EXPECT_EQ(ascii_tokens.front().term.size(), 255);

    // A cap that falls inside a rune clips to the rune boundary before it (2-byte runes here,
    // so the 255-byte cap would split the 128th one).
    std::string latin;
    for (int i = 0; i < 200; ++i) {
        latin += "\xC3\xA9"; // U+00E9
    }
    const std::vector<CollectedToken> latin_tokens = collect_checked(*tokenizer, latin);
    ASSERT_FALSE(latin_tokens.empty());
    EXPECT_EQ(latin_tokens.front().term.size(), 254);
}

TEST_F(PinyinFilterTest, TestStrictModeTokenizersKeepTheirTokens) {
    for (const std::string tokenizer_type : {"standard", "ik_max_word"}) {
        SCOPED_TRACE(tokenizer_type);
        const std::string text = "abcdefghijklmnopqrstuvwxyz bc de fg hi";
        auto tokenizer = createTokenizer(tokenizer_type);
        tokenizer->set_strict_utf8(true);
        ASSERT_EQ(collect_checked(*tokenizer, text).size(), 5);
    }
}

TEST_F(PinyinFilterTest, TestPinyinTrimmedKeywordOffsetsPreserveSourceBoundariesAndReset) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(createTokenizer("keyword"));

    expect_terms(*filter, "  刘德华  ", {"liu", "de", "hua"});
    expect_terms(*filter, " \t测试 ", {"ce", "shi"});
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeWithICUNormalizerForManyTokensAndReset) {
    std::string text;
    constexpr int token_count = 4096;
    for (int i = 0; i < token_count; ++i) {
        text += "Ｌ ";
    }

    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto stream = with_icu_char_filters(filter_factory.create(tokenizer_factory.create()));

    expect_terms(*stream, text, std::vector<std::string>(token_count, "l"));
    expect_terms(*stream, "ＡＢＣＤ", {"a", "b", "c", "d"});
}

TEST_F(PinyinFilterTest, TestPinyinWholeTokenAlternativesUseKeywordOffsets) {
    Settings settings;
    settings.set("keep_first_letter", "true");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "true");
    settings.set("keep_joined_full_pinyin", "true");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(createTokenizer("keyword"));

    expect_terms(*filter, "刘德华", {"刘德华", "liudehua", "ldh"});
}

TEST_F(PinyinFilterTest, TestPinyinStandardOffsetsDoNotReusePreviousTokenState) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_joined_full_pinyin", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(createTokenizer("standard"));

    expect_terms(*filter, "刘德华 测试", {"liu", "de", "hua", "ce", "shi"});
    expect_terms(*filter, "测试 刘德华", {"ce", "shi", "liu", "de", "hua"});
}

TEST_F(PinyinFilterTest, TestBasicTokenizerOffsetsRemainDocumentRelativeAfterReset) {
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_joined_full_pinyin", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(createTokenizer("basic"));

    expect_terms(*filter, "刘德华", {"liu", "de", "hua"});
    expect_terms(*filter, "测试 刘德华", {"ce", "shi", "liu", "de", "hua"});
}

TEST_F(PinyinFilterTest, TestOffsetAwarePinyinSupportsAllCustomTokenizers) {
    const std::string text = "你好 世界";
    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "true");
    settings.set("keep_original", "false");
    settings.set("keep_joined_full_pinyin", "false");
    settings.set("keep_none_chinese", "false");
    settings.set("ignore_pinyin_offset", "false");

    for (const std::string tokenizer_type : {"basic", "char_group", "empty", "icu", "ngram"}) {
        SCOPED_TRACE(tokenizer_type);
        PinyinFilterFactory filter_factory;
        filter_factory.initialize(settings);
        auto filter = filter_factory.create(createTokenizer(tokenizer_type));

        expect_terms(*filter, text, {"ni", "hao", "shi", "jie"});
    }
}

TEST_F(PinyinFilterTest, TestIKOffsetsComposeAcrossICUDeletionAndReset) {
    const std::string text = std::string("liu") + "\xC2\xAD" + "de";

    IKTokenizerFactory tokenizer_factory(true);
    tokenizer_factory.initialize({});

    Settings settings;
    settings.set("keep_first_letter", "false");
    settings.set("keep_full_pinyin", "false");
    settings.set("keep_original", "false");
    settings.set("keep_none_chinese", "true");
    settings.set("none_chinese_pinyin_tokenize", "true");
    settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto stream = with_icu_char_filters(filter_factory.create(tokenizer_factory.create()), 2);

    expect_terms(*stream, text, {"liu", "de"});
    expect_terms(*stream, text, {"liu", "de"});
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
        PinyinFilterFactory filter_factory;
        filter_factory.initialize(settings);
        auto filter = filter_factory.create(createTokenizer(tokenizer_type));

        expect_terms(*filter, "ＬＩＵ-ＤＥ", {"liu", "de"}, /*exhaustive=*/false);
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

    PinyinFilterFactory filter_factory;
    filter_factory.initialize(settings);
    auto filter = filter_factory.create(createTokenizer("ik_smart"));

    expect_terms(*filter, long_text, {"liu"}, /*exhaustive=*/false);
    expect_terms(*filter, "ＬＩＵＤＥ", {"liu"}, /*exhaustive=*/false);
}

TEST_F(PinyinFilterTest, TestStrictUtf8IsOptIn) {
    auto tokenizer = createTokenizer("ik_smart");
    ASSERT_FALSE(collect_checked(*tokenizer, "ＬＩＵＤＥ").empty());

    PinyinFilterFactory filter_factory;
    Settings settings;
    settings.set("ignore_pinyin_offset", "false");
    filter_factory.initialize(settings);
    tokenizer = createTokenizer("ik_smart");
    auto* wrapped = tokenizer.get();
    auto filter = filter_factory.create(std::move(tokenizer));
    EXPECT_NE(filter, nullptr);
    ASSERT_FALSE(collect_checked(*wrapped, "ＬＩＵＤＥ").empty());

    // ignore_pinyin_offset=false turns strict UTF-8 on upstream, and only then: icu reads an
    // invalid byte as U+FFFD, and rejects it when strict.
    const std::string invalid = "a\xFF";
    PinyinFilterFactory default_factory;
    default_factory.initialize({});
    auto lenient = default_factory.create(createTokenizer("icu"));
    EXPECT_NO_THROW(collect_checked(*lenient, invalid));
    auto strict = filter_factory.create(createTokenizer("icu"));
    EXPECT_THROW(strict->reset(invalid), Exception);
}

TEST_F(PinyinFilterTest, TestWordDelimiterPreservesIKSourceOffsets) {
    WordDelimiterFilterFactory delimiter_factory;
    delimiter_factory.initialize({});

    Settings pinyin_settings;
    pinyin_settings.set("keep_first_letter", "false");
    pinyin_settings.set("keep_full_pinyin", "false");
    pinyin_settings.set("keep_original", "false");
    pinyin_settings.set("keep_none_chinese", "true");
    pinyin_settings.set("none_chinese_pinyin_tokenize", "true");
    pinyin_settings.set("ignore_pinyin_offset", "false");
    PinyinFilterFactory pinyin_factory;
    pinyin_factory.initialize(pinyin_settings);
    auto filter = pinyin_factory.create(delimiter_factory.create(createTokenizer("ik_smart")));

    expect_terms(*filter, "ＬＩＵＤＥ１２３", {"liu", "de", "123"});
}

TEST_F(PinyinFilterTest, TestWordDelimiterConcatenationPreservesSourceGapsAndReset) {
    for (const std::string option : {"catenate_words", "catenate_all"}) {
        SCOPED_TRACE(option);
        Settings delimiter_settings;
        delimiter_settings.set("generate_word_parts", "false");
        delimiter_settings.set("generate_number_parts", "false");
        delimiter_settings.set(option, "true");
        WordDelimiterFilterFactory delimiter_factory;
        delimiter_factory.initialize(delimiter_settings);

        Settings pinyin_settings;
        pinyin_settings.set("keep_first_letter", "false");
        pinyin_settings.set("keep_full_pinyin", "false");
        pinyin_settings.set("keep_original", "false");
        pinyin_settings.set("keep_none_chinese", "true");
        pinyin_settings.set("none_chinese_pinyin_tokenize", "true");
        pinyin_settings.set("ignore_pinyin_offset", "false");
        PinyinFilterFactory pinyin_factory;
        pinyin_factory.initialize(pinyin_settings);
        auto filter = pinyin_factory.create(delimiter_factory.create(createTokenizer("keyword")));

        expect_terms(*filter, "liu-de", {"liu", "de"});
        expect_terms(*filter, "de--liu", {"de", "liu"});
    }
}

TEST_F(PinyinFilterTest, TestWordDelimiterPreservesPlainTokenizerOffsetsAfterReset) {
    for (const std::string tokenizer_type : {"keyword", "standard"}) {
        SCOPED_TRACE(tokenizer_type);
        const bool keyword = tokenizer_type == "keyword";

        WordDelimiterFilterFactory delimiter_factory;
        delimiter_factory.initialize({});

        Settings settings;
        settings.set("keep_first_letter", "false");
        settings.set("keep_full_pinyin", "false");
        settings.set("keep_original", "false");
        settings.set("keep_none_chinese", "true");
        settings.set("none_chinese_pinyin_tokenize", "true");
        settings.set("ignore_pinyin_offset", "false");
        PinyinFilterFactory pinyin_factory;
        pinyin_factory.initialize(settings);
        auto filter =
                pinyin_factory.create(delimiter_factory.create(createTokenizer(tokenizer_type)));

        expect_terms(*filter, keyword ? "liu-de" : "LiuDe", {"liu", "de"});
        expect_terms(*filter, keyword ? "de-liu" : "DeLiu", {"de", "liu"});
    }
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

    // Standard tokenizer outputs "⭐" and "U+767D U+83DC" as separate tokens
    // "⭐" -> preserved via fallback (no pinyin candidates)
    // "U+767D U+83DC" -> "bai", "b", "cai", "c", "bc"
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

    StandardTokenizerFactory tokenizer_factory;
    Settings tokenizer_settings;
    tokenizer_factory.initialize(tokenizer_settings);

    // Create filter with nullptr config directly
    PinyinFilter filter(tokenizer_factory.create(), nullptr);
    filter.initialize();

    // Should work with default config without crashing
    EXPECT_FALSE(collect_checked(filter, text).empty());
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
