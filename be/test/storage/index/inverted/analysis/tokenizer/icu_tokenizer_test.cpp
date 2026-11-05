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

#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.h"

#include <gtest/gtest.h>
#include <unicode/locid.h>
#include <unicode/unistr.h>
#include <unicode/utf8.h>

#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/token_filter/lower_case_filter.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_factory.h"
#include "util/defer_op.h"

namespace doris::segment_v2::inverted_index {
namespace {

// The rule files of the source tree.
class ICUTokenizerStreamTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_dict_path = config::inverted_index_dict_path;
        config::inverted_index_dict_path = "./be/dict";
    }

    void TearDown() override { config::inverted_index_dict_path = _saved_dict_path; }

private:
    std::string _saved_dict_path;
};

// The builtin icu parser as production builds it.
TextAnalyzerPtr icu_analyzer(bool lowercase) {
    return InvertedIndexAnalyzer::create_builtin_analyzer(InvertedIndexParserType::PARSER_ICU, "",
                                                          lowercase ? "true" : "false", "");
}

// The custom icu_tokenizer, and with `lowercase` the tokenizer of the builtin parser.
std::unique_ptr<TextTokenStream> icu_tokenizer(bool lowercase) {
    return ICUTokenizerFactory(lowercase).create();
}

// What the builtin parser was before its tokenizer lowercased: the custom tokenizer followed by
// LowerCaseFilter.
std::unique_ptr<TextTokenStream> icu_with_lower_case_filter() {
    auto filter = std::make_unique<LowerCaseFilter>(icu_tokenizer(false));
    filter->initialize();
    return filter;
}

// The tokens of `text`, or that it threw.
struct Outcome {
    bool threw = false;
    std::vector<CollectedToken> tokens;
};

Outcome read(TextTokenStream& stream, std::string_view text) {
    try {
        return {.threw = false, .tokens = collect_checked(stream, text)};
    } catch (const Exception&) {
        return {.threw = true, .tokens = {}};
    }
}

// Values with every kind of term: ASCII, CJK, cased letters that ICU lowercases (some to longer
// strings), supplementary letters, emoji, invalid bytes, and words long enough for the 255 cut.
std::string random_value(std::mt19937_64& random) {
    static const std::vector<std::string> kPieces = {"a",
                                                     "B",
                                                     "z9",
                                                     "I",
                                                     "İ",
                                                     "ı",
                                                     "Σ",
                                                     "ΣΑΣ",
                                                     "Ω",
                                                     "ẞ",
                                                     "ß",
                                                     "ǅ",
                                                     "Ⱥ",
                                                     "K",
                                                     "É",
                                                     "é",
                                                     "Я",
                                                     " ",
                                                     ".",
                                                     ",",
                                                     "'",
                                                     "-",
                                                     "\n",
                                                     "3",
                                                     "1.5",
                                                     "１２",
                                                     "ＡＢ",
                                                     "中",
                                                     "文",
                                                     "國",
                                                     "々",
                                                     "ー",
                                                     "ひら",
                                                     "カタ",
                                                     "ｶﾀ",
                                                     "한국",
                                                     "ภาษาไทย",
                                                     "العربية",
                                                     "עברית",
                                                     "हिन्दी",
                                                     "\u0301",
                                                     "\u200D",
                                                     "\uFE0F",
                                                     "\u20E3",
                                                     "\U0001F600",
                                                     "#\uFE0F\u20E3",
                                                     "©",
                                                     "\U0001F469\u200D\u2764\uFE0F\u200D\U0001F469",
                                                     "\U0001F1FA\U0001F1F8",
                                                     "\U00020000",
                                                     "\U0001D400",
                                                     "\U00010400",
                                                     "\U00010428",
                                                     "\xC0\x80",
                                                     "\xED\xA0\x80",
                                                     "\xF4\x90\x80\x80",
                                                     "\xFF",
                                                     "\x80",
                                                     "\xC3",
                                                     "\xE4\xB8"};
    const size_t size = random() % 400;
    std::string value;
    if (random() % 4 == 0) {
        for (size_t i = 0; i < size; ++i) {
            value.push_back(static_cast<char>(random()));
        }
        return value;
    }
    while (value.size() < size) {
        const std::string& piece = kPieces[random() % kPieces.size()];
        const size_t repeat = random() % 8 == 0 ? random() % 300 : 1;
        for (size_t i = 0; i < repeat; ++i) {
            value += piece;
        }
    }
    return value;
}

TEST_F(ICUTokenizerStreamTest, LowercasesAsTheLowerCaseFilterDid) {
    for (bool strict : {false, true}) {
        SCOPED_TRACE(strict);
        auto candidate = icu_tokenizer(true);
        auto oracle = icu_with_lower_case_filter();
        candidate->set_strict_utf8(strict);
        oracle->set_strict_utf8(strict);
        std::mt19937_64 random(20261104 + strict);
        for (int round = 0; round < 3000; ++round) {
            const std::string value = random_value(random);
            const Outcome expected = read(*oracle, value);
            const Outcome actual = read(*candidate, value);
            ASSERT_EQ(actual.threw, expected.threw) << value;
            ASSERT_EQ(actual.tokens, expected.tokens) << value;
            // A writer drops a stream that threw.
            if (expected.threw) {
                candidate = icu_tokenizer(true);
                oracle = icu_with_lower_case_filter();
                candidate->set_strict_utf8(strict);
                oracle->set_strict_utf8(strict);
            }
        }
    }
}

TEST_F(ICUTokenizerStreamTest, LowercasesEachKindOfTerm) {
    const TextAnalyzerPtr lower = icu_analyzer(true);
    // ASCII, byte by byte.
    EXPECT_EQ(collect_terms(*lower, "Hello WORLD"), (std::vector<std::string> {"hello", "world"}));
    // ICU's root-locale mapping, which ends a Greek word with ς and may lengthen a term:
    // İ (2 bytes) becomes i and U+0307 (3 bytes), Ⱥ (2 bytes) becomes ⱥ (3 bytes).
    EXPECT_EQ(collect_terms(*lower, "ÀÉÎ ǅungla ΣΑΣ ΟΔΟΣ Ⱥ"),
              (std::vector<std::string> {"àéî", "ǆungla", "σας", "οδος", "ⱥ"}));
    EXPECT_EQ(collect_terms(*lower, "ISTANBUL İstanbul DİYARBAKIR"),
              (std::vector<std::string> {"istanbul", "i\u0307stanbul", "di\u0307yarbakir"}));
    // An ASCII term of a value that is not ASCII.
    EXPECT_EQ(collect_terms(*lower, "ABC İ"), (std::vector<std::string> {"abc", "i\u0307"}));
    // Full-width letters have case; half-width kana, Hangul and CJK have none.
    EXPECT_EQ(collect_terms(*lower, "ＡＢＣ ｶﾀｶﾅ 한국어 中文 カタカナ"),
              (std::vector<std::string> {"ａｂｃ", "ｶﾀｶﾅ", "한국어", "中文", "カタカナ"}));
    // The custom tokenizer and the builtin parser with lower_case=false keep the case.
    EXPECT_EQ(collect_terms(*icu_tokenizer(false), "Hello WORLD İ"),
              (std::vector<std::string> {"Hello", "WORLD", "İ"}));
    EXPECT_EQ(collect_terms(*icu_analyzer(false), "Hello WORLD İ"),
              (std::vector<std::string> {"Hello", "WORLD", "İ"}));
}

// The tokenizer skips the case mapping for a term of only these code points.
TEST_F(ICUTokenizerStreamTest, CJKPunctuationKanaAndIdeographsHaveNoCase) {
    for (UChar32 c = 0x3000; c <= 0x9FFF; ++c) {
        char bytes[U8_MAX_LENGTH];
        int32_t length = 0;
        U8_APPEND_UNSAFE(bytes, length, c);
        std::string lowered(bytes, length);
        unicode_lower(lowered);
        ASSERT_EQ(lowered, std::string(bytes, length)) << c;
    }
}

TEST_F(ICUTokenizerStreamTest, KeepsItsOutputUnderATurkishDefaultLocale) {
    const std::vector<std::string> values = {"I İ ı i",
                                             "ISTANBUL İstanbul istanbul ıstanbul",
                                             "TITLE Iİıi DİYARBAKIR",
                                             "ΣΑΣ ΟΔΟΣ",
                                             "ǅungla ǄUNGLA",
                                             "KELVIN Kelvin Ⱥ Ⱦ",
                                             "İ\u0307 I\u0307 J\u0301"};
    std::vector<std::vector<CollectedToken>> root;
    for (const std::string& value : values) {
        root.push_back(collect_checked(*icu_analyzer(true), value));
    }
    EXPECT_EQ(terms_of(root[0]), (std::vector<std::string> {"i", "i\u0307", "ı", "i"}));

    const icu::Locale saved = icu::Locale::getDefault();
    Defer restore {[&] {
        UErrorCode status = U_ZERO_ERROR;
        icu::Locale::setDefault(saved, status);
    }};
    UErrorCode status = U_ZERO_ERROR;
    icu::Locale::setDefault(icu::Locale("tr"), status);
    ASSERT_TRUE(U_SUCCESS(status));
    // The default took effect: ICU's default-locale lowercase of I is now the dotless ı.
    icu::UnicodeString probe = icu::UnicodeString::fromUTF8("I");
    ASSERT_EQ(probe.toLower(), icu::UnicodeString::fromUTF8("ı"));
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(collect_checked(*icu_analyzer(true), values[i]), root[i]) << values[i];
    }
}

TEST_F(ICUTokenizerStreamTest, AReusedStreamMakesWhatANewStreamMakes) {
    std::string long_value;
    while (long_value.size() < 12000) {
        long_value += "word Ärger 中文 カタカナ ไทย 😀 ";
    }
    const std::vector<std::string> values = {
            "Hello World",
            "中文分词测试",
            "ひらがなとカタカナ",
            "ภาษาไทยง่าย",
            "مرحبا بالعالم",
            "한국어 형태소",
            "\U0001F4A9 \U0001F469\u200D\u2764\uFE0F\u200D\U0001F469 #\uFE0F\u20E3 3\uFE0F\u20E3",
            "Mixed 中文 English ひらがな ไทย 123 😀 İstanbul",
            "bad \xFF\xFE bytes \xC3 end \xE4\xB8",
            "",
            long_value};
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        expect_text_stream_contract([lowercase] { return icu_tokenizer(lowercase); }, values);
        const TextAnalyzerPtr analyzer = icu_analyzer(lowercase);
        expect_text_stream_contract([&] { return analyzer->create_stream(); }, values);
    }
}

TEST_F(ICUTokenizerStreamTest, StrictModeThrowsWhereLenientModeReplaces) {
    for (bool lowercase : {true, false}) {
        SCOPED_TRACE(lowercase);
        // Lenient: \xFF becomes U+FFFD, which is no word.
        EXPECT_EQ(collect_terms(*icu_tokenizer(lowercase),
                                "ab\xFF"
                                "cd"),
                  (std::vector<std::string> {"ab", "cd"}));

        auto strict = icu_tokenizer(lowercase);
        strict->set_strict_utf8(true);
        try {
            strict->reset(
                    "ab\xFF"
                    "cd");
            FAIL() << "invalid UTF-8 did not throw";
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::INVALID_ARGUMENT);
        }
        // No token after a reset that threw, and the next value reads as usual.
        TokenBatch batch;
        EXPECT_FALSE(strict->next_batch(batch));
        EXPECT_EQ(collect_terms(*strict, "Ab 中文"),
                  (std::vector<std::string> {lowercase ? "ab" : "Ab", "中文"}));
    }
}

// U+1D400 MATHEMATICAL BOLD CAPITAL A is a letter of the Common script, so it stays in the Latin
// word before it, as two UTF-16 units.
TEST_F(ICUTokenizerStreamTest, CutsAWordAt255UnitsWithoutSplittingASurrogatePair) {
    const std::string bold_a = "\U0001D400";
    auto repeat = [](const std::string& unit, size_t count) {
        std::string out;
        for (size_t i = 0; i < count; ++i) {
            out += unit;
        }
        return out;
    };
    const auto custom = [](const std::string& text) {
        return collect_terms(*icu_tokenizer(false), text);
    };
    // 254 + 2 units: cut at 255 would split the pair, so the word is cut at 254.
    EXPECT_EQ(custom(repeat("a", 254) + bold_a), (std::vector<std::string> {repeat("a", 254)}));
    EXPECT_EQ(custom(repeat("a", 253) + bold_a),
              (std::vector<std::string> {repeat("a", 253) + bold_a}));
    EXPECT_EQ(custom(repeat("a", 255) + bold_a), (std::vector<std::string> {repeat("a", 255)}));
    EXPECT_EQ(custom(repeat(bold_a, 128)), (std::vector<std::string> {repeat(bold_a, 127)}));
    EXPECT_EQ(custom(repeat(bold_a, 127) + "a"),
              (std::vector<std::string> {repeat(bold_a, 127) + "a"}));
    // The rest of a cut word is dropped.
    EXPECT_EQ(custom("x " + repeat("a", 300) + " y"),
              (std::vector<std::string> {"x", repeat("a", 255), "y"}));
    EXPECT_EQ(custom(repeat("a", 254) + bold_a + " next"),
              (std::vector<std::string> {repeat("a", 254), "next"}));

    const auto lower = [](const std::string& text) {
        return collect_terms(*icu_tokenizer(true), text);
    };
    EXPECT_EQ(lower(repeat("A", 254) + bold_a + "B"),
              (std::vector<std::string> {repeat("a", 254)}));
    EXPECT_EQ(lower(repeat("A", 253) + bold_a + "B"),
              (std::vector<std::string> {repeat("a", 253) + bold_a}));
    // Deseret capital letters, lowercased to Deseret small letters, both supplementary.
    EXPECT_EQ(lower(repeat("\U00010400", 128)),
              (std::vector<std::string> {repeat("\U00010428", 127)}));
}

TEST_F(ICUTokenizerStreamTest, ReleasesItsCopyOfALongValue) {
    auto stream = icu_tokenizer(true);
    auto& tokenizer = dynamic_cast<ICUTokenizer&>(*stream);
    std::string long_value;
    while (long_value.size() < 2 * 1024 * 1024) {
        long_value += "word İstanbul 中文 ";
    }
    const auto member_bytes = [&] {
        return static_cast<size_t>(tokenizer.buffer_.getCapacity()) * sizeof(UChar) +
               tokenizer.utf8Str_.capacity();
    };
    collect_checked(tokenizer, long_value);
    EXPECT_EQ(tokenizer.reserved_bytes(), member_bytes());
    EXPECT_GT(tokenizer.reserved_bytes(), 1024 * 1024);
    collect_checked(tokenizer, "short İ");
    EXPECT_EQ(tokenizer.reserved_bytes(), member_bytes());
    EXPECT_LE(tokenizer.reserved_bytes(), ANALYZER_SCRATCH_HIGH_WATER_BYTES);
}

} // namespace
} // namespace doris::segment_v2::inverted_index
