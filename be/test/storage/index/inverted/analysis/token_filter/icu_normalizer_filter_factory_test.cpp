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

#include "storage/index/inverted/analysis/token_filter/icu_normalizer_filter_factory.h"

#include <gtest/gtest.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#ifdef ADDRESS_SANITIZER
#include <sanitizer/allocator_interface.h>
#endif

#include <memory>
#include <string>
#include <vector>

#include "storage/index/inverted/analysis/text_stream_contract.h"
#include "storage/index/inverted/analysis/tokenizer/empty/empty_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

namespace {

std::unique_ptr<TextTokenizer> create_tokenizer(const std::string& tokenizer_type) {
    std::unique_ptr<TextTokenizer> tokenizer;
    Settings settings;

    if (tokenizer_type == "standard") {
        StandardTokenizerFactory factory;
        factory.initialize(settings);
        tokenizer = factory.create();
    } else if (tokenizer_type == "keyword") {
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
        tokenizer = factory.create();
    } else if (tokenizer_type == "empty") {
        EmptyTokenizerFactory factory;
        factory.initialize(settings);
        tokenizer = factory.create();
    } else {
        throw std::invalid_argument("Unknown tokenizer type: " + tokenizer_type);
    }
    return tokenizer;
}

std::string normalize_with_nfkc_cf(const std::string& text) {
    UErrorCode status = U_ZERO_ERROR;
    const icu::Normalizer2* normalizer =
            icu::Normalizer2::getInstance(nullptr, "nfkc_cf", UNORM2_COMPOSE, status);
    if (U_FAILURE(status) || normalizer == nullptr) {
        return text;
    }

    icu::UnicodeString src = icu::UnicodeString::fromUTF8(text);
    icu::UnicodeString dst;
    status = U_ZERO_ERROR;
    normalizer->normalize(src, dst, status);
    if (U_FAILURE(status)) {
        return text;
    }

    std::string result;
    dst.toUTF8String(result);
    return result;
}

std::vector<std::string> split_on_space(const std::string& text) {
    std::vector<std::string> parts;
    std::string current;
    for (char c : text) {
        if (c == ' ') {
            if (!current.empty()) {
                parts.push_back(current);
                current.clear();
            }
        } else {
            current.push_back(c);
        }
    }
    if (!current.empty()) {
        parts.push_back(current);
    }
    return parts;
}

} // namespace

class ICUNormalizerFilterFactoryTest : public ::testing::Test {};

TEST_F(ICUNormalizerFilterFactoryTest, DefaultNormalizationKeywordTokenizer) {
    std::string input = "Cafe\u0301 and co\uFB03ee";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("keyword"));

    auto tokens = collect_terms(*filter, input);
    ASSERT_EQ(tokens.size(), 1);

    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(tokens[0], expected);
}

TEST_F(ICUNormalizerFilterFactoryTest, DefaultNormalizationStandardTokenizer) {
    std::string input = "Cafe\u0301 resume\u0301";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("standard"));

    auto tokens = collect_terms(*filter, input);
    ASSERT_EQ(tokens.size(), 2);

    std::string normalized = normalize_with_nfkc_cf(input);
    auto expected_tokens = split_on_space(normalized);
    ASSERT_EQ(expected_tokens.size(), 2);

    EXPECT_EQ(tokens[0], expected_tokens[0]);
    EXPECT_EQ(tokens[1], expected_tokens[1]);
}

TEST_F(ICUNormalizerFilterFactoryTest, QuickCheckReturnsOriginal) {
    std::string input = "abc123";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("keyword"));

    auto tokens = collect_terms(*filter, input);
    ASSERT_EQ(tokens.size(), 1);
    EXPECT_EQ(tokens[0], input);
}

TEST_F(ICUNormalizerFilterFactoryTest, EmptyInputProducesNoTokens) {
    std::string input;

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("keyword"));

    auto tokens = collect_terms(*filter, input);
    EXPECT_TRUE(tokens.empty());
}

TEST_F(ICUNormalizerFilterFactoryTest, InvalidNameThrows) {
    Settings settings;
    settings.set("name", "unknown_normalizer");

    ICUNormalizerFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, EmptyUnicodeSetFilterUsesBaseNormalizer) {
    std::string input = "Cafe\u0301";

    Settings settings;
    settings.set("unicode_set_filter", "");

    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("keyword"));

    auto tokens = collect_terms(*filter, input);
    ASSERT_EQ(tokens.size(), 1);

    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(tokens[0], expected);
}

TEST_F(ICUNormalizerFilterFactoryTest, InvalidUnicodeSetFilterThrows) {
    Settings settings;
    settings.set("unicode_set_filter", "[invalid");

    ICUNormalizerFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, EmptyUnicodeSetFallsBackToBase) {
    std::string input = "Cafe\u0301";

    Settings settings;
    settings.set("unicode_set_filter", "[]");

    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("keyword"));

    auto tokens = collect_terms(*filter, input);
    ASSERT_EQ(tokens.size(), 1);

    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(tokens[0], expected);
}

TEST_F(ICUNormalizerFilterFactoryTest, NonEmptyUnicodeSetCreatesFilteredNormalizer) {
    std::string input = "Cafe\u0301 123";

    Settings settings;
    settings.set("unicode_set_filter", "[A-Za-z]");

    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("standard"));

    auto tokens = collect_terms(*filter, input);
    EXPECT_GE(tokens.size(), 1U);
}

TEST_F(ICUNormalizerFilterFactoryTest, CreateWithoutInitializeThrows) {
    ICUNormalizerFilterFactory factory;

    EXPECT_THROW(factory.create(create_tokenizer("keyword")), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, NullNormalizerInFilterThrows) {
    std::shared_ptr<const icu::Normalizer2> normalizer = nullptr;

    EXPECT_THROW(ICUNormalizerFilter(create_tokenizer("keyword"), normalizer), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, ResetResetsUnderlyingStream) {
    std::string input = "Cafe\u0301 resume\u0301";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("standard"));

    std::vector<std::string> first_pass = collect_terms(*filter, input);

    std::vector<std::string> second_pass = collect_terms(*filter, input);

    EXPECT_EQ(first_pass, second_pass);
}

TEST_F(ICUNormalizerFilterFactoryTest, LargeInputStaysCompact) {
    constexpr size_t input_size = 4 * 1024 * 1024;
    std::string input(input_size, 'a');
    input.back() = 'A';

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("empty"));
    filter->reset(input);

    TokenBatch batch;
#ifdef ADDRESS_SANITIZER
    const size_t allocated_before = __sanitizer_get_current_allocated_bytes();
#endif
    ASSERT_TRUE(filter->next_batch(batch));
#ifdef ADDRESS_SANITIZER
    const size_t allocated_after = __sanitizer_get_current_allocated_bytes();
    ASSERT_GE(allocated_after, allocated_before);
    EXPECT_LT(allocated_after - allocated_before, input_size * 4);
#endif
}

TEST_F(ICUNormalizerFilterFactoryTest, LargeInputStaysCompactInStrictMode) {
    constexpr size_t input_size = 4 * 1024 * 1024;
    std::string input(input_size, 'a');
    input.back() = 'A';

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create(create_tokenizer("empty"));
    filter->set_strict_utf8(true);
    filter->reset(input);

    TokenBatch batch;
#ifdef ADDRESS_SANITIZER
    const size_t allocated_before = __sanitizer_get_current_allocated_bytes();
#endif
    ASSERT_TRUE(filter->next_batch(batch));
#ifdef ADDRESS_SANITIZER
    const size_t allocated_after = __sanitizer_get_current_allocated_bytes();
    ASSERT_GE(allocated_after, allocated_before);
    EXPECT_LT(allocated_after - allocated_before, input_size * 12);
#endif
}

} // namespace doris::segment_v2::inverted_index
