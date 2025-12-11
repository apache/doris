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

#include "olap/rowset/segment_v2/inverted_index/token_filter/icu_normalizer_filter_factory.h"

#include <gtest/gtest.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#include <memory>
#include <string>
#include <vector>

#include "CLucene.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/keyword/keyword_tokenizer_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/standard/standard_tokenizer_factory.h"

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

namespace {

TokenizerPtr create_tokenizer(const std::string& tokenizer_type, const std::string& text) {
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);

    TokenizerPtr tokenizer;
    Settings settings;

    if (tokenizer_type == "standard") {
        StandardTokenizerFactory factory;
        factory.initialize(settings);
        tokenizer = factory.create();
    } else if (tokenizer_type == "keyword") {
        KeywordTokenizerFactory factory;
        factory.initialize(settings);
        tokenizer = factory.create();
    } else {
        throw std::invalid_argument("Unknown tokenizer type: " + tokenizer_type);
    }

    tokenizer->set_reader(reader);
    tokenizer->reset();
    return tokenizer;
}

std::vector<std::string> collect_tokens(TokenFilterPtr filter) {
    std::vector<std::string> tokens;
    Token t;
    while (filter->next(&t) != nullptr) {
        tokens.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }
    return tokens;
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

    auto tokenizer = create_tokenizer("keyword", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
    ASSERT_EQ(tokens.size(), 1);

    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(tokens[0], expected);
}

TEST_F(ICUNormalizerFilterFactoryTest, DefaultNormalizationStandardTokenizer) {
    std::string input = "Cafe\u0301 resume\u0301";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto tokenizer = create_tokenizer("standard", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
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

    auto tokenizer = create_tokenizer("keyword", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
    ASSERT_EQ(tokens.size(), 1);
    EXPECT_EQ(tokens[0], input);
}

TEST_F(ICUNormalizerFilterFactoryTest, EmptyInputProducesNoTokens) {
    std::string input;

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto tokenizer = create_tokenizer("keyword", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
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

    auto tokenizer = create_tokenizer("keyword", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
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

    auto tokenizer = create_tokenizer("keyword", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
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

    auto tokenizer = create_tokenizer("standard", input);
    auto filter = factory.create(tokenizer);

    auto tokens = collect_tokens(filter);
    EXPECT_GE(tokens.size(), 1u);
}

TEST_F(ICUNormalizerFilterFactoryTest, CreateWithoutInitializeThrows) {
    ICUNormalizerFilterFactory factory;

    auto tokenizer = create_tokenizer("keyword", "test");
    EXPECT_THROW(factory.create(tokenizer), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, NullNormalizerInFilterThrows) {
    auto tokenizer = create_tokenizer("keyword", "test");
    std::shared_ptr<const icu::Normalizer2> normalizer = nullptr;

    EXPECT_THROW(ICUNormalizerFilter(tokenizer, normalizer), Exception);
}

TEST_F(ICUNormalizerFilterFactoryTest, ResetResetsUnderlyingStream) {
    std::string input = "Cafe\u0301 resume\u0301";

    Settings settings;
    ICUNormalizerFilterFactory factory;
    factory.initialize(settings);

    auto tokenizer = create_tokenizer("standard", input);
    auto filter = factory.create(tokenizer);

    Token t;
    std::vector<std::string> first_pass;
    while (filter->next(&t) != nullptr) {
        first_pass.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }

    auto reader2 = std::make_shared<lucene::util::SStringReader<char>>();
    reader2->init(input.data(), static_cast<int32_t>(input.size()), false);
    tokenizer->set_reader(reader2);
    filter->reset();

    std::vector<std::string> second_pass;
    while (filter->next(&t) != nullptr) {
        second_pass.emplace_back(t.termBuffer<char>(), t.termLength<char>());
    }

    EXPECT_EQ(first_pass, second_pass);
}

} // namespace doris::segment_v2::inverted_index