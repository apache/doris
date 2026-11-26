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

#include "storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter_factory.h"

#include <gtest/gtest.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#include <memory>
#include <string>
#include <string_view>

namespace doris::segment_v2::inverted_index {

namespace {

std::string apply_all(TextCharFilter& filter, const std::string& text) {
    std::string scratch;
    return std::string(filter.apply(text, scratch));
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

} // namespace

class ICUNormalizerCharFilterFactoryTest : public ::testing::Test {};

TEST_F(ICUNormalizerCharFilterFactoryTest, DefaultNormalizationMatchesICU) {
    std::string input = "Café and coﬃee";
    Settings settings;

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string result = apply_all(*filter, input);
    std::string expected = normalize_with_nfkc_cf(input);

    EXPECT_EQ(result, expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, AlreadyNormalizedQuickCheck) {
    std::string input = "cafe";
    Settings settings;

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string result = apply_all(*filter, input);
    EXPECT_EQ(result, input);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, ComposeAndDecomposeModes) {
    std::string input = "Café";

    {
        Settings settings;
        settings.set("name", "nfc");
        settings.set("mode", "compose");

        ICUNormalizerCharFilterFactory factory;
        EXPECT_NO_THROW(factory.initialize(settings));

        auto filter = factory.create();
        std::string result = apply_all(*filter, input);
        EXPECT_FALSE(result.empty());
    }

    {
        Settings settings;
        settings.set("name", "nfc");
        settings.set("mode", "decompose");

        ICUNormalizerCharFilterFactory factory;
        EXPECT_NO_THROW(factory.initialize(settings));

        auto filter = factory.create();
        std::string result = apply_all(*filter, input);
        EXPECT_FALSE(result.empty());
    }
}

TEST_F(ICUNormalizerCharFilterFactoryTest, InvalidModeThrows) {
    Settings settings;
    settings.set("mode", "invalid_mode");

    ICUNormalizerCharFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, InvalidNameThrows) {
    Settings settings;
    settings.set("name", "unknown_normalizer");

    ICUNormalizerCharFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, EmptyUnicodeSetFilterUsesBaseNormalizer) {
    std::string input = "Café";

    Settings settings;
    settings.set("name", "nfkc_cf");
    settings.set("mode", "compose");
    settings.set("unicode_set_filter", "");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string result = apply_all(*filter, input);
    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(result, expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, InvalidUnicodeSetFilterThrows) {
    Settings settings;
    settings.set("unicode_set_filter", "[invalid");

    ICUNormalizerCharFilterFactory factory;
    EXPECT_THROW(factory.initialize(settings), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, EmptyUnicodeSetFallsBackToBase) {
    std::string input = "Café";

    Settings settings;
    settings.set("unicode_set_filter", "[]");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string result = apply_all(*filter, input);
    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(result, expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, NonEmptyUnicodeSetFilterCreatesFilteredNormalizer) {
    std::string input = "Café 123";

    Settings settings;
    settings.set("unicode_set_filter", "[A-Za-z]");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string result = apply_all(*filter, input);
    EXPECT_FALSE(result.empty());
}

TEST_F(ICUNormalizerCharFilterFactoryTest, CreateWithoutInitializeThrows) {
    ICUNormalizerCharFilterFactory factory;

    EXPECT_THROW(factory.create(), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, EmptyInput) {
    std::string input;

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();

    std::string scratch;
    const std::string_view result = filter->apply(input, scratch);
    EXPECT_TRUE(result.empty());
    EXPECT_TRUE(scratch.empty());
}

TEST_F(ICUNormalizerCharFilterFactoryTest, NullNormalizerInFilterThrows) {
    std::shared_ptr<const icu::Normalizer2> normalizer = nullptr;
    EXPECT_THROW(ICUNormalizerCharFilter {normalizer}, Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, ReusedFilterNormalizesEveryValue) {
    std::string input = "Café";

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();
    ASSERT_NE(dynamic_cast<ICUNormalizerCharFilter*>(filter.get()), nullptr);

    std::string first = apply_all(*filter, input);
    EXPECT_FALSE(first.empty());
    EXPECT_EQ(first, "café");

    EXPECT_EQ(apply_all(*filter, input), first);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, SparseEditInALongValue) {
    constexpr size_t unchanged_bytes = 4 * 1024 * 1024;
    std::string input(unchanged_bytes, 'a');
    input += "Ｌ";
    input.append(unchanged_bytes, 'a');

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();
    ASSERT_NE(dynamic_cast<ICUNormalizerCharFilter*>(filter.get()), nullptr);

    std::string expected(unchanged_bytes, 'a');
    expected += "l";
    expected.append(unchanged_bytes, 'a');
    EXPECT_EQ(apply_all(*filter, input), expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, DenseAlternatingEditsInALongValue) {
    // Every second rune is folded 3 -> 1 with one unchanged byte in between, so nothing
    // coalesces.
    constexpr int32_t pairs = 100000;
    std::string input;
    input.reserve(static_cast<size_t>(pairs) * 4);
    for (int32_t i = 0; i < pairs; ++i) {
        input += "aＡ";
    }

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto filter = factory.create();
    ASSERT_NE(dynamic_cast<ICUNormalizerCharFilter*>(filter.get()), nullptr);
    const std::string result = apply_all(*filter, input);
    ASSERT_EQ(result.size(), static_cast<size_t>(pairs) * 2);
    EXPECT_EQ(result, std::string(static_cast<size_t>(pairs) * 2, 'a'));
}

} // namespace doris::segment_v2::inverted_index
