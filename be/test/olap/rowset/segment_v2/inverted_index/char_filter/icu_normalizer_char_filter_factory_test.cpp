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

#include "olap/rowset/segment_v2/inverted_index/char_filter/icu_normalizer_char_filter_factory.h"

#include <gtest/gtest.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>

#include <memory>
#include <string>

using namespace lucene::analysis;

namespace doris::segment_v2::inverted_index {

namespace {

ReaderPtr make_reader(const std::string& text) {
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(text.data(), static_cast<int32_t>(text.size()), false);
    return reader;
}

std::string read_all(const ReaderPtr& reader) {
    const void* data = nullptr;
    int32_t len = reader->read(&data, 0, static_cast<int32_t>(reader->size()));
    if (len <= 0 || data == nullptr) {
        return {};
    }
    return std::string(static_cast<const char*>(data), len);
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
    std::string input = "Cafe\u0301 and co\uFB03ee";
    Settings settings;

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);
    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    std::string result = read_all(filter);
    std::string expected = normalize_with_nfkc_cf(input);

    EXPECT_EQ(result, expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, AlreadyNormalizedQuickCheck) {
    std::string input = "cafe";
    Settings settings;

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);
    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    std::string result = read_all(filter);
    EXPECT_EQ(result, input);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, ComposeAndDecomposeModes) {
    std::string input = "Cafe\u0301";

    {
        Settings settings;
        settings.set("name", "nfc");
        settings.set("mode", "compose");

        ICUNormalizerCharFilterFactory factory;
        EXPECT_NO_THROW(factory.initialize(settings));

        auto reader = make_reader(input);
        auto filter = factory.create(reader);
        filter->init(input.data(), static_cast<int32_t>(input.size()), false);
        std::string result = read_all(filter);
        EXPECT_FALSE(result.empty());
    }

    {
        Settings settings;
        settings.set("name", "nfc");
        settings.set("mode", "decompose");

        ICUNormalizerCharFilterFactory factory;
        EXPECT_NO_THROW(factory.initialize(settings));

        auto reader = make_reader(input);
        auto filter = factory.create(reader);
        filter->init(input.data(), static_cast<int32_t>(input.size()), false);
        std::string result = read_all(filter);
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
    std::string input = "Cafe\u0301";

    Settings settings;
    settings.set("name", "nfkc_cf");
    settings.set("mode", "compose");
    settings.set("unicode_set_filter", "");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);
    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    std::string result = read_all(filter);
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
    std::string input = "Cafe\u0301";

    Settings settings;
    settings.set("unicode_set_filter", "[]");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);
    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    std::string result = read_all(filter);
    std::string expected = normalize_with_nfkc_cf(input);
    EXPECT_EQ(result, expected);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, NonEmptyUnicodeSetFilterCreatesFilteredNormalizer) {
    std::string input = "Cafe\u0301 123";

    Settings settings;
    settings.set("unicode_set_filter", "[A-Za-z]");

    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);
    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    std::string result = read_all(filter);
    EXPECT_FALSE(result.empty());
}

TEST_F(ICUNormalizerCharFilterFactoryTest, CreateWithoutInitializeThrows) {
    ICUNormalizerCharFilterFactory factory;

    auto reader = make_reader("test");
    EXPECT_THROW(factory.create(reader), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, EmptyInput) {
    std::string input;

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = factory.create(reader);

    filter->init(input.data(), static_cast<int32_t>(input.size()), false);

    const void* data = nullptr;
    int32_t len = filter->read(&data, 0, static_cast<int32_t>(filter->size()));
    EXPECT_EQ(len, -1);
    EXPECT_TRUE(data == nullptr || filter->size() == 0);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, NullNormalizerInFilterThrows) {
    auto reader = make_reader("test");
    std::shared_ptr<const icu::Normalizer2> normalizer = nullptr;
    EXPECT_THROW(ICUNormalizerCharFilter(reader, normalizer), Exception);
}

TEST_F(ICUNormalizerCharFilterFactoryTest, InitializeOnlyFillsOnce) {
    std::string input = "Cafe\u0301";

    Settings settings;
    ICUNormalizerCharFilterFactory factory;
    factory.initialize(settings);

    auto reader = make_reader(input);
    auto filter = std::dynamic_pointer_cast<ICUNormalizerCharFilter>(factory.create(reader));
    ASSERT_NE(filter, nullptr);

    filter->initialize();
    std::string first = read_all(filter);
    EXPECT_FALSE(first.empty());
    EXPECT_EQ(first, "café");

    filter->initialize();
}

} // namespace doris::segment_v2::inverted_index