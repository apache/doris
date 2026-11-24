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

#include "storage/index/inverted/analysis/tokenizer/utf8_validator.h"

#include <CLucene.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>

#include "CLucene/util/stringUtil.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {
namespace {

bool clucene_valid(const std::string& text) {
    return StringUtil::validate_utf8(text) == 0;
}

std::string bytes(std::initializer_list<int> values) {
    std::string text;
    for (int value : values) {
        text.push_back(static_cast<char>(value));
    }
    return text;
}

TEST(Utf8ValidatorTest, AgreesWithCLuceneOnEveryValueOfUpToThreeBytes) {
    EXPECT_TRUE(lenient_utf8_valid(""));
    std::string text(3, '\0');
    for (int a = 0; a < 256; ++a) {
        text.resize(1);
        text[0] = static_cast<char>(a);
        ASSERT_EQ(lenient_utf8_valid(text), clucene_valid(text)) << a;
        for (int b = 0; b < 256; ++b) {
            text.resize(2);
            text[1] = static_cast<char>(b);
            ASSERT_EQ(lenient_utf8_valid(text), clucene_valid(text)) << a << " " << b;
            text.resize(3);
            for (int c = 0; c < 256; ++c) {
                text[2] = static_cast<char>(c);
                ASSERT_EQ(lenient_utf8_valid(text), clucene_valid(text))
                        << a << " " << b << " " << c;
            }
        }
    }
}

TEST(Utf8ValidatorTest, AgreesWithCLuceneOnRandomBytes) {
    std::mt19937_64 random(20261104);
    // Mostly bytes that make or break sequences, so that long values are not all invalid.
    const auto next_byte = [&random]() -> char {
        switch (random() % 4) {
        case 0:
            return static_cast<char>(random() % 0x80);
        case 1:
            return static_cast<char>(0x80 + random() % 0x40);
        case 2:
            return static_cast<char>(0xC0 + random() % 0x38);
        default:
            return static_cast<char>(random());
        }
    };
    for (int round = 0; round < 200000; ++round) {
        std::string text(random() % 24, '\0');
        for (char& c : text) {
            c = next_byte();
        }
        ASSERT_EQ(lenient_utf8_valid(text), clucene_valid(text)) << round;
    }
}

TEST(Utf8ValidatorTest, AcceptsOverlongFormsAndRefusesSurrogatesAndBeyondTheLastPlane) {
    EXPECT_TRUE(lenient_utf8_valid(bytes({0xC0, 0x80})));
    EXPECT_TRUE(lenient_utf8_valid(bytes({0xC1, 0x81})));
    EXPECT_TRUE(lenient_utf8_valid(bytes({0xE0, 0x80, 0x80})));
    EXPECT_TRUE(lenient_utf8_valid(bytes({0xF0, 0x80, 0x80, 0x80})));
    EXPECT_FALSE(lenient_utf8_valid(bytes({0xED, 0xA0, 0x80})));       // U+D800
    EXPECT_FALSE(lenient_utf8_valid(bytes({0xF4, 0x90, 0x80, 0x80}))); // U+110000
    EXPECT_FALSE(lenient_utf8_valid(bytes({0xE4, 0xB8})));             // cut short
    EXPECT_FALSE(lenient_utf8_valid(bytes({0x80})));
    EXPECT_FALSE(lenient_utf8_valid(bytes({0xF8, 0x80, 0x80, 0x80, 0x80})));
    // Strict UTF-8 refuses the overlong forms, so the tokenizers ask the lenient rule after it.
    const std::string overlong = bytes({0xC1, 0x81});
    EXPECT_FALSE(doris::validate_utf8(overlong.data(), overlong.size()));
}

// Every sequence the rule accepts decodes to what CLucene decodes it to, overlong forms included.
TEST(Utf8ValidatorTest, DecodesEveryAcceptedSequenceAsCLucene) {
    size_t checked = 0;
    const auto check = [&checked](const std::string& sequence) {
        if (!lenient_utf8_valid(sequence)) {
            return;
        }
        const std::wstring expected = StringUtil::string_to_wstring(sequence);
        ASSERT_EQ(expected.size(), 1);
        size_t length = 0;
        const uint32_t code_point =
                decode_lenient_utf8(reinterpret_cast<const uint8_t*>(sequence.data()), &length);
        ASSERT_EQ(length, sequence.size());
        ASSERT_EQ(code_point, static_cast<uint32_t>(expected[0]));
        ++checked;
    };
    for (int lead = 0xC0; lead <= 0xDF; ++lead) {
        for (int b = 0x80; b <= 0xBF; ++b) {
            check(bytes({lead, b}));
        }
    }
    for (int lead = 0xE0; lead <= 0xEF; ++lead) {
        for (int b = 0x80; b <= 0xBF; ++b) {
            for (int c = 0x80; c <= 0xBF; ++c) {
                check(bytes({lead, b, c}));
            }
        }
    }
    for (int lead = 0xF0; lead <= 0xF4; ++lead) {
        for (int b = 0x80; b <= 0xBF; ++b) {
            for (int c = 0x80; c <= 0xBF; c += 7) {
                for (int d = 0x80; d <= 0xBF; d += 5) {
                    check(bytes({lead, b, c, d}));
                }
            }
        }
    }
    EXPECT_GT(checked, 60000);
}

} // namespace
} // namespace doris::segment_v2::inverted_index
