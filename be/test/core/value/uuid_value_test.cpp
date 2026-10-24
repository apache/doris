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

#include "core/value/uuid_value.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <vector>

namespace doris {

TEST(UUIDValueTest, ParseAndFormat) {
    UUIDValueType value;
    ASSERT_TRUE(UUIDValue::from_string(value, "550e8400-e29b-41d4-a716-446655440000"));
    EXPECT_EQ(UUIDValue::to_string(value), "550e8400-e29b-41d4-a716-446655440000");
    EXPECT_EQ(UUIDValue::version(value), 4);

    UUIDValueType uppercase;
    ASSERT_TRUE(UUIDValue::from_string(uppercase, "550E8400-E29B-41D4-A716-446655440000"));
    EXPECT_EQ(uppercase, value);

    UUIDValueType compact;
    ASSERT_TRUE(UUIDValue::from_string(compact, "550e8400e29b41d4a716446655440000"));
    EXPECT_EQ(compact, value);

    UUIDValueType zero = 1;
    ASSERT_TRUE(UUIDValue::from_string(zero, "00000000-0000-0000-0000-000000000000"));
    EXPECT_EQ(zero, 0);
    EXPECT_EQ(UUIDValue::to_string(zero), "00000000-0000-0000-0000-000000000000");
}

TEST(UUIDValueTest, RejectInvalidText) {
    const std::vector<std::string> invalid = {"",
                                              "550e8400-e29b-41d4-a716-44665544000",
                                              "550e8400-e29b-41d4-a716-4466554400000",
                                              "550e8400e29b-41d4-a716-446655440000",
                                              "550e8400-e29b-41d4-a7164466-55440000",
                                              "550e8400-e29b-41d4-a716-44665544000g",
                                              "{550e8400-e29b-41d4-a716-446655440000}"};
    for (const auto& text : invalid) {
        UUIDValueType value = 0;
        EXPECT_FALSE(UUIDValue::from_string(value, text)) << text;
    }
}

TEST(UUIDValueTest, NumericOrderingMatchesCanonicalOrdering) {
    UUIDValueType smaller;
    UUIDValueType larger;
    ASSERT_TRUE(UUIDValue::from_string(smaller, "00000000-0000-0000-ffff-ffffffffffff"));
    ASSERT_TRUE(UUIDValue::from_string(larger, "00000000-0000-0001-0000-000000000000"));
    EXPECT_LT(smaller, larger);
}

TEST(UUIDValueTest, CanonicalByteOrder) {
    const std::array<uint8_t, UUIDValue::BINARY_LENGTH> bytes = {0x80, 0x11, 0x22, 0x33, 0x44, 0x55,
                                                                 0x76, 0x77, 0x88, 0x99, 0xaa, 0xbb,
                                                                 0xcc, 0xdd, 0xee, 0xff};
    const auto expected = UUIDValue::from_parts(0x8011223344557677ULL, 0x8899aabbccddeeffULL);
    const std::string text = "80112233-4455-7677-8899-aabbccddeeff";
    EXPECT_EQ(UUIDValue::from_big_endian(bytes.data()), expected);
    EXPECT_EQ(UUIDValue::to_big_endian(expected), bytes);
    EXPECT_EQ(UUIDValue::to_string(expected), text);
    EXPECT_EQ(UUIDValue::version(expected), 7);
    UUIDValueType parsed;
    ASSERT_TRUE(UUIDValue::from_string(parsed, text));
    EXPECT_EQ(parsed, expected);

    alignas(16) std::array<uint8_t, UUIDValue::BINARY_LENGTH + 1> unaligned {};
    std::ranges::copy(bytes, unaligned.begin() + 1);
    EXPECT_EQ(UUIDValue::from_big_endian(unaligned.data() + 1), expected);
}

TEST(UUIDValueTest, FormatBoundaries) {
    const std::array<std::pair<UUIDValueType, std::string>, 5> cases = {{
            {0, "00000000-0000-0000-0000-000000000000"},
            {1, "00000000-0000-0000-0000-000000000001"},
            {UUIDValue::from_parts(1, 0), "00000000-0000-0001-0000-000000000000"},
            {UUIDValue::from_parts(0x8000000000000000ULL, 0),
             "80000000-0000-0000-0000-000000000000"},
            {~UUIDValueType {0}, "ffffffff-ffff-ffff-ffff-ffffffffffff"},
    }};
    for (const auto& [value, expected] : cases) {
        alignas(16) std::array<char, UUIDValue::TEXT_LENGTH + 2> buffer;
        buffer.fill('#');
        UUIDValue::to_string(value, buffer.data() + 1);
        EXPECT_EQ(std::string(buffer.data() + 1, UUIDValue::TEXT_LENGTH), expected);
        EXPECT_EQ(buffer.front(), '#');
        EXPECT_EQ(buffer.back(), '#');
        EXPECT_EQ(UUIDValue::to_string(value), expected);
        EXPECT_EQ(UUIDValue(value).to_string(), expected);
    }
}

TEST(UUIDValueTest, EveryByteMatchesBoost) {
    boost::uuids::uuid reference {};
    for (auto& position : reference.data) {
        for (unsigned byte = 0; byte < 256; ++byte) {
            position = static_cast<uint8_t>(byte);
            const auto expected = boost::uuids::to_string(reference);
            const auto value = UUIDValue::from_big_endian(reference.data);
            EXPECT_EQ(UUIDValue::to_string(value), expected);
            const auto bytes = UUIDValue::to_big_endian(value);
            EXPECT_TRUE(std::equal(bytes.begin(), bytes.end(), reference.begin()));
            UUIDValueType parsed;
            ASSERT_TRUE(UUIDValue::from_string(parsed, expected));
            EXPECT_EQ(parsed, value);
        }
    }
}

} // namespace doris
