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

#include "core/value/variant/variant_decimal_paths.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace doris {
namespace {

VariantDecimalPathSet paths_of(std::vector<VariantDecimalPath> paths) {
    return VariantDecimalPathSet(std::move(paths));
}

// gtest cannot print an __int128; compare through a decimal rendering.
std::string digits_of(__int128 value) {
    if (value == 0) {
        return "0";
    }
    const bool negative = value < 0;
    unsigned __int128 magnitude = negative ? -static_cast<unsigned __int128>(value)
                                           : static_cast<unsigned __int128>(value);
    std::string out;
    while (magnitude != 0) {
        out.insert(out.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10)));
        magnitude /= 10;
    }
    return negative ? "-" + out : out;
}

void expect_decimal(std::string_view token, const char* unscaled, int scale) {
    VariantDecimalText out;
    ASSERT_EQ(JsonNumberToDecimal::OK, parse_json_number_as_decimal(token, &out)) << token;
    EXPECT_EQ(unscaled, digits_of(out.unscaled)) << token;
    EXPECT_EQ(scale, out.scale) << token;
}

} // namespace

TEST(VariantDecimalPathsTest, SetMatchesExactAndGlobPaths) {
    const VariantDecimalPathSet paths = paths_of(
            {{.pattern = "money.amount", .is_glob = false, .precision = 20, .scale = 6},
             {.pattern = "ratio*", .is_glob = true, .precision = 30, .scale = 18}});
    EXPECT_FALSE(paths.empty());
    EXPECT_TRUE(paths.matches("money.amount"));
    EXPECT_TRUE(paths.matches("ratio_a"));
    EXPECT_TRUE(paths.matches("ratio"));
    EXPECT_FALSE(paths.matches("money"));
    EXPECT_FALSE(paths.matches("money.amount.cents"));
    EXPECT_FALSE(paths.matches("amount"));
    EXPECT_FALSE(paths.matches(""));
    EXPECT_TRUE(paths_of({}).empty());
    EXPECT_FALSE(paths_of({}).matches("money.amount"));
}

TEST(VariantDecimalPathsTest, ReadsFractionsExactly) {
    expect_decimal("123456789012.345678", "123456789012345678", 6);
    expect_decimal("-1.5", "-15", 1);
    expect_decimal("0.123456789012345678901", "123456789012345678901", 21);
    expect_decimal("1.50", "150", 2);
    expect_decimal("0.0", "0", 1);
    expect_decimal("-0", "0", 0);
    expect_decimal("188118222.011121933", "188118222011121933", 9);
}

TEST(VariantDecimalPathsTest, FoldsAnExponentIntoTheScale) {
    expect_decimal("1.5e3", "1500", 0);
    expect_decimal("1.5E-3", "15", 4);
    expect_decimal("1e2", "100", 0);
    expect_decimal("-2.5e+1", "-25", 0);
    expect_decimal("12e-2", "12", 2);
    expect_decimal("0e100", "0", 0);
}

TEST(VariantDecimalPathsTest, ReadsIntegersUpToThirtyEightDigits) {
    expect_decimal("123456789012345678901", "123456789012345678901", 0);
    expect_decimal("18446744073709551616", "18446744073709551616", 0);
    expect_decimal("99999999999999999999999999999999999999", "99999999999999999999999999999999999999",
                   0);
    expect_decimal("-99999999999999999999999999999999999999",
                   "-99999999999999999999999999999999999999", 0);
}

TEST(VariantDecimalPathsTest, DeclinesWhatDoesNotFit) {
    VariantDecimalText out;
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("100000000000000000000000000000000000000", &out));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("0.1234567890123456789012345678901234567890123", &out));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE, parse_json_number_as_decimal("1e-40", &out));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE, parse_json_number_as_decimal("1e40", &out));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE, parse_json_number_as_decimal("1e99999", &out));
}

TEST(VariantDecimalPathsTest, DeclinesMalformedTokens) {
    VariantDecimalText out;
    for (const char* token : {"", "-", "1.", ".5", "1e", "1e+", "abc", "1.5x", "--1", "1..5", " 1"}) {
        EXPECT_EQ(JsonNumberToDecimal::MALFORMED, parse_json_number_as_decimal(token, &out)) << token;
    }
    EXPECT_EQ(JsonNumberToDecimal::MALFORMED, parse_json_number_as_decimal("1.5", nullptr));
}

} // namespace doris
