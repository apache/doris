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

#include <cstring>
#include <string>

#include "exprs/function/cast/cast_to_decimal.h"

namespace doris {

TEST(LosslessDecimalCastTest, StringMustBeExactlyRepresentable) {
    auto lossless = [](const char* value, UInt32 precision, UInt32 scale) {
        return CastToDecimal::is_lossless_decimal_string(value, std::strlen(value), precision,
                                                         scale);
    };

    EXPECT_TRUE(lossless("9223372036854775808", 38, 0));
    EXPECT_TRUE(lossless("100.000", 38, 0));
    EXPECT_TRUE(lossless("1.25e2", 38, 0));
    EXPECT_TRUE(lossless("0.0100", 5, 2));
    EXPECT_TRUE(lossless("999.99", 5, 2));

    EXPECT_FALSE(lossless("100.4", 38, 0));
    EXPECT_FALSE(lossless("1.001e2", 38, 0));
    EXPECT_FALSE(lossless("1000", 5, 2));
    EXPECT_FALSE(lossless("0.001", 5, 2));
    EXPECT_FALSE(lossless("100abc", 38, 0));
    EXPECT_FALSE(lossless("+.", 38, 0));
    EXPECT_FALSE(lossless("999999999999999999999999999999999999999", 38, 0));
    EXPECT_TRUE(lossless("0e9223372036854775808", 38, 0));
    EXPECT_FALSE(lossless("0e9223372036854775808x", 38, 0));
}

TEST(LosslessDecimalCastTest, ParseOnlyExactValues) {
    CastParameters params;
    Decimal128V3 value;

    EXPECT_TRUE(CastToDecimal::from_string_lossless(StringRef("100.000", 7), value, 38, 0, params));
    EXPECT_EQ(value.value, 100);

    EXPECT_TRUE(CastToDecimal::from_string_lossless(StringRef("1.25e2", 6), value, 38, 0, params));
    EXPECT_EQ(value.value, 125);

    EXPECT_FALSE(CastToDecimal::from_string_lossless(StringRef("100.4", 5), value, 38, 0, params));
    EXPECT_FALSE(CastToDecimal::from_string_lossless(StringRef("100abc", 6), value, 38, 0, params));
    EXPECT_TRUE(CastToDecimal::from_string_lossless(StringRef("0e9223372036854775808", 21), value,
                                                    38, 0, params));
    EXPECT_FALSE(CastToDecimal::from_string_lossless(StringRef("0e9223372036854775808x", 22), value,
                                                     38, 0, params));
}

TEST(LosslessDecimalCastTest, HandlesCancellingExponent) {
    std::string value = "0.";
    value.append(1024, '0');
    value.append("1e1025");

    EXPECT_TRUE(CastToDecimal::is_lossless_decimal_string(value.data(), value.size(), 38, 0));

    CastParameters params;
    Decimal128V3 decimal;
    EXPECT_TRUE(CastToDecimal::from_string_lossless(StringRef(value.data(), value.size()), decimal,
                                                    38, 0, params));
    EXPECT_EQ(decimal.value, 1);
}

} // namespace doris
