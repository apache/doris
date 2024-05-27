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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <functional>
#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "runtime/define_primitive_type.h"
#include "runtime/raw_value.h"
#include "runtime/type_limit.h"
#include "util/string_parser.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
namespace doris::vectorized {

TEST(DecimalTest, Decimal256) {
    // 9999999999999999999999999999999999999999999999999999999999999999999999999999
    Decimal256 dec1(type_limit<vectorized::Decimal256>::max());
    auto des_str = dec1.to_string(10);
    EXPECT_EQ(des_str,
              "999999999999999999999999999999999999999999999999999999999999999999.9999999999");
    des_str = dec1.to_string(0);
    EXPECT_EQ(des_str,
              "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    des_str = dec1.to_string(76);
    EXPECT_EQ(des_str,
              "0.9999999999999999999999999999999999999999999999999999999999999999999999999999");

    auto dec2 = type_limit<vectorized::Decimal256>::min();
    des_str = dec2.to_string(10);
    EXPECT_EQ(des_str,
              "-999999999999999999999999999999999999999999999999999999999999999999.9999999999");
    des_str = dec2.to_string(0);
    EXPECT_EQ(des_str,
              "-9999999999999999999999999999999999999999999999999999999999999999999999999999");
    des_str = dec2.to_string(76);
    EXPECT_EQ(des_str,
              "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999");

    // plus
    Decimal256 dec3 = dec1 + dec2;
    des_str = dec3.to_string(10);
    EXPECT_EQ(des_str, "0.0000000000");
    des_str = dec3.to_string(0);
    EXPECT_EQ(des_str, "0");
    des_str = dec3.to_string(76);
    EXPECT_EQ(des_str,
              "0.0000000000000000000000000000000000000000000000000000000000000000000000000000");

    // minus
    dec2 = type_limit<vectorized::Decimal256>::max();
    dec3 = dec1 - dec2;
    des_str = dec3.to_string(10);
    EXPECT_EQ(des_str, "0.0000000000");

    // multiply

    // divide
    dec1 = type_limit<vectorized::Decimal256>::max();
    dec2 = vectorized::Decimal256(10);
    dec3 = dec1 / dec2;
    des_str = dec3.to_string(1);
    EXPECT_EQ(des_str,
              "99999999999999999999999999999999999999999999999999999999999999999999999999.9");

    // overflow
}

TEST(DecimalTest, compare) {
    Decimal256 dec_max(type_limit<vectorized::Decimal256>::max());
    Decimal256 dec_min(type_limit<vectorized::Decimal256>::min());

    Decimal256 dec3 = vectorized::Decimal256(10);
    Decimal256 dec4 = vectorized::Decimal256(9);
    Decimal256 dec5 = vectorized::Decimal256(-10);

    Decimal256 dec_max2(type_limit<vectorized::Decimal256>::max());
    Decimal256 dec_min2(type_limit<vectorized::Decimal256>::min());

    Decimal256 dec3_2 = vectorized::Decimal256(10);
    Decimal256 dec4_2 = vectorized::Decimal256(9);
    Decimal256 dec5_2 = vectorized::Decimal256(-10);

    EXPECT_EQ(dec_max, dec_max2);
    EXPECT_EQ(dec_min, dec_min2);
    EXPECT_EQ(dec3, dec3_2);
    EXPECT_EQ(dec4, dec4_2);
    EXPECT_EQ(dec5, dec5_2);

    EXPECT_NE(dec_max, dec_min);
    EXPECT_NE(dec_max, dec3);
    EXPECT_NE(dec_max, dec5);
    EXPECT_NE(dec3, dec4);
    EXPECT_NE(dec3, dec5);

    EXPECT_GT(dec_max, dec_min);
    EXPECT_GT(dec_max, dec3);
    EXPECT_GT(dec_max, dec4);
    EXPECT_GT(dec_max, dec5);
    EXPECT_GT(dec3, dec4);
    EXPECT_GT(dec3, dec5);

    EXPECT_GE(dec_max, dec_max2);
    EXPECT_GE(dec_max, dec_min);
    EXPECT_GE(dec_max, dec3);
    EXPECT_GE(dec_max, dec4);
    EXPECT_GE(dec_max, dec5);
    EXPECT_GE(dec3, dec4);
    EXPECT_GE(dec3, dec3_2);
    EXPECT_GE(dec3, dec5);
    EXPECT_GE(dec5, dec5_2);

    EXPECT_LT(dec_min, dec_max);
    EXPECT_LT(dec_min, dec3);
    EXPECT_LT(dec_min, dec4);
    EXPECT_LT(dec_min, dec5);
    EXPECT_LT(dec4, dec3);
    EXPECT_LT(dec5, dec3);
    EXPECT_LT(dec5, dec4);

    EXPECT_LE(dec_min, dec_min);
    EXPECT_LE(dec_min, dec_max);
    EXPECT_LE(dec_min, dec3);
    EXPECT_LE(dec_min, dec4);
    EXPECT_LE(dec_min, dec5);
    EXPECT_LE(dec4, dec3);
    EXPECT_LE(dec5, dec3);
    EXPECT_LE(dec5, dec4);
}

TEST(DecimalTest, string_parser) {
    Decimal256 dec_max(type_limit<vectorized::Decimal256>::max());
    std::string dec_str(
            "999999999999999999999999999999999999999999999999999999999999999999.9999999999");

    StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
    wide::Int256 value = StringParser::string_to_decimal<TYPE_DECIMAL256>(
            dec_str.data(), dec_str.size(), 76, 10, &result);
    EXPECT_EQ(result, StringParser::PARSE_SUCCESS);
    EXPECT_EQ(value, dec_max.value);
}
TEST(DecimalTest, crc32) {
    PrimitiveType type = PrimitiveType::TYPE_DECIMAL256;
    DataTypeDecimal<vectorized::Decimal256> data_type(76, 10);
    auto col = data_type.create_column();
    Decimal256 dec_max(type_limit<vectorized::Decimal256>::max());
    Decimal256 dec_min(type_limit<vectorized::Decimal256>::min());
    Decimal256 dec3 = vectorized::Decimal256(1);
    Decimal256 dec4 = vectorized::Decimal256(-1);
    auto& decimal_data =
            ((vectorized::ColumnDecimal<vectorized::Decimal256>*)col.get())->get_data();
    decimal_data.push_back(dec_max);
    decimal_data.push_back(dec_min);
    decimal_data.push_back(dec3);
    decimal_data.push_back(dec4);

    auto column_value = col->get_data_at(0);
    uint32_t hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value.data, column_value.size, type, hash_val);
    EXPECT_EQ(hash_val, 1277249500);

    column_value = col->get_data_at(1);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value.data, column_value.size, type, hash_val);
    EXPECT_EQ(hash_val, 1537064144);

    column_value = col->get_data_at(2);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value.data, column_value.size, type, hash_val);
    EXPECT_EQ(hash_val, 3905966087);

    column_value = col->get_data_at(3);
    hash_val = 0;
    hash_val = RawValue::zlib_crc32(column_value.data, column_value.size, type, hash_val);
    EXPECT_EQ(hash_val, 4285311755);
}
TEST(DecimalTest, hash) {
    Decimal256 dec_max(type_limit<vectorized::Decimal256>::max());
    Decimal256 dec_min(type_limit<vectorized::Decimal256>::min());
    Decimal256 dec3 = vectorized::Decimal256(12345);
    Decimal256 dec4 = vectorized::Decimal256(-12345);

    {
        auto hash_op = std::hash<vectorized::Decimal256>();
        auto hash_val = hash_op(dec_max);
        EXPECT_EQ(hash_val, 11093810651088735436ULL);
        hash_val = hash_op(dec_min);
        EXPECT_EQ(hash_val, 11093810651088735437ULL);
        hash_val = hash_op(dec3);
        EXPECT_EQ(hash_val, 12345);
        hash_val = hash_op(dec4);
        EXPECT_EQ(hash_val, 12344);
    }
}

TEST(DecimalTest, to_string) {
    {
        Decimal32 dec(999999999);
        auto dec_str = dec.to_string(9, 0);
        EXPECT_EQ(dec_str, "999999999");
        dec_str = dec.to_string(9, 6);
        EXPECT_EQ(dec_str, "999.999999");
        dec_str = dec.to_string(9, 9);
        EXPECT_EQ(dec_str, "0.999999999");

        dec_str = dec.to_string(8, 0);
        EXPECT_EQ(dec_str, "99999999");
        dec_str = dec.to_string(8, 6);
        EXPECT_EQ(dec_str, "99.999999");
        dec_str = dec.to_string(8, 8);
        EXPECT_EQ(dec_str, "0.99999999");

        dec_str = dec.to_string(10, 0);
        EXPECT_EQ(dec_str, "999999999");
        dec_str = dec.to_string(10, 6);
        EXPECT_EQ(dec_str, "999.999999");
        dec_str = dec.to_string(10, 9);
        EXPECT_EQ(dec_str, "0.999999999");
    }
    {
        Decimal32 dec(-999999999);
        auto dec_str = dec.to_string(9, 0);
        EXPECT_EQ(dec_str, "-999999999");
        dec_str = dec.to_string(9, 6);
        EXPECT_EQ(dec_str, "-999.999999");
        dec_str = dec.to_string(9, 9);
        EXPECT_EQ(dec_str, "-0.999999999");

        dec_str = dec.to_string(8, 0);
        EXPECT_EQ(dec_str, "-99999999");
        dec_str = dec.to_string(8, 6);
        EXPECT_EQ(dec_str, "-99.999999");
        dec_str = dec.to_string(8, 8);
        EXPECT_EQ(dec_str, "-0.99999999");

        dec_str = dec.to_string(10, 0);
        EXPECT_EQ(dec_str, "-999999999");
        dec_str = dec.to_string(10, 6);
        EXPECT_EQ(dec_str, "-999.999999");
        dec_str = dec.to_string(10, 9);
        EXPECT_EQ(dec_str, "-0.999999999");
    }
    {
        std::string val_str("999999999999999999999999999999"); // 30 digits
        StringParser::ParseResult parse_result;
        Decimal128V3 dec = StringParser::string_to_decimal<TYPE_DECIMAL128I>(
                val_str.data(), val_str.size(), val_str.size(), 0, &parse_result);
        EXPECT_EQ(parse_result, StringParser::ParseResult::PARSE_SUCCESS);
        auto dec_str = dec.to_string(30, 0);
        EXPECT_EQ(dec_str, "999999999999999999999999999999");
        dec_str = dec.to_string(30, 6);
        EXPECT_EQ(dec_str, "999999999999999999999999.999999");
        dec_str = dec.to_string(30, 30);
        EXPECT_EQ(dec_str, "0.999999999999999999999999999999");

        dec_str = dec.to_string(20, 0);
        EXPECT_EQ(dec_str, "99999999999999999999");
        dec_str = dec.to_string(20, 6);
        EXPECT_EQ(dec_str, "99999999999999.999999");
        dec_str = dec.to_string(20, 20);
        EXPECT_EQ(dec_str, "0.99999999999999999999");
    }
    {
        std::string val_str("-999999999999999999999999999999"); // 30 digits
        StringParser::ParseResult parse_result;
        Decimal128V3 dec = StringParser::string_to_decimal<TYPE_DECIMAL128I>(
                val_str.data(), val_str.size(), val_str.size(), 0, &parse_result);
        EXPECT_EQ(parse_result, StringParser::ParseResult::PARSE_SUCCESS);
        auto dec_str = dec.to_string(30, 0);
        EXPECT_EQ(dec_str, "-999999999999999999999999999999");
        dec_str = dec.to_string(30, 6);
        EXPECT_EQ(dec_str, "-999999999999999999999999.999999");
        dec_str = dec.to_string(30, 30);
        EXPECT_EQ(dec_str, "-0.999999999999999999999999999999");

        dec_str = dec.to_string(20, 0);
        EXPECT_EQ(dec_str, "-99999999999999999999");
        dec_str = dec.to_string(20, 6);
        EXPECT_EQ(dec_str, "-99999999999999.999999");
        dec_str = dec.to_string(20, 20);
        EXPECT_EQ(dec_str, "-0.99999999999999999999");
    }

    {
        Decimal256 dec(type_limit<vectorized::Decimal256>::max());
        // Decimal256 dec_min(type_limit<vectorized::Decimal256>::min());
        auto dec_str = dec.to_string(76, 0);
        EXPECT_EQ(dec_str,
                  "9999999999999999999999999999999999999999999999999999999999999999999999999999");
        dec_str = dec.to_string(76, 6);
        EXPECT_EQ(dec_str,
                  "9999999999999999999999999999999999999999999999999999999999999999999999.999999");
        dec_str = dec.to_string(76, 76);
        EXPECT_EQ(dec_str,
                  "0.9999999999999999999999999999999999999999999999999999999999999999999999999999");
    }
}
} // namespace doris::vectorized