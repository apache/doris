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

#include "exprs/function/cast/cast_to_decimal.h"

#include <fstream>
#include <memory>

#include "common/exception.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/number_traits.h"
#include "core/extended_types.h"
#include "core/types.h"
#include "exprs/function/cast/cast_test.h"
#include "exprs/function/cast/cast_to_decimal_test.h"
#include "storage/olap_common.h"
#include "testutil/test_util.h"

namespace doris {

/*
TODO, fix:
mysql> select cast('+000999998000.5e-3' as Decimal(9, 3));
+---------------------------------------------+
| cast('+000999998000.5e-3' as Decimal(9, 3)) |
+---------------------------------------------+
|                                  999998.000 |
+---------------------------------------------+
1 row in set (8.54 sec)

// expected result: 9999999999999999.0
select cast('+0009999999999999999040000000.e-9' as decimal(18,1));
+------------------------------------------------------------+
| cast('+0009999999999999999040000000.e-9' as decimal(18,1)) |
+------------------------------------------------------------+
|                                        99999999999999999.9 |
+------------------------------------------------------------+
1 row in set (0.65 sec)

PG:
e1
postgres=# select cast('1e' as decimal(18,6));
ERROR:  invalid input syntax for type numeric: "1e"
LINE 1: select cast('1e' as decimal(18,6));

postgres=# select cast('.e1' as decimal(18,6));
ERROR:  invalid input syntax for type numeric: ".e1"
LINE 1: select cast('.e1' as decimal(18,6));
                    ^
postgres=# select cast('.1e1' as decimal(18,6));
 numeric  
----------
 1.000000
(1 row)

edge cases:
1000 digits
postgres=# select cast('1151111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111e-999' as decimal(38, 1));
 numeric 
---------
     1.2
(1 row)

MySQL 8.0
ysql> select cast('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111e-980' as decimal(38, 1));
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| cast('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                                                                         9999999999999999999999999999999999999.9 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set, 3 warnings (0.00 sec)

*/
TEST_F(FunctionCastToDecimalTest, test_from_string_invalid_input) {
    int table_index = 0;
    from_string_invalid_input_test_func<Decimal32>(9, 3, table_index++);
}

TEST_F(FunctionCastToDecimalTest, test_from_string_scientific_notation) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("1.4E+2")}, DECIMAL128V3(140, 0, 15)},
            {{std::string(".14E+3")}, DECIMAL128V3(140, 0, 15)},
            {{std::string("0.001E+5")}, DECIMAL128V3(100, 0, 15)},
            {{std::string("1.E+2")}, DECIMAL128V3(100, 0, 15)},
            {{std::string("1.4E+0")}, DECIMAL128V3(1, 400000000000000, 15)},
            {{std::string("1.4E-2")}, DECIMAL128V3(0, 14000000000000, 15)},
    };
    check_function_for_cast<DataTypeDecimal<Decimal128V3::PType>>(input_types, data_set, 15, 38);
}

TEST_F(FunctionCastToDecimalTest, string_parser_scientific_rounding) {
    auto parse_decimal128 = [](std::string_view value) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        auto parsed = StringParser::string_to_decimal<TYPE_DECIMAL128I>(value.data(), value.size(),
                                                                        38, 15, &result);
        EXPECT_EQ(result, StringParser::PARSE_SUCCESS);
        return parsed;
    };

    EXPECT_EQ(parse_decimal128("5e-16"), 1);
    EXPECT_EQ(parse_decimal128("5e-17"), 0);
    EXPECT_EQ(parse_decimal128("9e-17"), 0);
    EXPECT_EQ(parse_decimal128("-5e-17"), 0);
    EXPECT_EQ(parse_decimal128("0.0000000000000005"), 1);
    EXPECT_EQ(parse_decimal128("0.00000000000000005"), 0);
}

namespace {
template <typename T, typename F>
void check_float_decimal_value(F input, typename T::NativeType expected, UInt32 precision,
                               UInt32 scale, bool strict) {
    CastParameters params;
    params.is_strict = strict;
    T result;
    ASSERT_TRUE(CastToDecimal::from_float(input, result, precision, scale, params));
    EXPECT_EQ(result.value, expected);
}

template <typename T>
void check_float_decimal_overflow(double input, UInt32 precision, bool strict, UInt32 scale = 0) {
    CastParameters params;
    params.is_strict = strict;
    T result;
    EXPECT_FALSE(CastToDecimal::from_float(input, result, precision, scale, params));
    EXPECT_EQ(params.status.ok(), !strict);
}
} // namespace

TEST_F(FunctionCastToDecimalTest, float_rounding_and_bounds) {
    auto check = []<typename T>() {
        for (bool strict : {false, true}) {
            for (double sign : {-1.0, 1.0}) {
                for (double input : {9.0, 9.25, std::nextafter(9.5, 0.0)}) {
                    check_float_decimal_value<T>(sign * input, typename T::NativeType(sign * 9), 1,
                                                 0, strict);
                }
                for (double input : {9.5, 10.0, std::numeric_limits<double>::max()}) {
                    check_float_decimal_overflow<T>(sign * input, 1, strict);
                }
                check_float_decimal_value<T>(sign * std::nextafter(0.5, 0.0),
                                             typename T::NativeType(0), 1, 0, strict);
                check_float_decimal_value<T>(sign * 0.5, typename T::NativeType(sign), 1, 0,
                                             strict);
                check_float_decimal_value<T>(static_cast<float>(sign * 9.25),
                                             typename T::NativeType(sign * 9), 1, 0, strict);
            }
        }
    };
    check.operator()<Decimal32>();
    check.operator()<Decimal64>();
    check.operator()<Decimal128V3>();
    check.operator()<Decimal256>();
}

TEST_F(FunctionCastToDecimalTest, float_rounding_large_integers) {
    auto check = []<typename T>() {
        for (bool strict : {false, true}) {
            for (int64_t input : {4503599627370497LL, -4503599627370497LL}) {
                check_float_decimal_value<T>(static_cast<double>(input),
                                             typename T::NativeType(input), 16, 0, strict);
            }
            // The floating representation of the bound 10^18 - 1 rounds up to 10^18.
            for (double input : {1e18, -1e18}) {
                check_float_decimal_overflow<T>(input, 18, strict);
            }
        }
    };
    check.operator()<Decimal64>();
    check.operator()<Decimal128V3>();
    check.operator()<Decimal256>();
}

TEST_F(FunctionCastToDecimalTest, float_rounding_independent_of_backing_width) {
    for (bool strict : {false, true}) {
        for (int64_t sign : {-1, 1}) {
            check_float_decimal_value<Decimal128V3>(sign * 0.15, int128_t(sign * 2), 38, 1, strict);
            check_float_decimal_value<Decimal256>(sign * 0.15, wide::Int256(sign * 2), 39, 1,
                                                  strict);
        }
    }
}

TEST_F(FunctionCastToDecimalTest, float_rounding_decimal256_large_values) {
    CastParameters params;
    Decimal256 result;
    for (int64_t sign : {-1, 1}) {
        ASSERT_TRUE(CastToDecimal::from_float(sign * 9007199254740991.0, result, 39, 1, params));
        EXPECT_EQ(result.value, wide::Int256(sign) * 90071992547409904LL);
        // Exercise both 128-bit limbs, as well as a value with only the high limb set.
        ASSERT_TRUE(CastToDecimal::from_float(sign * (0x1p128 + 0x1p76), result, 76, 0, params));
        EXPECT_EQ(result.value, sign * ((wide::Int256(1) << 128) + (wide::Int256(1) << 76)));
        ASSERT_TRUE(CastToDecimal::from_float(sign * 0x1p200, result, 76, 0, params));
        EXPECT_EQ(result.value, sign * (wide::Int256(1) << 200));
    }
}

TEST_F(FunctionCastToDecimalTest, float_rounding_high_scale_overflow) {
    auto check = []<typename T>(UInt32 precision) {
        for (bool strict : {false, true}) {
            for (double sign : {-1.0, 1.0}) {
                check_float_decimal_overflow<T>(sign * 10, precision, strict, precision - 1);
                check_float_decimal_overflow<T>(sign, precision, strict, precision);
            }
        }
    };
    check.operator()<Decimal128V3>(38);
    check.operator()<Decimal256>(76);
}

TEST_F(FunctionCastToDecimalTest, float_rounding_decimalv2) {
    CastParameters params;
    DecimalV2Value result;
    for (int64_t sign : {-1, 1}) {
        ASSERT_TRUE(CastToDecimal::from_float(sign * 4503599.627370497, result, 27, 9, params));
        EXPECT_EQ(result.value(), sign * int128_t(4503599627370497LL));
        ASSERT_TRUE(CastToDecimal::from_float(sign * std::nextafter(0.5e-9, 0.0), result, 27, 9,
                                              params));
        EXPECT_EQ(result.value(), 0);
    }
}

TEST_F(FunctionCastToDecimalTest, test_from_bool) {
    from_bool_test_func<Decimal32>(9, 0);
    from_bool_test_func<Decimal32>(9, 1);
    from_bool_test_func<Decimal32>(9, 3);
    from_bool_test_func<Decimal32>(9, 8);

    from_bool_test_func<Decimal64>(18, 0);
    from_bool_test_func<Decimal64>(18, 1);
    from_bool_test_func<Decimal64>(18, 9);
    from_bool_test_func<Decimal64>(18, 17);

    from_bool_test_func<DecimalV2Value>(27, 9);
    // from_bool_test_func<DecimalV2Value, 27, 1>();
    // from_bool_test_func<DecimalV2Value, 27, 13>();
    // from_bool_test_func<DecimalV2Value, 27, 26>();

    from_bool_test_func<Decimal128V3>(38, 0);
    from_bool_test_func<Decimal128V3>(38, 1);
    from_bool_test_func<Decimal128V3>(38, 19);
    from_bool_test_func<Decimal128V3>(38, 37);

    from_bool_test_func<Decimal256>(76, 0);
    from_bool_test_func<Decimal256>(76, 1);
    from_bool_test_func<Decimal256>(76, 38);
    from_bool_test_func<Decimal256>(76, 75);
}

TEST_F(FunctionCastToDecimalTest, test_from_bool_overflow) {
    from_bool_overflow_test_func<Decimal32>();
    from_bool_overflow_test_func<Decimal64>();
    from_bool_overflow_test_func<DecimalV2Value>();
    from_bool_overflow_test_func<Decimal128V3>();
    from_bool_overflow_test_func<Decimal256>();
}

// A row that the input null map marks as NULL may still carry an arbitrary hidden payload in the
// nested column. The strict integer-to-decimal kernel must skip such rows instead of reporting the
// hidden value as out of range. check_overflow_for_decimal is disabled here so that the CAST
// wrapper does not replace the hidden payload beforehand and the kernel itself has to skip it.
TEST_F(FunctionCastToDecimalTest, int_to_decimal_skips_null_covered_payload) {
    auto ctx = create_context(true);
    ctx->set_check_overflow_for_decimal(false);

    auto from_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    auto to_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDecimal32>(9, 0));

    // Row 0 is NULL with hidden payload 999999999999999999, which overflows DECIMAL(9, 0).
    ColumnPtr from_column =
            ColumnHelper::create_nullable_column<DataTypeInt64>({999999999999999999LL, 1}, {1, 0});

    auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
    ASSERT_TRUE(fn != nullptr);

    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    ASSERT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_EQ(result.get_null_map_data()[0], 1);
    EXPECT_EQ(result.get_null_map_data()[1], 0);
    EXPECT_EQ(to_type->to_string(*block.get_by_position(1).column, 1), "1");

    // An out-of-range value in a visible (non NULL) row must still fail in strict mode.
    {
        ColumnPtr overflow_column = ColumnHelper::create_nullable_column<DataTypeInt64>(
                {999999999999999999LL, 999999999999999999LL}, {1, 0});
        Block overflow_block = {
                {std::move(overflow_column), from_type, "from"},
                {nullptr, to_type, "to"},
        };
        EXPECT_FALSE(fn(ctx.get(), overflow_block, {0}, 1, overflow_block.rows(), nullptr).ok());
    }
}
} // namespace doris
