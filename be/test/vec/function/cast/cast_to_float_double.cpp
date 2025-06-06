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

#include <type_traits>

#include "cast_test.h"
#include "olap/olap_common.h"
#include "runtime/primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/time_value.h"

namespace doris::vectorized {
using namespace ut_type;
struct FunctionCastToFloatTest : public FunctionCastTest {
    const std::vector<std::string> white_spaces = {" ", "\t", "\r", "\n", "\f", "\v"};
    std::string white_spaces_str = " \t\r\n\f\v";
    template <PrimitiveType FloatPType>
    void from_string_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                // Zero and sign variations
                {{std::string("0")}, FloatType(0.0)},
                {{std::string("+0")}, FloatType(0.0)},
                {{std::string("-0")}, FloatType(-0.0)},
                {{std::string("0.0")}, FloatType(0.0)},
                {{std::string("+0.0")}, FloatType(0.0)},
                {{std::string("-0.0")}, FloatType(-0.0)},
                {{std::string(".0")}, FloatType(0.0)},
                {{std::string("+.0")}, FloatType(0.0)},
                {{std::string("-.0")}, FloatType(-0.0)},

                // Normal positive values
                {{std::string("1")}, FloatType(1.0)},
                {{std::string("123")}, FloatType(123.0)},
                {{std::string("1.23")}, FloatType(1.23)},
                {{std::string("123.456")}, FloatType(123.456)},
                {{std::string("1.23456")}, FloatType(1.23456)},
                {{std::string("0.123456")}, FloatType(0.123456)},
                {{std::string(".123456")}, FloatType(0.123456)},

                // Normal negative values
                {{std::string("-1")}, FloatType(-1.0)},
                {{std::string("-123")}, FloatType(-123.0)},
                {{std::string("-1.23")}, FloatType(-1.23)},
                {{std::string("-123.456")}, FloatType(-123.456)},
                {{std::string("-1.23456")}, FloatType(-1.23456)},
                {{std::string("-0.123456")}, FloatType(-0.123456)},
                {{std::string("-.123456")}, FloatType(-0.123456)},

                // Scientific notation (exponent)
                {{std::string("1e0")}, FloatType(1.0)},
                {{std::string("1e1")}, FloatType(10.0)},
                {{std::string("1e-1")}, FloatType(0.1)},
                {{std::string("1.23e2")}, FloatType(123.0)},
                {{std::string("1.23e-2")}, FloatType(0.0123)},
                {{std::string("1.23E2")}, FloatType(123.0)},
                {{std::string("1.23E-2")}, FloatType(0.0123)},
                {{std::string("-1.23e2")}, FloatType(-123.0)},
                {{std::string("-1.23e-2")}, FloatType(-0.0123)},

                // Infinity values
                /*
                {{std::string("inf")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("INF")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("Inf")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("infinity")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("INFINITY")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("Infinity")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("+inf")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("-inf")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("+infinity")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("-infinity")}, std::numeric_limits<FloatType>::infinity()},

                // NaN values
                {{std::string("nan")}, std::numeric_limits<double>::quiet_NaN()},
                {{std::string("NAN")}, std::numeric_limits<double>::quiet_NaN()},
                {{std::string("NaN")}, std::numeric_limits<double>::quiet_NaN()},
                {{std::string("+nan")}, std::numeric_limits<double>::quiet_NaN()},
                {{std::string("-nan")}, std::numeric_limits<double>::quiet_NaN()},
                */

                // Edge values
                // {{std::string("1.7976931348623157e+308")},
                //  FloatType(std::numeric_limits<double>::max())},
                // {{std::string("-1.7976931348623157e+308")},
                //  FloatType(-std::numeric_limits<double>::max())},
                // {{std::string("2.2250738585072014e-308")},
                //  FloatType(std::numeric_limits<double>::min())},
                // {{std::string("-2.2250738585072014e-308")},
                //  FloatType(-std::numeric_limits<double>::min())},
                // Edge values - using type-specific limits
                {{fmt::format("{}", std::numeric_limits<FloatType>::max())},
                 FloatType(std::numeric_limits<FloatType>::max())},
                {{fmt::format("{}", -std::numeric_limits<FloatType>::max())},
                 FloatType(-std::numeric_limits<FloatType>::max())},
                {{fmt::format("{}", std::numeric_limits<FloatType>::min())},
                 FloatType(std::numeric_limits<FloatType>::min())},
                {{fmt::format("{}", -std::numeric_limits<FloatType>::min())},
                 FloatType(-std::numeric_limits<FloatType>::min())},

                // Very small values
                {{fmt::format("{}", std::numeric_limits<FloatType>::denorm_min())},
                 FloatType(std::numeric_limits<FloatType>::denorm_min())},
                {{fmt::format("{}", -std::numeric_limits<FloatType>::denorm_min())},
                 FloatType(-std::numeric_limits<FloatType>::denorm_min())},

                // Whitespace variations
                {{std::string(" 1.23")}, FloatType(1.23)},
                {{std::string("1.23 ")}, FloatType(1.23)},
                {{std::string(" 1.23 ")}, FloatType(1.23)},
                {{std::string("\t1.23")}, FloatType(1.23)},
                {{std::string("1.23\t")}, FloatType(1.23)},
                {{std::string("\n1.23")}, FloatType(1.23)},
                {{std::string("1.23\n")}, FloatType(1.23)},
                {{std::string("\r1.23")}, FloatType(1.23)},
                {{std::string("1.23\r")}, FloatType(1.23)},
                {{std::string("\f1.23")}, FloatType(1.23)},
                {{std::string("1.23\f")}, FloatType(1.23)},
                {{std::string("\v1.23")}, FloatType(1.23)},
                {{std::string("1.23\v")}, FloatType(1.23)},
                {{std::string(" \t\n\r\f\v1.23 \t\n\r\f\v")}, FloatType(1.23)},

                // Invalid cases (should throw or return error)
                /*
                {{std::string("")}, Exception("Empty string")},
                {{std::string(" ")}, Exception("Only whitespace")},
                {{std::string("abc")}, Exception("Invalid number")},
                {{std::string("1.2.3")}, Exception("Multiple decimal points")},
                {{std::string("1e2e3")}, Exception("Multiple exponents")},
                {{std::string("1e")}, Exception("Incomplete exponent")},
                {{std::string("e1")}, Exception("Missing significand")},
                {{std::string(".")}, Exception("Missing digits")},
                {{std::string("+")}, Exception("Missing digits")},
                {{std::string("-")}, Exception("Missing digits")},
                {{std::string("++1")}, Exception("Multiple signs")},
                {{std::string("--1")}, Exception("Multiple signs")},
                {{std::string("+-1")}, Exception("Multiple signs")},
                {{std::string("-+1")}, Exception("Multiple signs")},
                {{std::string("1e2.3")}, Exception("Decimal in exponent")},
                {{std::string("1e2e3")}, Exception("Multiple exponents")},
                {{std::string("1e2.3e4")}, Exception("Multiple exponents")},
                */
        };

        check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set);
    }

    template <PrimitiveType IntPType, PrimitiveType FloatPType>
    void from_int_test_func() {
        using IntType = typename PrimitiveTypeTraits<IntPType>::CppType;
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        DataTypeNumber<IntPType> dt_from;
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        DataSet data_set = {
                // Zero values
                {{IntType(0)}, FloatType(0.0)},
                {{IntType(-0)}, FloatType(0.0)},

                // Small positive values
                {{IntType(1)}, FloatType(1.0)},
                {{IntType(10)}, FloatType(10.0)},
                {{IntType(100)}, FloatType(100.0)},

                // Small negative values
                {{IntType(-1)}, FloatType(-1.0)},
                {{IntType(-10)}, FloatType(-10.0)},
                {{IntType(-100)}, FloatType(-100.0)},

                // Powers of 2 (important for floating-point representation)
                {{IntType(2)}, FloatType(2.0)},
                {{IntType(4)}, FloatType(4.0)},
                {{IntType(8)}, FloatType(8.0)},
                {{IntType(16)}, FloatType(16.0)},
                {{IntType(32)}, FloatType(32.0)},
                {{IntType(64)}, FloatType(64.0)},
                // {{IntType(128)}, FloatType(128.0)},
                {{IntType(-2)}, FloatType(-2.0)},
                {{IntType(-4)}, FloatType(-4.0)},
                {{IntType(-8)}, FloatType(-8.0)},
                {{IntType(-16)}, FloatType(-16.0)},
                {{IntType(-32)}, FloatType(-32.0)},
                {{IntType(-64)}, FloatType(-64.0)},
                {{IntType(-128)}, FloatType(-128.0)},

                // Edge values for each integer type
                {{std::numeric_limits<IntType>::min()},
                 FloatType(std::numeric_limits<IntType>::min())},
                {{std::numeric_limits<IntType>::max()},
                 FloatType(std::numeric_limits<IntType>::max())},
                {{static_cast<IntType>(std::numeric_limits<IntType>::min() + 1)},
                 FloatType(std::numeric_limits<IntType>::min() + 1)},
                {{static_cast<IntType>(std::numeric_limits<IntType>::max() - 1)},
                 FloatType(std::numeric_limits<IntType>::max() - 1)},

                // Values that might cause precision loss
                // {{IntType(16777215)}, FloatType(16777215.0)}, // 2^24 - 1 (float precision limit)
                // {{IntType(16777216)}, FloatType(16777216.0)}, // 2^24 (float precision limit)
                // {{IntType(16777217)}, FloatType(16777217.0)}, // 2^24 + 1 (might lose precision in float)
                // {{IntType(-16777215)}, FloatType(-16777215.0)},
                // {{IntType(-16777216)}, FloatType(-16777216.0)},
                // {{IntType(-16777217)}, FloatType(-16777217.0)},

                // Large values that might cause overflow
                // {{IntType(9007199254740991)},
                //  FloatType(9007199254740991.0)}, // 2^53 - 1 (double precision limit)
                // {{IntType(9007199254740992)},
                //  FloatType(9007199254740992.0)}, // 2^53 (double precision limit)
                // {{IntType(9007199254740993)},
                //  FloatType(9007199254740993.0)}, // 2^53 + 1 (might lose precision in double)
                // {{IntType(-9007199254740991)}, FloatType(-9007199254740991.0)},
                // {{IntType(-9007199254740992)}, FloatType(-9007199254740992.0)},
                // {{IntType(-9007199254740993)}, FloatType(-9007199254740993.0)},

                // Special cases for int128
                // {{IntType(1) << 63}, FloatType(std::pow(2.0, 63))},
                // {{IntType(1) << 64}, FloatType(std::pow(2.0, 64))},
                // {{IntType(1) << 96}, FloatType(std::pow(2.0, 96))},
                // {{IntType(1) << 127}, FloatType(std::pow(2.0, 127))},
                // {{-(IntType(1) << 63)}, FloatType(-std::pow(2.0, 63))},
                // {{-(IntType(1) << 64)}, FloatType(-std::pow(2.0, 64))},
                // {{-(IntType(1) << 96)}, FloatType(-std::pow(2.0, 96))},
                // {{-(IntType(1) << 127)}, FloatType(-std::pow(2.0, 127))},
        };

        check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set);
    }
    template <PrimitiveType FromPT, int FromPrecision, int FromScale, PrimitiveType FloatPType>
    void from_decimalv3_no_overflow_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(IsDecimalNumber<FromT>, "FromT must be a decimal type");
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        DataTypeDecimal<FromPT> dt_from(FromPrecision, FromScale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), FromScale, FromPrecision}};
        auto decimal_ctor = get_decimal_ctor<FromT>();

        // Compute valid ranges for integral and fractional parts
        // max_integral:    99999999
        // large_integral1: 9999999
        // large_integral2: 90000000
        // large_integral3: 90000001
        constexpr auto max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        constexpr auto large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                                 FromPrecision - FromScale - 1) -
                                         1;
        constexpr auto large_integral2 = max_integral - large_integral1;
        constexpr auto large_integral3 =
                large_integral2 > 9 ? large_integral2 + 1 : large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;
        std::cout << "large_integral1:\t" << fmt::format("{}", large_integral1) << std::endl;
        std::cout << "large_integral2:\t" << fmt::format("{}", large_integral2) << std::endl;
        std::cout << "large_integral3:\t" << fmt::format("{}", large_integral3) << std::endl;

        // max_fractional:    99999999
        // large_fractional1: 9999999
        // large_fractional2: 90000000
        // large_fractional3: 90000001
        constexpr auto max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale) - 1;
        constexpr auto large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale - 1) - 1;
        constexpr auto large_fractional2 = max_fractional - large_fractional1;
        constexpr auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        std::vector<typename FromT::NativeType> integral_part = {0,
                                                                 1,
                                                                 9,
                                                                 max_integral,
                                                                 max_integral - 1,
                                                                 large_integral1,
                                                                 large_integral2,
                                                                 large_integral3};
        std::vector<typename FromT::NativeType> fractional_part = {0,
                                                                   1,
                                                                   9,
                                                                   max_fractional,
                                                                   max_fractional - 1,
                                                                   large_fractional1,
                                                                   large_fractional2,
                                                                   large_fractional3};
        DataTypeDecimal<FromPT> dt(FromPrecision, FromScale);
        DataSet data_set;
        std::string dbg_str =
                fmt::format("test cast Decimal({}, {}) to {}: ", FromPrecision, FromScale,
                            std::is_same_v<FloatType, Float32> ? "float" : "double");

        auto scale_multiplier = decimal_scale_multiplier<typename FromT::NativeType>(FromScale);
        if constexpr (FromScale == 0) {
            // e.g. Decimal(9, 0), only int part
            for (const auto& i : integral_part) {
                auto decimal_num = decimal_ctor(i, 0, FromScale);
                auto float_v = static_cast<FloatType>(i);
                if (std::isinf(float_v)) {
                    std::cout << fmt::format("cast Decimal value {} to float_v result is inf\n",
                                             dt.to_string(decimal_num));
                } else {
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                    data_set.push_back({{decimal_num}, float_v});
                }

                decimal_num = decimal_ctor(-i, 0, FromScale);
                float_v = static_cast<FloatType>(-i);
                if (std::isinf(float_v)) {
                    std::cout << fmt::format("cast Decimal value {} to float_v result is inf\n",
                                             dt.to_string(decimal_num));
                } else {
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), -i);
                    data_set.push_back({{decimal_num}, FloatType(-i)});
                }
            }
            dbg_str += "\n";
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set);
            return;
        } else if constexpr (FromScale == FromPrecision) {
            // e.g. Decimal(9, 9), only fraction part
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(0, f, FromScale);
                auto float_v = FloatType(f) / scale_multiplier;
                dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});

                decimal_num = decimal_ctor(0, -f, FromScale);
                float_v = FloatType(-f) / scale_multiplier;
                dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
            }
            dbg_str += "\n";
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set);
            return;
        }

        for (const auto& i : integral_part) {
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(i, f, FromScale);
                auto float_v = static_cast<FloatType>(decimal_num.value) / scale_multiplier;
                if (std::isinf(float_v)) {
                    std::cout << fmt::format("cast Decimal value {} to float_v result is inf\n",
                                             dt.to_string(decimal_num));
                } else {
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                    data_set.push_back({{decimal_num}, float_v});
                }

                decimal_num = decimal_ctor(-i, -f, FromScale);
                float_v = static_cast<FloatType>(decimal_num.value) / scale_multiplier;
                if (std::isinf(float_v)) {
                    std::cout << fmt::format("cast Decimal value {} to float_v result is inf\n",
                                             dt.to_string(decimal_num));
                } else {
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                    data_set.push_back({{decimal_num}, float_v});
                }
            }
            dbg_str += "\n";
        }
        std::cout << dbg_str << std::endl;

        check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set);
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void from_decimal_test_func() {
        constexpr auto max_decimal_pre = max_decimal_precision<FromPT>();
        constexpr auto min_decimal_pre =
                FromPT == TYPE_DECIMAL32
                        ? 1
                        : (FromPT == TYPE_DECIMAL64
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (FromPT == TYPE_DECIMAL128I
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (FromPT == TYPE_DECIMAL256
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(min_decimal_pre == 1 || min_decimal_pre > 9);
        from_decimalv3_no_overflow_test_func<FromPT, min_decimal_pre, 0, ToPT>();
        if constexpr (min_decimal_pre != 1) {
            from_decimalv3_no_overflow_test_func<FromPT, min_decimal_pre, min_decimal_pre / 2,
                                                 ToPT>();
            from_decimalv3_no_overflow_test_func<FromPT, min_decimal_pre, min_decimal_pre - 1,
                                                 ToPT>();
        }
        from_decimalv3_no_overflow_test_func<FromPT, min_decimal_pre, min_decimal_pre, ToPT>();

        from_decimalv3_no_overflow_test_func<FromPT, max_decimal_pre, 0, ToPT>();
        from_decimalv3_no_overflow_test_func<FromPT, max_decimal_pre, 1, ToPT>();
        from_decimalv3_no_overflow_test_func<FromPT, max_decimal_pre, max_decimal_pre / 2, ToPT>();
        from_decimalv3_no_overflow_test_func<FromPT, max_decimal_pre, max_decimal_pre - 1, ToPT>();
        from_decimalv3_no_overflow_test_func<FromPT, max_decimal_pre, max_decimal_pre, ToPT>();
    }

    template <PrimitiveType FloatPType>
    void from_date_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        std::vector<uint16_t> years = {0,   1,    9,    10,   11,   99,   100,  101,
                                       999, 1000, 1001, 1999, 2000, 2024, 2025, 9999};
        std::vector<uint8_t> months = {1, 9, 10, 11, 12};
        std::vector<uint8_t> days = {1, 2, 9, 10, 11, 28};
        DataTypeDateV2 dt;
        DataSet data_set;
        std::string dbg_str = fmt::format("test cast date to {}: ",
                                          std::is_same_v<FloatType, Float32> ? "float" : "double");
        for (auto year : years) {
            for (auto month : months) {
                for (auto day : days) {
                    DateV2Value<DateV2ValueType> date_val(year, month, day, 0, 0, 0, 0);
                    FloatType expect_cast_result = year * 10000 + month * 100 + day;
                    dbg_str += fmt::format("({}, {})|", dt.to_string(date_val.to_date_int_val()),
                                           expect_cast_result);
                    data_set.push_back({{date_val}, expect_cast_result});
                }
            }
            dbg_str += "\n";
        }
        std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set, false);
    }

    template <PrimitiveType FloatPType, int Scale>
    void from_datetime_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, Scale}};
        std::vector<uint16_t> years = {0, 1, 10, 100, 2025, 9999};
        std::vector<uint8_t> months = {1, 10, 12};
        std::vector<uint8_t> days = {1, 10, 28};
        std::vector<uint8_t> hours = {0, 1, 10, 23};
        std::vector<uint8_t> minutes = {0, 1, 10, 59};
        std::vector<uint8_t> seconds = {0, 1, 10, 59};
        std::vector<uint32_t> mircoseconds = {0, 1, 999999};
        DataTypeDateTimeV2 dt(Scale);
        DataSet data_set;
        // std::string dbg_str = fmt::format("test cast datetimev2 to integers: ");
        for (auto year : years) {
            for (auto month : months) {
                for (auto day : days) {
                    for (auto hour : hours) {
                        for (auto minute : minutes) {
                            for (auto second : seconds) {
                                for (auto microsecond : mircoseconds) {
                                    DateV2Value<DateTimeV2ValueType> date_val(
                                            year, month, day, hour, minute, second, microsecond);
                                    FloatType expect_cast_result =
                                            (year * 10000L + month * 100 + day) * 1000000L +
                                            hour * 10000 + minute * 100 + second;
                                    // dbg_str += fmt::format("({}, {})|",
                                    //                        dt.to_string(date_val.to_date_int_val()),
                                    //                        expect_cast_result);
                                    data_set.push_back({{date_val}, expect_cast_result});
                                }
                            }
                        }
                    }
                    // dbg_str += "\n";
                }
            }
        }
        // std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<FloatPType>>(input_types, data_set, false);
    }

    template <PrimitiveType ToPT, bool negative>
    void from_time_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        std::vector<int64_t> hours = {0, 1, 10, 100, 838};
        std::vector<int64_t> minutes = {0, 1, 10, 59};
        std::vector<int64_t> seconds = {0, 1, 10, 59};

        std::string dbg_str = fmt::format("test cast time to {}: ",
                                          std::is_same_v<ToT, Float32> ? "float" : "double");
        DataSet data_set;
        for (auto h : hours) {
            for (auto m : minutes) {
                for (auto s : seconds) {
                    auto time_val = doris::TimeValue::make_time_with_negative(negative, h, m, s);
                    auto expect_cast_result = static_cast<ToT>(time_val);
                    data_set.push_back({{time_val}, expect_cast_result});
                    dbg_str += fmt::format("({}, {})|", doris::TimeValue::to_string(time_val, 6),
                                           expect_cast_result);
                }
            }
        }

        std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set, false);
    }
};
/*
<float> ::= <whitespace>* <value> <whitespace>*

<whitespace> ::= " " | "\t" | "\n" | "\r" | "\f" | "\v"

<value> ::= <decimal> | <infinity> | <nan>

<decimal> ::= <sign>? <significand> <exponent>?

<infinity> ::= <sign>? <inf_literal>

<nan> ::= <sign>? <nan_literal>

<sign> ::= "+" | "-"

<significand> ::= <digits> | <digits> "." <digits> | <digits> "." | "." <digits>

<digits> ::= <digit>+

<digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<exponent> ::= <e_marker> <sign>? <digits>

<e_marker> ::= "e" | "E"

<inf_literal> ::= <"INF" case-insensitive> | <"INFINITY" case-insensitive>

<nan_literal> ::= <"NAN" case-insensitive>
*/
TEST_F(FunctionCastToFloatTest, test_from_string) {
    from_string_test_func<TYPE_FLOAT>();
    from_string_test_func<TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_bool) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    {
        DataSet data_set = {
                {{UInt8 {0}}, Float32(0)},
                {{UInt8 {1}}, Float32(1)},
        };
        check_function_for_cast<DataTypeFloat32>(input_types, data_set);
    }
    {
        DataSet data_set = {
                {{UInt8 {0}}, Float64(0)},
                {{UInt8 {1}}, Float64(1)},
        };
        check_function_for_cast<DataTypeFloat64>(input_types, data_set);
    }
}

TEST_F(FunctionCastToFloatTest, test_from_int) {
    // Test Int8 to Float32/Float64
    from_int_test_func<TYPE_TINYINT, TYPE_FLOAT>();
    from_int_test_func<TYPE_TINYINT, TYPE_DOUBLE>();

    // Test Int16 to Float32/Float64
    from_int_test_func<TYPE_SMALLINT, TYPE_FLOAT>();
    from_int_test_func<TYPE_SMALLINT, TYPE_DOUBLE>();

    // Test Int32 to Float32/Float64
    from_int_test_func<TYPE_INT, TYPE_FLOAT>();
    from_int_test_func<TYPE_INT, TYPE_DOUBLE>();

    // Test Int64 to Float32/Float64
    from_int_test_func<TYPE_BIGINT, TYPE_FLOAT>();
    from_int_test_func<TYPE_BIGINT, TYPE_DOUBLE>();

    // Test Int128 to Float32/Float64
    from_int_test_func<TYPE_LARGEINT, TYPE_FLOAT>();
    from_int_test_func<TYPE_LARGEINT, TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_float_to_double) {
    InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
    DataSet data_set = {
            // Zero and sign variations
            {{Float32(0)}, Float64(0.0)},
            {{Float32(+0)}, Float64(0.0)},
            {{Float32(-0)}, Float64(-0.0)},
            {{Float32(0.0)}, Float64(0.0)},
            {{Float32(+0.0)}, Float64(0.0)},
            {{Float32(-0.0)}, Float64(-0.0)},
            {{Float32(.0)}, Float64(0.0)},
            {{Float32(+.0)}, Float64(0.0)},
            {{Float32(-.0)}, Float64(-0.0)},

            // Normal positive values
            {{Float32(1)}, Float64(1.0)},
            {{Float32(123)}, Float64(123.0)},
            {{Float32(1.23)}, Float64(Float32(1.23))},
            {{Float32(123.456)}, Float64(Float32(123.456))},
            {{Float32(1.23456)}, Float64(Float32(1.23456))},
            {{Float32(0.123456)}, Float64(Float32(0.123456))},
            {{Float32(.123456)}, Float64(Float32(0.123456))},

            // Normal negative values
            {{Float32(-1)}, Float64(-1.0)},
            {{Float32(-123)}, Float64(-123.0)},
            {{Float32(-1.23)}, Float64(Float32(-1.23))},
            {{Float32(-123.456)}, Float64(Float32(-123.456))},
            {{Float32(-1.23456)}, Float64(Float32(-1.23456))},
            {{Float32(-0.123456)}, Float64(Float32(-0.123456))},
            {{Float32(-.123456)}, Float64(Float32(-0.123456))},

            // Scientific notation (exponent)
            {{Float32(1e0)}, Float64(1.0)},
            {{Float32(1e1)}, Float64(10.0)},
            {{Float32(1e-1)}, Float64(Float32(0.1))},
            {{Float32(1.23e2)}, Float64(123.0)},
            {{Float32(1.23e-2)}, Float64(Float32(0.0123))},
            {{Float32(1.23E2)}, Float64(123.0)},
            {{Float32(1.23E-2)}, Float64(Float32(0.0123))},
            {{Float32(-1.23e2)}, Float64(-123.0)},
            {{Float32(-1.23e-2)}, Float64(Float32(-0.0123))},

            // Infinity values
            /*
                {{Float32(inf)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(INF)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(Inf)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(infinity)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(INFINITY)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(Infinity)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(+inf)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(-inf)}, Float64(-std::numeric_limits<double>::infinity())},
                {{Float32(+infinity)}, Float64(std::numeric_limits<double>::infinity())},
                {{Float32(-infinity)}, Float64(-std::numeric_limits<double>::infinity())},
                */

            // NaN values
            /*
                {{Float32(nan)}, Float64(std::numeric_limits<double>::quiet_NaN())},
                {{Float32(NAN)}, Float64(std::numeric_limits<double>::quiet_NaN())},
                {{Float32(NaN)}, Float64(std::numeric_limits<double>::quiet_NaN())},
                {{Float32(+nan)}, Float64(std::numeric_limits<double>::quiet_NaN())},
                {{Float32(-nan)}, Float64(std::numeric_limits<double>::quiet_NaN())},
                */

            // Edge values
            // {{Float32(1.7976931348623157e+308)},
            //  Float64(std::numeric_limits<double>::max())},
            // {{Float32(-1.7976931348623157e+308)},
            //  Float64(-std::numeric_limits<double>::max())},
            // {{Float32(2.2250738585072014e-308)},
            //  Float64(std::numeric_limits<double>::min())},
            // {{Float32(-2.2250738585072014e-308)},
            //  Float64(-std::numeric_limits<double>::min())},
            // Edge values - using type-specific limits
            {{std::numeric_limits<Float32>::max()}, Float64(std::numeric_limits<Float32>::max())},
            {{-std::numeric_limits<Float32>::max()}, Float64(-std::numeric_limits<Float32>::max())},
            {{std::numeric_limits<Float32>::min()}, Float64(std::numeric_limits<Float32>::min())},
            {{-std::numeric_limits<Float32>::min()}, Float64(-std::numeric_limits<Float32>::min())},

            // Very small values
            {{std::numeric_limits<Float32>::denorm_min()},
             Float64(std::numeric_limits<Float32>::denorm_min())},
            {{-std::numeric_limits<Float32>::denorm_min()},
             Float64(-std::numeric_limits<Float32>::denorm_min())},

    };

    check_function_for_cast<DataTypeFloat64>(input_types, data_set);
}
TEST_F(FunctionCastToFloatTest, test_from_double_to_float) {
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            // Zero and sign variations
            {{Float64(0)}, Float32(0.0)},
            {{Float64(+0)}, Float32(0.0)},
            {{Float64(-0)}, Float32(-0.0)},
            {{Float64(0.0)}, Float32(0.0)},
            {{Float64(+0.0)}, Float32(0.0)},
            {{Float64(-0.0)}, Float32(-0.0)},
            {{Float64(.0)}, Float32(0.0)},
            {{Float64(+.0)}, Float32(0.0)},
            {{Float64(-.0)}, Float32(-0.0)},

            // Normal positive values
            {{Float64(1)}, Float32(1.0)},
            {{Float64(123)}, Float32(123.0)},
            {{Float64(1.23)}, Float32(Float64(1.23))},
            {{Float64(123.456)}, Float32(Float64(123.456))},
            {{Float64(1.23456)}, Float32(Float64(1.23456))},
            {{Float64(0.123456)}, Float32(Float64(0.123456))},
            {{Float64(.123456)}, Float32(Float64(0.123456))},

            // Normal negative values
            {{Float64(-1)}, Float32(-1.0)},
            {{Float64(-123)}, Float32(-123.0)},
            {{Float64(-1.23)}, Float32(Float64(-1.23))},
            {{Float64(-123.456)}, Float32(Float64(-123.456))},
            {{Float64(-1.23456)}, Float32(Float64(-1.23456))},
            {{Float64(-0.123456)}, Float32(Float64(-0.123456))},
            {{Float64(-.123456)}, Float32(Float64(-0.123456))},

            // Scientific notation (exponent)
            {{Float64(1e0)}, Float32(1.0)},
            {{Float64(1e1)}, Float32(10.0)},
            {{Float64(1e-1)}, Float32(Float64(0.1))},
            {{Float64(1.23e2)}, Float32(123.0)},
            {{Float64(1.23e-2)}, Float32(Float64(0.0123))},
            {{Float64(1.23E2)}, Float32(123.0)},
            {{Float64(1.23E-2)}, Float32(Float64(0.0123))},
            {{Float64(-1.23e2)}, Float32(-123.0)},
            {{Float64(-1.23e-2)}, Float32(Float64(-0.0123))},

            // Infinity values
            /*
                {{Float64(inf)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(INF)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(Inf)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(infinity)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(INFINITY)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(Infinity)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(+inf)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(-inf)}, Float32(-std::numeric_limits<double>::infinity())},
                {{Float64(+infinity)}, Float32(std::numeric_limits<double>::infinity())},
                {{Float64(-infinity)}, Float32(-std::numeric_limits<double>::infinity())},
                */

            // NaN values
            /*
                {{Float64(nan)}, Float32(std::numeric_limits<double>::quiet_NaN())},
                {{Float64(NAN)}, Float32(std::numeric_limits<double>::quiet_NaN())},
                {{Float64(NaN)}, Float32(std::numeric_limits<double>::quiet_NaN())},
                {{Float64(+nan)}, Float32(std::numeric_limits<double>::quiet_NaN())},
                {{Float64(-nan)}, Float32(std::numeric_limits<double>::quiet_NaN())},
                */

            // Edge values - using type-specific limits
            {{Float64(std::numeric_limits<Float32>::max())}, std::numeric_limits<Float32>::max()},
            {{Float64(-std::numeric_limits<Float32>::max())}, -std::numeric_limits<Float32>::max()},
            {{Float64(std::numeric_limits<Float32>::min())}, std::numeric_limits<Float32>::min()},
            {{Float64(-std::numeric_limits<Float32>::min())}, -std::numeric_limits<Float32>::min()},

            // Very small values
            {{Float64(std::numeric_limits<Float32>::denorm_min())},
             Float32(std::numeric_limits<Float32>::denorm_min())},
            {{Float64(-std::numeric_limits<Float32>::denorm_min())},
             Float32(-std::numeric_limits<Float32>::denorm_min())},

    };

    check_function_for_cast<DataTypeFloat32>(input_types, data_set);
}
TEST_F(FunctionCastToFloatTest, test_from_decimal) {
    from_decimal_test_func<TYPE_DECIMAL32, TYPE_FLOAT>();
    from_decimal_test_func<TYPE_DECIMAL64, TYPE_FLOAT>();
    from_decimal_test_func<TYPE_DECIMAL128I, TYPE_FLOAT>();
    from_decimal_test_func<TYPE_DECIMAL256, TYPE_FLOAT>();

    from_decimal_test_func<TYPE_DECIMAL32, TYPE_DOUBLE>();
    from_decimal_test_func<TYPE_DECIMAL64, TYPE_DOUBLE>();
    from_decimal_test_func<TYPE_DECIMAL128I, TYPE_DOUBLE>();
    from_decimal_test_func<TYPE_DECIMAL256, TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_date) {
    from_date_test_func<TYPE_FLOAT>();
    from_date_test_func<TYPE_DOUBLE>();
}

TEST_F(FunctionCastToFloatTest, test_from_datetime) {
    from_datetime_test_func<TYPE_FLOAT, 0>();
    from_datetime_test_func<TYPE_FLOAT, 1>();
    from_datetime_test_func<TYPE_FLOAT, 3>();
    from_datetime_test_func<TYPE_FLOAT, 6>();

    from_datetime_test_func<TYPE_DOUBLE, 0>();
    from_datetime_test_func<TYPE_DOUBLE, 1>();
    from_datetime_test_func<TYPE_DOUBLE, 3>();
    from_datetime_test_func<TYPE_DOUBLE, 6>();
}
TEST_F(FunctionCastToFloatTest, test_from_time) {
    from_time_test_func<TYPE_FLOAT, false>();
    from_time_test_func<TYPE_FLOAT, true>();

    from_time_test_func<TYPE_DOUBLE, false>();
    from_time_test_func<TYPE_DOUBLE, true>();
}
} // namespace doris::vectorized