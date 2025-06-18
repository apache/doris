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

#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "cast_test.h"
#include "olap/olap_common.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
using namespace ut_type;
struct FunctionCastToIntTest : public FunctionCastTest {
    const std::vector<std::string> white_spaces = {" ", "\t", "\r", "\n", "\f", "\v"};
    std::string white_spaces_str = " \t\r\n\f\v";
    template <PrimitiveType PType>
    void from_string_test_func() {
        using T = typename PrimitiveTypeTraits<PType>::CppType;
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        using UnsignedT = typename std::make_unsigned<T>::type;
        DataTypeNumber<PType> dt;
        T max_val = std::numeric_limits<T>::max();
        T min_val = std::numeric_limits<T>::min() + T(1);
        auto max_val_minus_1 = max_val - T {1};
        auto min_val_plus_1 = min_val + T {1};
        auto min_val_abs_val = UnsignedT(max_val) + 1;
        std::string min_val_str_no_sign = DataTypeNumber<PType>::to_string(min_val_abs_val);
        std::vector<T> test_vals = {T {0},  T {1},  T {9},    T {123},    max_val,
                                    T {-1}, T {-9}, T {-123}, T {min_val}};
        test_vals.push_back(max_val_minus_1);
        test_vals.push_back(min_val_plus_1);

        // test leading zeros, sign, leading and trailing white spaces for positive values
        auto tmp_test_func = [&](bool with_spaces, bool with_sign, bool leading_zeros) {
            DataSet data_set;
            for (auto v : test_vals) {
                bool is_negative = (v < 0);
                std::string v_str;
                if (is_negative) {
                    // string format without sign
                    v_str = dt.to_string(-v);
                } else {
                    v_str = dt.to_string(v);
                }
                if (leading_zeros) {
                    v_str = "000" + v_str;
                }
                if (is_negative) {
                    v_str = "-" + v_str;
                } else {
                    // optional '+'
                    if (with_sign) {
                        v_str = "+" + v_str;
                    }
                }
                if (with_spaces) {
                    for (const auto& sp : white_spaces) {
                        // Single whitespace combinations
                        data_set.push_back({{sp + v_str}, v});
                        data_set.push_back({{v_str + sp}, v});
                        data_set.push_back({{sp + v_str + sp}, v});

                        // Multiple whitespace combinations
                        data_set.push_back({{sp + sp + sp + v_str}, v});
                        data_set.push_back({{v_str + sp + sp + sp}, v});
                        data_set.push_back({{sp + sp + sp + v_str + sp + sp + sp}, v});
                    }
                    data_set.push_back({{white_spaces_str + v_str}, v});
                    data_set.push_back({{v_str + white_spaces_str}, v});
                    data_set.push_back({{white_spaces_str + v_str + white_spaces_str}, v});
                } else {
                    data_set.push_back({{v_str}, v});
                }
            }
            std::string dbg_str;
            if (!with_spaces) {
                for (const auto& p : data_set) {
                    dbg_str += "|" + any_cast<ut_type::STRING>(p.first[0]) + "|, ";
                }
                std::cout << "test cast from string to int, data set: " << dbg_str << std::endl;
            }
            check_function_for_cast<DataTypeNumber<PType>>(input_types, data_set);
        };
        // test leading and trailing white spaces, sign and leading zeros
        tmp_test_func(true, true, true);
        // test leading and trailing spaces and sign
        tmp_test_func(true, true, false);
        // test with sign and leading zeros
        tmp_test_func(false, true, true);
        // test with sign
        tmp_test_func(false, true, false);
        /*
        DataSet data_set = {
                // Invalid cases - these should throw exceptions or return error status
                {{std::string("")}, Exception("Empty string")},
                {{std::string(" ")}, Exception("Only whitespace")},
                {{std::string("+")}, Exception("Only sign")},
                {{std::string("-")}, Exception("Only sign")},
                {{std::string("a123")}, Exception("Leading letter")},
                {{std::string("123a")}, Exception("Trailing letter")},
                {{std::string("12.34")}, Exception("Decimal point")},
                {{std::string("1,234")}, Exception("Comma")},
                {{std::string("1_234")}, Exception("Underscore")},
                {{std::string("0x123")}, Exception("Hex prefix")},
                {{std::string("123e4")}, Exception("Scientific notation")},
                {{std::string("++123")}, Exception("Double plus")},
                {{std::string("--123")}, Exception("Double minus")},
                {{std::string("+-123")}, Exception("Plus minus")},
                {{std::string("-+123")}, Exception("Minus plus")},
                {{std::string("123 456")}, Exception("Space between digits")},

                // Overflow cases
                {{std::string("128")}, Exception("Overflow")},      // > MAX_INT8
                {{std::string("-129")}, Exception("Underflow")},    // < MIN_INT8
                {{std::string("999999")}, Exception("Overflow")},   // Large number
                {{std::string("-999999")}, Exception("Underflow")}, // Large negative
        };
        */
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void narrow_to_wider_int_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(sizeof(FromT) <= sizeof(ToT), "FromT must be smaller than ToT");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};
        auto max_val = std::numeric_limits<FromT>::max();
        auto min_val = std::numeric_limits<FromT>::min();
        FromT max_val_minus_1 = max_val - FromT {1};
        FromT min_val_plus_1 = min_val + FromT {1};
        std::vector<std::pair<FromT, ToT>> test_vals = {
                {FromT {0}, ToT(0)},      {FromT {1}, ToT {1}},       {FromT {9}, ToT {9}},
                {FromT {123}, ToT {123}}, {max_val, ToT {max_val}},   {FromT {-1}, ToT {-1}},
                {FromT {-9}, ToT {-9}},   {FromT {-123}, ToT {-123}}, {min_val, ToT {min_val}}};
        test_vals.push_back({max_val_minus_1, ToT {max_val_minus_1}});
        test_vals.push_back({min_val_plus_1, ToT {min_val_plus_1}});
        DataSet data_set;
        for (const auto& p : test_vals) {
            data_set.push_back({{p.first}, p.second});
        }
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void wider_to_narrow_int_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(sizeof(FromT) > sizeof(ToT), "FromT must be larger than ToT");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};
        // using UnsignedT = typename std::make_unsigned<FromT>::type;
        // auto from_max_val = std::numeric_limits<FromT>::max();
        // auto from_min_val = std::numeric_limits<FromT>::min();
        auto to_max_val = std::numeric_limits<ToT>::max();
        auto to_min_val = std::numeric_limits<ToT>::min();
        std::vector<std::pair<FromT, ToT>> test_vals = {{FromT {0}, ToT(0)},
                                                        {FromT {1}, ToT {1}},
                                                        {FromT {9}, ToT {9}},
                                                        {FromT {123}, ToT {123}},
                                                        {FromT {to_max_val}, ToT {to_max_val}},
                                                        {FromT {-1}, ToT {-1}},
                                                        {FromT {-9}, ToT {-9}},
                                                        {FromT {-123}, ToT {-123}},
                                                        {FromT {to_min_val}, ToT {to_min_val}}};
        // test_vals.push_back({max_val_minus_1, ToT {max_val_minus_1}});
        // test_vals.push_back({min_val_plus_1, ToT {min_val_plus_1}});
        DataSet data_set;
        for (const auto& p : test_vals) {
            data_set.push_back({{p.first}, p.second});
        }
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void from_float_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_iec559, "FromT must be a floating point type");
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};

        DataSet data_set = {
                // Zero and sign
                {{FromT {0.0}}, ToT(0)},
                {{FromT {-0.0}}, ToT(0)},
                {{FromT {+0.0}}, ToT(0)},

                // Positive integers
                {{FromT {1.0}}, ToT(1)},
                {{FromT {9.0}}, ToT(9)},
                {{FromT {123.0}}, ToT(123)},
                {{static_cast<FromT>(std::numeric_limits<ToT>::max())},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::max()))}, // ToT max

                // Negative integers
                {{FromT {-1.0}}, ToT(-1)},
                {{FromT {-9.0}}, ToT(-9)},
                {{FromT {-123.0}}, ToT(-123)},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min())},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min()))}, // ToT min

                // Just below/above integer boundaries
                {{static_cast<FromT>(std::numeric_limits<ToT>::max() - 1)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::max() - 1))},
                {{static_cast<FromT>(std::numeric_limits<ToT>::max() - 0.001)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::max() - 0.001))},
                {{static_cast<FromT>(std::numeric_limits<ToT>::max() + 0.999)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::max() +
                                                     0.999))}, // Should clamp to max
                // {{FromT {128.0}}, ToT(127)},   // Overflow, clamp to max
                // {{FromT {128.1}}, ToT(127)},   // Overflow, clamp to max

                {{static_cast<FromT>(std::numeric_limits<ToT>::min() + 1)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min() + 1))},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min() + 0.001)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min() + 0.001))},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min() - 0.999)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min() -
                                                     0.999))}, // Should clamp to min
                // {{FromT {-129.0}}, ToT(-128)},   // Underflow, clamp to min
                // {{FromT {-129.1}}, ToT(-128)},   // Underflow, clamp to min

                // Fractional values (truncate toward zero)
                {{FromT {1.9}}, ToT(1)},
                {{FromT {-1.9}}, ToT(-1)},
                {{FromT {0.9999}}, ToT(0)},
                {{FromT {-0.9999}}, ToT(0)},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min() + 1.5)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min() + 1.5))},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min() + 0.5)},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min() + 0.5))},

                // Subnormal (denormalized) numbers
                {{FromT {std::numeric_limits<FromT>::denorm_min()}}, ToT(0)},
                {{FromT {-std::numeric_limits<FromT>::denorm_min()}}, ToT(0)},

                // // Large float values (overflow/underflow)
                // {{FromT {numeric_limits<double>::max()}}, ToT(127)},
                // {{FromT {-numeric_limits<double>::max()}}, ToT(-128)},
                // {{FromT {1e20}}, ToT(127)},
                // {{FromT {-1e20}}, ToT(-128)},

                // Infinities
                // {{FromT {numeric_limits<double>::infinity()}}, ToT(127)},
                // {{FromT {-numeric_limits<double>::infinity()}}, ToT(-128)},

                // NaN (should probably return 0 or error, depending on implementation)
                // {{FromT {std::numeric_limits<double>::quiet_NaN()}}, ToT(0)},
                // {{FromT {std::numeric_limits<double>::signaling_NaN()}}, ToT(0)},
        };

        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
    }

    template <PrimitiveType FromPT, int FromPrecision, int FromScale, PrimitiveType ToPT>
    void from_decimalv3_no_overflow_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(IsDecimalNumber<FromT>, "FromT must be a decimal type");
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        // static_assert(FromPrecision - FromScale >= 0 &&
        //                       FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>(),
        //               "Decimal integral part must be less than integer max ascii len");

        DataTypeDecimal<FromPT> dt_from(FromPrecision, FromScale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), FromScale, FromPrecision}};
        auto decimal_ctor = get_decimal_ctor<FromT>();

        using IntType = ToT;
        DataTypeNumber<ToPT> dt_to;
        // constexpr IntType int_max = std::numeric_limits<IntType>::max();
        // constexpr IntType int_min = std::numeric_limits<IntType>::min();

        // Compute valid ranges for integral and fractional parts
        // from_max_integral:    99999999
        // from_large_integral1: 9999999
        // from_large_integral2: 90000000
        // from_large_integral3: 90000001
        constexpr auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        constexpr auto from_large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                                      FromPrecision - FromScale - 1) -
                                              1;
        constexpr auto from_large_integral2 = from_max_integral - from_large_integral1;
        constexpr auto from_large_integral3 =
                from_large_integral2 > 9 ? from_large_integral2 + 1 : from_large_integral2 - 1;
        // constexpr auto min_integral = -from_max_integral;
        std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;
        std::cout << "from_large_integral1:\t" << fmt::format("{}", from_large_integral1)
                  << std::endl;
        std::cout << "from_large_integral2:\t" << fmt::format("{}", from_large_integral2)
                  << std::endl;
        std::cout << "from_large_integral3:\t" << fmt::format("{}", from_large_integral3)
                  << std::endl;

        // from_max_fractional:    99999999
        // from_large_fractional1: 9999999
        // from_large_fractional2: 90000000
        // from_large_fractional3: 90000001
        constexpr auto from_max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale) - 1;
        constexpr auto from_large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale - 1) - 1;
        constexpr auto from_large_fractional2 = from_max_fractional - from_large_fractional1;
        constexpr auto from_large_fractional3 = from_large_fractional2 > 9
                                                        ? from_large_fractional2 + 1
                                                        : from_large_fractional2 - 1;
        std::cout << "from_max_fractional:\t" << fmt::format("{}", from_max_fractional)
                  << std::endl;
        std::cout << "from_large_fractional1:\t" << fmt::format("{}", from_large_fractional1)
                  << std::endl;
        std::cout << "from_large_fractional2:\t" << fmt::format("{}", from_large_fractional2)
                  << std::endl;
        std::cout << "from_large_fractional3:\t" << fmt::format("{}", from_large_fractional3)
                  << std::endl;
        // constexpr auto min_fractional = -from_max_fractional;
        std::vector<IntType> integral_part = {
                0,
                1,
                9,
        };
        // e.g. from Decimal(9, 0) to int16
        if constexpr (FromPrecision - FromScale >= NumberTraits::max_ascii_len<ToT>()) {
            integral_part.push_back(std::numeric_limits<IntType>::max());
            integral_part.push_back(std::numeric_limits<IntType>::max() - 1);
            integral_part.push_back(std::numeric_limits<IntType>::min());
            integral_part.push_back(std::numeric_limits<IntType>::min() + 1);
        } else {
            integral_part.push_back(from_max_integral);
            integral_part.push_back(from_max_integral - 1);
            integral_part.push_back(from_large_integral1);
            integral_part.push_back(from_large_integral2);
            integral_part.push_back(from_large_integral3);
        }
        std::vector<typename FromT::NativeType> fractional_part = {0,
                                                                   1,
                                                                   9,
                                                                   from_max_fractional,
                                                                   from_max_fractional - 1,
                                                                   from_large_fractional1,
                                                                   from_large_fractional2,
                                                                   from_large_fractional3};
        DataTypeDecimal<FromPT> dt(FromPrecision, FromScale);
        DataSet data_set;
        std::string dbg_str = fmt::format("test cast {}({}, {}) to {}: ", type_to_string(FromPT),
                                          FromPrecision, FromScale, dt_to.get_family_name());

        if constexpr (FromScale == 0) {
            // e.g. Decimal(9, 0), only int part
            for (const auto& i : integral_part) {
                auto decimal_num = decimal_ctor(i, 0, FromScale);
                dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), i);
                data_set.push_back({{decimal_num}, IntType(i)});

                if constexpr (FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>()) {
                    decimal_num = decimal_ctor(-i, 0, FromScale);
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), -i);
                    data_set.push_back({{decimal_num}, IntType(-i)});
                }
            }
            dbg_str += "\n";
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
            return;
        } else if constexpr (FromScale == FromPrecision) {
            // e.g. Decimal(9, 9), only fraction part
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(0, f, FromScale);
                dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), 0);
                data_set.push_back({{decimal_num}, IntType(0)});

                decimal_num = decimal_ctor(0, -f, FromScale);
                dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), 0);
                data_set.push_back({{decimal_num}, IntType(0)});
            }
            dbg_str += "\n";
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
            return;
        }

        for (const auto& i : integral_part) {
            for (const auto& f : fractional_part) {
                if constexpr (FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>()) {
                    auto decimal_num = decimal_ctor(i, f, FromScale);
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), i);
                    data_set.push_back({{decimal_num}, IntType(i)});

                    decimal_num = decimal_ctor(-i, -f, FromScale);
                    dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), -i);
                    data_set.push_back({{decimal_num}, IntType(-i)});
                } else {
                    if (i >= 0) {
                        auto decimal_num = decimal_ctor(i, f, FromScale);
                        dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), i);
                        data_set.push_back({{decimal_num}, IntType(i)});
                    } else {
                        auto decimal_num = decimal_ctor(i, -f, FromScale);
                        dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), i);
                        data_set.push_back({{decimal_num}, IntType(i)});
                    }
                }
            }
            dbg_str += "\n";
        }
        std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set);
    }

    template <PrimitiveType FromT, PrimitiveType ToT>
    void from_decimal_to_int_test_func() {
        constexpr auto max_decimal_pre = max_decimal_precision<FromT>();
        constexpr auto min_decimal_pre =
                FromT == TYPE_DECIMAL32
                        ? 1
                        : (FromT == TYPE_DECIMAL64
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (FromT == TYPE_DECIMAL128I
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (FromT == TYPE_DECIMAL256
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(min_decimal_pre == 1 || min_decimal_pre > 9);
        from_decimalv3_no_overflow_test_func<FromT, min_decimal_pre, 0, ToT>();
        if constexpr (min_decimal_pre != 1) {
            from_decimalv3_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre / 2,
                                                 ToT>();
            from_decimalv3_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre - 1,
                                                 ToT>();
        }
        from_decimalv3_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre, ToT>();

        from_decimalv3_no_overflow_test_func<FromT, max_decimal_pre, 0, ToT>();
        from_decimalv3_no_overflow_test_func<FromT, max_decimal_pre, 1, ToT>();
        from_decimalv3_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre / 2, ToT>();
        from_decimalv3_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre - 1, ToT>();
        from_decimalv3_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre, ToT>();
    }

    template <PrimitiveType ToPT>
    void from_date_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        using IntType = ToT;

        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        // int scale = 6;
        // test ordinary date time with microsecond: '2024-12-31 23:59:59.999999'
        std::vector<uint16_t> years = {0,   1,    9,    10,   11,   99,   100,  101,
                                       999, 1000, 1001, 1999, 2000, 2024, 2025, 9999};
        std::vector<uint8_t> months = {1, 9, 10, 11, 12};
        std::vector<uint8_t> days = {1, 2, 9, 10, 11, 28};
        DataTypeDateV2 dt;
        DataSet data_set;
        std::string dbg_str = fmt::format("test cast datev2 to integers: ");
        for (auto year : years) {
            for (auto month : months) {
                for (auto day : days) {
                    DateV2Value<DateV2ValueType> date_val(year, month, day, 0, 0, 0, 0);
                    IntType expect_cast_result = year * 10000 + month * 100 + day;
                    dbg_str += fmt::format("({}, {})|", dt.to_string(date_val.to_date_int_val()),
                                           expect_cast_result);
                    data_set.push_back({{date_val}, expect_cast_result});
                }
            }
            dbg_str += "\n";
        }
        std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set, false);
    }

    template <PrimitiveType ToPT, int Scale>
    void from_datetime_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        using IntType = ToT;

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
                                    IntType expect_cast_result =
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
        check_function_for_cast<DataTypeNumber<ToPT>>(input_types, data_set, false);
    }

    template <PrimitiveType ToPT, bool negative>
    void from_time_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        using IntType = ToT;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        std::vector<int64_t> hours = {0, 1, 10, 100, 838};
        std::vector<int64_t> minutes = {0, 1, 10, 59};
        std::vector<int64_t> seconds = {0, 1, 10, 59};

        std::string dbg_str = fmt::format("test cast time to integers: ");
        DataSet data_set;
        for (auto h : hours) {
            for (auto m : minutes) {
                for (auto s : seconds) {
                    auto time_val = doris::TimeValue::make_time_with_negative(negative, h, m, s);
                    if (time_val > std::numeric_limits<IntType>::max() ||
                        time_val < std::numeric_limits<IntType>::min()) {
                        std::cout << fmt::format(
                                "time value {} is out of range for IntType, skip it\n", time_val);
                        continue;
                    }
                    auto expect_cast_result = static_cast<IntType>(time_val);
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
BNF:
<integer> ::= <whitespace>* <sign>? <decimal_digit>+ <whitespace>*

<sign> ::= "+" | "-"

<decimal_digit> ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

<whitespace> ::= " " | "\t" | "\n" | "\r" | "\f" | "\v"
*/
TEST_F(FunctionCastToIntTest, test_from_string) {
    from_string_test_func<TYPE_TINYINT>();
    from_string_test_func<TYPE_SMALLINT>();
    from_string_test_func<TYPE_INT>();
    from_string_test_func<TYPE_BIGINT>();
    from_string_test_func<TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_bool) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    // tinyint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int8(0)},
                {{UInt8 {1}}, Int8(1)},
        };
        check_function_for_cast<DataTypeInt8>(input_types, data_set);
    }
    // smallint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int16(0)},
                {{UInt8 {1}}, Int16(1)},
        };
        check_function_for_cast<DataTypeInt16>(input_types, data_set);
    }
    // int
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int32(0)},
                {{UInt8 {1}}, Int32(1)},
        };
        check_function_for_cast<DataTypeInt32>(input_types, data_set);
    }
    // bigint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int64(0)},
                {{UInt8 {1}}, Int64(1)},
        };
        check_function_for_cast<DataTypeInt64>(input_types, data_set);
    }
    // largeint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int128(0)},
                {{UInt8 {1}}, Int128(1)},
        };
        check_function_for_cast<DataTypeInt128>(input_types, data_set);
    }
}
TEST_F(FunctionCastToIntTest, test_from_narrow_to_wider_int) {
    narrow_to_wider_int_test_func<TYPE_TINYINT, TYPE_TINYINT>();
    narrow_to_wider_int_test_func<TYPE_TINYINT, TYPE_SMALLINT>();
    narrow_to_wider_int_test_func<TYPE_TINYINT, TYPE_INT>();
    narrow_to_wider_int_test_func<TYPE_TINYINT, TYPE_BIGINT>();
    narrow_to_wider_int_test_func<TYPE_TINYINT, TYPE_LARGEINT>();

    narrow_to_wider_int_test_func<TYPE_SMALLINT, TYPE_SMALLINT>();
    narrow_to_wider_int_test_func<TYPE_SMALLINT, TYPE_INT>();
    narrow_to_wider_int_test_func<TYPE_SMALLINT, TYPE_BIGINT>();
    narrow_to_wider_int_test_func<TYPE_SMALLINT, TYPE_LARGEINT>();

    narrow_to_wider_int_test_func<TYPE_INT, TYPE_INT>();
    narrow_to_wider_int_test_func<TYPE_INT, TYPE_BIGINT>();
    narrow_to_wider_int_test_func<TYPE_INT, TYPE_LARGEINT>();

    narrow_to_wider_int_test_func<TYPE_BIGINT, TYPE_BIGINT>();
    narrow_to_wider_int_test_func<TYPE_BIGINT, TYPE_LARGEINT>();

    narrow_to_wider_int_test_func<TYPE_LARGEINT, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_wider_to_narrow_int) {
    wider_to_narrow_int_test_func<TYPE_SMALLINT, TYPE_TINYINT>();

    wider_to_narrow_int_test_func<TYPE_INT, TYPE_SMALLINT>();
    wider_to_narrow_int_test_func<TYPE_INT, TYPE_TINYINT>();

    wider_to_narrow_int_test_func<TYPE_BIGINT, TYPE_INT>();
    wider_to_narrow_int_test_func<TYPE_BIGINT, TYPE_SMALLINT>();
    wider_to_narrow_int_test_func<TYPE_BIGINT, TYPE_TINYINT>();

    wider_to_narrow_int_test_func<TYPE_LARGEINT, TYPE_BIGINT>();
    wider_to_narrow_int_test_func<TYPE_LARGEINT, TYPE_INT>();
    wider_to_narrow_int_test_func<TYPE_LARGEINT, TYPE_SMALLINT>();
    wider_to_narrow_int_test_func<TYPE_LARGEINT, TYPE_TINYINT>();
}
TEST_F(FunctionCastToIntTest, test_from_float_double) {
    from_float_test_func<TYPE_FLOAT, TYPE_TINYINT>();
    from_float_test_func<TYPE_FLOAT, TYPE_SMALLINT>();
    from_float_test_func<TYPE_FLOAT, TYPE_INT>();
    from_float_test_func<TYPE_FLOAT, TYPE_BIGINT>();
    from_float_test_func<TYPE_FLOAT, TYPE_LARGEINT>();

    from_float_test_func<TYPE_DOUBLE, TYPE_TINYINT>();
    from_float_test_func<TYPE_DOUBLE, TYPE_SMALLINT>();
    from_float_test_func<TYPE_DOUBLE, TYPE_INT>();
    from_float_test_func<TYPE_DOUBLE, TYPE_BIGINT>();
    from_float_test_func<TYPE_DOUBLE, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_decimalv3) {
    from_decimal_to_int_test_func<TYPE_DECIMAL32, TYPE_TINYINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL64, TYPE_TINYINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL128I, TYPE_TINYINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL256, TYPE_TINYINT>();

    from_decimal_to_int_test_func<TYPE_DECIMAL32, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL64, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL128I, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL256, TYPE_SMALLINT>();

    from_decimal_to_int_test_func<TYPE_DECIMAL32, TYPE_INT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL64, TYPE_INT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL128I, TYPE_INT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL256, TYPE_INT>();

    from_decimal_to_int_test_func<TYPE_DECIMAL32, TYPE_BIGINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL64, TYPE_BIGINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL128I, TYPE_BIGINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL256, TYPE_BIGINT>();

    from_decimal_to_int_test_func<TYPE_DECIMAL32, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL64, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL128I, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<TYPE_DECIMAL256, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_date) {
    from_date_test_func<TYPE_INT>();
    from_date_test_func<TYPE_BIGINT>();
    from_date_test_func<TYPE_LARGEINT>();
}

TEST_F(FunctionCastToIntTest, test_from_datetime) {
    from_datetime_test_func<TYPE_BIGINT, 0>();
    from_datetime_test_func<TYPE_BIGINT, 1>();
    from_datetime_test_func<TYPE_BIGINT, 3>();
    from_datetime_test_func<TYPE_BIGINT, 6>();

    from_datetime_test_func<TYPE_LARGEINT, 0>();
    from_datetime_test_func<TYPE_LARGEINT, 1>();
    from_datetime_test_func<TYPE_LARGEINT, 3>();
    from_datetime_test_func<TYPE_LARGEINT, 6>();
}

TEST_F(FunctionCastToIntTest, test_from_time) {
    from_time_test_func<TYPE_TINYINT, false>();
    from_time_test_func<TYPE_TINYINT, true>();
    from_time_test_func<TYPE_SMALLINT, false>();
    from_time_test_func<TYPE_SMALLINT, true>();
    from_time_test_func<TYPE_INT, false>();
    from_time_test_func<TYPE_INT, true>();
    from_time_test_func<TYPE_BIGINT, false>();
    from_time_test_func<TYPE_BIGINT, true>();
    from_time_test_func<TYPE_LARGEINT, false>();
    from_time_test_func<TYPE_LARGEINT, true>();
}
} // namespace doris::vectorized
