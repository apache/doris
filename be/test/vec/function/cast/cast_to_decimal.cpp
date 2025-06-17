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

#include "cast_test.h"
#include "olap/olap_common.h"
#include "runtime/primitive_type.h"
#include "vec/core/types.h"
#include "vec/core/wide_integer.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {
using namespace ut_type;
struct FunctionCastToDecimalTest : public FunctionCastTest {
    const std::vector<std::string> white_spaces = {" ", "\t", "\r", "\n", "\f", "\v"};
    std::string white_spaces_str = " \t\r\n\f\v";

    template <typename DT>
    void format_decimal_number_func(const DT& dt, DataSet& data_set, std::string& dbg_str,
                                    std::string v_str, auto v, bool is_negative, bool with_spaces,
                                    bool with_sign, bool leading_zeros) {
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
            dbg_str += fmt::format("({}, {})|", v_str, dt.to_string(v));
        }
    }
    template <PrimitiveType PT, int Precision, int Scale, int ScientificExpShift = 0>
    void from_string_test_func() {
        using T = typename PrimitiveTypeTraits<PT>::CppType;
        std::cout << fmt::format("===================test cast string to {}({}, {})",
                                 type_to_string(PT), Precision, Scale);
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        auto decimal_ctor = get_decimal_ctor<T>();
        constexpr auto max_integral =
                decimal_scale_multiplier<typename T::NativeType>(Precision - Scale) - 1;
        constexpr auto large_integral1 =
                decimal_scale_multiplier<typename T::NativeType>(Precision - Scale - 1) - 1;
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
        constexpr auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(Scale) - 1;
        constexpr auto large_fractional1 =
                decimal_scale_multiplier<typename T::NativeType>(Scale - 1) - 1;
        constexpr auto large_fractional2 = max_fractional - large_fractional1;
        constexpr auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        // constexpr auto min_fractional = -max_fractional;
        std::set<typename T::NativeType> integral_part = {0,
                                                          1,
                                                          9,
                                                          max_integral,
                                                          max_integral - 1,
                                                          large_integral1,
                                                          large_integral2,
                                                          large_integral3};
        std::set<typename T::NativeType> fractional_part = {0,
                                                            1,
                                                            9,
                                                            max_fractional,
                                                            max_fractional - 1,
                                                            large_fractional1,
                                                            large_fractional2,
                                                            large_fractional3};
        DataTypeDecimal<PT> dt(Precision, Scale);
        auto only_int_part_test_func = [&](bool is_negative, bool with_trailing_dot) {
            std::string dbg_str = fmt::format("test cast string to {}({}, {}), only int part: ",
                                              type_to_string(PT), Precision, Scale);
            DataSet data_set;
            // only integral part
            for (const auto& i : integral_part) {
                std::string v_str;
                if (with_trailing_dot) {
                    v_str = fmt::format("{}.", i);
                } else {
                    v_str = fmt::format("{}", i);
                }
                auto v = decimal_ctor(is_negative ? -i : i, 0, Scale);
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true, true,
                                           true);
                // test leading and trailing spaces and sign
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true, true,
                                           false);
                // test leading and trailing spaces and leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                           false, true);
                // test leading and trailing spaces
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                           false, false);
                // test with sign and leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           true, true);
                // test with sign
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           true, false);
                // test only leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           false, true);
                // test strict digits
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           false, false);
            }
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<PT>, Scale, Precision>(input_types, data_set);
        };

        auto only_fraction_part_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str = fmt::format(
                    "test cast string to {}({}, {}), only fraction part: ", type_to_string(PT),
                    Precision, Scale);
            DataSet data_set;
            // only integral part
            for (auto f : fractional_part) {
                std::string v_str;
                if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                    if (test_rounding) {
                        v_str = fmt::format(".{:0{}}5", f, Scale);
                        ++f;
                    } else {
                        v_str = fmt::format(".{:0{}}4", f, Scale);
                    }
                } else {
                    std::string num_str {wide::to_string(f)};
                    auto len = num_str.length();
                    auto pad_count = Scale - len;
                    if (pad_count > 0) {
                        num_str.insert(0, pad_count, '0');
                    }
                    if (test_rounding) {
                        v_str = fmt::format(".{}5", num_str);
                        ++f;
                    } else {
                        v_str = fmt::format(".{}4", num_str);
                    }
                }
                auto v = decimal_ctor(0, is_negative ? -f : f, Scale);
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true, true,
                                           true);
                // test leading and trailing spaces and sign
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true, true,
                                           false);
                // test leading and trailing spaces and leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                           false, true);
                // test leading and trailing spaces
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                           false, false);
                // test with sign and leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           true, true);
                // test with sign
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           true, false);
                // test only leading zeros
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           false, true);
                // test strict digits
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           false, false);
            }
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<PT>, Scale, Precision>(input_types, data_set);
        };

        if constexpr (Scale == 0) {
            // e.g. Decimal(9, 0), only int part
            only_int_part_test_func(false, false);
            only_int_part_test_func(false, true);
            only_int_part_test_func(true, false);
            only_int_part_test_func(true, true);
            return;
        } else if constexpr (Scale == Precision) {
            // e.g. Decimal(9, 9), only fraction part
            only_fraction_part_test_func(false, false);
            only_fraction_part_test_func(false, true);

            only_fraction_part_test_func(true, false);
            only_fraction_part_test_func(true, true);
            return;
        }

        only_int_part_test_func(false, false);
        only_int_part_test_func(false, true);
        only_int_part_test_func(true, false);
        only_int_part_test_func(true, true);

        only_fraction_part_test_func(false, false);
        only_fraction_part_test_func(false, true);

        only_fraction_part_test_func(true, false);
        only_fraction_part_test_func(true, true);

        auto both_int_and_fraction_part_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str0 =
                    fmt::format("test cast string to {}({}, {}), both int and fraction part: ",
                                type_to_string(PT), Precision, Scale);
            for (const auto& i : integral_part) {
                DataSet data_set;
                std::string dbg_str = dbg_str0;
                for (auto f : fractional_part) {
                    std::string v_str, fraction_str;
                    auto int_str = fmt::format("{}", i);
                    if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{:0{}}5", f, Scale);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{:0{}}4", f, Scale);
                        }
                    } else {
                        std::string num_str {wide::to_string(f)};
                        auto len = num_str.length();
                        auto pad_count = Scale - len;
                        if (pad_count > 0) {
                            num_str.insert(0, pad_count, '0');
                        }
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{}5", num_str);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{}4", num_str);
                        }
                    }
                    int int_part_len = int_str.length();
                    if constexpr (ScientificExpShift > 0) {
                        // e.g., for original decimal number 123456789.123456789, the scientific format is 1.23456789123456789e+8
                        // so the decimal point of the original number need to shift to left by 8 digits
                        if (int_part_len <= ScientificExpShift) {
                            if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                                int_str = fmt::format("{:0{}}", i, ScientificExpShift + 1);
                            } else {
                                auto pad_count = ScientificExpShift - int_part_len + 1;
                                int_str.insert(0, pad_count, '0');
                            }
                            int_part_len = ScientificExpShift + 1;
                        }
                        auto new_int_str = int_str.insert(int_part_len - ScientificExpShift, ".");
                        v_str = fmt::format("{}{}e{}", new_int_str, fraction_str,
                                            ScientificExpShift);
                    } else if constexpr (ScientificExpShift < 0) {
                        int fraction_part_len = fraction_str.length();
                        if (fraction_part_len < -ScientificExpShift) {
                            if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                                fraction_str =
                                        fmt::format("{:0<{}}", fraction_str, -ScientificExpShift);
                            } else {
                                auto pad_count = -ScientificExpShift - fraction_part_len;
                                fraction_str.append(pad_count, '0');
                            }
                        }
                        auto new_fraction_str = fraction_str.insert(-ScientificExpShift, ".");
                        v_str = fmt::format("{}{}e{}", int_str, new_fraction_str,
                                            ScientificExpShift);
                    } else {
                        v_str = fmt::format("{}.{}", int_str, fraction_str);
                    }
                    auto v = decimal_ctor(is_negative ? -i : i, is_negative ? -f : f, Scale);
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                               true, true);
                    // test leading and trailing spaces and sign
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                               true, false);
                    // test leading and trailing spaces and leading zeros
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                               false, true);
                    // test leading and trailing spaces
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                               false, false);
                    // test with sign and leading zeros
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                               true, true);
                    // test with sign
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                               true, false);
                    // test only leading zeros
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                               false, true);
                    // test strict digits
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                               false, false);
                }
                std::cout << dbg_str << std::endl;
                check_function_for_cast<DataTypeDecimal<PT>, Scale, Precision>(input_types,
                                                                               data_set);
            }
        };
        both_int_and_fraction_part_test_func(false, false);
        both_int_and_fraction_part_test_func(false, true);

        both_int_and_fraction_part_test_func(true, false);
        both_int_and_fraction_part_test_func(true, true);
    }

    template <PrimitiveType PT, int Precision, int Scale>
    void from_bool_test_func() {
        using T = typename PrimitiveTypeTraits<PT>::ColumnItemType;
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        auto decimal_ctor = get_decimal_ctor<T>();
        DataSet data_set = {
                {{UInt8 {0}}, decimal_ctor(0, 0, Scale)},
                {{UInt8 {1}}, decimal_ctor(1, 0, Scale)},
        };
        check_function_for_cast<DataTypeDecimal<PT>, Scale, Precision>(input_types, data_set);
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT, int Precision, int Scale>
    void from_int_test_func_() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::ColumnItemType;
        static_assert(std::numeric_limits<FromT>::is_integer, "FromT must be an integer type");
        DataTypeNumber<FromPT> dt_from;
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        std::cout << "test cast from int to Decimal(" << Precision << ", " << Scale << ")\n";

        auto decimal_ctor = get_decimal_ctor<ToT>();
        constexpr auto decimal_max_integral =
                decimal_scale_multiplier<typename ToT::NativeType>(Precision - Scale) - 1;
        constexpr auto decimal_large_integral1 =
                decimal_scale_multiplier<typename ToT::NativeType>(Precision - Scale - 1) - 1;
        constexpr auto decimal_large_integral2 = decimal_max_integral - decimal_large_integral1;
        constexpr auto decimal_large_integral3 = decimal_large_integral2 > 9
                                                         ? decimal_large_integral2 + 1
                                                         : decimal_large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        std::cout << "decimal_max_integral:\t" << fmt::format("{}", decimal_max_integral)
                  << std::endl;
        std::cout << "decimal_large_integral1:\t" << fmt::format("{}", decimal_large_integral1)
                  << std::endl;
        std::cout << "decimal_large_integral2:\t" << fmt::format("{}", decimal_large_integral2)
                  << std::endl;
        std::cout << "decimal_large_integral3:\t" << fmt::format("{}", decimal_large_integral3)
                  << std::endl;

        auto from_max_val = std::numeric_limits<FromT>::max();
        auto from_min_val = std::numeric_limits<FromT>::min();
        FromT from_max_val_minus_1 = from_max_val - FromT {1};
        FromT from_min_val_plus_1 = from_min_val + FromT {1};

        std::set<FromT> integral_part = {0, 1, 9, -1, -9};
        DataSet data_set;
        for (auto i : integral_part) {
            data_set.push_back({{i}, decimal_ctor(i, 0, Scale)});
        }
        if constexpr (NumberTraits::max_ascii_len<FromT>() <= Precision - Scale) {
            data_set.push_back({{from_max_val}, decimal_ctor(from_max_val, 0, Scale)});
            data_set.push_back(
                    {{from_max_val_minus_1}, decimal_ctor(from_max_val_minus_1, 0, Scale)});
            data_set.push_back({{from_min_val}, decimal_ctor(from_min_val, 0, Scale)});
            data_set.push_back(
                    {{from_min_val_plus_1}, decimal_ctor(from_min_val_plus_1, 0, Scale)});
        } else {
            data_set.push_back({{static_cast<FromT>(decimal_max_integral)},
                                decimal_ctor(decimal_max_integral, 0, Scale)});
            data_set.push_back({{static_cast<FromT>(decimal_large_integral1)},
                                decimal_ctor(decimal_large_integral1, 0, Scale)});
            data_set.push_back({{static_cast<FromT>(decimal_large_integral2)},
                                decimal_ctor(decimal_large_integral2, 0, Scale)});
            data_set.push_back({{static_cast<FromT>(decimal_large_integral3)},
                                decimal_ctor(decimal_large_integral3, 0, Scale)});
        }
        check_function_for_cast<DataTypeDecimal<ToPT>, Scale, Precision>(input_types, data_set);
    }
    template <PrimitiveType DecimalType>
    void from_int_test_func() {
        from_int_test_func_<TYPE_TINYINT, DecimalType, 1, 0>();
        {
            constexpr int p = max_decimal_precision<DecimalType>() / 2;
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, p - 1>();
        }
        {
            constexpr int p = max_decimal_precision<DecimalType>();
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_TINYINT, DecimalType, p, p - 1>();
        }

        from_int_test_func_<TYPE_SMALLINT, DecimalType, 1, 0>();
        {
            constexpr int p = max_decimal_precision<DecimalType>() / 2;
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, p - 1>();
        }
        {
            constexpr int p = max_decimal_precision<DecimalType>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType, p, p - 1>();
        }

        from_int_test_func_<TYPE_INT, DecimalType, 1, 0>();
        {
            constexpr int p = max_decimal_precision<DecimalType>() / 2;
            from_int_test_func_<TYPE_INT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_INT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_INT, DecimalType, p, p - 1>();
        }
        {
            constexpr int p = max_decimal_precision<DecimalType>();
            from_int_test_func_<TYPE_INT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_INT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_INT, DecimalType, p, p - 1>();
        }

        from_int_test_func_<TYPE_BIGINT, DecimalType, 1, 0>();
        {
            constexpr int p = max_decimal_precision<DecimalType>() / 2;
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, p - 1>();
        }
        {
            constexpr int p = max_decimal_precision<DecimalType>();
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_BIGINT, DecimalType, p, p - 1>();
        }

        from_int_test_func_<TYPE_LARGEINT, DecimalType, 1, 0>();
        {
            constexpr int p = max_decimal_precision<DecimalType>() / 2;
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, p - 1>();
        }
        {
            constexpr int p = max_decimal_precision<DecimalType>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, 0>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, p / 2>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType, p, p - 1>();
        }
    }

    template <PrimitiveType FromPT, PrimitiveType PT, int Precision, int Scale>
    void from_float_double_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using T = typename PrimitiveTypeTraits<PT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_iec559, "FromT must be a floating point type");
        DataTypeNumber<FromPT> dt_from;
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        std::cout << fmt::format("test cast {} to {}({}, {}), both int and fraction part\n",
                                 type_to_string(FromPT), type_to_string(PT), Precision, Scale);

        constexpr auto max_integral =
                decimal_scale_multiplier<typename T::NativeType>(Precision - Scale) - 1;
        constexpr auto large_integral1 =
                decimal_scale_multiplier<typename T::NativeType>(Precision - Scale - 1) - 1;
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
        constexpr auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(Scale) - 1;
        constexpr auto large_fractional1 =
                decimal_scale_multiplier<typename T::NativeType>(Scale - 1) - 1;
        constexpr auto large_fractional2 = max_fractional - large_fractional1;
        constexpr auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        // constexpr auto min_fractional = -max_fractional;
        std::set<typename T::NativeType> integral_part = {0,
                                                          1,
                                                          9,
                                                          max_integral,
                                                          max_integral - 1,
                                                          large_integral1,
                                                          large_integral2,
                                                          large_integral3};
        std::set<typename T::NativeType> fractional_part = {0,
                                                            1,
                                                            9,
                                                            max_fractional,
                                                            max_fractional - 1,
                                                            large_fractional1,
                                                            large_fractional2,
                                                            large_fractional3};
        DataTypeDecimal<PT> dt_to(Precision, Scale);

        auto multiplier = dt_to.get_scale_multiplier(Scale);
        auto both_int_and_fraction_part_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str0 = fmt::format(
                    "test cast {} to {}({}, {}), both int and fraction part, with "
                    "rounding: {}: ",
                    type_to_string(FromPT), type_to_string(PT), Precision, Scale, test_rounding);
            for (const auto& i : integral_part) {
                DataSet data_set;
                std::string dbg_str = dbg_str0;
                for (auto f : fractional_part) {
                    std::string fraction_str;
                    auto int_str = fmt::format("{}", i);
                    if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{:0{}}5", f, Scale);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{:0{}}4", f, Scale);
                        }
                    } else {
                        std::string num_str {wide::to_string(f)};
                        auto len = num_str.length();
                        if (Scale > len) {
                            num_str.insert(0, Scale - len, '0');
                        }
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{}5", num_str);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{}4", num_str);
                        }
                    }
                    FromT float_value;
                    auto v_str = fmt::format("{}.{}", int_str, fraction_str);
                    char* end {};
                    if constexpr (std::is_same_v<FromT, Float32>) {
                        float_value = std::strtof(v_str.c_str(), &end);
                    } else {
                        float_value = std::strtod(v_str.c_str(), &end);
                    }
                    float_value = is_negative ? -float_value : float_value;
                    T v {};
                    v.value = typename T::NativeType(FromT(float_value * multiplier.value +
                                                           (float_value >= 0 ? 0.5 : -0.5)));
                    data_set.push_back({{float_value}, v});
                    dbg_str += fmt::format("({:f}, {})|", float_value, dt_to.to_string(v));
                }
                std::cout << dbg_str << std::endl;
                check_function_for_cast<DataTypeDecimal<PT>, Scale, Precision>(input_types,
                                                                               data_set);
            }
        };
        both_int_and_fraction_part_test_func(false, false);
        both_int_and_fraction_part_test_func(false, true);

        both_int_and_fraction_part_test_func(true, false);
        both_int_and_fraction_part_test_func(true, true);
    }

    template <PrimitiveType FromPT, int FromPrecision, int FromScale, PrimitiveType ToPT,
              int ToPrecision, int ToScale>
    void between_decimal_with_precision_and_scale_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(IsDecimalNumber<FromT> && IsDecimalNumber<ToT>,
                      "FromT and ToT must be a decimal type");
        std::string dbg_str0 = fmt::format(
                "===============test cast {}({}, {}) to {}({}, {}): ", type_to_string(FromPT),
                FromPrecision, FromScale, type_to_string(ToPT), ToPrecision, ToScale);
        std::cout << dbg_str0 << std::endl;
        static_assert(
                FromPrecision <= max_decimal_precision<FromPT>() && FromScale <= FromPrecision,
                "Wrong from precision or scale");
        static_assert(ToPrecision <= max_decimal_precision<ToPT>() && ToScale <= ToPrecision,
                      "Wrong to precision or scale");
        constexpr auto max_from_int_digit_count = FromPrecision - FromScale;
        constexpr auto max_to_int_digit_count = ToPrecision - ToScale;

        DataTypeDecimal<FromPT> dt_from(FromPrecision, FromScale);
        DataTypeDecimal<ToPT> dt_to(ToPrecision, ToScale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), FromScale, FromPrecision}};
        auto from_decimal_ctor = get_decimal_ctor<FromT>();
        auto to_decimal_ctor = get_decimal_ctor<ToT>();

        constexpr auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        constexpr auto from_large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                                      FromPrecision - FromScale - 1) -
                                              1;
        constexpr auto from_large_integral2 = from_max_integral - from_large_integral1;
        constexpr auto from_large_integral3 =
                from_large_integral2 > 9 ? from_large_integral2 + 1 : from_large_integral2 - 1;
        std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;
        std::cout << "from_large_integral1:\t" << fmt::format("{}", from_large_integral1)
                  << std::endl;
        std::cout << "from_large_integral2:\t" << fmt::format("{}", from_large_integral2)
                  << std::endl;
        std::cout << "from_large_integral3:\t" << fmt::format("{}", from_large_integral3)
                  << std::endl;

        // max_fractional:    99999999
        // large_fractional1: 9999999
        // large_fractional2: 90000000
        // large_fractional3: 90000001
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

        constexpr auto to_max_integral =
                decimal_scale_multiplier<typename ToT::NativeType>(ToPrecision - ToScale) - 1;
        constexpr auto to_large_integral1 =
                decimal_scale_multiplier<typename ToT::NativeType>(ToPrecision - ToScale - 1) - 1;
        constexpr auto to_large_integral2 = to_max_integral - to_large_integral1;
        constexpr auto to_large_integral3 =
                to_large_integral2 > 9 ? to_large_integral2 + 1 : to_large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        std::cout << "to_max_integral:\t" << fmt::format("{}", to_max_integral) << std::endl;
        std::cout << "to_large_integral1:\t" << fmt::format("{}", to_large_integral1) << std::endl;
        std::cout << "to_large_integral2:\t" << fmt::format("{}", to_large_integral2) << std::endl;
        std::cout << "to_large_integral3:\t" << fmt::format("{}", to_large_integral3) << std::endl;

        constexpr auto to_max_fractional =
                decimal_scale_multiplier<typename ToT::NativeType>(ToScale) - 1;
        auto to_max_decimal_num = to_decimal_ctor(to_max_integral, to_max_fractional, ToScale);

        // e.g. for, Decimal(9, 1), 123456789.95 will round with int part carry
        wide::Int256 to_min_fraction_will_round_to_int = 0;
        if constexpr (ToScale > 0) {
            to_min_fraction_will_round_to_int =
                    decimal_scale_multiplier<wide::Int256>(ToScale + 1) - 5;
        }
        std::cout << "to_min_fraction_will_round_to_int:\t"
                  << fmt::format("{}", to_min_fraction_will_round_to_int) << std::endl;
        std::set<typename FromT::NativeType> from_integral_part = {
                0,
                1,
                9,
        };
        std::set<typename FromT::NativeType> from_fractional_part = {0,
                                                                     1,
                                                                     9,
                                                                     from_max_fractional,
                                                                     from_max_fractional - 1,
                                                                     from_large_fractional1,
                                                                     from_large_fractional2,
                                                                     from_large_fractional3};
        constexpr bool may_overflow =
                (max_to_int_digit_count < max_from_int_digit_count) ||
                (max_to_int_digit_count == max_from_int_digit_count && ToScale < FromScale);
        if constexpr (max_to_int_digit_count < max_from_int_digit_count) {
            std::cout << "---------------------integral part may overflow\n";
            // integral part can overflow
            // cases: from 999999.999
            //        to    xxxxx.
            //              xxxxx.xx
            //              xxxxx.xxx
            //              xxxxx.xxxxx
            //
            // cases: from 999999.
            //        to    xxxxx.
            //              xxxxx.xx
            //              xxxxx.xxx
            //              xxxxx.xxxxx
            // e.g. from Decimal(9, 3) to Decimal(8, 3)
            from_integral_part.emplace(to_max_integral);
            from_integral_part.emplace(to_max_integral - 1);
            from_integral_part.emplace(to_large_integral1);
            from_integral_part.emplace(to_large_integral2);
            from_integral_part.emplace(to_large_integral3);
        } else if constexpr (max_to_int_digit_count == max_from_int_digit_count &&
                             ToScale < FromScale) {
            std::cout << "---------------------can overflow after rounding\n";
            // can overflow after rounding:
            // cases: from 999999.999
            //        to   xxxxxx.
            //             xxxxxx.xx
            // e.g.: from Decimal(9, 3) to Decimal(8, 2)
            from_integral_part.emplace(from_max_integral);
            from_integral_part.emplace(from_max_integral - 1);
            from_integral_part.emplace(from_large_integral1);
            from_integral_part.emplace(from_large_integral2);
            from_integral_part.emplace(from_large_integral3);
        } else {
            std::cout << "---------------------won't overflow\n";
            // won't overflow
            // cases: 999999.999
            //        xxxxxx.xxx
            //       xxxxxxx.
            //       xxxxxxx.xx
            //       xxxxxxx.xxx
            //       xxxxxxx.xxxx
            from_integral_part.emplace(from_max_integral);
            from_integral_part.emplace(from_max_integral - 1);
            from_integral_part.emplace(from_large_integral1);
            from_integral_part.emplace(from_large_integral2);
            from_integral_part.emplace(from_large_integral3);
        }
        for (const auto& i : from_integral_part) {
            std::string dbg_str = dbg_str0;
            DataSet data_set;
            typename ToT::NativeType to_int = i;
            for (const auto& f : from_fractional_part) {
                FromT from_decimal_num {};
                typename ToT::NativeType to_frac = f;
                if constexpr (FromScale == 0) {
                    from_decimal_num = from_decimal_ctor(i, 0, FromScale);
                    to_frac = 0;
                } else {
                    from_decimal_num = from_decimal_ctor(i, f, FromScale);
                }
                if constexpr (may_overflow) {
                    if constexpr (ToScale >= FromScale) {
                        // no round
                        if constexpr (ToScale != FromScale) {
                            to_frac = to_frac * decimal_scale_multiplier<typename ToT::NativeType>(
                                                        ToScale - FromScale);
                        }
                        auto to_decimal_num = to_decimal_ctor(to_int, to_frac, ToScale);
                        dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                                               dt_to.to_string(to_decimal_num));
                        data_set.push_back({{from_decimal_num}, to_decimal_num});
                    } else {
                        // need handle round
                        auto scale_multiplier =
                                decimal_scale_multiplier<typename FromT::NativeType>(FromScale -
                                                                                     ToScale);
                        to_frac = f / scale_multiplier;
                        auto remaining = f % scale_multiplier;
                        if (remaining >= scale_multiplier / 2) {
                            to_frac += 1;
                        }
                        auto to_decimal_num = to_decimal_ctor(to_int, to_frac, ToScale);
                        if (to_decimal_num > to_max_decimal_num) {
                            std::cout << fmt::format("{} cast {} will overlfow, skip\n", dbg_str0,
                                                     dt_from.to_string(from_decimal_num));
                            continue;
                        }
                        dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                                               dt_to.to_string(to_decimal_num));
                        data_set.push_back({{from_decimal_num}, to_decimal_num});
                    }
                } else {
                    // won't overflow
                    if constexpr (ToScale > FromScale) {
                        to_frac = to_frac * decimal_scale_multiplier<typename ToT::NativeType>(
                                                    ToScale - FromScale);
                    } else if constexpr (ToScale < FromScale) {
                        // need handle round
                        auto scale_multiplier =
                                decimal_scale_multiplier<typename FromT::NativeType>(FromScale -
                                                                                     ToScale);
                        to_frac = f / scale_multiplier;
                        auto remaining = f % scale_multiplier;
                        if (remaining >= scale_multiplier / 2) {
                            to_frac += 1;
                        }
                    } /* else {
                        if constexpr (FromScale != 0) {
                            to_frac = f;
                        }
                    }*/
                    auto to_decimal_num = to_decimal_ctor(to_int, to_frac, ToScale);
                    dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                                           dt_to.to_string(to_decimal_num));
                    data_set.push_back({{from_decimal_num}, to_decimal_num});
                }
                if constexpr (FromScale == 0) {
                    break;
                }
            }
            std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<ToPT>, ToScale, ToPrecision>(input_types,
                                                                                 data_set);
        }
    }
    // test all case with a target Decimal type with fixed precision and scale
    template <PrimitiveType FromT, PrimitiveType ToT, int ToPrecision, int ToScale>
    void between_decimals_with_to_p_and_s_test_func() {
        constexpr auto from_max_decimal_p = max_decimal_precision<FromT>();
        constexpr auto from_min_decimal_p =
                FromT == TYPE_DECIMAL32
                        ? 1
                        : (FromT == TYPE_DECIMAL64
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (FromT == TYPE_DECIMAL128I
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (FromT == TYPE_DECIMAL256
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(from_min_decimal_p == 1 || from_min_decimal_p > 9);
        auto aaa = [&]<int test_from_precision>() {
            between_decimal_with_precision_and_scale_test_func<FromT, test_from_precision, 0, ToT,
                                                               ToPrecision, ToScale>();
            between_decimal_with_precision_and_scale_test_func<FromT, test_from_precision, 1, ToT,
                                                               ToPrecision, ToScale>();
            if constexpr (test_from_precision != 1) {
                between_decimal_with_precision_and_scale_test_func<FromT, test_from_precision,
                                                                   test_from_precision / 2, ToT,
                                                                   ToPrecision, ToScale>();
                between_decimal_with_precision_and_scale_test_func<FromT, test_from_precision,
                                                                   test_from_precision - 1, ToT,
                                                                   ToPrecision, ToScale>();
                between_decimal_with_precision_and_scale_test_func<FromT, test_from_precision,
                                                                   test_from_precision - 1, ToT,
                                                                   ToPrecision, ToScale>();
            }
        };
        aaa.template operator()<from_min_decimal_p>();
        aaa.template operator()<from_max_decimal_p / 2>();
        aaa.template operator()<from_max_decimal_p - 1>();
        aaa.template operator()<from_max_decimal_p>();
    }
    // test all cases casting to a fixed target Decimal, with all possible target precisions and scales
    template <PrimitiveType FromT, PrimitiveType ToT>
    void between_decimal_test_func() {
        constexpr auto to_max_decimal_p = max_decimal_precision<ToT>();
        constexpr auto to_min_decimal_p =
                ToT == TYPE_DECIMAL32
                        ? 1
                        : (ToT == TYPE_DECIMAL64
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (ToT == TYPE_DECIMAL128I
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (ToT == TYPE_DECIMAL256
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(to_min_decimal_p == 1 || to_min_decimal_p > 9);
        auto aaa = [&]<int test_to_precision>() {
            between_decimals_with_to_p_and_s_test_func<FromT, ToT, test_to_precision, 0>();
            between_decimals_with_to_p_and_s_test_func<FromT, ToT, test_to_precision, 1>();
            if constexpr (test_to_precision != 1) {
                between_decimals_with_to_p_and_s_test_func<FromT, ToT, test_to_precision,
                                                           test_to_precision / 2>();
                between_decimals_with_to_p_and_s_test_func<FromT, ToT, test_to_precision,
                                                           test_to_precision - 1>();
                between_decimals_with_to_p_and_s_test_func<FromT, ToT, test_to_precision,
                                                           test_to_precision>();
            }
        };
        aaa.template operator()<to_min_decimal_p>();
        aaa.template operator()<to_max_decimal_p / 2>();
        aaa.template operator()<to_max_decimal_p - 1>();
        aaa.template operator()<to_max_decimal_p>();
    }

    // test all cases casting to a fixed target Decimal type
    template <PrimitiveType ToT>
    void from_any_decimals_to_this_decimal_test_func() {
        between_decimal_test_func<TYPE_DECIMAL32, ToT>();
        between_decimal_test_func<TYPE_DECIMAL64, ToT>();
        between_decimal_test_func<TYPE_DECIMAL128I, ToT>();
        between_decimal_test_func<TYPE_DECIMAL256, ToT>();
    }
};

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
*/
TEST_F(FunctionCastToDecimalTest, test_from_string) {
    from_string_test_func<TYPE_DECIMAL32, 9, 0>();
    from_string_test_func<TYPE_DECIMAL32, 9, 0, 3>();
    // from_string_test_func<Decimal32, 9, 0, -3>();

    from_string_test_func<TYPE_DECIMAL32, 9, 1>();
    from_string_test_func<TYPE_DECIMAL32, 9, 3>();
    from_string_test_func<TYPE_DECIMAL32, 9, 3, 3>();
    // from_string_test_func<Decimal32, 9, 3, -3>();
    from_string_test_func<TYPE_DECIMAL32, 9, 8>();
    from_string_test_func<TYPE_DECIMAL32, 9, 9>();

    from_string_test_func<TYPE_DECIMAL64, 18, 0>();
    from_string_test_func<TYPE_DECIMAL64, 18, 1>();
    from_string_test_func<TYPE_DECIMAL64, 18, 1, 9>();
    // from_string_test_func<Decimal64, 18, 1, -9>();
    // from_string_test_func<Decimal64, 18, 1, -18>();
    from_string_test_func<TYPE_DECIMAL64, 18, 9>();
    from_string_test_func<TYPE_DECIMAL64, 18, 17>();
    from_string_test_func<TYPE_DECIMAL64, 18, 18>();

    from_string_test_func<TYPE_DECIMAL128I, 38, 0>();
    from_string_test_func<TYPE_DECIMAL128I, 38, 1>();
    from_string_test_func<TYPE_DECIMAL128I, 38, 19>();
    from_string_test_func<TYPE_DECIMAL128I, 38, 37>();
    from_string_test_func<TYPE_DECIMAL128I, 38, 38>();

    from_string_test_func<TYPE_DECIMAL256, 76, 0>();
    from_string_test_func<TYPE_DECIMAL256, 76, 1>();
    from_string_test_func<TYPE_DECIMAL256, 76, 38>();
    from_string_test_func<TYPE_DECIMAL256, 76, 75>();
    from_string_test_func<TYPE_DECIMAL256, 76, 76>();
}
TEST_F(FunctionCastToDecimalTest, test_from_bool) {
    from_bool_test_func<TYPE_DECIMAL32, 9, 0>();
    from_bool_test_func<TYPE_DECIMAL32, 9, 1>();
    from_bool_test_func<TYPE_DECIMAL32, 9, 3>();
    from_bool_test_func<TYPE_DECIMAL32, 9, 8>();

    from_bool_test_func<TYPE_DECIMAL64, 18, 0>();
    from_bool_test_func<TYPE_DECIMAL64, 18, 1>();
    from_bool_test_func<TYPE_DECIMAL64, 18, 9>();
    from_bool_test_func<TYPE_DECIMAL64, 18, 17>();

    from_bool_test_func<TYPE_DECIMALV2, 27, 9>();
    // from_bool_test_func<Decimal128V2, 27, 1>();
    // from_bool_test_func<Decimal128V2, 27, 13>();
    // from_bool_test_func<Decimal128V2, 27, 26>();

    from_bool_test_func<TYPE_DECIMAL128I, 38, 0>();
    from_bool_test_func<TYPE_DECIMAL128I, 38, 1>();
    from_bool_test_func<TYPE_DECIMAL128I, 38, 19>();
    from_bool_test_func<TYPE_DECIMAL128I, 38, 37>();

    from_bool_test_func<TYPE_DECIMAL256, 76, 0>();
    from_bool_test_func<TYPE_DECIMAL256, 76, 1>();
    from_bool_test_func<TYPE_DECIMAL256, 76, 38>();
    from_bool_test_func<TYPE_DECIMAL256, 76, 75>();
}

TEST_F(FunctionCastToDecimalTest, test_from_int) {
    from_int_test_func<TYPE_DECIMAL32>();
    from_int_test_func<TYPE_DECIMAL64>();
    from_int_test_func<TYPE_DECIMALV2>();
    from_int_test_func<TYPE_DECIMAL128I>();
    from_int_test_func<TYPE_DECIMAL256>();
}
/*
MySQL 8.0:
mysql> create table test_float(f1 float, f2 float);
Query OK, 0 rows affected (0.10 sec)

mysql> insert into test_float values(900001.125, 900001.152);
Query OK, 1 row affected (0.02 sec)

mysql> select f1, f2, f1 = f2 from test_float;
+--------+--------+---------+
| f1     | f2     | f1 = f2 |
+--------+--------+---------+
| 900001 | 900001 |       1 |
+--------+--------+---------+
1 row in set (0.00 sec)
*/
TEST_F(FunctionCastToDecimalTest, test_from_float_double) {
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL32, 9, 0>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL32, 9, 3>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL32, 9, 8>();

    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL64, 18, 0>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL64, 18, 9>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL64, 18, 17>();

    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL128I, 38, 0>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL128I, 38, 19>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL128I, 38, 37>();

    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL256, 76, 0>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL256, 76, 38>();
    from_float_double_test_func<TYPE_FLOAT, TYPE_DECIMAL256, 76, 75>();

    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL32, 9, 0>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL32, 9, 3>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL32, 9, 8>();

    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL64, 18, 0>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL64, 18, 9>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL64, 18, 17>();

    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL128I, 38, 0>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL128I, 38, 19>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL128I, 38, 37>();

    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL256, 76, 0>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL256, 76, 38>();
    from_float_double_test_func<TYPE_DOUBLE, TYPE_DECIMAL256, 76, 75>();
}
TEST_F(FunctionCastToDecimalTest, test_between_decimal_types) {
    from_any_decimals_to_this_decimal_test_func<TYPE_DECIMAL32>();
    from_any_decimals_to_this_decimal_test_func<TYPE_DECIMAL64>();
    from_any_decimals_to_this_decimal_test_func<TYPE_DECIMAL128I>();
    from_any_decimals_to_this_decimal_test_func<TYPE_DECIMAL256>();
}
} // namespace doris::vectorized