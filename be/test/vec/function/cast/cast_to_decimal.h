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

#include <fstream>
#include <memory>
#include <string>

#include "cast_test.h"
#include "common/exception.h"
#include "testutil/test_util.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"

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
            /*
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
            */
            // data_set.push_back({{white_spaces_str + v_str}, v});
            // data_set.push_back({{v_str + white_spaces_str}, v});
            data_set.push_back({{white_spaces_str + v_str + white_spaces_str}, v});
        } else {
            data_set.push_back({{v_str}, v});
            // dbg_str += fmt::format("({}, {})|", v_str, dt.to_string(v));
        }
    }

    template <typename T>
    void from_string_test_func(int precision, int scale, int table_index, int& test_data_index,
                               int ScientificExpShift = 0, bool ForceZeroExp = false) {
        auto dbg_str0 = fmt::format(
                "test cast string to {}({}, {}), ScientificExpShift: {}, ForceZeroExp: {}",
                type_to_string(T::PType), precision, scale, ScientificExpShift, ForceZeroExp);
        // std::cout << fmt::format("\n\n==================={}\n", dbg_str0);
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        auto decimal_ctor = get_decimal_ctor<T>();
        auto max_integral = decimal_scale_multiplier<typename T::NativeType>(precision - scale) - 1;
        auto large_integral1 =
                decimal_scale_multiplier<typename T::NativeType>(precision - scale - 1) - 1;
        auto large_integral2 = max_integral - large_integral1;
        auto large_integral3 = large_integral2 > 9 ? large_integral2 + 1 : large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;
        // std::cout << "large_integral1:\t" << fmt::format("{}", large_integral1) << std::endl;
        // std::cout << "large_integral2:\t" << fmt::format("{}", large_integral2) << std::endl;
        // std::cout << "large_integral3:\t" << fmt::format("{}", large_integral3) << std::endl;

        // max_fractional:    99999999
        // large_fractional1: 9999999
        // large_fractional2: 90000000
        // large_fractional3: 90000001
        auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(scale) - 1;
        auto large_fractional1 = decimal_scale_multiplier<typename T::NativeType>(scale - 1) - 1;
        auto large_fractional2 = max_fractional - large_fractional1;
        auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        // std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        // std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        // std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        // constexpr auto min_fractional = -max_fractional;
        std::set<typename T::NativeType> integral_part = {0};
        integral_part.emplace(max_integral);
        if (max_integral > 0) {
            integral_part.emplace(1);
            integral_part.emplace(9);
            integral_part.emplace(max_integral - 1);
            integral_part.emplace(large_integral1);
            integral_part.emplace(large_integral2);
            integral_part.emplace(large_integral3);
        }
        std::set<typename T::NativeType> fractional_part = {0, max_fractional};
        if (max_fractional > 0) {
            fractional_part.emplace(1);
            fractional_part.emplace(9);
            fractional_part.emplace(max_fractional - 1);
            fractional_part.emplace(large_fractional1);
            fractional_part.emplace(large_fractional2);
            fractional_part.emplace(large_fractional3);
        }
        DataTypeDecimal<T::PType> dt(precision, scale);

        std::vector<std::pair<std::string, std::string>> regression_test_data_set;
        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string regression_case_name =
                        fmt::format("test_cast_to_{}_{}_{}_from_str",
                                    to_lower(type_to_string(T::PType)), precision, scale);
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_decimal/from_str");
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();
                (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
                if constexpr (IsDecimal256<T>) {
                    (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                    (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
                }

                std::string from_sql_type_name = "string";
                std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
                gen_normal_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        regression_test_data_set, table_index++, test_data_index, ofs_case,
                        ofs_expected_result, ofs_const_case, ofs_const_expected_result);

                (*ofs_const_case) << "}";
                (*ofs_case) << "}";
            }
        }};
        auto add_regression_cases = [&](const std::string& v_str,
                                        const std::string& expected_result, bool is_negative) {
            regression_test_data_set.emplace_back((is_negative ? "-" : "") + v_str,
                                                  expected_result);
        };
        // Edge cases
        {
            DataSet data_set;
            std::string dbg_str;
            auto v = decimal_ctor(0, 0, scale);
            std::string v_str = fmt::format("0.{:0{}}e{}", 0, 100, std::numeric_limits<int>::max());
            format_decimal_number_func(dt, data_set, dbg_str, v_str, v, false, false, false, false);
            format_decimal_number_func(dt, data_set, dbg_str, v_str, v, true, false, false, false);
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set, scale,
                                                                     precision);
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set, scale,
                                                                      precision);
            if (FLAGS_gen_regression_case) {
                auto expected_result = dt.to_string(v);
                add_regression_cases(v_str, expected_result, false);
                add_regression_cases(v_str, expected_result, true);
            }
        }
        auto only_int_part_test_func = [&](bool is_negative, bool with_trailing_dot) {
            std::string dbg_str =
                    fmt::format("{}, only int part, test negative: {}, with trailing dot: {}",
                                dbg_str0, is_negative, with_trailing_dot);
            DataSet data_set;
            // only integral part
            for (const auto& i : integral_part) {
                std::string v_str;
                if (with_trailing_dot) {
                    v_str = fmt::format("{}.", i);
                } else {
                    v_str = fmt::format("{}", i);
                }
                auto v = decimal_ctor(is_negative ? -i : i, 0, scale);
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
                if (FLAGS_gen_regression_case) {
                    auto expected_result = dt.to_string(v);
                    add_regression_cases(v_str, expected_result, is_negative);
                }
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set, scale,
                                                                     precision);
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set, scale,
                                                                      precision);
        };
        auto only_int_part_round_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str =
                    fmt::format("{}, only int part, test negative: {}, test round: ", dbg_str0,
                                is_negative, test_rounding);
            DataSet data_set;
            // only integral part
            for (auto i : integral_part) {
                std::string v_str;
                if (i != max_integral && test_rounding) {
                    v_str = fmt::format("{}.5", i);
                    ++i;
                } else {
                    v_str = fmt::format("{}.49999", i);
                }
                auto v = decimal_ctor(is_negative ? -i : i, 0, scale);
                format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                           false, false);
                if (FLAGS_gen_regression_case) {
                    auto expected_result = dt.to_string(v);
                    add_regression_cases(v_str, expected_result, is_negative);
                }
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set, scale,
                                                                     precision);
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set, scale,
                                                                      precision);
        };

        auto only_fraction_part_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str =
                    fmt::format("{}, only fraction part, test negative: {}, test round: {}",
                                dbg_str0, is_negative, test_rounding);
            DataSet data_set;
            for (auto f : fractional_part) {
                std::string v_str;
                if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                    if (f != max_fractional && test_rounding) {
                        v_str = fmt::format(".{:0{}}5", f, scale);
                        ++f;
                    } else {
                        v_str = fmt::format(".{:0{}}4", f, scale);
                    }
                } else {
                    std::string num_str {wide::to_string(f)};
                    auto len = num_str.length();
                    auto pad_count = scale - len;
                    if (pad_count > 0) {
                        num_str.insert(0, pad_count, '0');
                    }
                    if (f != max_fractional && test_rounding) {
                        v_str = fmt::format(".{}5", num_str);
                        ++f;
                    } else {
                        v_str = fmt::format(".{}4", num_str);
                    }
                }
                auto v = decimal_ctor(0, is_negative ? -f : f, scale);
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
                if (FLAGS_gen_regression_case) {
                    auto expected_result = dt.to_string(v);
                    add_regression_cases(v_str, expected_result, is_negative);
                }
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set, scale,
                                                                     precision);
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set, scale,
                                                                      precision);
        };

        if (scale == 0) {
            // e.g. Decimal(9, 0), only int part
            only_int_part_test_func(false, false);
            only_int_part_test_func(false, true);
            only_int_part_test_func(true, false);
            only_int_part_test_func(true, true);

            only_int_part_round_test_func(false, false);
            only_int_part_round_test_func(false, true);
            only_int_part_round_test_func(true, false);
            only_int_part_round_test_func(true, true);
            return;
        } else if (scale == precision) {
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
            std::string dbg_str1 =
                    fmt::format("{}, both int and fraction part, test negative: {}, test round: {}",
                                dbg_str0, is_negative, test_rounding);
            // std::cout << dbg_str1 << std::endl;
            for (const auto& i : integral_part) {
                DataSet data_set;
                std::string dbg_str = dbg_str1;
                for (auto f : fractional_part) {
                    std::string v_str, fraction_str;
                    auto int_str = fmt::format("{}", i);
                    if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{:0{}}5", f, scale);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{:0{}}4", f, scale);
                        }
                    } else {
                        std::string num_str {wide::to_string(f)};
                        auto len = num_str.length();
                        auto pad_count = scale - len;
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
                    if (ScientificExpShift > 0) {
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
                    } else if (ScientificExpShift < 0) {
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
                        if (ForceZeroExp) {
                            v_str = fmt::format("{}.{}e0", int_str, fraction_str);
                        } else {
                            v_str = fmt::format("{}.{}", int_str, fraction_str);
                        }
                    }
                    auto v = decimal_ctor(is_negative ? -i : i, is_negative ? -f : f, scale);
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                                               true, true);
                    // test leading and trailing spaces and sign
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                    //                            true, false);
                    // test leading and trailing spaces and leading zeros
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                    //                            false, true);
                    // test leading and trailing spaces
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, true,
                    //                            false, false);
                    // test with sign and leading zeros
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                    //                            true, true);
                    // test with sign
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                    //                            true, false);
                    // test only leading zeros
                    // format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                    //                            false, true);
                    // test strict digits
                    format_decimal_number_func(dt, data_set, dbg_str, v_str, v, is_negative, false,
                                               false, false);
                    if (FLAGS_gen_regression_case) {
                        auto expected_result = dt.to_string(v);
                        add_regression_cases(v_str, expected_result, is_negative);
                    }
                }
                check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set,
                                                                         scale, precision);
                check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set,
                                                                          scale, precision);
            }
        };
        both_int_and_fraction_part_test_func(false, false);
        both_int_and_fraction_part_test_func(false, true);

        both_int_and_fraction_part_test_func(true, false);
        both_int_and_fraction_part_test_func(true, true);
    }

    void from_string_to_decimal32_test_func();
    void from_string_to_decimal64_test_func();
    void from_string_to_decimal128v3_test_func();
    void from_string_to_decimal256_test_func();

    template <typename T>
    void from_string_overflow_test_func(int precision, int scale, std::ofstream* ofs_const_case,
                                        std::ofstream* ofs_const_expected_result,
                                        std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
                                        const std::string& regression_case_name, int table_index,
                                        int& test_data_index) {
        auto table_name =
                fmt::format("{}_{}_{}_{}", regression_case_name, table_index, precision, scale);

        auto dbg_str0 = fmt::format("test cast string to {}({}, {}) overflow\n",
                                    type_to_string(T::PType), precision, scale);
        // std::cout << fmt::format("==================={}\n", dbg_str0);
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        auto max_integral = decimal_scale_multiplier<typename T::NativeType>(precision - scale) - 1;
        auto min_overflow_int_part = max_integral + 1;

        static auto max_supported_decimal =
                decimal_scale_multiplier<wide::Int256>(BeConsts::MAX_DECIMAL256_PRECISION) - 1;
        static auto max_supported_int = std::numeric_limits<wide::Int256>::max();
        static auto min_supported_int = std::numeric_limits<wide::Int256>::min();
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;

        // max_fractional:    99999999
        auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(scale) - 1;
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;

        std::set<std::string> overflow_strs;
        overflow_strs.emplace(fmt::format("{}", min_overflow_int_part));
        overflow_strs.emplace(fmt::format("-{}", min_overflow_int_part));
        overflow_strs.emplace(
                fmt::format("{}", std::numeric_limits<typename T::NativeType>::max()));
        overflow_strs.emplace(
                fmt::format("{}", std::numeric_limits<typename T::NativeType>::min()));
        if (max_integral > 0) {
            overflow_strs.emplace(fmt::format("{}0", max_integral));
            overflow_strs.emplace(fmt::format("-{}0", max_integral));
        }
        overflow_strs.emplace(fmt::format("{}1", max_integral));
        overflow_strs.emplace(fmt::format("-{}1", max_integral));
        overflow_strs.emplace(fmt::format("{}9", max_integral));
        overflow_strs.emplace(fmt::format("-{}9", max_integral));
        overflow_strs.emplace(fmt::format("1{}", max_integral));
        overflow_strs.emplace(fmt::format("-1{}", max_integral));
        if (precision - scale < BeConsts::MAX_DECIMAL256_PRECISION) {
            overflow_strs.emplace(fmt::format("{}", max_supported_decimal));
            overflow_strs.emplace(fmt::format("-{}", max_supported_decimal));
        }
        overflow_strs.emplace(fmt::format("{}", max_supported_decimal + 1));
        overflow_strs.emplace(fmt::format("-{}", max_supported_decimal + 1));
        overflow_strs.emplace(fmt::format("{}", max_supported_int));
        overflow_strs.emplace(fmt::format("{}", min_supported_int));

        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                std::vector<std::string> regression_case_data_set(overflow_strs.begin(),
                                                                  overflow_strs.end());
                std::string from_sql_type_name = "string";
                std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
                gen_overflow_and_invalid_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        regression_case_data_set, table_index, ofs_case, ofs_expected_result,
                        ofs_const_case, ofs_const_expected_result);
            }
        }};

        auto test_func_no_round_int_overflow = [&](bool strict_cast) {
            for (const auto& v_str : overflow_strs) {
                DataSet data_set;
                std::visit(
                        [&](auto enable_strict_cast) {
                            if constexpr (enable_strict_cast) {
                                data_set.push_back({{v_str}, T {}});
                                check_function_for_cast<DataTypeDecimal<T::PType>,
                                                        enable_strict_cast>(
                                        input_types, data_set, scale, precision, true, true);
                            } else {
                                data_set.push_back({{v_str}, Null()});
                                check_function_for_cast<DataTypeDecimal<T::PType>,
                                                        enable_strict_cast>(input_types, data_set,
                                                                            scale, precision);
                            }
                        },
                        vectorized::make_bool_variant(strict_cast));
            }
        };

        test_func_no_round_int_overflow(true);
        test_func_no_round_int_overflow(false);

        auto test_func_overflow_after_round = [&](bool is_negative, bool strict_cast) {
            DataSet data_set;
            std::string v_str;
            if (scale == 0) {
                if (is_negative) {
                    v_str = fmt::format("-{}.5", max_integral);
                } else {
                    v_str = fmt::format("{}.5", max_integral);
                }
            } else if (scale == precision) {
                if (is_negative) {
                    v_str = fmt::format("-.{}5", max_fractional);
                } else {
                    v_str = fmt::format(".{}5", max_fractional);
                }
            } else {
                if (is_negative) {
                    v_str = fmt::format("-{}.{}5", max_integral, max_fractional);
                } else {
                    v_str = fmt::format("{}.{}5", max_integral, max_fractional);
                }
            }
            std::visit(
                    [&](auto enable_strict_cast) {
                        if constexpr (enable_strict_cast) {
                            data_set.push_back({{v_str}, T {}});
                            check_function_for_cast<DataTypeDecimal<T::PType>, enable_strict_cast>(
                                    input_types, data_set, scale, precision, true, true);
                        } else {
                            if (FLAGS_gen_regression_case) {
                                overflow_strs.emplace(v_str);
                            }
                            data_set.push_back({{v_str}, Null()});
                            check_function_for_cast<DataTypeDecimal<T::PType>, enable_strict_cast>(
                                    input_types, data_set, scale, precision);
                        }
                    },
                    vectorized::make_bool_variant(strict_cast));
        };
        test_func_overflow_after_round(false, true);
        test_func_overflow_after_round(false, false);

        test_func_overflow_after_round(true, true);
        test_func_overflow_after_round(true, false);
    }

    void from_string_to_decimal32_overflow_test_func();
    void from_string_to_decimal64_overflow_test_func();
    void from_string_to_decimal128v3_overflow_test_func();
    void from_string_to_decimal256_overflow_test_func();

    template <typename T>
    void from_string_invalid_input_test_func(int precision, int scale, int table_index) {
        // PG error msg: invalid input syntax for type double precision: "++123.456"
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        std::vector<std::string> invalid_inputs = {
                "",
                ".",
                " ",
                "\t",
                "\n",
                "\r",
                "\f",
                "\v",
                "abc",
                "abc123"
                "1abc12"
                "123abc",
                // Space between digits
                "1 23",
                "1\t23",
                "1\n23",
                "1\r23",
                "1\v23",
                "1\f23",
                // Multiple decimal points
                "1.2.3",
                // invalid leading and trailing characters
                "a123.456",
                " a123.456",
                "\ta123.456",
                "\na123.456",
                "\ra123.456",
                "\fa123.456",
                "\va123.456",
                "123.456a",
                "123.456a\t",
                "123.456a\n",
                "123.456a\r",
                "123.456a\f",
                "123.456a\v",
                "123.456\ta",
                "123.456\na",
                "123.456\ra",
                "123.456\fa",
                "123.456\va",
                // invalid char between numbers
                "12a3.456",
                "123a.456",
                "123.a456",
                "123.4a56",
                // multiple positive/negative signs
                "++123"
                "--123"
                "+-123"
                "-+123"
                "+-123.456",
                "+- 123.456", // sign with following spaces
                "-+123.456",
                "++123.456",
                "--123.456",
                "+-.456",
                "-+.456",
                "++.456",
                "--.456",
                // hexadecimal
                "0x123",
                "0x123.456",
                // invalid scientific notation
                "e",
                "-e",
                "+e",
                "e+",
                "e-",
                "e1",
                "e+1",
                "e-1",
                ".e",
                "+.e",
                "-.e",
                ".e+",
                ".e-",
                ".e+",
                "1e",
                "1e+",
                "1e-",
                "1e1a",
                "1ea1",
                "1e1.1",
                "1e+1.1",
                "1e-1.1",
                // Multiple exponents
                "1e2e3",
        };
        // non-strict mode
        {
            DataSet data_set;
            for (const auto& input : invalid_inputs) {
                data_set.push_back({{input}, Null()});
            }
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set, scale,
                                                                      precision);
        }

        // strict mode
        for (const auto& input : invalid_inputs) {
            DataSet data_set;
            data_set.push_back({{input}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set, scale,
                                                                     precision, true, true);
        }

        if (FLAGS_gen_regression_case) {
            std::vector<std::string> regression_invalid_inputs = {
                    "",
                    ".",
                    " ",
                    "\t",
                    "abc",
                    // Space between digits
                    "1 23",
                    "1\t23",
                    // Multiple decimal points
                    "1.2.3",
                    // invalid leading and trailing characters
                    "a123.456",
                    " a123.456",
                    "\ta123.456",
                    "123.456a",
                    "123.456a\t",
                    "123.456\ta",
                    "123.456\na",
                    // invalid char between numbers
                    "12a3.456",
                    "123a.456",
                    "123.a456",
                    "123.4a56",
                    // multiple positive/negative signs
                    "+-123.456",
                    "+- 123.456", // sign with following spaces
                    "-+123.456",
                    "++123.456",
                    "--123.456",
                    "+-.456",
                    "-+.456",
                    "++.456",
                    "--.456",
                    // hexadecimal
                    "0x123",
                    "0x123.456",
                    // invalid scientific notation
                    "e",
                    "-e",
                    "+e",
                    "e+",
                    "e-",
                    "e1",
                    "e+1",
                    "e-1",
                    ".e",
                    "+.e",
                    "-.e",
                    ".e+",
                    ".e-",
                    ".e+",
                    "1e",
                    "1e+",
                    "1e-",
                    "1e1a",
                    "1ea1",
                    "1e1.1",
                    "1e+1.1",
                    "1e-1.1",
                    // Multiple exponents
                    "1e2e3",
            };

            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string regression_case_name =
                    fmt::format("test_cast_to_decimal_{}_{}_from_str_invalid_input_{}", precision,
                                scale, table_index);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_decimal/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
            gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name, true,
                                                     to_sql_type_name, regression_invalid_inputs,
                                                     table_index, ofs_case, ofs_expected_result,
                                                     ofs_const_case, ofs_const_expected_result);

            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <typename T>
    void from_bool_test_func(int precision, int scale) {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        auto decimal_ctor = get_decimal_ctor<T>();
        DataSet data_set = {
                {{UInt8 {0}}, decimal_ctor(0, 0, scale)},
                {{UInt8 {1}}, decimal_ctor(1, 0, scale)},
        };
        check_function_for_cast<DataTypeDecimal<T::PType>>(input_types, data_set, scale, precision);
    }

    template <typename T>
    void from_bool_overflow_test_func() {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        constexpr auto max_decimal_p = max_decimal_precision<T::PType>();
        auto decimal_ctor = get_decimal_ctor<T>();
        // non strict mode
        {
            DataSet data_set = {
                    {{UInt8 {0}}, decimal_ctor(0, 0, max_decimal_p)},
                    {{UInt8 {1}}, Null()},
            };
            check_function_for_cast<DataTypeDecimal<T::PType>, false>(input_types, data_set,
                                                                      max_decimal_p, max_decimal_p);
        }
        // strict mode
        {
            DataSet data_set = {
                    {{UInt8 {0}}, decimal_ctor(0, 0, max_decimal_p)},
            };
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set,
                                                                     max_decimal_p, max_decimal_p);
        }
        {
            DataSet data_set = {
                    {{UInt8 {1}}, Null {}},
            };
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(
                    input_types, data_set, max_decimal_p, max_decimal_p, true, true);
        }
    }

    template <PrimitiveType FromPT, typename ToT>
    void from_int_test_func_(int precision, int scale, std::string regression_case_name,
                             int table_index, int& test_data_index, std::ofstream* ofs_case,
                             std::ofstream* ofs_expected_result, std::ofstream* ofs_const_case,
                             std::ofstream* ofs_const_expected_result) {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_integer, "FromT must be an integer type");
        DataTypeNumber<FromPT> dt_from;
        DataTypeDecimal<ToT::PType> dt_to = get_decimal_data_type<ToT>(precision, scale);
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        // std::cout << "test cast from int to Decimal(" << precision << ", " << scale << ")\n";

        auto decimal_ctor = get_decimal_ctor<ToT>();
        auto decimal_max_integral =
                decimal_scale_multiplier<typename ToT::NativeType>(precision - scale) - 1;
        auto decimal_large_integral1 =
                decimal_scale_multiplier<typename ToT::NativeType>(precision - scale - 1) - 1;
        auto decimal_large_integral2 = decimal_max_integral - decimal_large_integral1;
        auto decimal_large_integral3 = decimal_large_integral2 > 9 ? decimal_large_integral2 + 1
                                                                   : decimal_large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        // std::cout << "decimal_max_integral:\t" << fmt::format("{}", decimal_max_integral)
        //           << std::endl;
        // std::cout << "decimal_large_integral1:\t" << fmt::format("{}", decimal_large_integral1)
        //           << std::endl;
        // std::cout << "decimal_large_integral2:\t" << fmt::format("{}", decimal_large_integral2)
        //           << std::endl;
        // std::cout << "decimal_large_integral3:\t" << fmt::format("{}", decimal_large_integral3)
        //           << std::endl;

        auto from_max_val = std::numeric_limits<FromT>::max();
        auto from_min_val = std::numeric_limits<FromT>::min();
        FromT from_max_val_minus_1 = from_max_val - FromT {1};
        FromT from_min_val_plus_1 = from_min_val + FromT {1};

        std::set<FromT> integral_part = {0, 1, 9, -1, -9};
        if (NumberTraits::max_ascii_len<FromT>() <= precision - scale) {
            integral_part.emplace(from_max_val);
            integral_part.emplace(from_max_val_minus_1);
            integral_part.emplace(from_min_val);
            integral_part.emplace(from_min_val_plus_1);
        } else {
            integral_part.emplace(static_cast<FromT>(decimal_max_integral));
            integral_part.emplace(static_cast<FromT>(-decimal_max_integral));
            if (decimal_max_integral > 0) {
                integral_part.emplace(static_cast<FromT>(decimal_large_integral1));
                integral_part.emplace(static_cast<FromT>(-decimal_large_integral1));
                integral_part.emplace(static_cast<FromT>(decimal_large_integral2));
                integral_part.emplace(static_cast<FromT>(-decimal_large_integral2));
                integral_part.emplace(static_cast<FromT>(decimal_large_integral3));
                integral_part.emplace(static_cast<FromT>(-decimal_large_integral3));
            }
        }
        DataSet data_set;

        std::vector<std::pair<std::string, std::string>> regression_test_data_set;
        for (auto i : integral_part) {
            auto v = decimal_ctor(i, 0, scale);
            data_set.push_back({{i}, v});

            if (FLAGS_gen_regression_case) {
                auto expected_result = dt_to.to_string(v);
                regression_test_data_set.emplace_back(fmt::format("{}", i), expected_result);
            }
        }
        check_function_for_cast<DataTypeDecimal<ToT::PType>, false>(input_types, data_set, scale,
                                                                    precision);
        check_function_for_cast<DataTypeDecimal<ToT::PType>, true>(input_types, data_set, scale,
                                                                   precision);
        if (FLAGS_gen_regression_case) {
            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result);
        }
    }
    template <typename DecimalType>
    void from_int_test_func() {
        int table_index = 0;
        int test_data_index = 0;

        std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
        std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
        std::string regression_case_name = fmt::format(
                "test_cast_to_{}_from_int", to_lower(type_to_string(DecimalType::PType)));
        if (FLAGS_gen_regression_case) {
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_decimal/from_int");
        }
        auto* ofs_const_case = ofs_const_case_uptr.get();
        auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
        auto* ofs_case = ofs_case_uptr.get();
        auto* ofs_expected_result = ofs_expected_result_uptr.get();
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            if constexpr (IsDecimal256<DecimalType>) {
                (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
            }
        }
        from_int_test_func_<TYPE_TINYINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_test_func_<TYPE_INT, DecimalType>(1, 0, regression_case_name, table_index++,
                                                   test_data_index, ofs_case, ofs_expected_result,
                                                   ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_INT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_test_func_<TYPE_BIGINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FromPT, typename ToT>
    void from_int_overflow_test_func_(int precision, int scale, std::string regression_case_name,
                                      int table_index, int& test_data_index,
                                      std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
                                      std::ofstream* ofs_const_case,
                                      std::ofstream* ofs_const_expected_result) {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_integer, "FromT must be an integer type");
        if (NumberTraits::max_ascii_len<FromT>() > precision - scale) {
            DataTypeNumber<FromPT> dt_from;
            DataTypeDecimal<ToT::PType> dt_to(precision, scale);
            InputTypeSet input_types = {dt_from.get_primitive_type()};
            // std::cout << fmt::format("test cast from {} to {}({}, {}) overflow\n",
            //                          type_to_string(FromPT), type_to_string(ToT::PType), precision,
            //                          scale);

            auto decimal_max_integral =
                    decimal_scale_multiplier<typename ToT::NativeType>(precision - scale) - 1;
            // constexpr auto min_integral = -max_integral;
            // std::cout << "decimal_max_integral:\t" << fmt::format("{}", decimal_max_integral)
            //           << std::endl;

            constexpr auto from_max_val = std::numeric_limits<FromT>::max();
            constexpr auto from_min_val = std::numeric_limits<FromT>::min();

            std::set<FromT> test_input_vals;
            test_input_vals.emplace(decimal_max_integral + 1);
            test_input_vals.emplace(-decimal_max_integral - 1);
            test_input_vals.emplace(from_max_val);
            test_input_vals.emplace(from_min_val);
            // non strict mode
            {
                DataSet data_set;
                for (auto i : test_input_vals) {
                    data_set.push_back({{i}, Null()});
                }
                check_function_for_cast<DataTypeDecimal<ToT::PType>, false>(input_types, data_set,
                                                                            scale, precision);
            }
            // strict mode
            {
                for (auto i : test_input_vals) {
                    DataSet data_set;
                    data_set.push_back({{i}, Null()});
                    check_function_for_cast<DataTypeDecimal<ToT::PType>, true>(
                            input_types, data_set, scale, precision, true, true);
                }
            }
            if (FLAGS_gen_regression_case) {
                std::string from_sql_type_name = get_sql_type_name(FromPT);
                std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
                std::vector<std::string> regression_test_data_set;
                for (auto i : test_input_vals) {
                    regression_test_data_set.emplace_back(fmt::format("{}", i));
                }
                gen_overflow_and_invalid_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        regression_test_data_set, table_index++, ofs_case, ofs_expected_result,
                        ofs_const_case, ofs_const_expected_result);
            }
        }
    }
    template <typename DecimalType>
    void from_int_overflow_test_func() {
        int table_index = 0;
        int test_data_index = 0;

        std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
        std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
        std::string regression_case_name = fmt::format(
                "test_cast_to_{}_from_int_overflow", to_lower(type_to_string(DecimalType::PType)));
        if (FLAGS_gen_regression_case) {
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_decimal/from_int");
        }
        auto* ofs_const_case = ofs_const_case_uptr.get();
        auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
        auto* ofs_case = ofs_case_uptr.get();
        auto* ofs_expected_result = ofs_expected_result_uptr.get();
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            if constexpr (IsDecimal256<DecimalType>) {
                (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
            }
        }
        from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_TINYINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_SMALLINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_INT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_BIGINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }

        from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                1, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>() / 2;
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        {
            constexpr int p = max_decimal_precision<DecimalType::PType>();
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, 0, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p / 2, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_int_overflow_test_func_<TYPE_LARGEINT, DecimalType>(
                    p, p - 1, regression_case_name, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }
    template <PrimitiveType FromPT, typename T>
    void from_float_double_test_func(int precision, int scale, int table_index,
                                     int& test_data_index, std::ofstream* ofs_case,
                                     std::ofstream* ofs_expected_result,
                                     std::ofstream* ofs_const_case,
                                     std::ofstream* ofs_const_expected_result) {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_iec559, "FromT must be a floating point type");
        DataTypeNumber<FromPT> dt_from;
        DataTypeDecimal<T::PType> dt_to(precision, scale);
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        // std::cout << fmt::format("test cast {} to {}({}, {}), both int and fraction part\n",
        //                          type_to_string(FromPT), type_to_string(T::PType), precision,
        //                          scale);

        auto max_integral = decimal_scale_multiplier<typename T::NativeType>(precision - scale) - 1;
        auto large_integral1 =
                decimal_scale_multiplier<typename T::NativeType>(precision - scale - 1) - 1;
        auto large_integral2 = max_integral - large_integral1;
        auto large_integral3 = large_integral2 > 9 ? large_integral2 + 1 : large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;
        // std::cout << "large_integral1:\t" << fmt::format("{}", large_integral1) << std::endl;
        // std::cout << "large_integral2:\t" << fmt::format("{}", large_integral2) << std::endl;
        // std::cout << "large_integral3:\t" << fmt::format("{}", large_integral3) << std::endl;

        // max_fractional:    99999999
        // large_fractional1: 9999999
        // large_fractional2: 90000000
        // large_fractional3: 90000001
        auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(scale) - 1;
        auto large_fractional1 = decimal_scale_multiplier<typename T::NativeType>(scale - 1) - 1;
        auto large_fractional2 = max_fractional - large_fractional1;
        auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        // std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        // std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        // std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        // constexpr auto min_fractional = -max_fractional;
        std::set<typename T::NativeType> integral_part = {0, max_integral};
        if (max_integral > 0) {
            integral_part.emplace(1);
            integral_part.emplace(9);
            integral_part.emplace(max_integral - 1);
            integral_part.emplace(large_integral1);
            integral_part.emplace(large_integral2);
            integral_part.emplace(large_integral3);
        }
        std::set<typename T::NativeType> fractional_part = {0, max_fractional};
        if (max_fractional > 0) {
            fractional_part.emplace(1);
            fractional_part.emplace(9);
            fractional_part.emplace(max_fractional - 1);
            fractional_part.emplace(large_fractional1);
            fractional_part.emplace(large_fractional2);
            fractional_part.emplace(large_fractional3);
        }
        auto max_result = dt_to.get_max_digits_number(precision);
        auto min_result = -max_result;

        auto multiplier = dt_to.get_scale_multiplier(scale);

        std::vector<std::string> const_test_strs;
        std::vector<std::string> const_test_expected_results;
        std::vector<std::pair<std::string, std::string>> regression_test_data_set;
        int start_data_index = test_data_index;

        auto both_int_and_fraction_part_test_func = [&](bool is_negative, bool test_rounding) {
            std::string dbg_str0 = fmt::format(
                    "test cast {} to {}({}, {}), both int and fraction part, is negative: {}, with "
                    "rounding: {}: ",
                    type_to_string(FromPT), type_to_string(T::PType), precision, scale, is_negative,
                    test_rounding);
            // std::cout << dbg_str0 << std::endl;
            for (const auto& i : integral_part) {
                DataSet data_set;
                for (auto f : fractional_part) {
                    std::string fraction_str;
                    auto int_str = fmt::format("{}", i);
                    if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{:0{}}9", f, scale);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{:0{}}3", f, scale);
                        }
                    } else {
                        std::string num_str {wide::to_string(f)};
                        auto len = num_str.length();
                        if (scale > len) {
                            num_str.insert(0, scale - len, '0');
                        }
                        if (i != max_integral && test_rounding) {
                            fraction_str = fmt::format("{}9", num_str);
                            ++f;
                        } else {
                            fraction_str = fmt::format("{}3", num_str);
                        }
                    }
                    FromT float_value;
                    auto v_str =
                            fmt::format("{}{}.{}", is_negative ? "-" : "", int_str, fraction_str);
                    char* end {};
                    if constexpr (std::is_same_v<FromT, Float32>) {
                        float_value = std::strtof(v_str.c_str(), &end);
                    } else {
                        float_value = std::strtod(v_str.c_str(), &end);
                    }
                    // float_value = is_negative ? -float_value : float_value;
                    using DoubleType = std::conditional_t<IsDecimal256<T>, long double, double>;
                    DoubleType expect_value = float_value * DoubleType(multiplier);
                    if (expect_value <= DoubleType(min_result) ||
                        expect_value >= DoubleType(max_result)) {
                        // std::cerr << fmt::format("{:f} overflow\n", expect_value);
                    } else {
                        T v {};
                        // v.value = typename T::NativeType(FromT(float_value * multiplier +
                        //                                        (float_value >= 0 ? 0.5 : -0.5)));
                        v.value = typename T::NativeType(static_cast<double>(
                                float_value * static_cast<DoubleType>(multiplier) +
                                ((float_value >= 0) ? 0.5 : -0.5)));
                        data_set.push_back({{float_value}, v});
                        // dbg_str += fmt::format("({:f}, {})|", float_value, dt_to.to_string(v));

                        auto expected_result = dt_to.to_string(v);
                        int significant_digit_count = (FromPT == TYPE_FLOAT ? 7 : 15);
                        if (v_str.length() <= significant_digit_count) {
                            const_test_strs.emplace_back(v_str);
                            const_test_expected_results.emplace_back(expected_result);
                        }

                        regression_test_data_set.emplace_back(v_str, expected_result);
                        ++test_data_index;
                    }
                }
                check_function_for_cast<DataTypeDecimal<T::PType>>(input_types, data_set, scale,
                                                                   precision);
            }
        };
        both_int_and_fraction_part_test_func(false, false);
        both_int_and_fraction_part_test_func(false, true);

        both_int_and_fraction_part_test_func(true, false);
        both_int_and_fraction_part_test_func(true, true);

        if (FLAGS_gen_regression_case) {
            auto const_test_with_strict_arg = [&](bool enable_strict_cast) {
                (*ofs_const_case) << fmt::format("    sql \"set enable_strict_cast={};\"\n",
                                                 enable_strict_cast);
                auto value_count = const_test_strs.size();
                int data_index = start_data_index;
                for (int i = 0; i != value_count; ++i, ++data_index) {
                    auto groovy_var_name = fmt::format("const_sql_{}", data_index);
                    auto const_sql = fmt::format(
                            R"("""select cast(cast("{}" as {}) as decimalv3({}, {}));""")",
                            const_test_strs[i], FromPT == TYPE_FLOAT ? "float" : "double",
                            precision, scale);
                    if (enable_strict_cast) {
                        (*ofs_const_case)
                                << fmt::format("    def {} = {}\n", groovy_var_name, const_sql);
                    }
                    (*ofs_const_case) << fmt::format("    qt_sql_{}_{} \"${{{}}}\"\n", data_index,
                                                     enable_strict_cast ? "strict" : "non_strict",
                                                     groovy_var_name);
                    (*ofs_const_case)
                            << fmt::format("    testFoldConst(\"${{{}}}\")\n", groovy_var_name);

                    (*ofs_const_expected_result)
                            << fmt::format("-- !sql_{}_{} --\n", data_index,
                                           enable_strict_cast ? "strict" : "non_strict");
                    (*ofs_const_expected_result)
                            << fmt::format("{}\n\n", const_test_expected_results[i]);
                }
            };
            const_test_with_strict_arg(true);
            const_test_with_strict_arg(false);

            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
            std::string regression_case_name = fmt::format("test_cast_to_decimal_{}_{}_from_{}",
                                                           precision, scale, from_sql_type_name);
            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result, false);
        }
    }

    template <PrimitiveType FromPT, typename T>
    void from_float_double_overflow_test_func(int precision, int scale, int table_index,
                                              int& test_data_index, std::ofstream* ofs_case,
                                              std::ofstream* ofs_expected_result,
                                              std::ofstream* ofs_const_case,
                                              std::ofstream* ofs_const_expected_result) {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_iec559, "FromT must be a floating point type");
        DataTypeNumber<FromPT> dt_from;
        DataTypeDecimal<T::PType> dt_to(precision, scale);
        InputTypeSet input_types = {dt_from.get_primitive_type()};
        // std::cout << fmt::format("test cast {} to {}({}, {}), both int and fraction part\n",
        //                          type_to_string(FromPT), type_to_string(T::PType), precision,
        //                          scale);

        auto max_integral = decimal_scale_multiplier<typename T::NativeType>(precision - scale) - 1;
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;

        // max_fractional:    99999999
        auto max_fractional = decimal_scale_multiplier<typename T::NativeType>(scale) - 1;
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        std::set<typename T::NativeType> integral_part = {max_integral + max_integral / 10};
        std::set<typename T::NativeType> fractional_part = {max_fractional};

        std::vector<std::string> test_input_vals;
        auto both_int_and_fraction_part_test_func = [&]() {
            for (const auto& i : integral_part) {
                DataSet data_set;
                for (auto f : fractional_part) {
                    std::string fraction_str;
                    auto int_str = fmt::format("{}", i);
                    if (0 == f) {
                        f = 9;
                    }
                    if constexpr (!std::is_same_v<typename T::NativeType, wide::Int256>) {
                        fraction_str = fmt::format("{:0{}}9", f, scale);
                    } else {
                        std::string num_str {wide::to_string(f)};
                        fraction_str = fmt::format("{}9", num_str);
                    }
                    FromT float_value;
                    auto v_str = fmt::format("{}.{}", int_str, fraction_str);
                    char* end {};
                    if constexpr (std::is_same_v<FromT, Float32>) {
                        float_value = std::strtof(v_str.c_str(), &end);
                    } else {
                        float_value = std::strtod(v_str.c_str(), &end);
                    }
                    // strict mode
                    {
                        DataSet data_set_tmp;
                        data_set_tmp.push_back({{float_value}, Null()});
                        check_function_for_cast<DataTypeDecimal<T::PType>, true>(
                                input_types, data_set_tmp, scale, precision, true, true);
                    }
                    {
                        DataSet data_set_tmp;
                        data_set_tmp.push_back({{-float_value}, Null()});
                        check_function_for_cast<DataTypeDecimal<T::PType>, true>(
                                input_types, data_set_tmp, scale, precision, true, true);
                    }
                    data_set.push_back({{float_value}, Null()});
                    data_set.push_back({{-float_value}, Null()});
                    test_input_vals.push_back(fmt::format("{}", float_value));
                    test_input_vals.push_back(fmt::format("{}", -float_value));
                }
                check_function_for_cast<DataTypeDecimal<T::PType>>(input_types, data_set, scale,
                                                                   precision);
            }
        };
        both_int_and_fraction_part_test_func();

        // test +-infinify
        test_input_vals.emplace_back("inf");
        test_input_vals.emplace_back("-inf");
        {
            DataSet data_set_tmp;
            data_set_tmp.push_back({{std::numeric_limits<FromT>::infinity()}, Null()});
            data_set_tmp.push_back({{-std::numeric_limits<FromT>::infinity()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>>(input_types, data_set_tmp, scale,
                                                               precision);
        }
        {
            DataSet data_set_tmp;
            data_set_tmp.push_back({{std::numeric_limits<FromT>::infinity()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set_tmp,
                                                                     scale, precision, true, true);

            data_set_tmp.clear();
            data_set_tmp.push_back({{-std::numeric_limits<FromT>::infinity()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set_tmp,
                                                                     scale, precision, true, true);
        }
        // test +-nan
        test_input_vals.emplace_back("nan");
        test_input_vals.emplace_back("-nan");
        {
            DataSet data_set_tmp;
            data_set_tmp.push_back({{std::numeric_limits<FromT>::quiet_NaN()}, Null()});
            data_set_tmp.push_back({{-std::numeric_limits<FromT>::quiet_NaN()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>>(input_types, data_set_tmp, scale,
                                                               precision);
        }
        {
            DataSet data_set_tmp;
            data_set_tmp.push_back({{std::numeric_limits<FromT>::quiet_NaN()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set_tmp,
                                                                     scale, precision, true, true);

            data_set_tmp.clear();
            data_set_tmp.push_back({{-std::numeric_limits<FromT>::quiet_NaN()}, Null()});
            check_function_for_cast<DataTypeDecimal<T::PType>, true>(input_types, data_set_tmp,
                                                                     scale, precision, true, true);
        }
        if (FLAGS_gen_regression_case) {
            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = fmt::format("decimalv3({}, {})", precision, scale);
            std::string regression_case_name =
                    fmt::format("test_cast_to_decimal_{}_{}_from_{}_invalid", precision, scale,
                                from_sql_type_name);
            gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name, true,
                                                     to_sql_type_name, test_input_vals,
                                                     table_index++, ofs_case, ofs_expected_result,
                                                     ofs_const_case, ofs_const_expected_result);
        }
    }

    template <typename FromT, typename ToT>
    void between_decimal_with_precision_and_scale_test_func(
            int from_precision, int from_scale, int to_precision, int to_scale, int table_index,
            int& test_data_index, std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
            std::ofstream* ofs_const_case, std::ofstream* ofs_const_expected_result) {
        static_assert(IsDecimalNumber<FromT> && IsDecimalNumber<ToT>,
                      "FromT and ToT must be a decimal type");
        std::string dbg_str0 = fmt::format(
                "===============test cast {}({}, {}) to {}({}, {}): ", type_to_string(FromT::PType),
                from_precision, from_scale, type_to_string(ToT::PType), to_precision, to_scale);
        // std::cout << dbg_str0 << std::endl;
        // static_assert(from_precision <= max_decimal_precision<FromT::PType>() &&
        //                       from_scale <= from_precision,
        //               "Wrong from precision or scale");
        // static_assert(to_precision <= max_decimal_precision<ToT::PType>() && to_scale <= to_precision,
        //               "Wrong to precision or scale");
        auto max_from_int_digit_count = from_precision - from_scale;
        auto max_to_int_digit_count = to_precision - to_scale;

        DataTypeDecimal<FromT::PType> dt_from =
                get_decimal_data_type<FromT>(from_precision, from_scale);
        DataTypeDecimal<ToT::PType> dt_to(to_precision, to_scale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), from_scale, from_precision}};
        auto from_decimal_ctor = get_decimal_ctor<FromT>();
        auto to_decimal_ctor = get_decimal_ctor<ToT>();

        auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(from_precision - from_scale) -
                1;
        auto from_large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                            from_precision - from_scale - 1) -
                                    1;
        auto from_large_integral2 = from_max_integral - from_large_integral1;
        auto from_large_integral3 =
                from_large_integral2 > 9 ? from_large_integral2 + 1 : from_large_integral2 - 1;
        // std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;
        // std::cout << "from_large_integral1:\t" << fmt::format("{}", from_large_integral1)
        //           << std::endl;
        // std::cout << "from_large_integral2:\t" << fmt::format("{}", from_large_integral2)
        //           << std::endl;
        // std::cout << "from_large_integral3:\t" << fmt::format("{}", from_large_integral3)
        //           << std::endl;

        // max_fractional:    99999999
        // large_fractional1: 9999999
        // large_fractional2: 90000000
        // large_fractional3: 90000001
        auto from_max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(from_scale) - 1;
        auto from_large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(from_scale - 1) - 1;
        auto from_large_fractional2 = from_max_fractional - from_large_fractional1;
        auto from_large_fractional3 = from_large_fractional2 > 9 ? from_large_fractional2 + 1
                                                                 : from_large_fractional2 - 1;
        // std::cout << "from_max_fractional:\t" << fmt::format("{}", from_max_fractional)
        //           << std::endl;
        // std::cout << "from_large_fractional1:\t" << fmt::format("{}", from_large_fractional1)
        //           << std::endl;
        // std::cout << "from_large_fractional2:\t" << fmt::format("{}", from_large_fractional2)
        //           << std::endl;
        // std::cout << "from_large_fractional3:\t" << fmt::format("{}", from_large_fractional3)
        //           << std::endl;

        auto to_max_integral =
                decimal_scale_multiplier<typename ToT::NativeType>(to_precision - to_scale) - 1;
        auto to_large_integral1 =
                decimal_scale_multiplier<typename ToT::NativeType>(to_precision - to_scale - 1) - 1;
        auto to_large_integral2 = to_max_integral - to_large_integral1;
        auto to_large_integral3 =
                to_large_integral2 > 9 ? to_large_integral2 + 1 : to_large_integral2 - 1;
        // constexpr auto min_integral = -max_integral;
        // std::cout << "to_max_integral:\t" << fmt::format("{}", to_max_integral) << std::endl;
        // std::cout << "to_large_integral1:\t" << fmt::format("{}", to_large_integral1) << std::endl;
        // std::cout << "to_large_integral2:\t" << fmt::format("{}", to_large_integral2) << std::endl;
        // std::cout << "to_large_integral3:\t" << fmt::format("{}", to_large_integral3) << std::endl;

        auto to_max_fractional = decimal_scale_multiplier<typename ToT::NativeType>(to_scale) - 1;
        auto to_max_decimal_num = to_decimal_ctor(to_max_integral, to_max_fractional, to_scale);

        // e.g. for, Decimal(9, 1), 123456789.95 will round with int part carry
        wide::Int256 to_min_fraction_will_round_to_int = 0;
        if (to_scale > 0) {
            to_min_fraction_will_round_to_int =
                    decimal_scale_multiplier<wide::Int256>(to_scale + 1) - 5;
        }
        // std::cout << "to_min_fraction_will_round_to_int:\t"
        //           << fmt::format("{}", to_min_fraction_will_round_to_int) << std::endl;
        std::set<typename FromT::NativeType> from_integral_part = {0};
        std::set<typename FromT::NativeType> from_fractional_part = {0};
        typename FromT::NativeType from_fractional_part_multiplier = 1;
        if constexpr (IsDecimalV2<FromT>) {
            from_fractional_part_multiplier =
                    decimal_scale_multiplier<typename FromT::NativeType>(9 - from_scale);
        }
        from_fractional_part.emplace(from_max_fractional * from_fractional_part_multiplier);
        if (from_max_fractional > 0) {
            from_fractional_part.emplace(1 * from_fractional_part_multiplier);
            from_fractional_part.emplace(9 * from_fractional_part_multiplier);
            from_fractional_part.emplace((from_max_fractional - 1) *
                                         from_fractional_part_multiplier);
            from_fractional_part.emplace(from_large_fractional1 * from_fractional_part_multiplier);
            from_fractional_part.emplace(from_large_fractional2 * from_fractional_part_multiplier);
            from_fractional_part.emplace(from_large_fractional3 * from_fractional_part_multiplier);
        }
        bool may_overflow =
                (max_to_int_digit_count < max_from_int_digit_count) ||
                (max_to_int_digit_count == max_from_int_digit_count && to_scale < from_scale);
        if (max_to_int_digit_count < max_from_int_digit_count) {
            // std::cout << "---------------------integral part may overflow\n";
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
            if (to_max_integral > 0) {
                from_integral_part.emplace(to_max_integral - 1);
                from_integral_part.emplace(to_large_integral1);
                from_integral_part.emplace(to_large_integral2);
                from_integral_part.emplace(to_large_integral3);
            }
        } else if (max_to_int_digit_count == max_from_int_digit_count && to_scale < from_scale) {
            // std::cout << "---------------------can overflow after rounding\n";
            // can overflow after rounding:
            // cases: from 999999.999
            //        to   xxxxxx.
            //             xxxxxx.xx
            // e.g.: from Decimal(9, 3) to Decimal(8, 2)
            from_integral_part.emplace(from_max_integral);
            if (from_max_integral > 0) {
                from_integral_part.emplace(from_max_integral - 1);
                from_integral_part.emplace(from_large_integral1);
                from_integral_part.emplace(from_large_integral2);
                from_integral_part.emplace(from_large_integral3);
            }
        } else {
            // std::cout << "---------------------won't overflow\n";
            // won't overflow
            // cases: 999999.999
            //        xxxxxx.xxx
            //       xxxxxxx.
            //       xxxxxxx.xx
            //       xxxxxxx.xxx
            //       xxxxxxx.xxxx
            from_integral_part.emplace(from_max_integral);
            if (from_max_integral > 0) {
                from_integral_part.emplace(from_max_integral - 1);
                from_integral_part.emplace(from_large_integral1);
                from_integral_part.emplace(from_large_integral2);
                from_integral_part.emplace(from_large_integral3);
            }
        }

        std::vector<std::pair<std::string, std::string>> regression_test_data_set;
        int from_calc_scale = IsDecimalV2<FromT> ? 9 : from_scale;
        for (const auto& i : from_integral_part) {
            std::string dbg_str = dbg_str0;
            DataSet data_set;
            FromT from_decimal_num {};
            ToT to_decimal_num {};
            for (const auto& f : from_fractional_part) {
                typename ToT::NativeType to_int = i;
                typename ToT::NativeType to_frac = f;
                if (from_scale == 0) {
                    from_decimal_num = from_decimal_ctor(i, 0, from_scale);
                    to_frac = 0;
                } else {
                    from_decimal_num = from_decimal_ctor(i, f, from_scale);
                }
                if (may_overflow) {
                    if (to_scale >= from_calc_scale) {
                        // no round
                        if (to_scale != from_calc_scale) {
                            to_frac = to_frac * decimal_scale_multiplier<typename ToT::NativeType>(
                                                        to_scale - from_calc_scale);
                        }
                        to_decimal_num = to_decimal_ctor(to_int, to_frac, to_scale);
                        // dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                        //                        dt_to.to_string(to_decimal_num));
                        data_set.push_back({{from_decimal_num}, to_decimal_num});
                    } else {
                        // need handle round
                        auto scale_multiplier =
                                decimal_scale_multiplier<typename FromT::NativeType>(
                                        from_calc_scale - to_scale);
                        to_frac = f / scale_multiplier;
                        auto remaining = f % scale_multiplier;
                        if (remaining >= scale_multiplier / 2) {
                            to_frac += 1;
                            if (to_frac > to_max_fractional) {
                                to_frac = 0;
                                to_int += 1;
                            }
                        }
                        to_decimal_num = to_decimal_ctor(to_int, to_frac, to_scale);
                        if (to_decimal_num > to_max_decimal_num) {
                            // std::cout << fmt::format("{} cast {} will overlfow, skip\n", dbg_str0,
                            //                          dt_from.to_string(from_decimal_num));
                            continue;
                        }
                        // dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                        //                        dt_to.to_string(to_decimal_num));
                        data_set.push_back({{from_decimal_num}, to_decimal_num});
                    }
                } else {
                    // won't overflow
                    if (to_scale > from_calc_scale) {
                        to_frac = to_frac * decimal_scale_multiplier<typename ToT::NativeType>(
                                                    to_scale - from_calc_scale);
                    } else if (to_scale < from_calc_scale) {
                        // need handle round
                        auto scale_multiplier =
                                decimal_scale_multiplier<typename FromT::NativeType>(
                                        from_calc_scale - to_scale);
                        to_frac = f / scale_multiplier;
                        auto remaining = f % scale_multiplier;
                        if (remaining >= scale_multiplier / 2) {
                            to_frac += 1;
                            if (to_frac > to_max_fractional) {
                                to_frac = 0;
                                to_int += 1;
                            }
                        }
                    }
                    to_decimal_num = to_decimal_ctor(to_int, to_frac, to_scale);
                    // dbg_str += fmt::format("({}, {})|", dt_from.to_string(from_decimal_num),
                    //                        dt_to.to_string(to_decimal_num));
                    data_set.push_back({{from_decimal_num}, to_decimal_num});
                }
                if (FLAGS_gen_regression_case) {
                    auto from_value = dt_from.to_string(from_decimal_num);
                    auto expected_result = dt_to.to_string(to_decimal_num);
                    regression_test_data_set.emplace_back(from_value, expected_result);
                }
                if (from_scale == 0) {
                    break;
                }
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<ToT::PType>>(input_types, data_set, to_scale,
                                                                 to_precision);
        }
        if (FLAGS_gen_regression_case) {
            std::string from_decimal_sql_type_name =
                    FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimalv3";
            std::string from_sql_type_name = fmt::format("{}({}, {})", from_decimal_sql_type_name,
                                                         from_precision, from_scale);
            std::string to_sql_type_name = fmt::format("decimalv3({}, {})", to_precision, to_scale);
            std::string regression_case_name =
                    fmt::format("test_cast_to_decimal_{}_{}_from_{}_{}_{}", to_precision, to_scale,
                                FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimal",
                                from_precision, from_scale);
            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result);
        }
    }
    // test all case with a target Decimal type with fixed precision and scale
    template <typename FromT, typename ToT>
    void between_decimals_with_to_p_and_s_test_func(int to_precision, int to_scale) {
        constexpr auto from_max_decimal_p = max_decimal_precision<FromT::PType>();
        constexpr auto from_min_decimal_p =
                std::is_same_v<FromT, Decimal32>
                        ? 1
                        : (std::is_same_v<FromT, Decimal64>
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (std::is_same_v<FromT, Decimal128V3>
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (std::is_same_v<FromT, Decimal256>
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(from_min_decimal_p == 1 || from_min_decimal_p > 9);

        int table_index = 0;
        int test_data_index = 0;

        std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
        std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
        std::string regression_case_name =
                fmt::format("test_cast_to_{}_{}_{}_from_{}", to_lower(type_to_string(ToT::PType)),
                            to_precision, to_scale, to_lower(type_to_string(FromT::PType)));
        if (FLAGS_gen_regression_case) {
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_decimal/from_decimal");
        }
        auto* ofs_const_case = ofs_const_case_uptr.get();
        auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
        auto* ofs_case = ofs_case_uptr.get();
        auto* ofs_expected_result = ofs_expected_result_uptr.get();
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            if constexpr (IsDecimal256<FromT> || IsDecimal256<ToT>) {
                (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
            }
        }
        if constexpr (std::is_same_v<FromT, Decimal128V2>) {
            between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                    1, 0, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                    1, 1, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                    27, 9, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                    20, 5, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        } else {
            auto test_func = [&](int test_from_precision) {
                between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                        test_from_precision, 0, to_precision, to_scale, table_index++,
                        test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
                        ofs_const_expected_result

                );
                between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                        test_from_precision, 1, to_precision, to_scale, table_index++,
                        test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
                        ofs_const_expected_result);
                if (test_from_precision != 1) {
                    // between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                    //         test_from_precision, test_from_precision / 2, to_precision, to_scale,
                    //         table_index++, test_data_index, ofs_case, ofs_expected_result,
                    //         ofs_const_case, ofs_const_expected_result);
                    between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                            test_from_precision, test_from_precision - 1, to_precision, to_scale,
                            table_index++, test_data_index, ofs_case, ofs_expected_result,
                            ofs_const_case, ofs_const_expected_result);
                    between_decimal_with_precision_and_scale_test_func<FromT, ToT>(
                            test_from_precision, test_from_precision, to_precision, to_scale,
                            table_index++, test_data_index, ofs_case, ofs_expected_result,
                            ofs_const_case, ofs_const_expected_result);
                }
            };
            test_func(from_min_decimal_p);
            // if constexpr ((from_max_decimal_p / 2) > from_min_decimal_p) {
            //     test_func(from_max_decimal_p / 2);
            // }
            // test_func(from_max_decimal_p - 1);
            test_func(from_max_decimal_p);
        }

        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }
    // test all cases casting to a fixed target Decimal, with all possible target precisions and scales
    template <typename FromT, typename ToT>
    void between_decimal_test_func() {
        constexpr auto to_max_decimal_p = max_decimal_precision<ToT::PType>();
        constexpr auto to_min_decimal_p =
                std::is_same_v<ToT, Decimal32>
                        ? 1
                        : (std::is_same_v<ToT, Decimal64>
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (std::is_same_v<ToT, Decimal128V3>
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (std::is_same_v<ToT, Decimal256>
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(to_min_decimal_p == 1 || to_min_decimal_p > 9);

        auto test_func = [&](int test_to_precision) {
            between_decimals_with_to_p_and_s_test_func<FromT, ToT>(test_to_precision, 0);
            between_decimals_with_to_p_and_s_test_func<FromT, ToT>(test_to_precision, 1);
            if (test_to_precision != 1) {
                // between_decimals_with_to_p_and_s_test_func<FromT, ToT>(test_to_precision,
                //                                                        test_to_precision / 2);
                between_decimals_with_to_p_and_s_test_func<FromT, ToT>(test_to_precision,
                                                                       test_to_precision - 1);
                between_decimals_with_to_p_and_s_test_func<FromT, ToT>(test_to_precision,
                                                                       test_to_precision);
            }
        };
        test_func(to_min_decimal_p);
        // if constexpr ((to_max_decimal_p / 2) > to_min_decimal_p) {
        //     test_func(to_max_decimal_p / 2);
        // }
        // test_func(to_max_decimal_p - 1);
        test_func(to_max_decimal_p);
    }

    template <typename FromT, typename ToT>
    void between_decimal_with_precision_and_scale_overflow_test_func(
            int from_precision, int from_scale, int to_precision, int to_scale, int table_index,
            int& test_data_index, std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
            std::ofstream* ofs_const_case, std::ofstream* ofs_const_expected_result) {
        static_assert(IsDecimalNumber<FromT> && IsDecimalNumber<ToT>,
                      "FromT and ToT must be a decimal type");
        std::string dbg_str0 =
                fmt::format("===============test cast {}({}, {}) to {}({}, {}) overflow: ",
                            type_to_string(FromT::PType), from_precision, from_scale,
                            type_to_string(ToT::PType), to_precision, to_scale);
        // std::cout << dbg_str0 << std::endl;
        // static_assert(from_precision <= max_decimal_precision<FromT::PType>() &&
        //                       from_scale <= from_precision,
        //               "Wrong from precision or scale");
        // static_assert(to_precision <= max_decimal_precision<ToT::PType>() && to_scale <= to_precision,
        //               "Wrong to precision or scale");
        auto max_from_int_digit_count = from_precision - from_scale;
        auto max_to_int_digit_count = to_precision - to_scale;

        DataTypeDecimal<FromT::PType> dt_from =
                get_decimal_data_type<FromT>(from_precision, from_scale);
        DataTypeDecimal<ToT::PType> dt_to(to_precision, to_scale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), from_scale, from_precision}};
        auto from_decimal_ctor = get_decimal_ctor<FromT>();

        auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(from_precision - from_scale) -
                1;
        // std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;

        // max_fractional:    99999999
        auto from_max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(from_scale) - 1;
        if constexpr (IsDecimalV2<FromT>) {
            from_max_fractional *=
                    decimal_scale_multiplier<typename FromT::NativeType>(9 - from_scale);
        }

        auto to_max_integral =
                decimal_scale_multiplier<typename ToT::NativeType>(to_precision - to_scale) - 1;

        // e.g. for, Decimal(9, 1), 123456789.95 will round with int part carry
        std::set<typename FromT::NativeType> from_integral_part;
        bool may_overflow =
                (max_to_int_digit_count < max_from_int_digit_count) ||
                (max_to_int_digit_count == max_from_int_digit_count && to_scale < from_scale);
        if (may_overflow) {
            if (max_to_int_digit_count < max_from_int_digit_count) {
                // std::cout << "---------------------integral part may overflow\n";
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
                from_integral_part.emplace(to_max_integral + 1);
                from_integral_part.emplace(from_max_integral);
                if (from_precision - from_scale > 0) {
                    from_integral_part.emplace(from_max_integral - 1);
                }
            } else {
                // std::cout << "---------------------can overflow after rounding\n";
                // can overflow after rounding:
                // cases: from 999999.999
                //        to   xxxxxx.
                //             xxxxxx.xx
                // cases: from 0.999
                //        to   0.
                //             0.xx
                // e.g.: from Decimal(9, 3) to Decimal(8, 2)
                from_integral_part.emplace(to_max_integral);
            }
            DataSet data_set;
            std::string dbg_str = dbg_str0;
            std::vector<std::string> test_input_vals;
            int from_calc_scale = IsDecimalV2<FromT> ? 9 : from_scale;
            for (const auto& i : from_integral_part) {
                FromT from_decimal_num {};
                if (i == to_max_integral && to_scale >= from_scale) {
                    // cases: from 999999.999
                    //              xxxxx.xxx
                    //              xxxxx.xxxxx
                    continue;
                }
                from_decimal_num = from_decimal_ctor(i, from_max_fractional, from_scale);
                data_set.push_back({{from_decimal_num}, Null()});
                test_input_vals.push_back(dt_from.to_string(from_decimal_num));

                // dbg_str += fmt::format("{}|", dt_from.to_string(from_decimal_num));
                {
                    // strict mode
                    DataSet data_set_tmp;
                    data_set_tmp.push_back({{from_decimal_num}, Null()});
                    check_function_for_cast<DataTypeDecimal<ToT::PType>, true>(
                            input_types, data_set, to_scale, to_precision, true, true);
                }
                if (from_scale > to_scale) {
                    auto scale_multiplier = decimal_scale_multiplier<typename FromT::NativeType>(
                            from_calc_scale - to_scale);
                    typename FromT::NativeType from_frac = from_max_fractional / scale_multiplier;
                    from_frac *= 10;
                    from_frac += 5;
                    from_frac *= (typename FromT::NativeType)(scale_multiplier / 10);
                    from_decimal_num = from_decimal_ctor(i, from_frac, from_scale);
                    data_set.push_back({{from_decimal_num}, Null()});
                    test_input_vals.push_back(dt_from.to_string(from_decimal_num));

                    // dbg_str += fmt::format("{}|", dt_from.to_string(from_decimal_num));
                    {
                        // strict mode
                        DataSet data_set_tmp;
                        data_set_tmp.push_back({{from_decimal_num}, Null()});
                        check_function_for_cast<DataTypeDecimal<ToT::PType>, true>(
                                input_types, data_set, to_scale, to_precision, true, true);
                    }
                }
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeDecimal<ToT::PType>>(input_types, data_set, to_scale,
                                                                 to_precision);

            if (FLAGS_gen_regression_case) {
                std::string from_decimal_sql_type_name =
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimalv3";
                std::string from_sql_type_name = fmt::format(
                        "{}({}, {})", from_decimal_sql_type_name, from_precision, from_scale);
                std::string to_sql_type_name =
                        fmt::format("decimalv3({}, {})", to_precision, to_scale);
                std::string regression_case_name = fmt::format(
                        "test_cast_to_decimal_{}_{}_from_{}_{}_{}_overflow", to_precision, to_scale,
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimal", from_precision,
                        from_scale);
                gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name,
                                                         true, to_sql_type_name, test_input_vals,
                                                         table_index, ofs_case, ofs_expected_result,
                                                         ofs_const_case, ofs_const_expected_result);
            }
        }
    }

    // test all case with a target Decimal type with fixed precision and scale
    template <typename FromT, typename ToT>
    void between_decimals_with_to_p_and_s_overflow_test_func(
            int to_precision, int to_scale, int& table_index, int& test_data_index,
            std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
            std::ofstream* ofs_const_case, std::ofstream* ofs_const_expected_result) {
        constexpr auto from_max_decimal_p = max_decimal_precision<FromT::PType>();
        constexpr auto from_min_decimal_p =
                std::is_same_v<FromT, Decimal32>
                        ? 1
                        : (std::is_same_v<FromT, Decimal64>
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (std::is_same_v<FromT, Decimal128V3>
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (std::is_same_v<FromT, Decimal256>
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(from_min_decimal_p == 1 || from_min_decimal_p > 9);
        if constexpr (std::is_same_v<FromT, Decimal128V2>) {
            between_decimal_with_precision_and_scale_overflow_test_func<FromT, ToT>(
                    1, 0, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_overflow_test_func<FromT, ToT>(
                    1, 1, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_overflow_test_func<FromT, ToT>(
                    27, 9, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimal_with_precision_and_scale_overflow_test_func<FromT, ToT>(
                    20, 6, to_precision, to_scale, table_index++, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        } else {
            std::vector<int> from_precisions;
            from_precisions.emplace_back(from_min_decimal_p);
            // if (from_max_decimal_p / 2 > from_min_decimal_p) {
            //     from_precisions.emplace_back(from_max_decimal_p / 2);
            // }
            // from_precisions.emplace_back(from_max_decimal_p - 1);
            from_precisions.emplace_back(from_max_decimal_p);
            std::set<std::pair<int, int>> from_precision_scales;
            for (auto p : from_precisions) {
                from_precision_scales.emplace(p, 0);
                from_precision_scales.emplace(p, 1);
                if (p != 1) {
                    // from_precision_scales.emplace(p, p / 2);
                    from_precision_scales.emplace(p, p - 1);
                    from_precision_scales.emplace(p, p);
                }
            }
            for (const auto& from_p_s : from_precision_scales) {
                between_decimal_with_precision_and_scale_overflow_test_func<FromT, ToT>(
                        from_p_s.first, from_p_s.second, to_precision, to_scale, table_index++,
                        test_data_index, ofs_case, ofs_expected_result, ofs_const_case,
                        ofs_const_expected_result);
            }
        }
    }
    template <typename FromT, typename ToT>
    void between_decimal_overflow_test_func() {
        constexpr auto to_max_decimal_p = max_decimal_precision<ToT::PType>();
        constexpr auto to_min_decimal_p =
                std::is_same_v<ToT, Decimal32>
                        ? 1
                        : (std::is_same_v<ToT, Decimal64>
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (std::is_same_v<ToT, Decimal128V3>
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (std::is_same_v<ToT, Decimal256>
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(to_min_decimal_p == 1 || to_min_decimal_p > 9);

        // std::set<std::pair<int, int>> to_precision_scales;
        auto test_func = [&](int test_to_precision) {
            int table_index = 0;
            int test_data_index = 0;

            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string regression_case_name = fmt::format(
                    "test_cast_to_{}_{}_from_{}_overflow", to_lower(type_to_string(ToT::PType)),
                    test_to_precision, to_lower(type_to_string(FromT::PType)));
            if (FLAGS_gen_regression_case) {
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_decimal/from_decimal");
            }
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            if (FLAGS_gen_regression_case) {
                (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
                if constexpr (IsDecimal256<FromT> || IsDecimal256<ToT>) {
                    (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                    (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
                }
            }
            between_decimals_with_to_p_and_s_overflow_test_func<FromT, ToT>(
                    test_to_precision, 0, table_index, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            between_decimals_with_to_p_and_s_overflow_test_func<FromT, ToT>(
                    test_to_precision, 1, table_index, test_data_index, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            if (test_to_precision != 1) {
                // between_decimals_with_to_p_and_s_overflow_test_func<FromT, ToT>(
                //         test_to_precision, test_to_precision / 2, table_index, test_data_index,
                //         ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
                between_decimals_with_to_p_and_s_overflow_test_func<FromT, ToT>(
                        test_to_precision, test_to_precision - 1, table_index, test_data_index,
                        ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
                between_decimals_with_to_p_and_s_overflow_test_func<FromT, ToT>(
                        test_to_precision, test_to_precision, table_index, test_data_index,
                        ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            }

            if (FLAGS_gen_regression_case) {
                (*ofs_const_case) << "}";
                (*ofs_case) << "}";
            }
        };
        test_func(to_min_decimal_p);
        // if constexpr ((to_max_decimal_p / 2) > to_min_decimal_p) {
        //     test_func(to_max_decimal_p / 2);
        // }
        // test_func(to_max_decimal_p - 1);
        test_func(to_max_decimal_p);
    }
};
} // namespace doris::vectorized
