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

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <string>
#include <type_traits>

#include "cast_test.h"
#include "runtime/define_primitive_type.h"
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

    bool compareIgnoreCase(const std::string& str1, const std::string& str2) {
        std::string s1 = str1;
        std::string s2 = str2;

        std::transform(s1.begin(), s1.end(), s1.begin(), ::toupper);
        std::transform(s2.begin(), s2.end(), s2.begin(), ::toupper);

        return s1 == s2;
    }
    bool is_inf_or_nan_str(const std::string& str) {
        return compareIgnoreCase(str, "inf") || compareIgnoreCase(str, "infinity") ||
               compareIgnoreCase(str, "nan");
    }
    void format_decimal_number_func(DataSet& data_set, std::string v_str, auto v, bool is_negative,
                                    bool with_spaces, bool with_sign, bool leading_zeros) {
        if (leading_zeros) {
            if (!is_inf_or_nan_str(v_str)) {
                v_str = "000" + v_str;
            }
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
            data_set.push_back({{white_spaces_str + v_str}, v});
            data_set.push_back({{v_str + white_spaces_str}, v});
            data_set.push_back({{white_spaces_str + v_str + white_spaces_str}, v});
        } else {
            data_set.push_back({{v_str}, v});
        }
    }

    template <PrimitiveType FloatPType>
    void from_string_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        std::vector<std::string> test_strs = {
                "0",
                "1",
                "9",
                "100000",
                "123456",
                "999999",
                "0.",
                "1.",
                "9.",
                "100000.",
                "123456.",
                "999999.",
                ".0",
                ".1",
                ".9",
                ".000001",
                ".000009",
                ".100000",
                ".900000",
                ".100001",
                ".900009",
                "0.123456",
                ".999999",
                "0.0",
                "1.0",
                "9.0",
                "100000.0",
                "999999.0",
                "123456.0",
                "0.000000",
                "0.000001",
                "0.000009",
                "0.100001",
                "0.900009",
                "1.000000",
                "9.000000",
                "1.00001",
                "9.00001",
                "100000.000000",
                "100001.000000",
                "999999.000000",
                "123456.000000",
                "123.456",
                "999.999",
                "nan",
                "NaN",
                "NAN",
                "inf",
                "Inf",
                "INF",
                "infinity",
                "Infinity",
                "INFINITY",
        };
        // DataSet data_set = {
        //         // Zero and sign variations
        //         {{std::string("0")}, FloatType(0.0)},
        //         {{std::string("+0")}, FloatType(0.0)},
        //         {{std::string("-0")}, FloatType(-0.0)},
        //         {{std::string("0.0")}, FloatType(0.0)},
        //         {{std::string("+0.0")}, FloatType(0.0)},
        //         {{std::string("-0.0")}, FloatType(-0.0)},
        //         {{std::string(".0")}, FloatType(0.0)},
        //         {{std::string("+.0")}, FloatType(0.0)},
        //         {{std::string("-.0")}, FloatType(-0.0)},

        //         // Normal positive values
        //         {{std::string("1")}, FloatType(1.0)},
        //         {{std::string("123")}, FloatType(123.0)},
        //         {{std::string("1.23")}, FloatType(1.23)},
        //         {{std::string("123.456")}, FloatType(123.456)},
        //         {{std::string("1.23456")}, FloatType(1.23456)},
        //         {{std::string("0.123456")}, FloatType(0.123456)},
        //         {{std::string(".123456")}, FloatType(0.123456)},

        //         // positive values with plus sign
        //         {{std::string("+1")}, FloatType(1.0)},
        //         {{std::string("+123")}, FloatType(123.0)},
        //         {{std::string("+1.23")}, FloatType(1.23)},
        //         {{std::string("+123.456")}, FloatType(123.456)},
        //         {{std::string("+1.23456")}, FloatType(1.23456)},
        //         {{std::string("+0.123456")}, FloatType(0.123456)},
        //         {{std::string("+.123456")}, FloatType(0.123456)},

        //         // Normal negative values
        //         {{std::string("-1")}, FloatType(-1.0)},
        //         {{std::string("-123")}, FloatType(-123.0)},
        //         {{std::string("-1.23")}, FloatType(-1.23)},
        //         {{std::string("-123.456")}, FloatType(-123.456)},
        //         {{std::string("-1.23456")}, FloatType(-1.23456)},
        //         {{std::string("-0.123456")}, FloatType(-0.123456)},
        //         {{std::string("-.123456")}, FloatType(-0.123456)},

        //         // Scientific notation (exponent)
        //         {{std::string("1e0")}, FloatType(1.0)},
        //         {{std::string("1e1")}, FloatType(10.0)},
        //         {{std::string("1e-1")}, FloatType(0.1)},
        //         {{std::string("1.23e2")}, FloatType(123.0)},
        //         {{std::string("1.23e-2")}, FloatType(0.0123)},
        //         {{std::string("1.23E2")}, FloatType(123.0)},
        //         {{std::string("1.23E-2")}, FloatType(0.0123)},
        //         {{std::string("-1.23e2")}, FloatType(-123.0)},
        //         {{std::string("-1.23e-2")}, FloatType(-0.0123)},

        //         // Infinity values
        //         {{std::string("inf")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("INF")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("Inf")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("infinity")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("INFINITY")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("Infinity")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("+inf")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("-inf")}, -std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("+infinity")}, std::numeric_limits<FloatType>::infinity()},
        //         {{std::string("-infinity")}, -std::numeric_limits<FloatType>::infinity()},

        //         // NaN values
        //         {{std::string("nan")}, std::numeric_limits<FloatType>::quiet_NaN()},
        //         {{std::string("NAN")}, std::numeric_limits<FloatType>::quiet_NaN()},
        //         {{std::string("NaN")}, std::numeric_limits<FloatType>::quiet_NaN()},
        //         {{std::string("+nan")}, std::numeric_limits<FloatType>::quiet_NaN()},
        //         {{std::string("-nan")}, std::numeric_limits<FloatType>::quiet_NaN()},

        //         // Edge values - using type-specific limits
        //         {{fmt::format("{}", std::numeric_limits<FloatType>::max())},
        //          FloatType(std::numeric_limits<FloatType>::max())},
        //         {{fmt::format("{}", -std::numeric_limits<FloatType>::max())},
        //          FloatType(-std::numeric_limits<FloatType>::max())},
        //         {{fmt::format("{}", std::numeric_limits<FloatType>::min())},
        //          FloatType(std::numeric_limits<FloatType>::min())},
        //         {{fmt::format("{}", -std::numeric_limits<FloatType>::min())},
        //          FloatType(-std::numeric_limits<FloatType>::min())},

        //         // Very small values
        //         {{fmt::format("{}", std::numeric_limits<FloatType>::denorm_min())},
        //          FloatType(std::numeric_limits<FloatType>::denorm_min())},
        //         {{fmt::format("{}", -std::numeric_limits<FloatType>::denorm_min())},
        //          FloatType(-std::numeric_limits<FloatType>::denorm_min())},
        //         {{std::string("1e-1000")}, FloatType(0)},
        //         {{std::string("-1e-1000")}, FloatType(0)},

        //         // Whitespace variations
        //         {{std::string(" 1.23")}, FloatType(1.23)},
        //         {{std::string("1.23 ")}, FloatType(1.23)},
        //         {{std::string(" 1.23 ")}, FloatType(1.23)},
        //         {{std::string("\t1.23")}, FloatType(1.23)},
        //         {{std::string("1.23\t")}, FloatType(1.23)},
        //         {{std::string("\n1.23")}, FloatType(1.23)},
        //         {{std::string("1.23\n")}, FloatType(1.23)},
        //         {{std::string("\r1.23")}, FloatType(1.23)},
        //         {{std::string("1.23\r")}, FloatType(1.23)},
        //         {{std::string("\f1.23")}, FloatType(1.23)},
        //         {{std::string("1.23\f")}, FloatType(1.23)},
        //         {{std::string("\v1.23")}, FloatType(1.23)},
        //         {{std::string("1.23\v")}, FloatType(1.23)},
        //         {{std::string(" \t\n\r\f\v1.23 \t\n\r\f\v")}, FloatType(1.23)},

        //         // plus sign and Whitespace variations
        //         {{std::string(" +1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23 ")}, FloatType(1.23)},
        //         {{std::string(" +1.23 ")}, FloatType(1.23)},
        //         {{std::string("\t+1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23\t")}, FloatType(1.23)},
        //         {{std::string("\n+1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23\n")}, FloatType(1.23)},
        //         {{std::string("\r+1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23\r")}, FloatType(1.23)},
        //         {{std::string("\f+1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23\f")}, FloatType(1.23)},
        //         {{std::string("\v+1.23")}, FloatType(1.23)},
        //         {{std::string("+1.23\v")}, FloatType(1.23)},
        //         {{std::string(" \t\n\r\f\v+1.23 \t\n\r\f\v")}, FloatType(1.23)},

        //         // negative value and Whitespace variations
        //         {{std::string(" -1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23 ")}, FloatType(-1.23)},
        //         {{std::string(" -1.23 ")}, FloatType(-1.23)},
        //         {{std::string("\t-1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23\t")}, FloatType(-1.23)},
        //         {{std::string("\n-1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23\n")}, FloatType(-1.23)},
        //         {{std::string("\r-1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23\r")}, FloatType(-1.23)},
        //         {{std::string("\f-1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23\f")}, FloatType(-1.23)},
        //         {{std::string("\v-1.23")}, FloatType(-1.23)},
        //         {{std::string("-1.23\v")}, FloatType(-1.23)},
        //         {{std::string(" \t\n\r\f\v-1.23 \t\n\r\f\v")}, FloatType(-1.23)},

        //         // Invalid cases (should throw or return error)
        //         /*
        //         {{std::string("1e")}, Exception("Incomplete exponent")},
        //         {{std::string("e1")}, Exception("Missing significand")},
        //         {{std::string(".")}, Exception("Missing digits")},
        //         {{std::string("+")}, Exception("Missing digits")},
        //         {{std::string("-")}, Exception("Missing digits")},
        //         {{std::string("++1")}, Exception("Multiple signs")},
        //         {{std::string("--1")}, Exception("Multiple signs")},
        //         {{std::string("+-1")}, Exception("Multiple signs")},
        //         {{std::string("-+1")}, Exception("Multiple signs")},
        //         {{std::string("1e2.3")}, Exception("Decimal in exponent")},
        //         {{std::string("1e2e3")}, Exception("Multiple exponents")},
        //         {{std::string("1e2.3e4")}, Exception("Multiple exponents")},
        //         */
        // };
        /*
        std::cout << "cast string to float/double, max value:"
                  << fmt::format("{}", std::numeric_limits<FloatType>::max()) << "\n";
        std::cout << "cast string to float/double, -max value:"
                  << fmt::format("{}", -std::numeric_limits<FloatType>::max()) << "\n";
        std::cout << "cast string to float/double, min value:"
                  << fmt::format("{}", std::numeric_limits<FloatType>::min()) << "\n";
        std::cout << "cast string to float/double, -min value:"
                  << fmt::format("{}", -std::numeric_limits<FloatType>::min()) << "\n";
        std::cout << "cast string to float/double, denorm_min value:"
                  << fmt::format("{}", std::numeric_limits<FloatType>::denorm_min()) << "\n";
        std::cout << "cast string to float/double, -denorm_min value:"
                  << fmt::format("{}", -std::numeric_limits<FloatType>::denorm_min()) << "\n";
        */
        test_strs.emplace_back(fmt::format("{}", std::numeric_limits<FloatType>::max()));
        test_strs.emplace_back(fmt::format("{}", std::numeric_limits<FloatType>::min()));
        // test_strs.emplace_back(fmt::format("{}", std::numeric_limits<FloatType>::denorm_min()));

        std::vector<std::pair<std::string, std::string>> data_pairs;
        std::string table_test_expected_results;
        int data_index = 0;
        auto test_func = [&](bool is_negative) {
            DataSet data_set;
            for (const auto& v_str : test_strs) {
                FloatType v {};
                if constexpr (FloatPType == TYPE_FLOAT) {
                    v = std::strtof(v_str.data(), nullptr);
                } else {
                    v = std::strtod(v_str.data(), nullptr);
                }
                if (is_negative) {
                    v = -v;
                }
                if (FLAGS_gen_regression_case) {
                    std::string out_str = fmt::format("{}", v);
                    table_test_expected_results += fmt::format("{}\t{}\n", data_index++, out_str);
                    if (is_negative) {
                        data_pairs.emplace_back("-" + v_str, out_str);
                    } else {
                        data_pairs.emplace_back(v_str, out_str);
                    }
                }
                format_decimal_number_func(data_set, v_str, v, is_negative, true, true, true);
                // test leading and trailing spaces and sign
                format_decimal_number_func(data_set, v_str, v, is_negative, true, true, false);
                // test leading and trailing spaces and leading zeros
                format_decimal_number_func(data_set, v_str, v, is_negative, true, false, true);
                // test leading and trailing spaces
                format_decimal_number_func(data_set, v_str, v, is_negative, true, false, false);
                // test with sign and leading zeros
                format_decimal_number_func(data_set, v_str, v, is_negative, false, true, true);
                // test with sign
                format_decimal_number_func(data_set, v_str, v, is_negative, false, true, false);
                // test only leading zeros
                format_decimal_number_func(data_set, v_str, v, is_negative, false, false, true);
                // test strict digits
                format_decimal_number_func(data_set, v_str, v, is_negative, false, false, false);
            }
            check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1,
                                                                       -1);
            check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1,
                                                                      -1);
        };
        test_func(false);
        test_func(true);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string cast_type_name = (FloatPType == TYPE_FLOAT ? "float" : "double");
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(FloatPType);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            gen_normal_regression_case(regression_case_name, from_sql_type_name, false,
                                       to_sql_type_name, data_pairs, table_index++, test_data_index,
                                       ofs_case, ofs_expected_result, ofs_const_case,
                                       ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FloatPType>
    void from_string_overflow_test_func() {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        DataSet data_set = {
                // Edge values - using type-specific limits
                {{std::string("1.89769e+308")}, std::numeric_limits<FloatType>::infinity()},
                {{std::string("-1.89769e+308")}, -std::numeric_limits<FloatType>::infinity()},
        };

        // stric and non-strict mode
        check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1, -1);
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            std::vector<std::pair<std::string, FloatType>> data_pairs;
            data_pairs.emplace_back("1.89769e+308", std::numeric_limits<FloatType>::infinity());
            data_pairs.emplace_back("-1.89769e+308", -std::numeric_limits<FloatType>::infinity());
            std::string table_test_expected_results;
            table_test_expected_results +=
                    fmt::format("{}\t{}\n", 0, std::numeric_limits<FloatType>::infinity());
            table_test_expected_results +=
                    fmt::format("{}\t{}\n", 1, -std::numeric_limits<FloatType>::infinity());

            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(FloatPType);
            std::string regression_case_name = fmt::format("test_cast_to_{}_from_{}_overflow",
                                                           to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            auto table_name = fmt::format("{}_{}", regression_case_name, table_index);
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            (*ofs_case) << fmt::format("    sql \"drop table if exists {};\"\n", table_name);
            (*ofs_case) << fmt::format(
                    "    sql \"create table {}(f1 int, f2 string) "
                    "properties('replication_num'='1');\"\n",
                    table_name);
            auto value_count = data_pairs.size();
            auto const_test_with_strict_arg = [&](bool enable_strict_cast) {
                (*ofs_const_case) << fmt::format("    sql \"set enable_strict_cast={};\"\n",
                                                 enable_strict_cast);
                for (int i = 0; i != value_count; ++i) {
                    const auto& v_str = data_pairs[i].first;
                    (*ofs_const_case) << fmt::format(
                            "    qt_sql_{}_{} \"\"\"select \"{}\", cast(\"{}\" as {});\"\"\"\n", i,
                            enable_strict_cast ? "strict" : "non_strict", v_str, v_str,
                            to_sql_type_name);
                    (*ofs_const_expected_result) << fmt::format(
                            "-- !sql_{}_{} --\n", i, enable_strict_cast ? "strict" : "non_strict");
                    (*ofs_const_expected_result)
                            << fmt::format("{}\t{}\n\n", v_str, data_pairs[i].second);
                }
            };
            const_test_with_strict_arg(true);
            const_test_with_strict_arg(false);

            (*ofs_case) << fmt::format("    sql \"\"\"insert into {} values ", table_name);
            for (int i = 0; i != value_count;) {
                (*ofs_case) << fmt::format("({}, \"{}\")", i, data_pairs[i].first);
                ++i;
                if (i != value_count) {
                    (*ofs_case) << ",";
                }
                if (i % 20 == 0 && i != value_count) {
                    (*ofs_case) << "\n      ";
                }
            }
            (*ofs_case) << ";\n    \"\"\"\n\n";

            auto table_test_with_strict_arg = [&](bool enable_strict_cast) {
                (*ofs_case) << fmt::format("    sql \"set enable_strict_cast={};\"\n",
                                           enable_strict_cast);
                (*ofs_case) << fmt::format(
                        "    qt_sql_{}_{} 'select f1, cast(f2 as {}) from {} "
                        "order by "
                        "1;'\n\n",
                        table_index, enable_strict_cast ? "strict" : "non_strict", to_sql_type_name,
                        table_name);

                (*ofs_expected_result) << fmt::format("-- !sql_{}_{} --\n", table_index,
                                                      enable_strict_cast ? "strict" : "non_strict");
                (*ofs_expected_result) << table_test_expected_results;
                (*ofs_expected_result) << "\n";
            };
            table_test_with_strict_arg(true);
            table_test_with_strict_arg(false);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FloatPType>
    void from_string_invalid_input_test_func() {
        // PG error msg: invalid input syntax for type double precision: "++123.456"
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        std::vector<std::string> abnormal_inputs = {
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
            for (const auto& input : abnormal_inputs) {
                data_set.push_back({{input}, Null()});
            }
            check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1,
                                                                       -1);
        }

        // strict mode
        using ToDataType = DataTypeNumber<FloatPType>;
        for (const auto& input : abnormal_inputs) {
            DataSet data_set;
            data_set.push_back({{input}, Null()});
            check_function_for_cast<ToDataType, true>(input_types, data_set, -1, -1, true, true);
            // EXPECT_TRUE(caught_expection) << "Expected exception for input: " << input
            //                               << ", but no exception was thrown.";
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
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            // std::string cast_type_name = (FloatPType == TYPE_FLOAT ? "float" : "double");
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(FloatPType);
            std::string regression_case_name = fmt::format("test_cast_to_{}_from_{}_invalid",
                                                           to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            gen_overflow_and_invalid_regression_case(
                    regression_case_name, from_sql_type_name, false, to_sql_type_name,
                    regression_invalid_inputs, table_index++, ofs_case, ofs_expected_result,
                    ofs_const_case, ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
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
        };

        check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1, -1);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_type_name = get_sql_type_name(IntPType);
            std::string to_type_name = get_sql_type_name(FloatPType);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_type_name, from_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_int");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            std::vector<std::pair<std::string, FloatType>> regression_test_data_set;
            for (const auto& p : data_set) {
                regression_test_data_set.push_back(
                        {fmt::format("{}", any_cast<IntType>(p.first[0])),
                         any_cast<FloatType>(p.second)});
            }
            gen_normal_regression_case(regression_case_name, from_type_name, true, to_type_name,
                                       regression_test_data_set, table_index++, test_data_index,
                                       ofs_case, ofs_expected_result, ofs_const_case,
                                       ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }
    template <typename FromT, int FromPrecision, int FromScale, PrimitiveType FloatPType>
    void from_decimal_no_overflow_test_func() {
        static_assert(IsDecimalNumber<FromT>, "FromT must be a decimal type");
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        DataTypeDecimal<FromT::PType> dt_from =
                get_decimal_data_type<FromT>(FromPrecision, FromScale);

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
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;
        // std::cout << "large_integral1:\t" << fmt::format("{}", large_integral1) << std::endl;
        // std::cout << "large_integral2:\t" << fmt::format("{}", large_integral2) << std::endl;
        // std::cout << "large_integral3:\t" << fmt::format("{}", large_integral3) << std::endl;

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
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        // std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        // std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        // std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;
        std::set<typename FromT::NativeType> integral_part = {0, max_integral};
        if (max_integral > 0) {
            integral_part.emplace(1);
            integral_part.emplace(9);
            integral_part.emplace(max_integral - 1);
            integral_part.emplace(large_integral1);
            integral_part.emplace(large_integral2);
            integral_part.emplace(large_integral3);
        }
        typename FromT::NativeType from_fractional_part_multiplier = 1;
        if constexpr (IsDecimalV2<FromT>) {
            from_fractional_part_multiplier =
                    decimal_scale_multiplier<typename FromT::NativeType>(9 - FromScale);
        }
        std::set<typename FromT::NativeType> fractional_part = {0};
        fractional_part.emplace(max_fractional * from_fractional_part_multiplier);
        if (max_fractional > 0) {
            fractional_part.emplace(1 * from_fractional_part_multiplier);
            fractional_part.emplace(9 * from_fractional_part_multiplier);
            fractional_part.emplace((max_fractional - 1) * from_fractional_part_multiplier);
            fractional_part.emplace(large_fractional1 * from_fractional_part_multiplier);
            fractional_part.emplace(large_fractional2 * from_fractional_part_multiplier);
            fractional_part.emplace(large_fractional3 * from_fractional_part_multiplier);
        }
        DataSet data_set;
        std::string dbg_str = fmt::format("test cast to {} from {}({}, {})\n",
                                          std::is_same_v<FloatType, Float32> ? "float" : "double",
                                          type_to_string(FromT::PType), FromPrecision, FromScale);

        auto scale_multiplier = decimal_scale_multiplier<typename FromT::NativeType>(
                IsDecimalV2<FromT> ? 9 : FromScale);
        constexpr bool expect_inf =
                (FromPrecision - FromScale >= 39 && std::is_same_v<FromT, Float32>);
        bool have_inf = false;

        std::vector<std::pair<std::string, FloatType>> test_data_set;
        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                std::string from_decimal_sql_type_name =
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimalv3";
                std::string from_sql_type_name = fmt::format(
                        "{}({}, {})", from_decimal_sql_type_name, FromPrecision, FromScale);
                std::string to_sql_type_name = get_sql_type_name(FloatPType);
                int table_index = 0;
                int test_data_index = 0;

                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string regression_case_name = fmt::format(
                        "test_cast_to_{}_from_{}_{}_{}", to_sql_type_name,
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2"
                                                       : to_lower(type_to_string(FromT::PType)),
                        FromPrecision, FromScale);
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_float/from_decimal");
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();
                if constexpr (IsDecimal256<FromT>) {
                    (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                    (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
                }
                gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                           to_sql_type_name, test_data_set, table_index,
                                           test_data_index, ofs_case, ofs_expected_result,
                                           ofs_const_case, ofs_const_expected_result);
                (*ofs_const_case) << "}";
                (*ofs_case) << "}";
            }
        }};
        if constexpr (FromScale == 0) {
            // e.g. Decimal(9, 0), only int part
            for (const auto& i : integral_part) {
                auto decimal_num = decimal_ctor(i, 0, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                // auto float_v = static_cast<FloatType>(i);
                FloatType float_v;
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                if (std::isinf(float_v)) {
                    // std::cout << fmt::format("cast {}({}, {}) value {} to float_v result is inf\n",
                    //                          type_to_string(FromT::PType), FromPrecision, FromScale,
                    //                          dt.to_string(decimal_num));
                    have_inf = true;
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
                test_data_set.emplace_back(num_str, float_v);

                decimal_num = decimal_ctor(-i, 0, FromScale);
                num_str = dt_from.to_string(decimal_num);
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                if (std::isinf(float_v)) {
                    // std::cout << fmt::format("cast {}({}, {}) value {} to float_v result is inf\n",
                    //                          type_to_string(FromT::PType), FromPrecision, FromScale,
                    //                          dt.to_string(decimal_num));
                    have_inf = true;
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), -i);
                data_set.push_back({{decimal_num}, FloatType(-i)});
                test_data_set.emplace_back(num_str, FloatType(-i));
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1,
                                                                      -1);
            check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1,
                                                                       -1);
            return;
        } else if constexpr (FromScale == FromPrecision) {
            // e.g. Decimal(9, 9), only fraction part
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(0, f, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                FloatType float_v;
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
                test_data_set.emplace_back(num_str, float_v);

                decimal_num = decimal_ctor(0, -f, FromScale);
                num_str = dt_from.to_string(decimal_num);
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
                test_data_set.emplace_back(num_str, float_v);
            }
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1,
                                                                      -1);
            check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1,
                                                                       -1);
            return;
        }

        for (const auto& i : integral_part) {
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(i, f, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                FloatType float_v;
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                if (std::isinf(float_v)) {
                    // std::cout << fmt::format("cast {}({}, {}) value {} to float_v result is inf\n",
                    //                          type_to_string(FromT::PType), FromPrecision, FromScale,
                    //                          dt.to_string(decimal_num));
                    have_inf = true;
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
                test_data_set.emplace_back(num_str, float_v);

                decimal_num = decimal_ctor(-i, -f, FromScale);
                num_str = dt_from.to_string(decimal_num);
                if constexpr (IsDecimal256<FromT>) {
                    float_v = static_cast<long double>(decimal_num.value) /
                              static_cast<long double>(scale_multiplier);
                } else {
                    float_v = static_cast<double>(decimal_num.value) /
                              static_cast<double>(scale_multiplier);
                }
                if (std::isinf(float_v)) {
                    // std::cout << fmt::format("cast {}({}, {}) value {} to float_v result is inf\n",
                    //                          type_to_string(FromT::PType), FromPrecision, FromScale,
                    //                          dt.to_string(decimal_num));
                    have_inf = true;
                }
                // dbg_str += fmt::format("({}, {})|", dt.to_string(decimal_num), float_v);
                data_set.push_back({{decimal_num}, float_v});
                test_data_set.emplace_back(num_str, float_v);
            }
            // dbg_str += "\n";
        }
        // std::cout << dbg_str << std::endl;
        if constexpr (expect_inf) {
            EXPECT_TRUE(have_inf);
        }

        check_function_for_cast<DataTypeNumber<FloatPType>, true>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1, -1);
    }

    template <typename FromT, PrimitiveType ToPT>
    void from_decimal_test_func() {
        constexpr auto max_decimal_pre = max_decimal_precision<FromT::PType>();
        constexpr auto min_decimal_pre =
                std::is_same_v<FromT, Decimal32>
                        ? 1
                        : (std::is_same_v<FromT, Decimal64>
                                   ? BeConsts::MAX_DECIMAL32_PRECISION + 1
                                   : (std::is_same_v<FromT, Decimal128V3>
                                              ? BeConsts::MAX_DECIMAL64_PRECISION + 1
                                              : (std::is_same_v<FromT, Decimal256>
                                                         ? BeConsts::MAX_DECIMAL128_PRECISION + 1
                                                         : 1)));
        static_assert(min_decimal_pre == 1 || min_decimal_pre > 9);

        if constexpr (std::is_same_v<FromT, Decimal128V2>) {
            from_decimal_no_overflow_test_func<FromT, 1, 0, ToPT>();
            from_decimal_no_overflow_test_func<FromT, 1, 1, ToPT>();
            from_decimal_no_overflow_test_func<FromT, 27, 9, ToPT>();
            from_decimal_no_overflow_test_func<FromT, 20, 6, ToPT>();
        } else {
            from_decimal_no_overflow_test_func<FromT, min_decimal_pre, 0, ToPT>();
            if constexpr (min_decimal_pre != 1) {
                from_decimal_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre / 2,
                                                   ToPT>();
                from_decimal_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre - 1,
                                                   ToPT>();
            }
            from_decimal_no_overflow_test_func<FromT, min_decimal_pre, min_decimal_pre, ToPT>();

            from_decimal_no_overflow_test_func<FromT, max_decimal_pre, 0, ToPT>();
            from_decimal_no_overflow_test_func<FromT, max_decimal_pre, 1, ToPT>();
            from_decimal_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre / 2, ToPT>();
            from_decimal_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre - 1, ToPT>();
            from_decimal_no_overflow_test_func<FromT, max_decimal_pre, max_decimal_pre, ToPT>();
        }
    }

    template <int FromPrecision, int FromScale>
    void from_decimalv3_overflow_test_func(const std::string& regression_case_name, int table_index,
                                           int& test_data_index, std::ofstream* ofs_case,
                                           std::ofstream* ofs_expected_result,
                                           std::ofstream* ofs_const_case,
                                           std::ofstream* ofs_const_expected_result) {
        using FromT = Decimal256;
        using FloatType = float;
        DataTypeDecimal<TYPE_DECIMAL256> dt_from(FromPrecision, FromScale);
        InputTypeSet input_types = {{TYPE_DECIMAL256, FromScale, FromPrecision}};
        auto decimal_ctor = get_decimal_ctor<FromT>();

        constexpr auto max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        constexpr auto large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                                 FromPrecision - FromScale - 1) -
                                         1;
        constexpr auto large_integral2 = max_integral - large_integral1;
        constexpr auto large_integral3 =
                large_integral2 > 9 ? large_integral2 + 1 : large_integral2 - 1;
        // std::cout << "max_integral:\t" << fmt::format("{}", max_integral) << std::endl;
        // std::cout << "large_integral1:\t" << fmt::format("{}", large_integral1) << std::endl;
        // std::cout << "large_integral2:\t" << fmt::format("{}", large_integral2) << std::endl;
        // std::cout << "large_integral3:\t" << fmt::format("{}", large_integral3) << std::endl;

        constexpr auto max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale) - 1;
        constexpr auto large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale - 1) - 1;
        constexpr auto large_fractional2 = max_fractional - large_fractional1;
        constexpr auto large_fractional3 =
                large_fractional2 > 9 ? large_fractional2 + 1 : large_fractional2 - 1;
        // std::cout << "max_fractional:\t" << fmt::format("{}", max_fractional) << std::endl;
        // std::cout << "large_fractional1:\t" << fmt::format("{}", large_fractional1) << std::endl;
        // std::cout << "large_fractional2:\t" << fmt::format("{}", large_fractional2) << std::endl;
        // std::cout << "large_fractional3:\t" << fmt::format("{}", large_fractional3) << std::endl;

        std::set<typename FromT::NativeType> integral_part = {max_integral, large_integral2,
                                                              large_integral3};
        std::set<typename FromT::NativeType> fractional_part = {
                0, max_fractional, large_fractional1, large_fractional2, large_fractional3,
        };
        DataSet data_set;
        std::string dbg_str = fmt::format("test cast to {} from {}({}, {}) overflow\n",
                                          std::is_same_v<FloatType, Float32> ? "float" : "double",
                                          type_to_string(FromT::PType), FromPrecision, FromScale);

        std::vector<std::pair<std::string, FloatType>> test_data_set;
        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                std::string from_sql_type_name =
                        fmt::format("decimalv3({}, {})", FromPrecision, FromScale);
                std::string to_sql_type_name = "float";
                gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                           to_sql_type_name, test_data_set, table_index,
                                           test_data_index, ofs_case, ofs_expected_result,
                                           ofs_const_case, ofs_const_expected_result);
            }
        }};
        if constexpr (FromScale == 0) {
            // e.g. Decimal(76, 0), only int part
            for (const auto& i : integral_part) {
                auto decimal_num = decimal_ctor(i, 0, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("{}, ", num_str);
                data_set.push_back({{decimal_num}, std::numeric_limits<FloatType>::infinity()});
                test_data_set.emplace_back(num_str, std::numeric_limits<FloatType>::infinity());

                decimal_num = decimal_ctor(-i, 0, FromScale);
                num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("{}, ", num_str);
                data_set.push_back({{decimal_num}, -std::numeric_limits<FloatType>::infinity()});
                test_data_set.emplace_back(num_str, -std::numeric_limits<FloatType>::infinity());
            }
            // dbg_str += "\n";
            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<TYPE_FLOAT>, true>(input_types, data_set, -1,
                                                                      -1);
            check_function_for_cast<DataTypeNumber<TYPE_FLOAT>, false>(input_types, data_set, -1,
                                                                       -1);
            return;
        } else if constexpr (FromScale == FromPrecision) {
            return;
        }

        for (const auto& i : integral_part) {
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(i, f, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("{}, ", num_str);
                data_set.push_back({{decimal_num}, std::numeric_limits<FloatType>::infinity()});
                test_data_set.emplace_back(num_str, std::numeric_limits<FloatType>::infinity());

                decimal_num = decimal_ctor(-i, -f, FromScale);
                num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("{}, ", num_str);
                data_set.push_back({{decimal_num}, -std::numeric_limits<FloatType>::infinity()});
                test_data_set.emplace_back(num_str, -std::numeric_limits<FloatType>::infinity());
            }
            // dbg_str += "\n";
        }
        // std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<TYPE_FLOAT>, true>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<TYPE_FLOAT>, false>(input_types, data_set, -1, -1);
    }
    void from_decimal_overflow_test_func() {
        constexpr auto max_decimal_pre = max_decimal_precision<TYPE_DECIMAL256>();
        constexpr auto min_decimal_pre = BeConsts::MAX_DECIMAL128_PRECISION + 1;

        int table_index = 0;
        int test_data_index = 0;

        std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
        std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
        std::string regression_case_name = "test_cast_to_float_from_decimal256_overflow";
        if (FLAGS_gen_regression_case) {
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_decimal");
        }
        auto* ofs_const_case = ofs_const_case_uptr.get();
        auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
        auto* ofs_case = ofs_case_uptr.get();
        auto* ofs_expected_result = ofs_expected_result_uptr.get();
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
            (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
        }
        from_decimalv3_overflow_test_func<min_decimal_pre, 0>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<min_decimal_pre + 1, 0>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<min_decimal_pre + 1, 1>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);

        from_decimalv3_overflow_test_func<60, 0>(regression_case_name, table_index++,
                                                 test_data_index, ofs_case, ofs_expected_result,
                                                 ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<60, 20>(regression_case_name, table_index++,
                                                  test_data_index, ofs_case, ofs_expected_result,
                                                  ofs_const_case, ofs_const_expected_result);

        from_decimalv3_overflow_test_func<max_decimal_pre, 0>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<max_decimal_pre, 1>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<max_decimal_pre, 10>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        from_decimalv3_overflow_test_func<max_decimal_pre, 37>(
                regression_case_name, table_index++, test_data_index, ofs_case, ofs_expected_result,
                ofs_const_case, ofs_const_expected_result);
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FloatPType>
    void from_date_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        std::vector<uint16_t> years = {0, 1, 10, 100, 2025, 9999};
        std::vector<uint8_t> months = {1, 12};
        std::vector<uint8_t> days = {1, 10, 28};
        DataTypeDateV2 dt;
        std::string to_sql_type_name = get_sql_type_name(FloatPType);
        std::string dbg_str = fmt::format("test cast to {} from date\n", to_sql_type_name);
        DataSet data_set;
        std::vector<std::pair<std::string, FloatType>> regression_test_data_set;
        for (auto year : years) {
            for (auto month : months) {
                for (auto day : days) {
                    DateV2Value<DateV2ValueType> date_val(year, month, day, 0, 0, 0, 0);
                    FloatType expect_cast_result = year * 10000 + month * 100 + day;
                    // dbg_str += fmt::format("({}, {})|", dt.to_string(date_val.to_date_int_val()),
                    //                        expect_cast_result);
                    data_set.push_back({{date_val}, expect_cast_result});
                    if (FLAGS_gen_regression_case) {
                        regression_test_data_set.push_back(
                                {dt.to_string(date_val.to_date_int_val()), expect_cast_result});
                    }
                    {
                        DataSet data_set_tmp;
                        data_set_tmp.push_back({{date_val}, expect_cast_result});
                        check_function_for_cast<DataTypeNumber<FloatPType>, true>(
                                input_types, data_set_tmp, -1, -1, false, true);
                    }
                }
            }
            // dbg_str += "\n";
        }
        // std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1, -1,
                                                                   false);
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "datev2";
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_float/from_datetime", false);
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index++,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result, false, true,
                                       true);
            // (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FloatPType, int Scale>
    void from_datetime_test_func() {
        using FloatType = typename PrimitiveTypeTraits<FloatPType>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, Scale}};
        std::vector<uint16_t> years = {0, 1, 10, 100, 2025, 9999};
        std::vector<uint8_t> months = {1, 12};
        std::vector<uint8_t> days = {1, 28};
        std::vector<uint8_t> hours = {0, 1, 23};
        std::vector<uint8_t> minutes = {0, 1, 59};
        std::vector<uint8_t> seconds = {0, 1, 59};
        std::vector<uint32_t> mircoseconds = {0, 1, 999999};
        DataTypeDateTimeV2 dt(Scale);
        std::string to_sql_type_name = get_sql_type_name(FloatPType);
        std::string dbg_str =
                fmt::format("test cast to {} from datetimev2({})\n", Scale, to_sql_type_name);
        DataSet data_set;
        std::vector<std::pair<std::string, FloatType>> regression_test_data_set;
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
                                    if (FLAGS_gen_regression_case) {
                                        regression_test_data_set.push_back(
                                                {dt.to_string(date_val.to_date_int_val()),
                                                 expect_cast_result});
                                    }
                                    {
                                        DataSet data_set_tmp;
                                        data_set_tmp.push_back({{date_val}, expect_cast_result});
                                        check_function_for_cast<DataTypeNumber<FloatPType>, true>(
                                                input_types, data_set_tmp, -1, -1, false, true);
                                    }
                                }
                            }
                        }
                    }
                    // dbg_str += "\n";
                }
            }
        }
        // std::cout << dbg_str << std::endl;
        check_function_for_cast<DataTypeNumber<FloatPType>, false>(input_types, data_set, -1, -1,
                                                                   false);
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;

            auto test_data_total_row_count = regression_test_data_set.size();
            size_t num_pieces = 10;
            size_t regression_case_avg_row_count =
                    (test_data_total_row_count + num_pieces - 1) / num_pieces;
            for (size_t piece = 0; piece < num_pieces; ++piece) {
                size_t data_index = piece * regression_case_avg_row_count;
                if (data_index >= test_data_total_row_count) {
                    break;
                }
                size_t regression_case_row_count = regression_case_avg_row_count;
                if (data_index + regression_case_row_count > test_data_total_row_count) {
                    regression_case_row_count = test_data_total_row_count - data_index;
                }
                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string from_sql_type_name = fmt::format("datetimev2({})", Scale);
                std::string regression_case_name =
                        fmt::format("test_cast_to_{}_from_datetimev2_{}_part{}", to_sql_type_name,
                                    Scale, piece);
                setup_regression_case_output(
                        regression_case_name, ofs_const_case_uptr, ofs_const_expected_result_uptr,
                        ofs_case_uptr, ofs_expected_result_uptr, "to_float/from_datetime", false);
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();
                std::vector<std::pair<std::string, FloatType>> piece_data(
                        regression_test_data_set.begin() + data_index,
                        regression_test_data_set.begin() + data_index + regression_case_row_count);

                gen_normal_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        piece_data, table_index++, test_data_index, ofs_case, ofs_expected_result,
                        ofs_const_case, ofs_const_expected_result, false, true, true);
                // (*ofs_const_case) << "}";
                (*ofs_case) << "}";
            }
        }
    }

    template <PrimitiveType ToPT>
    void from_time_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        std::vector<int64_t> hours = {0, 1, 10, 100, 838};
        std::vector<int64_t> minutes = {0, 1, 10, 59};
        std::vector<int64_t> seconds = {0, 1, 10, 59};

        std::string to_sql_type_name = get_sql_type_name(ToPT);
        std::string dbg_str = fmt::format("test cast to {} from time\n", to_sql_type_name);
        std::vector<std::pair<std::string, ToT>> regression_test_data_set;
        auto test_func = [&](bool negative) {
            DataSet data_set;
            for (auto h : hours) {
                for (auto m : minutes) {
                    for (auto s : seconds) {
                        auto time_val = doris::TimeValue::make_time(h, m, s, negative);
                        auto expect_cast_result = static_cast<ToT>(time_val);
                        data_set.push_back({{time_val}, expect_cast_result});
                        // dbg_str +=
                        //         fmt::format("({}, {})|", doris::TimeValue::to_string(time_val, 6),
                        //                     expect_cast_result);
                        if (FLAGS_gen_regression_case) {
                            regression_test_data_set.emplace_back(
                                    doris::TimeValue::to_string(time_val, 6), expect_cast_result);
                        }
                        {
                            DataSet data_set_tmp;
                            data_set_tmp.push_back({{time_val}, expect_cast_result});
                            check_function_for_cast<DataTypeNumber<ToPT>, true>(
                                    input_types, data_set_tmp, -1, -1, false, true);
                        }
                    }
                }
            }

            // std::cout << dbg_str << std::endl;
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1,
                                                                 false);
        };
        test_func(false);
        test_func(true);
        /*
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "time";
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(
                    regression_case_name, ofs_const_case_uptr, ofs_const_expected_result_uptr,
                    ofs_case_uptr, ofs_expected_result_uptr, "to_float/from_datetime", true, false);
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index++,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result, true, false,
                                       true);
            (*ofs_const_case) << "}";
        }
        */
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
TEST_F(FunctionCastToFloatTest, test_from_string_overflow) {
    from_string_overflow_test_func<TYPE_FLOAT>();
    from_string_overflow_test_func<TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_string_invalid_input) {
    from_string_invalid_input_test_func<TYPE_FLOAT>();
    from_string_invalid_input_test_func<TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_bool) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    {
        DataSet data_set = {
                {{UInt8 {0}}, Float32(0)},
                {{UInt8 {1}}, Float32(1)},
        };
        check_function_for_cast<DataTypeFloat32, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeFloat32, true>(input_types, data_set, -1, -1);
    }
    {
        DataSet data_set = {
                {{UInt8 {0}}, Float64(0)},
                {{UInt8 {1}}, Float64(1)},
        };
        check_function_for_cast<DataTypeFloat64, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeFloat64, true>(input_types, data_set, -1, -1);
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
            {{std::numeric_limits<Float32>::infinity()}, std::numeric_limits<Float64>::infinity()},

            // NaN values
            {{std::numeric_limits<Float32>::quiet_NaN()},
             std::numeric_limits<Float64>::quiet_NaN()},

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

    check_function_for_cast<DataTypeFloat64, false>(input_types, data_set, -1, -1);
    check_function_for_cast<DataTypeFloat64, true>(input_types, data_set, -1, -1);
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
            {{std::numeric_limits<Float64>::infinity()}, std::numeric_limits<Float32>::infinity()},

            // NaN values
            {{std::numeric_limits<Float64>::quiet_NaN()},
             std::numeric_limits<Float32>::quiet_NaN()},

            // Edge values - using type-specific limits
            {{Float64(std::numeric_limits<Float32>::max())}, std::numeric_limits<Float32>::max()},
            {{Float64(-std::numeric_limits<Float32>::max())}, -std::numeric_limits<Float32>::max()},
            {{Float64(std::numeric_limits<Float32>::min())}, std::numeric_limits<Float32>::min()},
            {{Float64(-std::numeric_limits<Float32>::min())}, -std::numeric_limits<Float32>::min()},

            // overflow
            {{std::numeric_limits<Float64>::max()}, std::numeric_limits<Float32>::infinity()},

            // Very small values
            {{Float64(std::numeric_limits<Float32>::denorm_min())},
             Float32(std::numeric_limits<Float32>::denorm_min())},
            {{Float64(-std::numeric_limits<Float32>::denorm_min())},
             Float32(-std::numeric_limits<Float32>::denorm_min())},

    };

    check_function_for_cast<DataTypeFloat32, false>(input_types, data_set, -1, -1);
    check_function_for_cast<DataTypeFloat32, true>(input_types, data_set, -1, -1);
}
TEST_F(FunctionCastToFloatTest, test_from_decimal) {
    from_decimal_test_func<Decimal32, TYPE_FLOAT>();
    from_decimal_test_func<Decimal64, TYPE_FLOAT>();
    from_decimal_test_func<Decimal128V3, TYPE_FLOAT>();
    from_decimal_test_func<Decimal256, TYPE_FLOAT>();

    from_decimal_test_func<Decimal128V2, TYPE_FLOAT>();

    from_decimal_test_func<Decimal32, TYPE_DOUBLE>();
    from_decimal_test_func<Decimal64, TYPE_DOUBLE>();
    from_decimal_test_func<Decimal128V3, TYPE_DOUBLE>();
    from_decimal_test_func<Decimal256, TYPE_DOUBLE>();

    from_decimal_test_func<Decimal128V2, TYPE_DOUBLE>();
}
TEST_F(FunctionCastToFloatTest, test_from_decimal_overflow) {
    from_decimal_overflow_test_func();
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
    from_time_test_func<TYPE_FLOAT>();
    from_time_test_func<TYPE_DOUBLE>();
}
} // namespace doris::vectorized