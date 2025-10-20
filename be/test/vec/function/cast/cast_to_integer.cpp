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

#include <cstdlib>
#include <fstream>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "cast_test.h"
#include "runtime/define_primitive_type.h"
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
        auto max_val = std::numeric_limits<T>::max();
        auto min_val = std::numeric_limits<T>::min();
        auto max_val_minus_1 = max_val - T {1};
        auto min_val_plus_1 = min_val + T {1};
        auto min_val_abs_val = UnsignedT(max_val) + 1;
        std::string min_val_str_no_sign = fmt::format("{}", min_val_abs_val);
        std::vector<T> test_vals = {T {0},  T {1},  T {9},    T {123}, max_val,
                                    T {-1}, T {-9}, T {-123}, min_val};
        test_vals.push_back(max_val_minus_1);
        test_vals.push_back(min_val_plus_1);

        std::vector<std::pair<std::string, T>> data_pairs;

        // test leading zeros, sign, leading and trailing white spaces for positive values
        auto tmp_test_func = [&](bool with_spaces, bool with_sign, bool leading_zeros) {
            DataSet data_set;
            for (auto v : test_vals) {
                bool is_negative = (v < 0);
                std::string v_str;
                // first get string format of number without sign
                if (is_negative) {
                    if (v == min_val) {
                        v_str = min_val_str_no_sign;
                    } else {
                        v_str = dt.to_string(-v);
                    }
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
                    data_pairs.emplace_back(v_str, v);
                }
            }
            /*
            std::string dbg_str;
            if (!with_spaces) {
                for (const auto& p : data_set) {
                    dbg_str += "|" + any_cast<ut_type::STRING>(p.first[0]) + "|, ";
                }
                std::cout << "test cast from string to int, data set: " << dbg_str << std::endl;
            }
            */
            check_function_for_cast<DataTypeNumber<PType>, true>(input_types, data_set, -1, -1);
            check_function_for_cast<DataTypeNumber<PType>, false>(input_types, data_set, -1, -1);
        };
        // test leading and trailing white spaces, sign and leading zeros
        tmp_test_func(true, true, true);
        // test leading and trailing spaces and sign
        tmp_test_func(true, true, false);
        // test leading and trailing spaces and leading zeros
        tmp_test_func(true, false, true);
        // test leading and trailing spaces
        tmp_test_func(true, false, false);
        // test with sign and leading zeros
        tmp_test_func(false, true, true);
        // test with sign
        tmp_test_func(false, true, false);
        // test only leading zeros
        tmp_test_func(false, false, true);
        // test strict digits
        tmp_test_func(false, false, false);
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
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(PType);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_str");
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

    template <PrimitiveType PT>
    void from_string_with_fraction_part_test_func() {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        using T = typename PrimitiveTypeTraits<PT>::CppType;
        using UnsignedT = typename std::make_unsigned<T>::type;
        DataTypeNumber<PT> dt;
        auto max_val = std::numeric_limits<T>::max();
        auto min_val = std::numeric_limits<T>::min();
        auto max_val_minus_1 = max_val - T {1};
        auto min_val_plus_1 = min_val + T {1};
        auto min_val_abs_val = UnsignedT(max_val) + 1;
        std::string min_val_str_no_sign = fmt::format("{}", min_val_abs_val);
        std::vector<T> test_vals = {T {0},  T {1},  T {9},    T {123}, max_val,
                                    T {-1}, T {-9}, T {-123}, min_val};
        test_vals.push_back(max_val_minus_1);
        test_vals.push_back(min_val_plus_1);

        std::vector<std::pair<std::string, T>> data_pairs;
        std::string table_test_expected_results;
        int data_index = 0;

        auto test_func_with_strict_cast = [&](auto enable_strict_cast) {
            // std::cout << fmt::format("test cast from string to {}, enable_strict_cast: {}\n",
            //                          type_to_string(PT), enable_strict_cast);
            // test leading zeros, sign, leading and trailing white spaces
            auto tmp_test_func = [&](bool with_spaces, bool with_sign, bool leading_zeros) {
                DataSet data_set;
                for (auto v : test_vals) {
                    bool is_negative = (v < 0);
                    std::string v_str;
                    // first get string format of number without sign
                    if (is_negative) {
                        if (v == min_val) {
                            v_str = min_val_str_no_sign;
                        } else {
                            v_str = dt.to_string(-v);
                        }
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
                    if (enable_strict_cast) {
                        std::vector<std::string> test_strs;
                        if (with_spaces) {
                            test_strs.push_back(white_spaces_str + v_str + ".4");
                            test_strs.push_back(white_spaces_str + v_str + ".5");

                            test_strs.push_back(v_str + ".4" + white_spaces_str);
                            test_strs.push_back(v_str + ".5" + white_spaces_str);

                            test_strs.push_back(white_spaces_str + v_str + ".4" + white_spaces_str);
                            test_strs.push_back(white_spaces_str + v_str + ".5" + white_spaces_str);
                        } else {
                            test_strs.push_back(v_str + ".4");
                            test_strs.push_back(v_str + ".5");
                        }
                        for (const auto& v_str : test_strs) {
                            DataSet tmp_data_set;
                            tmp_data_set.push_back({{v_str}, Null()});
                            check_function_for_cast<DataTypeNumber<PT>, true>(
                                    input_types, tmp_data_set, -1, -1, true, true);
                        }
                    } else {
                        if (with_spaces) {
                            data_set.push_back({{white_spaces_str + v_str + ".4"}, v});
                            data_set.push_back({{white_spaces_str + v_str + ".5"}, v});
                            data_set.push_back({{v_str + ".4" + white_spaces_str}, v});
                            data_set.push_back({{v_str + ".5" + white_spaces_str}, v});
                            data_set.push_back(
                                    {{white_spaces_str + v_str + ".4" + white_spaces_str}, v});
                            data_set.push_back(
                                    {{white_spaces_str + v_str + ".5" + white_spaces_str}, v});
                        } else {
                            data_set.push_back({{v_str + ".4"}, v});
                            data_set.push_back({{v_str + ".5"}, v});

                            data_pairs.emplace_back(v_str + ".4", v);
                            table_test_expected_results += fmt::format("{}\t{}\n", data_index++, v);
                            data_pairs.emplace_back(v_str + ".5", v);
                            table_test_expected_results += fmt::format("{}\t{}\n", data_index++, v);
                        }
                    }
                }
                if (!enable_strict_cast) {
                    /*
                    std::string dbg_str;
                    for (const auto& p : data_set) {
                        dbg_str += "|" + any_cast<ut_type::STRING>(p.first[0]) + "|, ";
                    }
                    std::cout << fmt::format(
                            "test cast from string to {}, enable_strict_cast: {}, data set: {}\n",
                            type_to_string(PT), enable_strict_cast, dbg_str);
                    */
                    check_function_for_cast<DataTypeNumber<PT>, false>(input_types, data_set, -1,
                                                                       -1);
                }
            };
            // test leading and trailing white spaces, sign and leading zeros
            tmp_test_func(true, true, true);
            // test leading and trailing spaces and sign
            tmp_test_func(true, true, false);
            // test leading and trailing spaces and leading zeros
            tmp_test_func(true, false, true);
            // test leading and trailing spaces
            tmp_test_func(true, false, false);
            // test with sign and leading zeros
            tmp_test_func(false, true, true);
            // test with sign
            tmp_test_func(false, true, false);
            // test only leading zeros
            tmp_test_func(false, false, true);
            // test strict digits
            tmp_test_func(false, false, false);
        };
        test_func_with_strict_cast(true);
        test_func_with_strict_cast(false);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string cast_type_name = get_sql_type_name(PT);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_str_with_fraction", cast_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";

            auto table_name = fmt::format("{}_{}", regression_case_name, table_index);

            auto value_count = data_pairs.size();
            auto groovy_var_name = fmt::format("{}_{}_strs", regression_case_name, table_index);
            (*ofs_const_case) << fmt::format("    def {} = [", groovy_var_name);
            int i = 0;
            for (const auto& data : data_pairs) {
                (*ofs_const_case) << fmt::format(R"("""{}""")", data.first);
                ++i;
                if (i != value_count) {
                    (*ofs_const_case) << ",";
                }
                if (i % 20 == 0 && i != value_count) {
                    (*ofs_const_case) << "\n        ";
                }
            }
            (*ofs_const_case) << fmt::format("]\n");

            auto const_test_with_strict_arg = [&](bool enable_strict_cast) {
                (*ofs_const_case) << fmt::format("\n    sql \"set enable_strict_cast={};\"\n",
                                                 enable_strict_cast);
                if (enable_strict_cast) {
                    (*ofs_const_case) << fmt::format(R"(
    for (b in ["false", "true"]) {{
        sql """set debug_skip_fold_constant = "${{b}}";"""
        for (test_str in {}) {{
            test {{
                sql """select cast("${{test_str}}" as {});"""
                exception "{}"
            }}
        }}
    }}
)",
                                                     groovy_var_name, cast_type_name, "");

                } else {
                    (*ofs_const_case) << fmt::format(R"(
    for (test_str in {}) {{
        qt_sql_{} """select cast("${{test_str}}" as {});"""
        testFoldConst("""select cast("${{test_str}}" as {});""")
    }}
)",
                                                     groovy_var_name, table_name, cast_type_name,
                                                     cast_type_name);
                    for (int i = 0; i != value_count; ++i) {
                        (*ofs_const_expected_result) << fmt::format("-- !sql_{} --\n", table_name);
                        (*ofs_const_expected_result) << fmt::format("{}\n\n", data_pairs[i].second);
                    }
                }
            };
            const_test_with_strict_arg(true);
            const_test_with_strict_arg(false);

            (*ofs_case) << fmt::format("    sql \"drop table if exists {};\"\n", table_name);
            (*ofs_case) << fmt::format(
                    "    sql \"create table {}(f1 int, f2 string) "
                    "properties('replication_num'='1');\"\n",
                    table_name);
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
                if (enable_strict_cast) {
                    (*ofs_case) << fmt::format(R"(
    def {}_data_start_index = {}
    def {}_data_end_index = {}
    for (int data_index = {}_data_start_index; data_index < {}_data_end_index; data_index++) {{
        test {{
            sql "select f1, cast(f2 as {}) from {} where f1 = ${{data_index}}"
            exception "{}"
        }}
    }}
)",
                                               regression_case_name, 0, regression_case_name,
                                               value_count, regression_case_name,
                                               regression_case_name, cast_type_name,
                                               regression_case_name, "");

                } else {
                    (*ofs_case) << fmt::format(
                            "    qt_sql_{}_{} 'select f1, cast(f2 as {}) from {} "
                            "order by "
                            "1;'\n\n",
                            table_index, "non_strict", cast_type_name, table_name);

                    (*ofs_expected_result)
                            << fmt::format("-- !sql_{}_{} --\n", table_index, "non_strict");
                    (*ofs_expected_result) << table_test_expected_results;
                    (*ofs_expected_result) << "\n";
                }
            };
            table_test_with_strict_arg(true);
            table_test_with_strict_arg(false);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType PT>
    void from_string_overflow_test_func() {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
        using T = typename PrimitiveTypeTraits<PT>::CppType;
        using UnsignedT = typename std::make_unsigned<T>::type;
        DataTypeNumber<PT> dt;
        UnsignedT max_val = std::numeric_limits<T>::max();

        std::vector<std::string> test_vals;
        std::string dbg_str0 =
                fmt::format("test cast from string to {} overflow", type_to_string(PT));
        for (UnsignedT i = 1; i != 11; ++i) {
            auto val_str = fmt::format("{}", max_val + i);
            test_vals.push_back(val_str);

            val_str = fmt::format("{}", max_val + i + 1);
            val_str.insert(0, "-");
            test_vals.push_back(val_str);
        }
        int count = 0;
        for (auto i = std::numeric_limits<UnsignedT>::max(); count < 10; --i, ++count) {
            auto val_str = fmt::format("{}", i);
            test_vals.push_back(val_str);

            val_str.insert(0, "-");
            test_vals.push_back(val_str);
        }
        if constexpr (std::is_same_v<T, Int8>) {
            test_vals.push_back(std::to_string(std::numeric_limits<Int16>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int16>::min()));

            test_vals.push_back(std::to_string(std::numeric_limits<Int32>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int32>::min()));

            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::min()));

            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::max()));
            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::min()));
        } else if constexpr (std::is_same_v<T, Int16>) {
            test_vals.push_back(std::to_string(std::numeric_limits<Int32>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int32>::min()));

            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::min()));

            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::max()));
            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::min()));
        } else if constexpr (std::is_same_v<T, Int32>) {
            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::max()));
            test_vals.push_back(std::to_string(std::numeric_limits<Int64>::min()));

            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::max()));
            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::min()));
        } else if constexpr (std::is_same_v<T, Int64>) {
            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::max()));
            test_vals.push_back(fmt::format("{}", std::numeric_limits<Int128>::min()));
        }
        test_vals.push_back(fmt::format("{}", std::numeric_limits<wide::Int256>::max()));
        test_vals.push_back(fmt::format("{}", std::numeric_limits<wide::Int256>::min()));

        auto test_func_with_strict_cast = [&](auto enable_strict_cast) {
            // std::cout << fmt::format("{}, enable_strict_cast: {}", dbg_str0, enable_strict_cast);
            // test leading zeros, sign, leading and trailing white spaces for positive values
            auto tmp_test_func = [&](bool with_spaces, bool with_sign, bool leading_zeros) {
                DataSet data_set;
                for (auto v_str : test_vals) {
                    bool is_negative = (v_str[0] == '-');
                    if (is_negative) {
                        v_str = v_str.substr(1);
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
                        if (enable_strict_cast) {
                            DataSet tmp_data_set;
                            tmp_data_set.push_back(
                                    {{white_spaces_str + v_str + white_spaces_str}, Null()});
                            check_function_for_cast<DataTypeNumber<PT>, true>(
                                    input_types, tmp_data_set, -1, -1, true, true);
                        } else {
                            data_set.push_back(
                                    {{white_spaces_str + v_str + white_spaces_str}, Null()});
                        }
                    } else {
                        if (enable_strict_cast) {
                            DataSet tmp_data_set;
                            tmp_data_set.push_back({{v_str}, Null()});
                            check_function_for_cast<DataTypeNumber<PT>, true>(
                                    input_types, tmp_data_set, -1, -1, true, true);
                        } else {
                            data_set.push_back({{v_str}, Null()});
                        }
                    }
                }
                if (!enable_strict_cast) {
                    /*
                    std::string dbg_str = dbg_str0 + ", data set: ";
                    for (const auto& p : data_set) {
                        dbg_str += any_cast<ut_type::STRING>(p.first[0]) + ", ";
                    }
                    std::cout << dbg_str << std::endl;
                    */
                    check_function_for_cast<DataTypeNumber<PT>, false>(input_types, data_set, -1,
                                                                       -1);
                }
            };
            // test leading and trailing white spaces, sign and leading zeros
            tmp_test_func(true, true, true);
            // test leading and trailing spaces and sign
            tmp_test_func(true, true, false);
            // test leading and trailing spaces and leading zeros
            tmp_test_func(true, false, true);
            // test leading and trailing spaces
            tmp_test_func(true, false, false);
            // test with sign and leading zeros
            tmp_test_func(false, true, true);
            // test with sign
            tmp_test_func(false, true, false);
            // test only leading zeros
            tmp_test_func(false, false, true);
            // test strict digits
            tmp_test_func(false, false, false);
        };
        test_func_with_strict_cast(true);
        test_func_with_strict_cast(false);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(PT);
            std::string regression_case_name = fmt::format("test_cast_to_{}_from_{}_overflow",
                                                           to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_str");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name,
                                                     false, to_sql_type_name, test_vals,
                                                     table_index++, ofs_case, ofs_expected_result,
                                                     ofs_const_case, ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType PT>
    void from_string_invalid_test_func() {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        std::vector<std::string> test_vals;
        std::string dbg_str0 =
                fmt::format("test cast from string to {} abnormal cases", type_to_string(PT));
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
                // does not support scientific notation
                "1.234e3",
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
        };
        // non-strict mode
        {
            DataSet data_set;
            for (const auto& input : abnormal_inputs) {
                data_set.push_back({{input}, Null()});
            }
            check_function_for_cast<DataTypeNumber<PT>, false>(input_types, data_set, -1, -1);
        }

        // strict mode
        for (const auto& input : abnormal_inputs) {
            DataSet data_set;
            data_set.push_back({{input}, Null()});
            check_function_for_cast<DataTypeNumber<PT>, true>(input_types, data_set, -1, -1, true,
                                                              true);
        }
        if (FLAGS_gen_regression_case) {
            std::vector<std::string> regression_invalid_inputs = {
                    "",
                    ".",
                    " ",
                    "\t",
                    "abc",
                    "abc123"
                    "1abc12"
                    "123abc",
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
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = "string";
            std::string to_sql_type_name = get_sql_type_name(PT);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_str_invalid", to_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_str");
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
        check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_type_name = get_sql_type_name(FromPT);
            std::string to_type_name = get_sql_type_name(ToPT);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_type_name, from_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_int");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            gen_normal_regression_case(regression_case_name, from_type_name, true, to_type_name,
                                       test_vals, table_index++, test_data_index, ofs_case,
                                       ofs_expected_result, ofs_const_case,
                                       ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
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
        check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_type_name = get_sql_type_name(FromPT);
            std::string to_type_name = get_sql_type_name(ToPT);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_type_name, from_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_int");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            gen_normal_regression_case(regression_case_name, from_type_name, true, to_type_name,
                                       test_vals, table_index++, test_data_index, ofs_case,
                                       ofs_expected_result, ofs_const_case,
                                       ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void wider_to_narrow_int_overflow_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(sizeof(FromT) > sizeof(ToT), "FromT must be larger than ToT");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};
        constexpr auto to_max_val = std::numeric_limits<ToT>::max();
        constexpr auto to_min_val = std::numeric_limits<ToT>::min();
        std::vector<FromT> test_input_vals = {
                static_cast<FromT>(to_max_val) + 1,
                static_cast<FromT>(to_min_val) - 1,
                std::numeric_limits<FromT>::max(),
                std::numeric_limits<FromT>::min(),
        };
        // non strict mode
        {
            DataSet data_set;
            for (auto v : test_input_vals) {
                data_set.push_back({{v}, Null()});
            }
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        }

        // strict mode
        {
            for (auto v : test_input_vals) {
                DataSet data_set;
                data_set.push_back({{v}, Null()});
                check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1,
                                                                    true, true);
            }
        }
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = get_sql_type_name(ToPT);
            std::string regression_case_name = fmt::format("test_cast_to_{}_from_{}_overflow",
                                                           to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_int");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();
            gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name, true,
                                                     to_sql_type_name, test_input_vals,
                                                     table_index++, ofs_case, ofs_expected_result,
                                                     ofs_const_case, ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void from_float_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<FromT>::is_iec559, "FromT must be a floating point type");
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};
        // std::cout << fmt::format("test cast {} to {}\n", type_to_string(FromPT),
        //                          type_to_string(ToPT));

        DataSet data_set = {
                // Zero and sign
                {{FromT {0.0}}, ToT(0)},
                {{FromT {-0.0}}, ToT(0)},
                {{FromT {+0.0}}, ToT(0)},

                // Positive integers
                {{FromT {1.0}}, ToT(1)},
                {{FromT {9.0}}, ToT(9)},
                {{FromT {123.0}}, ToT(123)},
                {{FromT {127.9}}, ToT(127)},

                // Negative integers
                {{FromT {-1.0}}, ToT(-1)},
                {{FromT {-9.0}}, ToT(-9)},
                {{FromT {-123.0}}, ToT(-123)},
                {{FromT {-128.9}}, ToT(-128)},
                {{static_cast<FromT>(std::numeric_limits<ToT>::min())},
                 static_cast<ToT>(static_cast<FromT>(std::numeric_limits<ToT>::min()))},

                // Fractional values (truncate toward zero)
                {{FromT {1.9}}, ToT(1)},
                {{FromT {-1.9}}, ToT(-1)},
                {{FromT {0.9999}}, ToT(0)},
                {{FromT {-0.9999}}, ToT(0)},

                // Subnormal (denormalized) numbers
                {{FromT {std::numeric_limits<FromT>::denorm_min()}}, ToT(0)},
                {{FromT {-std::numeric_limits<FromT>::denorm_min()}}, ToT(0)},
        };
        switch (ToPT) {
        case TYPE_LARGEINT:
            data_set.push_back({{FromT(std::pow(2, 64))}, ToT(std::pow(2, 64))});
            data_set.push_back({{FromT(-std::pow(2, 64))}, ToT(-std::pow(2, 64))});

            data_set.push_back({{FromT(std::pow(2, 126))}, ToT(std::pow(2, 126))});
            data_set.push_back({{FromT(-std::pow(2, 126))}, ToT(-std::pow(2, 126))});
            [[fallthrough]];
        case TYPE_BIGINT:
            data_set.push_back({{FromT(std::pow(2, 32))}, ToT(std::pow(2, 32))});
            data_set.push_back({{FromT(-std::pow(2, 32))}, ToT(-std::pow(2, 32))});

            data_set.push_back({{FromT(std::pow(2, 62))}, ToT(std::pow(2, 62))});
            data_set.push_back({{FromT(-std::pow(2, 62))}, ToT(-std::pow(2, 62))});
            if constexpr (FromPT == TYPE_DOUBLE) {
                // 9223372036854775295 == 2^63 - 2^9 - 1
                data_set.push_back({{FromT(9223372036854775295.0)}, ToT(9223372036854774784)});

                // 9223372036854776832 == 2^63 + 2^10
                data_set.push_back({{FromT(-9223372036854776832.0)},
                                    ToT(std::numeric_limits<int64_t>::min())});
            } else {
                // 9223371761976868863 == 2^63 - 2^38 - 1
                data_set.push_back({{FromT(9223371761976868863.0f)}, ToT(9223371487098961920)});
                // 9223372586610589696 == 2^63 + 2^39
                data_set.push_back({{FromT(-9223372586610589696.0f)},
                                    ToT(std::numeric_limits<int64_t>::min())});
            }
            [[fallthrough]];
        case TYPE_INT:
            data_set.push_back({{FromT {32768.9}}, ToT(32768)});
            data_set.push_back({{FromT {-32769.9}}, ToT(-32769)});

            data_set.push_back({{FromT {999999.9}}, ToT(999999)});
            data_set.push_back({{FromT {-999999.9}}, ToT(-999999)});

            data_set.push_back({{FromT(std::pow(2, 30))}, ToT(std::pow(2, 30))});
            data_set.push_back({{FromT(-std::pow(2, 30))}, ToT(-std::pow(2, 30))});

            if constexpr (FromPT == TYPE_DOUBLE) {
                // 2147483647 == 2^31 - 1
                data_set.push_back({{FromT(2147483647.0)}, ToT(2147483647)});

                // 2147483648 == 2^31
                data_set.push_back(
                        {{FromT(-2147483648.0)}, ToT(std::numeric_limits<int32_t>::min())});
            } else {
                // 2147483583 == 2^31 - 2^6 - 1
                data_set.push_back({{FromT(2147483583.0f)}, ToT(2147483520)});
                // 2147483776 == 2^31 + 2^7
                data_set.push_back({{FromT(-2147483776.0f)}, ToT(-2147483648)});
            }
            [[fallthrough]];
        case TYPE_SMALLINT:
            data_set.push_back({{FromT {32767.9}}, ToT(32767)});
            data_set.push_back({{FromT {-32768.9}}, ToT(-32768)});

            // 32767 == 2^15 - 1
            data_set.push_back({{FromT(32767.0)}, ToT(32767)});
            // 32768 == 2^15
            data_set.push_back({{FromT(-32768.0)}, ToT(std::numeric_limits<int16_t>::min())});
            break;
        default:
            break;
        }
        check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);

        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            int test_data_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = get_sql_type_name(ToPT);
            std::string regression_case_name =
                    fmt::format("test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(
                    regression_case_name, ofs_const_case_uptr, ofs_const_expected_result_uptr,
                    ofs_case_uptr, ofs_expected_result_uptr, "to_int/from_float", false, true);
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            std::vector<std::pair<std::string, ToT>> regression_test_data_set;
            for (const auto& p : data_set) {
                regression_test_data_set.push_back(
                        {fmt::format("{}", any_cast<FromT>(p.first[0])), any_cast<ToT>(p.second)});
            }
            gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                       to_sql_type_name, regression_test_data_set, table_index++,
                                       test_data_index, ofs_case, ofs_expected_result,
                                       ofs_const_case, ofs_const_expected_result, false, true);
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void from_float_overflow_test_func() {
        using FromT = typename PrimitiveTypeTraits<FromPT>::CppType;
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        DataTypeNumber<FromPT> dt;
        InputTypeSet input_types = {dt.get_primitive_type()};
        std::vector<FromT> test_input_vals = {
                std::numeric_limits<FromT>::max(),
                std::numeric_limits<FromT>::lowest(),
                std::numeric_limits<FromT>::infinity(),
                std::numeric_limits<FromT>::quiet_NaN(),
        };
        switch (ToPT) {
        case TYPE_TINYINT:
            test_input_vals.push_back(FromT(128.0));
            test_input_vals.push_back(FromT(-129.0));
            [[fallthrough]];
        case TYPE_SMALLINT:
            test_input_vals.push_back(FromT(32768.0));
            test_input_vals.push_back(FromT(-32769.0));

            // 32768 == 2^15
            test_input_vals.push_back(FromT(32768.0));
            // 32769 == 2^15 + 1
            test_input_vals.push_back(FromT(-32769.0));
            [[fallthrough]];
        case TYPE_INT:
            test_input_vals.push_back(FromT(std::pow(2, 32)));
            test_input_vals.push_back(FromT(-std::pow(2, 32)));

            if constexpr (FromPT == TYPE_DOUBLE) {
                // 2147483647 == 2^31
                test_input_vals.push_back(FromT(2147483648.0));
                // 2147483648 == 2^31 + 1
                test_input_vals.push_back(FromT(-2147483649.0));
            } else {
                // 2147483584 = 2^31 - 2^6
                test_input_vals.push_back(FromT(2147483584.0f));
                // 2147483777 = 2^31 + 2^7 + 1
                test_input_vals.push_back(FromT(-2147483777.0f));
            }
            [[fallthrough]];
        case TYPE_BIGINT:
            test_input_vals.push_back(FromT(std::pow(2, 64)));
            test_input_vals.push_back(FromT(-std::pow(2, 64)));
            if constexpr (FromPT == TYPE_DOUBLE) {
                // 9223372036854775296 == 2^63 - 2^9
                test_input_vals.push_back(FromT(9223372036854775296.0));
                // 9223372036854776833 == 2^63 + 2^10 + 1
                test_input_vals.push_back(FromT(-9223372036854776833.0));
            } else {
                // 9223371761976868864 == 2^63 - 2^38
                test_input_vals.push_back(FromT(9223371761976868864.0f));
                // 92233725866105896967 = 2^63 + 2^39 + 1
                test_input_vals.push_back(FromT(-9223372586610589697.0f));
            }
            [[fallthrough]];
        case TYPE_LARGEINT:
            test_input_vals.push_back(FromT(std::pow(2, 128)));
            test_input_vals.push_back(FromT(-std::pow(2, 128)));

            if constexpr (FromPT == TYPE_DOUBLE) {
                test_input_vals.push_back(FromT(std::pow(2, 129)));
                test_input_vals.push_back(FromT(-std::pow(2, 129)));
                test_input_vals.push_back(FromT(170141183460469250621153235194464960513.0));
                test_input_vals.push_back(FromT(-170141183460469250621153235194464960513.0));
            }
            [[fallthrough]];
        default:
            break;
        }

        // non strict mode
        {
            DataSet data_set;
            for (auto v : test_input_vals) {
                data_set.push_back({{v}, Null()});
            }
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        }

        // strict mode
        {
            for (auto v : test_input_vals) {
                DataSet data_set;
                data_set.push_back({{v}, Null()});
                check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1,
                                                                    true, true);
            }
        }
        if (FLAGS_gen_regression_case) {
            int table_index = 0;
            std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
            std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
            std::string from_sql_type_name = get_sql_type_name(FromPT);
            std::string to_sql_type_name = get_sql_type_name(ToPT);
            std::string regression_case_name = fmt::format("test_cast_to_{}_from_{}_overflow",
                                                           to_sql_type_name, from_sql_type_name);
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_float");
            auto* ofs_const_case = ofs_const_case_uptr.get();
            auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
            auto* ofs_case = ofs_case_uptr.get();
            auto* ofs_expected_result = ofs_expected_result_uptr.get();

            std::vector<std::string> regression_test_data_set;
            for (const auto& v : test_input_vals) {
                regression_test_data_set.push_back(fmt::format("{}", v));
            }
            gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name, true,
                                                     to_sql_type_name, regression_test_data_set,
                                                     table_index++, ofs_case, ofs_expected_result,
                                                     ofs_const_case, ofs_const_expected_result);
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <typename FromT, PrimitiveType ToPT>
    void from_decimal_with_p_s_no_overflow_test_func(int FromPrecision, int FromScale) {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(IsDecimalNumber<FromT>, "FromT must be a decimal type");
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        // static_assert(FromPrecision - FromScale >= 0 &&
        //                       FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>(),
        //               "Decimal integral part must be less than integer max ascii len");

        DataTypeDecimal<FromT::PType> dt_from =
                get_decimal_data_type<FromT>(FromPrecision, FromScale);
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
        auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        auto from_large_integral1 = decimal_scale_multiplier<typename FromT::NativeType>(
                                            FromPrecision - FromScale - 1) -
                                    1;
        auto from_large_integral2 = from_max_integral - from_large_integral1;
        auto from_large_integral3 =
                from_large_integral2 > 9 ? from_large_integral2 + 1 : from_large_integral2 - 1;
        // constexpr auto min_integral = -from_max_integral;
        // std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;
        // std::cout << "from_large_integral1:\t" << fmt::format("{}", from_large_integral1)
        //           << std::endl;
        // std::cout << "from_large_integral2:\t" << fmt::format("{}", from_large_integral2)
        //           << std::endl;
        // std::cout << "from_large_integral3:\t" << fmt::format("{}", from_large_integral3)
        //           << std::endl;

        // from_max_fractional:    99999999
        // from_large_fractional1: 9999999
        // from_large_fractional2: 90000000
        // from_large_fractional3: 90000001
        auto from_max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale) - 1;
        auto from_large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale - 1) - 1;
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
        // constexpr auto min_fractional = -from_max_fractional;
        std::vector<IntType> integral_part = {
                0,
                1,
                9,
        };
        // e.g. from Decimal(9, 0) to int16
        if (FromPrecision - FromScale >= NumberTraits::max_ascii_len<ToT>()) {
            integral_part.push_back(std::numeric_limits<IntType>::max());
            integral_part.push_back(std::numeric_limits<IntType>::max() - 1);
            integral_part.push_back(std::numeric_limits<IntType>::min());
            integral_part.push_back(std::numeric_limits<IntType>::min() + 1);
        } else {
            integral_part.push_back(from_max_integral);
            if (FromPrecision - FromScale > 0) {
                integral_part.push_back(from_max_integral - 1);
                integral_part.push_back(from_large_integral1);
                integral_part.push_back(from_large_integral2);
                integral_part.push_back(from_large_integral3);
            }
        }
        std::vector<typename FromT::NativeType> fractional_part = {0,
                                                                   1,
                                                                   9,
                                                                   from_max_fractional,
                                                                   from_max_fractional - 1,
                                                                   from_large_fractional1,
                                                                   from_large_fractional2,
                                                                   from_large_fractional3};
        DataSet data_set;
        std::string dbg_str =
                fmt::format("test cast {}({}, {}) to {}: \n", type_to_string(FromT::PType),
                            FromPrecision, FromScale, dt_to.get_family_name());
        // std::cout << dbg_str << std::endl;

        std::vector<std::pair<std::string, ToT>> test_data_set;
        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                int table_index = 0;
                int test_data_index = 0;
                std::string from_decimal_sql_type_name =
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimalv3";
                std::string from_sql_type_name = fmt::format(
                        "{}({}, {})", from_decimal_sql_type_name, FromPrecision, FromScale);
                std::string to_sql_type_name = get_sql_type_name(ToPT);

                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string regression_case_name = fmt::format(
                        "test_cast_to_{}_from_{}_{}_{}", to_sql_type_name,
                        to_lower(type_to_string(FromT::PType)), FromPrecision, FromScale);
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_int/from_decimal");
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
        if (FromScale == 0) {
            // e.g. Decimal(9, 0), only int part
            for (const auto& i : integral_part) {
                auto decimal_num = decimal_ctor(i, 0, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("({}, {})|", num_str, i);
                data_set.push_back({{decimal_num}, IntType(i)});
                test_data_set.emplace_back(num_str, i);

                if (FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>()) {
                    decimal_num = decimal_ctor(-i, 0, FromScale);
                    num_str = dt_from.to_string(decimal_num);
                    // dbg_str += fmt::format("({}, {})|", num_str, -i);
                    data_set.push_back({{decimal_num}, IntType(-i)});
                    test_data_set.emplace_back(num_str, -i);
                }
            }
            // dbg_str += "\n";
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
            check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);
            return;
        } else if (FromScale == FromPrecision) {
            // e.g. Decimal(9, 9), only fraction part
            for (const auto& f : fractional_part) {
                auto decimal_num = decimal_ctor(0, f, FromScale);
                auto num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("({}, {})|", num_str, 0);
                data_set.push_back({{decimal_num}, IntType(0)});
                test_data_set.emplace_back(num_str, 0);

                decimal_num = decimal_ctor(0, -f, FromScale);
                num_str = dt_from.to_string(decimal_num);
                // dbg_str += fmt::format("({}, {})|", num_str, 0);
                data_set.push_back({{decimal_num}, IntType(0)});
                test_data_set.emplace_back(num_str, 0);
            }
            // dbg_str += "\n";
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
            check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);
            return;
        }

        for (const auto& i : integral_part) {
            for (const auto& f : fractional_part) {
                if (FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>()) {
                    auto decimal_num = decimal_ctor(i, f, FromScale);
                    auto num_str = dt_from.to_string(decimal_num);
                    // dbg_str += fmt::format("({}, {})|", num_str, i);
                    data_set.push_back({{decimal_num}, IntType(i)});
                    test_data_set.emplace_back(num_str, i);

                    decimal_num = decimal_ctor(-i, -f, FromScale);
                    num_str = dt_from.to_string(decimal_num);
                    // dbg_str += fmt::format("({}, {})|", num_str, -i);
                    data_set.push_back({{decimal_num}, IntType(-i)});
                    test_data_set.emplace_back(num_str, -i);
                } else {
                    if (i >= 0) {
                        auto decimal_num = decimal_ctor(i, f, FromScale);
                        auto num_str = dt_from.to_string(decimal_num);
                        // dbg_str += fmt::format("({}, {})|", num_str, i);
                        data_set.push_back({{decimal_num}, IntType(i)});
                        test_data_set.emplace_back(num_str, i);
                    } else {
                        auto decimal_num = decimal_ctor(i, -f, FromScale);
                        auto num_str = dt_from.to_string(decimal_num);
                        // dbg_str += fmt::format("({}, {})|", num_str, i);
                        data_set.push_back({{decimal_num}, IntType(i)});
                        test_data_set.emplace_back(num_str, i);
                    }
                }
            }
            // dbg_str += "\n";
        }
        check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1);
    }

    template <typename FromT, PrimitiveType ToT>
    void from_decimal_to_int_test_func() {
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
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(1, 0);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(1, 1);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(27, 9);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(20, 6);
        } else {
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(min_decimal_pre, 0);
            if constexpr (min_decimal_pre != 1) {
                from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(min_decimal_pre,
                                                                        min_decimal_pre / 2);
                from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(min_decimal_pre,
                                                                        min_decimal_pre - 1);
            }
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(min_decimal_pre,
                                                                    min_decimal_pre);

            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(max_decimal_pre, 0);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(max_decimal_pre, 1);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(max_decimal_pre,
                                                                    max_decimal_pre / 2);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(max_decimal_pre,
                                                                    max_decimal_pre - 1);
            from_decimal_with_p_s_no_overflow_test_func<FromT, ToT>(max_decimal_pre,
                                                                    max_decimal_pre);
        }
    }

    template <typename FromT, PrimitiveType ToPT>
    void from_decimal_with_p_s_overflow_test_func(int FromPrecision, int FromScale,
                                                  const std::string& regression_case_name,
                                                  int table_index, std::ofstream* ofs_case,
                                                  std::ofstream* ofs_expected_result,
                                                  std::ofstream* ofs_const_case,
                                                  std::ofstream* ofs_const_expected_result) {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(IsDecimalNumber<FromT>, "FromT must be a decimal type");
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        if (FromPrecision - FromScale < NumberTraits::max_ascii_len<ToT>()) {
            return;
        }

        DataTypeDecimal<FromT::PType> dt_from =
                get_decimal_data_type<FromT>(FromPrecision, FromScale);
        InputTypeSet input_types = {{dt_from.get_primitive_type(), FromScale, FromPrecision}};
        auto decimal_ctor = get_decimal_ctor<FromT>();

        using IntType = ToT;
        DataTypeNumber<ToPT> dt_to;

        // from_max_integral:    99999999
        auto from_max_integral =
                decimal_scale_multiplier<typename FromT::NativeType>(FromPrecision - FromScale) - 1;
        // constexpr auto min_integral = -from_max_integral;
        // std::cout << "from_max_integral:\t" << fmt::format("{}", from_max_integral) << std::endl;

        // from_max_fractional:    99999999
        // from_large_fractional1: 9999999
        // from_large_fractional2: 90000000
        // from_large_fractional3: 90000001
        auto from_max_fractional =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale) - 1;
        auto from_large_fractional1 =
                decimal_scale_multiplier<typename FromT::NativeType>(FromScale - 1) - 1;
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
        std::vector<typename FromT::NativeType> integral_part;
        if constexpr (sizeof(typename FromT::NativeType) > sizeof(IntType)) {
            integral_part.push_back(
                    static_cast<typename FromT::NativeType>(std::numeric_limits<IntType>::max()) +
                    1);
            integral_part.push_back(
                    static_cast<typename FromT::NativeType>(std::numeric_limits<IntType>::min()) -
                    1);
        }
        integral_part.push_back(from_max_integral);
        integral_part.push_back(-from_max_integral);
        if (FromPrecision - FromScale > 0) {
            integral_part.push_back(from_max_integral - 1);
            integral_part.push_back(-from_max_integral + 1);
        }

        std::vector<typename FromT::NativeType> fractional_part = {0,
                                                                   1,
                                                                   9,
                                                                   from_max_fractional,
                                                                   from_max_fractional - 1,
                                                                   from_large_fractional1,
                                                                   from_large_fractional2,
                                                                   from_large_fractional3};
        std::string dbg_str =
                fmt::format("test cast {}({}, {}) to {}: ", type_to_string(FromT::PType),
                            FromPrecision, FromScale, dt_to.get_family_name());
        // std::cout << dbg_str << std::endl;

        std::vector<std::string> test_data_set;
        Defer defer {[&]() {
            if (FLAGS_gen_regression_case) {
                std::string from_decimal_sql_type_name =
                        FromT::PType == TYPE_DECIMALV2 ? "decimalv2" : "decimalv3";
                std::string from_sql_type_name = fmt::format(
                        "{}({}, {})", from_decimal_sql_type_name, FromPrecision, FromScale);
                std::string to_sql_type_name = get_sql_type_name(ToPT);
                gen_overflow_and_invalid_regression_case(regression_case_name, from_sql_type_name,
                                                         true, to_sql_type_name, test_data_set,
                                                         table_index, ofs_case, ofs_expected_result,
                                                         ofs_const_case, ofs_const_expected_result);
            }
        }};
        if (FromScale == 0) {
            // non strict mode
            {
                DataSet data_set;
                // e.g. Decimal(9, 0), only int part
                for (const auto& i : integral_part) {
                    auto decimal_num = decimal_ctor(i, 0, FromScale);
                    auto num_str = dt_from.to_string(decimal_num);
                    // dbg_str += fmt::format("({}, {})|", num_str, i);
                    data_set.push_back({{decimal_num}, Null()});
                    test_data_set.emplace_back(num_str);
                }
                check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
            }
            // strict mode
            {
                for (const auto& i : integral_part) {
                    DataSet data_set;
                    auto decimal_num = decimal_ctor(i, 0, FromScale);
                    data_set.push_back({{decimal_num}, Null()});
                    check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1,
                                                                        -1, true, true);
                }
            }
            return;
        }

        // non strict mode
        {
            DataSet data_set;
            for (const auto& i : integral_part) {
                for (const auto& f : fractional_part) {
                    if (i >= 0) {
                        auto decimal_num = decimal_ctor(i, f, FromScale);
                        auto num_str = dt_from.to_string(decimal_num);
                        // dbg_str += fmt::format("({}, {})|", num_str, i);
                        data_set.push_back({{decimal_num}, Null()});
                        test_data_set.emplace_back(num_str);
                    } else {
                        auto decimal_num = decimal_ctor(i, -f, FromScale);
                        auto num_str = dt_from.to_string(decimal_num);
                        // dbg_str += fmt::format("({}, {})|", num_str, i);
                        data_set.push_back({{decimal_num}, Null()});
                        test_data_set.emplace_back(num_str);
                    }
                }
                // dbg_str += "\n";
            }
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1);
        }
        // strict mode
        {
            for (const auto& i : integral_part) {
                for (const auto& f : fractional_part) {
                    DataSet data_set;
                    if (i >= 0) {
                        auto decimal_num = decimal_ctor(i, f, FromScale);
                        data_set.push_back({{decimal_num}, Null()});
                    } else {
                        auto decimal_num = decimal_ctor(i, -f, FromScale);
                        data_set.push_back({{decimal_num}, Null()});
                    }
                    check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1,
                                                                        -1, true, true);
                }
            }
        }
    }

    template <typename FromT, PrimitiveType ToT>
    void from_decimal_to_int_overflow_test_func() {
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

        int table_index = 0;
        std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
        std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
        std::string regression_case_name =
                fmt::format("test_cast_to_{}_from_{}_overflow", to_lower(type_to_string(ToT)),
                            to_lower(type_to_string(FromT::PType)));
        if (FLAGS_gen_regression_case) {
            setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                         ofs_const_expected_result_uptr, ofs_case_uptr,
                                         ofs_expected_result_uptr, "to_int/from_decimal");
        }
        auto* ofs_const_case = ofs_const_case_uptr.get();
        auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
        auto* ofs_case = ofs_case_uptr.get();
        auto* ofs_expected_result = ofs_expected_result_uptr.get();
        if (FLAGS_gen_regression_case) {
            if constexpr (IsDecimal256<FromT>) {
                (*ofs_const_case) << "    sql \"set enable_decimal256 = true;\"\n";
                (*ofs_case) << "    sql \"set enable_decimal256 = true;\"\n";
            }
        }

        if constexpr (std::is_same_v<FromT, Decimal128V2>) {
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    1, 0, regression_case_name, table_index++, ofs_case, ofs_expected_result,
                    ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    1, 1, regression_case_name, table_index++, ofs_case, ofs_expected_result,
                    ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    27, 9, regression_case_name, table_index++, ofs_case, ofs_expected_result,
                    ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    19, 6, regression_case_name, table_index++, ofs_case, ofs_expected_result,
                    ofs_const_case, ofs_const_expected_result);
        } else {
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    min_decimal_pre, 0, regression_case_name, table_index++, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            if constexpr (min_decimal_pre != 1) {
                from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                        min_decimal_pre, min_decimal_pre / 2, regression_case_name, table_index++,
                        ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
                from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                        min_decimal_pre, min_decimal_pre - 1, regression_case_name, table_index++,
                        ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            }
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    min_decimal_pre, min_decimal_pre, regression_case_name, table_index++, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);

            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    max_decimal_pre, 0, regression_case_name, table_index++, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    max_decimal_pre, 1, regression_case_name, table_index++, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    max_decimal_pre, max_decimal_pre / 2, regression_case_name, table_index++,
                    ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    max_decimal_pre, max_decimal_pre - 1, regression_case_name, table_index++,
                    ofs_case, ofs_expected_result, ofs_const_case, ofs_const_expected_result);
            from_decimal_with_p_s_overflow_test_func<FromT, ToT>(
                    max_decimal_pre, max_decimal_pre, regression_case_name, table_index++, ofs_case,
                    ofs_expected_result, ofs_const_case, ofs_const_expected_result);
        }
        if (FLAGS_gen_regression_case) {
            (*ofs_const_case) << "}";
            (*ofs_case) << "}";
        }
    }

    template <PrimitiveType ToPT>
    void from_date_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        using IntType = ToT;

        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        // int scale = 6;
        // test ordinary date time with microsecond: '2024-12-31 23:59:59.999999'
        std::vector<uint16_t> years = {0, 1, 10, 100, 2025, 9999};
        std::vector<uint8_t> months = {1, 12};
        std::vector<uint8_t> days = {1, 10, 28};
        DataTypeDateV2 dt;

        std::string to_sql_type_name = get_sql_type_name(ToPT);
        std::string dbg_str = fmt::format("test cast datev2 to {}: ", to_sql_type_name);
        // std::cout << dbg_str << std::endl;
        constexpr bool is_supported_int_type =
                (ToPT == TYPE_INT || ToPT == TYPE_BIGINT || ToPT == TYPE_LARGEINT);

        DataSet data_set;
        std::vector<std::pair<std::string, ToT>> regression_test_data_set;
        for (auto year : years) {
            for (auto month : months) {
                for (auto day : days) {
                    DateV2Value<DateV2ValueType> date_val(year, month, day, 0, 0, 0, 0);
                    IntType expect_cast_result = year * 10000 + month * 100 + day;
                    if constexpr (!is_supported_int_type) {
                        DataSet data_set_tmp;
                        data_set_tmp.push_back({{date_val}, expect_cast_result});
                        check_function_for_cast<DataTypeNumber<ToPT>, false>(
                                input_types, data_set_tmp, -1, -1, false, true);
                        check_function_for_cast<DataTypeNumber<ToPT>, true>(
                                input_types, data_set_tmp, -1, -1, false, true);
                    } else {
                        // dbg_str +=
                        //         fmt::format("({}, {})|", dt.to_string(date_val.to_date_int_val()),
                        //                     expect_cast_result);
                        data_set.push_back({{date_val}, expect_cast_result});
                        if (FLAGS_gen_regression_case) {
                            regression_test_data_set.push_back(
                                    {dt.to_string(date_val.to_date_int_val()), expect_cast_result});
                        }
                    }
                }
            }
            // dbg_str += "\n";
        }
        if constexpr (is_supported_int_type) {
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1,
                                                                 false);
            check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1,
                                                                false);
            if (FLAGS_gen_regression_case) {
                int table_index = 0;
                int test_data_index = 0;
                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string from_sql_type_name = "datev2";
                std::string regression_case_name = fmt::format(
                        "test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
                setup_regression_case_output(
                        regression_case_name, ofs_const_case_uptr, ofs_const_expected_result_uptr,
                        ofs_case_uptr, ofs_expected_result_uptr, "to_int/from_datetime", false);
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();

                gen_normal_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        regression_test_data_set, table_index++, test_data_index, ofs_case,
                        ofs_expected_result, ofs_const_case, ofs_const_expected_result, false);
                // (*ofs_const_case) << "}";
                (*ofs_case) << "}";
            }
        }
    }

    template <PrimitiveType ToPT>
    void from_datetime_test_func(int Scale) {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        using IntType = ToT;

        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, Scale}};
        std::vector<uint16_t> years = {0, 1, 10, 100, 2025, 9999};
        std::vector<uint8_t> months = {1, 12};
        std::vector<uint8_t> days = {1, 28};
        std::vector<uint8_t> hours = {0, 1, 23};
        std::vector<uint8_t> minutes = {0, 1, 59};
        std::vector<uint8_t> seconds = {0, 1, 59};
        std::vector<uint32_t> mircoseconds = {0, 1, 999999};
        DataTypeDateTimeV2 dt(Scale);
        DataSet data_set;
        std::vector<std::pair<std::string, ToT>> regression_test_data_set;
        constexpr bool is_supported_int_type = (ToPT == TYPE_BIGINT || ToPT == TYPE_LARGEINT);

        std::string to_sql_type_name = get_sql_type_name(ToPT);
        std::string dbg_str =
                fmt::format("test cast datetimev2({}) to {}: ", Scale, to_sql_type_name);
        // std::cout << dbg_str << std::endl;
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
                                    if constexpr (!is_supported_int_type) {
                                        DataSet data_set_tmp;
                                        data_set_tmp.push_back({{date_val}, expect_cast_result});
                                        check_function_for_cast<DataTypeNumber<ToPT>, false>(
                                                input_types, data_set_tmp, -1, -1, false, true);
                                        check_function_for_cast<DataTypeNumber<ToPT>, true>(
                                                input_types, data_set_tmp, -1, -1, false, true);
                                    } else {
                                        data_set.push_back({{date_val}, expect_cast_result});
                                        if (FLAGS_gen_regression_case) {
                                            regression_test_data_set.push_back(
                                                    {dt.to_string(date_val.to_date_int_val()),
                                                     expect_cast_result});
                                        }
                                    }
                                }
                                if constexpr (!is_supported_int_type) {
                                    goto loop_end;
                                }
                            }
                        }
                    }
                    // dbg_str += "\n";
                }
            }
        }
    loop_end:
        if constexpr (is_supported_int_type) {
            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1,
                                                                 false);
            check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1,
                                                                false);
            if (FLAGS_gen_regression_case) {
                int table_index = 0;
                int test_data_index = 0;

                auto test_data_total_row_count = regression_test_data_set.size();
                size_t num_pieces = 10;
                size_t regression_case_avg_row_count =
                        (test_data_total_row_count + num_pieces - 1) / num_pieces;
                std::string from_sql_type_name = fmt::format("datetimev2({})", Scale);
                for (size_t piece = 0; piece < num_pieces; ++piece) {
                    size_t data_index = piece * regression_case_avg_row_count;
                    if (data_index >= test_data_total_row_count) {
                        break;
                    }
                    size_t regression_case_row_count = regression_case_avg_row_count;
                    if (data_index + regression_case_row_count > test_data_total_row_count) {
                        regression_case_row_count = test_data_total_row_count - data_index;
                    }
                    std::unique_ptr<std::ofstream> ofs_const_case_uptr,
                            ofs_const_expected_result_uptr;
                    std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                    std::string regression_case_name =
                            fmt::format("test_cast_to_{}_from_datetimev2_{}_part{}",
                                        to_sql_type_name, Scale, piece);
                    setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                                 ofs_const_expected_result_uptr, ofs_case_uptr,
                                                 ofs_expected_result_uptr, "to_int/from_datetime",
                                                 false);
                    auto* ofs_const_case = ofs_const_case_uptr.get();
                    auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                    auto* ofs_case = ofs_case_uptr.get();
                    auto* ofs_expected_result = ofs_expected_result_uptr.get();
                    std::vector<std::pair<std::string, ToT>> piece_data(
                            regression_test_data_set.begin() + data_index,
                            regression_test_data_set.begin() + data_index +
                                    regression_case_row_count);

                    gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                               to_sql_type_name, piece_data, table_index++,
                                               test_data_index, ofs_case, ofs_expected_result,
                                               ofs_const_case, ofs_const_expected_result, false);
                    // (*ofs_const_case) << "}";
                    (*ofs_case) << "}";
                }
            }
        }
    }

    template <PrimitiveType ToPT>
    void from_time_test_func() {
        using ToT = typename PrimitiveTypeTraits<ToPT>::CppType;
        InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        using IntType = ToT;
        static_assert(std::numeric_limits<ToT>::is_integer, "ToT must be an integer type");
        std::vector<int64_t> hours = {0, 1, 10, 100, 838};
        std::vector<int64_t> minutes = {0, 1, 10, 59};
        std::vector<int64_t> seconds = {0, 1, 10, 59};

        // constexpr TimeValue::TimeType max_time_value = std::min<TimeValue::TimeType>(std::numeric_limits<IntType>::max(), TimeValue::MAX_TIME);

        std::string to_sql_type_name = get_sql_type_name(ToPT);
        std::string dbg_str = fmt::format("test cast time to {}: ", to_sql_type_name);
        // std::cout << dbg_str << std::endl;
        std::vector<std::pair<std::string, ToT>> regression_test_data_set;
        std::vector<std::string> regression_test_overflow_data_set;
        auto test_func = [&](bool negative) {
            DataSet data_set;
            for (auto h : hours) {
                for (auto m : minutes) {
                    for (auto s : seconds) {
                        auto time_val = doris::TimeValue::make_time(h, m, s, negative);
                        if (time_val > std::numeric_limits<IntType>::max() ||
                            time_val < std::numeric_limits<IntType>::min()) {
                            DataSet data_set_tmp;
                            data_set_tmp.push_back({{time_val}, Null()});
                            check_function_for_cast<DataTypeNumber<ToPT>, false>(
                                    input_types, data_set_tmp, -1, -1, false);
                            check_function_for_cast<DataTypeNumber<ToPT>, true>(
                                    input_types, data_set_tmp, -1, -1, false, true);
                            if (FLAGS_gen_regression_case) {
                                regression_test_overflow_data_set.emplace_back(
                                        doris::TimeValue::to_string(time_val, 6));
                            }
                            continue;
                        }
                        auto expect_cast_result = static_cast<IntType>(time_val);
                        data_set.push_back({{time_val}, expect_cast_result});
                        // dbg_str +=
                        //         fmt::format("({}, {})|", doris::TimeValue::to_string(time_val, 6),
                        //                     expect_cast_result);
                        if (FLAGS_gen_regression_case) {
                            regression_test_data_set.emplace_back(
                                    doris::TimeValue::to_string(time_val, 6), expect_cast_result);
                        }
                    }
                }
            }

            check_function_for_cast<DataTypeNumber<ToPT>, false>(input_types, data_set, -1, -1,
                                                                 false);
            check_function_for_cast<DataTypeNumber<ToPT>, true>(input_types, data_set, -1, -1,
                                                                false);
        };
        test_func(false);
        test_func(true);

        /*
        if (FLAGS_gen_regression_case) {
            std::string from_sql_type_name = "time";
            {
                int table_index = 0;
                int test_data_index = 0;
                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string regression_case_name = fmt::format(
                        "test_cast_to_{}_from_{}", to_sql_type_name, from_sql_type_name);
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_int/from_datetime", true,
                                             false);
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();

                gen_normal_regression_case(regression_case_name, from_sql_type_name, true,
                                           to_sql_type_name, regression_test_data_set,
                                           table_index++, test_data_index, ofs_case,
                                           ofs_expected_result, ofs_const_case,
                                           ofs_const_expected_result, true, false);
                (*ofs_const_case) << "}";
            }

            {
                int table_index = 0;
                std::unique_ptr<std::ofstream> ofs_const_case_uptr, ofs_const_expected_result_uptr;
                std::unique_ptr<std::ofstream> ofs_case_uptr, ofs_expected_result_uptr;
                std::string regression_case_name = fmt::format(
                        "test_cast_to_{}_from_{}_overflow", to_sql_type_name, from_sql_type_name);
                setup_regression_case_output(regression_case_name, ofs_const_case_uptr,
                                             ofs_const_expected_result_uptr, ofs_case_uptr,
                                             ofs_expected_result_uptr, "to_int/from_datetime", true,
                                             false);
                auto* ofs_const_case = ofs_const_case_uptr.get();
                auto* ofs_const_expected_result = ofs_const_expected_result_uptr.get();
                auto* ofs_case = ofs_case_uptr.get();
                auto* ofs_expected_result = ofs_expected_result_uptr.get();
                gen_overflow_and_invalid_regression_case(
                        regression_case_name, from_sql_type_name, true, to_sql_type_name,
                        regression_test_overflow_data_set, table_index++, ofs_case,
                        ofs_expected_result, ofs_const_case, ofs_const_expected_result, true);
                (*ofs_const_case) << "}";
            }
        }
        */
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
TEST_F(FunctionCastToIntTest, test_from_string_with_fraction_part) {
    from_string_with_fraction_part_test_func<TYPE_TINYINT>();

    from_string_with_fraction_part_test_func<TYPE_SMALLINT>();

    from_string_with_fraction_part_test_func<TYPE_INT>();

    from_string_with_fraction_part_test_func<TYPE_BIGINT>();

    from_string_with_fraction_part_test_func<TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_string_overflow) {
    from_string_overflow_test_func<TYPE_TINYINT>();
    from_string_overflow_test_func<TYPE_SMALLINT>();
    from_string_overflow_test_func<TYPE_INT>();
    from_string_overflow_test_func<TYPE_BIGINT>();
    from_string_overflow_test_func<TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_string_invalid_input) {
    from_string_invalid_test_func<TYPE_TINYINT>();

    from_string_invalid_test_func<TYPE_SMALLINT>();

    from_string_invalid_test_func<TYPE_INT>();

    from_string_invalid_test_func<TYPE_BIGINT>();

    from_string_invalid_test_func<TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_bool) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    // tinyint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int8(0)},
                {{UInt8 {1}}, Int8(1)},
        };
        check_function_for_cast<DataTypeInt8, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeInt8, true>(input_types, data_set, -1, -1);
    }
    // smallint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int16(0)},
                {{UInt8 {1}}, Int16(1)},
        };
        check_function_for_cast<DataTypeInt16, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeInt16, true>(input_types, data_set, -1, -1);
    }
    // int
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int32(0)},
                {{UInt8 {1}}, Int32(1)},
        };
        check_function_for_cast<DataTypeInt32, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeInt32, true>(input_types, data_set, -1, -1);
    }
    // bigint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int64(0)},
                {{UInt8 {1}}, Int64(1)},
        };
        check_function_for_cast<DataTypeInt64, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeInt64, true>(input_types, data_set, -1, -1);
    }
    // largeint
    {
        DataSet data_set = {
                {{UInt8 {0}}, Int128(0)},
                {{UInt8 {1}}, Int128(1)},
        };
        check_function_for_cast<DataTypeInt128, false>(input_types, data_set, -1, -1);
        check_function_for_cast<DataTypeInt128, true>(input_types, data_set, -1, -1);
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
TEST_F(FunctionCastToIntTest, test_from_wider_to_narrow_int_overflow) {
    wider_to_narrow_int_overflow_test_func<TYPE_SMALLINT, TYPE_TINYINT>();

    wider_to_narrow_int_overflow_test_func<TYPE_INT, TYPE_SMALLINT>();
    wider_to_narrow_int_overflow_test_func<TYPE_INT, TYPE_TINYINT>();

    wider_to_narrow_int_overflow_test_func<TYPE_BIGINT, TYPE_INT>();
    wider_to_narrow_int_overflow_test_func<TYPE_BIGINT, TYPE_SMALLINT>();
    wider_to_narrow_int_overflow_test_func<TYPE_BIGINT, TYPE_TINYINT>();

    wider_to_narrow_int_overflow_test_func<TYPE_LARGEINT, TYPE_BIGINT>();
    wider_to_narrow_int_overflow_test_func<TYPE_LARGEINT, TYPE_INT>();
    wider_to_narrow_int_overflow_test_func<TYPE_LARGEINT, TYPE_SMALLINT>();
    wider_to_narrow_int_overflow_test_func<TYPE_LARGEINT, TYPE_TINYINT>();
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
TEST_F(FunctionCastToIntTest, test_from_float_double_overflow) {
    from_float_overflow_test_func<TYPE_FLOAT, TYPE_TINYINT>();
    from_float_overflow_test_func<TYPE_FLOAT, TYPE_SMALLINT>();
    from_float_overflow_test_func<TYPE_FLOAT, TYPE_INT>();
    from_float_overflow_test_func<TYPE_FLOAT, TYPE_BIGINT>();
    from_float_overflow_test_func<TYPE_FLOAT, TYPE_LARGEINT>();

    from_float_overflow_test_func<TYPE_DOUBLE, TYPE_TINYINT>();
    from_float_overflow_test_func<TYPE_DOUBLE, TYPE_SMALLINT>();
    from_float_overflow_test_func<TYPE_DOUBLE, TYPE_INT>();
    from_float_overflow_test_func<TYPE_DOUBLE, TYPE_BIGINT>();
    from_float_overflow_test_func<TYPE_DOUBLE, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_decimal) {
    from_decimal_to_int_test_func<Decimal32, TYPE_TINYINT>();
    from_decimal_to_int_test_func<Decimal64, TYPE_TINYINT>();
    from_decimal_to_int_test_func<Decimal128V3, TYPE_TINYINT>();
    from_decimal_to_int_test_func<Decimal256, TYPE_TINYINT>();

    from_decimal_to_int_test_func<Decimal128V2, TYPE_TINYINT>();

    from_decimal_to_int_test_func<Decimal32, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<Decimal64, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<Decimal128V3, TYPE_SMALLINT>();
    from_decimal_to_int_test_func<Decimal256, TYPE_SMALLINT>();

    from_decimal_to_int_test_func<Decimal128V2, TYPE_SMALLINT>();

    from_decimal_to_int_test_func<Decimal32, TYPE_INT>();
    from_decimal_to_int_test_func<Decimal64, TYPE_INT>();
    from_decimal_to_int_test_func<Decimal128V3, TYPE_INT>();
    from_decimal_to_int_test_func<Decimal256, TYPE_INT>();

    from_decimal_to_int_test_func<Decimal128V2, TYPE_INT>();

    from_decimal_to_int_test_func<Decimal32, TYPE_BIGINT>();
    from_decimal_to_int_test_func<Decimal64, TYPE_BIGINT>();
    from_decimal_to_int_test_func<Decimal128V3, TYPE_BIGINT>();
    from_decimal_to_int_test_func<Decimal256, TYPE_BIGINT>();

    from_decimal_to_int_test_func<Decimal128V2, TYPE_BIGINT>();

    from_decimal_to_int_test_func<Decimal32, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<Decimal64, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<Decimal128V3, TYPE_LARGEINT>();
    from_decimal_to_int_test_func<Decimal256, TYPE_LARGEINT>();

    from_decimal_to_int_test_func<Decimal128V2, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_decimal_overflow) {
    from_decimal_to_int_overflow_test_func<Decimal32, TYPE_TINYINT>();
    from_decimal_to_int_overflow_test_func<Decimal64, TYPE_TINYINT>();
    from_decimal_to_int_overflow_test_func<Decimal128V3, TYPE_TINYINT>();
    from_decimal_to_int_overflow_test_func<Decimal256, TYPE_TINYINT>();

    from_decimal_to_int_overflow_test_func<Decimal128V2, TYPE_TINYINT>();

    from_decimal_to_int_overflow_test_func<Decimal32, TYPE_SMALLINT>();
    from_decimal_to_int_overflow_test_func<Decimal64, TYPE_SMALLINT>();
    from_decimal_to_int_overflow_test_func<Decimal128V3, TYPE_SMALLINT>();
    from_decimal_to_int_overflow_test_func<Decimal256, TYPE_SMALLINT>();

    from_decimal_to_int_overflow_test_func<Decimal128V2, TYPE_SMALLINT>();

    from_decimal_to_int_overflow_test_func<Decimal32, TYPE_INT>();
    from_decimal_to_int_overflow_test_func<Decimal64, TYPE_INT>();
    from_decimal_to_int_overflow_test_func<Decimal128V3, TYPE_INT>();
    from_decimal_to_int_overflow_test_func<Decimal256, TYPE_INT>();

    from_decimal_to_int_overflow_test_func<Decimal128V2, TYPE_INT>();

    from_decimal_to_int_overflow_test_func<Decimal32, TYPE_BIGINT>();
    from_decimal_to_int_overflow_test_func<Decimal64, TYPE_BIGINT>();
    from_decimal_to_int_overflow_test_func<Decimal128V3, TYPE_BIGINT>();
    from_decimal_to_int_overflow_test_func<Decimal256, TYPE_BIGINT>();

    from_decimal_to_int_overflow_test_func<Decimal128V2, TYPE_BIGINT>();

    from_decimal_to_int_overflow_test_func<Decimal32, TYPE_LARGEINT>();
    from_decimal_to_int_overflow_test_func<Decimal64, TYPE_LARGEINT>();
    from_decimal_to_int_overflow_test_func<Decimal128V3, TYPE_LARGEINT>();
    from_decimal_to_int_overflow_test_func<Decimal256, TYPE_LARGEINT>();

    from_decimal_to_int_overflow_test_func<Decimal128V2, TYPE_LARGEINT>();
}
TEST_F(FunctionCastToIntTest, test_from_date) {
    from_date_test_func<TYPE_TINYINT>();
    from_date_test_func<TYPE_SMALLINT>();
    from_date_test_func<TYPE_INT>();
    from_date_test_func<TYPE_BIGINT>();
    from_date_test_func<TYPE_LARGEINT>();
}

TEST_F(FunctionCastToIntTest, test_from_datetime) {
    from_datetime_test_func<TYPE_TINYINT>(0);
    from_datetime_test_func<TYPE_TINYINT>(1);
    from_datetime_test_func<TYPE_TINYINT>(3);
    from_datetime_test_func<TYPE_TINYINT>(6);

    from_datetime_test_func<TYPE_SMALLINT>(0);
    from_datetime_test_func<TYPE_SMALLINT>(1);
    from_datetime_test_func<TYPE_SMALLINT>(3);
    from_datetime_test_func<TYPE_SMALLINT>(6);

    from_datetime_test_func<TYPE_INT>(0);
    from_datetime_test_func<TYPE_INT>(1);
    from_datetime_test_func<TYPE_INT>(3);
    from_datetime_test_func<TYPE_INT>(6);

    from_datetime_test_func<TYPE_BIGINT>(0);
    from_datetime_test_func<TYPE_BIGINT>(1);
    from_datetime_test_func<TYPE_BIGINT>(3);
    from_datetime_test_func<TYPE_BIGINT>(6);

    from_datetime_test_func<TYPE_LARGEINT>(0);
    from_datetime_test_func<TYPE_LARGEINT>(1);
    from_datetime_test_func<TYPE_LARGEINT>(3);
    from_datetime_test_func<TYPE_LARGEINT>(6);
}

TEST_F(FunctionCastToIntTest, test_from_time) {
    from_time_test_func<TYPE_TINYINT>();
    from_time_test_func<TYPE_SMALLINT>();
    from_time_test_func<TYPE_INT>();
    from_time_test_func<TYPE_BIGINT>();
    from_time_test_func<TYPE_LARGEINT>();
}
} // namespace doris::vectorized