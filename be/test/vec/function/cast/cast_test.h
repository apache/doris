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

#pragma once

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "util/to_string.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/function/function_test_util.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {
using namespace ut_type;

static const std::string regression_case_dir = "regression-test/suites/function_p0/cast";
static const std::string regression_expected_result_dir = "regression-test/data/function_p0/cast";

static const std::string regression_notes = R"(
    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
)";

template <typename DecimalType>
DataTypeDecimal<DecimalType::PType> get_decimal_data_type(int precision, int scale) {
    UInt32 precision_tmp = std::is_same_v<DecimalType, Decimal128V2> ? 27 : precision;
    UInt32 scale_tmp = std::is_same_v<DecimalType, Decimal128V2> ? 9 : scale;
    UInt32 orig_precision = std::is_same_v<DecimalType, Decimal128V2> ? precision : UINT32_MAX;
    UInt32 orig_scale = std::is_same_v<DecimalType, Decimal128V2> ? scale : UINT32_MAX;
    DataTypeDecimal<DecimalType::PType> dt(precision_tmp, scale_tmp, orig_precision, orig_scale);

    return dt;
}
template <typename DecimalType>
inline auto get_decimal_ctor() {
    if constexpr (std::is_same_v<DecimalType, Decimal32>) {
        return DECIMAL32;
    }
    if constexpr (std::is_same_v<DecimalType, Decimal64>) {
        return DECIMAL64;
    }
    if constexpr (std::is_same_v<DecimalType, Decimal128V2>) {
        return DECIMAL128V2;
    }
    if constexpr (std::is_same_v<DecimalType, Decimal128V3>) {
        return DECIMAL128V3;
    }
    if constexpr (std::is_same_v<DecimalType, Decimal256>) {
        return DECIMAL256;
    }
    __builtin_unreachable();
}

struct NullTag {};

template <typename DataType>
struct ColumnBuilder {
    using FieldType = typename DataType::FieldType;
    using Ty = std::variant<FieldType, NullTag>;

    void add(const Ty& value) {
        if (std::holds_alternative<FieldType>(value)) {
            values.push_back(std::get<FieldType>(value));
            null_map.push_back(false);
        } else {
            values.push_back(FieldType {});
            null_map.push_back(true);
        }
    }

    ColumnWithTypeAndName build() {
        auto nested_column = ColumnHelper::create_nullable_column<DataType>(values, null_map);
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataType>());

        return ColumnWithTypeAndName(std::move(nested_column), data_type, "column");
    }
    std::vector<FieldType> values;
    std::vector<DataTypeUInt8::FieldType> null_map;
};

struct FunctionCastTest : public testing::Test {
    void SetUp() override { TimezoneUtils::load_timezones_to_cache(); }
    void TearDown() override {}

    std::string get_sql_type_name(PrimitiveType PT, int precision = 0, int scale = 0) {
        switch (PT) {
        case INVALID_TYPE:
        case TYPE_NULL: /* 1 */
            __builtin_unreachable();
            break;
        case TYPE_BOOLEAN: /* 2: uint8 */
            return "bool";
        case TYPE_TINYINT: /* 3: int8 */
            return "tinyint";
        case TYPE_SMALLINT: /* 4: int16 */
            return "smallint";
        case TYPE_INT: /* 5: int32 */
            return "int";
        case TYPE_BIGINT: /* 6: int64 */
            return "bigint";
        case TYPE_LARGEINT: /* 7: int128 */
            return "largeint";
        case TYPE_FLOAT: /* 8: float32 */
            return "float";
        case TYPE_DOUBLE: /* 9: float64*/
            return "double";
        case TYPE_VARCHAR: /* 10 */
            return fmt::format("varchar({})", precision > 0 ? precision : 128);
        case TYPE_DATE: /* 11 */
            return "date";
        case TYPE_DATETIME: /* 12 */
            return "datetime";
        case TYPE_BINARY:
            return "binary";
        /* 13 */           // Not implemented
        case TYPE_DECIMAL: /* 14 */
            return "decimal";
        case TYPE_CHAR: /* 15 */
            return fmt::format("char({})", precision > 0 ? precision : 64);

        case TYPE_STRUCT: /* 16 */
            return "struct";
        case TYPE_ARRAY: /* 17 */
            return "array";
        case TYPE_MAP: /* 18 */
            return "map";
        case TYPE_HLL: /* 19 */
            return "hll";
        case TYPE_DECIMALV2: /* 20: v2 128bit */
            return fmt::format("decimalv2({}, {})", precision > 0 ? precision : 27,
                               scale > 0 ? scale : 9);

        case TYPE_TIME: /*TYPE_TIMEV2*/
            return "time";

        case TYPE_BITMAP: /* 22: bitmap */
            return "bitmap";
        case TYPE_STRING: /* 23 */
            return "string";
        case TYPE_QUANTILE_STATE: /* 24 */
            __builtin_unreachable();
            break;
        case TYPE_DATEV2: /* 25 */
            return "datev2";
        case TYPE_DATETIMEV2: /* 26 */
            return precision > 0 ? fmt::format("datetimev2{}", precision) : "datetimev2";
        case TYPE_TIMEV2: /* 27 */
            return "time";
        case TYPE_DECIMAL32: /* 28 */
            return fmt::format("decimalv3({}, {})", precision > 0 ? precision : 9, scale);
        case TYPE_DECIMAL64: /* 29 */
            return fmt::format("decimalv3({}, {})", precision > 0 ? precision : 18,
                               scale > 0 ? scale : 9);
        case TYPE_DECIMAL128I: /* 30: v3 128bit */
            return fmt::format("decimalv3({}, {})", precision > 0 ? precision : 38,
                               scale > 0 ? scale : 9);
        case TYPE_JSONB: /* 31 */
            return "jsonb";
        case TYPE_VARIANT: /* 32 */
            return "variant";
        case TYPE_LAMBDA_FUNCTION: /* 33 */
            __builtin_unreachable();
            break;
        case TYPE_AGG_STATE: /* 34 */
            __builtin_unreachable();
            break;
        case TYPE_DECIMAL256: /* 35 */
            return fmt::format("decimalv3({}, {})", precision > 0 ? precision : 76,
                               scale > 0 ? scale : 9);
        case TYPE_IPV4: /* 36 */
            return "ipv4";
        case TYPE_IPV6: /* 37 */
            return "ipv6";
        case TYPE_UINT32: /* 38: used as offset */
        case TYPE_UINT64: /* 39: used as offset */
        default:
            __builtin_unreachable();
            break;
        }
    }

    std::shared_ptr<FunctionContext> create_context(bool is_strict_mode) {
        auto ctx = std::make_shared<FunctionContext>();
        ctx->set_enable_strict_mode(is_strict_mode);
        return ctx;
    }

    void check_cast(ColumnWithTypeAndName from, ColumnWithTypeAndName to, bool is_strict_mode) {
        auto ctx = create_context(is_strict_mode);

        DataTypePtr from_type = from.type;
        DataTypePtr to_type = to.type;
        auto fn = get_cast_wrapper(ctx.get(), from_type, to_type);
        ASSERT_TRUE(fn != nullptr);

        Block block = {
                from,
                {nullptr, to_type, "to"},
        };

        EXPECT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));

        auto result = block.get_by_position(1).column;
        auto expected = to.column;
        for (int i = 0; i < block.rows(); i++) {
            EXPECT_EQ(result->compare_at(i, i, *expected, 1), 0)
                    << "from: " << from_type->to_string(*from.column, i)
                    << ", result: " << to_type->to_string(*result, i)
                    << ", expected: " << to_type->to_string(*expected, i);
        }

        std::cout << "block \n" << block.dump_data() << "\n";
    }

    // we always need return nullable=true for cast function because of its' get_return_type weird
    template <typename ResultDataType, bool enable_strict_cast = false>
    void check_function_for_cast(InputTypeSet input_types, DataSet data_set, int result_scale = -1,
                                 int result_precision = -1, bool datetime_is_string_format = true,
                                 bool expect_execute_fail = false, bool expect_result_ne = false) {
        std::string func_name = "CAST";

        InputTypeSet add_input_types = input_types;
        if constexpr (IsDataTypeDecimal<ResultDataType>) {
            ResultDataType result_data_type(result_precision, result_scale);
            add_input_types.emplace_back(ConstedNotnull {result_data_type.get_primitive_type()});
        } else {
            add_input_types.emplace_back(ConstedNotnull {ResultDataType {}.get_primitive_type()});
        }

        data_set[0].first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);
        static_cast<void>(check_function<ResultDataType, true, true>(
                func_name, add_input_types, data_set, result_scale, result_precision, expect_execute_fail, expect_result_ne,
                enable_strict_cast));
    }

    // we always need return nullable=true for cast function because of its' get_return_type weird
    template <typename ResultDataType>
    void check_function_for_cast_strict_mode(InputTypeSet input_types, DataSet data_set,
                                             std::string expect_error = "", int result_scale = -1,
                                             int result_precision = -1) {
        const bool expect_execute_fail = !expect_error.empty();
        std::string func_name = "CAST";

        InputTypeSet add_input_types = input_types;
        if constexpr (IsDataTypeDecimal<ResultDataType>) {
            ResultDataType result_data_type(result_precision, result_scale);
            add_input_types.emplace_back(ConstedNotnull {result_data_type.get_primitive_type()});
        } else {
            add_input_types.emplace_back(ConstedNotnull {ResultDataType {}.get_primitive_type()});
        }

        if (expect_execute_fail) {
            for (const auto& row : data_set) {
                auto add_row = row;
                add_row.first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);
                DataSet one_row_dataset = {add_row};

                Status st = check_function<ResultDataType, true, true>(
                        func_name, add_input_types, one_row_dataset, result_scale, result_precision,
                        true, false, true);
                EXPECT_TRUE(st.msg().find(expect_error) != std::string::npos)
                        << "\nexpect error: " << expect_error << ", but got: " << st.msg();
            }
        } else {
            // row.first is input, in cast we have two columns and the second is a placeholder of target type.
            // we need some data in it. it's a const column. so only need one row.
            data_set[0].first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);

            static_cast<void>(check_function<ResultDataType, true, true>(
                    func_name, add_input_types, data_set, result_scale, result_precision, false,
                    false, true));
        }
    }

    void setup_regression_case_output(const std::string& case_name,
                                      std::unique_ptr<std::ofstream>& ofs_case_const,
                                      std::unique_ptr<std::ofstream>& ofs_expected_result_const,
                                      std::unique_ptr<std::ofstream>& ofs_case,
                                      std::unique_ptr<std::ofstream>& ofs_expected_result,
                                      const std::string& sub_dir = "", bool gen_const_case = true,
                                      bool gen_table_case = true) {
        std::string case_dir = regression_case_dir;
        if (!sub_dir.empty()) {
            case_dir += "/" + sub_dir;
        }
        if (!std::filesystem::exists(case_dir)) {
            std::filesystem::create_directories(case_dir);
        }
        std::string result_dir = regression_expected_result_dir;
        if (!sub_dir.empty()) {
            result_dir += "/" + sub_dir;
        }
        if (!std::filesystem::exists(result_dir)) {
            std::filesystem::create_directories(result_dir);
        }
        std::string file_path;
        if (gen_const_case) {
            file_path = fmt::format("{}/{}/{}_const.groovy", std::string(getenv("ROOT")), case_dir,
                                    case_name);
            ofs_case_const = std::make_unique<std::ofstream>(
                    file_path, std::ios_base::out | std::ios_base::trunc);
            if (!ofs_case_const->is_open()) {
                throw std::runtime_error(fmt::format("Cannot open file: {}", file_path));
            }
            (*ofs_case_const) << kApacheLicenseHeader;
            (*ofs_case_const) << fmt::format("\n\nsuite(\"{}_const\") {{\n", case_name);
            (*ofs_case_const) << regression_notes;

            file_path = fmt::format("{}/{}/{}_const.out", std::string(getenv("ROOT")), result_dir,
                                    case_name);
            ofs_expected_result_const = std::make_unique<std::ofstream>(
                    file_path, std::ios_base::out | std::ios_base::trunc);
            if (!ofs_expected_result_const->is_open()) {
                throw std::runtime_error(fmt::format("Cannot open file: {}", file_path));
            }
            (*ofs_expected_result_const)
                    << "-- This file is automatically generated. You should know what you did if "
                       "you want to edit this\n";
        }

        if (gen_table_case) {
            file_path = fmt::format("{}/{}/{}.groovy", std::string(getenv("ROOT")), case_dir,
                                    case_name);
            ofs_case = std::make_unique<std::ofstream>(file_path,
                                                       std::ios_base::out | std::ios_base::trunc);
            if (!ofs_case->is_open()) {
                throw std::runtime_error(fmt::format("Cannot open file: {}", file_path));
            }
            (*ofs_case) << kApacheLicenseHeader;
            (*ofs_case) << fmt::format("\n\nsuite(\"{}\") {{\n", case_name);
            (*ofs_case) << regression_notes;

            file_path =
                    fmt::format("{}/{}/{}.out", std::string(getenv("ROOT")), result_dir, case_name);
            ofs_expected_result = std::make_unique<std::ofstream>(
                    file_path, std::ios_base::out | std::ios_base::trunc);
            if (!ofs_expected_result->is_open()) {
                throw std::runtime_error(fmt::format("Cannot open file: {}", file_path));
            }
            (*ofs_expected_result)
                    << "-- This file is automatically generated. You should know what "
                       "you did if you want to edit this\n";
        }
    }

    template <typename FromValueT, typename ToValueType>
    void gen_normal_regression_case(
            const std::string& regression_case_name, const std::string& src_sql_type_name,
            bool src_value_need_cast_from_str, const std::string& to_sql_type_name,
            const std::vector<std::pair<FromValueT, ToValueType>>& test_data_set, int table_index,
            int& test_data_index, std::ofstream* ofs_case, std::ofstream* ofs_expected_result,
            std::ofstream* ofs_const_case, std::ofstream* ofs_const_expected_result,
            bool gen_const_case = true, bool gen_table_case = true,
            bool expect_error_in_strict_mode = false) {
        auto table_name = fmt::format("{}_{}", regression_case_name, table_index);

        auto value_count = test_data_set.size();
        if (gen_const_case) {
            (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";
            auto const_test_with_strict_arg = [&](bool enable_strict_cast) {
                (*ofs_const_case) << fmt::format("\n    sql \"set enable_strict_cast={};\"\n",
                                                 enable_strict_cast);
                for (int i = 0; i != value_count; ++i) {
                    auto groovy_var_name = fmt::format("const_sql_{}_{}", table_index, i);
                    std::string const_sql;
                    if constexpr (std::is_same_v<FromValueT, std::string>) {
                        if (src_value_need_cast_from_str) {
                            const_sql = fmt::format(
                                    R"("""select "{}", cast(cast("{}" as {}) as {});""")",
                                    test_data_set[i].first, test_data_set[i].first,
                                    src_sql_type_name, to_sql_type_name);
                        } else {
                            const_sql = fmt::format(R"("""select "{}", cast("{}" as {});""")",
                                                    test_data_set[i].first, test_data_set[i].first,
                                                    to_sql_type_name);
                        }
                    } else {
                        if (src_value_need_cast_from_str) {
                            const_sql =
                                    fmt::format(R"("""select {}, cast(cast("{}" as {}) as {});""")",
                                                test_data_set[i].first, test_data_set[i].first,
                                                src_sql_type_name, to_sql_type_name);
                        } else {
                            const_sql = fmt::format(R"("""select {}, cast({} as {});""")",
                                                    test_data_set[i].first, test_data_set[i].first,
                                                    to_sql_type_name);
                        }
                    }

                    if (enable_strict_cast) {
                        (*ofs_const_case)
                                << fmt::format("    def {} = {}\n", groovy_var_name, const_sql);
                    }
                    if (expect_error_in_strict_mode & enable_strict_cast) {
                        (*ofs_const_case) << fmt::format(R"(
    test {{
        sql """${{{}}};"""
        exception "{}"
    }}
)",
                                                         groovy_var_name, "");
                    } else {
                        (*ofs_const_case) << fmt::format(
                                "    qt_sql_{}_{}_{} \"${{{}}}\"\n", table_index, i,
                                enable_strict_cast ? "strict" : "non_strict", groovy_var_name);
                        (*ofs_const_expected_result)
                                << fmt::format("-- !sql_{}_{}_{} --\n", table_index, i,
                                               enable_strict_cast ? "strict" : "non_strict");
                        if constexpr (std::is_same_v<ToValueType, float> ||
                                      std::is_same_v<ToValueType, double>) {
                            char buffer[64] = {0};
                            fast_to_buffer(test_data_set[i].second, buffer);
                            (*ofs_const_expected_result)
                                    << fmt::format("{}\t{}\n\n", test_data_set[i].first, buffer);
                        } else {
                            (*ofs_const_expected_result) << fmt::format(
                                    "{}\t{}\n\n", test_data_set[i].first, test_data_set[i].second);
                        }

                        (*ofs_const_case)
                                << fmt::format("    testFoldConst(\"${{{}}}\")\n", groovy_var_name);
                    }
                }
            };
            const_test_with_strict_arg(true);
            const_test_with_strict_arg(false);
        }
        if (gen_table_case) {
            auto table_test_func_with_nullable = [&](bool enable_nullable) {
                auto test_table_name = fmt::format("{}_{}", table_name,
                                                   enable_nullable ? "nullable" : "not_nullable");
                (*ofs_case) << fmt::format("    sql \"drop table if exists {};\"\n",
                                           test_table_name);
                (*ofs_case) << fmt::format(
                        "    sql \"create table {}(f1 int, f2 {}) "
                        "properties('replication_num'='1');\"\n",
                        test_table_name, src_sql_type_name);
                std::string table_test_expected_results;
                (*ofs_case) << fmt::format("    sql \"\"\"insert into {} values ", test_table_name);
                int i = 0;
                for (i = 0; i != value_count;) {
                    (*ofs_case) << fmt::format("({}, \"{}\")", i, test_data_set[i].first);
                    table_test_expected_results +=
                            fmt::format("{}\t{}\n", i, test_data_set[i].second);
                    ++i;
                    if (i != value_count) {
                        (*ofs_case) << ",";
                    }
                    if (i % 20 == 0 && i != value_count) {
                        (*ofs_case) << "\n      ";
                    }
                }
                if (enable_nullable) {
                    (*ofs_case) << fmt::format("\n      ,({}, null)", i);
                    table_test_expected_results += fmt::format("{}\t\\N\n", i);
                }
                (*ofs_case) << ";\n    \"\"\"\n\n";

                auto table_test_with_strict_arg = [&](bool enable_strict_cast) {
                    (*ofs_case) << fmt::format("    sql \"set enable_strict_cast={};\"\n",
                                               enable_strict_cast);
                    if (expect_error_in_strict_mode & enable_strict_cast) {
                        (*ofs_case) << fmt::format(R"(
    test {{
        sql """select f1, cast(f2 as {}) from {} order by 1;"""
        exception "{}"
    }}
)",
                                                   to_sql_type_name, test_table_name, "");
                    } else {
                        (*ofs_case) << fmt::format(
                                "    qt_sql_{}_{} 'select f1, cast(f2 as {}) from {} "
                                "order by "
                                "1;'\n\n",
                                table_index, enable_strict_cast ? "strict" : "non_strict",
                                to_sql_type_name, test_table_name);

                        (*ofs_expected_result)
                                << fmt::format("-- !sql_{}_{} --\n", table_index,
                                               enable_strict_cast ? "strict" : "non_strict");
                        (*ofs_expected_result) << table_test_expected_results;
                        (*ofs_expected_result) << "\n";
                    }
                };
                table_test_with_strict_arg(true);
                table_test_with_strict_arg(false);
            };
            table_test_func_with_nullable(true);
            table_test_func_with_nullable(false);
        }
    }

    template <typename FromValueT>
    void gen_overflow_and_invalid_regression_case(
            const std::string& regression_case_name, const std::string& src_sql_type_name,
            bool src_value_need_cast_from_str, const std::string& to_sql_type_name,
            const std::vector<FromValueT>& test_data_set, int table_index, std::ofstream* ofs_case,
            std::ofstream* ofs_expected_result, std::ofstream* ofs_const_case,
            std::ofstream* ofs_const_expected_result, bool only_const_case = false) {
        (*ofs_const_case) << "    sql \"set debug_skip_fold_constant = true;\"\n";

        auto table_name = fmt::format("{}_{}", regression_case_name, table_index);

        auto value_count = test_data_set.size();
        auto groovy_var_name = fmt::format("{}_test_data", table_name);
        (*ofs_const_case) << fmt::format("    def {} = [", groovy_var_name);
        int i = 0;
        for (const auto& data : test_data_set) {
            if constexpr (std::is_same_v<FromValueT, std::string>) {
                (*ofs_const_case) << fmt::format(R"("""{}""")", data);
            } else {
                if (src_value_need_cast_from_str) {
                    (*ofs_const_case) << fmt::format(R"("""{}""")", data);
                } else {
                    (*ofs_const_case) << fmt::format(R"({})", data);
                }
            }
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
            (*ofs_const_case) << fmt::format("    sql \"set enable_strict_cast={};\"\n",
                                             enable_strict_cast);
            std::string src_value;
            if constexpr (std::is_same_v<FromValueT, std::string>) {
                if (src_value_need_cast_from_str) {
                    src_value = fmt::format(R"(cast("${{test_str}}" as {}))", src_sql_type_name);
                } else {
                    src_value = R"("${{test_str}}")";
                }
            } else {
                if (src_value_need_cast_from_str) {
                    src_value = fmt::format(R"(cast("${{test_str}}" as {}))", src_sql_type_name);
                } else {
                    src_value = R"(${{test_str}})";
                }
            }

            if (enable_strict_cast) {
                (*ofs_const_case) << fmt::format(R"(
    for (b in ["false", "true"]) {{
        sql """set debug_skip_fold_constant = "${{b}}";"""
        for (test_str in {}) {{
            test {{
                sql """select cast({} as {});"""
                exception "{}"
            }}
        }}
    }}
)",
                                                 groovy_var_name, src_value, to_sql_type_name, "");

            } else {
                (*ofs_const_case) << fmt::format(R"(
    for (test_str in {}) {{
        qt_sql_{} """select cast({} as {});"""
        testFoldConst("""select cast({} as {});""")
    }}
)",
                                                 groovy_var_name, table_name, src_value,
                                                 to_sql_type_name, src_value, to_sql_type_name);
                for (int i = 0; i != value_count; ++i) {
                    (*ofs_const_expected_result) << fmt::format("-- !sql_{} --\n", table_name);
                    (*ofs_const_expected_result) << "\\N\n\n";
                }
            }
        };
        const_test_with_strict_arg(true);
        const_test_with_strict_arg(false);
        if (only_const_case) {
            return;
        }

        (*ofs_case) << fmt::format("    sql \"drop table if exists {};\"\n", table_name);
        (*ofs_case) << fmt::format(
                "    sql \"create table {}(f1 int, f2 {}) "
                "properties('replication_num'='1');\"\n",
                table_name, src_sql_type_name);
        (*ofs_case) << fmt::format("    sql \"\"\"insert into {} values ", table_name);
        for (int i = 0; i != value_count;) {
            if constexpr (std::is_same_v<FromValueT, std::string>) {
                (*ofs_case) << fmt::format("({}, \"{}\")", i, test_data_set[i]);
            } else {
                (*ofs_case) << fmt::format("({}, {})", i, test_data_set[i]);
            }
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
                                           table_name, 0, table_name, value_count, table_name,
                                           table_name, to_sql_type_name, table_name, "");

            } else {
                (*ofs_case) << fmt::format(
                        "    qt_sql_{}_{} 'select f1, cast(f2 as {}) from {} "
                        "order by "
                        "1;'\n\n",
                        table_index, "non_strict", to_sql_type_name, table_name);

                (*ofs_expected_result)
                        << fmt::format("-- !sql_{}_{} --\n", table_index, "non_strict");
                for (int i = 0; i < value_count; ++i) {
                    (*ofs_expected_result) << fmt::format("{}\t\\N\n", i);
                }
                (*ofs_expected_result) << "\n";
            }
        };
        table_test_with_strict_arg(true);
        table_test_with_strict_arg(false);
    }
};
} // namespace doris::vectorized
