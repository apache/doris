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
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/function/function_test_util.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {
using namespace ut_type;
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
struct FunctionCastTest : public testing::Test {
    void SetUp() override { TimezoneUtils::load_timezones_to_cache(); }
    void TearDown() override {}

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

        EXPECT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows()));

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
    template <typename ResultDataType, int ResultScale = -1, int ResultPrecision = -1,
              bool enable_strict_cast = false>
    void check_function_for_cast(InputTypeSet input_types, DataSet data_set,
                                 bool datetime_is_string_format = true,
                                 bool expect_execute_fail = false, bool expect_result_ne = false) {
        std::string func_name = "CAST";

        InputTypeSet add_input_types = input_types;
        if constexpr (IsDataTypeDecimal<ResultDataType>) {
            add_input_types.emplace_back(ConstedNotnull {
                    ResultDataType {ResultPrecision, ResultScale}.get_primitive_type()});
        } else {
            add_input_types.emplace_back(ConstedNotnull {ResultDataType {}.get_primitive_type()});
        }
        // add_input_types.push_back(ConstedNotnull {TypeId<typename ResultDataType::FieldType>::value});

        // the column-1(target type placeholder) must be const. so we must split the data_set into const_datasets with
        // only 1 row.
        for (const auto& row : data_set) {
            auto add_row = row;
            add_row.first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);
            DataSet const_dataset = {add_row};

            if (datetime_is_string_format) {
                static_cast<void>(
                        check_function<ResultDataType, true, ResultScale, ResultPrecision, true>(
                                func_name, add_input_types, const_dataset, expect_execute_fail,
                                expect_result_ne, enable_strict_cast));
            } else {
                static_cast<void>(
                        check_function<ResultDataType, true, ResultScale, ResultPrecision, false>(
                                func_name, add_input_types, const_dataset, expect_execute_fail,
                                expect_result_ne, enable_strict_cast));
            }
        }
    }

    // we always need return nullable=true for cast function because of its' get_return_type weird
    template <typename ResultDataType, int ResultScale = -1, int ResultPrecision = -1>
    void check_function_for_cast_strict_mode(InputTypeSet input_types, DataSet data_set,
                                             std::string expect_error = "",
                                             bool datetime_is_string_format = true) {
        constexpr bool expect_result_ne = false;

        const bool expect_execute_fail = !expect_error.empty();
        std::string func_name = "CAST";

        InputTypeSet add_input_types = input_types;
        if constexpr (IsDataTypeDecimal<ResultDataType>) {
            add_input_types.emplace_back(ConstedNotnull {
                    ResultDataType {ResultPrecision, ResultScale}.get_primitive_type()});
        } else {
            add_input_types.emplace_back(ConstedNotnull {ResultDataType {}.get_primitive_type()});
        }
        // add_input_types.push_back(ConstedNotnull {TypeId<typename ResultDataType::FieldType>::value});

        // the column-1(target type placeholder) must be const. so we must split the data_set into const_datasets with
        // only 1 row.
        for (const auto& row : data_set) {
            auto add_row = row;
            add_row.first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);
            DataSet const_dataset = {add_row};

            if (datetime_is_string_format) {
                if (expect_execute_fail) {
                    Status st = check_function<ResultDataType, true, ResultScale, ResultPrecision,
                                               true>(func_name, add_input_types, const_dataset,
                                                     expect_execute_fail, expect_result_ne, true);
                    EXPECT_TRUE(st.msg().find(expect_error) != std::string::npos)
                            << ""
                            << "expect error: " << expect_error << ", but got: " << st.msg();

                } else {
                    static_cast<void>(
                            check_function<ResultDataType, true, ResultScale, ResultPrecision,
                                           true>(func_name, add_input_types, const_dataset,
                                                 expect_execute_fail, expect_result_ne, true));
                }
            } else {
                if (expect_execute_fail) {
                    Status st = check_function<ResultDataType, true, ResultScale, ResultPrecision,
                                               false>(func_name, add_input_types, const_dataset,
                                                      expect_execute_fail, expect_result_ne, true);
                    EXPECT_TRUE(st.msg().find(expect_error) != std::string::npos)
                            << ""
                            << "expect error: " << expect_error << ", but got: " << st.msg();

                } else {
                    static_cast<void>(
                            check_function<ResultDataType, true, ResultScale, ResultPrecision,
                                           false>(func_name, add_input_types, const_dataset,
                                                  expect_execute_fail, expect_result_ne, true));
                }
            }
        }
    }
};
} // namespace doris::vectorized
