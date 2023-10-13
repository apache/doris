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

#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
// Operator <=>
class FunctionEqForNull : public IFunction {
public:
    static constexpr auto name = "eq_for_null";

    static FunctionPtr create() { return std::make_shared<FunctionEqForNull>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& col_left = block.get_by_position(arguments[0]);
        ColumnWithTypeAndName& col_right = block.get_by_position(arguments[1]);

        const auto& [left_col, left_const] = unpack_if_const(col_left.column);
        const auto& [right_col, right_const] = unpack_if_const(col_right.column);
        const auto left_column = check_and_get_column<ColumnNullable>(left_col);
        const auto right_column = check_and_get_column<ColumnNullable>(right_col);

        bool left_nullable = left_column != nullptr;
        bool right_nullable = right_column != nullptr;

        if (left_nullable == right_nullable) {
            auto return_type = std::make_shared<DataTypeUInt8>();

            ColumnsWithTypeAndName eq_columns {
                    ColumnWithTypeAndName {
                            left_nullable ? left_column->get_nested_column_ptr() : col_left.column,
                            left_nullable
                                    ? assert_cast<const DataTypeNullable*>(col_left.type.get())
                                              ->get_nested_type()
                                    : col_left.type,
                            ""},
                    ColumnWithTypeAndName {left_nullable ? right_column->get_nested_column_ptr()
                                                         : col_right.column,
                                           left_nullable ? assert_cast<const DataTypeNullable*>(
                                                                   col_right.type.get())
                                                                   ->get_nested_type()
                                                         : col_right.type,
                                           ""}};
            Block temporary_block(eq_columns);

            auto func_eq =
                    SimpleFunctionFactory::instance().get_function("eq", eq_columns, return_type);
            DCHECK(func_eq);
            temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
            static_cast<void>(
                    func_eq->execute(context, temporary_block, {0, 1}, 2, input_rows_count));

            if (left_nullable) {
                auto res_column = std::move(*temporary_block.get_by_position(2).column).mutate();
                auto& res_map = assert_cast<ColumnVector<UInt8>*>(res_column.get())->get_data();
                const auto& left_null_map = left_column->get_null_map_data();
                const auto& right_null_map = right_column->get_null_map_data();

                auto* __restrict res = res_map.data();
                auto* __restrict l = left_null_map.data();
                auto* __restrict r = right_null_map.data();

                _exec_nullable_equal(res, l, r, input_rows_count, left_const, right_const);
            }

            block.get_by_position(result).column = temporary_block.get_by_position(2).column;
        } else { //left_nullable != right_nullable
            auto return_type = make_nullable(std::make_shared<DataTypeUInt8>());

            const ColumnsWithTypeAndName eq_columns {
                    ColumnWithTypeAndName {col_left.column, col_left.type, ""},
                    ColumnWithTypeAndName {col_right.column, col_right.type, ""}};
            auto func_eq =
                    SimpleFunctionFactory::instance().get_function("eq", eq_columns, return_type);
            DCHECK(func_eq);

            Block temporary_block(eq_columns);
            temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
            static_cast<void>(
                    func_eq->execute(context, temporary_block, {0, 1}, 2, input_rows_count));

            auto res_nullable_column = assert_cast<ColumnNullable*>(
                    std::move(*temporary_block.get_by_position(2).column).mutate().get());
            auto& null_map = res_nullable_column->get_null_map_data();
            auto& res_map =
                    assert_cast<ColumnVector<UInt8>&>(res_nullable_column->get_nested_column())
                            .get_data();

            auto* __restrict res = res_map.data();
            auto* __restrict l = null_map.data();
            _exec_nullable_inequal(res, l, input_rows_count, left_const);

            block.get_by_position(result).column = res_nullable_column->get_nested_column_ptr();
        }
        return Status::OK();
    }

private:
    static void _exec_nullable_equal(unsigned char* result, const unsigned char* left,
                                     const unsigned char* right, size_t rows, bool left_const,
                                     bool right_const) {
        if (left_const) {
            for (int i = 0; i < rows; ++i) {
                result[i] |= left[0] & (left[0] == right[i]);
            }
        } else if (right_const) {
            for (int i = 0; i < rows; ++i) {
                result[i] |= left[i] & (left[i] == right[0]);
            }
        } else {
            for (int i = 0; i < rows; ++i) {
                result[i] |= left[i] & (left[i] == right[i]);
            }
        }
    }
    static void _exec_nullable_inequal(unsigned char* result, const unsigned char* left,
                                       size_t rows, bool left_const) {
        if (left_const) {
            for (int i = 0; i < rows; ++i) {
                result[i] &= (left[0] != 1);
            }
        } else {
            for (int i = 0; i < rows; ++i) {
                result[i] &= (left[i] != 1);
            }
        }
    }
};

void register_function_comparison_eq_for_null(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEqForNull>();
}
} // namespace doris::vectorized