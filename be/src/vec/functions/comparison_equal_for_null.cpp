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
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
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

        const bool left_const = is_column_const(*col_left.column);
        const bool right_const = is_column_const(*col_right.column);
        bool left_only_null = col_left.column->only_null();
        bool right_only_null = col_right.column->only_null();

        if (left_only_null && right_only_null) {
            // TODO: return ColumnConst after function.cpp::default_implementation_for_constant_arguments supports it.
            auto result_column = ColumnVector<UInt8>::create(input_rows_count, 1);
            block.get_by_position(result).column = std::move(result_column);
            return Status::OK();
        } else if (left_only_null) {
            auto right_type_nullable = col_right.type->is_nullable();
            if (!right_type_nullable) {
                // right_column is not nullable, so result is all false.
                block.get_by_position(result).column =
                        ColumnVector<UInt8>::create(input_rows_count, 0);
            } else {
                // right_column is nullable
                const ColumnNullable* nullable_right_col = nullptr;
                if (right_const) {
                    nullable_right_col = assert_cast<const ColumnNullable*>(
                            &(assert_cast<const ColumnConst*>(col_right.column.get())
                                      ->get_data_column()));
                    // Actually, when we reach here, the result can only be all false (all not null).
                    // Since if right column is const, and it is all null, we will be short-circuited
                    // to (left_only_null && right_only_null) branch. So here the right column is all not null.
                    block.get_by_position(result).column = ColumnUInt8::create(
                            input_rows_count,
                            nullable_right_col->get_null_map_column().get_data()[0]);
                } else {
                    nullable_right_col = assert_cast<const ColumnNullable*>(col_right.column.get());
                    // left column is all null, so result has same nullmap with right column.
                    block.get_by_position(result).column =
                            nullable_right_col->get_null_map_column().clone();
                }
            }
            return Status::OK();
        } else if (right_only_null) {
            auto left_type_nullable = col_left.type->is_nullable();
            if (!left_type_nullable) {
                // right column is all but left column is not nullable, so result is all false.
                block.get_by_position(result).column =
                        ColumnVector<UInt8>::create(input_rows_count, (UInt8)0);
            } else {
                const ColumnNullable* nullable_left_col = nullptr;
                if (left_const) {
                    nullable_left_col = assert_cast<const ColumnNullable*>(
                            &(assert_cast<const ColumnConst*>(col_left.column.get())
                                      ->get_data_column()));
                    block.get_by_position(result).column = ColumnUInt8::create(
                            input_rows_count,
                            nullable_left_col->get_null_map_column().get_data()[0]);
                } else {
                    nullable_left_col = assert_cast<const ColumnNullable*>(col_left.column.get());
                    // right column is all null, so result has same nullmap with left column.
                    block.get_by_position(result).column =
                            nullable_left_col->get_null_map_column().clone();
                }
            }
            return Status::OK();
        }

        const ColumnNullable* left_column = nullptr;
        const ColumnNullable* right_column = nullptr;

        if (left_const) {
            left_column = check_and_get_column<const ColumnNullable>(
                    assert_cast<const ColumnConst*>(col_left.column.get())->get_data_column_ptr());
        } else {
            left_column = check_and_get_column<const ColumnNullable>(col_left.column);
        }

        if (right_const) {
            right_column = check_and_get_column<const ColumnNullable>(
                    assert_cast<const ColumnConst*>(col_right.column.get())->get_data_column_ptr());
        } else {
            right_column = check_and_get_column<const ColumnNullable>(col_right.column);
        }

        bool left_nullable = left_column != nullptr;
        bool right_nullable = right_column != nullptr;

        if (left_nullable == right_nullable) {
            auto return_type = std::make_shared<DataTypeUInt8>();
            auto left_column_tmp =
                    left_nullable ? left_column->get_nested_column_ptr() : col_left.column;
            auto right_column_tmp =
                    right_nullable ? right_column->get_nested_column_ptr() : col_right.column;
            ColumnsWithTypeAndName eq_columns {
                    ColumnWithTypeAndName {
                            left_const ? ColumnConst::create(left_column_tmp, input_rows_count)
                                       : left_column_tmp,
                            left_nullable
                                    ? assert_cast<const DataTypeNullable*>(col_left.type.get())
                                              ->get_nested_type()
                                    : col_left.type,
                            ""},
                    ColumnWithTypeAndName {
                            right_const ? ColumnConst::create(right_column_tmp, input_rows_count)
                                        : right_column_tmp,
                            left_nullable
                                    ? assert_cast<const DataTypeNullable*>(col_right.type.get())
                                              ->get_nested_type()
                                    : col_right.type,
                            ""}};
            Block temporary_block(eq_columns);

            auto func_eq =
                    SimpleFunctionFactory::instance().get_function("eq", eq_columns, return_type);
            DCHECK(func_eq) << fmt::format("Left type {} right type {} return type {}",
                                           col_left.type->get_name(), col_right.type->get_name(),
                                           return_type->get_name());
            temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
            RETURN_IF_ERROR(
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
        } else {
            // left_nullable != right_nullable
            // If we go here, the DataType of left and right column is different.
            // So there will be EXACTLLY one Column has Nullable data type.
            // Possible cases:
            // 1. left Datatype is nullable, right column is not nullable.
            // 2. left Datatype is not nullable, right column is nullable.

            // Why make_nullable here?
            // Because function eq uses default implementation for null,
            // and we have nullable arguments, so the return type should be nullable.
            auto return_type = make_nullable(std::make_shared<DataTypeUInt8>());

            const ColumnsWithTypeAndName eq_columns {
                    ColumnWithTypeAndName {col_left.column, col_left.type, ""},
                    ColumnWithTypeAndName {col_right.column, col_right.type, ""}};
            auto func_eq =
                    SimpleFunctionFactory::instance().get_function("eq", eq_columns, return_type);
            DCHECK(func_eq);

            Block temporary_block(eq_columns);
            temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
            RETURN_IF_ERROR(
                    func_eq->execute(context, temporary_block, {0, 1}, 2, input_rows_count));

            auto res_nullable_column = assert_cast<ColumnNullable*>(
                    std::move(*temporary_block.get_by_position(2).column).mutate().get());
            auto& null_map = res_nullable_column->get_null_map_data();
            auto& res_nested_col =
                    assert_cast<ColumnVector<UInt8>&>(res_nullable_column->get_nested_column())
                            .get_data();

            // Input of eq_for_null:
            // Left: [1, 1, 1, 1](ColumnConst(ColumnInt32))
            // Right: [1, 1, 1, 1] & [0, 1, 0, 1] (ColumnNullable(ColumnInt32))
            // Above input will be passed to function eq, and result will be
            // Result: [1, x, 1, x] & [0, 1, 0, 1] (ColumnNullable(ColumnUInt8)), x means default value.
            // The expceted result of eq_for_null is:
            // Except: [1, 0, 1, 0] (ColumnUInt8)
            // We already have assumption that there is only one nullable column in input.
            // So if one row of res_nullable_column is null, the result row of eq_for_null should be 0.
            // For others, the result will be same with function eq.

            for (int i = 0; i < input_rows_count; ++i) {
                res_nested_col[i] &= (null_map[i] != 1);
            }

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
                result[i] &= (left[0] == right[i]);
            }
        } else if (right_const) {
            for (int i = 0; i < rows; ++i) {
                result[i] &= (left[i] == right[0]);
            }
        } else {
            for (int i = 0; i < rows; ++i) {
                result[i] = (left[i] == right[i]) & (left[i] | result[i]);
            }
        }
    }
};

void register_function_comparison_eq_for_null(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEqForNull>();
}
} // namespace doris::vectorized