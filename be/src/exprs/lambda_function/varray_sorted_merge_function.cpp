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

#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/lambda_function/lambda_execution_context.h"
#include "exprs/lambda_function/lambda_function.h"
#include "exprs/lambda_function/lambda_function_factory.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vlambda_function_expr.h"

namespace doris {

/**
 * array_sorted_merge(merge_fn, arr1, arr2, is_asc)
 *
 * Merges two arrays that are already sorted by the elements' natural order. The merge lambda
 * receives the two current candidates and returns the combined element; returning NULL means the
 * two elements must not be combined, so both are kept. Each input key occurs at most once per
 * array, so a key appears at most twice across the two arrays and the lambda is called at most
 * once per key.
 */
class ArraySortedMergeFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArraySortedMergeFunction);

public:
    ~ArraySortedMergeFunction() override = default;

    static constexpr auto name = "array_sorted_merge";

    static LambdaFunctionPtr create() { return std::make_shared<ArraySortedMergeFunction>(); }

    std::string get_name() const override { return name; }

    Status execute(VExprContext* context, const Block* block, const Selector* expr_selector,
                   size_t count, ColumnPtr& result_column, const DataTypePtr& result_type,
                   const VExprSPtrs& children) const override {
        DCHECK_EQ(children.size(), 4);

        const ColumnArray* arrays[2] = {nullptr, nullptr};
        const IColumn* data[2] = {nullptr, nullptr};
        DataTypePtr nested_types[2];
        ColumnPtr holders[2];
        const ColumnUInt8::Container* nullmaps[2] = {nullptr, nullptr};
        for (int i = 0; i < 2; ++i) {
            RETURN_IF_ERROR(children[i + 1]->execute_column(context, block, expr_selector, count,
                                                            holders[i]));
            holders[i] = holders[i]->convert_to_full_column_if_const();
            DataTypePtr type = children[i + 1]->execute_type(block);
            const IColumn* array_column = holders[i].get();
            if (const auto* nullable = check_and_get_column<ColumnNullable>(array_column)) {
                nullmaps[i] = &nullable->get_null_map_column().get_data();
                array_column = &nullable->get_nested_column();
                type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
            }
            arrays[i] = assert_cast<const ColumnArray*>(array_column);
            data[i] = arrays[i]->get_data_ptr().get();
            nested_types[i] = assert_cast<const DataTypeArray&>(*type).get_nested_type();
        }

        ColumnPtr flag_holder;
        RETURN_IF_ERROR(
                children[3]->execute_column(context, block, expr_selector, count, flag_holder));
        flag_holder = flag_holder->convert_to_full_column_if_const();
        const IColumn* flag_column = flag_holder.get();
        const ColumnUInt8::Container* flag_nullmap = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(flag_column)) {
            flag_nullmap = &nullable->get_null_map_column().get_data();
            flag_column = &nullable->get_nested_column();
        }
        const auto& flags = assert_cast<const ColumnUInt8&>(*flag_column).get_data();

        const size_t rows = arrays[0]->size();
        auto result_nested = nested_types[0]->create_column();
        auto result_offsets = ColumnArray::ColumnOffsets::create();
        auto& offsets_data = result_offsets->get_data();
        offsets_data.reserve(rows);
        auto result_nullmap = ColumnUInt8::create(rows, 0);
        auto& result_nullmap_data = result_nullmap->get_data();

        // Two one-row columns hold the lambda arguments of the current merge step. They carry
        // the lambda argument names because the column refs resolve by name when no
        // position-based gap is set.
        const auto* lambda_expr = assert_cast<const VLambdaFunctionExpr*>(children[0].get());
        const std::vector<std::string>* argument_names =
                lambda_expr->has_argument_names() ? &lambda_expr->argument_names() : nullptr;
        Block lambda_block;
        for (int i = 0; i < 2; ++i) {
            const std::string argument_name =
                    argument_names != nullptr && i < argument_names->size()
                            ? (*argument_names)[i]
                            : "merge_arg_" + std::to_string(i);
            lambda_block.insert(ColumnWithTypeAndName(nested_types[i]->create_column(),
                                                      nested_types[i], argument_name));
        }
        // The lambda arguments are bound by position (column ids 0 and 1), like array_sort's
        // comparator, so the body reads these two one-row columns.
        IColumn* argument_columns[2] = {
                lambda_block.get_by_position(0).column->assert_mutable().get(),
                lambda_block.get_by_position(1).column->assert_mutable().get()};

        LambdaExecutionContext::Frame lambda_frame;
        lambda_frame.bind_by_name = false;
        lambda_frame.parent_bindings_visible = false;
        LambdaExecutionContext::FrameGuard lambda_frame_guard(context->lambda_execution_context(),
                                                              std::move(lambda_frame));

        const auto& offsets0 = arrays[0]->get_offsets();
        const auto& offsets1 = arrays[1]->get_offsets();
        const int lambda_result_base = static_cast<int>(lambda_block.columns());

        for (size_t row = 0; row < rows; ++row) {
            const bool row_is_null = (nullmaps[0] != nullptr && (*nullmaps[0])[row] != 0) ||
                                     (nullmaps[1] != nullptr && (*nullmaps[1])[row] != 0);
            if (row_is_null) {
                result_nullmap_data[row] = 1;
                offsets_data.push_back(result_nested->size());
                continue;
            }
            if (flag_nullmap != nullptr && (*flag_nullmap)[row] != 0) {
                return Status::InvalidArgument(
                        "array_sorted_merge requires a non-null ascending flag");
            }
            const bool ascending = flags[row] != 0;

            size_t left = row == 0 ? 0 : offsets0[row - 1];
            const size_t left_end = offsets0[row];
            size_t right = row == 0 ? 0 : offsets1[row - 1];
            const size_t right_end = offsets1[row];

            while (left < left_end || right < right_end) {
                if (left == left_end) {
                    result_nested->insert_from(*data[1], right++);
                    continue;
                }
                if (right == right_end) {
                    result_nested->insert_from(*data[0], left++);
                    continue;
                }

                argument_columns[0]->clear();
                argument_columns[0]->insert_from(*data[0], left);
                argument_columns[1]->clear();
                argument_columns[1]->insert_from(*data[1], right);

                int lambda_res_id = lambda_result_base;
                RETURN_IF_ERROR(children[0]->execute(context, &lambda_block, &lambda_res_id));
                ColumnPtr merged = lambda_block.get_by_position(lambda_res_id)
                                           .column->convert_to_full_column_if_const();
                const IColumn* merged_column = merged.get();
                bool merged_is_null = false;
                if (const auto* nullable = check_and_get_column<ColumnNullable>(merged_column)) {
                    merged_is_null = nullable->is_null_at(0);
                }
                if (!merged_is_null) {
                    result_nested->insert_from(*merged_column, 0);
                    ++left;
                    ++right;
                } else {
                    const int cmp = data[0]->compare_at(left, right, *data[1], 1);
                    if (ascending ? cmp <= 0 : cmp >= 0) {
                        result_nested->insert_from(*data[0], left++);
                    } else {
                        result_nested->insert_from(*data[1], right++);
                    }
                }
                lambda_block.erase_tail(lambda_result_base);
            }
            offsets_data.push_back(result_nested->size());
        }

        ColumnPtr result_array =
                ColumnArray::create(std::move(result_nested), std::move(result_offsets));
        if (result_type->is_nullable()) {
            result_column = ColumnNullable::create(result_array, std::move(result_nullmap));
        } else {
            result_column = result_array;
        }
        return Status::OK();
    }
};

void register_function_array_sorted_merge(LambdaFunctionFactory& factory) {
    factory.register_function<ArraySortedMergeFunction>();
}

} // namespace doris
