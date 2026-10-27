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

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

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
#include "core/data_type/data_type_number.h"
#include "exprs/lambda_function/lambda_execution_context.h"
#include "exprs/lambda_function/lambda_function.h"
#include "exprs/lambda_function/lambda_function_factory.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/virtual_slot_ref.h"
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

    /// Rejects a nested lambda that captures one of this lambda's arguments: a merge argument
    /// stands for the candidate of one comparison step, which is not part of the supported
    /// contract for a nested lambda. Nested lambdas may still use their own arguments
    /// (shadowing) and may capture enclosing lambda arguments.
    static Status validate_nested_captures(const VExprSPtr& expr,
                                           const std::vector<std::string>& argument_names,
                                           bool inside_nested_lambda,
                                           std::vector<std::string> shadowed_names) {
        if (expr->is_column_ref()) {
            const std::string& column_name = expr->expr_name();
            if (std::ranges::find(shadowed_names, column_name) != shadowed_names.end()) {
                return Status::OK();
            }
            if (inside_nested_lambda &&
                std::ranges::find(argument_names, column_name) != argument_names.end()) {
                return Status::NotSupported(
                        "array_sorted_merge merge lambda argument '{}' cannot be captured by a "
                        "nested lambda",
                        column_name);
            }
            return Status::OK();
        }
        if (expr->node_type() == TExprNodeType::LAMBDA_FUNCTION_EXPR) {
            const auto* lambda = assert_cast<const VLambdaFunctionExpr*>(expr.get());
            if (lambda->has_argument_names()) {
                for (const auto& argument_name : lambda->argument_names()) {
                    shadowed_names.emplace_back(argument_name);
                }
            }
            return validate_nested_captures(lambda->get_child(0), argument_names, true,
                                            std::move(shadowed_names));
        }
        for (const auto& child : expr->children()) {
            RETURN_IF_ERROR(validate_nested_captures(child, argument_names, inside_nested_lambda,
                                                     shadowed_names));
        }
        return Status::OK();
    }

    /// A virtual column is materialized lazily on first read, from an expression over its own
    /// input columns; its block entry starts as a ColumnNothing placeholder and the virtual slot
    /// only records its own column id. Copying just that id into the private lambda block leaves
    /// the inputs behind the padded "temp" columns, so the lazy evaluation reads a wrong-typed
    /// column. Collect the virtual expressions' inputs as well, so they are copied with their
    /// real types and the evaluation reads the current row.
    static void collect_virtual_capture_inputs(const VExprSPtr& expr, std::set<int>& column_ids) {
        if (expr->node_type() == TExprNodeType::VIRTUAL_SLOT_REF) {
            const auto* virtual_slot = assert_cast<const VirtualSlotRef*>(expr.get());
            const auto virtual_expr = virtual_slot->get_virtual_column_expr();
            if (virtual_expr != nullptr) {
                virtual_expr->collect_slot_column_ids(column_ids);
                collect_virtual_capture_inputs(virtual_expr, column_ids);
            }
        }
        for (const auto& child : expr->children()) {
            collect_virtual_capture_inputs(child, column_ids);
        }
    }

    Status prepare(RuntimeState* state, const VExprSPtrs& children) override {
        RETURN_IF_ERROR(LambdaFunction::prepare(state, children));
        DCHECK_EQ(children.size(), 4);
        const auto* lambda_expr = assert_cast<const VLambdaFunctionExpr*>(children[0].get());
        if (!lambda_expr->has_argument_names()) {
            return Status::OK();
        }
        // A nested lambda that captures one of this lambda's arguments has no defined meaning
        // (the argument is the candidate of a single comparison step); reject it during prepare
        // instead of resolving it against some column at runtime.
        return validate_nested_captures(children[0]->get_child(0), lambda_expr->argument_names(),
                                        false, {});
    }

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

        // The lambda may capture enclosing columns (for example a table column compared against
        // the candidates). Materialize every captured input at its original position in the
        // one-row lambda block, so slot refs read the current row; the two merge candidates
        // take the positions after the last captured column and are bound by name.
        std::set<int> captured_ids;
        children[0]->collect_slot_column_ids(captured_ids);
        collect_virtual_capture_inputs(children[0], captured_ids);
        // An enclosing lambda's argument is a name reference rather than a slot, and this
        // function executes its lambda against a block of its own that hides the parent frames.
        // Publish the visible binding values under their names in that block, so the reference
        // resolves by name like the merge arguments do.
        std::vector<LambdaExecutionContext::Binding> enclosing_bindings;
        context->lambda_execution_context().collect_visible_bindings(enclosing_bindings);
        if ((!captured_ids.empty() || !enclosing_bindings.empty()) && block == nullptr) {
            return Status::InternalError(
                    "array_sorted_merge lambda references outer columns but has no input block");
        }
        std::map<int, ColumnPtr> captured_columns;
        for (int id : captured_ids) {
            if (id < 0 || static_cast<size_t>(id) >= block->columns()) {
                return Status::InternalError(
                        "array_sorted_merge lambda input column id {} is outside input block", id);
            }
            captured_columns[id] =
                    block->get_by_position(id).column->convert_to_full_column_if_const();
        }
        const int lambda_argument_base = captured_ids.empty() ? 0 : *captured_ids.rbegin() + 1;
        const auto* lambda_expr = assert_cast<const VLambdaFunctionExpr*>(children[0].get());
        const std::vector<std::string>* argument_names =
                lambda_expr->has_argument_names() ? &lambda_expr->argument_names() : nullptr;
        Block lambda_block;
        for (int i = 0; i < lambda_argument_base; ++i) {
            auto captured = captured_columns.find(i);
            if (captured != captured_columns.end()) {
                lambda_block.insert(ColumnWithTypeAndName(captured->second->clone_empty(),
                                                          block->get_by_position(i).type,
                                                          block->get_by_position(i).name));
            } else {
                lambda_block.insert(ColumnWithTypeAndName(
                        ColumnUInt8::create(1, 0), std::make_shared<DataTypeUInt8>(), "temp"));
            }
        }
        // The enclosing bindings go before the merge arguments, and the name lookup scans the
        // block backwards, so a merge argument with the same name shadows the outer one.
        std::vector<ColumnPtr> enclosing_columns;
        std::vector<int> enclosing_positions;
        for (const auto& binding : enclosing_bindings) {
            if (binding.column_position < 0 ||
                static_cast<size_t>(binding.column_position) >= block->columns()) {
                return Status::InternalError(
                        "array_sorted_merge enclosing lambda argument '{}' is outside the input "
                        "block (position {})",
                        binding.name, binding.column_position);
            }
            const auto& source = block->get_by_position(binding.column_position);
            enclosing_columns.push_back(source.column->convert_to_full_column_if_const());
            enclosing_positions.push_back(static_cast<int>(lambda_block.columns()));
            lambda_block.insert(ColumnWithTypeAndName(enclosing_columns.back()->clone_empty(),
                                                      source.type, binding.name));
        }
        const int merge_argument_base = static_cast<int>(lambda_block.columns());
        std::string merge_argument_names[2];
        for (int i = 0; i < 2; ++i) {
            merge_argument_names[i] = argument_names != nullptr && i < argument_names->size()
                                              ? (*argument_names)[i]
                                              : "merge_arg_" + std::to_string(i);
            lambda_block.insert(ColumnWithTypeAndName(nested_types[i]->create_column(),
                                                      nested_types[i], merge_argument_names[i]));
        }
        // The lambda arguments are bound by name (the column refs carry the argument names),
        // so the body reads these two one-row columns.
        IColumn* argument_columns[2] = {
                lambda_block.get_by_position(merge_argument_base).column->assert_mutable().get(),
                lambda_block.get_by_position(merge_argument_base + 1)
                        .column->assert_mutable()
                        .get()};

        LambdaExecutionContext::Frame lambda_frame;
        if (argument_names != nullptr) {
            // Rebase every binding this lambda can see onto the private block, so a nested lambda
            // resolves an enclosing argument to the copy materialized here instead of walking into
            // the parent frames, whose positions do not describe this block. The merge arguments
            // are published last and therefore shadow an outer binding with the same name.
            lambda_frame.bind_by_name = true;
            lambda_frame.parent_bindings_visible = false;
            for (size_t i = 0; i < enclosing_bindings.size(); ++i) {
                lambda_frame.argument_bindings.push_back(
                        {enclosing_bindings[i].name, enclosing_positions[i]});
            }
            for (int i = 0; i < 2; ++i) {
                lambda_frame.argument_bindings.push_back(
                        {merge_argument_names[i], merge_argument_base + i});
            }
        } else {
            // Positional (legacy) references resolve through the block, so keep the old frame.
            lambda_frame.bind_by_name = false;
            lambda_frame.parent_bindings_visible = false;
        }
        LambdaExecutionContext::FrameGuard lambda_frame_guard(context->lambda_execution_context(),
                                                              std::move(lambda_frame));

        const auto& offsets0 = arrays[0]->get_offsets();
        const auto& offsets1 = arrays[1]->get_offsets();
        const int lambda_result_base = static_cast<int>(lambda_block.columns());

        // The merged element may come from a non-nullable lambda body (for example COALESCE),
        // while array elements are always nullable; normalize before inserting.
        auto append_element = [&](const IColumn& source, size_t element_row) {
            if (auto* nullable_target = check_and_get_column<ColumnNullable>(result_nested.get())) {
                if (const auto* nullable_source = check_and_get_column<ColumnNullable>(&source)) {
                    nullable_target->insert_from(*nullable_source, element_row);
                } else {
                    nullable_target->get_nested_column().insert_from(source, element_row);
                    nullable_target->get_null_map_data().push_back(0);
                }
            } else {
                result_nested->insert_from(source, element_row);
            }
        };

        for (size_t row = 0; row < rows; ++row) {
            // The ascending flag is an argument contract, not data: it is validated on every row,
            // including a row whose input array is NULL, so a backend-evaluated NULL flag reports
            // the same error regardless of the value column.
            if (flag_nullmap != nullptr && (*flag_nullmap)[row] != 0) {
                return Status::InvalidArgument(
                        "array_sorted_merge requires a non-null ascending flag");
            }
            const bool row_is_null = (nullmaps[0] != nullptr && (*nullmaps[0])[row] != 0) ||
                                     (nullmaps[1] != nullptr && (*nullmaps[1])[row] != 0);
            if (row_is_null) {
                result_nullmap_data[row] = 1;
                offsets_data.push_back(result_nested->size());
                continue;
            }
            const bool ascending = flags[row] != 0;
            // A short-circuit branch receives a selector and only the selected rows, so the row
            // index is compacted: read the captured column at its original row, like the arrays.
            const auto source_row = expr_selector == nullptr ? row : (*expr_selector)[row];
            for (const auto& [id, column] : captured_columns) {
                lambda_block.get_by_position(id).column = column->cut(source_row, 1);
            }
            for (size_t i = 0; i < enclosing_columns.size(); ++i) {
                lambda_block.get_by_position(enclosing_positions[i]).column =
                        enclosing_columns[i]->cut(source_row, 1);
            }

            size_t left = row == 0 ? 0 : offsets0[row - 1];
            const size_t left_end = offsets0[row];
            size_t right = row == 0 ? 0 : offsets1[row - 1];
            const size_t right_end = offsets1[row];

            while (left < left_end || right < right_end) {
                if (left == left_end) {
                    append_element(*data[1], right++);
                    continue;
                }
                if (right == right_end) {
                    append_element(*data[0], left++);
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
                    append_element(*merged_column, 0);
                    ++left;
                    ++right;
                } else {
                    // Match the ordering of array_sort/array_reverse_sort: nan_direction_hint -1
                    // makes NULL and NaN the least values, so an ascending merge lists NULLs
                    // first and a descending one lists them last.
                    const int cmp = data[0]->compare_at(left, right, *data[1], -1);
                    if (ascending ? cmp <= 0 : cmp >= 0) {
                        append_element(*data[0], left++);
                    } else {
                        append_element(*data[1], right++);
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
