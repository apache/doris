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
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/lambda_function/lambda_execution_context.h"
#include "exprs/lambda_function/lambda_function.h"
#include "exprs/lambda_function/lambda_function_factory.h"
#include "exprs/vcolumn_ref.h"
#include "exprs/vexpr_context.h"
#include "exprs/vlambda_function_expr.h"
#include "util/block_budget.h"

namespace doris {

// extend a block with all required parameters
struct LambdaArgs {
    // which line is extended to the original block
    int64_t current_row_idx = 0;
    // when a block is filled, the array may be truncated, recording where it was truncated
    int64_t current_offset_in_array = 0;
    // the beginning position of the array
    size_t array_start = 0;
    // the size of the array
    int64_t cur_size = 0;
    // offset of column array
    const ColumnArray::Offsets64* offsets_ptr = nullptr;
    // whether the current row of the original block has been extended
    bool current_row_eos = false;
};

class ArrayMapFunction : public LambdaFunction {
    ENABLE_FACTORY_CREATOR(ArrayMapFunction);

public:
    ~ArrayMapFunction() override = default;

    static constexpr auto name = "array_map";

    static LambdaFunctionPtr create() { return std::make_shared<ArrayMapFunction>(); }

    std::string get_name() const override { return name; }

    Status prepare(RuntimeState* state, const VExprSPtrs& children) override {
        RETURN_IF_ERROR(LambdaFunction::prepare(state, children));
        DCHECK_GE(children.size(), 2);

        _lambda_block_budget =
                BlockBudget(state->batch_size(), state->preferred_block_size_bytes());

        return _prepare_lambda_argument_binding(children[0], children.size() - 1,
                                                _lambda_argument_binding);
    }

    Status execute(VExprContext* context, const Block* block, const Selector* expr_selector,
                   size_t count, ColumnPtr& result_column, const DataTypePtr& result_type,
                   const VExprSPtrs& children) const override {
        LambdaArgs args_info;

        ///* array_map(lambda,arg1,arg2,.....) *///
        //1. child[1:end]->execute(src_block)
        ColumnsWithTypeAndName arguments(children.size() - 1);
        for (int i = 1; i < children.size(); ++i) {
            ColumnPtr column;
            RETURN_IF_ERROR(
                    children[i]->execute_column(context, block, expr_selector, count, column));
            arguments[i - 1].column = column;
            arguments[i - 1].type = children[i]->execute_type(block);
            arguments[i - 1].name = children[i]->expr_name();
        }

        // used for save column array outside null map
        auto outside_null_map = ColumnUInt8::create(
                arguments[0].column->convert_to_full_column_if_const()->size(), 0);
        // offset column
        MutableColumnPtr array_column_offset;
        size_t nested_array_column_rows = 0;
        ColumnPtr first_array_offsets = nullptr;
        //2. get the result column from executed expr, and the needed is nested column of array
        std::vector<ColumnPtr> lambda_datas(arguments.size());
        DataTypes lambda_argument_types(arguments.size());

        for (int i = 0; i < arguments.size(); ++i) {
            const auto& array_column_type_name = arguments[i];
            auto column_array = array_column_type_name.column->convert_to_full_column_if_const();
            auto type_array = array_column_type_name.type;
            if (type_array->is_nullable()) {
                // get the nullmap of nullable column
                // hold the null column instead of a reference 'cause `column_array` will be assigned and freed below.
                DORIS_CHECK(is_column_nullable(*column_array));
                auto column_array_nullmap =
                        assert_cast<const ColumnNullable&>(*column_array).get_null_map_column_ptr();

                // get the array column from nullable column
                column_array = assert_cast<const ColumnNullable*>(column_array.get())
                                       ->get_nested_column_ptr();

                // get the nested type from nullable type
                type_array = assert_cast<const DataTypeNullable*>(array_column_type_name.type.get())
                                     ->get_nested_type();

                // need to union nullmap from all columns
                VectorizedUtils::update_null_map(outside_null_map->get_data(),
                                                 column_array_nullmap->get_data());
            }

            // here is the array column
            const auto& col_array = assert_cast<const ColumnArray&>(*column_array);

            if (i == 0) {
                nested_array_column_rows = col_array.get_data_ptr()->size();
                first_array_offsets = col_array.get_offsets_ptr();
                const auto& off_data = col_array.get_offsets_column();
                array_column_offset = off_data.clone_resized(col_array.get_offsets_column().size());
                args_info.offsets_ptr = &col_array.get_offsets();
            } else {
                // select array_map((x,y)->x+y,c_array1,[0,1,2,3]) from array_test2;
                // c_array1: [0,1,2,3,4,5,6,7,8,9]
                const auto& array_offsets =
                        assert_cast<const ColumnArray::ColumnOffsets&>(*first_array_offsets)
                                .get_data();
                if (nested_array_column_rows != col_array.get_data_ptr()->size() ||
                    (!array_offsets.empty() &&
                     memcmp(array_offsets.data(), col_array.get_offsets().data(),
                            sizeof(array_offsets[0]) * array_offsets.size()) != 0)) {
                    return Status::InvalidArgument(
                            "in array map function, the input column size "
                            "are "
                            "not equal completely, nested column data rows 1st size is {}, {}th "
                            "size is {}.",
                            nested_array_column_rows, i + 1, col_array.get_data_ptr()->size());
                }
            }
            lambda_datas[i] = col_array.get_data_ptr();
            const auto& col_type = assert_cast<const DataTypeArray&>(*type_array);
            lambda_argument_types[i] = col_type.get_nested_type();
        }
        std::set<int> required_input_column_ids;
        children[0]->collect_slot_column_ids(required_input_column_ids);
        context->lambda_execution_context().collect_visible_binding_column_positions(
                required_input_column_ids);
        const int lambda_argument_base =
                required_input_column_ids.empty() ? 0 : *required_input_column_ids.rbegin() + 1;
        if (!_lambda_argument_binding.bind_by_name) {
            RETURN_IF_ERROR(
                    _set_legacy_lambda_argument_gap(children[0]->get_child(0), lambda_argument_base,
                                                    _lambda_argument_binding.argument_size));
        }
        std::vector<std::string> names(lambda_argument_base);
        DataTypes data_types(lambda_argument_base);
        std::vector<bool> materialized_input_columns(lambda_argument_base, false);
        bool has_row_dependent_captures = false;
        names.reserve(lambda_argument_base + arguments.size());
        data_types.reserve(lambda_argument_base + arguments.size());
        for (int column_id : required_input_column_ids) {
            if (column_id < 0 || static_cast<size_t>(column_id) >= block->columns()) {
                return Status::InternalError(
                        "array_map lambda input column id {} is outside input block, block={}",
                        column_id, block->dump_structure());
            }
            materialized_input_columns[column_id] = true;
            names[column_id] = block->get_by_position(column_id).name;
            data_types[column_id] = block->get_by_position(column_id).type;
            const auto& input_column = block->get_by_position(column_id).column;
            has_row_dependent_captures |= !is_column_const(*input_column) &&
                                          !check_and_get_column<ColumnNothing>(input_column.get());
        }
        for (int i = 0; i < lambda_argument_base; ++i) {
            if (!materialized_input_columns[i]) {
                // Keep sparse input positions stable for SlotRef/parent lambda bindings without
                // materializing unrelated wide-table columns into every lambda batch.
                names[i] = "temp";
                data_types[i] = std::make_shared<DataTypeUInt8>();
            }
        }
        for (int i = 0; i < arguments.size(); ++i) {
            const auto& array_column_type_name = arguments[i];
            if (_lambda_argument_binding.bind_by_name &&
                i < _lambda_argument_binding.names.size()) {
                names.push_back(_lambda_argument_binding.names[i]);
            } else {
                names.push_back("R" + array_column_type_name.name);
            }
            data_types.push_back(lambda_argument_types[i]);
        }

        LambdaExecutionContext::Frame lambda_frame;
        lambda_frame.bind_by_name = _lambda_argument_binding.bind_by_name;
        lambda_frame.parent_bindings_visible = true;
        for (int i = 0; i < _lambda_argument_binding.argument_size; ++i) {
            const int column_position = lambda_argument_base + i;
            if (_lambda_argument_binding.bind_by_name) {
                lambda_frame.argument_bindings.push_back(
                        {_lambda_argument_binding.names[i], column_position});
            }
        }
        LambdaExecutionContext::FrameGuard lambda_frame_guard(context->lambda_execution_context(),
                                                              std::move(lambda_frame));

        // if column_array is NULL, we know the array_data_column will not write any data,
        // so the column is empty. eg : (x) -> concat('|',x + "1"). if still execute the lambda function, will cause the bolck rows are not equal
        // the x column is empty, but "|" is const literal, size of column is 1, so the block rows is 1, but the x column is empty, will be coredump.
        if (std::ranges::any_of(lambda_datas, [](const auto& v) { return v->empty(); })) {
            DataTypePtr nested_type;
            bool is_nullable = result_type->is_nullable();
            if (is_nullable) {
                nested_type =
                        assert_cast<const DataTypeNullable*>(result_type.get())->get_nested_type();
            } else {
                nested_type = result_type;
            }
            auto empty_nested_column = assert_cast<const DataTypeArray*>(nested_type.get())
                                               ->get_nested_type()
                                               ->create_column();
            auto result_array_column = ColumnArray::create(std::move(empty_nested_column),
                                                           std::move(array_column_offset));

            if (is_nullable) {
                result_column = ColumnNullable::create(std::move(result_array_column),
                                                       std::move(outside_null_map));
            } else {
                result_column = std::move(result_array_column);
            }
            return Status::OK();
        }

        const size_t lambda_batch_rows =
                _calculate_lambda_batch_size(children[0], lambda_datas, block,
                                             required_input_column_ids, has_row_dependent_captures);

        // Lambda arguments are already stored contiguously in the input arrays. When all nested
        // rows fit in one lambda batch, reuse those columns directly and only materialize captured
        // outer columns whose values depend on the outer row.
        if (nested_array_column_rows > 0 && nested_array_column_rows <= lambda_batch_rows) {
            Block lambda_block;
            PaddedPODArray<IColumn::ColumnIndex> captured_source_row_indices;
            MutableColumns captured_columns(lambda_argument_base);
            for (int i = 0; i < lambda_argument_base; ++i) {
                if (!materialized_input_columns[i]) {
                    captured_columns[i] = ColumnNothing::create(nested_array_column_rows);
                    continue;
                }

                const auto& source_column = block->get_by_position(i).column;
                if (is_column_const(*source_column)) {
                    captured_columns[i] = source_column->clone_resized(nested_array_column_rows);
                } else if (check_and_get_column<ColumnNothing>(source_column.get())) {
                    captured_columns[i] = ColumnNothing::create(nested_array_column_rows);
                } else {
                    if (captured_source_row_indices.empty()) {
                        captured_source_row_indices.reserve(nested_array_column_rows);
                        size_t previous_offset = 0;
                        for (size_t row_idx = 0; row_idx < count; ++row_idx) {
                            const size_t current_offset = (*args_info.offsets_ptr)[row_idx];
                            const size_t repeat_times = current_offset - previous_offset;
                            const auto source_row =
                                    expr_selector == nullptr
                                            ? static_cast<IColumn::ColumnIndex>(row_idx)
                                            : (*expr_selector)[row_idx];
                            _append_captured_source_row_indices(captured_source_row_indices,
                                                                source_row, repeat_times);
                            previous_offset = current_offset;
                        }
                    }
                    captured_columns[i] = data_types[i]->create_column();
                    captured_columns[i]->insert_indices_from(
                            *source_column, captured_source_row_indices.data(),
                            captured_source_row_indices.data() +
                                    captured_source_row_indices.size());
                }
            }
            for (int i = 0; i < lambda_argument_base; ++i) {
                lambda_block.insert(ColumnWithTypeAndName(std::move(captured_columns[i]),
                                                          data_types[i], names[i]));
            }
            for (int i = 0; i < arguments.size(); ++i) {
                lambda_block.insert(ColumnWithTypeAndName(lambda_datas[i], lambda_argument_types[i],
                                                          names[lambda_argument_base + i]));
            }

            ColumnPtr res_col;
            RETURN_IF_ERROR(children[0]->execute_column(context, &lambda_block, nullptr,
                                                        nested_array_column_rows, res_col));
            res_col = res_col->convert_to_full_column_if_const();
            auto res_type = children[0]->execute_type(&lambda_block);
            result_column =
                    _create_result_column(std::move(res_col), std::move(array_column_offset),
                                          std::move(outside_null_map), res_type, result_type);
            return Status::OK();
        }

        MutableColumnPtr result_col = nullptr;
        DataTypePtr res_type;

        //process first row
        args_info.array_start = (*args_info.offsets_ptr)[args_info.current_row_idx - 1];
        args_info.cur_size =
                (*args_info.offsets_ptr)[args_info.current_row_idx] - args_info.array_start;

        // lambda block to exectute the lambda, and reuse the memory
        Block lambda_block;
        auto column_size = names.size();
        MutableColumns columns(column_size);
        PaddedPODArray<IColumn::ColumnIndex> captured_source_row_indices;
        if (has_row_dependent_captures) {
            captured_source_row_indices.reserve(lambda_batch_rows);
        }
        do {
            captured_source_row_indices.clear();
            bool mem_reuse = lambda_block.mem_reuse();
            for (int i = 0; i < column_size; i++) {
                if (mem_reuse) {
                    columns[i] = lambda_block.get_by_position(i).column->assert_mutable();
                } else if (i < lambda_argument_base && !materialized_input_columns[i]) {
                    columns[i] = ColumnNothing::create(0);
                } else if (i < lambda_argument_base && materialized_input_columns[i] &&
                           is_column_const(*block->get_by_position(i).column)) {
                    columns[i] = block->get_by_position(i).column->clone_resized(0);
                } else {
                    columns[i] = data_types[i]->create_column();
                }
            }
            // lambda_batch_rows of array nested data every time inorder to avoid memory overflow
            while (columns[lambda_argument_base]->size() < lambda_batch_rows) {
                long max_step = lambda_batch_rows - columns[lambda_argument_base]->size();
                long current_step = std::min(
                        max_step, (long)(args_info.cur_size - args_info.current_offset_in_array));
                size_t pos = args_info.array_start + args_info.current_offset_in_array;
                for (int i = 0; i < arguments.size() && current_step > 0; ++i) {
                    columns[lambda_argument_base + i]->insert_range_from(*lambda_datas[i], pos,
                                                                         current_step);
                }
                args_info.current_offset_in_array += current_step;
                if (has_row_dependent_captures) {
                    const auto source_row =
                            expr_selector == nullptr
                                    ? static_cast<IColumn::ColumnIndex>(args_info.current_row_idx)
                                    : (*expr_selector)[args_info.current_row_idx];
                    _append_captured_source_row_indices(captured_source_row_indices, source_row,
                                                        current_step);
                }
                if (args_info.current_offset_in_array >= args_info.cur_size) {
                    args_info.current_row_eos = true;
                }
                if (args_info.current_row_eos) {
                    //current row is end of array, move to next row
                    args_info.current_row_idx++;
                    args_info.current_offset_in_array = 0;
                    if (args_info.current_row_idx >= count) {
                        break;
                    }
                    args_info.current_row_eos = false;
                    args_info.array_start = (*args_info.offsets_ptr)[args_info.current_row_idx - 1];
                    args_info.cur_size = (*args_info.offsets_ptr)[args_info.current_row_idx] -
                                         args_info.array_start;
                }
            }
            const size_t current_lambda_batch_rows = columns[lambda_argument_base]->size();
            _repeat_input_columns(columns, block, captured_source_row_indices,
                                  materialized_input_columns, current_lambda_batch_rows);

            if (!mem_reuse) {
                for (int i = 0; i < column_size; ++i) {
                    lambda_block.insert(
                            ColumnWithTypeAndName(std::move(columns[i]), data_types[i], names[i]));
                }
            }
            //3. child[0]->execute(new_block)

            ColumnPtr res_col;
            // lambda body executes on the internal lambda_block, not the original block.
            // The outer expr_selector is irrelevant here, so pass nullptr.
            RETURN_IF_ERROR(children[0]->execute_column(context, &lambda_block, nullptr,
                                                        lambda_block.rows(), res_col));
            res_col = res_col->convert_to_full_column_if_const();
            res_type = children[0]->execute_type(&lambda_block);

            if (!result_col) {
                result_col = IColumn::mutate(std::move(res_col));
            } else {
                result_col->insert_range_from(*res_col, 0, res_col->size());
            }
            lambda_block.clear_column_data(column_size);
        } while (args_info.current_row_idx < count);

        //4. get the result column after execution, reassemble it into a new array column, and return.
        result_column = _create_result_column(std::move(result_col), std::move(array_column_offset),
                                              std::move(outside_null_map), res_type, result_type);
        return Status::OK();
    }

private:
    static bool _has_variable_length_column(const VExprSPtr& expr) {
        return !expr->data_type()->have_maximum_size_of_value() ||
               std::ranges::any_of(expr->children(), [](const auto& child) {
                   return _has_variable_length_column(child);
               });
    }

    // A referenced non-const capture is expanded once for every nested array element before
    // lambda evaluation. Expanding the full nested cardinality at once can create multi-gigabyte
    // temporary columns (for example, a 5,000-byte VARCHAR repeated 1,000,000 times) and exceed
    // ColumnString's UInt32 offset limit. Keep capture expansion and lambda evaluation within the
    // runtime block budget, while retaining direct nested-input reuse when one batch is sufficient.
    // Rule: use the external row budget if any lambda input, output, or intermediate column
    // is variable-length. Fixed-width columns have predictable memory usage, so also apply
    // the external byte budget to their estimated bytes per row.
    size_t _calculate_lambda_batch_size(const VExprSPtr& lambda_expr,
                                        const std::vector<ColumnPtr>& lambda_datas,
                                        const Block* block,
                                        const std::set<int>& required_input_column_ids,
                                        bool has_row_dependent_captures) const {
        const auto add_bytes_with_saturation = [](size_t current_bytes, size_t additional_bytes) {
            constexpr size_t max_bytes = std::numeric_limits<size_t>::max();
            return additional_bytes > max_bytes - current_bytes ? max_bytes
                                                                : current_bytes + additional_bytes;
        };

        if (_has_variable_length_column(lambda_expr)) {
            return _lambda_block_budget.max_rows;
        }

        size_t estimated_lambda_bytes_per_row = lambda_expr->estimate_memory(1);
        for (const auto& lambda_data : lambda_datas) {
            estimated_lambda_bytes_per_row = add_bytes_with_saturation(
                    estimated_lambda_bytes_per_row, lambda_data->get_max_row_byte_size());
        }
        if (has_row_dependent_captures) {
            estimated_lambda_bytes_per_row = add_bytes_with_saturation(
                    estimated_lambda_bytes_per_row, sizeof(IColumn::ColumnIndex));
            for (int column_id : required_input_column_ids) {
                const auto& input_column = block->get_by_position(column_id).column;
                if (!is_column_const(*input_column) &&
                    !check_and_get_column<ColumnNothing>(input_column.get())) {
                    estimated_lambda_bytes_per_row = add_bytes_with_saturation(
                            estimated_lambda_bytes_per_row, input_column->get_max_row_byte_size());
                }
            }
        }
        return _lambda_block_budget.effective_max_rows(estimated_lambda_bytes_per_row);
    }

    static void _append_captured_source_row_indices(
            PaddedPODArray<IColumn::ColumnIndex>& captured_source_row_indices,
            IColumn::ColumnIndex source_row, size_t repeat_times) {
        const size_t old_size = captured_source_row_indices.size();
        captured_source_row_indices.resize(old_size + repeat_times);
        std::fill(captured_source_row_indices.begin() + old_size, captured_source_row_indices.end(),
                  source_row);
    }

    static ColumnPtr _create_result_column(ColumnPtr result_col,
                                           MutableColumnPtr array_column_offset,
                                           MutableColumnPtr outside_null_map,
                                           const DataTypePtr& res_type,
                                           const DataTypePtr& result_type) {
        ColumnPtr nested_column = std::move(result_col);
        if (!res_type->is_nullable()) {
            // deal with eg: select array_map(x -> x is null, [null, 1, 2]);
            // need to create the nested column null map for column array
            auto nested_null_map = ColumnUInt8::create(nested_column->size(), 0);
            nested_column =
                    ColumnNullable::create(std::move(nested_column), std::move(nested_null_map));
        }

        auto result_array_column =
                ColumnArray::create(std::move(nested_column), std::move(array_column_offset));
        if (result_type->is_nullable()) {
            return ColumnNullable::create(std::move(result_array_column),
                                          std::move(outside_null_map));
        }
        return result_array_column;
    }

    struct LambdaArgumentBinding {
        bool bind_by_name = true;
        size_t argument_size = 0;
        std::vector<std::string> names;
    };

    Status _prepare_lambda_argument_binding(const VExprSPtr& expr, size_t expected_argument_size,
                                            LambdaArgumentBinding& argument_binding) const {
        DORIS_CHECK_EQ(expr->node_type(), TExprNodeType::LAMBDA_FUNCTION_EXPR);
        const auto* lambda_expr = assert_cast<const VLambdaFunctionExpr*>(expr.get());

        argument_binding.argument_size = 0;
        argument_binding.names.clear();
        argument_binding.bind_by_name = lambda_expr->has_argument_names();

        if (!argument_binding.bind_by_name) {
            if (_contains_nested_lambda_call(expr->get_child(0))) {
                return Status::InternalError(
                        "Cannot resolve nested lambda argument without lambda metadata");
            }
            argument_binding.argument_size = expected_argument_size;
            argument_binding.names.resize(expected_argument_size);
            return Status::OK();
        }

        argument_binding.names = lambda_expr->argument_names();
        if (argument_binding.names.size() > expected_argument_size) {
            return Status::InternalError(
                    "lambda argument metadata size exceeds parameter size, maximum={}, actual={}",
                    expected_argument_size, argument_binding.names.size());
        }
        argument_binding.argument_size = argument_binding.names.size();
        if (std::ranges::any_of(argument_binding.names,
                                [](const auto& argument_name) { return argument_name.empty(); })) {
            return Status::InternalError("lambda argument metadata contains empty name");
        }
        return Status::OK();
    }

    Status _set_legacy_lambda_argument_gap(const VExprSPtr& expr, int lambda_argument_base,
                                           size_t argument_size) const {
        if (expr->is_column_ref()) {
            auto* ref = static_cast<VColumnRef*>(expr.get());
            DORIS_CHECK_GE(ref->column_id(), 0);
            DORIS_CHECK_LT(static_cast<size_t>(ref->column_id()), argument_size);
            const int argument_index = ref->column_id();
            ref->set_gap(lambda_argument_base + argument_index - ref->column_id());
        } else {
            for (const auto& child : expr->children()) {
                RETURN_IF_ERROR(_set_legacy_lambda_argument_gap(child, lambda_argument_base,
                                                                argument_size));
            }
        }
        return Status::OK();
    }

    bool _is_lambda_call_with_lambda_expr(const VExprSPtr& expr) const {
        return expr->node_type() == TExprNodeType::LAMBDA_FUNCTION_CALL_EXPR &&
               !expr->children().empty() &&
               expr->children()[0]->node_type() == TExprNodeType::LAMBDA_FUNCTION_EXPR;
    }

    bool _contains_nested_lambda_call(const VExprSPtr& expr) const {
        if (_is_lambda_call_with_lambda_expr(expr)) {
            return true;
        }
        return std::ranges::any_of(expr->children(), [this](const auto& child) {
            return _contains_nested_lambda_call(child);
        });
    }

    void _repeat_input_columns(
            std::vector<MutableColumnPtr>& columns, const Block* block,
            const PaddedPODArray<IColumn::ColumnIndex>& captured_source_row_indices,
            const std::vector<bool>& materialized_input_columns, size_t lambda_batch_rows) const {
        if (lambda_batch_rows == 0 || materialized_input_columns.empty()) {
            return;
        }
        for (size_t i = 0; i < materialized_input_columns.size(); i++) {
            if (!materialized_input_columns[i]) {
                columns[i]->resize(lambda_batch_rows);
                continue;
            }
            DORIS_CHECK(block != nullptr);
            const auto& src_column = block->get_by_position(i).column;
            if (is_column_const(*src_column)) {
                columns[i]->resize(lambda_batch_rows);
            } else if (check_and_get_column<ColumnNothing>(src_column.get())) {
                // A ColumnNothing in the outer block is a placeholder for an unmaterialized
                // virtual column. Keep it as a placeholder in the lambda block as well, so
                // VirtualSlotRef can still materialize it lazily if the lambda body reads it.
                if (!check_and_get_column<ColumnNothing>(columns[i].get())) {
                    columns[i] = ColumnNothing::create(lambda_batch_rows);
                } else {
                    columns[i]->resize(lambda_batch_rows);
                }
            } else {
                DCHECK_EQ(captured_source_row_indices.size(), lambda_batch_rows);
                columns[i]->insert_indices_from(
                        *src_column, captured_source_row_indices.data(),
                        captured_source_row_indices.data() + captured_source_row_indices.size());
            }
        }
    }

    LambdaArgumentBinding _lambda_argument_binding;
    BlockBudget _lambda_block_budget {1, 0};
};

void register_function_array_map(doris::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayMapFunction>();
}

} // namespace doris
