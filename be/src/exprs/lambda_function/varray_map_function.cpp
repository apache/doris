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
    // expend data of repeat times
    int current_repeat_times = 0;
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
        ColumnPtr array_column_offset;
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
                array_column_offset = first_array_offsets;
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
            auto result_array_column =
                    ColumnArray::create(std::move(empty_nested_column), array_column_offset);

            if (is_nullable) {
                result_column = ColumnNullable::create(std::move(result_array_column),
                                                       std::move(outside_null_map));
            } else {
                result_column = std::move(result_array_column);
            }

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
        do {
            bool mem_reuse = lambda_block.mem_reuse();
            for (int i = 0; i < column_size; i++) {
                if (mem_reuse) {
                    columns[i] = lambda_block.get_by_position(i).column->assert_mutable();
                } else {
                    columns[i] = data_types[i]->create_column();
                }
            }
            // batch_size of array nested data every time inorder to avoid memory overflow
            while (columns[lambda_argument_base]->size() < batch_size) {
                long max_step = batch_size - columns[lambda_argument_base]->size();
                long current_step = std::min(
                        max_step, (long)(args_info.cur_size - args_info.current_offset_in_array));
                size_t pos = args_info.array_start + args_info.current_offset_in_array;
                for (int i = 0; i < arguments.size() && current_step > 0; ++i) {
                    columns[lambda_argument_base + i]->insert_range_from(*lambda_datas[i], pos,
                                                                         current_step);
                }
                args_info.current_offset_in_array += current_step;
                args_info.current_repeat_times += current_step;
                if (args_info.current_offset_in_array >= args_info.cur_size) {
                    args_info.current_row_eos = true;
                }
                _repeat_input_columns(columns, block, args_info.current_repeat_times,
                                      materialized_input_columns, args_info.current_row_idx);
                args_info.current_repeat_times = 0;
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
                result_col = res_col->clone_empty();
            }
            result_col->insert_range_from(*res_col, 0, res_col->size());
            lambda_block.clear_column_data(column_size);
        } while (args_info.current_row_idx < count);

        //4. get the result column after execution, reassemble it into a new array column, and return.
        if (result_type->is_nullable()) {
            if (res_type->is_nullable()) {
                result_column = ColumnNullable::create(
                        ColumnArray::create(std::move(result_col), array_column_offset),
                        std::move(outside_null_map));
            } else {
                // deal with eg: select array_map(x -> x is null, [null, 1, 2]);
                // need to create the nested column null map for column array
                auto nested_null_map = ColumnUInt8::create(result_col->size(), 0);

                result_column = ColumnNullable::create(
                        ColumnArray::create(ColumnNullable::create(std::move(result_col),
                                                                   std::move(nested_null_map)),
                                            array_column_offset),
                        std::move(outside_null_map));
            }
        } else {
            if (res_type->is_nullable()) {
                result_column = ColumnArray::create(std::move(result_col), array_column_offset);
            } else {
                auto nested_null_map = ColumnUInt8::create(result_col->size(), 0);

                result_column = ColumnArray::create(
                        ColumnNullable::create(std::move(result_col), std::move(nested_null_map)),
                        array_column_offset);
            }
        }
        return Status::OK();
    }

private:
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

    void _repeat_input_columns(std::vector<MutableColumnPtr>& columns, const Block* block,
                               int repeat_times,
                               const std::vector<bool>& materialized_input_columns,
                               int64_t row_idx) const {
        if (!repeat_times || materialized_input_columns.empty()) {
            return;
        }
        for (size_t i = 0; i < materialized_input_columns.size(); i++) {
            if (!materialized_input_columns[i]) {
                columns[i]->resize(columns[i]->size() + repeat_times);
                continue;
            }
            DORIS_CHECK(block != nullptr);
            auto src_column = block->get_by_position(i).column->convert_to_full_column_if_const();
            if (check_and_get_column<ColumnNothing>(src_column.get())) {
                // A ColumnNothing in the outer block is a placeholder for an unmaterialized
                // virtual column. Keep it as a placeholder in the lambda block as well, so
                // VirtualSlotRef can still materialize it lazily if the lambda body reads it.
                if (!check_and_get_column<ColumnNothing>(columns[i].get())) {
                    columns[i] = ColumnNothing::create(columns[i]->size());
                }
            }
            columns[i]->insert_many_from(*src_column, row_idx, repeat_times);
        }
    }

    LambdaArgumentBinding _lambda_argument_binding;
};

void register_function_array_map(doris::LambdaFunctionFactory& factory) {
    factory.register_function<ArrayMapFunction>();
}

} // namespace doris
