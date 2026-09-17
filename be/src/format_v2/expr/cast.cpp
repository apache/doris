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

#include "format_v2/expr/cast.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <ostream>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"

namespace doris::format {

static bool can_truncate_datetimev2_precision(const DataTypePtr& source_type,
                                              const DataTypePtr& target_type) {
    const auto source = remove_nullable(source_type);
    const auto target = remove_nullable(target_type);
    return source->get_primitive_type() == TYPE_DATETIMEV2 &&
           target->get_primitive_type() == TYPE_DATETIMEV2 &&
           source->get_scale() > target->get_scale();
}

static void truncate_datetimev2_precision(ColumnPtr* column, const DataTypePtr& target_type) {
    DORIS_CHECK(column != nullptr);
    auto mutable_column = IColumn::mutate(std::move(*column));
    IColumn* nested_column = mutable_column.get();
    if (is_column_nullable(*nested_column)) {
        nested_column = static_cast<ColumnNullable*>(nested_column)->get_nested_column_ptr().get();
    }
    auto& data = assert_cast<ColumnDateTimeV2&>(*nested_column).get_data();
    const auto scale = remove_nullable(target_type)->get_scale();
    uint32_t divisor = 1;
    for (uint32_t i = scale; i < 6; ++i) {
        divisor *= 10;
    }
    for (auto& value : data) {
        value.unchecked_set_time_unit<TimeUnit::MICROSECOND>(value.microsecond() / divisor *
                                                             divisor);
    }
    *column = std::move(mutable_column);
}

Status Cast::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    if (_children.size() != 1) {
        return Status::InternalError(
                fmt::format("Cast should have exactly 1 child expr, but got {}", _children.size()));
    }
    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    if (_children[0]->is_literal()) {
        // For some functions, he needs some literal columns to derive the return type.
        auto literal_node = std::dynamic_pointer_cast<VLiteral>(_children[0]);
        argument_template.emplace_back(literal_node->get_column_ptr(), _children[0]->data_type(),
                                       _children[0]->expr_name());
    } else {
        argument_template.emplace_back(nullptr, _children[0]->data_type(),
                                       _children[0]->expr_name());
    }

    _expr_name = fmt::format("CAST(arguments={},return={})", _children[0]->data_type()->get_name(),
                             _data_type->get_name());
    // get the function. won't prepare function.
    _function = SimpleFunctionFactory::instance().get_function(
            "CAST", argument_template, _data_type,
            {.new_version_unix_timestamp = state->query_options().new_version_unix_timestamp,
             .new_version_bitmap_op_count =
                     state->query_options().__isset.new_version_bitmap_op_count &&
                     state->query_options().new_version_bitmap_op_count},
            state->be_exec_version());
    if (_function == nullptr) {
        return Status::InternalError("Could not find function {} ", _expr_name);
    }
    VExpr::register_function_context(state, context);
    _prepare_finished = true;
    return Status::OK();
}

Status Cast::open(RuntimeState* state, VExprContext* context,
                  FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void Cast::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status Cast::execute_column_impl(VExprContext* context, const Block* block,
                                 const Selector* selector, size_t count,
                                 ColumnPtr& result_column) const {
    return _do_execute(context, block, selector, count, result_column);
}

std::string Cast::debug_string() const {
    return _expr_name;
}

Status Cast::_do_execute(VExprContext* context, const Block* block, const Selector* selector,
                         size_t count, ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    if (_children.size() != 1) {
        return Status::InternalError(
                fmt::format("Cast should have exactly 1 child expr, but got {}", _children.size()));
    }
    if (is_const_and_have_executed()) { // const have executed in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }

    Block temp_block;
    ColumnNumbers args(1);

    ColumnPtr tmp_arg_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, tmp_arg_column));
    auto arg_type = _children[0]->execute_type(block);
    if (_truncate_datetimev2_precision && can_truncate_datetimev2_precision(arg_type, _data_type)) {
        auto result = _data_type->create_column();
        result->insert_range_from(*tmp_arg_column, 0, count);
        result_column = std::move(result);
        truncate_datetimev2_precision(&result_column, _data_type);
        return Status::OK();
    }
    temp_block.insert({tmp_arg_column, arg_type, _children[0]->expr_name()});
    args[0] = 0;

    uint32_t num_columns_without_result = temp_block.columns();
    // prepare a column to save result
    temp_block.insert({nullptr, _data_type, _expr_name});

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), temp_block, args,
                                       num_columns_without_result, count));
    result_column = temp_block.get_by_position(num_columns_without_result).column;
    DCHECK_EQ(result_column->size(), count);
    RETURN_IF_ERROR(result_column->column_self_check());
    return Status::OK();
}

} // namespace doris::format
