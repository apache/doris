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
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"

namespace doris::format {

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
