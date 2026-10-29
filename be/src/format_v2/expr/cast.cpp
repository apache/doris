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
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/vexpr_context.h"

namespace doris::format {

Status Cast::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    if (_children.size() != 1) {
        return Status::InternalError(
                fmt::format("Cast should have exactly 1 child expr, but got {}", _children.size()));
    }
    _expr_name = fmt::format("CAST(arguments={},return={})", _children[0]->data_type()->get_name(),
                             _data_type->get_name());
    VExpr::register_function_context(state, context);
    _runtime_state = state;
    auto* function_context = context->fn_context(_fn_context_index);
    CastOptions options {
            .strict_mode = function_context->enable_strict_mode(),
            .check_overflow_for_decimal = function_context->check_overflow_for_decimal(),
            .string_as_jsonb_string = function_context->string_as_jsonb_string(),
            .jsonb_string_as_string = function_context->jsonb_string_as_string()};
    configure_cast_options(options);
    _cast_executor =
            CastWrapper::bind_cast_executor(options, _children[0]->data_type(), _data_type);
    _prepare_finished = true;
    return Status::OK();
}

Status Cast::open(RuntimeState* state, VExprContext* context,
                  FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void Cast::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
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

    ColumnPtr input_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, input_column));
    ColumnWithTypeAndName source {input_column, _children[0]->execute_type(block),
                                  _children[0]->expr_name()};
    ColumnWithTypeAndName target {nullptr, _data_type, _expr_name};
    RETURN_IF_ERROR(
            CastWrapper::execute_wrapper(_runtime_state, _cast_executor, source, target, count));
    result_column = std::move(target.column);
    RETURN_IF_ERROR(result_column->column_self_check());
    return Status::OK();
}

} // namespace doris::format
