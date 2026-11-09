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

#include "exprs/vcast_expr.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {

doris::Status VCastExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    DCHECK_EQ(_children.size(), 1);
    _runtime_state = state;
    auto child = _children[0];
    const auto& child_name = child->expr_name();

    _target_data_type = _data_type;
    // TODO(xy): support return struct type name
    _target_data_type_name = _target_data_type->get_name();
    VExpr::register_function_context(state, context);
    auto* function_context = context->fn_context(_fn_context_index);
    if (_is_strict_cast) {
        function_context->set_enable_strict_mode(true);
    }
    const CastOptions options {
            .strict_mode = function_context->enable_strict_mode(),
            .check_overflow_for_decimal = function_context->check_overflow_for_decimal(),
            .string_as_jsonb_string = function_context->string_as_jsonb_string(),
            .jsonb_string_as_string = function_context->jsonb_string_as_string()};
    _cast_executor =
            CastWrapper::bind_cast_executor(options, child->data_type(), _target_data_type);
    _expr_name = fmt::format("({} {}({}) TO {})", cast_name(), child_name,
                             child->data_type()->get_name(), _target_data_type_name);
    _prepare_finished = true;
    return Status::OK();
}

const DataTypePtr& VCastExpr::get_target_type() const {
    return _target_data_type;
}

doris::Status VCastExpr::open(doris::RuntimeState* state, VExprContext* context,
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

void VCastExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

Status VCastExpr::execute_cast(RuntimeState* state, const ColumnWithTypeAndName& source,
                               const DataTypePtr& result_type, size_t rows,
                               ColumnPtr& result_column) const {
    ColumnWithTypeAndName target {nullptr, result_type, _expr_name};
    RETURN_IF_ERROR(CastWrapper::execute_wrapper(state, _cast_executor, source, target, rows));
    result_column = std::move(target.column);
    return Status::OK();
}

Status VCastExpr::execute_column_impl(VExprContext* context, const Block* block,
                                      const Selector* selector, size_t count,
                                      ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << _open_finished << _expr_name;
    if (is_const_and_have_executed()) { // const have executed in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }
    // for each child call execute

    ColumnPtr from_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, from_column));

    ColumnWithTypeAndName source {from_column, _children[0]->execute_type(block),
                                  _children[0]->expr_name()};
    return execute_cast(_runtime_state, source, _data_type, count, result_column);
}

bool cast_error_code(const Status& st) {
    // Value conversion failures use INVALID_ARGUMENT (parsing, numeric/date range checks,
    // JSONB and complex elements) or ARITHMETIC_OVERFLOW_ERRROR (decimal conversions).
    // This predicate relies on producers classifying errors correctly: execution-contract
    // violations (including invalid column shapes) must use INTERNAL_ERROR, not these codes.
    // Likewise, value failures reported as INTERNAL_ERROR must be corrected at their producer;
    // accepting all INTERNAL_ERROR/RUNTIME_ERROR statuses would hide execution defects.
    return st.is<ErrorCode::INVALID_ARGUMENT>() || st.is<ErrorCode::ARITHMETIC_OVERFLOW_ERRROR>();
}

DataTypePtr TryCastExpr::original_cast_return_type() const {
    if (_original_cast_return_is_nullable) {
        return _data_type;
    }
    return remove_nullable(_data_type);
}

Status TryCastExpr::execute_column_impl(VExprContext* context, const Block* block,
                                        const Selector* selector, size_t count,
                                        ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << _open_finished << _expr_name;
    if (is_const_and_have_executed()) { // const have executed in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }

    // For try_cast, try to execute it in batches first.

    // execute child first

    ColumnPtr from_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, from_column));
    auto from_type = _children[0]->execute_type(block);

    ColumnWithTypeAndName input_info {from_column, from_type, _children[0]->expr_name()};
    ColumnPtr batch_result;
    auto batch_exec_status = execute_cast(_runtime_state, input_info, original_cast_return_type(),
                                          count, batch_result);
    // If batch is executed successfully,
    // it means that there is no error and it will be returned directly.
    if (batch_exec_status.ok()) {
        result_column = make_nullable(std::move(batch_result));
        return batch_exec_status;
    }

    // If there is an error that cannot be handled by try cast, it will be returned directly.
    if (!cast_error_code(batch_exec_status)) {
        return batch_exec_status;
    }

    // If there is an error that can be handled by try cast,
    // it will be converted into line execution.
    RETURN_IF_ERROR(single_row_execute(context, input_info, result_column));
    DCHECK_EQ(result_column->size(), count);

    return Status::OK();
}

Status TryCastExpr::single_row_execute(VExprContext* context,
                                       const ColumnWithTypeAndName& input_info,
                                       ColumnPtr& return_column) const {
    auto input_column = input_info.column;
    const auto& input_type = input_info.type;
    const auto& input_name = input_info.name;
    auto result_column = _data_type->create_column();

    auto& result_null_column = assert_cast<ColumnNullable&>(*result_column);

    const auto size = input_column->size();
    for (size_t row = 0; row < size; ++row) {
        ColumnWithTypeAndName single_row_input {input_column->cut(row, 1), input_type, input_name};
        ColumnPtr single_row_result;
        auto single_exec_status = execute_cast(_runtime_state, single_row_input,
                                               original_cast_return_type(), 1, single_row_result);
        if (single_exec_status.ok()) {
            auto single_exec_column =
                    make_nullable(single_row_result->convert_to_full_column_if_const());
            DCHECK_EQ(single_exec_column->size(), 1);
            result_null_column.insert_from(*single_exec_column, 0);
        } else {
            if (!cast_error_code(single_exec_status)) {
                return single_exec_status;
            }
            result_null_column.insert_default();
        }
    }
    return_column = std::move(result_column);
    return Status::OK();
}

const std::string& VCastExpr::expr_name() const {
    return _expr_name;
}

std::string VCastExpr::debug_string() const {
    std::stringstream out;
    out << cast_name() << " Expr(CAST " << get_child(0)->data_type()->get_name() << " to "
        << _target_data_type->get_name() << "){";
    bool first = true;
    for (const auto& input_expr : children()) {
        if (first) {
            first = false;
        } else {
            out << ",";
        }
        out << input_expr->debug_string();
    }
    out << "}";
    return out.str();
}

} // namespace doris
