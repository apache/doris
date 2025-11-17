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

#include "vec/exprs/vcast_expr.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

doris::Status VCastExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    DCHECK_EQ(_children.size(), 1);
    auto child = _children[0];
    const auto& child_name = child->expr_name();

    // create a const string column
    _target_data_type = _data_type;
    // TODO(xy): support return struct type name
    _target_data_type_name = _target_data_type->get_name();
    // Using typeindex to indicate the datatype, not using type name because
    // type name is not stable, but type index is stable and immutable
    _cast_param_data_type = _target_data_type;

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(nullptr, child->data_type(), child_name);
    argument_template.emplace_back(nullptr, _cast_param_data_type, _target_data_type_name);
    _function = SimpleFunctionFactory::instance().get_function(
            function_name, argument_template, _data_type,
            {.enable_decimal256 = state->enable_decimal256()});

    if (_function == nullptr) {
        return Status::NotSupported("Cast from {} to {} is not implemented",
                                    child->data_type()->get_name(), _target_data_type_name);
    }
    VExpr::register_function_context(state, context);
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
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCastExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VCastExpr::execute_column(VExprContext* context, const Block* block,
                                 ColumnPtr& result_column) const {
    DCHECK(_open_finished || _getting_const_col)
            << _open_finished << _getting_const_col << _expr_name;
    if (is_const_and_have_executed()) { // const have executed in open function
        result_column = get_result_from_const(block);
        return Status::OK();
    }
    // for each child call execute

    ColumnPtr from_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, from_column));

    Block temp_block;
    temp_block.insert({from_column, _children[0]->execute_type(block), _children[0]->expr_name()});
    temp_block.insert({nullptr, _data_type, _expr_name});
    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), temp_block, {0}, 1,
                                       block->rows()));

    result_column = temp_block.get_by_position(1).column;
    return Status::OK();
}

bool cast_error_code(Status& st) {
    //There may be more error codes that need to be captured by try cast in the future.
    if (st.is<ErrorCode::INVALID_ARGUMENT>()) {
        return true;
    } else {
        return false;
    }
}

DataTypePtr TryCastExpr::original_cast_return_type() const {
    if (_original_cast_return_is_nullable) {
        return _data_type;
    } else {
        return remove_nullable(_data_type);
    }
}

Status TryCastExpr::execute_column(VExprContext* context, const Block* block,
                                   ColumnPtr& result_column) const {
    DCHECK(_open_finished || _getting_const_col)
            << _open_finished << _getting_const_col << _expr_name;
    if (is_const_and_have_executed()) { // const have executed in open function
        result_column = get_result_from_const(block);
        return Status::OK();
    }

    // For try_cast, try to execute it in batches first.

    // execute child first

    ColumnPtr from_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, from_column));
    auto from_type = _children[0]->execute_type(block);

    // prepare block

    Block temp_block;
    temp_block.insert({from_column, from_type, _children[0]->expr_name()});
    temp_block.insert({nullptr, original_cast_return_type(), _expr_name});

    // batch execute
    auto batch_exec_status = _function->execute(context->fn_context(_fn_context_index), temp_block,
                                                {0}, 1, block->rows());
    // If batch is executed successfully,
    // it means that there is no error and it will be returned directly.
    if (batch_exec_status.ok()) {
        result_column = temp_block.get_by_position(1).column;
        result_column = make_nullable(result_column);
        return batch_exec_status;
    }

    // If there is an error that cannot be handled by try cast, it will be returned directly.
    if (!cast_error_code(batch_exec_status)) {
        return batch_exec_status;
    }

    // If there is an error that can be handled by try cast,
    // it will be converted into line execution.
    ColumnWithTypeAndName input_info {from_column, from_type, _children[0]->expr_name()};
    // distinguish whether the return value of the original cast is nullable
    if (_original_cast_return_is_nullable) {
        RETURN_IF_ERROR(single_row_execute<true>(context, input_info, result_column));
    } else {
        RETURN_IF_ERROR(single_row_execute<false>(context, input_info, result_column));
    }
    // wrap nullable
    result_column = make_nullable(result_column);

    return Status::OK();
}

template <bool original_cast_reutrn_is_nullable>
Status TryCastExpr::single_row_execute(VExprContext* context,
                                       const ColumnWithTypeAndName& input_info,
                                       ColumnPtr& return_column) const {
    auto input_column = input_info.column;
    const auto& input_type = input_info.type;
    const auto& input_name = input_info.name;
    auto result_column = _data_type->create_column();

    ColumnNullable& result_null_column = assert_cast<ColumnNullable&>(*result_column);

    IColumn& result_nested_column = result_null_column.get_nested_column();
    auto& result_null_map_data = result_null_column.get_null_map_data();

    auto insert_from_single_row = [&](const IColumn& single_exec_column, size_t row) {
        DCHECK_EQ(single_exec_column.size(), 1);
        if constexpr (original_cast_reutrn_is_nullable) {
            result_null_column.insert_from(single_exec_column, 0);
        } else {
            DCHECK(!single_exec_column.is_nullable());
            result_nested_column.insert_from(single_exec_column, 0);
            result_null_map_data.push_back(0);
        }
    };

    auto insert_null = [&](size_t row) { result_null_column.insert_default(); };

    const auto size = input_column->size();
    for (size_t row = 0; row < size; ++row) {
        Block single_row_block;
        single_row_block.insert({input_column->cut(row, 1), input_type, input_name});
        single_row_block.insert({nullptr, original_cast_return_type(), _expr_name});

        auto single_exec_status = _function->execute(context->fn_context(_fn_context_index),
                                                     single_row_block, {0}, 1, 1);
        if (single_exec_status.ok()) {
            insert_from_single_row(*single_row_block.get_by_position(1).column, row);
        } else {
            if (!cast_error_code(single_exec_status)) {
                return single_exec_status;
            }
            insert_null(row);
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

#include "common/compile_check_end.h"
} // namespace doris::vectorized
