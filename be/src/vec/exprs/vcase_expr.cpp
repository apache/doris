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

#include "vec/exprs/vcase_expr.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>

#include <ostream>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

VCaseExpr::VCaseExpr(const TExprNode& node)
        : VExpr(node), _has_else_expr(node.case_expr.has_else_expr) {}

Status VCaseExpr::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    DataTypes arguments;
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
        arguments.emplace_back(child->data_type());
    }

    VExpr::register_function_context(state, context);
    _prepare_finished = true;
    return Status::OK();
}

Status VCaseExpr::open(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCaseExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

Status VCaseExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (is_const_and_have_executed()) { // const have execute in open function
        return get_result_from_const(block, EXPR_NAME, result_column_id);
    }
    DCHECK(_open_finished || _getting_const_col);

    size_t rows_count = block->rows();
    std::vector<ColumnPtr> when_columns;
    std::vector<ColumnPtr> then_columns;

    if (_has_else_expr) {
        int column_id = -1;
        RETURN_IF_ERROR(_children.back()->execute(context, block, &column_id));
        auto else_column_ptr = block->get_by_position(column_id).column;
        then_columns.emplace_back(else_column_ptr);
    } else {
        then_columns.emplace_back(nullptr);
    }

    size_t origin_block_size = block->columns();
    for (int i = 0; i < _children.size() - _has_else_expr; i += 2) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        auto when_column_ptr = block->get_by_position(column_id).column;
        if (calculate_false_number(when_column_ptr) == rows_count) {
            block->erase_tail(origin_block_size);
            continue;
        }
        when_columns.emplace_back(when_column_ptr);
        RETURN_IF_ERROR(_children[i + 1]->execute(context, block, &column_id));
        auto then_column_ptr = block->get_by_position(column_id).column;
        then_columns.emplace_back(then_column_ptr);
        block->erase_tail(origin_block_size);
    }

    if (then_columns.size() > UINT16_MAX) {
        return Status::NotSupported(
                "case when do not support more than UINT16_MAX then conditions");
    } else if (then_columns.size() > UINT8_MAX) {
        block->insert({_execute_impl_with_type<uint16_t>(when_columns, then_columns), _data_type,
                       EXPR_NAME});
    } else {
        block->insert({_execute_impl_with_type<uint8_t>(when_columns, then_columns), _data_type,
                       EXPR_NAME});
    }

    *result_column_id = block->columns() - 1;

    return Status::OK();
}

const std::string& VCaseExpr::expr_name() const {
    return EXPR_NAME;
}

std::string VCaseExpr::debug_string() const {
    std::stringstream out;
    out << "CaseExpr(has_else_expr=" << _has_else_expr << " function=" << FUNCTION_NAME << "){";
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
