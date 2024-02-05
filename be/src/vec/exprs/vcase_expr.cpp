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
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {

VCaseExpr::VCaseExpr(const TExprNode& node)
        : VExpr(node),
          _has_case_expr(node.case_expr.has_case_expr),
          _has_else_expr(node.case_expr.has_else_expr) {
    if (_has_case_expr) {
        _function_name += "_has_case";
    }
    if (_has_else_expr) {
        _function_name += "_has_else";
    }
}

Status VCaseExpr::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    DataTypes arguments;
    for (int i = 0; i < _children.size(); i++) {
        auto child = _children[i];
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
        arguments.emplace_back(child->data_type());
    }

    _function = SimpleFunctionFactory::instance().get_function(_function_name, argument_template,
                                                               _data_type);
    if (_function == nullptr) {
        return Status::NotSupported("vcase_expr Function {} is not implemented",
                                    _fn.name.function_name);
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
    RETURN_IF_ERROR(VExpr::init_function_context(context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCaseExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VCaseExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    if (is_const_and_have_executed()) { // const have execute in open function
        return get_result_from_const(block, _expr_name, result_column_id);
    }
    DCHECK(_open_finished || _getting_const_col);
    ColumnNumbers arguments(_children.size());
    for (int i = 0; i < _children.size(); i++) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    RETURN_IF_ERROR(check_constant(*block, arguments));

    size_t num_columns_without_result = block->columns();
    block->insert({nullptr, _data_type, _expr_name});

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, arguments,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;

    return Status::OK();
}

const std::string& VCaseExpr::expr_name() const {
    return _expr_name;
}

std::string VCaseExpr::debug_string() const {
    std::stringstream out;
    out << "CaseExpr(has_case_expr=" << _has_case_expr << " has_else_expr=" << _has_else_expr
        << " function=" << _function_name << "){";
    bool first = true;
    for (auto& input_expr : children()) {
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
} // namespace doris::vectorized
