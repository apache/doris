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

#include "common/status.h"
#include "vec/columns/column_nullable.h"

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

Status VCaseExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                          VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    ColumnsWithTypeAndName argument_template;
    DataTypes arguments;
    for (int i = 0; i < _children.size(); i++) {
        auto child = _children[i];
        const auto& child_name = child->expr_name();
        auto child_column = child->data_type()->create_column();
        argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);
        arguments.emplace_back(child->data_type());
    }

    _function = SimpleFunctionFactory::instance().get_function(_function_name, argument_template,
                                                               _data_type);
    if (_function == nullptr) {
        return Status::NotSupported("vcase_expr Function {} is not implemented",
                                    _fn.name.function_name);
    }

    VExpr::register_function_context(state, context);
    return Status::OK();
}

Status VCaseExpr::open(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    RETURN_IF_ERROR(VExpr::init_function_context(context, scope, _function));
    return Status::OK();
}

void VCaseExpr::close(RuntimeState* state, VExprContext* context,
                      FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(state, context, scope);
}

Status VCaseExpr::execute(VExprContext* context, Block* block, int* result_column_id) {
    ColumnNumbers arguments(_children.size());

    for (int i = 0; i < _children.size(); i++) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;

        block->replace_by_position_if_const(column_id);
        auto child_column = block->get_by_position(column_id).column;
    }

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
    for (VExpr* input_expr : children()) {
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
