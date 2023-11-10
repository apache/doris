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

#include "vec/exprs/vin_predicate.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <vector>

#include "common/status.h"
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

VInPredicate::VInPredicate(const TExprNode& node)
        : VExpr(node), _is_not_in(node.in_predicate.is_not_in) {}

Status VInPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                             VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() < 1) {
        return Status::InternalError("no Function operator in.");
    }

    _expr_name =
            fmt::format("({} {} set)", _children[0]->expr_name(), _is_not_in ? "not_in" : "in");

    DCHECK(_children.size() >= 1);
    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
    }

    // construct the proper function_name
    std::string head(_is_not_in ? "not_" : "");
    std::string real_function_name = head + std::string(function_name);
    _function = SimpleFunctionFactory::instance().get_function(real_function_name,
                                                               argument_template, _data_type);
    if (_function == nullptr) {
        return Status::NotSupported("Function {} is not implemented", real_function_name);
    }

    VExpr::register_function_context(state, context);
    return Status::OK();
}

Status VInPredicate::open(RuntimeState* state, VExprContext* context,
                          FunctionContext::FunctionStateScope scope) {
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    return Status::OK();
}

void VInPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VInPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    // TODO: not execute const expr again, but use the const column in function context
    doris::vectorized::ColumnNumbers arguments(_children.size());
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    // call function
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, arguments,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VInPredicate::expr_name() const {
    return _expr_name;
}

std::string VInPredicate::debug_string() const {
    std::stringstream out;
    out << "InPredicate(" << children()[0]->debug_string() << " " << _is_not_in << ",[";
    int num_children = children().size();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << children()[i]->debug_string();
    }

    out << "])";
    return out.str();
}

} // namespace doris::vectorized