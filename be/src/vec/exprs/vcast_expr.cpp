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

#include <string_view>

#include "vec/core/field.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

doris::Status VCastExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));

    DCHECK_EQ(_children.size(), 1);
    auto child = _children[0];
    const auto& child_name = child->expr_name();
    auto child_column = child->data_type()->create_column();

    // create a const string column
    _target_data_type = _data_type;
    _target_data_type_name = DataTypeFactory::instance().get(_target_data_type);
    _cast_param_data_type = std::make_shared<DataTypeString>();
    _cast_param = _cast_param_data_type->create_column_const(1, _target_data_type_name);

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);
    argument_template.emplace_back(_cast_param, _cast_param_data_type, _target_data_type_name);

    _function = SimpleFunctionFactory::instance().get_function(function_name, argument_template, _data_type);

    if (_function == nullptr) {
        return Status::NotSupported(
                fmt::format("Function {} is not implemented", _fn.name.function_name));
    }
    _expr_name = fmt::format("(CAST {}, TO {})", child_name, _target_data_type_name);
    return Status::OK();
}

doris::Status VCastExpr::open(doris::RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));
    return Status::OK();
}

void VCastExpr::close(doris::RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

doris::Status VCastExpr::execute(doris::vectorized::Block* block, int* result_column_id) {
    // for each child call execute
    doris::vectorized::ColumnNumbers arguments(2);
    int column_id = -1;
    _children[0]->execute(block, &column_id);
    arguments[0] = column_id;

    size_t const_param_id = block->columns();
    block->insert({_cast_param, _cast_param_data_type, _target_data_type_name});
    arguments[1] = const_param_id;

    // call function
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    _function->execute(*block, arguments, num_columns_without_result, block->rows(), false);
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VCastExpr::expr_name() const {
    return _expr_name;
}
} // namespace doris::vectorized