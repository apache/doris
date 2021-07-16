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

#include <string_view>

#include "vec/core/field.h"
#include "vec/columns/column_set.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

VInPredicate::VInPredicate(const TExprNode& node)
     : VExpr(node),
       _is_not_in(node.in_predicate.is_not_in),
       _is_prepare(false),
       _null_in_set(false),
       _hybrid_set() {}

doris::Status VInPredicate::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));

    if (_is_prepare) {
        return Status::OK();
    }
    if (_children.size() < 1) {
        return Status::InternalError("no Function operator in.");
    }
    _hybrid_set.reset(HybridSetBase::create_set(_children[0]->type().type));
    if (NULL == _hybrid_set.get()) {
        return Status::InternalError("Unknown column type.");
    }

    _expr_name = fmt::format("({} {} set)", _children[0]->expr_name(), _is_not_in ? "not_in" : "in");
    _is_prepare = true;
    return Status::OK();
}

doris::Status VInPredicate::open(doris::RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));

    Block block;
    for (int i = 1; i < _children.size(); ++i) {
        if (_children[0]->type().is_string_type()) {
            if (!_children[i]->type().is_string_type()) {
                return Status::InternalError("InPredicate type not same");
            }
        } else {
            if (_children[i]->type().type != _children[0]->type().type) {
                return Status::InternalError("InPredicate type not same");
            }
        }

        int result = -1;
        _children[i]->execute(&block, &result);

        DCHECK(result != -1);
        const auto& column = block.get_by_position(result).column;
        if (column->is_null_at(0)) {
            _null_in_set = true;
            continue;
        }
        const auto& ref_data = column->get_data_at(0);
        _hybrid_set->insert((void*)(ref_data.data), ref_data.size);
    }
    _set_param = COWHelper<IColumn, ColumnSet>::create(1, _hybrid_set);

    DCHECK(_children.size() > 1);
    auto child = _children[0];
    const auto& child_name = child->expr_name();
    auto child_column = child->data_type()->create_column();
    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);
    // here we should use column_set type, to simplify the code. we dirrect use the child->data_type()
    argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);

    // contruct the proper function_name
    std::string head(_is_not_in ? "not_" : "");
    std::string tail(_null_in_set ? "_null_in_set" : "");
    std::string real_function_name = head + std::string(function_name) + tail;
    _function = SimpleFunctionFactory::instance().get_function(real_function_name, argument_template, _data_type);
    if (_function == nullptr) {
        return Status::NotSupported(
                fmt::format("Function {} is not implemented", _fn.name.function_name));
    }

    return Status::OK();
}

void VInPredicate::close(doris::RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

doris::Status VInPredicate::execute(doris::vectorized::Block* block, int* result_column_id) {
    // for each child call execute
    doris::vectorized::ColumnNumbers arguments(2);
    int column_id = -1;
    _children[0]->execute(block, &column_id);
    arguments[0] = column_id;

    size_t const_param_id = block->columns();
    // here we should use column_set type, to simplify the code. we dirrect use the DataTypeInt64
    block->insert({_set_param, std::make_shared<DataTypeInt64>(), "set"});
    arguments[1] = const_param_id;

    // call function
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    _function->execute(*block, arguments, num_columns_without_result, block->rows(), false);
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VInPredicate::expr_name() const {
    return _expr_name;
}

} // namespace doris::vectorized