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

#pragma once

#include "common/status.h"
#include "exprs/hybrid_set.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
class VDirectInPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VDirectInPredicate);

public:
    VDirectInPredicate(const TExprNode& node, const std::shared_ptr<HybridSetBase>& filter)
            : VExpr(node), _filter(filter), _expr_name("direct_in_predicate") {}
    ~VDirectInPredicate() override = default;

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        DCHECK(_prepare_finished);
        RETURN_IF_ERROR(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        ColumnNumbers arguments;
        return _do_execute(context, block, result_column_id, arguments);
    }

    Status execute_runtime_fitler(doris::vectorized::VExprContext* context,
                                  doris::vectorized::Block* block, int* result_column_id,
                                  std::vector<size_t>& args) override {
        return _do_execute(context, block, result_column_id, args);
    }

    const std::string& expr_name() const override { return _expr_name; }

    std::shared_ptr<HybridSetBase> get_set_func() const override { return _filter; }

private:
    Status _do_execute(VExprContext* context, Block* block, int* result_column_id,
                       std::vector<size_t>& arguments) {
        DCHECK(_open_finished || _getting_const_col);
        arguments.resize(_children.size());
        for (int i = 0; i < _children.size(); ++i) {
            int column_id = -1;
            RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
            arguments[i] = column_id;
        }

        size_t num_columns_without_result = block->columns();
        auto res_data_column = ColumnVector<UInt8>::create(block->rows());
        ColumnPtr argument_column =
                block->get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        size_t sz = argument_column->size();
        res_data_column->resize(sz);

        if (argument_column->is_nullable()) {
            auto column_nested = static_cast<const ColumnNullable*>(argument_column.get())
                                         ->get_nested_column_ptr();
            const auto& null_map =
                    static_cast<const ColumnNullable*>(argument_column.get())->get_null_map_data();
            _filter->find_batch_nullable(*column_nested, sz, null_map, res_data_column->get_data());
        } else {
            _filter->find_batch(*argument_column, sz, res_data_column->get_data());
        }

        DCHECK(!_data_type->is_nullable());

        block->insert({std::move(res_data_column), _data_type, _expr_name});

        *result_column_id = num_columns_without_result;
        return Status::OK();
    }

    std::shared_ptr<HybridSetBase> _filter;
    std::string _expr_name;
};
} // namespace doris::vectorized