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

#include <gen_cpp/types.pb.h>

#include <utility>

#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// only used for dynamic topn filter
class VTopNPred : public VExpr {
    ENABLE_FACTORY_CREATOR(VTopNPred);

public:
    VTopNPred(const TExprNode& node, int source_node_id, VExprContextSPtr target_ctx)
            : VExpr(node),
              _source_node_id(source_node_id),
              _expr_name(fmt::format("VTopNPred(source_node_id={})", _source_node_id)),
              _target_ctx(std::move(target_ctx)) {}

    static Status create_vtopn_pred(const TExpr& target_expr, int source_node_id,
                                    vectorized::VExprSPtr& expr) {
        vectorized::VExprContextSPtr target_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(target_expr, target_ctx));

        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_is_nullable(target_ctx->root()->is_nullable());
        expr = vectorized::VTopNPred::create_shared(node, source_node_id, target_ctx);

        expr->add_child(target_ctx->root());

        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        _predicate = &state->get_query_ctx()->get_runtime_predicate(_source_node_id);
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

        ColumnsWithTypeAndName argument_template;
        argument_template.emplace_back(nullptr, _children[0]->data_type(),
                                       _children[0]->expr_name());
        argument_template.emplace_back(nullptr, _children[0]->data_type(), "topn value");

        _function = SimpleFunctionFactory::instance().get_function(
                _predicate->is_asc() ? "le" : "ge", argument_template, _data_type,
                state->be_exec_version());
        if (!_function) {
            return Status::InternalError("get function failed");
        }
        return Status::OK();
    }

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        if (!_predicate->has_value()) {
            block->insert({create_always_true_column(block->rows(), _data_type->is_nullable()),
                           _data_type, _expr_name});
            *result_column_id = block->columns() - 1;
            return Status::OK();
        }

        Field field = _predicate->get_value();
        auto column_ptr = _children[0]->data_type()->create_column_const(1, field);
        size_t row_size = std::max(block->rows(), column_ptr->size());
        int topn_value_id = VExpr::insert_param(
                block, {column_ptr, _children[0]->data_type(), _expr_name}, row_size);

        int slot_id = -1;
        RETURN_IF_ERROR(_children[0]->execute(context, block, &slot_id));

        std::vector<size_t> arguments = {(size_t)slot_id, (size_t)topn_value_id};

        size_t num_columns_without_result = block->columns();
        block->insert({nullptr, _data_type, _expr_name});
        RETURN_IF_ERROR(_function->execute(nullptr, *block, arguments, num_columns_without_result,
                                           block->rows(), false));
        *result_column_id = num_columns_without_result;

        if (is_nullable() && _predicate->nulls_first()) {
            // null values ​​are always not filtered
            change_null_to_true(block->get_by_position(num_columns_without_result).column);
        }
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    int _source_node_id;
    std::string _expr_name;
    RuntimePredicate* _predicate = nullptr;
    FunctionBasePtr _function;
    VExprContextSPtr _target_ctx;
};
} // namespace doris::vectorized
