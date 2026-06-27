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
#include <atomic>

#include "common/exception.h"
#include "exprs/function/function.h"
#include "exprs/lambda_function/lambda_execution_context.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {
class VColumnRef final : public VExpr {
    ENABLE_FACTORY_CREATOR(VColumnRef);

public:
    //this is different of slotref is using slot_id find a column_id
    //slotref: need to find the equal id in tuple, then return column_id, the plan of FE is very important
    //columnref: is columnid = slotid, not used to find, so you should know this column placed in block
    VColumnRef(const TExprNode& node)
            : VExpr(node),
              _column_id(node.column_ref.column_id),
              _column_name(node.column_ref.column_name) {}

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
        DCHECK_EQ(_children.size(), 0);
        if (_column_id < 0) {
            return Status::InternalError(
                    "VColumnRef have invalid slot id: {}, _column_name: {}, desc: {}", _column_id,
                    _column_name, desc.debug_string());
        }
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

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DCHECK(_open_finished || block == nullptr);
        const int column_position = _get_column_position(context, block);
        if (column_position < 0 || column_position >= block->columns()) {
            return Status::InternalError(
                    "input block not contain column ref {}, column_id={}, gap={}, block={}",
                    _column_name, _column_id, _gap.load(), block->dump_structure());
        }
        auto origin_column = block->get_by_position(column_position).column;
        result_column = filter_column_with_selector(origin_column, selector, count);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* block) const override {
        DCHECK(_open_finished || block == nullptr);
        const int column_position = _get_column_position_without_context(block);
        if (column_position < 0 || column_position >= block->columns()) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "input block not contain column ref {}, column_id={}, gap={}, block={}",
                    _column_name, _column_id, _gap.load(), block->dump_structure());
        }
        return block->get_by_position(column_position).type;
    }

    bool is_constant() const override { return false; }

    int column_id() const { return _column_id; }

    const std::string& expr_name() const override { return _column_name; }

    void set_gap(int gap) { _gap = gap; }

    int get_gap() const { return _gap.load(); }

    bool has_gap() const { return _gap.load() >= 0; }

    std::string debug_string() const override {
        std::stringstream out;
        out << "VColumnRef(slot_id: " << _column_id << ",column_name: " << _column_name
            << VExpr::debug_string() << ")";
        return out.str();
    }

    double execute_cost() const override { return 0.0; }

private:
    int _get_column_position(VExprContext* context, const Block* block) const {
        const auto resolve_result =
                context->lambda_execution_context().resolve_column_position(_column_name);
        if (resolve_result.found) {
            return resolve_result.column_position;
        }
        if (resolve_result.searched_named_scope) {
            return -1;
        }
        return _get_column_position_without_context(block);
    }

    int _get_column_position_without_context(const Block* block) const {
        if (_gap.load() == -1) {
            return _find_column_position_by_name(block);
        }
        return _column_id + _gap.load();
    }

    int _find_column_position_by_name(const Block* block) const {
        for (int position = block->columns() - 1; position >= 0; --position) {
            if (block->get_by_position(position).name == _column_name) {
                return position;
            }
        }
        return -1;
    }

    int _column_id;
    std::atomic<int> _gap = -1;
    std::string _column_name;
};
} // namespace doris
