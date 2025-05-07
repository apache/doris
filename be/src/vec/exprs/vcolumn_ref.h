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

#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {
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

    Status execute(VExprContext* context, Block* block, int* result_column_id) override {
        DCHECK(_open_finished || _getting_const_col);
        *result_column_id = _column_id + _gap;
        return Status::OK();
    }

    bool is_constant() const override { return false; }

    int column_id() const { return _column_id; }

    const std::string& expr_name() const override { return _column_name; }

    void set_gap(int gap) {
        if (_gap == 0) {
            _gap = gap;
        }
    }

    std::string debug_string() const override {
        std::stringstream out;
        out << "VColumnRef(slot_id: " << _column_id << ",column_name: " << _column_name
            << VExpr::debug_string() << ")";
        return out.str();
    }

private:
    int _column_id;
    std::atomic<int> _gap = 0;
    std::string _column_name;
};
} // namespace vectorized
} // namespace doris
