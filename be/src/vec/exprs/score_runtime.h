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

#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/virtual_slot_ref.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class ScoreRuntime {
    ENABLE_FACTORY_CREATOR(ScoreRuntime);

public:
    ScoreRuntime(VExprContextSPtr order_by_expr_ctx, bool asc, size_t limit)
            : _order_by_expr_ctx(std::move(order_by_expr_ctx)), _asc(asc), _limit(limit) {};

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) {
        RETURN_IF_ERROR(_order_by_expr_ctx->prepare(state, row_desc));
        RETURN_IF_ERROR(_order_by_expr_ctx->open(state));
        auto vir_slot_ref = std::dynamic_pointer_cast<VirtualSlotRef>(_order_by_expr_ctx->root());
        DCHECK(vir_slot_ref != nullptr);
        if (vir_slot_ref == nullptr) {
            return Status::InternalError(
                    "root of order by expr of score topn must be a VirtualSlotRef, got\n{}",
                    _order_by_expr_ctx->root()->debug_string());
        }
        DCHECK(vir_slot_ref->column_id() >= 0);
        _dest_column_idx = vir_slot_ref->column_id();
        return Status::OK();
    }

    size_t get_dest_column_idx() const { return _dest_column_idx; }

    bool is_asc() const { return _asc; }
    size_t get_limit() const { return _limit; }

private:
    VExprContextSPtr _order_by_expr_ctx;
    const bool _asc = false;
    const size_t _limit = 0;

    std::string _name = "score_runtime";
    size_t _dest_column_idx = -1;
};
using ScoreRuntimeSPtr = std::shared_ptr<ScoreRuntime>;

#include "common/compile_check_end.h"
} // namespace doris::vectorized