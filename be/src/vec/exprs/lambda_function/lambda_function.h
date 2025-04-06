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

#include <runtime/runtime_state.h>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/exprs/vcolumn_ref.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {
class VExpr;
class LambdaFunction {
public:
    virtual ~LambdaFunction() = default;

    virtual std::string get_name() const = 0;

    virtual doris::Status prepare(RuntimeState* state) {
        batch_size = state->batch_size();
        return Status::OK();
    }

    virtual doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                                  int* result_column_id, const DataTypePtr& result_type,
                                  const VExprSPtrs& children) = 0;

    static bool _contains_column_id(const std::vector<int>& output_slot_ref_indexs, int id) {
        const auto it = std::find(output_slot_ref_indexs.begin(), output_slot_ref_indexs.end(), id);
        return it != output_slot_ref_indexs.end();
    }

    static void _set_column_ref_column_id(VExprSPtr expr, int gap) {
        for (const auto& child : expr->children()) {
            if (child->is_column_ref()) {
                auto* ref = static_cast<VColumnRef*>(child.get());
                ref->set_gap(gap);
            } else {
                _set_column_ref_column_id(child, gap);
            }
        }
    }

    static void _collect_slot_ref_column_id(VExprSPtr expr,
                                            std::vector<int>& output_slot_ref_indexs) {
        for (const auto& child : expr->children()) {
            if (child->is_slot_ref()) {
                const auto* ref = static_cast<VSlotRef*>(child.get());
                output_slot_ref_indexs.push_back(ref->column_id());
            } else {
                _collect_slot_ref_column_id(child, output_slot_ref_indexs);
            }
        }
    }

    int batch_size;
};

using LambdaFunctionPtr = std::shared_ptr<LambdaFunction>;

} // namespace doris::vectorized
