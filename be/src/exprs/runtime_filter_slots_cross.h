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

#include <vector>

#include "exprs/runtime_filter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
// this class used in cross join node
template <typename ExprCtxType = ExprContext>
class RuntimeFilterSlotsCross {
public:
    RuntimeFilterSlotsCross(const std::vector<TRuntimeFilterDesc>& runtime_filter_descs,
                            const std::vector<vectorized::VExprContext*>& src_expr_ctxs)
            : _runtime_filter_descs(runtime_filter_descs), filter_src_expr_ctxs(src_expr_ctxs) {}

    ~RuntimeFilterSlotsCross() = default;

    Status init(RuntimeState* state) {
        for (auto& filter_desc : _runtime_filter_descs) {
            IRuntimeFilter* runtime_filter = nullptr;
            RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(filter_desc.filter_id,
                                                                             &runtime_filter));
            DCHECK(runtime_filter != nullptr);
            // cross join has not remote filter
            DCHECK(!runtime_filter->has_remote_target());
            _runtime_filters.push_back(runtime_filter);
        }
        return Status::OK();
    }

    Status insert(vectorized::Block* block) {
        for (int i = 0; i < _runtime_filters.size(); ++i) {
            auto* filter = _runtime_filters[i];
            auto* vexpr_ctx = filter_src_expr_ctxs[i];

            int result_column_id = -1;
            RETURN_IF_ERROR(vexpr_ctx->execute(block, &result_column_id));
            DCHECK(result_column_id != -1);
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();

            auto& column = block->get_by_position(result_column_id).column;
            if (auto* nullable =
                        vectorized::check_and_get_column<vectorized::ColumnNullable>(*column)) {
                auto& column_nested = nullable->get_nested_column_ptr();
                auto& column_nullmap = nullable->get_null_map_column_ptr();
                std::vector<int> indexs;
                for (int row_index = 0; row_index < column->size(); ++row_index) {
                    if (assert_cast<const vectorized::ColumnUInt8*>(column_nullmap.get())
                                ->get_bool(row_index)) {
                        continue;
                    }
                    indexs.push_back(row_index);
                }
                filter->insert_batch(column_nested, indexs);
            } else {
                std::vector<int> rows(column->size());
                std::iota(rows.begin(), rows.end(), 0);
                filter->insert_batch(column, rows);
            }
        }
        return Status::OK();
    }

    void publish() {
        for (auto& filter : _runtime_filters) {
            filter->publish();
        }
        for (auto& filter : _runtime_filters) {
            // todo: cross join may not need publish_finally()
            filter->publish_finally();
        }
    }

    bool empty() { return !_runtime_filters.size(); }

private:
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    const std::vector<vectorized::VExprContext*> filter_src_expr_ctxs;
    std::vector<IRuntimeFilter*> _runtime_filters;
};

using VRuntimeFilterSlotsCross = RuntimeFilterSlotsCross<vectorized::VExprContext>;
} // namespace doris
