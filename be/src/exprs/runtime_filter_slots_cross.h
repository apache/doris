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

#include "common/status.h"
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
class VRuntimeFilterSlotsCross {
public:
    VRuntimeFilterSlotsCross(const std::vector<TRuntimeFilterDesc>& runtime_filter_descs,
                             const vectorized::VExprContextSPtrs& src_expr_ctxs)
            : _runtime_filter_descs(runtime_filter_descs), filter_src_expr_ctxs(src_expr_ctxs) {}

    ~VRuntimeFilterSlotsCross() = default;

    Status init(RuntimeState* state) {
        for (auto& filter_desc : _runtime_filter_descs) {
            IRuntimeFilter* runtime_filter = nullptr;
            RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(filter_desc.filter_id,
                                                                             &runtime_filter));
            if (runtime_filter == nullptr) {
                return Status::InternalError("runtime filter is nullptr");
            }
            // cross join has not remote filter for bitmap filter(non shuffle join)
            if (runtime_filter->type() == RuntimeFilterType::BITMAP_FILTER &&
                runtime_filter->has_remote_target()) {
                return Status::InternalError("cross join runtime filter has remote target");
            }
            _runtime_filters.push_back(runtime_filter);
        }
        return Status::OK();
    }

    Status insert(vectorized::Block* block) {
        for (int i = 0; i < _runtime_filters.size(); ++i) {
            auto* filter = _runtime_filters[i];
            const auto& vexpr_ctx = filter_src_expr_ctxs[i];

            int result_column_id = -1;
            RETURN_IF_ERROR(vexpr_ctx->execute(block, &result_column_id));
            DCHECK(result_column_id != -1);
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();

            filter->insert_batch(block->get_by_position(result_column_id).column, 0);
        }
        return Status::OK();
    }

    Status publish() {
        for (auto& filter : _runtime_filters) {
            RETURN_IF_ERROR(filter->publish());
        }
        return Status::OK();
    }

    bool empty() { return _runtime_filters.empty(); }

private:
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    const vectorized::VExprContextSPtrs filter_src_expr_ctxs;
    std::vector<IRuntimeFilter*> _runtime_filters;
};

} // namespace doris
