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
#include "exprs/runtime_filter_slots.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
// this class used in cross join node
class RuntimeFilterSlotsCross : public RuntimeFilterSlots {
public:
    RuntimeFilterSlotsCross(const vectorized::VExprContextSPtrs& build_expr_ctxs,
                            RuntimeProfile* profile,
                            const std::vector<std::shared_ptr<IRuntimeFilter>>& runtime_filters,
                            bool should_build_hash_table)
            : RuntimeFilterSlots(build_expr_ctxs, profile, runtime_filters,
                                 should_build_hash_table) {}

    Status process(RuntimeState* state, vectorized::Blocks& blocks) {
        RETURN_IF_ERROR(_init(state));
        for (auto& block : blocks) {
            RETURN_IF_ERROR(_process_block(&block));
        }
        return _publish(state);
    }

private:
    Status _init(RuntimeState* state) {
        for (auto runtime_filter : _runtime_filters) {
            if (runtime_filter == nullptr) {
                return Status::InternalError("runtime filter is nullptr");
            }
            // cross join has not remote filter for bitmap filter(non shuffle join)
            if (runtime_filter->type() == RuntimeFilterType::BITMAP_FILTER &&
                runtime_filter->has_remote_target()) {
                return Status::InternalError("cross join runtime filter has remote target");
            }
        }
        return Status::OK();
    }

    Status _process_block(vectorized::Block* block) {
        for (int i = 0; i < _runtime_filters.size(); ++i) {
            auto filter = _runtime_filters[i];
            const auto& vexpr_ctx = _build_expr_context[i];

            int result_column_id = -1;
            RETURN_IF_ERROR(vexpr_ctx->execute(block, &result_column_id));
            DCHECK(result_column_id != -1);
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
        }
        _insert(block, 0);
        return Status::OK();
    }
};

} // namespace doris
