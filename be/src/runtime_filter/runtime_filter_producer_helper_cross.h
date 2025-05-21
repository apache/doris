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
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "runtime_filter/runtime_filter_producer_helper.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
#include "common/compile_check_begin.h"
// this class used in cross join node
class RuntimeFilterProducerHelperCross : public RuntimeFilterProducerHelper {
public:
    ~RuntimeFilterProducerHelperCross() override = default;

    RuntimeFilterProducerHelperCross() : RuntimeFilterProducerHelper(true, false) {}

    Status process(RuntimeState* state, vectorized::Blocks& blocks) {
        for (auto& block : blocks) {
            RETURN_IF_ERROR(_process_block(&block));
        }

        for (auto filter : _producers) {
            filter->set_wrapper_state_and_ready_to_publish(RuntimeFilterWrapper::State::READY);
        }
        return publish(state);
    }

private:
    Status _process_block(vectorized::Block* block) {
        for (const auto& vexpr_ctx : _filter_expr_contexts) {
            int result_column_id = -1;
            RETURN_IF_ERROR(vexpr_ctx->execute(block, &result_column_id));
            DCHECK_NE(result_column_id, -1) << vexpr_ctx->root()->debug_string();
            block->get_by_position(result_column_id).column =
                    block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
        }
        RETURN_IF_ERROR(_insert(block, 0));
        return Status::OK();
    }

    void _init_expr(const vectorized::VExprContextSPtrs& build_expr_ctxs,
                    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs) override {
        _filter_expr_contexts = build_expr_ctxs;
    }
};
#include "common/compile_check_end.h"
} // namespace doris
