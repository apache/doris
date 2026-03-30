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

#include "exec/operator/aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "exec/operator/operator.h"
#include "exprs/vectorized_agg_fn.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"

AggLocalState::AggLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}

Status AggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    return Status::OK();
}

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    return Base::close(state);
}

Status AggLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    return Status::OK();
}

AggSourceOperatorX::AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                       const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _needs_finalize(tnode.agg_node.need_finalize),
          _without_key(tnode.agg_node.grouping_exprs.empty()) {}

Status AggSourceOperatorX::get_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    SCOPED_PEAK_MEM(&local_state._estimate_memory_usage);
    local_state._ensure_agg_source_ready();
    auto* ctx = local_state._shared_state->agg_ctx.get();
    if (_needs_finalize) {
        RETURN_IF_ERROR(ctx->finalize(state, block, eos));
    } else {
        RETURN_IF_ERROR(ctx->serialize(state, block, eos));
    }
    local_state.make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    local_state.do_agg_limit(block, eos);
    return Status::OK();
}

void AggLocalState::do_agg_limit(Block* block, bool* eos) {
    auto* ctx = _shared_state->agg_ctx.get();
    if (ctx->apply_limit_filter(block)) {
        reached_limit(block, eos);
    } else if (auto rows = block->rows()) {
        _num_rows_returned += rows;
    }
}

void AggLocalState::make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status AggLocalState::merge_with_serialized_key_helper(Block* block) {
    _ensure_agg_source_ready();
    auto* ctx = _shared_state->agg_ctx.get();
    DCHECK(ctx);
    return ctx->merge_for_spill(block);
}

void AggLocalState::_ensure_agg_source_ready() {
    if (_agg_source_ready) {
        return;
    }
    _agg_source_ready = true;
    auto* ctx = _shared_state->agg_ctx.get();
    if (!ctx) {
        return;
    }
    ctx->init_source_profile(custom_profile());
    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._needs_finalize) {
        ctx->set_finalize_output(p.row_descriptor());
    }
}

Status AggSourceOperatorX::merge_with_serialized_key_helper(RuntimeState* state, Block* block) {
    auto& local_state = get_local_state(state);
    return local_state.merge_with_serialized_key_helper(block);
}

size_t AggSourceOperatorX::get_estimated_memory_size_for_merging(RuntimeState* state,
                                                                 size_t rows) const {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->agg_ctx.get();
    DCHECK(ctx);
    return ctx->estimated_memory_for_merging(rows);
}

Status AggSourceOperatorX::reset_hash_table(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->agg_ctx.get();
    DCHECK(ctx);
    return ctx->reset_hash_table();
}

Status AggSourceOperatorX::get_serialized_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->agg_ctx.get();
    DCHECK(ctx);
    return ctx->serialize(state, block, eos);
}

} // namespace doris
