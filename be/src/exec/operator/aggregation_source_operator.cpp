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
#include "exec/common/groupby_agg_context.h"
#include "exec/common/ungroupby_agg_context.h"
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

    // Init source-side profile counters on the groupby context if present.
    if (_shared_state->groupby_agg_ctx) {
        _shared_state->groupby_agg_ctx->init_source_profile(custom_profile());
    }

    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._without_key) {
        if (p._needs_finalize) {
            _executor.get_result = [this](RuntimeState* state, Block* block, bool* eos) {
                return _shared_state->ungroupby_agg_ctx->get_finalized_result(
                        state, block, eos,
                        _parent->cast<AggSourceOperatorX>().row_descriptor());
            };
        } else {
            _executor.get_result = [this](RuntimeState* state, Block* block, bool* eos) {
                return _shared_state->ungroupby_agg_ctx->get_serialized_result(state, block, eos);
            };
        }
    } else {
        if (p._needs_finalize) {
            auto columns_with_schema = VectorizedUtils::create_columns_with_type_and_name(
                    p.row_descriptor());
            _executor.get_result = [this, cols = std::move(columns_with_schema)](
                                           RuntimeState* state, Block* block, bool* eos) {
                return _shared_state->groupby_agg_ctx->get_finalized_results(
                        state, block, eos, cols);
            };
        } else {
            _executor.get_result = [this](RuntimeState* state, Block* block, bool* eos) {
                return _shared_state->groupby_agg_ctx->get_serialized_results(
                        state, block, eos);
            };
        }
    }

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
    RETURN_IF_ERROR(local_state._executor.get_result(state, block, eos));
    local_state.make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    local_state.do_agg_limit(block, eos);
    return Status::OK();
}

void AggLocalState::do_agg_limit(Block* block, bool* eos) {
    auto* ctx = _shared_state->groupby_agg_ctx.get();
    if (!ctx) {
        // without key, no limit/sort-limit support
        if (auto rows = block->rows()) {
            _num_rows_returned += rows;
        }
        return;
    }
    if (ctx->reach_limit) {
        if (ctx->do_sort_limit) {
            const size_t key_size = ctx->groupby_expr_ctxs().size();
            ColumnRawPtrs key_columns(key_size);
            for (size_t i = 0; i < key_size; ++i) {
                key_columns[i] = block->get_by_position(i).column.get();
            }
            if (ctx->do_limit_filter(block->rows(), key_columns)) {
                Block::filter_block_internal(block, ctx->need_computes());
                if (auto rows = block->rows()) {
                    _num_rows_returned += rows;
                }
            }
        } else {
            reached_limit(block, eos);
        }
    } else {
        if (auto rows = block->rows()) {
            _num_rows_returned += rows;
        }
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
    auto* ctx = _shared_state->groupby_agg_ctx.get();
    DCHECK(ctx);
    return ctx->merge_with_serialized_key_for_spill(block);
}

Status AggSourceOperatorX::merge_with_serialized_key_helper(RuntimeState* state, Block* block) {
    auto& local_state = get_local_state(state);
    return local_state.merge_with_serialized_key_helper(block);
}

size_t AggSourceOperatorX::get_estimated_memory_size_for_merging(RuntimeState* state,
                                                                 size_t rows) const {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->groupby_agg_ctx.get();
    DCHECK(ctx);
    size_t size = std::visit(
            Overload {
                    [&](std::monostate& arg) -> size_t { return 0; },
                    [&](auto& agg_method) { return agg_method.hash_table->estimate_memory(rows); }},
            ctx->hash_table_data()->method_variant);
    size += ctx->agg_data_container()->estimate_memory(rows);
    return size;
}

Status AggSourceOperatorX::reset_hash_table(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->groupby_agg_ctx.get();
    DCHECK(ctx);
    return ctx->reset_hash_table();
}

Status AggSourceOperatorX::get_serialized_block(RuntimeState* state, Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    auto* ctx = local_state._shared_state->groupby_agg_ctx.get();
    DCHECK(ctx);
    return ctx->get_serialized_results(state, block, eos);
}

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    return Base::close(state);
}

} // namespace doris
