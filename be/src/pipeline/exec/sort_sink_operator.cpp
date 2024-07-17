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

#include "sort_sink_operator.h"

#include <string>

#include "pipeline/exec/operator.h"
#include "runtime/query_context.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"

namespace doris::pipeline {

Status SortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _sort_blocks_memory_usage =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SortBlocks", TUnit::BYTES, "MemoryUsage", 1);
    return Status::OK();
}

Status SortSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = _parent->cast<SortSinkOperatorX>();

    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    switch (p._algorithm) {
    case TSortAlgorithm::HEAP_SORT: {
        _shared_state->sorter = vectorized::HeapSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc());
        break;
    }
    case TSortAlgorithm::TOPN_SORT: {
        _shared_state->sorter = vectorized::TopNSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc(), state, _profile);
        break;
    }
    case TSortAlgorithm::FULL_SORT: {
        _shared_state->sorter = vectorized::FullSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc(), state, _profile);
        break;
    }
    default: {
        return Status::InvalidArgument("Invalid sort algorithm!");
    }
    }

    _shared_state->sorter->init_profile(_profile);

    _profile->add_info_string("TOP-N", p._limit == -1 ? "false" : "true");
    return Status::OK();
}

SortSinkOperatorX::SortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs,
                                     const bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _pool(pool),
          _limit(tnode.limit),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _merge_by_exchange(tnode.sort_node.merge_by_exchange),
          _is_colocate(tnode.sort_node.__isset.is_colocate && tnode.sort_node.is_colocate),
          _require_bucket_distribution(require_bucket_distribution),
          _is_analytic_sort(tnode.sort_node.__isset.is_analytic_sort
                                    ? tnode.sort_node.is_analytic_sort
                                    : false),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _algorithm(tnode.sort_node.__isset.algorithm ? tnode.sort_node.algorithm
                                                       : TSortAlgorithm::FULL_SORT),
          _reuse_mem(_algorithm != TSortAlgorithm::HEAP_SORT) {}

Status SortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;

    auto* query_ctx = state->get_query_ctx();
    // init runtime predicate
    if (query_ctx->has_runtime_predicate(_node_id)) {
        query_ctx->get_runtime_predicate(_node_id).set_detected_source();
    }
    return Status::OK();
}

Status SortSinkOperatorX::prepare(RuntimeState* state) {
    return _vsort_exec_exprs.prepare(state, _child_x->row_desc(), _row_descriptor);
}

Status SortSinkOperatorX::open(RuntimeState* state) {
    return _vsort_exec_exprs.open(state);
}

Status SortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        COUNTER_UPDATE(local_state._sort_blocks_memory_usage, (int64_t)in_block->bytes());
        RETURN_IF_ERROR(local_state._shared_state->sorter->append_block(in_block));
        local_state._mem_tracker->set_consumption(local_state._shared_state->sorter->data_size());
        RETURN_IF_CANCELLED(state);

        if (state->get_query_ctx()->has_runtime_predicate(_node_id)) {
            auto& predicate = state->get_query_ctx()->get_runtime_predicate(_node_id);
            if (predicate.enable()) {
                vectorized::Field new_top = local_state._shared_state->sorter->get_top_value();
                if (!new_top.is_null() && new_top != local_state.old_top) {
                    auto* query_ctx = state->get_query_ctx();
                    RETURN_IF_ERROR(query_ctx->get_runtime_predicate(_node_id).update(new_top));
                    local_state.old_top = std::move(new_top);
                }
            }
        }
        if (!_reuse_mem) {
            in_block->clear();
        }
    }

    if (eos) {
        RETURN_IF_ERROR(local_state._shared_state->sorter->prepare_for_read());
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t SortSinkOperatorX::get_revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->data_size();
}

Status SortSinkOperatorX::prepare_for_spill(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->prepare_for_read();
}

Status SortSinkOperatorX::merge_sort_read_for_spill(RuntimeState* state,
                                                    doris::vectorized::Block* block, int batch_size,
                                                    bool* eos) {
    auto& local_state = get_local_state(state);
    return local_state._shared_state->sorter->merge_sort_read_for_spill(state, block, batch_size,
                                                                        eos);
}
void SortSinkOperatorX::reset(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    local_state._shared_state->sorter->reset();
}
} // namespace doris::pipeline
