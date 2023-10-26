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

OPERATOR_CODE_GENERATOR(SortSinkOperator, StreamingOperator)

Status SortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<SortDependency>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<SortSinkOperatorX>();

    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    switch (p._algorithm) {
    case SortAlgorithm::HEAP_SORT: {
        _shared_state->sorter = vectorized::HeapSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc());
        break;
    }
    case SortAlgorithm::TOPN_SORT: {
        _shared_state->sorter = vectorized::TopNSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc(), state, _profile);
        break;
    }
    case SortAlgorithm::FULL_SORT: {
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

    SCOPED_TIMER(_profile->total_time_counter());
    _profile->add_info_string("TOP-N", p._limit == -1 ? "false" : "true");

    _memory_usage_counter = ADD_LABEL_COUNTER(_profile, "MemoryUsage");
    return Status::OK();
}

SortSinkOperatorX::SortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _pool(pool),
          _reuse_mem(true),
          _limit(tnode.limit),
          _use_topn_opt(tnode.sort_node.use_topn_opt),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _use_two_phase_read(tnode.sort_node.sort_info.use_two_phase_read) {}

Status SortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;

    // init runtime predicate
    if (_use_topn_opt) {
        auto query_ctx = state->get_query_ctx();
        auto first_sort_expr_node = tnode.sort_node.sort_info.ordering_exprs[0].nodes[0];
        if (first_sort_expr_node.node_type == TExprNodeType::SLOT_REF) {
            auto first_sort_slot = first_sort_expr_node.slot_ref;
            for (auto tuple_desc : _row_descriptor.tuple_descriptors()) {
                if (tuple_desc->id() != first_sort_slot.tuple_id) {
                    continue;
                }
                for (auto slot : tuple_desc->slots()) {
                    if (slot->id() == first_sort_slot.slot_id) {
                        RETURN_IF_ERROR(query_ctx->get_runtime_predicate().init(slot->type().type,
                                                                                _nulls_first[0]));
                        break;
                    }
                }
            }
        }
        if (!query_ctx->get_runtime_predicate().inited()) {
            return Status::InternalError("runtime predicate is not properly initialized");
        }
    }
    return Status::OK();
}

Status SortSinkOperatorX::prepare(RuntimeState* state) {
    const auto& row_desc = _child_x->row_desc();

    // If `limit` is smaller than HEAP_SORT_THRESHOLD, we consider using heap sort in priority.
    // To do heap sorting, each income block will be filtered by heap-top row. There will be some
    // `memcpy` operations. To ensure heap sort will not incur performance fallback, we should
    // exclude cases which incoming blocks has string column which is sensitive to operations like
    // `filter` and `memcpy`
    if (_limit > 0 && _limit + _offset < vectorized::HeapSorter::HEAP_SORT_THRESHOLD &&
        (_use_two_phase_read || _use_topn_opt || !row_desc.has_varlen_slots())) {
        _algorithm = SortAlgorithm::HEAP_SORT;
        _reuse_mem = false;
    } else if (_limit > 0 && row_desc.has_varlen_slots() &&
               _limit + _offset < vectorized::TopNSorter::TOPN_SORT_THRESHOLD) {
        _algorithm = SortAlgorithm::TOPN_SORT;
    } else {
        _algorithm = SortAlgorithm::FULL_SORT;
    }
    return _vsort_exec_exprs.prepare(state, _child_x->row_desc(), _row_descriptor);
}

Status SortSinkOperatorX::open(RuntimeState* state) {
    return _vsort_exec_exprs.open(state);
}

Status SortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                               SourceState source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(local_state._shared_state->sorter->append_block(in_block));
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("vsort, while sorting input."));

        // update runtime predicate
        if (_use_topn_opt) {
            vectorized::Field new_top = local_state._shared_state->sorter->get_top_value();
            if (!new_top.is_null() && new_top != local_state.old_top) {
                auto& sort_description = local_state._shared_state->sorter->get_sort_description();
                auto col = in_block->get_by_position(sort_description[0].column_number);
                bool is_reverse = sort_description[0].direction < 0;
                auto query_ctx = state->get_query_ctx();
                RETURN_IF_ERROR(
                        query_ctx->get_runtime_predicate().update(new_top, col.name, is_reverse));
                local_state.old_top = std::move(new_top);
            }
        }
        if (!_reuse_mem) {
            in_block->clear();
        }
    }

    if (source_state == SourceState::FINISHED) {
        RETURN_IF_ERROR(local_state._shared_state->sorter->prepare_for_read());
        local_state._dependency->set_ready_for_read();
    }
    return Status::OK();
}

} // namespace doris::pipeline
