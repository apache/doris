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

#include "exchange_source_operator.h"

#include <fmt/core.h>

#include <cstdint>
#include <memory>

#include "pipeline/exec/operator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
ExchangeLocalState::ExchangeLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent), num_rows_skipped(0), is_ready(false) {}

std::string ExchangeLocalState::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}", Base::debug_string(indentation_level));
    fmt::format_to(debug_string_buffer, ", Queues: (");
    const auto& queues = stream_recvr->sender_queues();
    for (size_t i = 0; i < queues.size(); i++) {
        fmt::format_to(debug_string_buffer,
                       "No. {} queue: (_num_remaining_senders = {}, block_queue size = {})", i,
                       queues[i]->_num_remaining_senders, queues[i]->_block_queue.size());
    }
    fmt::format_to(debug_string_buffer, ")");
    return fmt::to_string(debug_string_buffer);
}

std::string ExchangeSourceOperatorX::debug_string(int indentation_level) const {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}",
                   OperatorX<ExchangeLocalState>::debug_string(indentation_level));
    fmt::format_to(debug_string_buffer, ", Info: (_num_senders = {}, _is_merging = {})",
                   _num_senders, _is_merging);
    return fmt::to_string(debug_string_buffer);
}

void ExchangeLocalState::create_stream_recvr(RuntimeState* state) {
    auto& p = _parent->cast<ExchangeSourceOperatorX>();
    stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _memory_used_counter, state->fragment_instance_id(), p.node_id(),
            p.num_senders(), custom_profile(), p.is_merging(),
            std::max(20480, config::exchg_node_buffer_size_bytes /
                                    (p.is_merging() ? p.num_senders() : 1)));
}

Status ExchangeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    get_data_from_recvr_timer = ADD_TIMER(custom_profile(), "GetDataFromRecvrTime");
    filter_timer = ADD_TIMER(custom_profile(), "FilterTime");
    create_merger_timer = ADD_TIMER(custom_profile(), "CreateMergerTime");
    custom_profile()->add_info_string("InstanceID", print_id(state->fragment_instance_id()));
    return Status::OK();
}

Status ExchangeLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    create_stream_recvr(state);
    const auto& queues = stream_recvr->sender_queues();
    deps.resize(queues.size());
    metrics.resize(queues.size());
    static const std::string timer_name = "WaitForDependencyTime";
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), timer_name, 1);
    for (size_t i = 0; i < queues.size(); i++) {
        deps[i] = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                            fmt::format("SHUFFLE_DATA_DEPENDENCY_{}", i));
        queues[i]->set_dependency(deps[i]);
        metrics[i] = custom_profile()->add_nonzero_counter(fmt::format("WaitForData{}", i),
                                                           TUnit ::TIME_NS, timer_name, 1);
    }

    auto& p = _parent->cast<ExchangeSourceOperatorX>();
    if (p.is_merging()) {
        RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, vsort_exec_exprs));
    }
    return Status::OK();
}

ExchangeSourceOperatorX::ExchangeSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs,
                                                 int num_senders)
        : OperatorX<ExchangeLocalState>(pool, tnode, operator_id, descs),
          _num_senders(num_senders),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _partition_type(tnode.exchange_node.__isset.partition_type
                                  ? tnode.exchange_node.partition_type
                                  : TPartitionType::UNPARTITIONED),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0) {}

Status ExchangeSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<ExchangeLocalState>::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;

    return Status::OK();
}

Status ExchangeSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<ExchangeLocalState>::prepare(state));
    DCHECK_GT(_num_senders, 0);

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor));
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    }
    return Status::OK();
}

Status ExchangeSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                          bool* eos) {
    auto& local_state = get_local_state(state);
    Defer is_eos([&]() {
        if (*eos) {
            local_state.stream_recvr->set_sink_dep_always_ready();
        }
    });
    SCOPED_TIMER(local_state.exec_time_counter());
    if (_is_merging && !local_state.is_ready) {
        SCOPED_TIMER(local_state.create_merger_timer);
        RETURN_IF_ERROR(local_state.stream_recvr->create_merger(
                local_state.vsort_exec_exprs.ordering_expr_ctxs(), _is_asc_order, _nulls_first,
                state->batch_size(), _limit, _offset));
        local_state.is_ready = true;
        return Status::OK();
    }
    {
        SCOPED_TIMER(local_state.get_data_from_recvr_timer);
        RETURN_IF_ERROR(local_state.stream_recvr->get_next(block, eos));
    }
    {
        SCOPED_TIMER(local_state.filter_timer);
        RETURN_IF_ERROR(doris::vectorized::VExprContext::filter_block(local_state.conjuncts(),
                                                                      block, block->columns()));
    }

    // In non-merge cases, if eos = true, the block must be empty
    // In merge cases, this cannot be guaranteed
    if (!*eos || block->rows() > 0) {
        if (!_is_merging) {
            // If it is merging, we will handle the offset inside the merger, and the exchange source does not need to handle it
            if (local_state.num_rows_skipped + block->rows() < _offset) {
                local_state.num_rows_skipped += block->rows();
                block->set_num_rows(0);
            } else if (local_state.num_rows_skipped < _offset) {
                int64_t offset = _offset - local_state.num_rows_skipped;
                local_state.num_rows_skipped = _offset;
                // should skip some rows
                block->skip_num_rows(offset);
            }
        }
        // Merge actually also handles the limit, but handling the limit one more time will not cause correctness issues
        if (local_state.num_rows_returned() + block->rows() < _limit) {
            local_state.add_num_rows_returned(block->rows());
        } else {
            *eos = true;
            auto limit = _limit - local_state.num_rows_returned();
            block->set_num_rows(limit);
            local_state.set_num_rows_returned(_limit);
        }
    }
    return Status::OK();
}

Status ExchangeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    const auto& queues = stream_recvr->sender_queues();
    for (size_t i = 0; i < deps.size(); i++) {
        COUNTER_SET(metrics[i], deps[i]->watcher_elapse_time());
    }
    if (stream_recvr != nullptr) {
        stream_recvr->close();
    }
    if (_parent->cast<ExchangeSourceOperatorX>()._is_merging) {
        vsort_exec_exprs.close(state);
    }
    return Base::close(state);
}

Status ExchangeSourceOperatorX::close(RuntimeState* state) {
    if (_is_merging && !is_closed()) {
        _vsort_exec_exprs.close(state);
    }
    _is_closed = true;
    return OperatorX<ExchangeLocalState>::close(state);
}
} // namespace doris::pipeline
