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

#include <memory>

#include "pipeline/exec/operator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/exec/vexchange_node.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(ExchangeSourceOperator, SourceOperator)

bool ExchangeSourceOperator::can_read() {
    return _node->_stream_recvr->ready_to_read();
}

bool ExchangeSourceOperator::is_pending_finish() const {
    return false;
}

ExchangeLocalState::ExchangeLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<>(state, parent), num_rows_skipped(0), is_ready(false) {}

Status ExchangeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<ExchangeSourceOperatorX>();
    stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, p.input_row_desc(), state->fragment_instance_id(), p.node_id(), p.num_senders(),
            profile(), p.is_merging(), p.sub_plan_query_statistics_recvr());
    source_dependency = AndDependency::create_shared(_parent->operator_id());
    const auto& queues = stream_recvr->sender_queues();
    deps.resize(queues.size());
    metrics.resize(queues.size());
    for (size_t i = 0; i < queues.size(); i++) {
        deps[i] = ExchangeDataDependency::create_shared(_parent->operator_id(), queues[i]);
        queues[i]->set_dependency(deps[i]);
        source_dependency->add_child(deps[i]);
    }
    for (size_t i = 0; i < queues.size(); i++) {
        static const std::string timer_name =
                "WaitForDependency[" + source_dependency->name() + "]Time";
        _wait_for_dependency_timer = ADD_TIMER(_runtime_profile, timer_name);
        metrics[i] = ADD_CHILD_TIMER(_runtime_profile, "WaitForData", timer_name);
    }
    RETURN_IF_ERROR(_parent->cast<ExchangeSourceOperatorX>()._vsort_exec_exprs.clone(
            state, vsort_exec_exprs));
    return Status::OK();
}

Status ExchangeLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXLocalState<>::open(state));
    return Status::OK();
}

ExchangeSourceOperatorX::ExchangeSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                 int operator_id, const DescriptorTbl& descs,
                                                 int num_senders)
        : OperatorX<ExchangeLocalState>(pool, tnode, operator_id, descs),
          _num_senders(num_senders),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _input_row_desc(descs, tnode.exchange_node.input_row_tuples,
                          std::vector<bool>(tnode.nullable_tuples.begin(),
                                            tnode.nullable_tuples.begin() +
                                                    tnode.exchange_node.input_row_tuples.size())),
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
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor));
    }

    return Status::OK();
}

Status ExchangeSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<ExchangeLocalState>::open(state));
    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    }
    return Status::OK();
}

Status ExchangeSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                          SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (_is_merging && !local_state.is_ready) {
        RETURN_IF_ERROR(local_state.stream_recvr->create_merger(
                local_state.vsort_exec_exprs.lhs_ordering_expr_ctxs(), _is_asc_order, _nulls_first,
                state->batch_size(), _limit, _offset));
        local_state.is_ready = true;
        return Status::OK();
    }
    bool eos = false;
    auto status = local_state.stream_recvr->get_next(block, &eos);
    RETURN_IF_ERROR(doris::vectorized::VExprContext::filter_block(local_state.conjuncts(), block,
                                                                  block->columns()));
    // In vsortrunmerger, it will set eos=true, and block not empty
    // so that eos==true, could not make sure that block not have valid data
    if (!eos || block->rows() > 0) {
        if (!_is_merging) {
            if (local_state.num_rows_skipped + block->rows() < _offset) {
                local_state.num_rows_skipped += block->rows();
                block->set_num_rows(0);
            } else if (local_state.num_rows_skipped < _offset) {
                auto offset = _offset - local_state.num_rows_skipped;
                local_state.num_rows_skipped = _offset;
                block->set_num_rows(block->rows() - offset);
            }
        }
        if (local_state.num_rows_returned() + block->rows() < _limit) {
            local_state.add_num_rows_returned(block->rows());
        } else {
            eos = true;
            auto limit = _limit - local_state.num_rows_returned();
            block->set_num_rows(limit);
            local_state.set_num_rows_returned(_limit);
        }
        COUNTER_SET(local_state.rows_returned_counter(), local_state.num_rows_returned());
        COUNTER_UPDATE(local_state.blocks_returned_counter(), 1);
    }
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    return status;
}

Status ExchangeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    const auto& queues = stream_recvr->sender_queues();
    for (size_t i = 0; i < deps.size(); i++) {
        COUNTER_SET(metrics[i], deps[i]->read_watcher_elapse_time());
    }
    if (stream_recvr != nullptr) {
        stream_recvr->close();
    }
    if (_parent->cast<ExchangeSourceOperatorX>()._is_merging) {
        vsort_exec_exprs.close(state);
    }
    return PipelineXLocalState<>::close(state);
}

Status ExchangeSourceOperatorX::close(RuntimeState* state) {
    if (_is_merging && !is_closed()) {
        _vsort_exec_exprs.close(state);
    }
    _is_closed = true;
    return OperatorX<ExchangeLocalState>::close(state);
}
} // namespace doris::pipeline
