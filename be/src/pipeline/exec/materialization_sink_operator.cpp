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

#include "pipeline/exec/materialization_sink_operator.h"

#include <bthread/countdown_event.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>

#include <utility>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

Status MaterializationSinkOperatorX::init(const doris::TPlanNode& tnode,
                                          doris::RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.materialization_node);
    _materialization_node = tnode.materialization_node;
    _gc_id_map = tnode.materialization_node.gc_id_map;
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& fetch_expr_lists = tnode.materialization_node.fetch_expr_lists;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(fetch_expr_lists, _rowid_exprs));
    return Status::OK();
}

Status MaterializationSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_rowid_exprs, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_rowid_exprs, state));
    return Status::OK();
}

template <typename Response>
class MaterializationCallback : public ::doris::DummyBrpcCallback<Response> {
    ENABLE_FACTORY_CREATOR(MaterializationCallback);

public:
    MaterializationCallback(std::weak_ptr<TaskExecutionContext> tast_exec_ctx,
                            MaterializationSharedState* shared_state, MonotonicStopWatch& rpc_timer)
            : _tast_exec_ctx(std::move(tast_exec_ctx)),
              _shared_state(shared_state),
              _rpc_timer(rpc_timer) {}

    ~MaterializationCallback() override = default;
    MaterializationCallback(const MaterializationCallback& other) = delete;
    MaterializationCallback& operator=(const MaterializationCallback& other) = delete;

    void call() noexcept override {
        auto tast_exec_ctx = _tast_exec_ctx.lock();
        if (!tast_exec_ctx) {
            return;
        }

        _rpc_timer.stop();
        if (::doris::DummyBrpcCallback<Response>::cntl_->Failed()) {
            std::string err = fmt::format(
                    "failed to send brpc when exchange, error={}, error_text={}, client: {}, "
                    "latency = {}",
                    berror(::doris::DummyBrpcCallback<Response>::cntl_->ErrorCode()),
                    ::doris::DummyBrpcCallback<Response>::cntl_->ErrorText(),
                    BackendOptions::get_localhost(),
                    ::doris::DummyBrpcCallback<Response>::cntl_->latency_us());
            _shared_state->rpc_status = Status::InternalError(err);
        } else {
            _shared_state->rpc_status =
                    Status::create(doris::DummyBrpcCallback<Response>::response_->status());
        }
        ((CountedFinishDependency*)_shared_state->source_deps.back().get())->sub();
    }

private:
    std::weak_ptr<TaskExecutionContext> _tast_exec_ctx;
    MaterializationSharedState* _shared_state;
    MonotonicStopWatch& _rpc_timer;
};

Status MaterializationSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                          bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (!local_state._shared_state->rpc_struct_inited) {
        RETURN_IF_ERROR(
                local_state._shared_state->init_multi_requests(_materialization_node, state));
    }

    if (in_block->rows() > 0 || eos) {
        // block the pipeline wait the rpc response
        if (!eos) {
            local_state._shared_state->sink_deps.back()->block();
        }
        // execute the rowid exprs
        vectorized::Columns columns;
        if (in_block->rows() != 0) {
            local_state._shared_state->rowid_locs.resize(_rowid_exprs.size());
            for (int i = 0; i < _rowid_exprs.size(); ++i) {
                auto& rowid_expr = _rowid_exprs[i];
                RETURN_IF_ERROR(
                        rowid_expr->execute(in_block, &local_state._shared_state->rowid_locs[i]));
                columns.emplace_back(
                        in_block->get_by_position(local_state._shared_state->rowid_locs[i]).column);
            }
            local_state._shared_state->origin_block.swap(*in_block);
        }
        RETURN_IF_ERROR(
                local_state._shared_state->create_muiltget_result(columns, eos, _gc_id_map));

        for (auto& [backend_id, rpc_struct] : local_state._shared_state->rpc_struct_map) {
            auto callback = MaterializationCallback<PMultiGetResponseV2>::create_shared(
                    state->get_task_execution_context(), local_state._shared_state,
                    rpc_struct.rpc_timer);
            callback->cntl_->set_timeout_ms(config::fetch_rpc_timeout_seconds * 1000);
            auto closure =
                    AutoReleaseClosure<int, ::doris::DummyBrpcCallback<PMultiGetResponseV2>>::
                            create_unique(
                                    std::make_shared<int>(), callback, state->get_query_ctx_weak(),
                                    "Materialization Sink node id:" + std::to_string(node_id()) +
                                            " target_backend_id:" + std::to_string(backend_id));
            // send brpc request
            rpc_struct.callback = callback;
            rpc_struct.rpc_timer.start();
            rpc_struct.stub->multiget_data_v2(callback->cntl_.get(), &rpc_struct.request,
                                              callback->response_.get(), closure.release());
        }
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris