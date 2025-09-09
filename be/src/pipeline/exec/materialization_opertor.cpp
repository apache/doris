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

#include "pipeline/exec/materialization_opertor.h"

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

Status MaterializationOperator::init(const doris::TPlanNode& tnode, doris::RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    DCHECK(tnode.__isset.materialization_node);
    _materialization_node = tnode.materialization_node;
    _gc_id_map = tnode.materialization_node.gc_id_map;
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& fetch_expr_lists = tnode.materialization_node.fetch_expr_lists;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(fetch_expr_lists, _rowid_exprs));
    return Status::OK();
}

Status MaterializationOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_rowid_exprs, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_rowid_exprs, state));
    return Status::OK();
}

static void fetch_callback(bthread::CountdownEvent* counter) {
    Defer __defer([&] { counter->signal(); });
}

bool MaterializationOperator::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return !local_state._uniq_state->origin_block.rows() && !local_state._uniq_state->last_block;
}

Status MaterializationOperator::pull(RuntimeState* state, vectorized::Block* output_block,
                                     bool* eos) const {
    auto& local_state = get_local_state(state);
    output_block->clear();
    if (local_state._uniq_state->need_merge_block) {
        local_state._uniq_state->get_block(output_block);
    }
    *eos = local_state._uniq_state->last_block;

    if (*eos) {
        uint64_t max_rpc_time = 0;
        for (auto& [_, rpc_struct] : local_state._uniq_state->rpc_struct_map) {
            max_rpc_time = std::max(max_rpc_time, rpc_struct.rpc_timer.elapsed_time());
        }
        COUNTER_SET(local_state._max_rpc_timer, (int64_t)max_rpc_time);

        for (const auto& [backend_id, child_info] :
             local_state._uniq_state->backend_profile_info_string) {
            auto child_profile = local_state.operator_profile()->create_child(
                    "RowIDFetcher: BackendId:" + std::to_string(backend_id));
            for (const auto& [info_key, info_value] :
                 local_state._uniq_state->backend_profile_info_string[backend_id]) {
                child_profile->add_info_string(info_key, "{" + fmt::to_string(info_value) + "}");
            }
            local_state.operator_profile()->add_child(child_profile, true);
        }
    }

    return Status::OK();
}

Status MaterializationOperator::push(RuntimeState* state, vectorized::Block* in_block,
                                     bool eos) const {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (!local_state._uniq_state->rpc_struct_inited) {
        RETURN_IF_ERROR(local_state._uniq_state->init_multi_requests(_materialization_node, state));
    }

    if (in_block->rows() > 0 || eos) {
        // execute the rowid exprs
        vectorized::Columns columns;
        if (in_block->rows() != 0) {
            local_state._uniq_state->rowid_locs.resize(_rowid_exprs.size());
            for (int i = 0; i < _rowid_exprs.size(); ++i) {
                auto& rowid_expr = _rowid_exprs[i];
                RETURN_IF_ERROR(
                        rowid_expr->execute(in_block, &local_state._uniq_state->rowid_locs[i]));
                columns.emplace_back(
                        in_block->get_by_position(local_state._uniq_state->rowid_locs[i]).column);
            }
            local_state._uniq_state->origin_block.swap(*in_block);
        }
        RETURN_IF_ERROR(local_state._uniq_state->create_muiltget_result(columns, eos, _gc_id_map));

        auto size = local_state._uniq_state->rpc_struct_map.size();
        bthread::CountdownEvent counter(static_cast<int>(size));
        MonotonicStopWatch rpc_timer(true);
        for (auto& [backend_id, rpc_struct] : local_state._uniq_state->rpc_struct_map) {
            auto callback = brpc::NewCallback(fetch_callback, &counter);
            rpc_struct.cntl->set_timeout_ms(state->execution_timeout() * 1000);
            // send brpc request
            rpc_struct.stub->multiget_data_v2(rpc_struct.cntl.get(), &rpc_struct.request,
                                              &rpc_struct.response, callback);
        }
        counter.wait();
        if (auto time = rpc_timer.elapsed_time(); time > local_state._max_rpc_timer->value()) {
            local_state._max_rpc_timer->set(static_cast<int64_t>(time));
        }

        for (auto& [backend_id, rpc_struct] : local_state._uniq_state->rpc_struct_map) {
            if (rpc_struct.cntl->Failed()) {
                std::string error_text =
                        "Failed to send brpc request, error_text=" + rpc_struct.cntl->ErrorText() +
                        " Materialization Sink node id:" + std::to_string(node_id()) +
                        " target_backend_id:" + std::to_string(backend_id);
                return Status::InternalError(error_text);
            }
            rpc_struct.cntl->Reset();
        }

        if (local_state._uniq_state->need_merge_block) {
            SCOPED_TIMER(local_state._merge_response_timer);
            RETURN_IF_ERROR(local_state._uniq_state->merge_multi_response());
        }
        local_state._uniq_state->last_block = eos;
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris