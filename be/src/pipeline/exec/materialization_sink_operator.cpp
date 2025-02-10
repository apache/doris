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

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>

#include <utility>

#include "common/status.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "util/brpc_client_cache.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

Status MaterializationSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _shared_state->data_queue.set_sink_dependency(_dependency, 0);
    return Status::OK();
}

Status MaterializationSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    _shared_state->data_queue.set_max_blocks_in_sub_queue(state->data_queue_max_blocks());
    return Status::OK();
}

Status MaterializationSinkOperatorX::init(const doris::TPlanNode& tnode,
                                          doris::RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.materialization_node);
    {
        // Create result_expr_ctx_lists_ from thrift exprs.
        auto& fetch_expr_lists = tnode.materialization_node.fetch_expr_lists;
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(fetch_expr_lists, ctxs));
        _rowid_expr = ctxs;
    }

    for (const auto& node_info : tnode.materialization_node.nodes_info.nodes) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.async_internal_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.async_internal_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client, host={}, port={}",
                                         node_info.host, node_info.async_internal_port);
        }
        _stubs.emplace(node_info.id, std::move(client));
    }

    return Status::OK();
}

Status MaterializationSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_rowid_expr, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_rowid_expr, state));
    return Status::OK();
}

Status MaterializationSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                          bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (in_block->rows() > 0) {
        local_state._shared_state->data_queue.push_block(
                vectorized::Block::create_unique(std::move(*in_block)), 0);
    }

    if (UNLIKELY(eos)) {
        local_state._shared_state->data_queue.set_finish(0);
    }
    return Status::OK();
}

} // namespace pipeline
} // namespace doris