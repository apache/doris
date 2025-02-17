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
    return Status::OK();
}

Status MaterializationSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    return Status::OK();
}

Status MaterializationSinkOperatorX::init(const doris::TPlanNode& tnode,
                                          doris::RuntimeState* state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    DCHECK(tnode.__isset.materialization_node);
    {
        // Create result_expr_ctx_lists_ from thrift exprs.
        auto& fetch_expr_lists = tnode.materialization_node.fetch_expr_lists;
        vectorized::VExprContextSPtrs ctxs;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(fetch_expr_lists, ctxs));
        _rowid_exprs = ctxs;
    }

    _fetch_row_stores = tnode.materialization_node.fetch_row_stores;
    PMultiGetRequestV2 multi_get_request;
    // init the base struct of PMultiGetRequestV2
    multi_get_request.set_be_exec_version(state->be_exec_version());
    auto query_id = multi_get_request.mutable_query_id();
    query_id->set_hi(state->query_id().hi);
    query_id->set_lo(state->query_id().lo);
    DCHECK_EQ(tnode.materialization_node.column_descs_lists.size(),
              tnode.materialization_node.slot_locs_lists.size());

    const auto& slots =
            state->desc_tbl().get_tuple_descriptor(tnode.materialization_node.tuple_id)->slots();
    for (int i = 0; i < tnode.materialization_node.column_descs_lists.size(); ++i) {
        auto block_quest = multi_get_request.add_schemas();
        // init the column_descs and slot_locs
        auto& column_descs = tnode.materialization_node.column_descs_lists[i];
        for (auto& column_desc_item : column_descs) {
            TabletColumn(column_desc_item).to_schema_pb(block_quest->add_column_descs());
        }

        auto& slot_locs = tnode.materialization_node.slot_locs_lists[i];
        for (auto& slot_loc_item : slot_locs) {
            slots[slot_loc_item]->to_protobuf(block_quest->add_slots());
        }
    }

    // init the stubs and requests for each BE
    for (const auto& node_info : tnode.materialization_node.nodes_info.nodes) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.async_internal_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.async_internal_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client, host={}, port={}",
                                         node_info.host, node_info.async_internal_port);
        }
        local_state._shared_state->rpc_struct_map.emplace(
                node_info.id,
                FetchRpcStruct {
                        .stub = std::move(client), .request = multi_get_request, .response = {}});
    }

    return Status::OK();
}

Status MaterializationSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_rowid_exprs, state, _child->row_desc()));
    RETURN_IF_ERROR(vectorized::VExpr::open(_rowid_exprs, state));
    return Status::OK();
}

Status MaterializationSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                          bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    if (in_block->rows() > 0 || eos) {
        vectorized::Columns columns;
        for (auto& _rowid_expr : _rowid_exprs) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_rowid_expr->execute(in_block, &result_column_id));
            columns.emplace_back(in_block->get_by_position(result_column_id).column);
        }

        RETURN_IF_ERROR(local_state._shared_state->create_muiltget_result(columns, eos));

        bthread::CountdownEvent counter(local_state._shared_state->rpc_struct_map.size());
        std::vector<brpc::Controller> cntls(local_state._shared_state->rpc_struct_map.size());
        size_t i = 0;

        for (auto& [_, rpc_struct] : local_state._shared_state->rpc_struct_map) {
            cntls[i].set_timeout_ms(config::fetch_rpc_timeout_seconds * 1000);
            auto callback = brpc::NewCallback(fetch_callback, &counter);
            rpc_struct.stub->multiget_data_v2(&cntls[i], &rpc_struct.request, &rpc_struct.response,
                                              callback);
            i++;
        }

        counter.wait();

        RETURN_IF_ERROR(local_state._shared_state->merge_multi_response(cntls));
    }
    return Status::OK();
}

} // namespace pipeline
} // namespace doris