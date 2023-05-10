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

#include "exec/rowid_fetcher.h"

#include <brpc/callback.h>
#include <brpc/controller.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include "bthread/countdown_event.h"
#include "common/config.h"
#include "exec/tablet_info.h" // DorisNodesInfo
#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"       // ExecEnv
#include "runtime/runtime_state.h"  // RuntimeState
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h" // Block

namespace doris {

Status RowIDFetcher::init(DorisNodesInfo* nodes_info) {
    for (auto [node_id, node_info] : nodes_info->nodes_info()) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.brpc_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.brpc_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client");
        }
        _stubs.push_back(client);
    }
    return Status::OK();
}

static std::string format_rowid(const GlobalRowLoacation& location) {
    return fmt::format("{} {} {} {}", location.tablet_id,
                       location.row_location.rowset_id.to_string(),
                       location.row_location.segment_id, location.row_location.row_id);
}

PMultiGetRequest RowIDFetcher::_init_fetch_request(const vectorized::ColumnString& row_ids) {
    PMultiGetRequest mget_req;
    _tuple_desc->to_protobuf(mget_req.mutable_desc());
    for (auto slot : _tuple_desc->slots()) {
        slot->to_protobuf(mget_req.add_slots());
    }
    for (size_t i = 0; i < row_ids.size(); ++i) {
        PMultiGetRequest::RowId row_id;
        StringRef row_id_rep = row_ids.get_data_at(i);
        auto location = reinterpret_cast<const GlobalRowLoacation*>(row_id_rep.data);
        row_id.set_tablet_id(location->tablet_id);
        row_id.set_rowset_id(location->row_location.rowset_id.to_string());
        row_id.set_segment_id(location->row_location.segment_id);
        row_id.set_ordinal_id(location->row_location.row_id);
        *mget_req.add_rowids() = std::move(row_id);
    }
    mget_req.set_be_exec_version(_st->be_exec_version());
    return mget_req;
}

static void fetch_callback(bthread::CountdownEvent* counter) {
    Defer __defer([&] { counter->signal(); });
}

static Status MergeRPCResults(const std::vector<PMultiGetResponse>& rsps,
                              const std::vector<brpc::Controller>& cntls,
                              vectorized::MutableBlock* output_block) {
    for (const auto& cntl : cntls) {
        if (cntl.Failed()) {
            LOG(WARNING) << "Failed to fetch meet rpc error:" << cntl.ErrorText()
                         << ", host:" << cntl.remote_side();
            return Status::InternalError(cntl.ErrorText());
        }
    }
    for (const auto& resp : rsps) {
        Status st(resp.status());
        if (!st.ok()) {
            LOG(WARNING) << "Failed to fetch " << st.to_string();
            return st;
        }
        vectorized::Block partial_block(resp.block());
        output_block->merge(partial_block);
    }
    return Status::OK();
}

Status RowIDFetcher::fetch(const vectorized::ColumnPtr& row_ids,
                           vectorized::MutableBlock* res_block) {
    CHECK(!_stubs.empty());
    res_block->clear_column_data();
    vectorized::MutableBlock mblock({_tuple_desc}, row_ids->size());
    PMultiGetRequest mget_req = _init_fetch_request(assert_cast<const vectorized::ColumnString&>(
            *vectorized::remove_nullable(row_ids).get()));
    std::vector<PMultiGetResponse> resps(_stubs.size());
    std::vector<brpc::Controller> cntls(_stubs.size());
    bthread::CountdownEvent counter(_stubs.size());
    for (size_t i = 0; i < _stubs.size(); ++i) {
        cntls[i].set_timeout_ms(config::fetch_rpc_timeout_seconds * 1000);
        auto callback = brpc::NewCallback(fetch_callback, &counter);
        _stubs[i]->multiget_data(&cntls[i], &mget_req, &resps[i], callback);
    }
    counter.wait();
    RETURN_IF_ERROR(MergeRPCResults(resps, cntls, &mblock));
    // final sort by row_ids sequence, since row_ids is already sorted
    vectorized::Block tmp = mblock.to_block();
    std::unordered_map<std::string, uint32_t> row_order;
    vectorized::ColumnPtr row_id_column = tmp.get_columns().back();
    for (size_t x = 0; x < row_id_column->size(); ++x) {
        auto location =
                reinterpret_cast<const GlobalRowLoacation*>(row_id_column->get_data_at(x).data);
        row_order[format_rowid(*location)] = x;
    }
    for (size_t x = 0; x < row_ids->size(); ++x) {
        auto location = reinterpret_cast<const GlobalRowLoacation*>(row_ids->get_data_at(x).data);
        res_block->add_row(&tmp, row_order[format_rowid(*location)]);
    }
    return Status::OK();
}

} // namespace doris
