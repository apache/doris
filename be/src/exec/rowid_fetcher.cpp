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
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/countdown_event.h"
#include "common/config.h"
#include "common/consts.h"
#include "exec/tablet_info.h" // DorisNodesInfo
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"      // ExecEnv
#include "runtime/runtime_state.h" // RuntimeState
#include "runtime/types.h"
#include "util/brpc_client_cache.h" // BrpcClientCache
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h" // Block
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/jsonb/serialize.h"

namespace doris {

Status RowIDFetcher::init() {
    DorisNodesInfo nodes_info;
    nodes_info.setNodes(_fetch_option.t_fetch_opt.nodes_info);
    for (auto [node_id, node_info] : nodes_info.nodes_info()) {
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

PMultiGetRequest RowIDFetcher::_init_fetch_request(const vectorized::ColumnString& row_locs) const {
    PMultiGetRequest mget_req;
    _fetch_option.desc->to_protobuf(mget_req.mutable_desc());
    for (SlotDescriptor* slot : _fetch_option.desc->slots()) {
        // ignore rowid
        if (slot->col_name() == BeConsts::ROWID_COL) {
            continue;
        }
        slot->to_protobuf(mget_req.add_slots());
    }
    for (size_t i = 0; i < row_locs.size(); ++i) {
        PRowLocation row_loc;
        StringRef row_id_rep = row_locs.get_data_at(i);
        // TODO: When transferring data between machines with different byte orders (endianness),
        // not performing proper handling may lead to issues in parsing and exchanging the data.
        auto location = reinterpret_cast<const GlobalRowLoacation*>(row_id_rep.data);
        row_loc.set_tablet_id(location->tablet_id);
        row_loc.set_rowset_id(location->row_location.rowset_id.to_string());
        row_loc.set_segment_id(location->row_location.segment_id);
        row_loc.set_ordinal_id(location->row_location.row_id);
        *mget_req.add_row_locs() = std::move(row_loc);
    }
    // Set column desc
    for (const TColumn& tcolumn : _fetch_option.t_fetch_opt.column_desc) {
        TabletColumn column(tcolumn);
        column.to_schema_pb(mget_req.add_column_desc());
    }
    PUniqueId& query_id = *mget_req.mutable_query_id();
    query_id.set_hi(_fetch_option.runtime_state->query_id().hi);
    query_id.set_lo(_fetch_option.runtime_state->query_id().lo);
    mget_req.set_be_exec_version(_fetch_option.runtime_state->be_exec_version());
    mget_req.set_fetch_row_store(_fetch_option.t_fetch_opt.fetch_row_store);
    return mget_req;
}

static void fetch_callback(bthread::CountdownEvent* counter) {
    Defer __defer([&] { counter->signal(); });
}

Status RowIDFetcher::_merge_rpc_results(const PMultiGetRequest& request,
                                        const std::vector<PMultiGetResponse>& rsps,
                                        const std::vector<brpc::Controller>& cntls,
                                        vectorized::Block* output_block,
                                        std::vector<PRowLocation>* rows_id) const {
    output_block->clear();
    for (const auto& cntl : cntls) {
        if (cntl.Failed()) {
            LOG(WARNING) << "Failed to fetch meet rpc error:" << cntl.ErrorText()
                         << ", host:" << cntl.remote_side();
            return Status::InternalError(cntl.ErrorText());
        }
    }
    vectorized::DataTypeSerDeSPtrs serdes;
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    auto merge_function = [&](const PMultiGetResponse& resp) {
        Status st(Status::create(resp.status()));
        if (!st.ok()) {
            LOG(WARNING) << "Failed to fetch " << st.to_string();
            return st;
        }
        for (const PRowLocation& row_id : resp.row_locs()) {
            rows_id->push_back(row_id);
        }
        // Merge binary rows
        if (request.fetch_row_store()) {
            CHECK(resp.row_locs().size() == resp.binary_row_data_size());
            if (output_block->is_empty_column()) {
                *output_block = vectorized::Block(_fetch_option.desc->slots(), 1);
            }
            if (serdes.empty() && col_uid_to_idx.empty()) {
                serdes = vectorized::create_data_type_serdes(_fetch_option.desc->slots());
                for (int i = 0; i < _fetch_option.desc->slots().size(); ++i) {
                    col_uid_to_idx[_fetch_option.desc->slots()[i]->col_unique_id()] = i;
                }
            }
            for (int i = 0; i < resp.binary_row_data_size(); ++i) {
                vectorized::JsonbSerializeUtil::jsonb_to_block(
                        serdes, resp.binary_row_data(i).data(), resp.binary_row_data(i).size(),
                        col_uid_to_idx, *output_block);
            }
            return Status::OK();
        }
        // Merge partial blocks
        vectorized::Block partial_block;
        RETURN_IF_ERROR(partial_block.deserialize(resp.block()));
        if (partial_block.is_empty_column()) {
            return Status::OK();
        }
        CHECK(resp.row_locs().size() == partial_block.rows());
        if (output_block->is_empty_column()) {
            output_block->swap(partial_block);
        } else if (partial_block.columns() != output_block->columns()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Merge block not match, self:[{}], input:[{}], ", output_block->dump_types(),
                    partial_block.dump_types());
        } else {
            for (int i = 0; i < output_block->columns(); ++i) {
                output_block->get_by_position(i).column->assume_mutable()->insert_range_from(
                        *partial_block.get_by_position(i)
                                 .column->convert_to_full_column_if_const()
                                 .get(),
                        0, partial_block.rows());
            }
        }
        return Status::OK();
    };

    for (const auto& resp : rsps) {
        RETURN_IF_ERROR(merge_function(resp));
    }
    return Status::OK();
}

Status RowIDFetcher::fetch(const vectorized::ColumnPtr& column_row_ids,
                           vectorized::Block* res_block) {
    CHECK(!_stubs.empty());
    PMultiGetRequest mget_req = _init_fetch_request(assert_cast<const vectorized::ColumnString&>(
            *vectorized::remove_nullable(column_row_ids).get()));
    std::vector<PMultiGetResponse> resps(_stubs.size());
    std::vector<brpc::Controller> cntls(_stubs.size());
    bthread::CountdownEvent counter(_stubs.size());
    for (size_t i = 0; i < _stubs.size(); ++i) {
        cntls[i].set_timeout_ms(config::fetch_rpc_timeout_seconds * 1000);
        auto callback = brpc::NewCallback(fetch_callback, &counter);
        _stubs[i]->multiget_data(&cntls[i], &mget_req, &resps[i], callback);
    }
    counter.wait();

    // Merge
    std::vector<PRowLocation> rows_locs;
    rows_locs.reserve(rows_locs.size());
    RETURN_IF_ERROR(_merge_rpc_results(mget_req, resps, cntls, res_block, &rows_locs));

    // Final sort by row_ids sequence, since row_ids is already sorted if need
    std::map<GlobalRowLoacation, size_t> positions;
    for (size_t i = 0; i < rows_locs.size(); ++i) {
        RowsetId rowset_id;
        rowset_id.init(rows_locs[i].rowset_id());
        GlobalRowLoacation grl(rows_locs[i].tablet_id(), rowset_id, rows_locs[i].segment_id(),
                               rows_locs[i].ordinal_id());
        positions[grl] = i;
    };
    vectorized::IColumn::Permutation permutation;
    permutation.reserve(column_row_ids->size());
    for (size_t i = 0; i < column_row_ids->size(); ++i) {
        auto location =
                reinterpret_cast<const GlobalRowLoacation*>(column_row_ids->get_data_at(i).data);
        permutation.push_back(positions[*location]);
    }
    size_t num_rows = res_block->rows();
    for (size_t i = 0; i < res_block->columns(); ++i) {
        res_block->get_by_position(i).column =
                res_block->get_by_position(i).column->permute(permutation, num_rows);
    }
    // shrink for char type
    std::vector<size_t> char_type_idx;
    for (size_t i = 0; i < _fetch_option.desc->slots().size(); i++) {
        const auto& column_desc = _fetch_option.desc->slots()[i];
        const TypeDescriptor* type_desc = &column_desc->type();
        do {
            if (type_desc->type == TYPE_CHAR) {
                char_type_idx.emplace_back(i);
                break;
            } else if (type_desc->type != TYPE_ARRAY) {
                break;
            }
            // for Array<Char> or Array<Array<Char>>
            type_desc = &type_desc->children[0];
        } while (true);
    }
    res_block->shrink_char_type_column_suffix_zero(char_type_idx);
    VLOG_DEBUG << "dump block:" << res_block->dump_data(0, 10);
    return Status::OK();
}

} // namespace doris
