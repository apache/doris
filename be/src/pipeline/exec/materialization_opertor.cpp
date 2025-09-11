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
#include "exec/rowid_fetcher.h"
#include "pipeline/exec/operator.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exec/scan/file_scanner.h"

namespace doris {
namespace pipeline {

void MaterializationSharedState::get_block(vectorized::Block* block) {
    for (int i = 0, j = 0, rowid_to_block_loc = rowid_locs[j]; i < origin_block.columns(); i++) {
        if (i != rowid_to_block_loc) {
            block->insert(origin_block.get_by_position(i));
        } else {
            auto response_block = response_blocks[j].to_block();
            for (int k = 0; k < response_block.columns(); k++) {
                auto& data = response_block.get_by_position(k);
                response_blocks[j].mutable_columns()[k] = data.column->clone_empty();
                block->insert(data);
            }
            if (++j < rowid_locs.size()) {
                rowid_to_block_loc = rowid_locs[j];
            }
        }
    }
    origin_block.clear();
}

Status MaterializationSharedState::merge_multi_response() {
    std::map<int64_t, std::pair<vectorized::Block, int>> _block_maps;
    for (int i = 0; i < block_order_results.size(); ++i) {
        for (auto& [backend_id, rpc_struct] : rpc_struct_map) {
            vectorized::Block partial_block;
            DCHECK(rpc_struct.response.blocks_size() > i);
            RETURN_IF_ERROR(partial_block.deserialize(rpc_struct.response.blocks(i).block()));
            if (rpc_struct.response.blocks(i).has_profile()) {
                auto response_profile =
                        RuntimeProfile::from_proto(rpc_struct.response.blocks(i).profile());
                _update_profile_info(backend_id, response_profile.get());
            }

            if (!partial_block.is_empty_column()) {
                _block_maps[backend_id] = std::make_pair(std::move(partial_block), 0);
            }
        }

        for (int j = 0; j < block_order_results[i].size(); ++j) {
            auto backend_id = block_order_results[i][j];
            if (backend_id) {
                auto& source_block_rows = _block_maps[backend_id];
                DCHECK(source_block_rows.second < source_block_rows.first.rows());
                for (int k = 0; k < response_blocks[i].columns(); ++k) {
                    response_blocks[i].get_column_by_position(k)->insert_from(
                            *source_block_rows.first.get_by_position(k).column,
                            source_block_rows.second);
                }
                source_block_rows.second++;
            } else {
                for (int k = 0; k < response_blocks[i].columns(); ++k) {
                    response_blocks[i].get_column_by_position(k)->insert_default();
                }
            }
        }
    }

    // clear request/response
    for (auto& [_, rpc_struct] : rpc_struct_map) {
        for (int i = 0; i < rpc_struct.request.request_block_descs_size(); ++i) {
            rpc_struct.request.mutable_request_block_descs(i)->clear_row_id();
            rpc_struct.request.mutable_request_block_descs(i)->clear_file_id();
        }
    }
    return Status::OK();
}

void MaterializationSharedState::_update_profile_info(int64_t backend_id,
                                                      RuntimeProfile* response_profile) {
    if (!backend_profile_info_string.contains(backend_id)) {
        backend_profile_info_string.emplace(backend_id,
                                            std::map<std::string, fmt::memory_buffer> {});
    }
    auto& info_map = backend_profile_info_string[backend_id];

    auto update_profile_info_key = [&](const std::string& info_key) {
        const auto* info_value = response_profile->get_info_string(info_key);
        if (info_value == nullptr) [[unlikely]] {
            LOG(WARNING) << "Get row id fetch rpc profile success, but no info key :" << info_key;
            return;
        }
        if (!info_map.contains(info_key)) {
            info_map.emplace(info_key, fmt::memory_buffer {});
        }
        fmt::format_to(info_map[info_key], "{}, ", *info_value);
    };

    update_profile_info_key(RowIdStorageReader::ScannersRunningTimeProfile);
    update_profile_info_key(RowIdStorageReader::InitReaderAvgTimeProfile);
    update_profile_info_key(RowIdStorageReader::GetBlockAvgTimeProfile);
    update_profile_info_key(RowIdStorageReader::FileReadLinesProfile);
    update_profile_info_key(vectorized::FileScanner::FileReadBytesProfile);
    update_profile_info_key(vectorized::FileScanner::FileReadTimeProfile);
}

Status MaterializationSharedState::create_muiltget_result(const vectorized::Columns& columns,
                                                          bool child_eos, bool gc_id_map) {
    const auto rows = columns.empty() ? 0 : columns[0]->size();
    block_order_results.resize(columns.size());

    for (int i = 0; i < columns.size(); ++i) {
        const uint8_t* null_map = nullptr;
        const vectorized::ColumnString* column_rowid = nullptr;
        auto& column = columns[i];

        if (auto column_ptr = check_and_get_column<vectorized::ColumnNullable>(*column)) {
            null_map = column_ptr->get_null_map_data().data();
            column_rowid = assert_cast<const vectorized::ColumnString*>(
                    column_ptr->get_nested_column_ptr().get());
        } else {
            column_rowid = assert_cast<const vectorized::ColumnString*>(column.get());
        }

        auto& block_order = block_order_results[i];
        block_order.resize(rows);

        for (int j = 0; j < rows; ++j) {
            if (!null_map || !null_map[j]) {
                DCHECK(column_rowid->get_data_at(j).size == sizeof(GlobalRowLoacationV2));
                GlobalRowLoacationV2 row_location =
                        *((GlobalRowLoacationV2*)column_rowid->get_data_at(j).data);
                auto rpc_struct = rpc_struct_map.find(row_location.backend_id);
                if (UNLIKELY(rpc_struct == rpc_struct_map.end())) {
                    return Status::InternalError(
                            "MaterializationSinkOperatorX failed to find rpc_struct, backend_id={}",
                            row_location.backend_id);
                }
                rpc_struct->second.request.mutable_request_block_descs(i)->add_row_id(
                        row_location.row_id);
                rpc_struct->second.request.mutable_request_block_descs(i)->add_file_id(
                        row_location.file_id);
                block_order[j] = row_location.backend_id;
            } else {
                block_order[j] = 0;
            }
        }
    }

    eos = child_eos;
    if (eos && gc_id_map) {
        for (auto& [_, rpc_struct] : rpc_struct_map) {
            rpc_struct.request.set_gc_id_map(true);
        }
    }
    need_merge_block = rows > 0;

    return Status::OK();
}

Status MaterializationSharedState::init_multi_requests(
        const TMaterializationNode& materialization_node, RuntimeState* state) {
    rpc_struct_inited = true;
    PMultiGetRequestV2 multi_get_request;
    // Initialize the base struct of PMultiGetRequestV2
    multi_get_request.set_be_exec_version(state->be_exec_version());
    multi_get_request.set_wg_id(state->get_query_ctx()->workload_group()->id());
    auto query_id = multi_get_request.mutable_query_id();
    query_id->set_hi(state->query_id().hi);
    query_id->set_lo(state->query_id().lo);
    DCHECK_EQ(materialization_node.column_descs_lists.size(),
              materialization_node.slot_locs_lists.size());

    const auto& tuple_desc =
            state->desc_tbl().get_tuple_descriptor(materialization_node.intermediate_tuple_id);
    const auto& slots = tuple_desc->slots();
    response_blocks =
            std::vector<vectorized::MutableBlock>(materialization_node.column_descs_lists.size());

    for (int i = 0; i < materialization_node.column_descs_lists.size(); ++i) {
        auto request_block_desc = multi_get_request.add_request_block_descs();
        request_block_desc->set_fetch_row_store(materialization_node.fetch_row_stores[i]);
        // Initialize the column_descs and slot_locs
        auto& column_descs = materialization_node.column_descs_lists[i];
        for (auto& column_desc_item : column_descs) {
            TabletColumn(column_desc_item).to_schema_pb(request_block_desc->add_column_descs());
        }

        auto& slot_locs = materialization_node.slot_locs_lists[i];
        tuple_desc->to_protobuf(request_block_desc->mutable_desc());

        auto& column_idxs = materialization_node.column_idxs_lists[i];
        for (auto idx : column_idxs) {
            request_block_desc->add_column_idxs(idx);
        }

        std::vector<SlotDescriptor*> slots_res;
        for (auto& slot_loc_item : slot_locs) {
            slots[slot_loc_item]->to_protobuf(request_block_desc->add_slots());
            slots_res.emplace_back(slots[slot_loc_item]);
        }
        response_blocks[i] = vectorized::MutableBlock(vectorized::Block(slots_res, 10));
    }

    // Initialize the stubs and requests for each BE
    for (const auto& node_info : materialization_node.nodes_info.nodes) {
        auto client = ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                node_info.host, node_info.async_internal_port);
        if (!client) {
            LOG(WARNING) << "Get rpc stub failed, host=" << node_info.host
                         << ", port=" << node_info.async_internal_port;
            return Status::InternalError("RowIDFetcher failed to init rpc client, host={}, port={}",
                                         node_info.host, node_info.async_internal_port);
        }
        rpc_struct_map.emplace(node_info.id,
                               FetchRpcStruct {.stub = std::move(client),
                                               .cntl = std::make_unique<brpc::Controller>(),
                                               .request = multi_get_request,
                                               .response = PMultiGetResponseV2()});
    }

    return Status::OK();
}

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

bool MaterializationOperator::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return !local_state._materialization_state.origin_block.rows() &&
           !local_state._materialization_state.eos;
}

Status MaterializationOperator::pull(RuntimeState* state, vectorized::Block* output_block,
                                     bool* eos) const {
    auto& local_state = get_local_state(state);
    output_block->clear();
    if (local_state._materialization_state.need_merge_block) {
        local_state._materialization_state.get_block(output_block);
    }
    *eos = local_state._materialization_state.eos;

    if (*eos) {
        for (const auto& [backend_id, child_info] :
             local_state._materialization_state.backend_profile_info_string) {
            auto child_profile = local_state.operator_profile()->create_child(
                    "RowIDFetcher: BackendId:" + std::to_string(backend_id));
            for (const auto& [info_key, info_value] :
                 local_state._materialization_state.backend_profile_info_string[backend_id]) {
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
    if (!local_state._materialization_state.rpc_struct_inited) {
        RETURN_IF_ERROR(local_state._materialization_state.init_multi_requests(
                _materialization_node, state));
    }

    if (in_block->rows() > 0 || eos) {
        // execute the rowid exprs
        vectorized::Columns columns;
        if (in_block->rows() != 0) {
            local_state._materialization_state.rowid_locs.resize(_rowid_exprs.size());
            for (int i = 0; i < _rowid_exprs.size(); ++i) {
                auto& rowid_expr = _rowid_exprs[i];
                RETURN_IF_ERROR(rowid_expr->execute(
                        in_block, &local_state._materialization_state.rowid_locs[i]));
                columns.emplace_back(
                        in_block->get_by_position(local_state._materialization_state.rowid_locs[i])
                                .column);
            }
            local_state._materialization_state.origin_block.swap(*in_block);
        }
        RETURN_IF_ERROR(local_state._materialization_state.create_muiltget_result(columns, eos,
                                                                                  _gc_id_map));

        auto size = local_state._materialization_state.rpc_struct_map.size();
        bthread::CountdownEvent counter(static_cast<int>(size));
        MonotonicStopWatch rpc_timer(true);
        for (auto& [backend_id, rpc_struct] : local_state._materialization_state.rpc_struct_map) {
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

        for (auto& [backend_id, rpc_struct] : local_state._materialization_state.rpc_struct_map) {
            if (rpc_struct.cntl->Failed()) {
                std::string error_text =
                        "Failed to send brpc request, error_text=" + rpc_struct.cntl->ErrorText() +
                        " Materialization Sink node id:" + std::to_string(node_id()) +
                        " target_backend_id:" + std::to_string(backend_id);
                return Status::InternalError(error_text);
            }
            rpc_struct.cntl->Reset();
        }

        if (local_state._materialization_state.need_merge_block) {
            SCOPED_TIMER(local_state._merge_response_timer);
            RETURN_IF_ERROR(local_state._materialization_state.merge_multi_response());
        }
    }

    return Status::OK();
}

} // namespace pipeline
} // namespace doris
