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

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "meta-service/meta_service_helper.h"
#include "meta-service/table_stream_metadata_reader.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta_service.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {
namespace {

TableStreamReadResult validate_request(const GetTableStreamReadStateRequest& request) {
    std::unordered_set<int64_t> stream_ids;
    for (const TableStreamPartitionSetPB& binding : request.bindings()) {
        if (!binding.has_identity() || !is_valid_table_stream_identity(binding.identity()) ||
            binding.partition_ids().empty()) {
            return {MetaServiceCode::INVALID_ARGUMENT, "invalid table stream binding"};
        }
        if (!stream_ids.insert(binding.identity().stream_id()).second) {
            return {MetaServiceCode::INVALID_ARGUMENT, "duplicate table stream binding"};
        }

        std::unordered_set<int64_t> partition_ids;
        for (int64_t partition_id : binding.partition_ids()) {
            if (partition_id <= 0) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("invalid partition id {}", partition_id)};
            }
            if (!partition_ids.insert(partition_id).second) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("duplicate partition id {}", partition_id)};
            }
        }
    }
    return {};
}

TableStreamReadResult validate_table_streams(
        const TableStreamMetadataReader& reader,
        const std::vector<TableStreamPartitionSetPB>& bindings) {
    std::vector<int64_t> stream_ids;
    stream_ids.reserve(bindings.size());
    for (const TableStreamPartitionSetPB& binding : bindings) {
        stream_ids.push_back(binding.identity().stream_id());
    }

    std::unordered_set<int64_t> recycling_stream_ids;
    TableStreamReadResult result = reader.read_recycling_streams(
            stream_ids, TableStreamReadIntent::SNAPSHOT, &recycling_stream_ids);
    if (!result.ok()) {
        return result;
    }
    for (int64_t stream_id : stream_ids) {
        if (recycling_stream_ids.contains(stream_id)) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} is being created or recycled", stream_id)};
        }
    }

    std::unordered_map<int64_t, IndexIndexPB> mappings;
    result = reader.read_effective_stream_mappings(stream_ids, TableStreamReadIntent::SNAPSHOT,
                                                   &mappings);
    if (!result.ok()) {
        return result;
    }
    for (const TableStreamPartitionSetPB& binding : bindings) {
        const int64_t stream_id = binding.identity().stream_id();
        auto mapping_it = mappings.find(stream_id);
        if (mapping_it == mappings.end()) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} does not exist", stream_id)};
        }
        if (!matches_table_stream_identity(mapping_it->second, binding.identity())) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} binding does not match", stream_id)};
        }
    }
    return {};
}

TableStreamReadResult fill_partition_read_state(const TableStreamIdentityPB& identity,
                                                int64_t partition_id, const VersionPB& version,
                                                const TableStreamOffsetPB* offset,
                                                TableStreamPartitionReadStatePB* state) {
    state->set_partition_id(partition_id);
    state->set_visible_version(version.version());
    state->set_end_tso(version.visible_tso());
    if (!offset) {
        state->set_offset_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_UNKNOWN);
        return {};
    }
    if (!offset->has_partition_id() || offset->partition_id() != partition_id ||
        !offset->has_state() ||
        offset->state() == TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_UNKNOWN ||
        !offset->has_offset_tso()) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                fmt::format("invalid offset for table stream {} partition {}", identity.stream_id(),
                            partition_id)};
    }

    state->set_offset_state(offset->state());
    state->set_offset_tso(offset->offset_tso());
    if (offset->has_last_consumption_time_ms()) {
        state->set_last_consumption_time_ms(offset->last_consumption_time_ms());
    }
    return {};
}

TableStreamReadResult read_bindings(const TableStreamMetadataReader& reader,
                                    const std::vector<TableStreamPartitionSetPB>& bindings,
                                    GetTableStreamReadStateResponse* response) {
    TableStreamPartitionVersionMap versions;
    TableStreamReadResult result = reader.read_and_validate_partitions(
            bindings, TableStreamReadIntent::SNAPSHOT, &versions);
    if (!result.ok()) {
        return result;
    }

    TableStreamOffsetMap offsets;
    result = reader.read_effective_offsets(bindings, TableStreamReadIntent::SNAPSHOT, &offsets);
    if (!result.ok()) {
        return result;
    }

    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        const int64_t stream_id = identity.stream_id();
        TableStreamReadBindingResultPB* binding_result = response->add_bindings();
        binding_result->mutable_identity()->CopyFrom(identity);
        auto stream_offset_it = offsets.find(stream_id);
        for (int64_t partition_id : binding.partition_ids()) {
            const TableStreamOffsetPB* offset = nullptr;
            if (stream_offset_it != offsets.end()) {
                auto offset_it = stream_offset_it->second.find(partition_id);
                if (offset_it != stream_offset_it->second.end()) {
                    offset = &offset_it->second;
                }
            }
            result = fill_partition_read_state(identity, partition_id,
                                               versions.at(stream_id).at(partition_id), offset,
                                               binding_result->add_partition_states());
            if (!result.ok()) {
                return result;
            }
        }
    }
    return {};
}

} // namespace

void MetaServiceImpl::get_table_stream_read_state(::google::protobuf::RpcController* controller,
                                                  const GetTableStreamReadStateRequest* request,
                                                  GetTableStreamReadStateResponse* response,
                                                  ::google::protobuf::Closure* done) {
    RPC_PREPROCESS(get_table_stream_read_state, get);
    if (!request->has_cloud_unique_id() || request->cloud_unique_id().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "cloud_unique_id not set";
        return;
    }
    if (request->bindings().empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty table stream bindings";
        return;
    }
    TableStreamReadResult validation = validate_request(*request);
    if (!validation.ok()) {
        code = validation.code;
        msg = std::move(validation.message);
        return;
    }

    instance_id = get_instance_id(resource_mgr_, request->cloud_unique_id());
    if (instance_id.empty()) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = "empty instance_id";
        return;
    }
    RPC_RATE_LIMIT(get_table_stream_read_state)

    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::CREATE>(err);
        msg = "failed to create read transaction";
        return;
    }
    MultiVersionStatus multi_version_status;
    TableStreamReadResult mode_result = read_table_stream_multi_version_status(
            txn.get(), instance_id, TableStreamReadIntent::SNAPSHOT, &multi_version_status);
    if (!mode_result.ok()) {
        code = mode_result.code;
        msg = std::move(mode_result.message);
        return;
    }

    CloneChainReader clone_chain_reader(instance_id, resource_mgr_.get());
    TableStreamMetadataReader reader(txn.get(), instance_id, multi_version_status,
                                     &clone_chain_reader);
    std::vector<TableStreamPartitionSetPB> bindings(request->bindings().begin(),
                                                    request->bindings().end());
    TableStreamReadResult stream_result = validate_table_streams(reader, bindings);
    if (!stream_result.ok()) {
        code = stream_result.code;
        msg = std::move(stream_result.message);
        return;
    }

    GetTableStreamReadStateResponse read_response;
    TableStreamReadResult read_result = read_bindings(reader, bindings, &read_response);
    if (!read_result.ok()) {
        code = read_result.code;
        msg = std::move(read_result.message);
        return;
    }
    response->mutable_bindings()->Swap(read_response.mutable_bindings());
}

} // namespace doris::cloud
