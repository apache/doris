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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "meta-service/meta_service_helper.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta_service.h"
#include "resource-manager/resource_manager.h"

namespace doris::cloud {
namespace {

using TableStreamPartitionVersionMap =
        std::unordered_map<int64_t, std::unordered_map<int64_t, VersionPB>>;

bool is_valid_table_stream_identity(const TableStreamIdentityPB& identity) {
    return identity.has_base_db_id() && identity.base_db_id() > 0 && identity.has_base_table_id() &&
           identity.base_table_id() > 0 && identity.has_stream_db_id() &&
           identity.stream_db_id() > 0 && identity.has_stream_id() && identity.stream_id() > 0;
}

bool matches_table_stream_identity(const IndexIndexPB& index,
                                   const TableStreamIdentityPB& identity) {
    return index.object_type() == IndexObjectTypePB::TABLE_STREAM && index.has_db_id() &&
           index.db_id() == identity.base_db_id() && index.has_table_id() &&
           index.table_id() == identity.base_table_id() && index.has_stream_db_id() &&
           index.stream_db_id() == identity.stream_db_id();
}

TxnErrorCode get_latest_table_stream_offsets(Transaction* txn, std::string_view instance_id,
                                             const std::vector<TableStreamPartitionSetPB>& bindings,
                                             TableStreamOffsetMap* offsets) {
    std::vector<std::string> keys;
    std::vector<std::pair<int64_t, int64_t>> positions;
    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        for (int64_t partition_id : binding.partition_ids()) {
            keys.push_back(table_stream_offset_key(
                    {instance_id, identity.base_db_id(), identity.base_table_id(),
                     identity.stream_db_id(), identity.stream_id(), partition_id}));
            positions.emplace_back(identity.stream_id(), partition_id);
        }
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err = txn->batch_get(&values, keys, Transaction::BatchGetOptions(true));
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }
        TableStreamOffsetPB offset;
        if (!offset.ParseFromString(*values[i])) {
            return TxnErrorCode::TXN_INVALID_DATA;
        }
        auto [stream_id, partition_id] = positions[i];
        (*offsets)[stream_id].emplace(partition_id, std::move(offset));
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode get_latest_partition_versions(Transaction* txn, std::string_view instance_id,
                                           const std::vector<TableStreamPartitionSetPB>& bindings,
                                           TableStreamPartitionVersionMap* versions) {
    std::vector<std::string> keys;
    std::vector<std::pair<int64_t, int64_t>> positions;
    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        for (int64_t partition_id : binding.partition_ids()) {
            keys.push_back(partition_version_key(
                    {instance_id, identity.base_db_id(), identity.base_table_id(), partition_id}));
            positions.emplace_back(identity.stream_id(), partition_id);
        }
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err = txn->batch_get(&values, keys, Transaction::BatchGetOptions(true));
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }
        VersionPB version;
        if (!version.ParseFromString(*values[i])) {
            return TxnErrorCode::TXN_INVALID_DATA;
        }
        auto [stream_id, partition_id] = positions[i];
        (*versions)[stream_id].emplace(partition_id, std::move(version));
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode get_recycling_partitions(Transaction* txn, std::string_view instance_id,
                                      const std::vector<int64_t>& partition_ids,
                                      std::unordered_set<int64_t>* recycling_partition_ids) {
    std::vector<std::string> keys;
    keys.reserve(partition_ids.size());
    for (int64_t partition_id : partition_ids) {
        keys.push_back(recycle_partition_key({instance_id, partition_id}));
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err = txn->batch_get(&values, keys, Transaction::BatchGetOptions(true));
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].has_value()) {
            recycling_partition_ids->insert(partition_ids[i]);
        }
    }
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode get_recycling_streams(Transaction* txn, std::string_view instance_id,
                                   const std::vector<int64_t>& stream_ids,
                                   std::unordered_set<int64_t>* recycling_stream_ids) {
    std::vector<std::string> keys;
    keys.reserve(stream_ids.size());
    for (int64_t stream_id : stream_ids) {
        keys.push_back(recycle_index_key({instance_id, stream_id}));
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err = txn->batch_get(&values, keys, Transaction::BatchGetOptions(true));
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].has_value()) {
            recycling_stream_ids->insert(stream_ids[i]);
        }
    }
    return TxnErrorCode::TXN_OK;
}

struct TableStreamReadContext {
    Transaction* txn;
    std::string_view instance_id;
    MultiVersionStatus metadata_mode;
    CloneChainReader* clone_reader;
    MetaReader* current_reader;

    bool is_versioned_read() const {
        return metadata_mode == MultiVersionStatus::MULTI_VERSION_READ_WRITE;
    }
};

struct TableStreamReadResult {
    MetaServiceCode code = MetaServiceCode::OK;
    std::string message;
};

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

TableStreamReadResult get_metadata_mode(Transaction* txn, std::string_view instance_id,
                                        MultiVersionStatus* metadata_mode) {
    std::string value;
    TxnErrorCode err = txn->get(instance_key({instance_id}), &value, true);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to read instance metadata for table stream, err={}", err)};
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                "malformed instance metadata for table stream"};
    }
    *metadata_mode = instance.multi_version_status();
    if (*metadata_mode == MultiVersionStatus::MULTI_VERSION_ENABLED) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                "table stream read is not supported in MULTI_VERSION_ENABLED mode"};
    }
    return {};
}

TableStreamReadResult validate_table_streams(Transaction* txn, std::string_view instance_id,
                                             MultiVersionStatus metadata_mode,
                                             const GetTableStreamReadStateRequest& request,
                                             CloneChainReader* clone_reader,
                                             MetaReader* current_reader) {
    std::vector<int64_t> stream_ids;
    stream_ids.reserve(request.bindings_size());
    for (const TableStreamPartitionSetPB& binding : request.bindings()) {
        stream_ids.push_back(binding.identity().stream_id());
    }

    std::unordered_set<int64_t> recycling_stream_ids;
    TxnErrorCode err = get_recycling_streams(txn, instance_id, stream_ids, &recycling_stream_ids);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to read recycle indexes for table streams, err={}", err)};
    }
    for (int64_t stream_id : stream_ids) {
        if (recycling_stream_ids.contains(stream_id)) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} is being created or recycled", stream_id)};
        }
    }

    bool is_versioned_read = metadata_mode == MultiVersionStatus::MULTI_VERSION_READ_WRITE;
    std::unordered_map<int64_t, IndexIndexPB> index_indexes;
    err = is_versioned_read
                  ? clone_reader->get_index_indexes(txn, stream_ids, &index_indexes, true)
                  : current_reader->get_index_indexes(txn, stream_ids, &index_indexes, true);
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                       ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to read table stream mappings, err={}", err)};
    }
    for (const TableStreamPartitionSetPB& binding : request.bindings()) {
        int64_t stream_id = binding.identity().stream_id();
        auto index_it = index_indexes.find(stream_id);
        if (index_it == index_indexes.end()) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} does not exist", stream_id)};
        }
        if (!matches_table_stream_identity(index_it->second, binding.identity())) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} binding does not match", stream_id)};
        }
    }
    if (!is_versioned_read) {
        return {};
    }

    std::unordered_set<int64_t> existing_stream_ids;
    err = clone_reader->get_existing_indexes(txn, stream_ids, &existing_stream_ids, true);
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                       ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to check table stream visibility, err={}", err)};
    }
    for (int64_t stream_id : stream_ids) {
        if (!existing_stream_ids.contains(stream_id)) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("table stream {} is not visible at the snapshot", stream_id)};
        }
    }
    return {};
}

std::vector<int64_t> collect_partition_ids(const std::vector<TableStreamPartitionSetPB>& bindings) {
    std::vector<int64_t> partition_ids;
    std::unordered_set<int64_t> seen_partition_ids;
    for (const TableStreamPartitionSetPB& binding : bindings) {
        for (int64_t partition_id : binding.partition_ids()) {
            if (seen_partition_ids.insert(partition_id).second) {
                partition_ids.push_back(partition_id);
            }
        }
    }
    return partition_ids;
}

TableStreamReadResult validate_partitions(const TableStreamReadContext& context,
                                          const std::vector<TableStreamPartitionSetPB>& bindings,
                                          const std::vector<int64_t>& partition_ids) {
    std::unordered_set<int64_t> recycling_partition_ids;
    TxnErrorCode err = get_recycling_partitions(context.txn, context.instance_id, partition_ids,
                                                &recycling_partition_ids);
    if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to read recycle partitions, err={}", err)};
    }
    for (int64_t partition_id : partition_ids) {
        if (recycling_partition_ids.contains(partition_id)) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("partition {} is being created or recycled", partition_id)};
        }
    }
    if (context.metadata_mode == MultiVersionStatus::MULTI_VERSION_DISABLED) {
        return {};
    }

    std::unordered_map<int64_t, PartitionIndexPB> partition_indexes;
    err = context.is_versioned_read()
                  ? context.clone_reader->get_partition_indexes(context.txn, partition_ids,
                                                                &partition_indexes, true)
                  : context.current_reader->get_partition_indexes(context.txn, partition_ids,
                                                                  &partition_indexes, true);
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                       ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to read partition mappings, err={}", err)};
    }
    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        for (int64_t partition_id : binding.partition_ids()) {
            auto index_it = partition_indexes.find(partition_id);
            if (index_it == partition_indexes.end()) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("partition {} does not exist", partition_id)};
            }
            const PartitionIndexPB& partition_index = index_it->second;
            if (!partition_index.has_db_id() || partition_index.db_id() != identity.base_db_id() ||
                !partition_index.has_table_id() ||
                partition_index.table_id() != identity.base_table_id()) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("partition {} does not belong to table {}", partition_id,
                                    identity.base_table_id())};
            }
        }
    }
    if (!context.is_versioned_read()) {
        return {};
    }

    std::unordered_set<int64_t> existing_partition_ids;
    err = context.clone_reader->get_existing_partitions(context.txn, partition_ids,
                                                        &existing_partition_ids, true);
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                       ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to check partition visibility, err={}", err)};
    }
    for (int64_t partition_id : partition_ids) {
        if (!existing_partition_ids.contains(partition_id)) {
            return {MetaServiceCode::INVALID_ARGUMENT,
                    fmt::format("partition {} is not visible at the snapshot", partition_id)};
        }
    }
    return {};
}

TableStreamReadResult get_visible_partition_versions(
        const TableStreamReadContext& context,
        const std::vector<TableStreamPartitionSetPB>& bindings,
        const std::vector<int64_t>& partition_ids, TableStreamPartitionVersionMap* versions) {
    std::unordered_map<int64_t, VersionPB> effective_versions;
    TxnErrorCode err;
    if (context.is_versioned_read()) {
        err = context.clone_reader->get_partition_versions(context.txn, partition_ids,
                                                           &effective_versions, nullptr, true);
        if (err == TxnErrorCode::TXN_OK) {
            for (const TableStreamPartitionSetPB& binding : bindings) {
                auto& stream_versions = (*versions)[binding.identity().stream_id()];
                for (int64_t partition_id : binding.partition_ids()) {
                    auto version_it = effective_versions.find(partition_id);
                    if (version_it != effective_versions.end()) {
                        stream_versions.emplace(partition_id, version_it->second);
                    }
                }
            }
        }
    } else {
        err = get_latest_partition_versions(context.txn, context.instance_id, bindings, versions);
    }
    if (err != TxnErrorCode::TXN_OK) {
        MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                       ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                       : cast_as<ErrCategory::READ>(err);
        return {code, fmt::format("failed to read visible partition versions, err={}", err)};
    }
    for (const TableStreamPartitionSetPB& binding : bindings) {
        int64_t stream_id = binding.identity().stream_id();
        auto stream_it = versions->find(stream_id);
        for (int64_t partition_id : binding.partition_ids()) {
            if (stream_it == versions->end()) {
                return {MetaServiceCode::VERSION_NOT_FOUND,
                        fmt::format("visible version or TSO for partition {} not found",
                                    partition_id)};
            }
            auto version_it = stream_it->second.find(partition_id);
            if (version_it == stream_it->second.end() || !version_it->second.has_version() ||
                !version_it->second.has_visible_tso()) {
                return {MetaServiceCode::VERSION_NOT_FOUND,
                        fmt::format("visible version or TSO for partition {} not found",
                                    partition_id)};
            }
        }
    }
    return {};
}

TableStreamReadResult get_effective_table_stream_offsets(
        const TableStreamReadContext& context,
        const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamOffsetMap* offsets) {
    TxnErrorCode err = context.is_versioned_read()
                               ? context.clone_reader->get_table_stream_offsets(
                                         context.txn, bindings, offsets, nullptr, true)
                               : get_latest_table_stream_offsets(context.txn, context.instance_id,
                                                                 bindings, offsets);
    if (err == TxnErrorCode::TXN_OK) {
        return {};
    }
    MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                   ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                   : cast_as<ErrCategory::READ>(err);
    return {code, fmt::format("failed to read table stream offsets, err={}", err)};
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

TableStreamReadResult read_bindings(const TableStreamReadContext& context,
                                    const std::vector<TableStreamPartitionSetPB>& bindings,
                                    GetTableStreamReadStateResponse* response) {
    std::vector<int64_t> partition_ids = collect_partition_ids(bindings);
    TableStreamReadResult status = validate_partitions(context, bindings, partition_ids);
    if (status.code != MetaServiceCode::OK) {
        return status;
    }

    TableStreamPartitionVersionMap versions;
    status = get_visible_partition_versions(context, bindings, partition_ids, &versions);
    if (status.code != MetaServiceCode::OK) {
        return status;
    }

    TableStreamOffsetMap offsets;
    status = get_effective_table_stream_offsets(context, bindings, &offsets);
    if (status.code != MetaServiceCode::OK) {
        return status;
    }

    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        int64_t stream_id = identity.stream_id();
        TableStreamReadBindingResultPB* result = response->add_bindings();
        result->mutable_identity()->CopyFrom(identity);
        auto stream_offset_it = offsets.find(stream_id);
        for (int64_t partition_id : binding.partition_ids()) {
            const TableStreamOffsetPB* offset = nullptr;
            if (stream_offset_it != offsets.end()) {
                auto offset_it = stream_offset_it->second.find(partition_id);
                if (offset_it != stream_offset_it->second.end()) {
                    offset = &offset_it->second;
                }
            }
            status = fill_partition_read_state(identity, partition_id,
                                               versions.at(stream_id).at(partition_id), offset,
                                               result->add_partition_states());
            if (status.code != MetaServiceCode::OK) {
                return status;
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
    if (validation.code != MetaServiceCode::OK) {
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
    MultiVersionStatus metadata_mode;
    TableStreamReadResult mode_result = get_metadata_mode(txn.get(), instance_id, &metadata_mode);
    if (mode_result.code != MetaServiceCode::OK) {
        code = mode_result.code;
        msg = std::move(mode_result.message);
        return;
    }

    CloneChainReader clone_reader(instance_id, resource_mgr_.get());
    MetaReader current_reader(instance_id);
    std::vector<TableStreamPartitionSetPB> bindings(request->bindings().begin(),
                                                    request->bindings().end());
    TableStreamReadResult stream_result = validate_table_streams(
            txn.get(), instance_id, metadata_mode, *request, &clone_reader, &current_reader);
    if (stream_result.code != MetaServiceCode::OK) {
        code = stream_result.code;
        msg = std::move(stream_result.message);
        return;
    }
    TableStreamReadContext context {txn.get(), instance_id, metadata_mode, &clone_reader,
                                    &current_reader};
    GetTableStreamReadStateResponse read_response;
    TableStreamReadResult read_result = read_bindings(context, bindings, &read_response);
    if (read_result.code != MetaServiceCode::OK) {
        code = read_result.code;
        msg = std::move(read_result.message);
        return;
    }
    response->mutable_bindings()->Swap(read_response.mutable_bindings());
}

} // namespace doris::cloud
