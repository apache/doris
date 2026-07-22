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

#include "meta-service/table_stream_metadata_reader.h"

#include <fmt/format.h>

#include <optional>
#include <utility>

#include "common/logging.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/clone_chain_reader.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {
namespace {

TableStreamReadResult read_error(TxnErrorCode err, std::string_view object) {
    const MetaServiceCode code = err == TxnErrorCode::TXN_INVALID_DATA
                                         ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                         : cast_as<ErrCategory::READ>(err);
    return {code, fmt::format("failed to read {}, err={}", object, err)};
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

} // namespace

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

TableStreamReadResult read_table_stream_multi_version_status(
        Transaction* txn, std::string_view instance_id, TableStreamReadIntent intent,
        MultiVersionStatus* multi_version_status) {
    std::string value;
    TxnErrorCode err = txn->get(instance_key({std::string(instance_id)}), &value,
                                intent == TableStreamReadIntent::SNAPSHOT);
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "instance metadata for Table Stream");
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                "malformed instance metadata for Table Stream"};
    }
    *multi_version_status = instance.multi_version_status();
    if (*multi_version_status == MultiVersionStatus::MULTI_VERSION_ENABLED) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                "Table Stream is not supported in MULTI_VERSION_ENABLED mode"};
    }
    return {};
}

TableStreamMetadataReader::TableStreamMetadataReader(Transaction* txn, std::string_view instance_id,
                                                     MultiVersionStatus multi_version_status,
                                                     CloneChainReader* clone_chain_reader)
        : txn_(txn),
          instance_id_(instance_id),
          multi_version_status_(multi_version_status),
          clone_chain_reader_(clone_chain_reader),
          current_reader_(instance_id_) {}

bool TableStreamMetadataReader::reads_from_clone_chain() const {
    return multi_version_status_ == MultiVersionStatus::MULTI_VERSION_READ_WRITE;
}

bool TableStreamMetadataReader::writes_versioned_metadata() const {
    return multi_version_status_ == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY ||
           reads_from_clone_chain();
}

bool TableStreamMetadataReader::snapshot(TableStreamReadIntent intent) const {
    return intent == TableStreamReadIntent::SNAPSHOT;
}

TableStreamReadResult TableStreamMetadataReader::read_current_stream_mappings(
        const std::vector<int64_t>& stream_ids, TableStreamReadIntent intent,
        std::unordered_map<int64_t, IndexIndexPB>* mappings) const {
    std::vector<std::string> keys;
    keys.reserve(stream_ids.size());
    for (int64_t stream_id : stream_ids) {
        keys.push_back(table_stream_index_key({instance_id_, stream_id}));
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err =
            txn_->batch_get(&values, keys, Transaction::BatchGetOptions(snapshot(intent)));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "current Table Stream mappings");
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }
        IndexIndexPB mapping;
        if (!mapping.ParseFromString(*values[i])) {
            return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                    fmt::format("malformed Table Stream mapping for stream {}", stream_ids[i])};
        }
        mappings->emplace(stream_ids[i], std::move(mapping));
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_effective_stream_mappings(
        const std::vector<int64_t>& stream_ids, TableStreamReadIntent intent,
        std::unordered_map<int64_t, IndexIndexPB>* mappings) const {
    if (!reads_from_clone_chain()) {
        return read_current_stream_mappings(stream_ids, intent, mappings);
    }

    DCHECK(clone_chain_reader_ != nullptr);
    TxnErrorCode err =
            clone_chain_reader_->get_index_indexes(txn_, stream_ids, mappings, snapshot(intent));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "effective Table Stream mappings");
    }

    std::unordered_set<int64_t> visible_stream_ids;
    err = clone_chain_reader_->get_existing_indexes(txn_, stream_ids, &visible_stream_ids,
                                                    snapshot(intent));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "Table Stream visibility");
    }
    for (int64_t stream_id : stream_ids) {
        if (!visible_stream_ids.contains(stream_id)) {
            mappings->erase(stream_id);
        }
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_recycling_streams(
        const std::vector<int64_t>& stream_ids, TableStreamReadIntent intent,
        std::unordered_set<int64_t>* recycling_stream_ids) const {
    std::vector<std::string> keys;
    keys.reserve(stream_ids.size());
    for (int64_t stream_id : stream_ids) {
        keys.push_back(recycle_index_key({instance_id_, stream_id}));
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err =
            txn_->batch_get(&values, keys, Transaction::BatchGetOptions(snapshot(intent)));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "Table Stream recycle indexes");
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].has_value()) {
            recycling_stream_ids->insert(stream_ids[i]);
        }
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_recycling_partitions(
        const std::vector<int64_t>& partition_ids, TableStreamReadIntent intent,
        std::unordered_set<int64_t>* recycling_partition_ids) const {
    std::vector<std::string> keys;
    keys.reserve(partition_ids.size());
    for (int64_t partition_id : partition_ids) {
        keys.push_back(recycle_partition_key({instance_id_, partition_id}));
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err =
            txn_->batch_get(&values, keys, Transaction::BatchGetOptions(snapshot(intent)));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "recycle partitions");
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i].has_value()) {
            recycling_partition_ids->insert(partition_ids[i]);
        }
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_and_validate_partitions(
        const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
        TableStreamPartitionVersionMap* versions) const {
    const std::vector<int64_t> partition_ids = collect_partition_ids(bindings);
    std::unordered_set<int64_t> recycling_partition_ids;
    TableStreamReadResult result =
            read_recycling_partitions(partition_ids, intent, &recycling_partition_ids);
    if (!result.ok()) {
        return result;
    }
    if (!recycling_partition_ids.empty()) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("partition {} is being created or recycled",
                            *recycling_partition_ids.begin())};
    }

    if (reads_from_clone_chain()) {
        DCHECK(clone_chain_reader_ != nullptr);
        std::unordered_map<int64_t, PartitionIndexPB> partition_mappings;
        TxnErrorCode err = clone_chain_reader_->get_partition_indexes(
                txn_, partition_ids, &partition_mappings, snapshot(intent));
        if (err != TxnErrorCode::TXN_OK) {
            return read_error(err, "partition mappings");
        }
        std::unordered_set<int64_t> visible_partition_ids;
        err = clone_chain_reader_->get_existing_partitions(
                txn_, partition_ids, &visible_partition_ids, snapshot(intent));
        if (err != TxnErrorCode::TXN_OK) {
            return read_error(err, "partition visibility");
        }
        for (const TableStreamPartitionSetPB& binding : bindings) {
            const TableStreamIdentityPB& identity = binding.identity();
            for (int64_t partition_id : binding.partition_ids()) {
                auto mapping_it = partition_mappings.find(partition_id);
                if (mapping_it == partition_mappings.end() ||
                    !visible_partition_ids.contains(partition_id)) {
                    return {MetaServiceCode::INVALID_ARGUMENT,
                            fmt::format("partition {} is not visible", partition_id)};
                }
                const PartitionIndexPB& mapping = mapping_it->second;
                if (!mapping.has_db_id() || mapping.db_id() != identity.base_db_id() ||
                    !mapping.has_table_id() || mapping.table_id() != identity.base_table_id()) {
                    return {MetaServiceCode::INVALID_ARGUMENT,
                            fmt::format("partition {} does not belong to base table {}",
                                        partition_id, identity.base_table_id())};
                }
            }
        }

        std::unordered_map<int64_t, VersionPB> effective_versions;
        err = clone_chain_reader_->get_partition_versions(txn_, partition_ids, &effective_versions,
                                                          nullptr, true);
        if (err != TxnErrorCode::TXN_OK) {
            return read_error(err, "source partition versions");
        }
        for (const TableStreamPartitionSetPB& binding : bindings) {
            auto& stream_versions = (*versions)[binding.identity().stream_id()];
            for (int64_t partition_id : binding.partition_ids()) {
                auto version_it = effective_versions.find(partition_id);
                if (version_it != effective_versions.end()) {
                    stream_versions.emplace(partition_id, version_it->second);
                }
            }
        }
    } else {
        std::vector<std::string> keys;
        std::vector<std::pair<int64_t, int64_t>> positions;
        for (const TableStreamPartitionSetPB& binding : bindings) {
            const TableStreamIdentityPB& identity = binding.identity();
            for (int64_t partition_id : binding.partition_ids()) {
                keys.push_back(partition_version_key({instance_id_, identity.base_db_id(),
                                                      identity.base_table_id(), partition_id}));
                positions.emplace_back(identity.stream_id(), partition_id);
            }
        }
        std::vector<std::optional<std::string>> values;
        TxnErrorCode err = txn_->batch_get(&values, keys, Transaction::BatchGetOptions(true));
        if (err != TxnErrorCode::TXN_OK) {
            return read_error(err, "source partition versions");
        }
        for (size_t i = 0; i < values.size(); ++i) {
            if (!values[i].has_value()) {
                continue;
            }
            VersionPB version;
            if (!version.ParseFromString(*values[i])) {
                return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                        fmt::format("malformed source version for partition {}",
                                    positions[i].second)};
            }
            (*versions)[positions[i].first].emplace(positions[i].second, std::move(version));
        }
    }

    for (const TableStreamPartitionSetPB& binding : bindings) {
        auto stream_it = versions->find(binding.identity().stream_id());
        for (int64_t partition_id : binding.partition_ids()) {
            if (stream_it == versions->end()) {
                return {MetaServiceCode::VERSION_NOT_FOUND,
                        fmt::format("source version is missing for partition {}", partition_id)};
            }
            auto version_it = stream_it->second.find(partition_id);
            if (version_it == stream_it->second.end() || !version_it->second.has_version() ||
                !version_it->second.has_visible_tso()) {
                return {MetaServiceCode::VERSION_NOT_FOUND,
                        fmt::format("source version or visible TSO is missing for partition {}",
                                    partition_id)};
            }
        }
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_latest_offsets(
        const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
        TableStreamOffsetMap* offsets) const {
    std::vector<std::string> keys;
    std::vector<std::pair<int64_t, int64_t>> positions;
    for (const TableStreamPartitionSetPB& binding : bindings) {
        const TableStreamIdentityPB& identity = binding.identity();
        for (int64_t partition_id : binding.partition_ids()) {
            keys.push_back(table_stream_offset_key(
                    {instance_id_, identity.base_db_id(), identity.base_table_id(),
                     identity.stream_db_id(), identity.stream_id(), partition_id}));
            positions.emplace_back(identity.stream_id(), partition_id);
        }
    }

    std::vector<std::optional<std::string>> values;
    TxnErrorCode err =
            txn_->batch_get(&values, keys, Transaction::BatchGetOptions(snapshot(intent)));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "latest Table Stream offsets");
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i].has_value()) {
            continue;
        }
        TableStreamOffsetPB offset;
        if (!offset.ParseFromString(*values[i])) {
            return {MetaServiceCode::PROTOBUF_PARSE_ERR,
                    fmt::format("malformed Table Stream offset for partition {}",
                                positions[i].second)};
        }
        (*offsets)[positions[i].first].emplace(positions[i].second, std::move(offset));
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_effective_offsets(
        const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
        TableStreamOffsetMap* offsets) const {
    if (!reads_from_clone_chain()) {
        return read_latest_offsets(bindings, intent, offsets);
    }

    TableStreamOffsetMap local_offsets;
    if (intent == TableStreamReadIntent::CONFLICT) {
        TableStreamReadResult result = read_latest_offsets(bindings, intent, &local_offsets);
        if (!result.ok()) {
            return result;
        }
    }

    DCHECK(clone_chain_reader_ != nullptr);
    TxnErrorCode err = clone_chain_reader_->get_table_stream_offsets(txn_, bindings, offsets,
                                                                     nullptr, snapshot(intent));
    if (err != TxnErrorCode::TXN_OK) {
        return read_error(err, "effective Table Stream offsets");
    }

    for (const auto& [stream_id, partition_offsets] : local_offsets) {
        auto effective_stream_it = offsets->find(stream_id);
        for (const auto& [partition_id, local_offset] : partition_offsets) {
            if (effective_stream_it == offsets->end()) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("local Table Stream offset exists without an effective offset "
                                    "for stream {} partition {}",
                                    stream_id, partition_id)};
            }
            auto effective_offset_it = effective_stream_it->second.find(partition_id);
            if (effective_offset_it == effective_stream_it->second.end()) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("local Table Stream offset exists without an effective offset "
                                    "for stream {} partition {}",
                                    stream_id, partition_id)};
            }
            if (local_offset.SerializeAsString() !=
                effective_offset_it->second.SerializeAsString()) {
                return {MetaServiceCode::INVALID_ARGUMENT,
                        fmt::format("local and effective Table Stream offsets differ for stream {} "
                                    "partition {}",
                                    stream_id, partition_id)};
            }
        }
    }
    return {};
}

TableStreamReadResult TableStreamMetadataReader::read_local_versioned_offsets(
        const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
        TableStreamOffsetMap* offsets) const {
    TxnErrorCode err = current_reader_.get_table_stream_offsets(txn_, bindings, offsets, nullptr,
                                                                snapshot(intent));
    return err == TxnErrorCode::TXN_OK ? TableStreamReadResult {}
                                       : read_error(err, "local versioned Table Stream offsets");
}

} // namespace doris::cloud
