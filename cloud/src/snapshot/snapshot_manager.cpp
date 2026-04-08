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

#include "snapshot/snapshot_manager.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/meta_reader.h"
#include "meta-store/versionstamp.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"

namespace doris::cloud {

bool SnapshotManager::parse_snapshot_versionstamp(std::string_view snapshot_id,
                                                  Versionstamp* versionstamp) {
    if (snapshot_id.size() != 20) {
        return false;
    }

    std::array<uint8_t, 10> versionstamp_data;
    for (size_t i = 0; i < 10; ++i) {
        const char* hex_chars = snapshot_id.data() + (i * 2);

        // Convert two hex digits to one byte more efficiently
        uint8_t high_nibble = 0, low_nibble = 0;

        // Parse high nibble
        if (hex_chars[0] >= '0' && hex_chars[0] <= '9') {
            high_nibble = hex_chars[0] - '0';
        } else if (hex_chars[0] >= 'a' && hex_chars[0] <= 'f') {
            high_nibble = hex_chars[0] - 'a' + 10;
        } else if (hex_chars[0] >= 'A' && hex_chars[0] <= 'F') {
            high_nibble = hex_chars[0] - 'A' + 10;
        } else {
            return false;
        }

        // Parse low nibble
        if (hex_chars[1] >= '0' && hex_chars[1] <= '9') {
            low_nibble = hex_chars[1] - '0';
        } else if (hex_chars[1] >= 'a' && hex_chars[1] <= 'f') {
            low_nibble = hex_chars[1] - 'a' + 10;
        } else if (hex_chars[1] >= 'A' && hex_chars[1] <= 'F') {
            low_nibble = hex_chars[1] - 'A' + 10;
        } else {
            return false;
        }

        versionstamp_data[i] = (high_nibble << 4) | low_nibble;
    }

    *versionstamp = Versionstamp(versionstamp_data);
    return true;
}

std::string SnapshotManager::serialize_snapshot_id(Versionstamp snapshot_versionstamp) {
    return snapshot_versionstamp.to_string();
}

void SnapshotManager::begin_snapshot(std::string_view instance_id,
                                     const BeginSnapshotRequest& request,
                                     BeginSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::update_snapshot(std::string_view instance_id,
                                      const UpdateSnapshotRequest& request,
                                      UpdateSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::commit_snapshot(std::string_view instance_id,
                                      const CommitSnapshotRequest& request,
                                      CommitSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::abort_snapshot(std::string_view instance_id,
                                     const AbortSnapshotRequest& request,
                                     AbortSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::drop_snapshot(std::string_view instance_id,
                                    const DropSnapshotRequest& request,
                                    DropSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::list_snapshot(std::string_view instance_id,
                                    const ListSnapshotRequest& request,
                                    ListSnapshotResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

void SnapshotManager::clone_instance(const CloneInstanceRequest& request,
                                     CloneInstanceResponse* response) {
    response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
    response->mutable_status()->set_msg("Not implemented");
}

std::pair<MetaServiceCode, std::string> SnapshotManager::compact_snapshot(
        std::string_view instance_id) {
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented"};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::decouple_instance(std::string_view id) {
    std::string instance_id(id);
    LOG_INFO("decouple_instance").tag("instance_id", instance_id);

    // 1. Create transaction and get current instance info
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_CREATE_ERR, "failed to create txn"};
    }

    std::string key = instance_key({instance_id});
    std::string value;
    err = txn->get(key, &value);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::CLUSTER_NOT_FOUND,
                fmt::format("instance not found, instance_id={}", instance_id)};
    } else if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_GET_ERR,
                fmt::format("failed to get instance info, instance_id={}, err={}", instance_id,
                            err)};
    }

    InstanceInfoPB instance;
    if (!instance.ParseFromString(value)) {
        return {MetaServiceCode::PROTOBUF_PARSE_ERR, "failed to parse instance info"};
    }

    // 2. Check the instance was created via clone_instance
    if (!instance.has_source_instance_id() || instance.source_instance_id().empty() ||
        !instance.has_source_snapshot_id() || instance.source_snapshot_id().empty()) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance {} was not a cloned instance (created via clone_instance)",
                            instance_id)};
    }

    // 3. Check snapshot_compact_status is SNAPSHOT_COMPACT_DONE
    if (instance.snapshot_compact_status() != SnapshotCompactStatus::SNAPSHOT_COMPACT_DONE) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance {} snapshot_compact_status is not SNAPSHOT_COMPACT_DONE, "
                            "current status={}",
                            instance_id,
                            SnapshotCompactStatus_Name(instance.snapshot_compact_status()))};
    }

    // 4. Remove the snapshot reference key in the source instance
    const std::string& source_instance_id = instance.source_instance_id();
    const std::string& source_snapshot_id = instance.source_snapshot_id();

    Versionstamp snapshot_versionstamp;
    if (!parse_snapshot_versionstamp(source_snapshot_id, &snapshot_versionstamp)) {
        return {MetaServiceCode::UNDEFINED_ERR,
                fmt::format("failed to parse source_snapshot_id={} to versionstamp",
                            source_snapshot_id)};
    }

    versioned::SnapshotReferenceKeyInfo ref_key_info {source_instance_id, snapshot_versionstamp,
                                                      instance_id};
    std::string reference_key = versioned::snapshot_reference_key(ref_key_info);
    txn->remove(reference_key);

    // 5. Clear source_snapshot_id and source_instance_id from the instance PB
    instance.clear_source_snapshot_id();
    instance.clear_source_instance_id();

    // 6. Persist the updated instance
    std::string updated_val;
    if (!instance.SerializeToString(&updated_val)) {
        return {MetaServiceCode::PROTOBUF_SERIALIZE_ERR,
                fmt::format("failed to serialize updated instance, instance_id={}", instance_id)};
    }

    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(key, updated_val);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_COMMIT_ERR,
                fmt::format("failed to commit txn, instance_id={}, err={}", instance_id, err)};
    }

    LOG_INFO("decouple_instance completed successfully")
            .tag("instance_id", instance_id)
            .tag("source_instance_id", source_instance_id)
            .tag("source_snapshot_id", source_snapshot_id);

    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::set_multi_version_status(
        std::string_view instance_id, MultiVersionStatus multi_version_status) {
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented"};
}

int SnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    return 0;
}

int SnapshotManager::check_snapshots(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int SnapshotManager::check_meta(MetaChecker* meta_checker) {
    return 0;
}

int SnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                    std::string_view resource_id,
                                                    StorageVaultAccessor* accessor,
                                                    Versionstamp snapshot_version,
                                                    const SnapshotPB& snapshot_pb) {
    return 0;
}

int SnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    LOG(WARNING) << "Migrate to versioned keys is not implemented";
    return -1;
}

int SnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    LOG(WARNING) << "Compact snapshot chains is not implemented";
    return -1;
}

static std::pair<MetaServiceCode, std::string> get_instance(Transaction* txn,
                                                            std::string_view instance_id,
                                                            InstanceInfoPB* instance_info) {
    InstanceKeyInfo instance_key_info {instance_id};
    std::string key = instance_key(instance_key_info);
    std::string val;
    TxnErrorCode err = txn->get(key, &val);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("instance not found, instance_id={}", instance_id)};
    } else if (err != TxnErrorCode::TXN_OK) {
        return {cast_as<ErrCategory::READ>(err),
                fmt::format("failed to get instance, instance_id={}, err={}", instance_id, err)};
    }

    if (!instance_info->ParseFromString(val)) {
        return {MetaServiceCode::INVALID_ARGUMENT, "failed to parse instance info"};
    }
    return {MetaServiceCode::OK, ""};
}

std::pair<MetaServiceCode, std::string> SnapshotManager::get_all_snapshots(
        Transaction* txn, std::string_view instance_id, std::string_view required_snapshot_id,
        std::vector<std::pair<SnapshotPB, Versionstamp>>* snapshots) {
    Versionstamp required_snapshot_versionstamp;
    if (!required_snapshot_id.empty()) {
        if (!parse_snapshot_versionstamp(required_snapshot_id, &required_snapshot_versionstamp)) {
            return {MetaServiceCode::INVALID_ARGUMENT, "invalid snapshot_id format"};
        }
    }

    InstanceInfoPB instance_info;
    auto [code, error_msg] = get_instance(txn, instance_id, &instance_info);
    if (code != MetaServiceCode::OK) {
        return {code, error_msg};
    }
    std::string current_instance_id(instance_id);
    if (instance_info.has_original_instance_id() && !instance_info.original_instance_id().empty()) {
        // the earliest instance_id for rollback
        current_instance_id = instance_info.original_instance_id();
    }

    std::unordered_set<std::string> visited;
    do {
        MetaReader meta_reader(current_instance_id);
        if (required_snapshot_id.empty()) {
            TxnErrorCode err = meta_reader.get_snapshots(txn, snapshots);
            if (err != TxnErrorCode::TXN_OK) {
                return {cast_as<ErrCategory::READ>(err), "failed to get snapshots"};
            }
        } else {
            SnapshotPB snapshot_pb;
            TxnErrorCode err =
                    meta_reader.get_snapshot(txn, required_snapshot_versionstamp, &snapshot_pb);
            if (err == TxnErrorCode::TXN_OK) {
                snapshots->emplace_back(snapshot_pb, required_snapshot_versionstamp);
                return {MetaServiceCode::OK, ""};
            } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
                return {cast_as<ErrCategory::READ>(err), "failed to get snapshot"};
            }
        }
        if (current_instance_id == instance_id) {
            break;
        }
        auto [code, error_msg] = get_instance(txn, current_instance_id, &instance_info);
        if (code != MetaServiceCode::OK) {
            std::string msg = fmt::format("failed to get ancestor instance {}: {}",
                                          current_instance_id, error_msg);
            LOG_WARNING(msg);
            return {code, msg};
        }
        if (!instance_info.has_successor_instance_id() ||
            instance_info.successor_instance_id().empty()) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = fmt::format(
                    "successor_instance_id is empty for current instance_id={}, instance_id={}",
                    current_instance_id, instance_id);
            LOG_WARNING(error_msg);
            return {code, error_msg};
        }
        if (visited.count(current_instance_id) > 0) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            error_msg = fmt::format(
                    "cycle detected in instance chain, current instance_id={}, instance_id={}",
                    current_instance_id, instance_id);
            LOG_WARNING(error_msg);
            return {code, error_msg};
        }
        visited.insert(current_instance_id);
        current_instance_id = instance_info.successor_instance_id();
    } while (true);
    return {MetaServiceCode::OK, ""};
}

} // namespace doris::cloud
