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

} // namespace doris::cloud
