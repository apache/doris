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

#pragma once

#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

/// DorisSnapshotManager implements all snapshot lifecycle operations
/// on top of the SnapshotManager base class.
///
/// Key design decisions:
///   - Holds its own `txn_kv_` reference because the base class member is private.
///   - Uses `versioned_put` (FDB atomic versionstamp) to create new snapshot keys.
///   - All reads/writes go through single FDB transactions for atomicity.
class DorisSnapshotManager : public SnapshotManager {
public:
    explicit DorisSnapshotManager(std::shared_ptr<TxnKv> txn_kv);
    ~DorisSnapshotManager() override = default;

    // --- Core snapshot lifecycle RPCs ---
    void begin_snapshot(std::string_view instance_id, const BeginSnapshotRequest& request,
                        BeginSnapshotResponse* response) override;
    void update_snapshot(std::string_view instance_id, const UpdateSnapshotRequest& request,
                         UpdateSnapshotResponse* response) override;
    void commit_snapshot(std::string_view instance_id, const CommitSnapshotRequest& request,
                         CommitSnapshotResponse* response) override;
    void abort_snapshot(std::string_view instance_id, const AbortSnapshotRequest& request,
                        AbortSnapshotResponse* response) override;
    void drop_snapshot(std::string_view instance_id, const DropSnapshotRequest& request,
                       DropSnapshotResponse* response) override;
    void list_snapshot(std::string_view instance_id, const ListSnapshotRequest& request,
                       ListSnapshotResponse* response) override;
    void clone_instance(const CloneInstanceRequest& request,
                        CloneInstanceResponse* response) override;

    // --- Operational capabilities (Phase 2 stubs for now) ---
    std::pair<MetaServiceCode, std::string> set_multi_version_status(
            std::string_view instance_id, MultiVersionStatus multi_version_status) override;

    int check_snapshots(InstanceChecker* checker) override;
    int inverted_check_snapshots(InstanceChecker* checker) override;
    int check_mvcc_meta_key(InstanceChecker* checker) override;
    int inverted_check_mvcc_meta_key(InstanceChecker* checker) override;
    int check_meta(MetaChecker* meta_checker) override;

    int recycle_snapshots(InstanceRecycler* recycler) override;
    int recycle_snapshot_meta_and_data(std::string_view instance_id, std::string_view resource_id,
                                       StorageVaultAccessor* accessor,
                                       Versionstamp snapshot_version,
                                       const SnapshotPB& snapshot_pb) override;
    int migrate_to_versioned_keys(InstanceDataMigrator* migrator) override;
    int compact_snapshot_chains(InstanceChainCompactor* compactor) override;

private:
    DorisSnapshotManager(const DorisSnapshotManager&) = delete;
    DorisSnapshotManager& operator=(const DorisSnapshotManager&) = delete;

    // --- Internal helpers ---

    /// Read a SnapshotPB by instance_id and versionstamp.
    /// Returns TxnErrorCode::TXN_OK on success.
    TxnErrorCode read_snapshot_pb(Transaction* txn, std::string_view instance_id,
                                  const Versionstamp& vs, SnapshotPB* pb);

    /// Write (overwrite) a SnapshotPB for the given instance_id and versionstamp.
    void write_snapshot_pb(Transaction* txn, std::string_view instance_id, const Versionstamp& vs,
                           const SnapshotPB& pb);

    /// Read InstanceInfoPB from TxnKv.
    TxnErrorCode read_instance_info(Transaction* txn, std::string_view instance_id,
                                    InstanceInfoPB* info);

    /// Validate IPv4 or IPv6 address format.
    static bool validate_ip_address(const std::string& ip);

    /// Build the object-storage image URL prefix for a snapshot.
    static std::string build_image_url(const InstanceInfoPB& instance,
                                       const std::string& snapshot_id);

    /// Convert SnapshotPB to SnapshotInfoPB for list responses.
    static void snapshot_pb_to_info(const SnapshotPB& pb, const Versionstamp& vs,
                                    SnapshotInfoPB* info);

    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace doris::cloud
