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

#include <gen_cpp/cloud.pb.h>

#include "meta-store/txn_kv.h"
#include "meta-store/versionstamp.h"

namespace doris::cloud {

class InstanceRecycler;
class StorageVaultAccessor;

// A abstract class for managing cluster snapshots.
class SnapshotManager {
public:
    SnapshotManager(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {}
    virtual ~SnapshotManager() = default;

    virtual void begin_snapshot(std::string_view instance_id, const BeginSnapshotRequest& request,
                                BeginSnapshotResponse* response);
    virtual void commit_snapshot(std::string_view instance_id, const CommitSnapshotRequest& request,
                                 CommitSnapshotResponse* response);
    virtual void abort_snapshot(std::string_view instance_id, const AbortSnapshotRequest& request,
                                AbortSnapshotResponse* response);
    virtual void drop_snapshot(std::string_view instance_id, const DropSnapshotRequest& request,
                               DropSnapshotResponse* response);
    virtual void list_snapshot(std::string_view instance_id, const ListSnapshotRequest& request,
                               ListSnapshotResponse* response);
    virtual void clone_instance(const CloneInstanceRequest& request,
                                CloneInstanceResponse* response);

    // Recycle snapshots that are expired or marked as recycled, based on the retention policy.
    // Return 0 for success otherwise error.
    virtual int recycle_snapshots(InstanceRecycler* recycler);

    // Recycle snapshot meta and data, return 0 for success otherwise error.
    virtual int recycle_snapshot_meta_and_data(std::string_view instance_id,
                                               std::string_view resource_id,
                                               StorageVaultAccessor* accessor,
                                               Versionstamp snapshot_version,
                                               const SnapshotPB& snapshot_pb);

private:
    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;

    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace doris::cloud
