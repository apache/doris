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

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/storage_policy.h"
#include "olap/tablet_fwd.h"

namespace doris {
class CloudStorageEngine;
class RowsetMetaPB;
class MemTrackerLimiter;

// In cloud mode, snapshot only includes tablet metas.
class CloudSnapshotMgr {
public:
    CloudSnapshotMgr(CloudStorageEngine& engine);

    ~CloudSnapshotMgr() = default;

    /**
     * Creates a snapshot for the specified tablet.
     * Behavior:
     * - For restore, convert and upload snapshot.
     * - For backup, not implemented
     *
     * @param storage_resource. Used for set the resource id.
     * @param file_mapping. A map to store file mappings. (source files -> target files).
     * @param is_restore. Whether the snapshot is for a restore job. (default: false).
     * @param slice. For hdr data input, only used for restore job. (default: nullptr).
     * @return status.
     */
    Status make_snapshot(int64_t target_tablet_id, StorageResource& storage_resource,
                         std::unordered_map<std::string, std::string>& file_mapping,
                         bool is_restore = false, const Slice* slice = nullptr);

    /**
     * Releases a snapshot for the specified tablet.
     * Behavior:
     * - Marks the snapshot to final state.
     * - This method will only be called at the last step of completing or canceling
     *   the job, and FE will not care whether the status returned here is successful
     *   or failed. After the status is returned, the entire job will end.
     *
     * @param is_completed. True, indicates the job is completing.
     *                      False, indicates the job is canceling.
     * @return status.
     */
    Status release_snapshot(int64_t tablet_id, bool is_completed);

    /**
     * Commits a snapshot for the specified tablet, only used for restore job.
     * Behavior:
     * - Finalizes the uploaded snapshot and clear local tablet cache.
     *
     * @return status.
     */
    Status commit_snapshot(int64_t tablet_id);

    Status convert_rowsets(TabletMetaPB* out, const TabletMetaPB& in, int64_t tablet_id,
                           CloudTabletSPtr& target_tablet, StorageResource& storage_resource,
                           std::unordered_map<std::string, std::string>& file_mapping);

private:
    Status _create_rowset_meta(RowsetMetaPB* new_rowset_meta_pb, const RowsetMetaPB& source_meta,
                               int64_t target_tablet_id, CloudTabletSPtr& target_tablet,
                               StorageResource& storage_resource, TabletSchemaSPtr tablet_schema,
                               std::unordered_map<std::string, std::string>& file_mapping,
                               std::unordered_map<RowsetId, RowsetId>& rowset_id_mapping);

private:
    CloudStorageEngine& _engine;
    std::atomic<uint64_t> _snapshot_base_id {0};
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
}; // CloudSnapshotMgr

} // namespace doris
