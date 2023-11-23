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

#include <stdint.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "runtime/memory/mem_tracker.h"

namespace doris {
class RowsetMetaPB;
class TSnapshotRequest;
struct RowsetId;

class SnapshotManager {
public:
    ~SnapshotManager() {}

    /// Create a snapshot
    /// snapshot_path: out param, the dir of snapshot
    /// allow_incremental_clone: out param, true if it is an incremental clone
    Status make_snapshot(const TSnapshotRequest& request, std::string* snapshot_path,
                         bool* allow_incremental_clone);

    std::string static get_schema_hash_full_path(const TabletSharedPtr& ref_tablet,
                                                 const std::string& prefix);

    // @brief 释放snapshot
    // @param snapshot_path [in] 要被释放的snapshot的路径，只包含到ID
    Status release_snapshot(const std::string& snapshot_path);

    static SnapshotManager* instance();

    Status convert_rowset_ids(const std::string& clone_dir, int64_t tablet_id, int64_t replica_id,
                              int64_t partition_id, const int32_t& schema_hash);

private:
    SnapshotManager() : _snapshot_base_id(0) {
        _mem_tracker = std::make_shared<MemTracker>("SnapshotManager");
    }

    Status _calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s,
                                  std::string* out_path);

    std::string _get_header_full_path(const TabletSharedPtr& ref_tablet,
                                      const std::string& schema_hash_path) const;

    std::string _get_json_header_full_path(const TabletSharedPtr& ref_tablet,
                                           const std::string& schema_hash_path) const;

    Status _link_index_and_data_files(const std::string& header_path,
                                      const TabletSharedPtr& ref_tablet,
                                      const std::vector<RowsetSharedPtr>& consistent_rowsets);

    Status _create_snapshot_files(const TabletSharedPtr& ref_tablet,
                                  const TSnapshotRequest& request, std::string* snapshot_path,
                                  bool* allow_incremental_clone);

    Status _prepare_snapshot_dir(const TabletSharedPtr& ref_tablet, std::string* snapshot_id_path);

    Status _rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const std::string& new_tablet_path,
                             TabletSchemaSPtr tablet_schema, const RowsetId& next_id,
                             RowsetMetaPB* new_rs_meta_pb);

private:
    static SnapshotManager* _s_instance;
    static std::mutex _mlock;

    // snapshot
    std::mutex _snapshot_mutex;
    uint64_t _snapshot_base_id;

    std::shared_ptr<MemTracker> _mem_tracker;
}; // SnapshotManager

} // namespace doris
