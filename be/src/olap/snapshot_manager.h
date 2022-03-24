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

#ifndef DORIS_BE_SRC_OLAP_SNAPSHOT_MANAGER_H
#define DORIS_BE_SRC_OLAP_SNAPSHOT_MANAGER_H

#include <condition_variable>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "olap/data_dir.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/push_handler.h"
#include "olap/rowset/column_data.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"

namespace doris {

class SnapshotManager {
public:
    ~SnapshotManager() {}

    /// Create a snapshot
    /// snapshot_path: out param, the dir of snapshot
    /// allow_incremental_clone: out param, true if it is an incremental clone
    OLAPStatus make_snapshot(const TSnapshotRequest& request, std::string* snapshot_path,
                             bool* allow_incremental_clone);

    FilePathDesc get_schema_hash_full_path(const TabletSharedPtr& ref_tablet,
                                           const FilePathDesc& location_desc) const;

    // @brief 释放snapshot
    // @param snapshot_path [in] 要被释放的snapshot的路径，只包含到ID
    OLAPStatus release_snapshot(const std::string& snapshot_path);

    static SnapshotManager* instance();

    OLAPStatus convert_rowset_ids(const FilePathDesc& clone_dir_desc, int64_t tablet_id,
                                  const int32_t& schema_hash);

private:
    SnapshotManager() : _snapshot_base_id(0) {
        _mem_tracker = MemTracker::create_tracker(-1, "SnapshotManager", nullptr,
                                                  MemTrackerLevel::OVERVIEW);
    }

    OLAPStatus _calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s,
                                      std::string* out_path);

    std::string _get_header_full_path(const TabletSharedPtr& ref_tablet,
                                      const std::string& schema_hash_path) const;

    OLAPStatus _link_index_and_data_files(const FilePathDesc& header_path_desc,
                                          const TabletSharedPtr& ref_tablet,
                                          const std::vector<RowsetSharedPtr>& consistent_rowsets);

    OLAPStatus _create_snapshot_files(const TabletSharedPtr& ref_tablet,
                                      const TSnapshotRequest& request, std::string* snapshot_path,
                                      bool* allow_incremental_clone);

    OLAPStatus _prepare_snapshot_dir(const TabletSharedPtr& ref_tablet,
                                     std::string* snapshot_id_path);

    OLAPStatus _rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const FilePathDesc& new_path_desc,
                                 TabletSchema& tablet_schema, const RowsetId& next_id,
                                 RowsetMetaPB* new_rs_meta_pb);

    OLAPStatus _convert_beta_rowsets_to_alpha(const TabletMetaSharedPtr& new_tablet_meta,
                                              const vector<RowsetMetaSharedPtr>& rowset_metas,
                                              const FilePathDesc& dst_path_desc);

private:
    static SnapshotManager* _s_instance;
    static std::mutex _mlock;

    // snapshot
    Mutex _snapshot_mutex;
    uint64_t _snapshot_base_id;

    // TODO(zxy) used after
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
}; // SnapshotManager

} // namespace doris

#endif
