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

#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <condition_variable>
#include <set>
#include <string>
#include <vector>
#include <thread>

#include "common/status.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/column_data.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/data_dir.h"
#include "util/file_utils.h"
#include "util/doris_metrics.h"

namespace doris {

class SnapshotManager {

public:
    ~SnapshotManager() {}
    // @brief 创建snapshot
    // @param tablet_id [in] 原表的id
    // @param schema_hash [in] 原表的schema，与tablet_id参数合起来唯一确定一张表
    // @param snapshot_path [out] 新生成的snapshot的路径
    OLAPStatus make_snapshot(
            const TSnapshotRequest& request,
            std::string* snapshot_path);

    std::string get_schema_hash_full_path(
            const TabletSharedPtr& ref_tablet,
            const std::string& location) const;

    // @brief 释放snapshot
    // @param snapshot_path [in] 要被释放的snapshot的路径，只包含到ID
    OLAPStatus release_snapshot(const std::string& snapshot_path);
                
    static SnapshotManager* instance();

    OLAPStatus convert_rowset_ids(DataDir& data_dir, const string& clone_dir, int64_t tablet_id, 
        const int32_t& schema_hash, TabletSharedPtr tablet);

private:
    SnapshotManager()
        : _snapshot_base_id(0) {}

    OLAPStatus _calc_snapshot_id_path(
            const TabletSharedPtr& tablet,
            std::string* out_path);

    std::string _get_header_full_path(
            const TabletSharedPtr& ref_tablet,
            const std::string& schema_hash_path) const;

    OLAPStatus _link_index_and_data_files(
            const std::string& header_path,
            const TabletSharedPtr& ref_tablet,
            const std::vector<RowsetSharedPtr>& consistent_rowsets);

    OLAPStatus _create_snapshot_files(
            const TabletSharedPtr& ref_tablet,
            const TSnapshotRequest& request,
            std::string* snapshot_path, 
            int32_t snapshot_version);

    OLAPStatus _prepare_snapshot_dir(const TabletSharedPtr& ref_tablet,
           std::string* snapshot_id_path);

    OLAPStatus _append_single_delta(
            const TSnapshotRequest& request,
            DataDir* store);
    
    OLAPStatus _rename_rowset_id(const RowsetMetaPB& rs_meta_pb, const string& new_path, 
        DataDir& data_dir, TabletSchema& tablet_schema, RowsetId& next_id, RowsetMetaPB* new_rs_meta_pb);

private:
    static SnapshotManager* _s_instance;
    static std::mutex _mlock;


    // snapshot
    Mutex _snapshot_mutex;
    uint64_t _snapshot_base_id;
}; // SnapshotManager

} // doris

#endif 
