// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_SNAPSHOT_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_SNAPSHOT_H

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/olap_header.h"
#include "olap/olap_table.h"
#include "olap/utils.h"

namespace palo {

class OLAPSnapshot {
    DECLARE_SINGLETON(OLAPSnapshot)
public:

    // @brief 创建snapshot
    // @param tablet_id [in] 原表的id
    // @param schema_hash [in] 原表的schema，与tablet_id参数合起来唯一确定一张表
    // @param snapshot_path [out] 新生成的snapshot的路径
    OLAPStatus make_snapshot(
            const TSnapshotRequest& request,
            std::string* snapshot_path);

    // @brief 释放snapshot
    // @param snapshot_path [in] 要被释放的snapshot的路径，只包含到ID
    OLAPStatus release_snapshot(const std::string& snapshot_path);

    // @brief 迁移数据，从一种存储介质到另一种存储介质
    OLAPStatus storage_medium_migrate(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            TStorageMedium::type storage_medium);

private:

    OLAPStatus _calc_snapshot_id_path(
            const SmartOLAPTable& olap_table,
            std::string* out_path);

    std::string _get_schema_hash_full_path(
            const SmartOLAPTable& ref_olap_table,
            const std::string& location) const;

    std::string _get_header_full_path(
            const SmartOLAPTable& ref_olap_table,
            const std::string& schema_hash_path) const;

    void _update_header_file_info(
            const std::vector<VersionEntity>& shortest_version_entity,
            OLAPHeader* olap_header);

    OLAPStatus _link_index_and_data_files(
            const std::string& header_path,
            const SmartOLAPTable& ref_olap_table,
            const std::vector<VersionEntity>& version_entity_vec);

    OLAPStatus _copy_index_and_data_files(
            const std::string& header_path,
            const SmartOLAPTable& ref_olap_table,
            std::vector<VersionEntity>& version_entity_vec);

    OLAPStatus _create_snapshot_files(
            const SmartOLAPTable& ref_olap_table,
            const TSnapshotRequest& request,
            std::string* snapshot_path);

    OLAPStatus _append_single_delta(
            const TSnapshotRequest& request,
            const std::string& header_path);

    std::string _construct_index_file_path(
            const std::string& header_path,
            const Version& version,
            VersionHash version_hash,
            uint32_t segment) const;

    std::string _construct_data_file_path(
            const std::string& header_path,
            const Version& version,
            VersionHash version_hash,
            uint32_t segment) const;

    OLAPStatus _generate_new_header(
            const SmartOLAPTable& tablet,
            const std::string& new_header_path,
            const std::vector<VersionEntity>& version_entity_vec);

    OLAPStatus _create_hard_link(const std::string& from_path, const std::string& to_path);

    MutexLock _mutex;
    uint64_t _base_id;

    DISALLOW_COPY_AND_ASSIGN(OLAPSnapshot);
}; // class OLAPSnapshot

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_SNAPSHOT_H
