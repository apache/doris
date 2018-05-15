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

#ifndef BDG_PALO_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H
#define BDG_PALO_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H

#include "gmock/gmock.h"
#include "olap/command_executor.h"

namespace palo {

class MockCommandExecutor : public CommandExecutor {
public:
    MOCK_METHOD1(create_table, OLAPStatus(const TCreateTabletReq& request));
    MOCK_METHOD2(get_table, SmartOLAPTable(TTabletId tablet_id, TSchemaHash schema_hash));
    MOCK_METHOD1(drop_table, OLAPStatus(const TDropTabletReq& request));
    MOCK_METHOD2(
            push,
            OLAPStatus(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec));
    MOCK_METHOD1(report_tablet_info, OLAPStatus(TTabletInfo* tablet_info));
    MOCK_METHOD1(
            report_all_tablets_info,
            OLAPStatus(std::map<TTabletId, TTablet>* tablets_info));
    MOCK_METHOD1(create_rollup_table, OLAPStatus(const TAlterTabletReq& request));
    MOCK_METHOD1(schema_change, OLAPStatus(const TAlterTabletReq& request));
    MOCK_METHOD2(
            show_alter_table_status,
            AlterTableStatus(TTabletId tablet_id, TSchemaHash schema_hash));
    MOCK_METHOD3(
            make_snapshot,
            OLAPStatus(
                    TTabletId tablet_id,
                    TSchemaHash schema_hash,
                    std::string* snapshot_path));
    MOCK_METHOD2(
            make_snapshot,
            OLAPStatus(
                    const TSnapshotRequest& request, std::string* snapshot_path));
    MOCK_METHOD2(obtain_shard_path, OLAPStatus(
            TStorageMedium::type storage_medium, std::string* root_path));
    MOCK_METHOD2(
            load_header,
            OLAPStatus(const std::string& root_path, const TCloneReq& request));
    MOCK_METHOD3(
            load_header,
            OLAPStatus(const std::string& root_path,
                       TTabletId tablet_id,
                       TSchemaHash schema_hash));
    MOCK_METHOD1(release_snapshot, OLAPStatus(const std::string& snapshot_path));
    MOCK_METHOD2(
            delete_data,
            OLAPStatus(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec));
    MOCK_METHOD1(cancel_delete, OLAPStatus(const TCancelDeleteDataReq& request));
    MOCK_METHOD3(
            base_compaction,
            OLAPStatus(TTabletId tablet_id, TSchemaHash schema_hash, TVersion version));
    MOCK_METHOD4(
            update_header,
            OLAPStatus(
                    TTabletId tablet_id,
                    TSchemaHash schema_hash,
                    const std::string& key,
                    const std::string& value));
    MOCK_METHOD5(
            compute_checksum,
            OLAPStatus(
                    TTabletId tablet_id,
                    TSchemaHash schema_hash,
                    TVersion version,
                    TVersionHash version_hash,
                    uint32_t* checksum));
    MOCK_METHOD1(reload_root_path, OLAPStatus(const std::string& root_paths));
    MOCK_METHOD2(check_table_exist, bool(TTabletId tablet_id, TSchemaHash schema_hash));
    MOCK_METHOD1(
            get_all_root_path_stat,
            OLAPStatus(std::vector<OLAPRootPathStat>* root_paths_stat));
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H
