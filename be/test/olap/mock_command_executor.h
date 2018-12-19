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

#ifndef DORIS_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H
#define DORIS_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H

#include "gmock/gmock.h"
#include "olap/storage_engine.h"

namespace doris {

class MockCommandExecutor : public StorageEngine {
public:
    MOCK_METHOD1(create_tablet, OLAPStatus(const TCreateTabletReq& request));
    MOCK_METHOD2(get_tablet, TabletSharedPtr(TTabletId tablet_id, TSchemaHash schema_hash));
    MOCK_METHOD1(drop_tablet, OLAPStatus(const TDropTabletReq& request));
    MOCK_METHOD2(
            push,
            OLAPStatus(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec));
    MOCK_METHOD1(report_tablet_info, OLAPStatus(TTabletInfo* tablet_info));
    MOCK_METHOD1(
            report_all_tablets_info,
            OLAPStatus(std::map<TTabletId, TTablet>* tablets_info));
    MOCK_METHOD1(create_rollup_tablet, OLAPStatus(const TAlterTabletReq& request));
    MOCK_METHOD1(schema_change, OLAPStatus(const TAlterTabletReq& request));
    MOCK_METHOD2(
            show_alter_tablet_status,
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
    MOCK_METHOD2(check_tablet_exist, bool(TTabletId tablet_id, TSchemaHash schema_hash));
    MOCK_METHOD1(
            get_all_data_dir_info,
            OLAPStatus(std::vector<DataDirInfo>* data_dir_infos));
    MOCK_METHOD2(
            publish_version,
            OLAPStatus(const TPublishVersionRequest& request,
                       std::vector<TTabletId>* error_tablet_ids));
    MOCK_METHOD3(
            get_info_before_incremental_clone,
            std::string(
                    TabletSharedPtr tablet,
                    int64_t committed_version,
                    std::vector<Version>* missing_versions));
    MOCK_METHOD4(
            finish_clone,
            OLAPStatus(
                    TabletSharedPtr tablet,
                    const std::string& clone_dir,
                    int64_t committed_version,
                    bool is_incremental_clone));
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_MOCK_MOCK_COMMAND_EXECUTOR_H
