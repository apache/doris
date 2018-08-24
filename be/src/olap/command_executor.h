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

#ifndef BDG_PALO_BE_SRC_OLAP_COMMAND_EXECUTOR_H
#define BDG_PALO_BE_SRC_OLAP_COMMAND_EXECUTOR_H

#include <map>
#include <string>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_rootpath.h"
#include "olap/olap_table.h"
#include "olap/utils.h"

namespace palo {

class CommandExecutor {
public:
    // Empty constructor and destructor.
    CommandExecutor() {}
    virtual ~CommandExecutor() {}
    // Create new tablet according to request info,
    // note that this interface is idempotent.
    //
    // @param [in] request contains tablet id info and tablet schema
    // @return error code
    virtual OLAPStatus create_table(const TCreateTabletReq& request);

    // Get specified tablet.
    //
    // @param [in] tablet_id & schema_hash specify a tablet
    // @return SmartOLAPTable point to NULL if tablet not exits
    virtual SmartOLAPTable get_table(TTabletId tablet_id, TSchemaHash schema_hash);

    // Drop specified tablet.
    //
    // @param [in] request specify tablet_id and schema_hash which
    //             uniquely identify a tablet
    // @return error code
    virtual OLAPStatus drop_table(const TDropTabletReq& request);

    // Push local data file into specified tablet,
    // note that this interface is idempotent.
    //
    // @param [in] request contains tablet id info and local data path
    // @param [out] tablet_info_vec return tablet lastest status, which
    //              include version info, row count, data size, etc
    // @return error code
    virtual OLAPStatus push(const TPushReq& request, std::vector<TTabletInfo>* tablet_info_vec);

    // Report tablet detail information including
    // version info, row count, data size, etc.
    //
    // @param [in][out] tablet_info specify tablet_id and schema_hash,
    //                  will be filled with tablet lastest status.
    // @return error code
    virtual OLAPStatus report_tablet_info(TTabletInfo* tablet_info);

    // Report all tablets detail info in current OLAPEngine.
    //
    // @param [out] tablets_info
    // @return error code
    virtual OLAPStatus report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    // ######################### ALTER TABLE BEGIN #########################
    // The following interfaces are all about alter tablet operation, 
    // the main logical is that generating a new tablet with different
    // schema on base tablet.
    
    // Create rollup tablet on base tablet, after create_rollup_table,
    // both base tablet and new tablet is effective.
    //
    // @param [in] request specify base tablet, new tablet and its schema
    // @return OLAP_SUCCESS if submit success
    virtual OLAPStatus create_rollup_table(const TAlterTabletReq& request);

    // Do schema change on tablet, OLAPEngine support
    // add column, drop column, alter column type and order,
    // after schema_change, base tablet is abandoned.
    // Note that the two tablets has same tablet_id but different schema_hash
    // 
    // @param [in] request specify base tablet, new tablet and its schema
    // @return OLAP_SUCCESS if submit success
    virtual OLAPStatus schema_change(const TAlterTabletReq& request);

    // Show status of all alter table operation.
    // 
    // @param [in] tablet_id & schema_hash specify a tablet
    // @return alter table status
    virtual AlterTableStatus show_alter_table_status(TTabletId tablet_id, TSchemaHash schema_hash);

    // ######################### ALTER TABLE END#########################

    // ######################### CLONE TABLE BEGIN #########################
    // The following interfaces are all about clone tablet operation, 
    // the main logical:
    //   1. make a snapshot of base tablet;
    //   2. obtain root path on another olapengine;
    //   3. rsync the snapshot to root path by agent;
    //   4. release the snapshot after clone process.

    // Make snapshot of base tablet.
    // 
    // @param [in] tablet_id & schema_hash specify base tablet
    // @param [out] snapshot_path return snapshot path
    // @return error code
    virtual OLAPStatus make_snapshot(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            std::string* snapshot_path);

    virtual OLAPStatus make_snapshot(
            const TSnapshotRequest& request,
            std::string* snapshot_path);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    virtual OLAPStatus obtain_shard_path(
            TStorageMedium::type storage_medium,
            std::string* shared_path);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @return OLAP_SUCCESS if load tablet success
    virtual OLAPStatus load_header(const std::string& shard_path, const TCloneReq& request);
    virtual OLAPStatus load_header(
            const std::string& shard_path,
            TTabletId tablet_id,
            TSchemaHash schema_hash);

    // Release snapshot of base tablet after clone finished.
    //
    // @param [in] snapshot_path
    // @return error code
    virtual OLAPStatus release_snapshot(const std::string& snapshot_path);

    // ######################### CLONE TABLE END #########################

    // Migrate specified tablet to specified storage media.
    //
    // @param [in] request specify tablet and destination storage media
    // @return error code
    virtual OLAPStatus storage_medium_migrate(const TStorageMediumMigrateReq& request);

    // Delete data of specified tablet according to delete conditions,
    // once delete_data command submit success, deleted data is not visible,
    // but not actually deleted util delay_delete_time run out.
    //
    // @param [in] request specify tablet and delete conditions
    // @param [out] tablet_info_vec return tablet lastest status, which
    //              include version info, row count, data size, etc
    // @return OLAP_SUCCESS if submit delete_data success
    virtual OLAPStatus delete_data(
            const TPushReq& request,
            std::vector<TTabletInfo>* tablet_info_vec);

    // Cancel delete operation before delay_delete_time run out.
    //
    // @param [in] request specify tablet and delete version
    // @return OLAP_SUCCESS if cancel success
    virtual OLAPStatus cancel_delete(const TCancelDeleteDataReq& request);

    // Start base compaction to expand base delta to version manually.
    //
    // @param [in] tablet_id & schema_hash specify tablet
    // @param [in] version specify base compaction range
    // @return OLAP_SUCCESS if start be success
    virtual OLAPStatus base_compaction(TTabletId tablet_id,
            TSchemaHash schema_hash,
            TVersion version);

    // Compute checksum of Version(0,version) to diff between 3 copies.
    //
    // @param [in] tablet_id & schema_hash specify tablet
    // @param [in] version
    // @param [out] checksum
    // @return error code
    virtual OLAPStatus compute_checksum(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            TVersion version,
            TVersionHash version_hash,
            uint32_t* checksum);

    // Reload multiple root paths split by ';'.
    //
    // @param root_paths for example: "/home/disk1/data;/home/disk2/data"
    // @return error code
    virtual OLAPStatus reload_root_path(const std::string& root_paths);

    // Get all root path state information.
    //
    // @param root_paths_stat each root path stat including total/used/available capacity
    // @return error code
    virtual OLAPStatus get_all_root_path_info(std::vector<RootPathInfo>* root_paths_info);

private:
    // Create initial base and delta version.
    OLAPStatus _create_init_version(SmartOLAPTable olap_table, const TCreateTabletReq& request);

    DISALLOW_COPY_AND_ASSIGN(CommandExecutor);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COMMAND_EXECUTOR_H
