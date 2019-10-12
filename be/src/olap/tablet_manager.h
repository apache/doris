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

#ifndef DORIS_BE_SRC_OLAP_TABLET_MANAGER_H
#define DORIS_BE_SRC_OLAP_TABLET_MANAGER_H

#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <condition_variable>
#include <set>
#include <string>
#include <vector>
#include <thread>

#include <rapidjson/document.h>
#include <pthread.h>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/atomic.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/olap_meta.h"
#include "olap/options.h"

namespace doris {

class Tablet;
class DataDir;

// TabletManager provides get,add, delete tablet method for storage engine
class TabletManager {
public:
    TabletManager();

    ~TabletManager() {
        _tablet_map.clear();
    }

    void cancel_unfinished_schema_change();

    bool check_tablet_id_exist(TTabletId tablet_id);

    void clear();

    OLAPStatus create_tablet(const TCreateTabletReq& request, 
                             std::vector<DataDir*> stores);

    // Create new tablet for StorageEngine
    //
    // Return Tablet *  succeeded; Otherwise, return NULL if failed
    TabletSharedPtr create_tablet(const AlterTabletType alter_type, 
                                  const TCreateTabletReq& request,
                                  const bool is_schema_change_tablet,
                                  const TabletSharedPtr ref_tablet, 
                                  std::vector<DataDir*> stores);

    // Drop a tablet by description
    // If set keep_files == true, files will NOT be deleted when deconstruction.
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_DELETE_NOEXIST_ERROR, if tablet not exist
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus drop_tablet(
            TTabletId tablet_id, SchemaHash schema_hash, bool keep_files = false);

    OLAPStatus drop_tablets_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec);

    TabletSharedPtr find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir);

    // Get tablet pointer
    TabletSharedPtr get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                               bool include_deleted = false, std::string* err = nullptr);

    TabletSharedPtr get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                               TabletUid tablet_uid, bool include_deleted = false,
                               std::string* err = nullptr);

    // Extract tablet_id and schema_hash from given path.
    //
    // The normal path pattern is like "/data/{shard_id}/{tablet_id}/{schema_hash}/xxx.data".
    // Besides that, this also support empty tablet path, which path looks like
    // "/data/{shard_id}/{tablet_id}"
    //
    // Return true when the path matches the path pattern, and tablet_id and schema_hash is
    // saved in input params. When input path is an empty tablet directory, schema_hash will
    // be set to 0. Return false if the path don't match valid pattern.
    bool get_tablet_id_and_schema_hash_from_path(const std::string& path,
            TTabletId* tablet_id, TSchemaHash* schema_hash);

    bool get_rowset_id_from_path(const std::string& path,
            RowsetId* rowset_id);

    void get_tablet_stat(TTabletStatResult& result);

    // parse tablet header msg to generate tablet object
    OLAPStatus load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
                TSchemaHash schema_hash, const std::string& header, bool update_meta, 
                bool force = false);

    OLAPStatus load_tablet_from_dir(DataDir* data_dir,
                               TTabletId tablet_id,
                               SchemaHash schema_hash,
                               const std::string& schema_hash_path,
                               bool force = false);
    
    void release_schema_change_lock(TTabletId tablet_id);
    
    // 获取所有tables的名字
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_INPUT_PARAMETER_ERROR, if tables is null
    OLAPStatus report_tablet_info(TTabletInfo* tablet_info);

    OLAPStatus report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    OLAPStatus start_trash_sweep();
    // Prevent schema change executed concurrently.
    bool try_schema_change_lock(TTabletId tablet_id);

    void update_root_path_info(std::map<std::string, DataDirInfo>* path_map, int* tablet_counter);

    void get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos);

    void do_tablet_meta_checkpoint(DataDir* data_dir);

private:
    // Add a tablet pointer to StorageEngine
    // If force, drop the existing tablet add this new one
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, if find duplication
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus _add_tablet_unlock(TTabletId tablet_id, SchemaHash schema_hash,
                         const TabletSharedPtr& tablet, bool update_meta, bool force);
    
    OLAPStatus _add_tablet_to_map(TTabletId tablet_id, SchemaHash schema_hash,
                                 const TabletSharedPtr& tablet, bool update_meta, 
                                 bool keep_files, bool drop_old);
    
    void _build_tablet_stat();
    bool _check_tablet_id_exist_unlock(TTabletId tablet_id);
    OLAPStatus _create_inital_rowset(TabletSharedPtr tablet, const TCreateTabletReq& request);
    

    OLAPStatus _create_tablet_meta(const TCreateTabletReq& request,
                                   DataDir* store,
                                   const bool is_schema_change_tablet,
                                   const TabletSharedPtr ref_tablet,
                                   TabletMetaSharedPtr* tablet_meta);
    
    OLAPStatus _drop_tablet_directly_unlocked(TTabletId tablet_id, TSchemaHash schema_hash, bool keep_files = false);

    OLAPStatus _drop_tablet_unlock(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files);

    TabletSharedPtr _get_tablet_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash);

    TabletSharedPtr _get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                bool include_deleted, std::string* err);

    TabletSharedPtr _internal_create_tablet(const AlterTabletType alter_type, const TCreateTabletReq& request, 
        const bool is_schema_change_tablet, const TabletSharedPtr ref_tablet, std::vector<DataDir*> data_dirs);
    
    TabletSharedPtr _create_tablet_meta_and_dir(const TCreateTabletReq& request, const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet, std::vector<DataDir*> data_dirs);

private:
    struct TableInstances {
        Mutex schema_change_lock;
        std::list<TabletSharedPtr> table_arr;
    };
    typedef std::map<int64_t, TableInstances> tablet_map_t;
    RWMutex _tablet_map_lock;
    RWMutex _create_tablet_lock;
    tablet_map_t _tablet_map;
    std::map<std::string, DataDir*> _store_map;

    // cache to save tablets' statistics, such as data size and row
    // TODO(cmy): for now, this is a naive implementation
    std::map<int64_t, TTabletStat> _tablet_stat_cache;
    std::mutex _tablet_stat_mutex;

    // last update time of tablet stat cache
    int64_t _tablet_stat_cache_update_time_ms;

    uint32_t _available_storage_medium_type_count;

    std::vector<TabletSharedPtr> _shutdown_tablets;

    // map from partition id to tablet_id
    std::map<int64_t, std::set<TabletInfo>> _partition_tablet_map;

    DISALLOW_COPY_AND_ASSIGN(TabletManager);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_TABLET_MANAGER_H
