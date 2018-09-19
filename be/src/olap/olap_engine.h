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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_ENGINE_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_ENGINE_H

#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <rapidjson/document.h>
#include <pthread.h>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_rootpath.h"
#include "olap/olap_snapshot.h"
#include "olap/olap_table.h"

namespace palo {

void* load_root_path_thread_callback(void* arg);

class OLAPTable;

// OLAPEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// OLAPEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class OLAPEngine {
    friend void* load_root_path_thread_callback(void* arg);

    DECLARE_SINGLETON(OLAPEngine)
public:
    // Get table pointer
    SmartOLAPTable get_table(TTabletId tablet_id, SchemaHash schema_hash);

    OLAPStatus get_tables_by_id(TTabletId tablet_id, std::list<SmartOLAPTable>* table_list);    

    bool check_tablet_id_exist(TTabletId tablet_id);

    // Create new table for OLAPEngine
    //
    // Return OLAPTable *  succeeded; Otherwise, return NULL if failed
    OLAPTable* create_table(const TCreateTabletReq& request,
                            const std::string* ref_root_path, 
                            const bool is_schema_change_table,
                            const SmartOLAPTable ref_olap_table);

    // Add a table pointer to OLAPEngine
    // If force, drop the existing table add this new one
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, if find duplication
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus add_table(TTabletId tablet_id, SchemaHash schema_hash,
            OLAPTable* table, bool force = false);

    // Add empty data for OLAPTable
    //
    // Return OLAP_SUCCESS, if run ok
    OLAPStatus create_init_version(
            TTabletId tablet_id, SchemaHash schema_hash,
            Version version, VersionHash version_hash);

    // Drop a table by description
    // If set keep_files == true, files will NOT be deleted when deconstruction.
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_DELETE_NOEXIST_ERROR, if table not exist
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus drop_table(
            TTabletId tablet_id, SchemaHash schema_hash, bool keep_files = false);

    // Drop table directly with check schema change info.
    OLAPStatus _drop_table_directly(
            TTabletId tablet_id, TSchemaHash schema_hash, bool keep_files = false);

    OLAPStatus drop_tables_on_error_root_path(const std::vector<TableInfo>& table_info_vec);

    // Prevent schema change executed concurrently.
    bool try_schema_change_lock(TTabletId tablet_id);
    void release_schema_change_lock(TTabletId tablet_id);

    // 获取所有tables的名字
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_INPUT_PARAMETER_ERROR, if tables is null
    OLAPStatus report_tablet_info(TTabletInfo* tablet_info);
    OLAPStatus report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    // Instance should be inited from create_instance
    // MUST NOT be called in other circumstances.
    OLAPStatus init();

    // Clear status(tables, ...)
    OLAPStatus clear();

    void start_clean_fd_cache();
    void start_base_compaction(std::string* last_base_compaction_fs, TTabletId* last_base_compaction_tablet_id);

    // 调度ce，优先级调度
    void start_cumulative_priority();

    // 获取cache的使用情况信息
    void get_cache_status(rapidjson::Document* document) const;

    // Note: 这里只能reload原先已经存在的root path，即re-load启动时就登记的root path
    // 是允许的，但re-load全新的path是不允许的，因为此处没有彻底更新ce调度器信息
    void load_root_paths(const OLAPRootPath::RootPathVec& root_paths);

    OLAPStatus load_one_tablet(TTabletId tablet_id,
                               SchemaHash schema_hash,
                               const std::string& schema_hash_path,
                               bool force = false);

    Cache* index_stream_lru_cache() {
        return _index_stream_lru_cache;
    }

    Cache* file_descriptor_lru_cache() {
        return _file_descriptor_lru_cache;
    }

    // 清理trash和snapshot文件，返回清理后的磁盘使用量
    OLAPStatus start_trash_sweep(double *usage);

    void add_tablet_to_base_compaction_queue(const TableInfo& tablet_info) {
        std::lock_guard<std::mutex> l(_base_compaction_queue_lock);
        _base_compaction_tablet_queue.push(tablet_info);
    }

    void add_tablet_to_cumulative_compaction_queue(const TableInfo& tablet_info) {
        std::lock_guard<std::mutex> l(_cumulative_compaction_queue_lock);
        _cumulative_compaction_tablet_queue.push(tablet_info);
    }
private:
    struct TableInstances {
        MutexLock schema_change_lock;
        std::list<SmartOLAPTable> table_arr;
    };

    struct CompactionCandidate {
        CompactionCandidate(uint32_t nicumulative_compaction_, int64_t tablet_id_, uint32_t index_) :
                nice(nicumulative_compaction_), tablet_id(tablet_id_), disk_index(index_) {}
        uint32_t nice; // 优先度
        int64_t tablet_id;
        uint32_t disk_index = -1;
    };

    struct CompactionCandidateComparator {
        bool operator()(const CompactionCandidate& a, const CompactionCandidate& b) {
            return a.nice > b.nice;
        }
    };

    struct CompactionDiskStat {
        CompactionDiskStat(std::string path, uint32_t index, bool used) :
                storage_path(path),
                disk_index(index),
                task_running(0),
                task_remaining(0),
                is_used(used){}
        const std::string storage_path;
        const uint32_t disk_index;
        uint32_t task_running;
        uint32_t task_remaining;
        bool is_used;
    };

    typedef std::map<int64_t, TableInstances> tablet_map_t;
    typedef std::map<std::string, uint32_t> file_system_task_count_t;

    SmartOLAPTable _get_table_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash);

    // 遍历root所指定目录, 通过dirs返回此目录下所有有文件夹的名字, files返回所有文件的名字
    OLAPStatus _dir_walk(const std::string& root,
                     std::set<std::string>* dirs,
                     std::set<std::string>* files);

    // 扫描目录, 加载表
    OLAPStatus _load_tables(const std::string& tables_root_path);

    OLAPStatus _create_new_table_header_file(const TCreateTabletReq& request,
                                             const std::string& root_path,
                                             std::string* header_path,
                                             const bool is_schema_change_table,
                                             const SmartOLAPTable ref_olap_table);

    OLAPStatus _check_existed_or_else_create_dir(const std::string& path);

    void _select_candidate();

    void _cancel_unfinished_schema_change();

    static OLAPStatus _spawn_load_root_path_thread(pthread_t* thread, const std::string& root_path);

    OLAPStatus _do_sweep(
            const std::string& scan_root, const time_t& local_tm_now, const uint32_t expire);

    RWLock _tablet_map_lock;
    tablet_map_t _tablet_map;
    size_t _global_table_id;
    Cache* _file_descriptor_lru_cache;
    Cache* _index_stream_lru_cache;
    uint32_t _max_base_compaction_task_per_disk;
    std::queue<TableInfo> _base_compaction_tablet_queue;
    std::mutex _base_compaction_queue_lock;
    uint32_t _max_cumulative_compaction_task_per_disk;
    std::queue<TableInfo> _cumulative_compaction_tablet_queue;
    std::mutex _cumulative_compaction_queue_lock;

    MutexLock _fs_task_mutex;
    file_system_task_count_t _fs_base_compaction_task_num_map;
    std::vector<CompactionCandidate> _cumulative_compaction_candidate;
    std::vector<CompactionDiskStat> _cumulative_compaction_disk_stat;
    std::map<std::string, uint32_t> _disk_id_map;

    DISALLOW_COPY_AND_ASSIGN(OLAPEngine);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_ENGINE_H
