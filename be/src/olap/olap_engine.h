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

#ifndef DORIS_BE_SRC_OLAP_OLAP_ENGINE_H
#define DORIS_BE_SRC_OLAP_OLAP_ENGINE_H

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
#include "olap/olap_table.h"
#include "olap/olap_meta.h"
#include "olap/options.h"

namespace doris {

class OLAPTable;
class OlapStore;

struct RootPathInfo {
    RootPathInfo():
            capacity(1),
            available(0),
            data_used_capacity(0),
            is_used(false) { }

    std::string path;
    int64_t path_hash;
    int64_t capacity;                  // 总空间，单位字节
    int64_t available;                 // 可用空间，单位字节
    int64_t data_used_capacity;
    bool is_used;                       // 是否可用标识
    TStorageMedium::type storage_medium;  // 存储介质类型：SSD|HDD
};

// OLAPEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// OLAPEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class OLAPEngine {
public:
    OLAPEngine() { }
    OLAPEngine(const EngineOptions& options);
    ~OLAPEngine();

    static Status open(const EngineOptions& options, OLAPEngine** engine_ptr);

    static void set_instance(OLAPEngine* engine) {
        _s_instance = engine;
    }

    static OLAPEngine *get_instance() {
        return _s_instance;
    }

    // Get table pointer
    OLAPTablePtr get_table(TTabletId tablet_id, SchemaHash schema_hash, bool load_table = true);

    OLAPStatus get_tables_by_id(TTabletId tablet_id, std::list<OLAPTablePtr>* table_list);    

    bool check_tablet_id_exist(TTabletId tablet_id);

    OLAPStatus create_table(const TCreateTabletReq& request);

    // Create new table for OLAPEngine
    //
    // Return OLAPTable *  succeeded; Otherwise, return NULL if failed
    OLAPTablePtr create_table(const TCreateTabletReq& request,
                              const std::string* ref_root_path, 
                              const bool is_schema_change_table,
                              const OLAPTablePtr ref_olap_table);

    // Add a table pointer to OLAPEngine
    // If force, drop the existing table add this new one
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, if find duplication
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus add_table(TTabletId tablet_id, SchemaHash schema_hash,
                         const OLAPTablePtr& table, bool force = false);

    OLAPStatus add_transaction(TPartitionId partition_id, TTransactionId transaction_id,
                               TTabletId tablet_id, SchemaHash schema_hash,
                               const PUniqueId& load_id);

    void delete_transaction(TPartitionId partition_id, TTransactionId transaction_id,
                            TTabletId tablet_id, SchemaHash schema_hash,
                            bool delete_from_tablet = true);

    void get_transactions_by_tablet(OLAPTablePtr tablet, int64_t* partition_id,
                                    std::set<int64_t>* transaction_ids);

    bool has_transaction(TPartitionId partition_id, TTransactionId transaction_id,
                         TTabletId tablet_id, SchemaHash schema_hash);

    OLAPStatus publish_version(const TPublishVersionRequest& publish_version_req,
                         std::vector<TTabletId>* error_tablet_ids);

    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId> partition_ids);

    OLAPStatus clone_incremental_data(OLAPTablePtr tablet, OLAPHeader& clone_header,
                                     int64_t committed_version);

    OLAPStatus clone_full_data(OLAPTablePtr tablet, OLAPHeader& clone_header);

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
    OLAPStatus _drop_table_directly(TTabletId tablet_id, TSchemaHash schema_hash, bool keep_files = false);
    OLAPStatus _drop_table_directly_unlocked(TTabletId tablet_id, TSchemaHash schema_hash, bool keep_files = false);

    OLAPStatus drop_tables_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec);

    // Prevent schema change executed concurrently.
    bool try_schema_change_lock(TTabletId tablet_id);
    void release_schema_change_lock(TTabletId tablet_id);

    // 获取所有tables的名字
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_INPUT_PARAMETER_ERROR, if tables is null
    OLAPStatus report_tablet_info(TTabletInfo* tablet_info);
    OLAPStatus report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    void get_tablet_stat(TTabletStatResult& result);

    // Instance should be inited from create_instance
    // MUST NOT be called in other circumstances.
    OLAPStatus open();

    // Clear status(tables, ...)
    OLAPStatus clear();

    void start_clean_fd_cache();
    void perform_cumulative_compaction();
    void perform_base_compaction();

    // 获取cache的使用情况信息
    void get_cache_status(rapidjson::Document* document) const;

    void check_none_row_oriented_table(const std::vector<OlapStore*>& stores);
    OLAPStatus check_none_row_oriented_table_in_path(
                    OlapStore* store, TTabletId tablet_id,
                    SchemaHash schema_hash, const std::string& schema_hash_path);
    OLAPStatus _check_none_row_oriented_table_in_store(OlapStore* store);

    // Note: 这里只能reload原先已经存在的root path，即re-load启动时就登记的root path
    // 是允许的，但re-load全新的path是不允许的，因为此处没有彻底更新ce调度器信息
    void load_stores(const std::vector<OlapStore*>& stores);

    OLAPStatus load_one_tablet(OlapStore* store,
                               TTabletId tablet_id,
                               SchemaHash schema_hash,
                               const std::string& schema_hash_path,
                               bool force = false);

    Cache* index_stream_lru_cache() {
        return _index_stream_lru_cache;
    }

    // 清理trash和snapshot文件，返回清理后的磁盘使用量
    OLAPStatus start_trash_sweep(double *usage);

    template<bool include_unused = false>
    std::vector<OlapStore*> get_stores();
    Status set_cluster_id(int32_t cluster_id);

    // @brief 设置root_path是否可用
    void set_store_used_flag(const std::string& root_path, bool is_used);

    // @brief 获取所有root_path信息
    OLAPStatus get_all_root_path_info(std::vector<RootPathInfo>* root_paths_info);

    void get_all_available_root_path(std::vector<std::string>* available_paths);

    OLAPStatus register_table_into_root_path(OLAPTable* olap_table);

    // 磁盘状态监测。监测unused_flag路劲新的对应root_path unused标识位，
    // 当检测到有unused标识时，从内存中删除对应表信息，磁盘数据不动。
    // 当磁盘状态为不可用，但未检测到unused标识时，需要从root_path上
    // 重新加载数据。
    void start_disk_stat_monitor();

    // get root path for creating table. The returned vector of root path should be random, 
    // for avoiding that all the table would be deployed one disk.
    std::vector<OlapStore*> get_stores_for_create_table(
        TStorageMedium::type storage_medium);
    OlapStore* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() {
        return _available_storage_medium_type_count;
    }

    int32_t effective_cluster_id() const {
        return _effective_cluster_id;
    }

    uint32_t get_file_system_count() {
        return _store_map.size();
    }

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

    void start_delete_unused_index();

    void add_unused_index(Rowset* olap_index);

    // ######################### ALTER TABLE BEGIN #########################
    // The following interfaces are all about alter tablet operation, 
    // the main logical is that generating a new tablet with different
    // schema on base tablet.
    
    // Create rollup tablet on base tablet, after create_rollup_table,
    // both base tablet and new tablet is effective.
    //
    // @param [in] request specify base tablet, new tablet and its schema
    // @return OLAP_SUCCESS if submit success
    OLAPStatus create_rollup_table(const TAlterTabletReq& request);

    // Do schema change on tablet, OLAPEngine support
    // add column, drop column, alter column type and order,
    // after schema_change, base tablet is abandoned.
    // Note that the two tablets has same tablet_id but different schema_hash
    // 
    // @param [in] request specify base tablet, new tablet and its schema
    // @return OLAP_SUCCESS if submit success
    OLAPStatus schema_change(const TAlterTabletReq& request);

    // Show status of all alter table operation.
    // 
    // @param [in] tablet_id & schema_hash specify a tablet
    // @return alter table status
    AlterTableStatus show_alter_table_status(TTabletId tablet_id, TSchemaHash schema_hash);

    OLAPStatus compute_checksum(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        TVersion version,
        TVersionHash version_hash,
        uint32_t* checksum);

    OLAPStatus cancel_delete(const TCancelDeleteDataReq& request);

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

    OLAPStatus recover_tablet_until_specfic_version(
        const TRecoverTabletReq& recover_tablet_req);

    // before doing incremental clone,
    // need to calculate tablet's download dir and tablet's missing versions
    virtual std::string get_info_before_incremental_clone(OLAPTablePtr tablet,
        int64_t committed_version, std::vector<Version>* missing_versions);

    virtual OLAPStatus finish_clone(OLAPTablePtr tablet, const std::string& clone_dir,
                                    int64_t committed_version, bool is_incremental_clone);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    virtual OLAPStatus obtain_shard_path(
            TStorageMedium::type storage_medium,
            std::string* shared_path,
            OlapStore** store);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @return OLAP_SUCCESS if load tablet success
    virtual OLAPStatus load_header(
        const std::string& shard_path, const TCloneReq& request);
    virtual OLAPStatus load_header(
        OlapStore* store,
            const std::string& shard_path,
            TTabletId tablet_id,
            TSchemaHash schema_hash);

    OLAPStatus clear_alter_task(const TTabletId tablet_id,
                                const TSchemaHash schema_hash);
    OLAPStatus push(
        const TPushReq& request,
        std::vector<TTabletInfo>* tablet_info_vec);

    // call this if you want to trigger a disk and tablet report
    void report_notify(bool is_all) {
        is_all ? _report_cv.notify_all() : _report_cv.notify_one();
    }

    // call this to wait a report notification until timeout
    void wait_for_report_notify(int64_t timeout_sec, bool is_tablet_report) {
        std::unique_lock<std::mutex> lk(_report_mtx);
        auto cv_status = _report_cv.wait_for(lk, std::chrono::seconds(timeout_sec));
        if (cv_status == std::cv_status::no_timeout) {
            is_tablet_report ? _is_report_olap_table_already = true : 
                    _is_report_disk_state_already = true;
        }
    }

private:
    OLAPStatus check_all_root_path_cluster_id();

    bool _used_disk_not_enough(uint32_t unused_num, uint32_t total_num);

    OLAPStatus _get_path_available_capacity(
            const std::string& root_path,
            int64_t* disk_available);

    OLAPStatus _config_root_path_unused_flag_file(
            const std::string& root_path,
            std::string* unused_flag_file);

    void _delete_tables_on_unused_root_path();

    void _update_storage_medium_type_count();

    OLAPStatus _judge_and_update_effective_cluster_id(int32_t cluster_id);

    OLAPStatus _calc_snapshot_id_path(
            const OLAPTablePtr& olap_table,
            std::string* out_path);

    std::string _get_schema_hash_full_path(
            const OLAPTablePtr& ref_olap_table,
            const std::string& location) const;

    std::string _get_header_full_path(
            const OLAPTablePtr& ref_olap_table,
            const std::string& schema_hash_path) const;

    void _update_header_file_info(
            const std::vector<VersionEntity>& shortest_version_entity,
            OLAPHeader* header);

    OLAPStatus _link_index_and_data_files(
            const std::string& header_path,
            const OLAPTablePtr& ref_olap_table,
            const std::vector<VersionEntity>& version_entity_vec);

    OLAPStatus _copy_index_and_data_files(
            const std::string& header_path,
            const OLAPTablePtr& ref_olap_table,
            std::vector<VersionEntity>& version_entity_vec);

    OLAPStatus _create_snapshot_files(
            const OLAPTablePtr& ref_olap_table,
            const TSnapshotRequest& request,
            std::string* snapshot_path);

    OLAPStatus _create_incremental_snapshot_files(
           const OLAPTablePtr& ref_olap_table,
           const TSnapshotRequest& request,
           std::string* snapshot_path);

    OLAPStatus _prepare_snapshot_dir(const OLAPTablePtr& ref_olap_table,
           std::string* snapshot_id_path);

    OLAPStatus _append_single_delta(
            const TSnapshotRequest& request,
            OlapStore* store);

    std::string _construct_index_file_path(
            const std::string& tablet_path_prefix,
            const Version& version,
            VersionHash version_hash,
            int32_t rowset_id, int32_t segment) const;

    std::string _construct_data_file_path(
            const std::string& tablet_path_prefix,
            const Version& version,
            VersionHash version_hash,
            int32_t rowset_id, int32_t segment) const;

    OLAPStatus _generate_new_header(
            OlapStore* store,
            const uint64_t new_shard,
            const OLAPTablePtr& tablet,
            const std::vector<VersionEntity>& version_entity_vec, OLAPHeader* new_olap_header);

    OLAPStatus _create_hard_link(const std::string& from_path, const std::string& to_path);

    OLAPStatus _start_bg_worker();

    OLAPStatus _create_init_version(OLAPTablePtr olap_table, const TCreateTabletReq& request);

private:
    struct TableInstances {
        Mutex schema_change_lock;
        std::list<OLAPTablePtr> table_arr;
    };

    enum CompactionType {
        BASE_COMPACTION = 1,
        CUMULATIVE_COMPACTION = 2
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

    OLAPTablePtr _get_table_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash);

    // 遍历root所指定目录, 通过dirs返回此目录下所有有文件夹的名字, files返回所有文件的名字
    OLAPStatus _dir_walk(const std::string& root,
                     std::set<std::string>* dirs,
                     std::set<std::string>* files);

    // 扫描目录, 加载表
    OLAPStatus _load_store(OlapStore* store);

    OLAPStatus _create_new_table_header(const TCreateTabletReq& request,
                                             OlapStore* store,
                                             const bool is_schema_change_table,
                                             const OLAPTablePtr ref_olap_table,
                                             OLAPHeader* header);

    OLAPStatus _check_existed_or_else_create_dir(const std::string& path);

    OLAPTablePtr _find_best_tablet_to_compaction(CompactionType compaction_type);
    bool _can_do_compaction(OLAPTablePtr table);

    void _cancel_unfinished_schema_change();

    OLAPStatus _do_sweep(
            const std::string& scan_root, const time_t& local_tm_now, const uint32_t expire);

    void _build_tablet_info(OLAPTablePtr olap_table, TTabletInfo* tablet_info);
    void _build_tablet_stat();

    EngineOptions _options;
    std::mutex _store_lock;
    std::map<std::string, OlapStore*> _store_map;
    uint32_t _available_storage_medium_type_count;

    int32_t _effective_cluster_id;
    bool _is_all_cluster_id_exist;
    bool _is_drop_tables;

    // 错误磁盘所在百分比，超过设定的值，则engine需要退出运行
    uint32_t _min_percentage_of_error_disk;

    RWMutex _tablet_map_lock;
    tablet_map_t _tablet_map;
    RWMutex _transaction_tablet_map_lock;
    using TxnKey = std::pair<int64_t, int64_t>; // partition_id, transaction_id;
    std::map<TxnKey, std::map<TabletInfo, std::vector<PUniqueId>>> _transaction_tablet_map;
    size_t _global_table_id;
    Cache* _file_descriptor_lru_cache;
    Cache* _index_stream_lru_cache;
    uint32_t _max_base_compaction_task_per_disk;
    uint32_t _max_cumulative_compaction_task_per_disk;

    Mutex _fs_task_mutex;
    file_system_task_count_t _fs_base_compaction_task_num_map;
    std::vector<CompactionCandidate> _cumulative_compaction_candidate;

    // cache to save tablets' statistics, such as data size and row
    // TODO(cmy): for now, this is a naive implementation
    std::map<int64_t, TTabletStat> _tablet_stat_cache;
    // last update time of tablet stat cache
    int64_t _tablet_stat_cache_update_time_ms;

    static OLAPEngine* _s_instance;

    // snapshot
    Mutex _snapshot_mutex;
    uint64_t _snapshot_base_id;

    std::unordered_map<Rowset*, std::vector<std::string>> _gc_files;
    Mutex _gc_mutex;

    // Thread functions

    // base compaction thread process function
    void* _base_compaction_thread_callback(void* arg);

    // garbage sweep thread process function. clear snapshot and trash folder
    void* _garbage_sweeper_thread_callback(void* arg);

    // delete table with io error process function
    void* _disk_stat_monitor_thread_callback(void* arg);

    // unused index process function
    void* _unused_index_thread_callback(void* arg);

    // cumulative process function
    void* _cumulative_compaction_thread_callback(void* arg);

    // clean file descriptors cache
    void* _fd_cache_clean_callback(void* arg);

    // thread to monitor snapshot expiry
    std::thread _garbage_sweeper_thread;

    // thread to monitor disk stat
    std::thread _disk_stat_monitor_thread;

    // thread to monitor unused index
    std::thread _unused_index_thread;

    // thread to run base compaction
    std::vector<std::thread> _base_compaction_threads;

    // thread to check cumulative
    std::vector<std::thread> _cumulative_compaction_threads;

    std::thread _fd_cache_clean_thread;

    static atomic_t _s_request_number;

    // for tablet and disk report
    std::mutex _report_mtx;
    std::condition_variable _report_cv;
    std::atomic_bool _is_report_disk_state_already;
    std::atomic_bool _is_report_olap_table_already;
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_ENGINE_H
