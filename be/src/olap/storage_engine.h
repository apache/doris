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

#ifndef DORIS_BE_SRC_OLAP_STORAGE_ENGINE_H
#define DORIS_BE_SRC_OLAP_STORAGE_ENGINE_H

#include <pthread.h>
#include <rapidjson/document.h>

#include <condition_variable>
#include <ctime>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gutil/ref_counted.h"
#include "olap/compaction_permit_limiter.h"
#include "olap/fs/fs_util.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/options.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_sync_service.h"
#include "olap/task/engine_task.h"
#include "olap/txn_manager.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace doris {

class DataDir;
class EngineTask;
class BlockManager;
class MemTableFlushExecutor;
class Tablet;
class TaskWorkerPool;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class StorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    ~StorageEngine();

    static Status open(const EngineOptions& options, StorageEngine** engine_ptr);

    static StorageEngine* instance() { return _s_instance; }

    OLAPStatus create_tablet(const TCreateTabletReq& request);

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId>& partition_ids);

    // Note: 这里只能reload原先已经存在的root path，即re-load启动时就登记的root path
    // 是允许的，但re-load全新的path是不允许的，因为此处没有彻底更新ce调度器信息
    void load_data_dirs(const std::vector<DataDir*>& stores);

    Cache* index_stream_lru_cache() { return _index_stream_lru_cache; }

    std::shared_ptr<Cache> file_cache() { return _file_cache; }

    template <bool include_unused = false>
    std::vector<DataDir*> get_stores();

    // @brief 设置root_path是否可用
    void set_store_used_flag(const std::string& root_path, bool is_used);

    // @brief 获取所有root_path信息
    OLAPStatus get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    // get root path for creating tablet. The returned vector of root path should be random,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(TStorageMedium::type storage_medium);
    DataDir* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() { return _available_storage_medium_type_count; }

    Status set_cluster_id(int32_t cluster_id);
    int32_t effective_cluster_id() const { return _effective_cluster_id; }

    void start_delete_unused_rowset();
    void add_unused_rowset(RowsetSharedPtr rowset);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    OLAPStatus obtain_shard_path(TStorageMedium::type storage_medium, std::string* shared_path,
                                 DataDir** store);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return OLAP_SUCCESS if load tablet success
    OLAPStatus load_header(const std::string& shard_path, const TCloneReq& request,
                           bool restore = false);

    void register_report_listener(TaskWorkerPool* listener);
    void deregister_report_listener(TaskWorkerPool* listener);
    void notify_listeners();

    OLAPStatus execute_task(EngineTask* task);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }
    TxnManager* txn_manager() { return _txn_manager.get(); }
    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }

    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);

    // TODO(ygl)
    TabletSyncService* tablet_sync_service() { return nullptr; }

    RowsetId next_rowset_id() { return _rowset_id_generator->next_id(); };

    bool rowset_id_in_use(const RowsetId& rowset_id) {
        return _rowset_id_generator->id_in_use(rowset_id);
    }

    void release_rowset_id(const RowsetId& rowset_id) {
        return _rowset_id_generator->release_id(rowset_id);
    }

    RowsetTypePB default_rowset_type() const {
        if (_heartbeat_flags != nullptr && _heartbeat_flags->is_set_default_rowset_type_to_beta()) {
            return BETA_ROWSET;
        }
        return _default_rowset_type;
    }

    void set_heartbeat_flags(HeartbeatFlags* heartbeat_flags) {
        _heartbeat_flags = heartbeat_flags;
    }

    // start all background threads. This should be call after env is ready.
    Status start_bg_threads();

    void stop();

    void create_cumulative_compaction(TabletSharedPtr best_tablet,
                                      std::shared_ptr<CumulativeCompaction>& cumulative_compaction);
    void create_base_compaction(TabletSharedPtr best_tablet,
                                std::shared_ptr<BaseCompaction>& base_compaction);

    std::shared_ptr<StreamLoadRecorder> get_stream_load_recorder() { return _stream_load_recorder; }

    Status get_compaction_status_json(std::string* result);

    std::shared_ptr<MemTracker> tablet_mem_tracker() { return _tablet_mem_tracker; }
    std::shared_ptr<MemTracker> schema_change_mem_tracker() { return _schema_change_mem_tracker; }

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open();

    // Clear status(tables, ...)
    void _clear();

    Status _init_store_map();

    void _update_storage_medium_type_count();

    // Some check methods
    Status _check_file_descriptor_number();
    Status _check_all_root_path_cluster_id();
    Status _judge_and_update_effective_cluster_id(int32_t cluster_id);

    bool _delete_tablets_on_unused_root_path();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    OLAPStatus _do_sweep(const std::string& scan_root, const time_t& local_tm_now,
                         const int32_t expire);

    // All these xxx_callback() functions are for Background threads
    // unused rowset monitor thread
    void _unused_rowset_monitor_thread_callback();

    // check cumulative compaction config
    void _check_cumulative_compaction_config();

    // garbage sweep thread process function. clear snapshot and trash folder
    void _garbage_sweeper_thread_callback();

    // delete tablet with io error process function
    void _disk_stat_monitor_thread_callback();

    // clean file descriptors cache
    void _fd_cache_clean_callback();

    // path gc process function
    void _path_gc_thread_callback(DataDir* data_dir);

    void _path_scan_thread_callback(DataDir* data_dir);

    void _tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs);

    // parse the default rowset type config to RowsetTypePB
    void _parse_default_rowset_type();

    void _start_clean_fd_cache();

    // 清理trash和snapshot文件，返回清理后的磁盘使用量
    OLAPStatus _start_trash_sweep(double* usage);
    // 磁盘状态监测。监测unused_flag路劲新的对应root_path unused标识位，
    // 当检测到有unused标识时，从内存中删除对应表信息，磁盘数据不动。
    // 当磁盘状态为不可用，但未检测到unused标识时，需要从root_path上
    // 重新加载数据。
    void _start_disk_stat_monitor();

    void _compaction_tasks_producer_callback();

    std::vector<TabletSharedPtr> _generate_compaction_tasks(CompactionType compaction_type,
                                                            std::vector<DataDir*>& data_dirs,
                                                            bool check_score);

    void _push_tablet_into_submitted_compaction(TabletSharedPtr tablet,
                                                CompactionType compaction_type);
    void _pop_tablet_from_submitted_compaction(TabletSharedPtr tablet,
                                               CompactionType compaction_type);

    Status _init_stream_load_recorder(const std::string& stream_load_record_path);

private:
    struct CompactionCandidate {
        CompactionCandidate(uint32_t nicumulative_compaction_, int64_t tablet_id_, uint32_t index_)
                : nice(nicumulative_compaction_), tablet_id(tablet_id_), disk_index(index_) {}
        uint32_t nice; // 优先度
        int64_t tablet_id;
        uint32_t disk_index = -1;
    };

    // In descending order
    struct CompactionCandidateComparator {
        bool operator()(const CompactionCandidate& a, const CompactionCandidate& b) {
            return a.nice > b.nice;
        }
    };

    struct CompactionDiskStat {
        CompactionDiskStat(std::string path, uint32_t index, bool used)
                : storage_path(path),
                  disk_index(index),
                  task_running(0),
                  task_remaining(0),
                  is_used(used) {}
        const std::string storage_path;
        const uint32_t disk_index;
        uint32_t task_running;
        uint32_t task_remaining;
        bool is_used;
    };

    EngineOptions _options;
    std::mutex _store_lock;
    std::map<std::string, DataDir*> _store_map;
    uint32_t _available_storage_medium_type_count;

    int32_t _effective_cluster_id;
    bool _is_all_cluster_id_exist;

    Cache* _index_stream_lru_cache;

    // _file_cache is a lru_cache for file descriptors of files opened by doris,
    // which can be shared by others. Why we need to share cache with others?
    // Because a unique memory space is easier for management. For example,
    // we can deal with segment v1's cache and segment v2's cache at same time.
    // Note that, we must create _file_cache before sharing it with other.
    // (e.g. the storage engine's open function must be called earlier than
    // FileBlockManager created.)
    std::shared_ptr<Cache> _file_cache;

    static StorageEngine* _s_instance;

    Mutex _gc_mutex;
    // map<rowset_id(str), RowsetSharedPtr>, if we use RowsetId as the key, we need custom hash func
    std::unordered_map<std::string, RowsetSharedPtr> _unused_rowsets;

    std::shared_ptr<MemTracker> _compaction_mem_tracker;
    std::shared_ptr<MemTracker> _tablet_mem_tracker;
    std::shared_ptr<MemTracker> _schema_change_mem_tracker;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    scoped_refptr<Thread> _garbage_sweeper_thread;
    // thread to monitor disk stat
    scoped_refptr<Thread> _disk_stat_monitor_thread;
    // thread to produce both base and cumulative compaction tasks
    scoped_refptr<Thread> _compaction_tasks_producer_thread;
    scoped_refptr<Thread> _fd_cache_clean_thread;
    // threads to clean all file descriptor not actively in use
    std::vector<scoped_refptr<Thread>> _path_gc_threads;
    // threads to scan disk paths
    std::vector<scoped_refptr<Thread>> _path_scan_threads;
    // thread to produce tablet checkpoint tasks
    scoped_refptr<Thread> _tablet_checkpoint_tasks_producer_thread;

    // For tablet and disk-stat report
    std::mutex _report_mtx;
    std::set<TaskWorkerPool*> _report_listeners;

    Mutex _engine_task_mutex;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;

    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;

    // Used to control the migration from segment_v1 to segment_v2, can be deleted in futrue.
    // Type of new loaded data
    RowsetTypePB _default_rowset_type;

    HeartbeatFlags* _heartbeat_flags;

    std::unique_ptr<ThreadPool> _compaction_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_meta_checkpoint_thread_pool;

    CompactionPermitLimiter _permit_limiter;

    std::mutex _tablet_submitted_compaction_mutex;
    // a tablet can do base and cumulative compaction at same time
    std::map<DataDir*, std::unordered_set<TTabletId>> _tablet_submitted_cumu_compaction;
    std::map<DataDir*, std::unordered_set<TTabletId>> _tablet_submitted_base_compaction;

    AtomicInt32 _wakeup_producer_flag;

    std::mutex _compaction_producer_sleep_mutex;
    std::condition_variable _compaction_producer_sleep_cv;

    std::shared_ptr<StreamLoadRecorder> _stream_load_recorder;

    std::shared_ptr<CumulativeCompactionPolicy> _cumulative_compaction_policy;

    DISALLOW_COPY_AND_ASSIGN(StorageEngine);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_STORAGE_ENGINE_H
