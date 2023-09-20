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

#pragma once

#include <butil/macros.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "agent/task_worker_pool.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/compaction_permit_limiter.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/special_dir.h"
#include "olap/tablet.h"
#include "olap/task/index_builder.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "util/countdown_latch.h"

namespace doris {

class DataDir;
class SpecialDir;
class EngineTask;
class MemTableFlushExecutor;
class SegcompactionWorker;
class BaseCompaction;
class CumulativeCompaction;
class SingleReplicaCompaction;
class CumulativeCompactionPolicy;
class MemTracker;
class StreamLoadRecorder;
class TCloneReq;
class TCreateTabletReq;
class TabletManager;
class Thread;
class ThreadPool;
class TxnManager;

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class StorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    ~StorageEngine();

    [[nodiscard]] Status open();

    static StorageEngine* instance() { return ExecEnv::GetInstance()->get_storage_engine(); }

    Status create_tablet(const TCreateTabletReq& request, RuntimeProfile* profile);

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId>& partition_ids);

    // Note: Only the previously existing root path can be reloaded here, that is, the root path registered when re load starts is allowed,
    // but the brand new path of re load is not allowed because the ce scheduler information has not been thoroughly updated here
    Status load_data_dirs(const std::vector<DataDir*>& stores);

    template <bool include_unused = false>
    std::vector<DataDir*> get_stores();

    // get all info of root_path
    Status get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);
    Status get_special_dir_info(SpecialDirInfo* dir_infos, TaskWorkerPool::DiskType type);

    int64_t get_file_or_directory_size(const std::string& file_path);

    // get root path for creating tablet. The returned vector of root path should be random,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(TStorageMedium::type storage_medium);
    DataDir* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() const {
        return _available_storage_medium_type_count;
    }

    Status set_cluster_id(int32_t cluster_id);
    int32_t effective_cluster_id() const { return _effective_cluster_id; }

    void start_delete_unused_rowset();
    void add_unused_rowset(RowsetSharedPtr rowset);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    Status obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash,
                             std::string* shared_path, DataDir** store);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return OK if load tablet success
    Status load_header(const std::string& shard_path, const TCloneReq& request,
                       bool restore = false);

    void register_report_listener(TaskWorkerPool* listener);
    void deregister_report_listener(TaskWorkerPool* listener);
    void notify_listeners();
    void notify_listener(TaskWorkerPool::TaskWorkerType task_worker_type);

    Status execute_task(EngineTask* task);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }
    TxnManager* txn_manager() { return _txn_manager.get(); }
    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }
    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() {
        return _calc_delete_bitmap_executor.get();
    }

    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);

    RowsetId next_rowset_id() { return _rowset_id_generator->next_id(); }

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

    // clear trash and snapshot file
    // option: update disk usage after sweep
    Status start_trash_sweep(double* usage, bool ignore_guard = false);

    // Must call stop() before storage_engine is deconstructed
    void stop();

    void get_tablet_rowset_versions(const PGetTabletVersionsRequest* request,
                                    PGetTabletVersionsResponse* response);

    void create_cumulative_compaction(TabletSharedPtr best_tablet,
                                      std::shared_ptr<CumulativeCompaction>& cumulative_compaction);
    void create_base_compaction(TabletSharedPtr best_tablet,
                                std::shared_ptr<BaseCompaction>& base_compaction);

    void create_full_compaction(TabletSharedPtr best_tablet,
                                std::shared_ptr<FullCompaction>& full_compaction);

    void create_single_replica_compaction(
            TabletSharedPtr best_tablet,
            std::shared_ptr<SingleReplicaCompaction>& single_replica_compaction,
            CompactionType compaction_type);
    bool get_peer_replica_info(int64_t tablet_id, TReplicaInfo* replica, std::string* token);

    bool should_fetch_from_peer(int64_t tablet_id);

    std::shared_ptr<StreamLoadRecorder> get_stream_load_recorder() { return _stream_load_recorder; }

    Status get_compaction_status_json(std::string* result);

    std::shared_ptr<MemTracker> segment_meta_mem_tracker() { return _segment_meta_mem_tracker; }
    std::shared_ptr<MemTracker> segcompaction_mem_tracker() { return _segcompaction_mem_tracker; }

    // check cumulative compaction config
    void check_cumulative_compaction_config();

    Status submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                  bool force);
    Status submit_seg_compaction_task(SegcompactionWorker* worker,
                                      SegCompactionCandidatesSharedPtr segments);

    std::unique_ptr<ThreadPool>& tablet_publish_txn_thread_pool() {
        return _tablet_publish_txn_thread_pool;
    }
    bool stopped() { return _stopped; }
    ThreadPool* get_bg_multiget_threadpool() { return _bg_multi_get_thread_pool.get(); }

    Status process_index_change_task(const TAlterInvertedIndexReq& reqest);

    void gc_binlogs(const std::unordered_map<int64_t, int64_t>& gc_tablet_infos);

    void add_async_publish_task(int64_t partition_id, int64_t tablet_id, int64_t publish_version,
                                int64_t transaction_id, bool is_recover);
    int64_t get_pending_publish_min_version(int64_t tablet_id);

    void add_quering_rowset(RowsetSharedPtr rs);

    RowsetSharedPtr get_quering_rowset(RowsetId rs_id);

    void evict_querying_rowset(RowsetId rs_id);

    bool add_broken_path(std::string path);
    bool remove_broken_path(std::string path);

    std::set<string> get_broken_paths() { return _broken_paths; }

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

    void _exit_if_too_many_disks_are_failed();

    void _clean_unused_txns();

    void _clean_unused_rowset_metas();

    void _clean_unused_binlog_metas();

    void _clean_unused_delete_bitmap();

    void _clean_unused_pending_publish_info();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now,
                     const int32_t expire);

    // All these xxx_callback() functions are for Background threads
    // unused rowset monitor thread
    void _unused_rowset_monitor_thread_callback();

    // garbage sweep thread process function. clear snapshot and trash folder
    void _garbage_sweeper_thread_callback();

    // delete tablet with io error process function
    void _disk_stat_monitor_thread_callback();

    // clean file descriptors cache
    void _cache_clean_callback();

    // path gc process function
    void _path_gc_thread_callback(DataDir* data_dir);

    void _path_scan_thread_callback(DataDir* data_dir);

    void _tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs);

    void _tablet_path_check_callback();

    // parse the default rowset type config to RowsetTypePB
    void _parse_default_rowset_type();

    // Disk status monitoring. Monitoring unused_flag Road King's new corresponding root_path unused flag,
    // When the unused mark is detected, the corresponding table information is deleted from the memory, and the disk data does not move.
    // When the disk status is unusable, but the unused logo is not _push_tablet_into_submitted_compactiondetected, you need to download it from root_path
    // Reload the data.
    void _start_disk_stat_monitor();

    void _compaction_tasks_producer_callback();

    void _update_replica_infos_callback();

    std::vector<TabletSharedPtr> _generate_compaction_tasks(CompactionType compaction_type,
                                                            std::vector<DataDir*>& data_dirs,
                                                            bool check_score);
    void _update_cumulative_compaction_policy();

    bool _push_tablet_into_submitted_compaction(TabletSharedPtr tablet,
                                                CompactionType compaction_type);
    void _pop_tablet_from_submitted_compaction(TabletSharedPtr tablet,
                                               CompactionType compaction_type);

    Status _init_stream_load_recorder(const std::string& stream_load_record_path);

    Status _submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                   bool force);

    Status _submit_single_replica_compaction_task(TabletSharedPtr tablet,
                                                  CompactionType compaction_type);

    void _adjust_compaction_thread_num();

    void _cooldown_tasks_producer_callback();
    void _remove_unused_remote_files_callback();
    void _cold_data_compaction_producer_callback();

    Status _handle_seg_compaction(SegcompactionWorker* worker,
                                  SegCompactionCandidatesSharedPtr segments);

    Status _handle_index_change(IndexBuilderSharedPtr index_builder);

    void _gc_binlogs(int64_t tablet_id, int64_t version);

    void _async_publish_callback();

    Status _persist_broken_paths();

private:
    struct CompactionCandidate {
        CompactionCandidate(uint32_t nicumulative_compaction_, int64_t tablet_id_, uint32_t index_)
                : nice(nicumulative_compaction_), tablet_id(tablet_id_), disk_index(index_) {}
        uint32_t nice; // priority
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
    std::mutex _trash_sweep_lock;
    std::map<std::string, DataDir*> _store_map;
    std::set<std::string> _broken_paths;
    std::mutex _broken_paths_mutex;

    uint32_t _available_storage_medium_type_count;

    int32_t _effective_cluster_id;
    bool _is_all_cluster_id_exist;

    std::atomic_bool _stopped {false};

    std::mutex _gc_mutex;
    // map<rowset_id(str), RowsetSharedPtr>, if we use RowsetId as the key, we need custom hash func
    std::unordered_map<std::string, RowsetSharedPtr> _unused_rowsets;

    // Hold reference of quering rowsets
    std::mutex _quering_rowsets_mutex;
    std::unordered_map<RowsetId, RowsetSharedPtr, HashOfRowsetId> _querying_rowsets;

    // Count the memory consumption of segment compaction tasks.
    std::shared_ptr<MemTracker> _segcompaction_mem_tracker;
    // This mem tracker is only for tracking memory use by segment meta data such as footer or index page.
    // The memory consumed by querying is tracked in segment iterator.
    std::shared_ptr<MemTracker> _segment_meta_mem_tracker;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _unused_rowset_monitor_thread;
    // thread to monitor snapshot expiry
    scoped_refptr<Thread> _garbage_sweeper_thread;
    // thread to monitor disk stat
    scoped_refptr<Thread> _disk_stat_monitor_thread;
    // thread to produce both base and cumulative compaction tasks
    scoped_refptr<Thread> _compaction_tasks_producer_thread;
    scoped_refptr<Thread> _update_replica_infos_thread;
    scoped_refptr<Thread> _cache_clean_thread;
    // threads to clean all file descriptor not actively in use
    std::vector<scoped_refptr<Thread>> _path_gc_threads;
    // threads to scan disk paths
    std::vector<scoped_refptr<Thread>> _path_scan_threads;
    // thread to produce tablet checkpoint tasks
    scoped_refptr<Thread> _tablet_checkpoint_tasks_producer_thread;
    // thread to check tablet path
    scoped_refptr<Thread> _tablet_path_check_thread;
    // thread to clean tablet lookup cache
    scoped_refptr<Thread> _lookup_cache_clean_thread;

    // For tablet and disk-stat report
    std::mutex _report_mtx;
    std::set<TaskWorkerPool*> _report_listeners;

    std::mutex _engine_task_mutex;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;

    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;
    std::unique_ptr<CalcDeleteBitmapExecutor> _calc_delete_bitmap_executor;

    // Used to control the migration from segment_v1 to segment_v2, can be deleted in futrue.
    // Type of new loaded data
    RowsetTypePB _default_rowset_type;

    HeartbeatFlags* _heartbeat_flags;

    std::unique_ptr<ThreadPool> _base_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cumu_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _single_replica_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _seg_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cold_data_compaction_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_publish_txn_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_meta_checkpoint_thread_pool;
    std::unique_ptr<ThreadPool> _bg_multi_get_thread_pool;

    CompactionPermitLimiter _permit_limiter;

    std::mutex _tablet_submitted_compaction_mutex;
    // a tablet can do base and cumulative compaction at same time
    std::map<DataDir*, std::unordered_set<TTabletId>> _tablet_submitted_cumu_compaction;
    std::map<DataDir*, std::unordered_set<TTabletId>> _tablet_submitted_base_compaction;

    std::mutex _peer_replica_infos_mutex;
    // key: tabletId
    std::unordered_map<int64_t, TReplicaInfo> _peer_replica_infos;
    std::string _token;

    std::atomic<int32_t> _wakeup_producer_flag {0};

    std::mutex _compaction_producer_sleep_mutex;
    std::condition_variable _compaction_producer_sleep_cv;

    std::shared_ptr<StreamLoadRecorder> _stream_load_recorder;
    std::unique_ptr<SpecialDir> _log_dir;
    std::unique_ptr<SpecialDir> _deploy_dir;

    // we use unordered_map to store all cumulative compaction policy sharded ptr
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            _cumulative_compaction_policies;

    scoped_refptr<Thread> _cooldown_tasks_producer_thread;
    scoped_refptr<Thread> _remove_unused_remote_files_thread;
    scoped_refptr<Thread> _cold_data_compaction_producer_thread;

    scoped_refptr<Thread> _cache_file_cleaner_tasks_producer_thread;

    std::unique_ptr<PriorityThreadPool> _cooldown_thread_pool;

    std::mutex _running_cooldown_mutex;
    std::unordered_set<int64_t> _running_cooldown_tablets;

    // tablet_id, publish_version, transaction_id, partition_id
    std::map<int64_t, std::map<int64_t, std::pair<int64_t, int64_t>>> _async_publish_tasks;
    // aync publish for discontinuous versions of merge_on_write table
    scoped_refptr<Thread> _async_publish_thread;
    std::mutex _async_publish_mutex;

    bool _clear_segment_cache = false;

    std::atomic<bool> _need_clean_trash {false};
    // next index for create tablet
    std::map<TStorageMedium::type, int> _store_next_index;

    DISALLOW_COPY_AND_ASSIGN(StorageEngine);
};

} // namespace doris
