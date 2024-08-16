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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/compaction_permit_limiter.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_fwd.h"
#include "olap/task/index_builder.h"
#include "runtime/heartbeat_flags.h"
#include "util/countdown_latch.h"

namespace doris {

class DataDir;
class EngineTask;
class MemTableFlushExecutor;
class SegcompactionWorker;
class BaseCompaction;
class CumulativeCompaction;
class SingleReplicaCompaction;
class CumulativeCompactionPolicy;
class StreamLoadRecorder;
class TCloneReq;
class TCreateTabletReq;
class TabletManager;
class Thread;
class ThreadPool;
class TxnManager;
class ReportWorker;
class CreateTabletIdxCache;
struct DirInfo;
class SnapshotManager;

using SegCompactionCandidates = std::vector<segment_v2::SegmentSharedPtr>;
using SegCompactionCandidatesSharedPtr = std::shared_ptr<SegCompactionCandidates>;
using CumuCompactionPolicyTable =
        std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>;

class StorageEngine;
class CloudStorageEngine;

// StorageEngine singleton to manage all Table pointers.
// Providing add/drop/get operations.
// StorageEngine instance doesn't own the Table resources, just hold the pointer,
// allocation/deallocation must be done outside.
class BaseStorageEngine {
protected:
    enum Type : uint8_t {
        LOCAL, // Shared-nothing integrated compute and storage architecture
        CLOUD, // Separating compute and storage architecture
    };
    Type _type;

public:
    BaseStorageEngine(Type type, const UniqueId& backend_uid);
    virtual ~BaseStorageEngine();

    StorageEngine& to_local();
    CloudStorageEngine& to_cloud();

    virtual Status open() = 0;
    virtual void stop() = 0;
    virtual bool stopped() = 0;

    // start all background threads. This should be call after env is ready.
    virtual Status start_bg_threads() = 0;

    virtual Result<BaseTabletSPtr> get_tablet(int64_t tablet_id) = 0;

    void register_report_listener(ReportWorker* listener);
    void deregister_report_listener(ReportWorker* listener);
    void notify_listeners();
    bool notify_listener(std::string_view name);

    void set_heartbeat_flags(HeartbeatFlags* heartbeat_flags) {
        _heartbeat_flags = heartbeat_flags;
    }
    virtual Status set_cluster_id(int32_t cluster_id) = 0;
    int32_t effective_cluster_id() const { return _effective_cluster_id; }

    RowsetId next_rowset_id();

    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }
    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() {
        return _calc_delete_bitmap_executor.get();
    }

    void add_quering_rowset(RowsetSharedPtr rs);

    RowsetSharedPtr get_quering_rowset(RowsetId rs_id);

    int64_t memory_limitation_bytes_per_thread_for_schema_change() const;

    int get_disk_num() { return _disk_num; }

    Status init_stream_load_recorder(const std::string& stream_load_record_path);

    const std::shared_ptr<StreamLoadRecorder>& get_stream_load_recorder() {
        return _stream_load_recorder;
    }

protected:
    void _evict_querying_rowset();
    void _evict_quring_rowset_thread_callback();

    int32_t _effective_cluster_id = -1;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    // For task, tablet and disk report
    std::mutex _report_mtx;
    std::vector<ReportWorker*> _report_listeners;

    std::unique_ptr<RowsetIdGenerator> _rowset_id_generator;
    std::unique_ptr<MemTableFlushExecutor> _memtable_flush_executor;
    std::unique_ptr<CalcDeleteBitmapExecutor> _calc_delete_bitmap_executor;
    CountDownLatch _stop_background_threads_latch;

    // Hold reference of quering rowsets
    std::mutex _quering_rowsets_mutex;
    std::unordered_map<RowsetId, RowsetSharedPtr> _querying_rowsets;
    scoped_refptr<Thread> _evict_quering_rowset_thread;

    int64_t _memory_limitation_bytes_for_schema_change;

    int _disk_num {-1};

    std::shared_ptr<StreamLoadRecorder> _stream_load_recorder;
};

class CompactionSubmitRegistry {
    using TabletSet = std::unordered_set<TabletSharedPtr>;
    using Registry = std::map<DataDir*, TabletSet>;

public:
    CompactionSubmitRegistry() = default;
    CompactionSubmitRegistry(CompactionSubmitRegistry&& r);

    // create a snapshot for current registry, operations to the snapshot can be lock-free.
    CompactionSubmitRegistry create_snapshot();

    void reset(const std::vector<DataDir*>& stores);

    uint32_t count_executing_compaction(DataDir* dir, CompactionType compaction_type);
    uint32_t count_executing_cumu_and_base(DataDir* dir);

    bool has_compaction_task(DataDir* dir, CompactionType compaction_type);

    bool insert(TabletSharedPtr tablet, CompactionType compaction_type);

    void remove(TabletSharedPtr tablet, CompactionType compaction_type,
                std::function<void()> wakeup_cb);

    void jsonfy_compaction_status(std::string* result);

    std::vector<TabletSharedPtr> pick_topn_tablets_for_compaction(
            TabletManager* tablet_mgr, DataDir* data_dir, CompactionType compaction_type,
            const CumuCompactionPolicyTable& cumu_compaction_policies, uint32_t* disk_max_score);

private:
    TabletSet& _get_tablet_set(DataDir* dir, CompactionType compaction_type);

    std::mutex _tablet_submitted_compaction_mutex;
    Registry _tablet_submitted_cumu_compaction;
    Registry _tablet_submitted_base_compaction;
    Registry _tablet_submitted_full_compaction;
};

class StorageEngine final : public BaseStorageEngine {
public:
    StorageEngine(const EngineOptions& options);
    ~StorageEngine() override;

    Status open() override;

    Status create_tablet(const TCreateTabletReq& request, RuntimeProfile* profile);

    Result<BaseTabletSPtr> get_tablet(int64_t tablet_id) override;

    void clear_transaction_task(const TTransactionId transaction_id);
    void clear_transaction_task(const TTransactionId transaction_id,
                                const std::vector<TPartitionId>& partition_ids);

    std::vector<DataDir*> get_stores(bool include_unused = false);

    // get all info of root_path
    Status get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos, bool need_update);

    static int64_t get_file_or_directory_size(const std::string& file_path);

    // get root path for creating tablet. The returned vector of root path should be round robin,
    // for avoiding that all the tablet would be deployed one disk.
    std::vector<DataDir*> get_stores_for_create_tablet(int64 partition_id,
                                                       TStorageMedium::type storage_medium);

    DataDir* get_store(const std::string& path);

    uint32_t available_storage_medium_type_count() const {
        return _available_storage_medium_type_count;
    }

    Status set_cluster_id(int32_t cluster_id) override;

    void start_delete_unused_rowset();
    void add_unused_rowset(RowsetSharedPtr rowset);

    // Obtain shard path for new tablet.
    //
    // @param [out] shard_path choose an available root_path to clone new tablet
    // @return error code
    Status obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash,
                             std::string* shared_path, DataDir** store, int64_t partition_id);

    // Load new tablet to make it effective.
    //
    // @param [in] root_path specify root path of new tablet
    // @param [in] request specify new tablet info
    // @param [in] restore whether we're restoring a tablet from trash
    // @return OK if load tablet success
    Status load_header(const std::string& shard_path, const TCloneReq& request,
                       bool restore = false);

    TabletManager* tablet_manager() { return _tablet_manager.get(); }
    TxnManager* txn_manager() { return _txn_manager.get(); }
    SnapshotManager* snapshot_mgr() { return _snapshot_mgr.get(); }
    MemTableFlushExecutor* memtable_flush_executor() { return _memtable_flush_executor.get(); }
    // Rowset garbage collection helpers
    bool check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id);
    PendingRowsetSet& pending_local_rowsets() { return _pending_local_rowsets; }
    PendingRowsetSet& pending_remote_rowsets() { return _pending_remote_rowsets; }
    PendingRowsetGuard add_pending_rowset(const RowsetWriterContext& ctx);

    RowsetTypePB default_rowset_type() const {
        if (_heartbeat_flags != nullptr && _heartbeat_flags->is_set_default_rowset_type_to_beta()) {
            return BETA_ROWSET;
        }
        return _default_rowset_type;
    }

    Status start_bg_threads() override;

    // clear trash and snapshot file
    // option: update disk usage after sweep
    Status start_trash_sweep(double* usage, bool ignore_guard = false);

    // Must call stop() before storage_engine is deconstructed
    void stop() override;

    void get_tablet_rowset_versions(const PGetTabletVersionsRequest* request,
                                    PGetTabletVersionsResponse* response);

    bool get_peer_replica_info(int64_t tablet_id, TReplicaInfo* replica, std::string* token);

    bool should_fetch_from_peer(int64_t tablet_id);

    const std::shared_ptr<StreamLoadRecorder>& get_stream_load_recorder() {
        return _stream_load_recorder;
    }

    void get_compaction_status_json(std::string* result);

    Status submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                  bool force, bool eager = true);
    Status submit_seg_compaction_task(std::shared_ptr<SegcompactionWorker> worker,
                                      SegCompactionCandidatesSharedPtr segments);

    ThreadPool* tablet_publish_txn_thread_pool() { return _tablet_publish_txn_thread_pool.get(); }
    bool stopped() override { return _stopped; }
    ThreadPool* get_bg_multiget_threadpool() { return _bg_multi_get_thread_pool.get(); }

    Status process_index_change_task(const TAlterInvertedIndexReq& reqest);

    void gc_binlogs(const std::unordered_map<int64_t, int64_t>& gc_tablet_infos);

    void add_async_publish_task(int64_t partition_id, int64_t tablet_id, int64_t publish_version,
                                int64_t transaction_id, bool is_recover);
    int64_t get_pending_publish_min_version(int64_t tablet_id);

    bool add_broken_path(std::string path);
    bool remove_broken_path(std::string path);

    std::set<string> get_broken_paths() { return _broken_paths; }

private:
    // Instance should be inited from `static open()`
    // MUST NOT be called in other circumstances.
    Status _open();

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

    void _clean_unused_partial_update_info();

    Status _do_sweep(const std::string& scan_root, const time_t& local_tm_now,
                     const int32_t expire);

    // All these xxx_callback() functions are for Background threads
    // unused rowset monitor thread
    void _unused_rowset_monitor_thread_callback();

    // garbage sweep thread process function. clear snapshot and trash folder
    void _garbage_sweeper_thread_callback();

    // delete tablet with io error process function
    void _disk_stat_monitor_thread_callback();

    // path gc process function
    void _path_gc_thread_callback(DataDir* data_dir);

    void _tablet_path_check_callback();

    void _tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs);

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

    void _pop_tablet_from_submitted_compaction(TabletSharedPtr tablet,
                                               CompactionType compaction_type);

    Status _submit_compaction_task(TabletSharedPtr tablet, CompactionType compaction_type,
                                   bool force);

    Status _submit_single_replica_compaction_task(TabletSharedPtr tablet,
                                                  CompactionType compaction_type);

    void _adjust_compaction_thread_num();

    void _cooldown_tasks_producer_callback();
    void _remove_unused_remote_files_callback();
    void do_remove_unused_remote_files();
    void _cold_data_compaction_producer_callback();

    Status _handle_seg_compaction(std::shared_ptr<SegcompactionWorker> worker,
                                  SegCompactionCandidatesSharedPtr segments,
                                  uint64_t submission_time);

    Status _handle_index_change(IndexBuilderSharedPtr index_builder);

    void _gc_binlogs(int64_t tablet_id, int64_t version);

    void _async_publish_callback();

    void _process_async_publish();

    Status _persist_broken_paths();

    bool _increase_low_priority_task_nums(DataDir* dir);

    void _decrease_low_priority_task_nums(DataDir* dir);

    void _get_candidate_stores(TStorageMedium::type storage_medium,
                               std::vector<DirInfo>& dir_infos);

    int _get_and_set_next_disk_index(int64 partition_id, TStorageMedium::type storage_medium);

    int32_t _auto_get_interval_by_disk_capacity(DataDir* data_dir);

private:
    EngineOptions _options;
    std::mutex _store_lock;
    std::mutex _trash_sweep_lock;
    std::map<std::string, std::unique_ptr<DataDir>> _store_map;
    std::set<std::string> _broken_paths;
    std::mutex _broken_paths_mutex;

    uint32_t _available_storage_medium_type_count;

    bool _is_all_cluster_id_exist;

    std::atomic_bool _stopped {false};

    std::mutex _gc_mutex;
    std::unordered_map<RowsetId, RowsetSharedPtr> _unused_rowsets;
    PendingRowsetSet _pending_local_rowsets;
    PendingRowsetSet _pending_remote_rowsets;

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
    // thread to produce tablet checkpoint tasks
    scoped_refptr<Thread> _tablet_checkpoint_tasks_producer_thread;
    // thread to check tablet path
    scoped_refptr<Thread> _tablet_path_check_thread;
    // thread to clean tablet lookup cache
    scoped_refptr<Thread> _lookup_cache_clean_thread;

    std::mutex _engine_task_mutex;

    std::unique_ptr<TabletManager> _tablet_manager;
    std::unique_ptr<TxnManager> _txn_manager;

    // Used to control the migration from segment_v1 to segment_v2, can be deleted in futrue.
    // Type of new loaded data
    RowsetTypePB _default_rowset_type;

    std::unique_ptr<ThreadPool> _base_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cumu_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _single_replica_compaction_thread_pool;

    std::unique_ptr<ThreadPool> _seg_compaction_thread_pool;
    std::unique_ptr<ThreadPool> _cold_data_compaction_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_publish_txn_thread_pool;

    std::unique_ptr<ThreadPool> _tablet_meta_checkpoint_thread_pool;
    std::unique_ptr<ThreadPool> _bg_multi_get_thread_pool;

    CompactionPermitLimiter _permit_limiter;

    CompactionSubmitRegistry _compaction_submit_registry;

    std::mutex _low_priority_task_nums_mutex;
    std::unordered_map<DataDir*, int32_t> _low_priority_task_nums;

    std::mutex _peer_replica_infos_mutex;
    // key: tabletId
    std::unordered_map<int64_t, TReplicaInfo> _peer_replica_infos;
    std::string _token;

    std::atomic<int32_t> _wakeup_producer_flag {0};

    std::mutex _compaction_producer_sleep_mutex;
    std::condition_variable _compaction_producer_sleep_cv;

    // we use unordered_map to store all cumulative compaction policy sharded ptr
    CumuCompactionPolicyTable _cumulative_compaction_policies;

    scoped_refptr<Thread> _cooldown_tasks_producer_thread;
    scoped_refptr<Thread> _remove_unused_remote_files_thread;
    scoped_refptr<Thread> _cold_data_compaction_producer_thread;

    scoped_refptr<Thread> _cache_file_cleaner_tasks_producer_thread;

    std::unique_ptr<PriorityThreadPool> _cooldown_thread_pool;

    std::mutex _running_cooldown_mutex;
    std::unordered_set<int64_t> _running_cooldown_tablets;

    std::mutex _cold_compaction_tablet_submitted_mtx;
    std::unordered_set<int64_t> _cold_compaction_tablet_submitted;

    // tablet_id, publish_version, transaction_id, partition_id
    std::map<int64_t, std::map<int64_t, std::pair<int64_t, int64_t>>> _async_publish_tasks;
    // aync publish for discontinuous versions of merge_on_write table
    scoped_refptr<Thread> _async_publish_thread;
    std::shared_mutex _async_publish_lock;

    std::atomic<bool> _need_clean_trash {false};

    // next index for create tablet
    std::map<TStorageMedium::type, int> _last_use_index;

    std::unique_ptr<CreateTabletIdxCache> _create_tablet_idx_lru_cache;

    std::unique_ptr<SnapshotManager> _snapshot_mgr;
};

// lru cache for create tabelt round robin in disks
// key: partitionId_medium
// value: index
class CreateTabletIdxCache : public LRUCachePolicyTrackingManual {
public:
    // get key, delimiter with DELIMITER '-'
    static std::string get_key(int64_t partition_id, TStorageMedium::type medium) {
        return fmt::format("{}-{}", partition_id, medium);
    }

    // -1 not found key in lru
    int get_index(const std::string& key);

    void set_index(const std::string& key, int next_idx);

    class CacheValue : public LRUCacheValueBase {
    public:
        int idx = 0;
    };

    CreateTabletIdxCache(size_t capacity)
            : LRUCachePolicyTrackingManual(CachePolicy::CacheType::CREATE_TABLET_RR_IDX_CACHE,
                                           capacity, LRUCacheType::NUMBER,
                                           /*stale_sweep_time_s*/ 30 * 60) {}
};

struct DirInfo {
    DataDir* data_dir;

    double usage = 0;
    int available_level = 0;

    bool operator<(const DirInfo& other) const {
        if (available_level != other.available_level) {
            return available_level < other.available_level;
        }
        return data_dir->path_hash() < other.data_dir->path_hash();
    }
};

} // namespace doris
