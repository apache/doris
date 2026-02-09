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

#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/bvars.h"
#include "meta-service/delete_bitmap_lock_white_list.h"
#include "meta-service/txn_lazy_committer.h"
#include "meta-store/versionstamp.h"
#include "recycler/snapshot_chain_compactor.h"
#include "recycler/snapshot_data_migrator.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/white_black_list.h"
#include "snapshot/snapshot_manager.h"

namespace brpc {
class Server;
} // namespace brpc

namespace doris::cloud {
class TxnKv;
class InstanceRecycler;
class StorageVaultAccessor;
class Checker;
class SimpleThreadPool;
class RecyclerMetricsContext;
class TabletRecyclerMetricsContext;
class SegmentRecyclerMetricsContext;
struct RecyclerThreadPoolGroup {
    RecyclerThreadPoolGroup() = default;
    RecyclerThreadPoolGroup(std::shared_ptr<SimpleThreadPool> s3_producer_pool,
                            std::shared_ptr<SimpleThreadPool> recycle_tablet_pool,
                            std::shared_ptr<SimpleThreadPool> group_recycle_function_pool)
            : s3_producer_pool(std::move(s3_producer_pool)),
              recycle_tablet_pool(std::move(recycle_tablet_pool)),
              group_recycle_function_pool(std::move(group_recycle_function_pool)) {}
    ~RecyclerThreadPoolGroup() = default;
    RecyclerThreadPoolGroup(const RecyclerThreadPoolGroup&) = default;
    RecyclerThreadPoolGroup& operator=(RecyclerThreadPoolGroup& other) = default;
    RecyclerThreadPoolGroup& operator=(RecyclerThreadPoolGroup&& other) = default;
    RecyclerThreadPoolGroup(RecyclerThreadPoolGroup&&) = default;
    // used for accessor.delete_files, accessor.delete_directory
    std::shared_ptr<SimpleThreadPool> s3_producer_pool;
    // used for InstanceRecycler::recycle_tablet
    std::shared_ptr<SimpleThreadPool> recycle_tablet_pool;
    std::shared_ptr<SimpleThreadPool> group_recycle_function_pool;
};

class Recycler {
public:
    explicit Recycler(std::shared_ptr<TxnKv> txn_kv);
    ~Recycler();

    // returns 0 for success otherwise error
    int start(brpc::Server* server);

    void stop();

    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

private:
    void recycle_callback();

    void instance_scanner_callback();

    void lease_recycle_jobs();

    void check_recycle_tasks();

private:
    friend class RecyclerServiceImpl;

    std::shared_ptr<TxnKv> txn_kv_;
    std::atomic_bool stopped_ {false};

    std::vector<std::thread> workers_;

    std::mutex mtx_;
    // notify recycle workers
    std::condition_variable pending_instance_cond_;
    std::deque<InstanceInfoPB> pending_instance_queue_;
    std::unordered_set<std::string> pending_instance_set_;
    std::unordered_map<std::string, std::shared_ptr<InstanceRecycler>> recycling_instance_map_;
    // notify instance scanner and lease thread
    std::condition_variable notifier_;

    std::string ip_port_;

    WhiteBlackList instance_filter_;
    std::unique_ptr<Checker> checker_;

    RecyclerThreadPoolGroup _thread_pool_group;

    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<SnapshotDataMigrator> snapshot_data_migrator_;
    std::shared_ptr<SnapshotChainCompactor> snapshot_chain_compactor_;
};

enum class RowsetRecyclingState {
    FORMAL_ROWSET,
    TMP_ROWSET,
};

// Represents a single rowset deletion task for batch delete
struct RowsetDeleteTask {
    RowsetMetaCloudPB rowset_meta;
    std::string recycle_rowset_key;       // Primary key marking "pending recycle"
    std::string non_versioned_rowset_key; // Legacy non-versioned rowset meta key
    std::string versioned_rowset_key;     // Versioned meta rowset key
    std::string rowset_ref_count_key;
};

class RecyclerMetricsContext {
public:
    RecyclerMetricsContext() = default;

    RecyclerMetricsContext(std::string instance_id, std::string operation_type)
            : operation_type(std::move(operation_type)), instance_id(std::move(instance_id)) {
        start();
    }

    ~RecyclerMetricsContext() = default;

    std::atomic_ullong total_need_recycle_data_size = 0;
    std::atomic_ullong total_need_recycle_num = 0;

    std::atomic_ullong total_recycled_data_size = 0;
    std::atomic_ullong total_recycled_num = 0;

    std::string operation_type;
    std::string instance_id;

    double start_time = 0;

    void start() {
        start_time = duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    }

    double duration() const {
        return duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count() -
               start_time;
    }

    void reset() {
        total_need_recycle_data_size = 0;
        total_need_recycle_num = 0;
        total_recycled_data_size = 0;
        total_recycled_num = 0;
        start_time = duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    }

    void finish_report() {
        if (!operation_type.empty()) {
            double cost = duration();
            g_bvar_recycler_instance_last_round_recycle_elpased_ts.put(
                    {instance_id, operation_type}, cost);
            g_bvar_recycler_instance_recycle_round.put({instance_id, operation_type}, 1);
            LOG(INFO) << "recycle instance: " << instance_id
                      << ", operation type: " << operation_type << ", cost: " << cost
                      << " ms, total recycled num: " << total_recycled_num.load()
                      << ", total recycled data size: " << total_recycled_data_size.load()
                      << " bytes";
            if (cost != 0) {
                if (total_recycled_num.load() != 0) {
                    g_bvar_recycler_instance_recycle_time_per_resource.put(
                            {instance_id, operation_type}, cost / total_recycled_num.load());
                }
                g_bvar_recycler_instance_recycle_bytes_per_ms.put(
                        {instance_id, operation_type}, total_recycled_data_size.load() / cost);
            }
        }
    }

    // `is_begin` is used to initialize total num of items need to be recycled
    void report(bool is_begin = false) {
        if (!operation_type.empty()) {
            // is init
            if (is_begin) {
                auto value = total_need_recycle_num.load();

                g_bvar_recycler_instance_last_round_to_recycle_bytes.put(
                        {instance_id, operation_type}, total_need_recycle_data_size.load());
                g_bvar_recycler_instance_last_round_to_recycle_num.put(
                        {instance_id, operation_type}, value);
            } else {
                g_bvar_recycler_instance_last_round_recycled_bytes.put(
                        {instance_id, operation_type}, total_recycled_data_size.load());
                g_bvar_recycler_instance_recycle_total_bytes_since_started.put(
                        {instance_id, operation_type}, total_recycled_data_size.load());
                g_bvar_recycler_instance_last_round_recycled_num.put({instance_id, operation_type},
                                                                     total_recycled_num.load());
                g_bvar_recycler_instance_recycle_total_num_since_started.put(
                        {instance_id, operation_type}, total_recycled_num.load());
            }
        }
    }
};

class TabletRecyclerMetricsContext : public RecyclerMetricsContext {
public:
    TabletRecyclerMetricsContext() : RecyclerMetricsContext("global_recycler", "recycle_tablet") {}
};

class SegmentRecyclerMetricsContext : public RecyclerMetricsContext {
public:
    SegmentRecyclerMetricsContext()
            : RecyclerMetricsContext("global_recycler", "recycle_segment") {}
};

struct OplogRecycleStats;

class InstanceRecycler {
public:
    struct PackedFileRecycleStats {
        int64_t num_scanned = 0;          // packed-file kv scanned
        int64_t num_corrected = 0;        // packed-file kv corrected
        int64_t num_deleted = 0;          // packed-file kv deleted
        int64_t num_failed = 0;           // packed-file kv failed
        int64_t bytes_deleted = 0;        // packed-file kv bytes deleted from txn-kv
        int64_t num_object_deleted = 0;   // packed-file objects deleted from storage (vault/HDFS)
        int64_t bytes_object_deleted = 0; // bytes deleted from storage objects
        int64_t rowset_scan_count = 0;    // rowset metas scanned during correction
    };

    explicit InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance,
                              RecyclerThreadPoolGroup thread_pool_group,
                              std::shared_ptr<TxnLazyCommitter> txn_lazy_committer);
    ~InstanceRecycler();

    std::string_view instance_id() const { return instance_id_; }
    const InstanceInfoPB& instance_info() const { return instance_info_; }

    // returns 0 for success otherwise error
    int init();

    void stop() { stopped_.store(true, std::memory_order_release); }
    bool stopped() const { return stopped_.load(std::memory_order_acquire); }

    // returns 0 for success otherwise error
    int do_recycle();

    // remove all kv and data in this instance, ONLY be called when instance has been deleted
    // returns 0 for success otherwise error
    int recycle_deleted_instance();

    // scan and recycle expired indexes:
    // 1. dropped table, dropped mv
    // 2. half-successtable/index when create
    // returns 0 for success otherwise error
    int recycle_indexes();

    // scan and recycle expired partitions:
    // 1. dropped parttion
    // 2. half-success partition when create
    // returns 0 for success otherwise error
    int recycle_partitions();

    // scan and recycle expired rowsets:
    // 1. prepare_rowset will produce recycle_rowset before uploading data to remote storage (memo)
    // 2. compaction will change the input rowsets to recycle_rowset
    // returns 0 for success otherwise error
    int recycle_rowsets();

    // like `recycle_rowsets`, but for versioned rowsets.
    int recycle_versioned_rowsets();

    // scan and recycle expired tmp rowsets:
    // 1. commit_rowset will produce tmp_rowset when finish upload data (load or compaction) to remote storage
    // returns 0 for success otherwise error
    int recycle_tmp_rowsets();

    /**
     * recycle all tablets belonging to the index specified by `index_id`
     *
     * @param partition_id if positive, only recycle tablets in this partition belonging to the specified index
     * @return 0 for success otherwise error
     */
    int recycle_tablets(int64_t table_id, int64_t index_id, RecyclerMetricsContext& ctx,
                        int64_t partition_id = -1);

    /**
     * recycle all rowsets belonging to the tablet specified by `tablet_id`
     *
     * @return 0 for success otherwise error
     */
    int recycle_tablet(int64_t tablet_id, RecyclerMetricsContext& metrics_context);

    /**
     * like `recycle_tablet`, but for versioned tablet
     */
    int recycle_versioned_tablet(int64_t tablet_id, RecyclerMetricsContext& metrics_context);

    // scan and recycle useless partition version kv
    int recycle_versions();

    // scan and recycle the orphan partitions
    int recycle_orphan_partitions();

    // scan and abort timeout txn label
    // returns 0 for success otherwise error
    int abort_timeout_txn();

    //scan and recycle expire txn label
    // returns 0 for success otherwise error
    int recycle_expired_txn_label();

    // scan and recycle finished or timeout copy jobs
    // returns 0 for success otherwise error
    int recycle_copy_jobs();

    // scan and recycle dropped internal stage
    // returns 0 for success otherwise error
    int recycle_stage();

    // scan and recycle expired stage objects
    // returns 0 for success otherwise error
    int recycle_expired_stage_objects();

    // scan and recycle operation logs
    // returns 0 for success otherwise error
    int recycle_operation_logs();

    // scan and recycle expired restore jobs
    // returns 0 for success otherwise error
    int recycle_restore_jobs();

    /**
     * Scan packed-file metadata, correct reference counters, and recycle unused packed files.
     *
     * @return 0 on success, non-zero error code otherwise
     */
    int recycle_packed_files();

    // scan and recycle snapshots
    // returns 0 for success otherwise error
    int recycle_cluster_snapshots();

    // scan and recycle ref rowsets for deleted instance
    // returns 0 for success otherwise error
    int recycle_ref_rowsets(bool* has_unrecycled_rowsets);

    bool check_recycle_tasks();

    int scan_and_statistics_indexes();

    int scan_and_statistics_partitions();

    int scan_and_statistics_rowsets();

    int scan_and_statistics_tmp_rowsets();

    int scan_and_statistics_abort_timeout_txn();

    int scan_and_statistics_expired_txn_label();

    int scan_and_statistics_copy_jobs();

    int scan_and_statistics_stage();

    int scan_and_statistics_expired_stage_objects();

    int scan_and_statistics_versions();

    int scan_and_statistics_restore_jobs();

    void scan_and_statistics_operation_logs();

    /**
     * Decode the key of a packed-file metadata record into the persisted object path.
     *
     * @param key raw key persisted in txn-kv
     * @param packed_path output object storage path referenced by the key
     * @return true if decoding succeeds, false otherwise
     */
    static bool decode_packed_file_key(std::string_view key, std::string* packed_path);

    void TEST_add_accessor(std::string_view id, std::shared_ptr<StorageVaultAccessor> accessor) {
        accessor_map_.insert({std::string(id), std::move(accessor)});
    }

    // Recycle snapshot meta and data, return 0 for success otherwise error.
    int recycle_snapshot_meta_and_data(const std::string& resource_id,
                                       Versionstamp snapshot_version,
                                       const SnapshotPB& snapshot_pb);

private:
    // returns 0 for success otherwise error
    int init_obj_store_accessors();

    // returns 0 for success otherwise error
    int init_storage_vault_accessors();

    /**
     * Scan key-value pairs between [`begin`, `end`), and perform `recycle_func` on each key-value pair.
     *
     * @param recycle_func defines how to recycle resources corresponding to a key-value pair. Returns 0 if the recycling is successful.
     * @param loop_done is called after `RangeGetIterator` has no next kv. Usually used to perform a batch recycling. Returns 0 if success. 
     * @return 0 if all corresponding resources are recycled successfully, otherwise non-zero
     */
    int scan_and_recycle(std::string begin, std::string_view end,
                         std::function<int(std::string_view k, std::string_view v)> recycle_func,
                         std::function<int()> loop_done = nullptr);

    // return 0 for success otherwise error
    int delete_rowset_data(const doris::RowsetMetaCloudPB& rs_meta_pb);

    // return 0 for success otherwise error
    // NOTE: this function ONLY be called when the file paths cannot be calculated
    int delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                           const std::string& rowset_id);

    // return 0 for success otherwise error
    int delete_rowset_data(const std::map<std::string, doris::RowsetMetaCloudPB>& rowsets,
                           RowsetRecyclingState type, RecyclerMetricsContext& metrics_context);

    // return 0 for success otherwise error
    int decrement_packed_file_ref_counts(const doris::RowsetMetaCloudPB& rs_meta_pb);

    int delete_packed_file_and_kv(const std::string& packed_file_path,
                                  const std::string& packed_key,
                                  const cloud::PackedFileInfoPB& packed_info);

    /**
     * Get stage storage info from instance and init StorageVaultAccessor
     * @return 0 if accessor is successfully inited, 1 if stage not found, negative for error
     */
    int init_copy_job_accessor(const std::string& stage_id, const StagePB::StageType& stage_type,
                               std::shared_ptr<StorageVaultAccessor>* accessor);

    void register_recycle_task(const std::string& task_name, int64_t start_time);

    void unregister_recycle_task(const std::string& task_name);

    // for scan all tablets and statistics metrics
    int scan_tablets_and_statistics(int64_t tablet_id, int64_t index_id,
                                    RecyclerMetricsContext& metrics_context,
                                    int64_t partition_id = -1, bool is_empty_tablet = false);

    // for scan all rs of tablet and statistics metrics
    int scan_tablet_and_statistics(int64_t tablet_id, RecyclerMetricsContext& metrics_context);

    // Recycle operation log and the log keys. The log keys are specified by `raw_keys`.
    //
    // Both `operation_log` and `raw_keys` will be removed in the same transaction, to ensure atomicity.
    int recycle_operation_log(Versionstamp log_version, const std::vector<std::string>& raw_keys,
                              OperationLogPB operation_log,
                              OplogRecycleStats* oplog_stats = nullptr);

    // Recycle rowset meta and data, return 0 for success otherwise error
    //
    // Both recycle_rowset_key and non_versioned_rowset_key will be removed in the same transaction.
    //
    // This function will decrease the rowset ref count and remove the rowset meta and data if the ref count is 1.
    int recycle_rowset_meta_and_data(std::string_view recycle_rowset_key,
                                     const RowsetMetaCloudPB& rowset_meta,
                                     std::string_view non_versioned_rowset_key = "");

    // Classify rowset task by ref_count, return 0 to add to batch delete, 1 if handled (ref>1), -1 on error
    int classify_rowset_task_by_ref_count(RowsetDeleteTask& task,
                                          std::vector<RowsetDeleteTask>& batch_delete_tasks);

    // Cleanup metadata for deleted rowsets, return 0 for success otherwise error
    int cleanup_rowset_metadata(const std::vector<RowsetDeleteTask>& tasks);

    // Whether the instance has any snapshots, return 0 for success otherwise error.
    int has_cluster_snapshots(bool* any);

    // Whether need to recycle versioned keys
    bool should_recycle_versioned_keys() const;

    /**
     * Parse the path of a packed-file fragment and output the owning tablet and rowset identifiers.
     *
     * @param path packed-file fragment path to decode
     * @param tablet_id output tablet identifier extracted from the path
     * @param rowset_id output rowset identifier extracted from the path
     * @return true if both identifiers are successfully parsed, false otherwise
     */
    static bool parse_packed_slice_path(std::string_view path, int64_t* tablet_id,
                                        std::string* rowset_id);
    // Check whether a rowset referenced by a packed file still exists in metadata.
    // @param stats optional recycle statistics collector.
    int check_rowset_exists(int64_t tablet_id, const std::string& rowset_id, bool* exists,
                            PackedFileRecycleStats* stats = nullptr);
    int check_recycle_and_tmp_rowset_exists(int64_t tablet_id, const std::string& rowset_id,
                                            int64_t txn_id, bool* recycle_exists, bool* tmp_exists);
    /**
     * Resolve which storage accessor should be used for a packed file.
     *
     * @param hint preferred storage resource identifier persisted with the file
     * @return pair of the resolved resource identifier and accessor; the accessor can be null if unavailable
     */
    std::pair<std::string, std::shared_ptr<StorageVaultAccessor>> resolve_packed_file_accessor(
            const std::string& hint);
    // Recompute packed-file counters and lifecycle state after validating contained fragments.
    // @param stats optional recycle statistics collector.
    int correct_packed_file_info(cloud::PackedFileInfoPB* packed_info, bool* changed,
                                 const std::string& packed_file_path,
                                 PackedFileRecycleStats* stats = nullptr);
    // Correct and recycle a single packed-file record, updating metadata and accounting statistics.
    // @param stats optional recycle statistics collector.
    int process_single_packed_file(const std::string& packed_key,
                                   const std::string& packed_file_path,
                                   PackedFileRecycleStats* stats);
    // Process a packed-file KV while scanning and aggregate recycling statistics.
    int handle_packed_file_kv(std::string_view key, std::string_view value,
                              PackedFileRecycleStats* stats, int* ret);

    // Abort the transaction/job associated with a rowset that is about to be recycled.
    // This function is called during rowset recycling to prevent data loss by ensuring that
    // the transaction/job cannot be committed after its rowset data has been deleted.
    //
    // Scenario:
    // When recycler detects an expired prepared rowset (e.g., from a failed load transaction/job),
    // it needs to recycle the rowset data. However, if the transaction/job is still active and gets
    // committed after the data is deleted, it would lead to data loss - the transaction/job would
    // reference non-existent data.
    //
    // Solution:
    // Before recycling the rowset data, this function aborts the associated transaction/job to ensure
    // it cannot be committed. This guarantees that:
    // 1. The transaction/job state is marked as ABORTED
    // 2. Any subsequent commit_rowset/commit_txn attempts will fail
    // 3. The rowset data can be safely deleted without risk of data loss
    //
    // Parameters:
    //   txn_id: The transaction/job ID associated with the rowset to be recycled
    //
    // Returns:
    //   0 on success, -1 on failure
    int abort_txn_for_related_rowset(int64_t txn_id);
    int abort_job_for_related_rowset(const RowsetMetaCloudPB& rowset_meta);

    template <typename T>
    int abort_txn_or_job_for_recycle(T& rowset_meta_pb);

private:
    std::atomic_bool stopped_ {false};
    std::shared_ptr<TxnKv> txn_kv_;
    std::string instance_id_;
    InstanceInfoPB instance_info_;

    // TODO(plat1ko): Add new accessor to map in runtime for new created storage vaults
    std::unordered_map<std::string, std::shared_ptr<StorageVaultAccessor>> accessor_map_;
    using InvertedIndexInfo =
            std::pair<InvertedIndexStorageFormatPB, std::vector<std::pair<int64_t, std::string>>>;

    class InvertedIndexIdCache;
    std::unique_ptr<InvertedIndexIdCache> inverted_index_id_cache_;

    std::mutex recycled_tablets_mtx_;
    // Store recycled tablets, we can skip deleting rowset data of these tablets because these data has already been deleted.
    std::unordered_set<int64_t> recycled_tablets_;

    std::mutex recycle_tasks_mutex;
    // <task_name, start_time>>
    std::map<std::string, int64_t> running_recycle_tasks;

    RecyclerThreadPoolGroup _thread_pool_group;

    std::shared_ptr<TxnLazyCommitter> txn_lazy_committer_;
    std::shared_ptr<SnapshotManager> snapshot_manager_;
    std::shared_ptr<DeleteBitmapLockWhiteList> delete_bitmap_lock_white_list_;
    std::shared_ptr<ResourceManager> resource_mgr_;

    TabletRecyclerMetricsContext tablet_metrics_context_;
    SegmentRecyclerMetricsContext segment_metrics_context_;
};

struct OperationLogReferenceInfo {
    bool referenced_by_instance = false;
    bool referenced_by_snapshot = false;
    Versionstamp referenced_snapshot_timestamp;
};

struct OplogRecycleStats {
    // Total oplog count scanned per round
    std::atomic<int64_t> total_num {0};
    // Oplogs not recycled this round (per round, written to mBvarStatus)
    std::atomic<int64_t> not_recycled_num {0};
    // Recycle failures (per round, accumulated to mBvarIntAdder at end)
    std::atomic<int64_t> failed_num {0};
    // Per-oplog-type recycled counts (incremented after successful commit)
    std::atomic<int64_t> recycled_commit_partition {0};
    std::atomic<int64_t> recycled_drop_partition {0};
    std::atomic<int64_t> recycled_commit_index {0};
    std::atomic<int64_t> recycled_drop_index {0};
    std::atomic<int64_t> recycled_update_tablet {0};
    std::atomic<int64_t> recycled_compaction {0};
    std::atomic<int64_t> recycled_schema_change {0};
    std::atomic<int64_t> recycled_commit_txn {0};
};

// Helper class to check if operation logs can be recycled based on snapshots and versionstamps
class OperationLogRecycleChecker {
public:
    OperationLogRecycleChecker(std::string_view instance_id, TxnKv* txn_kv,
                               const InstanceInfoPB& instance_info)
            : instance_id_(instance_id), txn_kv_(txn_kv), instance_info_(instance_info) {}

    // Initialize the checker by loading snapshots and setting max version stamp
    int init();

    // Check if an operation log can be recycled
    bool can_recycle(const Versionstamp& log_versionstamp, int64_t log_min_timestamp,
                     OperationLogReferenceInfo* reference_info) const;

    Versionstamp max_versionstamp() const { return max_versionstamp_; }

    const std::vector<std::pair<SnapshotPB, Versionstamp>>& get_snapshots() const {
        return snapshots_;
    }

private:
    std::string_view instance_id_;
    TxnKv* txn_kv_;
    const InstanceInfoPB& instance_info_;
    Versionstamp max_versionstamp_;
    Versionstamp source_snapshot_versionstamp_;
    std::map<Versionstamp, size_t> snapshot_indexes_;
    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots_;
};

class SnapshotDataSizeCalculator {
public:
    SnapshotDataSizeCalculator(std::string_view instance_id, std::shared_ptr<TxnKv> txn_kv)
            : instance_id_(instance_id), txn_kv_(std::move(txn_kv)) {}

    void init(const std::vector<std::pair<SnapshotPB, Versionstamp>>& snapshots);

    int calculate_operation_log_data_size(const std::string_view& log_key,
                                          OperationLogPB& operation_log,
                                          OperationLogReferenceInfo& reference_info);

    int save_snapshot_data_size_with_retry();

private:
    int get_all_index_partitions(int64_t db_id, int64_t table_id, int64_t index_id,
                                 std::vector<int64_t>* partition_ids);
    int get_index_partition_data_size(int64_t db_id, int64_t table_id, int64_t index_id,
                                      int64_t partition_id, int64_t* data_size);
    int save_operation_log(const std::string_view& log_key, OperationLogPB& operation_log);
    int save_snapshot_data_size();

    std::string_view instance_id_;
    std::shared_ptr<TxnKv> txn_kv_;

    int64_t instance_retained_data_size_ = 0;
    std::map<Versionstamp, int64_t> retained_data_size_;
    std::set<std::string> calculated_partitions_;
};

} // namespace doris::cloud
