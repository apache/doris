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
#include <glog/logging.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "olap/base_tablet.h"
#include "olap/binlog_config.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/version_graph.h"
#include "segment_loader.h"
#include "util/metrics.h"
#include "util/once.h"
#include "util/slice.h"

namespace bvar {
template <typename T>
class Adder;
}

namespace doris {

class Tablet;
class CumulativeCompactionPolicy;
class CompactionMixin;
class SingleReplicaCompaction;
class RowsetWriter;
struct RowsetWriterContext;
class TTabletInfo;
class TabletMetaPB;
class TupleDescriptor;
class CalcDeleteBitmapToken;
enum CompressKind : int;
class RowsetBinlogMetasPB;

namespace io {
class RemoteFileSystem;
} // namespace io
namespace vectorized {
class Block;
} // namespace vectorized
struct RowLocation;
enum KeysType : int;
enum SortType : int;

enum TabletStorageType { STORAGE_TYPE_LOCAL, STORAGE_TYPE_REMOTE, STORAGE_TYPE_REMOTE_AND_LOCAL };

extern bvar::Adder<uint64_t> unused_remote_rowset_num;

static inline constexpr auto TRACE_TABLET_LOCK_THRESHOLD = std::chrono::seconds(1);

struct WriteCooldownMetaExecutors {
    WriteCooldownMetaExecutors(size_t executor_nums = 5);

    void stop();

    void submit(TabletSharedPtr tablet);
    size_t _get_executor_pos(int64_t tablet_id) const {
        return std::hash<int64_t>()(tablet_id) % _executor_nums;
    };
    // Each executor is a mpsc to ensure uploads of the same tablet meta are not concurrent
    // FIXME(AlexYue): Use mpsc instead of `ThreadPool` with 1 thread
    // We use PriorityThreadPool since it would call status inside it's `shutdown` function.
    // Consider one situation where the StackTraceCache's singleton is detructed before
    // this WriteCooldownMetaExecutors's singleton, then invoking the status would also call
    // StackTraceCache which would then result in heap use after free like #23834
    std::vector<std::unique_ptr<PriorityThreadPool>> _executors;
    std::unordered_set<int64_t> _pending_tablets;
    std::mutex _latch;
    size_t _executor_nums;
};

class Tablet final : public BaseTablet {
public:
    Tablet(StorageEngine& engine, TabletMetaSharedPtr tablet_meta, DataDir* data_dir,
           const std::string_view& cumulative_compaction_type = "");

    DataDir* data_dir() const { return _data_dir; }
    int64_t replica_id() const { return _tablet_meta->replica_id(); }
    TabletUid tablet_uid() const { return _tablet_meta->tablet_uid(); }

    const std::string& tablet_path() const { return _tablet_path; }

    bool set_tablet_schema_into_rowset_meta();
    Status init();
    bool init_succeeded();

    bool is_used();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();

    void save_meta();
    // Used in clone task, to update local meta when finishing a clone job
    Status revise_tablet_meta(const std::vector<RowsetSharedPtr>& to_add,
                              const std::vector<RowsetSharedPtr>& to_delete,
                              bool is_incremental_clone);

    int64_t cumulative_layer_point() const;
    void set_cumulative_layer_point(int64_t new_point);
    inline int64_t cumulative_promotion_size() const;
    inline void set_cumulative_promotion_size(int64_t new_size);

    // Disk space occupied by tablet, contain local and remote.
    size_t tablet_footprint() override;
    // Local disk space occupied by tablet.
    size_t tablet_local_size();
    // Remote disk space occupied by tablet.
    size_t tablet_remote_size();

    size_t num_rows();
    int version_count() const;
    int stale_version_count() const;
    bool exceed_version_limit(int32_t limit) override;
    uint64_t segment_count() const;
    Version max_version() const;
    CumulativeCompactionPolicy* cumulative_compaction_policy();

    // properties encapsulated in TabletSchema
    SortType sort_type() const;
    size_t sort_col_num() const;
    size_t num_columns() const;
    size_t num_null_columns() const;
    size_t num_short_key_columns() const;
    size_t num_rows_per_row_block() const;
    CompressKind compress_kind() const;
    double bloom_filter_fpp() const;
    size_t next_unique_id() const;
    size_t row_size() const;
    int64_t avg_rs_meta_serialize_size() const;

    // operation in rowsets
    Status add_rowset(RowsetSharedPtr rowset);
    Status create_initial_rowset(const int64_t version);

    // MUST hold EXCLUSIVE `_meta_lock`.
    Status modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                          std::vector<RowsetSharedPtr>& to_delete, bool check_delete = false);

    Status add_inc_rowset(const RowsetSharedPtr& rowset);
    /// Delete stale rowset by timing. This delete policy uses now() minutes
    /// config::tablet_rowset_expired_stale_sweep_time_sec to compute the deadline of expired rowset
    /// to delete.  When rowset is deleted, it will be added to StorageEngine unused map and record
    /// need to delete flag.
    void delete_expired_stale_rowset();

    // Given spec_version, find a continuous version path and store it in version_path.
    // If quiet is true, then only "does this path exist" is returned.
    // If skip_missing_version is true, return ok even there are missing versions.
    Status capture_consistent_versions_unlocked(const Version& spec_version, Versions* version_path,
                                                bool skip_missing_version, bool quiet) const;

    // if quiet is true, no error log will be printed if there are missing versions
    Status check_version_integrity(const Version& version, bool quiet = false);
    bool check_version_exist(const Version& version) const;
    void acquire_version_and_rowsets(
            std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const;

    Status capture_consistent_rowsets_unlocked(
            const Version& spec_version, std::vector<RowsetSharedPtr>* rowsets) const override;

    // If skip_missing_version is true, skip versions if they are missing.
    Status capture_rs_readers(const Version& spec_version, std::vector<RowSetSplits>* rs_splits,
                              bool skip_missing_version) override;

    // Find the missed versions until the spec_version.
    //
    // for example:
    //     [0-4][5-5][8-8][9-9][14-14]
    // if spec_version = 12, it will return [6, 6], [7, 7], [10, 10], [11, 11], [12, 12]
    Versions calc_missed_versions(int64_t spec_version, Versions existing_versions) const override;

    // meta lock
    std::shared_mutex& get_header_lock() { return _meta_lock; }
    std::mutex& get_rowset_update_lock() { return _rowset_update_lock; }
    std::mutex& get_push_lock() { return _ingest_lock; }
    std::mutex& get_base_compaction_lock() { return _base_compaction_lock; }
    std::mutex& get_cumulative_compaction_lock() { return _cumulative_compaction_lock; }

    std::shared_timed_mutex& get_migration_lock() { return _migration_lock; }

    std::mutex& get_build_inverted_index_lock() { return _build_inverted_index_lock; }

    // operation for compaction
    bool can_do_compaction(size_t path_hash, CompactionType compaction_type);
    uint32_t calc_compaction_score(
            CompactionType compaction_type,
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy);

    // This function to find max continuous version from the beginning.
    // For example: If there are 1, 2, 3, 5, 6, 7 versions belongs tablet, then 3 is target.
    // 3 will be saved in "version", and 7 will be saved in "max_version", if max_version != nullptr
    void max_continuous_version_from_beginning(Version* version, Version* max_version = nullptr);

    void set_bad(bool is_bad) { _is_bad = is_bad; }

    int64_t last_cumu_compaction_failure_time() { return _last_cumu_compaction_failure_millis; }
    void set_last_cumu_compaction_failure_time(int64_t millis) {
        _last_cumu_compaction_failure_millis = millis;
    }

    int64_t last_base_compaction_failure_time() { return _last_base_compaction_failure_millis; }
    void set_last_base_compaction_failure_time(int64_t millis) {
        _last_base_compaction_failure_millis = millis;
    }

    int64_t last_full_compaction_failure_time() { return _last_full_compaction_failure_millis; }
    void set_last_full_compaction_failure_time(int64_t millis) {
        _last_full_compaction_failure_millis = millis;
    }

    int64_t last_cumu_compaction_success_time() { return _last_cumu_compaction_success_millis; }
    void set_last_cumu_compaction_success_time(int64_t millis) {
        _last_cumu_compaction_success_millis = millis;
    }

    int64_t last_base_compaction_success_time() { return _last_base_compaction_success_millis; }
    void set_last_base_compaction_success_time(int64_t millis) {
        _last_base_compaction_success_millis = millis;
    }

    int64_t last_full_compaction_success_time() { return _last_full_compaction_success_millis; }
    void set_last_full_compaction_success_time(int64_t millis) {
        _last_full_compaction_success_millis = millis;
    }

    int64_t last_base_compaction_schedule_time() { return _last_base_compaction_schedule_millis; }
    void set_last_base_compaction_schedule_time(int64_t millis) {
        _last_base_compaction_schedule_millis = millis;
    }

    void set_last_single_compaction_failure_status(std::string status) {
        _last_single_compaction_failure_status = std::move(status);
    }

    void set_last_fetched_version(Version version) { _last_fetched_version = std::move(version); }

    void delete_all_files();

    void check_tablet_path_exists();

    TabletInfo get_tablet_info() const;

    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_cumulative_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_base_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_full_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_build_inverted_index(
            const std::set<int64_t>& alter_index_uids, bool is_drop_op);

    // used for single compaction to get the local versions
    // Single compaction does not require remote rowsets and cannot violate the cooldown semantics
    std::vector<Version> get_all_local_versions();

    void calculate_cumulative_point();
    // TODO(ygl):
    bool is_primary_replica() { return false; }

    // return true if the checkpoint is actually done
    bool do_tablet_meta_checkpoint();

    // Check whether the rowset is useful or not, unuseful rowset can be swept up then.
    // Rowset which is under tablet's management is useful, i.e. rowset is in
    // _rs_version_map, or _stale_rs_version_map.
    // Rowset whose version range is not covered by this tablet is also useful.
    bool rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta);

    void build_tablet_report_info(TTabletInfo* tablet_info,
                                  bool enable_consecutive_missing_check = false,
                                  bool enable_path_check = false);

    // return a json string to show the compaction status of this tablet
    void get_compaction_status(std::string* json_result);

    static Status prepare_compaction_and_calculate_permits(
            CompactionType compaction_type, const TabletSharedPtr& tablet,
            std::shared_ptr<CompactionMixin>& compaction, int64_t& permits);

    void execute_compaction(CompactionMixin& compaction);
    void execute_single_replica_compaction(SingleReplicaCompaction& compaction);

    void set_cumulative_compaction_policy(
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }

    std::shared_ptr<CumulativeCompactionPolicy> get_cumulative_compaction_policy() {
        return _cumulative_compaction_policy;
    }

    void set_last_base_compaction_status(std::string status) {
        _last_base_compaction_status = std::move(status);
    }

    std::string get_last_base_compaction_status() { return _last_base_compaction_status; }

    std::tuple<int64_t, int64_t> get_visible_version_and_time() const;

    void set_visible_version(const std::shared_ptr<const VersionWithTime>& visible_version) {
        std::atomic_store_explicit(&_visible_version, visible_version, std::memory_order_relaxed);
    }

    bool should_fetch_from_peer();

    inline bool all_beta() const {
        std::shared_lock rdlock(_meta_lock);
        return _tablet_meta->all_beta();
    }

    const TabletSchemaSPtr& tablet_schema_unlocked() const { return _max_version_schema; }

    Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& context,
                                                               bool vertical) override;

    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
            int64_t txn_expiration = 0) override;
    Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            RowsetWriterContext& context, const RowsetId& rowset_id);

    Status create_rowset(const RowsetMetaSharedPtr& rowset_meta, RowsetSharedPtr* rowset);

    // MUST hold EXCLUSIVE `_meta_lock`
    void add_rowsets(const std::vector<RowsetSharedPtr>& to_add);
    // MUST hold EXCLUSIVE `_meta_lock`
    void delete_rowsets(const std::vector<RowsetSharedPtr>& to_delete, bool move_to_stale);

    // MUST hold SHARED `_meta_lock`
    const auto& rowset_map() const { return _rs_version_map; }
    // MUST hold SHARED `_meta_lock`
    const auto& stale_rowset_map() const { return _stale_rs_version_map; }

    ////////////////////////////////////////////////////////////////////////////
    // begin cooldown functions
    ////////////////////////////////////////////////////////////////////////////
    int64_t storage_policy_id() const { return _tablet_meta->storage_policy_id(); }
    void set_storage_policy_id(int64_t id) { _tablet_meta->set_storage_policy_id(id); }

    int64_t last_failed_follow_cooldown_time() const { return _last_failed_follow_cooldown_time; }

    // Cooldown to remote fs.
    Status cooldown(RowsetSharedPtr rowset = nullptr);

    RowsetSharedPtr pick_cooldown_rowset();

    RowsetSharedPtr need_cooldown(int64_t* cooldown_timestamp, size_t* file_size);

    struct CooldownConf {
        int64_t term = -1;
        int64_t cooldown_replica_id = -1;
    };

    CooldownConf cooldown_conf() const {
        std::shared_lock rlock(_cooldown_conf_lock);
        return _cooldown_conf;
    }

    CooldownConf cooldown_conf_unlocked() const { return _cooldown_conf; }

    // Return `true` if update success
    bool update_cooldown_conf(int64_t cooldown_term, int64_t cooldown_replica_id);

    Status remove_all_remote_rowsets();

    void record_unused_remote_rowset(const RowsetId& rowset_id, const std::string& resource,
                                     int64_t num_segments);

    uint32_t calc_cold_data_compaction_score() const;

    std::mutex& get_cold_compaction_lock() { return _cold_compaction_lock; }

    std::shared_mutex& get_cooldown_conf_lock() { return _cooldown_conf_lock; }

    static void async_write_cooldown_meta(TabletSharedPtr tablet);
    // Return `ABORTED` if should not to retry again
    Status write_cooldown_meta();
    ////////////////////////////////////////////////////////////////////////////
    // end cooldown functions
    ////////////////////////////////////////////////////////////////////////////

    CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() override;
    Status save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                              DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                              const RowsetIdUnorderedSet& cur_rowset_ids) override;

    void merge_delete_bitmap(const DeleteBitmap& delete_bitmap);
    bool check_all_rowset_segment();

    void update_max_version_schema(const TabletSchemaSPtr& tablet_schema);

    void set_skip_compaction(bool skip,
                             CompactionType compaction_type = CompactionType::CUMULATIVE_COMPACTION,
                             int64_t start = -1);
    bool should_skip_compaction(CompactionType compaction_type, int64_t now);

    std::vector<std::string> get_binlog_filepath(std::string_view binlog_version) const;
    std::pair<std::string, int64_t> get_binlog_info(std::string_view binlog_version) const;
    std::string get_rowset_binlog_meta(std::string_view binlog_version,
                                       std::string_view rowset_id) const;
    Status get_rowset_binlog_metas(const std::vector<int64_t>& binlog_versions,
                                   RowsetBinlogMetasPB* metas_pb);
    std::string get_segment_filepath(std::string_view rowset_id,
                                     std::string_view segment_index) const;
    std::string get_segment_filepath(std::string_view rowset_id, int64_t segment_index) const;
    std::string get_segment_index_filepath(std::string_view rowset_id,
                                           std::string_view segment_index,
                                           std::string_view index_id) const;
    std::string get_segment_index_filepath(std::string_view rowset_id, int64_t segment_index,
                                           int64_t index_id) const;
    bool can_add_binlog(uint64_t total_binlog_size) const;
    void gc_binlogs(int64_t version);
    Status ingest_binlog_metas(RowsetBinlogMetasPB* metas_pb);

    inline void report_error(const Status& st) {
        if (st.is<ErrorCode::IO_ERROR>()) {
            ++_io_error_times;
        } else if (st.is<ErrorCode::CORRUPTION>()) {
            _io_error_times = config::max_tablet_io_errors + 1;
        }
    }

    inline int64_t get_io_error_times() const { return _io_error_times; }

    inline bool is_io_error_too_times() const {
        return config::max_tablet_io_errors > 0 && _io_error_times >= config::max_tablet_io_errors;
    }

    int64_t get_table_id() { return _tablet_meta->table_id(); }

    // binlog related functions
    bool is_enable_binlog();
    bool is_binlog_enabled() { return _tablet_meta->binlog_config().is_enable(); }
    int64_t binlog_ttl_ms() const { return _tablet_meta->binlog_config().ttl_seconds(); }
    int64_t binlog_max_bytes() const { return _tablet_meta->binlog_config().max_bytes(); }

    void set_binlog_config(BinlogConfig binlog_config);

    void set_alter_failed(bool alter_failed) { _alter_failed = alter_failed; }
    bool is_alter_failed() { return _alter_failed; }

    void set_is_full_compaction_running(bool is_full_compaction_running) {
        _is_full_compaction_running = is_full_compaction_running;
    }
    inline bool is_full_compaction_running() const { return _is_full_compaction_running; }
    void clear_cache() override;

private:
    Status _init_once_action();
    bool _contains_rowset(const RowsetId rowset_id);
    Status _contains_version(const Version& version);

    // Returns:
    // version: the max continuous version from beginning
    // max_version: the max version of this tablet
    void _max_continuous_version_from_beginning_unlocked(Version* version, Version* max_version,
                                                         bool* has_version_cross) const;
    RowsetSharedPtr _rowset_with_largest_size();
    /// Delete stale rowset by version. This method not only delete the version in expired rowset map,
    /// but also delete the version in rowset meta vector.
    void _delete_stale_rowset_by_version(const Version& version);

    uint32_t _calc_cumulative_compaction_score(
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy);
    uint32_t _calc_base_compaction_score() const;

    std::vector<RowsetSharedPtr> _pick_visible_rowsets_to_compaction(int64_t min_start_version,
                                                                     int64_t max_start_version);

    void _init_context_common_fields(RowsetWriterContext& context);

    ////////////////////////////////////////////////////////////////////////////
    // begin cooldown functions
    ////////////////////////////////////////////////////////////////////////////
    Status _cooldown_data(RowsetSharedPtr rowset);
    Status _follow_cooldowned_data();
    Status _read_cooldown_meta(const StorageResource& storage_resource,
                               TabletMetaPB* tablet_meta_pb);
    bool _has_data_to_cooldown();
    int64_t _get_newest_cooldown_time(const RowsetSharedPtr& rowset);
    ////////////////////////////////////////////////////////////////////////////
    // end cooldown functions
    ////////////////////////////////////////////////////////////////////////////

    void _clear_cache_by_rowset(const BetaRowsetSharedPtr& rowset);

public:
    static const int64_t K_INVALID_CUMULATIVE_POINT = -1;

private:
    StorageEngine& _engine;
    DataDir* _data_dir = nullptr;

    std::string _tablet_path;

    DorisCallOnce<Status> _init_once;
    // meta store lock is used for prevent 2 threads do checkpoint concurrently
    // it will be used in econ-mode in the future
    std::shared_mutex _meta_store_lock;
    std::mutex _ingest_lock;
    std::mutex _base_compaction_lock;
    std::mutex _cumulative_compaction_lock;
    std::shared_timed_mutex _migration_lock;
    std::mutex _build_inverted_index_lock;

    // In unique key table with MoW, we should guarantee that only one
    // writer can update rowset and delete bitmap at the same time.
    // We use a separate lock rather than _meta_lock, to avoid blocking read queries
    // during publish_txn, which might take hundreds of milliseconds
    mutable std::mutex _rowset_update_lock;

    // if this tablet is broken, set to true. default is false
    std::atomic<bool> _is_bad;
    // timestamp of last cumu compaction failure
    std::atomic<int64_t> _last_cumu_compaction_failure_millis;
    // timestamp of last base compaction failure
    std::atomic<int64_t> _last_base_compaction_failure_millis;
    // timestamp of last full compaction failure
    std::atomic<int64_t> _last_full_compaction_failure_millis;
    // timestamp of last cumu compaction success
    std::atomic<int64_t> _last_cumu_compaction_success_millis;
    // timestamp of last base compaction success
    std::atomic<int64_t> _last_base_compaction_success_millis;
    // timestamp of last full compaction success
    std::atomic<int64_t> _last_full_compaction_success_millis;
    // timestamp of last base compaction schedule time
    std::atomic<int64_t> _last_base_compaction_schedule_millis;
    std::atomic<int64_t> _cumulative_point;
    std::atomic<int64_t> _cumulative_promotion_size;
    std::atomic<int32_t> _newly_created_rowset_num;
    std::atomic<int64_t> _last_checkpoint_time;
    std::string _last_base_compaction_status;

    // single replica compaction status
    std::string _last_single_compaction_failure_status;
    Version _last_fetched_version;

    // cumulative compaction policy
    std::shared_ptr<CumulativeCompactionPolicy> _cumulative_compaction_policy;
    std::string_view _cumulative_compaction_type;

    // use a seperate thread to check all tablets paths existance
    std::atomic<bool> _is_tablet_path_exists;

    int64_t _last_missed_version;
    int64_t _last_missed_time_s;

    bool _skip_cumu_compaction = false;
    int64_t _skip_cumu_compaction_ts;

    bool _skip_base_compaction = false;
    int64_t _skip_base_compaction_ts;

    // cooldown related
    CooldownConf _cooldown_conf;
    // `_cooldown_conf_lock` is used to serialize update cooldown conf and all operations that:
    // 1. read cooldown conf
    // 2. upload rowsets to remote storage
    // 3. update cooldown meta id
    mutable std::shared_mutex _cooldown_conf_lock;
    // `_cold_compaction_lock` is used to serialize cold data compaction and all operations that
    // may delete compaction input rowsets.
    std::mutex _cold_compaction_lock;
    int64_t _last_failed_follow_cooldown_time = 0;
    // `_alter_failed` is used to indicate whether the tablet failed to perform a schema change
    std::atomic<bool> _alter_failed = false;

    int64_t _io_error_times = 0;

    // partition's visible version. it sync from fe, but not real-time.
    std::shared_ptr<const VersionWithTime> _visible_version;

    std::atomic_bool _is_full_compaction_running = false;
};

inline CumulativeCompactionPolicy* Tablet::cumulative_compaction_policy() {
    return _cumulative_compaction_policy.get();
}

inline bool Tablet::init_succeeded() {
    return _init_once.has_called() && _init_once.stored_result().ok();
}

inline bool Tablet::is_used() {
    return !_is_bad && _data_dir->is_used();
}

inline void Tablet::register_tablet_into_dir() {
    _data_dir->register_tablet(this);
}

inline void Tablet::deregister_tablet_from_dir() {
    _data_dir->deregister_tablet(this);
}

inline int64_t Tablet::cumulative_layer_point() const {
    return _cumulative_point;
}

inline void Tablet::set_cumulative_layer_point(int64_t new_point) {
    // cumulative point should only be reset to -1, or be increased
    CHECK(new_point == Tablet::K_INVALID_CUMULATIVE_POINT || new_point >= _cumulative_point)
            << "Unexpected cumulative point: " << new_point
            << ", origin: " << _cumulative_point.load();
    _cumulative_point = new_point;
}

inline int64_t Tablet::cumulative_promotion_size() const {
    return _cumulative_promotion_size;
}

inline void Tablet::set_cumulative_promotion_size(int64_t new_size) {
    _cumulative_promotion_size = new_size;
}

// TODO(lingbin): Why other methods that need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t Tablet::tablet_footprint() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_footprint();
}

inline size_t Tablet::tablet_local_size() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_local_size();
}

inline size_t Tablet::tablet_remote_size() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->tablet_remote_size();
}

// TODO(lingbin): Why other methods which need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t Tablet::num_rows() {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->num_rows();
}

inline int Tablet::version_count() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->version_count();
}

inline int Tablet::stale_version_count() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->stale_version_count();
}

inline Version Tablet::max_version() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->max_version();
}

inline uint64_t Tablet::segment_count() const {
    std::shared_lock rdlock(_meta_lock);
    uint64_t segment_nums = 0;
    for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
        segment_nums += rs_meta->num_segments();
    }
    return segment_nums;
}

inline SortType Tablet::sort_type() const {
    return _tablet_meta->tablet_schema()->sort_type();
}

inline size_t Tablet::sort_col_num() const {
    return _tablet_meta->tablet_schema()->sort_col_num();
}

inline size_t Tablet::num_columns() const {
    return _tablet_meta->tablet_schema()->num_columns();
}

inline size_t Tablet::num_null_columns() const {
    return _tablet_meta->tablet_schema()->num_null_columns();
}

inline size_t Tablet::num_short_key_columns() const {
    return _tablet_meta->tablet_schema()->num_short_key_columns();
}

inline size_t Tablet::num_rows_per_row_block() const {
    return _tablet_meta->tablet_schema()->num_rows_per_row_block();
}

inline CompressKind Tablet::compress_kind() const {
    return _tablet_meta->tablet_schema()->compress_kind();
}

inline double Tablet::bloom_filter_fpp() const {
    return _tablet_meta->tablet_schema()->bloom_filter_fpp();
}

inline size_t Tablet::next_unique_id() const {
    return _tablet_meta->tablet_schema()->next_column_unique_id();
}

inline size_t Tablet::row_size() const {
    return _tablet_meta->tablet_schema()->row_size();
}

inline int64_t Tablet::avg_rs_meta_serialize_size() const {
    return _tablet_meta->avg_rs_meta_serialize_size();
}

} // namespace doris
