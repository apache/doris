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
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "olap/base_tablet.h"
#include "olap/binlog_config.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/version_graph.h"
#include "segment_loader.h"
#include "util/metrics.h"
#include "util/once.h"
#include "util/slice.h"

namespace doris {

class Tablet;
class CumulativeCompactionPolicy;
class CumulativeCompaction;
class BaseCompaction;
class FullCompaction;
class SingleReplicaCompaction;
class RowsetWriter;
struct TabletTxnInfo;
struct RowsetWriterContext;
class RowIdConversion;
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

using TabletSharedPtr = std::shared_ptr<Tablet>;

enum TabletStorageType { STORAGE_TYPE_LOCAL, STORAGE_TYPE_REMOTE, STORAGE_TYPE_REMOTE_AND_LOCAL };

extern const std::chrono::seconds TRACE_TABLET_LOCK_THRESHOLD;

class Tablet : public BaseTablet {
public:
    static TabletSharedPtr create_tablet_from_meta(TabletMetaSharedPtr tablet_meta,
                                                   DataDir* data_dir = nullptr);

    Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir,
           const std::string_view& cumulative_compaction_type = "");

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
    size_t tablet_footprint();
    // Local disk space occupied by tablet.
    size_t tablet_local_size();
    // Remote disk space occupied by tablet.
    size_t tablet_remote_size();

    size_t num_rows();
    int version_count() const;
    bool exceed_version_limit(int32_t limit) const;
    uint64_t segment_count() const;
    Version max_version() const;
    Version max_version_unlocked() const;
    CumulativeCompactionPolicy* cumulative_compaction_policy();
    bool enable_unique_key_merge_on_write() const;

    // properties encapsulated in TabletSchema
    KeysType keys_type() const;
    SortType sort_type() const;
    size_t sort_col_num() const;
    size_t num_columns() const;
    size_t num_null_columns() const;
    size_t num_key_columns() const;
    size_t num_short_key_columns() const;
    size_t num_rows_per_row_block() const;
    CompressKind compress_kind() const;
    double bloom_filter_fpp() const;
    size_t next_unique_id() const;
    size_t row_size() const;

    // operation in rowsets
    Status add_rowset(RowsetSharedPtr rowset);
    Status create_initial_rowset(const int64_t version);
    Status modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                          std::vector<RowsetSharedPtr>& to_delete, bool check_delete = false);

    // _rs_version_map and _stale_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this two function.
    const RowsetSharedPtr get_rowset_by_version(const Version& version,
                                                bool find_is_stale = false) const;
    const RowsetSharedPtr get_stale_rowset_by_version(const Version& version) const;

    const RowsetSharedPtr rowset_with_max_version() const;

    static RowsetMetaSharedPtr rowset_meta_with_max_schema_version(
            const std::vector<RowsetMetaSharedPtr>& rowset_metas);

    Status add_inc_rowset(const RowsetSharedPtr& rowset);
    /// Delete stale rowset by timing. This delete policy uses now() minutes
    /// config::tablet_rowset_expired_stale_sweep_time_sec to compute the deadline of expired rowset
    /// to delete.  When rowset is deleted, it will be added to StorageEngine unused map and record
    /// need to delete flag.
    void delete_expired_stale_rowset();

    // Given spec_version, find a continuous version path and store it in version_path.
    // If quiet is true, then only "does this path exist" is returned.
    Status capture_consistent_versions(const Version& spec_version,
                                       std::vector<Version>* version_path,
                                       bool quiet = false) const;
    // if quiet is true, no error log will be printed if there are missing versions
    Status check_version_integrity(const Version& version, bool quiet = false);
    bool check_version_exist(const Version& version) const;
    void acquire_version_and_rowsets(
            std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const;

    Status capture_consistent_rowsets(const Version& spec_version,
                                      std::vector<RowsetSharedPtr>* rowsets) const;
    Status capture_rs_readers(const Version& spec_version,
                              std::vector<RowSetSplits>* rs_splits) const;

    Status capture_rs_readers(const std::vector<Version>& version_path,
                              std::vector<RowSetSplits>* rs_splits) const;

    // meta lock
    std::shared_mutex& get_header_lock() { return _meta_lock; }
    std::mutex& get_rowset_update_lock() { return _rowset_update_lock; }
    std::mutex& get_push_lock() { return _ingest_lock; }
    std::mutex& get_base_compaction_lock() { return _base_compaction_lock; }
    std::mutex& get_cumulative_compaction_lock() { return _cumulative_compaction_lock; }
    std::mutex& get_full_compaction_lock() { return _full_compaction_lock; }

    std::shared_mutex& get_migration_lock() { return _migration_lock; }

    std::mutex& get_schema_change_lock() { return _schema_change_lock; }

    std::mutex& get_build_inverted_index_lock() { return _build_inverted_index_lock; }

    // operation for compaction
    bool can_do_compaction(size_t path_hash, CompactionType compaction_type);
    uint32_t calc_compaction_score(
            CompactionType compaction_type,
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy);

    // operation for clone
    void calc_missed_versions(int64_t spec_version, std::vector<Version>* missed_versions);
    void calc_missed_versions_unlocked(int64_t spec_version,
                                       std::vector<Version>* missed_versions) const;

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

    void delete_all_files();

    void check_tablet_path_exists();

    bool check_path(const std::string& check_path) const;
    bool check_rowset_id(const RowsetId& rowset_id);

    TabletInfo get_tablet_info() const;

    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_cumulative_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_base_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_full_compaction();
    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_build_inverted_index(
            const std::set<int32_t>& alter_index_uids, bool is_drop_op);

    std::vector<RowsetSharedPtr> pick_candidate_rowsets_to_single_replica_compaction();
    std::vector<Version> get_all_versions();

    void calculate_cumulative_point();
    // TODO(ygl):
    bool is_primary_replica() { return false; }

    // TODO(ygl):
    // eco mode means power saving in new energy car
    // eco mode also means save money in palo
    bool in_eco_mode() { return false; }

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

    void generate_tablet_meta_copy(TabletMetaSharedPtr new_tablet_meta) const;
    // caller should hold the _meta_lock before calling this method
    void generate_tablet_meta_copy_unlocked(TabletMetaSharedPtr new_tablet_meta) const;

    // return a json string to show the compaction status of this tablet
    void get_compaction_status(std::string* json_result);

    Status prepare_compaction_and_calculate_permits(CompactionType compaction_type,
                                                    TabletSharedPtr tablet, int64_t* permits);

    Status prepare_single_replica_compaction(TabletSharedPtr tablet,
                                             CompactionType compaction_type);
    void execute_compaction(CompactionType compaction_type);
    void reset_compaction(CompactionType compaction_type);
    void execute_single_replica_compaction(CompactionType compaction_type);
    void reset_single_replica_compaction();

    void set_clone_occurred(bool clone_occurred) { _is_clone_occurred = clone_occurred; }
    bool get_clone_occurred() { return _is_clone_occurred; }

    void set_cumulative_compaction_policy(
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }

    std::shared_ptr<CumulativeCompactionPolicy> get_cumulative_compaction_policy() {
        return _cumulative_compaction_policy;
    }

    inline bool all_beta() const {
        std::shared_lock rdlock(_meta_lock);
        return _tablet_meta->all_beta();
    }

    TabletSchemaSPtr tablet_schema() const override;

    const TabletSchemaSPtr& tablet_schema_unlocked() const { return _max_version_schema; }

    // Find the related rowset with specified version and return its tablet schema
    TabletSchemaSPtr tablet_schema(Version version) const {
        return _tablet_meta->tablet_schema(version);
    }

    Status create_rowset_writer(RowsetWriterContext& context,
                                std::unique_ptr<RowsetWriter>* rowset_writer);

    Status create_transient_rowset_writer(RowsetSharedPtr rowset_ptr,
                                          std::unique_ptr<RowsetWriter>* rowset_writer);
    Status create_transient_rowset_writer(RowsetWriterContext& context, const RowsetId& rowset_id,
                                          std::unique_ptr<RowsetWriter>* rowset_writer);

    Status create_vertical_rowset_writer(RowsetWriterContext& context,
                                         std::unique_ptr<RowsetWriter>* rowset_writer);

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
    int64_t last_failed_follow_cooldown_time() const { return _last_failed_follow_cooldown_time; }

    // Cooldown to remote fs.
    Status cooldown();

    RowsetSharedPtr pick_cooldown_rowset();

    bool need_cooldown(int64_t* cooldown_timestamp, size_t* file_size);

    std::pair<int64_t, int64_t> cooldown_conf() const {
        std::shared_lock rlock(_cooldown_conf_lock);
        return {_cooldown_replica_id, _cooldown_term};
    }

    std::pair<int64_t, int64_t> cooldown_conf_unlocked() const {
        return {_cooldown_replica_id, _cooldown_term};
    }

    // Return `true` if update success
    bool update_cooldown_conf(int64_t cooldown_term, int64_t cooldown_replica_id);

    Status remove_all_remote_rowsets();

    void record_unused_remote_rowset(const RowsetId& rowset_id, const std::string& resource,
                                     int64_t num_segments);

    static void remove_unused_remote_files();

    // If a rowset is to be written to remote filesystem, MUST add it to `pending_remote_rowsets` before uploading,
    // and then erase it from `pending_remote_rowsets` after it has been insert to the Tablet.
    // `remove_unused_remote_files` MUST NOT delete files of these pending rowsets.
    static void add_pending_remote_rowset(std::string rowset_id);
    static void erase_pending_remote_rowset(const std::string& rowset_id);

    uint32_t calc_cold_data_compaction_score() const;

    std::mutex& get_cold_compaction_lock() { return _cold_compaction_lock; }

    std::shared_mutex& get_cooldown_conf_lock() { return _cooldown_conf_lock; }

    static void async_write_cooldown_meta(TabletSharedPtr tablet);
    // Return `ABORTED` if should not to retry again
    Status write_cooldown_meta();
    ////////////////////////////////////////////////////////////////////////////
    // end cooldown functions
    ////////////////////////////////////////////////////////////////////////////

    // Lookup the row location of `encoded_key`, the function sets `row_location` on success.
    // NOTE: the method only works in unique key model with primary key index, you will got a
    //       not supported error in other data model.
    Status lookup_row_key(const Slice& encoded_key, bool with_seq_col,
                          const std::vector<RowsetSharedPtr>& specified_rowsets,
                          RowLocation* row_location, uint32_t version,
                          std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                          RowsetSharedPtr* rowset = nullptr);

    // Lookup a row with TupleDescriptor and fill Block
    Status lookup_row_data(const Slice& encoded_key, const RowLocation& row_location,
                           RowsetSharedPtr rowset, const TupleDescriptor* desc,
                           OlapReaderStatistics& stats, std::string& values,
                           bool write_to_cache = false);

    Status fetch_value_by_rowids(RowsetSharedPtr input_rowset, uint32_t segid,
                                 const std::vector<uint32_t>& rowids,
                                 const TabletColumn& tablet_column,
                                 vectorized::MutableColumnPtr& dst);

    Status fetch_value_through_row_column(RowsetSharedPtr input_rowset, uint32_t segid,
                                          const std::vector<uint32_t>& rowids,
                                          const std::vector<uint32_t>& cids,
                                          vectorized::Block& block);

    // calc delete bitmap when flush memtable, use a fake version to calc
    // For example, cur max version is 5, and we use version 6 to calc but
    // finally this rowset publish version with 8, we should make up data
    // for rowset 6-7. Also, if a compaction happens between commit_txn and
    // publish_txn, we should remove compaction input rowsets' delete_bitmap
    // and build newly generated rowset's delete_bitmap
    Status calc_delete_bitmap(RowsetSharedPtr rowset,
                              const std::vector<segment_v2::SegmentSharedPtr>& segments,
                              const std::vector<RowsetSharedPtr>& specified_rowsets,
                              DeleteBitmapPtr delete_bitmap, int64_t version,
                              CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer = nullptr);

    std::vector<RowsetSharedPtr> get_rowset_by_ids(
            const RowsetIdUnorderedSet* specified_rowset_ids);

    Status calc_segment_delete_bitmap(RowsetSharedPtr rowset,
                                      const segment_v2::SegmentSharedPtr& seg,
                                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                                      DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                      RowsetWriter* rowset_writer);

    Status calc_delete_bitmap_between_segments(
            RowsetSharedPtr rowset, const std::vector<segment_v2::SegmentSharedPtr>& segments,
            DeleteBitmapPtr delete_bitmap);
    Status read_columns_by_plan(TabletSchemaSPtr tablet_schema,
                                const std::vector<uint32_t> cids_to_read,
                                const PartialUpdateReadPlan& read_plan,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                vectorized::Block& block, std::map<uint32_t, uint32_t>* read_index);
    void prepare_to_read(const RowLocation& row_location, size_t pos,
                         PartialUpdateReadPlan* read_plan);
    Status generate_new_block_for_partial_update(
            TabletSchemaSPtr rowset_schema, const PartialUpdateReadPlan& read_plan_ori,
            const PartialUpdateReadPlan& read_plan_update,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
            vectorized::Block* output_block);

    Status update_delete_bitmap_without_lock(const RowsetSharedPtr& rowset);

    Status commit_phase_update_delete_bitmap(
            const RowsetSharedPtr& rowset, RowsetIdUnorderedSet& pre_rowset_ids,
            DeleteBitmapPtr delete_bitmap,
            const std::vector<segment_v2::SegmentSharedPtr>& segments, int64_t txn_id,
            CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer = nullptr);

    Status update_delete_bitmap(const RowsetSharedPtr& rowset,
                                const RowsetIdUnorderedSet& pre_rowset_ids,
                                DeleteBitmapPtr delete_bitmap, int64_t txn_id,
                                RowsetWriter* rowset_writer = nullptr);
    void calc_compaction_output_rowset_delete_bitmap(
            const std::vector<RowsetSharedPtr>& input_rowsets,
            const RowIdConversion& rowid_conversion, uint64_t start_version, uint64_t end_version,
            std::set<RowLocation>* missed_rows,
            std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>* location_map,
            const DeleteBitmap& input_delete_bitmap, DeleteBitmap* output_rowset_delete_bitmap);
    void merge_delete_bitmap(const DeleteBitmap& delete_bitmap);
    Status check_rowid_conversion(
            RowsetSharedPtr dst_rowset,
            const std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>&
                    location_map);
    RowsetIdUnorderedSet all_rs_id(int64_t max_version) const;
    void sort_block(vectorized::Block& in_block, vectorized::Block& output_block);

    bool check_all_rowset_segment();

    void update_max_version_schema(const TabletSchemaSPtr& tablet_schema);

    void set_skip_compaction(bool skip,
                             CompactionType compaction_type = CompactionType::CUMULATIVE_COMPACTION,
                             int64_t start = -1);
    bool should_skip_compaction(CompactionType compaction_type, int64_t now);

    RowsetSharedPtr get_rowset(const RowsetId& rowset_id);

    void traverse_rowsets(std::function<void(const RowsetSharedPtr&)> visitor) {
        std::shared_lock rlock(_meta_lock);
        for (auto& [v, rs] : _rs_version_map) {
            visitor(rs);
        }
    }

    std::vector<std::string> get_binlog_filepath(std::string_view binlog_version) const;
    std::pair<std::string, int64_t> get_binlog_info(std::string_view binlog_version) const;
    std::string get_rowset_binlog_meta(std::string_view binlog_version,
                                       std::string_view rowset_id) const;
    Status get_rowset_binlog_metas(const std::vector<int64_t>& binlog_versions,
                                   RowsetBinlogMetasPB* metas_pb);
    std::string get_segment_filepath(std::string_view rowset_id,
                                     std::string_view segment_index) const;
    std::string get_segment_filepath(std::string_view rowset_id, int64_t segment_index) const;
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

    // binlog releated functions
    bool is_enable_binlog();
    bool is_binlog_enabled() { return _tablet_meta->binlog_config().is_enable(); }
    int64_t binlog_ttl_ms() const { return _tablet_meta->binlog_config().ttl_seconds(); }
    int64_t binlog_max_bytes() const { return _tablet_meta->binlog_config().max_bytes(); }

    void set_binlog_config(BinlogConfig binlog_config);
    void add_sentinel_mark_to_delete_bitmap(DeleteBitmap* delete_bitmap,
                                            const RowsetIdUnorderedSet& rowsetids);
    Status check_delete_bitmap_correctness(DeleteBitmapPtr delete_bitmap, int64_t max_version,
                                           int64_t txn_id, const RowsetIdUnorderedSet& rowset_ids,
                                           std::vector<RowsetSharedPtr>* rowsets = nullptr);
    Status _get_segment_column_iterator(
            const BetaRowsetSharedPtr& rowset, uint32_t segid, const TabletColumn& target_column,
            std::unique_ptr<segment_v2::ColumnIterator>* column_iterator,
            OlapReaderStatistics* stats);

private:
    Status _init_once_action();
    void _print_missed_versions(const std::vector<Version>& missed_versions) const;
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
    Status _capture_consistent_rowsets_unlocked(const std::vector<Version>& version_path,
                                                std::vector<RowsetSharedPtr>* rowsets) const;

    uint32_t _calc_cumulative_compaction_score(
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy);
    uint32_t _calc_base_compaction_score() const;

    // When the proportion of empty edges in the adjacency matrix used to represent the version graph
    // in the version tracker is greater than the threshold, rebuild the version tracker
    bool _reconstruct_version_tracker_if_necessary();
    void _init_context_common_fields(RowsetWriterContext& context);

    void _rowset_ids_difference(const RowsetIdUnorderedSet& cur, const RowsetIdUnorderedSet& pre,
                                RowsetIdUnorderedSet* to_add, RowsetIdUnorderedSet* to_del);
    Status _load_rowset_segments(const RowsetSharedPtr& rowset,
                                 std::vector<segment_v2::SegmentSharedPtr>* segments);

    ////////////////////////////////////////////////////////////////////////////
    // begin cooldown functions
    ////////////////////////////////////////////////////////////////////////////
    Status _cooldown_data();
    Status _follow_cooldowned_data();
    Status _read_cooldown_meta(const std::shared_ptr<io::RemoteFileSystem>& fs,
                               TabletMetaPB* tablet_meta_pb);
    ////////////////////////////////////////////////////////////////////////////
    // end cooldown functions
    ////////////////////////////////////////////////////////////////////////////

    void _remove_sentinel_mark_from_delete_bitmap(DeleteBitmapPtr delete_bitmap);
    std::string _get_rowset_info_str(RowsetSharedPtr rowset, bool delete_flag);

public:
    static const int64_t K_INVALID_CUMULATIVE_POINT = -1;

private:
    TimestampedVersionTracker _timestamped_version_tracker;

    DorisCallOnce<Status> _init_once;
    // meta store lock is used for prevent 2 threads do checkpoint concurrently
    // it will be used in econ-mode in the future
    std::shared_mutex _meta_store_lock;
    std::mutex _ingest_lock;
    std::mutex _base_compaction_lock;
    std::mutex _cumulative_compaction_lock;
    std::mutex _full_compaction_lock;
    std::mutex _schema_change_lock;
    std::shared_mutex _migration_lock;
    std::mutex _build_inverted_index_lock;

    // TODO(lingbin): There is a _meta_lock TabletMeta too, there should be a comment to
    // explain how these two locks work together.
    mutable std::shared_mutex _meta_lock;

    // In unique key table with MoW, we should guarantee that only one
    // writer can update rowset and delete bitmap at the same time.
    // We use a separate lock rather than _meta_lock, to avoid blocking read queries
    // during publish_txn, which might take hundreds of milliseconds
    mutable std::mutex _rowset_update_lock;

    // After version 0.13, all newly created rowsets are saved in _rs_version_map.
    // And if rowset being compacted, the old rowsetis will be saved in _stale_rs_version_map;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;
    // This variable _stale_rs_version_map is used to record these rowsets which are be compacted.
    // These _stale rowsets are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _stale_rs_version_map;
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
    std::atomic<int64_t> _cumulative_point;
    std::atomic<int64_t> _cumulative_promotion_size;
    std::atomic<int32_t> _newly_created_rowset_num;
    std::atomic<int64_t> _last_checkpoint_time;

    // cumulative compaction policy
    std::shared_ptr<CumulativeCompactionPolicy> _cumulative_compaction_policy;
    std::string_view _cumulative_compaction_type;

    std::shared_ptr<CumulativeCompaction> _cumulative_compaction;
    std::shared_ptr<BaseCompaction> _base_compaction;
    std::shared_ptr<FullCompaction> _full_compaction;
    std::shared_ptr<SingleReplicaCompaction> _single_replica_compaction;

    // whether clone task occurred during the tablet is in thread pool queue to wait for compaction
    std::atomic<bool> _is_clone_occurred;

    // use a seperate thread to check all tablets paths existance
    std::atomic<bool> _is_tablet_path_exists;

    int64_t _last_missed_version;
    int64_t _last_missed_time_s;

    // Max schema_version schema from Rowset or FE
    TabletSchemaSPtr _max_version_schema;

    bool _skip_cumu_compaction = false;
    int64_t _skip_cumu_compaction_ts;

    bool _skip_base_compaction = false;
    int64_t _skip_base_compaction_ts;

    // cooldown related
    int64_t _cooldown_replica_id = -1;
    int64_t _cooldown_term = -1;
    // `_cooldown_conf_lock` is used to serialize update cooldown conf and all operations that:
    // 1. read cooldown conf
    // 2. upload rowsets to remote storage
    // 3. update cooldown meta id
    mutable std::shared_mutex _cooldown_conf_lock;
    // `_cold_compaction_lock` is used to serialize cold data compaction and all operations that
    // may delete compaction input rowsets.
    std::mutex _cold_compaction_lock;
    int64_t _last_failed_follow_cooldown_time = 0;

    DISALLOW_COPY_AND_ASSIGN(Tablet);

    int64_t _io_error_times = 0;

public:
    IntCounter* flush_bytes;
    IntCounter* flush_finish_count;
    std::atomic<int64_t> publised_count = 0;
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

inline bool Tablet::enable_unique_key_merge_on_write() const {
#ifdef BE_TEST
    if (_tablet_meta == nullptr) {
        return false;
    }
#endif
    return _tablet_meta->enable_unique_key_merge_on_write();
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

inline Version Tablet::max_version() const {
    std::shared_lock rdlock(_meta_lock);
    return _tablet_meta->max_version();
}

inline uint64_t Tablet::segment_count() const {
    std::shared_lock rdlock(_meta_lock);
    uint64_t segment_nums = 0;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        segment_nums += rs_meta->num_segments();
    }
    return segment_nums;
}

inline Version Tablet::max_version_unlocked() const {
    return _tablet_meta->max_version();
}

inline KeysType Tablet::keys_type() const {
    return _schema->keys_type();
}

inline SortType Tablet::sort_type() const {
    return _schema->sort_type();
}

inline size_t Tablet::sort_col_num() const {
    return _schema->sort_col_num();
}

inline size_t Tablet::num_columns() const {
    return _schema->num_columns();
}

inline size_t Tablet::num_null_columns() const {
    return _schema->num_null_columns();
}

inline size_t Tablet::num_key_columns() const {
    return _schema->num_key_columns();
}

inline size_t Tablet::num_short_key_columns() const {
    return _schema->num_short_key_columns();
}

inline size_t Tablet::num_rows_per_row_block() const {
    return _schema->num_rows_per_row_block();
}

inline CompressKind Tablet::compress_kind() const {
    return _schema->compress_kind();
}

inline double Tablet::bloom_filter_fpp() const {
    return _schema->bloom_filter_fpp();
}

inline size_t Tablet::next_unique_id() const {
    return _schema->next_column_unique_id();
}

inline size_t Tablet::row_size() const {
    return _schema->row_size();
}

} // namespace doris
