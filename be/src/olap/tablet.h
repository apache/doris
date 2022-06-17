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

#ifndef DORIS_BE_SRC_OLAP_TABLET_H
#define DORIS_BE_SRC_OLAP_TABLET_H
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/base_tablet.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet_meta.h"
#include "olap/tuple.h"
#include "olap/utils.h"
#include "olap/version_graph.h"
#include "util/once.h"

namespace doris {

class DataDir;
class Tablet;
class TabletMeta;
class CumulativeCompactionPolicy;
class CumulativeCompaction;
class BaseCompaction;

using TabletSharedPtr = std::shared_ptr<Tablet>;

class Tablet : public BaseTablet {
public:
    static TabletSharedPtr create_tablet_from_meta(TabletMetaSharedPtr tablet_meta,
                                                   DataDir* data_dir = nullptr);

    Tablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir,
           const std::string& cumulative_compaction_type = "");

    OLAPStatus init();
    inline bool init_succeeded();

    bool is_used();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();

    void save_meta();
    // Used in clone task, to update local meta when finishing a clone job
    OLAPStatus revise_tablet_meta(const std::vector<RowsetMetaSharedPtr>& rowsets_to_clone,
                              const std::vector<Version>& versions_to_delete);
    OLAPStatus pick_quick_compaction_rowsets(std::vector<RowsetSharedPtr>* input_rowsets,
                                         int64_t* permits);

    inline const int64_t cumulative_layer_point() const;
    inline void set_cumulative_layer_point(int64_t new_point);

    inline size_t tablet_footprint(); // disk space occupied by tablet
    inline size_t num_rows();
    inline int version_count() const;
    inline Version max_version() const;
    inline CumulativeCompactionPolicy* cumulative_compaction_policy();

    // properties encapsulated in TabletSchema
    inline KeysType keys_type() const;
    inline SortType sort_type() const;
    inline size_t sort_col_num() const;
    inline size_t num_columns() const;
    inline size_t num_null_columns() const;
    inline size_t num_key_columns() const;
    inline size_t num_short_key_columns() const;
    inline size_t num_rows_per_row_block() const;
    inline CompressKind compress_kind() const;
    inline double bloom_filter_fpp() const;
    inline size_t next_unique_id() const;
    inline size_t row_size() const;
    inline int32_t field_index(const std::string& field_name) const;

    // operation in rowsets
    OLAPStatus add_rowset(RowsetSharedPtr rowset, bool need_persist = true);
    OLAPStatus modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                        std::vector<RowsetSharedPtr>& to_delete, bool check_delete = false);

    // _rs_version_map and _stale_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this two function.
    const RowsetSharedPtr get_rowset_by_version(const Version& version,
                                                bool find_is_stale = false) const;
    const RowsetSharedPtr get_stale_rowset_by_version(const Version& version) const;

    const RowsetSharedPtr rowset_with_max_version() const;

    OLAPStatus add_inc_rowset(const RowsetSharedPtr& rowset);
    /// Delete stale rowset by timing. This delete policy uses now() minutes
    /// config::tablet_rowset_expired_stale_sweep_time_sec to compute the deadline of expired rowset
    /// to delete.  When rowset is deleted, it will be added to StorageEngine unused map and record
    /// need to delete flag.
    void delete_expired_stale_rowset();

    // Given spec_version, find a continuous version path and store it in version_path.
    // If quiet is true, then only "does this path exist" is returned.
    OLAPStatus capture_consistent_versions(const Version& spec_version,
                                           std::vector<Version>* version_path,
                                           bool quiet = false) const;
    // if quiet is true, no error log will be printed if there are missing versions
    OLAPStatus check_version_integrity(const Version& version, bool quiet = false);
    bool check_version_exist(const Version& version) const;
    void acquire_version_and_rowsets(
            std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const;

    OLAPStatus capture_consistent_rowsets(const Version& spec_version,
                                          std::vector<RowsetSharedPtr>* rowsets) const;
    OLAPStatus capture_rs_readers(const Version& spec_version,
                                  std::vector<RowsetReaderSharedPtr>* rs_readers,
                                  std::shared_ptr<MemTracker> parent_tracker = nullptr) const;

    OLAPStatus capture_rs_readers(const std::vector<Version>& version_path,
                                  std::vector<RowsetReaderSharedPtr>* rs_readers,
                                  std::shared_ptr<MemTracker> parent_tracker = nullptr) const;

    DelPredicateArray delete_predicates() { return _tablet_meta->delete_predicates(); }
    void add_delete_predicate(const DeletePredicatePB& delete_predicate, int64_t version);
    bool version_for_delete_predicate(const Version& version);
    bool version_for_load_deletion(const Version& version);

    // meta lock
    inline std::shared_mutex& get_header_lock() { return _meta_lock; }

    // ingest lock
    inline void obtain_push_lock() { _ingest_lock.lock(); }
    inline void release_push_lock() { _ingest_lock.unlock(); }
    inline Mutex* get_push_lock() { return &_ingest_lock; }

    // base lock
    inline void obtain_base_compaction_lock() { _base_lock.lock(); }
    inline void release_base_compaction_lock() { _base_lock.unlock(); }
    inline Mutex* get_base_lock() { return &_base_lock; }

    // cumulative lock
    inline void obtain_cumulative_lock() { _cumulative_lock.lock(); }
    inline void release_cumulative_lock() { _cumulative_lock.unlock(); }
    inline Mutex* get_cumulative_lock() { return &_cumulative_lock; }

    inline std::shared_mutex& get_migration_lock() { return _migration_lock; }

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

    // operation for query
    OLAPStatus split_range(const OlapTuple& start_key_strings, const OlapTuple& end_key_strings,
                           uint64_t request_block_row_count, std::vector<OlapTuple>* ranges);

    void set_bad(bool is_bad) { _is_bad = is_bad; }

    int64_t last_cumu_compaction_failure_time() { return _last_cumu_compaction_failure_millis; }
    void set_last_cumu_compaction_failure_time(int64_t millis) {
        _last_cumu_compaction_failure_millis = millis;
    }

    int64_t last_base_compaction_failure_time() { return _last_base_compaction_failure_millis; }
    void set_last_base_compaction_failure_time(int64_t millis) {
        _last_base_compaction_failure_millis = millis;
    }

    int64_t last_cumu_compaction_success_time() { return _last_cumu_compaction_success_millis; }
    void set_last_cumu_compaction_success_time(int64_t millis) {
        _last_cumu_compaction_success_millis = millis;
    }

    void set_last_quick_compaction_success_time(int64_t millis) {
        _last_quick_compaction_success_time_millis = millis;
    }

    int64_t last_base_compaction_success_time() { return _last_base_compaction_success_millis; }
    void set_last_base_compaction_success_time(int64_t millis) {
        _last_base_compaction_success_millis = millis;
    }

    void delete_all_files();

    bool check_path(const std::string& check_path) const;
    bool check_rowset_id(const RowsetId& rowset_id);

    OLAPStatus set_partition_id(int64_t partition_id);

    TabletInfo get_tablet_info() const;

    void pick_candidate_rowsets_to_cumulative_compaction(
            std::vector<RowsetSharedPtr>* candidate_rowsets);
    void pick_candidate_rowsets_to_base_compaction(std::vector<RowsetSharedPtr>* candidate_rowsets);

    void calculate_cumulative_point();
    // TODO(ygl):
    inline bool is_primary_replica() { return false; }

    // TODO(ygl):
    // eco mode means power saving in new energy car
    // eco mode also means save money in palo
    inline bool in_eco_mode() { return false; }

    // return true if the checkpoint is actually done
    bool do_tablet_meta_checkpoint();

    // Check whether the rowset is useful or not, unuseful rowset can be swept up then.
    // Rowset which is under tablet's management is useful, i.e. rowset is in
    // _rs_version_map, or _stale_rs_version_map.
    // Rowset whose version range is not covered by this tablet is also useful.
    bool rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta);

    void build_tablet_report_info(TTabletInfo* tablet_info,
                                  bool enable_consecutive_missing_check = false);

    void generate_tablet_meta_copy(TabletMetaSharedPtr new_tablet_meta) const;
    // caller should hold the _meta_lock before calling this method
    void generate_tablet_meta_copy_unlocked(TabletMetaSharedPtr new_tablet_meta) const;

    // return a json string to show the compaction status of this tablet
    void get_compaction_status(std::string* json_result);

    double calculate_scan_frequency();

    Status prepare_compaction_and_calculate_permits(CompactionType compaction_type,
                                                    TabletSharedPtr tablet, int64_t* permits);
    void execute_compaction(CompactionType compaction_type);
    void reset_compaction(CompactionType compaction_type);

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
        ReadLock rdlock(_meta_lock);
        return _tablet_meta->all_beta();
    }

    void find_alpha_rowsets(std::vector<RowsetSharedPtr>* rowsets) const;
private:
    OLAPStatus _init_once_action();
    void _print_missed_versions(const std::vector<Version>& missed_versions) const;
    bool _contains_rowset(const RowsetId rowset_id);
    OLAPStatus _contains_version(const Version& version);

    // Returns:
    // version: the max continuous version from beginning
    // max_version: the max version of this tablet
    void _max_continuous_version_from_beginning_unlocked(Version* version, Version* max_version,
                                                         bool* has_version_cross) const;
    RowsetSharedPtr _rowset_with_largest_size();
    /// Delete stale rowset by version. This method not only delete the version in expired rowset map,
    /// but also delete the version in rowset meta vector.
    void _delete_stale_rowset_by_version(const Version& version);
    OLAPStatus _capture_consistent_rowsets_unlocked(const std::vector<Version>& version_path,
                                                    std::vector<RowsetSharedPtr>* rowsets) const;

    const uint32_t _calc_cumulative_compaction_score(
            std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy);
    const uint32_t _calc_base_compaction_score() const;

    // When the proportion of empty edges in the adjacency matrix used to represent the version graph
    // in the version tracker is greater than the threshold, rebuild the version tracker
    bool _reconstruct_version_tracker_if_necessary();

public:
    static const int64_t K_INVALID_CUMULATIVE_POINT = -1;

private:
    TimestampedVersionTracker _timestamped_version_tracker;

    DorisCallOnce<OLAPStatus> _init_once;
    // meta store lock is used for prevent 2 threads do checkpoint concurrently
    // it will be used in econ-mode in the future
    std::shared_mutex _meta_store_lock;
    Mutex _ingest_lock;
    Mutex _base_lock;
    Mutex _cumulative_lock;
    std::shared_mutex _migration_lock;

    // TODO(lingbin): There is a _meta_lock TabletMeta too, there should be a comment to
    // explain how these two locks work together.
    mutable std::shared_mutex _meta_lock;
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
    // timestamp of last cumu compaction success
    std::atomic<int64_t> _last_cumu_compaction_success_millis;
    // timestamp of last base compaction success
    std::atomic<int64_t> _last_base_compaction_success_millis;
    std::atomic<int64_t> _last_quick_compaction_success_time_millis;
    std::atomic<int64_t> _cumulative_point;
    std::atomic<int32_t> _newly_created_rowset_num;
    std::atomic<int64_t> _last_checkpoint_time;

    // cumulative compaction policy
    std::shared_ptr<CumulativeCompactionPolicy> _cumulative_compaction_policy;
    std::string _cumulative_compaction_type;

    // the value of metric 'query_scan_count' and timestamp will be recorded when every time
    // 'config::tablet_scan_frequency_time_node_interval_second' passed to calculate tablet
    // scan frequency.
    // the value of metric 'query_scan_count' for the last record.
    int64_t _last_record_scan_count;
    // the timestamp of the last record.
    time_t _last_record_scan_count_timestamp;

    std::shared_ptr<CumulativeCompaction> _cumulative_compaction;
    std::shared_ptr<BaseCompaction> _base_compaction;
    // whether clone task occurred during the tablet is in thread pool queue to wait for compaction
    std::atomic<bool> _is_clone_occurred;

    int64_t _last_missed_version;
    int64_t _last_missed_time_s;

    DISALLOW_COPY_AND_ASSIGN(Tablet);

public:
    IntCounter* flush_bytes;
    IntCounter* flush_count;
    std::atomic<int64_t> publised_count = 0;
};

inline CumulativeCompactionPolicy* Tablet::cumulative_compaction_policy() {
    return _cumulative_compaction_policy.get();
}

inline bool Tablet::init_succeeded() {
    return _init_once.has_called() && _init_once.stored_result() == OLAP_SUCCESS;
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

inline const int64_t Tablet::cumulative_layer_point() const {
    return _cumulative_point;
}

inline void Tablet::set_cumulative_layer_point(int64_t new_point) {
    // cumulative point should only be reset to -1, or be increased
    CHECK(new_point == Tablet::K_INVALID_CUMULATIVE_POINT || new_point >= _cumulative_point)
            << "Unexpected cumulative point: " << new_point
            << ", origin: " << _cumulative_point.load();
    _cumulative_point = new_point;
}

// TODO(lingbin): Why other methods that need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t Tablet::tablet_footprint() {
    ReadLock rdlock(_meta_lock);
    return _tablet_meta->tablet_footprint();
}

// TODO(lingbin): Why other methods which need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t Tablet::num_rows() {
    ReadLock rdlock(_meta_lock);
    return _tablet_meta->num_rows();
}

inline int Tablet::version_count() const {
    return _tablet_meta->version_count();
}

inline Version Tablet::max_version() const {
    return _tablet_meta->max_version();
}

inline KeysType Tablet::keys_type() const {
    return _schema.keys_type();
}

inline SortType Tablet::sort_type() const {
    return _schema.sort_type();
}

inline size_t Tablet::sort_col_num() const {
    return _schema.sort_col_num();
}

inline size_t Tablet::num_columns() const {
    return _schema.num_columns();
}

inline size_t Tablet::num_null_columns() const {
    return _schema.num_null_columns();
}

inline size_t Tablet::num_key_columns() const {
    return _schema.num_key_columns();
}

inline size_t Tablet::num_short_key_columns() const {
    return _schema.num_short_key_columns();
}

inline size_t Tablet::num_rows_per_row_block() const {
    return _schema.num_rows_per_row_block();
}

inline CompressKind Tablet::compress_kind() const {
    return _schema.compress_kind();
}

inline double Tablet::bloom_filter_fpp() const {
    return _schema.bloom_filter_fpp();
}

inline size_t Tablet::next_unique_id() const {
    return _schema.next_column_unique_id();
}

inline int32_t Tablet::field_index(const std::string& field_name) const {
    return _schema.field_index(field_name);
}

inline size_t Tablet::row_size() const {
    return _schema.row_size();
}

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TABLET_H
