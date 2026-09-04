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

#include <gen_cpp/Types_types.h>
#include <netinet/in.h>

#include <atomic>
#include <charconv>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "core/extended_types.h"
#include "io/io_common.h"
#include "storage/field_type.h"
#include "storage/index/inverted/inverted_index_stats.h"
#include "storage/index/snii/snii_query_stats.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/rowset_id.h"
#include "util/hash_util.hpp"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
using SchemaHash = int32_t;

using TabletUid = UniqueId;

enum CompactionType {
    BASE_COMPACTION = 1,
    CUMULATIVE_COMPACTION = 2,
    FULL_COMPACTION = 3,
    // Only used by scheduler to route row-binlog tablets to the binlog thread pool.
    CUMU_BINLOG_COMPACTION = 4
};

struct CompactionScoreStats {
    int64_t max_score = 0;
    int64_t size_based_max_score = 0;
    int64_t time_series_max_score = 0;
    bool scanned = false;
};

enum DataDirType {
    SPILL_DISK_DIR,
    OLAP_DATA_DIR,
    DATA_CACHE_DIR,
};

struct DataDirInfo {
    std::string path;
    size_t path_hash = 0;
    int64_t disk_capacity = 1; // actual disk capacity
    int64_t available = 0;     // available space, in bytes unit
    int64_t local_used_capacity = 0;
    int64_t remote_used_capacity = 0;
    int64_t trash_used_capacity = 0;
    bool is_used = false;                                      // whether available mark
    TStorageMedium::type storage_medium = TStorageMedium::HDD; // Storage medium type: SSD|HDD
    DataDirType data_dir_type = DataDirType::OLAP_DATA_DIR;
    std::string metric_name;
};

// Sort DataDirInfo by available space.
struct DataDirInfoLessAvailability {
    bool operator()(const DataDirInfo& left, const DataDirInfo& right) const {
        return left.available < right.available;
    }
};

struct TabletInfo {
    TabletInfo(TTabletId in_tablet_id, UniqueId in_uid)
            : tablet_id(in_tablet_id), tablet_uid(in_uid) {}

    bool operator<(const TabletInfo& right) const {
        if (tablet_id != right.tablet_id) {
            return tablet_id < right.tablet_id;
        } else {
            return tablet_uid < right.tablet_uid;
        }
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "." << tablet_uid.to_string();
        return ss.str();
    }

    TTabletId tablet_id;
    UniqueId tablet_uid;
};

struct TabletSize {
    TabletSize(TTabletId in_tablet_id, size_t in_tablet_size)
            : tablet_id(in_tablet_id), tablet_size(in_tablet_size) {}

    TTabletId tablet_id;
    size_t tablet_size;
};

// FieldType moved to storage/field_type.h (included above) so that
// data-type headers can name storage cell types without pulling in the
// whole of olap_common.h.

// Define all aggregation methods supported by TabletColumn
// Note that in practice, not all types can use all the following aggregation methods
// For example, it is meaningless to use SUM for the string type (but it will not cause the program to crash)
// The implementation of the TabletColumn class does not perform such checks, and should be constrained when creating the table
enum class FieldAggregationMethod {
    OLAP_FIELD_AGGREGATION_NONE = 0,
    OLAP_FIELD_AGGREGATION_SUM = 1,
    OLAP_FIELD_AGGREGATION_MIN = 2,
    OLAP_FIELD_AGGREGATION_MAX = 3,
    OLAP_FIELD_AGGREGATION_REPLACE = 4,
    OLAP_FIELD_AGGREGATION_HLL_UNION = 5,
    OLAP_FIELD_AGGREGATION_UNKNOWN = 6,
    OLAP_FIELD_AGGREGATION_BITMAP_UNION = 7,
    // Replace if and only if added value is not null
    OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL = 8,
    OLAP_FIELD_AGGREGATION_QUANTILE_UNION = 9,
    OLAP_FIELD_AGGREGATION_GENERIC = 10
};

enum class PushType {
    PUSH_NORMAL = 1,          // for broker/hadoop load, not used any more
    PUSH_FOR_DELETE = 2,      // for delete
    PUSH_FOR_LOAD_DELETE = 3, // not used any more
    PUSH_NORMAL_V2 = 4,       // for spark load
};

// <start_version_id, end_version_id>, such as <100, 110>
//using Version = std::pair<TupleVersion, TupleVersion>;

struct Version {
    int64_t first;
    int64_t second;

    Version(int64_t first_, int64_t second_) : first(first_), second(second_) {}
    Version() : first(0), second(0) {}

    static Version mock() {
        // Every time SchemaChange is used for external rowing, some temporary versions (such as 999, 1000, 1001) will be written, in order to avoid Cache conflicts, temporary
        // The version number takes a BIG NUMBER plus the version number of the current SchemaChange
        return Version(1 << 28, 1 << 29);
    }

    friend std::ostream& operator<<(std::ostream& os, const Version& version);

    bool operator!=(const Version& rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator==(const Version& rhs) const { return first == rhs.first && second == rhs.second; }

    bool contains(const Version& other) const {
        return first <= other.first && second >= other.second;
    }

    std::string to_string() const { return fmt::format("[{}-{}]", first, second); }
};

struct TsoRange : public Version {
    TsoRange() : Version(-1, -1) {}
    TsoRange(int64_t start_tso, int64_t end_tso) : Version(start_tso, end_tso) {}

    int64_t start_tso() const { return first; }
    int64_t end_tso() const { return second; }

    bool contains(const TsoRange& other) const { return Version::contains(other); }
};

using Versions = std::vector<Version>;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << version.to_string();
}

inline std::ostream& operator<<(std::ostream& os, const Versions& versions) {
    for (auto& version : versions) {
        os << version;
    }
    return os;
}

// used for hash-struct of hash_map<Version, Rowset*>.
struct HashOfVersion {
    size_t operator()(const Version& version) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
        seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
        return seed;
    }
};

// It is used to represent Graph vertex.
struct Vertex {
    int64_t value = 0;
    std::list<int64_t> edges;

    Vertex(int64_t v) : value(v) {}
};

// ReaderStatistics used to collect statistics when scan data from storage
struct OlapReaderStatistics {
    int64_t io_ns = 0;
    int64_t compressed_bytes_read = 0;

    int64_t decompress_ns = 0;
    int64_t uncompressed_bytes_read = 0;

    // total read bytes in memory
    int64_t bytes_read = 0;

    int64_t block_fetch_ns = 0; // time of rowset reader's `next_batch()` call
    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    // Not used any more, will be removed after non-vectorized code is removed
    int64_t block_seek_num = 0;
    // Not used any more, will be removed after non-vectorized code is removed
    int64_t block_seek_ns = 0;

    // block_load_ns
    //      block_init_ns
    //          block_init_seek_ns
    //          generate_row_ranges_ns
    //      predicate_column_read_ns
    //          predicate_column_read_seek_ns
    //      lazy_read_ns
    //          block_lazy_read_seek_ns
    int64_t block_init_ns = 0;
    int64_t block_init_seek_num = 0;
    int64_t block_init_seek_ns = 0;
    int64_t predicate_column_read_ns = 0;
    int64_t non_predicate_read_ns = 0;
    int64_t predicate_column_read_seek_num = 0;
    int64_t predicate_column_read_seek_ns = 0;
    int64_t lazy_read_ns = 0;
    int64_t block_lazy_read_seek_num = 0;
    int64_t block_lazy_read_seek_ns = 0;
    int64_t lazy_read_pruned_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t rows_short_circuit_cond_filtered = 0;
    int64_t rows_expr_cond_filtered = 0;
    int64_t vec_cond_input_rows = 0;
    int64_t short_circuit_cond_input_rows = 0;
    int64_t expr_cond_input_rows = 0;
    int64_t rows_vec_del_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t short_cond_ns = 0;
    int64_t expr_filter_ns = 0;
    int64_t output_col_ns = 0;
    int64_t rows_key_range_filtered = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_stats_rp_filtered = 0;
    int64_t expr_zonemap_filtered_segments = 0;
    int64_t expr_zonemap_filtered_pages = 0;
    int64_t expr_zonemap_unusable_evals = 0;
    int64_t in_zonemap_point_check_count = 0;
    int64_t in_zonemap_range_only_count = 0;
    int64_t rows_bf_filtered = 0;
    int64_t segment_dict_filtered = 0;
    // Including the number of rows filtered out according to the Delete information in the Tablet,
    // and the number of rows filtered for marked deleted rows under the unique key model.
    // This metric is mainly used to record the number of rows filtered by the delete condition in Segment V1,
    // and it is also used to record the replaced rows in the Unique key model in the "Reader" class.
    // In segmentv2, if you want to get all filtered rows, you need the sum of "rows_del_filtered" and "rows_conditions_filtered".
    int64_t rows_del_filtered = 0;
    int64_t rows_del_by_bitmap = 0;
    // the number of rows filtered by various column indexes.
    int64_t rows_conditions_filtered = 0;
    int64_t generate_row_ranges_by_keys_ns = 0;
    int64_t generate_row_ranges_by_column_conditions_ns = 0;
    int64_t generate_row_ranges_by_bf_ns = 0;
    int64_t generate_row_ranges_by_zonemap_ns = 0;
    int64_t generate_row_ranges_by_dict_ns = 0;

    int64_t index_load_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_inverted_index_filtered = 0;
    int64_t inverted_index_filter_timer = 0;
    int64_t inverted_index_query_timer = 0;
    int64_t inverted_index_query_cache_hit = 0;
    int64_t inverted_index_query_cache_miss = 0;
    int64_t inverted_index_query_cache_lookup = 0;
    int64_t inverted_index_query_cache_insert = 0;
    int64_t inverted_index_query_null_bitmap_timer = 0;
    int64_t inverted_index_query_bitmap_copy_timer = 0;
    int64_t inverted_index_searcher_open_timer = 0;
    int64_t inverted_index_searcher_search_timer = 0;
    int64_t inverted_index_searcher_search_init_timer = 0;
    int64_t inverted_index_searcher_search_exec_timer = 0;
    int64_t inverted_index_searcher_cache_hit = 0;
    int64_t inverted_index_searcher_cache_miss = 0;
    int64_t inverted_index_downgrade_count = 0;
    // Pushed-down conjuncts skipped (never index-evaluated) because the row
    // bitmap was already empty when their turn came.
    int64_t inverted_index_conjuncts_short_circuited = 0;
    // Rows pruned by the approximate (gram) index: rows removed when the candidate bitmap is
    // intersected with _row_bitmap.
    int64_t rows_gram_index_filtered = 0;
    // Candidate rows produced by the approximate (gram) index: rows that still need expression
    // re-verification after the intersection.
    int64_t gram_index_candidate_rows = 0;
    int64_t inverted_index_analyzer_timer = 0;
    int64_t inverted_index_lookup_timer = 0;
    // See snii_query_stats.h: one field here instead of one per SNII counter.
    snii::SniiQueryStats snii_stats;
    InvertedIndexStatistics inverted_index_stats;

    int64_t ann_index_load_ns = 0;
    int64_t ann_topn_search_ns = 0;
    int64_t ann_index_topn_search_cnt = 0;
    int64_t ann_ivf_on_disk_load_ns = 0;
    int64_t ann_ivf_on_disk_cache_hit_cnt = 0;
    int64_t ann_ivf_on_disk_cache_miss_cnt = 0;
    int64_t ann_index_cache_hits = 0;

    // Detailed timing for ANN operations
    int64_t ann_index_topn_engine_search_ns = 0;  // time spent in engine for range search
    int64_t ann_index_topn_result_process_ns = 0; // time spent processing TopN results
    int64_t ann_index_topn_engine_convert_ns = 0; // time spent on FAISS-side conversions (TopN)
    int64_t ann_index_topn_engine_prepare_ns =
            0; // time spent preparing before engine search (TopN)
    int64_t rows_ann_index_topn_filtered = 0;

    int64_t ann_index_range_search_ns = 0;
    int64_t ann_index_range_search_cnt = 0;
    // Detailed timing for ANN Range search
    int64_t ann_range_engine_search_ns = 0; // time spent in engine for range search
    int64_t ann_range_pre_process_ns = 0;   // time spent preparing before engine search

    int64_t ann_range_result_convert_ns = 0; // time spent processing range results
    int64_t ann_range_engine_convert_ns = 0; // time spent on FAISS-side conversions (Range)
    int64_t rows_ann_index_range_filtered = 0;
    int64_t ann_index_range_cache_hits = 0;
    int64_t ann_fall_back_brute_force_cnt = 0;
    int64_t ann_topn_fallback_by_small_candidate_cnt = 0;
    int64_t ann_topn_fallback_small_candidate_rows = 0;
    int64_t ann_range_fallback_by_small_candidate_cnt = 0;
    int64_t ann_range_fallback_small_candidate_rows = 0;

    int64_t output_index_result_column_timer = 0;
    // number of segment filtered by column stat when creating seg iterator
    int64_t filtered_segment_number = 0;
    // number of segment with condition cache hit
    int64_t condition_cache_hit_seg_nums = 0;
    // number of rows filtered by condition cache hit
    int64_t condition_cache_filtered_rows = 0;
    // total number of segment
    int64_t total_segment_number = 0;

    io::FileCacheStatistics file_cache_stats;
    int64_t load_segments_timer = 0;

    int64_t collect_iterator_merge_next_timer = 0;
    int64_t collect_iterator_normal_next_timer = 0;
    int64_t delete_bitmap_get_agg_ns = 0;

    int64_t tablet_reader_init_timer_ns = 0;
    int64_t tablet_reader_capture_rs_readers_timer_ns = 0;
    int64_t tablet_reader_init_keys_param_timer_ns = 0;
    int64_t tablet_reader_init_orderby_keys_param_timer_ns = 0;
    int64_t tablet_reader_init_conditions_param_timer_ns = 0;
    int64_t tablet_reader_init_delete_condition_param_timer_ns = 0;
    int64_t block_reader_vcollect_iter_init_timer_ns = 0;
    int64_t block_reader_rs_readers_init_timer_ns = 0;
    int64_t block_reader_build_heap_init_timer_ns = 0;

    int64_t rowset_reader_get_segment_iterators_timer_ns = 0;
    int64_t rowset_reader_create_iterators_timer_ns = 0;
    int64_t rowset_reader_init_iterators_timer_ns = 0;
    int64_t rowset_reader_load_segments_timer_ns = 0;

    int64_t segment_iterator_init_timer_ns = 0;
    int64_t segment_iterator_init_column_iterators_timer_ns = 0;
    int64_t segment_iterator_init_index_iterators_timer_ns = 0;
    int64_t segment_iterator_init_segment_prefetchers_timer_ns = 0;

    int64_t segment_create_column_readers_timer_ns = 0;
    int64_t segment_load_index_timer_ns = 0;

    int64_t adaptive_batch_size_predict_min_rows = INT64_MAX;
    int64_t adaptive_batch_size_predict_max_rows = 0;

    int64_t variant_scan_sparse_column_timer_ns = 0;
    int64_t variant_scan_sparse_column_bytes = 0;
    int64_t variant_fill_path_from_sparse_column_timer_ns = 0;
    int64_t variant_subtree_default_iter_count = 0;
    int64_t variant_subtree_leaf_iter_count = 0;
    int64_t variant_subtree_hierarchical_iter_count = 0;
    int64_t variant_subtree_sparse_iter_count = 0;
    int64_t variant_doc_value_column_iter_count = 0;
};

using ColumnId = uint32_t;
// Column unique id set
using UniqueIdSet = std::set<uint32_t>;
// Column unique Id -> column id map
using UniqueIdToColumnIdMap = std::map<ColumnId, ColumnId>;

// RowsetId moved to storage/rowset_id.h (included above): core/column/column.h
// needs the complete type, and this way it gets it without the rest of
// olap_common.h.

using RowsetIdUnorderedSet = std::unordered_set<RowsetId>;

// Extract rowset id from filename, return uninitialized rowset id if filename is invalid
inline RowsetId extract_rowset_id(std::string_view filename) {
    RowsetId rowset_id;
    if (filename.ends_with(".dat")) {
        // filename format: {rowset_id}_{segment_num}.dat
        auto end = filename.find('_');
        if (end == std::string::npos) {
            return rowset_id;
        }
        rowset_id.init(filename.substr(0, end));
        return rowset_id;
    }
    if (filename.ends_with(".idx")) {
        // filename format: {rowset_id}_{segment_num}_{index_id}.idx
        auto end = filename.find('_');
        if (end == std::string::npos) {
            return rowset_id;
        }
        rowset_id.init(filename.substr(0, end));
        return rowset_id;
    }
    return rowset_id;
}

class DeleteBitmap;
// merge on write context
struct MowContext {
    MowContext(int64_t version, int64_t txnid, std::shared_ptr<RowsetIdUnorderedSet> ids,
               std::vector<RowsetSharedPtr> rowset_ptrs, std::shared_ptr<DeleteBitmap> db)
            : max_version(version),
              txn_id(txnid),
              rowset_ids(std::move(ids)),
              rowset_ptrs(std::move(rowset_ptrs)),
              delete_bitmap(std::move(db)) {}
    int64_t max_version;
    int64_t txn_id;
    std::shared_ptr<RowsetIdUnorderedSet> rowset_ids;
    std::vector<RowsetSharedPtr> rowset_ptrs;
    std::shared_ptr<DeleteBitmap> delete_bitmap;
};

// used for controll compaction
struct VersionWithTime {
    std::atomic<int64_t> version;
    int64_t update_ts;

    VersionWithTime() : version(0), update_ts(MonotonicMillis()) {}

    void update_version_monoto(int64_t new_version) {
        int64_t cur_version = version.load(std::memory_order_relaxed);
        while (cur_version < new_version) {
            if (version.compare_exchange_strong(cur_version, new_version, std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                update_ts = MonotonicMillis();
                break;
            }
        }
    }
};
} // namespace doris
