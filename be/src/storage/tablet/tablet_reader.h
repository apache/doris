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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "io/io_common.h"
#include "storage/delete/delete_handler.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/predicate/filter_olap_param.h"
#include "storage/row_cursor.h"
#include "storage/rowid_conversion.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/segment/variant/variant_compaction_paths.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

class RuntimeState;
class BloomFilterFuncBase;
class ColumnPredicate;
class DeleteBitmap;
class HybridSetBase;
class RuntimeProfile;

class Block;
class VExpr;
class VExprContext;

// Used to compare row with input scan key. Scan key only contains key columns,
// row contains all key columns, which is superset of key columns.
// So we should compare the common prefix columns of lhs and rhs.
//
// NOTE: if you are not sure if you can use it, please don't use this function.
inline int compare_row_key(const RowCursor& lhs, const RowCursor& rhs) {
    auto cmp_cids = std::min(lhs.field_count(), rhs.field_count());
    for (uint32_t cid = 0; cid < cmp_cids; ++cid) {
        const auto& lf = lhs.field(cid);
        const auto& rf = rhs.field(cid);
        // Handle nulls: null < non-null
        if (lf.is_null() != rf.is_null()) {
            return lf.is_null() ? -1 : 1;
        }
        if (lf.is_null()) {
            continue; // both null
        }
        auto cmp = lf <=> rf;
        if (cmp < 0) return -1;
        if (cmp > 0) return 1;
    }
    return 0;
}

class TabletReader {
    struct KeysParam {
        std::vector<std::optional<RowCursor>> start_keys;
        std::vector<std::optional<RowCursor>> end_keys;
        bool start_key_include = false;
        bool end_key_include = false;
    };

public:
    // Params for Reader,
    // mainly include tablet, data version and fetch range.
    struct ReaderParams {
        bool has_single_version() const {
            return (rs_splits.size() == 1 &&
                    rs_splits[0].rs_reader->rowset()->start_version() == 0 &&
                    !rs_splits[0].rs_reader->rowset()->rowset_meta()->is_segments_overlapping()) ||
                   (rs_splits.size() == 2 &&
                    rs_splits[0].rs_reader->rowset()->rowset_meta()->num_rows() == 0 &&
                    rs_splits[1].rs_reader->rowset()->start_version() == 2 &&
                    !rs_splits[1].rs_reader->rowset()->rowset_meta()->is_segments_overlapping());
        }

        int get_be_exec_version() const {
            if (runtime_state) {
                return runtime_state->be_exec_version();
            }
            return BeExecVersionManager::get_newest_version();
        }

        void set_read_source(TabletReadSource read_source, bool skip_delete_bitmap = false) {
            rs_splits = std::move(read_source.rs_splits);
            delete_predicates = std::move(read_source.delete_predicates);
#ifndef BE_TEST
            if (!skip_delete_bitmap && tablet->need_read_delete_bitmap()) {
                delete_bitmap = std::move(read_source.delete_bitmap);
            }
#endif
        }

        BaseTabletSPtr tablet;
        TabletSchemaSPtr tablet_schema;
        // Set only by compaction, alongside its extended tablet_schema.
        VariantCompactionPathsSPtr variant_compaction_paths = nullptr;
        ReaderType reader_type = ReaderType::READER_QUERY;
        bool read_changelog = false;
        // The storage layer does not fold the rows: the scanner aggregates them itself, or the
        // tablet holds a single version, or an aggregate is pushed down.
        bool direct_mode = false;
        // for compaction, schema_change, check_sum: we don't use page cache
        // for query, when the BE config disable_storage_page_cache is false, we use page cache
        bool use_page_cache = false;
        Version version = Version(-1, 0);

        // The vectors are range-aligned; nullopt represents an unbounded endpoint.
        std::vector<std::optional<OlapTuple>> start_key;
        std::vector<std::optional<OlapTuple>> end_key;
        bool start_key_include = false;
        bool end_key_include = false;

        std::vector<std::shared_ptr<ColumnPredicate>> predicates;
        std::vector<RowsetMetaSharedPtr> delete_predicates;
        // slots that cast may be eliminated in storage layer
        std::map<std::string, DataTypePtr> target_cast_type_for_variants;

        std::map<int32_t, TColumnAccessPaths> all_access_paths;
        std::map<int32_t, TColumnAccessPaths> predicate_access_paths;

        std::vector<RowSetSplits> rs_splits;
        // For unique key table with merge-on-write
        DeleteBitmapPtr delete_bitmap = nullptr;

        // The read schema of every Block at the TabletReader boundary.
        // Query scanners construct the FE-slot prefix in scan-tuple order;
        // TabletReader may append storage-only delete-predicate columns.
        ReadSchemaSPtr read_schema;
        // output_columns only contain columns in OrderByExprs and outputExprs
        // (column unique ids, not ordinals)
        std::set<int32_t> output_columns;
        // Ordinals (positions in read_schema) of extra storage key columns
        // that are present only for scan-schema alignment.
        // Example: for AGG keys (k1, k2), a query that returns k2 can scan
        // (k1, k2) and project away k1. Direct readers may avoid reading such
        // columns only if the lower iterator proves their real values are not
        // required by predicates, delete conditions, or expressions.
        std::set<ColumnId> extra_columns;
        RuntimeProfile* profile = nullptr;
        RuntimeState* runtime_state = nullptr;

        TPushAggOp::type push_down_agg_type_opt = TPushAggOp::NONE;
        VExprContextSPtrs common_expr_ctxs_push_down;

        // Used by compaction to initialize source segment row-id mappings.
        RowIdConversion* rowid_conversion = nullptr;
        std::vector<int> topn_filter_source_node_ids;
        // The rows must come out in key order whatever the table model asks for: ORDER BY key
        // LIMIT n pushed down, or the compaction of a changelog tablet. Changelog MIN_DELTA
        // and DETAIL scans are key-ordered without this flag. See MergeDecision::key_ordered.
        bool force_key_order = false;
        // ORDER BY key DESC LIMIT n: keys descending.
        bool read_orderby_key_reverse = false;
        // Among the rows of one key, the older one goes first (see MergeDecision::older_first).
        bool use_insert_order_when_same = false;
        // ORDER BY key LIMIT n: the number of leading key columns sorted by ...
        size_t read_orderby_key_num_prefix_columns = 0;
        // ... and the n; 0 when no TopN is pushed down.
        size_t read_orderby_key_limit = 0;
        // for vertical compaction
        bool is_key_column_group = false;
        std::vector<uint32_t> key_group_cluster_key_idxes;

        // For sparse column compaction optimization
        // When true, use optimized path for sparse wide tables
        bool enable_sparse_optimization = false;

        bool is_segcompaction = false;

        // Enable value predicate pushdown for MOR tables
        bool enable_mor_value_predicate_pushdown = false;

        std::vector<RowwiseIteratorUPtr>* segment_iters_ptr = nullptr;

        void check_validation() const;

        int64_t batch_size = -1;

        // virtual column ordinal (position in read_schema) -> expression
        std::map<ColumnId, VExprContextSPtr> virtual_column_exprs;

        std::shared_ptr<ScoreRuntime> score_runtime;
        CollectionStatisticsPtr collection_statistics;
        std::shared_ptr<segment_v2::AnnTopNRuntime> ann_topn_runtime;

        uint64_t condition_cache_digest = 0;

        // General LIMIT budget forwarded to SegmentIterator. -1 means no limit.
        int64_t general_read_limit = -1;
        TChangelogScanType::type changelog_scan_type = TChangelogScanType::NONE;
    };

    TabletReader() = default;

    virtual ~TabletReader() = default;

    TabletReader(const TabletReader&) = delete;
    void operator=(const TabletReader&) = delete;

    // Initialize TabletReader with tablet, data version and fetch range.
    virtual Status init(const ReaderParams& read_params);

    // Read next block with aggregation.
    // Return OK and set `*eof` to false when at least one row was read into `block`.
    // Return OK and set `*eof` to true when no more rows can be read; `block` is empty then.
    // Return others when unexpected error happens.
    virtual Status next_block_with_aggregation(Block* block, bool* eof) {
        return Status::Error<ErrorCode::READER_INITIALIZE_ERROR>(
                "TabletReader not support next_block_with_aggregation");
    }

    virtual uint64_t merged_rows() const { return _merged_rows; }

    uint64_t filtered_rows() const {
        return _stats.rows_del_filtered + _stats.rows_del_by_bitmap +
               _stats.rows_conditions_filtered + _stats.rows_vec_del_cond_filtered +
               _stats.rows_vec_cond_filtered + _stats.rows_short_circuit_cond_filtered;
    }

    void set_batch_size(int batch_size) { _reader_context.batch_size = batch_size; }

    size_t batch_max_rows() const { return _reader_context.batch_size; }

    void set_preferred_block_size_bytes(size_t bytes) {
        _reader_context.preferred_block_size_bytes = bytes;
    }

    // Returns the preferred output block byte budget. Subclasses that support adaptive batch size
    // should override this; the base returns 0 (disabled), so a TabletReader that has not been
    // configured reads with the row ceiling only.
    virtual size_t preferred_block_size_bytes() const { return 0; }

    const OlapReaderStatistics& stats() const { return _stats; }
    OlapReaderStatistics* mutable_stats() { return &_stats; }

    virtual void update_profile(RuntimeProfile* profile) {}

    // Remove the delete-condition columns from `all_access_paths` so they fall back to a full
    // read (a meta-only read would make the storage delete predicate match nothing and leak
    // deleted rows).
    static void remove_delete_columns_from_access_paths(
            const DeleteHandler& delete_handler, const ReadSchema& read_schema,
            std::map<int32_t, TColumnAccessPaths>& all_access_paths);

    // The MergeDecision of a read of `tablet_schema` through `read_schema` whose rows must
    // come out in key order when `key_ordered`. `compare_ordinals` empty compares by the whole
    // key; a cluster key or ORDER BY prefix otherwise. Rows of one key are ordered by the
    // sequence column, or by the changelog TSO when the read schema has one, and never by a
    // sequence column for a cluster key table, whose rows are not versions of one key.
    // Only merge-on-read UNIQUE_KEYS reads compared by the whole key drop the older versions;
    // a sequence map needs every version, since any of them may hold the winning value of a
    // column group.
    static MergeDecision decide_merge(const TabletSchema& tablet_schema,
                                      const ReadSchema& read_schema,
                                      std::vector<uint32_t> compare_ordinals, bool key_ordered,
                                      bool reverse, bool older_first);

protected:
    Status _init_params(const ReaderParams& read_params);

    Status _capture_rs_readers(const ReaderParams& read_params);

    Status _init_keys_param(const ReaderParams& read_params);

    Status _init_orderby_keys_param(const ReaderParams& read_params);

    Status _init_column_predicates(const ReaderParams& read_params);

    Status _init_delete_condition(const ReaderParams& read_params);

    const BaseTabletSPtr& tablet() { return _tablet; }

    // The merge of this read: whether the rows come out in key order, what they are compared
    // by, and which versions of a key survive. Decided here once, consumed by the rowset
    // readers and the iterators above them.
    MergeDecision _decide_merge(const ReaderParams& read_params) const;

    // The read schema shared by the caller, TabletReader and each rowset reader.
    ReadSchemaSPtr _read_schema;

    // used for special optimization for query : ORDER BY key [ASC|DESC] LIMIT n
    // columns for orderby keys
    std::vector<uint32_t> _orderby_key_columns;
    BaseTabletSPtr _tablet;
    RowsetReaderContext _reader_context;
    TabletSchemaSPtr _tablet_schema;
    KeysParam _keys_param;
    std::vector<bool> _is_lower_keys_included;
    std::vector<bool> _is_upper_keys_included;
    std::vector<std::shared_ptr<ColumnPredicate>> _col_predicates;
    std::vector<std::shared_ptr<ColumnPredicate>> _value_col_predicates;
    DeleteHandler _delete_handler;

    ReaderType _reader_type = ReaderType::READER_QUERY;
    bool _delete_sign_available = false;
    bool _filter_delete = false;
    bool _direct_mode = false;

    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;
};

} // namespace doris
