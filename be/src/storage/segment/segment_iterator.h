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

#include <gen_cpp/Exprs_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/adaptive_block_size_predictor.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exec/common/variant_util.h"
#include "exprs/score_runtime.h"
#include "exprs/vexpr_fwd.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "runtime/runtime_profile.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/index/index_iterator.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/row_cursor.h"
#include "storage/schema.h"
#include "storage/segment/common.h"
#include "storage/segment/segment.h"
#include "util/json/path_in_data.h"
#include "util/slice.h"

namespace doris {

class VExpr;
class VExprContext;
struct RowLocation;

namespace segment_v2 {

class ColumnIterator;
class RowRanges;
class IndexIterator;

class SegmentIterator : public RowwiseIterator {
public:
    // Within SegmentIterator, ColumnId means an ordinal in the read schema.
    // Storage UIDs and caller-visible Block positions are named explicitly.
    SegmentIterator(std::shared_ptr<Segment> segment, ReadSchemaSPtr schema);
    ~SegmentIterator() override;

    [[nodiscard]] Status init_iterators();
    [[nodiscard]] Status init(const StorageReadOptions& opts) override;
    [[nodiscard]] Status next_batch(Block* block) override;

    // Get current block row locations. This function should be called
    // after the `next_batch` function.
    // Only vectorized version is supported.
    [[nodiscard]] Status current_block_row_locations(
            std::vector<RowLocation>* block_row_locations) override;

    const ReadSchema& schema() const override { return *_schema; }
    uint64_t data_id() const override { return _segment->id(); }

    void update_profile(RuntimeProfile* profile) override {
        _update_profile(profile, _short_cir_eval_predicate, "ShortCircuitPredicates");
        _update_profile(profile, _pre_eval_block_predicate, "PreEvaluatePredicates");

        if (_opts.delete_condition_predicates != nullptr) {
            std::set<std::shared_ptr<const ColumnPredicate>> delete_predicate_set;
            _opts.delete_condition_predicates->get_all_column_predicate(delete_predicate_set);
            _update_profile(profile, delete_predicate_set, "DeleteConditionPredicates");
        }
    }

    bool has_index_in_iterators() const {
        return std::any_of(_index_iterators.begin(), _index_iterators.end(),
                           [](const auto& iterator) { return iterator != nullptr; });
    }

private:
    Status _next_batch_internal(Block* block);

    Status _check_output_block(Block* block);

    template <typename Container>
    void _update_profile(RuntimeProfile* profile, const Container& predicates,
                         const std::string& title) {
        if (predicates.empty()) {
            return;
        }
        std::string info;
        for (auto pred : predicates) {
            info += "\n" + pred->debug_string();
        }
        profile->add_info_string(title, info);
    }

    [[nodiscard]] Status _lazy_init(Block* block);
    [[nodiscard]] Status _init_impl(const StorageReadOptions& opts);
    [[nodiscard]] Status _init_column_iterators();
    [[nodiscard]] Status _init_index_iterators();

    // calculate row ranges that fall into requested key ranges using short key index
    [[nodiscard]] Status _get_row_ranges_by_keys();
    [[nodiscard]] Status _prepare_seek(const StorageReadOptions::KeyRange& key_range);
    [[nodiscard]] Status _lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                         rowid_t* rowid);
    // lookup the ordinal of given key from short key index
    // the returned rowid is rowid in primary index, not the rowid encoded in primary key
    [[nodiscard]] Status _lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include,
                                                       rowid_t upper_bound, rowid_t* rowid);
    // lookup the ordinal of given key from primary key index
    [[nodiscard]] Status _lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include,
                                                       rowid_t* rowid);
    [[nodiscard]] Status _seek_and_peek(rowid_t rowid);

    // calculate row ranges that satisfy requested column conditions using various column index
    [[nodiscard]] Status _get_row_ranges_by_column_conditions();
    [[nodiscard]] Status _get_row_ranges_from_conditions(RowRanges* condition_row_ranges);
    [[nodiscard]] Status _apply_expr_zonemap_to_row_ranges(const VExprContextSPtrs& conjuncts,
                                                           rowid_t min_rowid,
                                                           RowRanges* row_ranges);
    [[nodiscard]] Status _apply_inverted_index();
    [[nodiscard]] Status _apply_inverted_index_on_column_predicate(
            std::shared_ptr<ColumnPredicate> pred,
            std::vector<std::shared_ptr<ColumnPredicate>>& remaining_predicates,
            bool* continue_apply);
    [[nodiscard]] Status _apply_ann_topn_predicate();
    [[nodiscard]] Status _apply_index_expr();
    // 近似（超集）索引结果只用来裁剪 _row_bitmap：求交并记账两个 profile 计数，
    // 表达式本身必须留在 _common_expr_ctxs_push_down 里由 _execute_common_expr 复验。
    void _apply_approx_index_result(VExprContext* expr_ctx);
    // G02: true iff answering the single pushed-down MATCH predicate by its
    // match COUNT alone is indistinguishable from the row-accurate bitmap for
    // this COUNT_ON_INDEX scan (no deletes, no other filters, full row bitmap,
    // no row-id consumers). Gates IndexQueryContext::count_on_index_fastpath;
    // the decision predicate itself lives in count_on_index_fastpath.h.
    bool _count_on_index_fastpath_safe() const;
    // G03: teardown of the G02 handshake. Captures whether the reader answered
    // with a fabricated count bitmap into _count_fastpath_hit and clears both
    // context flags so no later read_from_index call can observe or forge
    // them. Runs on every exit of the index-apply scope.
    void _capture_count_fastpath_hit();
    // G03: true iff the per-batch defaults fill of _read_columns_by_index
    // would apply to `cid` (the _no_need_read_key_data or _prune_column
    // branch) AND the block column needs no storage->schema cast, i.e. the
    // emission shortcut can reproduce the column's batch content exactly.
    bool _column_emits_defaults_for_count(ColumnId cid);
    // G03: fills CountEmitShortcutFacts from live iterator state at the end of
    // _lazy_init and returns the pure-guard verdict; the decision predicate
    // itself lives in count_on_index_fastpath.h.
    bool _should_engage_count_emit_shortcut(const Block* block);
    // G03: one emission-shortcut batch: min(remaining, kCountEmitBatchRows)
    // default rows filled straight into the block (NOT-NULL defaults for
    // nullable columns, mirroring _prune_column), then EOF once the countdown
    // reaches zero. Replaces the whole per-rowid _next_batch_internal body for
    // engaged scans.
    Status _emit_count_shortcut_batch(Block* block);

    bool _column_has_fulltext_index(int32_t cid);
    bool _column_has_ann_index(int32_t cid);
    bool _downgrade_without_index(Status res, bool need_remaining = false);
    inline bool _inverted_index_not_support_pred_type(const PredicateType& type);

    void _init_column_states();
    void _rebuild_scan_predicate_states();
    void _mark_common_expr_states(const VExprSPtr& expr);
    Status _vec_init_lazy_materialization();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    [[nodiscard]] Status _read_columns_by_index(const std::vector<ColumnId>& read_ordinals,
                                                uint32_t nrows_read_limit, uint16_t& nrows_read);
    void _replace_version_col_if_needed(const std::vector<ColumnId>& ordinals, size_t num_rows);
    void _update_tso_col_if_needed(const std::vector<ColumnId>& ordinals, size_t num_rows);
    Status _init_current_block(Block* block, std::vector<MutableColumnPtr>& non_pred_vector,
                               uint32_t nrows_read_limit);
    uint16_t _evaluate_vectorization_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    uint16_t _evaluate_short_circuit_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    Status _apply_read_limit_to_selected_rows(Block* block, uint16_t& selected_size);
    Status _output_columns_to_block(Block* block);
    [[nodiscard]] Status _read_columns_by_rowids(const std::vector<ColumnId>& read_ordinals,
                                                 std::vector<rowid_t>& rowid_vector,
                                                 uint16_t* sel_rowid_idx, size_t select_size,
                                                 MutableColumns* mutable_columns,
                                                 bool init_condition_cache = false,
                                                 bool read_for_predicate = false);
    [[nodiscard]] Status _read_lazy_pruned_columns(Block* block);

    Status copy_column_data_by_selector(IColumn* input_col_ptr, MutableColumnPtr& output_col,
                                        uint16_t* sel_rowid_idx, uint16_t select_size,
                                        size_t batch_size);

    template <class Container>
    [[nodiscard]] Status _output_column_by_sel_idx(Block* block, const Container& ordinals,
                                                   uint16_t* sel_rowid_idx, uint16_t select_size) {
        SCOPED_RAW_TIMER(&_opts.stats->output_col_ns);
        for (auto ordinal : ordinals) {
            if (ordinal >= _schema->num_block_columns()) {
                continue;
            }
            const auto& file_column_type = _storage_name_and_type[ordinal].second;
            if (!file_column_type->equals(*block->get_by_position(ordinal).type)) {
                // Do additional cast
                MutableColumnPtr tmp = file_column_type->create_column();
                RETURN_IF_ERROR(copy_column_data_by_selector(_current_columns[ordinal].get(), tmp,
                                                             sel_rowid_idx, select_size,
                                                             _opts.block_row_max));
                RETURN_IF_ERROR(variant_util::cast_column({tmp->get_ptr(), file_column_type, ""},
                                                          block->get_by_position(ordinal).type,
                                                          &block->get_by_position(ordinal).column));
            } else {
                MutableColumnPtr output_column =
                        block->get_by_position(ordinal).column->assert_mutable();
                RETURN_IF_ERROR(copy_column_data_by_selector(_current_columns[ordinal].get(),
                                                             output_column, sel_rowid_idx,
                                                             select_size, _opts.block_row_max));
            }
        }
        return Status::OK();
    }

    bool _can_evaluated_by_vectorized(std::shared_ptr<ColumnPredicate> predicate);

    [[nodiscard]] Status _execute_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size,
                                              Block* block);
    Status _process_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size, Block* block);

    uint16_t _evaluate_common_expr_filter(uint16_t* sel_rowid_idx, uint16_t selected_size,
                                          const IColumn::Filter& filter);

    // Dictionary column should do something to initial.
    void _convert_dict_code_for_predicate_if_necessary();

    void _convert_dict_code_for_predicate_if_necessary_impl(const ColumnPredicate& predicate);

    bool _check_apply_by_inverted_index(std::shared_ptr<ColumnPredicate> pred);

    void _output_index_result_column(const VExprContextSPtrs& expr_ctxs, uint16_t* sel_rowid_idx,
                                     uint16_t select_size);

    bool _need_read_data(ColumnId cid);
    bool _prune_column(ColumnId cid, MutableColumnPtr& column, size_t num_of_defaults);

    Status _construct_compound_expr_context();

    // Both the key cursor and _seek_block lay out the leading tablet key
    // columns densely, so position i addresses the same column in both.
    int _compare_short_key_with_seek_block(const RowCursor& key, size_t num_key_cols) {
        for (uint32_t i = 0; i < num_key_cols; ++i) {
            auto ord = key.field(i) <=> (*_seek_block[i])[0];
            if (ord != std::strong_ordering::equal) {
                return ord == std::strong_ordering::less ? -1 : 1;
            }
        }
        return 0;
    }

    Status _convert_column_to_expected_type(ColumnId column_id);
    Status _convert_to_expected_type(const std::vector<ColumnId>& ordinals);

    bool _no_need_read_key_data(ColumnId cid, MutableColumnPtr& column, size_t nrows_read);
    // Side-effect-free eligibility half of _no_need_read_key_data (no column
    // fill); shared by the per-batch fill and the G03 engage-time per-column
    // proof so the two can never drift.
    bool _no_need_read_key_data_eligible(ColumnId cid);

    bool _has_delete_pred(ColumnId cid) const;
    bool _has_lazy_pruned_children(ColumnId cid) const;
    bool _can_skip_reading_extra_column(ColumnId cid);

    bool _can_opt_limit_reads();

    void _initialize_predicate_results();
    bool _check_all_conditions_passed_inverted_index_for_column(ColumnId cid,
                                                                bool default_return = false);

    void _calculate_common_expr_index_exec_status();

    Status _process_eof(Block* block);

    void _fill_column_nothing();

    Status _process_columns(const std::vector<ColumnId>& ordinals, Block* block);

    // Initialize virtual columns in the block, set all virtual columns in the block to ColumnNothing
    void _init_virtual_columns(Block* block);
    // Fallback logic for virtual column materialization, materializing all unmaterialized virtual columns through expressions
    Status _materialization_of_virtual_column(Block* block);
    void _prepare_score_column_materialization();

    void _init_row_bitmap_by_condition_cache();

    void _init_segment_prefetchers();

    class BitmapRangeIterator;
    class BackwardBitmapRangeIterator;

    // Example:
    //   SELECT k, s.b, o FROM t
    //   WHERE k > 1 AND abs(k) < 10 AND abs(s.a) < 5;
    //   ReadSchema ordinals: [0:k, 1:s STRUCT<a,b>, 2:o]
    // When no filter is fully evaluated by an index:
    //   state[0:k] = {has_delete_pred=false, has_scan_pred=true,
    //                 has_common_expr=true, need_read_data=true}
    //   state[1:s] = {has_delete_pred=false, has_scan_pred=false,
    //                 has_common_expr=true, need_read_data=true}
    //   state[2:o] = {has_delete_pred=false, has_scan_pred=false,
    //                 has_common_expr=false, need_read_data=true}
    // A storage-only column appended for a delete condition would have
    // has_delete_pred=true.
    struct ColumnReadState {
        bool has_delete_pred = false;
        // Mirrors the mutable _col_predicates list: initially all safe scan
        // predicates, then only residual predicates after index evaluation.
        bool has_scan_pred = false;
        bool has_common_expr = false;
        // Index evaluation sets this to false when it fully supplies the column result.
        // _need_read_data() applies the remaining read constraints.
        bool need_read_data = true;

        bool has_predicate() const { return has_delete_pred || has_scan_pred; }
    };

    std::shared_ptr<Segment> _segment;
    // read schema from scanner
    ReadSchemaSPtr _schema;
    // Inverted-index field name and storage/materialization type for each ReadSchema column.
    std::vector<IndexFieldNameAndTypePair> _storage_name_and_type;
    // vector idx -> column iterarator
    std::vector<std::unique_ptr<ColumnIterator>> _column_iterators;
    std::vector<std::unique_ptr<IndexIterator>> _index_iterators;
    // after init(), `_row_bitmap` contains all rowid to scan
    roaring::Roaring _row_bitmap;
    // an iterator for `_row_bitmap` that can be used to extract row range to scan
    std::unique_ptr<BitmapRangeIterator> _range_iter;
    // the next rowid to read
    rowid_t _cur_rowid;
    // members related to lazy materialization read
    // --------------------------------------------
    // remember the rowids we've read for the current row block.
    // could be a local variable of next_batch(), kept here to reuse vector memory
    std::vector<rowid_t> _block_rowids;
    bool _is_need_vec_eval = false;
    bool _is_need_short_eval = false;
    bool _is_need_expr_eval = false;

    bool _enable_prune_nested_column = false;

    // Per-column state indexed by read schema ordinal. Ordered column lists
    // below are execution plans rather than additional column membership sets.
    std::vector<ColumnReadState> _column_states;
    // Columns of the current batch, indexed by read schema ordinal.
    MutableColumns _current_columns;
    std::vector<std::shared_ptr<ColumnPredicate>> _pre_eval_block_predicate;
    std::vector<std::shared_ptr<ColumnPredicate>> _short_cir_eval_predicate;
    // Example:
    //   SELECT k, s.b, o FROM t
    //   WHERE k > 1 AND abs(k) < 10 AND abs(s.a) < 5;
    //   ReadSchema ordinals: [0:k, 1:s STRUCT<a,b>, 2:o]
    //
    // The first three lists assign each active column to its earliest materialization stage:
    //   _predicate_ordinals   = [0] // k is used by both k > 1 and abs(k) < 10; predicate wins.
    //   _common_expr_ordinals = [1] // s is read for the abs(s.a) < 5 expression.
    //   _output_ordinals      = [2] // o is needed only by output.
    std::vector<ColumnId> _predicate_ordinals;
    std::vector<ColumnId> _common_expr_ordinals;
    std::vector<ColumnId> _output_ordinals;
    //   _lazy_pruned_ordinals = [1] // After filtering on s.a, read s.b for surviving rows.
    // Unlike the first three disjoint lists, this recovery list may contain the same ordinal.
    std::vector<ColumnId> _lazy_pruned_ordinals;

    // the actual init process is delayed to the first call to next_batch()
    bool _lazy_inited;
    bool _inited;

    StorageReadOptions _opts;
    // Adaptive batch size predictor; null when the feature is disabled.
    std::unique_ptr<AdaptiveBlockSizePredictor> _block_size_predictor;
    // Build the AdaptiveBlockSizePredictor for this segment based on segment footer
    // metadata for the projected output columns. Returns nullptr if the feature is
    // disabled or the byte budget is non-positive.
    std::unique_ptr<AdaptiveBlockSizePredictor> _make_block_size_predictor() const;
    // Snapshot of _opts.block_row_max at init time; used as the hard upper bound so that
    // dynamic adjustments never exceed the capacity of pre-allocated buffers.
    uint32_t _initial_block_row_max = 0;
    // make a copy of `_opts.column_predicates` in order to make local changes
    std::vector<std::shared_ptr<ColumnPredicate>> _col_predicates;
    VExprContextSPtrs _common_expr_ctxs_push_down;
    // row schema of the key to seek
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<ReadSchema> _seek_schema;
    // used to binary search the rowid for a given key
    // only used in `_get_row_ranges_by_keys`
    MutableColumns _seek_block;
    // Per-seek-schema-ordinal column iterators for the short-key seek path.
    // Points into _column_iterators when the key column is also read, otherwise
    // into _owned_seek_column_iterators (a seek key column may not be part of
    // the read schema at all).
    std::vector<ColumnIterator*> _seek_column_iterators;
    std::vector<std::unique_ptr<ColumnIterator>> _owned_seek_column_iterators;

    io::FileReaderSPtr _file_reader;

    // used for compaction, record selectd rowids of current batch
    uint16_t _selected_size;
    std::vector<uint16_t> _sel_rowid_idx;

    // Rows already produced by this iterator. Used together with
    // _opts.read_limit to compute the remaining per-batch budget.
    size_t _rows_returned = 0;

    int64_t _tablet_id = 0;
    // Column UIDs requested by the caller. A -1 entry means light schema change is disabled and
    // the column has no UID, so the _need_read_data() optimization is disabled.
    std::set<int32_t> _output_column_uids;

    std::vector<uint8_t> _ret_flags;

    /*
    * column and column_predicates on it.
    * a boolean value to indicate whether the column has been read by the index.
    */
    std::unordered_map<ColumnId, std::unordered_map<std::shared_ptr<ColumnPredicate>, bool>>
            _column_predicate_index_exec_status;

    /*
    * column and common expr on it.
    * a boolean value to indicate whether the column has been read by the index.
    */
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>>
            _common_expr_index_exec_status;

    /*
    * common expr context to slotref map
    * slot ref map is used to get slot ref expr by using column id.
    */
    std::unordered_map<VExprContext*, std::unordered_map<ColumnId, VExpr*>>
            _common_expr_to_slotref_map;

    ScoreRuntimeSPtr _score_runtime;

    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;

    // cid to virtual column expr
    std::map<ColumnId, VExprContextSPtr> _virtual_column_exprs;

    IndexQueryContextPtr _index_query_context;

    // G03 count-emission shortcut state (see count_on_index_fastpath.h).
    // _count_fastpath_hit: the reader answered the single MATCH predicate with
    // a fabricated count bitmap (captured from the G02 handshake reply).
    // _count_emit_shortcut: engaged at the end of _lazy_init when
    // count_emit_shortcut_safe holds; every subsequent batch is emitted by
    // _emit_count_shortcut_batch from _count_emit_rows_remaining (initialized
    // to the post-apply _row_bitmap cardinality) without touching the row
    // bitmap iterator.
    bool _count_fastpath_hit = false;
    bool _count_emit_shortcut = false;
    uint64_t _count_emit_rows_remaining = 0;

    // An indexed conjunct prefix emptied _row_bitmap, proving the WHOLE
    // pushed-down conjunction false. Set by the _apply_index_expr short
    // circuit when it consumes (clears) the remaining conjuncts, and read
    // where an empty conjunct list would otherwise zero the condition-cache
    // digest: the all-false result stays valid for the full conjunction, so
    // it must remain cacheable.
    bool _index_conjuncts_proved_empty = false;
    // Batch size for shortcut emission: VStatisticsIterator's
    // MAX_ROW_SIZE_IN_COUNT, the largest default-rows block shape already
    // proven through every consumer above the segment iterator by the plain
    // COUNT pushdown (rowset reader, collect iterator, block reader, scanner).
    static constexpr uint64_t kCountEmitBatchRows = 65535;

    // key is column uid, value is the sparse column cache
    std::unordered_map<int32_t, PathToBinaryColumnCacheUPtr> _variant_sparse_column_cache;

    bool _find_condition_cache = false;
    std::shared_ptr<std::vector<bool>> _condition_cache;
    static constexpr int CONDITION_CACHE_OFFSET = 2048;
};

} // namespace segment_v2
} // namespace doris
