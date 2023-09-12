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
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/row_cursor_cell.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"

namespace doris {

class ObjectPool;

namespace vectorized {
class VExpr;
class VExprContext;
} // namespace vectorized
struct RowLocation;

namespace segment_v2 {

class BitmapIndexIterator;
class ColumnIterator;
class InvertedIndexIterator;
class RowRanges;

struct ColumnPredicateInfo {
    ColumnPredicateInfo() = default;

    std::string debug_string() const {
        std::stringstream ss;
        ss << "column_name=" << column_name << ", query_op=" << query_op
           << ", query_value=" << query_value;
        return ss.str();
    }

    bool is_empty() const { return column_name.empty() && query_value.empty() && query_op.empty(); }

    bool is_equal(const ColumnPredicateInfo& column_pred_info) const {
        if (column_pred_info.column_name != column_name) {
            return false;
        }

        if (column_pred_info.query_value != query_value) {
            return false;
        }

        if (column_pred_info.query_op != query_op) {
            return false;
        }

        return true;
    }

    std::string column_name;
    std::string query_value;
    std::string query_op;
};

class SegmentIterator : public RowwiseIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, SchemaSPtr schema);
    ~SegmentIterator() override;

    [[nodiscard]] Status init_iterators();
    [[nodiscard]] Status init(const StorageReadOptions& opts) override;
    [[nodiscard]] Status next_batch(vectorized::Block* block) override;

    // Get current block row locations. This function should be called
    // after the `next_batch` function.
    // Only vectorized version is supported.
    [[nodiscard]] Status current_block_row_locations(
            std::vector<RowLocation>* block_row_locations) override;

    const Schema& schema() const override { return *_schema; }
    bool is_lazy_materialization_read() const override { return _lazy_materialization_read; }
    uint64_t data_id() const override { return _segment->id(); }
    RowsetId rowset_id() const { return _segment->rowset_id(); }
    int32_t tablet_id() const { return _tablet_id; }

    bool update_profile(RuntimeProfile* profile) override {
        bool updated = false;
        updated |= _update_profile(profile, _short_cir_eval_predicate, "ShortCircuitPredicates");
        updated |= _update_profile(profile, _pre_eval_block_predicate, "PreEvaluatePredicates");

        if (_opts.delete_condition_predicates != nullptr) {
            std::set<const ColumnPredicate*> delete_predicate_set;
            _opts.delete_condition_predicates->get_all_column_predicate(delete_predicate_set);
            updated |= _update_profile(profile, delete_predicate_set, "DeleteConditionPredicates");
        }

        return updated;
    }

private:
    Status _next_batch_internal(vectorized::Block* block);

    template <typename Container>
    bool _update_profile(RuntimeProfile* profile, const Container& predicates,
                         const std::string& title) {
        if (predicates.empty()) {
            return false;
        }
        std::string info;
        for (auto pred : predicates) {
            info += "\n" + pred->debug_string();
        }
        profile->add_info_string(title, info);
        return true;
    }

    [[nodiscard]] Status _lazy_init();

    [[nodiscard]] Status _init_return_column_iterators();
    [[nodiscard]] Status _init_bitmap_index_iterators();
    [[nodiscard]] Status _init_inverted_index_iterators();

    // calculate row ranges that fall into requested key ranges using short key index
    [[nodiscard]] Status _get_row_ranges_by_keys();
    [[nodiscard]] Status _prepare_seek(const StorageReadOptions::KeyRange& key_range);
    [[nodiscard]] Status _lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                         rowid_t* rowid);
    // lookup the ordinal of given key from short key index
    [[nodiscard]] Status _lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include,
                                                       rowid_t upper_bound, rowid_t* rowid);
    // lookup the ordinal of given key from primary key index
    [[nodiscard]] Status _lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include,
                                                       rowid_t* rowid);
    [[nodiscard]] Status _seek_and_peek(rowid_t rowid);

    // calculate row ranges that satisfy requested column conditions using various column index
    [[nodiscard]] Status _get_row_ranges_by_column_conditions();
    [[nodiscard]] Status _get_row_ranges_from_conditions(RowRanges* condition_row_ranges);
    [[nodiscard]] Status _apply_bitmap_index();
    [[nodiscard]] Status _apply_inverted_index();
    [[nodiscard]] Status _apply_inverted_index_on_column_predicate(
            ColumnPredicate* pred, std::vector<ColumnPredicate*>& remaining_predicates,
            bool* continue_apply);
    [[nodiscard]] Status _apply_inverted_index_on_block_column_predicate(
            ColumnId column_id, MutilColumnBlockPredicate* pred,
            std::set<const ColumnPredicate*>& no_need_to_pass_column_predicate_set,
            bool* continue_apply);
    [[nodiscard]] Status _apply_index_except_leafnode_of_andnode();
    [[nodiscard]] Status _apply_bitmap_index_except_leafnode_of_andnode(
            ColumnPredicate* pred, roaring::Roaring* output_result);
    [[nodiscard]] Status _apply_inverted_index_except_leafnode_of_andnode(
            ColumnPredicate* pred, roaring::Roaring* output_result);
    bool _column_has_fulltext_index(int32_t cid);
    bool _downgrade_without_index(Status res, bool need_remaining = false);
    inline bool _inverted_index_not_support_pred_type(const PredicateType& type);
    bool _can_filter_by_preds_except_leafnode_of_andnode();
    [[nodiscard]] Status _execute_predicates_except_leafnode_of_andnode(
            const vectorized::VExprSPtr& expr);
    [[nodiscard]] Status _execute_compound_fn(const std::string& function_name);
    bool _is_literal_node(const TExprNodeType::type& node_type);

    Status _vec_init_lazy_materialization();
    // TODO: Fix Me
    // CHAR type in storage layer padding the 0 in length. But query engine need ignore the padding 0.
    // so segment iterator need to shrink char column before output it. only use in vec query engine.
    void _vec_init_char_column_id();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    [[nodiscard]] Status _seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos);
    // read `nrows` of columns specified by `column_ids` into `block` at `row_offset`.
    // for vectorization implementation
    [[nodiscard]] Status _read_columns(const std::vector<ColumnId>& column_ids,
                                       vectorized::MutableColumns& column_block, size_t nrows);
    [[nodiscard]] Status _read_columns_by_index(uint32_t nrows_read_limit, uint32_t& nrows_read,
                                                bool set_block_rowid);
    void _replace_version_col(size_t num_rows);
    void _init_current_block(vectorized::Block* block,
                             std::vector<vectorized::MutableColumnPtr>& non_pred_vector);
    uint16_t _evaluate_vectorization_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    uint16_t _evaluate_short_circuit_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    void _output_non_pred_columns(vectorized::Block* block);
    [[nodiscard]] Status _read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                                 std::vector<rowid_t>& rowid_vector,
                                                 uint16_t* sel_rowid_idx, size_t select_size,
                                                 vectorized::MutableColumns* mutable_columns);

    template <class Container>
    [[nodiscard]] Status _output_column_by_sel_idx(vectorized::Block* block,
                                                   const Container& column_ids,
                                                   uint16_t* sel_rowid_idx, uint16_t select_size) {
        SCOPED_RAW_TIMER(&_opts.stats->output_col_ns);
        for (auto cid : column_ids) {
            int block_cid = _schema_block_id_map[cid];
            RETURN_IF_ERROR(block->copy_column_data_to_block(_current_return_columns[cid].get(),
                                                             sel_rowid_idx, select_size, block_cid,
                                                             _opts.block_row_max));
        }
        return Status::OK();
    }

    bool _can_evaluated_by_vectorized(ColumnPredicate* predicate);

    [[nodiscard]] Status _extract_common_expr_columns(const vectorized::VExprSPtr& expr);
    [[nodiscard]] Status _execute_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size,
                                              vectorized::Block* block);
    uint16_t _evaluate_common_expr_filter(uint16_t* sel_rowid_idx, uint16_t selected_size,
                                          const vectorized::IColumn::Filter& filter);

    // Dictionary column should do something to initial.
    void _convert_dict_code_for_predicate_if_necessary();

    void _convert_dict_code_for_predicate_if_necessary_impl(ColumnPredicate* predicate);

    void _update_max_row(const vectorized::Block* block);

    bool _check_apply_by_bitmap_index(ColumnPredicate* pred);
    bool _check_apply_by_inverted_index(ColumnPredicate* pred, bool pred_in_compound = false);

    std::string _gen_predicate_result_sign(ColumnPredicate* predicate);
    std::string _gen_predicate_result_sign(ColumnPredicateInfo* predicate_info);

    void _build_index_result_column(uint16_t* sel_rowid_idx, uint16_t select_size,
                                    vectorized::Block* block, const std::string& pred_result_sign,
                                    const roaring::Roaring& index_result);
    void _output_index_result_column(uint16_t* sel_rowid_idx, uint16_t select_size,
                                     vectorized::Block* block);

    bool _need_read_data(ColumnId cid);
    bool _prune_column(ColumnId cid, vectorized::MutableColumnPtr& column, bool fill_defaults,
                       size_t num_of_defaults);

    // return true means one column's predicates all pushed down
    bool _check_column_pred_all_push_down(const std::string& column_name, bool in_compound = false,
                                          bool is_match = false);
    void _calculate_pred_in_remaining_conjunct_root(const vectorized::VExprSPtr& expr);

    // todo(wb) remove this method after RowCursor is removed
    void _convert_rowcursor_to_short_key(const RowCursor& key, size_t num_keys) {
        if (_short_key.size() == 0) {
            _short_key.resize(num_keys);
            for (auto cid = 0; cid < num_keys; cid++) {
                auto* field = key.schema()->column(cid);
                _short_key[cid] = Schema::get_column_by_field(*field);

                if (field->type() == FieldType::OLAP_FIELD_TYPE_DATE) {
                    _short_key[cid]->set_date_type();
                } else if (field->type() == FieldType::OLAP_FIELD_TYPE_DATETIME) {
                    _short_key[cid]->set_datetime_type();
                }
            }
        } else {
            for (int i = 0; i < num_keys; i++) {
                _short_key[i]->clear();
            }
        }

        for (auto cid = 0; cid < num_keys; cid++) {
            auto field = key.schema()->column(cid);
            if (field == nullptr) {
                break;
            }
            auto cell = key.cell(cid);
            if (cell.is_null()) {
                _short_key[cid]->insert_default();
            } else {
                if (field->type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                    field->type() == FieldType::OLAP_FIELD_TYPE_CHAR ||
                    field->type() == FieldType::OLAP_FIELD_TYPE_STRING) {
                    const Slice* slice = reinterpret_cast<const Slice*>(cell.cell_ptr());
                    _short_key[cid]->insert_data(slice->data, slice->size);
                } else {
                    _short_key[cid]->insert_many_fix_len_data(
                            reinterpret_cast<const char*>(cell.cell_ptr()), 1);
                }
            }
        }
    }

    int _compare_short_key_with_seek_block(const std::vector<ColumnId>& col_ids) {
        for (auto cid : col_ids) {
            // todo(wb) simd compare when memory layout in row
            auto res = _short_key[cid]->compare_at(0, 0, *_seek_block[cid], -1);
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }

    class BitmapRangeIterator;
    class BackwardBitmapRangeIterator;

    std::shared_ptr<Segment> _segment;
    SchemaSPtr _schema;
    // vector idx -> column iterarator
    std::vector<std::unique_ptr<ColumnIterator>> _column_iterators;
    std::vector<std::unique_ptr<BitmapIndexIterator>> _bitmap_index_iterators;
    std::vector<std::unique_ptr<InvertedIndexIterator>> _inverted_index_iterators;
    // after init(), `_row_bitmap` contains all rowid to scan
    roaring::Roaring _row_bitmap;
    // "column_name+operator+value-> <in_compound_query, rowid_result>
    std::unordered_map<std::string, std::pair<bool, roaring::Roaring>> _rowid_result_for_index;
    std::vector<std::pair<uint32_t, uint32_t>> _split_row_ranges;
    // an iterator for `_row_bitmap` that can be used to extract row range to scan
    std::unique_ptr<BitmapRangeIterator> _range_iter;
    // the next rowid to read
    rowid_t _cur_rowid;
    // members related to lazy materialization read
    // --------------------------------------------
    // whether lazy materialization read should be used.
    bool _lazy_materialization_read;
    // columns to read after predicate evaluation and remaining expr execute
    std::vector<ColumnId> _non_predicate_columns;
    std::set<ColumnId> _common_expr_columns;
    // remember the rowids we've read for the current row block.
    // could be a local variable of next_batch(), kept here to reuse vector memory
    std::vector<rowid_t> _block_rowids;
    bool _is_need_vec_eval = false;
    bool _is_need_short_eval = false;
    bool _is_need_expr_eval = false;

    // fields for vectorization execution
    std::vector<ColumnId>
            _vec_pred_column_ids; // keep columnId of columns for vectorized predicate evaluation
    std::vector<ColumnId>
            _short_cir_pred_column_ids; // keep columnId of columns for short circuit predicate evaluation
    std::vector<bool> _is_pred_column; // columns hold _init segmentIter
    std::map<uint32_t, bool> _need_read_data_indices;
    std::vector<bool> _is_common_expr_column;
    vectorized::MutableColumns _current_return_columns;
    std::vector<ColumnPredicate*> _pre_eval_block_predicate;
    std::vector<ColumnPredicate*> _short_cir_eval_predicate;
    std::vector<uint32_t> _delete_range_column_ids;
    std::vector<uint32_t> _delete_bloom_filter_column_ids;
    // when lazy materialization is enabled, segmentIter need to read data at least twice
    // first, read predicate columns by various index
    // second, read non-predicate columns
    // so we need a field to stand for columns first time to read
    std::vector<ColumnId> _first_read_column_ids;
    std::vector<ColumnId> _second_read_column_ids;
    std::vector<ColumnId> _columns_to_filter;
    std::vector<int> _schema_block_id_map; // map from schema column id to column idx in Block

    // the actual init process is delayed to the first call to next_batch()
    bool _lazy_inited;
    bool _inited;
    bool _estimate_row_size;
    // Read up to 100 rows at a time while waiting for the estimated row size.
    int _wait_times_estimate_row_size;

    StorageReadOptions _opts;
    // make a copy of `_opts.column_predicates` in order to make local changes
    std::vector<ColumnPredicate*> _col_predicates;
    std::vector<ColumnPredicate*> _col_preds_except_leafnode_of_andnode;
    vectorized::VExprContextSPtrs _common_expr_ctxs_push_down;
    bool _enable_common_expr_pushdown = false;
    std::vector<vectorized::VExprSPtr> _remaining_conjunct_roots;
    std::vector<roaring::Roaring> _pred_except_leafnode_of_andnode_evaluate_result;
    std::unique_ptr<ColumnPredicateInfo> _column_predicate_info;
    std::unordered_map<std::string, std::vector<ColumnPredicateInfo>>
            _column_pred_in_remaining_vconjunct;
    std::set<ColumnId> _not_apply_index_pred;

    std::shared_ptr<ColumnPredicate> _runtime_predicate {nullptr};

    // row schema of the key to seek
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<Schema> _seek_schema;
    // used to binary search the rowid for a given key
    // only used in `_get_row_ranges_by_keys`
    vectorized::MutableColumns _seek_block;

    //todo(wb) remove this field after Rowcursor is removed
    vectorized::MutableColumns _short_key;

    io::FileReaderSPtr _file_reader;

    // char_type or array<char> type columns cid
    std::vector<size_t> _char_type_idx;
    std::vector<size_t> _char_type_idx_no_0;

    // number of rows read in the current batch
    uint32_t _current_batch_rows_read = 0;
    // used for compaction, record selectd rowids of current batch
    uint16_t _selected_size;
    vector<uint16_t> _sel_rowid_idx;

    std::unique_ptr<ObjectPool> _pool;

    // used to collect filter information.
    std::vector<ColumnPredicate*> _filter_info_id;
    bool _record_rowids = false;
    int32_t _tablet_id = 0;
    std::set<int32_t> _output_columns;
};

} // namespace segment_v2
} // namespace doris
