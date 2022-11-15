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

#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "util/file_cache.h"

namespace doris {

class RowCursor;
class RowBlockV2;
class ShortKeyIndexIterator;

namespace fs {
class ReadableBlock;
}

namespace segment_v2 {

class BitmapIndexIterator;
class BitmapIndexReader;
class ColumnIterator;

class SegmentIterator : public RowwiseIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, const Schema& _schema);
    ~SegmentIterator() override;

    Status init(const StorageReadOptions& opts) override;
    Status next_batch(RowBlockV2* row_block) override;
    Status next_batch(vectorized::Block* block) override;

    // Get current block row locations. This function should be called
    // after the `next_batch` function.
    // Only vectorized version is supported.
    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override;

    const Schema& schema() const override { return _schema; }
    bool is_lazy_materialization_read() const override { return _lazy_materialization_read; }
    uint64_t data_id() const override { return _segment->id(); }

    bool update_profile(RuntimeProfile* profile) override {
        if (_short_cir_eval_predicate.empty() && _pre_eval_block_predicate.empty()) {
            if (_col_predicates.empty()) {
                return false;
            }

            std::string info;
            for (auto pred : _col_predicates) {
                info += "\n" + pred->debug_string();
            }
            profile->add_info_string("ColumnPredicates", info);
        } else {
            if (!_short_cir_eval_predicate.empty()) {
                std::string info;
                for (auto pred : _short_cir_eval_predicate) {
                    info += "\n" + pred->debug_string();
                }
                profile->add_info_string("Short Circuit ColumnPredicates", info);
            }
            if (!_pre_eval_block_predicate.empty()) {
                std::string info;
                for (auto pred : _pre_eval_block_predicate) {
                    info += "\n" + pred->debug_string();
                }
                profile->add_info_string("Pre Evaluate Block ColumnPredicates", info);
            }
        }

        return true;
    }

private:
    Status _init(bool is_vec = false);

    Status _init_return_column_iterators();
    Status _init_bitmap_index_iterators();

    // calculate row ranges that fall into requested key ranges using short key index
    Status _get_row_ranges_by_keys();
    Status _prepare_seek(const StorageReadOptions::KeyRange& key_range);
    Status _lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                           rowid_t* rowid);
    // lookup the ordinal of given key from short key index
    Status _lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                         rowid_t* rowid);
    // lookup the ordinal of given key from primary key index
    Status _lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include, rowid_t* rowid);
    Status _seek_and_peek(rowid_t rowid);

    // calculate row ranges that satisfy requested column conditions using various column index
    Status _get_row_ranges_by_column_conditions();
    Status _get_row_ranges_from_conditions(RowRanges* condition_row_ranges);
    Status _apply_bitmap_index();

    void _init_lazy_materialization();
    void _vec_init_lazy_materialization();
    // TODO: Fix Me
    // CHAR type in storage layer padding the 0 in length. But query engine need ignore the padding 0.
    // so segment iterator need to shrink char column before output it. only use in vec query engine.
    void _vec_init_char_column_id();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos);
    // read `nrows` of columns specified by `column_ids` into `block` at `row_offset`.
    Status _read_columns(const std::vector<ColumnId>& column_ids, RowBlockV2* block,
                         size_t row_offset, size_t nrows);

    // for vectorization implementation
    Status _read_columns(const std::vector<ColumnId>& column_ids,
                         vectorized::MutableColumns& column_block, size_t nrows);
    Status _read_columns_by_index(uint32_t nrows_read_limit, uint32_t& nrows_read,
                                  bool set_block_rowid);
    void _init_current_block(vectorized::Block* block,
                             std::vector<vectorized::MutableColumnPtr>& non_pred_vector);
    uint16_t _evaluate_vectorization_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    uint16_t _evaluate_short_circuit_predicate(uint16_t* sel_rowid_idx, uint16_t selected_size);
    void _output_non_pred_columns(vectorized::Block* block);
    Status _read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                   std::vector<rowid_t>& rowid_vector, uint16_t* sel_rowid_idx,
                                   size_t select_size);

    template <class Container>
    Status _output_column_by_sel_idx(vectorized::Block* block, const Container& column_ids,
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

    // Dictionary column should do something to initial.
    void _convert_dict_code_for_predicate_if_necessary();

    void _convert_dict_code_for_predicate_if_necessary_impl(ColumnPredicate* predicate);

    void _update_max_row(const vectorized::Block* block);

private:
    class BitmapRangeIterator;
    class BackwardBitmapRangeIterator;

    std::shared_ptr<Segment> _segment;
    const Schema& _schema;
    // _column_iterators_map.size() == _schema.num_columns()
    // map<unique_id, ColumnIterator*> _column_iterators_map/_bitmap_index_iterators;
    // can use _schema get unique_id by cid
    std::map<int32_t, ColumnIterator*> _column_iterators;
    std::map<int32_t, BitmapIndexIterator*> _bitmap_index_iterators;
    // after init(), `_row_bitmap` contains all rowid to scan
    roaring::Roaring _row_bitmap;
    // an iterator for `_row_bitmap` that can be used to extract row range to scan
    std::unique_ptr<BitmapRangeIterator> _range_iter;
    // the next rowid to read
    rowid_t _cur_rowid;
    // members related to lazy materialization read
    // --------------------------------------------
    // whether lazy materialization read should be used.
    bool _lazy_materialization_read;
    // columns to read before predicate evaluation
    std::vector<ColumnId> _predicate_columns;
    // columns to read after predicate evaluation
    std::vector<ColumnId> _non_predicate_columns;
    // remember the rowids we've read for the current row block.
    // could be a local variable of next_batch(), kept here to reuse vector memory
    std::vector<rowid_t> _block_rowids;
    bool _is_need_vec_eval = false;
    bool _is_need_short_eval = false;

    // fields for vectorization execution
    std::vector<ColumnId>
            _vec_pred_column_ids; // keep columnId of columns for vectorized predicate evaluation
    std::vector<ColumnId>
            _short_cir_pred_column_ids; // keep columnId of columns for short circuit predicate evaluation
    std::vector<bool> _is_pred_column; // columns hold by segmentIter
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
    std::vector<int> _schema_block_id_map; // map from schema column id to column idx in Block

    // the actual init process is delayed to the first call to next_batch()
    bool _inited;
    bool _estimate_row_size;

    StorageReadOptions _opts;
    // make a copy of `_opts.column_predicates` in order to make local changes
    std::vector<ColumnPredicate*> _col_predicates;

    // row schema of the key to seek
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<Schema> _seek_schema;
    // used to binary search the rowid for a given key
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<RowBlockV2> _seek_block;

    io::FileReaderSPtr _file_reader;

    // char_type or array<char> type columns cid
    std::vector<size_t> _char_type_idx;

    // number of rows read in the current batch
    uint32_t _current_batch_rows_read = 0;
    // used for compaction, record selectd rowids of current batch
    uint16_t _selected_size;
    vector<uint16_t> _sel_rowid_idx;
};

} // namespace segment_v2
} // namespace doris
