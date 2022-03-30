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

#include "olap/rowset/segment_v2/segment_iterator.h"

#include <memory>
#include <set>
#include <utility>

#include "gutil/strings/substitute.h"
#include "olap/column_predicate.h"
#include "olap/fs/fs_util.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_common.h"
#include "olap/row.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/short_key_index.h"
#include "util/doris_metrics.h"
#include "util/simd/bits.h"
#include "vec/columns/column_dictionary.h"

using strings::Substitute;

namespace doris {
namespace segment_v2 {

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class SegmentIterator::BitmapRangeIterator {
public:
    explicit BitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator(&bitmap.roaring, &_iter);
        _read_next_batch();
    }

    bool has_more_range() const { return !_eof; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    bool next_range(uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }
        *from = _buf[_buf_pos];
        uint32_t range_size = 0, last_val;
        do {
            last_val = _buf[_buf_pos];
            _buf_pos++;
            range_size++;
            if (UNLIKELY(_buf_pos == _buf_size)) { // read next batch
                _read_next_batch();
                if (_eof) {
                    break;
                }
            }
        } while (range_size < max_range_size && _buf[_buf_pos] == last_val + 1);
        *to = *from + range_size;
        return true;
    }

private:
    void _read_next_batch() {
        _buf_pos = 0;
        _buf_size = roaring::api::roaring_read_uint32_iterator(&_iter, _buf, kBatchSize);
        _eof = (_buf_size == 0);
    }

    static const uint32_t kBatchSize = 256;
    roaring::api::roaring_uint32_iterator_t _iter;
    uint32_t _buf[kBatchSize];
    uint32_t _buf_pos = 0;
    uint32_t _buf_size = 0;
    bool _eof = false;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, const Schema& schema)
        : _segment(std::move(segment)),
          _schema(schema),
          _column_iterators(_schema.num_columns(), nullptr),
          _bitmap_index_iterators(_schema.num_columns(), nullptr),
          _cur_rowid(0),
          _lazy_materialization_read(false),
          _inited(false) {}

SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter;
    }
    for (auto iter : _bitmap_index_iterators) {
        delete iter;
    }
}

Status SegmentIterator::init(const StorageReadOptions& opts) {
    _opts = opts;
    if (!opts.column_predicates.empty()) {
        _col_predicates = opts.column_predicates;
    }
    return Status::OK();
}

Status SegmentIterator::_init(bool is_vec) {
    DorisMetrics::instance()->segment_read_total->increment(1);
    // get file handle from file descriptor of segment
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(_segment->_path_desc.storage_medium);
    RETURN_IF_ERROR(block_mgr->open_block(_segment->_path_desc, &_rblock));
    _row_bitmap.addRange(0, _segment->num_rows());
    RETURN_IF_ERROR(_init_return_column_iterators());
    RETURN_IF_ERROR(_init_bitmap_index_iterators());
    // z-order can not use prefix index
    if (_segment->_tablet_schema->sort_type() != SortType::ZORDER) {
        RETURN_IF_ERROR(_get_row_ranges_by_keys());
    }
    RETURN_IF_ERROR(_get_row_ranges_by_column_conditions());
    if (is_vec) {
        _vec_init_lazy_materialization();
    } else {
        _init_lazy_materialization();
    }
    _range_iter.reset(new BitmapRangeIterator(_row_bitmap));
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_keys() {
    DorisMetrics::instance()->segment_row_total->increment(num_rows());

    // fast path for empty segment or empty key ranges
    if (_row_bitmap.isEmpty() || _opts.key_ranges.empty()) {
        return Status::OK();
    }

    RowRanges result_ranges;
    for (auto& key_range : _opts.key_ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();
        RETURN_IF_ERROR(_prepare_seek(key_range));
        if (key_range.upper_key != nullptr) {
            // If client want to read upper_bound, the include_upper is true. So we
            // should get the first ordinal at which key is larger than upper_bound.
            // So we call _lookup_ordinal with include_upper's negate
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.upper_key, !key_range.include_upper,
                                            num_rows(), &upper_rowid));
        }
        if (upper_rowid > 0 && key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.lower_key, key_range.include_lower,
                                            upper_rowid, &lower_rowid));
        }
        auto row_range = RowRanges::create_single(lower_rowid, upper_rowid);
        RowRanges::ranges_union(result_ranges, row_range, &result_ranges);
    }
    // pre-condition: _row_ranges == [0, num_rows)
    size_t pre_size = _row_bitmap.cardinality();
    _row_bitmap = RowRanges::ranges_to_roaring(result_ranges);
    _opts.stats->rows_key_range_filtered += (pre_size - _row_bitmap.cardinality());
    DorisMetrics::instance()->segment_rows_by_short_key->increment(_row_bitmap.cardinality());

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek(const StorageReadOptions::KeyRange& key_range) {
    std::vector<const Field*> key_fields;
    std::set<uint32_t> column_set;
    if (key_range.lower_key != nullptr) {
        for (auto cid : key_range.lower_key->schema()->column_ids()) {
            column_set.emplace(cid);
            key_fields.emplace_back(key_range.lower_key->column_schema(cid));
        }
    }
    if (key_range.upper_key != nullptr) {
        for (auto cid : key_range.upper_key->schema()->column_ids()) {
            if (column_set.count(cid) == 0) {
                key_fields.emplace_back(key_range.upper_key->column_schema(cid));
                column_set.emplace(cid);
            }
        }
    }
    _seek_schema = std::make_unique<Schema>(key_fields, key_fields.size());
    _seek_block = std::make_unique<RowBlockV2>(*_seek_schema, 1);

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.rblock = _rblock.get();
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_column_conditions() {
    if (_row_bitmap.isEmpty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_apply_bitmap_index());

    if (!_row_bitmap.isEmpty() &&
        (_opts.conditions != nullptr || !_opts.delete_conditions.empty())) {
        RowRanges condition_row_ranges = RowRanges::create_single(_segment->num_rows());
        RETURN_IF_ERROR(_get_row_ranges_from_conditions(&condition_row_ranges));
        size_t pre_size = _row_bitmap.cardinality();
        _row_bitmap &= RowRanges::ranges_to_roaring(condition_row_ranges);
        _opts.stats->rows_conditions_filtered += (pre_size - _row_bitmap.cardinality());
    }

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_from_conditions(RowRanges* condition_row_ranges) {
    std::set<int32_t> cids;
    if (_opts.conditions != nullptr) {
        for (auto& column_condition : _opts.conditions->columns()) {
            cids.insert(column_condition.first);
        }
    }

    // first filter data by bloom filter index
    // bloom filter index only use CondColumn
    RowRanges bf_row_ranges = RowRanges::create_single(num_rows());
    for (auto& cid : cids) {
        // get row ranges by bf index of this column,
        RowRanges column_bf_row_ranges = RowRanges::create_single(num_rows());
        CondColumn* column_cond = _opts.conditions->get_column(cid);
        RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_bloom_filter(
                column_cond, &column_bf_row_ranges));
        RowRanges::ranges_intersection(bf_row_ranges, column_bf_row_ranges, &bf_row_ranges);
    }
    size_t pre_size = condition_row_ranges->count();
    RowRanges::ranges_intersection(*condition_row_ranges, bf_row_ranges, condition_row_ranges);
    _opts.stats->rows_bf_filtered += (pre_size - condition_row_ranges->count());

    RowRanges zone_map_row_ranges = RowRanges::create_single(num_rows());
    // second filter data by zone map
    for (auto& cid : cids) {
        // get row ranges by zone map of this column,
        RowRanges column_row_ranges = RowRanges::create_single(num_rows());
        CondColumn* column_cond = nullptr;
        if (_opts.conditions != nullptr) {
            column_cond = _opts.conditions->get_column(cid);
        }
        RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(column_cond, nullptr,
                                                                           &column_row_ranges));
        // intersect different columns's row ranges to get final row ranges by zone map
        RowRanges::ranges_intersection(zone_map_row_ranges, column_row_ranges,
                                       &zone_map_row_ranges);
    }

    // final filter data with delete conditions
    for (auto& delete_condition : _opts.delete_conditions) {
        RowRanges delete_condition_row_ranges = RowRanges::create_single(0);
        for (auto& delete_column_condition : delete_condition->columns()) {
            const int32_t cid = delete_column_condition.first;
            CondColumn* column_cond = nullptr;
            if (_opts.conditions != nullptr) {
                column_cond = _opts.conditions->get_column(cid);
            }
            RowRanges single_delete_condition_row_ranges = RowRanges::create_single(num_rows());
            RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(
                    column_cond, delete_column_condition.second,
                    &single_delete_condition_row_ranges));
            RowRanges::ranges_union(delete_condition_row_ranges, single_delete_condition_row_ranges,
                                    &delete_condition_row_ranges);
        }
        RowRanges::ranges_intersection(zone_map_row_ranges, delete_condition_row_ranges,
                                       &zone_map_row_ranges);
    }

    DorisMetrics::instance()->segment_rows_read_by_zone_map->increment(zone_map_row_ranges.count());
    pre_size = condition_row_ranges->count();
    RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges,
                                   condition_row_ranges);
    _opts.stats->rows_stats_filtered += (pre_size - condition_row_ranges->count());
    return Status::OK();
}

// filter rows by evaluating column predicates using bitmap indexes.
// upon return, predicates that've been evaluated by bitmap indexes are removed from _col_predicates.
Status SegmentIterator::_apply_bitmap_index() {
    SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_filter_timer);
    size_t input_rows = _row_bitmap.cardinality();
    std::vector<ColumnPredicate*> remaining_predicates;

    for (auto pred : _col_predicates) {
        if (_bitmap_index_iterators[pred->column_id()] == nullptr) {
            // no bitmap index for this column
            remaining_predicates.push_back(pred);
        } else {
            RETURN_IF_ERROR(pred->evaluate(_schema, _bitmap_index_iterators, _segment->num_rows(),
                                           &_row_bitmap));
            if (_row_bitmap.isEmpty()) {
                break; // all rows have been pruned, no need to process further predicates
            }
        }
    }
    _col_predicates = std::move(remaining_predicates);
    _opts.stats->rows_bitmap_index_filtered += (input_rows - _row_bitmap.cardinality());
    return Status::OK();
}

Status SegmentIterator::_init_return_column_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.use_page_cache = _opts.use_page_cache;
            iter_opts.rblock = _rblock.get();
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_bitmap_index_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) {
        if (_bitmap_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(
                    _segment->new_bitmap_index_iterator(cid, &_bitmap_index_iterators[cid]));
        }
    }
    return Status::OK();
}

// Schema of lhs and rhs are different.
// callers should assure that rhs' schema has all columns in lhs schema
template <typename LhsRowType, typename RhsRowType>
int compare_row_with_lhs_columns(const LhsRowType& lhs, const RhsRowType& rhs) {
    for (auto cid : lhs.schema()->column_ids()) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// look up one key to get its ordinal at which can get data.
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                        rowid_t* rowid) {
    std::string index_key;
    encode_key_with_padding(&index_key, key, _segment->num_short_keys(), is_include);

    uint32_t start_block_id = 0;
    auto start_iter = _segment->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = _segment->last_block();
    }
    rowid_t start = start_block_id * _segment->num_rows_per_block();

    rowid_t end = upper_bound;
    auto end_iter = _segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * _segment->num_rows_per_block();
    }

    // binary search to find the exact key
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        RETURN_IF_ERROR(_seek_and_peek(mid));
        int cmp = compare_row_with_lhs_columns(key, _seek_block->row(0));
        if (cmp > 0) {
            start = mid + 1;
        } else if (cmp == 0) {
            if (is_include) {
                // lower bound
                end = mid;
            } else {
                // upper bound
                start = mid + 1;
            }
        } else {
            end = mid;
        }
    }

    *rowid = start;
    return Status::OK();
}

// seek to the row and load that row to _key_cursor
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    RETURN_IF_ERROR(_seek_columns(_seek_schema->column_ids(), rowid));
    size_t num_rows = 1;
    // please note that usually RowBlockV2.clear() is called to free MemPool memory before reading the next block,
    // but here since there won't be too many keys to seek, we don't call RowBlockV2.clear() so that we can use
    // a single MemPool for all seeked keys.
    RETURN_IF_ERROR(_read_columns(_seek_schema->column_ids(), _seek_block.get(), 0, num_rows));
    _seek_block->set_num_rows(num_rows);
    return Status::OK();
}

void SegmentIterator::_init_lazy_materialization() {
    if (!_col_predicates.empty()) {
        std::set<ColumnId> predicate_columns;
        for (auto predicate : _col_predicates) {
            predicate_columns.insert(predicate->column_id());
        }
        _opts.delete_condition_predicates->get_all_column_ids(predicate_columns);

        // when all return columns have predicates, disable lazy materialization to avoid its overhead
        if (_schema.column_ids().size() > predicate_columns.size()) {
            _lazy_materialization_read = true;
            _predicate_columns.assign(predicate_columns.cbegin(), predicate_columns.cend());
            for (auto cid : _schema.column_ids()) {
                if (predicate_columns.find(cid) == predicate_columns.end()) {
                    _non_predicate_columns.push_back(cid);
                }
            }
        }
    }
}

Status SegmentIterator::_seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos) {
    _opts.stats->block_seek_num += 1;
    SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
    for (auto cid : column_ids) {
        RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids, RowBlockV2* block,
                                      size_t row_offset, size_t nrows) {
    for (auto cid : column_ids) {
        auto column_block = block->column_block(cid);
        ColumnBlockView dst(&column_block, row_offset);
        size_t rows_read = nrows;
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, &dst));
        DCHECK_EQ(nrows, rows_read);
    }
    return Status::OK();
}

Status SegmentIterator::next_batch(RowBlockV2* block) {
    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_inited)) {
        RETURN_IF_ERROR(_init());
        if (_lazy_materialization_read) {
            _block_rowids.reserve(block->capacity());
        }
        _inited = true;
    }

    uint32_t nrows_read = 0;
    uint32_t nrows_read_limit = block->capacity();
    _block_rowids.resize(nrows_read_limit);
    const auto& read_columns =
            _lazy_materialization_read ? _predicate_columns : block->schema()->column_ids();

    // phase 1: read rows selected by various index (indicated by _row_bitmap) into block
    // when using lazy-materialization-read, only columns with predicates are read
    do {
        uint32_t range_from;
        uint32_t range_to;
        bool has_next_range =
                _range_iter->next_range(nrows_read_limit - nrows_read, &range_from, &range_to);
        if (!has_next_range) {
            break;
        }
        if (_cur_rowid == 0 || _cur_rowid != range_from) {
            _cur_rowid = range_from;
            RETURN_IF_ERROR(_seek_columns(read_columns, _cur_rowid));
        }
        size_t rows_to_read = range_to - range_from;
        RETURN_IF_ERROR(_read_columns(read_columns, block, nrows_read, rows_to_read));
        _cur_rowid += rows_to_read;
        if (_lazy_materialization_read) {
            for (uint32_t rid = range_from; rid < range_to; rid++) {
                _block_rowids[nrows_read++] = rid;
            }
        } else {
            nrows_read += rows_to_read;
        }
    } while (nrows_read < nrows_read_limit);

    block->set_num_rows(nrows_read);
    block->set_selected_size(nrows_read);
    if (nrows_read == 0) {
        return Status::EndOfFile("no more data in segment");
    }
    _opts.stats->raw_rows_read += nrows_read;
    _opts.stats->blocks_load += 1;

    // phase 2: run vectorized evaluation on remaining predicates to prune rows.
    // block's selection vector will be set to indicate which rows have passed predicates.
    // TODO(hkp): optimize column predicate to check column block once for one column
    if (!_col_predicates.empty() || _opts.delete_condition_predicates != nullptr) {
        // init selection position index
        uint16_t selected_size = block->selected_size();
        uint16_t original_size = selected_size;

        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
        for (auto column_predicate : _col_predicates) {
            auto column_id = column_predicate->column_id();
            auto column_block = block->column_block(column_id);
            column_predicate->evaluate(&column_block, block->selection_vector(), &selected_size);
        }
        _opts.stats->rows_vec_cond_filtered += original_size - selected_size;

        // set original_size again to check delete condition predicates
        // filter how many data
        original_size = selected_size;
        _opts.delete_condition_predicates->evaluate(block, &selected_size);
        _opts.stats->rows_vec_del_cond_filtered += original_size - selected_size;

        block->set_selected_size(selected_size);
        block->set_num_rows(selected_size);
    }

    // phase 3: read non-predicate columns of rows that have passed predicates
    if (_lazy_materialization_read) {
        uint16_t i = 0;
        const uint16_t* sv = block->selection_vector();
        const uint16_t sv_size = block->selected_size();
        while (i < sv_size) {
            // i: start offset the current range
            // j: past the last offset of the current range
            uint16_t j = i + 1;
            while (j < sv_size && _block_rowids[sv[j]] == _block_rowids[sv[j - 1]] + 1) {
                ++j;
            }
            uint16_t range_size = j - i;
            RETURN_IF_ERROR(_seek_columns(_non_predicate_columns, _block_rowids[sv[i]]));
            RETURN_IF_ERROR(_read_columns(_non_predicate_columns, block, sv[i], range_size));
            i += range_size;
        }
    }
    return Status::OK();
}

/* ---------------------- for vecterization implementation  ---------------------- */

// todo(wb) need a UT here
void SegmentIterator::_vec_init_lazy_materialization() {
    _is_pred_column.resize(_schema.columns().size(), false);

    // including short_cir_pred_col_id_set and vec_pred_col_id_set
    std::set<ColumnId> pred_column_ids;
    _is_all_column_basic_type = true;
    bool is_predicate_column_exists = false;
    bool is_non_predicate_column_exists = false;

    std::set<ColumnId> del_cond_id_set;
    _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);

    if (!_col_predicates.empty() || !del_cond_id_set.empty()) {
        is_predicate_column_exists = true;

        std::set<ColumnId> short_cir_pred_col_id_set; // using set for distinct cid
        std::set<ColumnId> vec_pred_col_id_set;

        for (auto predicate : _col_predicates) {
            auto cid = predicate->column_id();
            FieldType type = _schema.column(cid)->type();
            _is_pred_column[cid] = true;
            pred_column_ids.insert(cid);

            if (type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_CHAR ||
                type == OLAP_FIELD_TYPE_STRING || predicate->type() == PredicateType::BF ||
                predicate->type() == PredicateType::IN_LIST ||
                predicate->type() == PredicateType::NO_IN_LIST) {
                short_cir_pred_col_id_set.insert(cid);
                _short_cir_eval_predicate.push_back(predicate);
                _is_all_column_basic_type = false;
            } else {
                vec_pred_col_id_set.insert(predicate->column_id());
                if (_pre_eval_block_predicate == nullptr) {
                    _pre_eval_block_predicate.reset(new AndBlockColumnPredicate());
                }
                _pre_eval_block_predicate->add_column_predicate(
                        new SingleColumnBlockPredicate(predicate));
            }
        }

        // handle delete_condition
        if (!del_cond_id_set.empty()) {
            short_cir_pred_col_id_set.insert(del_cond_id_set.begin(), del_cond_id_set.end());
            pred_column_ids.insert(del_cond_id_set.begin(), del_cond_id_set.end());
            _is_all_column_basic_type = false;

            for (auto cid : del_cond_id_set) {
                _is_pred_column[cid] = true;
            }
        }

        if (_schema.column_ids().size() > pred_column_ids.size()) {
            for (auto cid : _schema.column_ids()) {
                if (!_is_pred_column[cid]) {
                    _non_predicate_columns.push_back(cid);
                    is_non_predicate_column_exists = true;

                    // todo(wb) make a cost-based lazy-materialization framework
                    // check non-pred column type to decide whether using lazy-materialization
                    FieldType type = _schema.column(cid)->type();
                    if (_is_all_column_basic_type && (type == OLAP_FIELD_TYPE_HLL || type == OLAP_FIELD_TYPE_OBJECT 
                            || type == OLAP_FIELD_TYPE_VARCHAR || type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_STRING)) {
                        _is_all_column_basic_type = false;
                    }
                }
            }
        }

        _vec_pred_column_ids.assign(vec_pred_col_id_set.cbegin(), vec_pred_col_id_set.cend());
        _short_cir_pred_column_ids.assign(short_cir_pred_col_id_set.cbegin(),
                                          short_cir_pred_col_id_set.cend());
    } else {
        _is_all_column_basic_type = false;
        is_non_predicate_column_exists = true;
        for (auto cid : _schema.column_ids()) {
            _non_predicate_columns.push_back(cid);
        }
    }

    // note(wb) in following cases we disable lazy materialization
    // case 1: when all column is basic type(is_all_column_basic_type = true)
    //   because we think `seek and read` cost > read page cost, lazy materialize may cause more `seek and read`, so disable it
    // case 2: all column is predicate column
    // case 3: all column is not predicate column
    // todo(wb) need further research more lazy materialization rule, such as get more info from `statistics` for better decision
    if (_is_all_column_basic_type) {
        std::set<ColumnId> pred_set(_vec_pred_column_ids.begin(), _vec_pred_column_ids.end());
        std::set<ColumnId> non_pred_set(_non_predicate_columns.begin(),
                                        _non_predicate_columns.end());

        // when _is_all_column_basic_type = true, _first_read_column_ids should keep the same order with _schema.column_ids which stands for return column order
        for (int i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            if (pred_set.find(cid) != pred_set.end()) {
                _first_read_column_ids.push_back(cid);
            } else if (non_pred_set.find(cid) != non_pred_set.end()) {
                _first_read_column_ids.push_back(cid);
                // in this case, non-predicate column should also be filtered by sel idx, so we regard it as pred columns
                _is_pred_column[cid] = true;
            }
        }

    } else if (is_predicate_column_exists && !is_non_predicate_column_exists) {
        _first_read_column_ids.assign(pred_column_ids.cbegin(), pred_column_ids.cend());
    } else if (!is_predicate_column_exists && is_non_predicate_column_exists) {
        for (auto cid : _non_predicate_columns) {
            _first_read_column_ids.push_back(cid);
        }
    } else {
        _lazy_materialization_read = true;
        _first_read_column_ids.assign(pred_column_ids.cbegin(), pred_column_ids.cend());
    }

    // make _schema_block_id_map
    _schema_block_id_map.resize(_schema.columns().size());
    for (int i = 0; i < _schema.num_column_ids(); i++) {
        auto cid = _schema.column_id(i);
        _schema_block_id_map[cid] = i;
    }
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids,
                                      vectorized::MutableColumns& column_block, size_t nrows) {
    for (auto cid : column_ids) {
        auto& column = column_block[cid];
        size_t rows_read = nrows;
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, column));
        DCHECK_EQ(nrows, rows_read);
    }
    return Status::OK();
}

void SegmentIterator::_init_current_block(
        vectorized::Block* block, std::vector<vectorized::MutableColumnPtr>& current_columns) {
    _char_type_idx.clear();

    bool is_block_mem_reuse = block->mem_reuse();
    if (is_block_mem_reuse) {
        block->clear_column_data(_schema.num_column_ids());
    } else { // pre fill output block here
        for (size_t i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            auto column_desc = _schema.column(cid);
            auto data_type = Schema::get_data_type_ptr(*column_desc);
            block->insert({nullptr, std::move(data_type), column_desc->name()});
        }
    }

    for (size_t i = 0; i < _schema.num_column_ids(); i++) {
        auto cid = _schema.column_id(i);
        auto column_desc = _schema.column(cid);

        if (column_desc->type() == OLAP_FIELD_TYPE_CHAR) {
            _char_type_idx.emplace_back(i);
        }

        if (_is_pred_column[cid]) { //todo(wb) maybe we can relase it after output block
            current_columns[cid]->clear();
        } else { // non-predicate column
            if (is_block_mem_reuse) {
                current_columns[cid] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                auto data_type = Schema::get_data_type_ptr(*column_desc);
                current_columns[cid] = data_type->create_column();
            }
            if (column_desc->type() == OLAP_FIELD_TYPE_DATE) {
                current_columns[cid]->set_date_type();
            } else if (column_desc->type() == OLAP_FIELD_TYPE_DATETIME) {
                current_columns[cid]->set_datetime_type();
            }
            current_columns[cid]->reserve(_opts.block_row_max);
        }
    }
}

void SegmentIterator::_output_non_pred_columns(vectorized::Block* block, bool is_block_mem_reuse) {
    for (auto cid : _non_predicate_columns) {
        block->replace_by_position(_schema_block_id_map[cid],
                                   std::move(_current_return_columns[cid]));
    }
}

Status SegmentIterator::_read_columns_by_index(uint32_t nrows_read_limit, uint32_t& nrows_read,
                                               bool set_block_rowid) {
    do {
        uint32_t range_from;
        uint32_t range_to;
        bool has_next_range =
                _range_iter->next_range(nrows_read_limit - nrows_read, &range_from, &range_to);
        if (!has_next_range) {
            break;
        }
        if (_cur_rowid == 0 || _cur_rowid != range_from) {
            _cur_rowid = range_from;
            RETURN_IF_ERROR(_seek_columns(_first_read_column_ids, _cur_rowid));
        }
        size_t rows_to_read = range_to - range_from;
        RETURN_IF_ERROR(
                _read_columns(_first_read_column_ids, _current_return_columns, rows_to_read));
        _cur_rowid += rows_to_read;
        if (set_block_rowid) {
            for (uint32_t rid = range_from; rid < range_to; rid++) {
                _block_rowids[nrows_read++] = rid;
            }
        } else {
            nrows_read += rows_to_read;
        }
    } while (nrows_read < nrows_read_limit);
    return Status::OK();
}

void SegmentIterator::_evaluate_vectorization_predicate(uint16_t* sel_rowid_idx,
                                                        uint16_t& selected_size) {
    if (_vec_pred_column_ids.empty()) {
        for (uint32_t i = 0; i < selected_size; ++i) {
            sel_rowid_idx[i] = i;
        }
        return;
    }

    uint16_t original_size = selected_size;
    bool ret_flags[selected_size];
    memset(ret_flags, 1, selected_size);
    _pre_eval_block_predicate->evaluate_vec(_current_return_columns, selected_size, ret_flags);

    uint32_t sel_pos = 0;
    const uint32_t sel_end = sel_pos + selected_size;
    static constexpr size_t SIMD_BYTES = 32;
    const uint32_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;
    uint16_t new_size = 0;

    while (sel_pos < sel_end_simd) {
        auto mask = simd::bytes32_mask_to_bits32_mask(ret_flags + sel_pos);
        while (mask) {
            const size_t bit_pos = __builtin_ctzll(mask);
            sel_rowid_idx[new_size++] = sel_pos + bit_pos;
            mask = mask & (mask - 1);
        }
        sel_pos += SIMD_BYTES;
    }

    for (; sel_pos < sel_end; sel_pos++) {
        if (ret_flags[sel_pos]) {
            sel_rowid_idx[new_size++] = sel_pos;
        }
    }

    _opts.stats->rows_vec_cond_filtered += original_size - new_size;
    selected_size = new_size;
}

void SegmentIterator::_evaluate_short_circuit_predicate(uint16_t* vec_sel_rowid_idx,
                                                        uint16_t* selected_size_ptr) {
    if (_short_cir_pred_column_ids.empty()) {
        return;
    }

    for (auto predicate : _short_cir_eval_predicate) {
        auto column_id = predicate->column_id();
        auto& short_cir_column = _current_return_columns[column_id];
        auto* col_ptr = short_cir_column.get();
        // range comparison predicate needs to sort the dict and convert the encoding
        if (predicate->type() == PredicateType::LT || predicate->type() == PredicateType::LE ||
            predicate->type() == PredicateType::GT || predicate->type() == PredicateType::GE) {
            col_ptr->convert_dict_codes_if_necessary();
        }
        predicate->set_dict_code_if_necessary(*short_cir_column);
        predicate->evaluate(*short_cir_column, vec_sel_rowid_idx, selected_size_ptr);
    }

    // evaluate delete condition
    _opts.delete_condition_predicates->evaluate(_current_return_columns, vec_sel_rowid_idx,
                                                selected_size_ptr);
}

void SegmentIterator::_read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                              std::vector<rowid_t>& rowid_vector,
                                              uint16_t* sel_rowid_idx, size_t select_size,
                                              vectorized::MutableColumns* mutable_columns) {
    size_t start_idx = 0;
    while (start_idx < select_size) {
        size_t end_idx = start_idx + 1;
        while (end_idx < select_size && (rowid_vector[sel_rowid_idx[end_idx - 1]] ==
                                         rowid_vector[sel_rowid_idx[end_idx]] - 1)) {
            end_idx++;
        }
        size_t range = end_idx - start_idx;
        _seek_columns(read_column_ids, rowid_vector[sel_rowid_idx[start_idx]]);
        _read_columns(read_column_ids, *mutable_columns, range);
        start_idx += range;
    }
}

Status SegmentIterator::next_batch(vectorized::Block* block) {
    bool is_mem_reuse = block->mem_reuse();
    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_inited)) {
        RETURN_IF_ERROR(_init(true));
        _inited = true;
        if (!_vec_pred_column_ids.empty() || !_short_cir_pred_column_ids.empty()) {
            _block_rowids.resize(_opts.block_row_max);
        }
        _current_return_columns.resize(_schema.columns().size());
        for (size_t i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            if (_is_pred_column[cid]) {
                auto column_desc = _schema.column(cid);
                _current_return_columns[cid] = Schema::get_predicate_column_nullable_ptr(
                        column_desc->type(), column_desc->is_nullable());
                _current_return_columns[cid]->reserve(_opts.block_row_max);
            }
        }
    }

    _init_current_block(block, _current_return_columns);

    uint32_t nrows_read = 0;
    uint32_t nrows_read_limit = _opts.block_row_max;
    _read_columns_by_index(nrows_read_limit, nrows_read, _lazy_materialization_read);

    _opts.stats->blocks_load += 1;
    _opts.stats->raw_rows_read += nrows_read;

    if (nrows_read == 0) {
        for (int i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            // todo(wb) abstract make column where
            if (!_is_pred_column[cid]) { // non-predicate
                block->replace_by_position(i, std::move(_current_return_columns[cid]));
            } else { // predicate
                if (!is_mem_reuse) {
                    auto column_desc = _schema.column(cid);
                    auto data_type = Schema::get_data_type_ptr(*column_desc);
                    block->replace_by_position(i, data_type->create_column());
                }
            }
        }
        // not sure whether block is clear before enter segmentIter, so clear it here.
        if (is_mem_reuse) {
            block->clear_column_data();
        }
        return Status::EndOfFile("no more data in segment");
    }

    // when no predicate(include delete condition) is provided, output column directly
    if (_vec_pred_column_ids.empty() && _short_cir_pred_column_ids.empty()) {
        _output_non_pred_columns(block, is_mem_reuse);
    } else { // need predicate evaluation
        uint16_t selected_size = nrows_read;
        uint16_t sel_rowid_idx[selected_size];

        // step 1: evaluate vectorization predicate
        _evaluate_vectorization_predicate(sel_rowid_idx, selected_size);

        // When predicate column and no-predicate column are both basic type, lazy materialization is eliminate
        // So output block directly after vectorization evaluation
        if (_is_all_column_basic_type) {
            RETURN_IF_ERROR(_output_column_by_sel_idx(block, _first_read_column_ids, sel_rowid_idx,
                                                      selected_size, is_mem_reuse));
        } else {
            // step 2: evaluate short ciruit predicate
            // todo(wb) research whether need to read short predicate after vectorization evaluation
            //          to reduce cost of read short circuit columns.
            //          In SSB test, it make no difference; So need more scenarios to test
            _evaluate_short_circuit_predicate(sel_rowid_idx, &selected_size);

            // step3: read non_predicate column
            if (!_non_predicate_columns.empty()) {
                _read_columns_by_rowids(_non_predicate_columns, _block_rowids, sel_rowid_idx,
                                        selected_size, &_current_return_columns);
            }

            // step4: output columns
            // 4.1 output non-predicate column
            _output_non_pred_columns(block, is_mem_reuse);

            // 4.2 get union of short_cir_pred and vec_pred
            std::set<ColumnId> pred_column_ids;
            pred_column_ids.insert(_short_cir_pred_column_ids.begin(),
                                   _short_cir_pred_column_ids.end());
            pred_column_ids.insert(_vec_pred_column_ids.begin(), _vec_pred_column_ids.end());

            // 4.3 output short circuit and predicate column
            RETURN_IF_ERROR(_output_column_by_sel_idx(block, pred_column_ids, sel_rowid_idx,
                                                      selected_size, is_mem_reuse));
        }
    }

    // shink char_type suffix zero data
    block->shrink_char_type_column_suffix_zero(_char_type_idx);

    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
