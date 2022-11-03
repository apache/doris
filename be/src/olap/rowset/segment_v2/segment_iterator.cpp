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

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/short_key_index.h"
#include "util/doris_metrics.h"
#include "util/key_util.h"
#include "util/simd/bits.h"

namespace doris {
namespace segment_v2 {

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,7), [7,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class SegmentIterator::BitmapRangeIterator {
public:
    BitmapRangeIterator() {}
    virtual ~BitmapRangeIterator() = default;

    explicit BitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator(&bitmap.roaring, &_iter);
        _read_next_batch();
    }

    bool has_more_range() const { return !_eof; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    virtual bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }

        *from = _buf[_buf_pos];
        uint32_t range_size = 0;
        uint32_t expect_val = _buf[_buf_pos]; // this initial value just make first batch valid

        // if array is contiguous sequence then the following conditions need to be met :
        // a_0: x
        // a_1: x+1
        // a_2: x+2
        // ...
        // a_p: x+p
        // so we can just use (a_p-a_0)-p to check conditions
        // and should notice the previous batch needs to be continuous with the current batch
        while (!_eof && range_size + _buf_size - _buf_pos <= max_range_size &&
               expect_val == _buf[_buf_pos] &&
               _buf[_buf_size - 1] - _buf[_buf_pos] == _buf_size - 1 - _buf_pos) {
            range_size += _buf_size - _buf_pos;
            expect_val = _buf[_buf_size - 1] + 1;
            _read_next_batch();
        }

        // promise remain range not will reach next batch
        if (!_eof && range_size < max_range_size && expect_val == _buf[_buf_pos]) {
            do {
                _buf_pos++;
                range_size++;
            } while (range_size < max_range_size && _buf[_buf_pos] == _buf[_buf_pos - 1] + 1);
        }
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

// A backward range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: , [15,20), [10,11), [4,8), [0,2) (when max_range_size=10)
//   output ranges: [17,20), [15,17), [10,11), [5,8), [4, 5), [0,2) (when max_range_size=3)
class SegmentIterator::BackwardBitmapRangeIterator : public SegmentIterator::BitmapRangeIterator {
public:
    explicit BackwardBitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator_last(&bitmap.roaring, &_riter);
    }

    bool has_more_range() const { return !_riter.has_value; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) override {
        if (!_riter.has_value) {
            return false;
        }

        uint32_t range_size = 0;
        *to = _riter.current_value + 1;

        do {
            *from = _riter.current_value;
            range_size++;
            roaring_previous_uint32_iterator(&_riter);
        } while (range_size < max_range_size && _riter.has_value &&
                 _riter.current_value + 1 == *from);

        return true;
    }

private:
    roaring::api::roaring_uint32_iterator_t _riter;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, const Schema& schema)
        : _segment(std::move(segment)),
          _schema(schema),
          _cur_rowid(0),
          _lazy_materialization_read(false),
          _inited(false),
          _estimate_row_size(true) {}

SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter.second;
    }
    for (auto iter : _bitmap_index_iterators) {
        delete iter.second;
    }
}

Status SegmentIterator::init(const StorageReadOptions& opts) {
    _opts = opts;
    if (!opts.column_predicates.empty()) {
        _col_predicates = opts.column_predicates;
    }
    // Read options will not change, so that just reserve here
    _block_rowids.reserve(_opts.block_row_max);
    return Status::OK();
}

Status SegmentIterator::_init(bool is_vec) {
    SCOPED_RAW_TIMER(&_opts.stats->block_init_ns);
    DorisMetrics::instance()->segment_read_total->increment(1);
    // get file handle from file descriptor of segment
    _file_reader = _segment->_file_reader;

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
        _vec_init_char_column_id();
    } else {
        _init_lazy_materialization();
    }
    // Remove rows that have been marked deleted
    if (_opts.delete_bitmap.count(segment_id()) > 0 &&
        _opts.delete_bitmap[segment_id()] != nullptr) {
        size_t pre_size = _row_bitmap.cardinality();
        _row_bitmap -= *(_opts.delete_bitmap[segment_id()]);
        _opts.stats->rows_del_by_bitmap += (pre_size - _row_bitmap.cardinality());
        VLOG_DEBUG << "read on segment: " << segment_id() << ", delete bitmap cardinality: "
                   << _opts.delete_bitmap[segment_id()]->cardinality() << ", "
                   << _opts.stats->rows_del_by_bitmap << " rows deleted by bitmap";
    }
    if (_opts.read_orderby_key_reverse) {
        _range_iter.reset(new BackwardBitmapRangeIterator(_row_bitmap));
    } else {
        _range_iter.reset(new BitmapRangeIterator(_row_bitmap));
    }
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
        int32_t unique_id = _opts.tablet_schema->column(cid).unique_id();
        if (_column_iterators.count(unique_id) < 1) {
            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[unique_id]));
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.file_reader = _file_reader.get();
            RETURN_IF_ERROR(_column_iterators[unique_id]->init(iter_opts));
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
        (!_opts.col_id_to_predicates.empty() ||
         _opts.delete_condition_predicates->num_of_column_predicate() > 0)) {
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
    for (auto& entry : _opts.col_id_to_predicates) {
        cids.insert(entry.first);
    }

    // first filter data by bloom filter index
    // bloom filter index only use CondColumn
    RowRanges bf_row_ranges = RowRanges::create_single(num_rows());
    for (auto& cid : cids) {
        // get row ranges by bf index of this column,
        RowRanges column_bf_row_ranges = RowRanges::create_single(num_rows());
        DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->get_row_ranges_by_bloom_filter(
                _opts.col_id_to_predicates[cid].get(), &column_bf_row_ranges));
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
        DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->get_row_ranges_by_zone_map(
                _opts.col_id_to_predicates[cid].get(),
                _opts.col_id_to_del_predicates.count(cid) > 0
                        ? &(_opts.col_id_to_del_predicates[cid])
                        : nullptr,
                &column_row_ranges));
        // intersect different columns's row ranges to get final row ranges by zone map
        RowRanges::ranges_intersection(zone_map_row_ranges, column_row_ranges,
                                       &zone_map_row_ranges);
    }

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
        int32_t unique_id = _schema.unique_id(pred->column_id());
        if (_bitmap_index_iterators.count(unique_id) < 1 ||
            _bitmap_index_iterators[unique_id] == nullptr) {
            // no bitmap index for this column
            remaining_predicates.push_back(pred);
        } else {
            RETURN_IF_ERROR(pred->evaluate(_bitmap_index_iterators[unique_id], _segment->num_rows(),
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
        int32_t unique_id = _opts.tablet_schema->column(cid).unique_id();
        if (_column_iterators.count(unique_id) < 1) {
            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[unique_id]));
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.use_page_cache = _opts.use_page_cache;
            iter_opts.file_reader = _file_reader.get();
            RETURN_IF_ERROR(_column_iterators[unique_id]->init(iter_opts));
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_bitmap_index_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) {
        int32_t unique_id = _opts.tablet_schema->column(cid).unique_id();
        if (_bitmap_index_iterators.count(unique_id) < 1) {
            RETURN_IF_ERROR(_segment->new_bitmap_index_iterator(
                    _opts.tablet_schema->column(cid), &_bitmap_index_iterators[unique_id]));
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

Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                        rowid_t* rowid) {
    if (_segment->_tablet_schema->keys_type() == UNIQUE_KEYS &&
        _segment->get_primary_key_index() != nullptr) {
        return _lookup_ordinal_from_pk_index(key, is_include, rowid);
    }
    return _lookup_ordinal_from_sk_index(key, is_include, upper_bound, rowid);
}

// look up one key to get its ordinal at which can get data by using short key index.
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
Status SegmentIterator::_lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include,
                                                      rowid_t upper_bound, rowid_t* rowid) {
    const ShortKeyIndexDecoder* sk_index_decoder = _segment->get_short_key_index();
    DCHECK(sk_index_decoder != nullptr);

    std::string index_key;
    encode_key_with_padding(&index_key, key, _segment->_tablet_schema->num_short_key_columns(),
                            is_include);

    uint32_t start_block_id = 0;
    auto start_iter = sk_index_decoder->lower_bound(index_key);
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
        start_block_id = sk_index_decoder->num_items() - 1;
    }
    rowid_t start = start_block_id * sk_index_decoder->num_rows_per_block();

    rowid_t end = upper_bound;
    auto end_iter = sk_index_decoder->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * sk_index_decoder->num_rows_per_block();
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

Status SegmentIterator::_lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include,
                                                      rowid_t* rowid) {
    DCHECK(_segment->_tablet_schema->keys_type() == UNIQUE_KEYS);
    const PrimaryKeyIndexReader* pk_index_reader = _segment->get_primary_key_index();
    DCHECK(pk_index_reader != nullptr);

    std::string index_key;
    encode_key_with_padding<RowCursor, true, true>(
            &index_key, key, _segment->_tablet_schema->num_key_columns(), is_include);
    if (index_key < _segment->min_key()) {
        *rowid = 0;
        return Status::OK();
    } else if (index_key > _segment->max_key()) {
        *rowid = num_rows();
        return Status::OK();
    }
    bool exact_match = false;

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(pk_index_reader->new_iterator(&index_iterator));

    Status status = index_iterator->seek_at_or_after(&index_key, &exact_match);
    if (UNLIKELY(!status.ok())) {
        *rowid = num_rows();
        if (status.is_not_found()) {
            return Status::OK();
        }
        return status;
    }
    *rowid = index_iterator->get_current_ordinal();

    // find the key in primary key index, and the is_include is false, so move
    // to the next row.
    if (exact_match && !is_include) {
        *rowid += 1;
    }
    return Status::OK();
}

// seek to the row and load that row to _key_cursor
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    {
        _opts.stats->block_init_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_init_seek_ns);
        RETURN_IF_ERROR(_seek_columns(_seek_schema->column_ids(), rowid));
    }
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

        // ARRAY column do not support lazy materialization read
        for (auto cid : _schema.column_ids()) {
            if (_schema.column(cid)->type() == OLAP_FIELD_TYPE_ARRAY) {
                predicate_columns.insert(cid);
            }
        }

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
    for (auto cid : column_ids) {
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids, RowBlockV2* block,
                                      size_t row_offset, size_t nrows) {
    for (auto cid : column_ids) {
        auto column_block = block->column_block(cid);
        ColumnBlockView dst(&column_block, row_offset);
        size_t rows_read = nrows;
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->next_batch(&rows_read, &dst));
        DCHECK_EQ(nrows, rows_read);
    }
    return Status::OK();
}

Status SegmentIterator::next_batch(RowBlockV2* block) {
    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_inited)) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

    uint32_t nrows_read = 0;
    uint32_t nrows_read_limit = block->capacity();
    const auto& read_columns =
            _lazy_materialization_read ? _predicate_columns : block->schema()->column_ids();

    // phase 1: read rows selected by various index (indicated by _row_bitmap) into block
    // when using lazy-materialization-read, only columns with predicates are read
    {
        SCOPED_RAW_TIMER(&_opts.stats->first_read_ns);
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
                _opts.stats->block_first_read_seek_num += 1;
                SCOPED_RAW_TIMER(&_opts.stats->block_first_read_seek_ns);
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
    }

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
        SCOPED_RAW_TIMER(&_opts.stats->lazy_read_ns);
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
            {
                _opts.stats->block_lazy_read_seek_num += 1;
                SCOPED_RAW_TIMER(&_opts.stats->block_lazy_read_seek_ns);
                RETURN_IF_ERROR(_seek_columns(_non_predicate_columns, _block_rowids[sv[i]]));
            }
            RETURN_IF_ERROR(_read_columns(_non_predicate_columns, block, sv[i], range_size));
            i += range_size;
        }
    }
    return Status::OK();
}

/* ---------------------- for vecterization implementation  ---------------------- */

/**
 *  For storage layer data type, can be measured from two perspectives:
 *  1 Whether the type can be read in a fast way(batch read using SIMD)
 *    Such as integer type and float type, this type can be read in SIMD way.
 *    For the type string/bitmap/hll, they can not be read in batch way, so read this type data is slow.
 *   If a type can be read fast, we can try to eliminate Lazy Materialization, because we think for this type, seek cost > read cost.
 *   This is an estimate, if we want more precise cost, statistics collection is necessary(this is a todo).
 *   In short, when returned non-pred columns contains string/hll/bitmap, we using Lazy Materialization.
 *   Otherwise, we disable it.
 *    
 *   When Lazy Materialization enable, we need to read column at least two times.
 *   First time to read Pred col, second time to read non-pred.
 *   Here's an interesting question to research, whether read Pred col once is the best plan.
 *   (why not read Pred col twice or more?)
 *
 *   When Lazy Materialization disable, we just need to read once.
 *   
 * 
 *  2 Whether the predicate type can be evaluate in a fast way(using SIMD to eval pred)
 *    Such as integer type and float type, they can be eval fast.
 *    But for BloomFilter/string/date, they eval slow.
 *    If a type can be eval fast, we use vectorization to eval it.
 *    Otherwise, we use short-circuit to eval it.
 * 
 *  
 */

// todo(wb) need a UT here
void SegmentIterator::_vec_init_lazy_materialization() {
    _is_pred_column.resize(_schema.columns().size(), false);

    // including short/vec/delete pred
    std::set<ColumnId> pred_column_ids;
    _lazy_materialization_read = false;

    std::set<ColumnId> del_cond_id_set;
    _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);

    std::set<const ColumnPredicate*> delete_predicate_set {};
    _opts.delete_condition_predicates->get_all_column_predicate(delete_predicate_set);
    for (const auto predicate : delete_predicate_set) {
        if (PredicateTypeTraits::is_range(predicate->type())) {
            _delete_range_column_ids.push_back(predicate->column_id());
        } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
            _delete_bloom_filter_column_ids.push_back(predicate->column_id());
        }
    }

    if (!_col_predicates.empty() || !del_cond_id_set.empty()) {
        std::set<ColumnId> short_cir_pred_col_id_set; // using set for distinct cid
        std::set<ColumnId> vec_pred_col_id_set;

        for (auto predicate : _col_predicates) {
            auto cid = predicate->column_id();
            _is_pred_column[cid] = true;
            pred_column_ids.insert(cid);

            // Step1: check pred using short eval or vec eval
            if (_can_evaluated_by_vectorized(predicate)) {
                vec_pred_col_id_set.insert(predicate->column_id());
                _pre_eval_block_predicate.push_back(predicate);
            } else {
                short_cir_pred_col_id_set.insert(cid);
                _short_cir_eval_predicate.push_back(predicate);
            }
        }

        // handle delete_condition
        if (!del_cond_id_set.empty()) {
            short_cir_pred_col_id_set.insert(del_cond_id_set.begin(), del_cond_id_set.end());
            pred_column_ids.insert(del_cond_id_set.begin(), del_cond_id_set.end());

            for (auto cid : del_cond_id_set) {
                _is_pred_column[cid] = true;
            }
        }

        _vec_pred_column_ids.assign(vec_pred_col_id_set.cbegin(), vec_pred_col_id_set.cend());
        _short_cir_pred_column_ids.assign(short_cir_pred_col_id_set.cbegin(),
                                          short_cir_pred_col_id_set.cend());
    }

    if (!_vec_pred_column_ids.empty()) {
        _is_need_vec_eval = true;
    }
    if (!_short_cir_pred_column_ids.empty()) {
        _is_need_short_eval = true;
    }

    // Step 2: check non-predicate read costs to determine whether need lazy materialization
    // fill _non_predicate_columns.
    // After some optimization, we suppose lazy materialization is better performance.
    if (_schema.column_ids().size() > pred_column_ids.size()) {
        for (auto cid : _schema.column_ids()) {
            if (!_is_pred_column[cid]) {
                _non_predicate_columns.push_back(cid);
                if (_is_need_vec_eval || _is_need_short_eval) {
                    _lazy_materialization_read = true;
                }
            }
        }
    }

    // Step 3: fill column ids for read and output
    if (_lazy_materialization_read) {
        // insert pred cid to first_read_columns
        for (auto cid : pred_column_ids) {
            _first_read_column_ids.push_back(cid);
        }
    } else if (!_is_need_vec_eval &&
               !_is_need_short_eval) { // no pred exists, just read and output column
        for (int i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            _first_read_column_ids.push_back(cid);
        }
    } else { // pred exits, but we can eliminate lazy materialization
        // insert pred/non-pred cid to first read columns
        std::set<ColumnId> pred_id_set;
        pred_id_set.insert(_short_cir_pred_column_ids.begin(), _short_cir_pred_column_ids.end());
        pred_id_set.insert(_vec_pred_column_ids.begin(), _vec_pred_column_ids.end());
        std::set<ColumnId> non_pred_set(_non_predicate_columns.begin(),
                                        _non_predicate_columns.end());

        for (int i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            if (pred_id_set.find(cid) != pred_id_set.end()) {
                _first_read_column_ids.push_back(cid);
            } else if (non_pred_set.find(cid) != non_pred_set.end()) {
                _first_read_column_ids.push_back(cid);
                // when _lazy_materialization_read = false, non-predicate column should also be filtered by sel idx, so we regard it as pred columns
                _is_pred_column[cid] = true;
            }
        }
    }

    // make _schema_block_id_map
    _schema_block_id_map.resize(_schema.columns().size());
    for (int i = 0; i < _schema.num_column_ids(); i++) {
        auto cid = _schema.column_id(i);
        _schema_block_id_map[cid] = i;
    }
}

bool SegmentIterator::_can_evaluated_by_vectorized(ColumnPredicate* predicate) {
    auto cid = predicate->column_id();
    FieldType field_type = _schema.column(cid)->type();
    switch (predicate->type()) {
    case PredicateType::EQ:
    case PredicateType::NE:
    case PredicateType::LE:
    case PredicateType::LT:
    case PredicateType::GE:
    case PredicateType::GT: {
        if (field_type == OLAP_FIELD_TYPE_VARCHAR || field_type == OLAP_FIELD_TYPE_CHAR ||
            field_type == OLAP_FIELD_TYPE_STRING) {
            return config::enable_low_cardinality_optimize &&
                   _column_iterators[_schema.unique_id(cid)]->is_all_dict_encoding();
        } else if (field_type == OLAP_FIELD_TYPE_DECIMAL) {
            return false;
        }
        return true;
    }
    default:
        return false;
    }
}

void SegmentIterator::_vec_init_char_column_id() {
    for (size_t i = 0; i < _schema.num_column_ids(); i++) {
        auto cid = _schema.column_id(i);
        auto column_desc = _schema.column(cid);

        do {
            if (column_desc->type() == OLAP_FIELD_TYPE_CHAR) {
                _char_type_idx.emplace_back(i);
                break;
            } else if (column_desc->type() != OLAP_FIELD_TYPE_ARRAY) {
                break;
            }
            // for Array<Char> or Array<Array<Char>>
            column_desc = column_desc->get_sub_field(0);
        } while (column_desc != nullptr);
    }
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids,
                                      vectorized::MutableColumns& column_block, size_t nrows) {
    for (auto cid : column_ids) {
        auto& column = column_block[cid];
        size_t rows_read = nrows;
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->next_batch(&rows_read, column));
        DCHECK_EQ(nrows, rows_read);
    }
    return Status::OK();
}

void SegmentIterator::_init_current_block(
        vectorized::Block* block, std::vector<vectorized::MutableColumnPtr>& current_columns) {
    block->clear_column_data(_schema.num_column_ids());

    for (size_t i = 0; i < _schema.num_column_ids(); i++) {
        auto cid = _schema.column_id(i);
        auto column_desc = _schema.column(cid);

        // the column in block must clear() here to insert new data
        if (_is_pred_column[cid] ||
            i >= block->columns()) { //todo(wb) maybe we can release it after output block
            current_columns[cid]->clear();
        } else { // non-predicate column
            current_columns[cid] = std::move(*block->get_by_position(i).column).mutate();

            if (column_desc->type() == OLAP_FIELD_TYPE_DATE) {
                current_columns[cid]->set_date_type();
            } else if (column_desc->type() == OLAP_FIELD_TYPE_DATETIME) {
                current_columns[cid]->set_datetime_type();
            } else if (column_desc->type() == OLAP_FIELD_TYPE_DECIMAL) {
                current_columns[cid]->set_decimalv2_type();
            }
            current_columns[cid]->reserve(_opts.block_row_max);
        }
    }
}

void SegmentIterator::_output_non_pred_columns(vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->output_col_ns);
    for (auto cid : _non_predicate_columns) {
        auto loc = _schema_block_id_map[cid];
        // if loc < block->block->columns() means the column is delete column and should
        // not output by block, so just skip the column.
        if (loc < block->columns()) {
            block->replace_by_position(loc, std::move(_current_return_columns[cid]));
        }
    }
}

Status SegmentIterator::_read_columns_by_index(uint32_t nrows_read_limit, uint32_t& nrows_read,
                                               bool set_block_rowid) {
    SCOPED_RAW_TIMER(&_opts.stats->first_read_ns);
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
            _opts.stats->block_first_read_seek_num += 1;
            SCOPED_RAW_TIMER(&_opts.stats->block_first_read_seek_ns);
            RETURN_IF_ERROR(_seek_columns(_first_read_column_ids, _cur_rowid));
        }
        size_t rows_to_read = range_to - range_from;
        RETURN_IF_ERROR(
                _read_columns(_first_read_column_ids, _current_return_columns, rows_to_read));
        _cur_rowid += rows_to_read;
        if (set_block_rowid) {
            // Here use std::iota is better performance than for-loop, maybe for-loop is not vectorized
            auto start = _block_rowids.data() + nrows_read;
            auto end = start + rows_to_read;
            std::iota(start, end, range_from);
            nrows_read += rows_to_read;
        } else {
            nrows_read += rows_to_read;
        }
        // if _opts.read_orderby_key_reverse is true, only read one range for fast reverse purpose
    } while (nrows_read < nrows_read_limit && !_opts.read_orderby_key_reverse);
    return Status::OK();
}

uint16_t SegmentIterator::_evaluate_vectorization_predicate(uint16_t* sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
    if (!_is_need_vec_eval) {
        for (uint32_t i = 0; i < selected_size; ++i) {
            sel_rowid_idx[i] = i;
        }
        return selected_size;
    }

    uint16_t original_size = selected_size;
    bool ret_flags[original_size];
    DCHECK(_pre_eval_block_predicate.size() > 0);
    auto column_id = _pre_eval_block_predicate[0]->column_id();
    auto& column = _current_return_columns[column_id];
    _pre_eval_block_predicate[0]->evaluate_vec(*column, original_size, ret_flags);
    for (int i = 1; i < _pre_eval_block_predicate.size(); i++) {
        auto column_id2 = _pre_eval_block_predicate[i]->column_id();
        auto& column2 = _current_return_columns[column_id2];
        _pre_eval_block_predicate[i]->evaluate_and_vec(*column2, original_size, ret_flags);
    }

    uint16_t new_size = 0;

    uint32_t sel_pos = 0;
    const uint32_t sel_end = sel_pos + selected_size;
    static constexpr size_t SIMD_BYTES = 32;
    const uint32_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;

    while (sel_pos < sel_end_simd) {
        auto mask = simd::bytes32_mask_to_bits32_mask(ret_flags + sel_pos);
        if (0 == mask) {
            //pass
        } else if (0xffffffff == mask) {
            for (uint32_t i = 0; i < SIMD_BYTES; i++) {
                sel_rowid_idx[new_size++] = sel_pos + i;
            }
        } else {
            while (mask) {
                const size_t bit_pos = __builtin_ctzll(mask);
                sel_rowid_idx[new_size++] = sel_pos + bit_pos;
                mask = mask & (mask - 1);
            }
        }
        sel_pos += SIMD_BYTES;
    }

    for (; sel_pos < sel_end; sel_pos++) {
        if (ret_flags[sel_pos]) {
            sel_rowid_idx[new_size++] = sel_pos;
        }
    }

    _opts.stats->rows_vec_cond_filtered += original_size - new_size;
    return new_size;
}

uint16_t SegmentIterator::_evaluate_short_circuit_predicate(uint16_t* vec_sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->short_cond_ns);
    if (!_is_need_short_eval) {
        return selected_size;
    }

    uint16_t original_size = selected_size;
    for (auto predicate : _short_cir_eval_predicate) {
        auto column_id = predicate->column_id();
        auto& short_cir_column = _current_return_columns[column_id];
        selected_size = predicate->evaluate(*short_cir_column, vec_sel_rowid_idx, selected_size);
    }
    _opts.stats->rows_vec_cond_filtered += original_size - selected_size;

    // evaluate delete condition
    original_size = selected_size;
    selected_size = _opts.delete_condition_predicates->evaluate(_current_return_columns,
                                                                vec_sel_rowid_idx, selected_size);
    _opts.stats->rows_vec_del_cond_filtered += original_size - selected_size;
    return selected_size;
}

Status SegmentIterator::_read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                                std::vector<rowid_t>& rowid_vector,
                                                uint16_t* sel_rowid_idx, size_t select_size) {
    SCOPED_RAW_TIMER(&_opts.stats->lazy_read_ns);
    std::vector<rowid_t> rowids(select_size);
    for (size_t i = 0; i < select_size; ++i) {
        rowids[i] = rowid_vector[sel_rowid_idx[i]];
    }
    for (auto cid : read_column_ids) {
        RETURN_IF_ERROR(_column_iterators[_schema.unique_id(cid)]->read_by_rowids(
                rowids.data(), select_size, _current_return_columns[cid]));
    }

    return Status::OK();
}

Status SegmentIterator::next_batch(vectorized::Block* block) {
    bool is_mem_reuse = block->mem_reuse();
    DCHECK(is_mem_reuse);

    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_inited)) {
        RETURN_IF_ERROR(_init(true));
        _inited = true;
        if (_lazy_materialization_read || _opts.record_rowids) {
            _block_rowids.resize(_opts.block_row_max);
        }
        _current_return_columns.resize(_schema.columns().size());
        for (size_t i = 0; i < _schema.num_column_ids(); i++) {
            auto cid = _schema.column_id(i);
            auto column_desc = _schema.column(cid);
            if (_is_pred_column[cid]) {
                _current_return_columns[cid] =
                        Schema::get_predicate_column_nullable_ptr(*column_desc);
                _current_return_columns[cid]->reserve(_opts.block_row_max);
            } else if (i >= block->columns()) {
                // if i >= block->columns means the column and not the pred_column means `column i` is
                // a delete condition column. but the column is not effective in the segment. so we just
                // create a column to hold the data.
                // a. origin data -> b. delete condition -> c. new load data
                // the segment of c do not effective delete condition, but it still need read the column
                // to match the schema.
                // TODO: skip read the not effective delete column to speed up segment read.
                _current_return_columns[cid] =
                        Schema::get_data_type_ptr(*column_desc)->create_column();
                _current_return_columns[cid]->reserve(_opts.block_row_max);
            }
        }
    }

    _init_current_block(block, _current_return_columns);

    _current_batch_rows_read = 0;
    uint32_t nrows_read_limit = _opts.block_row_max;
    if (UNLIKELY(_estimate_row_size)) {
        // read 100 rows to estimate average row size
        nrows_read_limit = 100;
    }
    _read_columns_by_index(nrows_read_limit, _current_batch_rows_read,
                           _lazy_materialization_read || _opts.record_rowids);

    _opts.stats->blocks_load += 1;
    _opts.stats->raw_rows_read += _current_batch_rows_read;

    if (_current_batch_rows_read == 0) {
        for (int i = 0; i < block->columns(); i++) {
            auto cid = _schema.column_id(i);
            // todo(wb) abstract make column where
            if (!_is_pred_column[cid]) { // non-predicate
                block->replace_by_position(i, std::move(_current_return_columns[cid]));
            }
        }
        block->clear_column_data();
        return Status::EndOfFile("no more data in segment");
    }

    if (!_is_need_vec_eval && !_is_need_short_eval) {
        _output_non_pred_columns(block);
    } else {
        _convert_dict_code_for_predicate_if_necessary();
        uint16_t selected_size = _current_batch_rows_read;
        uint16_t sel_rowid_idx[selected_size];

        // step 1: evaluate vectorization predicate
        selected_size = _evaluate_vectorization_predicate(sel_rowid_idx, selected_size);

        // step 2: evaluate short ciruit predicate
        // todo(wb) research whether need to read short predicate after vectorization evaluation
        //          to reduce cost of read short circuit columns.
        //          In SSB test, it make no difference; So need more scenarios to test
        selected_size = _evaluate_short_circuit_predicate(sel_rowid_idx, selected_size);

        if (UNLIKELY(_opts.record_rowids)) {
            _sel_rowid_idx.reserve(selected_size);
            _selected_size = selected_size;
            for (auto i = 0; i < _selected_size; i++) {
                _sel_rowid_idx[i] = sel_rowid_idx[i];
            }
        }

        if (!_lazy_materialization_read) {
            Status ret = Status::OK();
            if (selected_size > 0) {
                ret = _output_column_by_sel_idx(block, _first_read_column_ids, sel_rowid_idx,
                                                selected_size);
            }
            if (!ret.ok()) {
                return ret;
            }
            // shrink char_type suffix zero data
            block->shrink_char_type_column_suffix_zero(_char_type_idx);

            if (UNLIKELY(_estimate_row_size) && block->rows() > 0) {
                _update_max_row(block);
            }
            return ret;
        }

        // step3: read non_predicate column
        RETURN_IF_ERROR(_read_columns_by_rowids(_non_predicate_columns, _block_rowids,
                                                sel_rowid_idx, selected_size));

        // step4: output columns
        // 4.1 output non-predicate column
        _output_non_pred_columns(block);

        // 4.3 output short circuit and predicate column
        // when lazy materialization enables, _first_read_column_ids = distinct(_short_cir_pred_column_ids + _vec_pred_column_ids)
        // see _vec_init_lazy_materialization
        // todo(wb) need to tell input columnids from output columnids
        if (selected_size > 0) {
            RETURN_IF_ERROR(_output_column_by_sel_idx(block, _first_read_column_ids, sel_rowid_idx,
                                                      selected_size));
        }
    }

    // shrink char_type suffix zero data
    block->shrink_char_type_column_suffix_zero(_char_type_idx);

    if (UNLIKELY(_estimate_row_size) && block->rows() > 0) {
        _update_max_row(block);
    }

    // reverse block row order
    if (_opts.read_orderby_key_reverse) {
        size_t num_rows = block->rows();
        size_t num_columns = block->columns();
        vectorized::IColumn::Permutation permutation;
        for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);

        for (size_t i = 0; i < num_columns; ++i)
            block->get_by_position(i).column =
                    block->get_by_position(i).column->permute(permutation, num_rows);
    }

    return Status::OK();
}

void SegmentIterator::_convert_dict_code_for_predicate_if_necessary() {
    for (auto predicate : _short_cir_eval_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto predicate : _pre_eval_block_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto column_id : _delete_range_column_ids) {
        _current_return_columns[column_id].get()->convert_dict_codes_if_necessary();
    }

    for (auto column_id : _delete_bloom_filter_column_ids) {
        _current_return_columns[column_id].get()->generate_hash_values_for_runtime_filter();
    }
}

void SegmentIterator::_convert_dict_code_for_predicate_if_necessary_impl(
        ColumnPredicate* predicate) {
    auto& column = _current_return_columns[predicate->column_id()];
    auto* col_ptr = column.get();
    column->set_rowset_segment_id({_segment->rowset_id(), _segment->id()});

    if (PredicateTypeTraits::is_range(predicate->type())) {
        col_ptr->convert_dict_codes_if_necessary();
    } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
        col_ptr->generate_hash_values_for_runtime_filter();
    }
}

void SegmentIterator::_update_max_row(const vectorized::Block* block) {
    _estimate_row_size = false;
    auto avg_row_size = block->bytes() / block->rows();

    int block_row_max = config::doris_scan_block_max_mb / avg_row_size;
    _opts.block_row_max = std::min(block_row_max, _opts.block_row_max);
}

Status SegmentIterator::current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
    DCHECK(_opts.record_rowids);
    DCHECK_GE(_block_rowids.size(), _current_batch_rows_read);
    uint32_t sid = segment_id();
    if (!_is_need_vec_eval && !_is_need_short_eval) {
        block_row_locations->resize(_current_batch_rows_read);
        for (auto i = 0; i < _current_batch_rows_read; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[i]);
        }
    } else {
        block_row_locations->resize(_selected_size);
        for (auto i = 0; i < _selected_size; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[_sel_rowid_idx[i]]);
        }
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
