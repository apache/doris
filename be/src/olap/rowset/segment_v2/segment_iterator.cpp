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

#include <set>

#include "gutil/strings/substitute.h"
#include "util/doris_metrics.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/short_key_index.h"

using strings::Substitute;

namespace doris {
namespace segment_v2 {

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment,
                                 const Schema& schema)
    : _segment(std::move(segment)),
      _schema(schema),
      _column_iterators(_schema.num_columns(), nullptr),
      _row_ranges(RowRanges::create_single(_segment->num_rows())),
      _cur_rowid(0),
      _cur_range_id(0),
      _inited(false) {
}

SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter;
    }
}

Status SegmentIterator::_init() {
    DorisMetrics::segment_read_total.increment(1);
    RETURN_IF_ERROR(_get_row_ranges_by_keys());
    RETURN_IF_ERROR(_get_row_ranges_by_column_conditions());
    if (!_row_ranges.is_empty()) {
        _cur_range_id = 0;
        _cur_rowid = _row_ranges.get_range_from(_cur_range_id);
    }
    RETURN_IF_ERROR(_init_column_iterators());
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_keys() {
    DorisMetrics::segment_row_total.increment(num_rows());

    // fast path for empty segment or empty key ranges
    if (_row_ranges.is_empty() || _opts.key_ranges.empty()) {
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
            RETURN_IF_ERROR(_lookup_ordinal(
                *key_range.upper_key, !key_range.include_upper, num_rows(), &upper_rowid));
        }
        if (upper_rowid > 0 && key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(
                _lookup_ordinal(*key_range.lower_key, key_range.include_lower, upper_rowid, &lower_rowid));
        }
        auto row_range = RowRanges::create_single(lower_rowid, upper_rowid);
        RowRanges::ranges_union(result_ranges, row_range, &result_ranges);
    }
    // pre-condition: _row_ranges == [0, num_rows)
    _row_ranges = std::move(result_ranges);
    DorisMetrics::segment_rows_by_short_key.increment(_row_ranges.count());

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek(const StorageReadOptions::KeyRange& key_range) {
    std::vector<const Field*> key_fields;
    std::set<uint32_t> column_set;
    if (key_range.lower_key != nullptr) {
        for (auto cid : key_range.lower_key->schema()->column_ids()) {
            column_set.emplace(cid);
            key_fields.emplace_back(key_range.lower_key->schema()->column(cid));
        }
    }
    if (key_range.upper_key != nullptr) {
        for (auto cid : key_range.upper_key->schema()->column_ids()) {
            if (column_set.count(cid) == 0) {
                key_fields.emplace_back(key_range.upper_key->schema()->column(cid));
                column_set.emplace(cid);
            }
        }
    }
    _seek_schema.reset(new Schema(key_fields, key_fields.size()));
    _seek_block.reset(new RowBlockV2(*_seek_schema, 1, &_arena));

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
        }
    }

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_column_conditions() {
    if (_row_ranges.is_empty()) {
        // no data just return;
        return Status::OK();
    }

    if (_opts.conditions != nullptr) {
        RowRanges zone_map_row_ranges;
        RETURN_IF_ERROR(_get_row_ranges_from_zone_map(&zone_map_row_ranges));
        RowRanges::ranges_intersection(_row_ranges, zone_map_row_ranges, &_row_ranges);
        // TODO(hkp): get row ranges from bloom filter and secondary index
    }

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_from_zone_map(RowRanges* zone_map_row_ranges) {
    RowRanges origin_row_ranges = RowRanges::create_single(num_rows());
    for (auto& column_condition : _opts.conditions->columns()) {
        int32_t column_id = column_condition.first;
        // get row ranges from zone map
        if (!_segment->_column_readers[column_id]->has_zone_map()) {
            // there is no zone map for this column
            continue;
        }
        // get row ranges by zone map of this column
        RowRanges column_zone_map_row_ranges;
        _segment->_column_readers[column_id]->get_row_ranges_by_zone_map(column_condition.second, &column_zone_map_row_ranges);
        // intersection different columns's row ranges to get final row ranges by zone map
        RowRanges::ranges_intersection(origin_row_ranges, column_zone_map_row_ranges, &origin_row_ranges);
    }
    *zone_map_row_ranges = std::move(origin_row_ranges);
    DorisMetrics::segment_rows_read_by_zone_map.increment(zone_map_row_ranges->count());
    return Status::OK();
}

Status SegmentIterator::_init_column_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
        }

        _column_iterators[cid]->seek_to_ordinal(_cur_rowid);
    }
    return Status::OK();
}

// Schema of lhs and rhs are different.
// callers should assure that rhs' schema has all columns in lhs schema
template<typename LhsRowType, typename RhsRowType>
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
Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include,
                                        rowid_t upper_bound, rowid_t* rowid) {
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
    for (auto cid : _seek_schema->column_ids()) {
        _column_iterators[cid]->seek_to_ordinal(rowid);
    }
    size_t num_rows = 1;
    _seek_block->resize(num_rows);
    RETURN_IF_ERROR(_next_batch(_seek_block.get(), &num_rows));
    return Status::OK();
}

// Try to read data as much to block->num_rows(). The number of read rows
// will be set in rows_read when return OK. rows_read will small than
// block->num_rows() when reach the end of this segment
Status SegmentIterator::_next_batch(RowBlockV2* block, size_t* rows_read) {
    bool has_read = false;
    size_t first_read = 0;
    for (int i = 0; i < block->schema()->column_ids().size(); ++i) {
        auto cid = block->schema()->column_ids()[i];
        size_t num_rows = has_read ? first_read : *rows_read;
        auto column_block = block->column_block(i);
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&num_rows, &column_block));
        if (!has_read) {
            has_read = true;
            first_read = num_rows;
        } else if (num_rows != first_read) {
            return Status::InternalError(
                Substitute("Read different rows in different columns"
                           ", column($0) read $1 vs column($2) read $3",
                           block->schema()->column_ids()[0], first_read, cid, num_rows));
        }
    }
    *rows_read = first_read;
    return Status::OK();
}

Status SegmentIterator::next_batch(RowBlockV2* block) {
    if (UNLIKELY(!_inited)) {
        RETURN_IF_ERROR(_init());
        _inited = true;
    }

    if (_row_ranges.is_empty() || _cur_rowid >= _row_ranges.to()) {
        block->resize(0);
        return Status::EndOfFile("no more data in segment");
    }
    size_t rows_to_read = block->capacity();
    while (rows_to_read > 0) {
        if (_cur_rowid >= _row_ranges.get_range_to(_cur_range_id)) {
            // current row range is read over, trying to read from next range
            if (_cur_range_id >= _row_ranges.range_size() - 1) {
                // there is no more row range
                break;
            }
            // step to next row range
            ++_cur_range_id;
            _cur_rowid = _row_ranges.get_range_from(_cur_range_id);
            if (_row_ranges.get_range_count(_cur_range_id) == 0) {
                // current row range is empty, just skip seek
                continue;
            }
            for (auto cid : block->schema()->column_ids()) {
                RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(_cur_rowid));
            }
        }
        size_t to_read_in_range = std::min(rows_to_read, size_t(_row_ranges.get_range_to(_cur_range_id) - _cur_rowid));
        RETURN_IF_ERROR(_next_batch(block, &to_read_in_range));
        _cur_rowid += to_read_in_range;
        rows_to_read -= to_read_in_range;
    }
    block->resize(block->capacity() - rows_to_read);
    if (block->num_rows() == 0) {
        return Status::EndOfFile("no more data in segment");
    }
    return Status::OK();
}

}
}
