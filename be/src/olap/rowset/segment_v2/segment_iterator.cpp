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
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/short_key_index.h"

using strings::Substitute;

namespace doris {
namespace segment_v2 {

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment)
        : _segment(std::move(segment)),
        _column_iterators(_segment->schema().size(), nullptr) {
}

SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter;
    }
}

Status SegmentIterator::init(const StorageReadOptions& opts) {
    _opts = opts;
    RETURN_IF_ERROR(_init_short_key_range());
    RETURN_IF_ERROR(_init_column_iterators());
    return Status::OK();
}

// This function will use input key bounds to get a row range.
Status SegmentIterator::_init_short_key_range() {
    _lower_rowid = 0;
    _upper_rowid = num_rows();
    if (_opts.lower_bound == nullptr && _opts.upper_bound == nullptr) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_prepare_seek());

    _index_iter.reset(_segment->new_short_key_index_iterator());

    // init row range with short key range
    if (_opts.upper_bound != nullptr) {
        // If client want to read upper_bound, the include_upper_bound is true. So we
        // should get the first ordinal at which key is larger than upper_bound.
        // So we call _lookup_ordinal with include_upper_bound's negate
        RETURN_IF_ERROR(_lookup_ordinal(
                *_opts.upper_bound, !_opts.include_upper_bound, num_rows(), &_upper_rowid));
    }
    if (_opts.lower_bound != nullptr) {
        RETURN_IF_ERROR(_lookup_ordinal(
                *_opts.lower_bound, _opts.include_lower_bound, _upper_rowid, &_lower_rowid));
    }

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek() {
    std::set<uint32_t> column_set;
    if (_opts.lower_bound != nullptr) {
        for (auto cid : _opts.lower_bound->columns()) {
            column_set.emplace(cid);
        }
    }
    if (_opts.upper_bound != nullptr) {
        for (auto cid : _opts.upper_bound->columns()) {
            column_set.emplace(cid);
        }
    }
    _key_column_ids.assign(column_set.begin(), column_set.end());
    _seek_block.reset(new RowBlockV2(schema(), _key_column_ids, 1, &_arena));

    std::vector<FieldInfo> fields;
    for (auto& col_schema : schema()) {
        fields.push_back(col_schema.field_info());
    }
    _key_cursor.reset(new RowCursor());
    _key_cursor->init(fields, _key_column_ids);

    // create used column iterator
    for (auto cid : _key_column_ids) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_create_column_iterator(cid, &_column_iterators[cid]));
        }
    }

    return Status::OK();
}

Status SegmentIterator::_init_column_iterators() {
    for (auto cid : _opts.column_ids) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_create_column_iterator(cid, &_column_iterators[cid]));
        }

        _column_iterators[cid]->seek_to_ordinal(_lower_rowid);
    }
    _cur_rowid = _lower_rowid;
    return Status::OK();
}

Status SegmentIterator::_create_column_iterator(uint32_t cid, ColumnIterator** iter) {
    _segment->new_column_iterator(cid, iter);
    return Status::OK();
}

// look up one key to get its ordinal at which can get data. 
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include,
                                        rowid_t upper_bound, rowid_t* rowid) {
    rowid_t start = 0;
    _index_iter->seek_before(key);
    if (_index_iter->valid()) {
        // There may be rows that larger than or equal with this key
        start = _index_iter->block() * _segment->num_rows_per_block();
    } else {
        // This case only exist when this segment is empty. Otherwise
        // _index_iter will always point to a valid position
        // we can't find any rows smaller than input key,
        // return the end of segment
        *rowid = upper_bound;
        return Status::OK();
    }

    rowid_t end = upper_bound;
    _index_iter->seek_after(key);
    if (_index_iter->valid()) {
        // end is open bound, so we need +1 to make first row can be choosed
        // when the first row of the block matches the input condition
        end = _index_iter->block() * _segment->num_rows_per_block() + 1;
    } else {
        end = upper_bound;
    }

    // binary search to find the exact key
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        RETURN_IF_ERROR(_seek_and_peek(mid));
        int cmp = _key_cursor->cmp(key);
        if (cmp < 0) {
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
    for (auto cid : _key_column_ids) {
        _column_iterators[cid]->seek_to_ordinal(rowid);
    }
    size_t num_rows = 1;
    _seek_block->resize(num_rows);
    RETURN_IF_ERROR(_next_batch(_seek_block.get(), &num_rows));
    RETURN_IF_ERROR(_seek_block->copy_to_row_cursor(0, _key_cursor.get()));
    return Status::OK();
}

// Try to read data as much to block->num_rows(). The number of read rows
// will be set in rows_read when return OK. rows_read will small than
// block->num_rows() when reach the end of this segment
Status SegmentIterator::_next_batch(RowBlockV2* block, size_t* rows_read) {
    bool has_read = false;
    size_t first_read = 0;
    for (int i = 0; i < block->column_ids().size(); ++i) {
        auto cid = block->column_ids()[i];
        size_t num_rows = has_read ? first_read : block->num_rows();
        auto column_block = block->column_block(i);
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&num_rows, &column_block));
        if (!has_read) {
            has_read = true;
            first_read = num_rows;
        } else if (num_rows != first_read) {
            return Status::InternalError(
                Substitute("Read different rows in different columns"
                           ", column($0) read $1 vs column($2) read $3",
                           block->column_ids()[0], first_read, cid, num_rows));
        }
    }
    *rows_read = first_read;
    return Status::OK();
}

Status SegmentIterator::next_batch(RowBlockV2* block) {
    size_t rows_to_read = std::min((rowid_t)block->capacity(), _upper_rowid - _cur_rowid);
    block->resize(rows_to_read);
    RETURN_IF_ERROR(_next_batch(block, &rows_to_read));
    _cur_rowid += rows_to_read;
    return Status::OK();
}

}
}
