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
#include "olap/olap_cond.h"
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
    const Schema& schema() const override { return _schema; }
    bool is_lazy_materialization_read() const override { return _lazy_materialization_read; }
    uint64_t data_id() const { return _segment->id(); }

private:
    Status _init();

    Status _init_return_column_iterators();
    Status _init_bitmap_index_iterators();

    // calculate row ranges that fall into requested key ranges using short key index
    Status _get_row_ranges_by_keys();
    Status _prepare_seek(const StorageReadOptions::KeyRange& key_range);
    Status _lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                           rowid_t* rowid);
    Status _seek_and_peek(rowid_t rowid);

    // calculate row ranges that satisfy requested column conditions using various column index
    Status _get_row_ranges_by_column_conditions();
    Status _get_row_ranges_from_conditions(RowRanges* condition_row_ranges);
    Status _apply_bitmap_index();

    void _init_lazy_materialization();

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos);
    // read `nrows` of columns specified by `column_ids` into `block` at `row_offset`.
    Status _read_columns(const std::vector<ColumnId>& column_ids, RowBlockV2* block,
                         size_t row_offset, size_t nrows);

private:
    class BitmapRangeIterator;

    std::shared_ptr<Segment> _segment;
    // TODO(zc): rethink if we need copy it
    Schema _schema;
    // _column_iterators.size() == _schema.num_columns()
    // _column_iterators[cid] == nullptr if cid is not in _schema
    std::vector<ColumnIterator*> _column_iterators;
    // FIXME prefer vector<unique_ptr<BitmapIndexIterator>>
    std::vector<BitmapIndexIterator*> _bitmap_index_iterators;
    // after init(), `_row_bitmap` contains all rowid to scan
    Roaring _row_bitmap;
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

    // the actual init process is delayed to the first call to next_batch()
    bool _inited;

    StorageReadOptions _opts;
    // make a copy of `_opts.column_predicates` in order to make local changes
    std::vector<ColumnPredicate*> _col_predicates;

    int16_t** _select_vec;

    // row schema of the key to seek
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<Schema> _seek_schema;
    // used to binary search the rowid for a given key
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<RowBlockV2> _seek_block;

    // block for file to read
    std::unique_ptr<fs::ReadableBlock> _rblock;
};

} // namespace segment_v2
} // namespace doris
