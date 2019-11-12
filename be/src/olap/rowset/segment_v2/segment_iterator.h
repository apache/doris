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
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/rowset/segment_v2/column_zone_map.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "olap/olap_cond.h"

namespace doris {

class RowCursor;
class RowBlockV2;
class ShortKeyIndexIterator;

namespace segment_v2 {

class ColumnIterator;

class SegmentIterator : public RowwiseIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment, const Schema& _schema);
    ~SegmentIterator() override;
    Status init(const StorageReadOptions& opts) override {
        _opts = opts;
        return Status::OK();
    }
    Status next_batch(RowBlockV2* row_block) override;
    const Schema& schema() const override { return _schema; }
private:
    Status _init();

    // calculate row ranges that fall into requested key ranges using short key index
    Status _get_row_ranges_by_keys();
    Status _prepare_seek(const StorageReadOptions::KeyRange& key_range);
    Status _lookup_ordinal(const RowCursor& key, bool is_include,
                           rowid_t upper_bound, rowid_t* rowid);
    Status _seek_and_peek(rowid_t rowid);

    // calculate row ranges that satisfy requested column conditions using various column index
    Status _get_row_ranges_by_column_conditions();

    Status _get_row_ranges_from_conditions(RowRanges* condition_row_ranges);

    Status _init_column_iterators();

    Status _next_batch(RowBlockV2* block, size_t* rows_read);

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos);

private:
    std::shared_ptr<Segment> _segment;
    // TODO(zc): rethink if we need copy it
    Schema _schema;
    // _column_iterators.size() == _schema.num_columns()
    // _column_iterators[cid] == nullptr if cid is not in _schema
    std::vector<ColumnIterator*> _column_iterators;
    // after init(), `_row_ranges` contains all rowid to scan
    RowRanges _row_ranges;
    // the next rowid to read
    rowid_t _cur_rowid;
    // index of the row range where `_cur_rowid` belongs to
    size_t _cur_range_id;
    // the actual init process is delayed to the first call to next_batch()
    bool _inited;

    StorageReadOptions _opts;

    // row schema of the key to seek
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<Schema> _seek_schema;
    // used to binary search the rowid for a given key
    // only used in `_get_row_ranges_by_keys`
    std::unique_ptr<RowBlockV2> _seek_block;
};

}
}
