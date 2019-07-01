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
#include "util/arena.h"

namespace doris {

class RowCursor;
class RowBlockV2;
class ShortKeyIndexIterator;

namespace segment_v2 {

class ColumnIterator;

struct StorageReadOptions {
    // lower_bound defines the smallest key at which iterator will
    // return data.
    // If lower_bound is null, won't return
    std::shared_ptr<RowCursor> lower_bound;

    // If include_lower_bound is true, data equal with lower_bound will
    // be read
    bool include_lower_bound;

    // upper_bound defines the extend upto which the iterator can return
    // data.
    std::shared_ptr<RowCursor> upper_bound;

    // If include_upper_bound is true, data equal with upper_bound will
    // be read
    bool include_upper_bound;

    // columns need to be returned
    std::vector<uint32_t> column_ids;
};

class SegmentIterator {
public:
    SegmentIterator(std::shared_ptr<Segment> segment);
    ~SegmentIterator();
    Status init(const StorageReadOptions& opts);
    Status next_batch(RowBlockV2* row_block);

private:
    Status _init_short_key_range();
    Status _prepare_seek();
    Status _init_column_iterators();
    Status _create_column_iterator(uint32_t cid, ColumnIterator** iter);
    Status _lookup_ordinal(const RowCursor& key, bool is_include,
                           rowid_t upper_bound, rowid_t* rowid);
    Status _seek_and_peek(rowid_t rowid);
    Status _next_batch(RowBlockV2* block, size_t* rows_read);

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }
    const std::vector<ColumnSchemaV2>& schema() { return _segment->schema(); }
private:
    std::shared_ptr<Segment> _segment;

    StorageReadOptions _opts;

    // used to iterate short key index to obtain an approximate range
    std::unique_ptr<ShortKeyIndexIterator> _index_iter;
    // Only used when init is called, help to finish seek_and_peek.
    // Data will be saved in this batch
    std::vector<uint32_t> _key_column_ids;
    // used to read data from columns when do bianry search to find
    // oridnal for input bounds
    std::unique_ptr<RowBlockV2> _seek_block;
    // helper to save row to compare with input bounds
    std::unique_ptr<RowCursor> _key_cursor;

    std::vector<ColumnIterator*> _column_iterators;

    rowid_t _lower_rowid;
    rowid_t _upper_rowid;
    rowid_t _cur_rowid;

    Arena _arena;
};

}
}
