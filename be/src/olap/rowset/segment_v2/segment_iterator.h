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
#include "olap/iterators.h"
#include "olap/schema.h"
#include "util/arena.h"

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
    Status init(const StorageReadOptions& opts) override;
    Status next_batch(RowBlockV2* row_block) override;
    const Schema& schema() const override { return _schema; }
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

private:
    std::shared_ptr<Segment> _segment;
    // TODO(zc): rethink if we need copy it
    Schema _schema;

    StorageReadOptions _opts;

    // Only used when init is called, help to finish seek_and_peek.
    // Data will be saved in this batch
    std::unique_ptr<Schema> _seek_schema;

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
