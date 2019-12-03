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

#include <roaring/roaring.hh>

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

class BitmapIndexIterator;
class BitmapIndexReader;
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

    Status _get_row_ranges_from_bitmap_index(Roaring* bitmap_row_ranges);

    Status _init_column_iterators();

    Status _init_bitmap_index_iterators();

    Status _next_batch(RowBlockV2* block, size_t row_offset, size_t* rows_read);

    uint32_t segment_id() const { return _segment->id(); }
    uint32_t num_rows() const { return _segment->num_rows(); }

    Status _seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos);

private:
    class BitmapRangeIterator {
       public:
        explicit BitmapRangeIterator(const Roaring& bitmap) {
            roaring_init_iterator(&bitmap.roaring, &_iter);
            _last_val = 0;
            _buf = new uint32_t[256];
            _read_next_batch();
        }

        ~BitmapRangeIterator() {
            delete[] _buf;
        }

        bool has_more_range() const { return !_eof; }

        bool next_range(uint32_t max_range_size, uint32_t* from, uint32_t* to) {
            if (_eof) {
                return false;
            }
            *from = _buf[_buf_pos];
            uint32_t range_size = 0;
            do {
                _last_val = _buf[_buf_pos];
                _buf_pos++;
                range_size++;
                if (_buf_pos == _buf_size) { // read next batch
                    _read_next_batch();
                }
            } while (range_size < max_range_size && !_eof && _buf[_buf_pos] == _last_val + 1);
            *to = *from + range_size;
            return true;
        }

       private:
        void _read_next_batch() {
            uint32_t n = roaring_read_uint32_iterator(&_iter, _buf, kBatchSize);
            _buf_pos = 0;
            _buf_size = n;
            _eof = n == 0;
        }

        static const uint32_t kBatchSize = 256;
        roaring_uint32_iterator_t _iter;
        uint32_t _last_val;
        uint32_t* _buf = nullptr;
        uint32_t _buf_pos;
        uint32_t _buf_size;
        bool _eof;
    };

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
