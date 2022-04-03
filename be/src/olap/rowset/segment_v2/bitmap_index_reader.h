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

#include <roaring/roaring.hh>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/column_block.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

class TypeInfo;

namespace segment_v2 {

class BitmapIndexIterator;
class IndexedColumnReader;
class IndexedColumnIterator;

class BitmapIndexReader {
public:
    explicit BitmapIndexReader(const FilePathDesc& path_desc, const BitmapIndexPB* bitmap_index_meta)
            : _path_desc(path_desc), _bitmap_index_meta(bitmap_index_meta) {
        _typeinfo = get_scalar_type_info<OLAP_FIELD_TYPE_VARCHAR>();
    }

    Status load(bool use_page_cache, bool kept_in_memory);

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(BitmapIndexIterator** iterator);

    int64_t bitmap_nums() { return _bitmap_column_reader->num_values(); }

    const TypeInfo* type_info() { return _typeinfo; }

private:
    friend class BitmapIndexIterator;

    FilePathDesc _path_desc;
    const TypeInfo* _typeinfo;
    const BitmapIndexPB* _bitmap_index_meta;
    bool _has_null = false;
    std::unique_ptr<IndexedColumnReader> _dict_column_reader;
    std::unique_ptr<IndexedColumnReader> _bitmap_column_reader;
};

class BitmapIndexIterator {
public:
    explicit BitmapIndexIterator(BitmapIndexReader* reader)
            : _reader(reader),
              _dict_column_iter(reader->_dict_column_reader.get()),
              _bitmap_column_iter(reader->_bitmap_column_reader.get()),
              _current_rowid(0),
              _tracker(new MemTracker()),
              _pool(new MemPool(_tracker.get())) {}

    bool has_null_bitmap() const { return _reader->_has_null; }

    // Seek the dictionary to the first value that is >= the given value.
    //
    // Returns OK when such value exists. The seeked position can be retrieved
    // by `current_ordinal()`, *exact_match is set to indicate whether the
    // seeked value exactly matches `value` or not
    //
    // Returns NotFound when no such value exists (all values in dictionary < `value`).
    // Returns other error status otherwise.
    Status seek_dictionary(const void* value, bool* exact_match);

    // Read bitmap at the given ordinal into `result`.
    Status read_bitmap(rowid_t ordinal, roaring::Roaring* result);

    Status read_null_bitmap(roaring::Roaring* result) {
        if (has_null_bitmap()) {
            // null bitmap is always stored at last
            return read_bitmap(bitmap_nums() - 1, result);
        }
        return Status::OK(); // keep result empty
    }

    // Read and union all bitmaps in range [from, to) into `result`
    Status read_union_bitmap(rowid_t from, rowid_t to, roaring::Roaring* result);

    inline rowid_t bitmap_nums() const { return _reader->bitmap_nums(); }

    inline rowid_t current_ordinal() const { return _current_rowid; }

private:
    BitmapIndexReader* _reader;
    IndexedColumnIterator _dict_column_iter;
    IndexedColumnIterator _bitmap_column_iter;
    rowid_t _current_rowid;
    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _pool;
};

} // namespace segment_v2
} // namespace doris
