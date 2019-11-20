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

#include "common/status.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/slice.h"

namespace doris {

class RandomAccessFile;

namespace segment_v2 {

class IndexedColumnIterator;

// thread-safe reader for IndexedColumn (see comments of `IndexedColumnWriter` to understand what IndexedColumn is)
class IndexedColumnReader {
public:
    explicit IndexedColumnReader(RandomAccessFile* file);

    Status init();

    Status new_iterator(std::unique_ptr<IndexedColumnIterator>* iter);

    Status read_page(const PagePointer& pp, PageHandle* ret);

    const IndexedColumnMetaPB& meta() const;

    bool has_ordinal_index() const;

    bool has_value_index() const;

private:

};

class IndexedColumnIterator {
public:
    explicit IndexedColumnIterator(IndexedColumnReader* reader);

    ~IndexedColumnIterator();

    // Seek to the first entry. This works for both ordinal-indexed and value-indexed column
    Status seek_to_first();

    // Seek to the given ordinal entry. Entry 0 is the first entry.
    // Return NotFound if provided seek point is past the end.
    // Return NotSupported for column without ordinal index.
    Status seek_to_ordinal(rowid_t idx);

    // Seek the index to the given key, or to the index entry immediately
    // before it. Then seek the data block to the value matching value or to
    // the value immediately after it.
    //
    // Sets *exact_match to indicate whether the seek found the exact
    // key requested.
    //
    // Return NotFound if the given key is greater than all keys in this column.
    // Return NotSupported for column without value index.
    Status seek_at_or_after(const Slice &key,
                            bool *exact_match);

    // Return true if this reader is currently seeked.
    // If the iterator is not seeked, it is an error to call any functions except
    // for seek (including get_current_ordinal).
    bool seeked() const;

    // Get the ordinal index that the iterator is currently pointed to.
    rowid_t get_current_ordinal() const;
};

} // namespace segment_v2
} // namespace doris
