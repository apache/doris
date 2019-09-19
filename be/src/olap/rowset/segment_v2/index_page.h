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

#include <cstddef>
#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

class IndexPageIterator; // forward decl.

class IndexPageBuilder {
public:
    explicit IndexPageBuilder(size_t index_page_size, bool is_leaf)
        : _index_page_size(index_page_size), _is_leaf(is_leaf) {
    }

    void add(const Slice& key, const PagePointer& ptr);

    bool is_full() const;

    size_t count() const { return _entry_offsets.size(); }

    Slice finish();

    // Return the key of the first entry in this index block.
    // The pointed-to data is only valid until the next call to this builder.
    Status get_first_key(Slice* key) const;

    void reset() {
        _finished = false;
        _buffer.clear();
        _entry_offsets.clear();
    }

private:
    DISALLOW_COPY_AND_ASSIGN(IndexPageBuilder);
    const size_t _index_page_size;
    const bool _is_leaf;
    // is the builder currently between finish() and reset()?
    bool _finished = false;
    faststring _buffer;
    std::vector<uint32_t> _entry_offsets;
};

class IndexPageReader {
public:
    IndexPageReader();

    Status parse(const Slice& data);

    size_t count() const;

    bool is_leaf() const;

    void reset();
private:
    friend class IndexPageIterator;
    bool _parsed;

    // valid only when `_parsed == true`
    Slice _data;
    IndexPageFooterPB _footer;
    const uint8_t* _entry_offsets;
};

class IndexPageIterator {
public:
    explicit IndexPageIterator(const IndexPageReader* reader);

    // Reset the state of this iterator. This should be used
    // after the associated 'reader' parses a different page.
    void reset();

    // Find the highest index entry which has a key <= the given key.
    // If such a entry is found, returns OK status.
    // Otherwise Status::NotFound is returned. (i.e the smallest key in the
    // index is still larger than the provided key)
    //
    // If this function returns an error, then the state of this
    // iterator is undefined (i.e it may or may not have moved
    // since the previous call)
    Status seek_at_or_before(const Slice& search_key);

    Status seek_ordinal(size_t idx);

    bool has_next() const { return _cur_idx + 1 < _reader->count(); }

    Status next() { return seek_ordinal(_cur_idx + 1); }

    const Slice& current_key() const;

    const PagePointer& current_page_pointer() const;
private:
    const IndexPageReader* _reader;

    bool _seeked;
    size_t _cur_idx;
    Slice _cur_key;
    PagePointer _cur_ptr;
};

} // namespace segment_v2
} // namespace doris
