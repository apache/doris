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

#include <butil/macros.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <cstddef>
#include <vector>

#include "common/status.h"
#include "olap/metadata_adder.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

// IndexPage is the building block for IndexedColumn's ordinal index and value index.
// It is used to guide searching for a particular key to the data page containing it.
// We use the same general format for all index pages, regardless of the data type and node type (leaf or internal)
//   IndexPageBody := IndexEntry^NumEntry
//   IndexEntry := KeyLength(vint), Byte^KeyLength, PageOffset(vlong), PageSize(vint)
//
// IndexPageFooterPB records NumEntry and type (leaf/internal) of the index page.
// For leaf, IndexKey records the first/smallest key of the data page PagePointer points to.
// For internal, IndexKey records the first/smallest key of the next-level index page PagePointer points to.
//
// All keys are treated as binary string and compared with memcpy. Keys of other data type are encoded first by
// KeyCoder, e.g., ordinal index's original key type is uint64_t but is encoded to binary string.
class IndexPageBuilder {
public:
    explicit IndexPageBuilder(size_t index_page_size, bool is_leaf)
            : _index_page_size(index_page_size), _is_leaf(is_leaf) {}

    void add(const Slice& key, const PagePointer& ptr);

    bool is_full() const;

    size_t count() const { return _count; }

    void finish(OwnedSlice* body, PageFooterPB* footer);

    uint64_t size() { return _buffer.size(); }

    // Return the key of the first entry in this index block.
    // The pointed-to data is only valid until the next call to this builder.
    Status get_first_key(Slice* key) const;

    void reset() {
        _finished = false;
        _buffer.clear();
        _count = 0;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(IndexPageBuilder);
    const size_t _index_page_size;
    const bool _is_leaf;
    bool _finished = false;
    faststring _buffer;
    uint32_t _count = 0;
};

class IndexPageReader : public MetadataAdder<IndexPageReader> {
public:
    IndexPageReader() : _parsed(false) {}

    Status parse(const Slice& body, const IndexPageFooterPB& footer);

    size_t count() const {
        DCHECK(_parsed);
        return _footer.num_entries();
    }

    bool is_leaf() const {
        DCHECK(_parsed);
        return _footer.type() == IndexPageFooterPB::LEAF;
    }

    const Slice& get_key(int idx) const {
        DCHECK(_parsed);
        DCHECK(idx >= 0 && idx < _footer.num_entries());
        return _keys[idx];
    }

    const PagePointer& get_value(int idx) const {
        DCHECK(_parsed);
        DCHECK(idx >= 0 && idx < _footer.num_entries());
        return _values[idx];
    }

    void reset();

private:
    int64_t get_metadata_size() const override;

    bool _parsed;

    IndexPageFooterPB _footer;
    std::vector<Slice> _keys;
    std::vector<PagePointer> _values;
    int64_t _vl_field_mem_size {0};
};

class IndexPageIterator {
public:
    explicit IndexPageIterator(const IndexPageReader* reader) : _reader(reader), _pos(0) {}

    // Find the largest index entry whose key is <= search_key.
    // Return OK status when such entry exists.
    // Return ENTRY_NOT_FOUND when no such entry is found (all keys > search_key).
    // Return other error status otherwise.
    Status seek_at_or_before(const Slice& search_key);

    void seek_to_first() { _pos = 0; }

    // Move to the next index entry.
    // Return true on success, false when no more entries can be read.
    bool move_next() {
        _pos++;
        if (_pos >= _reader->count()) {
            return false;
        }
        return true;
    }

    // Return true when has next page.
    bool has_next() { return (_pos + 1) < _reader->count(); }

    const Slice& current_key() const { return _reader->get_key(_pos); }

    const PagePointer& current_page_pointer() const { return _reader->get_value(_pos); }

private:
    const IndexPageReader* _reader = nullptr;

    size_t _pos;
};

} // namespace segment_v2
} // namespace doris
