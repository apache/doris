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

#include "olap/rowset/segment_v2/index_page.h"

#include <string>

#include "common/logging.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

void IndexPageBuilder::add(const Slice& key, const PagePointer& ptr) {
    DCHECK(!_finished) << "must reset() after finish() to add new entry";
    _entry_offsets.push_back(_buffer.size());
    put_length_prefixed_slice(&_buffer, key);
    ptr.encode_to(&_buffer);
}

bool IndexPageBuilder::is_full() const {
    // estimate size of IndexPageFooterPB to be 16
    return _buffer.size() + _entry_offsets.size() * sizeof(uint32_t) + 16 > _index_page_size;
}

Slice IndexPageBuilder::finish() {
    DCHECK(!_finished) << "already called finish()";
    for (uint32_t offset : _entry_offsets) {
        put_fixed32_le(&_buffer, offset);
    }
    IndexPageFooterPB footer;
    footer.set_num_entries(_entry_offsets.size());
    footer.set_type(_is_leaf ? IndexPageFooterPB::LEAF : IndexPageFooterPB::INTERNAL);

    std::string footer_buf;
    footer.SerializeToString(&footer_buf);
    _buffer.append(footer_buf);
    put_fixed32_le(&_buffer, footer_buf.size());
    return Slice(_buffer);
}

Status IndexPageBuilder::get_first_key(Slice* key) const {
    if (_entry_offsets.empty()) {
        return Status::NotFound("index page is empty");
    }
    Slice input(_buffer);
    if (get_length_prefixed_slice(&input, key)) {
        return Status::OK();
    } else {
        return Status::Corruption("can't decode first key");
    }
}

///////////////////////////////////////////////////////////////////////////////

IndexPageReader::IndexPageReader() : _parsed(false) {
}

Status IndexPageReader::parse(const Slice& data) {
    return Status(); // FIXME
}

size_t IndexPageReader::count() const {
    CHECK(_parsed) << "not parsed";
    return _footer.num_entries();
}

bool IndexPageReader::is_leaf() const {
    CHECK(_parsed) << "not parsed";
    return _footer.type() == IndexPageFooterPB::LEAF;
}

void IndexPageReader::reset() {
    _parsed = false;
}

///////////////////////////////////////////////////////////////////////////////


IndexPageIterator::IndexPageIterator(const IndexPageReader* reader)
    : _reader(reader),
      _seeked(false),
      _cur_idx(-1) {
}

void IndexPageIterator::reset() {
    _seeked = false;
    _cur_idx = -1;
}

Status IndexPageIterator::seek_at_or_before(const Slice& search_key) {
    return Status(); // FIXME
}

Status IndexPageIterator::seek_ordinal(size_t idx) {
    return Status(); // FIXME
}

const Slice& IndexPageIterator::current_key() const {
    CHECK(_seeked) << "not seeked";
    return _cur_key;
}

const PagePointer& IndexPageIterator::current_page_pointer() const {
    CHECK(_seeked) << "not seeked";
    return _cur_ptr;
}

} // namespace segment_v2
} // namespace doris
