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
#include "olap/key_coder.h"
#include "util/coding.h"

namespace doris {
namespace segment_v2 {

void IndexPageBuilder::add(const Slice& key, const PagePointer& ptr) {
    DCHECK(!_finished) << "must reset() after finish() to add new entry";
    put_length_prefixed_slice(&_buffer, key);
    ptr.encode_to(&_buffer);
    _count++;
}

bool IndexPageBuilder::is_full() const {
    // estimate size of IndexPageFooterPB to be 16
    return _buffer.size()  + 16 > _index_page_size;
}

Slice IndexPageBuilder::finish() {
    DCHECK(!_finished) << "already called finish()";
    IndexPageFooterPB footer;
    footer.set_num_entries(_count);
    footer.set_type(_is_leaf ? IndexPageFooterPB::LEAF : IndexPageFooterPB::INTERNAL);

    std::string footer_buf;
    footer.SerializeToString(&footer_buf);
    _buffer.append(footer_buf);
    put_fixed32_le(&_buffer, footer_buf.size());
    return Slice(_buffer);
}

Status IndexPageBuilder::get_first_key(Slice* key) const {
    if (_count == 0) {
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

Status IndexPageReader::parse(const Slice& data) {
    size_t buffer_len = data.size;
    const uint8_t* buffer = (uint8_t*)data.data;
    size_t footer_size = decode_fixed32_le(buffer + buffer_len - 4);
    std::string footer_buf(data.data + buffer_len - 4 - footer_size, footer_size);
    _footer.ParseFromString(footer_buf);
    size_t num_entries = _footer.num_entries();

    Slice input(data);
    for (int i = 0; i < num_entries; ++i) {
        Slice key;
        PagePointer value;
        if (!get_length_prefixed_slice(&input, &key)) {
            return Status::InternalError("Data corruption");
        }
        if (!value.decode_from(&input)) {
            return Status::InternalError("Data corruption");
        }
        _keys.push_back(key);
        _values.push_back(value);
    }

    _parsed = true;
    return Status::OK();
}
///////////////////////////////////////////////////////////////////////////////

Status IndexPageIterator::seek_at_or_before(const Slice& search_key) {
    int32_t left = 0;
    int32_t right = _reader->count() - 1;
    while (left <= right) {
        int32_t mid = left + (right - left) / 2;
        int cmp = search_key.compare(_reader->get_key(mid));
        if (cmp < 0) {
            right = mid - 1;
        } else if (cmp > 0) {
            left = mid + 1;
        } else {
            _pos = mid;
            return Status::OK();
        }
    }
    // no exact match, the insertion point is `left`
    if (left == 0) {
        // search key is smaller than all keys
        return Status::NotFound("no page contains the given key");
    }
    // index entry records the first key of the indexed page,
    // therefore the first page with keys >= searched key is the one before the insertion point
    _pos = left - 1;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
