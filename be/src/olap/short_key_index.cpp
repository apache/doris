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

#include "olap/short_key_index.h"

#include <string>

#include "util/coding.h"
#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace doris {

Status ShortKeyIndexBuilder::add_item(const Slice& key) {
    put_varint32(&_offset_buf, _key_buf.size());
    _footer.set_num_items(_footer.num_items() + 1);
    _key_buf.append(key.data, key.size);
    return Status::OK();
}

Status ShortKeyIndexBuilder::finalize(uint32_t segment_bytes,
                                      uint32_t num_segment_rows,
                                      std::vector<Slice>* slices) {
    _footer.set_num_segment_rows(num_segment_rows);
    _footer.set_segment_bytes(segment_bytes);
    _footer.set_key_bytes(_key_buf.size());
    _footer.set_offset_bytes(_offset_buf.size());

    // encode header
    if (!_footer.SerializeToString(&_footer_buf)) {
        return Status::InternalError("Failed to serialize index footer");
    }

    put_fixed32_le(&_footer_buf, _footer_buf.size());
    // TODO(zc): checksum
    uint32_t checksum = 0;
    put_fixed32_le(&_footer_buf, checksum);

    slices->emplace_back(_key_buf);
    slices->emplace_back(_offset_buf);
    slices->emplace_back(_footer_buf);
    return Status::OK();
}

Status ShortKeyIndexDecoder::parse() {
    Slice data = _data;

    // 1. parse footer, get checksum and footer length
    if (data.size < 2 * sizeof(uint32_t)) {
        return Status::Corruption(
            Substitute("Short key is too short, need=$0 vs real=$1",
                       2 * sizeof(uint32_t), data.size));
    }
    size_t offset = data.size - 2 * sizeof(uint32_t);
    uint32_t footer_length = decode_fixed32_le((uint8_t*)data.data + offset);
    uint32_t checksum = decode_fixed32_le((uint8_t*)data.data + offset + 4);
    // TODO(zc): do checksum
    if (checksum != 0) {
        return Status::Corruption(
            Substitute("Checksum not match, need=$0 vs read=$1", 0, checksum));
    }
    // move offset to parse footer
    offset -= footer_length;
    std::string footer_buf(data.data + offset, footer_length);
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption("Fail to parse index footer from string");
    }

    // check if real data size match footer's content
    if (offset != _footer.key_bytes() + _footer.offset_bytes()) {
        return Status::Corruption(
            Substitute("Index size not match, need=$0, real=$1",
                       _footer.key_bytes() + _footer.offset_bytes(), offset));
    }

    // set index buffer
    _key_data = Slice(_data.data, _footer.key_bytes());
    
    // parse offset information
    Slice offset_slice(_data.data + _footer.key_bytes(), _footer.offset_bytes());
    // +1 for record total length
    _offsets.resize(_footer.num_items() + 1);
    _offsets[_footer.num_items()] = _footer.key_bytes();
    for (uint32_t i = 0; i < _footer.num_items(); ++i) {
        uint32_t offset = 0;
        if (!get_varint32(&offset_slice, &offset)) {
            return Status::Corruption("Fail to get varint from index offset buffer");
        }
        DCHECK(offset <= _footer.key_bytes())
            << "Offset is larger than total bytes, offset=" << offset
            << ", key_bytes=" << _footer.key_bytes();
        _offsets[i] = offset;
    }

    if (offset_slice.size != 0) {
        return Status::Corruption("Still has data after parse all key offset");
    }

    return Status::OK();
}

}
