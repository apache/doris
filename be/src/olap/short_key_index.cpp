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

#include <gen_cpp/segment_v2.pb.h>

#include <ostream>

#include "gutil/strings/substitute.h"
#include "short_key_index.h"
#include "util/bvar_helper.h"
#include "util/coding.h"

using strings::Substitute;

namespace doris {

static bvar::Adder<size_t> g_short_key_index_memory_bytes("doris_short_key_index_memory_bytes");

Status ShortKeyIndexBuilder::add_item(const Slice& key) {
    put_varint32(&_offset_buf, _key_buf.size());
    _key_buf.append(key.data, key.size);
    _num_items++;
    return Status::OK();
}

Status ShortKeyIndexBuilder::finalize(uint32_t num_segment_rows, std::vector<Slice>* body,
                                      segment_v2::PageFooterPB* page_footer) {
    page_footer->set_type(segment_v2::SHORT_KEY_PAGE);
    page_footer->set_uncompressed_size(_key_buf.size() + _offset_buf.size());

    segment_v2::ShortKeyFooterPB* footer = page_footer->mutable_short_key_page_footer();
    footer->set_num_items(_num_items);
    footer->set_key_bytes(_key_buf.size());
    footer->set_offset_bytes(_offset_buf.size());
    footer->set_segment_id(_segment_id);
    footer->set_num_rows_per_block(_num_rows_per_block);
    footer->set_num_segment_rows(num_segment_rows);

    body->emplace_back(_key_buf);
    body->emplace_back(_offset_buf);
    return Status::OK();
}

Status ShortKeyIndexDecoder::parse(const Slice& body, const segment_v2::ShortKeyFooterPB& footer) {
    _footer = footer;

    // check if body size match footer's information
    if (body.size != (_footer.key_bytes() + _footer.offset_bytes())) {
        return Status::Corruption("Index size not match, need={}, real={}",
                                  _footer.key_bytes() + _footer.offset_bytes(), body.size);
    }

    // set index buffer
    _key_data = Slice(body.data, _footer.key_bytes());

    // parse offset information
    Slice offset_slice(body.data + _footer.key_bytes(), _footer.offset_bytes());
    // +1 for record total length
    _offsets.resize(_footer.num_items() + 1);
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
    _offsets[_footer.num_items()] = _footer.key_bytes();

    if (offset_slice.size != 0) {
        return Status::Corruption("Still has data after parse all key offset");
    }
    _parsed = true;

    g_short_key_index_memory_bytes << sizeof(_footer) + _key_data.size +
                                              _offsets.size() * sizeof(uint32_t) + sizeof(*this);

    return Status::OK();
}

ShortKeyIndexDecoder::~ShortKeyIndexDecoder() {
    if (_parsed) {
        g_short_key_index_memory_bytes << -sizeof(_footer) - _key_data.size -
                                                  _offsets.size() * sizeof(uint32_t) -
                                                  sizeof(*this);
    }
}

} // namespace doris
