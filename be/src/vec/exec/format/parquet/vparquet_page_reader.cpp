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

#include "vparquet_page_reader.h"

#include <stdint.h>

#include "util/thrift_util.h"

namespace doris::vectorized {

static constexpr size_t initPageHeaderSize = 1024;

PageReader::PageReader(BufferedStreamReader* reader, uint64_t offset, uint64_t length)
        : _reader(reader), _start_offset(offset), _end_offset(offset + length) {}

Status PageReader::next_page() {
    if (_offset < _start_offset || _offset >= _end_offset) {
        return Status::IOError("Out-of-bounds Access");
    }
    if (_offset != _next_header_offset) {
        return Status::IOError("Wrong header position");
    }

    const uint8_t* page_header_buf = nullptr;
    size_t max_size = _end_offset - _offset;
    size_t header_size = std::min(initPageHeaderSize, max_size);
    uint32_t real_header_size = 0;
    while (true) {
        header_size = std::min(header_size, max_size);
        RETURN_IF_ERROR(_reader->read_bytes(&page_header_buf, _offset, &header_size));
        real_header_size = header_size;
        auto st =
                deserialize_thrift_msg(page_header_buf, &real_header_size, true, &_cur_page_header);
        if (st.ok()) {
            break;
        }
        if (_offset + header_size >= _end_offset ||
            real_header_size > config::parquet_header_max_size) {
            return Status::IOError("Failed to deserialize parquet page header");
        }
        header_size <<= 2;
    }

    _offset += real_header_size;
    _next_header_offset = _offset + _cur_page_header.compressed_page_size;
    return Status::OK();
}

Status PageReader::skip_page() {
    if (_offset == _next_header_offset) {
        return Status::InternalError("Should call next_page() to generate page header");
    }
    _offset = _next_header_offset;
    return Status::OK();
}

Status PageReader::get_page_date(Slice& slice) {
    if (_offset == _next_header_offset) {
        return Status::InternalError("Should call next_page() to generate page header");
    }
    slice.size = _cur_page_header.compressed_page_size;
    RETURN_IF_ERROR(_reader->read_bytes(slice, _offset));
    DCHECK_EQ(slice.size, _cur_page_header.compressed_page_size);
    _offset += slice.size;
    return Status::OK();
}

} // namespace doris::vectorized
