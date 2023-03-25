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

#include "io/fs/buffered_reader.h"

#include <algorithm>
#include <sstream>

#include "common/config.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"
#include "util/bit_util.h"

namespace doris {
namespace io {

BufferedFileStreamReader::BufferedFileStreamReader(io::FileReaderSPtr file, uint64_t offset,
                                                   uint64_t length, size_t max_buf_size)
        : _file(file),
          _file_start_offset(offset),
          _file_end_offset(offset + length),
          _max_buf_size(max_buf_size) {}

Status BufferedFileStreamReader::read_bytes(const uint8_t** buf, uint64_t offset,
                                            const size_t bytes_to_read) {
    if (offset < _file_start_offset || offset >= _file_end_offset) {
        return Status::IOError("Out-of-bounds Access");
    }
    int64_t end_offset = offset + bytes_to_read;
    if (_buf_start_offset <= offset && _buf_end_offset >= end_offset) {
        *buf = _buf.get() + offset - _buf_start_offset;
        return Status::OK();
    }
    size_t buf_size = std::max(_max_buf_size, bytes_to_read);
    if (_buf_size < buf_size) {
        std::unique_ptr<uint8_t[]> new_buf(new uint8_t[buf_size]);
        if (offset >= _buf_start_offset && offset < _buf_end_offset) {
            memcpy(new_buf.get(), _buf.get() + offset - _buf_start_offset,
                   _buf_end_offset - offset);
        }
        _buf = std::move(new_buf);
        _buf_size = buf_size;
    } else if (offset > _buf_start_offset && offset < _buf_end_offset) {
        memmove(_buf.get(), _buf.get() + offset - _buf_start_offset, _buf_end_offset - offset);
    }
    if (offset < _buf_start_offset || offset >= _buf_end_offset) {
        _buf_end_offset = offset;
    }
    _buf_start_offset = offset;
    int64_t buf_remaining = _buf_end_offset - _buf_start_offset;
    int64_t to_read = std::min(_buf_size - buf_remaining, _file_end_offset - _buf_end_offset);
    int64_t has_read = 0;
    SCOPED_RAW_TIMER(&_statistics.read_time);
    while (has_read < to_read) {
        size_t loop_read = 0;
        Slice resutl(_buf.get() + buf_remaining + has_read, to_read - has_read);
        RETURN_IF_ERROR(_file->read_at(_buf_end_offset + has_read, resutl, &loop_read));
        _statistics.read_calls++;
        if (loop_read == 0) {
            break;
        }
        has_read += loop_read;
    }
    if (has_read != to_read) {
        return Status::Corruption("Try to read {} bytes, but received {} bytes", to_read, has_read);
    }
    _statistics.read_bytes += to_read;
    _buf_end_offset += to_read;
    *buf = _buf.get();
    return Status::OK();
}

Status BufferedFileStreamReader::read_bytes(Slice& slice, uint64_t offset) {
    return read_bytes((const uint8_t**)&slice.data, offset, slice.size);
}

} // namespace io
} // namespace doris
