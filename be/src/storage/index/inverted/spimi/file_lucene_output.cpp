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

#include "storage/index/inverted/spimi/file_lucene_output.h"

#include <cstring>

#include "common/logging.h"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::spimi {

FileLuceneOutput::FileLuceneOutput(io::FileWriter* file_writer, size_t buffer_size)
        : _file_writer(file_writer), _buffer(buffer_size) {
    DCHECK(_file_writer != nullptr);
    DCHECK_GT(buffer_size, 0U);
}

void FileLuceneOutput::WriteByte(uint8_t b) {
    if (!_status.ok()) {
        return;
    }
    if (_buffer_pos == _buffer.size()) {
        FlushBuffer();
        if (!_status.ok()) {
            return;
        }
    }
    _buffer[_buffer_pos++] = b;
    ++_file_pointer;
}

void FileLuceneOutput::WriteBytes(const uint8_t* b, size_t len) {
    if (!_status.ok() || len == 0) {
        return;
    }
    size_t remaining = len;
    while (remaining > 0) {
        const size_t space = _buffer.size() - _buffer_pos;
        if (space == 0) {
            FlushBuffer();
            if (!_status.ok()) {
                return;
            }
            continue;
        }
        const size_t chunk = std::min(space, remaining);
        std::memcpy(_buffer.data() + _buffer_pos, b, chunk);
        _buffer_pos += chunk;
        b += chunk;
        remaining -= chunk;
        _file_pointer += static_cast<int64_t>(chunk);
    }
}

void FileLuceneOutput::FlushBuffer() {
    if (_buffer_pos == 0) {
        return;
    }
    const Slice slice(reinterpret_cast<const char*>(_buffer.data()), _buffer_pos);
    const Status s = _file_writer->append(slice);
    if (!s.ok()) {
        _status = s;
    }
    _buffer_pos = 0;
}

Status FileLuceneOutput::Finish() {
    FlushBuffer();
    return _status;
}

} // namespace doris::segment_v2::inverted_index::spimi
