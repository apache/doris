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

#include "filesystem/buffered_read_stream.h"

namespace doris {

BufferedReadStream::BufferedReadStream(std::unique_ptr<ReadStream>&& stream, size_t buffer_size)
        : _stream(std::move(stream)), _buffer_size(buffer_size) {
    _buffer = buffer_size ? new char[buffer_size] : nullptr;
    _stream->tell(&_offset);
    size_t avail = 0;
    _stream->available(&avail);
    _offset_limit = _offset + avail;
}

BufferedReadStream::~BufferedReadStream() {
    close();
    delete[] _buffer;
}

Status BufferedReadStream::read(char* to, size_t req_n, size_t* read_n) {
    RETURN_IF_ERROR(read_at(_offset, to, req_n, read_n));
    _offset += *read_n;
    return Status::OK();
}

Status BufferedReadStream::read_at(size_t position, char* to, size_t req_n, size_t* read_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _offset_limit) {
        return Status::IOError("Position exceeds range");
    }
    req_n = std::min(req_n, _offset_limit - position);
    if (req_n == 0) {
        *read_n = 0;
        return Status::OK();
    }
    // Beyond buffer range
    if (position >= _buffer_end || position < _buffer_begin) {
        // Request length is larger than the capacity of buffer,
        // do not copy data into buffer.
        if (req_n > _buffer_size) {
            RETURN_IF_ERROR(_stream->read_at(position, to, req_n, read_n));
            return Status::OK();
        }
        RETURN_IF_ERROR(fill(position));
    }

    size_t copied = std::min(_buffer_end - position, req_n);
    memcpy(to, _buffer + position - _buffer_begin, copied);
    position += copied;
    *read_n = copied;

    req_n -= copied;
    if (req_n > 0) {
        size_t read_n1 = 0;
        RETURN_IF_ERROR(read_at(position, to + copied, req_n, &read_n1));
        *read_n += read_n1;
    }
    return Status::OK();
}

Status BufferedReadStream::seek(size_t position) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _offset_limit) {
        return Status::IOError("Position exceeds range");
    }
    _offset = position;
    return Status::OK();
}

Status BufferedReadStream::tell(size_t* position) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *position = _offset;
    return Status::OK();
}

Status BufferedReadStream::available(size_t* n_bytes) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *n_bytes = _offset_limit - _offset;
    return Status::OK();
}

Status BufferedReadStream::close() {
    if (closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_stream->close());
    _stream = nullptr;
    delete[] _buffer;
    _buffer = nullptr;
    _closed = true;
    return Status::OK();
}

Status BufferedReadStream::fill(size_t position) {
    size_t read_n = 0;
    RETURN_IF_ERROR(_stream->read_at(position, _buffer, _buffer_size, &read_n));
    _buffer_begin = position;
    _buffer_end = position + read_n;
    return Status::OK();
}

} // namespace doris
