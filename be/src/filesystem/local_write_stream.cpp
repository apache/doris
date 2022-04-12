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

#include "filesystem/local_write_stream.h"

#include <unistd.h>

namespace doris {

namespace detail {

// Retry until `put_n` bytes are written.
bool write_retry(int fd, const char* from, size_t put_n) {
    while (put_n != 0) {
        auto res = ::write(fd, from, put_n);
        if (-1 == res && errno != EINTR) {
            return false;
        }
        if (res > 0) {
            from += res;
            put_n -= res;
        }
    }
    return true;
}

} // namespace detail

LocalWriteStream::LocalWriteStream(int fd, size_t buffer_size)
        : _fd(fd), _buffer_size(buffer_size) {
    _buffer = new char[buffer_size];
}

LocalWriteStream::~LocalWriteStream() {
    if (_fd != -1) {
        close();
    }
}

Status LocalWriteStream::write(const char* from, size_t put_n) {
    if (put_n > buffer_remain()) {
        RETURN_IF_ERROR(flush());
        // Write bytes is greater than the capacity of buffer,
        // do not copy data into buffer.
        // TODO(cyx): Perhaps a more appropriate water level line could be used here
        if (put_n > _buffer_size) {
            if (!detail::write_retry(_fd, from, put_n)) {
                return Status::IOError("Cannot write to file");
            }
            _dirty = true;
            return Status::OK();
        }
    }
    memcpy(_buffer + _buffer_used, from, put_n);
    _buffer_used += put_n;
    _dirty = true;
    return Status::OK();
}

Status LocalWriteStream::sync() {
    if (_dirty) {
        RETURN_IF_ERROR(flush());
        if (0 != ::fdatasync(_fd)) {
            return Status::IOError("Cannot fdatasync");
        }
        _dirty = false;
    }
    return Status::OK();
}

Status LocalWriteStream::flush() {
    if (_buffer_used) {
        if (!detail::write_retry(_fd, _buffer, _buffer_used)) {
            return Status::IOError("Cannot write to file");
        }
        _buffer_used = 0;
    }
    return Status::OK();
}

Status LocalWriteStream::close() {
    if (_fd != -1) {
        RETURN_IF_ERROR(sync());
        if (0 != ::close(_fd)) {
            return Status::IOError("Cannot close file");
        }
        delete[] _buffer;
        _buffer = nullptr;
        _fd = -1;
    }
    return Status::OK();
}

} // namespace doris
