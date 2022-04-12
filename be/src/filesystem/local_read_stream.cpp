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

#include "filesystem/local_read_stream.h"

#include "gutil/macros.h"

namespace doris {

LocalReadStream::LocalReadStream(int fd, size_t file_size, size_t buffer_size)
        : _fd(fd), _file_size(file_size), _buffer_size(buffer_size) {
    _buffer = new char[buffer_size];
}

LocalReadStream::~LocalReadStream() {
    if (_fd != -1) {
        close();
    }
}

Status LocalReadStream::read(char* to, size_t req_n, size_t* read_n) {
    if (eof()) {
        *read_n = 0;
        return Status::OK();
    }
    // Beyond buffer range
    if (_offset >= _buffer_end || _offset < _buffer_begin) {
        // Request length is larger than the capacity of buffer,
        // do not copy data into buffer.
        if (req_n > _buffer_size) {
            int res = 0;
            RETRY_ON_EINTR(res, ::pread(_fd, to, req_n, _offset));
            if (-1 == res) {
                return Status::IOError("Cannot read from file");
            }
            _offset += res;
            *read_n = res;
            return Status::OK();
        }
        RETURN_IF_ERROR(fill());
    }

    size_t copied = std::min(_buffer_end - _offset, req_n);
    memcpy(to, _buffer + _offset - _buffer_begin, copied);

    _offset += copied;
    *read_n = copied;

    size_t left_n = req_n - copied;
    if (left_n > 0) {
        size_t read_n1 = 0;
        RETURN_IF_ERROR(read(to + copied, left_n, &read_n1));
        *read_n += read_n1;
    }
    return Status::OK();
}

Status LocalReadStream::seek(int64_t position) {
    if (position > _file_size) {
        return Status::IOError("Position exceeds file size");
    }
    _offset = position;
    return Status::OK();
}

Status LocalReadStream::tell(int64_t* position) {
    *position = _offset;
    return Status::OK();
}

Status LocalReadStream::close() {
    if (_fd != -1) {
        _fd = -1;
        delete[] _buffer;
    }
    return Status::OK();
}

Status LocalReadStream::fill() {
    int res = 0;
    RETRY_ON_EINTR(res, ::pread(_fd, _buffer, _buffer_size, _offset));
    if (-1 == res) {
        return Status::IOError("Cannot read from file");
    }
    _buffer_begin = _offset;
    _buffer_end = _offset + res;
    return Status::OK();
}

} // namespace doris
