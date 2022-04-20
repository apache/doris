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

#include "filesystem/hdfs_read_stream.h"

#include <fmt/format.h>

namespace doris {

HdfsReadStream::HdfsReadStream(hdfsFS fs, hdfsFile file, size_t file_size)
        : _fs(fs), _file(file), _file_size(file_size) {}

HdfsReadStream::~HdfsReadStream() {
    close();
}

Status HdfsReadStream::read(char* to, size_t req_n, size_t* read_n) {
    RETURN_IF_ERROR(read_at(_offset, to, req_n, read_n));
    _offset += *read_n;
    return Status::OK();
}

Status HdfsReadStream::read_at(size_t position, char* to, size_t req_n, size_t* read_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds range");
    }
    req_n = std::min(req_n, _file_size - position);
    if (req_n == 0) {
        *read_n = 0;
        return Status::OK();
    }
    if (position != _offset) {
        if (0 != hdfsSeek(_fs, _file, position)) {
            return Status::IOError(fmt::format("hdfsSeek failed: {}", std::strerror(errno)));
        }
    }
    int res = hdfsRead(_fs, _file, to, req_n);
    if (-1 == res) {
        return Status::IOError(fmt::format("hdfsRead failed: {}", std::strerror(errno)));
    }
    *read_n = res;
    return Status::OK();
}

Status HdfsReadStream::seek(size_t position) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _file_size) {
        return Status::IOError("Position exceeds range");
    }
    _offset = position;
    return Status::OK();
}

Status HdfsReadStream::tell(size_t* position) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *position = _offset;
    return Status::OK();
}

Status HdfsReadStream::available(size_t* n_bytes) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *n_bytes = _file_size - _offset;
    return Status::OK();
}

Status HdfsReadStream::close() {
    if (!closed()) {
        hdfsCloseFile(_fs, _file);
        _file = nullptr;
        hdfsDisconnect(_fs);
        _fs = nullptr;
        _closed = true;
    }
    return Status::OK();
}

} // namespace doris
