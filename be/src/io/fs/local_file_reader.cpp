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

#include "io/fs/local_file_reader.h"

#include "util/doris_metrics.h"
#include "util/errno.h"

namespace doris {
namespace io {

LocalFileReader::LocalFileReader(Path path, size_t file_size,
                                 std::shared_ptr<OpenedFileHandle<int>> file_handle)
        : _file_handle(std::move(file_handle)),
          _path(std::move(path)),
          _file_size(file_size),
          _closed(false) {
    _fd = *_file_handle->file();
    DorisMetrics::instance()->local_file_open_reading->increment(1);
    DorisMetrics::instance()->local_file_reader_total->increment(1);
}

LocalFileReader::~LocalFileReader() {
    WARN_IF_ERROR(close(), fmt::format("Failed to close file {}", _path.native()));
}

Status LocalFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true)) {
        _file_handle.reset();
        DorisMetrics::instance()->local_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status LocalFileReader::read_at(size_t offset, Slice result, size_t* bytes_read) {
    DCHECK(!_closed.load());
    if (offset > _file_size) {
        return Status::IOError(
                fmt::format("offset exceeds file size(offset: {), file size: {}, path: {})", offset,
                            _file_size, _path.native()));
    }
    size_t bytes_req = result.size;
    char* to = result.data;
    bytes_req = std::min(bytes_req, _file_size - offset);
    *bytes_read = 0;

    while (bytes_req != 0) {
        auto res = ::pread(_fd, to, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            return Status::IOError(
                    fmt::format("cannot read from {}: {}", _path.native(), std::strerror(errno)));
        }
        if (res == 0) {
            return Status::IOError(
                    fmt::format("cannot read from {}: unexpected EOF", _path.native()));
        }
        if (res > 0) {
            to += res;
            offset += res;
            bytes_req -= res;
            *bytes_read += res;
        }
    }
    DorisMetrics::instance()->local_bytes_read_total->increment(*bytes_read);
    return Status::OK();
}

} // namespace io
} // namespace doris
